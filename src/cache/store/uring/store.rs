//! `UringPageStore` — io_uring backend for `PageStore`.
//!
//! Implements the same `PageStore` trait as `LocalPageStore` (tokio::fs
//! backend) but routes all hot-path IO through io_uring SQE/CQE, eliminating
//! `spawn_blocking` overhead.
//!
//! References: Lance `reader.rs:97-292` `UringReader` (extended from read-only
//! to full get/put/delete).
//!
//! See `docs/CLIENT_PAGE_CACHE_DESIGN.md` §3.4.

use super::driver::{submit_request, try_submit_request};
use super::future::UringOpFuture;
use super::requests::{IoRequest, RequestState, UringOpType};
use super::{hash_file_id, io_error, NUM_BUCKETS};
use crate::cache::page_id::PageId;
use crate::cache::store::{LocalPageStore, PageStore};
use crate::error::Result;
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use moka::future::Cache;
use std::ffi::CString;
use std::fs::File;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Sidecar filename holding a file's `(length, mtime)` identity — identical to
/// `LocalPageStore` so both backends share the same on-disk format.
const IDENTITY_FILE: &str = ".identity";

/// Maximum time to wait for a single io_uring operation before declaring
/// it hung. Under normal operation, NVMe IO completes in ~10 µs. The
/// timeout prevents a lost CQE (kernel bug, driver thread panic) from
/// hanging a tokio task forever (H4 fix).
const URING_OP_TIMEOUT: Duration = Duration::from_secs(30);

/// TTL for cached directory fds. Entries not accessed within this window are
/// eligible for lazy eviction on the next `get_dir_fd` cleanup pass.
const DIR_FD_TTL: Duration = Duration::from_secs(300);

/// Soft cap on the directory fd cache. When exceeded, a cleanup pass removes
/// stale (TTL-expired) entries. The cap is generous because the number of
/// unique files in a typical workload is small (1–100).
const DIR_FD_CACHE_SOFT_CAP: usize = 4096;

/// TTL for the page fd cache — entries not accessed within this window
/// are evicted by `moka`. Mirrors Lance's `HANDLE_CACHE` design
/// (`lance-io/src/uring/reader.rs:60`).
const PAGE_FD_CACHE_TTL: Duration = Duration::from_secs(60);

/// Maximum number of cached page fds. moka uses an LRU eviction policy
/// when this cap is exceeded. Mirrors Lance's `HANDLE_CACHE` design
/// (`lance-io/src/uring/reader.rs:61`).
///
/// 10,000 entries × `RawFd` (4 bytes) + `Arc<File>` overhead ≈ 1-2 MB.
/// **Requires `ulimit -n >= 10240`** to avoid `EMFILE` when the cap is
/// reached. Lance's README notes the same constraint.
const PAGE_FD_CACHE_MAX_CAPACITY: u64 = 10_000;

/// A cached directory file descriptor, keyed by `file_id`.
///
/// The directory is `<root>/<bucket>/<file_id>/` — the parent of all page
/// files for that `file_id`. Caching the directory fd lets `get()` use
/// `openat(dirfd, "page_index")` (1-level path resolution) instead of
/// `openat(AT_FDCWD, "<root>/<bucket>/<file_id>/<page_index>")` (4-level),
/// dramatically reducing kernel VFS lock contention under concurrency.
///
/// `last_access` is an `AtomicU64` (epoch nanos) so it can be updated through
/// a `DashMap::get()` **read guard** without taking a write lock — this is
/// critical to avoid blocking tokio workers on the hot path.
struct DirFdEntry {
    fd: RawFd,
    last_access: AtomicU64,
}

impl Drop for DirFdEntry {
    fn drop(&mut self) {
        // Close the fd synchronously — safe because `Drop` runs when the
        // entry is evicted from the DashMap, and no concurrent read holds
        // a reference to the *directory* fd (reads hold the *page* fd,
        // which is opened/closed independently per `get()`).
        unsafe { libc::close(self.fd) };
    }
}

/// Current time as epoch nanos — used for `DirFdEntry::last_access`.
fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Global page fd cache — keyed by `PageId`, holds the open file fd.
///
/// **Mirrors Lance's `HANDLE_CACHE`** (`lance-io/src/uring/reader.rs:55-63`):
/// TTL-based async cache with built-in LRU eviction. Entries expire after
/// `PAGE_FD_CACHE_TTL` (60s) of no access, and LRU eviction kicks in when
/// the cap (`PAGE_FD_CACHE_MAX_CAPACITY`, 10k) is reached.
///
/// **Why process-global?** A file's fd is valid for the entire process —
/// we don't need per-`UringPageStore` caches. This matches Lance's design
/// and lets caches survive `UringPageStore` re-creation (e.g. when the
/// cache directory rotates).
///
/// **Concurrency**: `moka::future::Cache::get` is wait-free in the common
/// case (lock-free hashmap read). `insert` may briefly contend on the
/// LRU list. The cold path (cache miss → `OP_OPENAT` → insert) is bounded
/// by the rate of distinct page accesses.
static PAGE_FD_CACHE: LazyLock<Cache<PageId, Arc<PageFdEntry>>> = LazyLock::new(|| {
    Cache::builder()
        .time_to_live(PAGE_FD_CACHE_TTL)
        .max_capacity(PAGE_FD_CACHE_MAX_CAPACITY)
        .build()
});

/// A cached page file descriptor, keyed by `PageId`.
///
/// **Method E** (mirrors Lance's `UringFileHandle` at
/// `lance-io/src/uring/reader.rs:68-90`): on a cache hit, `get()` uses the
/// cached `RawFd` directly for `OP_READ`, skipping `OP_OPENAT` and
/// `OP_CLOSE` entirely (1 SQE instead of 3). This eliminates the VFS
/// `inode_lock` contention that dominates the miss path under concurrency
/// (result_3: fd_open = 61.3% of total latency).
///
/// `file: Arc<File>` keeps the file alive while the cache holds the entry;
/// when the entry is evicted, `Arc::drop` closes the fd automatically —
/// identical to Lance's pattern.
struct PageFdEntry {
    /// `RawFd` cached at `new()` time to avoid repeated `as_raw_fd()` calls
    /// on the hot path. Valid as long as `file` is alive.
    fd: RawFd,
    /// The open file handle. Holding `Arc<File>` (not just `RawFd`) ensures
    /// the fd is closed only when the cache evicts the entry — not when a
    /// concurrent reader is still using it.
    #[allow(dead_code)]
    file: Arc<File>,
}

impl PageFdEntry {
    /// Wrap a freshly-opened `File` for the cache. The fd is captured once
    /// and reused for all subsequent `OP_READ` calls.
    fn new(file: File) -> Self {
        let fd = file.as_raw_fd();
        Self {
            fd,
            file: Arc::new(file),
        }
    }
}

/// io_uring-backed `PageStore`.
///
/// Disk layout is identical to `LocalPageStore`:
/// `<dir>/<page_size>/<bucket>/<file_id>/<page_index>`
///
/// so both backends can read each other's files (cross-backend compatibility).
///
/// # Read path (page fd cache + dir fd cache)
///
/// Each page is a separate file on disk. Two layers of caching are used:
///
/// 1. **Page fd cache** (Method E, keyed by `PageId`): on a hit, `get()` uses
///    the cached fd directly for `OP_READ` — **1 SQE, no openat**. This is the
///    hot path that eliminates VFS inode_lock contention entirely.
///
/// 2. **Dir fd cache** (Method A, keyed by `file_id`): on a page fd cache
///    miss, the directory fd cache provides `openat(dirfd, "page_index")`
///    (1-level path resolution) instead of `openat(AT_FDCWD, full_path)`
///    (4-level).
///
/// The previous per-page fd cache (old P4) used a `Mutex<LruCache>` with only
/// 1024 entries and achieved ~15% hit rate — it was removed because the
/// `Mutex` blocked tokio workers. Method E uses `DashMap` (lock-free reads)
/// with lazy TTL-based eviction, avoiding both issues.
pub struct UringPageStore {
    /// Root directory: `<dir>/<page_size>`.
    root: PathBuf,
    #[allow(dead_code)]
    page_size: u64,
    /// Directory fd cache: `file_id → DirFdEntry`.
    /// DashMap provides lock-free reads (shard-level RwLock, read-optimised).
    dir_fd_cache: DashMap<Arc<str>, DirFdEntry>,
}

impl UringPageStore {
    /// Create the store and its root directory.
    ///
    /// Directory creation uses `tokio::fs` (not on the hot path).
    pub async fn create(dir: &Path, page_size: u64) -> Result<Self> {
        let root = dir.join(page_size.to_string());
        tokio::fs::create_dir_all(&root)
            .await
            .map_err(|e| io_error(format!("create uring cache dir {}", root.display()), e))?;
        Ok(Self {
            root,
            page_size,
            dir_fd_cache: DashMap::new(),
        })
    }

    /// Page file path: `<root>/<bucket>/<file_id>/<page_index>`.
    /// Identical to `LocalPageStore::page_path`.
    fn page_path(&self, page_id: &PageId) -> PathBuf {
        let bucket = hash_file_id(&page_id.file_id) % NUM_BUCKETS;
        self.root
            .join(bucket.to_string())
            .join(page_id.file_id.as_ref())
            .join(page_id.page_index.to_string())
    }

    /// Parent directory path for a file's pages: `<root>/<bucket>/<file_id>/`.
    fn file_dir_path(&self, file_id: &str) -> PathBuf {
        let bucket = hash_file_id(file_id) % NUM_BUCKETS;
        self.root.join(bucket.to_string()).join(file_id)
    }

    /// Identity sidecar path: `<root>/<bucket>/<file_id>/.identity`.
    /// Identical to `LocalPageStore::identity_path`.
    fn identity_path(&self, file_id: &str) -> PathBuf {
        let bucket = hash_file_id(file_id) % NUM_BUCKETS;
        self.root
            .join(bucket.to_string())
            .join(file_id)
            .join(IDENTITY_FILE)
    }

    // ── Directory fd cache ────────────────────────────────────

    /// Get the parent directory fd for `file_id`, caching it in the lock-free
    /// `DashMap` for subsequent calls.
    ///
    /// **Hot path**: on a cache hit this is a single `DashMap::get` (shard
    /// read-lock, ~10 ns). On a miss it pays one `OP_OPENAT` for the
    /// directory (3-level path), but all subsequent pages of the same file
    /// reuse the cached fd.
    ///
    /// Returns `Ok(dirfd)` on success, or `Err(NotFound)` if the directory
    /// does not exist (meaning no pages are cached for this file).
    async fn get_dir_fd(&self, file_id: &Arc<str>) -> std::io::Result<RawFd> {
        // 1) Lock-free read — the common case (cache hit).
        //    `DashMap::get` takes a per-shard read guard (~10 ns).
        //    The `AtomicU64::store` updates `last_access` without upgrading
        //    to a write guard — this is the key to not blocking tokio workers.
        if let Some(entry) = self.dir_fd_cache.get(file_id) {
            entry.last_access.store(now_nanos(), Ordering::Relaxed);
            return Ok(entry.fd);
        }

        // 2) Cache miss — open the directory via io_uring OP_OPENAT.
        let dir_path = self.file_dir_path(file_id);
        let dirfd = self
            .open_fd(&dir_path, libc::O_RDONLY | libc::O_DIRECTORY)
            .await?;

        // 3) Insert. If another thread won the race, close our redundant fd.
        //    `entry()` takes a per-shard write guard only briefly — this is
        //    the cold path (first access to a file), so it does not contend.
        match self.dir_fd_cache.entry(file_id.clone()) {
            dashmap::mapref::entry::Entry::Occupied(existing) => {
                // Another thread already cached a dir fd — close ours.
                unsafe { libc::close(dirfd) };
                Ok(existing.get().fd)
            }
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                vacant.insert(DirFdEntry {
                    fd: dirfd,
                    last_access: AtomicU64::new(now_nanos()),
                });
                // Best-effort cleanup if the cache has grown large.
                self.maybe_cleanup_dir_fd_cache();
                Ok(dirfd)
            }
        }
    }

    /// Best-effort cleanup of stale directory fd cache entries.
    ///
    /// Called lazily when the cache exceeds `DIR_FD_CACHE_SOFT_CAP`. Removes
    /// entries whose `last_access` is older than `DIR_FD_TTL`. This prevents
    /// unbounded growth from many distinct files without blocking the hot
    /// path (cleanup runs only on the rare size-threshold breach).
    fn maybe_cleanup_dir_fd_cache(&self) {
        if self.dir_fd_cache.len() <= DIR_FD_CACHE_SOFT_CAP {
            return;
        }
        let now = now_nanos();
        let ttl_nanos = DIR_FD_TTL.as_nanos() as u64;
        // Collect stale keys first, then remove — avoids holding DashMap
        // write guards during iteration.
        let stale: Vec<Arc<str>> = self
            .dir_fd_cache
            .iter()
            .filter(|entry| {
                now.saturating_sub(entry.last_access.load(Ordering::Relaxed)) > ttl_nanos
            })
            .map(|entry| entry.key().clone())
            .collect();
        for key in stale {
            self.dir_fd_cache.remove(&key);
        }
    }

    // ── io_uring operation helpers ───────────────────────────

    /// Build a NUL-terminated path buffer for `OP_OPENAT` / `OP_UNLINKAT`.
    ///
    /// io_uring's `openat`/`unlinkat` SQEs take a C string pointer; without the
    /// trailing `\0` the kernel reads past the buffer (undefined behavior /
    /// open failures). Always use [`CString::to_bytes_with_nul`].
    fn path_buffer_with_nul(path: &str) -> std::io::Result<BytesMut> {
        let cstring = CString::new(path)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        Ok(BytesMut::from(cstring.to_bytes_with_nul()))
    }

    /// Asynchronously open a file via `OP_OPENAT` — zero `spawn_blocking`.
    ///
    /// `flags` should include `O_RDONLY` / `O_WRONLY` etc. `O_CLOEXEC` is
    /// added automatically. Returns the raw fd.
    async fn open_fd(&self, path: &Path, flags: i32) -> std::io::Result<RawFd> {
        let buffer = Self::path_buffer_with_nul(&path.to_string_lossy())?;

        let request = Arc::new(IoRequest {
            fd: libc::AT_FDCWD,
            offset: 0,
            length: 0,
            op_type: UringOpType::OpenAt,
            open_flags: flags,
            state: std::sync::Mutex::new(RequestState {
                completed: false,
                waker: None,
                err: None,
                buffer,
                bytes_transferred: 0,
                consumed: false,
                result_code: 0,
            }),
        });

        submit_request(Arc::clone(&request));
        let (result, _bytes) =
            match tokio::time::timeout(URING_OP_TIMEOUT, UringOpFuture { request }).await {
                Ok(res) => res,
                Err(_) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "io_uring open timed out",
                    ));
                }
            };

        if result < 0 {
            Err(std::io::Error::from_raw_os_error(-result))
        } else {
            Ok(result as RawFd)
        }
    }

    /// Asynchronously open a file relative to a directory fd via `OP_OPENAT`.
    ///
    /// Unlike `open_fd` (which uses `AT_FDCWD` + full path), this uses a
    /// cached directory fd (`dirfd`) and a relative name (e.g. `"42"` for
    /// page index 42). The kernel resolves only 1 path component instead of
    /// 4, avoiding 3 levels of dcache/inode lock contention under concurrency.
    ///
    /// `flags` should include `O_RDONLY` etc. `O_CLOEXEC` is added automatically.
    async fn openat_relative(
        &self,
        dirfd: RawFd,
        name: &str,
        flags: i32,
    ) -> std::io::Result<RawFd> {
        let buffer = Self::path_buffer_with_nul(name)?;

        let request = Arc::new(IoRequest {
            fd: dirfd,
            offset: 0,
            length: 0,
            op_type: UringOpType::OpenAt,
            open_flags: flags,
            state: std::sync::Mutex::new(RequestState {
                completed: false,
                waker: None,
                err: None,
                buffer,
                bytes_transferred: 0,
                consumed: false,
                result_code: 0,
            }),
        });

        submit_request(Arc::clone(&request));
        let (result, _bytes) =
            match tokio::time::timeout(URING_OP_TIMEOUT, UringOpFuture { request }).await {
                Ok(res) => res,
                Err(_) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "io_uring openat_relative timed out",
                    ));
                }
            };

        if result < 0 {
            Err(std::io::Error::from_raw_os_error(-result))
        } else {
            Ok(result as RawFd)
        }
    }

    /// Close an fd via `OP_CLOSE` — fire-and-forget (no await).
    ///
    /// The SQE is submitted to the io_uring ring; the kernel closes the fd
    /// asynchronously. This eliminates the third round-trip in `get()`
    /// (H3 fix). If submission fails (channel full or disconnected),
    /// falls back to synchronous `libc::close` to prevent fd leaks (H5 fix).
    fn close_fd_background(&self, fd: RawFd) {
        let request = Arc::new(IoRequest {
            fd,
            offset: 0,
            length: 0,
            op_type: UringOpType::Close,
            open_flags: 0,
            state: std::sync::Mutex::new(RequestState {
                completed: false,
                waker: None,
                err: None,
                buffer: BytesMut::new(),
                bytes_transferred: 0,
                consumed: false,
                result_code: 0,
            }),
        });
        if !try_submit_request(request) {
            // Channel full/disconnected — close synchronously to prevent fd leak.
            unsafe { libc::close(fd) };
        }
    }

    /// Asynchronously unlink a file via `OP_UNLINKAT`.
    /// Returns `Ok(())` if the file does not exist (idempotent).
    async fn unlink_path(&self, path: &Path) -> std::io::Result<()> {
        let buffer = Self::path_buffer_with_nul(&path.to_string_lossy())?;

        let request = Arc::new(IoRequest {
            fd: libc::AT_FDCWD,
            offset: 0,
            length: 0,
            op_type: UringOpType::UnlinkAt,
            open_flags: 0,
            state: std::sync::Mutex::new(RequestState {
                completed: false,
                waker: None,
                err: None,
                buffer,
                bytes_transferred: 0,
                consumed: false,
                result_code: 0,
            }),
        });

        submit_request(Arc::clone(&request));
        let (result, _) =
            match tokio::time::timeout(URING_OP_TIMEOUT, UringOpFuture { request }).await {
                Ok(res) => res,
                Err(_) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "io_uring unlink timed out",
                    ));
                }
            };

        if result < 0 {
            let e = std::io::Error::from_raw_os_error(-result);
            if e.kind() == std::io::ErrorKind::NotFound {
                return Ok(()); // idempotent
            }
            return Err(e);
        }
        Ok(())
    }

    /// Build a read request for an already-opened fd.
    fn new_read_request(fd: RawFd, offset: usize, len: usize) -> Arc<IoRequest> {
        let mut buffer = BytesMut::with_capacity(len);
        // SAFETY: buffer has capacity for `len` bytes; io_uring writes into it
        // before the future exposes it as `Bytes`.
        unsafe {
            buffer.set_len(len);
        }

        Arc::new(IoRequest {
            fd,
            offset: offset as u64,
            length: len,
            op_type: UringOpType::Read,
            open_flags: 0,
            state: std::sync::Mutex::new(RequestState {
                completed: false,
                waker: None,
                err: None,
                buffer,
                bytes_transferred: 0,
                consumed: false,
                result_code: 0,
            }),
        })
    }

    /// Await a submitted read request and return the kernel-filled buffer.
    async fn wait_read_request(request: Arc<IoRequest>) -> std::io::Result<Bytes> {
        let (result, read_bytes) =
            match tokio::time::timeout(URING_OP_TIMEOUT, UringOpFuture { request }).await {
                Ok(res) => res,
                Err(_) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "io_uring read timed out",
                    ));
                }
            };

        if result < 0 {
            Err(std::io::Error::from_raw_os_error(-result))
        } else {
            Ok(read_bytes)
        }
    }

    /// Read from an already-opened fd via io_uring `OP_READ` and return the
    /// kernel-filled buffer directly.
    async fn read_with_fd(&self, fd: RawFd, offset: usize, len: usize) -> std::io::Result<Bytes> {
        let request = Self::new_read_request(fd, offset, len);
        submit_request(Arc::clone(&request));
        Self::wait_read_request(request).await
    }

    /// Compatibility wrapper for existing tests/callers that provide output
    /// buffers. The cache hot path now gets concurrency from
    /// `LocalCacheManager::get_batch_bytes` + `join_all`, so we keep this
    /// method small and avoid a second, unused batch API layer.
    pub async fn get_batch(
        &self,
        requests: Vec<(PageId, usize, usize)>,
        results: Vec<&mut [u8]>,
    ) -> Result<()> {
        assert_eq!(
            requests.len(),
            results.len(),
            "requests and results must have the same length"
        );
        for ((page_id, offset, len), dst) in requests.into_iter().zip(results.into_iter()) {
            let bytes = self.get_bytes(&page_id, offset, len).await?;
            let n = bytes.len().min(dst.len());
            if n > 0 {
                dst[..n].copy_from_slice(&bytes[..n]);
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl PageStore for UringPageStore {
    /// Read a page via directory fd cache + `OP_OPENAT` + `OP_READ` + `OP_CLOSE`.
    ///
    /// The directory fd cache (keyed by `file_id`) eliminates 3 of 4 levels
    /// of kernel VFS path resolution on every `get`. On a cache hit:
    ///
    /// 1. `DashMap::get(file_id)` → returns cached `dirfd` (lock-free read,
    ///    ~10 ns, no tokio worker blocking).
    /// 2. `OP_OPENAT(dirfd, "page_index")` → 1-level path resolution.
    /// 3. `OP_READ(fd, offset, len)` → pread via io_uring.
    /// 4. `OP_CLOSE(fd)` → fire-and-forget.
    ///
    /// On a dir fd cache miss (first read of a new file), step 1 falls back
    /// to `OP_OPENAT(AT_FDCWD, "<root>/<bucket>/<file_id>")` (3-level), but
    /// all subsequent reads of any page in the same file reuse the cached
    /// `dirfd` — giving ~100% hit rate for workloads that read multiple pages
    /// per file (which is always the case for Lance columnar reads).
    ///
    /// **Concurrency**: `DashMap` uses per-shard RwLocks. The hot path
    /// (`get` → `get_dir_fd` cache hit) takes only a **read** guard and
    /// updates `last_access` via `AtomicU64` — no write lock, no tokio
    /// worker blocking. The cold path (cache miss → `entry()`) takes a brief
    /// write guard but only on the first access to each file.
    ///
    /// See `docs/perf/2026-07-09-oncpu6-concurrent-uring-analysis/README.md`
    /// for the concurrency analysis that motivated this design.
    /// Read a page via **page fd cache** (Method E) → dir fd cache fallback.
    ///
    /// **Hot path (page fd cache hit)** — 1 SQE:
    /// 1. `DashMap::get(page_id)` → returns cached `fd` (lock-free read).
    /// 2. `OP_READ(fd, offset, len)` → pread via io_uring.
    ///
    /// No `OP_OPENAT`, no `OP_CLOSE` — the fd is reused across reads. This
    /// eliminates VFS inode_lock contention entirely on the hot path.
    ///
    /// **Cold path (page fd cache miss)** — 3 SQEs (same as Method A):
    /// 1. `get_dir_fd(file_id)` → cached dirfd (1-level openat on miss).
    /// 2. `OP_OPENAT(dirfd, "page_index")` → 1-level path resolution.
    /// 3. `OP_READ(fd, offset, len)` → pread via io_uring.
    /// 4. Insert into `page_fd_cache` for future hits.
    /// 5. `OP_CLOSE` is **not** done — the fd is kept in the cache.
    ///
    /// **Concurrency**: same lock-free `DashMap` read guard pattern as
    /// `get_dir_fd`. `last_access` updated via `AtomicU64` — no write lock.
    ///
    /// See `docs/perf/2026-07-09-oncpu6-concurrent-uring-analysis/README.md`
    /// §8.5 and Method E for the analysis that motivated this design.
    async fn get(&self, page_id: &PageId, offset: usize, dst: &mut [u8]) -> Result<usize> {
        let bytes = self.get_bytes(page_id, offset, dst.len()).await?;
        let n = bytes.len().min(dst.len());
        if n > 0 {
            dst[..n].copy_from_slice(&bytes[..n]);
        }
        Ok(n)
    }

    async fn get_bytes(&self, page_id: &PageId, offset: usize, len: usize) -> Result<Bytes> {
        if len == 0 {
            return Ok(Bytes::new());
        }

        // ── Hot path: page fd cache hit → 1 SQE (OP_READ only) ───────
        if let Some(entry) = PAGE_FD_CACHE.get(page_id).await {
            let fd = entry.fd;
            // `entry: Arc<PageFdEntry>` keeps the underlying `Arc<File>` alive
            // for the duration of the read, so the fd is guaranteed valid.
            let _entry = entry;

            return match self.read_with_fd(fd, offset, len).await {
                Ok(bytes) => Ok(bytes),
                Err(e) => {
                    PAGE_FD_CACHE.invalidate(page_id).await;
                    if e.kind() == std::io::ErrorKind::NotFound {
                        Ok(Bytes::new())
                    } else {
                        Err(io_error("uring read (page fd cache hit)", e))
                    }
                }
            };
        }

        // ── Cold path: page fd cache miss → dir fd cache + openat + read ─
        let dirfd = match self.get_dir_fd(&page_id.file_id).await {
            Ok(fd) => fd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Bytes::new()),
            Err(e) => return Err(io_error("uring open dir", e)),
        };

        let page_name = page_id.page_index.to_string();
        let fd = match self
            .openat_relative(dirfd, &page_name, libc::O_RDONLY)
            .await
        {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Bytes::new()),
            Err(e) => return Err(io_error("uring open page", e)),
        };

        let read_bytes = match self.read_with_fd(fd, offset, len).await {
            Ok(bytes) => bytes,
            Err(e) => {
                self.close_fd_background(fd);
                if e.kind() == std::io::ErrorKind::NotFound {
                    return Ok(Bytes::new());
                }
                return Err(io_error("uring read", e));
            }
        };

        // SAFETY: `fd` was just successfully opened by io_uring and ownership
        // is transferred into `File`; moka closes it when the cache entry drops.
        let file = unsafe { std::fs::File::from_raw_fd(fd) };
        PAGE_FD_CACHE
            .insert(page_id.clone(), Arc::new(PageFdEntry::new(file)))
            .await;

        Ok(read_bytes)
    }

    /// Write a page — `OP_OPENAT` + `OP_WRITE` + `OP_CLOSE` + `rename`.
    ///
    /// Uses the atomic `tmp + rename` pattern identical to `LocalPageStore`.
    /// `rename` uses `std::fs::rename` (sync) because it is NOT on the cache
    /// hit hot path — it only runs on cache miss fill.
    ///
    /// See design §3.4.
    async fn put(&self, page_id: &PageId, page: &[u8]) -> Result<()> {
        let final_path = self.page_path(page_id);
        let parent = final_path
            .parent()
            .expect("page path always has a parent")
            .to_path_buf();

        // Directory creation is not on the hot path — use tokio::fs.
        tokio::fs::create_dir_all(&parent)
            .await
            .map_err(|e| io_error("create page dir", e))?;

        let tmp_path = parent.join(format!(
            "{}.tmp-{}",
            page_id.page_index,
            uuid::Uuid::new_v4()
        ));
        let tmp_buffer = Self::path_buffer_with_nul(&tmp_path.to_string_lossy())
            .map_err(|e| io_error("cstring", e))?;

        // 1) OP_OPENAT (O_WRONLY | O_CREAT | O_TRUNC)
        let fd = {
            let request = Arc::new(IoRequest {
                fd: libc::AT_FDCWD,
                offset: 0,
                length: 0,
                op_type: UringOpType::OpenAt,
                open_flags: libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC,
                state: std::sync::Mutex::new(RequestState {
                    completed: false,
                    waker: None,
                    err: None,
                    buffer: tmp_buffer,
                    bytes_transferred: 0,
                    consumed: false,
                    result_code: 0,
                }),
            });
            submit_request(Arc::clone(&request));
            let (result, _) =
                match tokio::time::timeout(URING_OP_TIMEOUT, UringOpFuture { request }).await {
                    Ok(res) => res,
                    Err(_) => {
                        return Err(io_error(
                            "uring open tmp timeout",
                            std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "io_uring open timed out",
                            ),
                        ));
                    }
                };
            if result < 0 {
                let e = std::io::Error::from_raw_os_error(-result);
                return Err(io_error("uring open tmp", e));
            }
            result as RawFd
        };

        // 2) OP_WRITE (entire page)
        {
            let request = Arc::new(IoRequest {
                fd,
                offset: 0,
                length: page.len(),
                op_type: UringOpType::Write,
                open_flags: 0,
                state: std::sync::Mutex::new(RequestState {
                    completed: false,
                    waker: None,
                    err: None,
                    buffer: BytesMut::from(page),
                    bytes_transferred: 0,
                    consumed: false,
                    result_code: 0,
                }),
            });
            submit_request(Arc::clone(&request));
            let (result, _) =
                match tokio::time::timeout(URING_OP_TIMEOUT, UringOpFuture { request }).await {
                    Ok(res) => res,
                    Err(_) => {
                        self.close_fd_background(fd);
                        return Err(io_error(
                            "uring write timeout",
                            std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "io_uring write timed out",
                            ),
                        ));
                    }
                };
            if result < 0 {
                self.close_fd_background(fd);
                let e = std::io::Error::from_raw_os_error(-result);
                return Err(io_error("uring write", e));
            }
        }

        // 3) OP_CLOSE — fire-and-forget (H3 fix).
        self.close_fd_background(fd);

        // 4) rename — async to avoid blocking the tokio worker (H2 fix).
        //    POSIX atomic rename on NVMe is ~5 µs.
        let rename_result = tokio::fs::rename(&tmp_path, &final_path).await;
        if rename_result.is_err() {
            // Best-effort cleanup of the temp file.
            let _ = tokio::fs::remove_file(&tmp_path).await;
        }
        rename_result.map_err(|e| io_error("rename temp page file", e))?;

        // Invalidate the global page fd cache entry — the atomic rename
        // replaced the page file, so any cached fd points to the old
        // inode. Must be removed so the next get() re-opens the new file.
        PAGE_FD_CACHE.invalidate(page_id).await;

        Ok(())
    }

    /// Delete a page — `OP_UNLINKAT`.
    ///
    /// The page file is unlinked relative to the full path (via `AT_FDCWD`).
    /// The directory fd cache entry for the file is **not** invalidated here
    /// because the directory itself still exists — only the page file inside
    /// it was removed. The dir fd remains valid for future `get` calls that
    /// read other pages of the same file.
    ///
    /// On Unix, unlinking a file with open fds is safe — the inode survives
    /// until all fds close, so any concurrent read completes normally.
    ///
    /// Compared to `LocalPageStore::delete` (1 × `spawn_blocking`), this uses
    /// 1 io_uring SQE. Returns `Ok(())` if the file does not exist.
    async fn delete(&self, page_id: &PageId) -> Result<()> {
        // Invalidate the global page fd cache entry BEFORE unlinking. The
        // fd in the cache points to the inode that will be unlinked — we
        // must close it so the inode can be freed. Concurrent reads using
        // the old fd are safe on Unix (inode survives until last close),
        // but we remove the cache entry so future reads go through the
        // miss path. `invalidate` is async (moka requires a future).
        PAGE_FD_CACHE.invalidate(page_id).await;

        let path = self.page_path(page_id);
        self.unlink_path(&path)
            .await
            .map_err(|e| io_error("uring unlink", e))?;
        Ok(())
    }

    // ── Identity sidecar (not on the hot path — uses tokio::fs) ──

    fn root_dir(&self) -> &Path {
        &self.root
    }

    async fn write_identity(&self, file_id: &str, length: i64, mtime: i64) -> Result<()> {
        let final_path = self.identity_path(file_id);
        let parent = final_path
            .parent()
            .expect("identity path always has a parent")
            .to_path_buf();
        tokio::fs::create_dir_all(&parent)
            .await
            .map_err(|e| io_error(format!("create identity dir {}", parent.display()), e))?;
        let tmp_path = parent.join(format!("{}.tmp-{}", IDENTITY_FILE, uuid::Uuid::new_v4()));
        let contents = format!("{length},{mtime}");
        let write_result = async {
            tokio::fs::write(&tmp_path, contents.as_bytes())
                .await
                .map_err(|e| io_error("write temp identity file", e))?;
            tokio::fs::rename(&tmp_path, &final_path)
                .await
                .map_err(|e| io_error("rename temp identity file", e))?;
            Ok::<(), crate::error::Error>(())
        }
        .await;
        if write_result.is_err() {
            let _ = tokio::fs::remove_file(&tmp_path).await;
        }
        write_result
    }

    async fn read_identity(&self, file_id: &str) -> Option<(i64, i64)> {
        let path = self.identity_path(file_id);
        let contents = tokio::fs::read_to_string(&path).await.ok()?;
        // Shared parser with LocalPageStore so both backends accept the same
        // sidecar format (and share unit-test coverage).
        LocalPageStore::parse_identity(&contents)
    }

    async fn delete_identity(&self, file_id: &str) -> Result<()> {
        let path = self.identity_path(file_id);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io_error("delete identity file", e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn temp_store(page_size: u64) -> (UringPageStore, PathBuf) {
        let base = std::env::temp_dir().join(format!("gfs_uring_test_{}", uuid::Uuid::new_v4()));
        let store = UringPageStore::create(&base, page_size).await.unwrap();
        (store, base)
    }

    #[tokio::test]
    async fn uring_put_get_roundtrip() {
        let (store, base) = temp_store(1024).await;
        let id = PageId::new("file-uring-a", 0);
        let data = b"hello uring page cache".to_vec();

        store.put(&id, &data).await.unwrap();

        let mut dst = vec![0u8; data.len()];
        let n = store.get(&id, 0, &mut dst).await.unwrap();
        assert_eq!(n, data.len());
        assert_eq!(&dst, &data);

        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn uring_get_with_offset() {
        let (store, base) = temp_store(1024).await;
        let id = PageId::new("file-uring-b", 0);
        store.put(&id, b"0123456789").await.unwrap();

        let mut dst = vec![0u8; 4];
        let n = store.get(&id, 3, &mut dst).await.unwrap();
        assert_eq!(n, 4);
        assert_eq!(&dst, b"3456");

        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn uring_get_missing_returns_zero() {
        let (store, base) = temp_store(1024).await;
        let id = PageId::new("nope-uring", 0);
        let mut dst = vec![0u8; 8];
        assert_eq!(store.get(&id, 0, &mut dst).await.unwrap(), 0);
        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn uring_get_short_read_at_tail() {
        let (store, base) = temp_store(1024).await;
        let id = PageId::new("file-uring-c", 0);
        store.put(&id, b"abc").await.unwrap();

        // Ask for more than the page holds → fills only the available bytes.
        let mut dst = vec![0u8; 16];
        let n = store.get(&id, 0, &mut dst).await.unwrap();
        assert_eq!(n, 3);
        assert_eq!(&dst[..3], b"abc");
        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn uring_delete_then_miss() {
        let (store, base) = temp_store(1024).await;
        let id = PageId::new("file-uring-d", 1);
        store.put(&id, b"data").await.unwrap();
        store.delete(&id).await.unwrap();

        let mut dst = vec![0u8; 4];
        assert_eq!(store.get(&id, 0, &mut dst).await.unwrap(), 0);
        // Deleting again is a no-op.
        store.delete(&id).await.unwrap();
        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn uring_concurrent_get_same_page() {
        let (store, base) = temp_store(1024).await;
        let id = PageId::new("file-uring-conc", 0);
        let data = vec![0x42u8; 64];
        store.put(&id, &data).await.unwrap();

        // 32 concurrent reads of the same page.
        let store = Arc::new(store);
        let mut handles = Vec::new();
        for _ in 0..32 {
            let store = Arc::clone(&store);
            let id = id.clone();
            handles.push(tokio::spawn(async move {
                let mut dst = vec![0u8; 64];
                let n = store.get(&id, 0, &mut dst).await.unwrap();
                assert_eq!(n, 64);
                assert_eq!(dst, vec![0x42u8; 64]);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn uring_repeated_reads() {
        // Repeated reads of the same page should all succeed. The directory
        // fd cache means the first read opens the directory, subsequent reads
        // reuse the cached dirfd (lock-free DashMap lookup) + 1-level
        // openat for the page file.
        let (store, base) = temp_store(1024).await;
        let id = PageId::new("file-repeat", 0);
        store.put(&id, b"repeated-read-data").await.unwrap();

        for _ in 0..10 {
            let mut dst = vec![0u8; 18];
            let n = store.get(&id, 0, &mut dst).await.unwrap();
            assert_eq!(n, 18);
            assert_eq!(&dst, b"repeated-read-data");
        }
        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn uring_dir_fd_cache_reused_across_pages() {
        // Multiple pages of the same file share the cached directory fd.
        // After the first get, subsequent gets for different page indices
        // reuse the cached dirfd — only 1-level openat is needed.
        let (store, base) = temp_store(1024).await;
        let id0 = PageId::new("file-multi", 0);
        let id1 = PageId::new("file-multi", 1);
        let id2 = PageId::new("file-multi", 2);

        store.put(&id0, b"page-zero-data!!").await.unwrap();
        store.put(&id1, b"page-one-data!!!").await.unwrap();
        store.put(&id2, b"page-two-data!!!").await.unwrap();

        // Read all three pages — the dir fd cache should be populated on
        // the first get and reused for the other two.
        let mut dst = vec![0u8; 16];
        assert_eq!(store.get(&id0, 0, &mut dst).await.unwrap(), 16);
        assert_eq!(&dst, b"page-zero-data!!");
        assert_eq!(store.get(&id1, 0, &mut dst).await.unwrap(), 16);
        assert_eq!(&dst, b"page-one-data!!!");
        assert_eq!(store.get(&id2, 0, &mut dst).await.unwrap(), 16);
        assert_eq!(&dst, b"page-two-data!!!");

        // Verify the dir fd cache has exactly one entry for this file.
        assert_eq!(store.dir_fd_cache.len(), 1);

        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn uring_dir_fd_cache_concurrent_different_pages() {
        // 32 concurrent reads of different pages from the same file.
        // This tests the DashMap's concurrency: all tasks share one dirfd
        // entry (lock-free reads), each does its own 1-level openat + read
        // + close. No tokio worker should be blocked.
        let (store, base) = temp_store(1024).await;
        let file_id: Arc<str> = Arc::from("file-conc-multi");

        // Write 32 pages.
        for i in 0..32u64 {
            let id = PageId::new(file_id.clone(), i);
            store.put(&id, &[i as u8; 8]).await.unwrap();
        }

        let store = Arc::new(store);
        let mut handles = Vec::new();
        for i in 0..32u64 {
            let store = Arc::clone(&store);
            let file_id = file_id.clone();
            handles.push(tokio::spawn(async move {
                let id = PageId::new(file_id, i);
                let mut dst = vec![0u8; 8];
                let n = store.get(&id, 0, &mut dst).await.unwrap();
                assert_eq!(n, 8);
                assert_eq!(dst, vec![i as u8; 8]);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // After 32 concurrent reads, the dir fd cache should still have
        // exactly one entry for this file (all tasks shared it).
        assert_eq!(store.dir_fd_cache.len(), 1);

        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn uring_get_batch_concurrent() {
        // Phase D: verify get_batch reads multiple pages concurrently and
        // returns the correct data for each.
        let (store, base) = temp_store(1024).await;
        let file_id: Arc<str> = Arc::from("file-batch");

        // Write 8 pages of 16 bytes each with distinct content.
        let n_pages = 8u64;
        for i in 0..n_pages {
            let id = PageId::new(file_id.clone(), i);
            let data: Vec<u8> = (0..16u8).map(|b| b.wrapping_add(i as u8)).collect();
            store.put(&id, &data).await.unwrap();
        }

        // Batch read all 8 pages.
        let requests: Vec<(PageId, usize, usize)> = (0..n_pages)
            .map(|i| (PageId::new(file_id.clone(), i), 0, 16))
            .collect();
        let mut bufs: Vec<Vec<u8>> = (0..n_pages).map(|_| vec![0u8; 16]).collect();
        let results: Vec<&mut [u8]> = bufs.iter_mut().map(|b| b.as_mut_slice()).collect();

        store
            .get_batch(requests, results)
            .await
            .expect("batch read should succeed");

        // Verify each result matches the expected data.
        for (i, buf) in bufs.iter().enumerate() {
            let expected: Vec<u8> = (0..16u8).map(|b| b.wrapping_add(i as u8)).collect();
            assert_eq!(buf, &expected, "page {i} data mismatch");
        }

        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn uring_get_batch_multi_file() {
        // Phase D: verify get_batch groups requests by file_id and uses the
        // per-file dirfd cache.
        let (store, base) = temp_store(1024).await;
        let file_a: Arc<str> = Arc::from("file-a");
        let file_b: Arc<str> = Arc::from("file-b");

        for i in 0..3u64 {
            store
                .put(&PageId::new(file_a.clone(), i), &[0xA0 + i as u8; 8])
                .await
                .unwrap();
            store
                .put(&PageId::new(file_b.clone(), i), &[0xB0 + i as u8; 8])
                .await
                .unwrap();
        }

        // Mixed batch: 2 pages from file_a, 2 from file_b.
        let requests = vec![
            (PageId::new(file_a.clone(), 0), 0, 8),
            (PageId::new(file_b.clone(), 0), 0, 8),
            (PageId::new(file_a.clone(), 1), 0, 8),
            (PageId::new(file_b.clone(), 1), 0, 8),
        ];
        let mut bufs: Vec<Vec<u8>> = (0..4).map(|_| vec![0u8; 8]).collect();
        let results: Vec<&mut [u8]> = bufs.iter_mut().map(|b| b.as_mut_slice()).collect();

        store.get_batch(requests, results).await.unwrap();

        assert_eq!(
            bufs[0],
            vec![0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0, 0xA0]
        );
        assert_eq!(
            bufs[1],
            vec![0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0, 0xB0]
        );
        assert_eq!(
            bufs[2],
            vec![0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1, 0xA1]
        );
        assert_eq!(
            bufs[3],
            vec![0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1, 0xB1]
        );

        // Both files should have a dirfd cache entry.
        assert_eq!(store.dir_fd_cache.len(), 2);

        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn uring_identity_roundtrip() {
        let (store, base) = temp_store(1024).await;
        store
            .write_identity("file-id-1", 4096, 1_700_000_000_000)
            .await
            .unwrap();
        let identity = store.read_identity("file-id-1").await;
        assert_eq!(identity, Some((4096, 1_700_000_000_000)));

        store.delete_identity("file-id-1").await.unwrap();
        assert_eq!(store.read_identity("file-id-1").await, None);

        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    // ── Method E: page fd cache tests (moka-based) ─────────

    /// Helper: get a unique `PageId` for each test to avoid moka cache
    /// collisions across tests (the cache is process-global).
    fn unique_id(label: &str, idx: u64) -> PageId {
        PageId::new(format!("file-{label}-{idx}-test"), idx)
    }

    /// Helper: check if a `PageId` is in the page fd cache.
    async fn is_in_page_cache(id: &PageId) -> bool {
        PAGE_FD_CACHE.get(id).await.is_some()
    }

    #[tokio::test]
    async fn uring_page_fd_cache_hit_after_first_read() {
        // First get() opens the file (miss) → inserts into PAGE_FD_CACHE.
        // Second get() should hit (1 SQE, no openat).
        let (store, base) = temp_store(1024).await;
        let id = unique_id("pgcache-hit", 0);
        let data = b"page fd cache test data".to_vec();

        store.put(&id, &data).await.unwrap();

        // First read — should populate the page fd cache.
        let mut dst = vec![0u8; data.len()];
        let n1 = store.get(&id, 0, &mut dst).await.unwrap();
        assert_eq!(n1, data.len());
        assert_eq!(&dst, &data);
        assert!(
            is_in_page_cache(&id).await,
            "page fd cache should contain entry after first read"
        );

        // Second read — should hit the page fd cache.
        let mut dst2 = vec![0u8; data.len()];
        let n2 = store.get(&id, 0, &mut dst2).await.unwrap();
        assert_eq!(n2, data.len());
        assert_eq!(&dst2, &data);
        assert!(
            is_in_page_cache(&id).await,
            "page fd cache should still contain entry (reuse)"
        );

        // Cleanup: invalidate cache entry so other tests don't see it.
        PAGE_FD_CACHE.invalidate(&id).await;
        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn uring_page_fd_cache_different_pages() {
        // Each page gets its own page fd cache entry.
        let (store, base) = temp_store(1024).await;
        let id0 = unique_id("multi-pg", 0);
        let id1 = unique_id("multi-pg", 1);
        let id2 = unique_id("multi-pg", 2);

        store.put(&id0, b"page-zero!!").await.unwrap();
        store.put(&id1, b"page-one!!").await.unwrap();
        store.put(&id2, b"page-two!!").await.unwrap();

        // Read all three pages — each should populate the page fd cache.
        let mut dst = vec![0u8; 10];
        assert_eq!(store.get(&id0, 0, &mut dst).await.unwrap(), 10);
        assert_eq!(&dst, b"page-zero!!");
        assert_eq!(store.get(&id1, 0, &mut dst).await.unwrap(), 10);
        assert_eq!(&dst, b"page-one!!");
        assert_eq!(store.get(&id2, 0, &mut dst).await.unwrap(), 10);
        assert_eq!(&dst, b"page-two!!");

        // 3 page fd entries + 1 dir fd entry.
        assert!(is_in_page_cache(&id0).await);
        assert!(is_in_page_cache(&id1).await);
        assert!(is_in_page_cache(&id2).await);
        assert_eq!(store.dir_fd_cache.len(), 1);

        // Re-read all three — should all be page fd cache hits.
        assert_eq!(store.get(&id0, 0, &mut dst).await.unwrap(), 10);
        assert_eq!(&dst, b"page-zero!!");
        assert_eq!(store.get(&id1, 0, &mut dst).await.unwrap(), 10);
        assert_eq!(&dst, b"page-one!!");
        assert_eq!(store.get(&id2, 0, &mut dst).await.unwrap(), 10);
        assert_eq!(&dst, b"page-two!!");

        // Cleanup.
        for id in [&id0, &id1, &id2] {
            PAGE_FD_CACHE.invalidate(id).await;
        }
        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn uring_page_fd_cache_concurrent_same_page() {
        // 32 concurrent reads of the same page — first one populates the
        // cache, the rest should hit (or race with the miss path).
        let (store, base) = temp_store(1024).await;
        let id = unique_id("conc-pgcache", 0);
        let data = vec![0xABu8; 64];
        store.put(&id, &data).await.unwrap();

        let store = Arc::new(store);
        let mut handles = Vec::new();
        for _ in 0..32 {
            let store = Arc::clone(&store);
            let id = id.clone();
            handles.push(tokio::spawn(async move {
                let mut dst = vec![0u8; 64];
                let n = store.get(&id, 0, &mut dst).await.unwrap();
                assert_eq!(n, 64);
                assert_eq!(dst, vec![0xABu8; 64]);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // After 32 concurrent reads, the page fd cache should have an entry.
        assert!(
            is_in_page_cache(&id).await,
            "page fd cache should contain entry after concurrent reads"
        );

        // Cleanup.
        PAGE_FD_CACHE.invalidate(&id).await;
        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn uring_page_fd_cache_invalidation_on_delete() {
        // After delete(), the page fd cache entry should be removed.
        // A subsequent get() should return 0 (miss).
        let (store, base) = temp_store(1024).await;
        let id = unique_id("invalidate-del", 0);
        store.put(&id, b"will-be-deleted").await.unwrap();

        // Populate the page fd cache.
        let mut dst = vec![0u8; 16];
        assert_eq!(store.get(&id, 0, &mut dst).await.unwrap(), 16);
        assert!(is_in_page_cache(&id).await);

        // Delete should invalidate the cache entry.
        store.delete(&id).await.unwrap();
        assert!(
            !is_in_page_cache(&id).await,
            "page fd cache should be empty after delete"
        );

        // Subsequent get should miss (return 0).
        let n = store.get(&id, 0, &mut dst).await.unwrap();
        assert_eq!(n, 0, "get should return 0 after delete");

        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn uring_page_fd_cache_invalidation_on_put_overwrite() {
        // After put() overwrites a page, the page fd cache entry should be
        // invalidated so the next get() reads the new data.
        let (store, base) = temp_store(1024).await;
        let id = unique_id("overwrite", 0);

        store.put(&id, b"old-data-here!").await.unwrap();
        let mut dst = vec![0u8; 14];
        assert_eq!(store.get(&id, 0, &mut dst).await.unwrap(), 14);
        assert_eq!(&dst, b"old-data-here!");
        assert!(is_in_page_cache(&id).await);

        // Overwrite — put() should invalidate the cache entry.
        store.put(&id, b"new-data-here!!").await.unwrap();
        assert!(
            !is_in_page_cache(&id).await,
            "page fd cache should be empty after overwrite"
        );

        // Next get() should read the new data (re-open the file).
        let mut dst2 = vec![0u8; 15];
        assert_eq!(store.get(&id, 0, &mut dst2).await.unwrap(), 15);
        assert_eq!(&dst2, b"new-data-here!!");

        // Cleanup.
        PAGE_FD_CACHE.invalidate(&id).await;
        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[test]
    fn path_buffer_with_nul_terminates_and_rejects_interior_nul() {
        let buf = UringPageStore::path_buffer_with_nul("/tmp/page-42").unwrap();
        assert_eq!(
            *buf.last().unwrap(),
            0,
            "OP_OPENAT/OP_UNLINKAT path buffer must end with NUL"
        );
        assert_eq!(&buf[..buf.len() - 1], b"/tmp/page-42");

        let err = UringPageStore::path_buffer_with_nul("bad\0name").unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn uring_get_bytes_returns_page_slice() {
        let (store, base) = temp_store(1024).await;
        let id = unique_id("get-bytes", 0);
        store.put(&id, b"0123456789").await.unwrap();

        let bytes = store.get_bytes(&id, 2, 5).await.unwrap();
        assert_eq!(&bytes[..], b"23456");

        let missing = store
            .get_bytes(&unique_id("missing-bytes", 0), 0, 8)
            .await
            .unwrap();
        assert!(missing.is_empty());

        PAGE_FD_CACHE.invalidate(&id).await;
        let _ = tokio::fs::remove_dir_all(&base).await;
    }
}
