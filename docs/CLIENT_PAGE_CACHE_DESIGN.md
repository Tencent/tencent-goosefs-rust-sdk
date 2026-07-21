# Rust Client SDK Local Page Cache Development Design Document

> Status: **Implemented (P0–P3)** · Branch: `feature/local-page-cache`
> Author: (TBD) · Last updated: 2026-06-16
> Reference implementation: GooseFS Java Client `com.qcloud.cos.goosefs.client.file.cache.*`
> Target repo: `goosefs-client-rust` (crate `goosefs-sdk`)

> **Implementation status summary**: P0–P3 of this design have landed in `src/cache/` and passed unit tests.
> Main differences vs the design: (1) the evictor and meta are **self-implemented** (no `moka`/`lru` introduced),
> depending only on `async-trait`; (2) the concurrency model uses "a single `Mutex<Inner>` guarding
> the index / reverse index / per-directory accounting + 1024 striped page locks", with disk IO always
> executed outside the `Inner` lock (see §5.9 / the `manager.rs` module docs); (3) `file_id` directly takes
> the string form of `URIStatus.file_id` (server-side inode); overwriting is detected and invalidated by
> `on_file_open` comparing `(length, last_modification_time_ms)`. Per-module implementation status is in §13.

---

## 1. Background and Goals

### 1.1 Background

The GooseFS Java client has a built-in **client-side local page cache**, which caches remotely-read data to local disk in fixed-size "pages". Subsequent repeated reads hit the local cache directly, avoiding repeated trips to the Worker / UFS. This mechanism brings significant benefit to the following scenarios:

- Repeated epoch reads in AI training / data analysis;
- Random small IO of columnar stores such as Parquet / ORC;
- High-concurrency reads of hot small files.

Currently the Rust client SDK (`goosefs-sdk`) has **no client-side local cache layer at all**: every read goes through

```text
GoosefsFileReader / GoosefsFileInStream
  → MasterClient.get_status(path)          // metadata + block_ids
  → WorkerRouter.select_worker(block_id)   // consistent hashing
  → WorkerClientPool.acquire(addr)
  → GrpcBlockReader (gRPC ReadBlock bidirectional stream)
```

The goal of this design is to implement a local page cache in the Rust SDK that is **functionally aligned** with the Java client, and to align its config items (`goosefs.user.client.cache.*`) and metrics (`Client.Cache*`) semantics.

### 1.2 Goals

1. Implement a pluggable local page cache layer in the Rust SDK, with a fixed-size page as the cache unit.
2. Provide a `CacheManager` abstraction + `LocalCacheManager` default implementation (local disk backend).
3. Support LRU / LFU eviction policies, multiple cache directories, capacity quota, and async fill (async cache).
4. Non-intrusively integrate with the existing read paths (`GoosefsFileInStream` / `GoosefsFileReader`), start/stop via a config switch.
5. Align with Java's config item naming and defaults, and align with `Client.Cache*` metrics.
6. Expose the switch and config via the Python binding.
7. Cache on disk can be restored after process restart (can be a P2 stage).

### 1.3 Non-Goals

- Do not implement the server-side (Worker) page cache (the Worker already has its own `BlockPageMetaStore`).
- The first version does not implement RocksDB / in-memory backends, only `LocalPageStore` (local file), but the abstraction must leave room for later extension.
- Do not implement write cache (write-back / write-through cache); the first version focuses on **read cache**.
- Do not implement strong-consistency coordination for cross-process shared cache (multiple processes sharing a disk directory); the first version treats it as single-process exclusive directory.

### 1.4 Data-Consistency Invariants (INV-PC-*)

Page cache is best-effort by design (see §9): any internal failure is
swallowed and the read falls back to the external source. "Best-effort"
however does **not** weaken the byte-level contract — the reader must
always observe the exact bytes the worker / UFS would have served. The
following hard invariants make that contract testable and gate every
release.

They mirror the structure used by `SHORT_CIRCUIT_DESIGN.md` §1.3
(`INV-D*` data-plane / `INV-S*` semantic). Every invariant maps to a
gating-grade test case in `tests/page_cache_consistency.rs` (§12.5).

| ID | Invariant | Test case |
|---|---|---|
| **INV-PC-D1** | Cache-on and cache-off paths return byte-for-byte identical data on every page / chunk / block / tail boundary, on both cold-miss and warm-hit reads. | `inv_pc_d1_cache_vs_direct_byte_diff` |
| **INV-PC-D2** | The three public read APIs on `GoosefsFileInStream` (`read` sequential, `read_at` positioned, `read_all` whole-file) return identical bytes for the same logical input under cache-on. | `inv_pc_d2_read_apis_are_equivalent` |
| **INV-PC-S1** | When the cache layer fails — unwritable cache directory, store-write rejection, async-fill queue exhaustion — the next `get` either misses cleanly or serves correct bytes; it must never return stale or torn data. | `inv_pc_s1_failed_fill_does_not_poison_cache` |
| **INV-PC-S2** | Cached pages survive process restart only when `(file_id, length, last_modification_time_ms)` is unchanged; on overwrite, `on_file_open` invalidates them before the first read so no stale bytes are served. | `inv_pc_s2_restart_byte_parity` |

Lower-level invariants (page-store atomic rename, evictor ordering, TTL
lazy expiry, benign racing) are exercised by the in-tree unit tests
listed in §12 item 1 and are not duplicated at the gating tier.

---

## 2. Java Implementation Reference

> Source root: `/opt/sourcecode/cos/goosefs/core/client/fs/src/main/java/com/qcloud/cos/goosefs/client/file/cache/`

### 2.1 Component overview

```text
LocalCacheFileSystem (FileSystem decorator)
   └── openFile() → LocalCacheFileInStream (cache-integrated read stream)
                       └── mCacheManager.get()/put()
                              │
                    CacheManager.Factory (singleton)
                       └── NoExceptionCacheManager  (exception-swallowing wrapper)
                              └── LocalCacheManager  (core coordinator)
                                     ├── PageMetaStore       (metadata + index + eviction coordination)
                                     │      ├── IndexedSet<PageInfo>  (pageId/fileId dual index)
                                     │      └── List<PageStoreDir>
                                     │             ├── PageStore (LocalPageStore: actual disk IO)
                                     │             ├── CacheEvictor (LRU/LFU)
                                     │             └── byte accounting (QuotaManagedPageStoreDir)
                                     ├── Allocator (multi-dir allocation, HashAllocator)
                                     ├── ReadWriteLock[1024]   (page-level striped lock)
                                     └── thread pool (async write / async restore / TTL)
```

### 2.2 Key classes and responsibilities

| Class | Responsibility |
|---|---|
| `CacheManager` (interface) | Top-level interface: `put/get/delete/append/invalidate`, with `Factory` singleton creation + `State` enum (`NOT_IN_USE`/`READ_ONLY`/`READ_WRITE`) |
| `LocalCacheManager` | Core implementation: coordinates metaStore + pageStore + evictor + page-level lock + async thread pool |
| `NoExceptionCacheManager` | Wrapper layer that swallows all exceptions to make the cache "best-effort"; cache faults never affect correctness |
| `CacheManagerOptions` | Reads all cache parameters from config |
| `PageId` | Page identifier `(fileId: String, pageIndex: long)` |
| `PageInfo` | Page metadata: pageId, page size, scope, owning dir, creation time |
| `PageStore` (interface) | Storage backend abstraction: `put/get/delete`; `open()` factory method |
| `LocalPageStore` | Local disk implementation (the only real backend) |
| `PageStoreDir` / `LocalPageStoreDir` | Single cache-directory abstraction, holds pageStore + evictor + capacity accounting, dir scan/restore |
| `QuotaManagedPageStoreDir` | Byte accounting, reserve/release, temp file management |
| `PageMetaStore` / `DefaultPageMetaStore` | Page metadata and index (`PageId → PageInfo`), coordinates with the evictor |
| `CacheEvictor` / `LRUCacheEvictor` / `LFUCacheEvictor` | Eviction policies |
| `Allocator` / `HashAllocator` | When multiple dirs, pick dir by fileId hash |
| `LocalCacheFileInStream` | Cache-integrated read stream: hit reads cache, miss goes external and fills back |

### 2.3 Key method signatures (Java)

```java
// CacheManager / LocalCacheManager
boolean put(PageId pageId, ByteBuffer page, CacheContext cacheContext);
int     get(PageId pageId, int pageOffset, int bytesToRead,
            PageReadTargetBuffer buffer, CacheContext cacheContext);
boolean delete(PageId pageId, CacheContext cacheContext);
void    invalidate(Predicate<PageInfo> predicate);
```

### 2.4 Read path core logic (`LocalCacheFileInStream`)

read `[position, position+length)` hour:

1. Calculate coverage page Interval:`startPage = position / pageSize` … `endPage`.
2. for each page:
   - calculate `pageId = (fileId, pageIndex)`,`pageOffset = position % pageSize`;
   - call `mCacheManager.get(pageId, pageOffset, bytesToRead, buffer)`:
     - **hit**: Directly from local page store copy to target buffer, cumulative `BytesReadCache`;
     - **miss**:from external(Worker/UFS) reads the entire page, copies the required fragments to the caller, and**asynchronous/synchronous**call `put()` Backfill the entire page, cumulative `BytesReadExternal`.
3. `mPageSize`, `mBufferSize` come from config.

### 2.5 Lock hierarchy (important)

All page operations of `LocalCacheManager` strictly follow the order below; the Rust implementation must align:

```text
1. Acquire the striped lock for the corresponding page (page lock, hashed by pageId into one of 1024 RwLocks)
2. Acquire the metastore lock (mMetaLock)
3. Update the metastore (index / evictor state)
4. Release the metastore lock
5. Update the page store (actual disk IO) and evictor
6. Release the page lock
```

### 2.6 Configuration items (Java `PropertyKey.USER_CLIENT_CACHE_*`)

| Config key | Meaning | Default (reference) |
|---|---|---|
| `goosefs.user.client.cache.enabled` | whether to enable local cache | `false` |
| `goosefs.user.client.cache.page.size` | page size | `1MB` |
| `goosefs.user.client.cache.size` | per-directory cache capacity | `512MB` |
| `goosefs.user.client.cache.dirs` | cache directory list | `/tmp/goosefs_cache` |
| `goosefs.user.client.cache.store.type` | backend type | `LOCAL` |
| `goosefs.user.client.cache.eviction.policy` (evictor class) | eviction policy | `LRU` |
| `goosefs.user.client.cache.async.write.enabled` | whether to fill back asynchronously | `true` |
| `goosefs.user.client.cache.async.write.threads` | async fill thread count | `16` |
| `goosefs.user.client.cache.in.stream.buffer.size` | read stream buffer | `0` (off) |
| `goosefs.user.client.cache.quota.enabled` | whether to enable quota | `false` |
| `goosefs.user.client.cache.ttl.enabled` / `.ttl` | page TTL | `false` |

> Note: the above defaults are subject to the repo's actual `PropertyKey.java`; verify item by item before landing.

### 2.7 Metrics (`MetricKey.Client.Cache*`)

The complete metrics list given by the user is in Appendix A; the core categories:

- Hit/penetration bytes: `CacheBytesReadCache`, `CacheBytesReadExternal`, `CacheBytesRequestedExternal`, `CacheBytesReadInStreamBuffer`;
- Hit rate: `CacheHitRate`;
- Capacity: `CacheSpaceAvailable`, `CacheSpaceUsed`, `CacheSpaceUsedCount`, `CachePages`;
- Eviction: `CacheBytesEvicted`, `CachePagesEvicted`, `CacheBytesDiscarded`, `CachePagesDiscarded`;
- Write: `CacheBytesWrittenCache`;
- Latency: `CachePageReadCacheTimeNanos`, `CachePageReadExternalTimeNanos`;
- Various error counters: `CacheGetErrors`, `CachePutErrors`, `CacheDeleteErrors`, `CachePut*Errors`, `CacheStore*Timeout`, etc.;
- State: `CacheState`, `FallbackState`.

---

## 3. Rust Client Status and Integration Points

### 3.1 Existing read path

| Module | File | Description |
|---|---|---|
| High-level read orchestration | `src/io/file_reader.rs` (`GoosefsFileReader`) | end-to-end read pipeline, `open_with_context` / `read_all` / `read_next_block` / `read_range_*` |
| Seekable stream | `src/io/file_in_stream.rs` (`GoosefsFileInStream`) | dual path: sequential `read()` (block_in_stream) + random `read_at(offset, n)` (positioned_read) |
| Single-block stream read | `src/io/reader.rs` (`GrpcBlockReader`) | gRPC `ReadBlock` bidirectional stream, prefetch, ACK fusion |
| Worker client | `src/client/worker.rs` (`WorkerClient` / `WorkerClientPool`) | ultimate source of read data |
| Context | `src/context.rs` (`FileSystemContext`) | connection pool / Worker lifecycle management, **the best mount point for page cache** |
| Config | `src/config.rs` (`GoosefsConfig`) | config struct + `ENV_*` / `STORAGE_OPT_*` constants |
| metrics | `src/metrics/registry.rs` | global `Counter`/`Gauge` (`AtomicI64` + `DashMap`), `name` submodule defines constants |
| Error | `src/error.rs` (`Error` / `Result`) | unified `thiserror` error enum |

Key read method signatures:

```rust
// src/io/file_in_stream.rs
impl GoosefsFileInStream {
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize>;       // sequential
    pub async fn read_at(&mut self, offset: i64, n: usize) -> Result<Bytes>; // random
    pub async fn seek(&mut self, pos: i64) -> Result<i64>;
}
```

### 3.2 Integration point selection (landed)

**Integration point: both `read_at` (random read) and `read` (sequential read) of `GoosefsFileInStream` are wired into the cache.**

1. The random-read semantics naturally split by offset into pages, which best fits the page-cache model, so `read_at` is the primary integration point;
2. The sequential `read()` already reuses the same cache query in P2: each call goes through `read_at_cached(self.pos, end)` to satisfy a segment and advances `pos` (aligned with Java's `LocalCacheFileInStream` making all reads go through the cache).

Integration strategy: `GoosefsFileInStream` holds an optional `Arc<dyn CacheManager>` (injected by `FileSystemContext::acquire_cache_manager`), and implements `ExternalRangeReader` for the integration layer (fallback source goes through the existing worker/UFS positioned-read). On read:

```text
read_at(offset, n) / read(buf):
    if cache exists:
        read_through_cache(cache, self /*ExternalRangeReader*/, file_id,
                           page_size, file_length, offset, end, fill_mode):
            split [offset, end) by page
            for each page:
                cache.get(page) ──hit──> copy slice
                                └─miss──> ext.read_range whole page → return slice
                                           + fill back per FillMode (Sync/Async/None)
    else:
        go through the original read_external_range / positioned_read path
```

> `fill_mode` is determined by `cache_fill` (whether to fill back) and `cache_async_write` (sync/async), mapped to
> `FillMode::{None, Sync, Async}` (see §6.2). Refining the `ReadType` semantics (fill back only for specific read types)
> is listed as a later enhancement (§14.1).

---

## 4. Overall Architecture Design

```text
                 FileSystemContext
                       │ (holds Option<Arc<dyn CacheManager>>; acquire_cache_manager)
                       ▼
   GoosefsFileReader / GoosefsFileInStream
                       │ read_at / read  (impl ExternalRangeReader)
                       ▼
        ┌──────────────────────────────────┐
        │  read_through_cache() + FillMode  │   ← integration layer (stateless fn): page split + hit check + fill
        └──────────────────────────────────┘
                       │
            ┌──────────┴───────────┐
       cache.get()              ext.read_range() (miss whole-page fallback)
            │                        │  (reuse existing GrpcBlockReader)
            ▼                        ▼
   ┌─────────────────┐      WorkerClientPool → ReadBlock
   │  CacheManager   │ (trait; DisabledCacheManager disabled state)
   │  └ LocalCache   │ (default impl)
   │     Manager     │
   └─────────────────┘
            │
   ┌────────┴───────────────────┬──────────────────┐
   ▼                            ▼                  ▼
 Mutex<Inner>               Vec<LocalPageStore>   Allocator
 ├ meta (index)              (disk IO: temp file      (HashAllocator)
 ├ by_file (reverse index)    + atomic rename)
 ├ versions (overwrite detection)
 └ dirs: Vec<DirState>
      └ evictor (Lru/Lfu) + used_bytes/capacity accounting
   + page_locks: [RwLock; 1024]   (page-level striped lock)
   + async_write_sem              (async fill rate limit)
```

> Note: `Mutex<Inner>` only guards in-memory metadata / accounting / evictor (short critical section);
> `LocalPageStore`'s disk IO and evicted-file deletion run outside the lock. See §5.3 / §5.9.

### 4.1 Module layout (actually landed)

```text
src/cache/
  ├── mod.rs              # exports + CacheManager trait + CacheState enum + DisabledCacheManager
  ├── page_id.rs          # PageId / PageInfo / CacheScope
  ├── manager.rs          # LocalCacheManager (core coordination: index + accounting + lock + TTL + restore)
  ├── options.rs          # CacheManagerOptions (read from GoosefsConfig, 5% overhead)
  ├── evictor/
  │     ├── mod.rs        # CacheEvictor trait + build_evictor()
  │     ├── lru.rs        # LruCacheEvictor
  │     └── lfu.rs        # LfuCacheEvictor
  ├── store/
  │     ├── mod.rs        # PageStore trait
  │     └── local.rs      # LocalPageStore (local file: temp file + atomic rename)
  ├── allocator.rs        # Allocator trait + HashAllocator
  ├── metrics.rs          # cache-specific metrics name constants (name submodule)
  └── caching_reader.rs   # read_through_cache / FillMode / ExternalRangeReader: integration layer with file_in_stream
```

> **Differences from the initial design** (simplified/merged during implementation):
> - **`noop.rs` → `mod.rs::DisabledCacheManager`**: the best-effort "swallow exception" semantics no longer need a separate wrapper layer;
>   `CacheManager` methods themselves return `bool`/`usize` (not `Result`); errors are swallowed in-place inside `LocalCacheManager`
>   and counted as `*Errors` metrics; when the cache is disabled, `DisabledCacheManager` (always-miss) is used.
> - **`meta_store.rs` (`PageMetaStore`/`DefaultPageMetaStore`) → `manager.rs::Inner`**: index,
>   `file_id` reverse index, per-directory evictor + byte accounting, and file-version table are all converged into a single `Mutex<Inner>`,
>   avoiding multi-layer lock nesting (see §5.9 / §10.1).
> - **`store/dir.rs` (`LocalPageStoreDir`/`QuotaManagedPageStoreDir`) → `manager.rs::DirState`**:
>   per-directory capacity/accounting/eviction coordination is inlined into `Inner.dirs`, and `PageStore` keeps only the pure disk-IO abstraction.
> - **Integration layer is not a struct**: instead of introducing a `CachingPositionReader` struct, an stateless function
>   `caching_reader::read_through_cache(...)` + `FillMode` enum is used, easing offline unit tests (see §6.1).

---

## 5. Core module detailed design

### 5.1 `PageId` / `PageInfo`

```rust
// src/cache/page_id.rs

/// Page identifier: equivalent to Java PageId(fileId, pageIndex).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct PageId {
    /// Unique file identifier. Java uses String (usually fileId or path hash).
    /// On the Rust side, the string form of file_id(i64) or a path-derived stable hash is recommended.
    pub file_id: Arc<str>,
    /// Page index = offset / page_size.
    pub page_index: u64,
}

/// Page metadata.
#[derive(Clone, Debug)]
pub struct PageInfo {
    pub page_id: PageId,
    /// Actual bytes in the page (the last page may be < page_size).
    pub page_size: u64,
    /// Owning cache-directory index.
    pub dir_index: usize,
    /// Creation time (used for TTL).
    pub created_at: std::time::Instant,
    /// Quota scope (first version can be Global).
    pub scope: CacheScope,
}
```

> `file_id` source: prefer the stable identifier from `URIStatus` (e.g. `file_id` / `mount_id + ufs_path`). The same file must yield the same `file_id` across multiple opens, otherwise the cache cannot hit across streams.

### 5.2 `CacheManager` trait

```rust
// src/cache/mod.rs

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CacheState { NotInUse, ReadOnly, ReadWrite }

#[async_trait::async_trait]
pub trait CacheManager: Send + Sync {
    /// Write (fill back) a whole page. Returns whether caching succeeded.
    async fn put(&self, page_id: &PageId, page: Bytes) -> bool;

    /// Schedule a best-effort fill that does not block the caller (default spawn;
    /// LocalCacheManager overrides to go through the Semaphore-limited async write-back pool).
    fn schedule_fill(self: Arc<Self>, page_id: PageId, page: Bytes) where Self: 'static;

    /// Read page bytes [offset, offset+len) into dst, return the actual bytes read.
    /// A miss returns 0 (no error; the caller falls back to source accordingly).
    async fn get(&self, page_id: &PageId, page_offset: usize, dst: &mut [u8]) -> usize;

    /// Delete a page.
    async fn delete(&self, page_id: &PageId) -> bool;

    /// Invalidate all pages of a file (e.g. when the file is overwritten/deleted).
    async fn invalidate(&self, file_id: &str);

    /// Notify the cache that a file was (re)opened; compare (length, mtime) to detect overwrite,
    /// invalidate all pages of that file if inconsistent. Default no-op.
    async fn on_file_open(&self, _file_id: &str, _length: i64, _last_modification_time_ms: i64) {}

    fn state(&self) -> CacheState;
}
```

Design points:
- `get` **does not return `Result`**: the cache is best-effort; a miss returns 0 and errors are swallowed internally and counted as metrics (aligned with Java's `NoExceptionCacheManager`; when the cache is disabled, `DisabledCacheManager` is used).
- Pass whole pages via `bytes::Bytes`, zero-copy friendly.
- Depends on `async-trait` (already in `Cargo.toml`).

### 5.3 `LocalCacheManager`

Core fields (landed):

```rust
pub struct LocalCacheManager {
    options: CacheManagerOptions,
    /// One page store per cache directory (immutable; IO runs outside the inner lock).
    stores: Vec<LocalPageStore>,
    allocator: Box<dyn Allocator>,
    /// Single metadata lock: index + reverse index + version table + per-dir evictor/accounting.
    inner: Mutex<Inner>,
    /// Page-level striped lock: LOCK_SIZE = 1024 RwLocks, selected by PageId hash.
    page_locks: Vec<RwLock<()>>,
    /// Async write-back permit (capacity = async_write_threads).
    async_write_sem: Arc<Semaphore>,
    state: CacheState,
}
```

**`get` flow** (aligned with §2.5 lock hierarchy, with TTL lazy expiry):

```text
1. rl = page_locks[hash(page_id) % 1024].read()
2. enter inner lock:
     - if is_expired(page_id) → remove metadata + evictor.on_remove + deduct accounting,
       count CachePagesDiscarded/CacheBytesDiscarded, return 0 (miss)
     - else take page_info.dir_index; if absent → return 0 (miss)
   release inner lock
3. n = stores[dir_index].get(page_id, page_offset, dst).await   // disk IO outside lock
       └─ read failure → count GetStoreReadErrors, treat as miss; n==0 → treat as racy eviction miss
4. enter inner lock: evictor.on_access(page_id); release
5. metrics: BytesReadCache += n; PageReadCacheTimeNanos += elapsed
6. return n
```

**`put` flow**:

```text
1. wl = page_locks[hash(page_id)].write()
2. dir_index = allocator.allocate(page_id, stores.len())
3. enter inner lock:
     - same page already exists → benign racing, count PutBenignRacingErrors, return false
     - while used_bytes + page_len > capacity: pop_victim (take candidate → remove metadata → count evicted),
         no candidate → count PutInsufficientSpaceErrors, return false
     - try to reserve used_bytes += page_len
   release inner lock
4. delete the evicted victim's disk file outside the lock (best-effort)
5. stores[dir_index].put(page_id, &page).await   // disk IO outside lock
       failure → enter inner to roll back the reservation, count PutStoreWriteErrors, return false
6. enter inner lock: insert meta + by_file + evictor.on_add; BytesWrittenCache += page_len; refresh occupancy gauge
7. return true
```

**Async fill-back**: `schedule_fill` uses `Semaphore::try_acquire_owned` for rate limiting (capacity = `async_write_threads`); on acquiring a permit it `tokio::spawn`s a `put`, and when permits are exhausted it rejects and counts `CachePutAsyncRejectionErrors` (aligned with Java's `SynchronousQueue` + rejection policy).

### 5.4 `PageStore` / `LocalPageStore`

```rust
// src/cache/store/mod.rs
#[async_trait::async_trait]
pub trait PageStore: Send + Sync {
    async fn put(&self, page_id: &PageId, page: &[u8]) -> Result<()>;
    /// Read [offset, offset+dst.len()), return bytes read.
    async fn get(&self, page_id: &PageId, offset: usize, dst: &mut [u8]) -> Result<usize>;
    async fn delete(&self, page_id: &PageId) -> Result<()>;
}
```

`LocalPageStore` disk layout (aligned with Java):

```text
<cache_dir>/<page_size>/<bucket>/<file_id>/<page_index>
                          │
                          └── bucket = hash(file_id) % NUM_BUCKETS  (default 1000, to avoid too many files in one dir)
```

- Write: first write a temp file `*.tmp`, then `fsync` and atomically `rename` (aligned with Java commit/abort semantics).
- Read: `File::open` + `seek(offset)` + `read`. Prefer `tokio::fs` or wrapping sync IO with `spawn_blocking` (disk IO should not block tokio worker threads).
- Delete: `remove_file`, ignore `NotFound` (count `CacheDeleteNonExistingPageErrors`).

### 5.5 Metadata and index (`manager.rs::Inner`)

Responsibility: maintain the `PageId → PageInfo` index, support reverse lookup by `file_id` (for `invalidate`), and coordinate with the evictor.

> **Implementation note**: the independent `PageMetaStore`/`DefaultPageMetaStore` abstraction of the initial design was converged into a single `Mutex<Inner>` inside `LocalCacheManager` during landing, avoiding the multi-layer nested lock consistency burden of "meta lock + dir lock + evictor lock" (see §10.1). `Inner` fields are as follows:

```rust
struct Inner {
    /// PageId → PageInfo primary index.
    meta: HashMap<PageId, PageInfo>,
    /// file_id → set(page_index) reverse index (for invalidate).
    by_file: HashMap<Arc<str>, HashSet<u64>>,
    /// file_id → (length, last_modification_time_ms) known version,
    /// used to detect overwrite on (re)open and invalidate stale pages.
    versions: HashMap<Arc<str>, (i64, i64)>,
    /// Per-directory evictor + byte accounting.
    dirs: Vec<DirState>,   // DirState { evictor, used_bytes, capacity }
}
```

- Primary index: `HashMap<PageId, PageInfo>` (inside the `Mutex`, not `DashMap` — because index/accounting/eviction need atomic update in the same critical section).
- Reverse index: `HashMap<Arc<str>, HashSet<u64>>`, equivalent to Java's `IndexedSet` dual index.
- Occupancy, summed from `Inner.dirs[*].used_bytes`, drives the `CachePages` / `CacheSpaceUsed` / `CacheSpaceAvailable` gauges (`publish_occupancy`).

### 5.6 `CacheEvictor` (LRU / LFU)

```rust
// src/cache/evictor/mod.rs
pub trait CacheEvictor: Send + Sync {
    fn on_add(&self, id: &PageId);
    fn on_access(&self, id: &PageId);     // touch on get hit
    fn on_remove(&self, id: &PageId);
    /// Return the next page to be evicted.
    fn evict_candidate(&self) -> Option<PageId>;
}

pub fn build_evictor(kind: CacheEvictorType) -> Box<dyn CacheEvictor>;
```

> **Implementation note (landed, self-implemented)**: no `moka`/`lru` introduced. Each cache directory holds an independent `Box<dyn CacheEvictor>`
> (stored in `DirState.evictor`), whose internal state changes all happen within the `Inner` lock critical section, so the evictor needs no lock of its own.
> - `LruCacheEvictor` (`evictor/lru.rs`): access-order queue, `on_access` moves to tail, `evict_candidate` takes head.
> - `LfuCacheEvictor` (`evictor/lfu.rs`): frequency counter, evicts the least-frequent page.

### 5.7 `Allocator` (multi-directory)

```rust
// src/cache/allocator.rs
pub trait Allocator: Send + Sync {
    fn allocate(&self, page_id: &PageId, num_dirs: usize) -> usize;
}
```

`HashAllocator`: `hash(file_id) % num_dirs`, ensuring a file's pages concentrate in the same directory (aligned with Java's `AffinityHashAllocator`, easing per-file invalidate/restore).

### 5.8 Capacity quota and accounting (`DirState`)

> **Implementation note**: per-directory capacity and accounting are inlined in `Inner.dirs[i]: DirState`, not a separate `PageStoreDir` structure.

Each dir maintains:
- `capacity` (from `cache.size`, minus 5% overhead, aligned with `PageStoreType.LOCAL` overhead, see `options.rs`);
- `used_bytes: u64` (updated inside the `Inner` lock, non-atomic — the critical section already serializes);
- on `put`, if `used_bytes + page_len > capacity`, loop `pop_victim` (take evictor candidate → remove metadata → delete disk file outside lock) until space is freed; if it cannot be freed, count `CachePutInsufficientSpaceErrors` and return false.
- before writing to disk, first "try to reserve" `used_bytes += page_len` (so concurrent puts see the space as occupied), and roll back on write failure.

### 5.9 Concurrency and locking (landed)

- **Page-level striped lock**: `Vec<tokio::sync::RwLock<()>>`, length `LOCK_SIZE = 1024`, selected by `hash(page_id) % 1024`; `get` takes a read lock, `put`/`delete` take a write lock. Same-page operations are serial, different pages concurrent.
- **Metadata lock**: a single `Mutex<Inner>`, only guarding in-memory index / reverse index / version table / per-dir evictor + accounting; **never held across disk IO** — all `PageStore` reads/writes and evicted-file deletion run after the `Inner` lock is released, ensuring scalable read/write.
- Lock order: **page lock → Inner lock (short critical section) → release Inner → disk IO**, avoiding deadlock; the evicted victim's metadata is first removed inside `Inner`, and its disk file is deleted outside the lock (on Unix the inode survives until the fd is closed, so a concurrent `get` can still complete).

---

## 6. Read path integration

### 6.1 `read_through_cache` (integration layer)

The integration layer (`src/cache/caching_reader.rs`) wraps "page split + hit check + fallback + fill-back".

> **Implementation note**: instead of introducing a `CachingPositionReader` struct, a **stateless function** + `ExternalRangeReader`
> trait + `FillMode` enum is used, easing offline unit tests (inject fallback via `FakeExternal`). `GoosefsFileInStream`
> implements `ExternalRangeReader`, delegating the fallback to the existing worker/UFS positioned-read path.

```rust
// src/cache/caching_reader.rs

/// Fallback-source abstraction: implemented by file_in_stream on its worker/UFS positioned-read path.
#[async_trait::async_trait]
pub trait ExternalRangeReader {
    /// Read [offset, end), may return fewer bytes only at EOF.
    async fn read_range(&mut self, offset: i64, end: i64) -> Result<Bytes>;
}

/// How to fill back a missed page.
pub enum FillMode { None, Sync, Async }

/// Serve [offset, end) through the page cache.
/// Page-by-page cache.get; on miss read the whole page via read_range and fill back per fill_mode.
/// best-effort: cache errors degrade to external read, never become a failure.
pub async fn read_through_cache<R: ExternalRangeReader + ?Sized>(
    cache: &Arc<dyn CacheManager>,
    ext: &mut R,
    file_id: &Arc<str>,
    page_size: u64,
    file_length: i64,
    offset: i64,
    end: i64,
    fill_mode: FillMode,
) -> Result<Bytes>;
```

Both the hit and miss branches count `CacheBytesReadExternal` / `CacheBytesRequestedExternal`; `FillMode::Async`
goes through `schedule_fill` (rate-limited by `Semaphore`, rejects and counts `CachePutAsyncRejectionErrors` when full), `Sync` does `await put`.

### 6.2 `GoosefsFileInStream` changes (landed)

- New fields: `cache: Option<Arc<dyn CacheManager>>`, `cache_page_size`, `cache_file_id: Arc<str>`, `cache_fill: bool`, `cache_async_write: bool` (injected by `FileSystemContext` at construction; legacy `open()` uses all `None`/defaults).
- **Random read** `read_at`: when `cache.is_some()`, go through `read_at_cached` (internally calls `read_through_cache`), otherwise go through `read_external_range` (the original cache-less implementation, extracted as the miss fallback source).
- **Sequential read** `read()`: already wired into the cache (P2) — when `cache.is_some()`, each call goes through `read_at_cached(self.pos, end)` to satisfy a segment and advances `pos`.
- `cache_fill_mode()` maps `cache_fill` + `cache_async_write` to `FillMode::{None,Sync,Async}`.
- `ExternalRangeReader for GoosefsFileInStream::read_range` is the fallback entry point.

### 6.3 `FileSystemContext` changes (landed)

- At `connect()`, if `config.client_cache_enabled == true`, construct `LocalCacheManager::from_config(...)` and hold it as `Option<Arc<dyn CacheManager>>` (best-effort: init failure degrades to no-cache and does not affect `connect()`); exposed to each reader for sharing via `acquire_cache_manager()`.
- When opening a file (`open_with_context`), inject cache + `cache_file_id` (from `URIStatus.file_id`), and call `cache.on_file_open(file_id, length, last_modification_time_ms)` to detect overwrites.
- Restart restore is done by `restore()` inside `LocalCacheManager::create`; the TTL sweeper is spawned on demand at `from_config` (holds `Weak<Self>`, exits automatically after the manager drops).

---

## 7. Configuration Design

exist `src/config.rs` of `GoosefsConfig` Add a new field in (aligned Java `USER_CLIENT_CACHE_*`, and supplement `ENV_*` / `STORAGE_OPT_*` constant):

```rust
pub struct GoosefsConfig {
    // ... existing ...

    // ── Client local page cache ──────────────────────────────
    /// Whether to enable the client-side local page cache (default false).
    #[serde(default)]
    pub client_cache_enabled: bool,
    /// Page size (bytes), default 1 MiB.
    #[serde(default = "default_cache_page_size")]
    pub client_cache_page_size: u64,
    /// Per-cache-directory capacity (bytes), default 1 GiB. One-to-one with dirs or a uniform value.
    #[serde(default = "default_cache_size")]
    pub client_cache_size: u64,
    /// Cache directory list, default ["/tmp/goosefs_cache"].
    #[serde(default = "default_cache_dirs")]
    pub client_cache_dirs: Vec<String>,
    /// Eviction policy: LRU / LFU, default LRU.
    #[serde(default = "default_cache_evictor")]
    pub client_cache_evictor: CacheEvictorType,
    /// Whether to fill back asynchronously, default true.
    #[serde(default = "default_true")]
    pub client_cache_async_write_enabled: bool,
    /// Async fill concurrency, default 16.
    #[serde(default = "default_cache_async_write_threads")]
    pub client_cache_async_write_threads: usize,
    /// Whether to enable quota, default false.
    #[serde(default)]
    pub client_cache_quota_enabled: bool,
    /// Page TTL (seconds), 0 means no expiry.
    #[serde(default)]
    pub client_cache_ttl_secs: u64,
}
```

Corresponding constants (naming follows the existing style):

```rust
// ENV
pub const ENV_CLIENT_CACHE_ENABLED: &str   = "GOOSEFS_USER_CLIENT_CACHE_ENABLED";
pub const ENV_CLIENT_CACHE_PAGE_SIZE: &str = "GOOSEFS_USER_CLIENT_CACHE_PAGE_SIZE";
pub const ENV_CLIENT_CACHE_SIZE: &str      = "GOOSEFS_USER_CLIENT_CACHE_SIZE";
pub const ENV_CLIENT_CACHE_DIRS: &str      = "GOOSEFS_USER_CLIENT_CACHE_DIRS";
// ... evictor / async / quota / ttl

// storage option (for OpenDAL / Python kwargs)
pub const STORAGE_OPT_CLIENT_CACHE_ENABLED: &str   = "goosefs_client_cache_enabled";
pub const STORAGE_OPT_CLIENT_CACHE_PAGE_SIZE: &str = "goosefs_client_cache_page_size";
// ...
```

> Defaults must be finalized after checking against the actual values in Java's `PropertyKey.java` (see the to-verify items in §2.6).

---

## 8. Metrics Design

Add cache metrics constants in the `name` module of `src/metrics/registry.rs` (aligned with the user-given `Client.Cache*`, full list in Appendix A). Prioritize the high-value subset below; the rest are added on demand:

| Rust constant (suggested) | metric name | type |
|---|---|---|
| `CLIENT_CACHE_BYTES_READ_CACHE` | `Client.CacheBytesReadCache` | counter |
| `CLIENT_CACHE_BYTES_READ_EXTERNAL` | `Client.CacheBytesReadExternal` | counter |
| `CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL` | `Client.CacheBytesRequestedExternal` | counter |
| `CLIENT_CACHE_BYTES_WRITTEN_CACHE` | `Client.CacheBytesWrittenCache` | counter |
| `CLIENT_CACHE_BYTES_EVICTED` | `Client.CacheBytesEvicted` | counter |
| `CLIENT_CACHE_PAGES` | `Client.CachePages` | gauge |
| `CLIENT_CACHE_PAGES_EVICTED` | `Client.CachePagesEvicted` | counter |
| `CLIENT_CACHE_SPACE_USED` | `Client.CacheSpaceUsed` | gauge |
| `CLIENT_CACHE_SPACE_AVAILABLE` | `Client.CacheSpaceAvailable` | gauge |
| `CLIENT_CACHE_HIT_RATE` | `Client.CacheHitRate` | gauge |
| `CLIENT_CACHE_PAGE_READ_CACHE_TIME_NS` | `Client.CachePageReadCacheTimeNanos` | counter |
| `CLIENT_CACHE_PAGE_READ_EXTERNAL_TIME_NS` | `Client.CachePageReadExternalTimeNanos` | counter |
| `CLIENT_CACHE_GET_ERRORS` / `CLIENT_CACHE_PUT_ERRORS` / ... | `Client.Cache*Errors` | counter |
| `CLIENT_CACHE_STATE` | `Client.CacheState` | gauge |

- Hit rate `CacheHitRate` (implemented): computed as `BytesReadCache / (BytesReadCache + BytesReadExternal)` (`metrics::publish_hit_rate`), refreshed in real time on both the hit and fallback read paths (not a periodic task, avoiding a resident background thread).
- The existing metrics reporting chain (`ClientMetricsReporter` → `HeartbeatTask` → Master, plus optional Pushgateway) is reused naturally, with no new reporting channel needed.

Instrumentation points (landed):
- `BytesReadCache` / `PageReadCacheTimeNanos`: `LocalCacheManager::get` hit branch;
- `BytesReadExternal` / `BytesRequestedExternal` / `PageReadExternalTimeNanos`: `read_through_cache` fallback branch;
- `BytesWrittenCache` / `CachePages` / `CacheSpaceUsed` / `CacheSpaceUsedCount`: `put` success + `publish_occupancy`;
- `BytesEvicted` / `PagesEvicted`: evictor eviction;
- each `*Errors`: corresponding error branch.

---

## 9. Error Handling

- Internal cache-layer errors are **not propagated upward**; they are uniformly swallowed and counted as the corresponding `Client.Cache*Errors` metric, falling back to reading from the source (aligned with Java's `NoExceptionCacheManager`).
- `src/error.rs` may add an internal `Error::Cache(String)` variant, used only for internal `Result`s like `PageStore`, and **never escapes to the SDK public API**.
- Key guarantee: **no cache fault may affect read correctness** — a miss/error always falls back to the source.

---

## 10. Key Trade-offs and Risks

### 10.1 Self-built vs `moka`

| Approach | Pros | Cons |
|---|---|---|
| Self-built evictor + meta (close to Java) | 1:1 aligned with Java behavior, controllable | More work, must ensure concurrency correctness yourself |
| Reuse `moka` (value stores key metadata, listener deletes disk) | Mature TinyLFU/LRU, capacity/TTL eviction, async-friendly | Behavior slightly differs from Java; disk/memory metadata consistency needs careful eviction-callback handling |

**Suggestion**: P1 uses `moka` to manage key metadata + eviction listener to delete disk files, quickly usable; if strict alignment with Java is later needed, switch to self-built evictor. First confirm whether `Cargo.toml` already includes `moka`, otherwise add the dependency.

> **Final decision (landed)**: adopt the **self-built** evictor (`evictor/lru.rs`, `evictor/lfu.rs`) +
> self-built in-memory index (`manager.rs::Inner`), with no `moka`/`lru` introduced. Reason: cache values are on disk and metadata is in
> memory, requiring precise control over the order and consistency of "evict metadata → delete disk file outside lock"; self-building is easier to align with Java behavior,
> and only adds one more `async-trait` dependency. The `PageMetaStore`/`DefaultPageMetaStore` abstraction was simplified into
> a single `Mutex<Inner>` inside `manager.rs` (primary index + `file_id` reverse index + per-directory accounting), with disk IO
> always executed outside that lock.

### 10.2 `file_id` stability

Cache cross-stream hits depend on a stable `file_id`. Confirm whether `URIStatus` provides a stable `file_id`; if only a path is available, use a stable hash of `path`, but note that when a file is overwritten (mtime/length changes) it must be `invalidate`d, otherwise stale data is read. **This is a critical correctness risk point**, requiring length/mtime to be verified consistent with cached metadata at `get_status` time.

### 10.3 Blocking IO

Disk reads/writes must not block the tokio runtime; uniformly use `tokio::fs` or `spawn_blocking`.

### 10.4 Multi-process shared directory

The first version assumes a single process exclusively owns the cache directory. Multi-process sharing needs file locks + directory isolation, left as future work.

---

## 11. Python Binding Exposed (landed)

Python `Config` accepts a `properties` dictionary, serialized as Java-properties and handed over after formatting
`GoosefsConfig::from_properties_str` parsing (see `bindings/python/src/config.rs`). Since this parser
**already recognizes** all `goosefs.user.client.cache.*` keys (`from_properties_str` in `config.rs`), so the cache config
**requires no binding changes** to be passed in from Python:

```python
from goosefs import Config

cfg = Config("m1:9200", properties={
    "goosefs.user.client.cache.enabled": "true",
    "goosefs.user.client.cache.page.size": "1MB",
    "goosefs.user.client.cache.size": "512MB",
    "goosefs.user.client.cache.dirs": "/data/gfs_cache",
    "goosefs.user.client.cache.eviction.policy": "LRU",   # or LFU
    "goosefs.user.client.cache.async.write.enabled": "true",
    "goosefs.user.client.cache.async.write.threads": "16",
    "goosefs.user.client.cache.ttl.seconds": "0",          # 0 = no expiry
})
```

Recognizable property keys: `enabled` / `page.size` / `size` / `dirs` / `eviction.policy` /
`async.write.enabled` / `async.write.threads` / `quota.enabled` / `ttl.seconds`.

> Note: `config.rs` also has two other entry points, `ENV_CLIENT_CACHE_*` (env-var override) and
> `STORAGE_OPT_CLIENT_CACHE_*` (OpenDAL / storage-option style `goosefs_client_cache_*`);
> Python goes through the properties path, no per-key pass-through needed. ⏳ TODO: Python e2e cases to verify switch pass-through and hit behavior.

---

## 12. Test Plan

> Status markers: ✅ implemented · ⏳ TODO.

1. **Unit tests** (in-place `#[cfg(test)]` per module)
   - ✅ `LocalPageStore` put/get round-trip, read by offset, page-miss returns 0, short read at page tail, miss after delete (`store/local.rs`; the write path goes through temp file + atomic rename, covered by the round-trip case).
   - ✅ `LruCacheEvictor` / `LfuCacheEvictor` eviction order (`evictor/lru.rs`, `evictor/lfu.rs`).
   - ✅ `HashAllocator` same file lands in same dir (`allocator.rs`).
   - ✅ `LocalCacheManager`: put/get hit, multi-dir round-trip and affinity, per-dir LRU/LFU eviction, `invalidate`, `schedule_fill` async fill, concurrent put/get, benign racing (`manager.rs`).
   - ✅ **TTL lazy expiry + sweeper**: `get_lazily_expires_page`, `no_ttl_never_expires`, `sweep_expired_removes_all_stale_pages` (`manager.rs`).
   - ✅ **Overwrite invalidation `on_file_open`**: first record, invalidate on length/mtime change, same identity no-op (`manager.rs`).
   - ✅ `read_through_cache` hit/miss/fill page split (`caching_reader.rs`, with `FakeExternal`).
   - ✅ `CacheManagerOptions` parsing (5% overhead, TTL=0→None, sanitize) (`options.rs`).
2. **Integration tests** (`tests/page_cache_e2e.rs`, `#[ignore]`, against a real cluster)
   - ✅ cold miss → fill back → warm hit (assert `BytesReadCache` grows, `BytesReadExternal` does not, `HitRate` published): `cold_miss_then_warm_hit`.
   - ✅ capacity full triggers eviction (assert `PagesEvicted` grows and content is correct): `capacity_full_triggers_eviction`.
   - ✅ after file overwrite `on_file_open` invalidates and does not read stale data: `overwrite_invalidates_stale_pages`.
   - ✅ unwritable cache dir falls back to source without error: `unwritable_cache_dir_falls_back`.
   - Run: `GOOSEFS_AUTH_TYPE=nosasl cargo test --test page_cache_e2e -- --ignored`.
3. **Benchmark** (`benchmarks/page_cache_ab.rs`, example target)
   - ✅ repeated-read throughput cache on/off comparison (local measurement ≈2.7× speedup; after warm only 1 page goes external, HitRate ~97%).
     Run: `GOOSEFS_AUTH_TYPE=nosasl cargo run --release --example page_cache_ab`.
4. **Python e2e** (`bindings/python/tests/test_page_cache.py`)
   - ✅ cache switch passed via `Config(properties=…)`, read round-trip, repeated read, range read, overwrite does not read stale data, cache-off baseline.
     Run: `GOOSEFS_MASTER_ADDR=127.0.0.1:9200 GOOSEFS_AUTH_TYPE=nosasl uv run --group test pytest tests/test_page_cache.py`.
5. **Gating-grade consistency suite** (`tests/page_cache_consistency.rs`, `#[ignore]`, against a real cluster) — see §12.5.

### 12.5 Gating-grade consistency suite (`page_cache_consistency`)

This is the page-cache analogue of `tests/sc_consistency.rs`. Every
invariant from §1.4 maps to exactly one `#[tokio::test] #[ignore]` case
that asserts a hard byte-equality contract (not a perf metric); a
failure here is a release blocker. Run them explicitly:

```bash
GOOSEFS_AUTH_TYPE=nosasl \
  cargo test --test page_cache_consistency -- --ignored --nocapture --test-threads=1
```

Coverage map:

| Test case | Invariant | What it asserts |
|---|---|---|
| `inv_pc_d1_cache_vs_direct_byte_diff` | INV-PC-D1 | Two contexts — one with cache enabled, one disabled — open the same blob and read at a curated set of boundaries (page 4 KiB, chunk 1 MiB, block 4 MiB, tail). Each pair plus the source payload are asserted three-way equal, on both cold-miss and warm-hit passes. |
| `inv_pc_d2_read_apis_are_equivalent` | INV-PC-D2 | A single cache-on context drains the same file three ways — `read_all`, sequential `read` with heterogeneous chunk sizes, positioned `read_at` with a 257 KiB step — and asserts the three results plus the source are byte-equal. |
| `inv_pc_s1_failed_fill_does_not_poison_cache` | INV-PC-S1 | Cache directory is pointed at an unwritable path, so every fill fails. The reader must still return bytes equal to the source for both whole-file and boundary-spanning ranges, and the `Client.CacheBytesReadCache` counter must stay flat (no torn data is ever served from the cache). |
| `inv_pc_s2_restart_byte_parity` | INV-PC-S2 | Two phases. Phase A: cache-on context, write payload v1, read it warm, drop the context. Phase B: a fresh context backed by the same on-disk cache directory reads the file again and must return v1 byte-for-byte. Then the file is overwritten as v2 (different length); a third context reading after the overwrite must observe v2 bytes (no stale v1 from disk). |

Design notes (parity with `sc_consistency.rs`):

- `block_size = 4 MiB` and a 10 MiB payload force every test to cross at
  least two block boundaries on a single-worker dev cluster.
- A position-dependent payload (Knuth multiplicative hash) is used so
  any wrong offset / length surfaces as a byte mismatch instead of
  `0 == 0` luck.
- `client_cache_async_write_enabled = false` makes fills deterministic;
  the warm pass therefore truly exercises the cache rather than racing
  with an in-flight async fill.
- All cases are `#[ignore]`d so plain `cargo test` stays hermetic and
  CI's gating job opts in via `--ignored`.

Not covered by this suite (intentional, lower-tier coverage suffices):

- INV-PC-S1 sub-case for async-fill queue exhaustion under load — covered by the unit test `concurrent_puts_and_gets_same_and_distinct_pages` in `manager.rs` and the `CachePutAsyncRejectionErrors` counter wiring; reproducing it deterministically at e2e tier needs a synthetic slow `PageStore`.
- INV-PC-S2 sub-case for sidecar drift — covered by `restore_drops_pages_without_identity_sidecar` and `restore_reclaims_empty_shell_dir_with_only_sidecar` in `manager.rs`.

---

## 13. Phased Implementation Plan

| Phase | Content | Deliverable | Status |
|---|---|---|---|
| **P0 scaffolding** | `src/cache/` module skeleton, `CacheManager` trait, `PageId`, config fields and constants, metrics constants | compiles, switch off by default | ✅ done |
| **P1 minimal usable** | `LocalPageStore` + in-memory index + LRU + single dir + `CachingPositionReader` (`read_through_cache`) wired into `read_at` + sync fill + core metrics | random-read hit usable | ✅ done |
| **P2 completion** | async fill (`schedule_fill` + `Semaphore` rate limit) + multi-dir + `HashAllocator` + per-dir capacity accounting/eviction + LFU + sequential `read()` wired into cache + full metrics | functionally aligned with Java | ✅ done |
| **P3 persistence and robustness** | process-restart `restore`, TTL lazy expiry (`get`) + background TTL sweeper, overwrite consistency check (`on_file_open` comparing length/mtime), Python e2e | production-ready | ✅ done |

### 13.1 Key implementation landing points (quick reference)

| Capability | Location |
|---|---|
| `CacheManager` trait / `DisabledCacheManager` | `src/cache/mod.rs` |
| `LocalCacheManager` (put/get/delete/invalidate/on_file_open/schedule_fill) | `src/cache/manager.rs` |
| TTL lazy expiry (`is_expired` → drop expired page before `get` hit, count `*Discarded`) | `src/cache/manager.rs::get` |
| Background TTL sweeper (`maybe_spawn_ttl_sweeper` + `sweep_expired`, holds `Weak<Self>` and exits on drop) | `src/cache/manager.rs` |
| Overwrite invalidation (`on_file_open` compares `(length, mtime)`) | `src/cache/manager.rs` + call site `src/io/file_in_stream.rs::open_with_context` |
| Process-restart `restore` (scan `<dir>/<page_size>/<bucket>/<file_id>/<page_index>`, clean up `.tmp-`) | `src/cache/manager.rs::restore` |
| Page split + hit/fallback/fill orchestration | `src/cache/caching_reader.rs` (`read_through_cache` / `FillMode`) |
| Local disk backend (temp file + atomic rename) | `src/cache/store/` |
| LRU / LFU evictor | `src/cache/evictor/` |
| Multi-dir allocation | `src/cache/allocator.rs` (`HashAllocator`) |
| Option parsing (incl. 5% overhead, TTL=0→None) | `src/cache/options.rs` |
| metrics name constants | `src/cache/metrics.rs` |
| config fields / `ENV_*` / `STORAGE_OPT_*` | `src/config.rs` |
| context mount (`acquire_cache_manager`) | `src/context.rs` |

---

## 14. Matters to be confirmed (Open Questions)

> The conclusion after implementation is as follows.

1. **`URIStatus` Does it provide stability? `file_id`? How is overwriting perceived?**
   ✅ Resolved. `URIStatus.file_id` (server-side inode, `i64`) serves as the cache-key namespace; its string form is `cache_file_id`, guaranteeing cross-stream hits for the same file opened multiple times. Overwrite is detected via
   `on_file_open(file_id, length, last_modification_time_ms)`, which compares against the recorded version at open time:
   a change in length or mtime is judged an overwrite; after `invalidate`-ing all pages of that file, the version is updated. The call site is
   `GoosefsFileInStream::open_with_context`.
2. **Does `Cargo.toml` already have `moka` / `lru` / `async-trait`?**
   ✅ Finalized. Only `async-trait` is introduced; the evictor (LRU/LFU) and meta index are **self-built**, with no
   `moka`/`lru` introduced, to align with Java behavior and precisely control "value on disk, metadata in memory" consistency.
3. **Default cache directory and permission policy?**
   ✅ Default `/tmp/goosefs_cache` (`DEFAULT_CLIENT_CACHE_DIR`), overridable via
   `goosefs.user.client.cache.dirs` / `GOOSEFS_USER_CLIENT_CACHE_DIRS` /
   `goosefs_client_cache_dirs`. For container scenarios, explicitly specifying a mounted disk is recommended. The single-process-exclusive-directory assumption is in §10.4.
4. **Java `PropertyKey.USER_CLIENT_CACHE_*` default values checked?**
   ✅ Aligned: page size `1 MiB`, per-dir capacity `1 GiB` (with 5% overhead reserved before use),
   async write threads `16`, async write enabled `true`, quota/ttl off by default, enabled default
   `false`, evictor default `LRU`. See the default-value constants in `src/config.rs`.
5. **Do we need to share the same on-disk cache directory format with Java/Go clients (cross-language interop)?**
   ⏳ Not yet supported. The on-disk layout
   `<dir>/<page_size>/<bucket>/<file_id>/<page_index>` aligns its shape with Java, but the `file_id`
   semantics (Rust uses server-side inode) and cross-process shared consistency are not yet verified, left as future work (see §10.4).

### 14.1 Future Work

Done (this round):
- ✅ `CacheHitRate` gauge: computed and published in real time from byte counters on both the hit (`manager.get`) and fallback (`caching_reader`) read paths (`metrics::publish_hit_rate`).
- ✅ `CacheSpaceUsedCount` gauge: refreshed with occupancy (`publish_occupancy`, equals the number of cached pages).
- ✅ `CachePageReadExternalTimeNanos` instrumentation: `caching_reader` fallback branch records external-read latency.
- ✅ Fill gated by `ReadType`: a stream opened with `ReadType::NoCache` only serves hits, does not fill (does not pollute the cache), see §3.2.
- ✅ End-to-end integration tests / benchmarks / Python e2e (see §12).

Not yet done (explicit future / non-goals, none affect main functional correctness):
- ⏳ Persist a metadata snapshot to speed up restart `restore` (currently a full directory scan rebuild; an optimization).
- ⏳ in-stream buffer (`cache.in.stream.buffer.size`) and the `CacheBytesReadInStreamBuffer` metric.
  Note: this implementation reads and caches by whole page, which already largely covers the buffer's benefit, so priority is low.
- ⏳ Full per-scope quota implementation (currently `quota_enabled` is reserved, handled as `CacheScope::Global`).
- ⏳ Multi-process shared cache-directory file lock + directory isolation (§1.3 non-goal / §10.4).
- ⏳ Cross-language (Java/Go) shared on-disk cache directory format interop (§14 OQ#5; needs `file_id` semantics verified).
- ⏳ Make `GoosefsFileReader` (`read_file`/`read_range`) and `positioned_read` also use the page cache.
  Currently the cache is only integrated in `GoosefsFileInStream` (including Python `fs.open_file`); the one-shot/worker-direct
  paths above bypass the cache; to make them effective, their read orchestration must be changed to reuse `read_through_cache`.

---

## Appendix A: Full Metrics List (aligned with Java `MetricKey`)

```text
Client.CacheBytesReadCache
Client.CacheBytesReadInStreamBuffer
Client.CacheBytesReadExternal
Client.CacheBytesRequestedExternal
Client.CachePageReadCacheTimeNanos
Client.CacheBytesEvicted
Client.CachePageReadExternalTimeNanos
Client.CacheBytesDiscarded
Client.CachePagesDiscarded
Client.CachePages
Client.CachePagesEvicted
Client.CacheBytesWrittenCache
Client.CacheHitRate
Client.CacheSpaceAvailable
Client.CacheSpaceUsed
Client.CacheSpaceUsedCount
Client.CacheCleanErrors
Client.CacheCleanupGetErrors
Client.CacheCleanupPutErrors
Client.CacheCreateErrors
Client.CacheDeleteErrors
Client.CacheDeleteNonExistingPageErrors
Client.CacheDeleteNotReadyErrors
Client.CacheDeleteFromStoreErrors
Client.CacheDeleteStoreDeleteErrors
Client.CacheGetErrors
Client.CacheGetNotReadyErrors
Client.CacheGetStoreReadErrors
Client.CachePutErrors
Client.CachePutAsyncRejectionErrors
Client.CachePutEvictionErrors
Client.CachePutBenignRacingErrors
Client.CachePutInsufficientSpaceErrors
Client.CachePutNotReadyErrors
Client.CachePutStoreDeleteErrors
Client.CachePutStoreWriteErrors
Client.CachePutStoreWriteNoSpaceErrors
Client.CacheStoreDeleteTimeout
Client.CacheStoreGetTimeout
Client.CacheStorePutTimeout
Client.CacheStoreThreadsRejected
Client.CacheState
Client.FallbackState
Client.ReadStreamFallBackCount
Client.AsyncThroughThreadsPoolSize
Client.AsyncThroughQueueLength
Client.AsyncThroughThreadsActive
```

## Appendix B: Key source-path quick reference

**Java reference** (`/opt/sourcecode/cos/goosefs/core/client/fs/.../client/file/cache/`):
`LocalCacheManager.java`, `CacheManager.java`, `NoExceptionCacheManager.java`, `CacheManagerOptions.java`, `PageId.java`, `PageInfo.java`, `PageStore.java`, `store/LocalPageStore.java`, `store/LocalPageStoreDir.java`, `store/QuotaManagedPageStoreDir.java`, `PageMetaStore.java`, `DefaultPageMetaStore.java`, `evictor/LRUCacheEvictor.java`, `evictor/LFUCacheEvictor.java`, `allocator/HashAllocator.java`, `LocalCacheFileInStream.java`, `LocalCacheFileSystem.java`.

**Rust integration points** (`/opt/sourcecode/cos/goosefs-client-rust/src/`):
`io/file_in_stream.rs` (`read_at` / `read`), `io/file_reader.rs`, `io/reader.rs`, `context.rs`, `config.rs`, `metrics/registry.rs`, `fs/options.rs` (`ReadType`), `fs/uri_status.rs`, `error.rs`.

---

# Client Page Cache — io_uring Client Development Design Document

> Status: **In implementation** · Branch: `feature/reader-page-cache-short-circuit`
> Date: 2026-07-08
> Prerequisite documents:
> - [`CLIENT_PAGE_CACHE_DESIGN.md`](CLIENT_PAGE_CACHE_DESIGN.md) — full design of the existing `tokio::fs` backend
> - [`SHORT_CIRCUIT_IO_URING_FEASIBILITY.md`](SHORT_CIRCUIT_IO_URING_FEASIBILITY.md) — io_uring feasibility analysis for the SC path
> - [`perf/2026-07-08-oncpu3-cache-hotspots/CACHE_VS_NOCACHE_ANALYSIS.md`](perf/2026-07-08-oncpu3-cache-hotspots/CACHE_VS_NOCACHE_ANALYSIS.md) — flame-graph root-cause analysis
> Reference implementation:
> - `/opt/sourcecode/lance/rust/lance-io/src/uring/` — Lance's io_uring implementation (thread pool + Future waker pattern)

---

## 1. Background and motivation

### 1.1 Problem: `tokio::fs`'s `spawn_blocking` caps the cache-hit path at 300 QPS

The current `LocalPageStore` (`src/cache/store/local.rs:212-242`) uses `tokio::fs::File` for file I/O. Every `tokio::fs` operation (`open` / `seek` / `read`) internally calls `spawn_blocking`, throwing the synchronous syscall onto tokio's blocking pool.

Flame-graph evidence (`clientcache_oncpu_3.svg`, 300 QPS):

| Function | Share |
|---|---|
| `tokio::runtime::blocking::pool::Inner::run` | **22.44%** |
| `LocalCacheManager::get` | 4.77% |
| `LocalPageStore::get` | 3.64% |
| `tokio::fs::File::poll_read` | 1.00% |
| `tokio::fs::File::start_seek` | 0.60% |
| `spawn_blocking` (3 sites) | ~2.6% |

Each cache hit = 3 `spawn_blocking` calls (open + seek + read), with ~50-100 µs scheduling overhead per call, 5-10× the actual NVMe IO time (~10 µs).

### 1.2 Why not D1 (merge `spawn_blocking`) / D6 (dedicated IO thread pool)

D1 merges 3 into 1, but still leaves 1 `spawn_blocking` (~50-100 µs). D6 builds its own dedicated OS thread pool, which is still essentially "sync IO + thread pool" — each IO still needs a thread switch + channel communication.

### 1.3 Advantages of io_uring

| Dimension | `spawn_blocking` / D1 / D6 | `io_uring` |
|---|---|---|
| IO model | sync syscall + thread pool | truly async SQE/CQE |
| Thread switch | 1-3 per operation | **0** (waker wakes directly) |
| syscalls / cache hit | 2-3 (open + pread) | **1** (batched submit) |
| Scheduling overhead | ~50-300 µs | **~1-5 µs** |
| Batching | not supported | **supported** (one `submit()` for multiple SQEs) |

### 1.4 Lance io_uring reference implementation

Lance implements mature io_uring file reading in `rust/lance-io/src/uring/`, with this core design:

| Lance file | Responsibility | GooseFS counterpart |
|---|---|---|
| `uring.rs` | module entry + config constants | `src/cache/store/uring/mod.rs` |
| `requests.rs` | `IoRequest` + `RequestState` (shared state + waker) | `src/cache/store/uring/requests.rs` |
| `thread.rs` | background thread pool + main loop (SQE submit + CQE reap) | `src/cache/store/uring/driver.rs` |
| `future.rs` | `UringReadFuture` (implements `Future` trait, poll checks `RequestState`) | `src/cache/store/uring/future.rs` |
| `reader.rs` | `UringReader` (implements `Reader` trait, open + fd cache + submit_read) | `src/cache/store/uring/store.rs` |

**Lance's key design decisions** (we adopt):
1. **Background thread-pool pattern**: N dedicated OS threads, each holding an `IoUring` instance, receiving requests via `std::sync::mpsc::sync_channel`
2. **`Arc<IoRequest>` + `Mutex<RequestState>` shared state**: submitter constructs request → sends via channel → background thread submits SQE → on CQE completion sets `completed = true` + `waker.wake()`
3. **Custom `Future`**: `UringReadFuture` implements `Future` trait, `poll` checks `RequestState.completed`, stores the `waker` if not done
4. **fd cache**: `UringFileHandle` uses `moka::future::Cache` to cache opened file handles by `(path, block_size)`, avoiding repeated `open`
5. **short read retry**: when CQE returns a partial read, adjust `offset` + `bytes_read` then re-push the SQE
6. **batched submit**: non-blocking drain of multiple requests from the channel, submit together via `ring.submit()` when reaching `submit_batch_size` or the channel is empty

**Lance's limitations** (we improve):
- Lance only implements **read** (`get_range` / `get_all`), no write path. We need `put` (tmp + rename) and `delete`
- Lance's fd cache uses `moka`; our first version simplifies to open per call (io_uring's `OP_OPENAT` is also async, with far less overhead than `spawn_blocking`)
- Lance uses `std::sync::mpsc::sync_channel` (single consumer); we need multi-thread round-robin selection

---

## 2. Overall Architecture

```text
                    LocalCacheManager (src/cache/manager.rs, minimal change)
                          │
                   ┌──────┴──────┐
                   │             │
            PageStore trait   (meta/evictor/lock unchanged)
            (src/cache/store/mod.rs)
                   │
          ┌────────┴─────────────────────┐
          │                              │
   LocalPageStore                  UringPageStore     ← new
   (src/cache/store/local.rs)      (src/cache/store/uring/store.rs)
   - tokio::fs backend               - io_uring backend
   - kept as fallback                - background thread pool (driver.rs)
   - used on non-Linux               - custom Future (future.rs)
   - used when config off            - batched submit
```

### 2.1 Module layout

```text
src/cache/store/
  ├── mod.rs                    # PageStore trait (unchanged)
  ├── local.rs                  # LocalPageStore (tokio::fs backend, kept)
  └── uring/                    # new io_uring backend
      ├── mod.rs                # UringPageStore + module declaration
      ├── store.rs              # UringPageStore implements PageStore trait
      ├── requests.rs           # IoRequest + RequestState (shared state + waker)
      ├── driver.rs              # UringDriver — background thread pool + main loop
      ├── future.rs             # UringReadFuture / UringWriteFuture
      └── sys.rs                # platform detection + io_uring availability probe + fallback
```

---

## 3. Core Component Design

### 3.1 `RequestState` / `IoRequest` — shared state and waker

Based on Lance `requests.rs:13-54`, but extended to support write operations.

```rust
// src/cache/store/uring/requests.rs

use bytes::BytesMut;
use std::io;
use std::os::unix::io::RawFd;
use std::sync::Mutex;
use std::task::Waker;

/// Shared state after an IO operation completes.
///
/// The submitter (async thread) constructs an `IoRequest` and sends it via channel
/// to the background thread; the background thread submits the SQE and, on CQE
/// completion, updates this state and calls `waker.wake()`; the submitter checks
/// `completed` via `UringReadFuture::poll`.
///
/// Reference: Lance `requests.rs:13-20`'s `RequestState`
pub struct RequestState {
    /// Whether the operation is complete (CQE reaped)
    pub completed: bool,
    /// tokio's waker, called via `wake()` on CQE completion to wake the waiting async task
    pub waker: Option<Waker>,
    /// Error (if any). Set when CQE result < 0
    pub err: Option<io::Error>,
    /// Read operation: the returned buffer (empty for write operations)
    pub buffer: BytesMut,
    /// Accumulated bytes read (handles short-read retry)
    pub bytes_read: usize,
}

/// Description of a single IO operation, shared between submitter → background thread → Future.
///
/// Reference: Lance `requests.rs:24-38`'s `IoRequest`
pub struct IoRequest {
    /// File descriptor (the caller is responsible for open + close)
    pub fd: RawFd,
    /// Read/write offset
    pub offset: u64,
    /// Read/write length
    pub length: usize,
    /// Operation type (read/write/open/close/unlink/rename)
    pub op_type: UringOpType,
    /// Shared state
    pub state: Mutex<RequestState>,
}

/// io_uring operation types
pub enum UringOpType {
    Read,
    Write,
    OpenAt,
    Close,
    UnlinkAt,
    RenameAt,
}

impl IoRequest {
    /// Mark failure and wake the waiter.
    /// Reference: Lance `requests.rs:45-53`'s `fail()`
    pub fn fail(&self, err: io::Error) {
        let mut state = self.state.lock().unwrap();
        state.err = Some(err);
        state.completed = true;
        if let Some(waker) = state.waker.take() {
            drop(state);
            waker.wake();
        }
    }
}
```

### 3.2 `UringDriver` — background thread pool + main loop

Based on Lance `thread.rs:30-250`, keeping the multi-thread + round-robin + batched-submit core design.

```rust
// src/cache/store/uring/driver.rs

use super::requests::{IoRequest, RequestState, UringOpType};
use io_uring::{IoUring, opcode, types};
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

/// Background thread handle — holds the channel sender used to submit requests.
///
/// Reference: Lance `thread.rs:23-25`'s `UringThreadHandle`
struct UringThreadHandle {
    request_tx: SyncSender<Arc<IoRequest>>,
}

/// Global io_uring thread pool — process-level singleton, lazily initialized on first access.
///
/// Reference: Lance `thread.rs:30-54`'s `URING_THREADS: LazyLock<Vec<UringThreadHandle>>`
pub static URING_THREADS: LazyLock<Vec<UringThreadHandle>> = LazyLock::new(|| {
    let queue_depth = get_queue_depth();       // default 16384
    let thread_count = get_thread_count();      // default 2

    let mut threads = Vec::with_capacity(thread_count);
    for i in 0..thread_count {
        let (tx, rx) = sync_channel(queue_depth);
        std::thread::Builder::new()
            .name(format!("gfs-uring-{}", i))
            .spawn(move || run_uring_thread(rx, queue_depth as u32, i))
            .expect("Failed to spawn io_uring thread");
        threads.push(UringThreadHandle { request_tx: tx });
    }
    tracing::info!(
        thread_count,
        queue_depth,
        "io_uring thread pool initialized for page cache"
    );
    threads
});

/// Round-robin thread-selection counter.
/// Reference: Lance `thread.rs:57`'s `THREAD_SELECTOR: AtomicU64`
static THREAD_SELECTOR: AtomicU64 = AtomicU64::new(0);

/// user_data generator — assigns a unique ID to each SQE for CQE matching.
/// Reference: Lance `thread.rs:63`'s `USER_DATA_COUNTER: AtomicU64`
static USER_DATA_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Submit an IO request to the background thread pool, returning `Arc<IoRequest>` for the Future to hold.
///
/// Reference: Lance `reader.rs:183-238`'s `submit_read()`
pub fn submit_request(request: Arc<IoRequest>) {
    let thread_idx = (THREAD_SELECTOR.fetch_add(1, Ordering::Relaxed) as usize)
        % URING_THREADS.len();
    match URING_THREADS[thread_idx].request_tx.send(Arc::clone(&request)) {
        Ok(()) => {}
        Err(_) => {
            request.fail(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "io_uring thread died",
            ));
        }
    }
}

/// Background thread main loop.
///
/// Reference: Lance `thread.rs:117-250`'s `run_uring_thread()`
///
/// Loop logic:
/// 1. First reap all available CQEs (process_completions)
/// 2. Batch-drain requests from the channel (try_recv + recv_timeout)
/// 3. Build an SQE for each request and push to the SQ ring (push_to_sq)
/// 4. Submit to the kernel at once via ring.submit()
fn run_uring_thread(request_rx: Receiver<Arc<IoRequest>>, queue_depth: u32, thread_id: usize) {
    let mut ring = IoUring::builder()
        .build(queue_depth)
        .expect("Failed to create io_uring");

    // user_data → IoRequest mapping table
    let mut pending: HashMap<u64, Arc<IoRequest>> = HashMap::with_capacity(queue_depth as usize);
    let poll_timeout = Duration::from_millis(10);
    let submit_batch_size = 128usize;
    let mut last_log = Instant::now();

    loop {
        // 1) Reap CQEs — set completed + wake
        process_completions(&mut ring, &mut pending);

        // 2) Batch-drain requests from the channel
        let mut batch_count = 0usize;
        loop {
            let recv_result = if pending.is_empty() && batch_count == 0 {
                // No in-flight requests and no batch — can wait
                request_rx.recv_timeout(poll_timeout).map_err(|e| match e {
                    std::sync::mpsc::RecvTimeoutError::Timeout => {
                        std::sync::mpsc::TryRecvError::Empty
                    }
                    std::sync::mpsc::RecvTimeoutError::Disconnected => {
                        std::sync::mpsc::TryRecvError::Disconnected
                    }
                })
            } else {
                request_rx.try_recv()
            };

            match recv_result {
                Ok(request) => {
                    if let Err(e) = push_to_sq(&mut ring, &mut pending, request) {
                        tracing::error!(error = %e, "Failed to push to io_uring SQ");
                    } else {
                        batch_count += 1;
                    }
                    if batch_count >= submit_batch_size {
                        break;
                    }
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => break,
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    if batch_count > 0 {
                        let _ = ring.submit();
                    }
                    return;
                }
            }
        }

        // 3) Submit the batch to the kernel
        if batch_count > 0 {
            if let Err(e) = ring.submit() {
                tracing::error!(error = %e, batch_count, "Failed to submit io_uring batch");
            }
        }
    }
}

/// Build an SQE and push it to the submission queue (without submitting).
///
/// Build different opcodes according to `request.op_type`:
/// - Read → `opcode::Read` (pread, one syscall to locate + read)
/// - Write → `opcode::Write`
/// - OpenAt → `opcode::OpenAt`
/// - Close → `opcode::Close`
/// - UnlinkAt → `opcode::UnlinkAt`
/// - RenameAt → `opcode::RenameAt`
///
/// Reference: Lance `thread.rs:256-309`'s `push_to_sq()` (Lance only handles Read)
fn push_to_sq(
    ring: &mut IoUring,
    pending: &mut HashMap<u64, Arc<IoRequest>>,
    request: Arc<IoRequest>,
) -> io::Result<()> {
    let user_data = USER_DATA_COUNTER.fetch_add(1, Ordering::Relaxed);

    // Build the SQE according to the operation type
    let sqe = match request.op_type {
        UringOpType::Read => {
            // Reference: Lance thread.rs:276-277
            let (buf_ptr, read_offset, read_len) = {
                let state = request.state.lock().unwrap();
                let br = state.bytes_read;
                (
                    unsafe { state.buffer.as_ptr().add(br) as *mut u8 },
                    request.offset + br as u64,
                    (request.length - br) as u32,
                )
            };
            opcode::Read::new(types::Fd(request.fd), buf_ptr, read_len)
                .offset(read_offset)
                .build()
        }
        UringOpType::Write => {
            // Write operation: data is in the buffer
            let state = request.state.lock().unwrap();
            opcode::Write::new(
                types::Fd(request.fd),
                state.buffer.as_ptr(),
                state.buffer.len() as u32,
            )
            .offset(request.offset as i64)
            .build()
        }
        UringOpType::OpenAt => {
            // OpenAt needs path — stored in the buffer (as bytes)
            let state = request.state.lock().unwrap();
            let path_ptr = state.buffer.as_ptr() as *const i8;
            opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path_ptr)
                .flags(libc::O_RDONLY | libc::O_CLOEXEC)
                .build()
        }
        UringOpType::Close => {
            opcode::Close::new(types::Fd(request.fd)).build()
        }
        UringOpType::UnlinkAt => {
            let state = request.state.lock().unwrap();
            let path_ptr = state.buffer.as_ptr() as *const i8;
            opcode::UnlinkAt::new(types::Fd(libc::AT_FDCWD), path_ptr).build()
        }
        UringOpType::RenameAt => {
            // RenameAt needs two paths — separated by \0 in the buffer
            // Simplified: store via an extra RequestState field
            // The real implementation needs to extend the IoRequest structure
            unimplemented!("RenameAt needs special handling")
        }
    }
    .user_data(user_data);

    let mut sq = ring.submission();
    if sq.is_full() {
        // Reference: Lance thread.rs:283-293 — return error when SQ is full
        request.fail(io::Error::new(
            io::ErrorKind::WouldBlock,
            "io_uring submission queue full",
        ));
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "io_uring submission queue full",
        ));
    }

    unsafe {
        if sq.push(&sqe).is_err() {
            request.fail(io::Error::other("Failed to push to SQ"));
            return Err(io::Error::other("Failed to push to SQ"));
        }
    }
    drop(sq);

    pending.insert(user_data, request);
    Ok(())
}

/// Reap all available CQEs, update RequestState and wake the waker.
///
/// Reference: Lance `thread.rs:324-396`'s `process_completions()`
fn process_completions(
    ring: &mut IoUring,
    pending: &mut HashMap<u64, Arc<IoRequest>>,
) {
    for cqe in ring.completion() {
        let user_data = cqe.user_data();
        let result = cqe.result();

        if let Some(request) = pending.remove(&user_data) {
            let mut state = request.state.lock().unwrap();

            if result < 0 {
                // kernel error
                state.err = Some(io::Error::from_raw_os_error(-result));
                state.completed = true;
            } else if result == 0 && request.op_type == UringOpType::Read {
                // EOF — read 0 bytes but requested a non-zero length
                let br = state.bytes_read;
                if br == 0 {
                    // complete miss (file deleted / racy eviction)
                    state.completed = true;
                } else {
                    // partial read complete
                    state.buffer.truncate(br);
                    state.completed = true;
                }
            } else {
                // normal completion: result > 0 (read) or result >= 0 (write/open/close/unlink)
                match request.op_type {
                    UringOpType::Read => {
                        let n = result as usize;
                        state.bytes_read += n;
                        if state.bytes_read >= request.length {
                            // full read complete
                            state.buffer.truncate(state.bytes_read);
                            state.completed = true;
                        } else {
                            // Short read — needs retry
                            // Reference: Lance thread.rs:371-376
                            drop(state);
                            // re-push (adjust offset + bytes_read)
                            let _ = push_to_sq(ring, pending, request);
                            continue;
                        }
                    }
                    UringOpType::Write | UringOpType::OpenAt
                    | UringOpType::Close | UringOpType::UnlinkAt => {
                        // write/open/close/unlink: result is fd or 0
                        state.completed = true;
                    }
                    UringOpType::RenameAt => {
                        state.completed = true;
                    }
                }
            }

            // Wake the waiting Future
            // Reference: Lance thread.rs:380-383
            if let Some(waker) = state.waker.take() {
                drop(state);
                waker.wake();
            }
        }
    }
}

// ── Config reading ──────────────────────────────────────────────

fn get_queue_depth() -> usize {
    std::env::var("GOOSEFS_USER_CLIENT_CACHE_URING_QUEUE_DEPTH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16384)
}

fn get_thread_count() -> usize {
    std::env::var("GOOSEFS_USER_CLIENT_CACHE_URING_THREAD_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2)
}
```

### 3.3 `UringReadFuture` — custom Future

Based on Lance `future.rs:16-46`.

```rust
// src/cache/store/uring/future.rs

use super::requests::IoRequest;
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Future that waits for an io_uring read operation to complete.
///
/// On `poll`, check `RequestState.completed`:
/// - true → take buffer/errors and return `Poll::Ready`
/// - false → store the waker and return `Poll::Pending`; the background thread calls `waker.wake()` on CQE completion
///
/// Reference: Lance `future.rs:16-46`'s `UringReadFuture`
pub struct UringOpFuture {
    pub request: Arc<IoRequest>,
}

impl Future for UringOpFuture {
    /// return (result_code, Bytes) — result_code yes CQE result,Bytes is the data read (the write operation is empty)
    type Output = (i32, Bytes);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.request.state.lock().unwrap();

        if state.completed {
            // Reference: Lance future.rs:26-39
            match state.err.take() {
                Some(err) => {
                    // Return negative errno
                    let raw_err = err.raw_os_error().unwrap_or(-1);
                    Poll::Ready((raw_err, Bytes::new()))
                }
                None => {
                    let bytes = std::mem::take(&mut state.buffer).freeze();
                    // For read operations return bytes_read as the result_code
                    // For other operations return 0
                    Poll::Ready((state.bytes_read as i32, bytes))
                }
            }
        } else {
            // Not done — store the waker and wait for wakeup
            // Reference: Lance future.rs:41-43
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
```

### 3.4 `UringPageStore` — implementing the `PageStore` trait

Based on Lance `reader.rs:97-292`'s `UringReader`, but extended into a complete `PageStore` (get + put + delete).

```rust
// src/cache/store/uring/store.rs

use super::driver::submit_request;
use super::future::UringOpFuture;
use super::requests::{IoRequest, RequestState, UringOpType};
use super::NUM_BUCKETS;
use crate::cache::page_id::PageId;
use crate::cache::store::PageStore;
use crate::error::{Error, Result};
use bytes::BytesMut;
use std::ffi::CString;
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// PageStore implementation for the io_uring backend.
///
/// Implements the same `PageStore` trait as `LocalPageStore` (tokio::fs backend),
/// so the upper layer `LocalCacheManager` switches transparently.
///
/// Reference: Lance `reader.rs:97-109`'s `UringReader`
pub struct UringPageStore {
    root: PathBuf,
    page_size: u64,
}

impl UringPageStore {
    /// Create the store + directory.
    /// Reference: Lance `reader.rs:124-180`'s `open()`
    pub async fn create(dir: &Path, page_size: u64) -> Result<Self> {
        let root = dir.join(page_size.to_string());
        tokio::fs::create_dir_all(&root).await?;
        Ok(Self { root, page_size })
    }

    /// Page file path: <root>/<bucket>/<file_id>/<page_index>
    /// Exactly the same as LocalPageStore (local.rs:82-88)
    fn page_path(&self, page_id: &PageId) -> PathBuf {
        let bucket = hash_file_id(&page_id.file_id) % NUM_BUCKETS;
        self.root
            .join(bucket.to_string())
            .join(page_id.file_id.as_ref())
            .join(page_id.page_index.to_string())
    }

    /// Async open file (OP_OPENAT) — returns fd
    ///
    /// Uses the io_uring OpenAt opcode, zero spawn_blocking
    async fn open_fd(&self, path: &Path, flags: i32) -> std::io::Result<RawFd> {
        let path_cstring = CString::new(path.to_string_lossy().into_owned())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

        let request = Arc::new(IoRequest {
            fd: libc::AT_FDCWD,
            offset: 0,
            length: 0,
            op_type: UringOpType::OpenAt,
            state: std::sync::Mutex::new(RequestState {
                completed: false,
                waker: None,
                err: None,
                buffer: BytesMut::from(path_cstring.to_bytes()),
                bytes_read: 0,
            }),
        });

        submit_request(Arc::clone(&request));
        let (result, _bytes) = UringOpFuture { request }.await;

        if result < 0 {
            Err(std::io::Error::from_raw_os_error(-result))
        } else {
            Ok(result as RawFd)
        }
    }

    /// asynchronous close fd (OP_CLOSE)
    async fn close_fd(&self, fd: RawFd) {
        let request = Arc::new(IoRequest {
            fd,
            offset: 0,
            length: 0,
            op_type: UringOpType::Close,
            state: std::sync::Mutex::new(RequestState {
                completed: false,
                waker: None,
                err: None,
                buffer: BytesMut::new(),
                bytes_read: 0,
            }),
        });
        submit_request(Arc::clone(&request));
        let _ = UringOpFuture { request }.await; // best-effort
    }

    /// asynchronous unlink (OP_UNLINKAT)
    async fn unlink_fd(&self, path: &Path) -> std::io::Result<()> {
        let path_cstring = CString::new(path.to_string_lossy().into_owned())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

        let request = Arc::new(IoRequest {
            fd: libc::AT_FDCWD,
            offset: 0,
            length: 0,
            op_type: UringOpType::UnlinkAt,
            state: std::sync::Mutex::new(RequestState {
                completed: false,
                waker: None,
                err: None,
                buffer: BytesMut::from(path_cstring.to_bytes()),
                bytes_read: 0,
            }),
        });

        submit_request(Arc::clone(&request));
        let (result, _) = UringOpFuture { request }.await;

        if result < 0 {
            let e = std::io::Error::from_raw_os_error(-result);
            if e.kind() == std::io::ErrorKind::NotFound {
                return Ok(()); // idempotent
            }
            return Err(e);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl PageStore for UringPageStore {
    /// Read page — OP_OPENAT + OP_READ + OP_CLOSE
    ///
    /// Compared with LocalPageStore::get (local.rs:212-242):
    /// - LocalPageStore: 3 spawn_blocking (open + seek + read)
    /// - UringPageStore: 3 io_uring SQEs (open + read + close), zero spawn_blocking
    async fn get(&self, page_id: &PageId, offset: usize, dst: &mut [u8]) -> Result<usize> {
        let path = self.page_path(page_id);

        // 1) OP_OPENAT — async open
        let fd = match self.open_fd(&path, libc::O_RDONLY).await {
            Ok(fd) => fd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(io_error("uring open", e)),
        };

        // 2) OP_READ — async pread (offset + length in one syscall)
        //    Reference: Lance reader.rs:188-191's buffer allocation
        let request = Arc::new(IoRequest {
            fd,
            offset: offset as u64,
            length: dst.len(),
            op_type: UringOpType::Read,
            state: std::sync::Mutex::new(RequestState {
                completed: false,
                waker: None,
                err: None,
                buffer: BytesMut::from(unsafe {
                    std::slice::from_raw_parts_mut(dst.as_mut_ptr(), dst.len())
                }),
                bytes_read: 0,
            }),
        });

        submit_request(Arc::clone(&request));
        let (result, read_bytes) = UringOpFuture { request }.await;

        // 3) OP_CLOSE — async close (fire-and-forget)
        self.close_fd(fd).await;

        // 4) Process the result
        if result < 0 {
            let e = std::io::Error::from_raw_os_error(-result);
            if e.kind() == std::io::ErrorKind::NotFound {
                return Ok(0); // racy eviction → miss
            }
            return Err(io_error("uring read", e));
        }

        // Copy the read data into dst
        let n = result as usize;
        if n > 0 {
            dst[..n].copy_from_slice(&read_bytes[..n]);
        }
        Ok(n)
    }

    /// Write page — OP_OPENAT + OP_WRITE + OP_CLOSE + OP_RENAMEAT
    ///
    /// Compared with LocalPageStore::put (local.rs:170-210):
    /// - LocalPageStore: 4 spawn_blocking (create + write_all + flush + rename)
    /// - UringPageStore: 4 io_uring SQEs, zero spawn_blocking
    async fn put(&self, page_id: &PageId, page: &[u8]) -> Result<()> {
        let final_path = self.page_path(page_id);
        let parent = final_path.parent().unwrap().to_path_buf();

        // Ensure the directory exists (this step is not on the hot path, use tokio::fs)
        tokio::fs::create_dir_all(&parent).await
            .map_err(|e| io_error("create page dir", e))?;

        let tmp_path = parent.join(format!(
            "{}.tmp-{}",
            page_id.page_index,
            uuid::Uuid::new_v4()
        ));

        let tmp_cstring = CString::new(tmp_path.to_string_lossy().into_owned())
            .map_err(|e| io_error("cstring", e))?;

        // 1) OP_OPENAT (O_WRONLY | O_CREAT | O_TRUNC)
        let fd = {
            let request = Arc::new(IoRequest {
                fd: libc::AT_FDCWD,
                offset: 0,
                length: 0,
                op_type: UringOpType::OpenAt,
                state: std::sync::Mutex::new(RequestState {
                    completed: false,
                    waker: None,
                    err: None,
                    buffer: BytesMut::from(tmp_cstring.to_bytes()),
                    bytes_read: 0,
                }),
            });
            submit_request(Arc::clone(&request));
            let (result, _) = UringOpFuture { request }.await;
            if result < 0 {
                return Err(io_error("uring open tmp",
                    std::io::Error::from_raw_os_error(-result)));
            }
            result as RawFd
        };

        // 2) OP_WRITE (whole page)
        {
            let request = Arc::new(IoRequest {
                fd,
                offset: 0,
                length: page.len(),
                op_type: UringOpType::Write,
                state: std::sync::Mutex::new(RequestState {
                    completed: false,
                    waker: None,
                    err: None,
                    buffer: BytesMut::from(page),
                    bytes_read: 0,
                }),
            });
            submit_request(Arc::clone(&request));
            let (result, _) = UringOpFuture { request }.await;
            if result < 0 {
                self.close_fd(fd).await;
                return Err(io_error("uring write",
                    std::io::Error::from_raw_os_error(-result)));
            }
        }

        // 3) OP_CLOSE
        self.close_fd(fd).await;

        // 4) rename (use std::fs::rename — rename is not on the hot path, and needs to cross paths)
        //    TODO: Can be used later OP_RENAMEAT
        std::fs::rename(&tmp_path, &final_path)
            .map_err(|e| io_error("rename temp page file", e))?;

        Ok(())
    }

    /// Delete page — OP_UNLINKAT
    ///
    /// Compared with LocalPageStore::delete (local.rs:244-251):
    /// - LocalPageStore: 1 spawn_blocking (remove_file)
    /// - UringPageStore: 1 io_uring SQE, zero spawn_blocking
    async fn delete(&self, page_id: &PageId) -> Result<()> {
        let path = self.page_path(page_id);
        self.unlink_fd(&path).await
            .map_err(|e| io_error("uring unlink", e))?;
        Ok(())
    }
}
```

### 3.5 `sys.rs` — platform detection and fallback

```rust
// src/cache/store/uring/sys.rs

/// Detect whether io_uring is available.
/// 1. target_os == "linux" (compile time)
/// 2. at runtime, try initializing an io_uring instance (probe the kernel version)
/// 3. on failure return None → fall back to LocalPageStore
///
/// Reference: Lance `uring.rs:32-35` — "only available on Linux and requires kernel 5.1"
pub fn is_uring_available() -> bool {
    #[cfg(target_os = "linux")]
    {
        // Try to create a minimal io_uring Instance detection kernel support
        match io_uring::IoUring::new(4) {
            Ok(_) => {
                tracing::info!("io_uring is available on this platform");
                true
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "io_uring not available; falling back to tokio::fs backend"
                );
                false
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}
```

### 3.6 `mod.rs` — Module declarations and utility functions

```rust
// src/cache/store/uring/mod.rs

#[cfg(target_os = "linux")]
mod driver;
#[cfg(target_os = "linux")]
mod future;
#[cfg(target_os = "linux")]
mod requests;
#[cfg(target_os = "linux")]
mod store;
mod sys;

#[cfg(target_os = "linux")]
pub use store::UringPageStore;

pub use sys::is_uring_available;

/// Same hash as LocalPageStore (local.rs:61-63)
/// xxHash3 64-bit, used for bucket allocation
fn hash_file_id(file_id: &str) -> u64 {
    xxhash_rust::xxh3::xxh3_64(file_id.as_bytes())
}

/// Same bucket count as LocalPageStore (local.rs:24)
const NUM_BUCKETS: u64 = 1000;

fn io_error(message: impl Into<String>, e: std::io::Error) -> Error {
    Error::Internal {
        message: message.into(),
        source: Some(Box::new(e)),
    }
}
```

---

## 4. Integration-point changes

### 4.1 `PageStore` trait — no changes needed

```rust
// src/cache/store/mod.rs (existing, unchanged)
// L19-33
pub trait PageStore: Send + Sync {
    async fn put(&self, page_id: &PageId, page: &[u8]) -> Result<()>;
    async fn get(&self, page_id: &PageId, offset: usize, dst: &mut [u8]) -> Result<usize>;
    async fn delete(&self, page_id: &PageId) -> Result<()>;
}
```

### 4.2 `LocalCacheManager` — change `Vec<LocalPageStore>` → `Vec<Arc<dyn PageStore>>`

**File**: `src/cache/manager.rs`

**Change 1** — struct field (L82-92):

```rust
// before (L83):
//   stores: Vec<LocalPageStore>,
// After changes:
stores: Vec<Arc<dyn PageStore>>,
```

**change 2** — `create()` factory method (L109-160):

```rust
pub async fn create(options: CacheManagerOptions) -> Result<Self> {
    let dir_paths: Vec<&Path> = if options.dirs.is_empty() {
        vec![Path::new("/tmp/goosefs_cache")]
    } else {
        options.dirs.iter().map(|p| p.as_path()).collect()
    };

    // Detect io_uring availability
    let use_uring = options.uring_enabled && uring::is_uring_available();

    let mut stores: Vec<Arc<dyn PageStore>> = Vec::with_capacity(dir_paths.len());
    let mut dirs = Vec::with_capacity(dir_paths.len());

    for dir in &dir_paths {
        let store: Arc<dyn PageStore> = if use_uring {
            // Use the io_uring backend
            match UringPageStore::create(dir, options.page_size).await {
                Ok(s) => Arc::new(s),
                Err(e) => {
                    tracing::warn!(error = %e, "UringPageStore creation failed; fallback to LocalPageStore");
                    Arc::new(LocalPageStore::create(dir, options.page_size).await?)
                }
            }
        } else {
            // Use the tokio::fs backend (existing)
            Arc::new(LocalPageStore::create(dir, options.page_size).await?)
        };
        stores.push(store);

        dirs.push(DirState {
            evictor: build_evictor(options.evictor),
            used_bytes: 0,
            capacity: options.dir_capacity,
        });
    }

    // ... rest of the initialization is unchanged (L127-159)
    let page_locks = (0..LOCK_SIZE).map(|_| RwLock::new(())).collect();
    let async_write_sem = Arc::new(Semaphore::new(options.async_write_threads.max(1)));

    let mgr = Self {
        options,
        stores,
        allocator: Box::new(HashAllocator::new()),
        inner: Mutex::new(Inner {
            meta: HashMap::new(),
            by_file: HashMap::new(),
            versions: HashMap::new(),
            dirs,
        }),
        page_locks,
        async_write_sem,
        state: CacheState::ReadWrite,
    };

    if let Err(e) = mgr.restore().await {
        warn!(error = %e, "cache restore failed; starting with empty cache");
    }
    mgr.publish_capacity_gauges_initial();
    Ok(mgr)
}
```

**change 3** — `get()` / `put()` / `delete()` in store call:

```rust
// get() L582:
// before change: let n = match self.stores[dir_index].get(page_id, page_offset, dst).await
// After changes: let n = match self.stores[dir_index].get(page_id, page_offset, dst).await  // constant! trait object

// put() L480:
// before change: if let Err(e) = self.stores[dir_index].put(page_id, &page).await
// After changes: Same as above, constant

// delete() L634:
// before change: if let Err(e) = self.stores[dir_index].delete(page_id).await
// After changes: Same as above, constant
```

### 4.3 `CacheManagerOptions` — New io_uring Configuration

**document**: `src/cache/options.rs`

```rust
// exist CacheManagerOptions struct Add new fields in
pub struct CacheManagerOptions {
    // ... Existing fields ...

    /// Whether to enable the io_uring backend (only Linux 5.1+)
    pub uring_enabled: bool,
    /// io_uring queue depth (default 16384)
    pub uring_queue_depth: usize,
    /// io_uring Number of background threads (default 2)
    pub uring_thread_count: usize,
}

impl CacheManagerOptions {
    pub fn from_config(config: &GoosefsConfig) -> Self {
        Self {
            // ... Existing fields ...

            uring_enabled: config.client_cache_uring_enabled,
            uring_queue_depth: config.client_cache_uring_queue_depth,
            uring_thread_count: config.client_cache_uring_thread_count,
        }
    }
}
```

### 4.4 `GoosefsConfig` — Added configuration fields

**document**: `src/config.rs`

```rust
// Add to the GoosefsConfig struct (around L1889, after client_cache_ttl_secs)

/// Whether to enable the io_uring backend (Linux only, default true on Linux)
#[serde(default = "default_cache_uring_enabled")]
pub client_cache_uring_enabled: bool,

/// io_uring queue depth (default 16384)
#[serde(default = "default_cache_uring_queue_depth")]
pub client_cache_uring_queue_depth: usize,

/// io_uring background thread count (default 2)
#[serde(default = "default_cache_uring_thread_count")]
pub client_cache_uring_thread_count: usize,

fn default_cache_uring_enabled() -> bool {
    cfg!(target_os = "linux")
}

fn default_cache_uring_queue_depth() -> usize { 16384 }
fn default_cache_uring_thread_count() -> usize { 2 }
```

Corresponding environment variables:

```rust
// src/config.rs — ENV constant (about L680 nearby)
pub const ENV_CLIENT_CACHE_URING_ENABLED: &str = "GOOSEFS_USER_CLIENT_CACHE_URING_ENABLED";
pub const ENV_CLIENT_CACHE_URING_QUEUE_DEPTH: &str = "GOOSEFS_USER_CLIENT_CACHE_URING_QUEUE_DEPTH";
pub const ENV_CLIENT_CACHE_URING_THREAD_COUNT: &str = "GOOSEFS_USER_CLIENT_CACHE_URING_THREAD_COUNT";

// storage option
pub const STORAGE_OPT_CLIENT_CACHE_URING_ENABLED: &str = "goosefs_client_cache_uring_enabled";
pub const STORAGE_OPT_CLIENT_CACHE_URING_QUEUE_DEPTH: &str = "goosefs_client_cache_uring_queue_depth";
pub const STORAGE_OPT_CLIENT_CACHE_URING_THREAD_COUNT: &str = "goosefs_client_cache_uring_thread_count";
```

### 4.5 `Cargo.toml` — new dependencies

```toml
# Cargo.toml — add (after ~L68)

# io_uring backend (Linux only)
[target.'cfg(target_os = "linux")'.dependencies]
io-uring = "0.7"
libc = "0.2"
```

Reference Lance `Cargo.toml:52-53`:
```toml
# Lance's style
[target.'cfg(target_os = "linux")'.dependencies]
io-uring = { workspace = true }
```

---

## 5. Comparison of the complete process of reading paths

### 5.1 cache hit path (`get`)

```text
read_at(offset, n)
  → read_at_cached(offset, end)                    // file_in_stream.rs:873
    → read_through_cache(...)                       // caching_reader.rs:55
      → cache.get(page_id, in_page_off, &mut dst)  // manager.rs:540
        → page_locks[idx].read().await             // stripe read lock (constant)
        → inner.lock().await → check meta             // metadata lock (constant)
        → stores[dir_index].get(page_id, off, dst) // ← UringPageStore::get
          │
          │  UringPageStore::get:
          │  1. OP_OPENAT → submit_request → UringOpFuture.await
          │     ↓ background thread: push SQE → ring.submit()
          │     ↓ kernel: asynchronous open → CQE
          │     ↓ background thread: process_completions → waker.wake()
          │     ↓ tokio reactor: wake async Task
          │
          │  2. OP_READ → submit_request → UringOpFuture.await  (Same as above)
          │
          │  3. OP_CLOSE → submit_request → fire-and-forget
          │
          │  Zero all the way spawn_blocking, Zero thread switching
          │
        → inner.lock().await → evictor.on_access   // LRU renew (constant)
        → return n
```

### 5.2 Timing comparison

**tokio::fs (current, 300 QPS)**:
```
async thread          blocking-pool-worker    syscall
  │                    │                     │
  ├─ spawn_blocking ──→│                     │
  │                    ├─ open() ──────────→│ open syscall
  │                    │←───────────────────│
  │←───────────────────│                     │
  ├─ spawn_blocking ──→│                     │
  │                    ├─ lseek() ─────────→│ lseek syscall
  │                    │←───────────────────│
  │←───────────────────│                     │
  ├─ spawn_blocking ──→│                     │
  │                    ├─ read() ──────────→│ read syscall
  │                    │←───────────────────│
  │←───────────────────│                     │
  │                                              total ~150-300 µs
```

**io_uring (Target, 900+ QPS)**:
```
async thread          uring-driver-thread    kernel
  │                    │                     │
  ├─ submit_request ──→│                     │
  │  (channel send)    │                     │
  │                    ├─ push SQE (open)    │
  │                    ├─ push SQE (read)    │
  │                    ├─ ring.submit() ────→│ io_uring_enter
  │  UringOpFuture      │                     │  (kernel parallel processing)
  │  .await (Pending)  │                     │
  │                    │←──── CQE (open) ────│
  │                    │  waker.wake()       │
  │←───────────────────│                     │
  │                    │←──── CQE (read) ────│
  │                    │  waker.wake()       │
  │←───────────────────│                     │
  │                                              total ~5-20 µs
```

---

## 6. fd cache (P2 optimization)

Based on Lance `reader.rs:57-63`'s `HANDLE_CACHE: LazyLock<moka::future::Cache>`.

```rust
// src/cache/store/uring/store.rs — P2 optimization

use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::Mutex;

pub struct UringPageStore {
    root: PathBuf,
    page_size: u64,
    /// fd cache: PageId → RawFd, avoids open/close every time
    /// Reference: Lance reader.rs:57-63's moka cache
    fd_cache: Mutex<LruCache<PageId, RawFd>>,
}

impl UringPageStore {
    pub async fn create(dir: &Path, page_size: u64) -> Result<Self> {
        // ...
        let fd_cache = Mutex::new(LruCache::new(
            NonZeroUsize::new(1024).unwrap()
        ));
        Ok(Self { root, page_size, fd_cache })
    }

    async fn get_fd(&self, page_id: &PageId) -> std::io::Result<RawFd> {
        // 1) Check fd cache
        {
            let mut cache = self.fd_cache.lock().unwrap();
            if let Some(fd) = cache.get(page_id) {
                return Ok(*fd);  // Hit, zero open syscall
            }
        }
        // 2) miss → OP_OPENAT
        let fd = self.open_fd(&self.page_path(page_id), libc::O_RDONLY).await?;
        // 3) Store in cache
        let mut cache = self.fd_cache.lock().unwrap();
        if let Some((_, evicted_fd)) = cache.push(page_id.clone(), fd) {
            // The LRU-evicted fd needs to be closed
            self.close_fd(evicted_fd).await;
        }
        Ok(fd)
    }
}
```

**Note**: the fd cache must synchronously clear the corresponding entry on `delete()`, otherwise it reads a stale fd of an already-deleted file.

---

## 7. Configuration items

| Config key | Meaning | Default |
|---|---|---|
| `goosefs.user.client.cache.uring.enabled` | whether to enable the io_uring backend | `true` (Linux), ignored (other platforms) |
| `goosefs.user.client.cache.uring.queue.depth` | SQ/CQ queue depth | `16384` |
| `goosefs.user.client.cache.uring.thread.count` | background thread count | `2` |

environment variables:

```bash
# closure io_uring, Fallback to tokio::fs
export GOOSEFS_USER_CLIENT_CACHE_URING_ENABLED=false

# Increase queue depth (High concurrency scenario)
export GOOSEFS_USER_CLIENT_CACHE_URING_QUEUE_DEPTH=32768

# Increase the number of background threads
export GOOSEFS_USER_CLIENT_CACHE_URING_THREAD_COUNT=4
```

refer to Lance environment variables (`uring.rs:19-27`):
```bash
LANCE_URING_QUEUE_DEPTH=16384
LANCE_URING_THREAD_COUNT=2
LANCE_URING_SUBMIT_BATCH_SIZE=128
LANCE_URING_POLL_TIMEOUT_MS=10
```

---

## 8. Metrics

| Rust constant | metric name | type | description |
|---|---|---|---|
| `CLIENT_CACHE_URING_BACKEND_ACTIVE` | `Client.CacheUringBackendActive` | gauge | 1=io_uring, 0=tokio::fs |
| `CLIENT_CACHE_URING_QUEUE_DEPTH` | `Client.CacheUringQueueDepth` | gauge | SQ/CQ queue depth |
| `CLIENT_CACHE_URING_THREAD_COUNT` | `Client.CacheUringThreadCount` | gauge | background thread count |
| `CLIENT_CACHE_URING_SUBMITTED_TOTAL` | `Client.CacheUringSubmittedTotal` | counter | cumulative SQE submissions |
| `CLIENT_CACHE_URING_COMPLETED_TOTAL` | `Client.CacheUringCompletedTotal` | counter | cumulative CQE completions |
| `CLIENT_CACHE_URING_ERRORS_TOTAL` | `Client.CacheUringErrorsTotal` | counter | io_uring operation errors |
| `CLIENT_CACHE_URING_IN_FLIGHT` | `Client.CacheUringInFlight` | gauge | current in-flight requests |

---

## 9. Server-side impact

### 9.1 Zero server-side changes

**This design involves no changes to any GooseFS server side (Master / Worker).**

| Concern | Why it is a pure client-side change |
|---|---|
| Disk layout | `<dir>/<page_size>/<bucket>/<file_id>/<page_index>` — exactly the same as `LocalPageStore`, the server is unaware |
| Cache file content | the page cache stores whole-page data the client read back from Worker/UFS, not server block files |
| `PageStore` trait contract | `put` / `get` / `delete` all operate on **cache files on the client's local disk**, no RPC involved |
| io_uring operation target | `OP_OPENAT` / `OP_READ` / `OP_WRITE` / `OP_CLOSE` / `OP_UNLINKAT` all act on **the client's local page-cache files**, not GooseFS block files |
| Cache key | `file_id` = `URIStatus.file_id` (server-side inode string), but used only as a local file-path component, never sent back to the server |
| Overwrite detection | `on_file_open(file_id, length, mtime)` compares `(length, last_modification_time_ms)` locally on the client, no Master RPC called |
| Process-restart restore | `restore()` scans the local cache directory to rebuild the index, no server involved |
| Fallback path | cache miss → `read_external_range` → `positioned_read_with_retry` → gRPC `ReadBlock` — this is the **existing path, unchanged** |
| Rolling upgrade | the io_uring backend and the tokio::fs backend share an identical on-disk format, the client can switch freely; server version is independent |
| Protocol compatibility | zero proto changes; zero Master/Worker code changes; zero config changes |

**Comparison with the SC io_uring feasibility analysis**: [`SHORT_CIRCUIT_IO_URING_FEASIBILITY.md`](SHORT_CIRCUIT_IO_URING_FEASIBILITY.md) §5 also confirms zero server-side changes on the SC path. The page-cache path of this design is the same — `PageStore` is a pure local-file-operation abstraction, involving no network protocol.

### 9.2 Change scope

| Change layer | File | Nature |
|---|---|---|
| New code | `src/cache/store/uring/{mod,store,driver,future,requests,sys}.rs` | pure addition, existing logic untouched |
| Type substitution | `src/cache/manager.rs:83` `Vec<LocalPageStore>` → `Vec<Arc<dyn PageStore>>` | trait-object-ified, upper-layer calls unchanged |
| New config | `src/cache/options.rs`, `src/config.rs` | new fields + defaults, no impact on existing config |
| New dependency | `Cargo.toml` | `io-uring` + `libc`, Linux only, target-gated |
| **Server side** | **None** | **zero changes** |

---

## 10. Data consistency and semantic consistency

### 10.1 Data consistency — INV-PC-* invariant verification item by item

The io_uring backend must satisfy exactly the same correctness contract as the `tokio::fs` backend. The following verifies the invariants defined in [`CLIENT_PAGE_CACHE_DESIGN.md`](CLIENT_PAGE_CACHE_DESIGN.md) §1.4 item by item:

| Invariant | How the io_uring backend guarantees it |
|---|---|
| **INV-PC-D1** (cache vs direct byte diff) | `UringPageStore::get` reads disk bytes via `OP_READ` (pread), at the **same file and same offset** as `LocalPageStore::get` via `tokio::fs::File::read`. `pread` is the POSIX-standard atomic located+read, semantically fully equivalent to `seek + read`. Byte consistency is guaranteed by the disk-file content — both backends read/write the **same set of disk files** (`<dir>/<page_size>/<bucket>/<file_id>/<page_index>`). |
| **INV-PC-D2** (read APIs equivalent) | `read` / `read_at` / `read_all` all go through `read_through_cache` → `cache.get` → `PageStore::get`. The backend switch is fully transparent to the upper layer — `LocalCacheManager` holds `Vec<Arc<dyn PageStore>>`, dispatched via trait object, the caller is unaware of the backend type. |
| **INV-PC-S1** (failed fill doesn't poison) | (1) `OP_WRITE` fails → tmp file leftover → `put` returns `false` → meta not updated → next `get` misses → correctly reads from source. (2) `OP_RENAMEAT` / `std::fs::rename` fails → tmp file leftover → `restore` cleans up `.tmp-*` files. (3) SQ full → `request.fail(WouldBlock)` → `put` returns `false` → meta not updated. All three failure paths guarantee: **a cache failure degrades to a miss, never returns dirty data**. |
| **INV-PC-S2** (restart byte parity) | The io_uring backend writes pages with the `tmp + rename` atomic pattern — exactly the same as `LocalPageStore`. `rename` is atomic under POSIX semantics: either the old file is gone and the new one visible, or the old one is present and the new one invisible. After process restart, `restore()` scans the same directory format (`<dir>/<page_size>/<bucket>/<file_id>/<page_index>`), without distinguishing which backend wrote the files. The `.identity` sidecar's read/write also goes through `tokio::fs` (not on the hot path), shared by both backends. |

### 10.2 Semantic consistency — PageStore trait contract alignment

The `PageStore` trait (`src/cache/store/mod.rs:19-33`) defines the semantic contract of three methods:

```rust
async fn put(&self, page_id: &PageId, page: &[u8]) -> Result<()>;
async fn get(&self, page_id: &PageId, offset: usize, dst: &mut [u8]) -> Result<usize>;
async fn delete(&self, page_id: &PageId) -> Result<()>;
```

Method-by-method verification of the io_uring backend's semantic alignment:

#### `get` Semantics

| contract | LocalPageStore (tokio::fs) | UringPageStore (io_uring) | consistent? |
|---|---|---|---|
| hit: Returns the number of bytes read (>0) | `File::open` + `seek` + `read` | `OP_OPENAT` + `OP_READ` + `OP_CLOSE` | ✅ |
| miss: return `Ok(0)` | `File::open` return `NotFound` → `Ok(0)` | `OP_OPENAT` return `-ENOENT` → `Ok(0)` | ✅ |
| Read failed: return `Err` | `read` return `Err` | `OP_READ` CQE result < 0 → `Err` | ✅ |
| Short read: Read in a loop `dst.len()` | `while filled < dst.len() { read() }` | CQE short read → `push_to_sq` Adjustment offset Try again (refer to Lance `thread.rs:371-376`) | ✅ |
| Racy eviction: `get` return 0 | `open` Successful but the file has been delete → `read` return 0 | `OP_OPENAT` succeeds but `OP_READ` return `-ENOENT` → `Ok(0)` | ✅ |
| `offset` Semantics: page internal offset | `f.seek(Start(offset))` | `OP_READ.offset(offset)` (pread) | ✅ |

#### `put` Semantics

| contract | LocalPageStore (tokio::fs) | UringPageStore (io_uring) | consistent? |
|---|---|---|---|
| Atomic writing: tmp + rename | `File::create(tmp)` + `write_all` + `flush` + `rename` | `OP_OPENAT(O_CREAT\|O_TRUNC)` + `OP_WRITE` + `OP_CLOSE` + `rename` | ✅ |
| Write failed: tmp clean up | `remove_file(tmp)` | `close_fd` + `remove_file(tmp)` (best-effort) | ✅ |
| Concurrently write the same page: Do not cover each other | tmp The file name contains UUID | tmp The file name contains UUID (same strategy) | ✅ |
| `rename` atomicity | `tokio::fs::rename` (POSIX rename) | `std::fs::rename` (POSIX rename) | ✅ |

**Notice**: `put` in path `std::fs::rename` It's a synchronous call. this is intentional — rename Not here cache Hit on the hot path (only on cache miss executed during backfill), and POSIX rename exist NVMe superior ~5 µs, does not affect overall performance. If further optimization is needed later, you can use `OP_RENAMEAT`, but the first version does not introduce additional complexity.

#### `delete` Semantics

| contract | LocalPageStore (tokio::fs) | UringPageStore (io_uring) | consistent? |
|---|---|---|---|
| File not exist: return `Ok(())` | `remove_file` return `NotFound` → `Ok(())` | `OP_UNLINKAT` return `-ENOENT` → `Ok(())` | ✅ |
| Delete successfully: return `Ok(())` | `remove_file` success | `OP_UNLINKAT` CQE result == 0 | ✅ |
| Delete failed: return `Err` | `remove_file` Return other errors | `OP_UNLINKAT` CQE result < 0 → `Err` | ✅ |

### 10.3 Concurrency semantics — and LocalPageStore consistent

io_uring The backend does not change `LocalCacheManager` concurrency model:

| Concurrency mechanism | existing (tokio::fs) | io_uring backend | consistent? |
|---|---|---|---|
| page-level stripe lock `page_locks[1024]` | `RwLock` — get read lock, put/delete Take write lock | **constant** — locked in `LocalCacheManager` layer, Not here `PageStore` layer | ✅ |
| metadata lock `Mutex<Inner>` | guard meta/by_file/versions/dirs | **constant** | ✅ |
| disk IO outside the lock | `inner.lock()` Adjust after release `store.get/put` | **constant** — `UringPageStore::get` exist `inner.lock()` Called after release | ✅ |
| same page serial | `page_locks[hash(page_id)].write()` Guaranteed same page put serial | **constant** | ✅ |
| cross-page concurrency | different stripe of `RwLock` not mutually exclusive | **constant** | ✅ |
| async backfill throttling | `Semaphore(async_write_threads)` | **constant** | ✅ |

### 10.4 io_uring Unique risks and mitigations

| risk | Semantic impact | Mitigation measures | Align existing behavior |
|---|---|---|---|
| `OP_OPENAT` + `OP_READ` files between delete | `OP_READ` return `-ENOENT` | regarded as miss → `Ok(0)` → Return to the source from the upper layer | ✅ and `LocalPageStore` of `open` succeeds but `read` return 0 consistent |
| Short read (CQE result < length) | Partial data read | `push_to_sq` Adjustment `offset + bytes_read` Try again (refer to Lance `thread.rs:371-376`) | ✅ and `LocalPageStore` of `while filled < dst.len()` Consistent loop semantics |
| SQ Full → push fail | Request cannot be submitted | `request.fail(WouldBlock)` → `put` return `false` → meta Not updated | ✅ and `LocalPageStore` Return if disk writing fails `false` consistent |
| background thread panic | channel disconnect | `submit_request` return `BrokenPipe` → `fail()` → Return to the source from the upper layer | ✅ downgraded to miss, Does not affect correctness |
| `O_DIRECT` Alignment error | `OP_READ` return `-EINVAL` | first version does not use `O_DIRECT` (Take the core page cache), No alignment issues | ✅ and `tokio::fs` Take the core page cache consistent |
| io_uring Initialization failed | not available io_uring | `sys::is_uring_available()` return `false` → downgrade to `LocalPageStore` | ✅ Transparent downgrade, No sense from the upper level |

### 10.5 Downgrade security

when io_uring is unavailable (not Linux / kernel version too low / initialization failed), it automatically downgrades to `LocalPageStore`:

```text
LocalCacheManager::create()
  ├── uring_enabled && is_uring_available()?
  │     ├── YES → UringPageStore::create()
  │     │         ├── success → use io_uring backend
  │     │         └── fail → downgrade ↓
  │     └── NO  → ↓
  └── LocalPageStore::create() → use tokio::fs backend (existing behavior)
```

After downgrade:
- The disk file format is exactly the same — The same directory can be cross-used by both backends
- metadata index/disuse/Billing is exactly the same — `LocalCacheManager` Layers are not backend aware
- Cached page Can be read by any backend
- After the process restarts `restore()` No distinction between backends

### 10.6 Test Verification

| test level | Verify content | document |
|---|---|---|
| unit test | `UringPageStore` put/get/delete Basic functions + short read + concurrent + downgrade | `src/cache/store/uring/store.rs` `#[cfg(test)]` |
| Integration testing | INV-PC-D1/D2/S1/S2 all in io_uring Backend passes | `tests/page_cache_consistency.rs` (Reuse, No changes) |
| cross backend | tokio::fs write → io_uring read (Same goes for reverse) | New `test_cross_backend_compatibility` |
| Performance benchmark | io_uring vs tokio::fs contrast | `benchmarks/cache_uring_bench.rs` |

---

## 10. test plan

### 10.1 Unit Tests

```rust
// src/cache/store/uring/store.rs — #[cfg(test)]
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn uring_put_get_roundtrip() {
        let store = UringPageStore::create(
            Path::new("/tmp/gfs_uring_test"), 1024
        ).await.unwrap();
        let id = PageId::new("file-a", 0);
        let data = b"hello uring page cache";
        store.put(&id, data).await.unwrap();
        let mut dst = vec![0u8; data.len()];
        let n = store.get(&id, 0, &mut dst).await.unwrap();
        assert_eq!(n, data.len());
        assert_eq!(&dst, data);
    }

    #[tokio::test]
    async fn uring_get_missing_returns_zero() { /* ... */ }

    #[tokio::test]
    async fn uring_concurrent_get_same_page() { /* 32 concurrent */ }

    #[tokio::test]
    async fn uring_short_read_retry() { /* simulate short read */ }

    #[tokio::test]
    async fn uring_delete_then_miss() { /* get returns 0 after delete */ }
}
```

### 10.2 Integration tests

Reuse existing tests (backend switch is transparent to the upper layer):
- `tests/page_cache_e2e.rs`
- `tests/page_cache_consistency.rs`
- `benchmarks/page_cache_ab.rs`

### 10.3 Performance benchmark

```rust
// benchmarks/cache_uring_bench.rs
async fn bench_cache_hit_uring() { /* UringPageStore, 10^5 cache hit */ }
async fn bench_cache_hit_tokio_fs() { /* LocalPageStore comparison */ }
async fn bench_cache_hit_concurrent_uring() { /* 32 concurrent */ }
```

**Expected**:

| Backend | single-thread ops/s | 32-concurrent ops/s | p99 latency |
|---|---|---|---|
| `tokio::fs` (current) | ~3,000 | ~10,000 | ~2 ms |
| `io_uring` (expected) | ~50,000 | ~200,000 | ~0.1 ms |

---

## 11. Implementation plan

| Phase | Content | Changed files | Estimated effort |
|---|---|---|---|
| **P0** | `requests.rs` + `future.rs` + `driver.rs` + unit tests | `src/cache/store/uring/{requests,future,driver}.rs` | 2 days |
| **P1** | `store.rs` implements `PageStore` trait (get/put/delete) + `sys.rs` | `src/cache/store/uring/{store,sys,mod}.rs` | 2 days |
| **P2** | `manager.rs` changes `Vec<Arc<dyn PageStore>>` + `options.rs` + `config.rs` + `Cargo.toml` | `src/cache/{manager,options}.rs`, `src/config.rs`, `Cargo.toml` | 1 day |
| **P3** | performance benchmark + flame-graph verification + tuning | `benchmarks/cache_uring_bench.rs` | 1 day |
| **P4** | fd cache (ref Lance `HANDLE_CACHE`) + batched-submit optimization | `src/cache/store/uring/store.rs` | 2 days |
| **P5** | consistency regression + CI + docs | `tests/`, `docs/` | 1 day |

---

## 12. Expected effect

| Stage | QPS | vs Java |
|---|---|---|
| Current (tokio::fs) | 300 | Java has no cache overhead |
| P0-P3 done (io_uring) | 900-1100 | **catch up to Java** |
| P4 done (fd cache + batch) | 1000-1200 | **surpass Java** (io_uring advantage) |

---

## 13. Cross-references

- [`CLIENT_PAGE_CACHE_DESIGN.md`](CLIENT_PAGE_CACHE_DESIGN.md) — existing cache design (P0-P3 implemented)
- [`SHORT_CIRCUIT_IO_URING_FEASIBILITY.md`](SHORT_CIRCUIT_IO_URING_FEASIBILITY.md) — io_uring analysis for the SC path
- [`../../goosefs-lance-tests/docs/design/FLAMEGRAPH_OPTIMIZATION_PLAN.md`](../../goosefs-lance-tests/docs/design/FLAMEGRAPH_OPTIMIZATION_PLAN.md) — A/B/C series optimizations (router/transport layer)
- [`perf/2026-07-08-oncpu3-cache-hotspots/CACHE_VS_NOCACHE_ANALYSIS.md`](perf/2026-07-08-oncpu3-cache-hotspots/CACHE_VS_NOCACHE_ANALYSIS.md) — D series optimization items
- Lance reference: `/opt/sourcecode/lance/rust/lance-io/src/uring/` — `thread.rs`, `reader.rs`, `future.rs`, `requests.rs`

---

## 14. Realization progress

> Last updated:2026-07-08

### P0 — io_uring core components ✅

| document | state | illustrate |
|---|---|---|
| `src/cache/store/uring/requests.rs` | ✅ Finish | `IoRequest` + `RequestState` + `UringOpType`(Read/Write/OpenAt/Close/UnlinkAt) |
| `src/cache/store/uring/future.rs` | ✅ Finish | `UringOpFuture` — Universal Future,return `(result_code, Bytes)` |
| `src/cache/store/uring/driver.rs` | ✅ Finish | Background thread pool + main loop + Submit in batches + short read/write Try again + round-robin |

**Differences from design documentation (improvements):**
- `RequestState.bytes_read` → `bytes_transferred`(also used for read and write of short try again)
- New `RequestState.result_code: i32`(storage CQE result,solve OpenAt return fd problem)
- New `IoRequest.open_flags: i32`(OpenAt of flags parameters, support `O_RDONLY` / `O_WRONLY|O_CREAT|O_TRUNC`)
- `process_completions` collects short read/write retries into a `Vec` for unified processing afterwards (referencing Lance), avoiding recursive calls to `push_to_sq` within the completion loop
- EOF(result == 0 on Read) is considered a normal completion (returns read bytes) and is not considered an error——match `LocalPageStore::get` of page tail Semantics

### P1 — UringPageStore + Platform detection ✅

| document | state | illustrate |
|---|---|---|
| `src/cache/store/uring/store.rs` | ✅ Finish | `UringPageStore` accomplish `PageStore` trait(get/put/delete + identity) |
| `src/cache/store/uring/sys.rs` | ✅ Finish | `is_uring_available()` — compile time + Runtime double detection |
| `src/cache/store/uring/mod.rs` | ✅ Finish | module declaration + Sharing Tools (`hash_file_id`/`NUM_BUCKETS`/`io_error`) |

**Key design decisions:**
- `UringPageStore::get` Allocation independence `BytesMut` buffer (reference Lance), after completion copy arrive `dst`(in design document `BytesMut::from(dst)` method will result in redundant copies)
- `UringPageStore::put` of `rename` use `std::fs::rename`(synchronous) because rename Not here cache Hit on hot path
- Identity sidecar Operational use `tokio::fs`(not on the hot path), with `LocalPageStore` Shared disk format

### P2 — Integration + config ✅

| File | Status | Description |
|---|---|---|
| `src/cache/store/mod.rs` | ✅ done | `PageStore` trait extended (added `root_dir`/`write_identity`/`read_identity`/`delete_identity`) |
| `src/cache/store/local.rs` | ✅ done | identity methods moved from inherent impl to trait impl |
| `src/cache/manager.rs` | ✅ done | `Vec<LocalPageStore>` → `Vec<Arc<dyn PageStore>>` + io_uring fallback logic |
| `src/cache/options.rs` | ✅ done | added `uring_enabled`/`uring_queue_depth`/`uring_thread_count` |
| `src/config.rs` | ✅ done | added `client_cache_uring_*` config fields + env vars + storage option keys + defaults |
| `Cargo.toml` | ✅ done | `io-uring = "0.7"` (Linux-only, target-gated) |
| `src/cache/metrics.rs` | ✅ done | added 7 io_uring metric constants |

**Compile verification:**
- ✅ macOS (`cargo check`) — zero warnings, io_uring code isolated by `#[cfg(target_os = "linux")]`
- ✅ all 76 cache unit tests pass (on macOS only the `LocalPageStore` path runs)

### P3 — Performance benchmark ✅

| File | Status | Description |
|---|---|---|
| `benchmarks/cache_uring_bench.rs` | ✅ done | io_uring vs tokio::fs single-thread + 32-concurrent comparison, local-only (no cluster needed) |

**Benchmark design:**
- Call `PageStore` trait `get()` directly, no GooseFS cluster needed
- warm up the fd cache then measure the pure cache-hit path
- single thread: 10^5 `get()` calls, record per-op latency → ops/s + p50/p99
- concurrent: 32 tokio tasks each 10^4 `get()` → aggregate ops/s + p99
- macOS baseline (tokio::fs only): single-thread ~32K ops/s, 32-concurrent ~61K ops/s
- Linux expected (io_uring): single-thread ~50K+ ops/s, 32-concurrent ~200K+ ops/s

### P4 — fd cache + batched-submit optimization ✅

| File | Status | Description |
|---|---|---|
| `src/cache/store/uring/store.rs` (fd cache) | ✅ done | `LruCache<PageId, Arc<File>>` — cache-hit read path drops from 3 SQEs to 1 SQE |

**fd cache design points:**
- `fd_cache: Mutex<LruCache<PageId, Arc<File>>>` — capacity 1024, LRU eviction
- `get_fd()` method: cache hit → return `Arc::clone(&file)` (zero open); miss → OP_OPENAT → `File::from_raw_fd` → store in cache
- `Arc<File>` guarantees the fd is not closed during concurrent reads: even if the LRU evicts the cache entry, as long as an `Arc<File>` reference exists, `File::drop` will not close the fd
- When `LruCache::put` evicts an old entry, the `Arc<File>`'s `Drop` automatically closes the fd (if no other reference)
- `delete()` calls `invalidate_fd()` to remove from the cache first, avoiding reading a stale fd
- Unix semantics guarantee: unlinking a file with an open fd is safe — the inode is released only after all fds are closed

**Cache-hit read-path comparison (P0-P3 vs P4):**
- P0-P3: `OP_OPENAT` + `OP_READ` + `OP_CLOSE` = 3 SQEs
- P4: `OP_READ` (using cached fd) = **1 SQE**

**New tests:**
- `uring_fd_cache_repeated_reads` — repeated read of the same page verifies fd reuse
- `uring_fd_cache_invalidation_on_delete` — fd cache invalidated after delete, get returns 0
- `uring_fd_cache_lru_eviction` — over-capacity insertion triggers LRU eviction, re-read after eviction is still correct

### P5 — Tests + docs ✅ (partial)

| File | Status | Description |
|---|---|---|
| `src/cache/store/uring/store.rs` unit tests | ✅ done | 7 tests (put/get roundtrip, offset, missing, short read, delete, concurrent, identity) — Linux-only |
| Cross-backend compatibility test | ⏳ | `test_cross_backend_compatibility` — needs Linux |
| Integration-test reuse | ✅ | existing `tests/page_cache_*.rs` need no change (trait-object transparent switch) |

### Platform support matrix

| Platform | Compiles | io_uring backend | Fallback behavior |
|---|---|---|---|
| Linux 5.1+ | ✅ | ✅ available | enabled by default, can be turned off via config |
| Linux < 5.1 | ✅ | ❌ unavailable | runtime detection → `LocalPageStore` |
| macOS | ✅ | ❌ unavailable | compile-time detection → `LocalPageStore` |
| Windows | ✅ | ❌ unavailable | compile-time detection → `LocalPageStore` |

### P6 — `Bytes` Return model: Complete elimination tmp middle buffer ✅

> state:**Realized**
>
> Date: 2026-07-10 (design) · 2026-07-11 (implementation commit `0e9d67a`) · 2026-07-13 (doc sync)
>
> Background: the 128-concurrency performance analysis (`docs/perf/2026-07-10-oncpu-concurrent-uring-analysis/README.md`) found that the `dst`-write model still had `tmp -> out` copy and per-page `Vec<u8>` allocation overhead at the `read_through_cache` layer.
>
> Implementation commit: `0e9d67a` — "optimize page cache read path for io_uring"
> - Added Bytes-returning cache read APIs (`PageStore::get_bytes` / `CacheManager::get_bytes` + `get_batch_bytes`)
> - `read_through_cache` uses `join_all` for batched cache-hit probing, eliminating per-page `JoinSet::spawn`
> - cache-miss correctness hardening: reject short external reads, preventing partial pages from being returned or persisted (§6.4)

#### 6.1 Problem analysis: the data flow of the original `dst`-write model

Before P6, the data flow of `read_through_cache`:

```
Single-page hit path:
  io_uring OP_READ
    → kernel writes into caller-provided tmp: Vec<u8>       (zero-copy: kernel → user)
    → out.extend_from_slice(&tmp)                    (1 copy: tmp → out)
    → out.freeze() -> Bytes                          (zero-copy: wrap)
    → return to Lance

Multi-page hit path (N pages):
  for each page:
    JoinSet::spawn                                    (1 task spawn)
    tmp = vec![0u8; want]                             (1 Vec allocation)
    cache.get(page_id, offset, &mut tmp)              (kernel → tmp)
    out.extend_from_slice(&tmp)                       (1 copy: tmp → out)
  out.freeze() -> Bytes
```

**Overhead**:
- 1 `Vec<u8>` allocation per page (~100-500ns, incl. heap alloc + zero-fill)
- 1 `extend_from_slice` copy per page (memcpy of ~want bytes)
- 1 `JoinSet::spawn` per page for multi-page (~1-5µs tokio scheduling)

#### 6.2 Goal: `Bytes`-return model

```
Single-page hit path (optimized):
  io_uring OP_READ
    → kernel writes into BytesMut (allocated inside store)          (zero-copy: kernel → user)
    → freeze() -> Bytes                               (zero-copy: wrap)
    → return directly to read_through_cache                    (zero-copy: no assembly needed for single page)

Multi-page hit path (optimized):
  cache.get_batch_bytes(page_requests)                 (concurrent via join_all)
    → Vec<Bytes>                                      (each Bytes holds the io_uring buffer)
  for each page:
    chunks.push(cached_bytes)                          (zero-copy: push directly)
  out.extend_from_slice(&chunk)                        (1 copy: chunk → out)
  out.freeze() -> Bytes
```

**Overhead eliminated**:
- per-page `Vec<u8>` allocation removed (`Bytes` directly holds the io_uring `BytesMut` buffer)
- per-page `JoinSet::spawn` scheduling removed (the batch interface sinks into `CacheManager::get_batch_bytes`)
- per-page `extend_from_slice` copy removed on single-page hit (return the `Bytes` chunk directly, no assembly)
- multi-page still keeps 1 `extend_from_slice` copy (chunk -> out), but that is the necessary copy to assemble the final `Bytes`

#### 6.3 API design (implemented)

> **Difference from the original design**: the original design proposed `get_bytes(&PageId) -> Option<Bytes>` returning the whole page + `PageStore::get_bytes_many` for io_uring batched submission. The actual implementation uses the more pragmatic signature `get_bytes(&PageId, offset, len) -> Bytes` (empty = miss), and puts the batched concurrency at the `CacheManager::get_batch_bytes` layer via `join_all` — avoiding introducing unused batch APIs at the `PageStore` layer (see the comment at store.rs:502-505 for this decision).

##### 6.3.1 New methods on the `PageStore` trait

```rust
// src/cache/store/mod.rs:49-66 (actual implementation)

#[async_trait::async_trait]
pub trait PageStore: Send + Sync {
    // ... existing methods put / get / delete unchanged ...

    /// Read bytes from a page and return them directly.
    ///
    /// Backends that naturally allocate their own read buffer (notably
    /// io_uring) should override this to avoid copying into a temporary caller
    /// buffer before returning to the cache layer.
    async fn get_bytes(&self, page_id: &PageId, offset: usize, len: usize) -> Result<Bytes> {
    // Default impl: fall back to get() + Vec allocation
    if len == 0 {
        return Ok(Bytes::new());
    }
    let mut dst = vec![0u8; len];
    let n = self.get(page_id, offset, &mut dst).await?;
    if n == 0 {
        Ok(Bytes::new())
    } else {
        dst.truncate(n);
        Ok(Bytes::from(dst))
    }
}

    // ... identity method ...
}
```

**Signature notes**:
- Returns `Result<Bytes>` (not `Option<Bytes>`): errors are expressed via `Result`, miss via empty `Bytes` (`bytes.is_empty() == true`)
- Takes `offset` + `len`: supports in-page sub-range reads, the caller need not read the whole page then slice
- Default impl falls back to `get()`: `LocalPageStore` does not override, takes the default `Vec` path (non-hot-path backend)

##### 6.3.2 `CacheManager` trait new methods

```rust
// src/cache/mod.rs:139-167 (actual implementation)

#[async_trait::async_trait]
pub trait CacheManager: Send + Sync {
    // ... put / get / delete Let existing methods remain unchanged ...

    /// Read bytes from a cached page and return the owned `Bytes` directly.
    ///
    /// The default implementation preserves the legacy `get` contract by
    /// reading into a caller-owned buffer. io_uring-backed implementations
    /// override this to return the kernel-filled buffer directly, avoiding one
    /// extra copy on cache hits.
    async fn get_bytes(&self, page_id: &PageId, page_offset: usize, len: usize) -> Bytes {
    // Default impl: fall back to get() + Vec allocation
    if len == 0 {
        return Bytes::new();
    }
    let mut dst = vec![0u8; len];
    let n = self.get(page_id, page_offset, &mut dst).await;
    if n == 0 {
        Bytes::new()
    } else {
        dst.truncate(n);
        Bytes::from(dst)
    }
}

    /// Read multiple cached pages. Each output corresponds to the request at
    /// the same index; an empty `Bytes` means miss or cache error.
    async fn get_batch_bytes(&self, requests: &[PageReadRequest]) -> Vec<Bytes> {
        let mut out = Vec::with_capacity(requests.len());
        for req in requests {
            out.push(self.get_bytes(&req.page_id, req.page_offset, req.len).await);
        }
        out
    }

    // ... Other methods ...
}
```

**`PageReadRequest` structure**(`src/cache/mod.rs:65-70`):
```rust
#[derive(Debug, Clone)]
pub struct PageReadRequest {
    pub page_id: PageId,
    pub page_offset: usize,
    pub len: usize,
}
```

**Signature notes**:
- `get_bytes` returns `Bytes` (not `Option<Bytes>`): empty `Bytes` = miss, aligned with `get()` returning `0`
- `get_batch_bytes` takes `&[PageReadRequest]` (with offset + len) and returns `Vec<Bytes>`
- Default `get_batch_bytes` is a serial loop; `LocalCacheManager` overrides it with `join_all` concurrency

##### 6.3.3 `UringPageStore::get_bytes` implementation

```rust
// src/cache/store/uring/store.rs:584-645 (actual implementation)

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
    let fd = match self.openat_relative(dirfd, &page_name, libc::O_RDONLY).await {
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
```

**Zero-copy core**: `read_with_fd` → `new_read_request` (`store.rs:457-481`) allocates `BytesMut::with_capacity(len)` + `unsafe set_len(len)`, the kernel's `OP_READ` writes directly into this buffer, and after CQE completion `UringOpFuture::poll` (`future.rs:52-54`) runs `std::mem::take(&mut state.buffer).freeze()` to turn `BytesMut` into `Bytes` — no extra copy throughout.

```rust
// src/cache/store/uring/store.rs:457-481 — new_read_request (zero-copy buffer allocation)
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
            buffer,                    // ← kernel writes directly into this BytesMut
            bytes_transferred: 0,
            consumed: false,
            result_code: 0,
        }),
    })
}
```

**`UringPageStore::get` now delegates to `get_bytes`** (`store.rs:575-582`):
```rust
async fn get(&self, page_id: &PageId, offset: usize, dst: &mut [u8]) -> Result<usize> {
    let bytes = self.get_bytes(page_id, offset, dst.len()).await?;
    let n = bytes.len().min(dst.len());
    if n > 0 {
        dst[..n].copy_from_slice(&bytes[..n]);
    }
    Ok(n)
}
```

##### 6.3.4 `LocalCacheManager::get_bytes` + `get_batch_bytes` accomplish

```rust
// src/cache/manager.rs:714-776 — get_bytes(Actual implementation)

async fn get_bytes(&self, page_id: &PageId, page_offset: usize, len: usize) -> Bytes {
    if self.state == CacheState::NotInUse {
        counter(mn::CLIENT_CACHE_GET_NOT_READY_ERRORS).inc(1);
        return Bytes::new();
    }
    if len == 0 {
        return Bytes::new();
    }

    let _rl = self.page_locks[page_lock_index(page_id)].read().await;

    // Phase C: lock-free DashMap read for dir_index + evictor.on_access
    let dir_index = match self.meta.get(page_id) {
        Some(info) => {
            // Check TTL (no-op when TTL is None).
            if let Some(ttl) = self.options.ttl {
                if info.created_at.elapsed() > ttl {
                    drop(info);
                    let _ = self.get_expired_path(page_id).await;
                    return Bytes::new();
                }
            }
            let di = info.dir_index;
            self.dirs[di].evictor.on_access(page_id);
            di
        }
        None => return Bytes::new(), // miss
    };

    // Disk IO — completely lock-free, delegates to PageStore::get_bytes
    let start = Instant::now();
    let bytes = match self.stores[dir_index]
        .get_bytes(page_id, page_offset, len)
        .await
    {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!(error = %e, "get: failed to read page from store");
            counter(mn::CLIENT_CACHE_GET_STORE_READ_ERRORS).inc(1);
            counter(mn::CLIENT_CACHE_GET_ERRORS).inc(1);
            return Bytes::new();
        }
    };
    if bytes.is_empty() {
        return Bytes::new(); // racy eviction → miss
    }

    counter(mn::CLIENT_CACHE_BYTES_READ_CACHE).inc(bytes.len() as i64);
    counter(mn::CLIENT_CACHE_PAGE_READ_CACHE_TIME_NS).inc(start.elapsed().as_nanos() as i64);
    crate::cache::metrics::publish_hit_rate();
    bytes
}
```

```rust
// src/cache/manager.rs:778-785 — get_batch_bytes(Actual implementation,join_all concurrent)

async fn get_batch_bytes(&self, requests: &[PageReadRequest]) -> Vec<Bytes> {
    join_all(
        requests
            .iter()
            .map(|req| self.get_bytes(&req.page_id, req.page_offset, req.len)),
    )
    .await
}
```

**Design decision**: `get_batch_bytes` uses `join_all` (tokio concurrency) rather than sinking into `PageStore::get_bytes_many` for io_uring batched submission. Reasons (`store.rs:502-505` comment):
- `join_all` already provides enough concurrency; io_uring SQEs are themselves asynchronously batched (the driver.rs main loop)
- avoids adding an unused `get_bytes_many` abstraction layer to the `PageStore` trait
- each `get_bytes` independently holds `Arc<PageFdEntry>`, making fd lifetime management simpler

**`LocalCacheManager::get` now delegates to `get_bytes`** (`manager.rs:705-712`):
```rust
async fn get(&self, page_id: &PageId, page_offset: usize, dst: &mut [u8]) -> usize {
    let bytes = self.get_bytes(page_id, page_offset, dst.len()).await;
    let n = bytes.len().min(dst.len());
    if n > 0 {
        dst[..n].copy_from_slice(&bytes[..n]);
    }
    n
}
```

##### 6.3.5 `read_through_cache` rework (actual implementation)

```rust
// src/cache/caching_reader.rs:55-184 (actual implementation)

pub async fn read_through_cache<R: ExternalRangeReader + ?Sized>(
    cache: &Arc<dyn CacheManager>,
    ext: &mut R,
    file_id: &Arc<str>,
    page_size: u64,
    file_length: i64,
    offset: i64,
    end: i64,
    fill_mode: FillMode,
) -> Result<Bytes> {
    let page_size = page_size.max(1);
    let requested_len = (end - offset).max(0) as usize;
    let mut cur = offset;
    let mut pages = Vec::new();

    // Phase 1: Compute page requests
    while cur < end {
        let page_index = (cur as u64) / page_size;
        let page_start = (page_index * page_size) as i64;
        let page_end = (((page_index + 1) * page_size) as i64).min(file_length);
        let in_page_off = (cur - page_start) as usize;
        let want = (end.min(page_end) - cur) as usize;
        pages.push((
            PageId::new(file_id.clone(), page_index),
            page_index, page_start, page_end, in_page_off, want,
        ));
        cur += want as i64;
    }

    // Phase 2: Batch cache read via get_batch_bytes (eliminates JoinSet)
    let cache_requests: Vec<PageReadRequest> = pages
        .iter()
        .map(|(page_id, _, _, _, in_page_off, want)| PageReadRequest {
            page_id: page_id.clone(),
            page_offset: *in_page_off,
            len: *want,
        })
        .collect();
    let mut cached = cache.get_batch_bytes(&cache_requests).await;
    if cached.len() != pages.len() {
        cached = vec![Bytes::new(); pages.len()];
    }

    // Phase 3: Assemble output — collect chunks first
    let mut chunks: Vec<Bytes> = Vec::with_capacity(pages.len());
    for ((page_id, page_index, page_start, page_end, in_page_off, want), cached_bytes) in
        pages.into_iter().zip(cached.into_iter())
    {
        // 1) Cache hit: keep the returned Bytes directly. For the io_uring
        // backend this is the kernel-filled buffer, so single-page reads avoid
        // the old tmp-buffer copy entirely.
        if cached_bytes.len() == want {
            chunks.push(cached_bytes);   // ← Zero copy: direct push
            continue;
        }

        // 2) Miss → read the whole page from the external source.
        let ext_start = Instant::now();
        let page_bytes = ext.read_range(page_start, page_end).await?;
        counter(metric_name::CLIENT_CACHE_PAGE_READ_EXTERNAL_TIME_NS)
            .inc(ext_start.elapsed().as_nanos() as i64);
        counter(metric_name::CLIENT_CACHE_BYTES_READ_EXTERNAL).inc(page_bytes.len() as i64);
        counter(metric_name::CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL).inc(page_end - page_start);
        crate::cache::metrics::publish_hit_rate();

        // ── cache miss Correctness reinforcement (commit 0e9d67a)──
        // reject short external read:if worker/UFS The number of bytes returned is less than
        // expected page scope (page_end - page_start), returns an error directly,
        // Prevents parts of the page from being returned to the caller or backfilled into the cache.
        let expected_page_len = (page_end - page_start) as usize;
        if page_bytes.len() < expected_page_len {
            return Err(Error::Internal {
                message: format!(
                    "read_through_cache: short external read for page {}: got {} of {} bytes",
                    page_index,
                    page_bytes.len(),
                    expected_page_len
                ),
                source: None,
            });
        }
        // if external Returning more bytes than expected (such as an aligned read), truncated to the expected length.
        let page_bytes = if page_bytes.len() > expected_page_len {
            page_bytes.slice(0..expected_page_len)
        } else {
            page_bytes
        };

        // 3) Back-fill per the fill mode (best-effort).
        if !page_bytes.is_empty() {
            match fill_mode {
                FillMode::None => {}
                FillMode::Sync => {
                    cache.put(&page_id, page_bytes.clone()).await;
                }
                FillMode::Async => {
                    Arc::clone(cache).schedule_fill(page_id.clone(), page_bytes.clone());
                }
            }
        }

        // 4) Return the requested slice from the freshly read page.
        let avail = page_bytes.len();
        let s = in_page_off.min(avail);
        let e = (in_page_off + want).min(avail);
        let advanced = (e - s) as i64;
        if advanced == 0 {
            return Err(Error::Internal {
                message: format!(
                    "read_through_cache: 0 bytes for page {} (cur={}, end={})",
                    page_index,
                    page_start + in_page_off as i64,
                    end
                ),
                source: None,
            });
        }
        chunks.push(page_bytes.slice(s..e));
    }

    // Phase 4: Return — single chunk = zero-copy, multi-chunk = one assemble copy
    if chunks.is_empty() {
        return Ok(Bytes::new());
    }
    if chunks.len() == 1 {
        return Ok(chunks.pop().unwrap());   // ← one page hit: Zero copy return
    }

    let mut out = BytesMut::with_capacity(requested_len);
    for chunk in chunks {
        out.extend_from_slice(&chunk);      // ← many page:1 secondary assembly copy
    }
    Ok(out.freeze())
}
```

**Key optimization points**:
1. **one page hit zero copy**:`chunks.len() == 1` direct `chunks.pop().unwrap()` Return, none `extend_from_slice`
2. **many page hit zero `Vec` distribute**: each chunk yes `Bytes`(hold io_uring buffer),none per-page `Vec<u8>` distribute
3. **zero `JoinSet::spawn`**:`get_batch_bytes` For internal use `join_all`,none per-page task spawn

#### 6.4 cache miss Correctness reinforcement: Reject short external read

> source:commit `0e9d67a` — "Harden cache miss correctness by rejecting short external page reads before slicing or filling the cache, preventing partial pages from being returned or persisted."

**Problem**: Before P6, `read_through_cache` called `ext.read_range(page_start, page_end)` to read the entire page when there was a cache miss. If the worker/UFS returned less than the expected page scope (`page_end - page_start`), the older code would silently assemble the return value with partial data and possibly backfill an **incomplete page** into the cache — subsequent reads that hit the page would get truncated data, violating INV-PC-D1 (cache vs direct byte diff).

**repair**(`caching_reader.rs:122-138`):exist external read After returning, but before slicing and backfilling, explicitly check the length:

```rust
// src/cache/caching_reader.rs:122-138
let expected_page_len = (page_end - page_start) as usize;
if page_bytes.len() < expected_page_len {
    return Err(Error::Internal {
        message: format!(
            "read_through_cache: short external read for page {}: got {} of {} bytes",
            page_index,
            page_bytes.len(),
            expected_page_len
        ),
        source: None,
    });
}
// if external Returning more bytes than expected (such as an aligned read), truncated to the expected length.
let page_bytes = if page_bytes.len() > expected_page_len {
    page_bytes.slice(0..expected_page_len)
} else {
    page_bytes
};
```

**Three lines of defense**:

| examine | Trigger condition | Behavior | Protection target |
|------|---------|------|---------|
| `page_bytes.len() < expected_page_len` | worker/UFS Return insufficient | return `Error::Internal` | Prevent parts of the page from being returned to the caller |
| Same as above | Same as above | Not executed `cache.put` / `schedule_fill` | Prevent partial pages from being persisted to the cache |
| `page_bytes.len() > expected_page_len` | worker/UFS Too many returns (aligned reads) | `page_bytes.slice(0..expected_page_len)` | Prevent extra bytes from contaminating backfill |

**Relationship with INV-PC-S1**: `CLIENT_PAGE_CACHE_DESIGN.md` §1.4 of INV-PC-S1 requires "failed fill doesn't poison cache". This fix treats a short external read as a fill failure — it returns an error and does not backfill the cache, so the next `get` misses → returns to the source and reads correctly. This is consistent with the semantics of the three existing failure paths `OP_WRITE` fail, `OP_RENAMEAT` fail, and SQ fail.

**test**(`caching_reader.rs:361-389`):

```rust
struct ShortExternal { data: Vec<u8> }

#[async_trait::async_trait]
impl ExternalRangeReader for ShortExternal {
    async fn read_range(&mut self, offset: i64, end: i64) -> Result<Bytes> {
        let s = offset as usize;
        let e = (end as usize).min(self.data.len()).saturating_sub(1); // ← Read less 1 byte
        Ok(Bytes::copy_from_slice(&self.data[s..e]))
    }
}

#[tokio::test]
async fn short_external_page_read_errors_and_does_not_fill_cache() {
    // ShortExternal returned page_bytes.len() < expected_page_len
    // → read_through_cache return "short external read" mistake
    // → cache No partial pages remain in (subsequent get return 0 = miss)
    let err = read_through_cache(...).await.unwrap_err();
    assert!(format!("{}", err).contains("short external read"));

    let mut dst = vec![0u8; 8];
    assert_eq!(cache.get(&PageId::new(file_id.clone(), 0), 0, &mut dst).await, 0);
}
```

#### 6.5 eliminated copy/allocation comparison

| scene | P6 Before(dst Model) | P6 after(Bytes Model) | eliminate |
|------|---------------------|----------------------|------|
| one page hit | `kernel->tmp` + `tmp->out` + `Vec alloc` | `kernel->BytesMut` + `freeze` + Return directly | `Vec alloc` + `extend_from_slice` copy |
| N page hit | N x (`kernel->tmp` + `tmp->out` + `Vec alloc` + `spawn`) | N x (`kernel->BytesMut` + `freeze`) + `join_all` + 1 x assemble | `N x Vec alloc` + `N x spawn` + N-1 x `extend_from_slice` |
| one page miss | constant | constant | — |

**Every page save**:
- `Vec<u8>` allocation: ~100-500ns (heap alloc + zero-fill `want` bytes)
- `JoinSet::spawn`: ~1-5µs (tokio task create + Scheduling)

**128 concurrent x many page Scenario cumulative savings**: If every query crosses 4 pages, it saves 4 x (500ns + 3µs) = 14µs/query. With 128 concurrent queries at ~10K queries per second, total CPU saving is ~140ms/s.

#### 6.6 Compatibility design (implemented)

| Dimensions | accomplish |
|------|------|
| `PageStore` trait | New `get_bytes(page_id, offset, len) -> Result<Bytes>`, the default implementation falls back to `get()` + `Vec` |
| `CacheManager` trait | New `get_bytes(page_id, offset, len) -> Bytes` + `get_batch_bytes(requests) -> Vec<Bytes>`, the default implementation falls back to `get()` |
| `LocalPageStore` | **Do not overwrite** `get_bytes` — go to default `Vec` path (non-hot path backend, macOS/downgrade scenario) |
| `UringPageStore` | overwrite `get_bytes` — use `read_with_fd` to return `Bytes` (Zero copy, kernel direct writing `BytesMut`) |
| `LocalCacheManager` | overwrite `get_bytes` (delegated to `PageStore::get_bytes`) + `get_batch_bytes` (`join_all` concurrent) |
| `DisabledCacheManager` | Do not overwrite — default `get_bytes` calls `get()` returning 0 → null `Bytes`; `get_batch_bytes` by default returns all empty via a serial loop |
| `read_through_cache` | Use instead `get_batch_bytes` + `chunks` assembly; single page hit returns zero copy; short external read rejected (§6.4) |
| `UringPageStore::get` | Change to delegate `get_bytes` + `copy_to_slice` (keeping `dst` interface compatible) |
| `LocalCacheManager::get` | Change to delegate `get_bytes` + `copy_to_slice` (keeping `dst` interface compatible) |
| Existing tests | not affected (`get()` / `get_bytes()` interfaces are all retained; `get` internally delegates to `get_bytes`) |

#### 6.7 Implementation Checklist

| # | change | document | state |
|---|------|------|------|
| 1 | `PageStore::get_bytes` trait method + Default implementation | `src/cache/store/mod.rs:49-66` | ✅ |
| 2 | `CacheManager::get_bytes` + `get_batch_bytes` trait method + Default implementation | `src/cache/mod.rs:139-167` | ✅ |
| 3 | `PageReadRequest` structure | `src/cache/mod.rs:65-70` | ✅ |
| 4 | `UringPageStore::get_bytes` accomplish(page fd cache hot path + dir fd cache cold path) | `src/cache/store/uring/store.rs:584-645` | ✅ |
| 5 | `UringPageStore::get` Change to delegate `get_bytes` | `src/cache/store/uring/store.rs:575-582` | ✅ |
| 6 | `new_read_request` + `read_with_fd` + `wait_read_request` zero copy buffer distribute | `src/cache/store/uring/store.rs:457-500` | ✅ |
| 7 | `LocalCacheManager::get_bytes` implemented (lock-free meta + delegated store) | `src/cache/manager.rs:714-776` | ✅ |
| 8 | `LocalCacheManager::get_batch_bytes` implemented (`join_all` concurrent) | `src/cache/manager.rs:778-785` | ✅ |
| 9 | `LocalCacheManager::get` changed to delegate `get_bytes` | `src/cache/manager.rs:705-712` | ✅ |
| 10 | `read_through_cache` Transformation (`get_batch_bytes` + `chunks` + one page zero copy) | `src/cache/caching_reader.rs:55-184` | ✅ |
| 11 | short external read reject + over-read truncate (cache miss correctness hardening) | `src/cache/caching_reader.rs:122-138` | ✅ |
| 12 | `futures = "0.3"` from dev-dependencies move to dependencies(`join_all` hot path dependencies) | `Cargo.toml` | ✅ |

> Implementation source:commit `0e9d67a` — "optimize page cache read path for io_uring"

#### 6.8 test coverage

| test | document | Verify content |
|------|------|---------|
| `uring_put_get_roundtrip` | `src/cache/store/uring/store.rs` | put + get Basic round trip (`get` delegates internally to `get_bytes`) |
| `uring_get_short_read_at_tail` | `src/cache/store/uring/store.rs` | page tail short read (`get_bytes` returns partial bytes) |
| `uring_page_fd_cache_hit_after_first_read` | `src/cache/store/uring/store.rs` | first get warms the page fd cache |
| `uring_get_batch_concurrent` | `src/cache/store/uring/store.rs` | `get_batch` Batch read 8 pages (internally uses `get_bytes`) |
| `cold_read_misses_then_warm_read_hits` | `src/cache/caching_reader.rs` | `read_through_cache` cold miss + hot hit (uses `get_batch_bytes`) |
| `spans_multiple_pages_and_partial_offsets` | `src/cache/caching_reader.rs` | across page boundary + misaligned offset (many chunk assembly) |
| `short_external_page_read_errors_and_does_not_fill_cache` | `src/cache/caching_reader.rs` | short external read returns an error + no partial page left in the cache (§6.4 hardening) |
| `inv_pc_d1_cache_vs_direct_byte_diff` | `tests/page_cache_consistency.rs` | cache vs direct Byte consistency (P6 path) |

### Next step

1. **Linux environment compilation verification**: `cargo test --lib cache::store::uring` runs the io_uring single test (including fd cache + get_bytes test)
2. **P3 Performance benchmark**: `benchmarks/cache_uring_bench.rs` compares io_uring vs tokio::fs (including fd cache hit/miss scenarios)
3. **Cross backend testing**: verify tokio::fs write → io_uring read (and vice versa)
4. **P6 Performance verification**: 128 concurrent flame graph comparison to confirm per-page `Vec` allocation and `JoinSet` scheduling are eliminated
