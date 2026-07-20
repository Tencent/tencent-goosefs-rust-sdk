//! Local filesystem [`PageStore`] backend.
//!
//! Mirrors Java `LocalPageStore`. Pages are stored as individual files under:
//!
//! ```text
//! <dir>/<page_size>/<bucket>/<file_id>/<page_index>
//! ```
//!
//! where `bucket = hash(file_id) % NUM_BUCKETS` keeps any single directory
//! from accumulating an unbounded number of per-file subdirectories.
//!
//! Writes are atomic: bytes are written to a unique `*.tmp-<uuid>` sibling and
//! then `rename`d into place, so a reader never observes a half-written page.

use std::path::{Path, PathBuf};

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::cache::page_id::PageId;
use crate::cache::store::PageStore;
use crate::error::{Error, Result};

/// Number of hash buckets used to spread per-file directories.
const NUM_BUCKETS: u64 = 1000;

/// Sidecar filename holding a file's `(length, mtime)` identity, persisted so
/// overwrite detection survives a process restart (see `LocalCacheManager`).
const IDENTITY_FILE: &str = ".identity";

/// A page store backed by the local filesystem.
#[derive(Debug, Clone)]
pub struct LocalPageStore {
    /// Root directory for this store: `<dir>/<page_size>`.
    root: PathBuf,
}

fn io_error(message: impl Into<String>, e: std::io::Error) -> Error {
    Error::Internal {
        message: message.into(),
        source: Some(Box::new(e)),
    }
}

/// Stable, platform- and Rust-version-independent hash of `file_id`.
///
/// **This must stay byte-for-byte stable forever.** The result picks the
/// on-disk bucket directory (`<root>/<bucket>/<file_id>/<page_index>`), so a
/// cache populated by one build has to remain locatable by any other build
/// after a process restart (the cache is designed to survive restarts; see the
/// `.identity` sidecar / restore logic in `LocalCacheManager`).
///
/// `std`'s `DefaultHasher` (SipHash) is explicitly **not** guaranteed stable
/// across Rust versions or platforms, so using it here would orphan the entire
/// on-disk cache after a toolchain upgrade (every `get` would recompute a
/// different bucket → `NotFound` → permanent miss + leaked disk space). We
/// therefore use xxHash3 — a spec-defined, fixed-constant hash whose output is
/// identical across versions, platforms and languages — the same hash Lance
/// uses (`xxhash_rust::xxh3`) and the single hash this project is standardised
/// on. It is faster and better-distributed than the previous hand-rolled
/// FNV-1a.
fn hash_file_id(file_id: &str) -> u64 {
    xxhash_rust::xxh3::xxh3_64(file_id.as_bytes())
}

impl LocalPageStore {
    /// Create (and `mkdir -p`) a local page store rooted under
    /// `<dir>/<page_size>`.
    pub async fn create(dir: &Path, page_size: u64) -> Result<Self> {
        let root = dir.join(page_size.to_string());
        tokio::fs::create_dir_all(&root)
            .await
            .map_err(|e| io_error(format!("create cache dir {}", root.display()), e))?;
        Ok(Self { root })
    }

    /// Absolute path of the file holding `page_id`.
    fn page_path(&self, page_id: &PageId) -> PathBuf {
        let bucket = hash_file_id(&page_id.file_id) % NUM_BUCKETS;
        self.root
            .join(bucket.to_string())
            .join(page_id.file_id.as_ref())
            .join(page_id.page_index.to_string())
    }

    /// Absolute path of `file_id`'s identity sidecar.
    fn identity_path(&self, file_id: &str) -> PathBuf {
        let bucket = hash_file_id(file_id) % NUM_BUCKETS;
        self.root
            .join(bucket.to_string())
            .join(file_id)
            .join(IDENTITY_FILE)
    }

    /// Whether `name` is the identity sidecar (so restore can skip it as a page).
    pub fn is_identity_file(name: &str) -> bool {
        name == IDENTITY_FILE
    }

    /// Parse a `(length, mtime)` identity from the sidecar contents.
    ///
    /// Returns `None` if the file is missing or malformed (restore then treats
    /// the identity as unknown for that file).
    pub fn parse_identity(contents: &str) -> Option<(i64, i64)> {
        let (l, m) = contents.trim().split_once(',')?;
        Some((l.trim().parse().ok()?, m.trim().parse().ok()?))
    }
}

#[async_trait::async_trait]
impl PageStore for LocalPageStore {
    async fn put(&self, page_id: &PageId, page: &[u8]) -> Result<()> {
        let final_path = self.page_path(page_id);
        let parent = final_path
            .parent()
            .expect("page path always has a parent")
            .to_path_buf();
        tokio::fs::create_dir_all(&parent)
            .await
            .map_err(|e| io_error(format!("create page dir {}", parent.display()), e))?;

        // Unique temp sibling so concurrent writers to the same page never
        // clobber each other's in-flight bytes.
        let tmp_path = parent.join(format!(
            "{}.tmp-{}",
            page_id.page_index,
            uuid::Uuid::new_v4()
        ));

        let write_result = async {
            let mut f = tokio::fs::File::create(&tmp_path)
                .await
                .map_err(|e| io_error("create temp page file", e))?;
            f.write_all(page)
                .await
                .map_err(|e| io_error("write temp page file", e))?;
            f.flush()
                .await
                .map_err(|e| io_error("flush temp page file", e))?;
            tokio::fs::rename(&tmp_path, &final_path)
                .await
                .map_err(|e| io_error("rename temp page file", e))?;
            Ok::<(), Error>(())
        }
        .await;

        if write_result.is_err() {
            // Best-effort cleanup of the temp file on failure.
            let _ = tokio::fs::remove_file(&tmp_path).await;
        }
        write_result
    }

    async fn get(&self, page_id: &PageId, offset: usize, dst: &mut [u8]) -> Result<usize> {
        let path = self.page_path(page_id);
        let mut f = match tokio::fs::File::open(&path).await {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Missing file → caller treats a 0-length read as a miss.
                return Ok(0);
            }
            Err(e) => return Err(io_error("open page file", e)),
        };

        if offset > 0 {
            f.seek(std::io::SeekFrom::Start(offset as u64))
                .await
                .map_err(|e| io_error("seek page file", e))?;
        }

        // Fill `dst` as far as possible (a single `read` may short-read).
        let mut filled = 0usize;
        while filled < dst.len() {
            let n = f
                .read(&mut dst[filled..])
                .await
                .map_err(|e| io_error("read page file", e))?;
            if n == 0 {
                break; // EOF (page tail)
            }
            filled += n;
        }
        Ok(filled)
    }

    async fn delete(&self, page_id: &PageId) -> Result<()> {
        let path = self.page_path(page_id);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io_error("delete page file", e)),
        }
    }

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
            Ok::<(), Error>(())
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
        Self::parse_identity(&contents)
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

    async fn temp_store(page_size: u64) -> (LocalPageStore, PathBuf) {
        let base =
            std::env::temp_dir().join(format!("gfs_pagestore_test_{}", uuid::Uuid::new_v4()));
        let store = LocalPageStore::create(&base, page_size).await.unwrap();
        (store, base)
    }

    #[tokio::test]
    async fn put_get_roundtrip() {
        let (store, base) = temp_store(1024).await;
        let id = PageId::new("file-a", 3);
        let data = b"hello page cache".to_vec();

        store.put(&id, &data).await.unwrap();

        let mut dst = vec![0u8; data.len()];
        let n = store.get(&id, 0, &mut dst).await.unwrap();
        assert_eq!(n, data.len());
        assert_eq!(dst, data);

        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn get_with_offset() {
        let (store, base) = temp_store(1024).await;
        let id = PageId::new("file-b", 0);
        store.put(&id, b"0123456789").await.unwrap();

        let mut dst = vec![0u8; 4];
        let n = store.get(&id, 3, &mut dst).await.unwrap();
        assert_eq!(n, 4);
        assert_eq!(&dst, b"3456");

        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn get_missing_returns_zero() {
        let (store, base) = temp_store(1024).await;
        let id = PageId::new("nope", 0);
        let mut dst = vec![0u8; 8];
        assert_eq!(store.get(&id, 0, &mut dst).await.unwrap(), 0);
        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn get_short_read_at_tail() {
        let (store, base) = temp_store(1024).await;
        let id = PageId::new("file-c", 0);
        store.put(&id, b"abc").await.unwrap();

        // Ask for more than the page holds → fills only the available bytes.
        let mut dst = vec![0u8; 16];
        let n = store.get(&id, 0, &mut dst).await.unwrap();
        assert_eq!(n, 3);
        assert_eq!(&dst[..3], b"abc");
        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn delete_then_miss() {
        let (store, base) = temp_store(1024).await;
        let id = PageId::new("file-d", 1);
        store.put(&id, b"data").await.unwrap();
        store.delete(&id).await.unwrap();

        let mut dst = vec![0u8; 4];
        assert_eq!(store.get(&id, 0, &mut dst).await.unwrap(), 0);
        // Deleting again is a no-op.
        store.delete(&id).await.unwrap();
        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    /// `hash_file_id` MUST stay byte-for-byte stable across Rust versions /
    /// platforms, otherwise the entire on-disk cache is orphaned after an
    /// upgrade. Lock the algorithm to canonical xxHash3 (64-bit, seed=0) test
    /// vectors; if this ever fails, the on-disk bucket layout has silently
    /// changed.
    #[test]
    fn hash_file_id_is_stable_xxh3() {
        // Canonical xxHash3 64-bit (seed = 0) vectors. The empty-input value
        // (0x2D06800538D394C2) is the well-known xxh3_64 constant.
        assert_eq!(hash_file_id(""), 3244421341483603138);
        assert_eq!(hash_file_id("a"), 16629034431890738719);
        assert_eq!(hash_file_id("foobar"), 15532873758901296260);
    }

    #[test]
    fn parse_identity_accepts_well_formed_and_rejects_malformed() {
        assert_eq!(
            LocalPageStore::parse_identity("4096,1700000000000\n"),
            Some((4096, 1_700_000_000_000))
        );
        assert_eq!(
            LocalPageStore::parse_identity("  12 , 34  "),
            Some((12, 34))
        );
        assert_eq!(LocalPageStore::parse_identity(""), None);
        assert_eq!(LocalPageStore::parse_identity("only-one"), None);
        assert_eq!(LocalPageStore::parse_identity("a,b"), None);
        // Trailing junk after the second field makes the mtime unparsable.
        assert_eq!(LocalPageStore::parse_identity("1,2,3"), None);
    }

    #[tokio::test]
    async fn get_bytes_returns_owned_page_slice() {
        let (store, base) = temp_store(1024).await;
        let id = PageId::new("file-bytes", 0);
        store.put(&id, b"abcdefghij").await.unwrap();

        let bytes = store.get_bytes(&id, 3, 4).await.unwrap();
        assert_eq!(&bytes[..], b"defg");

        let empty = store.get_bytes(&id, 0, 0).await.unwrap();
        assert!(empty.is_empty());

        let _ = tokio::fs::remove_dir_all(&base).await;
    }
}
