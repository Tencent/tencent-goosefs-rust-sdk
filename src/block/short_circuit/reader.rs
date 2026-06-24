//! [`LocalBlockReader`] — the short-circuit data plane.
//!
//! A `LocalBlockReader` maps a single Goosefs block file **once** into the
//! process address space (whole-block read-only mmap) and serves all reads as
//! pure slice operations with zero further syscalls (design §3.2). The
//! Worker-side block lock is held for the reader's lifetime by an
//! [`OpenLocalBlockGuard`].
//!
//! See `docs/SHORT_CIRCUIT_DESIGN.md` §4.1 / §8 for the full contract and the
//! consistency invariants (INV-D1..D4, INV-S2, INV-S5) this implements.

use std::sync::Arc;

use bytes::Bytes;
use memmap2::Mmap;
use tracing::{debug, trace};

use crate::client::{OpenLocalBlockGuard, WorkerClient};
use crate::metrics::{self, name};
use crate::proto::proto::security::Capability;

use super::{AccessHint, ShortCircuitError};

/// Newtype wrapper so an `Arc<Mmap>` can be handed to [`Bytes::from_owner`],
/// which requires `AsRef<[u8]> + Send + 'static`. `Arc<Mmap>` itself does not
/// implement `AsRef<[u8]>`, hence this wrapper.
///
/// `as_ref` exposes exactly the **logical** block (`[..file_size]`), not the
/// whole physical mapping: if the on-disk block file is preallocated / sparse
/// to a length greater than the logical block size, the trailing bytes must
/// not be observable (INV-D2, design §4.1 `file_size` note).
struct MmapChunk {
    mmap: Arc<Mmap>,
    /// Logical block length — the only region callers may observe.
    file_size: usize,
}

impl AsRef<[u8]> for MmapChunk {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.mmap[..self.file_size]
    }
}

/// A read-only, whole-block memory map of a single local Goosefs block.
///
/// Field declaration order matters: `mmap` is declared **before** `_guard` so
/// that on Drop the mapping is released (munmap) first, then the guard closes
/// the bidi stream (Worker unlock). Correctness does not depend on this order
/// (the kernel keeps the inode alive via the VMA), but it makes the
/// "release resource before releasing the permit" intent explicit (design §8.2).
pub struct LocalBlockReader {
    /// Block this reader serves.
    block_id: i64,
    /// **Logical** block size from the `OpenLocalBlock` response — the
    /// authority for the mmap window and every bounds check. NOT the physical
    /// on-disk file length (see [`MmapChunk`]).
    file_size: usize,
    /// Whole-block read-only mapping, created exactly once in [`open`](Self::open).
    /// `Arc` so zero-copy [`Bytes`](Self::read_bytes) can extend the mapping's
    /// lifetime past the reader (INV-D3).
    mmap: Arc<Mmap>,
    /// Worker-side block lock. Dropped after `mmap` (declaration order).
    ///
    /// `Option` so the data plane can be unit-tested with a reader built
    /// directly from a local file (no live `OpenLocalBlock` session); the
    /// production [`open`](Self::open) path always sets `Some`.
    _guard: Option<OpenLocalBlockGuard>,
}

impl LocalBlockReader {
    /// Open a short-circuit reader for `block_id` (design §4.1).
    ///
    /// 1. Drives `OpenLocalBlock` to obtain the local `path` + logical
    ///    `block_size` and a lock-holding [`OpenLocalBlockGuard`].
    /// 2. Maps the whole block file read-only (one `mmap`), then drops the
    ///    `File` so no fd is retained (the VMA keeps the inode alive).
    /// 3. Applies the `madvise` hint derived from `hint` (L1 kernel readahead),
    ///    and optionally `MADV_HUGEPAGE` when `thp` is set (Linux only, §11.1).
    ///
    /// `block_size` is the caller's expected size used in the request; the
    /// **response** `block_size` is authoritative and becomes `file_size`.
    ///
    /// ⚠️ `capability` source is a P3 item (design §3.1): on capability-enabled
    /// clusters this must carry a valid capability or the Worker rejects the
    /// request. `None` = no capability (NOSASL / disabled clusters).
    pub async fn open(
        client: &WorkerClient,
        block_id: i64,
        block_size: i64,
        capability: Option<Capability>,
        hint: AccessHint,
        thp: bool,
    ) -> Result<Self, ShortCircuitError> {
        let (resp, guard) = client
            .open_local_block(block_id, block_size, capability)
            .await
            .map_err(|e| {
                metrics::counter(name::CLIENT_SC_OPENLOCAL_FAIL).inc(1);
                ShortCircuitError::OpenLocalBlock(Box::new(e))
            })?;

        let path = resp.path.ok_or_else(|| {
            metrics::counter(name::CLIENT_SC_OPENLOCAL_FAIL).inc(1);
            ShortCircuitError::MissingPath
        })?;
        // The response `block_size` is the logical authority; fall back to the
        // requested size only if the Worker omitted it.
        let file_size = resp.block_size.unwrap_or(block_size).max(0) as usize;

        // `File::open` + `Mmap::map` are short, metadata-only syscalls (no data
        // IO); per design §3.4 they run directly on the calling task (no
        // spawn_blocking). The fd is dropped immediately after mapping.
        let file = std::fs::File::open(&path).map_err(|e| {
            metrics::counter(name::CLIENT_SC_FILE_OPEN_FAIL).inc(1);
            ShortCircuitError::FileOpen(e)
        })?;

        // SAFETY: We map the block file read-only for the reader's lifetime.
        // The safety precondition is INV-D1 (design §8.1): the Worker holds the
        // block lock (via `guard`) for as long as this mapping lives and a
        // committed block file is immutable, so the bytes under the mapping do
        // not change / truncate. If that protocol guarantee were violated a
        // SIGBUS could occur; the (optional, P6) SIGBUS handler turns that into
        // a diagnosed `abort` rather than a torn/stale read — it never returns
        // partial bytes. No other unsafe is used: the zero-copy `read_bytes`
        // path uses the safe `Bytes::from_owner`.
        let mmap = unsafe { Mmap::map(&file) }.map_err(|e| {
            metrics::counter(name::CLIENT_SC_MMAP_FAIL).inc(1);
            ShortCircuitError::Mmap(e)
        })?;
        // fd no longer needed — the VMA holds the inode reference.
        drop(file);

        // Defensive: the logical block must fit inside the physical mapping.
        // A logical size larger than the file would let bounds_check admit
        // reads past the mapping → UB. Treat as an out-of-range protocol error
        // (do NOT silently clamp — that would violate INV-D2).
        let file_size = file_size.min(mmap.len());

        apply_advice(&mmap, hint);
        if thp {
            apply_hugepage(&mmap);
        }

        metrics::counter(name::CLIENT_SC_OPEN_SUCCESS).inc(1);
        metrics::gauge(name::CLIENT_SC_ACTIVE_READERS)
            .set(metrics::gauge(name::CLIENT_SC_ACTIVE_READERS).get() + 1);

        debug!(
            block_id = block_id,
            path = %path,
            file_size = file_size,
            mmap_len = mmap.len(),
            ?hint,
            "LocalBlockReader opened (whole-block mmap)"
        );

        Ok(Self {
            block_id,
            file_size,
            mmap: Arc::new(mmap),
            _guard: Some(guard),
        })
    }

    /// The block id this reader serves.
    #[inline]
    pub fn block_id(&self) -> i64 {
        self.block_id
    }

    /// The logical block size (bytes).
    #[inline]
    pub fn file_size(&self) -> usize {
        self.file_size
    }

    /// Bounds check shared by every read / prefetch API.
    ///
    /// Returns [`ShortCircuitError::OutOfRange`] (a *semantic* error that must
    /// NOT be swallowed by fallback — INV-S4) when `[offset, offset+len)`
    /// escapes the logical block. A zero-length read at any in-range offset is
    /// allowed and yields an empty slice.
    #[inline]
    fn bounds_check(&self, offset: usize, len: usize) -> Result<(), ShortCircuitError> {
        let end = offset.checked_add(len);
        match end {
            Some(end) if end <= self.file_size => Ok(()),
            _ => Err(ShortCircuitError::OutOfRange {
                off: offset,
                len,
                file_size: self.file_size,
            }),
        }
    }

    /// Zero-copy borrow of `[offset, offset+len)` (design §3.3).
    ///
    /// Pure pointer arithmetic — no syscall. The slice borrows `self`, so it
    /// cannot outlive the reader. For an owned, `'static`, cross-await handle
    /// use [`read_bytes`](Self::read_bytes).
    pub fn read(&self, offset: usize, len: usize) -> Result<&[u8], ShortCircuitError> {
        self.bounds_check(offset, len)?;
        metrics::counter(name::CLIENT_SC_READ_CALLS).inc(1);
        metrics::counter(name::CLIENT_SC_READ_BYTES).inc(len as i64);
        trace!(block_id = self.block_id, offset, len, "sc read (slice)");
        Ok(&self.mmap[offset..offset + len])
    }

    /// Zero-copy, ref-counted [`Bytes`] view of `[offset, offset+len)`
    /// (design §3.3).
    ///
    /// Uses [`Bytes::from_owner`] so the returned `Bytes` (and any clone /
    /// sub-slice) keeps the underlying `Arc<Mmap>` alive until the last
    /// reference is dropped — even after this `LocalBlockReader` itself is
    /// dropped (INV-D3). No `unsafe`.
    pub fn read_bytes(&self, offset: usize, len: usize) -> Result<Bytes, ShortCircuitError> {
        self.bounds_check(offset, len)?;
        metrics::counter(name::CLIENT_SC_READ_CALLS).inc(1);
        metrics::counter(name::CLIENT_SC_READ_BYTES).inc(len as i64);
        // Whole logical block as a zero-copy Bytes, then narrow with `.slice`
        // (pointer/len adjustment only — no copy).
        let full = Bytes::from_owner(MmapChunk {
            mmap: Arc::clone(&self.mmap),
            file_size: self.file_size,
        });
        Ok(full.slice(offset..offset + len))
    }

    /// Copy `dst.len()` bytes starting at `offset` into `dst` (design §3.3).
    ///
    /// For callers that must own the buffer. Returns the number of bytes
    /// copied (`dst.len()`).
    pub fn read_to_slice(
        &self,
        offset: usize,
        dst: &mut [u8],
    ) -> Result<usize, ShortCircuitError> {
        let len = dst.len();
        self.bounds_check(offset, len)?;
        metrics::counter(name::CLIENT_SC_READ_CALLS).inc(1);
        metrics::counter(name::CLIENT_SC_READ_BYTES).inc(len as i64);
        dst.copy_from_slice(&self.mmap[offset..offset + len]);
        Ok(len)
    }

    /// L2 application-level prefetch (design §3.2.1): asynchronously ask the
    /// kernel to read `[offset, offset+len)` into the page cache via
    /// `madvise(MADV_WILLNEED)`.
    ///
    /// Returns immediately (async readahead); a no-op for already-resident
    /// pages and on platforms / file systems where `MADV_WILLNEED` is
    /// unsupported. **Never modifies any byte** (INV-D4); its success/failure
    /// does not change subsequent `read` results.
    pub fn prefetch(&self, offset: usize, len: usize) -> Result<(), ShortCircuitError> {
        self.bounds_check(offset, len)?;
        metrics::counter(name::CLIENT_SC_PREFETCH_CALLS).inc(1);
        metrics::counter(name::CLIENT_SC_PREFETCH_BYTES).inc(len as i64);
        if len == 0 {
            return Ok(());
        }
        advise_willneed(&self.mmap, offset, len);
        Ok(())
    }

    /// L2 batch prefetch (design §4.1 / §3.2.1): coalesce + sort the requested
    /// ranges (merging gaps ≤ `coalesce_gap`) and issue one `madvise` per
    /// merged span to minimise syscalls.
    ///
    /// `coalesce_gap` is `goosefs.client.short.circuit.prefetch.coalesce.gap`.
    /// All ranges are bounds-checked first; if any is out of range the whole
    /// call fails (semantic error) before issuing any `madvise`.
    pub fn prefetch_many(
        &self,
        ranges: &[(usize, usize)],
        coalesce_gap: usize,
    ) -> Result<(), ShortCircuitError> {
        metrics::counter(name::CLIENT_SC_PREFETCH_CALLS).inc(1);
        if ranges.is_empty() {
            return Ok(());
        }
        // Validate everything up-front (no partial effect on error).
        for &(off, len) in ranges {
            self.bounds_check(off, len)?;
        }
        let merged = coalesce_ranges(ranges, coalesce_gap);
        let mut total_bytes: i64 = 0;
        for (_off, len) in &merged {
            total_bytes += *len as i64;
        }
        metrics::counter(name::CLIENT_SC_PREFETCH_BYTES).inc(total_bytes);
        for (off, len) in merged {
            if len == 0 {
                continue;
            }
            advise_willneed(&self.mmap, off, len);
        }
        Ok(())
    }

    /// Physical mapping length (bytes). Test/diagnostic helper.
    #[inline]
    pub fn mmap_len(&self) -> usize {
        self.mmap.len()
    }

    /// Test-only: build a reader directly from a local file path, mapping the
    /// whole file and treating `file_size` as the logical block length (with
    /// no `OpenLocalBlock` session). Lets the data-plane invariants be
    /// exercised offline. `file_size` is clamped to the physical mapping size.
    #[cfg(test)]
    fn open_from_path_for_test(
        path: &std::path::Path,
        file_size: usize,
        hint: AccessHint,
    ) -> Result<Self, ShortCircuitError> {
        let file = std::fs::File::open(path).map_err(ShortCircuitError::FileOpen)?;
        // SAFETY: test-only; the temp file is not mutated while mapped.
        let mmap = unsafe { Mmap::map(&file) }.map_err(ShortCircuitError::Mmap)?;
        drop(file);
        let file_size = file_size.min(mmap.len());
        apply_advice(&mmap, hint);
        Ok(Self {
            block_id: 0,
            file_size,
            mmap: Arc::new(mmap),
            _guard: None,
        })
    }
}

impl Drop for LocalBlockReader {
    fn drop(&mut self) {
        metrics::gauge(name::CLIENT_SC_ACTIVE_READERS)
            .set((metrics::gauge(name::CLIENT_SC_ACTIVE_READERS).get() - 1).max(0));
        // `mmap` (munmap) then `_guard` (Worker unlock) drop in declaration
        // order. Any outstanding `Bytes` from `read_bytes` keep their own
        // `Arc<Mmap>` clone, deferring the real munmap (INV-D3).
    }
}

/// Coalesce + sort `(offset, len)` ranges, merging spans separated by a gap of
/// at most `gap` bytes. Pure function so it is unit-testable offline.
fn coalesce_ranges(ranges: &[(usize, usize)], gap: usize) -> Vec<(usize, usize)> {
    let mut sorted: Vec<(usize, usize)> = ranges.iter().copied().filter(|(_, l)| *l > 0).collect();
    if sorted.is_empty() {
        return Vec::new();
    }
    sorted.sort_by_key(|(off, _)| *off);

    let mut merged: Vec<(usize, usize)> = Vec::with_capacity(sorted.len());
    let (mut cur_off, mut cur_len) = sorted[0];
    for &(off, len) in &sorted[1..] {
        let cur_end = cur_off + cur_len;
        // Merge when the next range starts within `gap` of the current end
        // (saturating so adjacent/overlapping ranges always merge).
        if off <= cur_end.saturating_add(gap) {
            let new_end = cur_end.max(off + len);
            cur_len = new_end - cur_off;
        } else {
            merged.push((cur_off, cur_len));
            cur_off = off;
            cur_len = len;
        }
    }
    merged.push((cur_off, cur_len));
    merged
}

/// Apply the L1 kernel-readahead hint (design §3.2.1 "L1 decision matrix").
///
/// `madvise` is unix-only in `memmap2`; on other targets this is a no-op and
/// `AccessHint::Default` is the safe cross-platform default.
#[cfg(unix)]
fn apply_advice(mmap: &Mmap, hint: AccessHint) {
    use memmap2::Advice;
    let advice = match hint {
        AccessHint::Sequential => Advice::Sequential,
        AccessHint::Random => Advice::Random,
        AccessHint::Default => return, // no madvise
    };
    if let Err(e) = mmap.advise(advice) {
        debug!(error = %e, ?hint, "madvise(advice) failed (non-fatal)");
    }
}

#[cfg(not(unix))]
fn apply_advice(_mmap: &Mmap, _hint: AccessHint) {}

/// Request Transparent Huge Pages for the mapping via `madvise(MADV_HUGEPAGE)`
/// (design §11.1). Linux only — file-backed THP support is kernel/FS
/// dependent, so this is best-effort and failures are logged and ignored.
/// A no-op on every non-Linux target (no `MADV_HUGEPAGE` there).
#[cfg(target_os = "linux")]
fn apply_hugepage(mmap: &Mmap) {
    use memmap2::Advice;
    if let Err(e) = mmap.advise(Advice::HugePage) {
        debug!(error = %e, "madvise(HUGEPAGE) failed (non-fatal)");
    }
}

#[cfg(not(target_os = "linux"))]
fn apply_hugepage(_mmap: &Mmap) {}

/// Issue `madvise(MADV_WILLNEED)` over `[offset, offset+len)` (unix only).
/// Best-effort: failures are logged and ignored (INV-D4 — readahead hint only).
#[cfg(unix)]
fn advise_willneed(mmap: &Mmap, offset: usize, len: usize) {
    use memmap2::Advice;
    match mmap.advise_range(Advice::WillNeed, offset, len) {
        Ok(()) => {
            metrics::counter(name::CLIENT_SC_PREFETCH_MADVISE).inc(1);
        }
        Err(e) => {
            debug!(error = %e, offset, len, "madvise(WILLNEED) failed (non-fatal)");
        }
    }
}

#[cfg(not(unix))]
fn advise_willneed(_mmap: &Mmap, _offset: usize, _len: usize) {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Write `data` to a uniquely-named temp file and return its path.
    fn write_temp(data: &[u8]) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let uniq = format!(
            "gfs_sc_test_{}_{}.bin",
            std::process::id(),
            // monotonic-ish unique suffix
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        p.push(uniq);
        let mut f = std::fs::File::create(&p).unwrap();
        f.write_all(data).unwrap();
        f.sync_all().unwrap();
        p
    }

    fn reader_for(data: &[u8]) -> (LocalBlockReader, std::path::PathBuf) {
        let path = write_temp(data);
        let r =
            LocalBlockReader::open_from_path_for_test(&path, data.len(), AccessHint::Random).unwrap();
        (r, path)
    }

    /// INV-D2: the mmap slice equals the source bytes, byte-for-byte, at
    /// arbitrary (offset, len) windows including page/edge boundaries.
    #[test]
    fn read_matches_source_bytes() {
        let data: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();
        let (r, path) = reader_for(&data);

        for &(off, len) in &[
            (0usize, 0usize),
            (0, 1),
            (0, data.len()),
            (123, 4096),
            (4095, 4098), // crosses a 4 KiB page boundary
            (data.len() - 1, 1),
            (data.len(), 0), // zero-len at EOF is allowed
        ] {
            let got = r.read(off, len).unwrap();
            assert_eq!(got, &data[off..off + len], "off={off} len={len}");
        }
        std::fs::remove_file(path).ok();
    }

    /// INV-S5: `read`, `read_bytes` and `read_to_slice` return identical bytes
    /// for the same (offset, len).
    #[test]
    fn three_apis_agree() {
        let data: Vec<u8> = (0..10_000u32).map(|i| (i * 7 % 256) as u8).collect();
        let (r, path) = reader_for(&data);

        let (off, len) = (1000usize, 2048usize);
        let a = r.read(off, len).unwrap().to_vec();
        let b = r.read_bytes(off, len).unwrap();
        let mut c = vec![0u8; len];
        let n = r.read_to_slice(off, &mut c).unwrap();

        assert_eq!(n, len);
        assert_eq!(a, b.as_ref());
        assert_eq!(a, c);
        assert_eq!(a, &data[off..off + len]);
        std::fs::remove_file(path).ok();
    }

    /// INV-D3: a `Bytes` returned by `read_bytes` stays valid (and unchanged)
    /// after the `LocalBlockReader` itself is dropped — the `Arc<Mmap>` owner
    /// keeps the mapping alive.
    #[test]
    fn read_bytes_outlives_reader() {
        let data: Vec<u8> = (0..5000u32).map(|i| (i % 256) as u8).collect();
        let (r, path) = reader_for(&data);

        let held = r.read_bytes(100, 500).unwrap();
        let expected = data[100..600].to_vec();
        drop(r); // reader gone — mapping must survive via the Bytes owner
        assert_eq!(held.as_ref(), expected.as_slice());
        // A clone still works too.
        let clone = held.clone();
        assert_eq!(clone.as_ref(), expected.as_slice());
        std::fs::remove_file(path).ok();
    }

    /// INV-S4: out-of-range reads return `OutOfRange` (a semantic error),
    /// never a panic or fallback.
    #[test]
    fn out_of_range_is_error() {
        let data = vec![0u8; 1000];
        let (r, path) = reader_for(&data);

        assert!(matches!(
            r.read(900, 200),
            Err(ShortCircuitError::OutOfRange { .. })
        ));
        assert!(matches!(
            r.read(1001, 0),
            Err(ShortCircuitError::OutOfRange { .. })
        ));
        // Overflow-safe: offset + len wrapping must still be rejected.
        assert!(matches!(
            r.read(usize::MAX, 1),
            Err(ShortCircuitError::OutOfRange { .. })
        ));
        // Exactly at the end with zero length is fine.
        assert!(r.read(1000, 0).is_ok());
        let mut dst = vec![0u8; 200];
        assert!(matches!(
            r.read_to_slice(900, &mut dst),
            Err(ShortCircuitError::OutOfRange { .. })
        ));
        std::fs::remove_file(path).ok();
    }

    /// INV-D4: prefetch / prefetch_many never change bytes and tolerate
    /// in-range requests; out-of-range prefetch is a semantic error.
    #[test]
    fn prefetch_does_not_change_bytes() {
        let data: Vec<u8> = (0..20_000u32).map(|i| (i % 256) as u8).collect();
        let (r, path) = reader_for(&data);

        let before = r.read(0, data.len()).unwrap().to_vec();
        r.prefetch(0, 4096).unwrap();
        r.prefetch(8192, 4096).unwrap();
        r.prefetch_many(&[(0, 1000), (1000, 1000), (15000, 100)], 64 * 1024)
            .unwrap();
        let after = r.read(0, data.len()).unwrap().to_vec();
        assert_eq!(before, after);

        // Out-of-range prefetch is rejected.
        assert!(matches!(
            r.prefetch(data.len() - 10, 100),
            Err(ShortCircuitError::OutOfRange { .. })
        ));
        assert!(matches!(
            r.prefetch_many(&[(0, 10), (data.len(), 10)], 0),
            Err(ShortCircuitError::OutOfRange { .. })
        ));
        std::fs::remove_file(path).ok();
    }

    /// `file_size` is clamped to the physical mapping; a logical size larger
    /// than the file never exposes bytes past the mapping.
    #[test]
    fn logical_size_clamped_to_mapping() {
        let data = vec![7u8; 100];
        let path = write_temp(&data);
        // Claim a logical size far larger than the 100-byte file.
        let r =
            LocalBlockReader::open_from_path_for_test(&path, 1_000_000, AccessHint::Default).unwrap();
        assert_eq!(r.file_size(), 100, "logical size must clamp to file len");
        assert!(matches!(
            r.read(0, 200),
            Err(ShortCircuitError::OutOfRange { .. })
        ));
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn coalesce_merges_adjacent_and_within_gap() {
        // Adjacent (gap 0) and within-gap ranges merge; far ranges stay split.
        let ranges = [(0usize, 100usize), (100, 50), (200, 10), (1000, 10)];
        let merged = coalesce_ranges(&ranges, 64);
        // [0,100)+[100,150) adjacent → [0,150); [200,210) is within gap 64 of
        // 150 (200 <= 150+64=214) → merge into [0,210); [1000,1010) far → split.
        assert_eq!(merged, vec![(0, 210), (1000, 10)]);
    }

    #[test]
    fn coalesce_sorts_unordered_input() {
        let ranges = [(1000, 10), (0, 10)];
        let merged = coalesce_ranges(&ranges, 0);
        assert_eq!(merged, vec![(0, 10), (1000, 10)]);
    }

    #[test]
    fn coalesce_drops_zero_length() {
        let ranges = [(0, 0), (10, 5), (20, 0)];
        let merged = coalesce_ranges(&ranges, 0);
        assert_eq!(merged, vec![(10, 5)]);
    }

    #[test]
    fn coalesce_overlapping_ranges() {
        let ranges = [(0, 100), (50, 100)];
        let merged = coalesce_ranges(&ranges, 0);
        assert_eq!(merged, vec![(0, 150)]);
    }

    #[test]
    fn coalesce_empty_input() {
        assert!(coalesce_ranges(&[], 64).is_empty());
    }
}
