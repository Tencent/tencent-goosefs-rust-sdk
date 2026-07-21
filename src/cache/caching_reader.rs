// Copyright (C) 2026 Tencent. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Cache-aware random-read helper.
//!
//! [`read_through_cache`] implements the page-split read loop shared by the
//! file input stream: it serves a byte range page-by-page from a
//! [`CacheManager`], reading whole pages from an [`ExternalRangeReader`] on a
//! miss and (optionally) writing them back.
//!
//! Factoring the loop out of the stream lets it be unit-tested offline with a
//! fake external reader, while the stream supplies the real worker/UFS reader.

use std::sync::Arc;
use std::time::Instant;

use bytes::{Bytes, BytesMut};

use crate::cache::{metric_name, CacheManager, PageId, PageReadRequest};
use crate::error::{Error, Result};
use crate::metrics::counter;

/// Source for cache misses: reads an arbitrary byte range `[offset, end)`.
///
/// The file input stream implements this over its worker/UFS positioned-read
/// path.
#[async_trait::async_trait]
pub trait ExternalRangeReader {
    /// Read the bytes in `[offset, end)`. May return fewer bytes only at EOF.
    async fn read_range(&mut self, offset: i64, end: i64) -> Result<Bytes>;
}

/// How missed pages are written back into the cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillMode {
    /// Do not write missed pages back.
    None,
    /// Fill inline (`await` the `put`); the page is cached before returning.
    Sync,
    /// Fill via the bounded async write-back pool; returns without waiting.
    Async,
}

/// Serve `[offset, end)` through the page cache.
///
/// # Arguments
/// - `cache` — the page cache to consult / fill (shared `Arc` so async fills
///   can be scheduled without borrowing the caller).
/// - `ext` — miss source (worker/UFS).
/// - `file_id` — stable cache key namespace for this file.
/// - `page_size` — cache page size (bytes, must be > 0).
/// - `file_length` — total file length (clamps the last page).
/// - `offset`, `end` — requested half-open range (`0 <= offset < end <= file_length`).
/// - `fill_mode` — how to write missed pages back into the cache.
///
/// Best-effort: cache errors degrade to external reads, never to a failure.
#[allow(clippy::too_many_arguments)]
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

    while cur < end {
        let page_index = (cur as u64) / page_size;
        let page_start = (page_index * page_size) as i64;
        let page_end = (((page_index + 1) * page_size) as i64).min(file_length);
        let in_page_off = (cur - page_start) as usize;
        let want = (end.min(page_end) - cur) as usize;
        pages.push((
            PageId::new(file_id.clone(), page_index),
            page_index,
            page_start,
            page_end,
            in_page_off,
            want,
        ));
        cur += want as i64;
    }

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

    let mut chunks: Vec<Bytes> = Vec::with_capacity(pages.len());
    for ((page_id, page_index, page_start, page_end, in_page_off, want), cached_bytes) in
        pages.into_iter().zip(cached.into_iter())
    {
        // 1) Cache hit: keep the returned Bytes directly. For the io_uring
        // backend this is the kernel-filled buffer, so single-page reads avoid
        // the old tmp-buffer copy entirely.
        if cached_bytes.len() == want {
            chunks.push(cached_bytes);
            continue;
        }

        // 2) Miss → read the whole page from the external source.
        let ext_start = Instant::now();
        let page_bytes = ext.read_range(page_start, page_end).await?;
        counter(metric_name::CLIENT_CACHE_PAGE_READ_EXTERNAL_TIME_NS)
            .inc(ext_start.elapsed().as_nanos() as i64);
        counter(metric_name::CLIENT_CACHE_BYTES_READ_EXTERNAL).inc(page_bytes.len() as i64);
        counter(metric_name::CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL).inc(page_end - page_start);
        // Refresh the hit-rate gauge now that the external-read counter moved.
        crate::cache::metrics::publish_hit_rate();

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

    if chunks.is_empty() {
        return Ok(Bytes::new());
    }
    if chunks.len() == 1 {
        return Ok(chunks.pop().unwrap());
    }

    let mut out = BytesMut::with_capacity(requested_len);
    for chunk in chunks {
        out.extend_from_slice(&chunk);
    }
    Ok(out.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use crate::cache::manager::LocalCacheManager;
    use crate::cache::options::CacheManagerOptions;
    use crate::config::CacheEvictorType;

    /// In-memory external source backed by a byte buffer; counts the number of
    /// bytes served so tests can assert hit vs. miss behaviour.
    struct FakeExternal {
        data: Vec<u8>,
        bytes_served: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl ExternalRangeReader for FakeExternal {
        async fn read_range(&mut self, offset: i64, end: i64) -> Result<Bytes> {
            let s = offset as usize;
            let e = (end as usize).min(self.data.len());
            self.bytes_served.fetch_add(e - s, Ordering::SeqCst);
            Ok(Bytes::copy_from_slice(&self.data[s..e]))
        }
    }

    struct ShortExternal {
        data: Vec<u8>,
    }

    #[async_trait::async_trait]
    impl ExternalRangeReader for ShortExternal {
        async fn read_range(&mut self, offset: i64, end: i64) -> Result<Bytes> {
            let s = offset as usize;
            let e = (end as usize).min(self.data.len()).saturating_sub(1);
            Ok(Bytes::copy_from_slice(&self.data[s..e]))
        }
    }

    async fn mgr(page_size: u64, capacity: u64) -> (Arc<dyn CacheManager>, std::path::PathBuf) {
        let base = std::env::temp_dir().join(format!("gfs_cr_test_{}", uuid::Uuid::new_v4()));
        let options = CacheManagerOptions {
            page_size,
            dir_capacity: capacity,
            dirs: vec![base.clone()],
            evictor: CacheEvictorType::Lru,
            async_write_enabled: false,
            async_write_threads: 1,
            quota_enabled: false,
            ttl: None,
            uring_enabled: false,
            uring_queue_depth: 0,
            uring_thread_count: 0,
        };
        let mgr: Arc<dyn CacheManager> =
            Arc::new(LocalCacheManager::create(options).await.unwrap());
        (mgr, base)
    }

    #[tokio::test]
    async fn cold_read_misses_then_warm_read_hits() {
        let (cache, base) = mgr(4, 4096).await;
        let data: Vec<u8> = (0u8..=99).collect();
        let served = Arc::new(AtomicUsize::new(0));
        let file_id: Arc<str> = Arc::from("file-1");

        let mut ext = FakeExternal {
            data: data.clone(),
            bytes_served: served.clone(),
        };

        // Cold read [10, 30): all misses → external serves whole pages.
        let cold = read_through_cache(
            &cache,
            &mut ext,
            &file_id,
            4,
            data.len() as i64,
            10,
            30,
            FillMode::Sync,
        )
        .await
        .unwrap();
        assert_eq!(&cold[..], &data[10..30]);
        let after_cold = served.load(Ordering::SeqCst);
        assert!(after_cold > 0, "cold read must hit the external source");

        // Warm read of the same range: every page is now cached → no external.
        let warm = read_through_cache(
            &cache,
            &mut ext,
            &file_id,
            4,
            data.len() as i64,
            10,
            30,
            FillMode::Sync,
        )
        .await
        .unwrap();
        assert_eq!(&warm[..], &data[10..30]);
        assert_eq!(
            served.load(Ordering::SeqCst),
            after_cold,
            "warm read must be served entirely from cache (no new external bytes)"
        );

        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn spans_multiple_pages_and_partial_offsets() {
        let (cache, base) = mgr(8, 4096).await;
        let data: Vec<u8> = (0u8..=200).collect();
        let served = Arc::new(AtomicUsize::new(0));
        let file_id: Arc<str> = Arc::from("file-multi");
        let mut ext = FakeExternal {
            data: data.clone(),
            bytes_served: served.clone(),
        };

        // Range crossing several 8-byte pages with non-aligned start/end.
        let got = read_through_cache(
            &cache,
            &mut ext,
            &file_id,
            8,
            data.len() as i64,
            5,
            37,
            FillMode::Sync,
        )
        .await
        .unwrap();
        assert_eq!(&got[..], &data[5..37]);
        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn no_fill_does_not_populate_cache() {
        let (cache, base) = mgr(4, 4096).await;
        let data: Vec<u8> = (0u8..=50).collect();
        let served = Arc::new(AtomicUsize::new(0));
        let file_id: Arc<str> = Arc::from("file-nofill");
        let mut ext = FakeExternal {
            data: data.clone(),
            bytes_served: served.clone(),
        };

        for _ in 0..2 {
            let got = read_through_cache(
                &cache,
                &mut ext,
                &file_id,
                4,
                data.len() as i64,
                0,
                12,
                FillMode::None,
            )
            .await
            .unwrap();
            assert_eq!(&got[..], &data[0..12]);
        }
        // With FillMode::None, both passes go to the external source.
        assert!(
            served.load(Ordering::SeqCst) >= 24,
            "FillMode::None must not cache; both reads hit external"
        );
        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn short_external_page_read_errors_and_does_not_fill_cache() {
        let (cache, base) = mgr(8, 4096).await;
        let data: Vec<u8> = (0u8..=50).collect();
        let file_id: Arc<str> = Arc::from("file-short-read");
        let mut ext = ShortExternal { data: data.clone() };

        let err = read_through_cache(
            &cache,
            &mut ext,
            &file_id,
            8,
            data.len() as i64,
            0,
            8,
            FillMode::Sync,
        )
        .await
        .unwrap_err();
        assert!(format!("{}", err).contains("short external read"));

        let mut dst = vec![0u8; 8];
        assert_eq!(
            cache
                .get(&PageId::new(file_id.clone(), 0), 0, &mut dst)
                .await,
            0
        );
        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn async_fill_eventually_caches() {
        let (cache, base) = mgr(4, 4096).await;
        let data: Vec<u8> = (0u8..=50).collect();
        let served = Arc::new(AtomicUsize::new(0));
        let file_id: Arc<str> = Arc::from("file-async");
        let mut ext = FakeExternal {
            data: data.clone(),
            bytes_served: served.clone(),
        };

        // First read with async fill: the slice is still returned correctly,
        // but the fill happens in the background.
        let got = read_through_cache(
            &cache,
            &mut ext,
            &file_id,
            4,
            data.len() as i64,
            0,
            12,
            FillMode::Async,
        )
        .await
        .unwrap();
        assert_eq!(&got[..], &data[0..12]);

        // Eventually the pages land and subsequent reads stop hitting external.
        let mut cached = false;
        for _ in 0..100 {
            let before = served.load(Ordering::SeqCst);
            let _ = read_through_cache(
                &cache,
                &mut ext,
                &file_id,
                4,
                data.len() as i64,
                0,
                12,
                FillMode::Async,
            )
            .await
            .unwrap();
            if served.load(Ordering::SeqCst) == before {
                cached = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(cached, "async fill should eventually populate the cache");
        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    /// Partial hit: some pages warm, some cold. Cold pages must hit external
    /// once; warm pages must not. Validates the P6 `get_batch_bytes` path.
    #[tokio::test]
    async fn mixed_hit_miss_only_fetches_cold_pages() {
        let (cache, base) = mgr(4, 4096).await;
        let data: Vec<u8> = (0u8..=63).collect();
        let served = Arc::new(AtomicUsize::new(0));
        let file_id: Arc<str> = Arc::from("file-mixed");
        let mut ext = FakeExternal {
            data: data.clone(),
            bytes_served: served.clone(),
        };

        // Warm pages 0 and 1 ([0,8)).
        let _ = read_through_cache(
            &cache,
            &mut ext,
            &file_id,
            4,
            data.len() as i64,
            0,
            8,
            FillMode::Sync,
        )
        .await
        .unwrap();
        let after_warm = served.load(Ordering::SeqCst);
        assert!(after_warm > 0);

        // Range [2, 14) spans pages 0..3: pages 0-1 warm, 2-3 cold.
        let got = read_through_cache(
            &cache,
            &mut ext,
            &file_id,
            4,
            data.len() as i64,
            2,
            14,
            FillMode::Sync,
        )
        .await
        .unwrap();
        assert_eq!(&got[..], &data[2..14]);

        let after_mixed = served.load(Ordering::SeqCst);
        assert!(
            after_mixed > after_warm,
            "cold pages must still hit external"
        );
        // Warm pages must not be re-fetched: external only serves the two cold
        // 4-byte pages (indices 2 and 3) = 8 bytes.
        assert_eq!(
            after_mixed - after_warm,
            8,
            "only the two cold pages should be fetched from external"
        );

        let _ = tokio::fs::remove_dir_all(&base).await;
    }
}
