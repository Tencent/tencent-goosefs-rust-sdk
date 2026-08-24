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

//! Cache eviction policies.
//!
//! A [`CacheEvictor`] owns the per-directory byte quota and decides which pages
//! to drop when that quota is exceeded. Mirrors Java `CacheEvictor`. The
//! evictor only tracks page *identity* and size — the on-disk page removal and
//! the manager-side indices are updated by
//! [`LocalCacheManager`](crate::cache::manager::LocalCacheManager) when it
//! reclaims the pages reported by [`CacheEvictor::drain_victims`].
//!
//! # Admit-then-evict
//!
//! Unlike the previous `evict_candidate()` contract ("name one page I should
//! drop"), the evictor is now driven by admission: [`CacheEvictor::on_add`]
//! admits a page and, if that pushes the directory over quota, evicts as many
//! pages as needed. The victims are queued and handed to the manager by
//! [`CacheEvictor::drain_victims`].
//!
//! The change exists because naming a victim on demand cannot be implemented in
//! O(1) on top of a sharded cache: the quota is global to the directory while
//! the eviction order is per shard, so "who should leave" is only answerable at
//! the moment a specific shard overflows. The old contract forced an
//! `iter().min_by_key()` scan over every tracked page, which is O(page count) —
//! roughly 1-2 ms per eviction at the 100 GB / 100k-page working point. See
//! `docs/FOYER_SSD_CACHE_MIGRATION.md` §3.4 / §5.7.
//!
//! # Backends
//!
//! - [`FoyerCacheEvictor`] (default) — `foyer-memory`'s sharded intrusive LRU /
//!   w-TinyLFU containers. Victim selection is O(1).
//! - [`MokaCacheEvictor`] — the previous `moka`-backed implementation, retained
//!   as the A/B baseline behind
//!   [`CacheEvictorBackend::Moka`](crate::config::CacheEvictorBackend). Still
//!   O(page count) per eviction; not intended for production use.

mod foyer_evictor;
mod moka_evictor;

pub use foyer_evictor::FoyerCacheEvictor;
pub use moka_evictor::MokaCacheEvictor;

use crate::cache::page_id::PageId;
use crate::config::{CacheEvictorBackend, CacheEvictorType};

/// Eviction policy abstraction.
///
/// Implementations must be internally synchronized (the manager calls these
/// from async contexts, potentially concurrently).
pub trait CacheEvictor: Send + Sync {
    /// Admit a newly cached page of `size` bytes.
    ///
    /// If admitting the page pushes the directory over its quota, the policy
    /// evicts until it fits and queues the evicted pages for
    /// [`CacheEvictor::drain_victims`]. The caller must reclaim those pages
    /// (index, byte accounting, and the page files on disk).
    ///
    /// A policy may evict the page that was just admitted — the caller must
    /// tolerate seeing `id` itself among the victims.
    fn on_add(&self, id: &PageId, size: u64);

    /// Record that a page was accessed (read hit).
    ///
    /// Must be O(1): this runs on the cache-hit hot path.
    fn on_access(&self, id: &PageId);

    /// Record that a page was removed by the caller (TTL expiry, invalidation,
    /// explicit delete). Pages dropped this way are *not* reported as victims.
    fn on_remove(&self, id: &PageId);

    /// Take the pages evicted since the last call.
    ///
    /// Returns an empty vector when the policy has evicted nothing, which is
    /// the common case while the directory is below quota.
    fn drain_victims(&self) -> Vec<PageId>;

    /// Number of pages currently tracked.
    fn len(&self) -> usize;

    /// `true` if no pages are tracked.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Build an evictor for the configured policy and per-directory byte quota.
///
/// `page_size` is only used to size the shard count: a directory holding few
/// pages is given few shards so that each shard still has a usable quota.
pub fn build_evictor(
    backend: CacheEvictorBackend,
    policy: CacheEvictorType,
    capacity: u64,
    page_size: u64,
) -> Box<dyn CacheEvictor> {
    match backend {
        CacheEvictorBackend::Foyer => Box::new(FoyerCacheEvictor::new(policy, capacity, page_size)),
        CacheEvictorBackend::Moka => Box::new(MokaCacheEvictor::new(policy, capacity)),
    }
}
