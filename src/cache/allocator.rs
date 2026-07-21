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

//! Multi-directory page allocation.
//!
//! When the cache spans several directories (e.g. multiple disks), an
//! [`Allocator`] decides which directory a page is stored in. Mirrors Java
//! `Allocator` / `AffinityHashAllocator`.

use std::hash::{Hash, Hasher};

use xxhash_rust::xxh3::Xxh3Default;

use crate::cache::page_id::PageId;

/// Directory selection strategy for multi-dir caches.
pub trait Allocator: Send + Sync {
    /// Choose the directory index (`0..num_dirs`) for `page_id`.
    fn allocate(&self, page_id: &PageId, num_dirs: usize) -> usize;
}

/// Affinity hash allocator: all pages of a file land in the same directory.
///
/// Hashing on `file_id` (not the page index) keeps a file's pages co-located,
/// which makes per-file invalidation and recovery cheaper and improves
/// locality. Mirrors Java `AffinityHashAllocator`.
#[derive(Debug, Default, Clone, Copy)]
pub struct HashAllocator;

impl HashAllocator {
    /// Create a new allocator.
    pub fn new() -> Self {
        Self
    }
}

impl Allocator for HashAllocator {
    fn allocate(&self, page_id: &PageId, num_dirs: usize) -> usize {
        if num_dirs <= 1 {
            return 0;
        }
        // xxHash3 (same hash Lance uses via `xxhash_rust::xxh3`): fast and
        // non-cryptographic. Directory selection needs no DoS resistance, so the
        // lightweight hasher beats `DefaultHasher`'s SipHash here. The whole
        // project is standardised on xxHash3.
        let mut h = Xxh3Default::default();
        page_id.file_id.hash(&mut h);
        (h.finish() % num_dirs as u64) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_dir_always_zero() {
        let a = HashAllocator::new();
        assert_eq!(a.allocate(&PageId::new("f", 0), 1), 0);
        assert_eq!(a.allocate(&PageId::new("f", 9), 1), 0);
    }

    #[test]
    fn same_file_same_dir() {
        let a = HashAllocator::new();
        let d0 = a.allocate(&PageId::new("file-x", 0), 8);
        let d1 = a.allocate(&PageId::new("file-x", 1), 8);
        let d2 = a.allocate(&PageId::new("file-x", 99), 8);
        assert_eq!(d0, d1);
        assert_eq!(d1, d2);
        assert!(d0 < 8);
    }

    #[test]
    fn distributes_across_dirs() {
        let a = HashAllocator::new();
        let dirs: std::collections::HashSet<usize> = (0..200)
            .map(|i| a.allocate(&PageId::new(format!("file-{i}"), 0), 8))
            .collect();
        // With 200 distinct files over 8 dirs, expect more than one bucket used.
        assert!(dirs.len() > 1, "allocator should spread files across dirs");
    }
}
