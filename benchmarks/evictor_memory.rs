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

//! Resident memory held by the evictor, per tracked page.
//!
//! The page cache keeps page *data* on SSD; both evictors keep only page
//! *identity* in memory. That makes the evictor's footprint `O(page count)`,
//! not `O(cache bytes)` — so the number that matters when sizing a host is
//! bytes *per page*, which is what this measures.
//!
//! It is worth measuring rather than estimating because the per-entry cost is
//! dominated by container internals (hash table slot, intrusive list links,
//! refcounts) rather than by the `PageId` itself, and because the `Arc<str>`
//! file id is shared between pages of the same file — so the answer depends on
//! how many files the pages are spread across.
//!
//! ```text
//! MEM_PAGES=1000000 MEM_FILES=1000 cargo run --release --example evictor_memory
//! ```
//!
//! Capacity is set high enough that nothing is evicted, so the measurement is
//! of a fully populated evictor.
//!
//! Measurement is by a counting global allocator rather than RSS: it reports
//! live heap bytes attributable to the evictor exactly, without allocator slack
//! or the resolution limit of `ps`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use goosefs_sdk::cache::evictor::build_evictor;
use goosefs_sdk::cache::PageId;
use goosefs_sdk::config::{CacheEvictorBackend, CacheEvictorType};

/// Tracks live heap bytes so the evictor's footprint can be read directly.
struct CountingAllocator;

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            LIVE_BYTES.fetch_add(new_size, Ordering::Relaxed);
            LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        new_ptr
    }
}

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

fn live_bytes() -> u64 {
    LIVE_BYTES.load(Ordering::Relaxed) as u64
}

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn fmt_bytes(b: u64) -> String {
    if b >= 1 << 20 {
        format!("{:.1} MiB", b as f64 / (1u64 << 20) as f64)
    } else {
        format!("{:.1} KiB", b as f64 / 1024.0)
    }
}

fn measure(
    backend: CacheEvictorBackend,
    policy: CacheEvictorType,
    pages: u64,
    files: u64,
    page_size: u64,
) -> u64 {
    // Pre-build the file ids so their allocations are not counted as evictor
    // overhead — the manager owns these strings either way.
    let file_ids: Vec<Arc<str>> = (0..files)
        .map(|f| Arc::from(format!("file-{f:08}").as_str()))
        .collect();

    // Capacity generous enough that nothing evicts: we want the footprint of a
    // full evictor, not of one at its eviction equilibrium.
    let capacity = pages.saturating_mul(page_size).saturating_mul(2);

    let before = live_bytes();
    let evictor = build_evictor(backend, policy, capacity, page_size);
    for i in 0..pages {
        let file_id = Arc::clone(&file_ids[(i % files) as usize]);
        evictor.on_add(&PageId::new(file_id, i / files), page_size);
    }
    let after = live_bytes();

    let used = after.saturating_sub(before);
    let tracked = evictor.len();
    assert_eq!(
        tracked as u64, pages,
        "evictor evicted during the measurement; raise the capacity headroom"
    );
    drop(evictor);
    used
}

fn main() {
    let pages: u64 = env_or("MEM_PAGES", 100_000);
    let files: u64 = env_or("MEM_FILES", 100);
    let page_size: u64 = env_or("MEM_PAGE_SIZE", 1024 * 1024);

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  Evictor resident memory per tracked page                    ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("  pages={pages}  files={files}  page_size={page_size}B");
    println!("  Page data lives on SSD; this is identity/metadata only.");
    println!();

    for backend in [CacheEvictorBackend::Foyer, CacheEvictorBackend::Moka] {
        for policy in [CacheEvictorType::Lfu, CacheEvictorType::Lru] {
            let used = measure(backend, policy, pages, files, page_size);
            let label = format!("{backend:?}/{policy:?}").to_lowercase();
            println!(
                "  {label:<12} heap={:>10}   {:>6.0} B/page   → at 1M pages: {}",
                fmt_bytes(used),
                used as f64 / pages as f64,
                fmt_bytes((used as f64 / pages as f64 * 1_000_000.0) as u64),
            );
        }
    }

    println!();
    println!("  Footprint scales with page *count*, so a small page_size against");
    println!("  a large quota is the case to watch: 100 GB of 64 KiB pages is");
    println!("  1.6M pages, 16x the metadata of the same quota in 1 MiB pages.");
}
