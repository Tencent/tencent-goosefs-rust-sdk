//! Range coalescing planner
//! (FLAMEGRAPH_OPTIMIZATION_PLAN §B2).
//!
//! # Rationale
//!
//! The Lance / DuckDB scan pattern issues many small `get_range` calls
//! that end up as one HTTP/2 stream each on the transport layer. In the
//! flame graph this shows up as ~40 % self time under
//! `<Arc<T> as ObjectStore>::get_ranges` — the H2 client's per-stream
//! bookkeeping cost dominates, not the actual bytes.
//!
//! When adjacent ranges are close (within `gap`), fetching one merged
//! larger range is much cheaper on H2 than N small ones. We over-read
//! at most `sum(gap_i)` bytes, which callers like Lance / DuckDB
//! already tolerate because they page-align.
//!
//! # This module
//!
//! Pure planner only. No I/O. Given a slice of `(offset, len)` inputs
//! plus policy knobs, it produces:
//!
//! - a small list of **merged fetches** to issue, and
//! - a mapping so each caller-visible output slice is byte-identical
//!   to a standalone `read_range(offset, len)` for the same input.
//!
//! I/O is done by the caller (`GoosefsFileReader::read_ranges_with_context`)
//! so this module is trivially unit-testable offline.
//!
//! # Guarantees
//!
//! - Output vector length equals input length. Order is preserved
//!   (i-th output byte range corresponds to the i-th input range).
//! - Every input range with `len == 0` is preserved as an empty slice
//!   in the output, without triggering any fetch.
//! - `max_bytes` is a **merge-time** cap: the planner will not merge
//!   two ranges when doing so would grow the fetch past `max_bytes`.
//!   A caller-requested range whose own length already exceeds
//!   `max_bytes` is served as one fetch of that size (splitting a
//!   single caller request would violate the byte-equivalence
//!   contract). This mirrors the "avoid pathological blow-ups" wording
//!   in FLAMEGRAPH_OPTIMIZATION_PLAN §B2.
//! - Overlapping inputs are handled correctly: the merged fetch spans
//!   the union of all overlapping ranges, and per-input slice indices
//!   still recover exactly the original bytes.

use std::cmp::max;

/// Where to find one caller-visible input range inside the merged
/// fetch it was assigned to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SliceMap {
    /// Index into [`CoalescePlan::fetches`] of the merged fetch that
    /// contains this range's bytes.
    pub fetch_index: usize,
    /// Byte offset **inside the merged fetch** at which this range
    /// starts.
    pub offset_in_fetch: usize,
    /// Length of this input range (== the corresponding input's `len`).
    pub len: usize,
}

/// One merged fetch to issue. `offset` and `len` are absolute file
/// coordinates, so callers can pass them straight to
/// `read_range_with_context(_, _, offset, len)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergedFetch {
    pub offset: u64,
    pub len: u64,
}

/// Result of the planner. Consumed by
/// `GoosefsFileReader::read_ranges_with_context`.
#[derive(Debug, Clone)]
pub struct CoalescePlan {
    /// Merged fetches, in ascending `offset` order.
    pub fetches: Vec<MergedFetch>,
    /// One entry per **input** range, in the caller's original order.
    /// `slices[i].fetch_index` is `usize::MAX` iff `input[i].1 == 0`
    /// (empty range — no fetch needed, output is empty `Bytes`).
    pub slices: Vec<SliceMap>,
    /// Sum of caller-requested bytes (Σ input.len). Cheap to compute
    /// here and useful for the wasted-bytes metric.
    pub total_input_bytes: u64,
    /// Sum of merged fetch bytes. Always `>= total_input_bytes`.
    pub total_fetch_bytes: u64,
}

impl CoalescePlan {
    /// Bytes fetched but not returned to the caller — i.e. the "waste"
    /// introduced by merging across gaps. Zero when
    /// `total_fetch_bytes == total_input_bytes`.
    pub fn wasted_bytes(&self) -> u64 {
        self.total_fetch_bytes
            .saturating_sub(self.total_input_bytes)
    }
}

/// Sentinel for "empty input, no fetch assigned". Chosen as
/// `usize::MAX` so any accidental use as an index panics loudly on
/// bounds check rather than silently reading fetch #0.
pub const NO_FETCH: usize = usize::MAX;

/// Build a coalesce plan for `ranges`.
///
/// - `gap`: two adjacent ranges are merged when the gap between them
///   is `<= gap`. `0` means "only merge exactly-adjacent or overlapping
///   ranges".
/// - `max_bytes`: hard upper bound on any single merged fetch. Values
///   `< 1` are clamped to `1` (a fetch cannot be empty).
///
/// Empty inputs (`len == 0`) are preserved with `fetch_index == NO_FETCH`.
pub fn plan(ranges: &[(u64, u64)], gap: u64, max_bytes: u64) -> CoalescePlan {
    let n = ranges.len();
    let max_bytes = max_bytes.max(1);
    let mut slices: Vec<SliceMap> = vec![
        SliceMap {
            fetch_index: NO_FETCH,
            offset_in_fetch: 0,
            len: 0,
        };
        n
    ];
    let total_input_bytes: u64 = ranges.iter().map(|(_, l)| *l).sum();

    // Attach original indices so we can restore caller order at the end.
    let mut sorted: Vec<(usize, u64, u64)> = ranges
        .iter()
        .enumerate()
        .filter(|(_, (_, l))| *l > 0)
        .map(|(i, &(o, l))| (i, o, l))
        .collect();
    if sorted.is_empty() {
        // All inputs are empty. `slices` already carries NO_FETCH, len=0.
        // Fill in `len` for the empty entries (all zero, but explicit).
        for (i, &(_, l)) in ranges.iter().enumerate() {
            slices[i].len = l as usize;
        }
        return CoalescePlan {
            fetches: Vec::new(),
            slices,
            total_input_bytes,
            total_fetch_bytes: 0,
        };
    }
    sorted.sort_by_key(|(_, off, _)| *off);

    let mut fetches: Vec<MergedFetch> = Vec::with_capacity(sorted.len());
    // Start the first merged fetch from the first sorted range.
    let (first_orig_idx, first_off, first_len) = sorted[0];
    fetches.push(MergedFetch {
        offset: first_off,
        len: first_len,
    });
    slices[first_orig_idx] = SliceMap {
        fetch_index: 0,
        offset_in_fetch: 0,
        len: first_len as usize,
    };

    for &(orig_idx, off, len) in &sorted[1..] {
        // Access via index to avoid holding a mutable borrow of `fetches`
        // while we read `fetches.len()` (the borrow checker rejects
        // `last_mut() + fetches.len()`, and the semantics are identical).
        let last_idx = fetches.len() - 1;
        let cur_offset = fetches[last_idx].offset;
        let cur_len = fetches[last_idx].len;
        let cur_end = cur_offset + cur_len;
        // Compute what the merged fetch WOULD be if we absorbed this range.
        let candidate_end = max(cur_end, off + len);
        let candidate_len = candidate_end - cur_offset;
        // Merge iff (a) adjacency-with-gap holds AND (b) merging does not
        // grow the fetch **past** the cap. The second half is
        // deliberately expressed as "candidate does not exceed the max
        // of `max_bytes` and the current fetch length" so that a caller
        // who explicitly requests one range larger than `max_bytes` is
        // still served (splitting a single caller request is not our
        // job) — the cap only prevents *merging* from ballooning the
        // request. This matches the "avoid pathological blow-ups"
        // wording in FLAMEGRAPH_OPTIMIZATION_PLAN §B2.
        let within_gap = off <= cur_end.saturating_add(gap);
        let effective_cap = max(max_bytes, cur_len);
        let within_cap = candidate_len <= effective_cap;
        if within_gap && within_cap {
            fetches[last_idx].len = candidate_len;
            slices[orig_idx] = SliceMap {
                fetch_index: last_idx,
                offset_in_fetch: (off - cur_offset) as usize,
                len: len as usize,
            };
        } else {
            fetches.push(MergedFetch { offset: off, len });
            let fetch_idx = fetches.len() - 1;
            slices[orig_idx] = SliceMap {
                fetch_index: fetch_idx,
                offset_in_fetch: 0,
                len: len as usize,
            };
        }
    }

    // Fill in `len` for the entries corresponding to zero-len inputs
    // (they never went through the sorted loop). `fetch_index` stays
    // `NO_FETCH`.
    for (i, &(_, l)) in ranges.iter().enumerate() {
        if l == 0 {
            slices[i].len = 0;
        }
    }

    let total_fetch_bytes: u64 = fetches.iter().map(|f| f.len).sum();
    CoalescePlan {
        fetches,
        slices,
        total_input_bytes,
        total_fetch_bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Trivial / degenerate inputs ─────────────────────────────────

    #[test]
    fn empty_input_produces_empty_plan() {
        let p = plan(&[], 64 * 1024, 4 * 1024 * 1024);
        assert!(p.fetches.is_empty());
        assert!(p.slices.is_empty());
        assert_eq!(p.total_input_bytes, 0);
        assert_eq!(p.total_fetch_bytes, 0);
        assert_eq!(p.wasted_bytes(), 0);
    }

    #[test]
    fn all_zero_len_inputs_produce_no_fetches_but_preserve_slots() {
        let p = plan(&[(0, 0), (100, 0), (7, 0)], 64 * 1024, 4 * 1024 * 1024);
        assert!(p.fetches.is_empty());
        assert_eq!(p.slices.len(), 3);
        for s in &p.slices {
            assert_eq!(s.fetch_index, NO_FETCH);
            assert_eq!(s.len, 0);
        }
    }

    #[test]
    fn zero_len_mixed_with_real_ranges_preserves_order_and_indices() {
        // input order: (100,0), (200,10), (0,0), (500,20)
        let inputs = &[(100, 0), (200, 10), (0, 0), (500, 20)];
        let p = plan(inputs, 0, 1024);
        assert_eq!(p.slices.len(), 4);
        assert_eq!(p.slices[0].fetch_index, NO_FETCH);
        assert_eq!(p.slices[0].len, 0);
        assert_eq!(p.slices[2].fetch_index, NO_FETCH);
        assert_eq!(p.slices[2].len, 0);
        // Real ranges are far apart (gap 0) so each gets its own fetch.
        assert_eq!(p.fetches.len(), 2);
        assert_eq!(p.slices[1].len, 10);
        assert_eq!(p.slices[3].len, 20);
    }

    // ── Merging semantics ─────────────────────────────────────────

    #[test]
    fn adjacent_ranges_merge_when_gap_permits() {
        // [0,10) then [10,20) — abut, gap=0, must merge.
        let p = plan(&[(0, 10), (10, 10)], 0, 1024);
        assert_eq!(p.fetches.len(), 1);
        assert_eq!(p.fetches[0], MergedFetch { offset: 0, len: 20 });
        assert_eq!(p.slices[0].offset_in_fetch, 0);
        assert_eq!(p.slices[1].offset_in_fetch, 10);
        assert_eq!(p.wasted_bytes(), 0);
    }

    #[test]
    fn ranges_within_gap_merge() {
        // [0,10) then [15,25) — gap = 5 <= 8, must merge; waste = 5.
        let p = plan(&[(0, 10), (15, 10)], 8, 1024);
        assert_eq!(p.fetches.len(), 1);
        assert_eq!(p.fetches[0], MergedFetch { offset: 0, len: 25 });
        assert_eq!(p.slices[1].offset_in_fetch, 15);
        assert_eq!(p.wasted_bytes(), 5);
    }

    #[test]
    fn ranges_beyond_gap_do_not_merge() {
        // [0,10) then [20,30) — gap = 10 > 4, must NOT merge.
        let p = plan(&[(0, 10), (20, 10)], 4, 1024);
        assert_eq!(p.fetches.len(), 2);
        assert_eq!(p.wasted_bytes(), 0);
    }

    #[test]
    fn overlapping_ranges_merge_and_map_correctly() {
        // [0,20) and [10,15) — overlap. Merged = [0,25).
        let p = plan(&[(0, 20), (10, 15)], 0, 1024);
        assert_eq!(p.fetches.len(), 1);
        assert_eq!(p.fetches[0], MergedFetch { offset: 0, len: 25 });
        // Both slices carve into the merged fetch at their own offsets:
        assert_eq!(p.slices[0].offset_in_fetch, 0);
        assert_eq!(p.slices[0].len, 20);
        assert_eq!(p.slices[1].offset_in_fetch, 10);
        assert_eq!(p.slices[1].len, 15);
    }

    // ── max_bytes cap ─────────────────────────────────────────────

    #[test]
    fn max_bytes_cap_forces_split_even_when_gap_permits() {
        // Two abutting 100-byte ranges; cap = 100 — the second must
        // start a fresh fetch even though gap == 0.
        let p = plan(&[(0, 100), (100, 100)], 1024, 100);
        assert_eq!(p.fetches.len(), 2);
        assert_eq!(p.fetches[0].len, 100);
        assert_eq!(
            p.fetches[1],
            MergedFetch {
                offset: 100,
                len: 100
            }
        );
    }

    #[test]
    fn max_bytes_zero_is_clamped_to_one() {
        // Passing max_bytes == 0 must not panic and must not stall
        // progress (each range gets its own fetch).
        let p = plan(&[(0, 5), (10, 5)], 1024, 0);
        assert_eq!(p.fetches.len(), 2);
    }

    #[test]
    fn cap_larger_than_gap_still_respects_gap() {
        // [0,10) and [1000,10) — cap huge but gap tiny — no merge.
        let p = plan(&[(0, 10), (1000, 10)], 100, 1_000_000);
        assert_eq!(p.fetches.len(), 2);
    }

    // ── Ordering + slice-map correctness ──────────────────────────

    #[test]
    fn caller_order_is_preserved_across_reorders() {
        // Feed input in reverse order; the plan sorts internally, but
        // slice_map[i] must still describe input[i].
        let inputs = &[(200, 10), (0, 20), (100, 5)];
        let p = plan(inputs, 0, 4096);
        // Two disjoint fetches (gap 0 between [0,20) and [100,5), and
        // between [100,5) and [200,10)). So 3 fetches total.
        assert_eq!(p.fetches.len(), 3);
        // slices[i].len must match inputs[i].1:
        for (i, &(_, l)) in inputs.iter().enumerate() {
            assert_eq!(p.slices[i].len as u64, l);
        }
        // slices[0] describes input (200,10); the merged fetch that
        // contains offset 200 is the one at fetches[i].offset==200.
        let s0 = p.slices[0];
        assert_eq!(p.fetches[s0.fetch_index].offset, 200);
        assert_eq!(s0.offset_in_fetch, 0);
        // slices[1] describes input (0,20).
        let s1 = p.slices[1];
        assert_eq!(p.fetches[s1.fetch_index].offset, 0);
        assert_eq!(s1.offset_in_fetch, 0);
    }

    // ── Byte-equivalence property test ────────────────────────────

    /// This is the strongest guarantee of the module: for any input
    /// set, "read the merged fetches then splice per SliceMap" must
    /// produce the SAME bytes as "read each input range independently".
    ///
    /// We simulate the underlying storage as a virtual byte at every
    /// offset (`byte(off) == off as u8 xor (off >> 8) as u8`) so any
    /// off-by-one in the mapping is detected.
    #[test]
    fn slice_mapping_is_byte_equivalent_on_random_inputs() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn simulated_byte(off: u64) -> u8 {
            let mut h = DefaultHasher::new();
            off.hash(&mut h);
            (h.finish() & 0xff) as u8
        }
        fn read_range(off: u64, len: u64) -> Vec<u8> {
            (0..len).map(|i| simulated_byte(off + i)).collect()
        }

        // Deterministic pseudo-random inputs (avoid adding a rand dep).
        let cases: &[Vec<(u64, u64)>] = &[
            vec![(0, 0)],
            vec![(0, 10), (10, 10), (30, 5)],
            vec![(100, 50), (0, 20), (200, 10), (155, 5)],
            vec![(0, 1), (2, 1), (4, 1), (6, 1), (8, 1)],
            vec![(0, 1000), (500, 10)],                         // overlap
            vec![(0, 100), (100, 100), (200, 100), (300, 100)], // chain
        ];

        for gap in [0u64, 1, 4, 64, 4096] {
            for max_bytes in [1u64, 8, 128, 4096, u64::MAX] {
                for input in cases {
                    let p = plan(input, gap, max_bytes);
                    // Simulate the merged reads.
                    let fetch_bufs: Vec<Vec<u8>> = p
                        .fetches
                        .iter()
                        .map(|f| read_range(f.offset, f.len))
                        .collect();
                    // Reconstruct per-input outputs via slice map.
                    for (i, &(off, len)) in input.iter().enumerate() {
                        let expected = read_range(off, len);
                        let s = p.slices[i];
                        let got: Vec<u8> = if s.fetch_index == NO_FETCH {
                            assert_eq!(len, 0, "NO_FETCH must only be used for empty ranges");
                            Vec::new()
                        } else {
                            let buf = &fetch_bufs[s.fetch_index];
                            buf[s.offset_in_fetch..s.offset_in_fetch + s.len].to_vec()
                        };
                        assert_eq!(
                            got, expected,
                            "byte mismatch at input #{i} = ({off},{len}), gap={gap}, cap={max_bytes}"
                        );
                    }
                    // Cap invariant: no merged fetch may exceed
                    // max(cap, largest single input assigned to it).
                    // (The cap only rejects merges that would grow a
                    // fetch past `cap`; a single caller-requested
                    // range larger than `cap` is served as-is.)
                    let cap = max_bytes.max(1);
                    let mut per_fetch_max_input: Vec<u64> = vec![0; p.fetches.len()];
                    for (i, &(_, l)) in input.iter().enumerate() {
                        let s = p.slices[i];
                        if s.fetch_index != NO_FETCH {
                            per_fetch_max_input[s.fetch_index] =
                                per_fetch_max_input[s.fetch_index].max(l);
                        }
                    }
                    for (fi, f) in p.fetches.iter().enumerate() {
                        let allowed = cap.max(per_fetch_max_input[fi]);
                        assert!(
                            f.len <= allowed,
                            "fetch {:?} exceeds cap-or-largest-input {}",
                            f,
                            allowed
                        );
                    }
                }
            }
        }
    }
}
