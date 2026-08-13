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

//! Guava-compatible MurmurHash3 x64 128-bit (seed 0).
//!
//! Matches Java:
//! `Hashing.murmur3_128().newHasher().putLong(key).putInt(index).hash().asLong()`
//!
//! Ring / lookup key is `asLong()` = little-endian first 8 bytes of the 16-byte
//! digest = MurmurHash3_x64_128 `h1` interpreted as a signed Java `long`.
//!
//! Implemented in-tree (no crates.io mur3 dependency) so the SDK owns the
//! algorithm and can keep golden-vector parity with GooseFS
//! `ConsistentHashProvider`.

/// MurmurHash3_x64_128 over `data` with `seed`, returning `(h1, h2)`.
///
/// Matches SMHasher / Guava byte layout (little-endian blocks + tail).
#[inline]
pub(crate) fn murmurhash3_x64_128(data: &[u8], seed: u32) -> (u64, u64) {
    const C1: u64 = 0x87c3_7b91_1142_53d5;
    const C2: u64 = 0x4cf5_ad43_2745_937f;

    let len = data.len();
    let nblocks = len / 16;

    let mut h1 = u64::from(seed);
    let mut h2 = u64::from(seed);

    for i in 0..nblocks {
        let off = i * 16;
        let mut k1 = read_u64_le(data, off);
        let mut k2 = read_u64_le(data, off + 8);

        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
        h1 = h1.rotate_left(27);
        h1 = h1.wrapping_add(h2);
        h1 = h1.wrapping_mul(5).wrapping_add(0x52dc_e729);

        k2 = k2.wrapping_mul(C2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(C1);
        h2 ^= k2;
        h2 = h2.rotate_left(31);
        h2 = h2.wrapping_add(h1);
        h2 = h2.wrapping_mul(5).wrapping_add(0x3849_5ab5);
    }

    let tail = &data[nblocks * 16..];
    let n = tail.len(); // 0..=15
    let mut k1 = 0u64;
    let mut k2 = 0u64;

    // SMHasher fall-through switch for (len & 15).
    if n >= 9 {
        if n >= 15 {
            k2 ^= u64::from(tail[14]) << 48;
        }
        if n >= 14 {
            k2 ^= u64::from(tail[13]) << 40;
        }
        if n >= 13 {
            k2 ^= u64::from(tail[12]) << 32;
        }
        if n >= 12 {
            k2 ^= u64::from(tail[11]) << 24;
        }
        if n >= 11 {
            k2 ^= u64::from(tail[10]) << 16;
        }
        if n >= 10 {
            k2 ^= u64::from(tail[9]) << 8;
        }
        k2 ^= u64::from(tail[8]);
        k2 = k2.wrapping_mul(C2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(C1);
        h2 ^= k2;
    }
    if n >= 1 {
        if n >= 8 {
            k1 ^= u64::from(tail[7]) << 56;
        }
        if n >= 7 {
            k1 ^= u64::from(tail[6]) << 48;
        }
        if n >= 6 {
            k1 ^= u64::from(tail[5]) << 40;
        }
        if n >= 5 {
            k1 ^= u64::from(tail[4]) << 32;
        }
        if n >= 4 {
            k1 ^= u64::from(tail[3]) << 24;
        }
        if n >= 3 {
            k1 ^= u64::from(tail[2]) << 16;
        }
        if n >= 2 {
            k1 ^= u64::from(tail[1]) << 8;
        }
        k1 ^= u64::from(tail[0]);
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
    }

    h1 ^= len as u64;
    h2 ^= len as u64;
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    h1 = fmix64(h1);
    h2 = fmix64(h2);
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    (h1, h2)
}

/// Guava `murmur3_128().newHasher().putLong(key).putInt(index).hash().asLong()`.
#[inline]
pub(crate) fn murmur3_128_as_long_put_long_int(key: i64, index: i32) -> i64 {
    let mut buf = [0u8; 12];
    buf[..8].copy_from_slice(&key.to_le_bytes());
    buf[8..].copy_from_slice(&index.to_le_bytes());
    let (h1, _h2) = murmurhash3_x64_128(&buf, 0);
    h1 as i64
}

#[inline]
fn read_u64_le(data: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(data[off..off + 8].try_into().unwrap())
}

#[inline]
fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Guava / GooseFS `ConsistentHashProvider` golden vectors.
    #[test]
    fn guava_put_long_int_as_long_vectors() {
        assert_eq!(murmur3_128_as_long_put_long_int(1, 0), 8673688779682957586);
        assert_eq!(murmur3_128_as_long_put_long_int(42, 7), 1969424773395075097);
        assert_eq!(
            murmur3_128_as_long_put_long_int(100, 1),
            -8854681018154386345
        );
        assert_eq!(murmur3_128_as_long_put_long_int(-1, 0), 7097917686268154775);
        assert_eq!(murmur3_128_as_long_put_long_int(0, 0), -6568239567428591645);
        assert_eq!(
            murmur3_128_as_long_put_long_int(85429583872, 0),
            -5867849384608515022
        );
        assert_eq!(
            murmur3_128_as_long_put_long_int(8769479697893324776, 0),
            -6296241382218419536
        );
    }
}
