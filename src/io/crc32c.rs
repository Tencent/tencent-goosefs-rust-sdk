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

//! Castagnoli CRC32C for `CompleteFilePOptions.crc_value`.
//!
//! Matches Java `DataChecksum.Type.CRC32C` / Hadoop `PureJavaCrc32C`
//! (ITU-T V.42, reflected polynomial `0x82F63B78`). Implemented in-tree
//! (no crates.io `crc32c` dependency) so embedding this SDK in Lance does
//! not add a CRC crate that Lance does not already carry. Same pattern as
//! Guava-compatible murmur3 in `src/block/murmur3.rs`.
//!
//! `crc32c_append` takes the previous *output* CRC (after xorout), matching
//! the `crc32c` crate and Java `Checksum.update` running state:
//! `crc32c_append(crc32c(a), b) == crc32c(a||b)`.

/// Reflected Castagnoli polynomial (normal form `0x1EDC6F41`).
const POLY: u32 = 0x82f6_3b78;

const fn make_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ POLY;
            } else {
                crc >>= 1;
            }
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

const TABLE: [u32; 256] = make_table();

/// CRC32C of `data` from a fresh checksum (empty input is `0`).
#[inline]
#[allow(dead_code)] // one-shot form; the writer uses `crc32c_append`
pub(crate) fn crc32c(data: &[u8]) -> u32 {
    crc32c_append(0, data)
}

/// Continue a running CRC32C from a previous output value.
#[inline]
pub(crate) fn crc32c_append(crc: u32, data: &[u8]) -> u32 {
    let mut crc = !crc;
    for &b in data {
        let idx = ((crc as u8) ^ b) as usize;
        crc = TABLE[idx] ^ (crc >> 8);
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ITU-T V.42 / Castagnoli check vector. Java `PureJavaCrc32C` of
    /// `"123456789"` is the same `0xe3069283` sent as `crc_value`.
    #[test]
    fn matches_java_castagnoli_check_vector() {
        assert_eq!(crc32c(b"123456789"), 0xe3069283);
        assert_eq!(crc32c(b""), 0);
        let incremental = crc32c_append(crc32c(b"12345"), b"6789");
        assert_eq!(incremental, 0xe3069283);
    }

    /// Documented vector from the former `crc32c` crate.
    #[test]
    fn matches_crc32c_crate_hello_world() {
        assert_eq!(crc32c(b"Hello world!"), 0x7b98_e751);
    }

    #[test]
    fn multi_block_e2e_payload_golden() {
        let block = 64 * 1024;
        let payload: Vec<u8> = (0..block * 3 + 123).map(|i| (i % 251) as u8).collect();
        assert_eq!(crc32c(&payload), 0x9fa3_63fa);
    }
}
