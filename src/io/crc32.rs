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

//! IEEE CRC32 for `CompleteFilePOptions.crc_value` when
//! `goosefs.user.streaming.writer.checksum.type=CRC32`.
//!
//! Matches Java `DataChecksum.Type.CRC32` / `java.util.zip.CRC32`
//! (ISO 3309 / ITU-T V.42 IEEE polynomial, reflected `0xEDB88320`).
//! In-tree so embedding this SDK in Lance does not add a CRC crate.

/// Reflected IEEE CRC-32 polynomial (normal form `0x04C11DB7`).
const POLY: u32 = 0xedb8_8320;

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

/// IEEE CRC32 of `data` from a fresh checksum (empty input is `0`).
#[inline]
#[allow(dead_code)] // one-shot form; the writer uses `crc32_append`
pub(crate) fn crc32(data: &[u8]) -> u32 {
    crc32_append(0, data)
}

/// Continue a running IEEE CRC32 from a previous output value.
#[inline]
pub(crate) fn crc32_append(crc: u32, data: &[u8]) -> u32 {
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

    /// ITU-T V.42 IEEE check vector. Java `java.util.zip.CRC32` of
    /// `"123456789"` is the same `0xcbf43926` sent as `crc_value`.
    #[test]
    fn matches_java_ieee_check_vector() {
        assert_eq!(crc32(b"123456789"), 0xcbf4_3926);
        assert_eq!(crc32(b""), 0);
        let incremental = crc32_append(crc32(b"12345"), b"6789");
        assert_eq!(incremental, 0xcbf4_3926);
        assert_ne!(crc32(b"123456789"), 0xe306_9283);
    }

    #[test]
    fn multi_block_e2e_payload_golden() {
        let block = 64 * 1024;
        let payload: Vec<u8> = (0..block * 3 + 123).map(|i| (i % 251) as u8).collect();
        assert_eq!(crc32(&payload), 0x9c20_a08f);
    }
}
