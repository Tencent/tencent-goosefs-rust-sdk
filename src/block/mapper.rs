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

//! Block mapper: converts file-level byte ranges into block-level read plans.
//!
//! Goosefs splits files into fixed-size blocks. A range read like
//! `read(offset=70MB, length=100MB)` on a file with 64MB blocks
//! must be split into multiple `ReadBlock` RPCs to different blocks.
//!
//! ```text
//! File:  [--- Block 0 (64MB) ---][--- Block 1 (64MB) ---][--- Block 2 (64MB) ---]
//!                      ^------- read(70MB, 100MB) ----------^
//! Plan:
//!   Block 1: offset_in_block=6MB, length=58MB
//!   Block 2: offset_in_block=0,   length=42MB
//! ```

use crate::proto::grpc::file::FileInfo;

/// A single block-level read segment computed by the mapper.
#[derive(Debug, Clone)]
pub struct BlockReadPlan {
    /// Goosefs block ID.
    pub block_id: i64,
    /// Index of this block in the file's block list.
    pub block_index: u64,
    /// Byte offset within this block to start reading.
    pub offset_in_block: u64,
    /// Number of bytes to read from this block.
    pub length: u64,
}

/// Maps file-level byte ranges to block-level read plans.
pub struct BlockMapper;

impl BlockMapper {
    /// Split a file-level `[offset, offset+length)` range into block-level
    /// read plans based on the file's block size and block IDs.
    ///
    /// # Arguments
    /// - `file_info` — The `FileInfo` containing `blockSizeBytes` and `blockIds`.
    /// - `offset` — Start byte offset in the file.
    /// - `length` — Number of bytes to read.
    ///
    /// # Returns
    /// A vector of `BlockReadPlan` entries, one per block touched.
    pub fn plan_read(file_info: &FileInfo, offset: u64, length: u64) -> Vec<BlockReadPlan> {
        let block_size = file_info.block_size_bytes.unwrap_or(64 * 1024 * 1024) as u64;
        let file_length = file_info.length.unwrap_or(0) as u64;

        if block_size == 0 || length == 0 || offset >= file_length {
            return Vec::new();
        }

        // Clamp to actual file length
        let effective_length = std::cmp::min(length, file_length.saturating_sub(offset));
        if effective_length == 0 {
            return Vec::new();
        }

        let mut plans = Vec::new();
        let mut remaining = effective_length;
        let mut current_offset = offset;

        while remaining > 0 {
            let block_index = current_offset / block_size;
            let offset_in_block = current_offset % block_size;
            let bytes_in_block = std::cmp::min(remaining, block_size - offset_in_block);

            let block_id = file_info
                .block_ids
                .get(block_index as usize)
                .copied()
                .unwrap_or(-1);

            plans.push(BlockReadPlan {
                block_id,
                block_index,
                offset_in_block,
                length: bytes_in_block,
            });

            current_offset += bytes_in_block;
            remaining -= bytes_in_block;
        }

        plans
    }

    /// Compute the block-level write plan for appending `length` bytes
    /// starting at the given file offset. Used when writing new data.
    pub fn plan_write(block_size: u64, file_offset: u64, length: u64) -> Vec<BlockWritePlan> {
        if block_size == 0 || length == 0 {
            return Vec::new();
        }

        let mut plans = Vec::new();
        let mut remaining = length;
        let mut current_offset = file_offset;

        while remaining > 0 {
            let block_index = current_offset / block_size;
            let offset_in_block = current_offset % block_size;
            let bytes_in_block = std::cmp::min(remaining, block_size - offset_in_block);

            plans.push(BlockWritePlan {
                block_index,
                offset_in_block,
                length: bytes_in_block,
            });

            current_offset += bytes_in_block;
            remaining -= bytes_in_block;
        }

        plans
    }
}

/// A single block-level write segment.
#[derive(Debug, Clone)]
pub struct BlockWritePlan {
    /// Index of this block in the file's block list.
    pub block_index: u64,
    /// Byte offset within this block to start writing.
    pub offset_in_block: u64,
    /// Number of bytes to write to this block.
    pub length: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_file_info(block_size: i64, file_length: i64, block_ids: Vec<i64>) -> FileInfo {
        FileInfo {
            block_size_bytes: Some(block_size),
            length: Some(file_length),
            block_ids,
            ..Default::default()
        }
    }

    #[test]
    fn test_single_block_full_read() {
        let info = make_file_info(64 * 1024 * 1024, 32 * 1024 * 1024, vec![100]);
        let plans = BlockMapper::plan_read(&info, 0, 32 * 1024 * 1024);
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].block_id, 100);
        assert_eq!(plans[0].block_index, 0);
        assert_eq!(plans[0].offset_in_block, 0);
        assert_eq!(plans[0].length, 32 * 1024 * 1024);
    }

    #[test]
    fn test_cross_block_read() {
        let block_size = 64 * 1024 * 1024_u64;
        let info = make_file_info(
            block_size as i64,
            (block_size * 3) as i64,
            vec![100, 200, 300],
        );

        // Read 100MB starting at offset 70MB → crosses block 1 and block 2
        let offset = 70 * 1024 * 1024;
        let length = 100 * 1024 * 1024;
        let plans = BlockMapper::plan_read(&info, offset, length);

        assert_eq!(plans.len(), 2);

        // Block 1: offset_in_block = 70MB - 64MB = 6MB, length = 64MB - 6MB = 58MB
        assert_eq!(plans[0].block_id, 200);
        assert_eq!(plans[0].block_index, 1);
        assert_eq!(plans[0].offset_in_block, 6 * 1024 * 1024);
        assert_eq!(plans[0].length, 58 * 1024 * 1024);

        // Block 2: offset_in_block = 0, length = 100MB - 58MB = 42MB
        assert_eq!(plans[1].block_id, 300);
        assert_eq!(plans[1].block_index, 2);
        assert_eq!(plans[1].offset_in_block, 0);
        assert_eq!(plans[1].length, 42 * 1024 * 1024);
    }

    #[test]
    fn test_read_clamped_to_file_length() {
        let info = make_file_info(64 * 1024 * 1024, 10 * 1024 * 1024, vec![100]);
        let plans = BlockMapper::plan_read(&info, 0, 100 * 1024 * 1024);
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].length, 10 * 1024 * 1024);
    }

    #[test]
    fn test_read_past_eof() {
        let info = make_file_info(64 * 1024 * 1024, 10 * 1024 * 1024, vec![100]);
        let plans = BlockMapper::plan_read(&info, 10 * 1024 * 1024, 100);
        assert!(plans.is_empty());
    }

    #[test]
    fn test_zero_length_read() {
        let info = make_file_info(64 * 1024 * 1024, 100, vec![100]);
        let plans = BlockMapper::plan_read(&info, 0, 0);
        assert!(plans.is_empty());
    }

    #[test]
    fn test_write_plan_cross_block() {
        let block_size = 64 * 1024 * 1024;
        let plans = BlockMapper::plan_write(block_size, 60 * 1024 * 1024, 10 * 1024 * 1024);

        assert_eq!(plans.len(), 2);
        // First part: 4MB in block 0
        assert_eq!(plans[0].block_index, 0);
        assert_eq!(plans[0].offset_in_block, 60 * 1024 * 1024);
        assert_eq!(plans[0].length, 4 * 1024 * 1024);
        // Second part: 6MB in block 1
        assert_eq!(plans[1].block_index, 1);
        assert_eq!(plans[1].offset_in_block, 0);
        assert_eq!(plans[1].length, 6 * 1024 * 1024);
    }
}
