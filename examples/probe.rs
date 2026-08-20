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

//! Probe OpenDAL-style puts (prep + write tmp + finalize rename) plus a read,
//! and write Java-style reports to a log.
//!
//! Default write types: `CACHE_THROUGH` then `ASYNC_THROUGH`.
//!
//! ```text
//! GOOSEFS_MASTER_ADDR=host:port cargo run --example probe
//! PROBE_WRITE_TYPES=async_through cargo run --example probe
//! PROBE_WRITE_TYPES=cache_through,async_through cargo run --example probe
//! PROBE_BLOCKS=3 PROBE_BLOCK_SIZE=1048576 cargo run --example probe
//! ```
//!
//! Default payload is 3 × 1 MiB so Data Write prints Block #0/#1/#2. Raise
//! `PROBE_BLOCK_SIZE` (e.g. 67108864) to match the cluster 64 MiB block.
//!
//! The log file is truncated at start so leftover reports from earlier runs
//! are not mixed in. Expect standalone GetStatus / Remove / CreateDirectory /
//! Rename RPC reports around the write/read session reports.

use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::fs::options::{CreateFileOptions, DeleteOptions, OpenFileOptions};
use goosefs_sdk::fs::{BaseFileSystem, FileSystem};
use goosefs_sdk::WriteType;

#[tokio::main]
async fn main() -> goosefs_sdk::error::Result<()> {
    let mut config = GoosefsConfig::from_env();
    config.probe_enabled = true;
    let out = std::env::temp_dir().join("goosefs-probe.log");
    config.probe_output = Some(out.to_string_lossy().into_owned());
    // Truncate leftovers from previous runs (the writer is append-only).
    let _ = std::fs::remove_file(&out);

    let write_types = write_types_from_env()?;
    let (n_blocks, block_size) = probe_layout_from_env()?;

    println!("master        = {}", config.master_addr);
    println!("probe_output  = {}", out.display());
    println!(
        "write_types   = {}",
        write_types
            .iter()
            .map(|wt| wt.as_str().to_ascii_uppercase())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!(
        "payload       = {n_blocks} blocks × {} ({})",
        format_bytes(block_size),
        format_bytes(n_blocks as u64 * block_size)
    );

    let fs = BaseFileSystem::connect(config).await?;
    println!("process probe = {}", goosefs_sdk::probe::is_enabled());

    let parent = "/probe-example";
    let _ = fs.mkdir(parent, true).await;

    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis();
    for write_type in write_types {
        opendal_put(&fs, parent, stamp, write_type, n_blocks, block_size).await?;
    }

    println!("probe report written to {}", out.display());
    Ok(())
}

/// `PROBE_WRITE_TYPES=cache_through,async_through` (comma-separated).
/// Unset / empty → both, so ASYNC_THROUGH is always in the default run.
fn write_types_from_env() -> goosefs_sdk::error::Result<Vec<WriteType>> {
    let raw = std::env::var("PROBE_WRITE_TYPES").unwrap_or_default();
    let parsed: goosefs_sdk::error::Result<Vec<WriteType>> = if raw.trim().is_empty() {
        Ok(vec![WriteType::CacheThrough, WriteType::AsyncThrough])
    } else {
        raw.split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.parse::<WriteType>()
                    .map_err(|e| goosefs_sdk::error::Error::InvalidArgument {
                        message: format!(
                            "PROBE_WRITE_TYPES: {e} (use cache_through, async_through, …)"
                        ),
                    })
            })
            .collect()
    };
    let types = parsed?;
    if types.is_empty() {
        return Err(goosefs_sdk::error::Error::InvalidArgument {
            message: "PROBE_WRITE_TYPES is empty after parsing".into(),
        });
    }
    Ok(types)
}

/// `PROBE_BLOCKS` (default 3) × `PROBE_BLOCK_SIZE` (default 1 MiB).
fn probe_layout_from_env() -> goosefs_sdk::error::Result<(u32, u64)> {
    let n_blocks = env_u64("PROBE_BLOCKS", 3)?;
    let block_size = env_u64("PROBE_BLOCK_SIZE", 1024 * 1024)?;
    if n_blocks == 0 {
        return Err(goosefs_sdk::error::Error::InvalidArgument {
            message: "PROBE_BLOCKS must be > 0".into(),
        });
    }
    if block_size == 0 {
        return Err(goosefs_sdk::error::Error::InvalidArgument {
            message: "PROBE_BLOCK_SIZE must be > 0".into(),
        });
    }
    if n_blocks > u32::MAX as u64 {
        return Err(goosefs_sdk::error::Error::InvalidArgument {
            message: "PROBE_BLOCKS is too large".into(),
        });
    }
    Ok((n_blocks as u32, block_size))
}

fn env_u64(name: &str, default: u64) -> goosefs_sdk::error::Result<u64> {
    match std::env::var(name) {
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(e) => Err(goosefs_sdk::error::Error::InvalidArgument {
            message: format!("{name}: {e}"),
        }),
        Ok(raw) if raw.trim().is_empty() => Ok(default),
        Ok(raw) => {
            raw.trim()
                .parse::<u64>()
                .map_err(|e| goosefs_sdk::error::Error::InvalidArgument {
                    message: format!("{name}: {e}"),
                })
        }
    }
}

fn format_bytes(n: u64) -> String {
    const MIB: u64 = 1024 * 1024;
    const KIB: u64 = 1024;
    if n % MIB == 0 {
        format!("{} MiB", n / MIB)
    } else if n % KIB == 0 {
        format!("{} KiB", n / KIB)
    } else {
        format!("{n} B")
    }
}

/// Mirror OpenDAL atomic write: prep final path → write tmp → rename → read.
async fn opendal_put(
    fs: &BaseFileSystem,
    parent: &str,
    stamp: u128,
    write_type: WriteType,
    n_blocks: u32,
    block_size: u64,
) -> goosefs_sdk::error::Result<()> {
    let label = write_type.as_str().to_ascii_uppercase();
    println!("\n=== {label} ===");

    let tmp_path = format!(
        "{parent}/.opendal.tmp.{stamp}.{}.probe.dat",
        write_type.as_str()
    );
    let path = format!("{parent}/probe-{stamp}-{}.dat", write_type.as_str());

    match fs.get_status(&path).await {
        Ok(_) => println!("get_status {path} = exists"),
        Err(e) => println!("get_status {path} = {e}"),
    }

    let total = n_blocks as u64 * block_size;
    {
        let mut opts = CreateFileOptions::with_write_type(write_type);
        opts.recursive = true;
        opts.block_size_bytes = Some(block_size as i64);
        let mut writer = fs.create_file(&tmp_path, opts).await?;
        // Fill each GooseFS block with a repeating pattern so Block #0..#(n-1)
        // show up as separate Data Write trees in the probe report.
        let chunk_len = (block_size as usize).min(1024 * 1024).max(1);
        let chunk = vec![0xAB; chunk_len];
        let mut written = 0u64;
        while written < total {
            let n = ((total - written) as usize).min(chunk.len());
            writer.write(&chunk[..n]).await?;
            written += n as u64;
        }
        writer.close().await?;
        println!(
            "wrote {written} bytes ({n_blocks} × {}) to {tmp_path}",
            format_bytes(block_size)
        );
    }

    match fs.delete(&path, DeleteOptions::default()).await {
        Ok(()) => println!("delete {path} = ok"),
        Err(e) => println!("delete {path} = {e}"),
    }
    match fs.get_status(&path).await {
        Ok(_) => println!("get_status {path} = exists"),
        Err(e) => println!("get_status {path} = {e}"),
    }
    fs.mkdir(parent, true).await?;
    println!("create_directory {parent} = ok");

    fs.rename(&tmp_path, &path).await?;
    println!("renamed {tmp_path} -> {path}");

    {
        let mut reader = fs.open_file(&path, OpenFileOptions::default()).await?;
        let mut buf = vec![0u8; (block_size as usize).min(1024 * 1024).max(1)];
        let mut n = 0u64;
        loop {
            let got = reader.read(&mut buf).await?;
            if got == 0 {
                break;
            }
            n += got as u64;
        }
        reader.close().await?;
        println!("read {n} bytes back from {path}");
    }

    let _ = fs.delete(&path, DeleteOptions::default()).await;
    let _ = fs.delete(&tmp_path, DeleteOptions::default()).await;
    Ok(())
}
