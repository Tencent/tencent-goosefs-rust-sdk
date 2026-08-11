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

//! Local-cluster smoke test for CheckBlocks location enrichment.
//!
//! Usage:
//!   cargo run --example verify_checkblocks_locations

use goosefs_sdk::block::router::WorkerRouterView;
use goosefs_sdk::block::{enrich_file_block_locations, ensure_block_ids_from_file_block_infos};
use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::Result;
use goosefs_sdk::fs::{BaseFileSystem, FileSystem};
use goosefs_sdk::io::{GoosefsFileReader, GoosefsFileWriter};
use goosefs_sdk::proto::grpc::file::CreateFilePOptions;
use goosefs_sdk::WritePType;

const PATH: &str = "/rust-sdk-verify/checkblocks-locations.bin";

fn summarize_locations(fi: &goosefs_sdk::proto::grpc::file::FileInfo) -> String {
    if fi.file_block_infos.is_empty() {
        return "file_block_infos=[]".to_string();
    }
    fi.file_block_infos
        .iter()
        .map(|fbi| {
            let bi = fbi.block_info.as_ref();
            let id = bi.and_then(|b| b.block_id).unwrap_or(-1);
            let locs = bi.map(|b| b.locations.as_slice()).unwrap_or(&[]);
            let hosts: Vec<String> = locs
                .iter()
                .map(|l| {
                    format!(
                        "wid={:?}/{}",
                        l.worker_id,
                        l.worker_address
                            .as_ref()
                            .and_then(|a| a.host.clone())
                            .unwrap_or_else(|| "?".into())
                    )
                })
                .collect();
            format!("block {id}: locations={} {:?}", locs.len(), hosts)
        })
        .collect::<Vec<_>>()
        .join("\n  ")
}

fn location_count(fi: &goosefs_sdk::proto::grpc::file::FileInfo) -> usize {
    fi.file_block_infos
        .iter()
        .map(|fbi| {
            fbi.block_info
                .as_ref()
                .map(|b| b.locations.len())
                .unwrap_or(0)
        })
        .sum()
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== CheckBlocks location enrichment verification ===\n");

    let master_addr =
        std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".into());
    let config = GoosefsConfig::new(&master_addr).with_check_block_replicas(1);
    let ctx = FileSystemContext::connect(config.clone()).await?;
    println!("connected to Master {}", config.master_addr);

    let master = ctx.acquire_master();
    let _ = master.create_directory("/rust-sdk-verify", true).await;
    let _ = master.delete(PATH, false).await;

    let payload: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
    println!("writing {PATH} ({} bytes, MUST_CACHE)...", payload.len());
    let opts = CreateFilePOptions {
        recursive: Some(true),
        write_type: Some(WritePType::MustCache as i32),
        ..Default::default()
    };
    GoosefsFileWriter::write_file_with_context_and_options(ctx.clone(), PATH, &payload, Some(opts))
        .await?;
    println!("write ok");

    // ── A) Master-only GetStatus (no CheckBlocks) ──────────────────────────
    let mut master_only = master.get_status(PATH).await?;
    ensure_block_ids_from_file_block_infos(&mut master_only);
    let master_loc_count = location_count(&master_only);
    println!("\n[A] Master GetStatus (no CheckBlocks):");
    println!(
        "  inGooseFSPercentage={:?}\n  {}",
        master_only.in_goose_fs_percentage,
        summarize_locations(&master_only)
    );

    // ── B) Explicit CheckBlocks enrichment ─────────────────────────────────
    let mut enriched = master_only.clone();
    let router = WorkerRouterView::from_shared(&ctx.acquire_router());
    let pool = ctx.acquire_worker_pool();
    enrich_file_block_locations(
        &mut enriched,
        &router,
        Some(&pool),
        ctx.config(),
        ctx.config().check_block_replicas.max(1) as usize,
    )
    .await?;
    let enriched_loc_count = location_count(&enriched);
    println!("\n[B] After CheckBlocks enrichment (check_block_replicas=1):");
    println!(
        "  inGooseFSPercentage={:?}\n  {}",
        enriched.in_goose_fs_percentage,
        summarize_locations(&enriched)
    );

    // ── C) BaseFileSystem::get_status (wired enrichment) ───────────────────
    let fs = BaseFileSystem::from_context(ctx.clone());
    let status = fs.get_status(PATH).await?;
    let open_loc_count: usize = status
        .block_infos()
        .values()
        .map(|fbi| {
            fbi.block_info
                .as_ref()
                .map(|b| b.locations.len())
                .unwrap_or(0)
        })
        .sum();
    println!("\n[C] BaseFileSystem::get_status locations total = {open_loc_count}");
    for (id, fbi) in status.block_infos() {
        let n = fbi
            .block_info
            .as_ref()
            .map(|b| b.locations.len())
            .unwrap_or(0);
        let host = fbi
            .block_info
            .as_ref()
            .and_then(|b| b.locations.first())
            .and_then(|l| l.worker_address.as_ref())
            .and_then(|a| a.host.clone())
            .unwrap_or_else(|| "-".into());
        println!("  block {id}: locations={n} first_host={host}");
    }

    // ── D) Read via FileReader (also enriches on open) ─────────────────────
    let got = GoosefsFileReader::read_file_with_context(ctx.clone(), PATH).await?;
    let read_ok = got.as_ref() == payload.as_slice();
    println!(
        "\n[D] Read round-trip: {} (got {} bytes)",
        if read_ok { "OK" } else { "MISMATCH" },
        got.len()
    );

    println!("\n=== Verdict ===");
    if enriched_loc_count > 0 && open_loc_count > 0 && read_ok {
        println!(
            "PASS: CheckBlocks filled locations (master={master_loc_count} → enriched={enriched_loc_count}, get_status={open_loc_count})"
        );
        if master_loc_count == 0 {
            println!("  Master had empty locations — enrichment is doing real work.");
        }
        Ok(())
    } else {
        println!(
            "FAIL: master={master_loc_count} enriched={enriched_loc_count} get_status={open_loc_count} read_ok={read_ok}"
        );
        std::process::exit(1);
    }
}
