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

//! Live-cluster check: Rust `select_workers` vs Java `getWorkersByBlockId`.
//!
//! ```bash
//! GOOSEFS_AUTH_TYPE=simple cargo run --example verify_java_hash_parity
//! ```

use goosefs_sdk::auth::AuthType;
use goosefs_sdk::block::WorkerRouter;
use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::Result;
use goosefs_sdk::io::{GoosefsFileReader, GoosefsFileWriter};

fn master_addr() -> String {
    std::env::var("GOOSEFS_MASTER_ADDR").unwrap_or_else(|_| "127.0.0.1:9200".to_string())
}

fn config() -> GoosefsConfig {
    let mut cfg = GoosefsConfig::new(master_addr());
    cfg.auth_type = match std::env::var("GOOSEFS_AUTH_TYPE") {
        Ok(s) => s.parse().unwrap_or(AuthType::Simple),
        Err(_) => AuthType::Simple,
    };
    cfg.auth_username = std::env::var("GOOSEFS_AUTH_USERNAME").unwrap_or_else(|_| {
        std::env::var("USER")
            .or_else(|_| std::env::var("LOGNAME"))
            .unwrap_or_else(|_| "forwardxu".to_string())
    });
    cfg
}

fn json_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = config();
    println!(
        "master={} auth_type={:?} user={}",
        master_addr(),
        cfg.auth_type,
        cfg.auth_username
    );

    let ctx = FileSystemContext::connect(cfg).await?;
    let workers = match ctx.acquire_worker_manager() {
        Some(wm) => wm.get_worker_info_list().await?,
        None => {
            eprintln!("FAIL: WorkerManager unavailable");
            std::process::exit(1);
        }
    };
    println!("workers={}", workers.len());
    if workers.is_empty() {
        eprintln!("FAIL: no workers registered");
        std::process::exit(1);
    }

    let mut workers_json = String::from("{\"workers\":[\n");
    for (i, w) in workers.iter().enumerate() {
        let addr = w.address.as_ref();
        let host = addr.and_then(|a| a.host.as_deref()).unwrap_or("");
        let port = addr.and_then(|a| a.rpc_port).unwrap_or(0);
        let vn = w.virtual_node_num.unwrap_or(200);
        let id = w.id.unwrap_or(0);
        println!("  id={id} host={host} rpc_port={port} virtual_node_num={vn}");
        if i > 0 {
            workers_json.push_str(",\n");
        }
        workers_json.push_str(&format!(
            "  {{\"id\":{id},\"host\":\"{}\",\"rpc_port\":{port},\"virtual_node_num\":{vn}}}",
            json_escape(host)
        ));
    }
    workers_json.push_str("\n]}\n");
    std::fs::write("/tmp/goosefs_hash_workers.json", &workers_json).expect("write workers json");
    println!("wrote /tmp/goosefs_hash_workers.json");

    let router = WorkerRouter::new();
    router.update_workers(workers).await;

    let block_ids: Vec<i64> = (0..32)
        .map(|i| 1_000_000i64 + i * 17)
        .chain([1, 42, 99, 1234567890, -1])
        .collect();

    let mut sel_json = String::from("{\n");
    println!("\nRust select_workers(block_id, count=3):");
    for (i, &bid) in block_ids.iter().enumerate() {
        let selected = router.select_workers(bid, 3).await?;
        let ids: Vec<String> = selected
            .iter()
            .filter_map(|w| w.id.map(|id| id.to_string()))
            .collect();
        println!("  block_id={bid} -> [{}]", ids.join(", "));
        if i > 0 {
            sel_json.push_str(",\n");
        }
        sel_json.push_str(&format!("  \"{bid}\":[{}]", ids.join(",")));
    }
    sel_json.push_str("\n}\n");
    std::fs::write("/tmp/goosefs_hash_rust_selections.json", &sel_json)
        .expect("write selections json");
    println!("wrote /tmp/goosefs_hash_rust_selections.json");

    println!("\nE2E Rust write/read (hash routing, check_block_replicas=0)...");
    let path = format!(
        "/tmp/rust_hash_parity_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    let payload: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();

    let written = GoosefsFileWriter::write_file_with_context(ctx.clone(), &path, &payload).await?;
    println!("  wrote {written} bytes to {path}");

    let got = GoosefsFileReader::read_file_with_context(ctx.clone(), &path).await?;
    if got.as_ref() != payload.as_slice() {
        eprintln!(
            "FAIL: read mismatch (wrote {}, got {})",
            payload.len(),
            got.len()
        );
        std::process::exit(1);
    }
    println!("  read OK ({} bytes match)", got.len());

    let _ = ctx.acquire_master().delete(&path, false).await;
    println!("\nRust-side checks passed. Run Java comparator next.");
    Ok(())
}
