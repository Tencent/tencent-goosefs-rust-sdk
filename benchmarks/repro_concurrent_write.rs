//! Reproducer for the opendal `test_writer_write_with_concurrent` failure:
//!
//!   "WriteBlock server error for block_id=...: status: Internal,
//!    message: \"The file length is inconsistent with the amount of
//!    data that has been written\""
//!
//! ## Key reproduction conditions (derived from 1.log analysis)
//!
//! libtest_mimic runs behavior tests in parallel by default. From the log:
//!   15:40:19.713 path=A: write started
//!   15:40:20.116 path=B: write started   <-- before A has closed
//!   15:40:20.235 path=A: write close failed
//!
//! In other words: **multiple writers sharing the same FileSystemContext
//! and concurrently writing different files** is what triggers the
//! server-side block-length accounting corruption. Running a single
//! writer serially cannot reproduce it.
//!
//! Reproduction conditions:
//!   * Same FileSystemContext
//!   * write_type = MUST_CACHE
//!   * N concurrent tasks, each performing 3 writes (5..6 MiB random size),
//!     followed by close
//!   * Paths placed at the root directory (matches opendal `op.write(uuid, ...)`)
//!
//! Usage:
//!   cargo run --example repro_concurrent_write
//!   cargo run --example repro_concurrent_write -- 20 8
//!         # 20 rounds, 8 concurrent tasks per round
//!
//! Environment variables (controlled experiments):
//!   * SHARED=0|1   Whether each task shares the FileSystemContext (default 1)
//!   * ALIGN=0|1    Whether to strictly align data to 1 MiB (default 0;
//!                  with ALIGN=1, condition 2 is invalidated, expecting 0 failures)
//!   * WP=must_cache|cache_through|through|async_through
//!                  WritePType mode comparison (default must_cache):
//!                    - must_cache    cache stream only (GOOSEFS_BLOCK), hits the bug
//!                    - cache_through cache + UFS dual stream, hits the bug with doubled load
//!                    - through       UFS stream only (UFS_FILE), fully bypasses the bug
//!                    - async_through cache stream + async persist after close
//!
//! Prerequisite: a local GooseFS cluster is running with Master on 127.0.0.1:9200.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use goosefs_sdk::config::GoosefsConfig;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::error::Result;
use goosefs_sdk::io::GoosefsFileWriter;
use goosefs_sdk::proto::grpc::file::CreateFilePOptions;
use goosefs_sdk::WritePType;
use rand::Rng;

const MIN_SIZE: usize = 5 * 1024 * 1024;
const MAX_SIZE: usize = 6 * 1024 * 1024;
// Enabled via the ALIGN=1 environment variable: total written bytes are
// strictly aligned to CHUNK_SIZE.
// Default (ALIGN=0) mimics the opendal test using random sizes with a
// partial final chunk.
const CHUNK_SIZE: usize = 1024 * 1024;

fn gen_bytes_with_range(min: usize, max: usize) -> Vec<u8> {
    let mut rng = rand::rng();
    let mut size = rng.random_range(min..max);
    let align = std::env::var("ALIGN").ok().as_deref() == Some("1");
    if align {
        size -= size % CHUNK_SIZE;
        if size < CHUNK_SIZE {
            size = CHUNK_SIZE;
        }
    }
    let mut buf = vec![0u8; size];
    rng.fill(&mut buf[..]);
    buf
}

fn make_root_path() -> String {
    // Equivalent to opendal's TEST_FIXTURE.new_file_path(): a pure UUID at the root.
    // We avoid pulling in a uuid dependency here and craft a unique name
    // from nanos + counter + pid.
    static C: AtomicUsize = AtomicUsize::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let n = C.fetch_add(1, Ordering::Relaxed);
    format!("/repro-{}-{}-{}.bin", std::process::id(), n, nanos)
}

/// Resolves the WP environment variable into a WritePType. Defaults to MUST_CACHE.
fn resolve_write_ptype() -> (WritePType, &'static str) {
    match std::env::var("WP")
        .ok()
        .map(|s| s.to_ascii_lowercase())
        .as_deref()
    {
        Some("cache_through") | Some("cache-through") => {
            (WritePType::CacheThrough, "CACHE_THROUGH")
        }
        Some("through") => (WritePType::Through, "THROUGH"),
        Some("async_through") | Some("async-through") => {
            (WritePType::AsyncThrough, "ASYNC_THROUGH")
        }
        Some("must_cache") | Some("must-cache") | None => (WritePType::MustCache, "MUST_CACHE"),
        Some(other) => {
            panic!("unknown WP={other}; expected must_cache|cache_through|through|async_through")
        }
    }
}

/// Mimics the tmp_path of the opendal goosefs writer.
fn make_tmp_path(path: &str) -> String {
    let (dir, base) = match path.rfind('/') {
        Some(idx) => (&path[..idx], &path[idx + 1..]),
        None => ("", path),
    };
    let pid = std::process::id();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    if dir.is_empty() {
        // path looks like "/foo"; rfind hits the leading '/', so dir=="" and base=="foo".
        format!("/.opendal.tmp.{pid}.{nanos}.{base}")
    } else {
        format!("{dir}/.opendal.tmp.{pid}.{nanos}.{base}")
    }
}

/// A single worker: simulates one complete flow of opendal
/// `test_writer_write_with_concurrent`:
///   - Write 3 chunks on tmp_path
///   - close
///   - rename to the final path
///
/// If `shared_ctx` is None, the task builds its own ctx (not shared with others).
async fn one_writer_round(
    shared_ctx: Option<Arc<FileSystemContext>>,
    idx: usize,
    write_ptype: WritePType,
) -> std::result::Result<(usize, usize), (usize, String)> {
    let ctx = match shared_ctx {
        Some(c) => c,
        None => {
            let cfg = GoosefsConfig::new("127.0.0.1:9200");
            match FileSystemContext::connect(cfg).await {
                Ok(c) => c,
                Err(e) => return Err((idx, format!("private connect failed: {e}"))),
            }
        }
    };
    let path = make_root_path();
    let tmp = make_tmp_path(&path);

    let options = CreateFilePOptions {
        write_type: Some(write_ptype as i32),
        recursive: Some(true),
        ..Default::default()
    };

    let mut writer =
        match GoosefsFileWriter::create_with_context(ctx.clone(), &tmp, Some(options)).await {
            Ok(w) => w,
            Err(e) => return Err((idx, format!("create_with_context failed: {e}"))),
        };

    let a = gen_bytes_with_range(MIN_SIZE, MAX_SIZE);
    let b = gen_bytes_with_range(MIN_SIZE, MAX_SIZE);
    let c = gen_bytes_with_range(MIN_SIZE, MAX_SIZE);
    let total = a.len() + b.len() + c.len();

    if let Err(e) = writer.write(&a).await {
        return Err((idx, format!("write A failed: {e}")));
    }
    if let Err(e) = writer.write(&b).await {
        return Err((idx, format!("write B failed: {e}")));
    }
    if let Err(e) = writer.write(&c).await {
        return Err((idx, format!("write C failed: {e}")));
    }

    if let Err(e) = writer.close().await {
        // Mimic opendal's behavior of cleaning up tmp on failure.
        let master = ctx.acquire_master();
        let _ = master.delete(&tmp, false).await;
        return Err((idx, format!("close failed (total={total}): {e}")));
    }

    // On success: rename tmp -> path (matching opendal's finalize_rename).
    let master = ctx.acquire_master();
    if let Err(e) = master.rename(&tmp, &path).await {
        return Err((idx, format!("rename failed: {e}")));
    }
    let _ = master.delete(&path, false).await; // cleanup
    Ok((idx, total))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let rounds: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(20);
    let concurrency: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(8);
    // SHARED=0 means each task builds its own ctx; SHARED=1 (default) shares a single ctx.
    let shared = std::env::var("SHARED")
        .ok()
        .map(|v| v != "0")
        .unwrap_or(true);
    let align = std::env::var("ALIGN").ok().as_deref() == Some("1");
    let (write_ptype, wp_label) = resolve_write_ptype();

    println!("===============================================================");
    println!(" Reproducer: opendal test_writer_write_with_concurrent failure");
    println!(
        " rounds = {}, concurrency-per-round = {}, shared_ctx = {}",
        rounds, concurrency, shared
    );
    println!(" write_type = {}, align_to_chunk = {}", wp_label, align);
    println!("===============================================================");

    println!("\n[0] Connecting to GooseFS master at 127.0.0.1:9200 ...");
    let shared_ctx: Option<Arc<FileSystemContext>> = if shared {
        let config = GoosefsConfig::new("127.0.0.1:9200");
        let ctx = FileSystemContext::connect(config).await?;
        println!("    ✅ FileSystemContext connected (shared across all tasks)");
        Some(ctx)
    } else {
        println!("    (each task will build its own FileSystemContext)");
        None
    };

    let mut total_runs = 0usize;
    let mut total_fails = 0usize;
    let mut first_err: Option<String> = None;

    for r in 0..rounds {
        let mut handles = Vec::with_capacity(concurrency);
        for i in 0..concurrency {
            let ctx_clone = shared_ctx.clone();
            let task_idx = r * concurrency + i;
            handles.push(tokio::spawn(one_writer_round(
                ctx_clone,
                task_idx,
                write_ptype,
            )));
        }

        let mut round_fails = 0;
        for h in handles {
            total_runs += 1;
            match h.await {
                Ok(Ok((_idx, _bytes))) => {}
                Ok(Err((idx, msg))) => {
                    total_fails += 1;
                    round_fails += 1;
                    if first_err.is_none() {
                        first_err = Some(format!("task {idx}: {msg}"));
                    }
                    if round_fails <= 3 {
                        println!("    ❌ task {idx}: {msg}");
                    }
                }
                Err(join_err) => {
                    total_fails += 1;
                    round_fails += 1;
                    println!("    ❌ join error: {join_err}");
                }
            }
        }
        println!(
            "[round {:>3}] tasks={}, failures={}",
            r + 1,
            concurrency,
            round_fails
        );

        // Stop early to speed up diagnosis.
        if total_fails >= 3 {
            println!("    >>> reached 3 failures, stopping early");
            break;
        }
    }

    if let Some(ctx) = shared_ctx {
        ctx.close().await?;
    }

    println!("\n===============================================================");
    println!(
        " total_runs = {}, total_failures = {}",
        total_runs, total_fails
    );
    if let Some(e) = &first_err {
        println!(" first error: {e}");
    }
    println!("===============================================================");

    if total_fails > 0 {
        std::process::exit(1);
    }
    Ok(())
}
