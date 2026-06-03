# Rust SDK — Concurrent Batch Usage Patterns

> Audience: Rust callers of `goosefs_sdk` who need to issue many metadata or
> I/O operations concurrently (e.g. `get_status` / `exists` over a large list
> of paths).
>
> Companion to [`PYTHON_SDK_PERFORMANCE_OPTIMIZATION_ANALYSIS.md`](./PYTHON_SDK_PERFORMANCE_OPTIMIZATION_ANALYSIS.md),
> which covers the equivalent topic for the Python binding.

---

## 1. TL;DR

The Rust SDK intentionally exposes **single-future, per-operation** APIs
(`fs.get_status(path)`, `fs.exists(path)`, …). It does **not** provide
`batch_*` helpers of its own. Concurrency and back-pressure are the
caller's responsibility, composed from standard `futures` / `tokio`
primitives.

| Situation | Recommended pattern |
|---|---|
| Small, bounded N (≤ ~32, known statically) | `futures::future::join_all` |
| User-supplied or potentially large N | `stream::iter(..).buffered(N)` (ordered) or `.buffer_unordered(N)` |
| Fail-fast on first error | `try_buffered(N).try_collect()` / `try_for_each_concurrent` |
| Multiple concurrent batches sharing a global budget | `Arc<tokio::sync::Semaphore>` |
| **Anti-pattern** | `join_all` over an unbounded user input — never do this |

The only hard rule: **never `join_all` (or unbounded `FuturesUnordered`) a
user-supplied `Vec<String>` of unknown size**. That is the Rust-side
equivalent of the unbounded fan-out concern already addressed in the
Python binding.

---

## 2. Why the SDK Does Not Provide a `batch_*` API

The Python binding ships `batch_get_status` / `batch_exists` because every
PyO3 boundary crossing serialises through the GIL; collapsing N ops into a
single boundary crossing is the only way to break the per-op GIL ceiling
(see analysis §3.1). The binding therefore has to pick a concurrency
model on the user's behalf, and is internally bounded by
`BATCH_CONCURRENCY_LIMIT` (`bindings/python/src/context.rs`) to protect
the master from unbounded fan-out.

In Rust there is no such ceiling. A single-future API:

- has zero per-op overhead beyond the gRPC call itself,
- composes cleanly with any concurrency primitive the caller already uses
  (a tokio task pool, an `mpsc` worker, an OpenDAL layer, …),
- lets the caller pick the right policy (ordered vs unordered, fail-fast
  vs collect-all, per-batch vs global budget).

This is the same convention used by `tokio-postgres`, `redis-rs`,
`reqwest`, etc.: the driver exposes single-op futures, the caller
composes concurrency.

---

## 3. Patterns

All examples assume:

```rust
use std::sync::Arc;
use goosefs_sdk::context::FileSystemContext;
use goosefs_sdk::fs::{BaseFileSystem, FileSystem, URIStatus};
use goosefs_sdk::error::{Error, Result};

# async fn build_fs() -> Result<Arc<BaseFileSystem>> {
#     let ctx = FileSystemContext::connect(Default::default()).await?;
#     Ok(Arc::new(BaseFileSystem::new(ctx)))
# }
```

### 3.1 Small, bounded N — `join_all`

Use this only when N is statically bounded by the call site (e.g. a
fixed list of well-known paths, ≤ ~32). Every future is in flight at the
same time.

```rust
use futures::future::join_all;

async fn batch_status_unbounded(
    fs: Arc<BaseFileSystem>,
    paths: &[String],
) -> Vec<Result<URIStatus>> {
    let futs = paths.iter().map(|p| fs.get_status(p));
    join_all(futs).await
}
```

Pros: simplest possible code; preserves input order via `Vec` indexing.
Cons: **unbounded fan-out**. Do not feed user input straight into this.

### 3.2 Bounded concurrency, ordered — `stream::buffered(N)` (recommended default)

This is the same shape used inside the Python binding and is the
recommended default for any user-facing batch logic.

```rust
use futures::stream::{self, StreamExt};

const CONCURRENCY: usize = 64;

async fn batch_status(
    fs: Arc<BaseFileSystem>,
    paths: Vec<String>,
) -> Vec<Result<URIStatus>> {
    stream::iter(paths.into_iter().map(move |p| {
        let fs = fs.clone();
        async move { fs.get_status(&p).await }
    }))
    .buffered(CONCURRENCY)
    .collect()
    .await
}
```

- `buffered(N)` preserves input order; at most `N` futures are in flight.
- Use `buffer_unordered(N)` instead if order does not matter — slightly
  higher throughput because slow futures do not stall faster ones.
- Keep failures per-element (`Vec<Result<_>>`) so one bad path does not
  abort the rest.

### 3.3 Fail-fast on first error — `try_buffered`

If the whole batch is meaningless once any path fails, short-circuit on
the first error. Note the same caveat that applies to the Python binding:
**already-dispatched RPCs are not cancelled**, the early return only
stops feeding new requests into the buffer.

```rust
use futures::stream::{self, StreamExt, TryStreamExt};

async fn batch_status_fail_fast(
    fs: Arc<BaseFileSystem>,
    paths: Vec<String>,
) -> Result<Vec<URIStatus>> {
    stream::iter(paths.into_iter().map(move |p| {
        let fs = fs.clone();
        Ok::<_, Error>(async move { fs.get_status(&p).await })
    }))
    .try_buffered(64)
    .try_collect()
    .await
}
```

For a "side-effects only" variant (no result vector), use
`try_for_each_concurrent(Some(64), …)`.

### 3.4 Global budget shared across batches — `Arc<Semaphore>`

`buffered(N)` bounds **a single stream**. If the application runs
multiple batches at once (e.g. one per worker thread, or one per
incoming HTTP request), their per-stream limits stack up and you are
back to unbounded fan-out at the master.

For that case, share a `tokio::sync::Semaphore` across all batches:

```rust
use std::sync::Arc;
use tokio::sync::Semaphore;
use futures::future::join_all;

// Build once, share for the lifetime of the process.
fn make_master_budget() -> Arc<Semaphore> {
    Arc::new(Semaphore::new(64))
}

async fn batch_status_global(
    fs: Arc<BaseFileSystem>,
    sem: Arc<Semaphore>,
    paths: Vec<String>,
) -> Vec<Result<URIStatus>> {
    let futs = paths.into_iter().map(|p| {
        let fs = fs.clone();
        let sem = sem.clone();
        async move {
            // Permit is released when `_permit` drops at end of scope.
            let _permit = sem.acquire_owned().await.expect("semaphore closed");
            fs.get_status(&p).await
        }
    });
    join_all(futs).await
}
```

This composes naturally with `buffered(N)` too: the semaphore caps the
**global** in-flight count, while `buffered(N)` caps the **per-batch**
count.

---

## 4. Choosing `N`

There is no universally correct value; it depends on your master's
provisioning, the gRPC channel's HTTP/2 `MAX_CONCURRENT_STREAMS`, and
the latency/throughput trade-off you want.

Starting points:

| Workload | Suggested `N` |
|---|---|
| Latency-sensitive, small batches | 8–16 |
| Throughput-oriented metadata sweeps | 32–128 |
| Large directory walks driven by a single client | 64 (matches the Python binding default) |

If you see RPC errors that look like channel saturation
(`UNAVAILABLE` / `RESOURCE_EXHAUSTED`) or master-side queueing latency
spikes, lower `N` first.

---

## 5. Anti-Patterns

```rust
// ❌ Unbounded fan-out — do not feed user input directly into join_all.
let futs = user_paths.iter().map(|p| fs.get_status(p));
let _ = futures::future::join_all(futs).await;
```

```rust
// ❌ FuturesUnordered without a cap — same problem, just more verbose.
let mut set = futures::stream::FuturesUnordered::new();
for p in user_paths { set.push(fs.get_status(&p)); }
while let Some(_) = set.next().await {}
```

```rust
// ❌ buffered(N) created fresh inside a hot loop with no global cap —
//    every iteration permits N more in-flight RPCs.
for chunk in user_paths.chunks(CHUNK) {
    stream::iter(chunk.iter().map(|p| fs.get_status(p)))
        .buffered(64)
        .collect::<Vec<_>>()
        .await;
}
// Either await each chunk before starting the next (this code already
// does, so it's fine for a single producer), or share a Semaphore if
// multiple producers run this loop concurrently.
```

---

## 6. What the SDK Already Does for You

Even with concurrent callers, the lower layers provide some passive
protection:

- `MasterClient` wraps each RPC in `with_retry(...)` (see
  [`src/client/master.rs`](../src/client/master.rs)) — transient errors
  are retried with backoff, so a momentary overload does not bubble up
  as a hard failure.
- gRPC keep-alive and connection reuse mean a batch of 64 concurrent
  `get_status` calls multiplexes over a single HTTP/2 connection rather
  than opening 64 sockets.

What it does **not** do:

- No per-client RPC concurrency cap.
- No global token bucket.
- No automatic batching of independent requests into a single RPC.

These are deliberately left to the caller.

---

## 7. Cross-Reference

- Python binding equivalent — `BATCH_CONCURRENCY_LIMIT` in
  [`bindings/python/src/context.rs`](../bindings/python/src/context.rs),
  used by `batch_get_status` / `batch_exists` in
  [`bindings/python/src/filesystem.rs`](../bindings/python/src/filesystem.rs)
  and [`bindings/python/src/sync_fs.rs`](../bindings/python/src/sync_fs.rs).
- Rationale and benchmark context —
  [`PYTHON_SDK_PERFORMANCE_OPTIMIZATION_ANALYSIS.md`](./PYTHON_SDK_PERFORMANCE_OPTIMIZATION_ANALYSIS.md)
  §3.1 (Batch API) and §8.3.1.
