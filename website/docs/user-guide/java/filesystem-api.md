---
sidebar_position: 3
---

# FileSystem API

The Java binding exposes two parallel APIs:

- **`Goosefs`** — blocking wrapper that drives the async core on a shared Tokio runtime. Use for scripts and non-async code.
- **`AsyncGoosefs`** — `CompletableFuture` API for the same operations.

Both share the same underlying `FileSystemContext`. Construct once per process and reuse. Both are `AutoCloseable`; native handles are not GC-visible, so try-with-resources (or an explicit `close()`) is mandatory.

## Connecting

```java
import com.tencent.goosefs.AsyncGoosefs;
import com.tencent.goosefs.Config;
import com.tencent.goosefs.Goosefs;

Config cfg = new Config("127.0.0.1:9200");

try (Goosefs fs = Goosefs.connect(cfg)) {
    fs.getStatus("/data");
}

AsyncGoosefs async = AsyncGoosefs.connect(cfg).join();
try {
    async.getStatus("/data").join();
} finally {
    async.close();
}
```

:::caution
Do not call blocking `Goosefs` methods from a `CompletableFuture` callback that may run on a Tokio-attached thread. Use `AsyncGoosefs` in async code. `Goosefs` / `AsyncGoosefs.close()` throw `IllegalStateException` if they detect that deadlock.
:::

## Metadata operations

```java
URIStatus status = fs.getStatus("/data/file.parquet");
boolean exists = fs.exists("/data/missing");

List<URIStatus> entries = fs.listStatus("/data", false);
URIStatusList grouped = fs.listStatusGrouped("/data", false);

fs.mkdir("/data/new", true);   // existing directories are idempotent
fs.rename("/data/a", "/data/b");
fs.delete("/data/b", DeleteOptions.builder().recursive(true).build());
```

`listStatus` materialises every `URIStatus`. `listStatusGrouped` returns a `URIStatusList` that materialises entries on demand.

Batch helpers (`batchGetStatus`, `batchExists`, `batchCreateFile`, `batchListStatus`, …) use one JNI crossing and bounded concurrency; the first error in input order is returned.

## High-level read / write

```java
byte[] all = fs.readFile("/data/file.bin");
byte[] slice = fs.readRange("/data/file.bin", 0, 512);
long n = fs.writeFile("/data/out.bin", payload);

byte[] block = fs.positionedRead("/data/file.bin"); // blockIndex=0, offset=0, length=-1
try (WorkerClient worker = fs.acquireWorkerForBlock(blockId)) {
    byte[] raw = worker.readBlockPositioned(blockId, 0, -1);
}
```

`WorkerClient.connect(addr, config)` is the only direct constructor (no `connectSimple`). Sync `acquireWorkerForBlock` returns a blocking `WorkerClient`.

For files that do not fit in memory, use [streaming](./streaming).
