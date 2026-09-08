---
sidebar_position: 2
---

# Quickstart

## Synchronous API

```java
import com.tencent.goosefs.Config;
import com.tencent.goosefs.FileWriter;
import com.tencent.goosefs.Goosefs;
import java.nio.charset.StandardCharsets;

try (Goosefs fs = Goosefs.connect(new Config("127.0.0.1:9200"))) {
    fs.mkdir("/hello", true);
    try (FileWriter w = fs.createFile("/hello/world.txt")) {
        w.write("hi".getBytes(StandardCharsets.UTF_8));
        w.commit();
    }
    byte[] data = fs.readFile("/hello/world.txt");
    assert new String(data, StandardCharsets.UTF_8).equals("hi");
    fs.delete("/hello", com.tencent.goosefs.DeleteOptions.builder().recursive(true).build());
}
```

Clients, readers, and writers are `AutoCloseable`. Prefer try-with-resources: native handles are not visible to the GC.

## Asynchronous API

```java
import com.tencent.goosefs.AsyncGoosefs;
import com.tencent.goosefs.Config;
import java.nio.charset.StandardCharsets;

AsyncGoosefs fs = AsyncGoosefs.connect(new Config("127.0.0.1:9200")).join();
try {
    fs.mkdir("/hello", true).join();
    fs.writeFile("/hello/world.txt", "hi".getBytes(StandardCharsets.UTF_8)).join();
    byte[] data = fs.readFile("/hello/world.txt").join();
    assert new String(data, StandardCharsets.UTF_8).equals("hi");
} finally {
    fs.close();
}
```

`AsyncGoosefs` methods return `CompletableFuture`. `close()` is `closeAsync().join()` and must not run on a Tokio-attached thread. Do not call blocking `Goosefs` methods from those callbacks; stay on `AsyncGoosefs`.

## Enabling logs

The binding does not install a `tracing` subscriber by default:

```java
com.tencent.goosefs.Tracing.enable();           // info → stderr
com.tencent.goosefs.Tracing.enable("debug");
```

`RUST_LOG` (when set) overrides the `level` argument. A second `enable` call is a no-op.

The runnable guide example is `bindings/java/src/test/java/com/tencent/goosefs/examples/Main.java`.
