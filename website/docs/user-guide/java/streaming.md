---
sidebar_position: 6
---

# Streaming Read / Write

For files that do not fit in memory, use `openFile` / `createFile` (`FileReader` / `FileWriter`) or the async twins.

## Streaming read

```java
try (FileReader reader = fs.openFile("/data/large.bin")) {
    byte[] all = reader.read();              // remainder to EOF
    byte[] chunk = reader.read(4096);        // up to N bytes; short only at EOF
    byte[] tail = reader.readAt(4096, 512);  // does not move the cursor
    reader.seek(8192, FileReader.SEEK_SET);
    long pos = reader.tell();
    long total = reader.length();
}
```

`FileReader` is not safe to share across threads. For parallel reads, open multiple readers.

JDK adapter:

```java
try (InputStream in = fs.createInputStream("/data/large.bin")) {
    in.readAllBytes();
}
```

## Streaming write

```java
import java.nio.charset.StandardCharsets;

try (FileWriter writer = fs.createFile("/data/output.bin")) {
    writer.write("first chunk ".getBytes(StandardCharsets.UTF_8));
    writer.write("second chunk".getBytes(StandardCharsets.UTF_8));
    writer.commit();   // finalise the inode
}
```

| Call | Meaning |
| --- | --- |
| `commit()` | SDK `close()` — persist the inode |
| `cancel()` | abandon uncommitted blocks |
| `FileWriter.close()` / `AsyncFileWriter.close()` | if neither `commit()` nor `cancel()` ran, **cancel**. Never commits. |
| `GoosefsOutputStream.close()` | **commits** (Java `OutputStream` contract) |

:::warning
`try (FileWriter w = fs.createFile(path)) { w.write(data); }` **does not** create a visible file. `close()` cancels unless you called `commit()`. This is intentional and differs from Python `with` / from `GoosefsOutputStream`.
:::

```java
try (GoosefsOutputStream out = fs.createOutputStream("/data/out.bin")) {
    out.write(payload);
} // close() commits

GoosefsOutputStream out = fs.createOutputStream("/data/abandoned.bin");
out.cancel();
out.close(); // does not commit
```

`commit()` and `cancel()` are idempotent. After either has run, further `write` calls fail.
