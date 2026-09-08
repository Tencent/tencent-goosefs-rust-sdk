# GooseFS Java Binding

JNI binding over `goosefs-sdk`. **Not a replacement** for `com.qcloud.cos.goosefs`.

Maven coordinates: `com.tencent.goosefs:goosefs` (version lockstep with the Rust crate).

Downstream depends on **both** the unclassified JAR (Java classes) and one classified JAR (native library). Linux gnu artifacts are built with [cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild) against **glibc 2.28**.

## Prerequisites

- JDK 11+ (Spotless / Palantir Java Format and release dry-run: JDK 17+)
- Rust 1.88+ (`rust-toolchain.toml` at the repo root)
- Python 3 (only for `tools/build.py`)
- Maven Wrapper (`./mvnw` in this directory)

## Maven dependency

```xml
<build>
  <extensions>
    <extension>
      <groupId>kr.motd.maven</groupId>
      <artifactId>os-maven-plugin</artifactId>
      <version>1.7.1</version>
    </extension>
  </extensions>
</build>

<dependencies>
  <dependency>
    <groupId>com.tencent.goosefs</groupId>
    <artifactId>goosefs</artifactId>
    <version>${goosefs.version}</version>
  </dependency>
  <dependency>
    <groupId>com.tencent.goosefs</groupId>
    <artifactId>goosefs</artifactId>
    <version>${goosefs.version}</version>
    <classifier>${os.detected.classifier}</classifier>
  </dependency>
</dependencies>
```

The artifacts are not on Maven Central yet. Install locally with `./mvnw -DskipTests install`, or collect classified JARs with `bash scripts/release/java.sh`. See [`docs/release/JAVA_RELEASE.md`](../../docs/release/JAVA_RELEASE.md).

## Build

```bash
cd bindings/java
./mvnw clean package -DskipTests
./mvnw verify                         # unit tests; behaviour skipped without env
./mvnw test -Dtest="behavior.*Test"   # needs GOOSEFS_MASTER_ADDR
./mvnw spotless:apply                 # JDK 17+

cargo clippy -p goosefs-java --all-targets --no-deps -- -D warnings
cargo test -p goosefs-java --lib
```

`cargo-build.profile` (`dev` default, `release` for `-Prelease`) is passed to `tools/build.py`. Linux gnu release builds set `cargo-build.enableZigbuild=true`.

## Surface

- `Config` — SDK parser + `GOOSEFS_*` env overlay; `goosefs.user.client.cache.*` flows through
- `AsyncGoosefs` / `Goosefs`: metadata, one-shot I/O, streaming, batch, `positionedRead`,
  `acquireWorkerForBlock`
- `WorkerClient` / `AsyncWorkerClient` — `connect(addr, Config)` only (no `connectSimple`)
- Streaming: `FileReader` / `FileWriter` and async twins
- JDK adapters: `Goosefs.createInputStream` / `createOutputStream`
- `Tracing.enable` — off by default; `RUST_LOG` wins; second call is a no-op

**Writer contract:** `commit()` finalises the inode. `FileWriter.close()` /
`AsyncFileWriter.close()` **cancel** if neither `commit()` nor `cancel()` ran.
`GoosefsOutputStream.close()` **commits** (Java IO contract).

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for classified-library and musl notes.

## Quickstart (needs a live master)

```java
import com.tencent.goosefs.Config;
import com.tencent.goosefs.FileWriter;
import com.tencent.goosefs.Goosefs;

try (Goosefs fs = Goosefs.connect(new Config("127.0.0.1:9200"))) {
    fs.mkdir("/hello");
    try (FileWriter w = fs.createFile("/hello/world.txt")) {
        w.write("hi".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        w.commit();
    }
    byte[] data = fs.readFile("/hello/world.txt");
    System.out.println(new String(data, java.nio.charset.StandardCharsets.UTF_8));
}
```
