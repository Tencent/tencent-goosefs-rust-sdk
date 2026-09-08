# Contributing to the Java binding

Build, test, and packaging notes for `bindings/java`. For repository-wide
conventions see the root [`CONTRIBUTING.md`](../../CONTRIBUTING.md).

## Prerequisites

- JDK **11+** for compile / tests. Spotless (Palantir Java Format) and the
  release dry-run need **JDK 17+**.
- Rust **1.88+** (`rust-toolchain.toml` at the repo root)
- Python 3 (`tools/build.py`)
- Maven Wrapper (`./mvnw`)

## Local loop

```bash
cd bindings/java
./mvnw -B verify
./mvnw -B spotless:apply          # JDK 17+
cargo fmt -p goosefs-java
cargo clippy -p goosefs-java --all-targets --no-deps -- -D warnings
cargo test -p goosefs-java --lib
```

Behaviour tests need a live master:

```bash
export GOOSEFS_MASTER_ADDR=127.0.0.1:9200
export GOOSEFS_AUTH_TYPE=simple
./mvnw -B test -Dtest="behavior.*Test,examples.Main"
```

`./mvnw verify` compiles the host `cdylib` into
`target/classes/native/${os.detected.classifier}/` and packages two JARs:
the unclassified classes JAR (no `native/`) and one classified native JAR.

## Classified library

Downstream must depend on **both** artifacts:

| Artifact | Classifier | Contents |
| --- | --- | --- |
| `goosefs-${version}.jar` | (none) | Java classes and resources, **no** native lib |
| `goosefs-${version}-${classifier}.jar` | see below | `/native/{classifier}/libgoosefs_java.{so,dylib,dll}` plus `META-INF/LICENSE` |

P4 publishes / dry-runs:

- `linux-x86_64` — `x86_64-unknown-linux-gnu`, cargo-zigbuild, glibc **2.28**
- `linux-aarch_64` — `aarch64-unknown-linux-gnu`, cargo-zigbuild, glibc **2.28**
- `osx-aarch_64` — `aarch64-apple-darwin`, host `cargo build --release`

The loader also understands `linux-*-musl`, `osx-x86_64`, and `windows-x86_64`;
those are not in the first publish set.

Maven consumers use `kr.motd.maven:os-maven-plugin` so `${os.detected.classifier}`
matches the table. Gradle can use `com.google.osdetector`.

```xml
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
```

On Spark / Flink, put **both** JARs on the executor classpath once. Fat-JARs
must keep `/native/**`. Two class loaders loading `NativeObject` in one JVM
will hit `UnsatisfiedLinkError` on the second load.

Release dry-run (version lockstep + classified JARs + sources/javadoc):

```bash
# Linux gnu (zig) + macOS arm64 when run on Darwin
bash scripts/release/java.sh

# Version numbers only
bash scripts/release/java.sh --check
```

`./mvnw -Prelease package` sets `cargo-build.profile=release` and
`cargo-build.enableZigbuild=true` (zigbuild applies only to linux gnu).

## musl

Classifier `linux-x86_64-musl` / `linux-aarch_64-musl` maps to
`*-unknown-linux-musl`. `tools/build.py` refuses to build musl artifacts
unless the process is running on a musl loader (for example Alpine).

Downstream on Alpine should override the classifier to `linux-*-musl` and
install `libgcc`. musl JARs are **not** part of the P4 dry-run set.

## Version lockstep

Keep the same number in:

- root `Cargo.toml`
- `bindings/python/Cargo.toml`
- `bindings/java/Cargo.toml`
- `bindings/java/pom.xml`

CI runs `bash scripts/release/java.sh --check`.
