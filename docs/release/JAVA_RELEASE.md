# Release Guide — Java (`com.tencent.goosefs:goosefs`)

Produce classified JARs for the JNI binding. For the Rust crate see
[`RELEASE.md`](RELEASE.md); for the Python wheel see [`PYTHON_RELEASE.md`](PYTHON_RELEASE.md).

The Java binding is a native library: Linux gnu artifacts must be built with
**cargo-zigbuild** against **glibc 2.28** so they are not tied to the build
machine's glibc. That matches Python `manylinux_2_28`.

Publishing destination (Maven Central vs an internal registry) is still open.
This guide covers the **layout** and the local / CI dry-run. Do not upload
until that decision is made.

## Artifact set per version

```text
goosefs-${version}.jar
goosefs-${version}-linux-x86_64.jar      # zigbuild, glibc 2.28
goosefs-${version}-linux-aarch_64.jar
goosefs-${version}-osx-aarch_64.jar
goosefs-${version}-sources.jar
goosefs-${version}-javadoc.jar
```

Optional later (not required for the first dry-run): `osx-x86_64`,
`windows-x86_64`, musl classifiers.

Downstream depends on **both** the unclassified JAR and one classified JAR.
See [`bindings/java/README.md`](../../bindings/java/README.md).

## Version lockstep

Bump **the same number** in:

1. root `Cargo.toml`
2. `bindings/python/Cargo.toml`
3. `bindings/java/Cargo.toml`
4. `bindings/java/pom.xml`

CI fails if they drift (`bash scripts/release/java.sh --check` in `ci.yml`
and `ci_bindings_java.yml`).

## GitHub Actions (dry-run)

Workflow: **Bindings Java** (`.github/workflows/ci_bindings_java.yml`).

On paths that touch the Java binding (and on `workflow_dispatch`) it:

- checks version lockstep
- runs Spotless + `cargo clippy -p goosefs-java --all-targets --no-deps`
- `./mvnw verify` on JDK 11 / 17 / 21 and macOS
- builds classified JARs:
  - Ubuntu: `linux-x86_64` and `linux-aarch_64` via zigbuild
  - macOS: `osx-aarch_64` via host `cargo build --release`

The package jobs upload `bindings/java/dist/*.jar` as workflow artifacts.
Nothing is published to a Maven repository.

## Script (local)

From the repository root. JDK **17+**, Rust, Python 3, and for Linux gnu
JARs **Zig 0.13+** plus `cargo install cargo-zigbuild`:

```bash
# Version lockstep only
bash scripts/release/java.sh --check

# linux-x86_64 + linux-aarch_64 via zig (glibc 2.28);
# plus osx-aarch_64 when run on Darwin
bash scripts/release/java.sh

# One Linux classifier
bash scripts/release/java.sh --classifier linux-x86_64
bash scripts/release/java.sh --arch x86_64

# Host classifier, no zig (macOS arm64 on Apple Silicon)
bash scripts/release/java.sh --native
```

| Flag | Meaning |
| --- | --- |
| `--check` | Compare Cargo.toml / Python crate / Java crate / pom.xml versions |
| `--classifier NAME` | Build one Maven classifier (repeatable) |
| `--arch x86_64\|aarch64\|all` | Linux gnu classifiers (`linux-x86_64` / `linux-aarch_64`) |
| `--native` | Current host classifier, `enableZigbuild=false` |

JARs land in `bindings/java/dist/`. The script never publishes.

Local install of the host pair (classes + this machine's native lib):

```bash
cd bindings/java
./mvnw -B -DskipTests install
```

`-Prelease` sets `cargo-build.profile=release` and enables zigbuild for
linux gnu. Attach sources and javadoc JARs in that profile.

## Notes

- Prefer zigbuild Linux JARs. A host `./mvnw package` on a new glibc can
  fail on older hosts (`GLIBC_x.y not found`).
- The unclassified JAR must not contain `native/`. The classified JAR
  contains only `/native/{classifier}/…` and `META-INF/LICENSE`.
- `staticlib` is compiled by cargo but only the `cdylib` is packaged.
- Spark / Flink: both JARs on the **executor** classpath; fat-JARs must
  keep `/native/**`.
- Never commit registry tokens.
