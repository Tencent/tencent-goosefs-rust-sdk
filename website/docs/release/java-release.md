---
sidebar_position: 3
---

# Release Java Package

Produce classified JARs for `com.tencent.goosefs:goosefs`. Linux gnu artifacts use **cargo-zigbuild** with glibc **2.28**.

Publishing destination (Maven Central vs an internal registry) is still open. This page is the dry-run layout.

## Artifact set

```text
goosefs-${version}.jar
goosefs-${version}-linux-x86_64.jar
goosefs-${version}-linux-aarch_64.jar
goosefs-${version}-osx-aarch_64.jar
goosefs-${version}-sources.jar
goosefs-${version}-javadoc.jar
```

Downstream depends on the unclassified JAR **and** one classified JAR.

## Version lockstep

Bump the same number in root `Cargo.toml`, `bindings/python/Cargo.toml`, `bindings/java/Cargo.toml`, and `bindings/java/pom.xml`. CI runs `bash scripts/release/java.sh --check`.

## GitHub Actions (dry-run)

Workflow: [Bindings Java](https://github.com/Tencent/tencent-goosefs-rust-sdk/actions/workflows/ci_bindings_java.yml).

Package jobs build zigbuild Linux classifiers and the macOS arm64 classifier, then upload `bindings/java/dist/*.jar`. Nothing is published to a Maven repository.

## Script (local)

```bash
bash scripts/release/java.sh --check
bash scripts/release/java.sh
bash scripts/release/java.sh --classifier linux-x86_64
bash scripts/release/java.sh --native
```

| Flag | Meaning |
| --- | --- |
| `--check` | Version lockstep only |
| `--classifier NAME` | One Maven classifier (repeatable) |
| `--arch x86_64\|aarch64\|all` | Linux gnu classifiers |
| `--native` | Host classifier, no zig |

JARs land in `bindings/java/dist/`. See [`docs/release/JAVA_RELEASE.md`](https://github.com/Tencent/tencent-goosefs-rust-sdk/blob/main/docs/release/JAVA_RELEASE.md).
