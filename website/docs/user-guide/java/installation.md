---
sidebar_position: 1
---

# Installation

`com.tencent.goosefs:goosefs` is the JNI Java client over `goosefs-sdk`. It is **not** a replacement for `com.qcloud.cos.goosefs`; the packages are disjoint so both can sit on one classpath.

Depend on **both** the unclassified JAR (classes) and one classified JAR (native library):

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

The first publish set is `linux-x86_64` / `linux-aarch_64` (glibc 2.28) and `osx-aarch_64`. Artifacts are not on Maven Central yet — install from a local `./mvnw install` or the classified JARs produced by `bash scripts/release/java.sh`. See [Release Java](../../release/java-release).

## Requirements

- JDK **11+**
- A reachable GooseFS Master
- Native classifier matching the runtime OS/arch (`os.detected.classifier`)

On Alpine, override the classifier to `linux-*-musl` and install `libgcc`.

## Build from source

```bash
cd bindings/java
./mvnw -B verify
```

See [`bindings/java/CONTRIBUTING.md`](https://github.com/Tencent/tencent-goosefs-rust-sdk/blob/main/bindings/java/CONTRIBUTING.md).
