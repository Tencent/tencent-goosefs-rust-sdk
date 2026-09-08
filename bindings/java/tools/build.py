#!/usr/bin/env python3
# Copyright (C) 2026 Tencent. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Compile the `goosefs-java` cdylib and copy it into Maven `target/classes/native`.

Classifier mapping matches goosefs-lance-tests `docs/design/JAVA_BINDING_DESIGN.md` §7.1.
Linux gnu release builds use cargo-zigbuild with glibc 2.28 (`--target {triple}.2.28`).
"""

from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
from pathlib import Path
import glob
import shutil
import subprocess


def classifier_to_target(classifier: str) -> str:
    mapping = {
        "osx-aarch_64": "aarch64-apple-darwin",
        "osx-x86_64": "x86_64-apple-darwin",
        "linux-aarch_64": "aarch64-unknown-linux-gnu",
        "linux-aarch_64-musl": "aarch64-unknown-linux-musl",
        "linux-x86_64": "x86_64-unknown-linux-gnu",
        "linux-x86_64-musl": "x86_64-unknown-linux-musl",
        "windows-x86_64": "x86_64-pc-windows-msvc",
    }
    if classifier not in mapping:
        raise Exception(f"Unsupported classifier: {classifier}")
    return mapping[classifier]


def get_cargo_artifact_name(classifier: str) -> str:
    if classifier.startswith("osx"):
        return "libgoosefs_java.dylib"
    if classifier.startswith("linux"):
        return "libgoosefs_java.so"
    if classifier.startswith("windows"):
        return "goosefs_java.dll"
    raise Exception(f"Unsupported classifier: {classifier}")


def is_musl_runtime() -> bool:
    return bool(
        glob.glob("/lib/ld-musl-*.so.1") or glob.glob("/usr/lib/ld-musl-*.so.1")
    )


if __name__ == "__main__":
    basedir = Path(__file__).resolve().parent.parent

    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("--classifier", type=str, required=True)
    parser.add_argument("--target", type=str, default="")
    parser.add_argument("--profile", type=str, default="dev")
    parser.add_argument("--enable-zigbuild", type=str, default="false")
    args = parser.parse_args()

    if args.target:
        target = args.target
    else:
        target = classifier_to_target(args.classifier)

    command = ["rustup", "target", "add", target]
    print("$ " + subprocess.list2cmdline(command))
    subprocess.run(command, cwd=basedir, check=True)

    # Linux gnu release artifacts pin glibc 2.28 (same baseline as Python
    # manylinux_2_28). musl / macOS / Windows keep a plain `cargo build`.
    enable_zigbuild = (
        args.enable_zigbuild == "true"
        and "linux" in target
        and not target.endswith("-musl")
    )

    cmd = [
        "cargo",
        "zigbuild" if enable_zigbuild else "build",
        "--color=always",
        f"--profile={args.profile}",
        "-p",
        "goosefs-java",
    ]

    if enable_zigbuild and target.endswith("-gnu"):
        cmd += ["--target", f"{target}.2.28"]
    else:
        cmd += ["--target", target]

    output = basedir / "target" / "bindings"
    Path(output).mkdir(exist_ok=True, parents=True)
    cmd += ["--target-dir", str(output)]

    print("$ " + subprocess.list2cmdline(cmd))
    subprocess.run(cmd, cwd=basedir, check=True)

    profile = "debug" if args.profile in ["dev", "test", "bench"] else args.profile
    artifact = get_cargo_artifact_name(args.classifier)
    src = output / target / profile / artifact
    if enable_zigbuild and not src.exists():
        zig_src = output / f"{target}.2.28" / profile / artifact
        if zig_src.exists():
            src = zig_src
    dst = basedir / "target" / "classes" / "native" / args.classifier / artifact
    dst.parent.mkdir(exist_ok=True, parents=True)

    if target.endswith("-musl") and not is_musl_runtime():
        raise Exception(
            "Building musl artifacts requires running inside a musl environment (e.g. Alpine)."
        )

    if not src.exists():
        raise Exception(f"native artifact not found: {src}")

    shutil.copy2(src, dst)
    print(f"copied {src} -> {dst}")
