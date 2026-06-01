use std::io::Result;

/// Build script for `goosefs-sdk`.
///
/// Pre-generated protobuf code under `src/generated/` is checked into the
/// repository and shipped with the crate so that downstream users do NOT
/// need `protoc` installed to build this crate.
///
/// To regenerate the protobuf code (after modifying any `.proto` file),
/// run the build with the opt-in environment variable:
///
/// ```bash
/// GOOSEFS_SDK_REGEN_PROTO=1 cargo build
/// ```
fn main() -> Result<()> {
    let proto_root = "proto";

    // Only re-run the build script when the opt-in env var changes, so that
    // normal builds (without `protoc` available) stay fast and reproducible.
    println!("cargo:rerun-if-env-changed=GOOSEFS_SDK_REGEN_PROTO");

    if std::env::var("GOOSEFS_SDK_REGEN_PROTO").as_deref() != Ok("1") {
        // Default path: use the pre-generated code shipped under src/generated/.
        return Ok(());
    }

    // Opt-in path: regenerate protobuf code in-tree. Requires `protoc`.
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(true)
        // Output all generated code into src/generated/ so it can be
        // checked into the repository and shipped with the crate.
        .out_dir("src/generated")
        .compile_protos(
            &[
                "proto/grpc/common.proto",
                "proto/grpc/fscommon.proto",
                "proto/grpc/version.proto",
                "proto/grpc/file_system_master.proto",
                "proto/grpc/block_worker.proto",
                "proto/grpc/worker_manager_master.proto",
                "proto/grpc/metric_master.proto",
                "proto/proto/dataserver/protocol.proto",
                "proto/proto/dataserver/status.proto",
                "proto/proto/security/capability_token.proto",
                "proto/proto/security/token.proto",
                "proto/proto/shared/acl.proto",
                "proto/proto/shared/location.proto",
                "proto/grpc/sasl/sasl_server.proto",
            ],
            &[proto_root],
        )?;

    println!("cargo:rerun-if-changed={}", proto_root);
    Ok(())
}
