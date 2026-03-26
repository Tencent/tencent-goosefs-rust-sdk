use std::io::Result;

fn main() -> Result<()> {
    let proto_root = "proto";

    // Compile all protos together in a single pass so that cross-package
    // references are resolved within the same compilation unit.
    // tonic-build / prost will generate one .rs file per proto package.
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        // Output all generated code into a single file to avoid
        // cross-module resolution issues with deeply nested packages.
        .out_dir("src/generated")
        .compile_protos(
            &[
                "proto/grpc/common.proto",
                "proto/grpc/fscommon.proto",
                "proto/grpc/version.proto",
                "proto/grpc/file_system_master.proto",
                "proto/grpc/block_worker.proto",
                "proto/grpc/worker_manager_master.proto",
                "proto/proto/dataserver/protocol.proto",
                "proto/proto/dataserver/status.proto",
                "proto/proto/security/capability_token.proto",
                "proto/proto/security/token.proto",
                "proto/proto/shared/acl.proto",
            ],
            &[proto_root],
        )?;

    println!("cargo:rerun-if-changed={}", proto_root);
    Ok(())
}
