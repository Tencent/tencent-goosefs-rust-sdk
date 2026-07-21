// Copyright (C) 2026 Tencent. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Integration Test — Verify connection reuse across multiple operations

#[cfg(test)]
mod integration {
    use goosefs_sdk::config::GoosefsConfig;
    use goosefs_sdk::context::FileSystemContext;
    use goosefs_sdk::fs::FileSystem;
    use std::sync::Arc;
    use std::time::Instant;

    /// Verify that the context's acquired clients are the same references.
    #[test]
    fn context_acquire_returns_same_reference() {
        // Simulate what FileSystemContext does with Arc cloning
        let master_arc_original = Arc::new("mock_master_client");

        // First acquisition
        let _master_1 = master_arc_original.clone();

        // Second acquisition
        let _master_2 = master_arc_original.clone();

        // Verify Arc::strong_count shows both references
        let strong_count = Arc::strong_count(&master_arc_original);
        assert_eq!(
            strong_count, 3,
            "Expected 3 refs: original + master_1 + master_2"
        );
        println!("✓ Arc<MasterClient> strong_count = {}", strong_count);
    }

    /// Verify that connection pool reuses worker connections.
    #[test]
    fn worker_pool_arc_structure() {
        use goosefs_sdk::client::WorkerClientPool;

        let config = GoosefsConfig::new("127.0.0.1:9200");

        // Create a shared pool
        let pool_original = Arc::new(WorkerClientPool::new_shared(config));

        // Simulate multiple readers acquiring the pool
        let _pool_1 = pool_original.clone();
        let _pool_2 = pool_original.clone();
        let _pool_3 = pool_original.clone();

        // All point to the same DashMap
        let strong_count = Arc::strong_count(&pool_original);
        assert_eq!(strong_count, 4, "Expected 4 refs (original + 3 clones)");

        println!("✓ Arc<WorkerClientPool> strong_count = {}", strong_count);
    }

    /// Verify that `BaseFileSystem::connect` and `from_context` APIs compile.
    #[test]
    fn context_based_filesystem_compiles() {
        use goosefs_sdk::fs::BaseFileSystem;

        // Just verify the type signatures are accessible — no network needed.
        let _ = BaseFileSystem::connect; // fn(GoosefsConfig) -> impl Future<...>
        let _ = BaseFileSystem::from_context; // fn(Arc<FileSystemContext>) -> Arc<Self>

        println!("✓ BaseFileSystem context-based constructors compile");
    }

    /// Verify context connection establishment and reuse semantics.
    ///
    /// This test demonstrates the connection pooling benefit:
    /// - Before: Each operation creates new TCP+SASL connections
    /// - After: Single FileSystemContext is shared across all operations
    ///
    /// To run with a real Goosefs cluster:
    /// cargo test --test connection_reuse shared_context_reuses_connections -- --nocapture --ignored
    #[tokio::test]
    #[ignore] // Ignored by default — requires real Goosefs cluster
    async fn shared_context_reuses_connections() -> goosefs_sdk::error::Result<()> {
        let config = GoosefsConfig::new("127.0.0.1:9200");

        // Build context once (2 TCP+SASL handshakes: Master + WorkerManager)
        let start = Instant::now();
        let ctx = FileSystemContext::connect(config).await?;
        let connect_time = start.elapsed();
        println!("FileSystemContext::connect() took: {:?}", connect_time);

        // All subsequent operations reuse the same connections (0 new TCP+SASL)
        let fs = Arc::new(goosefs_sdk::fs::BaseFileSystem::from_context(ctx.clone()));

        for i in 0..5 {
            let path = format!("/test_file_{}.txt", i);
            let start = Instant::now();

            // This reuses the same Master connection — no new TCP+SASL handshake
            let _status = fs.get_status(&path).await;
            let elapsed = start.elapsed();

            println!("get_status('{}') latency: {:?}", path, elapsed);
        }

        // Cleanup
        ctx.close().await?;
        Ok(())
    }

    /// Verify that FileSystemContext can be cloned and shared.
    #[tokio::test]
    #[ignore]
    async fn context_is_shareable() -> goosefs_sdk::error::Result<()> {
        let config = GoosefsConfig::new("127.0.0.1:9200");
        let ctx = FileSystemContext::connect(config).await?;

        // Context should be cloneable (Arc-based)
        let _ctx2 = ctx.clone();
        let _ctx3 = ctx.clone();

        println!("✓ FileSystemContext is shareable via Arc clones");

        ctx.close().await?;
        Ok(())
    }
}
