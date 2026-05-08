//! gRPC client modules for Goosefs Master and Worker services.

pub mod master;
pub mod master_inquire;
pub mod worker;
pub mod worker_manager;

pub use master::MasterClient;
pub use master_inquire::{
    create_master_inquire_client, MasterInquireClient, PollingMasterInquireClient,
    SingleMasterInquireClient,
};
pub use worker::WorkerClient;
pub use worker::WorkerClientPool;
pub use worker::WriteBlockHandle;
pub use worker::WriteBlockOptions;
pub use worker_manager::WorkerManagerClient;
