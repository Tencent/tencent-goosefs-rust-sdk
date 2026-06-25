//! Block mapping and worker routing modules.

pub mod mapper;
pub mod router;
pub mod short_circuit;

pub use mapper::{BlockMapper, BlockReadPlan, BlockWritePlan};
pub use router::WorkerRouter;
pub use short_circuit::{
    should_use_short_circuit, AccessHint, LocalBlockReader, ScDecisionCtx, ShortCircuitConfig,
    ShortCircuitError, ShortCircuitFactory,
};
