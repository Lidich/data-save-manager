//! Typed queues retain accepted batches until their writer acknowledges success.
//! Database transactions, idempotency and diagnostics belong to writer adapters.
//! Storage is volatile: process termination and adapter panics are not recoverable.

mod admission;
#[cfg(feature = "postgres")]
pub mod postgres;
mod queue;
mod telemetry;
mod worker;

pub use admission::{Admission, EnqueueGuard};
#[cfg(feature = "postgres")]
pub use postgres::MeasuredJson;
pub use queue::{
    BatchIdentity, BatchInfo, BatchQueue, BatchWriter, FlushOutcome, Observer, Queued,
};
pub use telemetry::QueueTelemetry;
pub use worker::{FlushCycle, Worker, WorkerCadence, WorkerEvent};
