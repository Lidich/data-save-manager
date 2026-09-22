//! Typed queues retain accepted batches until their writer acknowledges success.
//! The PostgreSQL writer owns deadlines, atomic receipts and failure diagnostics.
//! Storage is volatile: process termination and adapter panics are not recoverable.

mod admission;
#[cfg(feature = "postgres")]
pub mod postgres;
mod queue;
mod telemetry;
mod timeouts;
mod worker;

pub use admission::{Admission, EnqueueGuard};
#[cfg(feature = "postgres")]
pub use postgres::MeasuredJson;
pub use queue::{
    BatchIdentity, BatchInfo, BatchQueue, BatchWriter, FlushOutcome, Observer, Queued,
};
pub use telemetry::QueueTelemetry;
pub use timeouts::{TimeoutConfigError, WriteTimeouts};
pub use worker::{FlushCycle, Worker, WorkerCadence, WorkerEvent};
