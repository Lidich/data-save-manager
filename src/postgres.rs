use std::future::Future;
use std::mem::size_of;

use serde::Serialize;
use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use sqlx::postgres::{PgArgumentBuffer, PgTypeInfo};
use sqlx::types::Json;
use sqlx::{Encode, Postgres, Type};

use crate::QueueTelemetry;

mod writer;
pub use writer::{write_error, DatabaseWriter, PreparedBatch, TransactionBatch, WriteFailure};

tokio::task_local! {
    static ACTIVE_TELEMETRY: Option<QueueTelemetry>;
}

/// Encodes JSON and counts payload bytes across all attempts, excluding the JSONB version byte.
/// Successful encoding counts even if the subsequent SQL operation fails or is cancelled.
#[derive(Clone, Copy, Debug)]
pub struct MeasuredJson<T>(pub T);

impl<T> Type<Postgres> for MeasuredJson<T> {
    fn type_info() -> PgTypeInfo {
        <Json<T> as Type<Postgres>>::type_info()
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        <Json<T> as Type<Postgres>>::compatible(ty)
    }
}

impl<'q, T: Serialize> Encode<'q, Postgres> for MeasuredJson<T> {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let before = buf.len();
        let result = Json(&self.0).encode_by_ref(buf)?;
        let bytes = buf.len() - before - 1;
        let _ = ACTIVE_TELEMETRY.try_with(|telemetry| {
            if let Some(telemetry) = telemetry {
                telemetry.record_payload_bytes(bytes);
            }
        });
        Ok(result)
    }

    fn produces(&self) -> Option<PgTypeInfo> {
        <Json<&T> as Encode<'q, Postgres>>::produces(&Json(&self.0))
    }

    /// Matches SQLx 0.8's default hint for owned Json<T>, without cloning T.
    fn size_hint(&self) -> usize {
        size_of::<Json<T>>()
    }
}

pub(crate) async fn scope_payload_telemetry<F: Future>(
    telemetry: Option<QueueTelemetry>,
    future: F,
) -> F::Output {
    ACTIVE_TELEMETRY.scope(telemetry, future).await
}
