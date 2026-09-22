use std::error::Error;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::postgres::PgConnectOptions;
use sqlx::{Connection, PgConnection, PgPool, Row};
use tokio::sync::Mutex;
use uuid::Uuid;

use super::MeasuredJson;
use crate::WriteTimeouts;

#[derive(Debug)]
struct ReportWriteError<E>(E);

impl<E> fmt::Display for ReportWriteError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("batch operation failed")
    }
}

impl<E: fmt::Debug + AsRef<dyn Error + Send + Sync>> Error for ReportWriteError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}

/// Preserves the root error's concrete type in the cause chain for SQLSTATE classification.
pub fn write_error<E>(source: E) -> anyhow::Error
where
    E: fmt::Debug + AsRef<dyn Error + Send + Sync> + Send + Sync + 'static,
{
    anyhow::Error::new(ReportWriteError(source))
}

/// A single statement whose mutations consume data_save_input.payload; an empty input must write nothing.
/// Count statements must return zero on replay. Parameters $1, $2 and $3 belong to the writer.
pub struct PreparedBatch {
    pub sql: String,
    pub rows: Value,
    pub select_count: bool,
}

impl PreparedBatch {
    pub fn new(sql: impl Into<String>, rows: Value) -> Self {
        Self {
            sql: sql.into(),
            rows,
            select_count: false,
        }
    }

    pub fn with_count_sql(sql: impl Into<String>, rows: Value) -> Self {
        Self {
            sql: sql.into(),
            rows,
            select_count: true,
        }
    }

    /// Executes directly without receipts or writer deadlines, for explicit repository calls.
    pub async fn execute(&self, pool: &PgPool) -> Result<u64> {
        let sql = format!(
            "WITH data_save_input AS (SELECT $1::jsonb AS payload) {}",
            self.sql
        );
        if self.select_count {
            let count: i64 = sqlx::query_scalar(&sql)
                .bind(MeasuredJson(&self.rows))
                .fetch_one(pool)
                .await?;
            return Ok(u64::try_from(count)?);
        }
        Ok(sqlx::query(&sql)
            .bind(MeasuredJson(&self.rows))
            .execute(pool)
            .await?
            .rows_affected())
    }
}

/// Runs domain statements on the library-owned transaction without beginning or committing it.
pub trait TransactionBatch: Sync {
    fn execute<'a>(
        &'a self,
        connection: &'a mut PgConnection,
    ) -> impl Future<Output = Result<u64>> + Send + 'a;
}

#[derive(Debug, thiserror::Error)]
#[error("data save failed queue={queue} batch_id={batch_id} stage={stage} kind={kind} elapsed_ms={elapsed_ms} sqlstate={sqlstate:?} backend={backend}: {source:#}")]
pub struct WriteFailure {
    pub queue: &'static str,
    pub batch_id: Uuid,
    pub stage: &'static str,
    pub kind: &'static str,
    pub elapsed_ms: u128,
    pub sqlstate: Option<String>,
    pub backend: String,
    #[source]
    pub source: anyhow::Error,
}

#[derive(Debug, thiserror::Error)]
#[error("client write deadline exceeded before SQL submission")]
struct WriteDeadline;

fn check_deadline(deadline: tokio::time::Instant) -> Result<()> {
    if tokio::time::Instant::now() >= deadline {
        return Err(WriteDeadline.into());
    }
    Ok(())
}

#[derive(Clone)]
pub struct DatabaseWriter {
    connection: Arc<Mutex<Option<PgConnection>>>,
    options: PgConnectOptions,
    diagnostics_pool: PgPool,
    application_name: Arc<str>,
    timeouts: WriteTimeouts,
    timezone: Option<Arc<str>>,
}

impl DatabaseWriter {
    pub fn new(diagnostics_pool: PgPool, timeouts: WriteTimeouts) -> Self {
        let application_name: Arc<str> = format!("data-save-writer-{}", Uuid::new_v4()).into();
        let options = (*diagnostics_pool.connect_options())
            .clone()
            .application_name(&application_name)
            .options([
                (
                    "statement_timeout",
                    format!("{}ms", timeouts.statement().as_millis()),
                ),
                ("lock_timeout", format!("{}ms", timeouts.lock().as_millis())),
            ]);
        Self {
            connection: Arc::new(Mutex::new(None)),
            options,
            diagnostics_pool,
            application_name,
            timeouts,
            timezone: None,
        }
    }

    /// Sets the PostgreSQL session timezone once when each dedicated connection is established.
    pub fn with_timezone(mut self, timezone: &str) -> Self {
        self.timezone = Some(timezone.into());
        self.connection = Arc::new(Mutex::new(None));
        self
    }

    pub async fn check_ready(&self) -> Result<()> {
        tokio::time::timeout(self.timeouts.batch(), async {
            let mut stored = self.connection.lock().await;
            let mut connection = self.connect().await.context("connect data save writer")?;
            sqlx::query("SELECT batch_id, queue FROM data_save_batches LIMIT 0")
                .execute(&mut connection)
                .await
                .context("data save requires the data_save_batches migration")?;
            *stored = Some(connection);
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("data save writer readiness deadline exceeded")??;
        Ok(())
    }

    async fn connect(&self) -> Result<PgConnection> {
        let mut connection = PgConnection::connect_with(&self.options).await?;
        if let Some(timezone) = &self.timezone {
            sqlx::query("SELECT set_config('TimeZone', $1, false)")
                .bind(timezone.as_ref())
                .execute(&mut connection)
                .await?;
        }
        Ok(connection)
    }

    pub async fn write_prepared<P>(
        &self,
        batch_id: Uuid,
        queue: &'static str,
        prepare: P,
    ) -> Result<u64>
    where
        P: FnOnce() -> Result<PreparedBatch> + Send,
    {
        self.run(batch_id, queue, prepare).await
    }

    pub async fn write_transaction<B: TransactionBatch>(
        &self,
        batch_id: Uuid,
        queue: &'static str,
        batch: &B,
    ) -> Result<u64> {
        self.run(batch_id, queue, || Ok(Transactional(batch))).await
    }

    async fn run<P, B>(&self, batch_id: Uuid, queue: &'static str, prepare: P) -> Result<u64>
    where
        P: FnOnce() -> Result<B> + Send,
        B: Operation,
    {
        let started = Instant::now();
        let deadline = tokio::time::Instant::now() + self.timeouts.batch();
        let mut stage = "prepare";
        let mut connection = None;
        let mut stored = None;
        let operation = async {
            let prepared = prepare().context("prepare batch")?;
            check_deadline(deadline)?;
            stage = "acquire";
            stored = Some(self.connection.lock().await);
            check_deadline(deadline)?;
            connection = stored.as_mut().expect("writer guard").take();
            if connection.is_none() {
                connection = Some(self.connect().await?);
            }
            check_deadline(deadline)?;
            stage = "execute";
            let rows = prepared
                .run(
                    connection.as_mut().expect("acquired connection"),
                    batch_id,
                    queue,
                )
                .await?;
            **stored.as_mut().expect("writer guard") = connection.take();
            Ok::<_, anyhow::Error>(rows)
        };
        let result = tokio::time::timeout_at(deadline, operation).await;
        let (source, deadline) = match result {
            Ok(Ok(rows)) => return Ok(rows),
            Ok(Err(error)) => {
                let exceeded = error.is::<WriteDeadline>();
                (error, exceeded)
            }
            Err(error) => (
                anyhow::Error::new(error)
                    .context("client write deadline exceeded; commit outcome may be unknown"),
                true,
            ),
        };
        let sqlstate = source.chain().find_map(|error| {
            error
                .downcast_ref::<sqlx::Error>()
                .and_then(|error| match error {
                    sqlx::Error::Database(error) => error.code().map(|code| code.into_owned()),
                    _ => None,
                })
        });
        let kind = if deadline {
            "client_deadline"
        } else {
            match sqlstate.as_deref() {
                Some("55P03") => "lock_timeout",
                Some("57014") => "statement_cancelled",
                Some(_) => "database_error",
                None if stage == "acquire" => "acquire_error",
                None if stage == "prepare" => "serialization_error",
                None => "connection_error",
            }
        };
        drop(connection);
        drop(stored);
        let backend = if deadline && stage == "execute" {
            let diagnostics = self.clone();
            tokio::spawn(async move {
                let backend = diagnostics.backend_wait().await;
                tracing::info!(%batch_id, queue, %backend, "data save timeout backend diagnostic");
            });
            "scheduled_after_disconnect".into()
        } else {
            "not_sampled".into()
        };
        Err(WriteFailure {
            queue,
            batch_id,
            stage,
            kind,
            elapsed_ms: started.elapsed().as_millis(),
            sqlstate,
            backend,
            source,
        }
        .into())
    }

    async fn backend_wait(&self) -> String {
        let mut connection = None;
        let query = async {
            connection = Some(self.diagnostics_pool.acquire().await?.detach());
            sqlx::query("SELECT pid, state, wait_event_type, wait_event, pg_blocking_pids(pid)::text AS blockers FROM pg_stat_activity WHERE application_name=$1")
                .bind(&*self.application_name).fetch_all(connection.as_mut().expect("diagnostic connection")).await
        };
        let result = tokio::time::timeout(Duration::from_millis(500), query).await;
        drop(connection);
        match result {
            Ok(Ok(rows)) if rows.is_empty() => "not_visible_through_proxy".into(),
            Ok(Ok(rows)) => rows
                .iter()
                .map(|row| {
                    format!(
                        "pid={:?} state={:?} wait={:?}/{:?} blockers={:?}",
                        row.try_get::<i32, _>("pid").ok(),
                        row.try_get::<String, _>("state").ok(),
                        row.try_get::<String, _>("wait_event_type").ok(),
                        row.try_get::<String, _>("wait_event").ok(),
                        row.try_get::<String, _>("blockers").ok()
                    )
                })
                .collect::<Vec<_>>()
                .join("; "),
            Ok(Err(_)) => "diagnostics_unavailable".into(),
            Err(_) => "diagnostics_timeout".into(),
        }
    }
}

trait Operation: Send {
    fn run(
        self,
        connection: &mut PgConnection,
        id: Uuid,
        queue: &'static str,
    ) -> impl Future<Output = Result<u64>> + Send;
}

impl Operation for PreparedBatch {
    async fn run(
        self,
        connection: &mut PgConnection,
        id: Uuid,
        queue: &'static str,
    ) -> Result<u64> {
        let sql = format!("WITH data_save_receipt AS (INSERT INTO data_save_batches (batch_id, queue) VALUES ($2, $3) ON CONFLICT (batch_id) DO NOTHING RETURNING batch_id), data_save_input AS (SELECT $1::jsonb AS payload WHERE EXISTS (SELECT 1 FROM data_save_receipt)) {}", self.sql);
        if self.select_count {
            let count: i64 = sqlx::query_scalar(&sql)
                .bind(MeasuredJson(&self.rows))
                .bind(id)
                .bind(queue)
                .fetch_one(connection)
                .await?;
            Ok(u64::try_from(count)?)
        } else {
            Ok(sqlx::query(&sql)
                .bind(MeasuredJson(&self.rows))
                .bind(id)
                .bind(queue)
                .execute(connection)
                .await?
                .rows_affected())
        }
    }
}

struct Transactional<'a, B>(&'a B);

impl<B: TransactionBatch> Operation for Transactional<'_, B> {
    async fn run(
        self,
        connection: &mut PgConnection,
        id: Uuid,
        queue: &'static str,
    ) -> Result<u64> {
        let mut transaction = connection.begin().await?;
        let inserted = sqlx::query("INSERT INTO data_save_batches (batch_id, queue) VALUES ($1, $2) ON CONFLICT (batch_id) DO NOTHING")
            .bind(id).bind(queue).execute(&mut *transaction).await?.rows_affected();
        let rows = if inserted == 0 {
            0
        } else {
            self.0.execute(&mut transaction).await?
        };
        transaction.commit().await?;
        Ok(rows)
    }
}
