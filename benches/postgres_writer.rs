use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{ensure, Result};
use data_save_manager::postgres::{DatabaseWriter, MeasuredJson, PreparedBatch, TransactionBatch};
use data_save_manager::WriteTimeouts;
use serde_json::{json, Value};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, PgConnection};
use uuid::Uuid;

const SQL: &str = "INSERT INTO samples SELECT * FROM jsonb_populate_recordset(NULL::samples, (SELECT payload FROM data_save_input))";
const BATCHES: usize = 40;
const ROWS: usize = 500;

struct CountingAllocator;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(size, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, size) }
    }
}

struct MultiStatement<'a>(&'a Value);

impl TransactionBatch for MultiStatement<'_> {
    async fn execute<'a>(&'a self, connection: &'a mut PgConnection) -> Result<u64> {
        Ok(sqlx::query(
            "INSERT INTO samples SELECT * FROM jsonb_populate_recordset(NULL::samples, $1)",
        )
        .bind(MeasuredJson(self.0))
        .execute(connection)
        .await?
        .rows_affected())
    }
}

async fn benchmark() -> Result<()> {
    let options: PgConnectOptions = std::env::var("DSM_TEST_DATABASE_URL")?.parse()?;
    ensure!(
        matches!(options.get_host(), "localhost" | "127.0.0.1" | "::1"),
        "benchmark requires local PostgreSQL"
    );
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options.clone())
        .await?;
    let schema = format!("dsm_bench_{}", Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await?;
    let options = options.options([("search_path", schema.as_str())]);
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options.clone())
        .await?;
    sqlx::raw_sql("CREATE TABLE data_save_batches(batch_id uuid PRIMARY KEY, queue text NOT NULL, committed_at timestamptz NOT NULL DEFAULT now()); CREATE TABLE samples(id bigint, payload text);").execute(&pool).await?;
    let writer = DatabaseWriter::new(pool.clone(), WriteTimeouts::default());
    writer.check_ready().await?;
    let mut legacy = PgConnection::connect_with(&options).await?;
    let payload = Value::Array(
        (0..ROWS)
            .map(|id| json!({"id": id, "payload": "x".repeat(128)}))
            .collect(),
    );
    let mut samples: [Vec<Duration>; 3] = Default::default();
    let mut allocations: [Vec<usize>; 3] = Default::default();
    let mut allocated_bytes: [Vec<usize>; 3] = Default::default();
    for round in 0..8 {
        for variant in [round % 3, (round + 1) % 3, (round + 2) % 3] {
            sqlx::raw_sql("TRUNCATE samples, data_save_batches")
                .execute(&pool)
                .await?;
            ALLOCATIONS.store(0, Ordering::Relaxed);
            ALLOCATED_BYTES.store(0, Ordering::Relaxed);
            let started = Instant::now();
            for _ in 0..BATCHES {
                let id = Uuid::new_v4();
                let affected = match variant {
                    0 => {
                        let prepared = PreparedBatch::new(SQL, payload.clone());
                        let sql = format!("WITH data_save_receipt AS (INSERT INTO data_save_batches (batch_id, queue) VALUES ($2, $3) ON CONFLICT (batch_id) DO NOTHING RETURNING batch_id), data_save_input AS (SELECT $1::jsonb AS payload WHERE EXISTS (SELECT 1 FROM data_save_receipt)) {}", prepared.sql);
                        sqlx::query(&sql)
                            .bind(MeasuredJson(&prepared.rows))
                            .bind(id)
                            .bind("bench")
                            .execute(&mut legacy)
                            .await?
                            .rows_affected()
                    }
                    1 => {
                        writer
                            .write_prepared(id, "bench", || {
                                Ok(PreparedBatch::new(SQL, payload.clone()))
                            })
                            .await?
                    }
                    _ => {
                        writer
                            .write_transaction(id, "bench", &MultiStatement(&payload))
                            .await?
                    }
                };
                ensure!(affected == ROWS as u64, "incorrect acknowledgement");
            }
            let elapsed = started.elapsed();
            let calls = ALLOCATIONS.load(Ordering::Relaxed);
            let bytes = ALLOCATED_BYTES.load(Ordering::Relaxed);
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM samples")
                .fetch_one(&pool)
                .await?;
            ensure!(
                count == (BATCHES * ROWS) as i64,
                "incorrect saved row count"
            );
            if round != 0 {
                samples[variant].push(elapsed);
                allocations[variant].push(calls);
                allocated_bytes[variant].push(bytes);
            }
        }
    }
    for (index, (name, measurements)) in [
        "existing receipt SQL",
        "shared prepared writer",
        "shared multi-statement writer",
    ]
    .iter()
    .zip(samples.iter_mut())
    .enumerate()
    {
        measurements.sort();
        let median = measurements[measurements.len() / 2];
        allocations[index].sort();
        allocated_bytes[index].sort();
        println!(
            "{name}: median_ms/batch={:.3} rows/s={:.0} allocs/batch={:.1} allocated_bytes/batch={:.0}",
            median.as_secs_f64() * 1000.0 / BATCHES as f64,
            (BATCHES * ROWS) as f64 / median.as_secs_f64(),
            allocations[index][allocations[index].len() / 2] as f64 / BATCHES as f64,
            allocated_bytes[index][allocated_bytes[index].len() / 2] as f64 / BATCHES as f64
        );
    }
    drop(writer);
    legacy.close().await?;
    pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await?;
    admin.close().await;
    Ok(())
}

fn main() -> Result<()> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(benchmark())
}
