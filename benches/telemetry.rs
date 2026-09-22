use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use data_save_manager::{Admission, BatchQueue, BatchWriter, QueueTelemetry, Queued};
use opentelemetry::metrics::MeterProvider;
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use tokio::sync::mpsc::unbounded_channel;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

struct Allocator;

unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Allocator = Allocator;

struct Writer;

impl BatchWriter<[u64; 8]> for Writer {
    type Error = ();

    async fn write(&self, _: (), _: &'static str, rows: &[[u64; 8]]) -> Result<u64, ()> {
        black_box(rows);
        Ok(rows.len() as u64)
    }
}

#[derive(Clone, Copy)]
struct Measurement {
    elapsed: Duration,
    allocations: usize,
}

fn measure(f: impl FnOnce()) -> Measurement {
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    let start = Instant::now();
    f();
    Measurement {
        elapsed: start.elapsed(),
        allocations: ALLOCATIONS.load(Ordering::Relaxed) - before,
    }
}

fn telemetry(depth: Arc<AtomicUsize>) -> (SdkMeterProvider, QueueTelemetry) {
    let provider = SdkMeterProvider::builder()
        .with_reader(
            PeriodicReader::builder(InMemoryMetricExporter::default())
                .with_interval(Duration::from_secs(3600))
                .build(),
        )
        .build();
    let metrics = QueueTelemetry::new(
        &provider.meter("data-save-manager"),
        "bench",
        depth,
        vec![
            KeyValue::new("project", "benchmark"),
            KeyValue::new("instId", "inst1"),
        ],
    );
    (provider, metrics)
}

fn producers(enabled: bool, count: usize, rows: usize) -> Measurement {
    let (sender, mut receiver) = unbounded_channel();
    let depth = Arc::new(AtomicUsize::new(0));
    let (_provider, metrics) = telemetry(depth.clone());
    let admission = Admission::default();
    let result = measure(|| {
        std::thread::scope(|scope| {
            for _ in 0..count {
                let sender = &sender;
                let depth = &depth;
                let metrics = &metrics;
                let admission = &admission;
                scope.spawn(move || {
                    for index in 0..rows / count {
                        let _guard = admission.enter().unwrap();
                        depth.fetch_add(1, Ordering::Relaxed);
                        sender
                            .send(Queued {
                                value: [index as u64; 8],
                                enqueued_at: Instant::now(),
                            })
                            .unwrap();
                        if enabled {
                            metrics.accepted(1);
                        }
                    }
                });
            }
        });
    });
    assert_eq!(depth.load(Ordering::Relaxed), rows);
    for _ in 0..rows {
        black_box(receiver.try_recv().unwrap());
    }
    assert!(receiver.try_recv().is_err());
    result
}

fn batches(runtime: &tokio::runtime::Runtime, enabled: bool, rows: usize) -> Measurement {
    let (sender, receiver) = unbounded_channel();
    let depth = Arc::new(AtomicUsize::new(1));
    let (_provider, metrics) = telemetry(depth.clone());
    let mut queue = BatchQueue::<_, ()>::new("bench", depth.clone(), None, receiver);
    if enabled {
        metrics.accepted(1);
        queue = queue.with_telemetry(metrics.clone());
    }
    sender
        .send(Queued {
            value: [0; 8],
            enqueued_at: Instant::now(),
        })
        .unwrap();
    runtime.block_on(queue.flush(&Writer, &mut ()));
    depth.store(rows, Ordering::Relaxed);
    for index in 0..rows {
        sender
            .send(Queued {
                value: [index as u64; 8],
                enqueued_at: Instant::now(),
            })
            .unwrap();
    }
    if enabled {
        metrics.accepted(rows);
    }
    measure(|| {
        runtime.block_on(async {
            while !queue.is_empty() {
                queue.flush(&Writer, &mut ()).await;
            }
        });
    })
}

fn compare(label: &str, rows: usize, mut run: impl FnMut(bool) -> Measurement) {
    run(false);
    run(true);
    let mut baseline = Vec::new();
    let mut instrumented = Vec::new();
    for round in 0..9 {
        for enabled in if round % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        } {
            let result = run(enabled);
            if enabled {
                instrumented.push(result);
            } else {
                baseline.push(result);
            }
        }
    }
    baseline.sort_by_key(|sample| sample.elapsed);
    instrumented.sort_by_key(|sample| sample.elapsed);
    let baseline = baseline[4];
    let instrumented = instrumented[4];
    println!(
        "{label}: rows={rows} off_ns/row={:.2} on_ns/row={:.2} ratio={:.3} allocations_off/on={}/{}",
        baseline.elapsed.as_nanos() as f64 / rows as f64,
        instrumented.elapsed.as_nanos() as f64 / rows as f64,
        instrumented.elapsed.as_secs_f64() / baseline.elapsed.as_secs_f64(),
        baseline.allocations,
        instrumented.allocations,
    );
}

fn main() {
    let rows = 128_000;
    for count in [1, 4, 16] {
        compare(&format!("enqueue producers={count}"), rows, |enabled| {
            producers(enabled, count, rows)
        });
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    compare("batch writer", rows, |enabled| {
        batches(&runtime, enabled, rows)
    });
}
