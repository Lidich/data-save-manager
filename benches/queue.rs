use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use data_save_manager::{Admission, BatchIdentity, BatchQueue, BatchWriter, Queued};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);

struct Allocator;

unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Allocator = Allocator;

struct Writer;

impl<Id: BatchIdentity> BatchWriter<[u64; 8], Id> for Writer {
    type Error = ();
    async fn write(&self, id: Id, _: &'static str, items: &[[u64; 8]]) -> Result<u64, ()> {
        black_box(id);
        black_box(items);
        Ok(0)
    }
}

struct LegacyAdmission {
    accepting: AtomicBool,
    inflight: AtomicUsize,
}

impl LegacyAdmission {
    fn new() -> Self {
        Self {
            accepting: AtomicBool::new(true),
            inflight: AtomicUsize::new(0),
        }
    }

    #[inline]
    fn enqueue(
        &self,
        sender: &UnboundedSender<Queued<[u64; 8]>>,
        depth: &AtomicUsize,
        value: [u64; 8],
    ) {
        self.inflight.fetch_add(1, Ordering::AcqRel);
        if !self.accepting.load(Ordering::Acquire) {
            self.inflight.fetch_sub(1, Ordering::AcqRel);
            return;
        }
        depth.fetch_add(1, Ordering::Relaxed);
        if sender
            .send(Queued {
                value,
                enqueued_at: Instant::now(),
            })
            .is_err()
        {
            depth.fetch_sub(1, Ordering::Relaxed);
        }
        self.inflight.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Clone, Copy)]
struct Measurement {
    elapsed: Duration,
    allocations: usize,
    bytes: usize,
}

fn measure(operation: impl FnOnce()) -> Measurement {
    let allocations = ALLOCATIONS.load(Ordering::Relaxed);
    let bytes = BYTES.load(Ordering::Relaxed);
    let start = Instant::now();
    operation();
    Measurement {
        elapsed: start.elapsed(),
        allocations: ALLOCATIONS.load(Ordering::Relaxed) - allocations,
        bytes: BYTES.load(Ordering::Relaxed) - bytes,
    }
}

fn producer_run(shared: bool, producers: usize, rows: usize) -> Measurement {
    let (tx, mut rx) = unbounded_channel();
    let admission = Admission::default();
    let old = LegacyAdmission::new();
    let depth = AtomicUsize::new(0);
    let result = measure(|| {
        std::thread::scope(|scope| {
            for _ in 0..producers {
                let tx = &tx;
                let admission = &admission;
                let old = &old;
                let depth = &depth;
                scope.spawn(move || {
                    if shared {
                        for row in 0..rows / producers {
                            let _guard = admission.enter().unwrap();
                            depth.fetch_add(1, Ordering::Relaxed);
                            if tx
                                .send(Queued {
                                    value: [row as u64; 8],
                                    enqueued_at: Instant::now(),
                                })
                                .is_err()
                            {
                                depth.fetch_sub(1, Ordering::Relaxed);
                            }
                        }
                    } else {
                        for row in 0..rows / producers {
                            old.enqueue(tx, depth, [row as u64; 8]);
                        }
                    }
                });
            }
        });
    });
    assert_eq!(depth.load(Ordering::Relaxed), rows);
    for _ in 0..rows {
        black_box(rx.try_recv().unwrap());
    }
    assert!(rx.try_recv().is_err());
    result
}

fn batch_run<Id: BatchIdentity>(
    runtime: &tokio::runtime::Runtime,
    shared: bool,
    rows: usize,
) -> Measurement {
    let (tx, mut rx) = unbounded_channel();
    let depth = Arc::new(AtomicUsize::new(rows));
    for row in 0..rows {
        tx.send(Queued {
            value: [row as u64; 8],
            enqueued_at: Instant::now(),
        })
        .unwrap();
    }
    measure(|| {
        runtime.block_on(async {
            if shared {
                let mut queue = BatchQueue::<_, Id>::new("bench", depth.clone(), None, rx);
                while !queue.is_empty() {
                    queue.flush(&Writer, &mut ()).await;
                }
            } else {
                loop {
                    let mut batch = Vec::with_capacity(500);
                    let mut oldest = None;
                    while batch.len() < 500 {
                        let Ok(queued) = rx.try_recv() else { break };
                        oldest =
                            Some(oldest.map_or(queued.enqueued_at, |at: Instant| {
                                at.min(queued.enqueued_at)
                            }));
                        batch.push(queued.value);
                    }
                    let Some(oldest) = oldest else { break };
                    let started = Instant::now();
                    Writer
                        .write(Id::new_batch(), "bench", &batch)
                        .await
                        .unwrap();
                    depth.fetch_sub(batch.len(), Ordering::Relaxed);
                    black_box(oldest.elapsed());
                    black_box(started.elapsed());
                }
            }
            assert_eq!(depth.load(Ordering::Relaxed), 0);
        })
    })
}

fn compare(label: &str, rows: usize, mut run: impl FnMut(bool) -> Measurement) {
    run(false);
    run(true);
    let mut old = Vec::new();
    let mut shared = Vec::new();
    for round in 0..9 {
        for variant in if round % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        } {
            let sample = run(variant);
            if variant {
                shared.push(sample);
            } else {
                old.push(sample);
            }
        }
    }
    old.sort_by_key(|sample| sample.elapsed);
    shared.sort_by_key(|sample| sample.elapsed);
    let old = old[old.len() / 2];
    let shared = shared[shared.len() / 2];
    println!("{label}: rows={rows} legacy_ns/row={:.2} shared_ns/row={:.2} shared/legacy={:.3} allocs={}/{} allocated_bytes={}/{}",
        old.elapsed.as_nanos() as f64 / rows as f64,
        shared.elapsed.as_nanos() as f64 / rows as f64,
        shared.elapsed.as_secs_f64() / old.elapsed.as_secs_f64(),
        old.allocations, shared.allocations, old.bytes, shared.bytes);
}

fn main() {
    let rows = 128_000;
    for producers in [1, 4, 16] {
        compare(&format!("enqueue producers={producers}"), rows, |shared| {
            producer_run(shared, producers, rows)
        });
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    compare("batch no receipt", rows, |shared| {
        batch_run::<()>(&runtime, shared, rows)
    });
    #[cfg(feature = "receipt-id")]
    compare("batch with receipt", rows, |shared| {
        batch_run::<uuid::Uuid>(&runtime, shared, rows)
    });
}
