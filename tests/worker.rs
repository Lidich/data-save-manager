use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use data_save_manager::{
    Admission, BatchQueue, BatchWriter, FlushCycle, Queued, Worker, WorkerCadence, WorkerEvent,
};
use tokio::sync::mpsc::unbounded_channel;

struct Cycle {
    remaining: Arc<AtomicUsize>,
    events: Arc<Mutex<Vec<WorkerEvent>>>,
}

impl FlushCycle for Cycle {
    async fn flush(&mut self) {
        self.remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
            .ok();
    }
    fn is_empty(&self) -> bool {
        self.remaining.load(Ordering::Relaxed) == 0
    }
    fn has_ready_queue(&self) -> bool {
        !self.is_empty()
    }
    fn on_event(&self, event: WorkerEvent) {
        self.events.lock().unwrap().push(event);
    }
}

#[tokio::test(start_paused = true)]
async fn drain_waits_for_admitted_producers_and_saves_every_item() {
    let admission = Admission::default();
    let guard = admission.enter().unwrap();
    let remaining = Arc::new(AtomicUsize::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));
    let worker = Worker::spawn(
        admission.clone(),
        Cycle {
            remaining: remaining.clone(),
            events: events.clone(),
        },
    );
    let shutdown = tokio::spawn(worker.shutdown());
    tokio::task::yield_now().await;
    assert!(!shutdown.is_finished());
    assert!(admission.enter().is_none());
    remaining.store(100, Ordering::Relaxed);
    drop(guard);
    shutdown.await.unwrap().unwrap();
    assert_eq!(remaining.load(Ordering::Relaxed), 0);
    assert_eq!(
        *events.lock().unwrap(),
        vec![
            WorkerEvent::Started,
            WorkerEvent::DrainStarted,
            WorkerEvent::DrainCompleted
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn backlog_is_drained_without_waiting_a_tick_per_batch() {
    let remaining = Arc::new(AtomicUsize::new(100));
    let worker = Worker::spawn(
        Admission::default(),
        Cycle {
            remaining: remaining.clone(),
            events: Arc::default(),
        },
    );
    for _ in 0..250 {
        tokio::task::yield_now().await;
    }
    assert_eq!(remaining.load(Ordering::Relaxed), 0);
    worker.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn interval_cadence_preserves_normal_write_rate_but_drains_immediately() {
    let remaining = Arc::new(AtomicUsize::new(100));
    let worker = Worker::spawn_with_cadence(
        Admission::default(),
        Cycle {
            remaining: remaining.clone(),
            events: Arc::default(),
        },
        WorkerCadence::Interval,
    );
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(remaining.load(Ordering::Relaxed), 99);
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(remaining.load(Ordering::Relaxed), 98);
    worker.shutdown().await.unwrap();
    assert_eq!(remaining.load(Ordering::Relaxed), 0);
}

#[tokio::test(start_paused = true)]
async fn cancelling_shutdown_does_not_abort_background_drain() {
    let admission = Admission::default();
    let guard = admission.enter().unwrap();
    let remaining = Arc::new(AtomicUsize::new(1));
    let events = Arc::new(Mutex::new(Vec::new()));
    let worker = Worker::spawn(
        admission.clone(),
        Cycle {
            remaining: remaining.clone(),
            events: events.clone(),
        },
    );
    let shutdown = tokio::spawn(worker.shutdown());
    tokio::task::yield_now().await;
    assert!(!shutdown.is_finished());
    shutdown.abort();
    assert!(shutdown.await.unwrap_err().is_cancelled());
    drop(guard);
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(remaining.load(Ordering::Relaxed), 0);
    assert_eq!(
        events.lock().unwrap().last(),
        Some(&WorkerEvent::DrainCompleted)
    );
    assert!(admission.enter().is_none());
}

#[tokio::test(start_paused = true)]
async fn unpolled_shutdown_still_closes_and_drains() {
    let admission = Admission::default();
    let remaining = Arc::new(AtomicUsize::new(1));
    let events = Arc::new(Mutex::new(Vec::new()));
    let worker = Worker::spawn(
        admission.clone(),
        Cycle {
            remaining: remaining.clone(),
            events: events.clone(),
        },
    );
    drop(worker.shutdown());
    assert!(admission.enter().is_none());
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(remaining.load(Ordering::Relaxed), 0);
    assert_eq!(
        events.lock().unwrap().last(),
        Some(&WorkerEvent::DrainCompleted)
    );
}

#[tokio::test(start_paused = true)]
async fn immediate_writers_yield_between_drain_cycles() {
    let remaining = Arc::new(AtomicUsize::new(10_000));
    let worker = Worker::spawn(
        Admission::default(),
        Cycle {
            remaining: remaining.clone(),
            events: Arc::default(),
        },
    );
    let shutdown = tokio::spawn(worker.shutdown());
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    assert!(remaining.load(Ordering::Relaxed) > 0);
    shutdown.await.unwrap().unwrap();
    assert_eq!(remaining.load(Ordering::Relaxed), 0);
}

#[test]
fn enqueue_guard_releases_on_panic() {
    let admission = Admission::default();
    let _ = std::panic::catch_unwind(|| {
        let _guard = admission.enter().unwrap();
        panic!("producer panic");
    });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(admission.close_and_wait());
    assert!(admission.enter().is_none());
}

struct ConcurrentWriter(Arc<AtomicUsize>);

impl BatchWriter<usize> for ConcurrentWriter {
    type Error = ();
    async fn write(&self, _: (), _: &'static str, items: &[usize]) -> Result<u64, ()> {
        self.0.fetch_add(items.len(), Ordering::Relaxed);
        Ok(items.len() as u64)
    }
}

struct QueueCycle {
    queue: BatchQueue<usize>,
    writer: ConcurrentWriter,
}

struct PanickingCycle;

impl FlushCycle for PanickingCycle {
    async fn flush(&mut self) {
        panic!("invalid writer adapter");
    }
    fn is_empty(&self) -> bool {
        false
    }
    fn has_ready_queue(&self) -> bool {
        true
    }
}

#[tokio::test(start_paused = true)]
async fn worker_panic_closes_admission_instead_of_accepting_more_items() {
    let admission = Admission::default();
    let worker = Worker::spawn(admission.clone(), PanickingCycle);
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert!(admission.enter().is_none());
    assert!(worker.shutdown().await.unwrap_err().is_panic());
}

impl FlushCycle for QueueCycle {
    async fn flush(&mut self) {
        self.queue.flush(&self.writer, &mut ()).await;
    }
    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
    fn has_ready_queue(&self) -> bool {
        self.queue.is_ready()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_shutdown_never_outruns_an_accepted_enqueue() {
    for _ in 0..20 {
        let (tx, rx) = unbounded_channel();
        let admission = Admission::default();
        let depth = Arc::new(AtomicUsize::new(0));
        let accepted = Arc::new(AtomicUsize::new(0));
        let saved = Arc::new(AtomicUsize::new(0));
        let worker = Worker::spawn(
            admission.clone(),
            QueueCycle {
                queue: BatchQueue::new("concurrent", depth.clone(), None, rx),
                writer: ConcurrentWriter(saved.clone()),
            },
        );
        let barrier = Arc::new(std::sync::Barrier::new(9));
        let mut producers = Vec::new();
        for _ in 0..8 {
            let admission = admission.clone();
            let depth = depth.clone();
            let accepted = accepted.clone();
            let tx = tx.clone();
            let barrier = barrier.clone();
            producers.push(std::thread::spawn(move || {
                barrier.wait();
                for value in 0..1000 {
                    let Some(_guard) = admission.enter() else {
                        break;
                    };
                    depth.fetch_add(1, Ordering::Relaxed);
                    tx.send(Queued {
                        value,
                        enqueued_at: std::time::Instant::now(),
                    })
                    .unwrap();
                    accepted.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        barrier.wait();
        while accepted.load(Ordering::Relaxed) < 10 {
            tokio::task::yield_now().await;
        }
        worker.shutdown().await.unwrap();
        for producer in producers {
            producer.join().unwrap();
        }
        assert_eq!(
            saved.load(Ordering::Relaxed),
            accepted.load(Ordering::Relaxed)
        );
        assert_eq!(depth.load(Ordering::Relaxed), 0);
    }
}
