use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use data_save_manager::{BatchInfo, BatchQueue, BatchWriter, FlushOutcome, Observer, Queued};
use tokio::sync::mpsc::unbounded_channel;

#[derive(Default)]
struct Recorder {
    failures: AtomicUsize,
    calls: Mutex<Vec<Vec<usize>>>,
}

impl BatchWriter<usize> for Recorder {
    type Error = &'static str;

    async fn write(&self, _: (), _: &'static str, items: &[usize]) -> Result<u64, Self::Error> {
        self.calls.lock().unwrap().push(items.to_vec());
        if self
            .failures
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
            .is_ok()
        {
            Err("database unavailable")
        } else {
            Ok(0)
        }
    }
}

#[derive(Default)]
struct Events {
    successes: Vec<BatchInfo>,
    failures: Vec<(BatchInfo, bool)>,
}

impl Observer<&'static str> for Events {
    fn success(&mut self, info: BatchInfo, _: u64, _: Duration) {
        self.successes.push(info);
    }

    fn failure(&mut self, info: BatchInfo, _: &&'static str, _: Duration, alert: bool) {
        self.failures.push((info, alert));
    }
}

fn queue(items: usize) -> (BatchQueue<usize>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let (tx, rx) = unbounded_channel();
    for value in 0..items {
        tx.send(Queued {
            value,
            enqueued_at: Instant::now(),
        })
        .unwrap();
    }
    let depth = Arc::new(AtomicUsize::new(items));
    let total = Arc::new(AtomicUsize::new(items));
    (
        BatchQueue::new("test", depth.clone(), Some(total.clone()), rx),
        depth,
        total,
    )
}

#[tokio::test(start_paused = true)]
async fn batches_are_bounded_and_zero_rows_acknowledges_inputs() {
    let (mut queue, depth, total) = queue(501);
    let writer = Recorder::default();
    let mut events = Events::default();
    assert_eq!(
        queue.flush(&writer, &mut events).await,
        FlushOutcome::Success
    );
    assert_eq!(depth.load(Ordering::Relaxed), 1);
    assert_eq!(total.load(Ordering::Relaxed), 1);
    assert_eq!(events.successes[0].len, 500);
    assert_eq!(events.successes[0].pending, 1);
    queue.flush(&writer, &mut events).await;
    assert_eq!(writer.calls.lock().unwrap()[1], vec![500]);
    assert_eq!(depth.load(Ordering::Relaxed), 0);
    assert_eq!(total.load(Ordering::Relaxed), 0);
    assert!(queue.is_empty());
    assert!(!queue.is_ready());
    assert_eq!(queue.flush(&writer, &mut events).await, FlushOutcome::Idle);
}

#[tokio::test(start_paused = true)]
async fn failures_retain_payload_age_fifo_and_depth_without_splitting() {
    let (mut queue, depth, total) = queue(501);
    let writer = Recorder {
        failures: AtomicUsize::new(6),
        ..Default::default()
    };
    let mut events = Events::default();
    for attempt in 1..=6 {
        assert_eq!(
            queue.flush(&writer, &mut events).await,
            FlushOutcome::Retained
        );
        assert!(!queue.is_ready());
        assert!(!queue.is_empty());
        assert_eq!(depth.load(Ordering::Relaxed), 501);
        assert_eq!(total.load(Ordering::Relaxed), 501);
        assert_eq!(events.failures.last().unwrap().0.attempt, attempt);
        assert_eq!(queue.flush(&writer, &mut events).await, FlushOutcome::Idle);
        tokio::time::advance(Duration::from_secs(1)).await;
    }
    assert_eq!(
        events.failures.iter().filter(|(_, alert)| *alert).count(),
        1
    );
    assert!(events.failures[4].1);
    assert!(events
        .failures
        .iter()
        .all(|(info, _)| info.oldest_enqueued_at == events.failures[0].0.oldest_enqueued_at));
    queue.flush(&writer, &mut events).await;
    queue.flush(&writer, &mut events).await;
    let calls = writer.calls.lock().unwrap();
    assert!(calls[..7].iter().all(|items| items == &calls[0]));
    assert_eq!(calls[7], vec![500]);
    assert!(queue.is_empty());
}

#[tokio::test(start_paused = true)]
async fn failed_queue_does_not_prevent_another_queue_progress() {
    let (mut bad, _, _) = queue(1);
    let (mut healthy, _, _) = queue(1);
    let failing = Recorder {
        failures: AtomicUsize::new(1),
        ..Default::default()
    };
    bad.flush(&failing, &mut ()).await;
    assert!(!bad.is_ready());
    assert!(healthy.is_ready());
    healthy.flush(&Recorder::default(), &mut ()).await;
    assert!(healthy.is_empty());
    assert!(!bad.is_empty());
}

#[tokio::test]
async fn failure_diagnostics_include_items_accepted_during_the_write() {
    struct GrowingWriter(Arc<AtomicUsize>);
    impl BatchWriter<usize> for GrowingWriter {
        type Error = &'static str;
        async fn write(&self, _: (), _: &'static str, _: &[usize]) -> Result<u64, Self::Error> {
            self.0.fetch_add(2, Ordering::Relaxed);
            Err("slow write failed")
        }
    }
    let (mut queue, depth, _) = queue(1);
    let mut events = Events::default();
    queue.flush(&GrowingWriter(depth), &mut events).await;
    assert_eq!(events.failures[0].0.pending, 3);
}

struct NeverCompletes;

impl BatchWriter<usize> for NeverCompletes {
    type Error = &'static str;

    async fn write(&self, _: (), _: &'static str, _: &[usize]) -> Result<u64, Self::Error> {
        std::future::pending().await
    }
}

#[tokio::test(start_paused = true)]
async fn cancelled_flush_keeps_the_owned_batch_for_retry() {
    let (mut queue, depth, _) = queue(3);
    assert!(tokio::time::timeout(
        Duration::from_secs(1),
        queue.flush(&NeverCompletes, &mut ())
    )
    .await
    .is_err());
    assert_eq!(depth.load(Ordering::Relaxed), 3);
    let writer = Recorder::default();
    queue.flush(&writer, &mut ()).await;
    assert_eq!(writer.calls.lock().unwrap()[0], vec![0, 1, 2]);
    assert!(queue.is_empty());
}

#[tokio::test]
async fn oldest_timestamp_is_minimum_even_for_concurrent_producer_order() {
    let (tx, rx) = unbounded_channel();
    let old = Instant::now() - Duration::from_secs(2);
    tx.send(Queued {
        value: 1,
        enqueued_at: Instant::now(),
    })
    .unwrap();
    tx.send(Queued {
        value: 2,
        enqueued_at: old,
    })
    .unwrap();
    let mut queue = BatchQueue::new("age", Arc::new(AtomicUsize::new(2)), None, rx);
    let mut events = Events::default();
    queue.flush(&Recorder::default(), &mut events).await;
    assert_eq!(events.successes[0].oldest_enqueued_at, old);
}

struct OwnedOnly(String);
struct OwnedWriter;

impl BatchWriter<OwnedOnly> for OwnedWriter {
    type Error = &'static str;

    async fn write(&self, _: (), _: &'static str, items: &[OwnedOnly]) -> Result<u64, Self::Error> {
        assert_eq!(items[0].0, "no Clone bound");
        Ok(1)
    }
}

#[tokio::test]
async fn payload_does_not_require_clone_or_serialization() {
    let (tx, rx) = unbounded_channel();
    tx.send(Queued {
        value: OwnedOnly("no Clone bound".into()),
        enqueued_at: Instant::now(),
    })
    .ok()
    .unwrap();
    let mut queue = BatchQueue::new("owned", Arc::new(AtomicUsize::new(1)), None, rx);
    assert_eq!(
        queue.flush(&OwnedWriter, &mut ()).await,
        FlushOutcome::Success
    );
}

#[cfg(feature = "receipt-id")]
#[tokio::test(start_paused = true)]
async fn receipt_id_is_stable_across_errors_and_cancellation() {
    struct Writer(Mutex<Vec<uuid::Uuid>>, AtomicUsize);
    impl BatchWriter<usize, uuid::Uuid> for Writer {
        type Error = &'static str;
        async fn write(
            &self,
            id: uuid::Uuid,
            _: &'static str,
            _: &[usize],
        ) -> Result<u64, Self::Error> {
            self.0.lock().unwrap().push(id);
            match self.1.fetch_add(1, Ordering::Relaxed) {
                0 => Err("lost commit acknowledgement"),
                1 => std::future::pending().await,
                _ => Ok(0),
            }
        }
    }
    let (tx, rx) = unbounded_channel();
    tx.send(Queued {
        value: 1,
        enqueued_at: Instant::now(),
    })
    .unwrap();
    let mut queue = BatchQueue::new("receipt", Arc::new(AtomicUsize::new(1)), None, rx);
    let writer = Writer(Mutex::default(), AtomicUsize::new(0));
    queue.flush(&writer, &mut ()).await;
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(1), queue.flush(&writer, &mut ()))
            .await
            .is_err()
    );
    queue.flush(&writer, &mut ()).await;
    let ids = writer.0.lock().unwrap();
    assert_eq!(ids.len(), 3);
    assert!(!ids[0].is_nil());
    assert!(ids.iter().all(|id| *id == ids[0]));
}
