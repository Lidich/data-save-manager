use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc::UnboundedReceiver;

use crate::QueueTelemetry;

const MAX_BATCH_SIZE: usize = 500;
const RETRY_DELAY: Duration = Duration::from_secs(1);
const ALERT_AFTER_FAILURES: u32 = 5;

pub struct Queued<T> {
    pub value: T,
    pub enqueued_at: Instant,
}

pub trait BatchIdentity: Copy + Send + Sync + 'static {
    fn new_batch() -> Self;
}

impl BatchIdentity for () {
    fn new_batch() {}
}

#[cfg(feature = "receipt-id")]
impl BatchIdentity for uuid::Uuid {
    fn new_batch() -> Self {
        Self::new_v4()
    }
}

/// Success acknowledges every input, including a retry returning zero SQL rows.
/// Adapters must make replays safe even when the previous commit outcome is unknown.
pub trait BatchWriter<T, Id = ()>: Send + Sync {
    type Error: Send;

    fn write<'a>(
        &'a self,
        id: Id,
        queue: &'static str,
        items: &'a [T],
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send + 'a;
}

#[derive(Clone, Copy, Debug)]
pub struct BatchInfo<Id = ()> {
    pub id: Id,
    pub queue: &'static str,
    pub len: usize,
    pub oldest_enqueued_at: Instant,
    pub attempt: u32,
    pub pending: usize,
}

pub trait Observer<E, Id = ()> {
    fn attempt(&mut self, _info: BatchInfo<Id>) {}
    fn success(&mut self, _info: BatchInfo<Id>, _rows: u64, _elapsed: Duration) {}
    fn failure(&mut self, _info: BatchInfo<Id>, _error: &E, _elapsed: Duration, _alert: bool) {}
}

impl<E, Id> Observer<E, Id> for () {}

struct PendingBatch<T, Id> {
    id: Id,
    items: Vec<T>,
    oldest_enqueued_at: Instant,
    failures: u32,
    alerted: bool,
    attempted: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FlushOutcome {
    Idle,
    Success,
    Retained,
}

pub struct BatchQueue<T, Id = ()> {
    pub name: &'static str,
    pub depth: Arc<AtomicUsize>,
    total_depth: Option<Arc<AtomicUsize>>,
    receiver: UnboundedReceiver<Queued<T>>,
    pending: Option<PendingBatch<T, Id>>,
    retry_after: Option<tokio::time::Instant>,
    telemetry: Option<QueueTelemetry>,
}

impl<T, Id: BatchIdentity> BatchQueue<T, Id> {
    /// Attaches shared queue instrumentation independently of logging observers.
    pub fn with_telemetry(mut self, telemetry: QueueTelemetry) -> Self {
        self.telemetry = Some(telemetry);
        self
    }

    pub fn new(
        name: &'static str,
        depth: Arc<AtomicUsize>,
        total_depth: Option<Arc<AtomicUsize>>,
        receiver: UnboundedReceiver<Queued<T>>,
    ) -> Self {
        Self {
            name,
            depth,
            total_depth,
            receiver,
            pending: None,
            retry_after: None,
            telemetry: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_none()
            && self.receiver.is_empty()
            && self.depth.load(Ordering::Relaxed) == 0
    }

    pub fn is_ready(&self) -> bool {
        !self.is_empty()
            && self
                .retry_after
                .is_none_or(|at| tokio::time::Instant::now() >= at)
    }

    fn prepare(&mut self) -> bool {
        if self
            .retry_after
            .is_some_and(|at| tokio::time::Instant::now() < at)
        {
            return false;
        }
        if self.pending.is_some() {
            return true;
        }
        let Ok(first) = self.receiver.try_recv() else {
            return false;
        };
        let mut items = Vec::with_capacity(MAX_BATCH_SIZE);
        items.push(first.value);
        let mut oldest_enqueued_at = first.enqueued_at;
        while items.len() < MAX_BATCH_SIZE {
            let Ok(queued) = self.receiver.try_recv() else {
                break;
            };
            oldest_enqueued_at = oldest_enqueued_at.min(queued.enqueued_at);
            items.push(queued.value);
        }
        self.pending = Some(PendingBatch {
            id: Id::new_batch(),
            items,
            oldest_enqueued_at,
            failures: 0,
            alerted: false,
            attempted: false,
        });
        true
    }

    /// Writes the owned batch without removing it while the future can be cancelled.
    pub async fn flush<W, O>(&mut self, writer: &W, observer: &mut O) -> FlushOutcome
    where
        W: BatchWriter<T, Id>,
        O: Observer<W::Error, Id>,
    {
        if !self.prepare() {
            return FlushOutcome::Idle;
        }
        let batch = self.pending.as_mut().expect("prepared batch");
        let mut info = BatchInfo {
            id: batch.id,
            queue: self.name,
            len: batch.items.len(),
            oldest_enqueued_at: batch.oldest_enqueued_at,
            attempt: batch.failures.saturating_add(1),
            pending: self.depth.load(Ordering::Relaxed),
        };
        let started = Instant::now();
        observer.attempt(info);
        let attempt = self.telemetry.as_ref().map(|telemetry| {
            telemetry.start(batch.items.len(), batch.oldest_enqueued_at, batch.attempted)
        });
        batch.attempted = true;
        let write = async { writer.write(batch.id, self.name, &batch.items).await };
        #[cfg(feature = "postgres")]
        let write = crate::postgres::scope_payload_telemetry(self.telemetry.clone(), write);
        match write.await {
            Ok(rows) => {
                if let Some(attempt) = attempt {
                    attempt.complete(Some(rows));
                }
                self.depth.fetch_sub(info.len, Ordering::Relaxed);
                if let Some(total) = &self.total_depth {
                    total.fetch_sub(info.len, Ordering::Relaxed);
                }
                self.pending = None;
                self.retry_after = None;
                info.pending = self.depth.load(Ordering::Relaxed);
                observer.success(info, rows, started.elapsed());
                FlushOutcome::Success
            }
            Err(error) => {
                if let Some(attempt) = attempt {
                    attempt.complete(None);
                }
                batch.failures = batch.failures.saturating_add(1);
                let alert = !batch.alerted && batch.failures >= ALERT_AFTER_FAILURES;
                batch.alerted |= alert;
                self.retry_after = Some(tokio::time::Instant::now() + RETRY_DELAY);
                info.pending = self.depth.load(Ordering::Relaxed);
                observer.failure(info, &error, started.elapsed(), alert);
                FlushOutcome::Retained
            }
        }
    }
}
