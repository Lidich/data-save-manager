use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use opentelemetry::metrics::{Counter, Histogram, Meter};
use opentelemetry::KeyValue;

/// Register once per queue/attribute set and clone handles; duplicate registrations are unsupported.
/// Cumulative totals live with the meter, while gauges stop observing dropped queue state.
#[derive(Clone)]
pub struct QueueTelemetry(Arc<Instruments>);

struct Observations {
    depth: Arc<AtomicUsize>,
    in_flight: AtomicUsize,
    attributes: Vec<KeyValue>,
}

struct Totals {
    accepted: AtomicU64,
    payload_bytes: AtomicU64,
    attributes: Vec<KeyValue>,
}

struct Instruments {
    observations: Arc<Observations>,
    totals: Arc<Totals>,
    success_attributes: Vec<KeyValue>,
    failure_attributes: Vec<KeyValue>,
    acknowledged: Counter<u64>,
    attempted_items: Counter<u64>,
    attempts: Counter<u64>,
    retries: Counter<u64>,
    writer_rows: Counter<u64>,
    outcomes: Counter<u64>,
    duration: Histogram<f64>,
    age: Histogram<f64>,
    size: Histogram<u64>,
}

pub(crate) struct Attempt<'a> {
    telemetry: &'a QueueTelemetry,
    started: Instant,
    oldest_enqueued_at: Instant,
    len: usize,
}

impl QueueTelemetry {
    /// Registers metrics with canonical queue/outcome labels and caller-supplied project attributes.
    pub fn new(
        meter: &Meter,
        queue: &'static str,
        depth: Arc<AtomicUsize>,
        mut attributes: Vec<KeyValue>,
    ) -> Self {
        attributes.retain(|attribute| !matches!(attribute.key.as_str(), "queue" | "outcome"));
        attributes.push(KeyValue::new("queue", queue));
        let mut success_attributes = attributes.clone();
        success_attributes.push(KeyValue::new("outcome", "success"));
        let mut failure_attributes = attributes.clone();
        failure_attributes.push(KeyValue::new("outcome", "failure"));
        let totals = Arc::new(Totals {
            accepted: AtomicU64::new(0),
            payload_bytes: AtomicU64::new(0),
            attributes: attributes.clone(),
        });
        let observations = Arc::new(Observations {
            depth,
            in_flight: AtomicUsize::new(0),
            attributes,
        });
        let weak = Arc::downgrade(&observations);
        meter
            .u64_observable_gauge("data_save_pending")
            .with_description("Accepted items including queued, in-flight and retry batches")
            .with_unit("{item}")
            .with_callback(move |observer| {
                if let Some(state) = weak.upgrade() {
                    observer.observe(
                        state.depth.load(Ordering::Relaxed) as u64,
                        &state.attributes,
                    );
                }
            })
            .build();
        let weak = Arc::downgrade(&observations);
        meter
            .u64_observable_gauge("data_save_in_flight")
            .with_description("Active writer attempts, cleared on completion or cancellation")
            .with_unit("{attempt}")
            .with_callback(move |observer| {
                if let Some(state) = weak.upgrade() {
                    observer.observe(
                        state.in_flight.load(Ordering::Relaxed) as u64,
                        &state.attributes,
                    );
                }
            })
            .build();
        let accepted_totals = Arc::clone(&totals);
        meter
            .u64_observable_counter("data_save_accepted_items_total")
            .with_description("Items successfully admitted to the queue")
            .with_unit("{item}")
            .with_callback(move |observer| {
                observer.observe(
                    accepted_totals.accepted.load(Ordering::Relaxed),
                    &accepted_totals.attributes,
                );
            })
            .build();
        let payload_totals = Arc::clone(&totals);
        meter
            .u64_observable_counter("data_save_payload_bytes_total")
            .with_description(
                "Encoded payload bytes including retries, not transmitted, committed or disk bytes",
            )
            .with_unit("By")
            .with_callback(move |observer| {
                observer.observe(
                    payload_totals.payload_bytes.load(Ordering::Relaxed),
                    &payload_totals.attributes,
                );
            })
            .build();
        Self(Arc::new(Instruments {
            observations,
            totals,
            success_attributes,
            failure_attributes,
            acknowledged: meter
                .u64_counter("data_save_acknowledged_items_total")
                .with_description(
                    "Queue items acknowledged by successful writes, independent of affected rows",
                )
                .with_unit("{item}")
                .build(),
            attempted_items: meter
                .u64_counter("data_save_attempted_items_total")
                .with_description(
                    "Items submitted to writer attempts including retries and cancelled attempts",
                )
                .with_unit("{item}")
                .build(),
            attempts: meter
                .u64_counter("data_save_attempts_total")
                .with_description("Started writer attempts including cancelled attempts")
                .with_unit("{attempt}")
                .build(),
            retries: meter
                .u64_counter("data_save_retries_total")
                .with_description(
                    "Writer attempts after a previous failure or cancellation of the same batch",
                )
                .with_unit("{attempt}")
                .build(),
            writer_rows: meter
                .u64_counter("data_save_writer_rows_total")
                .with_description("Affected rows returned by successful writers, not queue items")
                .with_unit("{row}")
                .build(),
            outcomes: meter
                .u64_counter("data_save_flush_total")
                .with_description("Completed writer attempts by success or failure")
                .build(),
            duration: meter
                .f64_histogram("data_save_flush_duration_seconds")
                .with_description("Completed writer attempt duration")
                .with_boundaries(vec![
                    0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0,
                ])
                .with_unit("s")
                .build(),
            age: meter
                .f64_histogram("data_save_batch_age_seconds")
                .with_description(
                    "Oldest item age in the attempted batch at completion, not whole queue age",
                )
                .with_boundaries(vec![0.1, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0])
                .with_unit("s")
                .build(),
            size: meter
                .u64_histogram("data_save_batch_size")
                .with_description(
                    "Items per started writer attempt including retries and cancellations",
                )
                .with_unit("{item}")
                .build(),
        }))
    }

    /// Atomically counts successfully enqueued items without changing queue depth.
    pub fn accepted(&self, count: usize) {
        self.0
            .totals
            .accepted
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    /// Atomically counts actual encoded payload bytes, including encoding repeated on retries.
    pub fn record_payload_bytes(&self, bytes: usize) {
        self.0
            .totals
            .payload_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn start(
        &self,
        len: usize,
        oldest_enqueued_at: Instant,
        retry: bool,
    ) -> Attempt<'_> {
        self.0
            .observations
            .in_flight
            .fetch_add(1, Ordering::Relaxed);
        let mut attempt = Attempt {
            telemetry: self,
            started: Instant::now(),
            oldest_enqueued_at,
            len,
        };
        let attrs = &self.0.observations.attributes;
        self.0.attempts.add(1, attrs);
        self.0.attempted_items.add(len as u64, attrs);
        if retry {
            self.0.retries.add(1, attrs);
        }
        self.0.size.record(len as u64, attrs);
        attempt.started = Instant::now();
        attempt
    }
}

impl Attempt<'_> {
    pub(crate) fn complete(self, rows: Option<u64>) {
        let elapsed = self.started.elapsed().as_secs_f64();
        let age = self.oldest_enqueued_at.elapsed().as_secs_f64();
        let metrics = &self.telemetry.0;
        let attributes = if let Some(rows) = rows {
            metrics
                .acknowledged
                .add(self.len as u64, &metrics.observations.attributes);
            metrics
                .writer_rows
                .add(rows, &metrics.observations.attributes);
            &metrics.success_attributes
        } else {
            &metrics.failure_attributes
        };
        metrics.outcomes.add(1, attributes);
        metrics.duration.record(elapsed, attributes);
        metrics.age.record(age, attributes);
    }
}

impl Drop for Attempt<'_> {
    fn drop(&mut self) {
        self.telemetry
            .0
            .observations
            .in_flight
            .fetch_sub(1, Ordering::Relaxed);
    }
}
