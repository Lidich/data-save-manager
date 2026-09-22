use std::future::{pending, poll_fn, Future};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use data_save_manager::{BatchQueue, BatchWriter, FlushOutcome, QueueTelemetry, Queued};
use opentelemetry::metrics::{Meter, MeterProvider};
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, Metric, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

struct Metrics {
    provider: SdkMeterProvider,
    exporter: InMemoryMetricExporter,
    meter: Meter,
}

impl Metrics {
    fn new() -> Self {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(
                PeriodicReader::builder(exporter.clone())
                    .with_interval(Duration::from_secs(3600))
                    .build(),
            )
            .build();
        let meter = provider.meter("queue-test");
        Self {
            provider,
            exporter,
            meter,
        }
    }

    fn collect(&self) -> ResourceMetrics {
        self.exporter.reset();
        self.provider.force_flush().unwrap();
        self.exporter
            .get_finished_metrics()
            .unwrap()
            .into_iter()
            .next()
            .unwrap_or_default()
    }
}

fn metric<'a>(snapshot: &'a ResourceMetrics, name: &str) -> Option<&'a Metric> {
    snapshot
        .scope_metrics()
        .flat_map(|scope| scope.metrics())
        .find(|metric| metric.name() == name)
}

fn points(snapshot: &ResourceMetrics, name: &str) -> Vec<(Vec<KeyValue>, u64)> {
    let Some(metric) = metric(snapshot, name) else {
        return Vec::new();
    };
    match metric.data() {
        AggregatedMetrics::U64(MetricData::Sum(sum)) => {
            assert!(sum.is_monotonic());
            sum.data_points()
                .map(|p| (p.attributes().cloned().collect(), p.value()))
                .collect()
        }
        AggregatedMetrics::U64(MetricData::Gauge(gauge)) => gauge
            .data_points()
            .map(|p| (p.attributes().cloned().collect(), p.value()))
            .collect(),
        data => panic!("unexpected {name} data: {data:?}"),
    }
}

fn total(snapshot: &ResourceMetrics, name: &str) -> u64 {
    points(snapshot, name).iter().map(|(_, value)| value).sum()
}

fn histogram(snapshot: &ResourceMetrics, name: &str) -> (u64, f64) {
    let metric = metric(snapshot, name).unwrap_or_else(|| panic!("missing {name}"));
    match metric.data() {
        AggregatedMetrics::F64(MetricData::Histogram(hist)) => hist
            .data_points()
            .fold((0, 0.0), |(n, sum), p| (n + p.count(), sum + p.sum())),
        AggregatedMetrics::U64(MetricData::Histogram(hist)) => {
            hist.data_points().fold((0, 0.0), |(n, sum), p| {
                (n + p.count(), sum + p.sum() as f64)
            })
        }
        data => panic!("unexpected {name} data: {data:?}"),
    }
}

fn assert_histogram_bounds(snapshot: &ResourceMetrics, name: &str, expected: &[f64]) {
    let metric = metric(snapshot, name).unwrap();
    let AggregatedMetrics::F64(MetricData::Histogram(histogram)) = metric.data() else {
        panic!("expected f64 histogram for {name}");
    };
    for point in histogram.data_points() {
        assert_eq!(point.bounds().collect::<Vec<_>>(), expected, "{name}");
    }
}

fn attrs(project: &'static str, instance: &'static str) -> Vec<KeyValue> {
    vec![
        KeyValue::new("project", project),
        KeyValue::new("instId", instance),
    ]
}

fn enqueue(
    sender: &UnboundedSender<Queued<usize>>,
    depth: &AtomicUsize,
    telemetry: Option<&QueueTelemetry>,
    value: usize,
) {
    depth.fetch_add(1, Ordering::Relaxed);
    sender
        .send(Queued {
            value,
            enqueued_at: Instant::now() - Duration::from_secs(7),
        })
        .unwrap();
    if let Some(telemetry) = telemetry {
        telemetry.accepted(1);
    }
}

struct Writer(Result<u64, ()>);

impl BatchWriter<usize> for Writer {
    type Error = ();
    async fn write(&self, _: (), _: &'static str, _: &[usize]) -> Result<u64, ()> {
        self.0
    }
}

struct WaitingWriter;

impl BatchWriter<usize> for WaitingWriter {
    type Error = ();
    async fn write(&self, _: (), _: &'static str, _: &[usize]) -> Result<u64, ()> {
        pending().await
    }
}

#[test]
fn producer_clones_count_once_and_collect_current_depth_without_mutating_it() {
    let metrics = Metrics::new();
    let depth = Arc::new(AtomicUsize::new(9));
    let telemetry = QueueTelemetry::new(
        &metrics.meter,
        "tokens",
        depth.clone(),
        attrs("chunpy", "a"),
    );
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let telemetry = telemetry.clone();
            scope.spawn(move || {
                for _ in 0..1000 {
                    telemetry.accepted(1);
                    telemetry.record_payload_bytes(3);
                }
            });
        }
    });
    assert_eq!(depth.load(Ordering::Relaxed), 9);
    let first = metrics.collect();
    assert_eq!(total(&first, "data_save_accepted_items_total"), 4000);
    assert_eq!(total(&first, "data_save_payload_bytes_total"), 12000);
    let pending = points(&first, "data_save_pending");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].1, 9);
    assert_eq!(pending[0].0.len(), 3);
    for attribute in [
        KeyValue::new("instId", "a"),
        KeyValue::new("project", "chunpy"),
        KeyValue::new("queue", "tokens"),
    ] {
        assert!(pending[0].0.contains(&attribute));
    }
    depth.store(2, Ordering::Relaxed);
    let second = metrics.collect();
    assert_eq!(total(&second, "data_save_pending"), 2);
    assert_eq!(total(&second, "data_save_accepted_items_total"), 4000);
    assert_eq!(points(&second, "data_save_accepted_items_total").len(), 1);
    assert!(metric(&second, "data_save_flush_total").is_none());
}

#[tokio::test(start_paused = true)]
async fn cancellation_counts_attempt_not_completion_and_retry_zero_rows_acknowledges_all() {
    let metrics = Metrics::new();
    let depth = Arc::new(AtomicUsize::new(0));
    let telemetry = QueueTelemetry::new(
        &metrics.meter,
        "transactions",
        depth.clone(),
        attrs("chunpy", "a"),
    );
    let (sender, receiver) = unbounded_channel();
    let mut queue = BatchQueue::new("transactions", depth.clone(), None, receiver)
        .with_telemetry(telemetry.clone());
    for value in 0..2 {
        enqueue(&sender, &depth, Some(&telemetry), value);
    }
    let mut observer = ();
    tokio::select! {
        biased;
        _ = queue.flush(&WaitingWriter, &mut observer) => panic!("pending writer completed"),
        _ = tokio::task::yield_now() => {}
    }
    let cancelled = metrics.collect();
    assert_eq!(total(&cancelled, "data_save_attempts_total"), 1);
    assert_eq!(total(&cancelled, "data_save_attempted_items_total"), 2);
    assert_eq!(total(&cancelled, "data_save_pending"), 2);
    assert_eq!(total(&cancelled, "data_save_acknowledged_items_total"), 0);
    assert!(metric(&cancelled, "data_save_flush_total").is_none());
    assert!(metric(&cancelled, "data_save_flush_duration_seconds").is_none());
    assert_eq!(
        queue.flush(&Writer(Err(())), &mut ()).await,
        FlushOutcome::Retained
    );
    assert_eq!(
        queue.flush(&Writer(Ok(0)), &mut ()).await,
        FlushOutcome::Idle
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(
        queue.flush(&Writer(Ok(0)), &mut ()).await,
        FlushOutcome::Success
    );
    let done = metrics.collect();
    assert_eq!(total(&done, "data_save_attempts_total"), 3);
    assert_eq!(total(&done, "data_save_attempted_items_total"), 6);
    assert_eq!(total(&done, "data_save_retries_total"), 2);
    assert_eq!(total(&done, "data_save_accepted_items_total"), 2);
    assert_eq!(total(&done, "data_save_acknowledged_items_total"), 2);
    assert_eq!(total(&done, "data_save_writer_rows_total"), 0);
    assert_eq!(total(&done, "data_save_pending"), 0);
    let outcomes = points(&done, "data_save_flush_total");
    assert_eq!(outcomes.len(), 2);
    for outcome in ["success", "failure"] {
        assert_eq!(
            outcomes
                .iter()
                .find(|(a, _)| a.contains(&KeyValue::new("outcome", outcome)))
                .unwrap()
                .1,
            1
        );
    }
    assert_eq!(histogram(&done, "data_save_batch_size"), (3, 6.0));
    assert_eq!(histogram(&done, "data_save_flush_duration_seconds").0, 2);
    let (count, age) = histogram(&done, "data_save_batch_age_seconds");
    assert_eq!(count, 2);
    assert!(age >= 14.0);
    assert_eq!(
        metric(&done, "data_save_flush_duration_seconds")
            .unwrap()
            .unit(),
        "s"
    );
    assert_eq!(
        metric(&done, "data_save_batch_age_seconds").unwrap().unit(),
        "s"
    );
    assert_histogram_bounds(
        &done,
        "data_save_flush_duration_seconds",
        &[
            0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0,
        ],
    );
    assert_histogram_bounds(
        &done,
        "data_save_batch_age_seconds",
        &[0.1, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0],
    );
    assert!(queue.is_empty());
}

#[tokio::test]
async fn writer_rows_are_not_acknowledged_item_count_and_idle_does_not_record() {
    let metrics = Metrics::new();
    let depth = Arc::new(AtomicUsize::new(0));
    let telemetry = QueueTelemetry::new(&metrics.meter, "tokens", depth.clone(), vec![]);
    let (sender, receiver) = unbounded_channel();
    let mut queue =
        BatchQueue::new("tokens", depth.clone(), None, receiver).with_telemetry(telemetry.clone());
    enqueue(&sender, &depth, Some(&telemetry), 1);
    assert_eq!(
        queue.flush(&Writer(Ok(17)), &mut ()).await,
        FlushOutcome::Success
    );
    assert_eq!(
        queue.flush(&Writer(Ok(17)), &mut ()).await,
        FlushOutcome::Idle
    );
    let snapshot = metrics.collect();
    assert_eq!(total(&snapshot, "data_save_acknowledged_items_total"), 1);
    assert_eq!(total(&snapshot, "data_save_writer_rows_total"), 17);
    assert_eq!(total(&snapshot, "data_save_flush_total"), 1);
    assert_eq!(total(&snapshot, "data_save_retries_total"), 0);
}

#[tokio::test]
async fn queues_projects_and_instances_are_isolated_with_canonical_queue_attribute() {
    let metrics = Metrics::new();
    let mut handles = Vec::new();
    for (queue_name, project, instance, count) in [
        ("tokens", "chunpy", "a", 1),
        ("tokens", "fuddo", "b", 2),
        ("candles", "chunpy", "a", 3),
        ("tokens", "chunpy", "b", 4),
    ] {
        let depth = Arc::new(AtomicUsize::new(0));
        let mut attributes = attrs(project, instance);
        attributes.push(KeyValue::new("queue", "wrong"));
        attributes.push(KeyValue::new("outcome", "wrong"));
        let telemetry = QueueTelemetry::new(&metrics.meter, queue_name, depth.clone(), attributes);
        let (sender, receiver) = unbounded_channel();
        let mut queue = BatchQueue::new(queue_name, depth.clone(), None, receiver)
            .with_telemetry(telemetry.clone());
        for value in 0..count {
            enqueue(&sender, &depth, Some(&telemetry), value);
        }
        queue.flush(&Writer(Ok(0)), &mut ()).await;
        handles.push(telemetry);
    }
    let snapshot = metrics.collect();
    for name in [
        "data_save_accepted_items_total",
        "data_save_acknowledged_items_total",
        "data_save_attempted_items_total",
    ] {
        let series = points(&snapshot, name);
        assert_eq!(series.len(), 4, "{name}");
        for (queue, project, instance, count) in [
            ("tokens", "chunpy", "a", 1),
            ("tokens", "fuddo", "b", 2),
            ("candles", "chunpy", "a", 3),
            ("tokens", "chunpy", "b", 4),
        ] {
            assert!(series.iter().any(|(a, n)| *n == count
                && a.len() == 3
                && a.contains(&KeyValue::new("queue", queue))
                && a.contains(&KeyValue::new("project", project))
                && a.contains(&KeyValue::new("instId", instance))));
        }
    }
    for (attributes, count) in points(&snapshot, "data_save_flush_total") {
        assert_eq!(count, 1);
        assert_eq!(attributes.len(), 4);
        assert!(attributes.contains(&KeyValue::new("outcome", "success")));
    }
}

#[test]
fn cumulative_totals_survive_all_handles_dropped_before_first_collection() {
    let metrics = Metrics::new();
    let depth = Arc::new(AtomicUsize::new(3));
    let weak = Arc::downgrade(&depth);
    let telemetry = QueueTelemetry::new(&metrics.meter, "tokens", depth, attrs("korefan", "a"));
    let clone = telemetry.clone();
    telemetry.accepted(2);
    clone.accepted(1);
    telemetry.record_payload_bytes(100);
    clone.record_payload_bytes(25);
    drop(telemetry);
    drop(clone);
    assert!(weak.upgrade().is_none());

    for _ in 0..2 {
        let snapshot = metrics.collect();
        for (name, expected) in [
            ("data_save_accepted_items_total", 3),
            ("data_save_payload_bytes_total", 125),
        ] {
            let series = points(&snapshot, name);
            assert_eq!(series.len(), 1, "{name}");
            assert_eq!(series[0].1, expected, "{name}");
            assert_eq!(series[0].0.len(), 3);
            for attribute in [
                KeyValue::new("queue", "tokens"),
                KeyValue::new("project", "korefan"),
                KeyValue::new("instId", "a"),
            ] {
                assert!(series[0].0.contains(&attribute));
            }
        }
        assert!(points(&snapshot, "data_save_pending").is_empty());
        assert!(points(&snapshot, "data_save_in_flight").is_empty());
    }
}

#[test]
fn observable_callbacks_do_not_keep_dead_queue_depth_alive() {
    let metrics = Metrics::new();
    let depth = Arc::new(AtomicUsize::new(11));
    let weak = Arc::downgrade(&depth);
    let telemetry = QueueTelemetry::new(&metrics.meter, "tokens", depth, vec![]);
    let clone = telemetry.clone();
    drop(telemetry);
    assert_eq!(total(&metrics.collect(), "data_save_pending"), 11);
    drop(clone);
    assert!(weak.upgrade().is_none());
    assert!(points(&metrics.collect(), "data_save_pending").is_empty());
}

#[tokio::test]
async fn in_flight_is_visible_before_outcome_and_clears_on_cancellation() {
    let metrics = Metrics::new();
    let depth = Arc::new(AtomicUsize::new(0));
    let telemetry = QueueTelemetry::new(&metrics.meter, "tokens", depth.clone(), vec![]);
    let (sender, receiver) = unbounded_channel();
    let mut queue =
        BatchQueue::new("tokens", depth.clone(), None, receiver).with_telemetry(telemetry.clone());
    enqueue(&sender, &depth, Some(&telemetry), 1);
    let mut observer = ();
    let mut attempt = Box::pin(queue.flush(&WaitingWriter, &mut observer));
    poll_fn(|cx| {
        assert!(attempt.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    let snapshot = metrics.collect();
    assert_eq!(total(&snapshot, "data_save_in_flight"), 1);
    assert_eq!(total(&snapshot, "data_save_pending"), 1);
    assert!(metric(&snapshot, "data_save_flush_total").is_none());
    drop(attempt);
    assert_eq!(total(&metrics.collect(), "data_save_in_flight"), 0);
    assert_eq!(
        queue.flush(&Writer(Ok(0)), &mut ()).await,
        FlushOutcome::Success
    );
    assert_eq!(total(&metrics.collect(), "data_save_in_flight"), 0);
}

#[tokio::test(start_paused = true)]
async fn disabled_telemetry_keeps_retry_and_depth_behavior_without_metrics() {
    let metrics = Metrics::new();
    let depth = Arc::new(AtomicUsize::new(0));
    let (sender, receiver) = unbounded_channel();
    let mut queue = BatchQueue::new("tokens", depth.clone(), None, receiver);
    enqueue(&sender, &depth, None, 1);
    assert_eq!(
        queue.flush(&Writer(Err(())), &mut ()).await,
        FlushOutcome::Retained
    );
    assert_eq!(depth.load(Ordering::Relaxed), 1);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(
        queue.flush(&Writer(Ok(0)), &mut ()).await,
        FlushOutcome::Success
    );
    assert!(queue.is_empty());
    assert!(metrics
        .collect()
        .scope_metrics()
        .all(|scope| scope.metrics().count() == 0));
}
