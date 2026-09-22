#![cfg(feature = "postgres")]

use std::future::{pending, poll_fn, Future};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use data_save_manager::postgres::{write_error, MeasuredJson};
use data_save_manager::{BatchQueue, BatchWriter, FlushOutcome, QueueTelemetry, Queued};
use opentelemetry::metrics::{Meter, MeterProvider};
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use serde::ser::{Error, SerializeSeq};
use serde::{Serialize, Serializer};
use serde_json::json;
use sqlx::postgres::{PgArgumentBuffer, PgTypeInfo};
use sqlx::types::Json;
use sqlx::{Encode, Postgres, Type};
use tokio::sync::{mpsc::unbounded_channel, Barrier};

#[test]
fn report_write_errors_preserve_the_root_sqlx_type_and_context() {
    for nested in [false, true] {
        let source = anyhow::Error::new(sqlx::Error::Protocol("typed root".into()));
        let source = if nested {
            source.context("adapter context")
        } else {
            source
        };
        let error = write_error(source);
        assert!(error
            .chain()
            .any(|cause| cause.downcast_ref::<sqlx::Error>().is_some()));
        let rendered = format!("{error:#}");
        assert!(rendered.contains("typed root"));
        assert_eq!(rendered.contains("adapter context"), nested);
    }
}

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
        let meter = provider.meter("postgres-test");
        Self {
            provider,
            exporter,
            meter,
        }
    }

    fn telemetry(&self, name: &'static str) -> QueueTelemetry {
        QueueTelemetry::new(&self.meter, name, Arc::new(AtomicUsize::new(0)), vec![])
    }

    fn bytes(&self, queue: &'static str) -> u64 {
        self.exporter.reset();
        self.provider.force_flush().unwrap();
        let mut total = 0;
        for snapshot in self.exporter.get_finished_metrics().unwrap() {
            for metric in snapshot.scope_metrics().flat_map(|scope| scope.metrics()) {
                if metric.name() != "data_save_payload_bytes_total" {
                    continue;
                }
                assert_eq!(metric.unit(), "By");
                let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() else {
                    panic!("payload bytes must be a u64 counter");
                };
                assert!(sum.is_monotonic());
                total += sum
                    .data_points()
                    .filter(|point| {
                        point
                            .attributes()
                            .any(|attr| attr == &KeyValue::new("queue", queue))
                    })
                    .map(|point| point.value())
                    .sum::<u64>();
            }
        }
        total
    }
}

fn queue(name: &'static str, telemetry: Option<QueueTelemetry>) -> BatchQueue<()> {
    let (sender, receiver) = unbounded_channel();
    sender
        .send(Queued {
            value: (),
            enqueued_at: Instant::now(),
        })
        .unwrap();
    let queue = BatchQueue::new(name, Arc::new(AtomicUsize::new(1)), None, receiver);
    match telemetry {
        Some(telemetry) => queue.with_telemetry(telemetry),
        None => queue,
    }
}

fn encode<T: Serialize>(value: T) -> usize {
    let mut buffer = PgArgumentBuffer::default();
    buffer.extend_from_slice(b"unrelated prefix");
    let before = buffer.len();
    assert!(!MeasuredJson(value).encode(&mut buffer).unwrap().is_null());
    buffer.len() - before - 1
}

struct Writer<T> {
    payload: T,
    fail: bool,
}

impl<T: Serialize + Send + Sync> BatchWriter<()> for Writer<T> {
    type Error = ();

    fn write<'a>(
        &'a self,
        _: (),
        _: &'static str,
        _: &'a [()],
    ) -> impl Future<Output = Result<u64, ()>> + Send + 'a {
        encode(&self.payload);
        async move {
            if self.fail {
                Err(())
            } else {
                Ok(0)
            }
        }
    }
}

#[test]
fn without_scope_encoding_and_metadata_match_sqlx_json() {
    let value = json!({"text": "quote\" slash\\ newline\n \u{0416}\u{1f680}", "n": null});
    let measured = MeasuredJson(value.clone());
    let ordinary = Json(value);
    let mut actual = PgArgumentBuffer::default();
    let mut expected = PgArgumentBuffer::default();
    actual.extend_from_slice(b"prefix");
    expected.extend_from_slice(b"prefix");
    assert!(!measured.encode_by_ref(&mut actual).unwrap().is_null());
    assert!(!ordinary.encode_by_ref(&mut expected).unwrap().is_null());
    assert_eq!(&actual[..], &expected[..]);
    assert_eq!(actual[6], 1);
    assert_eq!(
        <MeasuredJson<serde_json::Value> as Type<Postgres>>::type_info(),
        <Json<serde_json::Value> as Type<Postgres>>::type_info()
    );
    for name in ["JSON", "JSONB", "TEXT"] {
        let info = PgTypeInfo::with_name(name);
        assert_eq!(
            <MeasuredJson<serde_json::Value> as Type<Postgres>>::compatible(&info),
            <Json<serde_json::Value> as Type<Postgres>>::compatible(&info)
        );
    }
    assert_eq!(measured.produces(), ordinary.produces());
    assert_eq!(measured.size_hint(), ordinary.size_hint());
    assert_eq!(
        MeasuredJson([1_u64; 8]).size_hint(),
        Json([1_u64; 8]).size_hint()
    );
    assert_eq!(
        MeasuredJson(&ordinary.0).size_hint(),
        Json(&ordinary.0).size_hint()
    );
}

#[tokio::test]
async fn counts_only_original_utf8_json_payload_and_not_unscoped_encodes() {
    let metrics = Metrics::new();
    let telemetry = metrics.telemetry("tokens");
    let value = json!({"text": "quote\" slash\\ newline\n \u{0416}\u{1f680}", "n": null});
    let expected = serde_json::to_vec(&value).unwrap().len() as u64;
    encode(&value);
    assert_eq!(metrics.bytes("tokens"), 0);
    let writer = Writer {
        payload: value,
        fail: false,
    };
    let mut queue = queue("tokens", Some(telemetry));
    assert_eq!(queue.flush(&writer, &mut ()).await, FlushOutcome::Success);
    assert_eq!(metrics.bytes("tokens"), expected);
    encode(&writer.payload);
    assert_eq!(metrics.bytes("tokens"), expected);
}

struct CountedPayload(AtomicUsize);

impl Serialize for CountedPayload {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.fetch_add(1, Ordering::Relaxed);
        serializer.serialize_str("payload")
    }
}

#[tokio::test(start_paused = true)]
async fn retry_counts_each_successful_encode_once_even_after_failed_commit() {
    let metrics = Metrics::new();
    let mut queue = queue("tokens", Some(metrics.telemetry("tokens")));
    let mut writer = Writer {
        payload: CountedPayload(AtomicUsize::new(0)),
        fail: true,
    };
    assert_eq!(queue.flush(&writer, &mut ()).await, FlushOutcome::Retained);
    assert_eq!(writer.payload.0.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.bytes("tokens"), 9);
    assert_eq!(queue.flush(&writer, &mut ()).await, FlushOutcome::Idle);
    assert_eq!(writer.payload.0.load(Ordering::Relaxed), 1);
    tokio::time::advance(Duration::from_secs(1)).await;
    writer.fail = false;
    assert_eq!(queue.flush(&writer, &mut ()).await, FlushOutcome::Success);
    assert_eq!(writer.payload.0.load(Ordering::Relaxed), 2);
    assert_eq!(metrics.bytes("tokens"), 18);
    assert!(queue.is_empty());
}

struct BrokenJson;

impl Serialize for BrokenJson {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(2))?;
        seq.serialize_element("partial output")?;
        Err(S::Error::custom("encoding failed"))
    }
}

struct EncodingErrorWriter;

impl BatchWriter<()> for EncodingErrorWriter {
    type Error = ();

    async fn write(&self, _: (), _: &'static str, _: &[()]) -> Result<u64, ()> {
        encode("ok");
        let mut actual = PgArgumentBuffer::default();
        let mut expected = PgArgumentBuffer::default();
        let error = MeasuredJson(BrokenJson)
            .encode_by_ref(&mut actual)
            .err()
            .unwrap();
        let original = Json(BrokenJson).encode_by_ref(&mut expected).err().unwrap();
        assert_eq!(error.to_string(), original.to_string());
        assert_eq!(&actual[..], &expected[..]);
        assert!(actual.len() > 1);
        Err(())
    }
}

#[tokio::test]
async fn partial_encoding_error_does_not_increment_payload_counter() {
    let metrics = Metrics::new();
    let mut queue = queue("tokens", Some(metrics.telemetry("tokens")));
    assert_eq!(
        queue.flush(&EncodingErrorWriter, &mut ()).await,
        FlushOutcome::Retained
    );
    assert_eq!(metrics.bytes("tokens"), 4);
}

struct NestedWriter {
    inner: Option<QueueTelemetry>,
}

impl BatchWriter<()> for NestedWriter {
    type Error = ();

    async fn write(&self, _: (), _: &'static str, _: &[()]) -> Result<u64, ()> {
        encode("outer");
        let mut inner = queue("inner", self.inner.clone());
        let writer = Writer {
            payload: "inner payload",
            fail: false,
        };
        assert_eq!(inner.flush(&writer, &mut ()).await, FlushOutcome::Success);
        tokio::task::yield_now().await;
        encode("outer");
        Ok(0)
    }
}

#[tokio::test]
async fn nested_scope_counts_inner_separately_and_restores_outer() {
    let metrics = Metrics::new();
    let mut outer = queue("outer", Some(metrics.telemetry("outer")));
    let writer = NestedWriter {
        inner: Some(metrics.telemetry("inner")),
    };
    assert_eq!(outer.flush(&writer, &mut ()).await, FlushOutcome::Success);
    assert_eq!(metrics.bytes("outer"), 14);
    assert_eq!(metrics.bytes("inner"), 15);
}

#[tokio::test]
async fn nested_queue_without_telemetry_masks_outer_scope() {
    let metrics = Metrics::new();
    let mut outer = queue("outer", Some(metrics.telemetry("outer")));
    assert_eq!(
        outer.flush(&NestedWriter { inner: None }, &mut ()).await,
        FlushOutcome::Success
    );
    assert_eq!(metrics.bytes("outer"), 14);
    assert_eq!(metrics.bytes("inner"), 0);
}

struct ConcurrentWriter {
    payload: &'static str,
    barrier: Arc<Barrier>,
}

impl BatchWriter<()> for ConcurrentWriter {
    type Error = ();

    async fn write(&self, _: (), _: &'static str, _: &[()]) -> Result<u64, ()> {
        encode(self.payload);
        self.barrier.wait().await;
        tokio::task::yield_now().await;
        encode(self.payload);
        self.barrier.wait().await;
        Ok(0)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_task_scopes_are_isolated_across_awaits() {
    let metrics = Metrics::new();
    let barrier = Arc::new(Barrier::new(2));
    let mut handles = Vec::new();
    for (name, payload) in [("one", "a"), ("two", "longer")] {
        let mut queue = queue(name, Some(metrics.telemetry(name)));
        let writer = ConcurrentWriter {
            payload,
            barrier: barrier.clone(),
        };
        handles.push(tokio::spawn(async move {
            assert_eq!(queue.flush(&writer, &mut ()).await, FlushOutcome::Success);
            queue
        }));
    }
    let mut queues = Vec::new();
    for handle in handles {
        queues.push(
            tokio::time::timeout(Duration::from_secs(5), handle)
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert_eq!(metrics.bytes("one"), 6);
    assert_eq!(metrics.bytes("two"), 16);
    assert!(queues.iter().all(BatchQueue::is_empty));
}

struct CancelledWriter;

impl BatchWriter<()> for CancelledWriter {
    type Error = ();

    async fn write(&self, _: (), _: &'static str, _: &[()]) -> Result<u64, ()> {
        encode("cancelled");
        pending().await
    }
}

#[tokio::test]
async fn cancelling_writer_keeps_encoded_bytes_and_removes_scope() {
    let metrics = Metrics::new();
    let mut queue = queue("tokens", Some(metrics.telemetry("tokens")));
    let mut observer = ();
    let mut attempt = Box::pin(queue.flush(&CancelledWriter, &mut observer));
    poll_fn(|cx| {
        assert!(attempt.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(attempt);
    assert_eq!(metrics.bytes("tokens"), 11);
    assert!(!queue.is_empty());
    encode("outside cancelled scope");
    assert_eq!(metrics.bytes("tokens"), 11);
    assert_eq!(
        queue
            .flush(
                &Writer {
                    payload: "retry",
                    fail: false
                },
                &mut ()
            )
            .await,
        FlushOutcome::Success
    );
    assert_eq!(metrics.bytes("tokens"), 18);
}
