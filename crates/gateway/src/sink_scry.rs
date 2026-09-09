//! The scry sink: re-encode a fanned-out `*Batch` to the native binschema wire
//! and ship it to an upstream scry ingest server.
//!
//! This is the destination that used to be the gateway's *only* output (the old
//! `upstream.rs`); it is now **one optional best-effort sink among several** — a
//! gateway that only fans logs to Loki/OpenSearch runs with no scry sink at all
//! (see `main.rs`, which builds this only when `--upstream` is set).
//!
//! It connects **lazily, inside its own worker**: a down-or-absent scry server
//! at startup does not abort the gateway (mirroring the Loki/OpenSearch sinks,
//! which connect per request). The worker drains its queue serially — one
//! binschema batch per fanned-out item — re-deriving `ts_min`/`ts_max`/
//! `record_count` from the decoded batch (the original frame's stamps don't
//! survive fan-out). If it isn't connected it connects first; on a send failure
//! it reconnects once and retries; on a persistent failure it drops the batch
//! (best-effort), forgets the dead client, and re-attempts a fresh connect on
//! the next item rather than blocking the queue behind a dead upstream.

use std::borrow::Cow;

use scry_client::Client;
use scry_proto::{
    build,
    constants::{
        Signal, CAP_LOGS_V2, CAP_STRUCTURED_METRICS_V2, COMPRESSION_ZSTD, PROTOCOL_VERSION_V2,
    },
    generated::{LogsBatch, MetricsBatch, MetricsBatchV2, ProfilesBatch, TracesBatch},
    LabelPair,
};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::sink::Fanout;

const ZSTD_LEVEL: i32 = 3;

/// The parameters needed to (re)connect to the upstream scry ingest server. The
/// sink holds these so it can connect lazily and reconnect after a drop, rather
/// than requiring a live connection at construction time.
#[derive(Clone)]
pub struct ScryConnect {
    pub addr: String,
    pub agent_id: [u8; 16],
    pub hostname: String,
    pub signals: u8,
    pub resource_attrs: Vec<LabelPair>,
}

/// Worker that forwards fanned-out batches to one upstream scry ingest server.
pub struct ScrySink {
    conn: ScryConnect,
    /// `None` until the first successful connect, and reset to `None` after a
    /// connection is found dead so the next item triggers a fresh connect.
    client: Option<Client>,
    batch_id: u64,
    reporter: crate::metrics::SinkReporter,
}

impl ScrySink {
    pub fn new(conn: ScryConnect, reporter: crate::metrics::SinkReporter) -> Self {
        Self {
            conn,
            client: None,
            batch_id: 0,
            reporter,
        }
    }

    /// Drain the queue until it closes, shipping each item upstream.
    pub async fn run(mut self, mut rx: mpsc::Receiver<Fanout>) {
        while let Some(item) = rx.recv().await {
            let gateway_signal = item.signal();
            if let Err(e) = self.send(&item, gateway_signal).await {
                self.reporter.failed(gateway_signal);
                warn!(error = %e, signal = gateway_signal.name(), "scry sink send failed; dropping batch");
            } else {
                self.reporter.delivered(gateway_signal);
            }
        }
        info!("scry sink worker exiting (queue closed)");
    }

    async fn send(
        &mut self,
        item: &Fanout,
        gateway_signal: crate::metrics::GatewaySignal,
    ) -> anyhow::Result<()> {
        // Lazy connect comes before capability-dependent encoding. HelloAck can
        // change after any reconnect, so retaining the logical fan-out item is
        // what prevents an SL2 payload from being sent to a downgraded peer.
        self.reporter.attempt(gateway_signal);
        if self.client.is_none() {
            self.client = Some(self.connect().await.inspect_err(|_| {
                self.reporter.attempt_failed(gateway_signal);
            })?);
        }

        let batch_id = self.batch_id;
        self.batch_id += 1;
        let client = self.client.as_ref().unwrap();
        let mut frame = encode_item(
            item,
            client.supports_logs_v2(),
            client.supports_structured_metrics(),
            batch_id,
        )?
        .ok_or_else(|| anyhow::anyhow!("empty batch"))?;
        if self
            .client
            .as_mut()
            .unwrap()
            .send_batch_stamped(&mut frame)
            .await
            .is_ok()
        {
            return Ok(());
        }

        // Upstream likely restarted. One reconnect + format re-selection so a
        // changed HelloAck cannot make the retry use stale capabilities.
        self.reporter.attempt_failed(gateway_signal);
        self.reporter.retry(gateway_signal);
        self.reporter.attempt(gateway_signal);
        warn!("upstream send failed; reconnecting once");
        if let Err(e) = self.client.as_mut().unwrap().reconnect().await {
            self.reporter.attempt_failed(gateway_signal);
            self.client = None;
            return Err(e);
        }
        info!("reconnected to upstream ingest server");
        let client = self.client.as_ref().unwrap();
        let mut frame = encode_item(
            item,
            client.supports_logs_v2(),
            client.supports_structured_metrics(),
            batch_id,
        )?
        .ok_or_else(|| anyhow::anyhow!("empty batch"))?;
        let resend = self
            .client
            .as_mut()
            .unwrap()
            .send_batch_stamped(&mut frame)
            .await;
        if resend.is_err() {
            self.reporter.attempt_failed(gateway_signal);
            self.client = None;
        }
        resend
    }

    /// Open a fresh session to the upstream ingest server.
    async fn connect(&self) -> anyhow::Result<Client> {
        Client::connect_with_protocol(
            &self.conn.addr,
            self.conn.agent_id,
            &self.conn.hostname,
            self.conn.signals,
            self.conn.resource_attrs.clone(),
            PROTOCOL_VERSION_V2,
            CAP_STRUCTURED_METRICS_V2 | CAP_LOGS_V2,
        )
        .await
    }
}

/// A `*Batch` re-encoded to its binschema payload plus the frame stamps derived
/// from its records.
struct EncodedBatch<'a> {
    signal: Signal,
    record_count: u32,
    ts_min: u64,
    ts_max: u64,
    payload: Cow<'a, [u8]>,
    requires_structured_metrics: bool,
}

impl EncodedBatch<'_> {
    fn into_frame(self, batch_id: u64) -> anyhow::Result<scry_proto::Frame> {
        let uncompressed_size = self.payload.len().try_into()?;
        let payload = zstd::encode_all(self.payload.as_ref(), ZSTD_LEVEL)
            .expect("zstd encode_all is infallible on in-memory input");
        Ok(build::batch(build::BatchArgs {
            session_id: 0,
            batch_id,
            signal: self.signal.as_u8(),
            ts_min_unix_nano: self.ts_min,
            ts_max_unix_nano: self.ts_max,
            record_count: self.record_count,
            compression: COMPRESSION_ZSTD,
            uncompressed_size,
            payload,
        }))
    }
}

/// Encode a fanned-out item under the current session's negotiated capabilities.
fn encode_item(
    item: &Fanout,
    supports_logs_v2: bool,
    supports_structured_metrics: bool,
    batch_id: u64,
) -> anyhow::Result<Option<scry_proto::Frame>> {
    let encoded = match item {
        Fanout::Logs(b) => match &b.canonical {
            Some(canonical) => {
                if !supports_logs_v2 {
                    anyhow::bail!("upstream did not negotiate logs v2; refusing lossy downgrade")
                }
                Some(EncodedBatch {
                    signal: Signal::Logs,
                    record_count: canonical.record_count,
                    ts_min: canonical.ts_min,
                    ts_max: canonical.ts_max,
                    payload: Cow::Borrowed(&canonical.payload),
                    requires_structured_metrics: false,
                })
            }
            None => encode_logs(&b.projection),
        },
        Fanout::Metrics(b) => encode_metrics(b),
        Fanout::StructuredMetrics(b) => encode_structured_metrics(b),
        Fanout::Traces(b) => encode_traces(b),
        Fanout::Profiles(b) => encode_profiles(b),
    };
    let Some(encoded) = encoded else {
        return Ok(None);
    };
    if encoded.requires_structured_metrics && !supports_structured_metrics {
        anyhow::bail!("upstream does not negotiate structured metrics v2");
    }
    encoded.into_frame(batch_id).map(Some)
}

fn encode_logs(b: &LogsBatch) -> Option<EncodedBatch<'_>> {
    let mut record_count = 0u32;
    let mut ts_min = u64::MAX;
    let mut ts_max = 0u64;
    for s in &b.streams {
        for e in &s.entries {
            record_count += 1;
            ts_min = ts_min.min(e.ts_unix_nano);
            ts_max = ts_max.max(e.ts_unix_nano);
        }
    }
    if record_count == 0 {
        return None;
    }
    Some(EncodedBatch {
        signal: Signal::Logs,
        record_count,
        ts_min,
        ts_max,
        payload: Cow::Owned(
            b.encode()
                .expect("LogsBatch encode is infallible for well-formed inputs"),
        ),
        requires_structured_metrics: false,
    })
}

fn point_timestamp(point: &scry_proto::generated::MetricPointV2) -> u64 {
    use scry_proto::generated::MetricPointV2Value;
    match &point.value {
        MetricPointV2Value::ScalarPointV2(value) => value.ts_unix_nano,
        MetricPointV2Value::HistogramPointV2(value) => value.ts_unix_nano,
        MetricPointV2Value::ExponentialHistogramPointV2(value) => value.ts_unix_nano,
        MetricPointV2Value::SummaryPointV2(value) => value.ts_unix_nano,
    }
}

fn encode_structured_metrics(b: &MetricsBatchV2) -> Option<EncodedBatch<'_>> {
    if b.points.is_empty() {
        return None;
    }
    if scry_proto::metrics_v2::validate_for_encode(b).is_err() {
        return None;
    }
    let mut ts_min = u64::MAX;
    let mut ts_max = 0;
    for point in &b.points {
        let timestamp = point_timestamp(point);
        ts_min = ts_min.min(timestamp);
        ts_max = ts_max.max(timestamp);
    }
    Some(EncodedBatch {
        signal: Signal::Metrics,
        record_count: b.points.len().try_into().ok()?,
        ts_min,
        ts_max,
        payload: Cow::Owned(b.encode().ok()?),
        requires_structured_metrics: true,
    })
}

fn encode_metrics(b: &MetricsBatch) -> Option<EncodedBatch<'_>> {
    if b.samples.is_empty() {
        return None;
    }
    let mut ts_min = u64::MAX;
    let mut ts_max = 0u64;
    for s in &b.samples {
        ts_min = ts_min.min(s.ts_unix_nano);
        ts_max = ts_max.max(s.ts_unix_nano);
    }
    Some(EncodedBatch {
        signal: Signal::Metrics,
        record_count: b.samples.len() as u32,
        ts_min,
        ts_max,
        payload: Cow::Owned(
            b.encode()
                .expect("MetricsBatch encode is infallible for well-formed inputs"),
        ),
        requires_structured_metrics: false,
    })
}

fn encode_traces(b: &TracesBatch) -> Option<EncodedBatch<'_>> {
    if b.spans.is_empty() {
        return None;
    }
    let mut ts_min = u64::MAX;
    let mut ts_max = 0u64;
    for s in &b.spans {
        ts_min = ts_min.min(s.start_unix_nano);
        ts_max = ts_max.max(s.end_unix_nano);
    }
    Some(EncodedBatch {
        signal: Signal::Traces,
        record_count: b.spans.len() as u32,
        ts_min,
        ts_max,
        payload: Cow::Owned(
            b.encode()
                .expect("TracesBatch encode is infallible for well-formed inputs"),
        ),
        requires_structured_metrics: false,
    })
}

fn encode_profiles(b: &ProfilesBatch) -> Option<EncodedBatch<'_>> {
    if b.samples.is_empty() {
        return None;
    }
    let mut ts_min = u64::MAX;
    let mut ts_max = 0u64;
    for s in &b.samples {
        ts_min = ts_min.min(s.ts_unix_nano);
        ts_max = ts_max.max(s.ts_unix_nano.saturating_add(s.duration_nano));
    }
    Some(EncodedBatch {
        signal: Signal::Profiles,
        record_count: b.samples.len() as u32,
        ts_min,
        ts_max,
        payload: Cow::Owned(
            b.encode()
                .expect("ProfilesBatch encode is infallible for well-formed inputs"),
        ),
        requires_structured_metrics: false,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use scry_proto::{
        framing::{read_frame, write_frame},
        generated::{
            DoubleValueV2Input, Frame, FrameMsg, LogEntry, LogStream, LogsBatchV2Input,
            MetricDescriptorV2, MetricNumberV2, MetricNumberV2Value, MetricPointV2,
            MetricPointV2Value, MetricsBatchV2Input, OpaqueLogRecordV2, ScalarPointV2Input,
        },
        metrics_v2::{MetricKind, Temporality},
    };
    use tokio::{
        io::{AsyncWriteExt, BufReader, BufWriter},
        net::TcpListener,
        sync::oneshot,
        time::{sleep, timeout, Duration},
    };

    use super::*;
    use crate::{
        metrics::{GatewaySignal, SinkKind, SinkReporter},
        sink::{CanonicalLogsBatch, LogsFanout},
    };

    fn projected_logs() -> LogsBatch {
        LogsBatch {
            streams: vec![LogStream {
                fingerprint: 17,
                labels: vec![],
                entries: vec![LogEntry {
                    ts_unix_nano: 123,
                    severity: 9,
                    body: "lossy projection".into(),
                    attributes: vec![],
                }],
            }],
        }
    }

    fn batch_payload(frame: scry_proto::Frame) -> (scry_proto::generated::Batch, Vec<u8>) {
        let FrameMsg::Batch(batch) = frame.msg else {
            panic!("expected Batch frame")
        };
        let payload = zstd::decode_all(batch.payload.as_slice()).unwrap();
        (batch, payload)
    }

    #[test]
    fn canonical_logs_use_exact_sl2_bytes_when_negotiated() {
        // Deliberately unlike the v1 projection: selecting/fabricating v1 would
        // make the byte-for-byte assertion fail.
        let canonical = LogsBatchV2Input {
            records: vec![OpaqueLogRecordV2 {
                value: b"opaque canonical record".to_vec(),
            }],
        }
        .encode()
        .unwrap();
        let item = Fanout::Logs(Arc::new(LogsFanout {
            projection: projected_logs(),
            canonical: Some(CanonicalLogsBatch {
                payload: canonical.clone(),
                record_count: 3,
                ts_min: 100,
                ts_max: 900,
            }),
        }));

        let frame = encode_item(&item, true, false, 42).unwrap().unwrap();
        let (batch, payload) = batch_payload(frame);
        assert_eq!(payload, canonical);
        assert_eq!(batch.batch_id, 42);
        assert_eq!(batch.signal, Signal::Logs.as_u8());
        assert_eq!(batch.record_count, 3);
        assert_eq!(batch.ts_min_unix_nano, 100);
        assert_eq!(batch.ts_max_unix_nano, 900);
        assert_eq!(batch.uncompressed_size as usize, payload.len());
    }

    #[test]
    fn mapped_otlp_payload_survives_scry_frame_compression() {
        let mapped = crate::otlp_logs::map_logs(crate::otlp_logs::sample_request(4));
        let item = Fanout::Logs(Arc::new(LogsFanout {
            projection: mapped.batch,
            canonical: Some(mapped.canonical),
        }));
        let frame = encode_item(&item, true, true, 43).unwrap().unwrap();
        let (_, payload) = batch_payload(frame);
        struct Count(u32);
        impl scry_proto::LogsV2Appender for Count {
            fn record(&mut self, _: scry_proto::CanonicalLogRecord<'_>) -> Result<(), String> {
                self.0 += 1;
                Ok(())
            }
        }
        let mut count = Count(0);
        scry_proto::decode_logs_batch_v2_into(
            &payload,
            scry_proto::LogsV2DecodeLimits::default(),
            &mut count,
        )
        .unwrap();
        assert_eq!(count.0, 4);
    }

    #[test]
    fn canonical_logs_without_negotiation_error_instead_of_falling_back_to_v1() {
        let projection = projected_logs();
        let projection_bytes = projection.encode().unwrap();
        let item = Fanout::Logs(Arc::new(LogsFanout {
            projection,
            canonical: Some(CanonicalLogsBatch {
                payload: b"canonical-only".to_vec(),
                record_count: 1,
                ts_min: 1,
                ts_max: 1,
            }),
        }));

        let error = encode_item(&item, false, true, 1).unwrap_err();
        assert!(error.to_string().contains("refusing lossy downgrade"));
        // Keep this explicit so the fixture itself proves that a usable v1
        // projection existed and was nevertheless not selected as a fallback.
        assert_ne!(projection_bytes, b"canonical-only");
    }

    #[test]
    fn v1_origin_logs_still_send_v1_without_logs_v2_capability() {
        let projection = projected_logs();
        let expected = projection.encode().unwrap();
        let item = Fanout::Logs(Arc::new(LogsFanout::v1(projection)));

        let frame = encode_item(&item, false, false, 7).unwrap().unwrap();
        let (batch, payload) = batch_payload(frame);
        assert_eq!(payload, expected);
        assert_eq!(batch.signal, Signal::Logs.as_u8());
        assert_eq!(batch.record_count, 1);
        assert_eq!(batch.ts_min_unix_nano, 123);
        assert_eq!(batch.ts_max_unix_nano, 123);
    }

    fn structured_metrics() -> MetricsBatchV2 {
        MetricsBatchV2Input {
            descriptors: vec![MetricDescriptorV2 {
                id: 5,
                name: "requests".into(),
                description: String::new(),
                unit: "1".into(),
                metric_kind: MetricKind::Gauge as u8,
                temporality: Temporality::Unspecified as u8,
                monotonic: 0,
                resource_attrs: vec![],
                scope_name: String::new(),
                scope_version: String::new(),
                scope_attrs: vec![],
            }],
            points: vec![MetricPointV2 {
                value: MetricPointV2Value::ScalarPointV2(
                    ScalarPointV2Input {
                        descriptor_id: 5,
                        start_unix_nano: 0,
                        ts_unix_nano: 456,
                        flags: 0,
                        attributes: vec![],
                        exemplars: vec![],
                        number: MetricNumberV2 {
                            value: MetricNumberV2Value::DoubleValueV2(
                                DoubleValueV2Input { value: 2.5 }.into(),
                            ),
                        },
                    }
                    .into(),
                ),
            }],
        }
        .into()
    }

    #[test]
    fn structured_metrics_remain_capability_gated() {
        let metrics = structured_metrics();
        let expected = metrics.encode().unwrap();
        let item = Fanout::StructuredMetrics(Arc::new(metrics));

        let error = encode_item(&item, true, false, 8).unwrap_err();
        assert!(error
            .to_string()
            .contains("does not negotiate structured metrics v2"));

        let frame = encode_item(&item, false, true, 8).unwrap().unwrap();
        let (batch, payload) = batch_payload(frame);
        assert_eq!(payload, expected);
        assert_eq!(batch.signal, Signal::Metrics.as_u8());
        assert_eq!(batch.record_count, 1);
        assert_eq!(batch.ts_min_unix_nano, 456);
        assert_eq!(batch.ts_max_unix_nano, 456);
    }

    async fn handshake(
        listener: &TcpListener,
        session_id: u64,
        capabilities: u32,
    ) -> (
        scry_proto::generated::Hello,
        BufReader<tokio::net::tcp::OwnedReadHalf>,
        BufWriter<tokio::net::tcp::OwnedWriteHalf>,
    ) {
        let (stream, _) = listener.accept().await.unwrap();
        let (rd, wr) = stream.into_split();
        let mut rd = BufReader::new(rd);
        let mut wr = BufWriter::new(wr);
        let hello = match read_frame::<Frame, _>(&mut rd).await.unwrap().msg {
            FrameMsg::Hello(hello) => hello,
            other => panic!("expected Hello, got {other:?}"),
        };
        write_frame(
            &mut wr,
            &build::hello_ack(build::HelloAckArgs {
                protocol_version: PROTOCOL_VERSION_V2,
                writer_id: "sink-test",
                session_id,
                capabilities,
                suggested_batch_bytes: 0,
                max_batch_bytes: 0,
                max_inflight_batches: 8,
            }),
        )
        .await
        .unwrap();
        wr.flush().await.unwrap();
        (hello, rd, wr)
    }

    #[tokio::test]
    async fn reconnect_downgrade_does_not_write_stale_sl2_and_requests_both_capabilities() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (closed_tx, closed_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (first_hello, first_rd, first_wr) =
                handshake(&listener, 71, CAP_STRUCTURED_METRICS_V2 | CAP_LOGS_V2).await;
            assert_eq!(first_hello.protocol_version, PROTOCOL_VERSION_V2);
            assert_eq!(
                first_hello.capabilities,
                CAP_STRUCTURED_METRICS_V2 | CAP_LOGS_V2
            );
            // Close before any batch so the client's reader reports EOF and
            // the sink takes its reconnect/re-encode path.
            drop(first_rd);
            drop(first_wr);
            closed_tx.send(()).unwrap();

            let (second_hello, mut second_rd, _second_wr) =
                handshake(&listener, 72, CAP_STRUCTURED_METRICS_V2).await;
            assert_eq!(
                second_hello.capabilities,
                CAP_STRUCTURED_METRICS_V2 | CAP_LOGS_V2
            );
            // A stale pre-reconnect SL2 frame would be observable here. The
            // downgraded HelloAck must instead make re-encoding fail locally.
            assert!(timeout(
                Duration::from_millis(250),
                read_frame::<Frame, _>(&mut second_rd)
            )
            .await
            .is_err());
        });

        let mut sink = ScrySink::new(
            ScryConnect {
                addr,
                agent_id: [3; 16],
                hostname: "sink-test".into(),
                signals: scry_proto::constants::SIGNAL_BIT_LOGS,
                resource_attrs: vec![],
            },
            SinkReporter::new(None, SinkKind::Scry),
        );
        let item = Fanout::Logs(Arc::new(LogsFanout {
            projection: projected_logs(),
            canonical: Some(CanonicalLogsBatch {
                payload: b"SL2 retry must not leak".to_vec(),
                record_count: 1,
                ts_min: 123,
                ts_max: 123,
            }),
        }));

        // Establish the first negotiated session, then wait until the mock has
        // closed it before asking the sink to write.
        sink.client = Some(sink.connect().await.unwrap());
        closed_rx.await.unwrap();
        sleep(Duration::from_millis(25)).await;
        let error = sink.send(&item, GatewaySignal::Logs).await.unwrap_err();
        assert!(error.to_string().contains("refusing lossy downgrade"));
        server.await.unwrap();
    }
}
