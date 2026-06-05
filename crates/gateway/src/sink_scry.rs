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

use scry_client::Client;
use scry_proto::{
    build,
    constants::{Signal, COMPRESSION_ZSTD},
    generated::{LogsBatch, MetricsBatch, ProfilesBatch, TracesBatch},
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
}

impl ScrySink {
    pub fn new(conn: ScryConnect) -> Self {
        Self {
            conn,
            client: None,
            batch_id: 0,
        }
    }

    /// Drain the queue until it closes, shipping each item upstream.
    pub async fn run(mut self, mut rx: mpsc::Receiver<Fanout>) {
        while let Some(item) = rx.recv().await {
            let Some(encoded) = encode_item(&item) else {
                continue; // empty batch (offer guards this, but be defensive)
            };
            let signal = encoded.signal;
            if let Err(e) = self.send(encoded).await {
                warn!(error = %e, signal = signal.name(), "scry sink send failed; dropping batch");
            }
        }
        info!("scry sink worker exiting (queue closed)");
    }

    async fn send(&mut self, encoded: EncodedBatch) -> anyhow::Result<()> {
        let EncodedBatch {
            signal,
            record_count,
            ts_min,
            ts_max,
            payload,
        } = encoded;
        let uncompressed_size = payload.len() as u32;
        let payload = zstd::encode_all(payload.as_slice(), ZSTD_LEVEL)
            .expect("zstd encode_all is infallible on Vec input");

        // session_id is stamped by the client under the hood on each attempt (it
        // changes on every reconnect), so build with a placeholder.
        let batch_id = self.batch_id;
        self.batch_id += 1;
        let mut frame = build::batch(build::BatchArgs {
            session_id: 0,
            batch_id,
            signal: signal.as_u8(),
            ts_min_unix_nano: ts_min,
            ts_max_unix_nano: ts_max,
            record_count,
            compression: COMPRESSION_ZSTD,
            uncompressed_size,
            payload,
        });

        // Lazy connect: a down/absent upstream at startup (or after a prior
        // drop) surfaces here as a per-item connect error, not a fatal boot
        // error — the best-effort contract.
        if self.client.is_none() {
            self.client = Some(self.connect().await?);
        }

        // First attempt against the live client.
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

        // Upstream likely restarted. One reconnect + resend so this batch can
        // land and, more importantly, so the client is healthy for the next
        // item. No backoff loop here — a longer outage surfaces as repeated
        // single-attempt drops, which is the best-effort contract.
        warn!("upstream send failed; reconnecting once");
        if let Err(e) = self.client.as_mut().unwrap().reconnect().await {
            // Connection is dead; forget it so the next item connects afresh.
            self.client = None;
            return Err(e);
        }
        info!("reconnected to upstream ingest server");
        let resend = self
            .client
            .as_mut()
            .unwrap()
            .send_batch_stamped(&mut frame)
            .await;
        if resend.is_err() {
            self.client = None;
        }
        resend
    }

    /// Open a fresh session to the upstream ingest server.
    async fn connect(&self) -> anyhow::Result<Client> {
        Client::connect(
            &self.conn.addr,
            self.conn.agent_id,
            &self.conn.hostname,
            self.conn.signals,
            self.conn.resource_attrs.clone(),
        )
        .await
    }
}

/// A `*Batch` re-encoded to its binschema payload plus the frame stamps derived
/// from its records.
struct EncodedBatch {
    signal: Signal,
    record_count: u32,
    ts_min: u64,
    ts_max: u64,
    payload: Vec<u8>,
}

/// Encode a fanned-out item to its binschema payload + frame stamps. Returns
/// `None` for an empty batch.
fn encode_item(item: &Fanout) -> Option<EncodedBatch> {
    match item {
        Fanout::Logs(b) => encode_logs(b),
        Fanout::Metrics(b) => encode_metrics(b),
        Fanout::Traces(b) => encode_traces(b),
        Fanout::Profiles(b) => encode_profiles(b),
    }
}

fn encode_logs(b: &LogsBatch) -> Option<EncodedBatch> {
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
        payload: b
            .encode()
            .expect("LogsBatch encode is infallible for well-formed inputs"),
    })
}

fn encode_metrics(b: &MetricsBatch) -> Option<EncodedBatch> {
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
        payload: b
            .encode()
            .expect("MetricsBatch encode is infallible for well-formed inputs"),
    })
}

fn encode_traces(b: &TracesBatch) -> Option<EncodedBatch> {
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
        payload: b
            .encode()
            .expect("TracesBatch encode is infallible for well-formed inputs"),
    })
}

fn encode_profiles(b: &ProfilesBatch) -> Option<EncodedBatch> {
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
        payload: b
            .encode()
            .expect("ProfilesBatch encode is infallible for well-formed inputs"),
    })
}
