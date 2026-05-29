//! Shared upstream path: encode a built `*Batch`, zstd-compress it, wrap it in
//! a binschema `Batch` frame, and send it over the shared wire client with
//! inflight flow control.
//!
//! v0 sends **one binschema batch per inbound HTTP request**. This is simple,
//! correct, and loss-aware (the HTTP caller sees a 502 and applies its own
//! retry). Follow-ups: size/age batch accumulation, a local at-least-once
//! spool, and an mpsc single-writer task instead of the shared mutex (which
//! currently serialises all sends behind one lock — fine for v0's request
//! rates, a bottleneck only under heavy fan-in).

use std::sync::{atomic::{AtomicU64, Ordering}, Arc};

use anyhow::Result;
use binschema_runtime::{BinSchemaError, BitOrder, BitStreamEncoder};
use scry_client::Client;
use scry_proto::{
    build,
    constants::{Signal, COMPRESSION_ZSTD},
    generated::{MetricsBatch, ProfilesBatch, TracesBatch},
};
use tokio::sync::Mutex;

const ZSTD_LEVEL: i32 = 3;

/// Axum handler state: the shared wire client, the server-assigned session id,
/// and a monotonic batch-id counter. Cheap to clone (all `Arc`/`Copy`).
#[derive(Clone)]
pub struct AppState {
    client: Arc<Mutex<Client>>,
    session_id: u64,
    batch_id: Arc<AtomicU64>,
}

impl AppState {
    pub fn new(client: Client) -> Self {
        let session_id = client.session_id();
        Self {
            client: Arc::new(Mutex::new(client)),
            session_id,
            batch_id: Arc::new(AtomicU64::new(0)),
        }
    }

    fn next_batch_id(&self) -> u64 {
        self.batch_id.fetch_add(1, Ordering::Relaxed)
    }
}

/// Encode + send a metrics batch upstream.
pub async fn send_metrics(state: &AppState, batch: MetricsBatch) -> Result<()> {
    if batch.samples.is_empty() {
        return Ok(());
    }
    let mut ts_min = u64::MAX;
    let mut ts_max = 0u64;
    for s in &batch.samples {
        ts_min = ts_min.min(s.ts_unix_nano);
        ts_max = ts_max.max(s.ts_unix_nano);
    }
    let record_count = batch.samples.len() as u32;
    let payload = encode(&batch, |b, e| b.encode_into(e));
    send(state, Signal::Metrics, record_count, ts_min, ts_max, payload).await
}

/// Encode + send a traces batch upstream. Returns once the frame is written
/// (after acquiring an inflight slot).
pub async fn send_traces(state: &AppState, batch: TracesBatch) -> Result<()> {
    if batch.spans.is_empty() {
        return Ok(());
    }
    let mut ts_min = u64::MAX;
    let mut ts_max = 0u64;
    for s in &batch.spans {
        ts_min = ts_min.min(s.start_unix_nano);
        ts_max = ts_max.max(s.end_unix_nano);
    }
    let record_count = batch.spans.len() as u32;
    let payload = encode(&batch, |b, e| b.encode_into(e));
    send(state, Signal::Traces, record_count, ts_min, ts_max, payload).await
}

/// Encode + send a profiles batch upstream.
pub async fn send_profiles(state: &AppState, batch: ProfilesBatch) -> Result<()> {
    if batch.samples.is_empty() {
        return Ok(());
    }
    let mut ts_min = u64::MAX;
    let mut ts_max = 0u64;
    for b in &batch.samples {
        ts_min = ts_min.min(b.ts_unix_nano);
        ts_max = ts_max.max(b.ts_unix_nano.saturating_add(b.duration_nano));
    }
    let record_count = batch.samples.len() as u32;
    let payload = encode(&batch, |b, e| b.encode_into(e));
    send(state, Signal::Profiles, record_count, ts_min, ts_max, payload).await
}

async fn send(
    state: &AppState,
    signal: Signal,
    record_count: u32,
    ts_min: u64,
    ts_max: u64,
    payload_uncompressed: Vec<u8>,
) -> Result<()> {
    let uncompressed_size = payload_uncompressed.len() as u32;
    let payload = zstd::encode_all(payload_uncompressed.as_slice(), ZSTD_LEVEL)
        .expect("zstd encode_all is infallible on Vec input");

    let frame = build::batch(build::BatchArgs {
        session_id: state.session_id,
        batch_id: state.next_batch_id(),
        signal: signal.as_u8(),
        ts_min_unix_nano: ts_min,
        ts_max_unix_nano: ts_max,
        record_count,
        compression: COMPRESSION_ZSTD,
        uncompressed_size,
        payload,
    });

    let mut client = state.client.lock().await;
    client.send_batch(&frame).await
}

/// Encode a binschema value to bytes — mirrors `noise-spewer::gen::encode`.
fn encode<T, F>(value: &T, encode_into: F) -> Vec<u8>
where
    F: Fn(&T, &mut BitStreamEncoder) -> Result<(), BinSchemaError>,
{
    let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
    encode_into(value, &mut encoder).expect("payload encode is infallible for well-formed inputs");
    encoder.finish()
}
