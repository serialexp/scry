//! Live fan-in for the merged history+live query (D-054).
//!
//! The query daemon serves a `live` logs query by unioning the stored
//! parquet blocks with the still-in-flight records at the ingesters. This
//! module owns the *live* half:
//!
//! - [`LiveDiscovery`] — how the daemon learns which ingesters to ask. It's a
//!   trait so `scry-server` stays Valkey-agnostic (the same way the ingest
//!   engines take a `&dyn Fence`): the `scry query` binary injects a
//!   Valkey-backed impl over the D-053 tail registry; with none injected the
//!   live half is refused outright (`QUERY_ERR_LIVE_UNAVAILABLE`).
//! - [`fetch_live_from_ingester`] — dial one ingester's ingest port, do the
//!   Hello handshake, send a `LiveQuery`, and read back its single
//!   `LiveBatch`. The `(wal_shard, wal_seg)` tag on each record is the dedup
//!   key the caller compares against the catalog watermark.
//!
//! The dedup + Arrow assembly live in the query service (they need the
//! catalog); this module is purely "how do I talk to an ingester."

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use scry_proto::{
    build,
    constants::{PROTOCOL_VERSION_V0, SIGNAL_BIT_LOGS},
    framing::{read_frame, write_frame, FrameError},
    generated::{Frame, FrameMsg, LiveBatchOutput},
};
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;

/// Discovery of the live ingester endpoints for a merged query (D-054).
/// Injected into the query service so `scry-server` doesn't depend on
/// Valkey; the `scry query` binary supplies an impl backed by the D-053 tail
/// registry (`scry_valkey::discover_tail_endpoints`). `None` on the service ⇒
/// the live half is refused (per decision 3, Valkey-required).
#[async_trait]
pub trait LiveDiscovery: Send + Sync {
    /// Return the current set of live ingester `host:port` endpoints (the
    /// ingest ports advertised for tail/live-query). Best-effort; an empty
    /// vec simply yields no live rows.
    async fn discover(&self) -> Result<Vec<String>>;
}

/// Dial one ingester's ingest port, handshake, send a `LiveQuery`, and return
/// its single `LiveBatch` reply. Best-effort: the caller wraps this in a
/// timeout and skips (logs) a dead/slow ingester rather than failing the
/// whole query.
pub async fn fetch_live_from_ingester(
    addr: &str,
    signal: u8,
    matchers: &[String],
    ts_min_unix_nano: u64,
    ts_max_unix_nano: u64,
    body_contains: &str,
) -> Result<LiveBatchOutput> {
    let stream = TcpStream::connect(addr)
        .await
        .with_context(|| format!("connecting to {addr}"))?;
    stream.set_nodelay(true)?;
    let (rd, wr) = stream.into_split();
    let mut rd = BufReader::new(rd);
    let mut wr = BufWriter::new(wr);

    // Hello: announce logs (the only live signal in v1). agent_id is a
    // throwaway per-invocation UUID; the query daemon never ingests.
    write_frame(
        &mut wr,
        &build::hello(build::HelloArgs {
            protocol_version: PROTOCOL_VERSION_V0,
            agent_id: uuid::Uuid::now_v7().into_bytes(),
            agent_version: env!("CARGO_PKG_VERSION"),
            hostname: "scry-queryd",
            signals: SIGNAL_BIT_LOGS,
            capabilities: 0,
            resource_attrs: Vec::new(),
        }),
    )
    .await?;
    wr.flush().await?;

    match read_frame::<Frame, _>(&mut rd).await?.msg {
        FrameMsg::HelloAck(_) => {}
        FrameMsg::Error(e) => {
            bail!(
                "ingester rejected handshake: code={} msg={:?}",
                e.code,
                e.message
            )
        }
        other => bail!("expected HelloAck, got a different frame: {other:?}"),
    }

    write_frame(
        &mut wr,
        &build::live_query(build::LiveQueryArgs {
            signal,
            matchers: matchers.to_vec(),
            ts_min_unix_nano,
            ts_max_unix_nano,
            body_contains: body_contains.to_string(),
        }),
    )
    .await?;
    wr.flush().await?;

    // Exactly one LiveBatch, then the ingester closes. Ignore any other
    // frame kinds defensively.
    loop {
        match read_frame::<Frame, _>(&mut rd).await {
            Ok(f) => match f.msg {
                FrameMsg::LiveBatch(b) => return Ok(b),
                FrameMsg::Error(e) => {
                    bail!("ingester sent Error: code={} msg={:?}", e.code, e.message)
                }
                _ => {} // ignore
            },
            Err(FrameError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                bail!("ingester closed the connection before sending a LiveBatch")
            }
            Err(e) => return Err(e).context("reading LiveBatch frame"),
        }
    }
}
