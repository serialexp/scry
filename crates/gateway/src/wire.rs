//! Native binschema ingest listener.
//!
//! Lets `scry-agent` (and anything that speaks the native wire) point at the
//! gateway instead of directly at a scry ingest server. This is a **fan-out
//! front-end**, not a store: it performs the same Hello/HelloAck handshake and
//! Batch/Ping/Goodbye dispatch as `scry-server`, but instead of a WAL+parquet
//! pipeline it decodes each batch into a typed `*Batch` and hands it to
//! [`AppState`], which fans it out to every configured sink.
//!
//! The handshake/dispatch is a deliberately trimmed copy of
//! `crates/server/src/server.rs::handle` (no sharding, no scratch decode path,
//! no stats) — `scry-server`'s `Server` is hardwired to the storage pipelines
//! and can't be reused as a pluggable sink. Decoding uses the typed
//! `*Batch::decode()` (one batch per frame; not the hot per-connection ingest
//! loop, so the zero-alloc streaming decoders aren't needed here).

use std::{
    future::Future,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use anyhow::{Context, Result};
use scry_proto::{
    build,
    constants::{
        Signal, ACK_ACCEPTED, ACK_REJECTED, COMPRESSION_NONE, COMPRESSION_ZSTD,
        DEFAULT_MAX_BATCH_BYTES, DEFAULT_MAX_INFLIGHT_BATCHES, DEFAULT_SUGGESTED_BATCH_BYTES,
        ERR_HELLO_REQUIRED, ERR_PROTOCOL_VERSION, ERR_SESSION_MISMATCH, GOODBYE_NORMAL,
        PROTOCOL_VERSION_V0, REJECT_BAD_SCHEMA, REJECT_BATCH_TOO_LARGE,
        REJECT_SIGNAL_NOT_ANNOUNCED, SIGNAL_BIT_LOGS, SIGNAL_BIT_METRICS, SIGNAL_BIT_PROFILES,
        SIGNAL_BIT_TRACES,
    },
    framing::{read_frame, write_frame, FrameError},
    generated::{FrameMsg, HelloOutput, LogsBatch, MetricsBatch, ProfilesBatch, TracesBatch},
};
use tokio::{
    io::{AsyncWriteExt, BufReader, BufWriter},
    net::{TcpListener, TcpStream},
};
use tracing::{info, warn};

use crate::sink::AppState;

/// The `writer_id` announced to native clients in `HelloAck`. Human-readable,
/// not parsed.
const WRITER_ID: &str = "scry-gateway";

/// Bind `listen_addr`, accept native binschema connections until `shutdown`
/// completes, fanning each accepted batch into `state`.
pub async fn serve_wire<F>(listen_addr: String, state: AppState, shutdown: F) -> Result<()>
where
    F: Future<Output = ()>,
{
    let listener = TcpListener::bind(&listen_addr)
        .await
        .with_context(|| format!("binding native wire listener on {listen_addr}"))?;
    info!(addr = %listen_addr, "scry-gateway native wire listener ready");

    let next_session_id = Arc::new(AtomicU64::new(1));

    let accept_loop = async {
        loop {
            let (sock, peer) = listener.accept().await?;
            let session_id = next_session_id.fetch_add(1, Ordering::Relaxed);
            let state = state.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_conn(sock, peer, session_id, state).await {
                    warn!(%peer, error = %e, "wire connection ended with error");
                }
            });
        }
        #[allow(unreachable_code)]
        Ok::<(), anyhow::Error>(())
    };

    tokio::select! {
        r = accept_loop => { r?; }
        _ = shutdown    => { info!("native wire listener shutting down"); }
    }
    Ok(())
}

async fn handle_conn(
    sock: TcpStream,
    peer: SocketAddr,
    session_id: u64,
    state: AppState,
) -> Result<()> {
    sock.set_nodelay(true)?;
    let (rd, wr) = sock.into_split();
    let mut rd = BufReader::new(rd);
    let mut wr = BufWriter::new(wr);

    // ── Handshake ──────────────────────────────────────────────────────
    let first = match read_frame::<scry_proto::Frame, _>(&mut rd).await {
        Ok(f) => f,
        Err(e) => {
            warn!(%peer, error = %e, "no frame before handshake");
            return Ok(());
        }
    };
    let hello: HelloOutput = match first.msg {
        FrameMsg::Hello(h) => h,
        other => {
            let _ = write_frame(
                &mut wr,
                &build::error(ERR_HELLO_REQUIRED, "Hello required first"),
            )
            .await;
            let _ = wr.flush().await;
            warn!(%peer, kind = short_msg_name(&other), "non-Hello first frame");
            return Ok(());
        }
    };

    if hello.protocol_version != PROTOCOL_VERSION_V0 {
        let _ = write_frame(
            &mut wr,
            &build::error(ERR_PROTOCOL_VERSION, "unsupported protocol version"),
        )
        .await;
        let _ = wr.flush().await;
        warn!(%peer, ver = hello.protocol_version, "unsupported protocol version");
        return Ok(());
    }

    let signals_announced = hello.signals;
    info!(
        %peer,
        session_id,
        agent_version = %hello.agent_version,
        hostname = %hello.hostname,
        signals = format!("{signals_announced:#06b}"),
        "wire hello"
    );

    write_frame(
        &mut wr,
        &build::hello_ack(build::HelloAckArgs {
            protocol_version: PROTOCOL_VERSION_V0,
            writer_id: WRITER_ID,
            session_id,
            capabilities: 0,
            suggested_batch_bytes: DEFAULT_SUGGESTED_BATCH_BYTES,
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
            max_inflight_batches: DEFAULT_MAX_INFLIGHT_BATCHES,
        }),
    )
    .await?;
    wr.flush().await?;

    // ── Message loop ───────────────────────────────────────────────────
    loop {
        let frame = match read_frame::<scry_proto::Frame, _>(&mut rd).await {
            Ok(f) => f,
            Err(FrameError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                info!(%peer, session_id, "peer closed");
                break;
            }
            Err(e) => {
                warn!(%peer, error = %e, "frame read failed");
                break;
            }
        };

        match frame.msg {
            FrameMsg::Batch(b) => {
                if b.session_id != session_id {
                    let _ = write_frame(
                        &mut wr,
                        &build::error(ERR_SESSION_MISMATCH, "session_id mismatch"),
                    )
                    .await;
                    let _ = wr.flush().await;
                    break;
                }

                let sig = Signal::from_u8(b.signal);
                let announced_ok = match sig {
                    Some(Signal::Metrics) => signals_announced & SIGNAL_BIT_METRICS != 0,
                    Some(Signal::Logs) => signals_announced & SIGNAL_BIT_LOGS != 0,
                    Some(Signal::Traces) => signals_announced & SIGNAL_BIT_TRACES != 0,
                    Some(Signal::Profiles) => signals_announced & SIGNAL_BIT_PROFILES != 0,
                    // Dummy is a v0.1 storage placeholder with no fan-out target;
                    // accept it so a dummy producer doesn't error, but drop it.
                    Some(Signal::Dummy) => true,
                    None => false,
                };
                if !announced_ok {
                    reject(
                        &mut wr,
                        session_id,
                        b.batch_id,
                        REJECT_SIGNAL_NOT_ANNOUNCED,
                        "signal not in Hello.signals",
                    )
                    .await?;
                    continue;
                }

                if b.uncompressed_size > DEFAULT_MAX_BATCH_BYTES {
                    reject(
                        &mut wr,
                        session_id,
                        b.batch_id,
                        REJECT_BATCH_TOO_LARGE,
                        "uncompressed_size > max_batch_bytes",
                    )
                    .await?;
                    continue;
                }

                let decompressed = match b.compression {
                    COMPRESSION_NONE => b.payload.clone(),
                    COMPRESSION_ZSTD => match zstd::decode_all(b.payload.as_slice()) {
                        Ok(d) => d,
                        Err(e) => {
                            warn!(%peer, batch_id = b.batch_id, error = %e, "zstd decompress failed");
                            reject(
                                &mut wr,
                                session_id,
                                b.batch_id,
                                REJECT_BAD_SCHEMA,
                                "zstd decompress failed",
                            )
                            .await?;
                            continue;
                        }
                    },
                    other => {
                        warn!(%peer, batch_id = b.batch_id, compression = other, "unknown compression");
                        reject(
                            &mut wr,
                            session_id,
                            b.batch_id,
                            REJECT_BAD_SCHEMA,
                            "unknown compression codec",
                        )
                        .await?;
                        continue;
                    }
                };

                // Decode into a typed batch and fan it out. Dummy is accepted but
                // has no sink, so it's a no-op.
                let decode_result: Result<()> = match sig.unwrap() {
                    Signal::Logs => LogsBatch::decode(&decompressed)
                        .map(|batch| state.offer_logs(batch))
                        .map_err(|e| anyhow::anyhow!("LogsBatch: {e}")),
                    Signal::Metrics => MetricsBatch::decode(&decompressed)
                        .map(|batch| state.offer_metrics(batch))
                        .map_err(|e| anyhow::anyhow!("MetricsBatch: {e}")),
                    Signal::Traces => TracesBatch::decode(&decompressed)
                        .map(|batch| state.offer_traces(batch))
                        .map_err(|e| anyhow::anyhow!("TracesBatch: {e}")),
                    Signal::Profiles => ProfilesBatch::decode(&decompressed)
                        .map(|batch| state.offer_profiles(batch))
                        .map_err(|e| anyhow::anyhow!("ProfilesBatch: {e}")),
                    Signal::Dummy => Ok(()),
                };

                match decode_result {
                    Ok(()) => {
                        write_frame(
                            &mut wr,
                            &build::batch_ack(session_id, b.batch_id, ACK_ACCEPTED, 0, 0, ""),
                        )
                        .await?;
                    }
                    Err(e) => {
                        warn!(%peer, batch_id = b.batch_id, error = %e, "payload decode failed");
                        write_frame(
                            &mut wr,
                            &build::batch_ack(
                                session_id,
                                b.batch_id,
                                ACK_REJECTED,
                                0,
                                REJECT_BAD_SCHEMA,
                                "payload decode failed",
                            ),
                        )
                        .await?;
                    }
                }
                wr.flush().await?;
            }

            FrameMsg::Ping(p) => {
                write_frame(&mut wr, &build::pong(p.nonce)).await?;
                wr.flush().await?;
            }

            FrameMsg::Goodbye(g) => {
                info!(%peer, session_id, reason = g.reason_code, msg = %g.message, "goodbye");
                let _ = write_frame(&mut wr, &build::goodbye(GOODBYE_NORMAL, "")).await;
                let _ = wr.flush().await;
                break;
            }

            FrameMsg::Hello(_) => {
                let _ = write_frame(
                    &mut wr,
                    &build::error(ERR_HELLO_REQUIRED, "duplicate Hello"),
                )
                .await;
                let _ = wr.flush().await;
                break;
            }

            FrameMsg::Error(e) => {
                warn!(%peer, code = e.code, msg = %e.message, "client sent Error frame");
                break;
            }

            other => {
                tracing::debug!(%peer, kind = short_msg_name(&other), "ignoring client frame");
            }
        }
    }

    let _ = tokio::time::timeout(std::time::Duration::from_millis(200), wr.shutdown()).await;
    Ok(())
}

/// Write a `BatchAck` rejection and flush.
async fn reject<W: tokio::io::AsyncWrite + Unpin>(
    wr: &mut W,
    session_id: u64,
    batch_id: u64,
    reason_code: u16,
    message: &str,
) -> Result<()> {
    write_frame(
        wr,
        &build::batch_ack(session_id, batch_id, ACK_REJECTED, 0, reason_code, message),
    )
    .await?;
    wr.flush().await?;
    Ok(())
}

fn short_msg_name(m: &FrameMsg) -> &'static str {
    match m {
        FrameMsg::Hello(_) => "Hello",
        FrameMsg::HelloAck(_) => "HelloAck",
        FrameMsg::Batch(_) => "Batch",
        FrameMsg::BatchAck(_) => "BatchAck",
        FrameMsg::FlowControl(_) => "FlowControl",
        FrameMsg::Ping(_) => "Ping",
        FrameMsg::Pong(_) => "Pong",
        FrameMsg::Goodbye(_) => "Goodbye",
        FrameMsg::Error(_) => "Error",
        FrameMsg::Subscribe(_) => "Subscribe",
        FrameMsg::TailRecord(_) => "TailRecord",
        FrameMsg::LiveQuery(_) => "LiveQuery",
        FrameMsg::LiveBatch(_) => "LiveBatch",
    }
}

#[cfg(test)]
mod tests {
    use scry_proto::generated::{LogEntry, LogStream, LogsBatch};
    use scry_proto::LabelPair;

    /// The wire path round-trips a `LogsBatch` through encode → decode (the same
    /// `decode()` the listener uses), preserving streams, labels, and entries.
    #[test]
    fn logs_batch_encode_decode_roundtrip() {
        let batch = LogsBatch {
            streams: vec![LogStream {
                fingerprint: 42,
                labels: vec![LabelPair {
                    key: "namespace".into(),
                    value: "prod".into(),
                }],
                entries: vec![LogEntry {
                    ts_unix_nano: 1_700_000_000_000_000_000,
                    severity: 9,
                    body: "hello".into(),
                    attributes: vec![LabelPair {
                        key: "stream".into(),
                        value: "stdout".into(),
                    }],
                }],
            }],
        };
        let bytes = batch.encode().unwrap();
        let back = LogsBatch::decode(&bytes).unwrap();
        assert_eq!(back, batch);
    }
}
