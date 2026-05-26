//! Ingest-server lifecycle: bind, accept, per-connection handshake +
//! Batch/Ping/Goodbye dispatch, graceful shutdown.
//!
//! The server owns the listener and the accept loop. The
//! [`DummyPipeline`] is passed in by the caller (so the same pipeline
//! can be shared with future background tasks). On shutdown the server
//! flushes the pipeline once before returning.

use anyhow::{Context, Result};
use binschema_runtime::{BitOrder, BitStreamDecoder};
use scry_proto::{
    build,
    constants::{
        ACK_ACCEPTED, ACK_REJECTED, COMPRESSION_NONE, COMPRESSION_ZSTD,
        DEFAULT_MAX_BATCH_BYTES, DEFAULT_MAX_INFLIGHT_BATCHES, DEFAULT_SUGGESTED_BATCH_BYTES,
        ERR_HELLO_REQUIRED, ERR_PROTOCOL_VERSION, ERR_SESSION_MISMATCH, GOODBYE_NORMAL,
        PROTOCOL_VERSION_V0, REJECT_BAD_SCHEMA, REJECT_BATCH_TOO_LARGE,
        REJECT_SIGNAL_NOT_ANNOUNCED, SIGNAL_BIT_LOGS, SIGNAL_BIT_METRICS, SIGNAL_BIT_PROFILES,
        SIGNAL_BIT_TRACES, Signal,
    },
    framing::{FrameError, read_frame, write_frame},
    generated::{
        DummyBatch, FrameMsg, HelloOutput, LogsBatch, MetricsBatch, ProfilesBatch, TracesBatch,
    },
};
use std::{
    future::Future,
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncWriteExt, BufReader, BufWriter},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};
use tracing::{info, warn};
use uuid::Uuid;

use crate::pipeline::DummyPipeline;

/// Static configuration for a [`Server`]. Cheap to construct, cloned
/// into each spawned connection task.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    /// `host:port` to bind. Anything `TcpListener::bind` accepts.
    pub listen_addr: String,
    /// Identifier announced to agents in `HelloAck.writer_id`. Human-
    /// readable; not parsed.
    pub writer_id: String,
    /// UUIDv7 used as the writer identity in block paths + WAL replay.
    /// One per process; reusing it across restarts is what allows the
    /// WAL to replay into the same writer's lineage.
    pub writer_uuid: Uuid,
}

/// The ingest server. Constructed with [`Server::new`], driven to
/// completion with [`Server::serve_with_shutdown`].
pub struct Server {
    config: ServerConfig,
    pipeline: Option<Arc<Mutex<DummyPipeline>>>,
}

impl Server {
    pub fn new(config: ServerConfig, pipeline: Option<Arc<Mutex<DummyPipeline>>>) -> Self {
        Self { config, pipeline }
    }

    /// Bind the listener, accept connections until `shutdown`
    /// completes, then flush the pipeline (if any) and return. The
    /// shutdown future is typically `tokio::signal::ctrl_c()`, but
    /// any `Future<Output = ()>` works — pass an `oneshot::Receiver`
    /// from the supervisor in the eventual single-binary world.
    pub async fn serve_with_shutdown<F>(self, shutdown: F) -> Result<()>
    where
        F: Future<Output = ()>,
    {
        let listener = TcpListener::bind(&self.config.listen_addr)
            .await
            .with_context(|| format!("binding {}", self.config.listen_addr))?;
        info!(
            addr = %self.config.listen_addr,
            writer_id = %self.config.writer_id,
            writer_uuid = %self.config.writer_uuid,
            "scry-server listening"
        );

        let next_session_id = Arc::new(AtomicU64::new(1));
        let config = Arc::new(self.config);

        let accept_loop = async {
            loop {
                let (sock, peer) = listener.accept().await?;
                let session_id = next_session_id.fetch_add(1, Ordering::Relaxed);
                let config = config.clone();
                let pipeline = self.pipeline.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle(sock, peer, config, session_id, pipeline).await {
                        warn!(peer = %peer, error = %e, "connection ended with error");
                    }
                });
            }
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        };

        tokio::select! {
            r = accept_loop => { r?; }
            _ = shutdown    => { info!("shutdown signalled; flushing"); }
        }

        if let Some(pipe) = self.pipeline.as_ref() {
            let mut guard = pipe.lock().await;
            if let Err(e) = guard.flush().await {
                warn!(error = %e, "final flush failed");
            } else {
                info!("final flush complete");
            }
        }

        Ok(())
    }
}

#[derive(Default)]
struct Counters {
    batches:           AtomicU64,
    metric_samples:    AtomicU64,
    log_entries:       AtomicU64,
    spans:             AtomicU64,
    profile_blobs:     AtomicU64,
    dummy_records:     AtomicU64,
    payload_bytes_in:  AtomicU64, // compressed
    payload_bytes_out: AtomicU64, // decompressed
    rejected:          AtomicU64,
}

async fn handle(
    sock: TcpStream,
    peer: SocketAddr,
    config: Arc<ServerConfig>,
    session_id: u64,
    pipeline: Option<Arc<Mutex<DummyPipeline>>>,
) -> Result<()> {
    sock.set_nodelay(true)?;
    let (rd, wr) = sock.into_split();
    let mut rd = BufReader::new(rd);
    let mut wr = BufWriter::new(wr);

    info!(%peer, session_id, writer_uuid = %config.writer_uuid, "accept");

    // ── Handshake ──────────────────────────────────────────────────────
    let first = match read_frame(&mut rd).await {
        Ok(f) => f,
        Err(e) => {
            warn!(%peer, error = %e, "no frame before handshake");
            return Ok(());
        }
    };
    let hello: HelloOutput = match first.msg {
        FrameMsg::Hello(h) => h,
        other => {
            let _ = write_frame(&mut wr, &build::error(ERR_HELLO_REQUIRED, "Hello required first")).await;
            let _ = wr.flush().await;
            warn!(%peer, kind = short_msg_name(&other), "non-Hello first frame");
            return Ok(());
        }
    };

    if hello.protocol_version != PROTOCOL_VERSION_V0 {
        let _ = write_frame(
            &mut wr,
            &build::error(
                ERR_PROTOCOL_VERSION,
                &format!(
                    "server supports v{:#06x}; agent asked for v{:#06x}",
                    PROTOCOL_VERSION_V0, hello.protocol_version
                ),
            ),
        )
        .await;
        let _ = wr.flush().await;
        warn!(%peer, ver = hello.protocol_version, "unsupported protocol version");
        return Ok(());
    }

    info!(
        %peer,
        session_id,
        agent_version = %hello.agent_version,
        hostname = %hello.hostname,
        signals = format!("{:#06b}", hello.signals),
        attrs = hello.resource_attrs.len(),
        "hello"
    );

    write_frame(
        &mut wr,
        &build::hello_ack(build::HelloAckArgs {
            protocol_version: PROTOCOL_VERSION_V0,
            writer_id: &config.writer_id,
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
    let counters = Counters::default();
    let signals_announced = hello.signals;

    loop {
        let frame = match read_frame(&mut rd).await {
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
                // Dummy is v0.1-only and has no Hello.signals bit; we
                // accept it unconditionally as long as it decodes.
                // Every real signal must be in the announce mask.
                let announced_ok = match sig {
                    Some(Signal::Dummy) => true,
                    Some(Signal::Metrics)  => signals_announced & SIGNAL_BIT_METRICS  != 0,
                    Some(Signal::Logs)     => signals_announced & SIGNAL_BIT_LOGS     != 0,
                    Some(Signal::Traces)   => signals_announced & SIGNAL_BIT_TRACES   != 0,
                    Some(Signal::Profiles) => signals_announced & SIGNAL_BIT_PROFILES != 0,
                    None => false,
                };
                if !announced_ok {
                    counters.rejected.fetch_add(1, Ordering::Relaxed);
                    write_frame(
                        &mut wr,
                        &build::batch_ack(
                            session_id,
                            b.batch_id,
                            ACK_REJECTED,
                            0,
                            REJECT_SIGNAL_NOT_ANNOUNCED,
                            "signal not in Hello.signals",
                        ),
                    )
                    .await?;
                    wr.flush().await?;
                    continue;
                }

                if b.uncompressed_size > DEFAULT_MAX_BATCH_BYTES {
                    counters.rejected.fetch_add(1, Ordering::Relaxed);
                    write_frame(
                        &mut wr,
                        &build::batch_ack(
                            session_id,
                            b.batch_id,
                            ACK_REJECTED,
                            0,
                            REJECT_BATCH_TOO_LARGE,
                            "uncompressed_size > max_batch_bytes",
                        ),
                    )
                    .await?;
                    wr.flush().await?;
                    continue;
                }

                counters.batches.fetch_add(1, Ordering::Relaxed);
                counters
                    .payload_bytes_in
                    .fetch_add(b.payload.len() as u64, Ordering::Relaxed);

                let decompressed = match b.compression {
                    COMPRESSION_NONE => b.payload.clone(),
                    COMPRESSION_ZSTD => match zstd::decode_all(b.payload.as_slice()) {
                        Ok(d) => d,
                        Err(e) => {
                            counters.rejected.fetch_add(1, Ordering::Relaxed);
                            warn!(%peer, batch_id = b.batch_id, error = %e, "zstd decompress failed");
                            write_frame(
                                &mut wr,
                                &build::batch_ack(
                                    session_id,
                                    b.batch_id,
                                    ACK_REJECTED,
                                    0,
                                    REJECT_BAD_SCHEMA,
                                    "zstd decompress failed",
                                ),
                            )
                            .await?;
                            wr.flush().await?;
                            continue;
                        }
                    },
                    other => {
                        warn!(%peer, batch_id = b.batch_id, compression = other, "unknown compression");
                        counters.rejected.fetch_add(1, Ordering::Relaxed);
                        write_frame(
                            &mut wr,
                            &build::batch_ack(
                                session_id,
                                b.batch_id,
                                ACK_REJECTED,
                                0,
                                REJECT_BAD_SCHEMA,
                                "unknown compression codec",
                            ),
                        )
                        .await?;
                        wr.flush().await?;
                        continue;
                    }
                };

                if decompressed.len() != b.uncompressed_size as usize {
                    warn!(
                        %peer,
                        batch_id = b.batch_id,
                        claimed = b.uncompressed_size,
                        actual = decompressed.len(),
                        "uncompressed_size mismatch"
                    );
                }
                counters
                    .payload_bytes_out
                    .fetch_add(decompressed.len() as u64, Ordering::Relaxed);

                let signal = sig.unwrap();

                // Dummy gets the WAL+block path; other signals just
                // get counted (v0.1 storage is Dummy-only). If the
                // storage pipeline is enabled we run the whole
                // WAL-append + builder-append + maybe-flush sequence
                // under the pipeline mutex.
                let decode_result: Result<u64> = if signal == Signal::Dummy {
                    let mut decoder = BitStreamDecoder::new(&decompressed, BitOrder::MsbFirst);
                    match DummyBatch::decode_with_decoder(&mut decoder) {
                        Ok(batch) => {
                            if let Some(pipe) = pipeline.as_ref() {
                                let mut guard = pipe.lock().await;
                                guard.ingest(&decompressed, batch).await
                            } else {
                                Ok(batch.records.len() as u64)
                            }
                        }
                        Err(e) => Err(anyhow::anyhow!("DummyBatch: {e}")),
                    }
                } else {
                    decode_payload(signal, &decompressed)
                };

                match decode_result {
                    Ok(records) => {
                        match signal {
                            Signal::Metrics  => counters.metric_samples.fetch_add(records, Ordering::Relaxed),
                            Signal::Logs     => counters.log_entries   .fetch_add(records, Ordering::Relaxed),
                            Signal::Traces   => counters.spans         .fetch_add(records, Ordering::Relaxed),
                            Signal::Profiles => counters.profile_blobs .fetch_add(records, Ordering::Relaxed),
                            Signal::Dummy    => counters.dummy_records .fetch_add(records, Ordering::Relaxed),
                        };
                        write_frame(
                            &mut wr,
                            &build::batch_ack(session_id, b.batch_id, ACK_ACCEPTED, 0, 0, ""),
                        )
                        .await?;
                    }
                    Err(e) => {
                        counters.rejected.fetch_add(1, Ordering::Relaxed);
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
                // Echo a Goodbye back for symmetry, then close.
                let _ = write_frame(&mut wr, &build::goodbye(GOODBYE_NORMAL, "")).await;
                let _ = wr.flush().await;
                break;
            }

            FrameMsg::Hello(_) => {
                let _ = write_frame(&mut wr, &build::error(ERR_HELLO_REQUIRED, "duplicate Hello")).await;
                let _ = wr.flush().await;
                break;
            }

            FrameMsg::Error(e) => {
                warn!(%peer, code = e.code, msg = %e.message, "agent sent Error frame");
                break;
            }

            other => {
                tracing::debug!(%peer, kind = short_msg_name(&other), "ignoring agent frame");
            }
        }
    }

    let summary = format!(
        "session_id={} batches={} samples={} log_entries={} spans={} profiles={} dummy={} \
         bytes_in={} bytes_out={} rejected={}",
        session_id,
        counters.batches.load(Ordering::Relaxed),
        counters.metric_samples.load(Ordering::Relaxed),
        counters.log_entries.load(Ordering::Relaxed),
        counters.spans.load(Ordering::Relaxed),
        counters.profile_blobs.load(Ordering::Relaxed),
        counters.dummy_records.load(Ordering::Relaxed),
        counters.payload_bytes_in.load(Ordering::Relaxed),
        counters.payload_bytes_out.load(Ordering::Relaxed),
        counters.rejected.load(Ordering::Relaxed),
    );
    info!(%peer, "{}", summary);

    let _ = tokio::time::timeout(Duration::from_millis(200), wr.shutdown()).await;
    Ok(())
}

fn decode_payload(signal: Signal, bytes: &[u8]) -> Result<u64> {
    let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
    let n: u64 = match signal {
        Signal::Metrics => {
            let m = MetricsBatch::decode_with_decoder(&mut decoder)
                .map_err(|e| anyhow::anyhow!("MetricsBatch: {e}"))?;
            // Records = samples (series are dictionary entries, not records).
            m.samples.len() as u64
        }
        Signal::Logs => {
            let l = LogsBatch::decode_with_decoder(&mut decoder)
                .map_err(|e| anyhow::anyhow!("LogsBatch: {e}"))?;
            l.streams.iter().map(|s| s.entries.len() as u64).sum()
        }
        Signal::Traces => {
            let t = TracesBatch::decode_with_decoder(&mut decoder)
                .map_err(|e| anyhow::anyhow!("TracesBatch: {e}"))?;
            t.spans.len() as u64
        }
        Signal::Profiles => {
            let p = ProfilesBatch::decode_with_decoder(&mut decoder)
                .map_err(|e| anyhow::anyhow!("ProfilesBatch: {e}"))?;
            p.samples.len() as u64
        }
        Signal::Dummy => {
            // Reachable only when storage mode is off (the storage
            // branch decodes inline). Cheap to keep the symmetry.
            let d = DummyBatch::decode_with_decoder(&mut decoder)
                .map_err(|e| anyhow::anyhow!("DummyBatch: {e}"))?;
            d.records.len() as u64
        }
    };
    Ok(n)
}

fn short_msg_name(m: &FrameMsg) -> &'static str {
    match m {
        FrameMsg::Hello(_)       => "Hello",
        FrameMsg::HelloAck(_)    => "HelloAck",
        FrameMsg::Batch(_)       => "Batch",
        FrameMsg::BatchAck(_)    => "BatchAck",
        FrameMsg::FlowControl(_) => "FlowControl",
        FrameMsg::Ping(_)        => "Ping",
        FrameMsg::Pong(_)        => "Pong",
        FrameMsg::Goodbye(_)     => "Goodbye",
        FrameMsg::Error(_)       => "Error",
    }
}
