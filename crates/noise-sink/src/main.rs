//! noise-sink — minimal scry ingest server, *no storage layer*.
//!
//! Accepts TCP connections, completes the handshake, decodes Batch
//! payloads (after zstd decompression), validates them against the
//! announced signal's schema, and replies with BatchAck. Maintains
//! per-connection counters and prints a summary on disconnect.
//!
//! Run:
//!   noise-sink --listen 127.0.0.1:4000
//!
//! Designed strictly for protocol exercise. The next iteration replaces
//! this with the real ingest server (WAL + block builder + parquet
//! upload).

use anyhow::{Context, Result};
use binschema_runtime::{BitOrder, BitStreamDecoder};
use clap::Parser;
use object_store::ObjectStore;
use scry_block::{BlockBuilderConfig, DummyBlockBuilder};
use scry_objstore::{open as open_objstore, ObjStoreConfig};
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
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncWriteExt, BufReader, BufWriter},
    net::{TcpListener, TcpStream},
};
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Listen address.
    #[arg(long, default_value = "127.0.0.1:4000")]
    listen: String,

    /// writer_id reported in HelloAck. Default: random per-process.
    #[arg(long)]
    writer_id: Option<String>,

    /// Enable the v0.1 storage path: Dummy batches are accumulated
    /// into parquet blocks and uploaded to object storage. Requires
    /// `SCRY_OBJSTORE_*` env vars (see `docker/garage/.env`).
    #[arg(long)]
    storage: bool,
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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let args = Args::parse();

    let writer_id = args
        .writer_id
        .unwrap_or_else(|| format!("noise-sink-{}", rand_short()));
    let writer_uuid = Uuid::now_v7();

    // Open the object store up front if storage mode is on; failing
    // fast here is much better than failing on the first Dummy batch.
    let store: Option<Arc<dyn ObjectStore>> = if args.storage {
        let cfg = ObjStoreConfig::from_env()
            .context("loading SCRY_OBJSTORE_* env (try `source docker/garage/.env`)")?;
        info!(
            endpoint = %cfg.endpoint,
            bucket   = %cfg.bucket,
            "storage mode: writing Dummy blocks to object storage"
        );
        Some(open_objstore(&cfg)?)
    } else {
        None
    };

    let listener = TcpListener::bind(&args.listen)
        .await
        .with_context(|| format!("binding {}", args.listen))?;
    info!(addr = %args.listen, writer_id, %writer_uuid, "noise-sink listening");

    let next_session_id = Arc::new(AtomicU64::new(1));

    loop {
        let (sock, peer) = listener.accept().await?;
        let session_id = next_session_id.fetch_add(1, Ordering::Relaxed);
        let writer_id = writer_id.clone();
        let store = store.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(sock, peer, writer_id, writer_uuid, session_id, store).await {
                warn!(peer = %peer, error = %e, "connection ended with error");
            }
        });
    }
}

async fn handle(
    sock: TcpStream,
    peer: SocketAddr,
    writer_id: String,
    writer_uuid: Uuid,
    session_id: u64,
    store: Option<Arc<dyn ObjectStore>>,
) -> Result<()> {
    sock.set_nodelay(true)?;
    let (rd, wr) = sock.into_split();
    let mut rd = BufReader::new(rd);
    let mut wr = BufWriter::new(wr);

    info!(%peer, session_id, "accept");

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
            writer_id: &writer_id,
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

    // Per-session block builder for Dummy records. `None` either
    // because storage mode is off or because the session has not yet
    // received a Dummy batch — lazy so non-Dummy sessions don't
    // allocate arrow buffers they'll never use.
    let mut dummy_block: Option<DummyBlockBuilder> = None;

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

                // Dummy gets a custom path: decode to records, push
                // each into the per-session DummyBlockBuilder when
                // storage is on, then count.
                let decode_result: Result<u64> = if signal == Signal::Dummy {
                    let mut decoder = BitStreamDecoder::new(&decompressed, BitOrder::MsbFirst);
                    DummyBatch::decode_with_decoder(&mut decoder)
                        .map_err(|e| anyhow::anyhow!("DummyBatch: {e}"))
                        .map(|d| {
                            let n = d.records.len() as u64;
                            if store.is_some() {
                                let builder = dummy_block.get_or_insert_with(|| {
                                    DummyBlockBuilder::new(
                                        writer_uuid,
                                        BlockBuilderConfig::default(),
                                    )
                                });
                                for rec in d.records {
                                    builder.append(rec);
                                }
                            }
                            n
                        })
                } else {
                    decode_payload(signal, &decompressed)
                };

                // If the builder filled up, flush it now so a long
                // session doesn't accumulate a giant block in RAM.
                if let (Some(store), Some(builder)) = (store.as_ref(), dummy_block.as_ref()) {
                    if builder.should_close() {
                        // Take ownership to consume `finish_and_upload`.
                        let b = dummy_block.take().unwrap();
                        if let Err(e) = b.finish_and_upload(store.as_ref()).await {
                            warn!(%peer, error = %e, "dummy block upload failed mid-session");
                        }
                    }
                }

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

    // Flush any in-progress Dummy block before tearing the session
    // down. Failure here is logged but does not propagate — the peer
    // is already gone and there's nothing useful we can do about it.
    if let (Some(store), Some(b)) = (store.as_ref(), dummy_block.take()) {
        if !b.is_empty() {
            match b.finish_and_upload(store.as_ref()).await {
                Ok(Some(meta)) => info!(
                    session_id,
                    block_uuid = %meta.uuid,
                    row_count = meta.row_count,
                    "dummy block flushed on session close"
                ),
                Ok(None) => {}
                Err(e) => warn!(%peer, error = %e, "dummy block flush failed"),
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

fn rand_short() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ns = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
    format!("{:08x}", ns & 0xFFFF_FFFF)
}
