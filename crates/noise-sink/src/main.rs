//! noise-sink — minimal scry ingest server.
//!
//! Accepts TCP connections, completes the handshake, decodes Batch
//! payloads (after zstd decompression), validates them against the
//! announced signal's schema, and replies with BatchAck. Maintains
//! per-connection counters and prints a summary on disconnect.
//!
//! With `--storage --wal-dir DIR` the Dummy ingest path is *durable*:
//! every accepted DummyBatch is appended to a local WAL, the records
//! are added to an in-memory parquet builder, and the builder is
//! flushed to object storage when it fills (or on graceful shutdown).
//! The WAL is replayed at startup so an unclean exit doesn't lose
//! acknowledged records.
//!
//! Run (no storage):
//!   noise-sink --listen 127.0.0.1:4000
//!
//! Run (v0.1 storage path):
//!   source docker/garage/.env
//!   noise-sink --listen 127.0.0.1:4000 --storage --wal-dir ./wal

use anyhow::{Context, Result};
use binschema_runtime::{BitOrder, BitStreamDecoder};
use clap::Parser;
use object_store::ObjectStore;
use scry_block::{BlockBuilderConfig, DummyBlockBuilder};
use scry_catalog::Catalog;
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
use scry_wal::{Wal, WalConfig};
use std::{
    net::SocketAddr,
    path::PathBuf,
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

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Listen address.
    #[arg(long, default_value = "127.0.0.1:4000")]
    listen: String,

    /// writer_id reported in HelloAck. Default: random per-process.
    #[arg(long)]
    writer_id: Option<String>,

    /// Enable the v0.1 storage path: Dummy batches are durably
    /// recorded in the WAL, accumulated into parquet blocks, and
    /// uploaded to object storage. Requires `--wal-dir` and the
    /// `SCRY_OBJSTORE_*` env vars (see `docker/garage/.env`).
    #[arg(long)]
    storage: bool,

    /// Root directory for the WAL. A `dummy/` subdirectory is created
    /// for v0.1; real signals get their own subdirs later. Required
    /// when `--storage` is set.
    #[arg(long)]
    wal_dir: Option<PathBuf>,

    /// Path to the SQLite catalog file. If provided, every uploaded
    /// block is recorded into the catalog inline (no reconcile loop
    /// needed for catalog freshness). The file is created with the
    /// canonical schema if it doesn't already exist. Optional —
    /// scry-list can always rebuild the catalog from the bucket via
    /// `reconcile_from_bucket`.
    #[arg(long)]
    catalog: Option<PathBuf>,
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

/// Process-scoped Dummy durability pipeline: a WAL, an active block
/// builder, and the object store the builder uploads to. Shared
/// across sessions via `Arc<Mutex<_>>`. Per `ARCHITECTURE.md § The WAL`
/// — the WAL is per-writer, not per-session.
struct DummyPipeline {
    wal: Wal,
    builder: DummyBlockBuilder,
    store: Arc<dyn ObjectStore>,
    /// Optional online catalog. Updates here are best-effort: a failed
    /// insert (sqlite locked, disk full, etc.) is logged but does not
    /// fail the ingest path, because the bucket is the source of
    /// truth and a future `scry-list --reconcile` would re-derive the
    /// row anyway.
    catalog: Option<Catalog>,
    writer_uuid: Uuid,
    cfg: BlockBuilderConfig,
}

impl DummyPipeline {
    /// Open the WAL, replay any leftover records into a fresh builder,
    /// and return a pipeline ready to ingest. The replayed records
    /// are *not* re-acked to the agent (agents will resend any
    /// in-flight batches they hadn't yet seen an ack for, and dedup
    /// is a v0.2 concern), but they are durable and will be uploaded
    /// in the next flush.
    async fn open(
        wal_dir: PathBuf,
        store: Arc<dyn ObjectStore>,
        catalog: Option<Catalog>,
        writer_uuid: Uuid,
    ) -> Result<Self> {
        let wal = Wal::open(WalConfig::new(wal_dir, "dummy"))
            .await
            .context("opening Dummy WAL")?;

        let cfg = BlockBuilderConfig::default();
        let mut builder = DummyBlockBuilder::new(writer_uuid, cfg);
        let mut replayed_records = 0u64;
        let mut replayed_frames = 0u64;
        for frame in wal.replay().context("scanning WAL for replay")? {
            let payload = frame.context("reading WAL frame")?;
            let mut dec = BitStreamDecoder::new(&payload, BitOrder::MsbFirst);
            let batch = DummyBatch::decode_with_decoder(&mut dec)
                .map_err(|e| anyhow::anyhow!("WAL replay: decode DummyBatch: {e}"))?;
            for rec in batch.records {
                builder.append(rec);
                replayed_records += 1;
            }
            replayed_frames += 1;
        }
        if replayed_records > 0 {
            info!(
                replayed_records,
                replayed_frames, "WAL replay complete; records merged into next block"
            );
        }

        let _ = replayed_records; // silence unused if we ever drop the log
        Ok(Self {
            wal,
            builder,
            store,
            catalog,
            writer_uuid,
            cfg,
        })
    }

    /// Append a single DummyBatch payload (already zstd-decoded, the
    /// binschema-encoded form is what hits the WAL). On replay we
    /// decode the same bytes back into records, so this is the unit
    /// of crash-recovery atomicity. Auto-flushes if the builder hits
    /// its close threshold.
    async fn ingest(&mut self, payload: &[u8], batch: DummyBatch) -> Result<u64> {
        // Order matters: WAL first, builder second. If the WAL append
        // fails we never put the records into the in-memory builder
        // — the agent will see the resulting BatchAck failure and
        // retry. If the WAL append succeeds but builder.append
        // somehow fails (it can't, today — Vec::push is infallible),
        // the records are still durable and will appear via replay
        // on next start.
        self.wal.append(payload).await.context("WAL append")?;

        let n = batch.records.len() as u64;
        for rec in batch.records {
            self.builder.append(rec);
        }

        if self.builder.should_close() {
            self.flush().await?;
        }
        Ok(n)
    }

    /// Seal the active WAL segment, upload the active block, and
    /// delete the uploaded WAL segments. A no-op if the builder is
    /// empty (no segment to seal, nothing to upload).
    async fn flush(&mut self) -> Result<()> {
        if self.builder.is_empty() {
            return Ok(());
        }
        // Rotate the WAL *before* we drain the builder. Everything we
        // are about to upload is contained in (current segment & all
        // earlier sealed-but-not-uploaded segments). After rotation,
        // any subsequent appends go into a fresh segment that does
        // not participate in this block.
        let sealed = self.wal.rotate().await.context("WAL rotate on flush")?;

        let new_builder = DummyBlockBuilder::new(self.writer_uuid, self.cfg);
        let old_builder = std::mem::replace(&mut self.builder, new_builder);
        match old_builder.finish_and_upload(self.store.as_ref()).await {
            Ok(Some(meta)) => {
                self.wal
                    .mark_uploaded(sealed)
                    .await
                    .context("WAL mark_uploaded after block upload")?;
                // Catalog update is best-effort by design: the bucket
                // is the source of truth, and reconcile_from_bucket
                // can always re-derive a missing row. We don't want a
                // transient sqlite hiccup to fail the ingest path.
                if let Some(cat) = self.catalog.as_ref() {
                    match cat.insert_block(&meta) {
                        Ok(true) => {}
                        Ok(false) => {
                            tracing::debug!(block_uuid = %meta.uuid, "catalog row already present");
                        }
                        Err(e) => {
                            warn!(
                                block_uuid = %meta.uuid,
                                error = %e,
                                "catalog insert failed; bucket has the data — recover via scry-list --reconcile"
                            );
                        }
                    }
                }
                info!(
                    block_uuid = %meta.uuid,
                    row_count = meta.row_count,
                    byte_size = meta.byte_size,
                    "dummy block uploaded; WAL segments through {} released",
                    sealed.0,
                );
            }
            Ok(None) => {
                // Builder was empty after rotation — vanishingly
                // unlikely since we checked above, but possible if
                // someone called flush() under tight races. Leave the
                // sealed WAL segment in place; replay will pick it up
                // next time.
                warn!("flush() produced no block; WAL segment retained for replay");
            }
            Err(e) => {
                // The upload failed. The sealed WAL segment is *not*
                // marked uploaded, so a future flush (or next-start
                // replay) will retry. Returning the error lets the
                // caller decide whether the ingest path should fail
                // the batch.
                return Err(e.context("dummy block upload"));
            }
        }
        Ok(())
    }
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

    // Build the storage pipeline up front. Failing fast on a missing
    // bucket or unreadable WAL dir is much better than failing on the
    // first Dummy batch from an agent that's already mid-stream.
    let pipeline: Option<Arc<Mutex<DummyPipeline>>> = if args.storage {
        let wal_dir = args
            .wal_dir
            .clone()
            .context("--storage requires --wal-dir")?;
        let cfg = ObjStoreConfig::from_env()
            .context("loading SCRY_OBJSTORE_* env (try `source docker/garage/.env`)")?;
        let bucket = cfg.bucket.clone();
        info!(
            endpoint = %cfg.endpoint,
            bucket   = %bucket,
            wal_dir  = %wal_dir.display(),
            catalog  = ?args.catalog,
            "storage mode: WAL + parquet blocks → object storage"
        );
        let store = open_objstore(&cfg)?;
        let catalog = match args.catalog.as_ref() {
            Some(p) => Some(
                Catalog::open(p, &bucket)
                    .with_context(|| format!("opening catalog at {}", p.display()))?,
            ),
            None => None,
        };
        let pipe = DummyPipeline::open(wal_dir, store, catalog, writer_uuid).await?;
        Some(Arc::new(Mutex::new(pipe)))
    } else {
        if args.wal_dir.is_some() {
            warn!("--wal-dir set but --storage is not; ignoring WAL");
        }
        if args.catalog.is_some() {
            warn!("--catalog set but --storage is not; ignoring catalog");
        }
        None
    };

    let listener = TcpListener::bind(&args.listen)
        .await
        .with_context(|| format!("binding {}", args.listen))?;
    info!(addr = %args.listen, writer_id, %writer_uuid, "noise-sink listening");

    let next_session_id = Arc::new(AtomicU64::new(1));

    // Accept loop. Ctrl-C breaks out of the loop and triggers a
    // graceful flush of the in-progress block (if any).
    let accept_loop = async {
        loop {
            let (sock, peer) = listener.accept().await?;
            let session_id = next_session_id.fetch_add(1, Ordering::Relaxed);
            let writer_id = writer_id.clone();
            let pipeline = pipeline.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    handle(sock, peer, writer_id, writer_uuid, session_id, pipeline).await
                {
                    warn!(peer = %peer, error = %e, "connection ended with error");
                }
            });
        }
        #[allow(unreachable_code)]
        Ok::<(), anyhow::Error>(())
    };

    tokio::select! {
        r = accept_loop => { r?; }
        _ = tokio::signal::ctrl_c() => {
            info!("ctrl-c received; flushing");
        }
    }

    if let Some(pipe) = pipeline.as_ref() {
        let mut guard = pipe.lock().await;
        if let Err(e) = guard.flush().await {
            warn!(error = %e, "final flush failed");
        } else {
            info!("final flush complete");
        }
    }

    Ok(())
}

async fn handle(
    sock: TcpStream,
    peer: SocketAddr,
    writer_id: String,
    writer_uuid: Uuid,
    session_id: u64,
    pipeline: Option<Arc<Mutex<DummyPipeline>>>,
) -> Result<()> {
    sock.set_nodelay(true)?;
    let (rd, wr) = sock.into_split();
    let mut rd = BufReader::new(rd);
    let mut wr = BufWriter::new(wr);

    info!(%peer, session_id, %writer_uuid, "accept");

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

fn rand_short() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ns = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
    format!("{:08x}", ns & 0xFFFF_FFFF)
}
