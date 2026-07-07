//! Ingest-server lifecycle: bind, accept, per-connection handshake +
//! Batch/Ping/Goodbye dispatch, graceful shutdown.
//!
//! The server owns the listener and the accept loop. The
//! [`DummyPipeline`] is passed in by the caller (so the same pipeline
//! can be shared with future background tasks). On shutdown the server
//! flushes the pipeline once before returning.

use anyhow::{Context, Result};
use scry_block::{
    BlockBuilder, DummyBlockBuilder, LogsBlockBuilder, MetricsBlockBuilder, ProfilesBlockBuilder,
    TracesBlockBuilder,
};
use scry_proto::{
    build,
    constants::{
        Signal, ACK_ACCEPTED, ACK_REJECTED, COMPRESSION_NONE, COMPRESSION_ZSTD,
        DEFAULT_MAX_BATCH_BYTES, DEFAULT_MAX_INFLIGHT_BATCHES, DEFAULT_SUGGESTED_BATCH_BYTES,
        ERR_BAD_MATCHER, ERR_HELLO_REQUIRED, ERR_PROTOCOL_VERSION, ERR_SESSION_MISMATCH,
        GOODBYE_NORMAL, PROTOCOL_VERSION_V0, REJECT_BAD_SCHEMA, REJECT_BATCH_TOO_LARGE,
        REJECT_SIGNAL_NOT_ANNOUNCED, SIGNAL_BIT_LOGS, SIGNAL_BIT_METRICS, SIGNAL_BIT_PROFILES,
        SIGNAL_BIT_TRACES,
    },
    framing::{read_frame, write_frame, FrameError},
    generated::{FrameMsg, HelloOutput, LiveRecord},
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
};
use tracing::{info, warn};
use uuid::Uuid;

use crate::live_ring::{LiveLogRecord, LiveRing, RetainingLogsAppender};
use crate::pipeline::{DecodeFn, Pipeline, ShardedPipeline};
use crate::stats::ServerMetrics;
use crate::tail::{SubscriptionRegistry, TappingLogsAppender};

/// Per-subscriber live-tail delivery channel depth. Bounds how many
/// records can queue for a slow tail client before the ingest tap starts
/// dropping (best-effort — see [`crate::tail`]). A few thousand entries is
/// a fraction of a second of a busy stream; larger just delays the drop.
const TAIL_CHANNEL_CAP: usize = 4096;

/// Type alias for the Dummy storage pipeline. Same generic machinery
/// every signal uses; the type alias just spares call sites from
/// repeating the builder parameter.
pub type DummyPipeline = Pipeline<DummyBlockBuilder>;

/// Type alias for the Metrics storage pipeline. Same generic
/// machinery as Dummy — only the builder, the WAL signal subdir, and
/// the decode function differ.
pub type MetricsPipeline = Pipeline<MetricsBlockBuilder>;

/// Type alias for the Logs storage pipeline. Same generic machinery
/// as Dummy / Metrics; the LogsBlockBuilder owns the per-signal
/// parquet schema and postings layout.
pub type LogsPipeline = Pipeline<LogsBlockBuilder>;

/// Type alias for the Traces storage pipeline. The TracesBlockBuilder
/// owns the per-signal nested parquet schema (one row per span, with
/// native `List<Struct>` events/links); no postings (trace-by-id rides
/// row-group `trace_id` stats).
pub type TracesPipeline = Pipeline<TracesBlockBuilder>;

/// Type alias for the Profiles storage pipeline. The
/// ProfilesBlockBuilder stores one row per blob with the pprof bytes
/// verbatim in an opaque Binary column; no postings.
pub type ProfilesPipeline = Pipeline<ProfilesBlockBuilder>;

/// Sharded (per-connection-striped) variants of the storage
/// pipelines — what the server actually holds. See [`ShardedPipeline`].
pub type DummyShards = ShardedPipeline<DummyBlockBuilder>;
pub type MetricsShards = ShardedPipeline<MetricsBlockBuilder>;
pub type LogsShards = ShardedPipeline<LogsBlockBuilder>;
pub type TracesShards = ShardedPipeline<TracesBlockBuilder>;
pub type ProfilesShards = ShardedPipeline<ProfilesBlockBuilder>;

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
///
/// One [`ShardedPipeline`] per signal — at N=3 signals the three
/// `Option` fields are still clearer than a
/// `HashMap<Signal, Box<dyn ErasedPipeline>>`. Each signal's pipeline is
/// internally sharded (see [`ShardedPipeline`]) so the per-signal ingest
/// mutex is no longer a single contention point; a connection is pinned
/// to one shard by its session id.
pub struct Server {
    config: ServerConfig,
    dummy_pipeline: Option<ShardedPipeline<DummyBlockBuilder>>,
    metrics_pipeline: Option<ShardedPipeline<MetricsBlockBuilder>>,
    logs_pipeline: Option<ShardedPipeline<LogsBlockBuilder>>,
    traces_pipeline: Option<ShardedPipeline<TracesBlockBuilder>>,
    profiles_pipeline: Option<ShardedPipeline<ProfilesBlockBuilder>>,
    /// Optional process-global stats. When present, the ingest path
    /// bumps it at batch granularity and the connection count is
    /// tracked; the stats HTTP endpoint reads from the same `Arc`.
    metrics: Option<Arc<ServerMetrics>>,
    /// Maximum age of an open block before it's sealed regardless of
    /// size. `None` disables the time-based flush (size-only — the old
    /// behaviour). Set via [`Server::with_block_max_age`]. Without it a
    /// low-volume/idle signal never crosses the size threshold, so its
    /// records never seal and never become queryable.
    block_max_age: Option<Duration>,
    /// Process-local live-tail subscription registry, shared across every
    /// connection handler. Free when idle (the ingest tap gates on
    /// `subscriber_count() > 0`); populated by `Subscribe` frames. See
    /// [`crate::tail`] and D-050.
    tail: Arc<SubscriptionRegistry>,
    /// Optional retained recent-window ring — the *live source* for the
    /// merged history+live query (D-054). When present, the logs decode path
    /// collects each batch's entries and pushes them (tagged with their WAL
    /// shard+segment) so a `LiveQuery` can serve the last window from
    /// memory. `None` ⇒ live query disabled and zero cost on the logs path
    /// (byte-identical to pre-D-054). Only the logs signal feeds it.
    live_ring: Option<Arc<LiveRing>>,
}

impl Server {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: ServerConfig,
        dummy_pipeline: Option<ShardedPipeline<DummyBlockBuilder>>,
        metrics_pipeline: Option<ShardedPipeline<MetricsBlockBuilder>>,
        logs_pipeline: Option<ShardedPipeline<LogsBlockBuilder>>,
        traces_pipeline: Option<ShardedPipeline<TracesBlockBuilder>>,
        profiles_pipeline: Option<ShardedPipeline<ProfilesBlockBuilder>>,
    ) -> Self {
        Self {
            config,
            dummy_pipeline,
            metrics_pipeline,
            logs_pipeline,
            traces_pipeline,
            profiles_pipeline,
            metrics: None,
            block_max_age: None,
            tail: SubscriptionRegistry::new(),
            live_ring: None,
        }
    }

    /// Attach a retained recent-window [`LiveRing`] as the live source for
    /// the merged history+live query (D-054). Builder-style so `new` keeps
    /// its signature; `None`/unset keeps the logs path byte-identical to
    /// pre-D-054. The same `Arc` is served by the `LiveQuery` endpoint.
    pub fn with_live_ring(mut self, ring: Option<Arc<LiveRing>>) -> Self {
        self.live_ring = ring;
        self
    }

    /// The process-local live-tail subscription registry. Exposed so a
    /// caller can observe `dropped_total()` / `subscriber_count()` for
    /// operator stats; the server wires it into every connection handler
    /// automatically.
    pub fn tail_registry(&self) -> Arc<SubscriptionRegistry> {
        self.tail.clone()
    }

    /// Attach process-global stats. Builder-style so `new` keeps its
    /// signature; pass the same `Arc<ServerMetrics>` that's handed to
    /// `serve_stats` so the endpoint and the ingest path share state.
    pub fn with_metrics(mut self, metrics: Arc<ServerMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Seal any open block older than `max_age`, regardless of size, via
    /// a per-signal background ticker. Builder-style; `None`/unset keeps
    /// the size-only behaviour. This is what makes low-volume and idle
    /// signals queryable in a timely way (and bounds WAL growth /
    /// restart-replay cost). See [`Pipeline::flush_if_aged`].
    pub fn with_block_max_age(mut self, max_age: Option<Duration>) -> Self {
        self.block_max_age = max_age;
        self
    }

    /// Bind the listener, accept connections until `shutdown`
    /// completes, then flush the pipeline (if any) and return. The
    /// shutdown future is typically `tokio::signal::ctrl_c()`, but
    /// any `Future<Output = ()>` works — e.g. pass an `oneshot::Receiver`
    /// from a supervisor instead of the process-wide ctrl-c.
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

        // Per-signal time-based flush tickers. Each seals its signal's
        // open block once it's older than `block_max_age`, so a trickle
        // of data still lands in the bucket promptly instead of waiting
        // for the size threshold. Aborted on shutdown; the final
        // `flush_all_shards` below drains whatever remains.
        let mut flushers: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        if let Some(max_age) = self.block_max_age {
            if let Some(p) = self.dummy_pipeline.as_ref() {
                flushers.push(spawn_age_flusher(p, max_age, "dummy"));
            }
            if let Some(p) = self.metrics_pipeline.as_ref() {
                flushers.push(spawn_age_flusher(p, max_age, "metrics"));
            }
            if let Some(p) = self.logs_pipeline.as_ref() {
                flushers.push(spawn_age_flusher(p, max_age, "logs"));
            }
            if let Some(p) = self.traces_pipeline.as_ref() {
                flushers.push(spawn_age_flusher(p, max_age, "traces"));
            }
            if let Some(p) = self.profiles_pipeline.as_ref() {
                flushers.push(spawn_age_flusher(p, max_age, "profiles"));
            }
            info!(
                max_age_secs = max_age.as_secs(),
                "time-based block flush enabled"
            );
        }

        let next_session_id = Arc::new(AtomicU64::new(1));
        let config = Arc::new(self.config);

        let accept_loop = async {
            loop {
                let (sock, peer) = listener.accept().await?;
                let session_id = next_session_id.fetch_add(1, Ordering::Relaxed);
                let config = config.clone();
                let dummy_pipeline = self.dummy_pipeline.clone();
                let metrics_pipeline = self.metrics_pipeline.clone();
                let logs_pipeline = self.logs_pipeline.clone();
                let traces_pipeline = self.traces_pipeline.clone();
                let profiles_pipeline = self.profiles_pipeline.clone();
                let metrics = self.metrics.clone();
                let tail = self.tail.clone();
                let live_ring = self.live_ring.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle(
                        sock,
                        peer,
                        config,
                        session_id,
                        dummy_pipeline,
                        metrics_pipeline,
                        logs_pipeline,
                        traces_pipeline,
                        profiles_pipeline,
                        metrics,
                        tail,
                        live_ring,
                    )
                    .await
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
            _ = shutdown    => { info!("shutdown signalled; flushing"); }
        }

        // Stop the time-based flush tickers before the final drain so
        // they don't race the shutdown flush for the shard locks. Abort
        // is safe: a ticker only ever holds a shard lock briefly across
        // `flush_if_aged`, and dropping the task releases it; the WAL is
        // the source of truth for anything mid-flight.
        for h in &flushers {
            h.abort();
        }

        // Flush every active pipeline shard. Order doesn't matter; each
        // shard owns its own WAL + JoinSet of inflight uploads.
        if let Some(sharded) = self.dummy_pipeline.as_ref() {
            flush_all_shards(sharded, "dummy").await;
        }
        if let Some(sharded) = self.metrics_pipeline.as_ref() {
            flush_all_shards(sharded, "metrics").await;
        }
        if let Some(sharded) = self.logs_pipeline.as_ref() {
            flush_all_shards(sharded, "logs").await;
        }
        if let Some(sharded) = self.traces_pipeline.as_ref() {
            flush_all_shards(sharded, "traces").await;
        }
        if let Some(sharded) = self.profiles_pipeline.as_ref() {
            flush_all_shards(sharded, "profiles").await;
        }

        Ok(())
    }
}

/// Spawn a background ticker that seals each shard's open block once it
/// is older than `max_age` (see [`Pipeline::flush_if_aged`]). The check
/// runs every `max_age / 4` (clamped to 1..=30s), so a block is sealed
/// within roughly `max_age + max_age/4` of its first record. The
/// [`ShardedPipeline`] handle is a cheap `Arc` clone; the task loops
/// until aborted on shutdown.
fn spawn_age_flusher<B: BlockBuilder>(
    sharded: &ShardedPipeline<B>,
    max_age: Duration,
    signal: &'static str,
) -> tokio::task::JoinHandle<()> {
    let sharded = sharded.clone();
    let check = (max_age / 4).clamp(Duration::from_secs(1), Duration::from_secs(30));
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(check);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // The first tick fires immediately; skip it so we don't seal a
        // block that was just opened this instant.
        interval.tick().await;
        loop {
            interval.tick().await;
            for (k, shard) in sharded.shards().iter().enumerate() {
                let mut guard = shard.lock().await;
                match guard.flush_if_aged(max_age).await {
                    Ok(true) => info!(signal, shard = k, "time-based flush sealed block"),
                    Ok(false) => {}
                    Err(e) => warn!(signal, shard = k, error = %e, "time-based flush failed"),
                }
            }
        }
    })
}

/// Flush every shard of one signal's [`ShardedPipeline`] on shutdown,
/// logging per-shard outcomes. Each shard owns an independent WAL +
/// inflight-upload set, so they're flushed one after another.
async fn flush_all_shards<B: BlockBuilder>(sharded: &ShardedPipeline<B>, signal: &str) {
    for (k, shard) in sharded.shards().iter().enumerate() {
        let mut guard = shard.lock().await;
        match guard.flush().await {
            Ok(()) => info!(signal, shard = k, "final flush complete"),
            Err(e) => warn!(signal, shard = k, error = %e, "final flush failed"),
        }
    }
}

/// RAII guard that increments the live connection gauge on accept and
/// decrements it on drop — covering every early-return path through
/// `handle` (handshake rejections, EOF, errors) without scattering
/// `conn_close()` calls.
struct ConnGuard(Option<Arc<ServerMetrics>>);

impl ConnGuard {
    fn new(metrics: Option<Arc<ServerMetrics>>) -> Self {
        if let Some(m) = metrics.as_ref() {
            m.conn_open();
        }
        Self(metrics)
    }
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        if let Some(m) = self.0.as_ref() {
            m.conn_close();
        }
    }
}

#[derive(Default)]
struct Counters {
    batches: AtomicU64,
    metric_samples: AtomicU64,
    log_entries: AtomicU64,
    spans: AtomicU64,
    profile_blobs: AtomicU64,
    dummy_records: AtomicU64,
    payload_bytes_in: AtomicU64,  // compressed
    payload_bytes_out: AtomicU64, // decompressed
    rejected: AtomicU64,
}

#[allow(clippy::too_many_arguments)]
async fn handle(
    sock: TcpStream,
    peer: SocketAddr,
    config: Arc<ServerConfig>,
    session_id: u64,
    dummy_sharded: Option<ShardedPipeline<DummyBlockBuilder>>,
    metrics_sharded: Option<ShardedPipeline<MetricsBlockBuilder>>,
    logs_sharded: Option<ShardedPipeline<LogsBlockBuilder>>,
    traces_sharded: Option<ShardedPipeline<TracesBlockBuilder>>,
    profiles_sharded: Option<ShardedPipeline<ProfilesBlockBuilder>>,
    metrics: Option<Arc<ServerMetrics>>,
    tail: Arc<SubscriptionRegistry>,
    live_ring: Option<Arc<LiveRing>>,
) -> Result<()> {
    // Pin this connection to one shard per signal (by session id). All of
    // this connection's batches for a signal funnel through the same
    // shard's (WAL + builder + mutex) — stable WAL ordering for replay —
    // while different connections spread across shards, so the per-signal
    // ingest lock is N independent locks rather than one. The resolved
    // `Arc<Mutex<Pipeline>>` is what the rest of `handle` uses, exactly as
    // the unsharded pipeline did.
    let dummy_pipeline = dummy_sharded
        .as_ref()
        .map(|s| s.shard_for(session_id).clone());
    let metrics_pipeline = metrics_sharded
        .as_ref()
        .map(|s| s.shard_for(session_id).clone());
    let logs_pipeline = logs_sharded
        .as_ref()
        .map(|s| s.shard_for(session_id).clone());
    let traces_pipeline = traces_sharded
        .as_ref()
        .map(|s| s.shard_for(session_id).clone());
    let profiles_pipeline = profiles_sharded
        .as_ref()
        .map(|s| s.shard_for(session_id).clone());
    sock.set_nodelay(true)?;
    // Track this connection in the live gauge for as long as `handle`
    // runs, regardless of which path returns.
    let _conn_guard = ConnGuard::new(metrics.clone());
    let (rd, wr) = sock.into_split();
    let mut rd = BufReader::new(rd);
    let mut wr = BufWriter::new(wr);

    info!(%peer, session_id, writer_uuid = %config.writer_uuid, "accept");

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

    // Per-connection private scratch builders for the decode-out-of-lock
    // ingest path. Lazily initialised on the first batch of each signal:
    // we lock the pipeline once to grab its decode fn pointer + a fresh
    // scratch builder, then decode every subsequent batch into the
    // scratch with no lock held and merge it under the lock via
    // `ingest_decoded`. A connection can carry more than one signal, so
    // there's one slot per signal.
    let mut dummy_scratch: Option<(DecodeFn<DummyBlockBuilder>, DummyBlockBuilder)> = None;
    let mut metrics_scratch: Option<(DecodeFn<MetricsBlockBuilder>, MetricsBlockBuilder)> = None;
    let mut logs_scratch: Option<(DecodeFn<LogsBlockBuilder>, LogsBlockBuilder)> = None;
    let mut traces_scratch: Option<(DecodeFn<TracesBlockBuilder>, TracesBlockBuilder)> = None;
    let mut profiles_scratch: Option<(DecodeFn<ProfilesBlockBuilder>, ProfilesBlockBuilder)> = None;

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
                // Dummy is v0.1-only and has no Hello.signals bit; we
                // accept it unconditionally as long as it decodes.
                // Every real signal must be in the announce mask.
                let announced_ok = match sig {
                    Some(Signal::Dummy) => true,
                    Some(Signal::Metrics) => signals_announced & SIGNAL_BIT_METRICS != 0,
                    Some(Signal::Logs) => signals_announced & SIGNAL_BIT_LOGS != 0,
                    Some(Signal::Traces) => signals_announced & SIGNAL_BIT_TRACES != 0,
                    Some(Signal::Profiles) => signals_announced & SIGNAL_BIT_PROFILES != 0,
                    None => false,
                };
                if !announced_ok {
                    counters.rejected.fetch_add(1, Ordering::Relaxed);
                    if let Some(m) = metrics.as_ref() {
                        m.add_rejected();
                    }
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
                    if let Some(m) = metrics.as_ref() {
                        m.add_rejected();
                    }
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
                if let Some(m) = metrics.as_ref() {
                    m.add_batch(b.payload.len() as u64);
                }

                let decompressed = match b.compression {
                    COMPRESSION_NONE => b.payload.clone(),
                    COMPRESSION_ZSTD => match zstd::decode_all(b.payload.as_slice()) {
                        Ok(d) => d,
                        Err(e) => {
                            counters.rejected.fetch_add(1, Ordering::Relaxed);
                            if let Some(m) = metrics.as_ref() {
                                m.add_rejected();
                            }
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
                        if let Some(m) = metrics.as_ref() {
                            m.add_rejected();
                        }
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
                if let Some(m) = metrics.as_ref() {
                    m.add_bytes_out(decompressed.len() as u64);
                }

                let signal = sig.unwrap();

                // Each signal that has a pipeline configured gets the
                // WAL+block path; the rest fall back to streaming
                // validate-and-count. Wire decode never materialises a
                // typed `*Batch` / per-record allocation — see
                // `scry_proto::streaming` and CLAUDE.md § Performance.
                let decode_result: Result<u64> = match signal {
                    Signal::Dummy => {
                        if let Some(pipe) = dummy_pipeline.as_ref() {
                            if dummy_scratch.is_none() {
                                let g = pipe.lock().await;
                                dummy_scratch = Some((g.decode_fn(), g.new_scratch()));
                            }
                            let (decode_fn, scratch) = dummy_scratch.as_mut().unwrap();
                            // Phase 1: decode into private scratch, no lock held.
                            match decode_fn(&decompressed, scratch) {
                                Ok(n) => {
                                    // Phase 2: commit (WAL append + merge) under lock.
                                    let mut guard = pipe.lock().await;
                                    match guard.ingest_decoded(&decompressed, scratch).await {
                                        Ok(_seg) => Ok(n as u64),
                                        Err(e) => {
                                            scratch.reset();
                                            Err(e)
                                        }
                                    }
                                }
                                Err(e) => {
                                    scratch.reset();
                                    Err(e)
                                }
                            }
                        } else {
                            let mut counter = CountDummyAppender(0);
                            scry_proto::streaming::decode_dummy_batch_into(
                                &decompressed,
                                &mut counter,
                            )
                            .map(|_| counter.0)
                            .map_err(|e| anyhow::anyhow!("DummyBatch: {e}"))
                        }
                    }
                    Signal::Metrics => {
                        if let Some(pipe) = metrics_pipeline.as_ref() {
                            if metrics_scratch.is_none() {
                                let g = pipe.lock().await;
                                metrics_scratch = Some((g.decode_fn(), g.new_scratch()));
                            }
                            let (decode_fn, scratch) = metrics_scratch.as_mut().unwrap();
                            // Phase 1: decode into private scratch, no lock held.
                            match decode_fn(&decompressed, scratch) {
                                Ok(n) => {
                                    // Phase 2: commit (WAL append + merge) under lock.
                                    let mut guard = pipe.lock().await;
                                    match guard.ingest_decoded(&decompressed, scratch).await {
                                        Ok(_seg) => Ok(n as u64),
                                        Err(e) => {
                                            scratch.reset();
                                            Err(e)
                                        }
                                    }
                                }
                                Err(e) => {
                                    scratch.reset();
                                    Err(e)
                                }
                            }
                        } else {
                            // No metrics pipeline: validate + count
                            // samples (series are dictionary entries,
                            // not records — same accounting as the
                            // pipeline path).
                            let mut counter = CountMetricsAppender::default();
                            scry_proto::streaming::decode_metrics_batch_into(
                                &decompressed,
                                &mut counter,
                            )
                            .map(|(_series, samples)| samples as u64)
                            .map_err(|e| anyhow::anyhow!("MetricsBatch: {e}"))
                        }
                    }
                    Signal::Logs => {
                        if let Some(pipe) = logs_pipeline.as_ref() {
                            if logs_scratch.is_none() {
                                let g = pipe.lock().await;
                                logs_scratch = Some((g.decode_fn(), g.new_scratch()));
                            }
                            let (decode_fn, scratch) = logs_scratch.as_mut().unwrap();
                            // Phase 1: decode into private scratch, no lock held.
                            // Two optional decorators can wrap the scratch:
                            //   - the live ring (D-054): a `RetainingLogsAppender`
                            //     collects this batch's entries so they can be
                            //     pushed (tagged with their WAL shard+segment) in
                            //     phase 2 — the live source for the merged query;
                            //   - the tail tap (D-050): forwards matching entries
                            //     to live-tail subscribers, nested *outside* the
                            //     retainer when both are active.
                            // Storage semantics are identical in every case — both
                            // decorators only observe entries flowing into the
                            // scratch builder. `into_records()` releases the
                            // scratch borrow before phase 2 re-uses it.
                            let tap_on = tail.subscriber_count() > 0;
                            let (decoded, live_records): (Result<usize>, Vec<LiveLogRecord>) =
                                if live_ring.is_some() {
                                    let mut retaining = RetainingLogsAppender::new(scratch);
                                    let d = if tap_on {
                                        let mut tap = TappingLogsAppender::new(
                                            &mut retaining,
                                            tail.as_ref(),
                                            Signal::Logs as u8,
                                        )
                                        .await;
                                        scry_proto::streaming::decode_logs_batch_into(
                                            &decompressed,
                                            &mut tap,
                                        )
                                        .map_err(|e| anyhow::anyhow!("LogsBatch: {e}"))
                                    } else {
                                        scry_proto::streaming::decode_logs_batch_into(
                                            &decompressed,
                                            &mut retaining,
                                        )
                                        .map_err(|e| anyhow::anyhow!("LogsBatch: {e}"))
                                    };
                                    let recs = retaining.into_records();
                                    (d, recs)
                                } else if tap_on {
                                    let mut tap = TappingLogsAppender::new(
                                        scratch,
                                        tail.as_ref(),
                                        Signal::Logs as u8,
                                    )
                                    .await;
                                    let d = scry_proto::streaming::decode_logs_batch_into(
                                        &decompressed,
                                        &mut tap,
                                    )
                                    .map_err(|e| anyhow::anyhow!("LogsBatch: {e}"));
                                    (d, Vec::new())
                                } else {
                                    (decode_fn(&decompressed, scratch), Vec::new())
                                };
                            match decoded {
                                Ok(n) => {
                                    // Phase 2: commit (WAL append + merge) under lock.
                                    let mut guard = pipe.lock().await;
                                    match guard.ingest_decoded(&decompressed, scratch).await {
                                        Ok(seg) => {
                                            // Feed the live ring: stamp the batch's
                                            // WAL shard+segment onto the collected
                                            // records and push. Nothing collected
                                            // when the ring is disabled.
                                            if let Some(ring) = live_ring.as_ref() {
                                                ring.push_stamped(
                                                    live_records,
                                                    guard.shard_index(),
                                                    seg.0,
                                                );
                                            }
                                            Ok(n as u64)
                                        }
                                        Err(e) => {
                                            scratch.reset();
                                            Err(e)
                                        }
                                    }
                                }
                                Err(e) => {
                                    scratch.reset();
                                    Err(e)
                                }
                            }
                        } else {
                            // No logs pipeline: validate + count
                            // entries (streams are dictionary entries,
                            // not records — same accounting as the
                            // pipeline path). Live tail still works here —
                            // a storage-less ingester can serve `scry tail`.
                            let mut counter = CountLogsAppender::default();
                            let decoded = if tail.subscriber_count() > 0 {
                                let mut tap = TappingLogsAppender::new(
                                    &mut counter,
                                    tail.as_ref(),
                                    Signal::Logs as u8,
                                )
                                .await;
                                scry_proto::streaming::decode_logs_batch_into(
                                    &decompressed,
                                    &mut tap,
                                )
                            } else {
                                scry_proto::streaming::decode_logs_batch_into(
                                    &decompressed,
                                    &mut counter,
                                )
                            };
                            decoded
                                .map(|entries| entries as u64)
                                .map_err(|e| anyhow::anyhow!("LogsBatch: {e}"))
                        }
                    }
                    Signal::Traces => {
                        if let Some(pipe) = traces_pipeline.as_ref() {
                            if traces_scratch.is_none() {
                                let g = pipe.lock().await;
                                traces_scratch = Some((g.decode_fn(), g.new_scratch()));
                            }
                            let (decode_fn, scratch) = traces_scratch.as_mut().unwrap();
                            // Phase 1: decode into private scratch, no lock held.
                            match decode_fn(&decompressed, scratch) {
                                Ok(n) => {
                                    // Phase 2: commit (WAL append + merge) under lock.
                                    let mut guard = pipe.lock().await;
                                    match guard.ingest_decoded(&decompressed, scratch).await {
                                        Ok(_seg) => Ok(n as u64),
                                        Err(e) => {
                                            scratch.reset();
                                            Err(e)
                                        }
                                    }
                                }
                                Err(e) => {
                                    scratch.reset();
                                    Err(e)
                                }
                            }
                        } else {
                            // No traces pipeline: validate + count spans
                            // (resources/scopes are dictionary entries,
                            // not records — same accounting as the
                            // pipeline path).
                            let mut counter = CountTracesAppender::default();
                            scry_proto::streaming::decode_traces_batch_into(
                                &decompressed,
                                &mut counter,
                            )
                            .map(|spans| spans as u64)
                            .map_err(|e| anyhow::anyhow!("TracesBatch: {e}"))
                        }
                    }
                    Signal::Profiles => {
                        if let Some(pipe) = profiles_pipeline.as_ref() {
                            if profiles_scratch.is_none() {
                                let g = pipe.lock().await;
                                profiles_scratch = Some((g.decode_fn(), g.new_scratch()));
                            }
                            let (decode_fn, scratch) = profiles_scratch.as_mut().unwrap();
                            // Phase 1: decode into private scratch, no lock held.
                            match decode_fn(&decompressed, scratch) {
                                Ok(n) => {
                                    // Phase 2: commit (WAL append + merge) under lock.
                                    let mut guard = pipe.lock().await;
                                    match guard.ingest_decoded(&decompressed, scratch).await {
                                        Ok(_seg) => Ok(n as u64),
                                        Err(e) => {
                                            scratch.reset();
                                            Err(e)
                                        }
                                    }
                                }
                                Err(e) => {
                                    scratch.reset();
                                    Err(e)
                                }
                            }
                        } else {
                            // No profiles pipeline: validate + count blobs.
                            let mut counter = CountProfilesAppender::default();
                            scry_proto::streaming::decode_profiles_batch_into(
                                &decompressed,
                                &mut counter,
                            )
                            .map(|blobs| blobs as u64)
                            .map_err(|e| anyhow::anyhow!("ProfilesBatch: {e}"))
                        }
                    }
                };

                match decode_result {
                    Ok(records) => {
                        match signal {
                            Signal::Metrics => counters
                                .metric_samples
                                .fetch_add(records, Ordering::Relaxed),
                            Signal::Logs => {
                                counters.log_entries.fetch_add(records, Ordering::Relaxed)
                            }
                            Signal::Traces => counters.spans.fetch_add(records, Ordering::Relaxed),
                            Signal::Profiles => {
                                counters.profile_blobs.fetch_add(records, Ordering::Relaxed)
                            }
                            Signal::Dummy => {
                                counters.dummy_records.fetch_add(records, Ordering::Relaxed)
                            }
                        };
                        if let Some(m) = metrics.as_ref() {
                            m.add_records(signal, records);
                        }
                        write_frame(
                            &mut wr,
                            &build::batch_ack(session_id, b.batch_id, ACK_ACCEPTED, 0, 0, ""),
                        )
                        .await?;
                    }
                    Err(e) => {
                        counters.rejected.fetch_add(1, Ordering::Relaxed);
                        if let Some(m) = metrics.as_ref() {
                            m.add_rejected();
                        }
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
                let _ = write_frame(
                    &mut wr,
                    &build::error(ERR_HELLO_REQUIRED, "duplicate Hello"),
                )
                .await;
                let _ = wr.flush().await;
                break;
            }

            FrameMsg::Error(e) => {
                warn!(%peer, code = e.code, msg = %e.message, "agent sent Error frame");
                break;
            }

            FrameMsg::Subscribe(sub) => {
                // A live-tail subscription turns this connection into a
                // one-way delivery stream: we spray matching `TailRecord`s
                // down `wr` until the client hangs up (EOF/Goodbye) or the
                // registry channel closes. Best-effort — see `tail` + D-050.
                let specs: Vec<String> = sub.matchers.into_iter().map(|m| m.spec).collect();
                let filter = match scry_match::LabelFilter::parse(&specs) {
                    Ok(f) => f,
                    Err(e) => {
                        warn!(%peer, error = %e, "bad Subscribe matcher");
                        let _ = write_frame(
                            &mut wr,
                            &build::error(ERR_BAD_MATCHER, &format!("bad matcher: {e}")),
                        )
                        .await;
                        let _ = wr.flush().await;
                        break;
                    }
                };
                info!(
                    %peer, session_id, signal = sub.signal,
                    matchers = filter.len(), "tail subscribe"
                );
                let (sub_id, mut rx) = tail.register(sub.signal, filter, TAIL_CHANNEL_CAP).await;

                loop {
                    tokio::select! {
                        item = rx.recv() => {
                            match item {
                                Some(it) => {
                                    let frame = build::tail_record(build::TailRecordArgs {
                                        signal: it.signal,
                                        ts_unix_nano: it.ts_unix_nano,
                                        severity: it.severity,
                                        labels: (*it.labels).clone(),
                                        body: it.body.clone(),
                                        attributes: it.attributes.clone(),
                                    });
                                    if write_frame(&mut wr, &frame).await.is_err()
                                        || wr.flush().await.is_err()
                                    {
                                        break;
                                    }
                                }
                                // Registry dropped the sender (shutdown). Done.
                                None => break,
                            }
                        }
                        // Watch the read half so a client hangup / Goodbye
                        // deregisters promptly instead of leaking a sub.
                        r = read_frame::<scry_proto::Frame, _>(&mut rd) => {
                            match r {
                                Ok(f) => {
                                    if matches!(f.msg, FrameMsg::Goodbye(_)) {
                                        break;
                                    }
                                    // Ignore any other frame on a tail conn.
                                }
                                Err(_) => break, // EOF or framing error → hangup
                            }
                        }
                    }
                }
                tail.deregister(sub_id).await;
                info!(%peer, session_id, "tail unsubscribe");
                break;
            }

            FrameMsg::LiveQuery(lq) => {
                // Merged-query live snapshot (D-054): reply with the retained
                // recent records matching the predicate, each tagged with its
                // WAL (shard, seg) so the query daemon can dedup against the
                // catalog high-water. Exactly one `LiveBatch`, then close.
                //
                // Logs only in v1. An ingester with no live ring (live disabled
                // or a non-logs query) still answers — with an empty batch —
                // so the daemon's fan-in never blocks on a silent peer.
                let is_logs = lq.signal == Signal::Logs as u8;
                let specs: Vec<String> = lq.matchers.into_iter().map(|m| m.spec).collect();
                let filter = match scry_match::LabelFilter::parse(&specs) {
                    Ok(f) => f,
                    Err(e) => {
                        warn!(%peer, error = %e, "bad LiveQuery matcher");
                        let _ = write_frame(
                            &mut wr,
                            &build::error(ERR_BAD_MATCHER, &format!("bad matcher: {e}")),
                        )
                        .await;
                        let _ = wr.flush().await;
                        break;
                    }
                };
                let ts_min = lq.ts_min_unix_nano;
                let ts_max = lq.ts_max_unix_nano;
                let needle = lq.body_contains;

                let records: Vec<LiveRecord> = match (is_logs, live_ring.as_ref()) {
                    (true, Some(ring)) => {
                        let snapshot = ring.collect(|r| {
                            if ts_min != 0 && r.ts_unix_nano < ts_min {
                                return false;
                            }
                            if ts_max != 0 && r.ts_unix_nano > ts_max {
                                return false;
                            }
                            if !needle.is_empty() && !r.body.contains(&needle) {
                                return false;
                            }
                            filter.keeps(&r.labels)
                        });
                        snapshot
                            .into_iter()
                            .map(|r| LiveRecord {
                                wal_shard: r.wal_shard,
                                wal_seg: r.wal_seg,
                                ts_unix_nano: r.ts_unix_nano,
                                severity: r.severity,
                                labels: (*r.labels).clone(),
                                body: r.body,
                                attributes: r.attributes,
                            })
                            .collect()
                    }
                    _ => Vec::new(),
                };
                info!(
                    %peer, session_id, signal = lq.signal,
                    records = records.len(), "live query"
                );
                let frame = build::live_batch(config.writer_uuid.into_bytes(), records);
                let _ = write_frame(&mut wr, &frame).await;
                let _ = wr.flush().await;
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

/// Trivial [`scry_proto::streaming::DummyAppender`] that just counts
/// records and discards the bytes. Used by the no-storage path so we
/// still validate the wire format on every Dummy batch.
struct CountDummyAppender(u64);

impl scry_proto::streaming::DummyAppender for CountDummyAppender {
    #[inline]
    fn append_raw(&mut self, _ts: u64, _key: &[u8], _value: &[u8]) {
        self.0 += 1;
    }
}

/// Counter-only `MetricsAppender` for the no-metrics-pipeline case.
/// Series observations are dropped wholesale; only sample counts are
/// kept to match what the configured-pipeline path reports back
/// (`samples = records`).
#[derive(Default)]
struct CountMetricsAppender {
    samples: u64,
}

impl scry_proto::streaming::MetricsAppender for CountMetricsAppender {
    #[inline]
    fn observe_series(
        &mut self,
        _fingerprint: u64,
        _metric_type: u8,
        _labels: Vec<(Vec<u8>, Vec<u8>)>,
    ) {
    }
    #[inline]
    fn append_sample(&mut self, _fingerprint: u64, _ts_unix_nano: u64, _value: f64) {
        self.samples += 1;
    }
}

/// Counter-only `LogsAppender` for the no-logs-pipeline case.
/// Stream observations are dropped; only entry counts are kept to
/// match the configured-pipeline path's accounting
/// (`entries = records`).
#[derive(Default)]
struct CountLogsAppender {
    entries: u64,
}

impl scry_proto::streaming::LogsAppender for CountLogsAppender {
    #[inline]
    fn observe_stream(&mut self, _fingerprint: u64, _labels: Vec<(Vec<u8>, Vec<u8>)>) {}
    #[inline]
    fn append_entry(
        &mut self,
        _fingerprint: u64,
        _ts_unix_nano: u64,
        _severity: u8,
        _body: Vec<u8>,
        _attributes: Vec<(Vec<u8>, Vec<u8>)>,
    ) {
        self.entries += 1;
    }
}

/// Counter-only `TracesAppender` for the no-traces-pipeline case.
/// Spans are counted; resources/scopes are dictionary entries and the
/// per-span data is discarded — matching the configured-pipeline path's
/// accounting (`spans = records`).
#[derive(Default)]
struct CountTracesAppender {
    spans: u64,
}

impl scry_proto::streaming::TracesAppender for CountTracesAppender {
    #[inline]
    fn append_span(&mut self, _span: &scry_proto::streaming::DecodedSpan<'_>) {
        self.spans += 1;
    }
}

/// Counter-only `ProfilesAppender` for the no-profiles-pipeline case.
#[derive(Default)]
struct CountProfilesAppender {
    blobs: u64,
}

impl scry_proto::streaming::ProfilesAppender for CountProfilesAppender {
    #[inline]
    fn append_blob(
        &mut self,
        _ts_unix_nano: u64,
        _duration_nano: u64,
        _labels: Vec<(Vec<u8>, Vec<u8>)>,
        _format: u8,
        _data: Vec<u8>,
    ) {
        self.blobs += 1;
    }
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
