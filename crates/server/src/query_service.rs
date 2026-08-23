//! Query daemon for the v0.3 query path.
//!
//! Long-running TCP service that exposes the [`scry_query`] machinery
//! — `MetricsQuery` preselect via postings sidecars → DataFusion
//! `TableProvider` → Parquet scan — over scry's own binschema wire
//! protocol (`proto/query.schema.json`). Same shape as the ingest
//! server: TCP listener, length-prefixed binschema frames, per-
//! connection task.
//!
//! Pre-step-5 this lived in `flight.rs` and rode on Arrow Flight (gRPC
//! over HTTP/2). The switch to binschema gives us a single wire
//! vocabulary across ingest + query and drops the `arrow-flight` +
//! `tonic` dependencies; the Arrow IPC payload is unchanged (we keep
//! zero-copy decode + per-batch streaming) — binschema is purely the
//! envelope. See `docs/ARCHITECTURE.md` for the reversal of D-024.
//!
//! Wire shape (per connection):
//!
//! - Client sends one [`QueryFrameMsg::QueryRequest`].
//! - Server replies with exactly one [`QueryFrameMsg::SchemaMsg`]
//!   (the Arrow IPC schema), then zero or more
//!   [`QueryFrameMsg::BatchMsg`] (one per IPC record-batch or
//!   dictionary message), then exactly one terminator:
//!   - [`QueryFrameMsg::EndOfStream`] on success.
//!   - [`QueryFrameMsg::StreamError`] on any DataFusion / catalog /
//!     resource failure.
//! - The server closes the socket after the terminator.
//!
//! Step 4's shared [`GreedyMemoryPool`] on a process-wide [`RuntimeEnv`]
//! still applies: every per-request `SessionContext` reuses the same
//! pool, so the budget is enforced across concurrent queries and a
//! pathological one returns [`QUERY_ERR_RESOURCES`](scry_proto::constants::QUERY_ERR_RESOURCES)
//! rather than OOM-ing the daemon.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result};
use arrow_ipc::writer::{write_message, DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use datafusion::common::DataFusionError;
use datafusion::execution::context::SessionContext;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::{execute_stream, ExecutionPlan, SendableRecordBatchStream};
use datafusion::prelude::SessionConfig;
use futures::StreamExt;
use object_store::{path::Path as ObjPath, ObjectMeta, ObjectStore, ObjectStoreExt};
use scry_block::BlockMeta;
use scry_catalog::{Catalog, CatalogEntry, TerminalResolution};
use scry_objstore::{BufPool, PoolStats};
use scry_proto::{
    constants::{
        Signal, QUERY_CAP_ATTEMPT_SUPERSESSION, QUERY_ERR_BAD_REQUEST, QUERY_ERR_FLEET_UNAVAILABLE,
        QUERY_ERR_INTERNAL, QUERY_ERR_LIVE_UNAVAILABLE, QUERY_ERR_PLAN, QUERY_ERR_RESOURCES,
        QUERY_ERR_SQL_PARSE, QUERY_SUPERSEDED_REASON_RETIRED_BLOCK_DISAPPEARED,
        QUERY_SUPERSEDED_REASON_SUPERSEDED_BLOCK_DISAPPEARED,
    },
    framing::{read_frame, write_frame, Framed, MAX_FRAME_BYTES},
    BatchMsgInput, EndOfStreamInput, FleetStatusResponseInput, LabelNamesRequestOutput,
    LabelNamesResponseInput, LabelValuesRequestOutput, LabelValuesResponseInput, QueryFrame,
    QueryFrameMsg, ResponseSupersededInput, SchemaMsgInput, StreamErrorInput,
};
use scry_query::{
    collect_label_names, collect_label_values, hash128, list_metrics_candidates,
    logs::{
        build_logs_table_from_candidates, list_logs_candidates, register_live_logs_table,
        register_logs_table_from_candidates, LiveLogRow, LOGS_LIVE_TABLE_NAME, LOGS_TABLE_NAME,
    },
    meta_query,
    profiles::{
        list_profiles_candidates, register_profiles_table_from_candidates, PROFILES_TABLE_NAME,
    },
    register_metrics_table_from_candidates,
    traces::{list_traces_candidates, register_traces_table_from_candidates, TRACES_TABLE_NAME},
    BloomCache, BloomCacheStats, EvictOnNotFound, PostingsCache, PostingsCacheStats, Query,
    QueryRequest, QueryResultCache, QueryResultCacheStats, METRICS_TABLE_NAME,
};

use crate::live_merge::{fetch_live_from_ingester, LiveDiscovery};
use crate::memory_guard::{QueryMemoryGuard, QUERY_TOO_LARGE_MESSAGE};
use crate::stats::QueryMetrics;
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OnceCell, Semaphore};
use tracing::{info, info_span, warn, Instrument, Span};
use uuid::Uuid;

/// Per-ingester deadline for the live fan-in (D-054). A slow/dead ingester
/// is skipped after this so one straggler can't stall the merged query; the
/// stored (block) half always returns regardless.
const LIVE_FETCH_DEADLINE: Duration = Duration::from_secs(2);
const QUERY_REQUEST_DEADLINE: Duration = Duration::from_secs(10);

/// Memory and concurrency limits for the live half of a merged logs query.
#[derive(Debug, Clone, Copy)]
pub struct LiveFetchLimits {
    pub concurrency: usize,
    pub max_peer_bytes: usize,
    pub max_total_bytes: usize,
    pub max_total_rows: usize,
}

impl Default for LiveFetchLimits {
    fn default() -> Self {
        Self {
            concurrency: 8,
            max_peer_bytes: 16 * 1024 * 1024,
            max_total_bytes: 128 * 1024 * 1024,
            max_total_rows: 1_000_000,
        }
    }
}

/// Process-wide bounds for authoritative partition repair after a block 404.
#[derive(Debug, Clone, Copy)]
pub struct TargetedRepairLimits {
    pub get_concurrency: usize,
    pub deadline: Duration,
    pub cache_ttl: Duration,
    pub stable_attempts: usize,
}

impl Default for TargetedRepairLimits {
    fn default() -> Self {
        Self {
            get_concurrency: 8,
            deadline: Duration::from_secs(10),
            cache_ttl: Duration::from_secs(2),
            stable_attempts: 3,
        }
    }
}

type RepairPartition = (String, String);

#[derive(Debug)]
struct PartitionRepair {
    loaded_at: Instant,
    metas: Arc<[BlockMeta]>,
}

#[derive(Debug)]
struct ReconciledRepair {
    metas: Vec<BlockMeta>,
    /// Missing UUIDs whose stable partition snapshot either omitted the old
    /// sidecar (retired) or contained a descendant that durably claims it
    /// (superseded while the old sidecar remains during grace).
    authoritatively_resolved: std::collections::HashSet<Uuid>,
    confirmed_absent: std::collections::HashSet<Uuid>,
}

#[derive(Debug, Default)]
struct RepairSlot {
    cell: OnceCell<Arc<PartitionRepair>>,
}

/// Per-query start snapshots of the two sidecar caches, bundled so the
/// many `emit_scan_complete` call sites can pass one `cache_start` value
/// by name. Both inner stats types are `Copy`.
#[derive(Debug, Clone, Copy)]
struct CacheStarts {
    postings: PostingsCacheStats,
    bloom: BloomCacheStats,
    result: QueryResultCacheStats,
}

/// Long-lived query service. One instance per daemon process. All
/// fields are `Arc`'d / `Clone` so per-connection captures are cheap.
pub struct QueryService {
    /// `rusqlite::Connection` (and therefore `scry_catalog::Catalog`)
    /// is `!Sync` because of its interior `RefCell`. Wrapping it in a
    /// `std::sync::Mutex` makes the whole service `Sync`. Lock
    /// contention is a non-issue: we only hold the guard for the brief
    /// synchronous `list_metrics_candidates` call (one SELECT against
    /// an indexed table), then drop it before any async work.
    catalog: Arc<Mutex<Catalog>>,
    store: Arc<dyn ObjectStore>,
    pool: BufPool,
    /// Per-block postings cache. Shared across all queries — the
    /// daemon's reason for existing is that blocks are immutable so
    /// caching their sidecars is a pure win after the first hit.
    /// Single-flight built in: concurrent misses on the same block
    /// only do one parquet fetch.
    postings_cache: Arc<PostingsCache>,
    /// Per-block body-bloom cache for the logs full-text path. Same
    /// immutable-block rationale as `postings_cache`, but a separate
    /// budget (blooms are ~2% of body size) so cheap blooms aren't
    /// evicted by larger postings. Only the logs signal consults it.
    bloom_cache: Arc<BloomCache>,
    /// Shared DataFusion runtime env. Every per-request
    /// `SessionContext` is constructed with `new_with_config_rt(...,
    /// runtime_env.clone())`, which is the only way DataFusion
    /// enforces the memory budget across queries (the runtime_env
    /// docs spell this out explicitly).
    runtime_env: Arc<RuntimeEnv>,
    /// Same pool that lives inside `runtime_env`, kept here as a
    /// concrete `Arc<GreedyMemoryPool>` so we can call `reserved()`
    /// at scan_complete time without downcasting from
    /// `Arc<dyn MemoryPool>`. Pure observability handle.
    memory_pool: Arc<GreedyMemoryPool>,
    /// Whole-response cache, keyed by the normalized request ⊕ the
    /// candidate block-UUID set (see [`data_query_cache_key`]). Turns a
    /// repeated data query — the shape a dashboard re-polling the same
    /// panel produces — into a single `write_all` of the cached frame
    /// bytes, with no DataFusion or object-store work. A budget of `0`
    /// disables it. Only the QueryRequest (data) path consults it;
    /// metadata answers are already served from the catalog label cache.
    result_cache: Arc<QueryResultCache>,
    /// Per-entry cap: a response is buffered for caching only while its
    /// framed bytes stay under this. Larger responses (big log dumps)
    /// stream normally but are never cached — keeps the cache to the
    /// small aggregation/metadata results dashboards re-poll.
    result_cache_entry_bytes: usize,
    /// Monotonic per-process counter, used only when the client
    /// didn't supply `request_id`. A `u64` is plenty — the daemon
    /// would have to serve 18 quintillion requests to wrap.
    next_request_id: AtomicU64,
    /// Discovery of live ingester endpoints for the merged history+live
    /// query (D-054). `None` when the daemon has no Valkey — a `live`
    /// logs query is then refused with `QUERY_ERR_LIVE_UNAVAILABLE`
    /// rather than silently degraded to blocks-only (decision 3).
    live_discovery: Option<Arc<dyn LiveDiscovery>>,
    /// Process-global query metrics for the status page (D-057) and the
    /// periodic activity log. Built unconditionally on the daemon (a handful of
    /// relaxed atomics) so `queries_in_flight` / `blocks_scanned` are always
    /// visible; the `--stats-listen` HTTP page just reads the same handle.
    metrics: Option<Arc<QueryMetrics>>,
    /// Valkey-backed fleet discovery for `FleetStatusRequest`. Kept behind the
    /// server-level trait so this crate has no direct Valkey dependency. `None`
    /// deliberately means "fleet unavailable", not a one-instance fallback.
    fleet: Option<Arc<dyn crate::stats::FleetSource>>,
    /// Default query look-back window in nanoseconds. When a request carries no
    /// time bounds at all, `ts_min` is clamped to `now - this` before candidate
    /// selection so a boundless query doesn't fan out over the whole catalog.
    /// `0` disables the default (unbounded queries scan everything). Set via
    /// [`QueryService::with_default_window`].
    default_query_window_nanos: u64,
    /// Process-level guard for memory DataFusion does not account for. When it
    /// reaches its cgroup safety threshold, new work is refused and running
    /// planning/streaming work is cancelled with QUERY_ERR_RESOURCES.
    memory_guard: Option<Arc<dyn QueryMemoryGuard>>,
    /// Bounded-parallel fan-in and retained-row limits for merged live logs.
    live_fetch_limits: LiveFetchLimits,
    /// Active query handlers. Connections may wait only while holding one of
    /// `query_wait_permits`; beyond that they receive an immediate overload.
    query_active_permits: Arc<Semaphore>,
    query_wait_permits: Arc<Semaphore>,
    query_queue_timeout: Duration,
    targeted_repair_limits: TargetedRepairLimits,
    targeted_repair_get_permits: Arc<Semaphore>,
    targeted_repair_slots: Arc<Mutex<HashMap<RepairPartition, Arc<RepairSlot>>>>,
}

impl QueryService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        catalog: Arc<Mutex<Catalog>>,
        store: Arc<dyn ObjectStore>,
        pool: BufPool,
        postings_cache: Arc<PostingsCache>,
        bloom_cache: Arc<BloomCache>,
        runtime_env: Arc<RuntimeEnv>,
        memory_pool: Arc<GreedyMemoryPool>,
        result_cache: Arc<QueryResultCache>,
        result_cache_entry_bytes: usize,
    ) -> Self {
        Self {
            catalog,
            store,
            pool,
            postings_cache,
            bloom_cache,
            runtime_env,
            memory_pool,
            result_cache,
            result_cache_entry_bytes,
            next_request_id: AtomicU64::new(0),
            live_discovery: None,
            metrics: None,
            fleet: None,
            default_query_window_nanos: scry_query::DEFAULT_QUERY_WINDOW_SECS * 1_000_000_000,
            memory_guard: None,
            live_fetch_limits: LiveFetchLimits::default(),
            query_active_permits: Arc::new(Semaphore::new(32)),
            query_wait_permits: Arc::new(Semaphore::new(64)),
            query_queue_timeout: Duration::from_secs(5),
            targeted_repair_limits: TargetedRepairLimits::default(),
            targeted_repair_get_permits: Arc::new(Semaphore::new(
                TargetedRepairLimits::default().get_concurrency,
            )),
            targeted_repair_slots: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Attach the status-page query metrics (D-057). Builder-style — call
    /// before wrapping the service in an `Arc`.
    pub fn with_metrics(mut self, metrics: Option<Arc<QueryMetrics>>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Attach the fleet registry exposed over the query wire. A daemon should
    /// only set this when it has Valkey; callers otherwise receive an explicit
    /// `QUERY_ERR_FLEET_UNAVAILABLE` rather than an incomplete local view.
    pub fn with_fleet_source(mut self, fleet: Option<Arc<dyn crate::stats::FleetSource>>) -> Self {
        self.fleet = fleet;
        self
    }

    /// Set the default look-back window (seconds) applied to a request that
    /// carries no time bounds. `0` disables the default. Builder-style — call
    /// before wrapping the service in an `Arc`.
    pub fn with_default_window(mut self, window_secs: u64) -> Self {
        self.default_query_window_nanos = window_secs.saturating_mul(1_000_000_000);
        self
    }

    pub fn with_memory_guard(mut self, guard: Option<Arc<dyn QueryMemoryGuard>>) -> Self {
        self.memory_guard = guard;
        self
    }

    pub fn with_live_fetch_limits(mut self, limits: LiveFetchLimits) -> Self {
        self.live_fetch_limits = LiveFetchLimits {
            concurrency: limits.concurrency.max(1),
            ..limits
        };
        self
    }

    pub fn with_targeted_repair_limits(mut self, limits: TargetedRepairLimits) -> Self {
        self.targeted_repair_limits = TargetedRepairLimits {
            get_concurrency: limits.get_concurrency.max(1),
            stable_attempts: limits.stable_attempts.max(1),
            ..limits
        };
        self.targeted_repair_get_permits =
            Arc::new(Semaphore::new(self.targeted_repair_limits.get_concurrency));
        self
    }

    pub fn with_query_admission(
        mut self,
        active: usize,
        waiting: usize,
        queue_timeout: Duration,
    ) -> Self {
        self.query_active_permits = Arc::new(Semaphore::new(active.max(1)));
        self.query_wait_permits = Arc::new(Semaphore::new(waiting));
        self.query_queue_timeout = queue_timeout;
        self
    }

    fn apply_default_window(&self, query: &mut Query) -> bool {
        let now_unix_nano = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        scry_query::apply_default_window(query, now_unix_nano, self.default_query_window_nanos)
    }

    /// Attach a live-ingester discovery source (D-054). Enables the merged
    /// history+live logs query; without it a `live` query is refused with
    /// `QUERY_ERR_LIVE_UNAVAILABLE`. Builder-style — call before wrapping the
    /// service in an `Arc`.
    pub fn with_live_discovery(mut self, discovery: Option<Arc<dyn LiveDiscovery>>) -> Self {
        self.live_discovery = discovery;
        self
    }

    /// Borrow the postings cache — exposed so the binary can log
    /// budget state at startup and so callers can inspect stats.
    pub fn postings_cache(&self) -> &Arc<PostingsCache> {
        &self.postings_cache
    }

    /// Borrow the bloom cache — exposed so the binary can log its budget
    /// at startup and so callers can inspect stats.
    pub fn bloom_cache(&self) -> &Arc<BloomCache> {
        &self.bloom_cache
    }

    /// Borrow the pool — exposed so the binary can log warmup state.
    pub fn pool(&self) -> &BufPool {
        &self.pool
    }

    /// Borrow the memory pool — exposed so the binary can log the
    /// configured budget at startup.
    pub fn memory_pool(&self) -> &Arc<GreedyMemoryPool> {
        &self.memory_pool
    }

    /// Borrow the result cache — exposed so the binary can log its
    /// budget at startup and so callers can inspect stats.
    pub fn result_cache(&self) -> &Arc<QueryResultCache> {
        &self.result_cache
    }

    /// Bind a TCP listener on `listen_addr`, accept connections, and
    /// drive each through `handle_connection` until `shutdown`
    /// resolves. Mirrors the shape of [`crate::Server::serve_with_shutdown`]
    /// so a future single binary can drive both ingest and query
    /// from one supervisor.
    pub async fn serve_with_shutdown<F>(
        self: Arc<Self>,
        listen_addr: SocketAddr,
        shutdown: F,
    ) -> Result<()>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let listener = TcpListener::bind(listen_addr)
            .await
            .with_context(|| format!("binding query listener on {listen_addr}"))?;
        let bound = listener.local_addr().ok();
        info!(
            listen = ?bound.unwrap_or(listen_addr),
            "query service listening"
        );

        tokio::pin!(shutdown);
        let accept_loop = async {
            loop {
                let (sock, peer) = listener.accept().await?;
                let svc = self.clone();
                let active = svc.query_active_permits.clone().try_acquire_owned();
                match active {
                    Ok(permit) => {
                        tokio::spawn(async move {
                            let _permit = permit;
                            if let Err(e) = svc.handle_connection(sock, peer).await {
                                warn!(%peer, error = %e, "connection ended with error");
                            }
                        });
                    }
                    Err(_) => {
                        let waiting = svc.query_wait_permits.clone().try_acquire_owned();
                        match waiting {
                            Ok(wait_permit) => tokio::spawn(async move {
                                let active = tokio::time::timeout(
                                    svc.query_queue_timeout,
                                    svc.query_active_permits.clone().acquire_owned(),
                                )
                                .await;
                                drop(wait_permit);
                                match active {
                                    Ok(Ok(permit)) => {
                                        let _permit = permit;
                                        if let Err(e) = svc.handle_connection(sock, peer).await {
                                            warn!(%peer, error = %e, "queued connection ended with error");
                                        }
                                    }
                                    _ => {
                                        send_query_overload(sock, "query queue wait timed out")
                                            .await;
                                    }
                                }
                            }),
                            Err(_) => tokio::spawn(async move {
                                send_query_overload(sock, "query service is saturated").await;
                            }),
                        };
                    }
                }
            }
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        };

        tokio::select! {
            r = accept_loop => { r?; }
            _ = &mut shutdown => { info!("shutdown signalled"); }
        }
        Ok(())
    }

    /// Per-connection handler. One request → one response stream →
    /// one terminator (EndOfStream or StreamError) → close. Tracing
    /// span carries `request_id` so every emitted event in the
    /// query life-cycle (`register_metrics_table_done`,
    /// `physical_plan_done`, `scan_complete`) gets the same
    /// correlation id.
    async fn handle_connection(self: Arc<Self>, sock: TcpStream, peer: SocketAddr) -> Result<()> {
        sock.set_nodelay(true)?;
        let (rd, wr) = sock.into_split();
        let mut rd = BufReader::new(rd);
        let mut wr = BufWriter::new(wr);

        // ── Read the request frame ───────────────────────────────────
        let req_frame: QueryFrame = match tokio::time::timeout(
            QUERY_REQUEST_DEADLINE,
            read_frame(&mut rd),
        )
        .await
        {
            Ok(Ok(f)) => f,
            Ok(Err(scry_proto::framing::FrameError::Io(e)))
                if e.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                // TCP readiness/liveness probes connect and close without
                // sending a frame. That proves the listener is accepting;
                // it is not an operator-actionable protocol failure.
                tracing::debug!(%peer, "connection closed before QueryRequest (likely TCP probe)");
                return Ok(());
            }
            Ok(Err(e)) => {
                warn!(%peer, error = %e, "no QueryRequest frame from client");
                return Ok(());
            }
            Err(_) => {
                warn!(%peer, "timed out waiting for QueryRequest frame");
                return Ok(());
            }
        };
        if let Some(guard) = &self.memory_guard {
            if let Err(e) = guard.check() {
                warn!(%peer, error = %e, "refusing query at process memory safety threshold");
                let _ =
                    emit_stream_error(&mut wr, QUERY_ERR_RESOURCES, QUERY_TOO_LARGE_MESSAGE).await;
                let _ = wr.flush().await;
                return Ok(());
            }
        }
        let wire_req = match req_frame.msg {
            QueryFrameMsg::QueryRequest(q) => q,
            // Metadata (discoverability) requests are self-contained: one
            // request → one response frame → close. They share the connection
            // handshake but not the query pipeline (no Arrow stream). See D-050.
            QueryFrameMsg::LabelNamesRequest(m) => {
                return self.handle_label_names(m, &mut wr, peer).await;
            }
            QueryFrameMsg::LabelValuesRequest(m) => {
                return self.handle_label_values(m, &mut wr, peer).await;
            }
            QueryFrameMsg::FleetStatusRequest(_) => {
                return self.handle_fleet_status(&mut wr, peer).await;
            }
            other => {
                let name = match other {
                    QueryFrameMsg::SchemaMsg(_) => "SchemaMsg",
                    QueryFrameMsg::BatchMsg(_) => "BatchMsg",
                    QueryFrameMsg::ResponseSuperseded(_) => "ResponseSuperseded",
                    QueryFrameMsg::EndOfStream(_) => "EndOfStream",
                    QueryFrameMsg::LabelNamesResponse(_) => "LabelNamesResponse",
                    QueryFrameMsg::LabelValuesResponse(_) => "LabelValuesResponse",
                    QueryFrameMsg::FleetStatusResponse(_) => "FleetStatusResponse",
                    QueryFrameMsg::StreamError(_) => "StreamError",
                    QueryFrameMsg::QueryRequest(_)
                    | QueryFrameMsg::LabelNamesRequest(_)
                    | QueryFrameMsg::LabelValuesRequest(_)
                    | QueryFrameMsg::FleetStatusRequest(_) => unreachable!(),
                };
                let _ = emit_stream_error(
                    &mut wr,
                    QUERY_ERR_BAD_REQUEST,
                    format!("expected QueryRequest as first frame, got {name}"),
                )
                .await;
                let _ = wr.flush().await;
                warn!(%peer, "client did not send QueryRequest first ({name})");
                return Ok(());
            }
        };
        // Attempt resets are required for correctness once compaction can reap
        // an input immediately. Reject legacy clients before any response bytes
        // (and, in particular, before an Arrow schema makes an attempt visible).
        if wire_req.capabilities & QUERY_CAP_ATTEMPT_SUPERSESSION == 0 {
            let _ = emit_stream_error(
                &mut wr,
                QUERY_ERR_BAD_REQUEST,
                "QueryRequest lacks required attempt-supersession capability",
            )
            .await;
            let _ = wr.flush().await;
            return Ok(());
        }
        let mut req = QueryRequest::from_wire(wire_req);

        let request_id = req.request_id.clone().unwrap_or_else(|| {
            format!("q-{}", self.next_request_id.fetch_add(1, Ordering::Relaxed))
        });

        // Guard-rail: a query with neither time bound would otherwise select
        // every block in the bucket as a scan candidate (`time_overlaps` lets
        // all blocks through when both bounds are `None`), doing O(catalog)
        // work. Clamp the look-back to the last `default_query_window_nanos`
        // unless the caller was explicit. Applied *before* the span so it
        // logs the effective bounds. See D-059.
        if self.apply_default_window(&mut req.query) {
            info!(
                request_id = %request_id,
                ts_min = req.query.ts_min,
                window_secs = self.default_query_window_nanos / 1_000_000_000,
                "query has no time bounds; defaulting look-back to last window"
            );
        }

        // Resolve the signal byte up-front. An unknown / zero byte
        // is a client bug and we'd rather surface a clean
        // QUERY_ERR_BAD_REQUEST than rope it through the rest of
        // the pipeline.
        let signal = match Signal::from_u8(req.signal) {
            Some(s @ (Signal::Metrics | Signal::Logs | Signal::Traces | Signal::Profiles)) => s,
            Some(other) => {
                let _ = emit_stream_error(
                    &mut wr,
                    QUERY_ERR_BAD_REQUEST,
                    format!(
                        "signal {other:?} has no query table \
                         (expected metrics, logs, traces, or profiles)"
                    ),
                )
                .await;
                let _ = wr.flush().await;
                return Ok(());
            }
            None => {
                let _ = emit_stream_error(
                    &mut wr,
                    QUERY_ERR_BAD_REQUEST,
                    format!(
                        "unknown signal byte {} \
                         (expected 1=metrics, 2=logs, 3=traces, 4=profiles)",
                        req.signal
                    ),
                )
                .await;
                let _ = wr.flush().await;
                return Ok(());
            }
        };

        // Span is the parent for every event emitted while building
        // and executing the query. Field shapes match existing
        // codebase conventions (% for Display, ? for Debug).
        let span = info_span!(
            "query",
            request_id = %request_id,
            signal = signal_name(signal),
            matcher_count = req.query.matchers.len(),
            ts_min = req.query.ts_min,
            ts_max = req.query.ts_max,
            sql = req.sql.as_deref().unwrap_or(""),
            limit = req.limit,
        );

        // All the actual query work runs under the span so the events
        // it emits (`register_metrics_table_done` etc.) land in the
        // right trace.
        let svc = self.clone();
        async move { svc.run_query(signal, req, wr).await }
            .instrument(span)
            .await
    }

    /// Plan and begin executing one query against `store`: list candidate
    /// blocks from the catalog, register the signal's table (resolving
    /// postings/bloom sidecars for metrics/logs), build the `DataFrame`,
    /// create the physical plan, and kick off the execution stream.
    ///
    /// Returns the live stream + the physical plan (for `scan_complete`
    /// metrics), or a `(wire error code, message)` on any failure. No bytes
    /// are written to the client here, so the caller can retry this whole
    /// method transparently — which it does once if a `NotFound` was recorded
    /// against `store` (a peer deleted a block we still list). For metrics/logs
    /// the 404 surfaces here (postings GET); traces/profiles resolve no sidecar
    /// at plan time so theirs only appears mid-scan (handled in `run_query`).
    /// List the candidate blocks for a query from the catalog — one
    /// indexed SELECT under the mutex, dropped before any async work.
    /// Hoisted out of [`plan_and_execute`] so [`run_query`] can list
    /// candidates once, fold them into the result-cache key, and reuse
    /// the same `Vec` for planning on a miss.
    fn list_candidates(
        &self,
        signal: Signal,
        query: &Query,
    ) -> std::result::Result<Vec<CatalogEntry>, (u16, String)> {
        let guard = self
            .catalog
            .lock()
            .map_err(|e| (QUERY_ERR_INTERNAL, format!("catalog mutex poisoned: {e}")))?;
        match signal {
            Signal::Metrics => list_metrics_candidates(&guard, query).map_err(|e| {
                (
                    QUERY_ERR_INTERNAL,
                    format!("list_metrics_candidates: {e:#}"),
                )
            }),
            Signal::Logs => list_logs_candidates(&guard, query)
                .map_err(|e| (QUERY_ERR_INTERNAL, format!("list_logs_candidates: {e:#}"))),
            Signal::Traces => list_traces_candidates(&guard, query)
                .map_err(|e| (QUERY_ERR_INTERNAL, format!("list_traces_candidates: {e:#}"))),
            Signal::Profiles => list_profiles_candidates(&guard, query).map_err(|e| {
                (
                    QUERY_ERR_INTERNAL,
                    format!("list_profiles_candidates: {e:#}"),
                )
            }),
            other => Err((
                QUERY_ERR_INTERNAL,
                format!("BUG: unsupported signal {other:?} reached run_query"),
            )),
        }
    }

    /// Register the merged history+live logs surface (D-054) into `ctx`: the
    /// block-backed table under a private name, the fanned-in live rows as an
    /// in-memory table, and a `logs` view = their `UNION ALL`. After this the
    /// rest of [`plan_and_execute`] proceeds unchanged — `logs` resolves to the
    /// union for both the default `SELECT *` and any caller SQL.
    async fn register_logs_merged(
        &self,
        ctx: &SessionContext,
        candidates: Vec<CatalogEntry>,
        store: Arc<dyn ObjectStore>,
        live_rows: &[LiveLogRow],
        query: &Query,
    ) -> Result<()> {
        // Blocks under a private name so `logs` is free for the union view.
        let table = build_logs_table_from_candidates(
            candidates,
            store.clone(),
            Some(self.postings_cache.as_ref()),
            Some(self.bloom_cache.as_ref()),
            query,
        )
        .await?;
        // `object_store_url().as_ref()` resolves to `&url::Url` from
        // `register_object_store`'s signature — no need to name the `url`
        // crate (not a direct dep here).
        let store_url = table.object_store_url().clone();
        ctx.runtime_env()
            .register_object_store(store_url.as_ref(), store);
        ctx.register_table("__logs_blocks", Arc::new(table))
            .map_err(|e| anyhow::anyhow!("register __logs_blocks: {e}"))?;

        // Live (in-flight, already watermark-deduped) rows as `logs_live`.
        register_live_logs_table(ctx, live_rows)?;

        // Expose `logs` as the union of both. `logs_live` is schema-identical
        // to `__logs_blocks`, so UNION ALL is well-typed.
        ctx.sql(&format!(
            "CREATE VIEW {LOGS_TABLE_NAME} AS \
             SELECT * FROM __logs_blocks UNION ALL SELECT * FROM {LOGS_LIVE_TABLE_NAME}"
        ))
        .await
        .map_err(|e| anyhow::anyhow!("create logs union view: {e}"))?;
        Ok(())
    }

    /// Fan in the live half of a merged logs query (D-054): discover the
    /// ingesters, ask each for its retained recent records matching the
    /// predicate, and dedup them against the catalog's durable WAL high-water
    /// so a record already in a block is dropped. Returns the survivors as
    /// [`LiveLogRow`]s ready to register as `logs_live`.
    ///
    /// `discovery` is the caller-verified `Some` source. A dead/slow ingester
    /// is skipped (logged), not fatal; only a discovery failure (Valkey down)
    /// surfaces as `QUERY_ERR_LIVE_UNAVAILABLE`.
    async fn fetch_live_logs(
        &self,
        discovery: &Arc<dyn LiveDiscovery>,
        req: &QueryRequest,
        candidates: &[CatalogEntry],
    ) -> std::result::Result<Vec<LiveLogRow>, (u16, String)> {
        // Discovery failure = the live half can't be served → refuse.
        let endpoints = discovery.discover().await.map_err(|e| {
            (
                QUERY_ERR_LIVE_UNAVAILABLE,
                format!("live ingester discovery failed: {e:#}"),
            )
        })?;

        // Turn the request's equality matchers into scry-match spec strings
        // (`key="value"`), the same grammar the ingester's ring filter parses.
        let matchers: Vec<String> = req
            .query
            .matchers
            .iter()
            .map(|(k, v)| format!("{k}=\"{v}\""))
            .collect();
        let ts_min = req.query.ts_min.unwrap_or(0);
        let ts_max = req.query.ts_max.unwrap_or(0);
        let body_contains = req.query.body_contains.clone().unwrap_or_default();
        let signal = Signal::Logs as u8;

        // Fetch peers concurrently in a fixed-width window. `buffer_unordered`
        // holds at most N wire responses/futures, and each completed response is
        // converted immediately rather than accumulating a second fleet-sized
        // `Vec<LiveBatchOutput>`.
        let mut fetches = futures::stream::iter(endpoints.into_iter().map(|addr| {
            let matchers = matchers.clone();
            let body_contains = body_contains.clone();
            async move {
                let result = tokio::time::timeout(
                    LIVE_FETCH_DEADLINE,
                    fetch_live_from_ingester(
                        &addr,
                        signal,
                        &matchers,
                        ts_min,
                        ts_max,
                        &body_contains,
                    ),
                )
                .await;
                (addr, result)
            }
        }))
        .buffer_unordered(self.live_fetch_limits.concurrency);

        // Dedup each record against the durable WAL high-water for its
        // (writer, "logs", shard). Keep iff `wal_seg > H` (absent ⇒ 0). Cache
        // the watermark per (writer, shard) so we lock the catalog once per
        // distinct WAL instance, not once per record.
        // Watermark per WAL instance: `None` = no block yet for this
        // (writer, shard) ⇒ *nothing* is durable, so every live record is
        // kept (segment 0 included — WAL segments are 0-based, so an
        // `unwrap_or(0)` here would wrongly treat the first segment as
        // already-durable and drop it before the first flush). `Some(h)` =
        // segments `≤ h` are durable in a block.
        // Dedup against the exact block candidate snapshot for this attempt,
        // not a later catalog read. This prevents a concurrent block commit from
        // being included in the block half while its WAL rows were retained
        // against an older watermark.
        let mut wm_cache: std::collections::HashMap<(Uuid, u32), Option<u64>> =
            std::collections::HashMap::new();
        for entry in candidates {
            if let (Some(segment), Some(shard)) = (entry.meta.wal_seg_max, entry.meta.wal_shard) {
                wm_cache
                    .entry((entry.meta.writer_id, shard))
                    .and_modify(|current| {
                        *current = Some(current.unwrap_or(0).max(segment));
                    })
                    .or_insert(Some(segment));
            }
        }
        let mut rows: Vec<LiveLogRow> = Vec::new();
        let mut retained_bytes = 0usize;
        while let Some((addr, result)) = fetches.next().await {
            let batch = match result {
                Ok(Ok(batch)) => batch,
                Ok(Err(e)) => {
                    warn!(%addr, error = %format!("{e:#}"), "live fetch from ingester failed; skipping");
                    continue;
                }
                Err(_) => {
                    warn!(%addr, "live fetch from ingester timed out; skipping");
                    continue;
                }
            };
            let peer_bytes: usize = batch.records.iter().map(live_record_owned_bytes).sum();
            if peer_bytes > self.live_fetch_limits.max_peer_bytes {
                return Err((
                    QUERY_ERR_RESOURCES,
                    format!(
                        "live response from {addr} is {peer_bytes} bytes; per-peer limit is {}",
                        self.live_fetch_limits.max_peer_bytes
                    ),
                ));
            }
            let writer = match Uuid::from_slice(&batch.writer_uuid) {
                Ok(u) => u,
                Err(e) => {
                    warn!(error = %e, "live batch had a malformed writer_uuid; skipping");
                    continue;
                }
            };
            for r in batch.records {
                let key = (writer, r.wal_shard);
                let hw = match wm_cache.get(&key) {
                    Some(&h) => h,
                    None => {
                        wm_cache.insert(key, None);
                        None
                    }
                };
                if live_record_is_durable(r.wal_seg, hw) {
                    continue;
                }
                let row_bytes = live_record_owned_bytes(&r);
                let next_bytes = retained_bytes.saturating_add(row_bytes);
                if rows.len() >= self.live_fetch_limits.max_total_rows
                    || next_bytes > self.live_fetch_limits.max_total_bytes
                {
                    return Err((
                        QUERY_ERR_RESOURCES,
                        format!(
                            "live query exceeds aggregate limit (rows max {}, bytes max {})",
                            self.live_fetch_limits.max_total_rows,
                            self.live_fetch_limits.max_total_bytes
                        ),
                    ));
                }
                retained_bytes = next_bytes;
                rows.push(LiveLogRow {
                    ts_unix_nano: r.ts_unix_nano,
                    severity: r.severity,
                    body: r.body,
                    labels: r.labels.into_iter().map(|p| (p.key, p.value)).collect(),
                    attributes: r.attributes.into_iter().map(|p| (p.key, p.value)).collect(),
                });
            }
            if let Some(guard) = self.memory_guard.as_ref() {
                guard.check().map_err(|e| {
                    (
                        QUERY_ERR_RESOURCES,
                        format!("{QUERY_TOO_LARGE_MESSAGE} {e:#}"),
                    )
                })?;
            }
        }
        Ok(rows)
    }

    async fn plan_and_execute(
        &self,
        signal: Signal,
        req: &QueryRequest,
        store: Arc<dyn ObjectStore>,
        candidates: Vec<CatalogEntry>,
        live_rows: Option<Vec<LiveLogRow>>,
    ) -> std::result::Result<(SendableRecordBatchStream, Arc<dyn ExecutionPlan>), (u16, String)>
    {
        let ctx =
            SessionContext::new_with_config_rt(SessionConfig::new(), self.runtime_env.clone());

        // ── Register the signal's table (sidecar GETs happen here) ────
        let register_result = match signal {
            Signal::Metrics => {
                register_metrics_table_from_candidates(
                    &ctx,
                    candidates,
                    store.clone(),
                    Some(self.postings_cache.as_ref()),
                    &req.query,
                )
                .await
            }
            Signal::Logs => match live_rows {
                // Merged history+live view (D-054): register the block-backed
                // logs table under a private name, the fanned-in live rows as
                // an in-memory table, and expose `logs` as their UNION ALL —
                // so both the default `SELECT * FROM logs` and any caller SQL
                // referencing `logs` transparently span the block-commit seam.
                Some(rows) => {
                    self.register_logs_merged(&ctx, candidates, store.clone(), &rows, &req.query)
                        .await
                }
                None => {
                    register_logs_table_from_candidates(
                        &ctx,
                        candidates,
                        store.clone(),
                        Some(self.postings_cache.as_ref()),
                        Some(self.bloom_cache.as_ref()),
                        &req.query,
                    )
                    .await
                }
            },
            Signal::Traces => {
                register_traces_table_from_candidates(
                    &ctx,
                    candidates,
                    store.clone(),
                    Some(self.postings_cache.as_ref()),
                    &req.query,
                )
                .await
            }
            Signal::Profiles => {
                register_profiles_table_from_candidates(
                    &ctx,
                    candidates,
                    store.clone(),
                    Some(self.postings_cache.as_ref()),
                    &req.query,
                )
                .await
            }
            other => Err(anyhow::anyhow!(
                "BUG: unsupported signal {other:?} reached run_query"
            )),
        };
        register_result.map_err(|e| {
            (
                QUERY_ERR_INTERNAL,
                format!("register_{}_table: {e:#}", signal_name(signal)),
            )
        })?;

        // ── Build the DataFrame (SQL or default SELECT *) ────────────
        let default_table = match signal {
            Signal::Metrics => METRICS_TABLE_NAME,
            Signal::Logs => LOGS_TABLE_NAME,
            Signal::Traces => TRACES_TABLE_NAME,
            Signal::Profiles => PROFILES_TABLE_NAME,
            _ => METRICS_TABLE_NAME,
        };
        let df = if let Some(sql) = req.sql.as_deref() {
            ctx.sql(sql)
                .await
                .map_err(|e| (QUERY_ERR_SQL_PARSE, format!("SQL parse: {e:#}")))?
        } else {
            let mut df = ctx
                .table(default_table)
                .await
                .map_err(|e| (QUERY_ERR_INTERNAL, format!("table lookup: {e:#}")))?;
            if let Some(limit) = req.limit {
                df = df
                    .limit(0, Some(limit))
                    .map_err(|e| (QUERY_ERR_PLAN, format!("applying limit: {e:#}")))?;
            }
            df
        };

        let physical = df
            .create_physical_plan()
            .await
            .map_err(|e| (QUERY_ERR_PLAN, format!("create_physical_plan: {e:#}")))?;
        let task_ctx = ctx.task_ctx();
        let stream = execute_stream(physical.clone(), task_ctx)
            .map_err(|e| (QUERY_ERR_INTERNAL, format!("execute_stream: {e:#}")))?;
        Ok((stream, physical))
    }

    async fn list_partition_meta_objects(
        &self,
        partition: &RepairPartition,
    ) -> std::result::Result<Vec<ObjectMeta>, String> {
        let (signal, date) = partition;
        let prefix = ObjPath::from(format!("{signal}/{}/", date.replace('-', "/")));
        let mut stream = self.store.list(Some(&prefix));
        let mut objects = Vec::new();
        while let Some(item) = stream.next().await {
            let object = item.map_err(|e| format!("targeted list {signal}/{date}: {e}"))?;
            if object.location.as_ref().ends_with(".meta.json") {
                objects.push(object);
            }
        }
        objects.sort_by(|a, b| a.location.cmp(&b.location));
        Ok(objects)
    }

    fn partition_token(
        objects: &[ObjectMeta],
    ) -> Vec<(String, u64, Option<String>, Option<String>)> {
        objects
            .iter()
            .map(|object| {
                (
                    object.location.to_string(),
                    object.size,
                    object.e_tag.clone(),
                    object.version.clone(),
                )
            })
            .collect()
    }

    async fn fetch_partition_metas(
        &self,
        objects: Vec<ObjectMeta>,
    ) -> std::result::Result<Vec<BlockMeta>, String> {
        let store = self.store.clone();
        let permits = self.targeted_repair_get_permits.clone();
        let concurrency = self.targeted_repair_limits.get_concurrency;
        let mut fetched = futures::stream::iter(objects.into_iter().map(move |object| {
            let store = store.clone();
            let permits = permits.clone();
            async move {
                let _permit = permits
                    .acquire_owned()
                    .await
                    .map_err(|_| "targeted repair GET semaphore closed".to_string())?;
                let result = store
                    .get(&object.location)
                    .await
                    .map_err(|e| format!("targeted fetch {}: {e}", object.location))?;
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|e| format!("targeted fetch {}: {e}", object.location))?;
                serde_json::from_slice::<BlockMeta>(&bytes)
                    .map_err(|e| format!("targeted parse {}: {e}", object.location))
            }
        }))
        .buffer_unordered(concurrency);

        let mut metas = Vec::new();
        while let Some(meta) = fetched.next().await {
            metas.push(meta?);
        }
        metas.sort_by_key(|meta| meta.uuid);
        Ok(metas)
    }

    async fn load_stable_partition(
        &self,
        partition: &RepairPartition,
    ) -> std::result::Result<Arc<PartitionRepair>, String> {
        for _ in 0..self.targeted_repair_limits.stable_attempts {
            let before = self.list_partition_meta_objects(partition).await?;
            let before_token = Self::partition_token(&before);
            let metas = self.fetch_partition_metas(before).await?;
            let after = self.list_partition_meta_objects(partition).await?;
            if before_token == Self::partition_token(&after) {
                return Ok(Arc::new(PartitionRepair {
                    loaded_at: Instant::now(),
                    metas: metas.into(),
                }));
            }
        }
        Err(format!(
            "targeted repair partition {}/{} remained unstable after {} attempts",
            partition.0, partition.1, self.targeted_repair_limits.stable_attempts
        ))
    }

    async fn get_or_load_partition(
        &self,
        partition: RepairPartition,
    ) -> std::result::Result<Arc<PartitionRepair>, String> {
        let slot = {
            let mut slots = self
                .targeted_repair_slots
                .lock()
                .map_err(|e| format!("targeted repair slot mutex poisoned: {e}"))?;
            if let Some(slot) = slots.get(&partition) {
                if slot.cell.get().is_none_or(|result| {
                    result.loaded_at.elapsed() <= self.targeted_repair_limits.cache_ttl
                }) {
                    slot.clone()
                } else {
                    let slot = Arc::new(RepairSlot::default());
                    slots.insert(partition.clone(), slot.clone());
                    slot
                }
            } else {
                let slot = Arc::new(RepairSlot::default());
                slots.insert(partition.clone(), slot.clone());
                slot
            }
        };

        let loaded = slot
            .cell
            .get_or_try_init(|| async {
                tokio::time::timeout(
                    self.targeted_repair_limits.deadline,
                    self.load_stable_partition(&partition),
                )
                .await
                .map_err(|_| {
                    format!(
                        "targeted repair timed out reconciling {}/{}",
                        partition.0, partition.1
                    )
                })?
            })
            .await
            .cloned();
        if loaded.is_err() {
            if let Ok(mut slots) = self.targeted_repair_slots.lock() {
                if slots
                    .get(&partition)
                    .is_some_and(|current| Arc::ptr_eq(current, &slot))
                {
                    slots.remove(&partition);
                }
            }
        }
        loaded
    }

    /// Load stable authoritative snapshots for every affected partition. The
    /// returned metadata is applied with stale-row resolution under one catalog
    /// lock by [`Self::apply_reconciled_repair`].
    async fn reconcile_missing_partitions(
        &self,
        missing: &[(Uuid, String, String)],
    ) -> std::result::Result<ReconciledRepair, String> {
        if missing.is_empty() {
            return Err("targeted repair has no authoritative partition context".into());
        }
        let mut partitions: Vec<RepairPartition> = missing
            .iter()
            .map(|(_, signal, date)| (signal.clone(), date.clone()))
            .collect();
        partitions.sort();
        partitions.dedup();
        let operation_deadline = tokio::time::Instant::now() + self.targeted_repair_limits.deadline;
        let mut metas = Vec::new();
        for partition in partitions {
            let loaded = tokio::time::timeout_at(
                operation_deadline,
                self.get_or_load_partition(partition.clone()),
            )
            .await
            .map_err(|_| {
                format!(
                    "targeted repair whole-operation deadline exceeded at {}/{}",
                    partition.0, partition.1
                )
            })??;
            metas.extend_from_slice(&loaded.metas);
        }
        metas.sort_by_key(|meta| meta.uuid);
        metas.dedup_by_key(|meta| meta.uuid);
        let present: std::collections::HashSet<_> = metas.iter().map(|meta| meta.uuid).collect();
        let represented: std::collections::HashSet<_> = metas
            .iter()
            .flat_map(|meta| meta.compacted_from.iter().copied())
            .collect();
        let confirmed_absent: std::collections::HashSet<_> = missing
            .iter()
            .filter_map(|(uuid, _, _)| (!present.contains(uuid)).then_some(*uuid))
            .collect();
        let authoritatively_resolved = missing
            .iter()
            .filter_map(|(uuid, _, _)| {
                (confirmed_absent.contains(uuid) || represented.contains(uuid)).then_some(*uuid)
            })
            .collect();
        Ok(ReconciledRepair {
            metas,
            authoritatively_resolved,
            confirmed_absent,
        })
    }

    fn apply_reconciled_repair(
        &self,
        uuids: &[Uuid],
        reconciled: &ReconciledRepair,
    ) -> std::result::Result<u8, String> {
        let guard = self
            .catalog
            .lock()
            .map_err(|e| format!("catalog mutex poisoned while repairing 404: {e}"))?;
        for meta in &reconciled.metas {
            guard
                .insert_block(meta)
                .map_err(|e| format!("targeted repair insert {}: {e:#}", meta.uuid))?;
        }
        if let Some(unproven) = uuids
            .iter()
            .find(|uuid| !reconciled.authoritatively_resolved.contains(uuid))
        {
            return Err(format!(
                "missing block {unproven} had no authoritative stable-partition absence proof"
            ));
        }
        // Lineage pruning is intentionally disabled during the initial rollout.
        // A stable pair of LIST results cannot exclude a commit immediately after
        // the second LIST; retaining this rebuildable index is safer than racing
        // a newly learned descendant claim.
        let mut superseded = false;
        for &uuid in uuids {
            match guard
                .resolve_terminal(uuid)
                .map_err(|e| format!("resolve_terminal({uuid}): {e:#}"))?
            {
                TerminalResolution::Unique(replacement) if replacement != uuid => {
                    superseded = true;
                }
                TerminalResolution::None => {}
                TerminalResolution::Unique(_) if reconciled.confirmed_absent.contains(&uuid) => {
                    // The stable authoritative partition snapshot omitted this
                    // UUID, so its stale local live row is a confirmed
                    // retention/deletion outcome rather than unexplained loss.
                }
                TerminalResolution::Unique(_) => {
                    return Err(format!(
                        "missing block {uuid} is still authoritative-live with no known replacement; refusing a lossy re-plan"
                    ));
                }
                TerminalResolution::Fork(terminals) => {
                    return Err(format!(
                        "lineage fork resolving missing block {uuid}: {terminals:?}"
                    ));
                }
            }
        }
        guard
            .delete_blocks(uuids)
            .map_err(|e| format!("delete repaired catalog rows: {e:#}"))?;
        Ok(if superseded {
            QUERY_SUPERSEDED_REASON_SUPERSEDED_BLOCK_DISAPPEARED
        } else {
            QUERY_SUPERSEDED_REASON_RETIRED_BLOCK_DISAPPEARED
        })
    }

    // ── Label metadata (discoverability) ─────────────────────────────────
    // A materialized view over the authoritative postings sidecars, cached in
    // the catalog and warmed lazily. Answers "what can I match on?" without a
    // data scan. See D-050 and `scry_catalog`'s `block_labels*` tables.

    /// `LabelNamesRequest` → one `LabelNamesResponse`.
    async fn handle_label_names<W>(
        &self,
        m: LabelNamesRequestOutput,
        wr: &mut BufWriter<W>,
        peer: SocketAddr,
    ) -> Result<()>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        let Some(signal) = self.resolve_meta_signal(m.signal, wr).await? else {
            return Ok(());
        };
        let mut q = meta_query(
            (m.ts_min_present != 0).then_some(m.ts_min),
            (m.ts_max_present != 0).then_some(m.ts_max),
        );
        self.apply_default_window(&mut q);
        let collected = if let Some(guard) = &self.memory_guard {
            tokio::select! {
                result = self.collect_label_names(signal, &q) => result,
                _ = guard.wait_until_exhausted() => {
                    Err((QUERY_ERR_RESOURCES, QUERY_TOO_LARGE_MESSAGE.to_string()))
                }
            }
        } else {
            self.collect_label_names(signal, &q).await
        };
        let names = match collected {
            Ok(n) => n,
            Err((code, msg)) => {
                warn!(%peer, signal = signal_name(signal), code, %msg, "label-names request failed");
                let _ = emit_stream_error(wr, code, msg).await;
                let _ = wr.flush().await;
                return Ok(());
            }
        };
        let frame = QueryFrame {
            msg: QueryFrameMsg::LabelNamesResponse(LabelNamesResponseInput { names }.into()),
        };
        if let Err(e) = write_frame(wr, &frame).await {
            warn!(%peer, error = %e, "writing LabelNamesResponse");
        }
        let _ = wr.flush().await;
        Ok(())
    }

    /// `LabelValuesRequest` → one `LabelValuesResponse`.
    async fn handle_label_values<W>(
        &self,
        m: LabelValuesRequestOutput,
        wr: &mut BufWriter<W>,
        peer: SocketAddr,
    ) -> Result<()>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        let Some(signal) = self.resolve_meta_signal(m.signal, wr).await? else {
            return Ok(());
        };
        let mut q = meta_query(
            (m.ts_min_present != 0).then_some(m.ts_min),
            (m.ts_max_present != 0).then_some(m.ts_max),
        );
        self.apply_default_window(&mut q);
        let collected = if let Some(guard) = &self.memory_guard {
            tokio::select! {
                result = self.collect_label_values(signal, &m.label_name, &q) => result,
                _ = guard.wait_until_exhausted() => {
                    Err((QUERY_ERR_RESOURCES, QUERY_TOO_LARGE_MESSAGE.to_string()))
                }
            }
        } else {
            self.collect_label_values(signal, &m.label_name, &q).await
        };
        let values = match collected {
            Ok(v) => v,
            Err((code, msg)) => {
                warn!(%peer, signal = signal_name(signal), code, %msg, "label-values request failed");
                let _ = emit_stream_error(wr, code, msg).await;
                let _ = wr.flush().await;
                return Ok(());
            }
        };
        let frame = QueryFrame {
            msg: QueryFrameMsg::LabelValuesResponse(LabelValuesResponseInput { values }.into()),
        };
        if let Err(e) = write_frame(wr, &frame).await {
            warn!(%peer, error = %e, "writing LabelValuesResponse");
        }
        let _ = wr.flush().await;
        Ok(())
    }

    /// `FleetStatusRequest` → one `FleetStatusResponse`. Invalid JSON blobs are
    /// dropped at this trust boundary so every document sent to clients is a
    /// valid, canonical [`StatusSnapshot`](crate::stats::StatusSnapshot).
    async fn handle_fleet_status<W>(&self, wr: &mut BufWriter<W>, peer: SocketAddr) -> Result<()>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        let Some(fleet) = &self.fleet else {
            let _ = emit_stream_error(
                wr,
                QUERY_ERR_FLEET_UNAVAILABLE,
                "fleet unavailable: queryd is not connected to Valkey",
            )
            .await;
            let _ = wr.flush().await;
            return Ok(());
        };

        let instances_json = canonical_fleet_json(fleet.blobs().await);
        let frame = QueryFrame {
            msg: QueryFrameMsg::FleetStatusResponse(
                FleetStatusResponseInput { instances_json }.into(),
            ),
        };
        if let Err(e) = write_frame(wr, &frame).await {
            warn!(%peer, error = %e, "writing FleetStatusResponse");
        }
        let _ = wr.flush().await;
        Ok(())
    }

    /// Resolve a metadata request's signal byte, emitting a `StreamError` +
    /// flushing on an invalid byte. `Ok(None)` means the error was already sent
    /// and the caller should return.
    async fn resolve_meta_signal<W>(
        &self,
        byte: u8,
        wr: &mut BufWriter<W>,
    ) -> Result<Option<Signal>>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        match Signal::from_u8(byte) {
            Some(s @ (Signal::Metrics | Signal::Logs | Signal::Traces | Signal::Profiles)) => {
                Ok(Some(s))
            }
            _ => {
                let _ = emit_stream_error(
                    wr,
                    QUERY_ERR_BAD_REQUEST,
                    format!(
                        "signal byte {byte} has no label metadata \
                         (expected 1=metrics, 2=logs, 3=traces, 4=profiles)"
                    ),
                )
                .await;
                let _ = wr.flush().await;
                Ok(None)
            }
        }
    }

    /// Distinct, sorted label names for a signal + time window. Thin wrapper
    /// over the shared [`scry_query::collect_label_names`] so the daemon and the
    /// `scry get` CLI answer identically.
    async fn collect_label_names(
        &self,
        signal: Signal,
        q: &Query,
    ) -> std::result::Result<Vec<String>, (u16, String)> {
        for _ in 0..=3 {
            let candidates = self.list_candidates(signal, q)?;
            let context: Vec<_> = candidates
                .iter()
                .map(|entry| {
                    (
                        entry.meta.uuid,
                        entry.meta.signal.clone(),
                        entry.date.clone(),
                    )
                })
                .collect();
            let evict = Arc::new(EvictOnNotFound::new(self.store.clone()));
            match collect_label_names(
                &self.catalog,
                evict.clone(),
                self.postings_cache.as_ref(),
                self.runtime_env.clone(),
                signal,
                q,
            )
            .await
            {
                Ok(names) => return Ok(names),
                Err(error) => {
                    let missing = evict.take_evicted();
                    if missing.is_empty() {
                        return Err(error);
                    }
                    let affected: Vec<_> = context
                        .iter()
                        .filter(|(uuid, _, _)| missing.contains(uuid))
                        .cloned()
                        .collect();
                    let metas = self
                        .reconcile_missing_partitions(&affected)
                        .await
                        .map_err(|message| (QUERY_ERR_INTERNAL, message))?;
                    self.apply_reconciled_repair(&missing, &metas)
                        .map_err(|message| (QUERY_ERR_INTERNAL, message))?;
                }
            }
        }
        Err((
            QUERY_ERR_INTERNAL,
            "label-names repair limit exhausted".to_string(),
        ))
    }

    /// Distinct, sorted values for one label name over a signal + time window.
    /// Thin wrapper over the shared [`scry_query::collect_label_values`].
    async fn collect_label_values(
        &self,
        signal: Signal,
        name: &str,
        q: &Query,
    ) -> std::result::Result<Vec<String>, (u16, String)> {
        for _ in 0..=3 {
            let candidates = self.list_candidates(signal, q)?;
            let context: Vec<_> = candidates
                .iter()
                .map(|entry| {
                    (
                        entry.meta.uuid,
                        entry.meta.signal.clone(),
                        entry.date.clone(),
                    )
                })
                .collect();
            let evict = Arc::new(EvictOnNotFound::new(self.store.clone()));
            match collect_label_values(
                &self.catalog,
                evict.clone(),
                self.postings_cache.as_ref(),
                self.runtime_env.clone(),
                signal,
                name,
                q,
            )
            .await
            {
                Ok(values) => return Ok(values),
                Err(error) => {
                    let missing = evict.take_evicted();
                    if missing.is_empty() {
                        return Err(error);
                    }
                    let affected: Vec<_> = context
                        .iter()
                        .filter(|(uuid, _, _)| missing.contains(uuid))
                        .cloned()
                        .collect();
                    let metas = self
                        .reconcile_missing_partitions(&affected)
                        .await
                        .map_err(|message| (QUERY_ERR_INTERNAL, message))?;
                    self.apply_reconciled_repair(&missing, &metas)
                        .map_err(|message| (QUERY_ERR_INTERNAL, message))?;
                }
            }
        }
        Err((
            QUERY_ERR_INTERNAL,
            "label-values repair limit exhausted".to_string(),
        ))
    }

    /// The actual query execution. Split out from `handle_connection`
    /// so the request decode + span setup is one orderly block, and
    /// this fn can return early via `?` on framing errors without
    /// losing the `scan_complete` emission.
    async fn run_query<W>(
        self: Arc<Self>,
        signal: Signal,
        req: QueryRequest,
        mut wr: BufWriter<W>,
    ) -> Result<()>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        let pool_start = self.pool.stats();
        // Bundle the three cache start-snapshots under one name so every
        // `emit_scan_complete` call site stays unchanged (they pass
        // `cache_start` by name); only the type and the reads here move.
        let cache_start = CacheStarts {
            postings: self.postings_cache.stats(),
            bloom: self.bloom_cache.stats(),
            result: self.result_cache.stats(),
        };
        let t0 = Instant::now();
        // Status-page accounting (D-057): count this query and track it
        // in-flight. The guard folds wall-time into the latency total on drop
        // and, unless `mark_ok` is called at a success terminator, counts an
        // error — so every early return below is accounted correctly.
        let mut inflight = self.metrics.as_ref().map(|m| m.begin());
        // Pre-allocate row counter so we can pass it to emit_scan_complete
        // on every exit path (success + each error path).
        let mut rows_total: u64 = 0;

        // A second admission check closes the gap between reading the request
        // and beginning expensive planning (metadata requests are checked at
        // connection admission and use their projected streaming path).
        if let Some(guard) = &self.memory_guard {
            if guard.check().is_err() {
                let _ =
                    emit_stream_error(&mut wr, QUERY_ERR_RESOURCES, QUERY_TOO_LARGE_MESSAGE).await;
                let _ = wr.flush().await;
                self.emit_scan_complete(
                    signal,
                    None,
                    rows_total,
                    pool_start,
                    cache_start,
                    "miss",
                    t0.elapsed(),
                );
                return Ok(());
            }
        }

        // ── Result-cache lookup + plan/execute, with one transparent
        //    re-plan on a peer's deletion ───────────────────────────────
        //
        // Each iteration: (1) list candidate blocks (one indexed catalog
        // SELECT), (2) build the result-cache key from the normalized request
        // ⊕ that candidate set and short-circuit on a hit — the 2 ms path,
        // no SessionContext / scan / object store, (3) on a miss plan+execute.
        //
        // The re-plan handles a peer (compaction reaping a superseded input,
        // retention reaping an expired block) hard-deleting a block this
        // instance still lists — convergence just hasn't caught up. We wrap
        // the store in `EvictOnNotFound`: a `NotFound` during the planning
        // reads records the dead block's UUID; we delete those stale rows and
        // loop **once** — re-listing candidates (a different set → a different
        // cache key) — fully transparent, the client never saw a byte.
        // (Traces/profiles resolve no sidecar at plan time, so their 404 only
        // surfaces mid-scan, below.)
        let mut active_attempt = 0u32;
        'attempt: loop {
            // Object-store misses, live rows, and Arrow dictionary state are
            // attempt-local: a reset must not inherit any provisional state.
            rows_total = 0;
            if req.live && signal == Signal::Logs && self.live_discovery.is_none() {
                let _ = emit_stream_error(
                    &mut wr,
                    QUERY_ERR_LIVE_UNAVAILABLE,
                    "live query requires Valkey ingester discovery; this server has none"
                        .to_string(),
                )
                .await;
                let _ = wr.flush().await;
                return Ok(());
            }
            let evict = Arc::new(EvictOnNotFound::new(self.store.clone()));
            let store: Arc<dyn ObjectStore> = evict.clone();
            let mut repair_rounds = 0u8;
            let (mut stream, physical, cache_key, candidate_context) = loop {
                // (1) Candidate blocks for this request.
                let candidates = match self.list_candidates(signal, &req.query) {
                    Ok(c) => c,
                    Err((code, msg)) => {
                        let _ = emit_stream_error(&mut wr, code, msg).await;
                        let _ = wr.flush().await;
                        self.emit_scan_complete(
                            signal,
                            None,
                            rows_total,
                            pool_start,
                            cache_start,
                            "miss",
                            t0.elapsed(),
                        );
                        return Ok(());
                    }
                };

                let candidate_context: Vec<(Uuid, String, String)> = candidates
                    .iter()
                    .map(|c| (c.meta.uuid, c.meta.signal.clone(), c.date.clone()))
                    .collect();
                let live_rows = if req.live && signal == Signal::Logs {
                    match self
                        .fetch_live_logs(
                            self.live_discovery.as_ref().expect("checked above"),
                            &req,
                            &candidates,
                        )
                        .await
                    {
                        Ok(rows) => Some(rows),
                        Err((code, msg)) => {
                            let _ = emit_stream_error(&mut wr, code, msg).await;
                            let _ = wr.flush().await;
                            return Ok(());
                        }
                    }
                } else {
                    None
                };

                // Record the candidate count (pre-pruning) so the periodic activity
                // log surfaces query fan-out — exactly what explodes on an unbounded
                // query. See D-059.
                if let Some(m) = self.metrics.as_ref() {
                    m.record_candidates(candidates.len() as u64);
                }

                // (2) Cache key + hit short-circuit. The cached value is the exact
                // concatenation of the SchemaMsg + BatchMsg… + EndOfStream frames,
                // so a hit is a single write_all. A `live` query is time-varying
                // (fresh in-flight records every instant), so it never consults or
                // populates the result cache.
                let key = data_query_cache_key(signal, &req, &candidates);
                if !req.live {
                    if let Some(bytes) = self.result_cache.get(key) {
                        if let Some(g) = inflight.as_mut() {
                            g.mark_ok();
                        }
                        if let Err(e) = wr.write_all(&bytes).await {
                            warn!(error = %e, "client disconnected while writing cached response");
                        }
                        let _ = wr.flush().await;
                        // total_rows is not recomputed on a hit (the count rides inside
                        // the cached EndOfStream frame the client parses); `cache=hit`
                        // marks the fast path in telemetry.
                        self.emit_scan_complete(
                            signal,
                            None,
                            rows_total,
                            pool_start,
                            cache_start,
                            "hit",
                            t0.elapsed(),
                        );
                        return Ok(());
                    }
                }

                // (3) Miss → plan + execute against the candidate set. Planning
                // resolves postings and may build DataFusion state, so race it
                // against the process-level guard as well as guarding the stream.
                let planned = if let Some(guard) = &self.memory_guard {
                    tokio::select! {
                        result = self.plan_and_execute(
                            signal,
                            &req,
                            store.clone(),
                            candidates,
                            live_rows.clone(),
                        ) => result,
                        _ = guard.wait_until_exhausted() => {
                            Err((QUERY_ERR_RESOURCES, QUERY_TOO_LARGE_MESSAGE.to_string()))
                        }
                    }
                } else {
                    self.plan_and_execute(
                        signal,
                        &req,
                        store.clone(),
                        candidates,
                        live_rows.clone(),
                    )
                    .await
                };
                match planned {
                    Ok((stream, physical)) => break (stream, physical, key, candidate_context),
                    Err((code, msg)) => {
                        let evicted = evict.take_evicted();
                        if repair_rounds < 3 && !evicted.is_empty() {
                            repair_rounds += 1;
                            let missing_context: Vec<_> = candidate_context
                                .iter()
                                .filter(|(uuid, _, _)| evicted.contains(uuid))
                                .cloned()
                                .collect();
                            let reconciled =
                                match self.reconcile_missing_partitions(&missing_context).await {
                                    Ok(metas) => metas,
                                    Err(repair) => {
                                        let _ =
                                            emit_stream_error(&mut wr, QUERY_ERR_INTERNAL, repair)
                                                .await;
                                        let _ = wr.flush().await;
                                        return Ok(());
                                    }
                                };
                            match self.apply_reconciled_repair(&evicted, &reconciled) {
                                Ok(_) => {
                                    info!(
                                    signal = signal_name(signal),
                                    evicted = evicted.len(),
                                    repair_rounds,
                                    "block(s) 404'd during planning; repaired catalog and re-planning transparently"
                                );
                                    continue;
                                }
                                Err(repair) => {
                                    let _ = emit_stream_error(&mut wr, QUERY_ERR_INTERNAL, repair)
                                        .await;
                                    let _ = wr.flush().await;
                                    return Ok(());
                                }
                            }
                        }
                        let _ = emit_stream_error(&mut wr, code, msg).await;
                        let _ = wr.flush().await;
                        self.emit_scan_complete(
                            signal,
                            None,
                            rows_total,
                            pool_start,
                            cache_start,
                            "miss",
                            t0.elapsed(),
                        );
                        return Ok(());
                    }
                }
            };
            // Planning-time observations belong only to the transparent planning
            // repair loop. A successful plan starts the emitted attempt with a
            // clean eviction set so an unrelated stream error cannot consume a
            // stale planning miss and trigger a spurious reset.
            let _ = evict.take_evicted();
            let schema = stream.schema();

            // Buffer the response for the cache while it streams (dropped past the
            // per-entry cap → response still streams, just isn't cached). Skipped
            // entirely when the cache is disabled or the query is `live` (whose
            // result is time-varying and must never be cached).
            let mut tee = if self.result_cache.enabled() && !req.live {
                ResponseTee::new(self.result_cache_entry_bytes)
            } else {
                ResponseTee::disabled()
            };

            let data_gen = IpcDataGenerator::default();
            let mut dict_tracker = DictionaryTracker::new(false);
            let options = IpcWriteOptions::default();

            // Schema message: one SchemaMsg before any BatchMsg.
            let schema_enc = data_gen.schema_to_bytes_with_dictionary_tracker(
                &schema,
                &mut dict_tracker,
                &options,
            );
            let mut schema_bytes = Vec::new();
            if let Err(e) = write_message(&mut schema_bytes, schema_enc, &options) {
                let _ = emit_stream_error(
                    &mut wr,
                    QUERY_ERR_INTERNAL,
                    format!("write_message(schema): {e}"),
                )
                .await;
                let _ = wr.flush().await;
                self.emit_scan_complete(
                    signal,
                    Some(physical.as_ref()),
                    rows_total,
                    pool_start,
                    cache_start,
                    "miss",
                    t0.elapsed(),
                );
                return Ok(());
            }
            let schema_frame = QueryFrame {
                msg: QueryFrameMsg::SchemaMsg(
                    SchemaMsgInput {
                        ipc_bytes: schema_bytes,
                    }
                    .into(),
                ),
            };
            if let Err(e) = write_and_tee(&mut wr, &schema_frame, &mut tee).await {
                warn!(error = %e, "client disconnected while writing SchemaMsg");
                self.emit_scan_complete(
                    signal,
                    Some(physical.as_ref()),
                    rows_total,
                    pool_start,
                    cache_start,
                    "miss",
                    t0.elapsed(),
                );
                return Ok(());
            }

            // Stream batches. Race each await against the process-level memory
            // guard so work outside DataFusion's reservation accounting cannot run
            // all the way into the cgroup OOM killer.
            loop {
                let next = if let Some(guard) = &self.memory_guard {
                    tokio::select! {
                        batch = stream.next() => batch,
                        _ = guard.wait_until_exhausted() => {
                            let _ = emit_stream_error(
                                &mut wr,
                                QUERY_ERR_RESOURCES,
                                QUERY_TOO_LARGE_MESSAGE,
                            ).await;
                            let _ = wr.flush().await;
                            self.emit_scan_complete(
                                signal,
                                Some(physical.as_ref()),
                                rows_total,
                                pool_start,
                                cache_start,
                                "miss",
                                t0.elapsed(),
                            );
                            return Ok(());
                        }
                    }
                } else {
                    stream.next().await
                };
                let Some(batch_res) = next else { break };
                let batch = match batch_res {
                    Ok(b) => b,
                    Err(e) => {
                        // A recorded parquet 404 after SchemaMsg makes this attempt
                        // provisional. Resolve lineage and atomically repair the
                        // local catalog, then tell the capable client to retract the
                        // attempt and restart the same request from a fresh listing.
                        let evicted = evict.take_evicted();
                        if !evicted.is_empty() && active_attempt < 2 {
                            let missing_context: Vec<_> = candidate_context
                                .iter()
                                .filter(|(uuid, _, _)| evicted.contains(uuid))
                                .cloned()
                                .collect();
                            let reconciled =
                                match self.reconcile_missing_partitions(&missing_context).await {
                                    Ok(metas) => metas,
                                    Err(repair) => {
                                        let _ =
                                            emit_stream_error(&mut wr, QUERY_ERR_INTERNAL, repair)
                                                .await;
                                        let _ = wr.flush().await;
                                        return Ok(());
                                    }
                                };
                            match self.apply_reconciled_repair(&evicted, &reconciled) {
                                Ok(reason) => {
                                    let reset = QueryFrame {
                                        msg: QueryFrameMsg::ResponseSuperseded(
                                            ResponseSupersededInput {
                                                superseded_attempt: active_attempt,
                                                next_attempt: active_attempt + 1,
                                                reason,
                                            }
                                            .into(),
                                        ),
                                    };
                                    if write_frame(&mut wr, &reset).await.is_err()
                                        || wr.flush().await.is_err()
                                    {
                                        return Ok(());
                                    }
                                    info!(
                                        signal = signal_name(signal),
                                        evicted = evicted.len(),
                                        superseded_attempt = active_attempt,
                                        next_attempt = active_attempt + 1,
                                        "block(s) 404'd mid-scan; restarting response attempt"
                                    );
                                    active_attempt += 1;
                                    continue 'attempt;
                                }
                                Err(repair) => {
                                    let _ = emit_stream_error(&mut wr, QUERY_ERR_INTERNAL, repair)
                                        .await;
                                    let _ = wr.flush().await;
                                    return Ok(());
                                }
                            }
                        }
                        let code =
                            if matches!(e.find_root(), DataFusionError::ResourcesExhausted(_)) {
                                QUERY_ERR_RESOURCES
                            } else {
                                QUERY_ERR_INTERNAL
                            };
                        let message = if !evicted.is_empty() {
                            format!("query attempt supersession limit exhausted: DataFusion: {e}")
                        } else {
                            format!("DataFusion: {e}")
                        };
                        let _ = emit_stream_error(&mut wr, code, message).await;
                        let _ = wr.flush().await;
                        self.emit_scan_complete(
                            signal,
                            Some(physical.as_ref()),
                            rows_total,
                            pool_start,
                            cache_start,
                            "miss",
                            t0.elapsed(),
                        );
                        return Ok(());
                    }
                };

                // encoded_batch returns (dictionary batches, record batch).
                // Each is one IPC message — we frame each as its own
                // BatchMsg so a single batch with new dictionaries lands
                // as N+1 BatchMsg frames in order.
                #[allow(deprecated)]
                let (dict_batches, batch_enc) =
                    match data_gen.encoded_batch(&batch, &mut dict_tracker, &options) {
                        Ok(v) => v,
                        Err(e) => {
                            let _ = emit_stream_error(
                                &mut wr,
                                QUERY_ERR_INTERNAL,
                                format!("encoded_batch: {e}"),
                            )
                            .await;
                            let _ = wr.flush().await;
                            self.emit_scan_complete(
                                signal,
                                Some(physical.as_ref()),
                                rows_total,
                                pool_start,
                                cache_start,
                                "miss",
                                t0.elapsed(),
                            );
                            return Ok(());
                        }
                    };

                for d in dict_batches {
                    if let Err(e) = write_one_batch(&mut wr, d, &options, &mut tee).await {
                        warn!(error = %e, "client disconnected while writing BatchMsg (dict)");
                        self.emit_scan_complete(
                            signal,
                            Some(physical.as_ref()),
                            rows_total,
                            pool_start,
                            cache_start,
                            "miss",
                            t0.elapsed(),
                        );
                        return Ok(());
                    }
                }
                if let Err(e) = write_one_batch(&mut wr, batch_enc, &options, &mut tee).await {
                    warn!(error = %e, "client disconnected while writing BatchMsg");
                    self.emit_scan_complete(
                        signal,
                        Some(physical.as_ref()),
                        rows_total,
                        pool_start,
                        cache_start,
                        "miss",
                        t0.elapsed(),
                    );
                    return Ok(());
                }

                rows_total = rows_total.saturating_add(batch.num_rows() as u64);
            }

            // Normal completion: EndOfStream terminator. The attempt is final only
            // once both the frame write and the buffered socket flush succeed.
            let end_frame = QueryFrame {
                msg: QueryFrameMsg::EndOfStream(
                    EndOfStreamInput {
                        total_rows: rows_total,
                    }
                    .into(),
                ),
            };
            if let Err(e) = write_and_tee(&mut wr, &end_frame, &mut tee).await {
                warn!(error = %e, "client disconnected while writing EndOfStream");
                return Ok(());
            }
            if let Err(e) = wr.flush().await {
                warn!(error = %e, "client disconnected while flushing EndOfStream");
                return Ok(());
            }
            if let Some(g) = inflight.as_mut() {
                g.mark_ok();
            }

            // Admit only this final, cleanly-completed attempt after EOS is known to
            // have reached the socket. Provisional attempt bytes never enter `tee`.
            if let Some(bytes) = tee.take() {
                self.result_cache.insert(cache_key, bytes.into());
            }

            self.emit_scan_complete(
                signal,
                Some(physical.as_ref()),
                rows_total,
                pool_start,
                cache_start,
                "miss",
                t0.elapsed(),
            );
            return Ok(());
        }
    }

    /// Emit the per-query `scan_complete` event with the same field
    /// shape as the pre-step-5 implementation, plus the v0.4 `signal`
    /// field so dashboards / log-parsing can split per-signal cleanly.
    fn emit_scan_complete(
        &self,
        signal: Signal,
        plan: Option<&dyn ExecutionPlan>,
        rows_total: u64,
        pool_start: PoolStats,
        cache_start: CacheStarts,
        cache_status: &'static str,
        wall: Duration,
    ) {
        let pool_end = self.pool.stats();
        let pool_delta = pool_end.delta(pool_start);
        let cache_end = self.postings_cache.stats();
        let cache_delta = cache_end.delta(cache_start.postings);
        let bloom_end = self.bloom_cache.stats();
        let bloom_delta = bloom_end.delta(cache_start.bloom);
        let result_end = self.result_cache.stats();
        let result_delta = result_end.delta(cache_start.result);
        let metrics = plan.and_then(collect_leaf_metrics);

        let (row_groups_pruned, row_groups_matched, files_pruned, bytes_scanned) = match metrics {
            Some(m) => summarise_metrics(&m),
            None => (0, 0, 0, 0),
        };

        // Process-wide snapshot. Under sequential single-query
        // workloads this is effectively this query's high-water mark;
        // under concurrent queries it's noisy but still useful for
        // budget-headroom telemetry.
        let memory_reserved_bytes_end = self.memory_pool.reserved();

        info!(
            signal = signal_name(signal),
            cache = cache_status,
            total_rows = rows_total,
            row_groups_matched,
            row_groups_pruned,
            files_pruned,
            bytes_scanned,
            pool_reuses_delta = pool_delta.reuses,
            pool_allocs_delta = pool_delta.allocs,
            pool_misses_delta = pool_delta.misses,
            pool_grows_delta = pool_delta.grows,
            pool_capacity = pool_end.capacity,
            pool_free_count = pool_end.free_count,
            pool_free_bytes = pool_end.free_bytes,
            pool_max_retained_bytes = pool_end.max_retained_bytes,
            postings_cache_hits_delta = cache_delta.hits,
            postings_cache_misses_delta = cache_delta.misses,
            postings_cache_evictions_delta = cache_delta.evictions,
            postings_cache_fetch_errors_delta = cache_delta.fetch_errors,
            postings_cache_entries = cache_end.entries,
            postings_cache_bytes_in = cache_end.bytes_in,
            bloom_cache_hits_delta = bloom_delta.hits,
            bloom_cache_misses_delta = bloom_delta.misses,
            bloom_cache_evictions_delta = bloom_delta.evictions,
            bloom_cache_fetch_errors_delta = bloom_delta.fetch_errors,
            bloom_cache_entries = bloom_end.entries,
            bloom_cache_bytes_in = bloom_end.bytes_in,
            result_cache_hits_delta = result_delta.hits,
            result_cache_misses_delta = result_delta.misses,
            result_cache_inserts_delta = result_delta.inserts,
            result_cache_evictions_delta = result_delta.evictions,
            result_cache_entries = result_end.entries,
            result_cache_bytes_in = result_end.bytes_in,
            query_memory_reserved_bytes_end = memory_reserved_bytes_end,
            pool_in_flight = pool_end.in_flight,
            wall_ms = wall.as_millis() as u64,
            "scan_complete"
        );
        // The Span wrapping the call site carries `request_id`; no
        // need to log it explicitly here.
        let _ = Span::current();

        // Status-page accounting (D-057): fold this scan's row + object-store
        // byte counts into the process-global totals. One call per terminal
        // path, so it double-counts nothing.
        if let Some(m) = &self.metrics {
            m.record_scan(rows_total, bytes_scanned as u64);
        }
    }
}

/// Stable, lowercase signal name for tracing fields. Matches the
/// shape used by `crates/query/src/cli.rs::CliSignal::name`,
/// so dashboards filtering on `signal="metrics"` agree at both ends.
fn signal_name(s: Signal) -> &'static str {
    match s {
        Signal::Metrics => "metrics",
        Signal::Logs => "logs",
        Signal::Traces => "traces",
        Signal::Profiles => "profiles",
        Signal::Dummy => "dummy",
    }
}

/// Frame one IPC-encoded message as a [`QueryFrameMsg::BatchMsg`], writing it to
/// the socket and teeing its exact wire bytes into the result-cache buffer.
/// Encapsulated so the dictionary / record-batch arms in
/// [`QueryService::run_query`] don't repeat the same lines.
async fn write_one_batch<W>(
    wr: &mut BufWriter<W>,
    enc: arrow_ipc::writer::EncodedData,
    options: &IpcWriteOptions,
    tee: &mut ResponseTee,
) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut bytes = Vec::new();
    write_message(&mut bytes, enc, options).map_err(|e| anyhow::anyhow!("write_message: {e}"))?;
    let frame = QueryFrame {
        msg: QueryFrameMsg::BatchMsg(BatchMsgInput { ipc_bytes: bytes }.into()),
    };
    write_and_tee(wr, &frame, tee).await
}

/// Accumulates the exact response bytes for the result cache while they're also
/// streamed to the socket. Once the buffer would exceed `cap` it's dropped and
/// stays `None` — the response streams on but won't be cached (keeps large log
/// dumps out of the cache). [`ResponseTee::disabled`] skips buffering entirely
/// when the cache is off.
struct ResponseTee {
    buf: Option<Vec<u8>>,
    cap: usize,
}

impl ResponseTee {
    fn new(cap: usize) -> Self {
        Self {
            buf: Some(Vec::new()),
            cap,
        }
    }

    fn disabled() -> Self {
        Self { buf: None, cap: 0 }
    }

    /// Append `bytes` unless the buffer is already dropped or would exceed the
    /// cap (in which case it's dropped, permanently, for this response).
    fn push(&mut self, bytes: &[u8]) {
        if let Some(b) = self.buf.as_mut() {
            if b.len().saturating_add(bytes.len()) > self.cap {
                self.buf = None;
            } else {
                b.extend_from_slice(bytes);
            }
        }
    }

    /// The buffered response, or `None` if it was dropped / disabled.
    fn take(self) -> Option<Vec<u8>> {
        self.buf
    }
}

/// Encode a `QueryFrame` to its exact on-wire bytes: `[u32 BE payload-len]
/// [payload]` — byte-identical to what [`write_frame`] emits, so a tee'd copy
/// replays perfectly on a cache hit.
fn frame_to_wire(frame: &QueryFrame) -> Result<Vec<u8>> {
    let payload = Framed::encode(frame).map_err(|e| anyhow::anyhow!("frame encode: {e}"))?;
    if payload.len() > MAX_FRAME_BYTES {
        return Err(anyhow::anyhow!(
            "frame {} exceeds max {}",
            payload.len(),
            MAX_FRAME_BYTES
        ));
    }
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Write one frame to the socket and tee its bytes for caching.
async fn write_and_tee<W>(
    wr: &mut BufWriter<W>,
    frame: &QueryFrame,
    tee: &mut ResponseTee,
) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let wire = frame_to_wire(frame)?;
    wr.write_all(&wire)
        .await
        .map_err(|e| anyhow::anyhow!("write_all: {e}"))?;
    tee.push(&wire);
    Ok(())
}

/// Build the result-cache key for a data query: a 128-bit content hash over the
/// normalized request **plus** the candidate block-UUID set. Folding the
/// candidate set in is what makes invalidation free — any ingest / compaction /
/// retention that changes which blocks a range touches changes the candidate
/// set, hence the key, hence it's a miss; a closed past range keeps a stable
/// set and stays cached. `request_id` is deliberately excluded (per-call
/// correlation id, not part of the result identity).
fn data_query_cache_key(signal: Signal, req: &QueryRequest, candidates: &[CatalogEntry]) -> u128 {
    fn push_str(buf: &mut Vec<u8>, s: &str) {
        buf.extend_from_slice(&(s.len() as u32).to_be_bytes());
        buf.extend_from_slice(s.as_bytes());
    }
    fn push_opt_u64(buf: &mut Vec<u8>, v: Option<u64>) {
        match v {
            Some(x) => {
                buf.push(1);
                buf.extend_from_slice(&x.to_be_bytes());
            }
            None => buf.push(0),
        }
    }

    let mut buf: Vec<u8> = Vec::new();
    buf.push(0x01); // kind discriminator: data query (vs future metadata keys)
    buf.push(signal as u8);

    // Matchers, canonically sorted so ordering doesn't perturb the key.
    let mut matchers: Vec<&(String, String)> = req.query.matchers.iter().collect();
    matchers.sort();
    buf.extend_from_slice(&(matchers.len() as u32).to_be_bytes());
    for (k, v) in matchers {
        push_str(&mut buf, k);
        push_str(&mut buf, v);
    }

    push_opt_u64(&mut buf, req.query.ts_min);
    push_opt_u64(&mut buf, req.query.ts_max);

    match req.sql.as_deref() {
        Some(s) => {
            buf.push(1);
            push_str(&mut buf, s);
        }
        None => buf.push(0),
    }
    push_opt_u64(&mut buf, req.limit.map(|l| l as u64));
    match &req.query.trace_id {
        Some(t) => {
            buf.push(1);
            buf.extend_from_slice(t);
        }
        None => buf.push(0),
    }
    match req.query.body_contains.as_deref() {
        Some(s) => {
            buf.push(1);
            push_str(&mut buf, s);
        }
        None => buf.push(0),
    }
    // `with_labels` changes the result SCHEMA (metrics gains a `labels`
    // column), so a label-less and a label-ful request over the same
    // candidates must NOT share a cache entry — else the client decodes the
    // wrong column count. Hash it into the key.
    buf.push(u8::from(req.query.with_labels));

    // Candidate block UUIDs, sorted — the invalidation-carrying component.
    let mut uuids: Vec<Uuid> = candidates.iter().map(|c| c.meta.uuid).collect();
    uuids.sort();
    buf.extend_from_slice(&(uuids.len() as u32).to_be_bytes());
    for u in uuids {
        buf.extend_from_slice(u.as_bytes());
    }

    hash128(&buf)
}

async fn send_query_overload(sock: TcpStream, message: &'static str) {
    let (_rd, wr) = sock.into_split();
    let mut wr = BufWriter::new(wr);
    let _ = emit_stream_error(&mut wr, QUERY_ERR_RESOURCES, message).await;
    let _ = wr.flush().await;
}

/// Build + transmit one [`QueryFrameMsg::StreamError`] frame. Errors
/// from `write_frame` itself are swallowed by the caller (they
/// usually mean the client already dropped the socket); the scan
/// trailer is still emitted via `emit_scan_complete`.
async fn emit_stream_error<W>(
    wr: &mut BufWriter<W>,
    code: u16,
    message: impl Into<String>,
) -> Result<(), scry_proto::framing::FrameError>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let frame = QueryFrame {
        msg: QueryFrameMsg::StreamError(
            StreamErrorInput {
                code,
                message: message.into(),
            }
            .into(),
        ),
    };
    write_frame(wr, &frame).await
}

/// Walk the plan tree, merging every leaf node's `MetricsSet` into
/// one. Step 4 emits one `DataSourceExec` per metrics block under a
/// `UnionExec` — each branch carries its own pruning + bytes-scanned
/// counters. Summing them gives the per-query trailer the same shape
/// it had under the single-`DataSourceExec` design while preserving
/// per-block pruning sharpness in the underlying scans.
///
/// `MetricsSet::aggregate_by_name` (called downstream) sums same-named
/// metrics across the merged set, so two `bytes_scanned` counters
/// from two leaves collapse to a single summed row.
fn collect_leaf_metrics(plan: &dyn ExecutionPlan) -> Option<MetricsSet> {
    let mut out: Option<MetricsSet> = None;
    fn descend(plan: &dyn ExecutionPlan, out: &mut Option<MetricsSet>) {
        let children = plan.children();
        if children.is_empty() {
            if let Some(m) = plan.metrics() {
                match out.as_mut() {
                    None => *out = Some(m),
                    Some(acc) => {
                        for v in m.iter() {
                            acc.push(v.clone());
                        }
                    }
                }
            }
        } else {
            for c in children {
                descend(c.as_ref(), out);
            }
        }
    }
    descend(plan, &mut out);
    out
}

/// Sum the pruning + bytes counters into the same trailer shape the
/// CLI prints. `(row_groups_pruned, row_groups_matched, files_pruned,
/// bytes_scanned)`.
fn summarise_metrics(metrics: &MetricsSet) -> (usize, usize, usize, usize) {
    let agg = metrics.aggregate_by_name();
    let pruning = |name: &str| -> (usize, usize) {
        for m in agg.iter() {
            if m.value().name() == name {
                if let MetricValue::PruningMetrics {
                    pruning_metrics, ..
                } = m.value()
                {
                    return (pruning_metrics.pruned(), pruning_metrics.matched());
                }
            }
        }
        (0, 0)
    };
    let (row_groups_pruned, _) = pruning("row_groups_pruned_statistics");
    let (_, row_groups_matched) = pruning("row_groups_matched_statistics");
    let (files_pruned, _) = pruning("files_ranges_pruned_statistics");
    let bytes_scanned = agg
        .iter()
        .find(|m| m.value().name() == "bytes_scanned")
        .map(|m| m.value().as_usize())
        .unwrap_or(0);
    (
        row_groups_pruned,
        row_groups_matched,
        files_pruned,
        bytes_scanned,
    )
}

/// Conservative owned-memory estimate for one converted live row. String heap
/// contents and pair/vector structural storage dominate; allocator rounding is
/// intentionally left to the aggregate headroom configured by the daemon.
fn live_record_owned_bytes(r: &scry_proto::generated::LiveRecord) -> usize {
    const ROW_OVERHEAD: usize = std::mem::size_of::<LiveLogRow>();
    const PAIR_OVERHEAD: usize = std::mem::size_of::<(String, String)>();
    ROW_OVERHEAD.saturating_add(r.body.len()).saturating_add(
        r.labels
            .iter()
            .chain(r.attributes.iter())
            .map(|p| {
                PAIR_OVERHEAD
                    .saturating_add(p.key.len())
                    .saturating_add(p.value.len())
            })
            .sum::<usize>(),
    )
}

/// The merged history+live dedup selector (D-054). A live record tagged with
/// `wal_seg` is *durable* — already committed to a block and therefore must be
/// dropped from the live half to avoid a double across the block-commit seam —
/// iff a watermark exists for its WAL instance and its segment is at or below
/// it. An absent watermark (`None`) means no block has ever sealed for that
/// `(writer, shard)`, so nothing is durable yet and every live record is kept.
///
/// WAL segments are 0-based, so this must be `Option`-aware: `Some(0)` (the
/// first block sealed segment 0) legitimately covers segment 0, whereas `None`
/// (no block) must *not* — collapsing the two via `unwrap_or(0)` would drop a
/// fresh ingester's first-segment records before its first flush.
#[inline]
fn live_record_is_durable(wal_seg: u64, watermark: Option<u64>) -> bool {
    matches!(watermark, Some(h) if wal_seg <= h)
}

/// Validate, sort, and canonicalize the opaque registry values before exposing
/// them over the public query wire. Invalid or partially-written entries are
/// ignored just like the standalone status endpoint ignores them.
fn canonical_fleet_json(blobs: Vec<String>) -> Vec<String> {
    let mut snapshots: Vec<crate::stats::StatusSnapshot> = blobs
        .into_iter()
        .filter_map(|blob| serde_json::from_str(&blob).ok())
        .collect();
    snapshots.sort_by(|a, b| {
        a.role
            .cmp(&b.role)
            .then_with(|| a.instance_id.cmp(&b.instance_id))
    });
    snapshots
        .into_iter()
        .filter_map(|snapshot| serde_json::to_string(&snapshot).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use bytes::Bytes;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
        Result as OsResult,
    };
    use scry_block::BlockMeta;
    use scry_catalog::Catalog;
    use scry_objstore::BufPool;
    use scry_proto::{generated::LiveRecord, LabelPair};
    use scry_query::{BloomCache, PostingsCache, QueryResultCache};
    use tempfile::TempDir;
    use tokio::time::Duration;
    use uuid::Uuid;

    use super::{
        canonical_fleet_json, live_record_is_durable, live_record_owned_bytes, QueryService,
        TargetedRepairLimits,
    };

    #[derive(Debug)]
    struct CountingStore {
        inner: InMemory,
        lists: AtomicUsize,
        gets: AtomicUsize,
        active_gets: AtomicUsize,
        max_active_gets: AtomicUsize,
        get_delay: Duration,
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("CountingStore")
        }
    }

    impl CountingStore {
        fn new(get_delay: Duration) -> Self {
            Self {
                inner: InMemory::new(),
                lists: AtomicUsize::new(0),
                gets: AtomicUsize::new(0),
                active_gets: AtomicUsize::new(0),
                max_active_gets: AtomicUsize::new(0),
                get_delay,
            }
        }

        async fn seed(&self, path: Path, bytes: Vec<u8>) {
            self.inner.put(&path, bytes.into()).await.unwrap();
        }
    }

    #[async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(&self, p: &Path, v: PutPayload, o: PutOptions) -> OsResult<PutResult> {
            self.inner.put_opts(p, v, o).await
        }

        async fn put_multipart_opts(
            &self,
            p: &Path,
            o: PutMultipartOptions,
        ) -> OsResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(p, o).await
        }

        async fn get_opts(&self, p: &Path, o: GetOptions) -> OsResult<GetResult> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            let active = self.active_gets.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active_gets.fetch_max(active, Ordering::SeqCst);
            tokio::time::sleep(self.get_delay).await;
            let result = self.inner.get_opts(p, o).await;
            self.active_gets.fetch_sub(1, Ordering::SeqCst);
            result
        }

        async fn get_ranges(&self, p: &Path, r: &[Range<u64>]) -> OsResult<Vec<Bytes>> {
            self.inner.get_ranges(p, r).await
        }

        fn delete_stream(
            &self,
            paths: BoxStream<'static, OsResult<Path>>,
        ) -> BoxStream<'static, OsResult<Path>> {
            self.inner.delete_stream(paths)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
            self.lists.fetch_add(1, Ordering::SeqCst);
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, OsResult<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, o: CopyOptions) -> OsResult<()> {
            self.inner.copy_opts(from, to, o).await
        }

        async fn rename_opts(&self, from: &Path, to: &Path, o: RenameOptions) -> OsResult<()> {
            self.inner.rename_opts(from, to, o).await
        }
    }

    fn repair_meta(uuid: Uuid, writer_id: Uuid) -> BlockMeta {
        BlockMeta {
            uuid,
            signal: "logs".into(),
            writer_id,
            ts_min_unix_nano: 1,
            ts_max_unix_nano: 2,
            row_count: 1,
            byte_size: 1,
            schema_version: 1,
            level: 0,
            compacted_from: Vec::new(),
            producer_version: "test".into(),
            label_fingerprint_bloom: None,
            has_postings: false,
            postings_size_bytes: None,
            series_types: None,
            all_fingerprints: None,
            has_body_bloom: false,
            body_bloom_size_bytes: None,
            wal_seg_max: None,
            wal_shard: None,
        }
    }

    fn repair_service(store: Arc<dyn ObjectStore>, limits: TargetedRepairLimits) -> QueryService {
        let tmp = TempDir::new().unwrap();
        let catalog = Catalog::open(&tmp.path().join("catalog.sqlite"), "test").unwrap();
        let memory_pool = Arc::new(GreedyMemoryPool::new(64 * 1024 * 1024));
        let runtime_env = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(memory_pool.clone())
                .build()
                .unwrap(),
        );
        QueryService::new(
            Arc::new(Mutex::new(catalog)),
            store,
            BufPool::new(),
            Arc::new(PostingsCache::with_budget_bytes(0)),
            Arc::new(BloomCache::with_budget_bytes(0)),
            runtime_env,
            memory_pool,
            Arc::new(QueryResultCache::with_budget_bytes(0)),
            0,
        )
        .with_targeted_repair_limits(limits)
    }

    #[tokio::test]
    async fn targeted_repair_single_flights_and_caches_partition() {
        let store = Arc::new(CountingStore::new(Duration::from_millis(20)));
        let writer = Uuid::now_v7();
        let meta = repair_meta(Uuid::now_v7(), writer);
        let path = Path::from(format!("logs/2026/08/23/{writer}/{}.meta.json", meta.uuid));
        store.seed(path, serde_json::to_vec(&meta).unwrap()).await;
        let service = Arc::new(repair_service(
            store.clone(),
            TargetedRepairLimits {
                get_concurrency: 2,
                deadline: Duration::from_secs(2),
                cache_ttl: Duration::from_secs(1),
                stable_attempts: 2,
            },
        ));
        let partition = ("logs".to_string(), "2026-08-23".to_string());

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let service = service.clone();
            let partition = partition.clone();
            tasks.push(tokio::spawn(async move {
                service.get_or_load_partition(partition).await.unwrap()
            }));
        }
        for task in tasks {
            assert_eq!(task.await.unwrap().metas.len(), 1);
        }
        assert_eq!(store.lists.load(Ordering::SeqCst), 2);
        assert_eq!(store.gets.load(Ordering::SeqCst), 1);

        service.get_or_load_partition(partition).await.unwrap();
        assert_eq!(store.lists.load(Ordering::SeqCst), 2, "TTL cache hit");
        assert_eq!(store.gets.load(Ordering::SeqCst), 1, "TTL cache hit");
    }

    #[tokio::test]
    async fn targeted_repair_bounds_gets_process_wide() {
        let store = Arc::new(CountingStore::new(Duration::from_millis(30)));
        for day in ["23", "24"] {
            let writer = Uuid::now_v7();
            for _ in 0..4 {
                let meta = repair_meta(Uuid::now_v7(), writer);
                let path = Path::from(format!(
                    "logs/2026/08/{day}/{writer}/{}.meta.json",
                    meta.uuid
                ));
                store.seed(path, serde_json::to_vec(&meta).unwrap()).await;
            }
        }
        let service = Arc::new(repair_service(
            store.clone(),
            TargetedRepairLimits {
                get_concurrency: 2,
                deadline: Duration::from_secs(3),
                cache_ttl: Duration::ZERO,
                stable_attempts: 1,
            },
        ));
        let a = {
            let service = service.clone();
            tokio::spawn(async move {
                service
                    .get_or_load_partition(("logs".into(), "2026-08-23".into()))
                    .await
                    .unwrap()
            })
        };
        let b = {
            let service = service.clone();
            tokio::spawn(async move {
                service
                    .get_or_load_partition(("logs".into(), "2026-08-24".into()))
                    .await
                    .unwrap()
            })
        };
        assert_eq!(a.await.unwrap().metas.len(), 4);
        assert_eq!(b.await.unwrap().metas.len(), 4);
        assert_eq!(store.gets.load(Ordering::SeqCst), 8);
        assert!(
            store.max_active_gets.load(Ordering::SeqCst) <= 2,
            "global repair GET semaphore was exceeded"
        );
    }

    #[test]
    fn fleet_json_drops_invalid_records_and_sorts_stably() {
        let make = |role: &str, id: &str| {
            serde_json::json!({
                "role": role,
                "instance_id": id,
                "addr": id,
                "now_unix_ms": 1,
                "uptime_secs": 2.0,
                "rss_kib": null,
                "data": {}
            })
            .to_string()
        };
        let result = canonical_fleet_json(vec![
            make("query", "q-2"),
            "not json".into(),
            make("agent", "a-1"),
            make("query", "q-1"),
        ]);
        let ids: Vec<String> = result
            .iter()
            .map(|json| {
                serde_json::from_str::<crate::stats::StatusSnapshot>(json)
                    .unwrap()
                    .instance_id
            })
            .collect();
        assert_eq!(ids, ["a-1", "q-1", "q-2"]);
    }

    #[test]
    fn live_record_memory_estimate_counts_owned_strings_and_pairs() {
        let record = LiveRecord {
            wal_shard: 0,
            wal_seg: 0,
            ts_unix_nano: 1,
            severity: 2,
            labels: vec![LabelPair {
                key: "service".into(),
                value: "api".into(),
            }],
            body: "hello".into(),
            attributes: vec![LabelPair {
                key: "zone".into(),
                value: "west".into(),
            }],
        };
        let base = std::mem::size_of::<scry_query::logs::LiveLogRow>();
        let pairs = 2 * std::mem::size_of::<(String, String)>();
        assert_eq!(
            live_record_owned_bytes(&record),
            base + pairs + "serviceapihellozonewest".len()
        );
    }

    #[test]
    fn absent_watermark_keeps_everything_including_segment_zero() {
        // No block yet for this (writer, shard): nothing is durable, so even
        // segment 0 stays live. This is the case `unwrap_or(0)` got wrong.
        assert!(!live_record_is_durable(0, None));
        assert!(!live_record_is_durable(7, None));
    }

    #[test]
    fn watermark_zero_covers_only_segment_zero() {
        // First block sealed segment 0 ⇒ H = Some(0). Segment 0 is durable;
        // segment 1+ (appended after the post-flush rotation) is still live.
        assert!(live_record_is_durable(0, Some(0)));
        assert!(!live_record_is_durable(1, Some(0)));
    }

    #[test]
    fn straddle_splits_exactly_at_the_watermark() {
        // A block that spanned segments up to 5 (size-based mid-block WAL
        // rotation) advances H to 5. Live records ≤ 5 are durable; > 5 live.
        for seg in 0..=5 {
            assert!(live_record_is_durable(seg, Some(5)), "seg {seg} ≤ 5");
        }
        assert!(!live_record_is_durable(6, Some(5)));
        assert!(!live_record_is_durable(u64::MAX, Some(5)));
    }
}
