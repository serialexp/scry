//! Arrow Flight query service for the long-running query daemon.
//!
//! This is the v0.3 step 2 slice of the eventual scatter-gather query
//! architecture (`docs/ARCHITECTURE.md` §"Query"): a single node that
//! exposes the existing [`scry_query`] machinery — `MetricsQuery`
//! preselect via postings sidecars → DataFusion `TableProvider` →
//! Parquet scan — through Arrow Flight, so the `SessionContext` and
//! the [`BufPool`] stay warm across queries. The CLI's per-process
//! cold-start cost (~1.3 s) becomes a one-time daemon-startup cost
//! amortised across every subsequent query.
//!
//! Wire shape:
//!
//! - Client encodes a [`scry_query::flight_proto::QueryRequest`] as
//!   JSON into the Flight `Ticket` bytes (see `flight_proto.rs` for
//!   the justification).
//! - Server's `do_get` decodes, registers a per-request
//!   [`MetricsTable`] on a fresh `SessionContext`, executes the
//!   plan, and streams the resulting `RecordBatch`es back as
//!   `FlightData` via [`FlightDataEncoderBuilder`].
//! - `get_flight_info` is degenerate (returns one endpoint pointing
//!   back at the same daemon with the same ticket) so clients that
//!   want the canonical Flight "negotiate first" pattern can use it.
//! - Every other Flight RPC returns `Status::unimplemented`.
//!
//! What's intentionally NOT here (deferred to v0.3.x):
//!
//! - Coordinator↔worker scatter-gather. This is client↔daemon; the
//!   architecture's worker pool is a separate layer.
//! - Auth / TLS. Localhost dev binary.
//! - Per-query-tagged pool counters. Pool stats are process
//!   cumulative; deltas over the request are the best we can do
//!   under concurrent queries (and are race-y under contention).
//!
//! Step 4 wired in a shared [`GreedyMemoryPool`] on a process-wide
//! [`RuntimeEnv`]: every per-request `SessionContext` reuses the same
//! pool, so the budget is enforced across concurrent queries and a
//! pathological one returns `ResourcesExhausted` rather than OOM-ing
//! the daemon.

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, Location, PollInfo, PutResult, SchemaResult, Ticket,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::context::SessionContext;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::SessionConfig;
use futures::stream::{BoxStream, Stream, StreamExt, TryStreamExt};
use object_store::ObjectStore;
use scry_catalog::Catalog;
use scry_objstore::{BufPool, PoolStats};
use scry_query::{
    flight_proto::QueryRequest, list_metrics_candidates, register_metrics_table_from_candidates,
    PostingsCache, PostingsCacheStats, METRICS_TABLE_NAME,
};
use std::net::SocketAddr;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, info_span, Instrument, Span};

/// Long-lived query service. One instance per daemon process. All
/// fields are `Arc`'d / `Clone` so per-request handler captures are
/// cheap.
pub struct QueryService {
    /// `rusqlite::Connection` (and therefore `scry_catalog::Catalog`)
    /// is `!Sync` because of its interior `RefCell`. Wrapping it in a
    /// `std::sync::Mutex` makes the whole service `Sync`, which the
    /// `tonic` Flight service trait requires (the generated future
    /// captures `&Self` and must be `Send`). Lock contention is a
    /// non-issue: we only hold the guard for the brief synchronous
    /// `list_metrics_candidates` call (one SELECT against an indexed
    /// table), then drop it before any async work.
    catalog: Arc<Mutex<Catalog>>,
    store: Arc<dyn ObjectStore>,
    pool: BufPool,
    /// Per-block postings cache. Shared across all queries — the
    /// daemon's reason for existing in the first place is that
    /// blocks are immutable so caching their sidecars is a pure
    /// win after the first hit. Single-flight built in: concurrent
    /// misses on the same block only do one parquet fetch.
    postings_cache: Arc<PostingsCache>,
    /// Shared DataFusion runtime env. Every per-request
    /// `SessionContext` is constructed with `new_with_config_rt(...,
    /// runtime_env.clone())`, which is the *only* way DataFusion
    /// enforces the memory budget across queries
    /// (`datafusion/execution/src/runtime_env.rs` calls this out
    /// explicitly: "resource limits are only enforced when contexts
    /// share a `RuntimeEnv`"). Also carries the object-store registry
    /// — registrations from `register_metrics_table_from_candidates`
    /// land here once and are reused on subsequent queries.
    runtime_env: Arc<RuntimeEnv>,
    /// Same pool that lives inside `runtime_env`, kept here as a
    /// concrete `Arc<GreedyMemoryPool>` so we can call `reserved()`
    /// at scan_complete time without downcasting from
    /// `Arc<dyn MemoryPool>`. Pure observability handle.
    memory_pool: Arc<GreedyMemoryPool>,
    /// Monotonic per-process counter, used only when the client
    /// didn't supply `request_id`. A `u64` is plenty — the daemon
    /// would have to serve 18 quintillion requests to wrap.
    next_request_id: AtomicU64,
}

impl QueryService {
    pub fn new(
        catalog: Arc<Mutex<Catalog>>,
        store: Arc<dyn ObjectStore>,
        pool: BufPool,
        postings_cache: Arc<PostingsCache>,
        runtime_env: Arc<RuntimeEnv>,
        memory_pool: Arc<GreedyMemoryPool>,
    ) -> Self {
        Self {
            catalog,
            store,
            pool,
            postings_cache,
            runtime_env,
            memory_pool,
            next_request_id: AtomicU64::new(0),
        }
    }

    /// Borrow the postings cache — exposed so the binary can log
    /// budget state at startup and so callers can inspect stats.
    pub fn postings_cache(&self) -> &Arc<PostingsCache> {
        &self.postings_cache
    }

    /// Borrow the pool — exposed so the binary can log warmup
    /// state at startup.
    pub fn pool(&self) -> &BufPool {
        &self.pool
    }

    /// Borrow the memory pool — exposed so the binary can log the
    /// configured budget at startup.
    pub fn memory_pool(&self) -> &Arc<GreedyMemoryPool> {
        &self.memory_pool
    }

    /// Bind a `tonic::transport::Server` on `listen_addr`, register
    /// ourselves as the only Flight service, and serve until
    /// `shutdown` resolves. Mirrors the shape of
    /// [`crate::Server::serve_with_shutdown`] so a future single
    /// binary can drive both ingest and query from one supervisor.
    pub async fn serve_with_shutdown<F>(
        self: Arc<Self>,
        listen_addr: SocketAddr,
        shutdown: F,
    ) -> anyhow::Result<()>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let listener = tokio::net::TcpListener::bind(listen_addr)
            .await
            .map_err(|e| anyhow::anyhow!("binding Flight listener on {listen_addr}: {e}"))?;
        let bound = listener.local_addr().ok();
        info!(
            listen = ?bound.unwrap_or(listen_addr),
            "Flight query service listening"
        );

        // `FlightServiceServer::new(self)` requires `T: FlightService`.
        // We hand it the Arc<Self> through a thin adapter so cloning
        // the service across connections is cheap.
        let svc = FlightServiceServer::new(QueryServiceAdapter(self.clone()));

        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
            .await
            .map_err(|e| anyhow::anyhow!("Flight server terminated with error: {e}"))?;
        Ok(())
    }
}

/// Adapter that lets us implement `FlightService` on an `Arc<QueryService>`
/// without bumping into orphan rules — we can't `impl FlightService for
/// Arc<QueryService>` directly since `Arc` isn't ours. The wrapper
/// holds the `Arc` and delegates to its inner methods.
struct QueryServiceAdapter(Arc<QueryService>);

#[tonic::async_trait]
impl FlightService for QueryServiceAdapter {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented(
            "Handshake is not used by scry-queryd; no auth in v0.3",
        ))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented(
            "ListFlights is not implemented; scry-queryd has no flight registry",
        ))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // Degenerate: one endpoint, no location (client reuses the
        // same channel), ticket = descriptor.cmd. Lets clients that
        // follow the canonical "ask first" pattern keep working, but
        // we don't fan out anywhere yet.
        let desc = request.into_inner();
        let ticket = Ticket {
            ticket: desc.cmd.clone(),
        };
        let endpoint = FlightEndpoint {
            ticket: Some(ticket),
            location: Vec::<Location>::new(),
            expiration_time: None,
            app_metadata: Default::default(),
        };
        // Empty schema bytes — the client will get the schema from
        // the first FlightData message of the resulting DoGet stream.
        let info = FlightInfo {
            schema: Default::default(),
            flight_descriptor: Some(desc),
            endpoint: vec![endpoint],
            total_records: -1,
            total_bytes: -1,
            ordered: false,
            app_metadata: Default::default(),
        };
        Ok(Response::new(info))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        // Could be implemented by running register_metrics_table and
        // returning the table's schema; not used by our CLI client so
        // skipped for v0.3 step 2.
        Err(Status::unimplemented(
            "GetSchema is not implemented; the schema lives in the first DoGet FlightData",
        ))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let service = self.0.clone();
        service.do_get_impl(ticket).await
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented(
            "DoPut is not implemented; scry-queryd is read-only",
        ))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("DoExchange is not implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented(
            "DoAction is not implemented; no custom verbs in v0.3",
        ))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        // No actions to list. Return an empty stream so well-behaved
        // clients see an explicit "zero" rather than an error.
        Ok(Response::new(futures::stream::empty().boxed()))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented(
            "PollFlightInfo is not implemented; no async/long-poll flights in v0.3",
        ))
    }
}

impl QueryService {
    /// Core of `do_get`: decode the ticket, build + execute the query
    /// plan, return a Flight-encoded stream of `RecordBatch`es. Owns
    /// the per-request tracing span — every event from
    /// `register_metrics_table` through `scan_complete` gets the
    /// `request_id` field for log correlation.
    async fn do_get_impl(
        self: Arc<Self>,
        ticket: Ticket,
    ) -> Result<Response<BoxStream<'static, Result<FlightData, Status>>>, Status> {
        // ── Decode request ──────────────────────────────────────────
        let req = QueryRequest::from_ticket_bytes(&ticket.ticket)
            .map_err(|e| Status::invalid_argument(format!("bad ticket: {e}")))?;

        let request_id = req.request_id.clone().unwrap_or_else(|| {
            format!("q-{}", self.next_request_id.fetch_add(1, Ordering::Relaxed))
        });

        // Span is the parent for every event emitted while building
        // and executing the query. Field shapes match existing
        // codebase conventions (% for Display, ? for Debug).
        let span = info_span!(
            "query",
            request_id = %request_id,
            matcher_count = req.metrics_query.matchers.len(),
            ts_min = req.metrics_query.ts_min,
            ts_max = req.metrics_query.ts_max,
            sql = req.sql.as_deref().unwrap_or(""),
            limit = req.limit,
        );

        // Future that produces the streaming response, all under
        // the span so registered_metrics_table_done etc. land in
        // the right trace.
        let fut = async move {
            let pool_start = self.pool.stats();
            let cache_start = self.postings_cache.stats();
            let t0 = Instant::now();

            // ── Build the per-request SessionContext ─────────────────
            //
            // Fresh ctx per request keeps the table list per-query —
            // the `metrics` registration is post-postings, post-time-
            // overlap-narrow, so we don't want one request's narrowed
            // table to bleed into the next. The construction is cheap
            // (~µs); DataFusion's heavy initialization is one-time.
            //
            // Crucially we hand it the *shared* `RuntimeEnv` rather
            // than letting `SessionContext::new()` build a fresh one
            // — that's what couples this query's allocations to the
            // process-wide memory budget (DataFusion enforces the
            // pool only across SessionContexts that share a
            // RuntimeEnv).
            let ctx = SessionContext::new_with_config_rt(
                SessionConfig::new(),
                self.runtime_env.clone(),
            );

            // Lock the catalog *only* to list blocks. The candidates
            // Vec is owned, so the lock guard is dropped before we
            // start any async work — multiple concurrent queries
            // serialize for a single SQLite SELECT each, no more.
            let candidates = {
                let guard = self
                    .catalog
                    .lock()
                    .map_err(|e| Status::internal(format!("catalog mutex poisoned: {e}")))?;
                list_metrics_candidates(&guard, &req.metrics_query)
                    .map_err(|e| Status::internal(format!("list_metrics_candidates: {e:#}")))?
            };
            register_metrics_table_from_candidates(
                &ctx,
                candidates,
                self.store.clone(),
                Some(self.postings_cache.as_ref()),
                &req.metrics_query,
            )
            .await
            .map_err(|e| Status::internal(format!("register_metrics_table: {e:#}")))?;

            let t_reg = t0.elapsed();
            info!(
                elapsed_ms = t_reg.as_millis() as u64,
                "register_metrics_table_done"
            );

            // ── Build the DataFrame (SQL or default SELECT *) ────────
            let df = if let Some(sql) = req.sql.as_deref() {
                ctx.sql(sql)
                    .await
                    .map_err(|e| Status::invalid_argument(format!("SQL parse: {e:#}")))?
            } else {
                let mut df = ctx
                    .table(METRICS_TABLE_NAME)
                    .await
                    .map_err(|e| Status::internal(format!("table lookup: {e:#}")))?;
                if let Some(limit) = req.limit {
                    df = df.limit(0, Some(limit)).map_err(|e| {
                        Status::internal(format!("applying limit: {e:#}"))
                    })?;
                }
                df
            };

            // Hold onto the physical plan so the wrapper stream can
            // pull `MetricsSet` after the row stream is drained.
            let physical = df
                .create_physical_plan()
                .await
                .map_err(|e| Status::internal(format!("create_physical_plan: {e:#}")))?;
            let task_ctx = ctx.task_ctx();
            let t_plan = t0.elapsed();
            info!(
                elapsed_ms = t_plan.as_millis() as u64,
                "physical_plan_done"
            );

            // ── Execute → RecordBatch stream ─────────────────────────
            let df_stream = execute_stream(physical.clone(), task_ctx)
                .map_err(|e| Status::internal(format!("execute_stream: {e:#}")))?;
            let schema: SchemaRef = df_stream.schema();

            // Row counter is incremented by `inspect_ok` before the
            // batch reaches the Flight encoder. Cheap atomic; shared
            // with the completion handler via Arc.
            let rows_seen = Arc::new(AtomicUsize::new(0));
            let rows_seen_for_inspect = rows_seen.clone();

            let counted = df_stream
                .inspect_ok(move |batch| {
                    rows_seen_for_inspect
                        .fetch_add(batch.num_rows(), Ordering::Relaxed);
                })
                .map_err(|e| FlightError::from_external_error(Box::new(e)));

            // Flight-encode. The encoder emits one schema message
            // followed by one or more data messages per batch.
            let encoded = FlightDataEncoderBuilder::new()
                .with_schema(schema)
                .build(counted)
                .map_err(|e: FlightError| Status::internal(format!("flight encode: {e}")));

            // Wrap so that the final `Poll::Ready(None)` triggers the
            // scan_complete tracing event with the actual MetricsSet
            // counters + pool deltas.
            let traced = ScanCompleteStream::new(
                encoded,
                physical,
                self.pool.clone(),
                pool_start,
                self.postings_cache.clone(),
                cache_start,
                self.memory_pool.clone(),
                rows_seen,
                t0,
                Span::current(),
            );

            Ok::<_, Status>(Response::new(traced.boxed()))
        };

        fut.instrument(span).await
    }
}

/// Wraps a Flight-encoded stream so the final `None` emits a
/// `scan_complete` tracing event with the produced
/// [`ExecutionPlan`]'s [`MetricsSet`] counters + the per-request
/// pool stats delta + wall-clock time. The event is what the smoke
/// tests grep for to verify pool warmth between queries.
struct ScanCompleteStream<S> {
    inner: S,
    plan: Arc<dyn ExecutionPlan>,
    pool: BufPool,
    pool_start: PoolStats,
    postings_cache: Arc<PostingsCache>,
    cache_start: PostingsCacheStats,
    /// Process-wide memory pool. Sampled at scan_complete time
    /// (`reserved()` is a snapshot, not a per-query reservation).
    /// Single-query workloads see exactly this query's bookkeeping;
    /// under concurrent queries this is the daemon-wide reserved
    /// count at the moment the row stream ended — same race-y
    /// caveat that applies to `BufPool` deltas.
    memory_pool: Arc<GreedyMemoryPool>,
    rows_seen: Arc<AtomicUsize>,
    t0: Instant,
    span: Span,
    /// Once we've emitted the trailer we transition to a "done"
    /// state and return `Ready(None)` forever after, even if the
    /// stream is polled again (shouldn't happen, but cheap to be
    /// correct about it).
    done: bool,
}

impl<S> ScanCompleteStream<S> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        inner: S,
        plan: Arc<dyn ExecutionPlan>,
        pool: BufPool,
        pool_start: PoolStats,
        postings_cache: Arc<PostingsCache>,
        cache_start: PostingsCacheStats,
        memory_pool: Arc<GreedyMemoryPool>,
        rows_seen: Arc<AtomicUsize>,
        t0: Instant,
        span: Span,
    ) -> Self {
        Self {
            inner,
            plan,
            pool,
            pool_start,
            postings_cache,
            cache_start,
            memory_pool,
            rows_seen,
            t0,
            span,
            done: false,
        }
    }

    fn emit_scan_complete(&self, wall: Duration) {
        let _g = self.span.enter();
        let pool_end = self.pool.stats();
        let pool_delta = pool_end.delta(self.pool_start);
        let cache_end = self.postings_cache.stats();
        let cache_delta = cache_end.delta(self.cache_start);
        let metrics = collect_leaf_metrics(&*self.plan);

        let (row_groups_pruned, row_groups_matched, files_pruned, bytes_scanned) = match metrics {
            Some(m) => summarise_metrics(&m),
            None => (0, 0, 0, 0),
        };

        // Sample at scan_complete time, not per-query peak — process-
        // wide snapshot. Under sequential single-query workloads this
        // is effectively this query's high-water mark (everything else
        // has released by now); under concurrent queries it's noisy
        // but still useful for budget-headroom telemetry.
        let memory_reserved_bytes_end = self.memory_pool.reserved();

        info!(
            total_rows    = self.rows_seen.load(Ordering::Relaxed),
            row_groups_matched,
            row_groups_pruned,
            files_pruned,
            bytes_scanned,
            pool_reuses_delta = pool_delta.reuses,
            pool_allocs_delta = pool_delta.allocs,
            pool_misses_delta = pool_delta.misses,
            pool_grows_delta  = pool_delta.grows,
            pool_capacity     = pool_end.capacity,
            postings_cache_hits_delta         = cache_delta.hits,
            postings_cache_misses_delta       = cache_delta.misses,
            postings_cache_evictions_delta    = cache_delta.evictions,
            postings_cache_fetch_errors_delta = cache_delta.fetch_errors,
            postings_cache_entries            = cache_end.entries,
            postings_cache_bytes_in           = cache_end.bytes_in,
            query_memory_reserved_bytes_end   = memory_reserved_bytes_end,
            pool_in_flight    = pool_end.in_flight,
            wall_ms = wall.as_millis() as u64,
            "scan_complete"
        );
    }
}

impl<S> Stream for ScanCompleteStream<S>
where
    S: Stream<Item = Result<FlightData, Status>> + Unpin,
{
    type Item = Result<FlightData, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
            Poll::Ready(None) => {
                self.done = true;
                let wall = self.t0.elapsed();
                self.emit_scan_complete(wall);
                Poll::Ready(None)
            }
        }
    }
}

/// Walk the plan tree, merging every leaf node's `MetricsSet` into
/// one. We need the *merged* view (not the deepest single node)
/// because step 4 emits one `DataSourceExec` per metrics block under
/// a `UnionExec` — each branch carries its own pruning + bytes-scanned
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

/// Sum up the pruning + bytes counters into the same trailer shape
/// the CLI prints. `(row_groups_pruned, row_groups_matched, files_pruned,
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

// Re-export the error type the encoder produces so consumers wiring
// up custom inspect/map chains can name it.
pub use arrow_flight::error::FlightError as ArrowFlightError;
