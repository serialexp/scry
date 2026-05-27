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

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use arrow_ipc::writer::{write_message, DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use datafusion::common::DataFusionError;
use datafusion::execution::context::SessionContext;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::SessionConfig;
use futures::StreamExt;
use object_store::ObjectStore;
use scry_catalog::Catalog;
use scry_objstore::{BufPool, PoolStats};
use scry_proto::{
    constants::{
        QUERY_ERR_BAD_REQUEST, QUERY_ERR_INTERNAL, QUERY_ERR_PLAN, QUERY_ERR_RESOURCES,
        QUERY_ERR_SQL_PARSE,
    },
    framing::{read_frame, write_frame},
    BatchMsgInput, EndOfStreamInput, QueryFrame, QueryFrameMsg, SchemaMsgInput, StreamErrorInput,
};
use scry_query::{
    list_metrics_candidates, register_metrics_table_from_candidates, PostingsCache,
    PostingsCacheStats, QueryRequest, METRICS_TABLE_NAME,
};
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, info_span, warn, Instrument, Span};

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

    /// Borrow the pool — exposed so the binary can log warmup state.
    pub fn pool(&self) -> &BufPool {
        &self.pool
    }

    /// Borrow the memory pool — exposed so the binary can log the
    /// configured budget at startup.
    pub fn memory_pool(&self) -> &Arc<GreedyMemoryPool> {
        &self.memory_pool
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
                tokio::spawn(async move {
                    if let Err(e) = svc.handle_connection(sock, peer).await {
                        warn!(%peer, error = %e, "connection ended with error");
                    }
                });
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
    async fn handle_connection(
        self: Arc<Self>,
        sock: TcpStream,
        peer: SocketAddr,
    ) -> Result<()> {
        sock.set_nodelay(true)?;
        let (rd, wr) = sock.into_split();
        let mut rd = BufReader::new(rd);
        let mut wr = BufWriter::new(wr);

        // ── Read the request frame ───────────────────────────────────
        let req_frame: QueryFrame = match read_frame(&mut rd).await {
            Ok(f) => f,
            Err(e) => {
                warn!(%peer, error = %e, "no QueryRequest frame from client");
                return Ok(());
            }
        };
        let wire_req = match req_frame.msg {
            QueryFrameMsg::QueryRequest(q) => q,
            other => {
                let name = match other {
                    QueryFrameMsg::SchemaMsg(_) => "SchemaMsg",
                    QueryFrameMsg::BatchMsg(_) => "BatchMsg",
                    QueryFrameMsg::EndOfStream(_) => "EndOfStream",
                    QueryFrameMsg::StreamError(_) => "StreamError",
                    QueryFrameMsg::QueryRequest(_) => unreachable!(),
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
        let req = QueryRequest::from_wire(wire_req);

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

        // All the actual query work runs under the span so the events
        // it emits (`register_metrics_table_done` etc.) land in the
        // right trace.
        let svc = self.clone();
        async move { svc.run_query(req, wr).await }
            .instrument(span)
            .await
    }

    /// The actual query execution. Split out from `handle_connection`
    /// so the request decode + span setup is one orderly block, and
    /// this fn can return early via `?` on framing errors without
    /// losing the `scan_complete` emission.
    async fn run_query<W>(
        self: Arc<Self>,
        req: QueryRequest,
        mut wr: BufWriter<W>,
    ) -> Result<()>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        let pool_start = self.pool.stats();
        let cache_start = self.postings_cache.stats();
        let t0 = Instant::now();
        // Pre-allocate row counter so we can pass it to emit_scan_complete
        // on every exit path (success + each error path).
        let mut rows_total: u64 = 0;

        // ── Build the per-request SessionContext ─────────────────────
        //
        // Fresh ctx per request keeps the table list per-query. The
        // `metrics` registration is post-postings, post-time-overlap-
        // narrow, so we don't want one request's narrowed table to
        // bleed into the next. The construction is cheap (~µs);
        // DataFusion's heavy initialisation is one-time. Sharing the
        // RuntimeEnv across contexts is the only way DataFusion
        // enforces the memory budget across queries.
        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::new(),
            self.runtime_env.clone(),
        );

        // ── Catalog lookup (sync) ────────────────────────────────────
        //
        // Lock the catalog only to list candidate blocks. The
        // candidates Vec is owned, so the guard drops before any
        // async work — multiple concurrent queries serialise on a
        // single SELECT each, no more.
        // Compute candidates synchronously and drop the guard before
        // any .await — MutexGuard isn't Send, so we can't hold it
        // across an await point in a tokio::spawn-ed future.
        let candidates_result: Result<Vec<_>, String> = match self.catalog.lock() {
            Ok(guard) => list_metrics_candidates(&guard, &req.metrics_query)
                .map_err(|e| format!("list_metrics_candidates: {e:#}")),
            Err(e) => Err(format!("catalog mutex poisoned: {e}")),
        };
        let candidates = match candidates_result {
            Ok(v) => v,
            Err(msg) => {
                let _ = emit_stream_error(&mut wr, QUERY_ERR_INTERNAL, msg).await;
                let _ = wr.flush().await;
                self.emit_scan_complete(None, rows_total, pool_start, cache_start, t0.elapsed());
                return Ok(());
            }
        };

        if let Err(e) = register_metrics_table_from_candidates(
            &ctx,
            candidates,
            self.store.clone(),
            Some(self.postings_cache.as_ref()),
            &req.metrics_query,
        )
        .await
        {
            let _ = emit_stream_error(
                &mut wr,
                QUERY_ERR_INTERNAL,
                format!("register_metrics_table: {e:#}"),
            )
            .await;
            let _ = wr.flush().await;
            self.emit_scan_complete(None, rows_total, pool_start, cache_start, t0.elapsed());
            return Ok(());
        }

        let t_reg = t0.elapsed();
        info!(
            elapsed_ms = t_reg.as_millis() as u64,
            "register_metrics_table_done"
        );

        // ── Build the DataFrame (SQL or default SELECT *) ────────────
        let df_res = if let Some(sql) = req.sql.as_deref() {
            match ctx.sql(sql).await {
                Ok(df) => Ok(df),
                Err(e) => Err((QUERY_ERR_SQL_PARSE, format!("SQL parse: {e:#}"))),
            }
        } else {
            match ctx.table(METRICS_TABLE_NAME).await {
                Ok(mut df) => {
                    if let Some(limit) = req.limit {
                        match df.limit(0, Some(limit)) {
                            Ok(d) => df = d,
                            Err(e) => {
                                df = match Err::<_, (u16, String)>((
                                    QUERY_ERR_PLAN,
                                    format!("applying limit: {e:#}"),
                                )) {
                                    Err((code, msg)) => {
                                        let _ =
                                            emit_stream_error(&mut wr, code, msg).await;
                                        let _ = wr.flush().await;
                                        self.emit_scan_complete(
                                            None,
                                            rows_total,
                                            pool_start,
                                            cache_start,
                                            t0.elapsed(),
                                        );
                                        return Ok(());
                                    }
                                    Ok(d) => d,
                                };
                            }
                        }
                    }
                    Ok(df)
                }
                Err(e) => Err((QUERY_ERR_INTERNAL, format!("table lookup: {e:#}"))),
            }
        };
        let df = match df_res {
            Ok(df) => df,
            Err((code, msg)) => {
                let _ = emit_stream_error(&mut wr, code, msg).await;
                let _ = wr.flush().await;
                self.emit_scan_complete(None, rows_total, pool_start, cache_start, t0.elapsed());
                return Ok(());
            }
        };

        let physical = match df.create_physical_plan().await {
            Ok(p) => p,
            Err(e) => {
                let _ = emit_stream_error(
                    &mut wr,
                    QUERY_ERR_PLAN,
                    format!("create_physical_plan: {e:#}"),
                )
                .await;
                let _ = wr.flush().await;
                self.emit_scan_complete(None, rows_total, pool_start, cache_start, t0.elapsed());
                return Ok(());
            }
        };
        let task_ctx = ctx.task_ctx();
        let t_plan = t0.elapsed();
        info!(
            elapsed_ms = t_plan.as_millis() as u64,
            "physical_plan_done"
        );

        // ── Execute → RecordBatch stream → IPC encode → wire ────────
        let mut stream = match execute_stream(physical.clone(), task_ctx) {
            Ok(s) => s,
            Err(e) => {
                let _ = emit_stream_error(
                    &mut wr,
                    QUERY_ERR_INTERNAL,
                    format!("execute_stream: {e:#}"),
                )
                .await;
                let _ = wr.flush().await;
                self.emit_scan_complete(
                    Some(physical.as_ref()),
                    rows_total,
                    pool_start,
                    cache_start,
                    t0.elapsed(),
                );
                return Ok(());
            }
        };
        let schema = stream.schema();

        let data_gen = IpcDataGenerator::default();
        let mut dict_tracker = DictionaryTracker::new(false);
        let options = IpcWriteOptions::default();

        // Schema message: one SchemaMsg before any BatchMsg.
        let schema_enc =
            data_gen.schema_to_bytes_with_dictionary_tracker(&schema, &mut dict_tracker, &options);
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
                Some(physical.as_ref()),
                rows_total,
                pool_start,
                cache_start,
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
        if let Err(e) = write_frame(&mut wr, &schema_frame).await {
            warn!(error = %e, "client disconnected while writing SchemaMsg");
            self.emit_scan_complete(
                Some(physical.as_ref()),
                rows_total,
                pool_start,
                cache_start,
                t0.elapsed(),
            );
            return Ok(());
        }

        // Stream batches.
        while let Some(batch_res) = stream.next().await {
            let batch = match batch_res {
                Ok(b) => b,
                Err(e) => {
                    let code = if matches!(
                        e.find_root(),
                        DataFusionError::ResourcesExhausted(_)
                    ) {
                        QUERY_ERR_RESOURCES
                    } else {
                        QUERY_ERR_INTERNAL
                    };
                    let _ = emit_stream_error(&mut wr, code, format!("DataFusion: {e}")).await;
                    let _ = wr.flush().await;
                    self.emit_scan_complete(
                        Some(physical.as_ref()),
                        rows_total,
                        pool_start,
                        cache_start,
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
            let (dict_batches, batch_enc) = match data_gen
                .encoded_batch(&batch, &mut dict_tracker, &options)
            {
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
                        Some(physical.as_ref()),
                        rows_total,
                        pool_start,
                        cache_start,
                        t0.elapsed(),
                    );
                    return Ok(());
                }
            };

            for d in dict_batches {
                if let Err(e) = write_one_batch(&mut wr, d, &options).await {
                    warn!(error = %e, "client disconnected while writing BatchMsg (dict)");
                    self.emit_scan_complete(
                        Some(physical.as_ref()),
                        rows_total,
                        pool_start,
                        cache_start,
                        t0.elapsed(),
                    );
                    return Ok(());
                }
            }
            if let Err(e) = write_one_batch(&mut wr, batch_enc, &options).await {
                warn!(error = %e, "client disconnected while writing BatchMsg");
                self.emit_scan_complete(
                    Some(physical.as_ref()),
                    rows_total,
                    pool_start,
                    cache_start,
                    t0.elapsed(),
                );
                return Ok(());
            }

            rows_total = rows_total.saturating_add(batch.num_rows() as u64);
        }

        // Normal completion: EndOfStream terminator.
        let end_frame = QueryFrame {
            msg: QueryFrameMsg::EndOfStream(
                EndOfStreamInput {
                    total_rows: rows_total,
                }
                .into(),
            ),
        };
        if let Err(e) = write_frame(&mut wr, &end_frame).await {
            warn!(error = %e, "client disconnected while writing EndOfStream");
        }
        let _ = wr.flush().await;

        self.emit_scan_complete(
            Some(physical.as_ref()),
            rows_total,
            pool_start,
            cache_start,
            t0.elapsed(),
        );
        Ok(())
    }

    /// Emit the per-query `scan_complete` event with the same field
    /// shape as the pre-step-5 implementation, so existing log-parsing
    /// (smoke tests, dashboards) keeps working.
    fn emit_scan_complete(
        &self,
        plan: Option<&dyn ExecutionPlan>,
        rows_total: u64,
        pool_start: PoolStats,
        cache_start: PostingsCacheStats,
        wall: Duration,
    ) {
        let pool_end = self.pool.stats();
        let pool_delta = pool_end.delta(pool_start);
        let cache_end = self.postings_cache.stats();
        let cache_delta = cache_end.delta(cache_start);
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
            total_rows = rows_total,
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
        // The Span wrapping the call site carries `request_id`; no
        // need to log it explicitly here.
        let _ = Span::current();
    }
}

/// Frame one IPC-encoded message as a [`QueryFrameMsg::BatchMsg`].
/// Encapsulated so the dictionary / record-batch arms in
/// [`QueryService::run_query`] don't repeat the same five lines.
async fn write_one_batch<W>(
    wr: &mut BufWriter<W>,
    enc: arrow_ipc::writer::EncodedData,
    options: &IpcWriteOptions,
) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut bytes = Vec::new();
    write_message(&mut bytes, enc, options)
        .map_err(|e| anyhow::anyhow!("write_message: {e}"))?;
    let frame = QueryFrame {
        msg: QueryFrameMsg::BatchMsg(BatchMsgInput { ipc_bytes: bytes }.into()),
    };
    write_frame(wr, &frame)
        .await
        .map_err(|e| anyhow::anyhow!("write_frame: {e}"))?;
    Ok(())
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
