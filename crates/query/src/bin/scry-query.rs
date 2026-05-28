//! scry-query — DataFusion-backed metrics querier CLI (v0.3).
//!
//! Opens a SQLite catalog + the configured object store (same
//! `SCRY_OBJSTORE_*` env vars as scry-ingestd + scry-list), pre-resolves
//! AND'd label matchers via the postings sidecars, registers the
//! result as a DataFusion `metrics` table, and either:
//!
//! * Runs `--sql` (if given) against the registered table, or
//! * `SELECT * FROM metrics` for the v0.2-compatible "dump matching
//!   samples" shape.
//!
//! By default the CLI **drains** the result stream without printing
//! rows — at v0.3 the binary's job is "did the scan work, what did
//! it cost?", not "render a million UInt64s onto a terminal".
//! Profile evidence (`flamegraphs/20260527T020354Z-selective-df-v2.svg`)
//! showed 22.5% of wall in `pretty_format_batches`/`comfy_table` for
//! the 1M-row dump case — pure throw-away formatting cost. Pass
//! `--show` to opt into pretty-printing (small result sets only).
//! The per-block scan trailer always prints on stderr.
//!
//! Per-block pruning trailer comes from the produced `ExecutionPlan`'s
//! `MetricsSet`: rows pruned at the row-group level, files read,
//! etc. Same architectural payoff signal the v0.2 CLI surfaced, just
//! sourced from DataFusion's own counters.
//!
//! Run (after `source docker/garage/.env`):
//!
//! ```bash
//! scry-query \
//!     --catalog ./online.sqlite \
//!     --matcher __name__=scry_http_requests_total \
//!     --matcher env=prod
//!
//! scry-query --catalog ./online.sqlite \
//!     --matcher __name__=scry_http_requests_total \
//!     --sql 'SELECT count(*) FROM metrics'
//! ```

use std::io::{BufWriter, Write};
use std::path::PathBuf;

/// Swap glibc's malloc for mimalloc.
///
/// The query hot path's biggest cost on the dump case was kernel
/// page-fault servicing (~30% of wall on the v0.3 DWARF profile —
/// `clear_page_erms` + `do_anonymous_page`). Root cause: every
/// per-range `Vec<u8>` allocated for an HTTP body goes past glibc's
/// ~128 KB `mmap` threshold, so each fetch is a fresh `mmap` → fresh
/// kernel page-zeroing on first write → `munmap` on Drop. The next
/// query repeats the whole dance.
///
/// mimalloc keeps large allocations inside its own segment heaps and
/// reuses pages across allocations within the process, sidestepping
/// the `mmap`/`munmap` churn entirely. Same allocator that
/// `scry-ingestd` uses for the ingest hot path, applied here for the
/// same reason.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use anyhow::{Context, Result};
use arrow::util::pretty::pretty_format_batches;
use arrow_buffer::Buffer;
use arrow_ipc::reader::StreamDecoder;
use clap::Parser;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::{
    collect, display::DisplayableExecutionPlan, execute_stream, ExecutionPlan,
};
use futures::StreamExt;
use scry_catalog::Catalog;
use scry_objstore::{open_with_pool_config, BufPool, BufPoolConfig, ObjStoreConfig};
use scry_proto::{
    constants::{query_err_name, Signal},
    framing::{read_frame, write_frame},
    QueryFrame, QueryFrameMsg,
};
use scry_query::{
    logs::{register_logs_table, LOGS_TABLE_NAME},
    register_metrics_table, Query, QueryRequest, METRICS_TABLE_NAME,
};
use tokio::io::{BufReader as TokioBufReader, BufWriter as TokioBufWriter};
use tokio::net::TcpStream;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to the SQLite catalog file. Required for local mode;
    /// ignored when `--remote` is set (the daemon owns the catalog).
    #[arg(long, conflicts_with = "remote", required_unless_present = "remote")]
    catalog: Option<PathBuf>,

    /// `host:port` of a running `scry-queryd`. When set, the query is
    /// sent over Arrow Flight instead of evaluated locally. The
    /// matcher/from/until/limit/sql flags get serialised into the
    /// request; pool flags are ignored (the daemon owns the pool).
    /// The local trailer collapses to "rows total (via remote)" — the
    /// per-block scan stats live in the daemon's logs.
    #[arg(long)]
    remote: Option<String>,

    /// Equality matcher in `name=value` form. Repeatable; AND'd.
    /// Resolves via the postings sidecar before SQL/DataFrame eval.
    /// Empty list = "scan every series/stream in every overlapping
    /// block".
    #[arg(long = "matcher", value_parser = parse_matcher)]
    matchers: Vec<(String, String)>,

    /// Target signal: `metrics` (default) or `logs`. Drives the
    /// table name (`metrics` vs `logs`) and the per-signal postings
    /// layer. Required when querying logs; metrics is the default
    /// because it's been the only signal until v0.4.
    #[arg(long, value_parser = parse_signal, default_value = "metrics")]
    signal: CliSignal,

    /// Lower bound on `ts_unix_nano` (inclusive). Both catalog-time
    /// (block overlap) and a `>=` row predicate.
    #[arg(long)]
    from: Option<u64>,

    /// Upper bound on `ts_unix_nano` (inclusive).
    #[arg(long)]
    until: Option<u64>,

    /// Row cap. Applied via a `LIMIT` on the implicit `SELECT *`.
    /// Ignored when `--sql` is given (write `LIMIT N` in your SQL).
    #[arg(long)]
    limit: Option<usize>,

    /// Arbitrary SQL against the registered table for the chosen
    /// signal (`metrics` or `logs`). The matcher / time flags
    /// still apply as a *preselect* — the SQL runs against the
    /// already-narrowed table.
    #[arg(long)]
    sql: Option<String>,

    /// Print the produced physical plan to stderr after execution.
    /// Useful for verifying pushdown / pruning behaviour.
    #[arg(long)]
    explain: bool,

    /// Pretty-print result rows to stdout via `comfy_table`. Off by
    /// default — at v0.3 we care that the scan ran + what it cost,
    /// not about painting a million rows onto a TTY. Suitable for
    /// small result sets (an aggregate via `--sql`, or `--limit N`).
    #[arg(long)]
    show: bool,

    // ── Buffer-pool knobs (override env / defaults) ──────────────
    //
    // Each flag, when present, overrides the corresponding
    // `SCRY_OBJSTORE_POOL_*` env var. Absent flag = use env value
    // (or `BufPoolConfig::default()` fallback). For one-shot CLI
    // queries, set `--pool-warmup-count` to the expected per-query
    // fetch concurrency so the first query skips page-fault cost.
    /// Pool buffers to pre-allocate + page-fault at startup. 0 (default)
    /// = cold start (subsequent queries warm naturally, but the *first*
    /// pays full kernel-zero cost for response Vecs).
    #[arg(long)]
    pool_warmup_count: Option<usize>,

    /// Capacity (MiB) of each warmup buffer. Should match the typical
    /// per-fetch coalesced range size for the workload.
    #[arg(long)]
    pool_warmup_size_mib: Option<usize>,

    /// Starting free-list cap.
    #[arg(long)]
    pool_initial_capacity: Option<usize>,

    /// Hard ceiling that autoscale won't cross. Caps pool RSS.
    #[arg(long)]
    pool_max_capacity: Option<usize>,

    /// Autoscale grows capacity by this many slots when peak in-flight
    /// exceeds current capacity. 0 disables autoscale.
    #[arg(long)]
    pool_autoscale_headroom: Option<usize>,
}

fn parse_matcher(raw: &str) -> Result<(String, String), String> {
    let (k, v) = raw
        .split_once('=')
        .ok_or_else(|| format!("matcher `{raw}` must be name=value"))?;
    if k.is_empty() {
        return Err(format!("matcher `{raw}` has empty name"));
    }
    Ok((k.to_string(), v.to_string()))
}

/// Clap-parsed signal selector. Wrapper around [`Signal`] so we can
/// give it a `Display` (used in the trailer) and a parser without
/// touching the proto crate.
#[derive(Debug, Clone, Copy)]
struct CliSignal(Signal);

impl CliSignal {
    fn name(&self) -> &'static str {
        match self.0 {
            Signal::Metrics => "metrics",
            Signal::Logs => "logs",
            Signal::Traces => "traces",
            Signal::Profiles => "profiles",
            Signal::Dummy => "dummy",
        }
    }
    fn table_name(&self) -> &'static str {
        match self.0 {
            Signal::Metrics => METRICS_TABLE_NAME,
            Signal::Logs => LOGS_TABLE_NAME,
            // Traces/profiles/dummy don't have query tables yet —
            // parse_signal rejects them before we ever hit this.
            _ => "<unsupported>",
        }
    }
}

fn parse_signal(raw: &str) -> Result<CliSignal, String> {
    match raw.to_ascii_lowercase().as_str() {
        "metrics" => Ok(CliSignal(Signal::Metrics)),
        "logs" => Ok(CliSignal(Signal::Logs)),
        other => Err(format!(
            "unknown signal `{other}` (supported at v0.4: metrics, logs)"
        )),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();
    let args = Args::parse();

    let q = Query {
        matchers: args.matchers.clone(),
        ts_min: args.from,
        ts_max: args.until,
    };

    // ── Remote mode short-circuit ──────────────────────────────────
    //
    // The daemon owns everything (catalog, store, pool). We just
    // serialize the request, drain the result stream, and print a
    // degenerate trailer. Per-block scan stats live in the daemon's
    // `scan_complete` log event.
    if let Some(addr) = args.remote.as_deref() {
        return run_remote(addr, args.signal, q, args.sql.clone(), args.limit).await;
    }

    let cfg = ObjStoreConfig::from_env()
        .context("loading SCRY_OBJSTORE_* env (try `source docker/garage/.env`)")?;

    // Pool config: env defaults, overridden by any --pool-* CLI flag.
    let mut pool_cfg =
        BufPoolConfig::from_env().context("parsing SCRY_OBJSTORE_POOL_* env vars")?;
    if let Some(v) = args.pool_warmup_count {
        pool_cfg.warmup_count = v;
    }
    if let Some(v) = args.pool_warmup_size_mib {
        pool_cfg.warmup_size = v * 1024 * 1024;
    }
    if let Some(v) = args.pool_initial_capacity {
        pool_cfg.initial_capacity = v;
    }
    if let Some(v) = args.pool_max_capacity {
        pool_cfg.max_capacity = v;
    }
    if let Some(v) = args.pool_autoscale_headroom {
        pool_cfg.autoscale_headroom = v;
    }
    let (store, pool) = open_with_pool_config(&cfg, pool_cfg)?;

    // Local mode requires `--catalog` (enforced via clap's
    // `required_unless_present = "remote"`).
    let catalog_path = args
        .catalog
        .as_ref()
        .expect("clap guarantees catalog is set in local mode");
    let catalog = Catalog::open(catalog_path, &cfg.bucket)
        .with_context(|| format!("opening catalog at {}", catalog_path.display()))?;

    // ── Register the chosen signal's table on a fresh ctx ─────────
    //
    // The postings GETs + per-block fingerprint resolve happen inside
    // the register call; once it returns, the table is fully pre-
    // narrowed and `scan()` is pure CPU. We branch on the requested
    // signal so `scry-query --signal logs ...` registers `logs`
    // instead of `metrics`.
    let ctx = SessionContext::new();
    let table_name = args.signal.table_name();
    match args.signal.0 {
        Signal::Metrics => register_metrics_table(&ctx, &catalog, store, &q).await?,
        Signal::Logs => register_logs_table(&ctx, &catalog, store, &q).await?,
        other => anyhow::bail!("CLI signal {other:?} has no query table yet"),
    }

    // ── Produce a DataFrame ──────────────────────────────────────
    let df = if let Some(sql) = args.sql.as_deref() {
        ctx.sql(sql)
            .await
            .with_context(|| format!("parsing SQL `{sql}`"))?
    } else {
        let mut df = ctx
            .table(table_name)
            .await
            .with_context(|| format!("looking up table {table_name}"))?;
        if let Some(limit) = args.limit {
            df = df.limit(0, Some(limit))?;
        }
        df
    };

    // Build the physical plan ourselves (rather than `df.show()`) so
    // we can hold onto it and pull `MetricsSet` after execution for
    // the per-block trailer.
    let physical = df.create_physical_plan().await?;
    let task_ctx = ctx.task_ctx();

    // ── Execute ───────────────────────────────────────────────────
    //
    // Two paths:
    //   * `--show`: collect everything, pretty-print via comfy_table.
    //     Suitable for small result sets (an aggregate, a `--limit N`).
    //     For 1M-row dumps this path is dominated by comfy_table
    //     formatting (~22% of wall in the v0.3 profile); use `--sql`
    //     with an aggregate or `--limit` instead.
    //   * default: stream-drain. The bytes still get read + filtered
    //     by ParquetSource and the MetricsSet still fills in, but we
    //     never materialise the rows past the batch boundary or
    //     format them. Row count is taken from the streamed batches.
    let total_rows = if args.show {
        let batches = collect(physical.clone(), task_ctx).await?;
        let stdout = std::io::stdout();
        let mut out = BufWriter::new(stdout.lock());
        let table = pretty_format_batches(&batches)?;
        writeln!(out, "{table}")?;
        out.flush()?;
        batches.iter().map(|b| b.num_rows()).sum::<usize>()
    } else {
        let mut stream = execute_stream(physical.clone(), task_ctx)?;
        let mut rows: usize = 0;
        while let Some(batch) = stream.next().await {
            rows += batch?.num_rows();
        }
        rows
    };

    // ── Per-block / pruning trailer (stderr) ──────────────────────
    //
    // DataFusion's `MetricsSet` carries the same kind of evidence
    // the v0.2 CLI synthesised by hand (row groups skipped, files
    // pruned, bytes read). We walk the plan tree once and surface
    // each `DataSourceExec`'s aggregated metrics.
    eprintln!();
    print_plan_metrics(&*physical, total_rows);
    print_pool_stats(&pool);

    if args.explain {
        eprintln!();
        eprintln!(
            "{}",
            DisplayableExecutionPlan::with_metrics(&*physical).indent(true)
        );
    }

    Ok(())
}

/// Send the query to a remote `scry-queryd` over the binschema query
/// protocol, drain the resulting Arrow IPC stream into `RecordBatch`es,
/// and print a degenerate trailer. The per-block scan stats and pool
/// deltas live in the daemon's `scan_complete` log event — we don't
/// have access to them here.
///
/// Wire shape (see `proto/query.schema.json`):
///   client → server: QueryFrame::QueryRequest
///   server → client: QueryFrame::SchemaMsg { ipc_bytes }
///                    QueryFrame::BatchMsg  { ipc_bytes }* (zero or more)
///                    QueryFrame::EndOfStream { total_rows } OR
///                    QueryFrame::StreamError { code, message }
async fn run_remote(
    addr: &str,
    signal: CliSignal,
    query: Query,
    sql: Option<String>,
    limit: Option<usize>,
) -> Result<()> {
    // `addr` is host:port. No URL scheme — this is a raw TCP wire
    // protocol, not HTTP/gRPC. Accept `http://` / `https://` prefixes
    // for ergonomic continuity with the previous Flight-based shape
    // but strip them.
    let host_port = addr
        .strip_prefix("http://")
        .or_else(|| addr.strip_prefix("https://"))
        .unwrap_or(addr);

    let sock = TcpStream::connect(host_port)
        .await
        .with_context(|| format!("connecting to {host_port}"))?;
    let (r, w) = sock.into_split();
    let mut r = TokioBufReader::new(r);
    let mut w = TokioBufWriter::new(w);

    // Send the request frame.
    let req = QueryRequest {
        signal: signal.0 as u8,
        query,
        sql,
        limit,
        request_id: None,
    };
    let request_frame = QueryFrame {
        msg: QueryFrameMsg::QueryRequest(req.to_wire().into()),
    };
    write_frame(&mut w, &request_frame)
        .await
        .context("writing QueryRequest frame")?;
    tokio::io::AsyncWriteExt::flush(&mut w)
        .await
        .context("flushing QueryRequest frame")?;

    // Drain the response stream. StreamDecoder is fed every ipc_bytes
    // payload from SchemaMsg / BatchMsg verbatim; the server's
    // `write_message` calls produced exactly the IPC stream framing
    // StreamDecoder expects (continuation marker + length + flatbuf
    // + body), so no client-side reframing is needed.
    let mut decoder = StreamDecoder::new();
    let mut total_rows: usize = 0;

    let server_total_rows: u64 = loop {
        let frame: QueryFrame = read_frame(&mut r).await.context("reading response frame")?;
        match frame.msg {
            QueryFrameMsg::SchemaMsg(s) => {
                let mut buf = Buffer::from(s.ipc_bytes);
                // Schema messages don't yield a RecordBatch but they
                // do populate `decoder.schema()`. Calling `decode`
                // until the buffer is empty advances the state machine.
                while !buf.is_empty() {
                    let maybe = decoder
                        .decode(&mut buf)
                        .context("decoding schema IPC bytes")?;
                    if let Some(batch) = maybe {
                        total_rows += batch.num_rows();
                    }
                }
            }
            QueryFrameMsg::BatchMsg(b) => {
                let mut buf = Buffer::from(b.ipc_bytes);
                while !buf.is_empty() {
                    let maybe = decoder
                        .decode(&mut buf)
                        .context("decoding batch IPC bytes")?;
                    if let Some(batch) = maybe {
                        total_rows += batch.num_rows();
                    }
                }
            }
            QueryFrameMsg::EndOfStream(end) => break end.total_rows,
            QueryFrameMsg::StreamError(err) => {
                anyhow::bail!(
                    "server returned {} (code={:#06x}): {}",
                    query_err_name(err.code),
                    err.code,
                    err.message,
                );
            }
            QueryFrameMsg::QueryRequest(_) => {
                anyhow::bail!("server sent QueryRequest as response (protocol violation)");
            }
        }
    };

    let signal_name = signal.name();
    eprintln!();
    if server_total_rows as usize != total_rows {
        eprintln!(
            "# scan: {total_rows} {signal_name} rows total (server reported {server_total_rows}; mismatch!) via remote {host_port}"
        );
    } else {
        eprintln!(
            "# scan: {server_total_rows} {signal_name} rows total (via remote {host_port})"
        );
    }
    Ok(())
}

/// Walk the physical plan, finding the deepest node that exposes
/// `MetricsSet` (typically the `DataSourceExec` over our
/// `ParquetSource`) and pretty-print the aggregated counters. Falls
/// back to a row-count-only line if no node exposes metrics.
fn print_plan_metrics(plan: &dyn ExecutionPlan, total_rows: usize) {
    let mut deepest_metrics = None;
    walk_for_metrics(plan, &mut deepest_metrics);

    match deepest_metrics {
        Some(metrics) => {
            // `MetricsSet` aggregates across partitions; we surface
            // the counters that map back to the v0.2 trailer concepts.
            let agg = metrics.aggregate_by_name();
            // `PruningMetrics`-shaped counters expose `pruned/matched`
            // via the variant; `as_usize` deliberately returns 0 for
            // them. `bytes_scanned` is a `Count`, so `as_usize` works.
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
            let (row_groups_pruned, row_groups_matched) = {
                let (p, _) = pruning("row_groups_pruned_statistics");
                let (_, m) = pruning("row_groups_matched_statistics");
                (p, m)
            };
            let (files_pruned, _) = pruning("files_ranges_pruned_statistics");
            let bytes_scanned = agg
                .iter()
                .find(|m| m.value().name() == "bytes_scanned")
                .map(|m| m.value().as_usize())
                .unwrap_or(0);

            eprintln!(
                "# scan: {} rows total | row groups {} matched / {} pruned by stats | \
                 files pruned: {} | bytes scanned: {}",
                total_rows,
                row_groups_matched,
                row_groups_pruned,
                files_pruned,
                bytes_scanned,
            );
        }
        None => {
            eprintln!("# scan: {total_rows} rows total (plan exposed no MetricsSet)");
        }
    }
}

/// Print the buffer-pool counters on stderr. `allocs` is the number
/// of fresh `Vec<u8>` allocations the pool had to make (LIFO scan
/// miss); `reuses` is checkouts that hit a pooled buffer; `misses`
/// is checkins dropped because the pool was full and the returning
/// buffer wasn't bigger than existing members. `peak` is the highest
/// concurrent in-flight count seen, and `cap` is the current free-list
/// cap (may exceed `initial_capacity` after autoscale fires `grows`
/// times). A healthy steady state is "small `allocs` once at warmup,
/// then `reuses` dominates and `peak ≤ cap`."
fn print_pool_stats(pool: &BufPool) {
    eprintln!(
        "# pool: {} reuses / {} allocs / {} drops | parked: {} | peak in-flight: {} | cap: {} (grew {}× toward max {})",
        pool.reuses(),
        pool.allocs(),
        pool.misses(),
        pool.free_count(),
        pool.peak_in_flight(),
        pool.capacity(),
        pool.grows(),
        pool.max_capacity(),
    );
}

/// Walk the plan tree, merging every leaf node's `MetricsSet` into
/// `out`. v0.3 step 4 changed `MetricsTable::scan` to emit one
/// `DataSourceExec` per block under a `UnionExec` — N branches each
/// carry their own pruning + bytes-scanned counters, and we want the
/// sum across all of them. `MetricsSet::aggregate_by_name` (the
/// caller of this fn) then collapses same-named metrics from
/// different leaves into one summed row.
fn walk_for_metrics(
    plan: &dyn ExecutionPlan,
    out: &mut Option<datafusion::physical_plan::metrics::MetricsSet>,
) {
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
        for child in children {
            walk_for_metrics(child.as_ref(), out);
        }
    }
}
