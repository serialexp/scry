//! `scry` — the single-binary entry point for every operator role.
//!
//! scry is an opinionated single-binary replacement for the Grafana
//! observability stack. This binary is the one you deploy; the
//! subcommand selects the role:
//!
//! - `scry ingest` — ingest server daemon (native wire; optional storage + multi-instance).
//! - `scry query` — long-running query daemon (binschema QueryFrame wire over TCP).
//! - `scry get` — one-shot query CLI (locally or against a `scry query` daemon).
//! - `scry list` — catalog inspector / bucket reconciler.
//! - `scry agent` — Kubernetes log-collection + Prometheus-scrape agent.
//! - `scry gateway` — foreign-protocol fan-out hub (OTLP / Pyroscope / remote-write → sinks).
//! - `scry web` — browser query UI + byte-pipe relay to `scry query`.
//! - `scry compact` — size-tiered block compaction (one-shot / watch).
//! - `scry retention` — per-signal TTL retention (dry-run by default).
//! - `scry tail` — live log tailing (best-effort, straight off the ingest hot path).
//!
//! Each role's flags and behaviour are identical to what the former
//! per-binary tools exposed; this binary only adds the dispatch layer,
//! a single tracing init, and the single process-global allocator.

use anyhow::Result;
use clap::{Parser, Subcommand};

/// Swap glibc's malloc for mimalloc, process-wide.
///
/// A binary may declare exactly one `#[global_allocator]`, so the
/// allocator that the ingest/agent daemons used to declare individually
/// lives here, once, covering every role. The ingest hot path makes
/// ~2 M small allocations/sec in steady state; mimalloc decommits
/// aggressively and runs the small-allocation path faster, keeping RSS
/// smaller and less ragged. No behavioural change.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[command(
    name = "scry",
    version,
    about = "single-binary observability — metrics, logs, traces, profiles",
    long_about = None,
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Ingest server daemon (native wire; optional storage + multi-instance).
    Ingest(scry_ingestd::Args),
    /// Query daemon (binschema QueryFrame wire over TCP).
    Query(scry_queryd::Args),
    /// One-shot query CLI (locally or against a `scry query` daemon).
    Get(scry_query::cli::Args),
    /// Catalog inspector / bucket reconciler.
    List(scry_list::Args),
    /// Kubernetes log-collection + Prometheus-scrape agent.
    Agent(scry_agent::Args),
    /// Foreign-protocol fan-out hub (OTLP / Pyroscope / remote-write → sinks).
    Gateway(scry_gateway::cli::Args),
    /// Browser query UI + byte-pipe relay to `scry query`.
    Web(scry_webui::Args),
    /// Size-tiered block compaction (one-shot / watch).
    Compact(scry_compact::Args),
    /// Per-signal TTL retention (dry-run by default).
    Retention(scry_retention::Args),
    /// Live log tailing (best-effort, straight off the ingest hot path).
    Tail(scry_tail::Args),
}

#[tokio::main]
async fn main() -> Result<()> {
    // Single tracing init for the whole binary (was duplicated in every
    // per-role main). RUST_LOG selects filters; default INFO.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    match Cli::parse().cmd {
        Cmd::Ingest(a) => scry_ingestd::run(a).await,
        Cmd::Query(a) => scry_queryd::run(a).await,
        Cmd::Get(a) => scry_query::cli::run(a).await,
        Cmd::List(a) => scry_list::run(a).await,
        Cmd::Agent(a) => scry_agent::run(a).await,
        Cmd::Gateway(a) => scry_gateway::cli::run(a).await,
        Cmd::Web(a) => scry_webui::run(a).await,
        Cmd::Compact(a) => scry_compact::run(a).await,
        Cmd::Retention(a) => scry_retention::run(a).await,
        Cmd::Tail(a) => scry_tail::run(a).await,
    }
}
