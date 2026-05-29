//! `scry-gateway` daemon: an HTTP front-end that terminates OTLP/HTTP trace
//! push and legacy Pyroscope profile ingest and forwards every request to a
//! scry ingest server over one shared binschema wire connection.

use anyhow::{Context, Result};
use clap::Parser;
use scry_client::Client;
use scry_gateway::{router, AppState};
use scry_proto::{
    constants::{SIGNAL_BIT_METRICS, SIGNAL_BIT_PROFILES, SIGNAL_BIT_TRACES},
    LabelPair,
};
use uuid::Uuid;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[command(name = "scry-gateway", about = "OTLP + Pyroscope push gateway for scry")]
struct Args {
    /// HTTP listen address (serves both /v1/traces and /ingest).
    #[arg(long, default_value = "0.0.0.0:4318")]
    listen: String,

    /// Upstream scry ingest server address (binschema wire).
    #[arg(long, default_value = "127.0.0.1:4000")]
    upstream: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();

    let hostname = hostname_string();
    let agent_id = *Uuid::now_v7().as_bytes();

    let client = Client::connect(
        &args.upstream,
        agent_id,
        &hostname,
        SIGNAL_BIT_METRICS | SIGNAL_BIT_TRACES | SIGNAL_BIT_PROFILES,
        vec![LabelPair { key: "service".into(), value: "scry-gateway".into() }],
    )
    .await
    .with_context(|| format!("connecting to upstream ingest server at {}", args.upstream))?;

    let state = AppState::new(client);
    let app = router(state);

    let listener = tokio::net::TcpListener::bind(&args.listen)
        .await
        .with_context(|| format!("binding HTTP listener on {}", args.listen))?;
    tracing::info!(listen = %args.listen, upstream = %args.upstream, "scry-gateway ready");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("HTTP server error")?;

    tracing::info!("scry-gateway shutting down");
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
}

fn hostname_string() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| "scry-gateway".to_string())
}
