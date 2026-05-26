//! noise-sink — thin CLI shell around `scry-server`.
//!
//! Parses flags, optionally constructs a `DummyPipeline` (WAL + parquet
//! builder + optional online catalog targeting object storage), then
//! hands everything to `scry-server::Server::serve_with_shutdown`.
//! Ctrl-C triggers a graceful flush of the in-progress block.
//!
//! Run (no storage):
//!   noise-sink --listen 127.0.0.1:4000
//!
//! Run (v0.1 storage path):
//!   source docker/garage/.env
//!   noise-sink --listen 127.0.0.1:4000 --storage --wal-dir ./wal

use anyhow::{Context, Result};
use clap::Parser;
use scry_catalog::Catalog;
use scry_objstore::{open as open_objstore, ObjStoreConfig};
use scry_server::{DummyPipeline, Server, ServerConfig};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::Mutex;
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

    let server = Server::new(
        ServerConfig {
            listen_addr: args.listen,
            writer_id,
            writer_uuid,
        },
        pipeline,
    );

    server
        .serve_with_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
}

fn rand_short() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ns = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
    format!("{:08x}", ns & 0xFFFF_FFFF)
}
