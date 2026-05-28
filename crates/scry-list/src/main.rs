//! scry-list — v0.1 catalog inspector.
//!
//! Opens a SQLite catalog (creating an empty one if it doesn't yet
//! exist), reconciles it against the bucket via
//! [`scry_catalog::Catalog::reconcile_from_bucket`], and prints one
//! line per known block:
//!
//! ```text
//!   <uuid>  <date>  rows=<n>  bytes=<n>  ts_min..ts_max  writer=<short>
//! ```
//!
//! The point of the exercise is the v0.1 exit criterion: "drop into
//! an empty catalog dir, point at the bucket, recover exactly what's
//! there." If the catalog row count matches the sum of writes the
//! sink performed, the storage layer round-trips.
//!
//! Run (after `source docker/garage/.env`):
//!
//! ```bash
//! scry-list --catalog ./catalog.sqlite
//! ```

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use scry_catalog::Catalog;
use scry_objstore::{open as open_objstore, ObjStoreConfig};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to the SQLite catalog file. Created (with schema) if it
    /// doesn't exist.
    #[arg(long)]
    catalog: PathBuf,

    /// Skip the bucket walk and just print what's already in the
    /// catalog. Useful for sanity-checking the online insert path
    /// (scry-ingestd's [`scry_catalog::Catalog::insert_block`] calls)
    /// without depending on the bucket.
    #[arg(long)]
    no_reconcile: bool,
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

    let cfg = ObjStoreConfig::from_env()
        .context("loading SCRY_OBJSTORE_* env (try `source docker/garage/.env`)")?;
    let bucket = cfg.bucket.clone();

    let catalog = Catalog::open(&args.catalog, &bucket)
        .with_context(|| format!("opening catalog at {}", args.catalog.display()))?;

    if !args.no_reconcile {
        let store = open_objstore(&cfg)?;
        let report = catalog.reconcile_from_bucket(store.as_ref()).await?;
        eprintln!(
            "reconcile: seen={} inserted={} already_present={} failed={}",
            report.seen, report.inserted, report.already_present, report.failed
        );
    }

    let blocks = catalog.list_blocks()?;
    println!("# {} block(s) in catalog (bucket={})", blocks.len(), bucket);
    let mut total_rows: u64 = 0;
    let mut total_bytes: u64 = 0;
    for b in &blocks {
        total_rows += b.meta.row_count;
        total_bytes += b.meta.byte_size;
        // writer_id short = first 8 hex chars
        let writer_short = b
            .meta
            .writer_id
            .as_simple()
            .to_string()
            .chars()
            .take(8)
            .collect::<String>();
        println!(
            "{}  {}  rows={:>9}  bytes={:>11}  ts={}..{}  signal={}  writer={}",
            b.meta.uuid,
            b.date,
            b.meta.row_count,
            b.meta.byte_size,
            b.meta.ts_min_unix_nano,
            b.meta.ts_max_unix_nano,
            b.meta.signal,
            writer_short,
        );
    }
    println!("# total rows={} bytes={}", total_rows, total_bytes);
    Ok(())
}
