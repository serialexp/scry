//! Measurement harness: what does one compaction partition actually cost?
//!
//! Not a correctness test — a stopwatch. It exists because compaction was
//! observed reclaiming ~7 blocks per 17 minutes against a 346k-block bucket,
//! with the merge itself taking only 1.6–45 s. Something outside the merge was
//! eating ~16 of every 17 minutes, and guessing at it from logs was not
//! working.
//!
//! `run_compaction_pass` does three things per planned partition: a
//! `reconcile_partition`, a full `catalog.list_blocks()`, and the merge. This
//! harness times the first two in isolation against a **real** object store,
//! because the suspect is GET latency and an `InMemory` store would answer a
//! different question.
//!
//! The thing being pinned down: `reconcile_partition` calls `fetch_and_apply`
//! with `known: None` — deliberately unfiltered, and commented as such, since
//! it runs under a partition lease to establish authoritative truth before a
//! merge commits. So unlike the full walk (D-066), it GETs **every** sidecar in
//! the `(signal, date)` prefix on every pass, whether or not the catalog
//! already has it. The cost is therefore O(blocks in the partition) GETs per
//! merge, and it does not fall as the catalog converges.
//!
//! Ignored by default — needs Garage and takes minutes. Run it with:
//!
//! ```text
//! source docker/garage/.env
//! cargo test -p scry-cluster --test partition_cost -- --ignored --nocapture
//! ```
//!
//! `N_BLOCKS` scales the partition; the interesting regime is "large enough
//! that per-GET latency dominates", not realism of block contents.

use std::sync::Arc;
use std::time::{Duration, Instant};

use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use scry_block::{block_path, BlockMeta};
use scry_catalog::{date_dir, Catalog, CatalogHandle};
use scry_cluster::{full_walk, reconcile_partition};
use tempfile::TempDir;
use uuid::Uuid;

/// How many sidecars to plant in the single partition under test. Override
/// with `N_BLOCKS=20000` to watch the reconcile cost grow linearly while the
/// full walk over the same objects stays flat.
fn n_blocks() -> usize {
    std::env::var("N_BLOCKS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000)
}

/// Fixed instant inside one UTC day, so every planted block lands in the same
/// `(signal, date)` prefix — which is the unit `reconcile_partition` works on.
const TS_BASE: u64 = 1_756_000_000 * 1_000_000_000;

fn meta_for(writer: Uuid, ts: u64) -> BlockMeta {
    BlockMeta {
        uuid: Uuid::now_v7(),
        signal: "logs".to_string(),
        writer_id: writer,
        ts_min_unix_nano: ts,
        ts_max_unix_nano: ts + 1_000,
        row_count: 1000,
        byte_size: 10_000,
        schema_version: 1,
        level: 0,
        producer_version: "measure".to_string(),
        label_fingerprint_bloom: None,
        has_postings: false,
        postings_size_bytes: None,
        series_types: None,
        all_fingerprints: None,
        has_body_bloom: false,
        body_bloom_size_bytes: None,
        wal_seg_max: None,
        wal_shard: None,
        compacted_from: Vec::new(),
    }
}

fn rate(n: usize, d: Duration) -> String {
    if d.as_secs_f64() <= 0.0 {
        return "-".to_string();
    }
    format!("{:.0}/s", n as f64 / d.as_secs_f64())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Garage; minutes-long measurement, not an assertion"]
async fn measure_compaction_partition_overhead() {
    let cfg = scry_objstore::ObjStoreConfig::from_env()
        .expect("SCRY_OBJSTORE_* must be set (source docker/garage/.env)");
    let bucket = cfg.bucket.clone();
    let store: Arc<dyn ObjectStore> = scry_objstore::open(&cfg).await.expect("open object store");

    // A unique writer per run keeps repeated runs from colliding in the bucket
    // without needing to clean up between them.
    let writer = Uuid::now_v7();

    let n_blocks = n_blocks();
    println!("\n=== planting {n_blocks} sidecars in one (logs, date) partition ===");
    let plant_start = Instant::now();
    let mut planted = Vec::with_capacity(n_blocks);
    // Upload concurrently — the *setup* cost is not what we are measuring, and
    // doing it sequentially would make the harness itself take the same shape
    // as the pathology under investigation.
    let mut puts = futures::stream::iter((0..n_blocks).map(|i| {
        let store = store.clone();
        let meta = meta_for(writer, TS_BASE + i as u64 * 1_000_000);
        async move {
            let key = block_path(
                &meta.signal,
                meta.ts_min_unix_nano,
                meta.writer_id,
                meta.uuid,
                "meta.json",
            );
            let body = serde_json::to_vec(&meta).unwrap();
            store
                .put(&key.clone().into(), PutPayload::from(body))
                .await
                .expect("put sidecar");
            meta.uuid
        }
    }))
    .buffer_unordered(32);
    use futures::StreamExt;
    while let Some(uuid) = puts.next().await {
        planted.push(uuid);
    }
    let plant = plant_start.elapsed();
    println!(
        "planted {} in {:.1}s ({})",
        planted.len(),
        plant.as_secs_f64(),
        rate(planted.len(), plant)
    );

    let tmp = TempDir::new().unwrap();
    let handle = Catalog::open(&tmp.path().join("cat.sqlite"), &bucket).unwrap();

    let date = date_dir(TS_BASE);

    // ---- pass 1: cold. Nothing in the catalog, so every sidecar is new. ----
    println!("\n=== reconcile_partition, COLD catalog ===");
    let t = Instant::now();
    let r1 = reconcile_partition(
        store.as_ref(),
        &handle,
        &bucket,
        "logs",
        &date,
        Duration::from_secs(0),
    )
    .await
    .expect("cold reconcile");
    let cold = t.elapsed();
    println!(
        "seen={} inserted={} skipped={} in {:.1}s ({})",
        r1.seen,
        r1.inserted,
        r1.skipped,
        cold.as_secs_f64(),
        rate(r1.seen, cold)
    );

    // ---- pass 2: warm. The catalog now knows every block. -------------------
    // This is the measurement that matters. If reconcile filtered on known
    // UUIDs the way the full walk does, this would be ~free. It does not, so
    // it should cost about the same as the cold pass — meaning a partition
    // pays this on *every* compaction, forever, and the cost grows with the
    // partition rather than with the work being done.
    println!("\n=== reconcile_partition, WARM catalog (same partition again) ===");
    let t = Instant::now();
    let r2 = reconcile_partition(
        store.as_ref(),
        &handle,
        &bucket,
        "logs",
        &date,
        Duration::from_secs(0),
    )
    .await
    .expect("warm reconcile");
    let warm = t.elapsed();
    println!(
        "seen={} inserted={} skipped={} in {:.1}s ({})",
        r2.seen,
        r2.inserted,
        r2.skipped,
        warm.as_secs_f64(),
        rate(r2.seen, warm)
    );

    // ---- contrast: the full walk over the same objects, warm. --------------
    // D-066 taught this path to skip what the catalog already has, so it should
    // be dramatically cheaper than the warm reconcile above despite covering
    // the same (and more) objects. The gap between these two numbers is the
    // finding.
    println!("\n=== full_walk, WARM catalog (the D-066 path, for contrast) ===");
    let t = Instant::now();
    let r3 = full_walk(store.as_ref(), &handle, &bucket)
        .await
        .expect("warm full walk");
    let walk = t.elapsed();
    println!(
        "seen={} inserted={} skipped={} in {:.1}s ({})",
        r3.seen,
        r3.inserted,
        r3.skipped,
        walk.as_secs_f64(),
        rate(r3.seen, walk)
    );

    // ---- the other suspect: list_blocks() per partition. -------------------
    println!("\n=== catalog.list_blocks() (run once per partition in the pass) ===");
    let t = Instant::now();
    let n = handle.with(|c| c.list_blocks()).unwrap().len();
    let lb = t.elapsed();
    println!("{n} live blocks in {:.1}ms", lb.as_secs_f64() * 1000.0);

    println!("\n=== summary ===");
    println!("  reconcile cold      : {:>8.1}s", cold.as_secs_f64());
    println!(
        "  reconcile warm      : {:>8.1}s   <-- paid on every merge",
        warm.as_secs_f64()
    );
    println!(
        "  full walk warm      : {:>8.1}s   <-- D-066 fixed this one",
        walk.as_secs_f64()
    );
    println!(
        "  list_blocks         : {:>8.1}ms",
        lb.as_secs_f64() * 1000.0
    );
    println!(
        "  warm reconcile is {:.0}x the full walk over the same objects",
        warm.as_secs_f64() / walk.as_secs_f64().max(0.000_001)
    );
}

/// The *other* suspect, settled on its own: `run_compaction_pass` calls
/// `catalog.list_blocks()` once per planned partition, and gothab's catalog has
/// ~346k rows. `list_blocks` is a full scan with `ORDER BY date, ts_min, uuid`,
/// so the question is whether that scan is minutes or milliseconds at that
/// size.
///
/// No object store needed — this is pure SQLite, so it runs anywhere.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "minutes to build the catalog; measurement, not an assertion"]
async fn measure_list_blocks_at_production_scale() {
    // Production scale as observed on gothab, plus smaller points so the growth
    // is visible rather than asserted.
    const SIZES: [usize; 4] = [10_000, 50_000, 150_000, 350_000];

    for n in SIZES {
        let tmp = TempDir::new().unwrap();
        let catalog = Catalog::open(&tmp.path().join("cat.sqlite"), "measure-bucket").unwrap();
        let writer = Uuid::now_v7();

        let build = Instant::now();
        for i in 0..n {
            // Spread across ~90 date partitions, matching a 90-day TTL, so the
            // ORDER BY has realistic cardinality to sort rather than one value.
            let day = (i % 90) as u64;
            let ts = TS_BASE + day * 86_400 * 1_000_000_000 + (i as u64 % 1000);
            let meta = meta_for(writer, ts);
            catalog.insert_block(&meta).unwrap();
        }
        let built = build.elapsed();

        let t = Instant::now();
        let got = catalog.list_blocks().unwrap().len();
        let listed = t.elapsed();

        println!(
            "list_blocks over {n:>7} rows: {:>8.1}ms  (returned {got}, built in {:.1}s)",
            listed.as_secs_f64() * 1000.0,
            built.as_secs_f64()
        );
    }
}
