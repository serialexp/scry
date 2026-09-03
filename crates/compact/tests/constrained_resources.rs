//! Focused regression tests for compaction under constrained resource envelopes.

use std::{sync::Arc, time::Duration};

use futures::StreamExt;
use object_store::{memory::InMemory, ObjectStore};
use scry_block::{BlockBuilder, BlockBuilderConfig, LogsBlockBuilder};
use scry_catalog::Catalog;
use scry_compact::{compact_once, CompactConfig, CompactResources, ResourceConfig};
use scry_proto::streaming::LogsAppender;
use tempfile::TempDir;
use uuid::Uuid;

const MIB: u64 = 1024 * 1024;
const BUCKET: &str = "test";

fn block_cfg() -> BlockBuilderConfig {
    BlockBuilderConfig {
        max_rows: 1_000_000,
        target_bytes: 128 * 1024 * 1024,
        row_group_size: 1_000,
        ..Default::default()
    }
}

fn compact_cfg() -> CompactConfig {
    CompactConfig {
        fanout: 2,
        max_level: 1,
        grace: Duration::ZERO,
        signal_filter: Some("logs".into()),
        parallelism: 1,
    }
}

fn constrained(timeout: Duration) -> Arc<CompactResources> {
    CompactResources::new(ResourceConfig {
        envelope_bytes: 128 * MIB,
        datafusion_memory_bytes: 64 * MIB,
        non_datafusion_memory_bytes: 16 * MIB,
        spill_bytes: 64 * MIB,
        spill_dir: None,
        output_buffer_bytes: 5 * MIB as usize,
        parquet_writer_memory_bytes: MIB as usize,
        max_waiters: 2,
        admission_timeout: timeout,
    })
    .unwrap()
}

async fn fixture(
    bodies: [&str; 2],
) -> (
    Arc<dyn ObjectStore>,
    Arc<std::sync::Mutex<Catalog>>,
    TempDir,
) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();
    let tmp = TempDir::new().unwrap();
    let catalog = Catalog::open(&tmp.path().join("catalog.sqlite"), BUCKET).unwrap();
    for (i, body) in bodies.into_iter().enumerate() {
        let mut builder = LogsBlockBuilder::new(writer, block_cfg());
        let fp = i as u64 + 1;
        builder.observe_stream(fp, vec![(b"service".to_vec(), b"api".to_vec())]);
        builder.append_entry(
            fp,
            1_000_000 + i as u64,
            9,
            body.as_bytes().to_vec(),
            vec![],
        );
        let meta = builder
            .finish_and_upload(store.as_ref())
            .await
            .unwrap()
            .unwrap();
        assert!(catalog.insert_block(&meta).unwrap());
    }
    (store, Arc::new(std::sync::Mutex::new(catalog)), tmp)
}

async fn meta_count(store: &Arc<dyn ObjectStore>) -> usize {
    let mut objects = store.list(None);
    let mut count = 0;
    while let Some(object) = objects.next().await {
        if object.unwrap().location.as_ref().ends_with("meta.json") {
            count += 1;
        }
    }
    count
}

#[tokio::test]
async fn default_envelope_admits_a_real_merge_without_starvation() {
    let (store, catalog, _tmp) =
        fixture(["first ordinary log body", "second ordinary log body"]).await;
    let resources = CompactResources::new(ResourceConfig::default()).unwrap();
    let report = tokio::time::timeout(
        Duration::from_secs(20),
        compact_once(
            store,
            &catalog,
            BUCKET,
            &compact_cfg(),
            &block_cfg(),
            resources.clone(),
        ),
    )
    .await
    .expect("default admission must not starve")
    .unwrap();

    assert_eq!(report.merges, 1);
    assert_eq!(report.resource_failed, 0);
    assert_eq!(resources.telemetry().weighted_running_bytes, 0);
}

#[tokio::test]
async fn admission_deferral_does_not_abort_pass_and_next_pass_recovers() {
    let (store, catalog, _tmp) = fixture(["first", "second"]).await;
    let resources = constrained(Duration::from_millis(20));
    let all = resources.admit(16 * MIB).await.unwrap();

    let deferred = compact_once(
        store.clone(),
        &catalog,
        BUCKET,
        &compact_cfg(),
        &block_cfg(),
        resources.clone(),
    )
    .await
    .expect("resource deferral is a report outcome, not a pass error");
    assert_eq!(deferred.merges, 0);
    assert_eq!(deferred.resource_failed, 1);
    assert_eq!(meta_count(&store).await, 2, "no output committed");

    drop(all);
    let recovered = compact_once(
        store,
        &catalog,
        BUCKET,
        &compact_cfg(),
        &block_cfg(),
        resources.clone(),
    )
    .await
    .unwrap();
    assert_eq!(recovered.merges, 1);
    assert_eq!(recovered.resource_failed, 0);
    assert_eq!(resources.telemetry().weighted_running_bytes, 0);
}

/// Produce many mostly-distinct trigrams without relying on randomness.
fn high_cardinality_body(seed: usize) -> String {
    let mut out = String::with_capacity(180_000);
    for i in 0..30_000usize {
        out.push_str(&format!("{seed:x}{i:05x}"));
    }
    out
}

#[tokio::test]
async fn body_bloom_limit_is_controlled_resource_failure_without_committed_output() {
    let a = high_cardinality_body(10);
    let b = high_cardinality_body(11);
    let (store, catalog, _tmp) = fixture([&a, &b]).await;
    let resources = constrained(Duration::from_secs(1));
    // Wider grams make sequential textual input high-cardinality while keeping
    // the fixture small and deterministic (byte trigrams have a small alphabet).
    let mut bloom_cfg = block_cfg();
    bloom_cfg.bloom_ngram = 12;

    let report = compact_once(
        store.clone(),
        &catalog,
        BUCKET,
        &compact_cfg(),
        &bloom_cfg,
        resources.clone(),
    )
    .await
    .expect("sidecar exhaustion must not abort the standalone pass");
    assert_eq!(report.merges, 0);
    assert_eq!(report.resource_failed, 1);
    assert_eq!(
        meta_count(&store).await,
        2,
        "failed merge must not commit meta.json"
    );
    assert_eq!(catalog.lock().unwrap().list_blocks().unwrap().len(), 2);
    assert_eq!(resources.telemetry().weighted_running_bytes, 0);
}
