//! Commit-point fence proof for v0.9 multi-instance compaction.
//!
//! `compact_partition` consults a [`Fence`] (the "do I still hold the lease?"
//! re-check) before every irreversible step. The load-bearing one is the
//! **commit-point fence** inside `merge_blocks`, right before the `meta.json`
//! PUT: blocks are addressed by random UUID (not content hash), so two
//! instances merging the same partition would each commit a *distinct* block
//! with identical rows — a duplicate-count corruption a later merge unions
//! rather than dedupes. The fence guarantees that a lease lost mid-merge
//! aborts cleanly: the inputs survive untouched for the rightful holder, and
//! nothing the merge half-committed becomes visible to reconcile.
//!
//! These tests drive `compact_partition` with a `CountingFence` that goes
//! invalid after K checks, proving:
//!  - flip before the `meta.json` commit ⇒ aborted, no merged `meta.json`
//!    on the bucket, inputs live, no events emitted;
//!  - flip after the merge commits but before `mark_superseded` ⇒ aborted,
//!    inputs still live (merged block not inserted into the catalog), no
//!    `Created`/`Superseded`/`Deleted` events;
//!  - an always-valid fence ⇒ merged, inputs reaped, exactly the
//!    `Created` → `Superseded` → `Deleted` event sequence emitted.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use futures::StreamExt;
use object_store::{memory::InMemory, ObjectStore};
use scry_block::{
    AlwaysValid, BlockBuilder, BlockBuilderConfig, BlockEvent, BlockEventSink, Fence,
    LogsBlockBuilder,
};
use scry_catalog::Catalog;
use scry_compact::{compact_partition, plan_merges, CompactConfig, PartitionOutcome};
use scry_proto::streaming::LogsAppender;
use tempfile::TempDir;
use uuid::Uuid;

const BUCKET: &str = "test";

fn test_cfg() -> BlockBuilderConfig {
    BlockBuilderConfig {
        max_rows: 1_000_000,
        target_bytes: 128 * 1024 * 1024,
        row_group_size: 100,
        ..Default::default()
    }
}

/// A [`Fence`] that returns `Ok` for its first `valid_for` checks and `Err`
/// for every check after that. Lets a test pin exactly which step of the
/// compaction lifecycle the lease is "lost" at.
struct CountingFence {
    valid_for: usize,
    calls: AtomicUsize,
}

impl CountingFence {
    fn new(valid_for: usize) -> Self {
        Self {
            valid_for,
            calls: AtomicUsize::new(0),
        }
    }
}

impl Fence for CountingFence {
    fn check(&self) -> Result<()> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if n < self.valid_for {
            Ok(())
        } else {
            Err(anyhow!("lease lost (check #{n})"))
        }
    }
}

/// A [`BlockEventSink`] that records every emitted event for assertion.
#[derive(Default)]
struct CapturingSink {
    events: Mutex<Vec<BlockEvent>>,
}

impl BlockEventSink for CapturingSink {
    fn emit(&self, event: BlockEvent) {
        self.events.lock().unwrap().push(event);
    }
}

fn labels(pairs: &[(&str, &str)]) -> Vec<(Vec<u8>, Vec<u8>)> {
    pairs
        .iter()
        .map(|(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec()))
        .collect()
}

/// Build three single-stream L0 logs blocks, upload them, and return a
/// catalog holding all three plus the store and their metas.
async fn three_logs_blocks() -> (
    Arc<dyn ObjectStore>,
    Catalog,
    TempDir,
    Vec<scry_block::BlockMeta>,
) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();
    let mut metas = Vec::new();
    for (i, fp) in [0xA001u64, 0xB001, 0xC001].into_iter().enumerate() {
        let mut b = LogsBlockBuilder::new(writer, test_cfg());
        b.observe_stream(fp, labels(&[("service", "api")]));
        let ts0 = 1_000_000 + (i as u64) * 100;
        for j in 0..50u64 {
            b.append_entry(
                fp,
                ts0 + j,
                9,
                format!("row {j} fp={fp:#x}").into_bytes(),
                vec![(b"status".to_vec(), b"ok".to_vec())],
            );
        }
        metas.push(
            b.finish_and_upload(store.as_ref())
                .await
                .unwrap()
                .expect("block uploaded"),
        );
    }

    let tmp = TempDir::new().unwrap();
    let catalog = Catalog::open(&tmp.path().join("cat.sqlite"), BUCKET).unwrap();
    for m in &metas {
        assert!(catalog.insert_block(m).unwrap());
    }
    assert_eq!(catalog.block_count().unwrap(), 3);

    (store, catalog, tmp, metas)
}

fn one_logs_plan(catalog: &Catalog) -> scry_compact::PlannedMerge {
    let cfg = CompactConfig {
        fanout: 3,
        max_level: 3,
        grace: std::time::Duration::ZERO,
        signal_filter: Some("logs".into()),
    };
    let live = catalog.list_blocks().unwrap();
    let mut plans = plan_merges(&live, &cfg);
    assert_eq!(plans.len(), 1, "exactly one logs partition to merge");
    plans.pop().unwrap()
}

/// Count `*.meta.json` objects in the bucket — the reconcile-visible "block
/// exists" markers.
async fn count_meta_json(store: &Arc<dyn ObjectStore>) -> usize {
    let mut list = store.list(None);
    let mut n = 0;
    while let Some(item) = list.next().await {
        let meta = item.unwrap();
        if meta.location.as_ref().ends_with("meta.json") {
            n += 1;
        }
    }
    n
}

fn cfg() -> CompactConfig {
    CompactConfig {
        fanout: 3,
        max_level: 3,
        grace: std::time::Duration::ZERO,
        signal_filter: Some("logs".into()),
    }
}

#[tokio::test]
async fn fence_lost_before_meta_commit_aborts_with_inputs_intact() {
    let (store, catalog, _tmp, metas) = three_logs_blocks().await;
    let plan = one_logs_plan(&catalog);

    // valid_for = 1: the early pre-merge check passes; the in-merge
    // commit-point check (the 2nd) fails → merge returns None before writing
    // meta.json.
    let fence = CountingFence::new(1);
    let sink = CapturingSink::default();
    let writer_id = Uuid::now_v7();

    let outcome = compact_partition(
        &plan,
        store.clone(),
        &catalog,
        BUCKET,
        writer_id,
        &cfg(),
        &test_cfg(),
        &fence,
        &sink,
    )
    .await
    .unwrap();

    assert!(matches!(outcome, PartitionOutcome::Aborted));

    // No merged meta.json was committed — only the three inputs' remain.
    assert_eq!(
        count_meta_json(&store).await,
        3,
        "merge must not commit meta.json after losing the lease"
    );

    // Inputs are untouched: all three live, none superseded.
    let live = catalog.list_blocks().unwrap();
    assert_eq!(live.len(), 3, "all inputs survive the aborted merge");
    for m in &metas {
        assert!(catalog.get_block(m.uuid).unwrap().is_some());
    }

    // No events leaked to peers.
    assert!(sink.events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn fence_lost_after_commit_before_supersede_aborts_inputs_live() {
    let (store, catalog, _tmp, metas) = three_logs_blocks().await;
    let plan = one_logs_plan(&catalog);

    // valid_for = 2: early check (1) and the in-merge commit-point check (2)
    // pass → the merge commits its meta.json. The post-merge check (3, right
    // before insert + mark_superseded) fails → abort before any catalog
    // mutation.
    let fence = CountingFence::new(2);
    let sink = CapturingSink::default();
    let writer_id = Uuid::now_v7();

    let outcome = compact_partition(
        &plan,
        store.clone(),
        &catalog,
        BUCKET,
        writer_id,
        &cfg(),
        &test_cfg(),
        &fence,
        &sink,
    )
    .await
    .unwrap();

    assert!(matches!(outcome, PartitionOutcome::Aborted));

    // The merge DID commit its meta.json (the fence was valid at the commit
    // point), so a 4th, leaked, meta.json exists on the bucket — but it was
    // never inserted into the catalog.
    assert_eq!(
        count_meta_json(&store).await,
        4,
        "merge committed before the post-merge fence tripped"
    );

    // The local catalog is clean: only the three live inputs, none superseded,
    // and the merged block was not inserted.
    let live = catalog.list_blocks().unwrap();
    assert_eq!(live.len(), 3, "inputs stay live; merged block not cataloged");
    for m in &metas {
        assert!(catalog.get_block(m.uuid).unwrap().is_some());
    }

    // No events — Created is only emitted after the post-merge fence passes.
    assert!(sink.events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn valid_fence_merges_reaps_inputs_and_emits_events() {
    let (store, catalog, _tmp, metas) = three_logs_blocks().await;
    let plan = one_logs_plan(&catalog);

    let fence = AlwaysValid;
    let sink = CapturingSink::default();
    let writer_id = Uuid::now_v7();
    let input_uuids: Vec<Uuid> = metas.iter().map(|m| m.uuid).collect();

    let outcome = compact_partition(
        &plan,
        store.clone(),
        &catalog,
        BUCKET,
        writer_id,
        &cfg(),
        &test_cfg(),
        &fence,
        &sink,
    )
    .await
    .unwrap();

    let bytes_out = match outcome {
        PartitionOutcome::Merged { bytes_out } => bytes_out,
        PartitionOutcome::Aborted => panic!("expected a committed merge"),
    };
    assert!(bytes_out > 0);

    // One merged L1 block live; the three inputs reaped from the catalog.
    let live = catalog.list_blocks().unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].level, 1);
    assert_eq!(live[0].meta.row_count, 150);
    for m in &metas {
        assert!(catalog.get_block(m.uuid).unwrap().is_none());
    }

    // Exactly the Created → Superseded → Deleted sequence, in order.
    let events = sink.events.lock().unwrap();
    assert_eq!(events.len(), 3, "one event per lifecycle point");
    let merged_uuid = live[0].meta.uuid;
    match &events[0] {
        BlockEvent::Created { meta } => {
            assert_eq!(meta.uuid, merged_uuid);
            assert_eq!(meta.signal, "logs");
        }
        other => panic!("first event should be Created, got {other:?}"),
    }
    match &events[1] {
        BlockEvent::Superseded { inputs, by, by_meta } => {
            assert_eq!(*by, merged_uuid);
            assert_eq!(by_meta.uuid, merged_uuid);
            let mut got = inputs.clone();
            let mut want = input_uuids.clone();
            got.sort();
            want.sort();
            assert_eq!(got, want, "all inputs superseded by the merged block");
        }
        other => panic!("second event should be Superseded, got {other:?}"),
    }
    match &events[2] {
        BlockEvent::Deleted { signal, uuids } => {
            assert_eq!(signal, "logs");
            let mut got = uuids.clone();
            let mut want = input_uuids.clone();
            got.sort();
            want.sort();
            assert_eq!(got, want, "all inputs deleted");
        }
        other => panic!("third event should be Deleted, got {other:?}"),
    }
}
