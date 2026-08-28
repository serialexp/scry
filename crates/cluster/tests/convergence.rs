//! In-process proof of multi-instance convergence + single-winner
//! maintenance, with no Valkey in sight.
//!
//! - **apply_event** is idempotent and order-independent: a duplicated
//!   `Created` inserts once; a `Superseded` arriving *before* its merged
//!   block's `Created` still satisfies the foreign key (the event carries
//!   `by_meta`); a `Deleted` removes the row and is a no-op when re-applied.
//! - **poll_once** recovers blocks pub/sub dropped: a block on the bucket but
//!   missing from the catalog is found by the incremental list, and a second
//!   poll (cursor advanced) lists nothing new.
//! - **run_compaction_pass** under a shared [`LocalLeaseProvider`] yields a
//!   single winner: two concurrent passes over the same partition produce one
//!   merged block, never duplicate rows.
//! - **run_retention_pass** respects the global lease: held by a peer ⇒ the
//!   pass reports `aborted` and reaps nothing; released ⇒ it reaps.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use object_store::{memory::InMemory, ObjectStore, ObjectStoreExt};
use scry_block::{
    BlockBuilder, BlockBuilderConfig, BlockEvent, BlockMeta, LogsBlockBuilder, NoopSink,
};
use scry_catalog::{date_dir, Catalog};
use scry_cluster::{
    apply_event, full_walk, poll_once, run_compaction_pass, run_retention_pass, LeaseGuard,
    LeaseProvider, LocalLeaseProvider, RETENTION_LEASE_KEY,
};
use scry_compact::CompactConfig;
use scry_proto::streaming::LogsAppender;
use scry_retention::RetentionConfig;
use tempfile::TempDir;
use uuid::Uuid;

const BUCKET: &str = "test";
const DAY: u64 = 86_400 * 1_000_000_000;
const NOW: u64 = 1_000 * DAY;

fn test_cfg() -> BlockBuilderConfig {
    BlockBuilderConfig {
        max_rows: 1_000_000,
        target_bytes: 128 * 1024 * 1024,
        row_group_size: 100,
        ..Default::default()
    }
}

fn labels(pairs: &[(&str, &str)]) -> Vec<(Vec<u8>, Vec<u8>)> {
    pairs
        .iter()
        .map(|(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec()))
        .collect()
}

/// Build and upload a single-stream logs block; return its meta.
async fn build_logs_block(
    store: &Arc<dyn ObjectStore>,
    writer: Uuid,
    fp: u64,
    ts0: u64,
    n: u64,
) -> BlockMeta {
    let mut b = LogsBlockBuilder::new(writer, test_cfg());
    b.observe_stream(fp, labels(&[("service", "api")]));
    for i in 0..n {
        b.append_entry(
            fp,
            ts0 + i,
            9,
            format!("row {i} fp={fp:#x}").into_bytes(),
            vec![(b"status".to_vec(), b"ok".to_vec())],
        );
    }
    b.finish_and_upload(store.as_ref())
        .await
        .unwrap()
        .expect("block uploaded")
}

/// A fabricated meta (no bucket objects) — enough to exercise catalog applies.
fn fake_meta(signal: &str, writer: Uuid, ts: u64) -> BlockMeta {
    BlockMeta {
        uuid: Uuid::now_v7(),
        signal: signal.to_string(),
        writer_id: writer,
        ts_min_unix_nano: ts,
        ts_max_unix_nano: ts + 1,
        row_count: 10,
        byte_size: 100,
        schema_version: 1,
        level: 0,
        producer_version: String::new(),
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

fn open_catalog() -> (Catalog, TempDir) {
    let tmp = TempDir::new().unwrap();
    let catalog = Catalog::open(&tmp.path().join("cat.sqlite"), BUCKET).unwrap();
    (catalog, tmp)
}

#[test]
fn created_apply_is_idempotent_and_advances_cursor() {
    let (catalog, _tmp) = open_catalog();
    let writer = Uuid::now_v7();
    let m = fake_meta("logs", writer, NOW);

    let ev = BlockEvent::Created { meta: m.clone() };
    let first = apply_event(&catalog, &ev).unwrap();
    assert_eq!(first.inserted, 1, "first Created inserts");
    // Cursor seeded at this block.
    let date = date_dir(m.ts_min_unix_nano);
    assert_eq!(
        catalog.get_cursor("logs", writer, &date).unwrap(),
        Some(m.uuid)
    );

    // Duplicate (e.g. self-delivered, or a publish retry) is a no-op.
    let second = apply_event(&catalog, &ev).unwrap();
    assert_eq!(second.inserted, 0, "duplicate Created inserts nothing");
    assert_eq!(catalog.block_count().unwrap(), 1);
}

#[test]
fn superseded_before_created_satisfies_foreign_key() {
    let (catalog, _tmp) = open_catalog();
    let writer = Uuid::now_v7();

    // Two inputs known to this peer.
    let in1 = fake_meta("logs", writer, NOW);
    let in2 = fake_meta("logs", writer, NOW + 1);
    apply_event(&catalog, &BlockEvent::Created { meta: in1.clone() }).unwrap();
    apply_event(&catalog, &BlockEvent::Created { meta: in2.clone() }).unwrap();

    // The merged block's Created never arrived (dropped). The Superseded
    // event carries by_meta so the FK still holds.
    let merged = fake_meta("logs", writer, NOW);
    let ev = BlockEvent::Superseded {
        inputs: vec![in1.uuid, in2.uuid],
        by: merged.uuid,
        by_meta: merged.clone(),
        reap_eligible_at_unix_nano: NOW + 600,
    };
    apply_event(&catalog, &ev).unwrap();

    // Merged block present and live; inputs superseded (gone from live set).
    let live = catalog.list_blocks().unwrap();
    assert_eq!(live.len(), 1, "only the merged block is live");
    assert_eq!(live[0].meta.uuid, merged.uuid);

    assert!(
        catalog.list_pending_reaps(NOW + 599).unwrap().is_empty(),
        "peer event must preserve the committing compactor's grace"
    );
    assert_eq!(catalog.list_pending_reaps(NOW + 600).unwrap().len(), 2);

    // Re-applying an older/self-delivered event cannot shorten eligibility.
    let immediate = BlockEvent::Superseded {
        inputs: vec![in1.uuid, in2.uuid],
        by: merged.uuid,
        by_meta: merged.clone(),
        reap_eligible_at_unix_nano: 0,
    };
    apply_event(&catalog, &immediate).unwrap();
    assert!(catalog.list_pending_reaps(NOW + 599).unwrap().is_empty());
    assert_eq!(catalog.list_blocks().unwrap().len(), 1);
}

#[test]
fn legacy_superseded_event_uses_receivers_local_grace() {
    let (catalog, _tmp) = open_catalog();
    let writer = Uuid::now_v7();
    let input = fake_meta("logs", writer, NOW);
    let output = fake_meta("logs", writer, NOW + 1);
    apply_event(
        &catalog,
        &BlockEvent::Created {
            meta: input.clone(),
        },
    )
    .unwrap();
    let legacy = BlockEvent::Superseded {
        inputs: vec![input.uuid],
        by: output.uuid,
        by_meta: output,
        reap_eligible_at_unix_nano: 0,
    };
    scry_cluster::apply_event_with_grace(&catalog, &legacy, Duration::from_secs(600)).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    assert!(catalog
        .list_pending_reaps(now + 599_000_000_000)
        .unwrap()
        .is_empty());
    assert_eq!(
        catalog
            .list_pending_reaps(now + 601_000_000_000)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn deleted_apply_removes_row_and_is_idempotent() {
    let (catalog, _tmp) = open_catalog();
    let writer = Uuid::now_v7();
    let m = fake_meta("metrics", writer, NOW);
    apply_event(&catalog, &BlockEvent::Created { meta: m.clone() }).unwrap();
    assert_eq!(catalog.block_count().unwrap(), 1);

    let del = BlockEvent::Deleted {
        signal: "metrics".into(),
        uuids: vec![m.uuid],
    };
    apply_event(&catalog, &del).unwrap();
    assert!(catalog.get_block(m.uuid).unwrap().is_none());

    // Re-apply: still gone, no error.
    apply_event(&catalog, &del).unwrap();
    assert_eq!(catalog.block_count().unwrap(), 0);
}

#[tokio::test]
async fn poll_recovers_dropped_block_then_finds_nothing_new() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();

    // Two blocks on the bucket, same (signal, writer, date).
    let b1 = build_logs_block(&store, writer, 0xA001, NOW, 50).await;
    let b2 = build_logs_block(&store, writer, 0xB001, NOW + 100, 50).await;
    assert!(b2.uuid > b1.uuid, "UUIDv7 is monotonic");

    let (catalog, _tmp) = open_catalog();
    // Simulate pub/sub delivered b1 (catalog + cursor) but DROPPED b2.
    catalog.insert_block(&b1).unwrap();
    let date = date_dir(b1.ts_min_unix_nano);
    catalog
        .advance_cursor("logs", writer, &date, b1.uuid)
        .unwrap();
    assert_eq!(catalog.block_count().unwrap(), 1);

    // First poll finds exactly the dropped b2.
    let r1 = poll_once(store.as_ref(), &catalog, BUCKET).await.unwrap();
    assert_eq!(r1.inserted, 1, "poll recovers the dropped block");
    assert!(catalog.get_block(b2.uuid).unwrap().is_some());
    assert_eq!(
        catalog.get_cursor("logs", writer, &date).unwrap(),
        Some(b2.uuid)
    );

    // Second poll: cursor advanced past b2, nothing new.
    let r2 = poll_once(store.as_ref(), &catalog, BUCKET).await.unwrap();
    assert_eq!(r2.inserted, 0, "no new blocks on a healthy re-poll");
}

#[tokio::test]
async fn full_walk_discovers_untracked_prefixes() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();
    let b1 = build_logs_block(&store, writer, 0xA001, NOW, 30).await;
    let b2 = build_logs_block(&store, writer, 0xB001, NOW + 50, 30).await;

    // Empty catalog with no cursors at all — incremental poll would find
    // nothing (no prefixes known). A full walk discovers both.
    let (catalog, _tmp) = open_catalog();
    let poll = poll_once(store.as_ref(), &catalog, BUCKET).await.unwrap();
    assert_eq!(poll.inserted, 0, "no cursors ⇒ incremental poll is blind");

    let walk = full_walk(store.as_ref(), &catalog, BUCKET).await.unwrap();
    assert_eq!(walk.inserted, 2, "full walk discovers untracked blocks");
    assert!(catalog.get_block(b1.uuid).unwrap().is_some());
    assert!(catalog.get_block(b2.uuid).unwrap().is_some());

    // And it seeded the cursor, so a subsequent incremental poll is cheap.
    let date = date_dir(b1.ts_min_unix_nano);
    assert_eq!(
        catalog.get_cursor("logs", writer, &date).unwrap(),
        Some(b2.uuid)
    );
}

#[tokio::test]
async fn lease_holder_reconciles_prior_committed_output_before_remerging_inputs() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();
    let mut inputs = Vec::new();
    for (i, fp) in [0xA001u64, 0xB001, 0xC001].into_iter().enumerate() {
        inputs.push(build_logs_block(&store, writer, fp, NOW + (i as u64) * 100, 50).await);
    }

    // Simulate holder A reaching the meta.json commit point and crashing before
    // applying output/lineage to this stale peer's catalog.
    let (source_catalog, _source_tmp) = open_catalog();
    for meta in &inputs {
        source_catalog.insert_block(meta).unwrap();
    }
    let source_plan = scry_compact::plan_merges(
        &source_catalog.list_blocks().unwrap(),
        &CompactConfig {
            fanout: 3,
            max_level: 3,
            grace: Duration::ZERO,
            signal_filter: Some("logs".into()),
        },
    )
    .merges
    .pop()
    .unwrap();
    let committed = scry_compact::merge_blocks(
        store.clone(),
        BUCKET,
        &source_plan.signal,
        &source_plan.inputs,
        source_plan.output_level(),
        Uuid::now_v7(),
        &test_cfg(),
        &scry_block::AlwaysValid,
    )
    .await
    .unwrap()
    .expect("prior holder committed output meta.json");

    let (stale_catalog, _stale_tmp) = open_catalog();
    for meta in &inputs {
        stale_catalog.insert_block(meta).unwrap();
    }
    let stale_catalog = Arc::new(Mutex::new(stale_catalog));
    let report = run_compaction_pass(
        &LocalLeaseProvider::new(),
        store.clone(),
        stale_catalog.as_ref(),
        BUCKET,
        &CompactConfig {
            fanout: 3,
            max_level: 3,
            grace: Duration::ZERO,
            signal_filter: Some("logs".into()),
        },
        &test_cfg(),
        &NoopSink,
        Duration::from_secs(30),
    )
    .await
    .unwrap();

    assert_eq!(report.merges, 0, "stale inputs must not be re-merged");
    let live = stale_catalog.lock().unwrap().list_blocks().unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].meta.uuid, committed.uuid);
    assert_eq!(live[0].meta.row_count, 150);

    let mut metas = store.list(None);
    let mut committed_outputs = 0;
    while let Some(object) = futures::StreamExt::next(&mut metas).await {
        let object = object.unwrap();
        if object.location.as_ref().ends_with(".meta.json") {
            let bytes = store
                .get(&object.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            let meta: BlockMeta = serde_json::from_slice(&bytes).unwrap();
            if meta.level == 1 {
                committed_outputs += 1;
            }
        }
    }
    assert_eq!(committed_outputs, 1, "no duplicate committed output");
}

#[tokio::test]
async fn concurrent_compaction_has_a_single_winner() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();

    // Three L0 inputs in one partition.
    let mut inputs = Vec::new();
    for (i, fp) in [0xA001u64, 0xB001, 0xC001].into_iter().enumerate() {
        inputs.push(build_logs_block(&store, writer, fp, NOW + (i as u64) * 100, 50).await);
    }

    let (catalog, _tmp) = open_catalog();
    for m in &inputs {
        catalog.insert_block(m).unwrap();
    }
    let catalog = Arc::new(Mutex::new(catalog));

    let provider = LocalLeaseProvider::new();
    let cfg = CompactConfig {
        fanout: 3,
        max_level: 3,
        grace: Duration::ZERO,
        signal_filter: Some("logs".into()),
    };

    // Two instances run a compaction pass concurrently over the same
    // partition, sharing one lease provider.
    let h1 = {
        let (p, s, c, cfg) = (
            provider.clone(),
            store.clone(),
            catalog.clone(),
            cfg.clone(),
        );
        tokio::spawn(async move {
            run_compaction_pass(
                &p,
                s,
                c.as_ref(),
                BUCKET,
                &cfg,
                &test_cfg(),
                &NoopSink,
                Duration::from_secs(30),
            )
            .await
            .unwrap()
        })
    };
    let h2 = {
        let (p, s, c, cfg) = (
            provider.clone(),
            store.clone(),
            catalog.clone(),
            cfg.clone(),
        );
        tokio::spawn(async move {
            run_compaction_pass(
                &p,
                s,
                c.as_ref(),
                BUCKET,
                &cfg,
                &test_cfg(),
                &NoopSink,
                Duration::from_secs(30),
            )
            .await
            .unwrap()
        })
    };
    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();

    // Exactly one merge happened across both instances.
    assert_eq!(
        r1.merges + r2.merges,
        1,
        "exactly one instance merged the partition"
    );

    // The catalog holds exactly one live block — the merged L1 — with the
    // full row count and no duplicates.
    let cat = catalog.lock().unwrap();
    let live = cat.list_blocks().unwrap();
    assert_eq!(live.len(), 1, "no duplicate merged blocks");
    assert_eq!(live[0].level, 1);
    assert_eq!(
        live[0].meta.row_count, 150,
        "every input row survives exactly once"
    );
    for m in &inputs {
        assert!(cat.get_block(m.uuid).unwrap().is_none(), "inputs reaped");
    }
}

#[tokio::test]
async fn retention_pass_defers_to_the_global_lease() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();
    // One aged logs block (90 days old) — a candidate under a 7-day TTL.
    let old = build_logs_block(&store, writer, 0xA001, NOW - 90 * DAY, 50).await;

    let (catalog, _tmp) = open_catalog();
    catalog.insert_block(&old).unwrap();

    let mut overrides = std::collections::BTreeMap::new();
    overrides.insert("logs".to_string(), Duration::from_nanos(7 * DAY));
    let cfg = RetentionConfig {
        default_ttl: None,
        overrides,
        grace: Duration::ZERO,
        apply: true,
    };

    let provider = LocalLeaseProvider::new();

    // A peer holds the global retention lease.
    let peer_guard = provider
        .try_acquire(RETENTION_LEASE_KEY, Duration::from_secs(30))
        .await
        .unwrap()
        .expect("peer takes the lease");

    let blocked = run_retention_pass(
        &provider,
        store.clone(),
        &catalog,
        &cfg,
        NOW,
        &NoopSink,
        Duration::from_secs(30),
    )
    .await
    .unwrap();
    assert!(
        blocked.aborted,
        "pass aborts when the lease is held by a peer"
    );
    assert!(
        catalog.get_block(old.uuid).unwrap().is_some(),
        "nothing reaped without the lease"
    );

    // Peer releases; now the pass acquires and reaps.
    peer_guard.release().await;
    let done = run_retention_pass(
        &provider,
        store.clone(),
        &catalog,
        &cfg,
        NOW,
        &NoopSink,
        Duration::from_secs(30),
    )
    .await
    .unwrap();
    assert!(!done.aborted);
    assert_eq!(done.reaped, 1);
    assert!(
        catalog.get_block(old.uuid).unwrap().is_none(),
        "aged block reaped"
    );
}

/// A peer told that blocks were *staged* for deletion must hide them from
/// queries straight away, while their objects are still in the bucket.
///
/// This is what gives peers the same grace window the retention owner gives
/// itself. Without it, a peer keeps planning queries against these blocks right
/// up until the objects vanish, and every such query 404s and has to self-heal.
#[test]
fn soft_deleted_apply_hides_rows_for_the_owners_grace_window() {
    let (catalog, _tmp) = open_catalog();
    let writer = Uuid::now_v7();
    let a = fake_meta("logs", writer, NOW);
    let b = fake_meta("logs", writer, NOW + 1);
    apply_event(&catalog, &BlockEvent::Created { meta: a.clone() }).unwrap();
    apply_event(&catalog, &BlockEvent::Created { meta: b.clone() }).unwrap();
    assert_eq!(catalog.list_blocks().unwrap().len(), 2);

    const GRACE: u64 = 600 * 1_000_000_000;
    let soft = BlockEvent::SoftDeleted {
        signal: "logs".into(),
        uuids: vec![a.uuid, b.uuid],
        deleted_at_unix_nano: NOW,
        delete_eligible_at_unix_nano: NOW + GRACE,
    };
    let outcome = apply_event(&catalog, &soft).unwrap();
    assert_eq!(outcome.soft_deleted, 2);

    // Hidden from queries immediately …
    assert!(
        catalog.list_blocks().unwrap().is_empty(),
        "a peer must stop planning against staged blocks at once"
    );
    // … but the rows survive, carrying the owner's deadline.
    assert!(catalog.get_block(a.uuid).unwrap().is_some());
    assert!(
        catalog
            .list_pending_deletions(NOW + GRACE - 1)
            .unwrap()
            .is_empty(),
        "not eligible before the owner's deadline"
    );
    assert_eq!(
        catalog.list_pending_deletions(NOW + GRACE).unwrap().len(),
        2,
        "eligible exactly at the deadline"
    );

    // Duplicated / reordered delivery is a no-op, and an event carrying an
    // *earlier* deadline must never shorten the window already recorded.
    apply_event(&catalog, &soft).unwrap();
    apply_event(
        &catalog,
        &BlockEvent::SoftDeleted {
            signal: "logs".into(),
            uuids: vec![a.uuid, b.uuid],
            deleted_at_unix_nano: NOW - 10,
            delete_eligible_at_unix_nano: NOW + 1,
        },
    )
    .unwrap();
    assert!(
        catalog
            .list_pending_deletions(NOW + GRACE - 1)
            .unwrap()
            .is_empty(),
        "a late/earlier event must not shorten the grace window"
    );

    // The hard delete still lands normally afterwards.
    apply_event(
        &catalog,
        &BlockEvent::Deleted {
            signal: "logs".into(),
            uuids: vec![a.uuid, b.uuid],
        },
    )
    .unwrap();
    assert_eq!(catalog.block_count().unwrap(), 0);
}

/// A `SoftDeleted` for blocks this peer has never seen is a no-op, not an
/// error — events are reordered and peers converge at different rates.
#[test]
fn soft_deleted_apply_tolerates_unknown_blocks() {
    let (catalog, _tmp) = open_catalog();
    let outcome = apply_event(
        &catalog,
        &BlockEvent::SoftDeleted {
            signal: "logs".into(),
            uuids: vec![Uuid::now_v7()],
            deleted_at_unix_nano: NOW,
            delete_eligible_at_unix_nano: NOW + 1,
        },
    )
    .unwrap();
    assert_eq!(outcome.soft_deleted, 1, "intent is reported");
    assert_eq!(catalog.block_count().unwrap(), 0);
}
