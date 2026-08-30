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
///
/// The window is re-based on the *local* clock: only the owner's grace
/// *duration* crosses the wire, never its absolute deadline. Reaping pending
/// deletions is lease-free, so an owner whose clock ran behind would otherwise
/// hand every peer a deadline already in their past and collapse the window to
/// nothing — see `a_stagers_slow_clock_cannot_collapse_the_grace_window`.
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
    /// `applied_at` is read just *before* the apply, which stamps its own
    /// `now` microseconds later, so the eligibility boundary sits slightly
    /// past `applied_at + GRACE`. A second of slack is far below the window.
    const SLACK: u64 = 1_000_000_000;
    let local_now = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    };

    // The owner's timestamps are *its* clock — here, deliberately nothing like
    // ours (NOW is 1000 days after the epoch). Only their difference is used.
    let soft = BlockEvent::SoftDeleted {
        signal: "logs".into(),
        uuids: vec![a.uuid, b.uuid],
        deleted_at_unix_nano: NOW,
        delete_eligible_at_unix_nano: NOW + GRACE,
    };
    let applied_at = local_now();
    let outcome = apply_event(&catalog, &soft).unwrap();
    assert_eq!(outcome.soft_deleted, 2);

    // Hidden from queries immediately …
    assert!(
        catalog.list_blocks().unwrap().is_empty(),
        "a peer must stop planning against staged blocks at once"
    );
    // … but the rows survive, carrying a locally-based deadline. The owner's
    // absolute deadline (NOW + GRACE) is decades in our past; adopting it
    // verbatim would make these instantly reapable.
    assert!(catalog.get_block(a.uuid).unwrap().is_some());
    assert!(
        catalog
            .list_pending_deletions(NOW + GRACE)
            .unwrap()
            .is_empty(),
        "the owner's absolute deadline was adopted; a slow clock would delete data early"
    );
    assert!(
        catalog
            .list_pending_deletions(applied_at + GRACE - 1)
            .unwrap()
            .is_empty(),
        "not eligible before a full grace window has passed on our clock"
    );
    assert_eq!(
        catalog
            .list_pending_deletions(applied_at + GRACE + SLACK)
            .unwrap()
            .len(),
        2,
        "eligible once our own grace window elapses"
    );

    // Duplicated / reordered delivery is a no-op, and must not re-date the
    // hiding — the applier recomputes `now + grace` every time, so without
    // first-application-wins the deadline would creep forward forever and the
    // blocks would never become reapable here.
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
    assert_eq!(
        catalog
            .list_pending_deletions(applied_at + GRACE + SLACK)
            .unwrap()
            .len(),
        2,
        "a repeat application moved the deadline"
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

/// Collects everything emitted, so a test can assert on the events a pass
/// broadcast rather than only on its catalog after-state.
#[derive(Default)]
struct RecordingSink(Mutex<Vec<BlockEvent>>);

impl scry_block::BlockEventSink for RecordingSink {
    fn emit(&self, event: BlockEvent) {
        self.0.lock().unwrap().push(event);
    }
}

/// Deletion work that is staged but not yet reaped must be re-announced on
/// every retention pass, for as long as it stays outstanding.
///
/// The `SoftDeleted` emitted when a block is staged is a one-shot, and the
/// Valkey staged-deletions registry it feeds holds entries under a TTL sized
/// for a reap that happens roughly on schedule. When the reap stalls — a
/// crashed reaper, a bucket that keeps failing — the entry would expire while
/// the objects are still there, and an instance booting afterwards would walk
/// the bucket and serve data retention had deliberately hidden. Re-announcing
/// keeps the registry alive exactly as long as the work is.
#[tokio::test]
async fn retention_re_announces_deletions_that_have_not_been_reaped() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();
    let staged = build_logs_block(&store, writer, 0xB001, NOW - 90 * DAY, 10).await;

    let (catalog, _tmp) = open_catalog();
    catalog.insert_block(&staged).unwrap();
    // Staged, with a grace window that has not elapsed: hidden from queries,
    // objects still in the bucket, not yet eligible for the reap.
    catalog
        .mark_deleted(&[staged.uuid], NOW, NOW + 10 * DAY)
        .unwrap();

    // No TTL configured, so this pass plans no *new* staging — the only thing
    // it can legitimately emit is the re-announcement.
    let cfg = RetentionConfig {
        default_ttl: None,
        overrides: Default::default(),
        grace: Duration::ZERO,
        apply: true,
    };
    let sink = RecordingSink::default();
    let report = run_retention_pass(
        &LocalLeaseProvider::new(),
        store.clone(),
        &catalog,
        &cfg,
        NOW,
        &sink,
        Duration::from_secs(30),
    )
    .await
    .unwrap();
    assert_eq!(report.candidates, 0, "nothing newly expired");
    assert_eq!(report.reaped, 0, "the grace window has not elapsed");
    assert!(
        catalog.get_block(staged.uuid).unwrap().is_some(),
        "the staged block is still in the catalog"
    );

    let events = sink.0.lock().unwrap();
    let announced: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            BlockEvent::SoftDeleted {
                signal,
                uuids,
                deleted_at_unix_nano,
                delete_eligible_at_unix_nano,
            } => Some((
                signal.clone(),
                uuids.clone(),
                *deleted_at_unix_nano,
                *delete_eligible_at_unix_nano,
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        announced,
        vec![("logs".to_string(), vec![staged.uuid], NOW, NOW + 10 * DAY)],
        "the outstanding deletion is re-announced with its deadline pair intact"
    );
}

/// Once the objects are actually gone, there is nothing left to keep alive, so
/// the pass must stop re-announcing — otherwise the registry would carry an
/// entry for a block that no longer exists anywhere, forever.
#[tokio::test]
async fn a_reaped_deletion_is_no_longer_re_announced() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();
    let staged = build_logs_block(&store, writer, 0xB002, NOW - 90 * DAY, 10).await;

    let (catalog, _tmp) = open_catalog();
    catalog.insert_block(&staged).unwrap();
    // Staged with a grace window that has already elapsed: this pass reaps it.
    catalog
        .mark_deleted(&[staged.uuid], NOW - DAY, NOW - 1)
        .unwrap();

    let cfg = RetentionConfig {
        default_ttl: None,
        overrides: Default::default(),
        grace: Duration::ZERO,
        apply: true,
    };
    let sink = RecordingSink::default();
    let first = run_retention_pass(
        &LocalLeaseProvider::new(),
        store.clone(),
        &catalog,
        &cfg,
        NOW,
        &sink,
        Duration::from_secs(30),
    )
    .await
    .unwrap();
    assert_eq!(first.reaped, 1, "the elapsed grace window is reaped");
    assert!(catalog.get_block(staged.uuid).unwrap().is_none());

    // The next pass has nothing outstanding to announce.
    let sink2 = RecordingSink::default();
    run_retention_pass(
        &LocalLeaseProvider::new(),
        store.clone(),
        &catalog,
        &cfg,
        NOW,
        &sink2,
        Duration::from_secs(30),
    )
    .await
    .unwrap();
    assert!(
        sink2.0.lock().unwrap().is_empty(),
        "a reaped block is not re-announced"
    );
}

// ── D-066: the walk must not re-fetch what the catalog already has ────
//
// gothab's queryd walked 346,386 sidecars per pass, reporting `inserted=0`
// every time: one GET per block, forever, to learn nothing. At ~5 GETs/sec a
// pass took 15-20 hours on a 30-minute timer, so it ran permanently and
// starved live queries of object-store throughput. These tests pin the three
// properties that fix it.

/// An `InMemory` store that records every `get` and can be told to fail one.
#[derive(Debug)]
struct ProbeStore {
    inner: InMemory,
    gets: Mutex<Vec<String>>,
    fail_path: Mutex<Option<String>>,
}

impl ProbeStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            gets: Mutex::new(Vec::new()),
            fail_path: Mutex::new(None),
        }
    }

    /// GETs of block sidecars only — parquet reads are noise for these tests.
    fn meta_gets(&self) -> usize {
        self.gets
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p.ends_with(".meta.json"))
            .count()
    }

    fn reset(&self) {
        self.gets.lock().unwrap().clear();
    }

    fn fail(&self, path: Option<String>) {
        *self.fail_path.lock().unwrap() = path;
    }
}

impl std::fmt::Display for ProbeStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ProbeStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for ProbeStore {
    async fn put_opts(
        &self,
        p: &object_store::path::Path,
        v: object_store::PutPayload,
        o: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(p, v, o).await
    }

    async fn put_multipart_opts(
        &self,
        p: &object_store::path::Path,
        o: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(p, o).await
    }

    async fn get_opts(
        &self,
        p: &object_store::path::Path,
        o: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.gets.lock().unwrap().push(p.as_ref().to_string());
        if self.fail_path.lock().unwrap().as_deref() == Some(p.as_ref()) {
            // Deliberately NOT NotFound: that path means "the block is gone",
            // which is a legitimate outcome the walk already tolerated. This
            // is the "I could not read it" case that used to abort the pass.
            return Err(object_store::Error::Generic {
                store: "ProbeStore",
                source: "injected transient failure".into(),
            });
        }
        self.inner.get_opts(p, o).await
    }

    async fn get_ranges(
        &self,
        p: &object_store::path::Path,
        r: &[std::ops::Range<u64>],
    ) -> object_store::Result<Vec<bytes::Bytes>> {
        self.inner.get_ranges(p, r).await
    }

    fn delete_stream(
        &self,
        paths: futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(paths)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&object_store::path::Path>,
        offset: &object_store::path::Path,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        o: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, o).await
    }

    async fn rename_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        o: object_store::RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, o).await
    }
}

#[tokio::test]
async fn converged_full_walk_fetches_no_sidecars_at_all() {
    let probe = Arc::new(ProbeStore::new());
    let store: Arc<dyn ObjectStore> = probe.clone();
    let writer = Uuid::now_v7();
    build_logs_block(&store, writer, 0xA001, NOW, 20).await;
    build_logs_block(&store, writer, 0xB001, NOW + 50, 20).await;
    build_logs_block(&store, writer, 0xC001, NOW + 100, 20).await;

    let (catalog, _tmp) = open_catalog();

    // Cold: nothing is known, so every sidecar is genuinely needed.
    probe.reset();
    let cold = full_walk(store.as_ref(), &catalog, BUCKET).await.unwrap();
    assert_eq!(cold.inserted, 3, "cold walk discovers all three");
    assert_eq!(cold.skipped, 0, "nothing to skip against an empty catalog");
    assert_eq!(probe.meta_gets(), 3, "cold walk pays one GET per block");

    // Warm: the catalog has all three, and the UUID is readable off the
    // object key, so the pass costs a LIST and *zero* GETs.
    probe.reset();
    let warm = full_walk(store.as_ref(), &catalog, BUCKET).await.unwrap();
    assert_eq!(warm.inserted, 0);
    assert_eq!(warm.skipped, 3, "every listed block was already known");
    assert_eq!(
        probe.meta_gets(),
        0,
        "a converged walk must not fetch a single sidecar"
    );

    // Cursor bookkeeping still happens, derived from the key alone.
    let date = date_dir(NOW);
    assert!(
        catalog.get_cursor("logs", writer, &date).unwrap().is_some(),
        "skipping a fetch must not skip the cursor advance"
    );
}

#[tokio::test]
async fn superseded_and_soft_deleted_blocks_count_as_known() {
    // The skip filter keys off "we have a row", not "we would serve it".
    // Using the live-only `list_blocks` predicate instead would re-fetch every
    // superseded compaction input on every pass forever, and — far worse —
    // re-`insert_block` soft-deleted rows, resurrecting blocks a peer has
    // staged for deletion (D-063) as though they were new discoveries.
    let probe = Arc::new(ProbeStore::new());
    let store: Arc<dyn ObjectStore> = probe.clone();
    let writer = Uuid::now_v7();
    let b1 = build_logs_block(&store, writer, 0xA001, NOW, 20).await;
    let b2 = build_logs_block(&store, writer, 0xB001, NOW + 50, 20).await;
    let b3 = build_logs_block(&store, writer, 0xC001, NOW + 100, 20).await;

    let (catalog, _tmp) = open_catalog();
    full_walk(store.as_ref(), &catalog, BUCKET).await.unwrap();

    // b1 merged away by b3; b2 soft-deleted by a peer's retention pass.
    catalog.mark_superseded(&[b1.uuid], b3.uuid).unwrap();
    catalog
        .mark_deleted(&[b2.uuid], NOW + 500, NOW + 600)
        .unwrap();
    let live_before = catalog.list_blocks().unwrap().len();
    assert_eq!(live_before, 1, "only b3 is live");

    probe.reset();
    let walk = full_walk(store.as_ref(), &catalog, BUCKET).await.unwrap();
    assert_eq!(walk.skipped, 3, "hidden blocks are still *known* blocks");
    assert_eq!(walk.inserted, 0, "nothing is rediscovered");
    assert_eq!(
        probe.meta_gets(),
        0,
        "a superseded or soft-deleted block must not be re-fetched"
    );
    assert_eq!(
        catalog.list_blocks().unwrap().len(),
        live_before,
        "the walk must not resurrect hidden blocks"
    );
}

#[tokio::test]
async fn a_transient_sidecar_failure_neither_aborts_the_pass_nor_skips_the_block() {
    let probe = Arc::new(ProbeStore::new());
    let store: Arc<dyn ObjectStore> = probe.clone();
    let writer = Uuid::now_v7();
    let b1 = build_logs_block(&store, writer, 0xA001, NOW, 20).await;
    let b2 = build_logs_block(&store, writer, 0xB001, NOW + 50, 20).await;
    let b3 = build_logs_block(&store, writer, 0xC001, NOW + 100, 20).await;
    assert!(
        b1.uuid < b2.uuid && b2.uuid < b3.uuid,
        "UUIDv7 is monotonic"
    );

    let (catalog, _tmp) = open_catalog();
    let b2_meta = scry_block::block_path(
        &b2.signal,
        b2.ts_min_unix_nano,
        b2.writer_id,
        b2.uuid,
        "meta.json",
    );
    probe.fail(Some(b2_meta));

    // The pass completes rather than returning Err. Before D-066 one flaky GET
    // aborted the walk and discarded every cursor advance it had earned — on
    // gothab, up to 15 hours of work, three times in two days.
    let r1 = full_walk(store.as_ref(), &catalog, BUCKET).await.unwrap();
    assert_eq!(r1.fetch_failed, 1, "the failure is counted, not fatal");
    assert_eq!(r1.inserted, 2, "the other two blocks still land");
    assert!(catalog.get_block(b2.uuid).unwrap().is_none());

    // And the cursor is held behind the gap. b3 succeeded and sorts *after*
    // b2, so advancing to it would put the incremental poll permanently past
    // a block that never made it into the catalog.
    let date = date_dir(NOW);
    assert_eq!(
        catalog.get_cursor("logs", writer, &date).unwrap(),
        None,
        "a prefix with an unapplied block must not advance its cursor"
    );

    // Cleared failure ⇒ the next pass picks up exactly the missing block.
    probe.fail(None);
    let r2 = full_walk(store.as_ref(), &catalog, BUCKET).await.unwrap();
    assert_eq!(r2.inserted, 1, "the retry recovers b2");
    assert_eq!(
        r2.skipped, 2,
        "and does not re-fetch the two it already has"
    );
    assert!(catalog.get_block(b2.uuid).unwrap().is_some());
    assert_eq!(
        catalog.get_cursor("logs", writer, &date).unwrap(),
        Some(b3.uuid),
        "with the gap filled the cursor advances to the prefix head"
    );
}
