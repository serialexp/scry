//! Catalog tests: schema init, insert idempotency, listing, and
//! reconcile against an in-memory object store.

use bytes::Bytes;
use object_store::{
    memory::InMemory, path::Path as ObjPath, ObjectStore, ObjectStoreExt, PutPayload,
};
use rusqlite::Connection;
use scry_block::BlockMeta;
use scry_catalog::{Catalog, CATALOG_SCHEMA_VERSION};
use tempfile::TempDir;
use uuid::Uuid;

fn meta(uuid: Uuid, writer: Uuid, ts_min: u64, rows: u64) -> BlockMeta {
    BlockMeta {
        uuid,
        signal: "dummy".into(),
        writer_id: writer,
        ts_min_unix_nano: ts_min,
        ts_max_unix_nano: ts_min + 10_000_000_000,
        row_count: rows,
        byte_size: rows * 64,
        schema_version: 1,
        level: 0,
        producer_version: "test".into(),
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

/// D-054: inserting a block advances the persistent WAL high-water
/// atomically, monotonically, and per-`(writer, signal, shard)`; and the
/// `wal_seg_max`/`wal_shard` columns round-trip through `list_blocks`.
#[test]
fn wal_watermark_advances_monotonically_per_instance() {
    let tmp = TempDir::new().unwrap();
    let cat = Catalog::open(&tmp.path().join("cat.sqlite"), "scry-dev").unwrap();
    let writer = Uuid::now_v7();
    let ts = 1_700_000_000_000_000_000;

    let with_wm = |seg: u64, shard: u32| {
        let mut m = meta(Uuid::now_v7(), writer, ts, 10);
        m.signal = "logs".into();
        m.wal_seg_max = Some(seg);
        m.wal_shard = Some(shard);
        m
    };

    // No blocks yet → absent (treated as 0 by the dedup).
    assert_eq!(cat.get_watermark(writer, "logs", 0).unwrap(), None);

    // First block on shard 0 at seg 5 sets the high-water.
    cat.insert_block(&with_wm(5, 0)).unwrap();
    assert_eq!(cat.get_watermark(writer, "logs", 0).unwrap(), Some(5));

    // A later (higher) segment advances it.
    cat.insert_block(&with_wm(9, 0)).unwrap();
    assert_eq!(cat.get_watermark(writer, "logs", 0).unwrap(), Some(9));

    // An out-of-order (lower) segment must NOT roll it back.
    cat.insert_block(&with_wm(7, 0)).unwrap();
    assert_eq!(cat.get_watermark(writer, "logs", 0).unwrap(), Some(9));

    // A different shard is an independent instance.
    assert_eq!(cat.get_watermark(writer, "logs", 3).unwrap(), None);
    cat.insert_block(&with_wm(2, 3)).unwrap();
    assert_eq!(cat.get_watermark(writer, "logs", 3).unwrap(), Some(2));
    assert_eq!(cat.get_watermark(writer, "logs", 0).unwrap(), Some(9));

    // A different signal is independent too.
    assert_eq!(cat.get_watermark(writer, "metrics", 0).unwrap(), None);

    // The columns round-trip through the catalog.
    let rows = cat.list_blocks().unwrap();
    let one = rows
        .iter()
        .find(|r| r.meta.wal_seg_max == Some(5) && r.meta.wal_shard == Some(0))
        .expect("seg=5 shard=0 block present with watermark columns");
    assert_eq!(one.meta.wal_shard, Some(0));

    // Direct advance_watermark is also monotonic (used by convergence).
    cat.advance_watermark(writer, "logs", 0, 4).unwrap();
    assert_eq!(cat.get_watermark(writer, "logs", 0).unwrap(), Some(9));
    cat.advance_watermark(writer, "logs", 0, 42).unwrap();
    assert_eq!(cat.get_watermark(writer, "logs", 0).unwrap(), Some(42));
}

#[test]
fn open_creates_schema_and_is_empty() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("cat.sqlite");
    let cat = Catalog::open(&path, "scry-dev").unwrap();
    assert_eq!(cat.block_count().unwrap(), 0);
    assert!(cat.list_blocks().unwrap().is_empty());
    assert_eq!(cat.bucket(), "scry-dev");
    drop(cat);

    let conn = Connection::open(path).unwrap();
    let version: u32 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, CATALOG_SCHEMA_VERSION);
}

#[test]
fn open_upgrades_v1_blocks_table_before_stamping_current_version() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("cat.sqlite");
    let conn = Connection::open(&path).unwrap();
    let live = Uuid::now_v7();
    let input = Uuid::now_v7();
    let output = Uuid::now_v7();
    let writer = Uuid::now_v7();
    conn.execute_batch(&format!(
        r#"
        PRAGMA foreign_keys = ON;
        CREATE TABLE blocks (
          uuid TEXT PRIMARY KEY, bucket TEXT NOT NULL, signal TEXT NOT NULL,
          date TEXT NOT NULL, writer_id TEXT NOT NULL, level INTEGER NOT NULL DEFAULT 0,
          ts_min INTEGER NOT NULL, ts_max INTEGER NOT NULL, row_count INTEGER NOT NULL,
          byte_size INTEGER NOT NULL, postings_size_bytes INTEGER,
          has_postings INTEGER NOT NULL DEFAULT 0, body_bloom_size_bytes INTEGER,
          has_body_bloom INTEGER NOT NULL DEFAULT 0, schema_version INTEGER NOT NULL,
          fingerprint BLOB, superseded_by TEXT REFERENCES blocks(uuid), deleted_at INTEGER
        );
        INSERT INTO blocks VALUES
          ('{live}', 'scry-dev', 'logs', '2023-11-14', '{writer}', 0,
           1, 2, 10, 100, NULL, 0, NULL, 0, 1, NULL, NULL, NULL),
          ('{output}', 'scry-dev', 'logs', '2023-11-14', '{writer}', 1,
           1, 2, 10, 100, NULL, 0, NULL, 0, 1, NULL, NULL, NULL),
          ('{input}', 'scry-dev', 'logs', '2023-11-14', '{writer}', 0,
           1, 2, 10, 100, NULL, 0, NULL, 0, 1, NULL, '{output}', NULL);
        PRAGMA user_version = 1;
        "#
    ))
    .unwrap();
    drop(conn);

    let cat = Catalog::open(&path, "scry-dev").unwrap();
    let live_rows = cat.list_blocks().unwrap();
    assert_eq!(live_rows.len(), 2);
    assert!(live_rows.iter().any(|entry| entry.meta.uuid == live));
    assert!(live_rows.iter().any(|entry| entry.meta.uuid == output));
    assert_eq!(cat.list_pending_reaps(0).unwrap().len(), 1);
    assert_eq!(cat.list_pending_reaps(0).unwrap()[0].entry.meta.uuid, input);
    drop(cat);

    let conn = Connection::open(path).unwrap();
    let columns: Vec<String> = conn
        .prepare("PRAGMA table_info(blocks)")
        .unwrap()
        .query_map([], |row| row.get(1))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert!(columns.iter().any(|name| name == "wal_seg_max"));
    assert!(columns.iter().any(|name| name == "wal_shard"));
    assert!(columns.iter().any(|name| name == "superseded"));
    let foreign_keys: usize = conn
        .prepare("PRAGMA foreign_key_list(blocks)")
        .unwrap()
        .query_map([], |_| Ok(()))
        .unwrap()
        .count();
    assert_eq!(foreign_keys, 0, "v3 removes the self-referential FK");
    let version: u32 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, CATALOG_SCHEMA_VERSION);
}

#[test]
fn reopen_preserves_rows() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("cat.sqlite");
    let writer = Uuid::now_v7();
    let uuid = Uuid::now_v7();
    {
        let cat = Catalog::open(&path, "scry-dev").unwrap();
        assert!(cat
            .insert_block(&meta(uuid, writer, 1_700_000_000_000_000_000, 100))
            .unwrap());
    }
    let cat = Catalog::open(&path, "scry-dev").unwrap();
    assert_eq!(cat.block_count().unwrap(), 1);
    let rows = cat.list_blocks().unwrap();
    assert_eq!(rows[0].meta.uuid, uuid);
    assert_eq!(rows[0].meta.row_count, 100);
    assert_eq!(rows[0].bucket, "scry-dev");
    assert_eq!(rows[0].level, 0);
    assert_eq!(rows[0].date, "2023-11-14"); // ts_min derived
}

#[test]
fn insert_is_idempotent_on_uuid() {
    let tmp = TempDir::new().unwrap();
    let cat = Catalog::open(&tmp.path().join("cat.sqlite"), "scry-dev").unwrap();
    let writer = Uuid::now_v7();
    let uuid = Uuid::now_v7();
    let m = meta(uuid, writer, 1_700_000_000_000_000_000, 100);
    assert!(cat.insert_block(&m).unwrap(), "first insert is new");
    // Re-inserting the same uuid is a no-op (returns false) — blocks
    // are immutable, the existing row wins.
    assert!(!cat.insert_block(&m).unwrap(), "second insert is a no-op");
    assert_eq!(cat.block_count().unwrap(), 1);
}

#[test]
fn list_orders_by_date_then_ts_min() {
    let tmp = TempDir::new().unwrap();
    let cat = Catalog::open(&tmp.path().join("cat.sqlite"), "scry-dev").unwrap();
    let writer = Uuid::now_v7();
    // Three blocks across two days; ensure they come back in
    // ascending ts_min order.
    let day1_early = 1_700_000_000_000_000_000;
    let day1_late = day1_early + 3_600_000_000_000;
    let day2 = day1_early + 86_400_000_000_000;
    cat.insert_block(&meta(Uuid::now_v7(), writer, day2, 30))
        .unwrap();
    cat.insert_block(&meta(Uuid::now_v7(), writer, day1_early, 10))
        .unwrap();
    cat.insert_block(&meta(Uuid::now_v7(), writer, day1_late, 20))
        .unwrap();
    let rows = cat.list_blocks().unwrap();
    let counts: Vec<u64> = rows.iter().map(|r| r.meta.row_count).collect();
    assert_eq!(counts, vec![10, 20, 30]);
}

#[test]
fn get_block_returns_none_for_unknown() {
    let tmp = TempDir::new().unwrap();
    let cat = Catalog::open(&tmp.path().join("cat.sqlite"), "scry-dev").unwrap();
    assert!(cat.get_block(Uuid::now_v7()).unwrap().is_none());
}

#[test]
fn insert_honours_meta_level() {
    // The compactor writes blocks at level > 0; the level must survive
    // the insert (and a reconcile, which goes through insert_block too)
    // rather than being reset to 0.
    let tmp = TempDir::new().unwrap();
    let cat = Catalog::open(&tmp.path().join("cat.sqlite"), "scry-dev").unwrap();
    let writer = Uuid::now_v7();
    let uuid = Uuid::now_v7();
    let mut m = meta(uuid, writer, 1_700_000_000_000_000_000, 100);
    m.level = 2;
    cat.insert_block(&m).unwrap();
    let rows = cat.list_blocks().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].level, 2);
    assert_eq!(rows[0].meta.level, 2);
}

#[test]
fn lineage_replay_is_order_independent_and_resolves_intermediate() {
    let tmp = TempDir::new().unwrap();
    let cat = Catalog::open(&tmp.path().join("cat.sqlite"), "scry-dev").unwrap();
    let writer = Uuid::now_v7();
    let ts = 1_700_000_000_000_000_000;
    let leaf = Uuid::now_v7();
    let intermediate = Uuid::now_v7();
    let terminal = Uuid::now_v7();

    let mut b17 = meta(terminal, writer, ts, 10);
    b17.level = 2;
    b17.compacted_from = vec![leaf, intermediate];
    b17.compacted_from.sort_unstable();
    cat.insert_block(&b17).unwrap();

    let mut b9 = meta(intermediate, writer, ts, 10);
    b9.level = 1;
    b9.compacted_from = vec![leaf];
    cat.insert_block(&b9).unwrap();
    cat.insert_block(&meta(leaf, writer, ts, 10)).unwrap();

    assert_eq!(
        cat.resolve_terminal(leaf).unwrap(),
        scry_catalog::TerminalResolution::Unique(terminal)
    );
    assert_eq!(
        cat.resolve_terminal(intermediate).unwrap(),
        scry_catalog::TerminalResolution::Unique(terminal)
    );
    let live = cat.list_blocks().unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].meta.uuid, terminal);
}

#[test]
fn incomparable_lineage_claims_fail_closed_as_fork() {
    let tmp = TempDir::new().unwrap();
    let cat = Catalog::open(&tmp.path().join("cat.sqlite"), "scry-dev").unwrap();
    let writer = Uuid::now_v7();
    let ts = 1_700_000_000_000_000_000;
    let leaf = Uuid::now_v7();
    for _ in 0..2 {
        let mut output = meta(Uuid::now_v7(), writer, ts, 10);
        output.level = 1;
        output.compacted_from = vec![leaf];
        cat.insert_block(&output).unwrap();
    }
    assert!(matches!(
        cat.resolve_terminal(leaf).unwrap(),
        scry_catalog::TerminalResolution::Fork(ids) if ids.len() == 2
    ));
}

#[test]
fn stable_partition_pruning_keeps_extant_terminal_claims_only() {
    let tmp = TempDir::new().unwrap();
    let cat = Catalog::open(&tmp.path().join("cat.sqlite"), "scry-dev").unwrap();
    let writer = Uuid::now_v7();
    let ts = 1_700_000_000_000_000_000;
    let leaf = Uuid::now_v7();
    let intermediate = Uuid::now_v7();
    let terminal = Uuid::now_v7();

    let mut middle = meta(intermediate, writer, ts, 1);
    middle.level = 1;
    middle.compacted_from = vec![leaf];
    cat.insert_block(&middle).unwrap();

    let mut top = meta(terminal, writer, ts, 1);
    top.level = 2;
    top.compacted_from = vec![leaf, intermediate];
    cat.insert_block(&top).unwrap();
    assert_eq!(cat.lineage_row_count().unwrap(), 3);

    let date = scry_catalog::date_dir(ts);
    assert_eq!(
        cat.prune_lineage_partition("dummy", &date, &[terminal])
            .unwrap(),
        1,
        "only the edge whose descendant sidecar disappeared is stale"
    );
    assert_eq!(cat.lineage_row_count().unwrap(), 2);
    assert_eq!(
        cat.resolve_terminal(leaf).unwrap(),
        scry_catalog::TerminalResolution::Unique(terminal)
    );
    assert_eq!(
        cat.resolve_terminal(intermediate).unwrap(),
        scry_catalog::TerminalResolution::Unique(terminal)
    );
}

#[test]
fn superseded_blocks_drop_out_of_list_blocks() {
    // The compaction supersede → delete lifecycle: once inputs point at
    // their merged replacement they must vanish from the query set, and
    // delete_blocks then drops the rows entirely.
    let tmp = TempDir::new().unwrap();
    let cat = Catalog::open(&tmp.path().join("cat.sqlite"), "scry-dev").unwrap();
    let writer = Uuid::now_v7();
    let ts = 1_700_000_000_000_000_000;

    let in_a = Uuid::now_v7();
    let in_b = Uuid::now_v7();
    cat.insert_block(&meta(in_a, writer, ts, 10)).unwrap();
    cat.insert_block(&meta(in_b, writer, ts, 20)).unwrap();

    // Merged block at the next level, then supersede the two inputs.
    let merged = Uuid::now_v7();
    let mut merged_meta = meta(merged, writer, ts, 30);
    merged_meta.level = 1;
    cat.insert_block(&merged_meta).unwrap();
    cat.mark_superseded(&[in_a, in_b], merged).unwrap();
    let pending = cat.list_pending_reaps(0).unwrap();
    assert_eq!(pending.len(), 2, "both inputs are durable pending reaps");

    // Queries now see only the merged block; the inputs are hidden but
    // their rows still exist (grace window) — block_count counts them.
    let live = cat.list_blocks().unwrap();
    assert_eq!(live.len(), 1, "only the merged block is live");
    assert_eq!(live[0].meta.uuid, merged);
    assert_eq!(live[0].level, 1);

    // After the objects are deleted, drop the input rows.
    cat.delete_blocks(&[in_a, in_b]).unwrap();
    assert_eq!(cat.list_blocks().unwrap().len(), 1);
    assert!(cat.get_block(in_a).unwrap().is_none());
    assert!(cat.get_block(in_b).unwrap().is_none());
    assert!(cat.get_block(merged).unwrap().is_some());
}

#[test]
fn marked_deleted_blocks_drop_out_of_list_blocks() {
    // The retention reaper soft-deletes via `mark_deleted` so queries
    // stop listing a block before its objects are removed (the grace
    // window). `get_block` still finds the row until `delete_blocks`.
    let tmp = TempDir::new().unwrap();
    let cat = Catalog::open(&tmp.path().join("cat.sqlite"), "scry-dev").unwrap();
    let writer = Uuid::now_v7();
    let ts = 1_700_000_000_000_000_000;

    let a = Uuid::now_v7();
    let b = Uuid::now_v7();
    cat.insert_block(&meta(a, writer, ts, 10)).unwrap();
    cat.insert_block(&meta(b, writer, ts, 20)).unwrap();
    assert_eq!(cat.list_blocks().unwrap().len(), 2);

    // Soft-delete A: it leaves the live set immediately, but the row
    // (and so block_count) still exists during the grace window.
    cat.mark_deleted(&[a], ts + 1, ts + 1).unwrap();
    let live = cat.list_blocks().unwrap();
    assert_eq!(live.len(), 1, "marked block is hidden from queries");
    assert_eq!(live[0].meta.uuid, b);
    assert!(
        cat.get_block(a).unwrap().is_some(),
        "row survives until delete_blocks"
    );

    // Hard delete drops the row.
    cat.delete_blocks(&[a]).unwrap();
    assert!(cat.get_block(a).unwrap().is_none());
    assert_eq!(cat.list_blocks().unwrap().len(), 1);
}

/// A soft-deleted block stays discoverable as *pending deletion work*
/// until its durable grace deadline passes — and then indefinitely, until
/// something actually reaps it.
///
/// This is what makes an interrupted grace window recoverable. Before the
/// deadline was persisted, a crash mid-grace left the row invisible to
/// `list_blocks` (deleted) and to the planner (never re-selected), so its
/// objects leaked forever.
#[test]
fn pending_deletions_appear_only_once_their_grace_has_elapsed() {
    let tmp = TempDir::new().unwrap();
    let cat = Catalog::open(&tmp.path().join("cat.sqlite"), "scry-dev").unwrap();
    let writer = Uuid::now_v7();
    let ts = 1_700_000_000_000_000_000;

    let a = Uuid::now_v7();
    cat.insert_block(&meta(a, writer, ts, 10)).unwrap();

    let deleted_at = ts + 1;
    let eligible_at = deleted_at + 600_000_000_000; // +600s grace
    cat.mark_deleted(&[a], deleted_at, eligible_at).unwrap();

    // Mid-grace: hidden from queries, but not yet reapable.
    assert!(cat.list_blocks().unwrap().is_empty());
    assert!(
        cat.list_pending_deletions(eligible_at - 1)
            .unwrap()
            .is_empty(),
        "a block inside its grace window must not be reaped yet"
    );

    // At the deadline it becomes eligible (boundary is inclusive).
    let due = cat.list_pending_deletions(eligible_at).unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].meta.uuid, a);

    // It stays pending across restarts/passes until actually reaped —
    // the property the old in-process sleep could not provide.
    assert_eq!(
        cat.list_pending_deletions(eligible_at + 1_000_000_000)
            .unwrap()
            .len(),
        1,
        "pending work must persist until it is done"
    );

    cat.delete_blocks(&[a]).unwrap();
    assert!(cat.list_pending_deletions(u64::MAX).unwrap().is_empty());
}

/// Re-staging a block already awaiting deletion never shortens the grace
/// window a concurrent reader may be relying on, and never rewrites the
/// original `deleted_at`.
#[test]
fn restaging_a_pending_deletion_only_extends_its_grace() {
    let tmp = TempDir::new().unwrap();
    let cat = Catalog::open(&tmp.path().join("cat.sqlite"), "scry-dev").unwrap();
    let writer = Uuid::now_v7();
    let ts = 1_700_000_000_000_000_000;

    let a = Uuid::now_v7();
    cat.insert_block(&meta(a, writer, ts, 10)).unwrap();

    let first_eligible = ts + 1_000;
    cat.mark_deleted(&[a], ts, first_eligible).unwrap();

    // An earlier deadline must not win.
    cat.mark_deleted(&[a], ts + 500, first_eligible - 500)
        .unwrap();
    assert!(
        cat.list_pending_deletions(first_eligible - 1)
            .unwrap()
            .is_empty(),
        "a re-stage must not pull the deadline earlier"
    );

    // A later one does.
    let later = first_eligible + 5_000;
    cat.mark_deleted(&[a], ts + 600, later).unwrap();
    assert!(
        cat.list_pending_deletions(later - 1).unwrap().is_empty(),
        "the extended deadline should now apply"
    );
    assert_eq!(cat.list_pending_deletions(later).unwrap().len(), 1);
}

#[tokio::test]
async fn reconcile_walks_bucket_and_upserts_sidecars() {
    let tmp = TempDir::new().unwrap();
    let cat = Catalog::open(&tmp.path().join("cat.sqlite"), "scry-dev").unwrap();
    let store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());

    // Plant three sidecars in the bucket. Also drop a non-sidecar
    // object and one malformed sidecar — they should be observed
    // (malformed → failed; non-meta.json → ignored entirely).
    let writer = Uuid::now_v7();
    let metas: Vec<BlockMeta> = (0..3)
        .map(|i| {
            meta(
                Uuid::now_v7(),
                writer,
                1_700_000_000_000_000_000 + i * 86_400_000_000_000,
                10 * (i + 1),
            )
        })
        .collect();
    for m in &metas {
        let path = ObjPath::from(scry_block::block_path(
            &m.signal,
            m.ts_min_unix_nano,
            m.writer_id,
            m.uuid,
            "meta.json",
        ));
        let body = serde_json::to_vec_pretty(m).unwrap();
        store.put(&path, PutPayload::from(body)).await.unwrap();
    }
    // Decoy parquet — should be ignored by the reconciler.
    store
        .put(
            &ObjPath::from("dummy/2025/01/01/abc/def.parquet"),
            PutPayload::from(Bytes::from_static(b"not a sidecar")),
        )
        .await
        .unwrap();
    // Malformed sidecar — should bump `failed`.
    store
        .put(
            &ObjPath::from("dummy/2025/01/01/abc/bad.meta.json"),
            PutPayload::from(Bytes::from_static(b"{not-json")),
        )
        .await
        .unwrap();

    let report = cat.reconcile_from_bucket(store.as_ref()).await.unwrap();
    assert_eq!(report.seen, 4, "three good + one malformed sidecar = 4");
    assert_eq!(report.inserted, 3);
    assert_eq!(report.already_present, 0);
    assert_eq!(report.failed, 1);
    assert_eq!(cat.block_count().unwrap(), 3);

    // Second reconcile pass: everything is `already_present`, nothing
    // newly inserted. Idempotency under repeated bootstrap.
    let again = cat.reconcile_from_bucket(store.as_ref()).await.unwrap();
    assert_eq!(again.inserted, 0);
    assert_eq!(again.already_present, 3);
    assert_eq!(again.failed, 1);
}

fn pairs(items: &[(&str, &str)]) -> Vec<(String, String)> {
    items
        .iter()
        .map(|(n, v)| (n.to_string(), v.to_string()))
        .collect()
}

#[test]
fn label_cache_warm_enumerate_and_reap() {
    let tmp = TempDir::new().unwrap();
    let cat = Catalog::open(&tmp.path().join("cat.sqlite"), "scry-dev").unwrap();
    let a = Uuid::now_v7();
    let b = Uuid::now_v7();
    let cold = Uuid::now_v7();

    // Nothing warmed yet.
    assert!(cat.warmed_blocks(&[a, b, cold]).unwrap().is_empty());

    // Warm two blocks with overlapping + distinct labels.
    cat.upsert_block_labels(a, &pairs(&[("service", "api"), ("env", "prod")]))
        .unwrap();
    cat.upsert_block_labels(b, &pairs(&[("service", "web"), ("env", "prod")]))
        .unwrap();

    let warmed = cat.warmed_blocks(&[a, b, cold]).unwrap();
    assert!(warmed.contains(&a) && warmed.contains(&b) && !warmed.contains(&cold));

    // Names: union across a+b, deduped + sorted.
    assert_eq!(
        cat.distinct_label_names(&[a, b]).unwrap(),
        vec!["env".to_string(), "service".to_string()]
    );
    // Values scoped to blocks: querying only `a` sees only its service value.
    assert_eq!(
        cat.distinct_label_values("service", &[a]).unwrap(),
        vec!["api"]
    );
    // Across both, deduped + sorted.
    assert_eq!(
        cat.distinct_label_values("service", &[a, b]).unwrap(),
        vec!["api".to_string(), "web".to_string()]
    );
    assert_eq!(
        cat.distinct_label_values("env", &[a, b]).unwrap(),
        vec!["prod"]
    );
    // Empty candidate set → empty result (no panic on IN ()).
    assert!(cat.distinct_label_names(&[]).unwrap().is_empty());

    // Idempotent re-warm doesn't duplicate.
    cat.upsert_block_labels(a, &pairs(&[("service", "api"), ("env", "prod")]))
        .unwrap();
    assert_eq!(
        cat.distinct_label_values("service", &[a]).unwrap(),
        vec!["api"]
    );

    // Deleting a block reaps its label rows + warmed marker.
    cat.delete_blocks(&[a]).unwrap();
    assert!(!cat.warmed_blocks(&[a]).unwrap().contains(&a));
    assert_eq!(
        cat.distinct_label_values("service", &[a]).unwrap(),
        Vec::<String>::new()
    );
    // b is untouched.
    assert_eq!(
        cat.distinct_label_values("service", &[b]).unwrap(),
        vec!["web"]
    );
}

#[test]
fn label_cache_warms_a_label_less_block() {
    // A block with zero labels must still be recorded as warmed, so the
    // metadata handler doesn't rescan its postings on every request.
    let tmp = TempDir::new().unwrap();
    let cat = Catalog::open(&tmp.path().join("cat.sqlite"), "scry-dev").unwrap();
    let u = Uuid::now_v7();
    cat.upsert_block_labels(u, &[]).unwrap();
    assert!(cat.warmed_blocks(&[u]).unwrap().contains(&u));
    assert!(cat.distinct_label_names(&[u]).unwrap().is_empty());
}

#[test]
fn poll_cursor_absent_then_advances_monotonically() {
    let tmp = TempDir::new().unwrap();
    let cat = Catalog::open(&tmp.path().join("cat.sqlite"), "scry-dev").unwrap();
    let writer = Uuid::now_v7();

    // No cursor yet for an unseen partition.
    assert_eq!(
        cat.get_cursor("metrics", writer, "2026-05-30").unwrap(),
        None
    );

    // First observation sets it.
    let u1 = Uuid::now_v7();
    cat.advance_cursor("metrics", writer, "2026-05-30", u1)
        .unwrap();
    assert_eq!(
        cat.get_cursor("metrics", writer, "2026-05-30").unwrap(),
        Some(u1)
    );

    // A newer (lexically-greater, since v7) UUID advances the cursor.
    let u2 = Uuid::now_v7();
    assert!(u2 > u1, "v7 UUIDs minted later sort greater");
    cat.advance_cursor("metrics", writer, "2026-05-30", u2)
        .unwrap();
    assert_eq!(
        cat.get_cursor("metrics", writer, "2026-05-30").unwrap(),
        Some(u2)
    );

    // Re-applying an older observation is a no-op (monotonic high-water mark).
    cat.advance_cursor("metrics", writer, "2026-05-30", u1)
        .unwrap();
    assert_eq!(
        cat.get_cursor("metrics", writer, "2026-05-30").unwrap(),
        Some(u2),
        "an out-of-order older UUID must not roll the cursor backward"
    );
}

#[test]
fn poll_cursors_are_keyed_per_signal_writer_date_and_listed() {
    let tmp = TempDir::new().unwrap();
    let cat = Catalog::open(&tmp.path().join("cat.sqlite"), "scry-dev").unwrap();
    let w1 = Uuid::now_v7();
    let w2 = Uuid::now_v7();

    cat.advance_cursor("metrics", w1, "2026-05-30", Uuid::now_v7())
        .unwrap();
    cat.advance_cursor("logs", w1, "2026-05-30", Uuid::now_v7())
        .unwrap();
    cat.advance_cursor("metrics", w2, "2026-05-30", Uuid::now_v7())
        .unwrap();
    cat.advance_cursor("metrics", w1, "2026-05-29", Uuid::now_v7())
        .unwrap();

    // Distinct keys → four independent cursors.
    let mut cursors = cat.list_cursors().unwrap();
    assert_eq!(cursors.len(), 4);

    // Each is individually retrievable; a different key is absent.
    assert!(cat
        .get_cursor("metrics", w1, "2026-05-30")
        .unwrap()
        .is_some());
    assert!(cat
        .get_cursor("traces", w1, "2026-05-30")
        .unwrap()
        .is_none());

    // list_cursors round-trips the (signal, writer, date) tuples.
    cursors.sort();
    assert!(cursors.contains(&("logs".to_string(), w1, "2026-05-30".to_string())));
    assert!(cursors.contains(&("metrics".to_string(), w2, "2026-05-30".to_string())));
}

/// Lineage learned from the *bucket* (a merged block's `compacted_from`, as
/// `reconcile_from_bucket` replays it) hides the inputs from queries but does
/// **not** schedule their objects for deletion — `list_pending_reaps` requires
/// a `reap_eligible_at`, which only `mark_superseded` or an explicit staging
/// call sets.
///
/// This is the crash-recovery shape: a merge PUT its `meta.json` (the commit
/// point) and died before publishing. Whoever reconciles next must stage the
/// recovered inputs, or their objects leak with nothing left to notice them.
#[test]
fn lineage_recovered_from_the_bucket_needs_explicit_reap_staging() {
    let tmp = TempDir::new().unwrap();
    let cat = Catalog::open(&tmp.path().join("cat.sqlite"), "scry-dev").unwrap();
    let writer = Uuid::now_v7();
    let ts = 1_700_000_000_000_000_000;

    let in_a = Uuid::now_v7();
    let in_b = Uuid::now_v7();
    cat.insert_block(&meta(in_a, writer, ts, 10)).unwrap();
    cat.insert_block(&meta(in_b, writer, ts, 20)).unwrap();

    // The merged block arrives carrying its ancestry, exactly as a sidecar
    // read back off the bucket would.
    let merged = Uuid::now_v7();
    let mut merged_meta = meta(merged, writer, ts, 30);
    merged_meta.level = 1;
    merged_meta.compacted_from = {
        let mut v = vec![in_a, in_b];
        v.sort_unstable();
        v
    };
    cat.insert_block(&merged_meta).unwrap();

    // Query correctness is already right: inputs are hidden, merged is live.
    let live = cat.list_blocks().unwrap();
    assert_eq!(live.len(), 1, "inputs superseded by lineage");
    assert_eq!(live[0].meta.uuid, merged);

    // But physical cleanup is not scheduled — the leak.
    assert!(
        cat.list_pending_reaps(u64::MAX).unwrap().is_empty(),
        "lineage alone must not schedule deletion"
    );

    // Staging is what closes the gap.
    let staged = cat.stage_unstaged_superseded(ts).unwrap();
    assert_eq!(staged, 2, "both recovered inputs staged");
    let mut pending: Vec<Uuid> = cat
        .list_pending_reaps(ts)
        .unwrap()
        .into_iter()
        .map(|p| p.entry.meta.uuid)
        .collect();
    pending.sort_unstable();
    let mut expect = vec![in_a, in_b];
    expect.sort_unstable();
    assert_eq!(pending, expect);

    // Idempotent: a second staging pass must not move an existing deadline.
    assert_eq!(
        cat.stage_unstaged_superseded(ts + 10_000).unwrap(),
        0,
        "already-staged rows keep their original deadline"
    );
    assert_eq!(cat.list_pending_reaps(ts).unwrap().len(), 2);
}
