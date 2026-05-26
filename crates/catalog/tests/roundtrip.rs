//! Catalog tests: schema init, insert idempotency, listing, and
//! reconcile against an in-memory object store.

use bytes::Bytes;
use object_store::{memory::InMemory, path::Path as ObjPath, ObjectStore, PutPayload};
use scry_block::BlockMeta;
use scry_catalog::Catalog;
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
        producer_version: "test".into(),
        label_fingerprint_bloom: None,
    }
}

#[test]
fn open_creates_schema_and_is_empty() {
    let tmp = TempDir::new().unwrap();
    let cat = Catalog::open(&tmp.path().join("cat.sqlite"), "scry-dev").unwrap();
    assert_eq!(cat.block_count().unwrap(), 0);
    assert!(cat.list_blocks().unwrap().is_empty());
    assert_eq!(cat.bucket(), "scry-dev");
}

#[test]
fn reopen_preserves_rows() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("cat.sqlite");
    let writer = Uuid::now_v7();
    let uuid = Uuid::now_v7();
    {
        let cat = Catalog::open(&path, "scry-dev").unwrap();
        assert!(cat.insert_block(&meta(uuid, writer, 1_700_000_000_000_000_000, 100)).unwrap());
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
    cat.insert_block(&meta(Uuid::now_v7(), writer, day2, 30)).unwrap();
    cat.insert_block(&meta(Uuid::now_v7(), writer, day1_early, 10)).unwrap();
    cat.insert_block(&meta(Uuid::now_v7(), writer, day1_late, 20)).unwrap();
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
