//! Catalog snapshot bootstrap (D-055).
//!
//! Cold-starting a query daemon by walking every `*.meta.json` in the bucket
//! costs one network GET per block — O(total blocks), every boot (see
//! [`crate::Catalog::reconcile_from_bucket`]). Instead, a writer with an
//! authoritative catalog periodically uploads it as **one object**
//! ([`SNAPSHOT_KEY`]) and a cold consumer **restores that one object** on boot,
//! then lets incremental cursor-polling fill the small delta since the snapshot.
//!
//! The snapshot is a plain SQLite file produced with `VACUUM INTO` (a
//! consistent, defragmented copy taken through a read transaction, safe against
//! concurrent WAL readers/writers on the live catalog). It is uploaded as a
//! single object, so a reader always sees a whole, valid catalog — a torn read
//! is impossible.
//!
//! ## Cross-version safety
//!
//! The catalog schema has no `ALTER`-based migrations
//! ([`crate::Catalog`]'s `init_schema` is `CREATE TABLE IF NOT EXISTS` only),
//! which is safe *only* because the catalog is normally created fresh by the
//! current binary. Snapshots introduce cross-version persistence, so the schema
//! version travels inside the db as `PRAGMA user_version` ([`CATALOG_SCHEMA_VERSION`])
//! and [`restore_snapshot`] refuses a snapshot whose version doesn't match the
//! running binary — the caller then falls back to a full reconcile. Bump
//! [`CATALOG_SCHEMA_VERSION`] whenever the DDL changes.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use object_store::{path::Path as ObjPath, ObjectStore, ObjectStoreExt, PutPayload};
use rusqlite::{Connection, OpenFlags};

/// Object key of the canonical catalog snapshot in the bucket. The `_catalog/`
/// prefix is reserved: it is deliberately excluded from every block walk
/// (`reconcile_from_bucket`, the cluster `poll_once`/`full_walk`) so a snapshot
/// object is never mis-parsed as a block sidecar.
pub const SNAPSHOT_KEY: &str = "_catalog/snapshot.sqlite";

/// Monotonic version of the catalog's on-disk SQLite schema, stamped into every
/// catalog as `PRAGMA user_version`. Bump this whenever the DDL in
/// `Catalog::init_schema` changes so a snapshot written by an older binary is
/// rejected by a newer consumer (which then rebuilds via reconcile) rather than
/// opened with missing columns.
pub const CATALOG_SCHEMA_VERSION: u32 = 1;

/// Result of [`save_snapshot`].
#[derive(Debug, Clone)]
pub struct SaveReport {
    /// Size in bytes of the uploaded snapshot object.
    pub bytes: u64,
}

/// Result of [`restore_snapshot`]. Only `Restored` leaves a catalog file at the
/// target path; the other outcomes leave the target untouched so the caller can
/// fall back to a full reconcile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestoreOutcome {
    /// A snapshot was downloaded, version-checked, and moved into place.
    Restored {
        /// Live block rows in the restored catalog (best-effort, for logging).
        blocks: u64,
    },
    /// No snapshot object exists in the bucket yet (first-ever boot).
    NoSnapshot,
    /// A snapshot exists but its schema version doesn't match this binary.
    VersionMismatch { found: u32, expected: u32 },
}

/// Take a consistent copy of the catalog at `catalog_path` and upload it to
/// [`SNAPSHOT_KEY`], overwriting any previous snapshot (a single atomic PUT).
///
/// Non-destructive: it only reads the catalog (via a separate read-only
/// connection, never the shared one) and overwrites one object. Safe to run
/// without any lease — concurrent writers just last-writer-win on the key, and
/// every copy is a valid full catalog.
pub async fn save_snapshot(catalog_path: &Path, store: &dyn ObjectStore) -> Result<SaveReport> {
    let src = catalog_path.to_path_buf();
    let tmp = tmp_sibling(catalog_path, "snapshot.tmp");
    let tmp_for_task = tmp.clone();
    tokio::task::spawn_blocking(move || vacuum_into(&src, &tmp_for_task))
        .await
        .context("joining snapshot VACUUM INTO task")??;

    let data =
        std::fs::read(&tmp).with_context(|| format!("reading snapshot temp {}", tmp.display()))?;
    let bytes = data.len() as u64;
    let key = ObjPath::from(SNAPSHOT_KEY);
    let put_res = store.put(&key, PutPayload::from(data)).await;
    // Best-effort cleanup regardless of PUT outcome.
    let _ = std::fs::remove_file(&tmp);
    put_res.with_context(|| format!("PUT {SNAPSHOT_KEY}"))?;
    Ok(SaveReport { bytes })
}

/// `VACUUM main INTO <tmp>` from a fresh read-only connection to `src`. The
/// target must not already exist (SQLite errors otherwise), so it's removed
/// first.
fn vacuum_into(src: &Path, tmp: &Path) -> Result<()> {
    let _ = std::fs::remove_file(tmp);
    let conn = Connection::open_with_flags(
        src,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .with_context(|| format!("opening catalog {} for snapshot", src.display()))?;
    let tmp_str = tmp
        .to_str()
        .context("snapshot temp path is not valid UTF-8")?;
    conn.execute("VACUUM main INTO ?1", [tmp_str])
        .context("VACUUM INTO snapshot temp")?;
    Ok(())
}

/// Download the catalog snapshot from [`SNAPSHOT_KEY`] and, if its schema
/// version matches `expected_version`, move it into place at `catalog_path`.
///
/// Returns without touching `catalog_path` when there is no snapshot or the
/// version doesn't match — the caller falls back to a full reconcile.
pub async fn restore_snapshot(
    catalog_path: &Path,
    store: &dyn ObjectStore,
    expected_version: u32,
) -> Result<RestoreOutcome> {
    let key = ObjPath::from(SNAPSHOT_KEY);
    let data = match store.get(&key).await {
        Ok(g) => g
            .bytes()
            .await
            .with_context(|| format!("reading {SNAPSHOT_KEY} body"))?,
        Err(object_store::Error::NotFound { .. }) => return Ok(RestoreOutcome::NoSnapshot),
        Err(e) => return Err(e).with_context(|| format!("GET {SNAPSHOT_KEY}")),
    };

    let tmp = tmp_sibling(catalog_path, "restore.tmp");
    std::fs::write(&tmp, &data)
        .with_context(|| format!("writing snapshot to {}", tmp.display()))?;

    let tmp_for_check = tmp.clone();
    let found = tokio::task::spawn_blocking(move || read_user_version(&tmp_for_check))
        .await
        .context("joining snapshot version-check task")??;
    if found != expected_version {
        let _ = std::fs::remove_file(&tmp);
        return Ok(RestoreOutcome::VersionMismatch {
            found,
            expected: expected_version,
        });
    }

    let tmp_for_count = tmp.clone();
    let blocks = tokio::task::spawn_blocking(move || count_live_blocks(&tmp_for_count))
        .await
        .context("joining snapshot block-count task")?
        .unwrap_or(0);

    std::fs::rename(&tmp, catalog_path).with_context(|| {
        format!(
            "moving restored snapshot into place at {}",
            catalog_path.display()
        )
    })?;
    Ok(RestoreOutcome::Restored { blocks })
}

fn read_user_version(path: &Path) -> Result<u32> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("opening snapshot {} to read version", path.display()))?;
    let v: i64 = conn
        .pragma_query_value(None, "user_version", |r| r.get(0))
        .context("reading PRAGMA user_version")?;
    Ok(v as u32)
}

fn count_live_blocks(path: &Path) -> Result<u64> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let n: i64 = conn.query_row(
        "SELECT COUNT(*) FROM blocks WHERE deleted_at IS NULL",
        [],
        |r| r.get(0),
    )?;
    Ok(n as u64)
}

/// `<catalog>.<suffix>` next to the catalog file (same directory, which is
/// always writable — it's where the catalog itself lives).
fn tmp_sibling(catalog_path: &Path, suffix: &str) -> PathBuf {
    let mut name = catalog_path
        .file_name()
        .map(|s| s.to_os_string())
        .unwrap_or_default();
    name.push(".");
    name.push(suffix);
    catalog_path.with_file_name(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Catalog;
    use object_store::memory::InMemory;
    use scry_block::BlockMeta;
    use tempfile::TempDir;
    use uuid::Uuid;

    fn meta(uuid: Uuid, writer: Uuid, ts_min: u64, rows: u64) -> BlockMeta {
        BlockMeta {
            uuid,
            signal: "logs".into(),
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
            wal_seg_max: Some(7),
            wal_shard: Some(2),
        }
    }

    #[tokio::test]
    async fn save_then_restore_round_trips_the_catalog() {
        let dir = TempDir::new().unwrap();
        let src_path = dir.path().join("src.sqlite");
        let writer = Uuid::now_v7();
        {
            let cat = Catalog::open(&src_path, "scry-dev").unwrap();
            for i in 0..5u64 {
                cat.insert_block(&meta(
                    Uuid::now_v7(),
                    writer,
                    1_700_000_000_000_000_000 + i,
                    10,
                ))
                .unwrap();
            }
            // A watermark must survive the snapshot (VACUUM INTO copies the whole db).
            assert_eq!(cat.get_watermark(writer, "logs", 2).unwrap(), Some(7));
        }

        let store = InMemory::new();
        let report = save_snapshot(&src_path, &store).await.unwrap();
        assert!(report.bytes > 0);

        let dst_path = dir.path().join("restored.sqlite");
        let outcome = restore_snapshot(&dst_path, &store, CATALOG_SCHEMA_VERSION)
            .await
            .unwrap();
        assert_eq!(outcome, RestoreOutcome::Restored { blocks: 5 });
        assert!(dst_path.exists());

        // The restored catalog is byte-for-byte the same logical state.
        let restored = Catalog::open(&dst_path, "scry-dev").unwrap();
        assert_eq!(restored.list_blocks().unwrap().len(), 5);
        assert_eq!(restored.get_watermark(writer, "logs", 2).unwrap(), Some(7));
    }

    #[tokio::test]
    async fn no_snapshot_is_reported_not_errored() {
        let dir = TempDir::new().unwrap();
        let store = InMemory::new();
        let dst = dir.path().join("cat.sqlite");
        let outcome = restore_snapshot(&dst, &store, CATALOG_SCHEMA_VERSION)
            .await
            .unwrap();
        assert_eq!(outcome, RestoreOutcome::NoSnapshot);
        assert!(!dst.exists(), "no catalog file should be left behind");
    }

    #[tokio::test]
    async fn version_mismatch_is_rejected_and_leaves_no_file() {
        let dir = TempDir::new().unwrap();
        let src_path = dir.path().join("src.sqlite");
        {
            let cat = Catalog::open(&src_path, "scry-dev").unwrap();
            cat.insert_block(&meta(
                Uuid::now_v7(),
                Uuid::now_v7(),
                1_700_000_000_000_000_000,
                10,
            ))
            .unwrap();
        }
        let store = InMemory::new();
        save_snapshot(&src_path, &store).await.unwrap();

        let dst = dir.path().join("restored.sqlite");
        let outcome = restore_snapshot(&dst, &store, CATALOG_SCHEMA_VERSION + 1)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            RestoreOutcome::VersionMismatch {
                found: CATALOG_SCHEMA_VERSION,
                expected: CATALOG_SCHEMA_VERSION + 1,
            }
        );
        assert!(
            !dst.exists(),
            "a mismatched snapshot must not be moved into place"
        );
    }
}
