//! Errors projection snapshot transport.
//!
//! The writer (ingestd or standalone errorsd) periodically uploads
//! `errors.sqlite` as one object so a cold queryd can restore it in a single
//! GET and start serving issues immediately. The snapshot is a plain SQLite
//! file produced with `VACUUM INTO` (consistent, defragmented, safe against
//! concurrent WAL readers/writers).
//!
//! Cross-version safety: the schema version travels inside the db as
//! `PRAGMA user_version` ([`crate::sqlite::ERRORS_SCHEMA_VERSION`]);
//! [`restore_errors_snapshot`] refuses a snapshot whose version doesn't match
//! the running binary.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use object_store::{path::Path as ObjPath, ObjectStore, ObjectStoreExt, PutPayload};
use rusqlite::{Connection, OpenFlags};

/// Object key of the errors projection snapshot in the bucket. Lives under
/// the reserved `_scry/` prefix so it is never mistaken for a block sidecar.
pub const ERRORS_SNAPSHOT_KEY: &str = "_scry/errors/v1/snapshot.sqlite";

/// Result of [`save_errors_snapshot`].
#[derive(Debug, Clone)]
pub struct SaveReport {
    /// Size in bytes of the uploaded snapshot object.
    pub bytes: u64,
}

/// Result of [`restore_errors_snapshot`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestoreOutcome {
    /// A snapshot was downloaded, version-checked, and moved into place.
    Restored {
        /// Issue rows in the restored database (best-effort, for logging).
        issues: u64,
    },
    /// No snapshot object exists in the bucket yet (first-ever boot).
    NoSnapshot,
    /// A snapshot exists but its schema version doesn't match this binary.
    VersionMismatch { found: u32, expected: u32 },
}

/// Take a consistent copy of the errors database and upload it to
/// [`ERRORS_SNAPSHOT_KEY`], overwriting any previous snapshot.
///
/// Non-destructive: reads the database via a separate read-only connection
/// and overwrites one object. Safe to run without any lease.
pub async fn save_errors_snapshot(
    errors_db_path: &Path,
    store: &dyn ObjectStore,
) -> Result<SaveReport> {
    let src = errors_db_path.to_path_buf();
    let tmp = tmp_sibling(errors_db_path, "snapshot.tmp");
    let tmp_for_task = tmp.clone();
    tokio::task::spawn_blocking(move || vacuum_into(&src, &tmp_for_task))
        .await
        .context("joining errors snapshot VACUUM INTO task")??;

    let data = std::fs::read(&tmp)
        .with_context(|| format!("reading errors snapshot temp {}", tmp.display()))?;
    let bytes = data.len() as u64;
    let key = ObjPath::from(ERRORS_SNAPSHOT_KEY);
    let put_res = store.put(&key, PutPayload::from(data)).await;
    let _ = std::fs::remove_file(&tmp);
    put_res.with_context(|| format!("PUT {ERRORS_SNAPSHOT_KEY}"))?;
    Ok(SaveReport { bytes })
}

/// Download the errors snapshot from [`ERRORS_SNAPSHOT_KEY`] and, if its schema
/// version matches `expected_version`, move it into place at `errors_db_path`.
///
/// Returns without touching `errors_db_path` when there is no snapshot or the
/// version doesn't match.
pub async fn restore_errors_snapshot(
    errors_db_path: &Path,
    store: &dyn ObjectStore,
    expected_version: u32,
) -> Result<RestoreOutcome> {
    let key = ObjPath::from(ERRORS_SNAPSHOT_KEY);
    let data = match store.get(&key).await {
        Ok(g) => g
            .bytes()
            .await
            .with_context(|| format!("reading {ERRORS_SNAPSHOT_KEY} body"))?,
        Err(object_store::Error::NotFound { .. }) => return Ok(RestoreOutcome::NoSnapshot),
        Err(e) => return Err(e).with_context(|| format!("GET {ERRORS_SNAPSHOT_KEY}")),
    };

    let tmp = tmp_sibling(errors_db_path, "restore.tmp");
    std::fs::write(&tmp, &data)
        .with_context(|| format!("writing errors snapshot to {}", tmp.display()))?;

    let tmp_for_check = tmp.clone();
    let found = tokio::task::spawn_blocking(move || read_user_version(&tmp_for_check))
        .await
        .context("joining errors snapshot version-check task")??;
    if found != expected_version {
        let _ = std::fs::remove_file(&tmp);
        return Ok(RestoreOutcome::VersionMismatch {
            found,
            expected: expected_version,
        });
    }

    let tmp_for_count = tmp.clone();
    let issues = tokio::task::spawn_blocking(move || count_issues(&tmp_for_count))
        .await
        .context("joining errors snapshot issue-count task")?
        .unwrap_or(0);

    std::fs::rename(&tmp, errors_db_path).with_context(|| {
        format!(
            "moving restored errors snapshot into place at {}",
            errors_db_path.display()
        )
    })?;
    Ok(RestoreOutcome::Restored { issues })
}

fn vacuum_into(src: &Path, tmp: &Path) -> Result<()> {
    let _ = std::fs::remove_file(tmp);
    let conn = Connection::open_with_flags(
        src,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .with_context(|| format!("opening errors database {} for snapshot", src.display()))?;
    let tmp_str = tmp
        .to_str()
        .context("errors snapshot temp path is not valid UTF-8")?;
    conn.execute("VACUUM main INTO ?1", [tmp_str])
        .context("VACUUM INTO errors snapshot temp")?;
    Ok(())
}

fn read_user_version(path: &Path) -> Result<u32> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("opening errors snapshot {} to read version", path.display()))?;
    let v: i64 = conn
        .pragma_query_value(None, "user_version", |r| r.get(0))
        .context("reading PRAGMA user_version")?;
    Ok(v as u32)
}

fn count_issues(path: &Path) -> Result<u64> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let n: i64 = conn
        .query_row("SELECT COUNT(*) FROM issues", [], |r| r.get(0))
        .unwrap_or(0);
    Ok(n as u64)
}

fn tmp_sibling(db_path: &Path, suffix: &str) -> PathBuf {
    let mut name = db_path
        .file_name()
        .map(|s| s.to_os_string())
        .unwrap_or_default();
    name.push(".");
    name.push(suffix);
    db_path.with_file_name(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite::{ErrorsDb, ERRORS_SCHEMA_VERSION};
    use object_store::memory::InMemory;

    const DEPLOYMENT: [u8; 16] = [1; 16];

    #[tokio::test]
    async fn round_trip_save_and_restore() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("errors.sqlite");
        let _db = ErrorsDb::open(&src, DEPLOYMENT).unwrap();

        let store = InMemory::new();
        let report = save_errors_snapshot(&src, &store).await.unwrap();
        assert!(report.bytes > 0);

        let dst = dir.path().join("restored.sqlite");
        let outcome = restore_errors_snapshot(&dst, &store, ERRORS_SCHEMA_VERSION)
            .await
            .unwrap();
        assert!(matches!(outcome, RestoreOutcome::Restored { issues: 0 }));
        assert!(dst.exists());

        // The restored database should be openable read-only.
        let _ro = ErrorsDb::open_read_only(&dst).unwrap();
    }

    #[tokio::test]
    async fn no_snapshot_returns_no_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let dst = dir.path().join("errors.sqlite");
        let store = InMemory::new();
        let outcome = restore_errors_snapshot(&dst, &store, ERRORS_SCHEMA_VERSION)
            .await
            .unwrap();
        assert_eq!(outcome, RestoreOutcome::NoSnapshot);
        assert!(!dst.exists());
    }

    #[tokio::test]
    async fn version_mismatch_rejects_without_touching_target() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("errors.sqlite");
        let _db = ErrorsDb::open(&src, DEPLOYMENT).unwrap();

        let store = InMemory::new();
        save_errors_snapshot(&src, &store).await.unwrap();

        let dst = dir.path().join("restored.sqlite");
        let outcome = restore_errors_snapshot(&dst, &store, ERRORS_SCHEMA_VERSION + 1)
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            RestoreOutcome::VersionMismatch {
                found: ERRORS_SCHEMA_VERSION,
                ..
            }
        ));
        assert!(!dst.exists());
    }
}
