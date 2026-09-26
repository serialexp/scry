//! Errors projection snapshot transport.
//!
//! The lease-holding writer (ingestd's errors worker, or standalone errorsd)
//! periodically uploads `errors.sqlite` as one object so a cold instance can
//! restore it and serve issues without first re-folding every committed
//! occurrence projection. The snapshot is a plain SQLite file produced with
//! `VACUUM INTO` (consistent and defragmented while the writer keeps running)
//! and normalized to rollback-journal mode, so a read-only reader never needs a
//! `-wal`/`-shm` pair for it.
//!
//! # Bounded transfer
//!
//! Upload and download stream through [`scry_objstore::upload_file`] and
//! [`scry_objstore::download_to_file`]: memory stays bounded by a few multipart
//! parts whatever the snapshot size, a download over the caller's byte cap is
//! refused before anything is written, and the downloaded file is fsynced.
//!
//! # Installation
//!
//! [`install_snapshot`] removes any stale `-wal`, `-shm`, and `-journal`
//! siblings of the destination *before* renaming the validated file over it,
//! then fsyncs the directory. A stale WAL left beside a replaced database would
//! otherwise be replayed into (and corrupt) the new file on the next open.
//!
//! # Validity and races
//!
//! The snapshot is a cache: object storage's committed occurrence projections
//! stay the source of truth, and any errors database rebuilds from them. Only
//! the holder of the errors lease uploads, and [`save_errors_snapshot`] checks
//! the lease fence immediately before the upload. An upload already in flight
//! when the lease moves can still land after the new holder's first upload and
//! briefly regress the snapshot; the next holder upload corrects it, and a
//! reader restoring the older copy only serves slightly older issue summaries.
//!
//! Cross-version safety: the schema version travels inside the db as
//! `PRAGMA user_version` ([`crate::sqlite::ERRORS_SCHEMA_VERSION`]); a snapshot
//! whose version differs from the running binary is never installed.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use object_store::{path::Path as ObjPath, ObjectStore, ObjectStoreExt};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use scry_block::Fence;

use crate::sqlite::{SqliteError, DEPLOYMENT_METADATA_KEY, ERRORS_SCHEMA_VERSION};

/// Object key of the errors projection snapshot in the bucket. Lives under
/// the reserved `_scry/` prefix so it is never mistaken for a block sidecar.
pub const ERRORS_SNAPSHOT_KEY: &str = "_scry/errors/v1/snapshot.sqlite";

/// Default cap on a downloaded snapshot. Far above any realistic errors
/// database (1M occurrences with ~1 KiB canonical bodies is ~1.5 GiB) while
/// still refusing an absurd object before it fills the disk.
pub const DEFAULT_MAX_SNAPSHOT_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Result of [`save_errors_snapshot`].
#[derive(Debug, Clone)]
pub struct SaveReport {
    /// Size in bytes of the uploaded snapshot object.
    pub bytes: u64,
}

/// Opaque identity of one uploaded snapshot object, for change detection.
/// Uses the object's ETag, or its size and modification time when the store
/// reports no ETag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotVersion(String);

impl SnapshotVersion {
    /// Rebuild a version from a token previously returned by [`Self::token`].
    pub fn from_token(token: String) -> Self {
        Self(token)
    }

    pub fn token(&self) -> &str {
        &self.0
    }
}

/// A downloaded snapshot that passed validation and awaits installation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedSnapshot {
    /// Temporary file holding the snapshot; the caller installs or removes it.
    pub path: PathBuf,
    pub bytes: u64,
    pub deployment_id: [u8; 16],
    /// Issue rows in the snapshot (all grouping generations).
    pub issues: u64,
    /// Folded projection commits: a monotone measure of how much of the
    /// committed occurrence set the snapshot contains.
    pub projection_commits: u64,
}

/// Result of downloading and validating the snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchOutcome {
    Valid(ValidatedSnapshot),
    /// No snapshot object exists in the bucket yet (first-ever boot).
    NoSnapshot,
    /// A snapshot exists but its schema version doesn't match this binary.
    VersionMismatch {
        found: u32,
        expected: u32,
    },
    /// A snapshot exists but belongs to a different deployment.
    DeploymentMismatch,
}

/// Result of [`restore_errors_snapshot`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestoreOutcome {
    /// A snapshot was downloaded, validated, and moved into place.
    Restored {
        issues: u64,
        bytes: u64,
    },
    NoSnapshot,
    VersionMismatch {
        found: u32,
        expected: u32,
    },
    DeploymentMismatch,
}

/// HEAD the snapshot object. `None` when no snapshot exists.
pub async fn head_errors_snapshot(store: &dyn ObjectStore) -> Result<Option<SnapshotVersion>> {
    match store.head(&ObjPath::from(ERRORS_SNAPSHOT_KEY)).await {
        Ok(meta) => Ok(Some(SnapshotVersion(match meta.e_tag {
            Some(tag) => format!("etag:{tag}"),
            None => format!(
                "stat:{}:{}",
                meta.size,
                meta.last_modified.timestamp_nanos_opt().unwrap_or_default()
            ),
        }))),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(error).with_context(|| format!("HEAD {ERRORS_SNAPSHOT_KEY}")),
    }
}

/// Take a consistent copy of the errors database and upload it to
/// [`ERRORS_SNAPSHOT_KEY`], overwriting any previous snapshot.
///
/// Reads the database through a separate read-only connection. The caller must
/// hold the errors lease; `fence` is checked after the copy is taken and
/// immediately before the upload starts.
pub async fn save_errors_snapshot(
    errors_db_path: &Path,
    store: &dyn ObjectStore,
    fence: &dyn Fence,
) -> Result<SaveReport> {
    let src = errors_db_path.to_path_buf();
    let tmp = tmp_sibling(errors_db_path, "snapshot.tmp");
    let tmp_for_task = tmp.clone();
    let result = async {
        tokio::task::spawn_blocking(move || vacuum_into(&src, &tmp_for_task))
            .await
            .context("joining errors snapshot VACUUM INTO task")??;
        fence
            .check()
            .context("errors lease lost before snapshot upload")?;
        let bytes =
            scry_objstore::upload_file(store, &tmp, &ObjPath::from(ERRORS_SNAPSHOT_KEY)).await?;
        Ok(SaveReport { bytes })
    }
    .await;
    let _ = tokio::fs::remove_file(&tmp).await;
    result
}

/// Download the snapshot to `tmp` (streamed, capped at `max_bytes`, fsynced)
/// and validate it. The file is left at `tmp` only for [`FetchOutcome::Valid`];
/// every other outcome and every error removes it.
pub async fn fetch_errors_snapshot(
    store: &dyn ObjectStore,
    tmp: &Path,
    max_bytes: u64,
    expected_deployment: Option<[u8; 16]>,
) -> Result<FetchOutcome> {
    let key = ObjPath::from(ERRORS_SNAPSHOT_KEY);
    let Some(bytes) = scry_objstore::download_to_file(store, &key, tmp, max_bytes).await? else {
        return Ok(FetchOutcome::NoSnapshot);
    };
    let path = tmp.to_path_buf();
    let outcome = tokio::task::spawn_blocking(move || validate(&path, bytes, expected_deployment))
        .await
        .context("joining errors snapshot validation task")?;
    if !matches!(outcome, Ok(FetchOutcome::Valid(_))) {
        let _ = tokio::fs::remove_file(tmp).await;
    }
    outcome
}

/// Atomically replace `dest` with the validated snapshot file at `validated`.
/// Blocking; call from a blocking context. `dest` must not be open by a
/// writer: its stale `-wal`/`-shm`/`-journal` siblings are removed first.
pub fn install_snapshot(validated: &Path, dest: &Path) -> Result<()> {
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut sibling = dest.as_os_str().to_os_string();
        sibling.push(suffix);
        match std::fs::remove_file(&sibling) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("removing stale {}", Path::new(&sibling).display()))
            }
        }
    }
    std::fs::rename(validated, dest).with_context(|| {
        format!(
            "moving errors snapshot {} into place at {}",
            validated.display(),
            dest.display()
        )
    })?;
    sync_parent(dest)
}

/// Download, validate, and install the snapshot at `errors_db_path`.
/// Returns without touching `errors_db_path` unless a valid snapshot exists.
pub async fn restore_errors_snapshot(
    errors_db_path: &Path,
    store: &dyn ObjectStore,
    max_bytes: u64,
    expected_deployment: Option<[u8; 16]>,
) -> Result<RestoreOutcome> {
    let tmp = tmp_sibling(errors_db_path, "restore.tmp");
    Ok(
        match fetch_errors_snapshot(store, &tmp, max_bytes, expected_deployment).await? {
            FetchOutcome::Valid(snapshot) => {
                let dest = errors_db_path.to_path_buf();
                let (issues, bytes) = (snapshot.issues, snapshot.bytes);
                let installed =
                    tokio::task::spawn_blocking(move || install_snapshot(&snapshot.path, &dest))
                        .await
                        .context("joining errors snapshot install task")?;
                if installed.is_err() {
                    let _ = tokio::fs::remove_file(&tmp).await;
                }
                installed?;
                RestoreOutcome::Restored { issues, bytes }
            }
            FetchOutcome::NoSnapshot => RestoreOutcome::NoSnapshot,
            FetchOutcome::VersionMismatch { found, expected } => {
                RestoreOutcome::VersionMismatch { found, expected }
            }
            FetchOutcome::DeploymentMismatch => RestoreOutcome::DeploymentMismatch,
        },
    )
}

/// Number of folded projection commits in a local errors database, or `None`
/// when it has no schema yet. Blocking.
pub fn projection_commit_count(path: &Path) -> Result<Option<u64>> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("opening errors database {}", path.display()))?;
    let exists: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'projection_commits'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    if exists.is_none() {
        return Ok(None);
    }
    let count: i64 = conn.query_row("SELECT count(*) FROM projection_commits", [], |row| {
        row.get(0)
    })?;
    Ok(Some(count as u64))
}

/// Whether the SQLite file at `path` is in WAL mode, i.e. owned by a live
/// local writer rather than installed from a snapshot. Reads the file header
/// (bytes 18 and 19 are 2 for WAL); `false` for a missing file.
pub fn is_wal_database(path: &Path) -> Result<bool> {
    use std::io::Read;
    let mut file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error).with_context(|| format!("opening {}", path.display())),
    };
    let mut header = [0u8; 20];
    match file.read_exact(&mut header) {
        Ok(()) => Ok(header[18] == 2 && header[19] == 2),
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => Err(error).with_context(|| format!("reading {}", path.display())),
    }
}

fn validate(
    path: &Path,
    bytes: u64,
    expected_deployment: Option<[u8; 16]>,
) -> Result<FetchOutcome> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)
        .with_context(|| format!("opening downloaded errors snapshot {}", path.display()))?;
    let found: i64 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .context("reading snapshot PRAGMA user_version")?;
    if found != i64::from(ERRORS_SCHEMA_VERSION) {
        return Ok(FetchOutcome::VersionMismatch {
            found: found as u32,
            expected: ERRORS_SCHEMA_VERSION,
        });
    }
    // Normalize older snapshots that were copied from a WAL database: a
    // WAL-flagged file needs a writable -shm even for read-only readers.
    let mode: String = conn
        .pragma_update_and_check(None, "journal_mode", "DELETE", |row| row.get(0))
        .context("normalizing snapshot journal mode")?;
    anyhow::ensure!(
        mode.eq_ignore_ascii_case("delete"),
        "snapshot journal mode stayed {mode}"
    );
    let deployment: Option<Vec<u8>> = conn
        .query_row(
            "SELECT value FROM metadata WHERE key = ?1",
            [DEPLOYMENT_METADATA_KEY],
            |row| row.get(0),
        )
        .optional()
        .context("reading snapshot deployment")?;
    let deployment_id: [u8; 16] = deployment
        .as_deref()
        .and_then(|value| value.try_into().ok())
        .ok_or_else(|| anyhow::Error::new(SqliteError::MissingDeployment))?;
    if expected_deployment.is_some_and(|expected| expected != deployment_id) {
        return Ok(FetchOutcome::DeploymentMismatch);
    }
    let issues: i64 = conn.query_row("SELECT count(*) FROM issues", [], |row| row.get(0))?;
    let projection_commits: i64 =
        conn.query_row("SELECT count(*) FROM projection_commits", [], |row| {
            row.get(0)
        })?;
    drop(conn);
    std::fs::File::open(path)
        .and_then(|file| file.sync_all())
        .with_context(|| format!("fsync {}", path.display()))?;
    Ok(FetchOutcome::Valid(ValidatedSnapshot {
        path: path.to_path_buf(),
        bytes,
        deployment_id,
        issues: issues as u64,
        projection_commits: projection_commits as u64,
    }))
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
    drop(conn);
    // VACUUM INTO copies the source's WAL file-format bytes; publish the copy
    // in rollback-journal mode so read-only readers need no -shm.
    let copy = Connection::open(tmp).context("opening errors snapshot copy")?;
    copy.pragma_update(None, "journal_mode", "DELETE")
        .context("setting snapshot copy journal mode")?;
    Ok(())
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    std::fs::File::open(parent)
        .and_then(|dir| dir.sync_all())
        .with_context(|| format!("fsync directory {}", parent.display()))
}

/// A temporary sibling of `db_path` (same directory, so rename is atomic).
pub fn tmp_sibling(db_path: &Path, suffix: &str) -> PathBuf {
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
    use crate::sqlite::ErrorsDb;
    use object_store::memory::InMemory;
    use scry_block::AlwaysValid;

    const DEPLOYMENT: [u8; 16] = [1; 16];

    struct Lost;
    impl Fence for Lost {
        fn check(&self) -> Result<()> {
            anyhow::bail!("lost")
        }
    }

    #[tokio::test]
    async fn round_trip_save_and_restore_replaces_stale_wal() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("errors.sqlite");
        let _db = ErrorsDb::open(&src, DEPLOYMENT).unwrap();

        let store = InMemory::new();
        let report = save_errors_snapshot(&src, &store, &AlwaysValid)
            .await
            .unwrap();
        assert!(report.bytes > 0);
        assert!(!tmp_sibling(&src, "snapshot.tmp").exists());

        let dst = dir.path().join("restored.sqlite");
        std::fs::write(&dst, b"old database").unwrap();
        std::fs::write(dir.path().join("restored.sqlite-wal"), b"stale wal").unwrap();
        std::fs::write(dir.path().join("restored.sqlite-shm"), b"stale shm").unwrap();
        let outcome =
            restore_errors_snapshot(&dst, &store, DEFAULT_MAX_SNAPSHOT_BYTES, Some(DEPLOYMENT))
                .await
                .unwrap();
        assert_eq!(
            outcome,
            RestoreOutcome::Restored {
                issues: 0,
                bytes: report.bytes
            }
        );
        assert!(!dir.path().join("restored.sqlite-wal").exists());
        assert!(!dir.path().join("restored.sqlite-shm").exists());
        assert!(!is_wal_database(&dst).unwrap());
        assert!(is_wal_database(&src).unwrap());
        assert_eq!(projection_commit_count(&dst).unwrap(), Some(0));

        // The restored database opens read-only without creating a WAL pair.
        let ro = ErrorsDb::open_read_only(&dst).unwrap();
        assert_eq!(ro.deployment_id(), DEPLOYMENT);
    }

    #[tokio::test]
    async fn no_snapshot_returns_no_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let dst = dir.path().join("errors.sqlite");
        let store = InMemory::new();
        assert_eq!(head_errors_snapshot(&store).await.unwrap(), None);
        let outcome = restore_errors_snapshot(&dst, &store, DEFAULT_MAX_SNAPSHOT_BYTES, None)
            .await
            .unwrap();
        assert_eq!(outcome, RestoreOutcome::NoSnapshot);
        assert!(!dst.exists());
    }

    #[tokio::test]
    async fn version_mismatch_rejects_without_touching_target() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("errors.sqlite");
        drop(ErrorsDb::open(&src, DEPLOYMENT).unwrap());
        Connection::open(&src)
            .unwrap()
            .pragma_update(None, "user_version", ERRORS_SCHEMA_VERSION + 1)
            .unwrap();

        let store = InMemory::new();
        save_errors_snapshot(&src, &store, &AlwaysValid)
            .await
            .unwrap();

        let dst = dir.path().join("restored.sqlite");
        let outcome = restore_errors_snapshot(&dst, &store, DEFAULT_MAX_SNAPSHOT_BYTES, None)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            RestoreOutcome::VersionMismatch {
                found: ERRORS_SCHEMA_VERSION + 1,
                expected: ERRORS_SCHEMA_VERSION
            }
        );
        assert!(!dst.exists());
        assert!(!tmp_sibling(&dst, "restore.tmp").exists());
    }

    #[tokio::test]
    async fn other_deployment_and_oversize_snapshots_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("errors.sqlite");
        drop(ErrorsDb::open(&src, DEPLOYMENT).unwrap());
        let store = InMemory::new();
        save_errors_snapshot(&src, &store, &AlwaysValid)
            .await
            .unwrap();
        let dst = dir.path().join("restored.sqlite");
        let outcome =
            restore_errors_snapshot(&dst, &store, DEFAULT_MAX_SNAPSHOT_BYTES, Some([2; 16]))
                .await
                .unwrap();
        assert_eq!(outcome, RestoreOutcome::DeploymentMismatch);
        assert!(!dst.exists());

        let error = restore_errors_snapshot(&dst, &store, 16, None)
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("download limit"), "{error:#}");
        assert!(!dst.exists());
        assert!(!tmp_sibling(&dst, "restore.tmp").exists());
    }

    #[tokio::test]
    async fn lost_fence_uploads_nothing_and_head_tracks_changes() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("errors.sqlite");
        drop(ErrorsDb::open(&src, DEPLOYMENT).unwrap());
        let store = InMemory::new();
        assert!(save_errors_snapshot(&src, &store, &Lost).await.is_err());
        assert_eq!(head_errors_snapshot(&store).await.unwrap(), None);
        assert!(!tmp_sibling(&src, "snapshot.tmp").exists());

        save_errors_snapshot(&src, &store, &AlwaysValid)
            .await
            .unwrap();
        let first = head_errors_snapshot(&store).await.unwrap().unwrap();
        assert_eq!(
            head_errors_snapshot(&store).await.unwrap(),
            Some(first.clone())
        );
        save_errors_snapshot(&src, &store, &AlwaysValid)
            .await
            .unwrap();
        assert_ne!(head_errors_snapshot(&store).await.unwrap(), Some(first));
    }
}
