//! Keeps a query server's errors database current.
//!
//! [`SnapshotFollower`] owns the local errors database path of a reader
//! (queryd) and a [`SharedErrorsDb`] handle the query service serves from. Each
//! [`SnapshotFollower::tick`] runs in one of two modes, chosen from the file
//! itself:
//!
//! - **Local writer.** The file is in WAL mode, so a writer on this host (an
//!   ingest or errors role configured with the same path) owns it. The follower
//!   never downloads over it; it only (re)opens a read-only connection when the
//!   file's identity (device, inode) changes — e.g. the writer installed a
//!   snapshot — so readers never keep serving an unlinked file.
//! - **Snapshot.** Otherwise the follower HEADs the bucket snapshot. When its
//!   version differs from the installed one it streams the object to a
//!   temporary sibling (size-capped, fsynced), validates the schema version,
//!   installs it atomically (stale `-wal`/`-shm` removed first), opens a new
//!   read-only connection and swaps the handle. In-flight queries finish on the
//!   old connection. The installed version is recorded in a
//!   `<db>.snapshot-version` sibling so a restart does not re-download an
//!   unchanged snapshot.
//!
//! A follower built without a store (queryd's `--no-snapshot-restore`) only
//! serves the local file, as in local-writer mode.
//!
//! Operators should not point a reader at the same path as a writer that has
//! not started yet: the follower would install a snapshot there, which the
//! writer then adopts as its database. That is valid (snapshots are complete
//! errors databases), but the file changes owner.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use object_store::ObjectStore;
use std::sync::Arc;

use crate::{
    handle::SharedErrorsDb,
    snapshot::{
        fetch_errors_snapshot, head_errors_snapshot, install_snapshot, is_wal_database,
        tmp_sibling, FetchOutcome, SnapshotVersion,
    },
    sqlite::ErrorsDb,
};

/// What one [`SnapshotFollower::tick`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FollowOutcome {
    /// A local writer owns the file (or snapshots are disabled); the handle
    /// serves the local file when it exists.
    Local { reopened: bool },
    /// No snapshot exists; an existing local file (if any) is served.
    NoSnapshot,
    /// The installed snapshot is current.
    Unchanged,
    /// A newer snapshot was installed and the handle swapped.
    Refreshed { issues: u64, bytes: u64 },
    /// The bucket snapshot was produced by a different schema version.
    VersionMismatch { found: u32, expected: u32 },
}

pub struct SnapshotFollower {
    path: PathBuf,
    store: Option<Arc<dyn ObjectStore>>,
    handle: SharedErrorsDb,
    max_bytes: u64,
    installed: Option<SnapshotVersion>,
    opened_identity: Option<(u64, u64)>,
}

impl SnapshotFollower {
    /// `store: None` disables snapshot following (local file only).
    pub fn new(
        path: PathBuf,
        store: Option<Arc<dyn ObjectStore>>,
        handle: SharedErrorsDb,
        max_bytes: u64,
    ) -> Self {
        let installed = std::fs::read_to_string(version_sibling(&path))
            .ok()
            .map(|token| SnapshotVersion::from_token(token.trim().to_owned()));
        Self {
            path,
            store,
            handle,
            max_bytes,
            installed,
            opened_identity: None,
        }
    }

    pub fn handle(&self) -> &SharedErrorsDb {
        &self.handle
    }

    pub async fn tick(&mut self) -> Result<FollowOutcome> {
        let path = self.path.clone();
        let local = tokio::task::spawn_blocking(move || is_wal_database(&path))
            .await
            .context("joining errors database mode probe")??;
        let store = match self.store.clone() {
            Some(store) if !local => store,
            _ => {
                let reopened = self.open_if_changed().await?;
                return Ok(FollowOutcome::Local { reopened });
            }
        };
        let Some(version) = head_errors_snapshot(store.as_ref()).await? else {
            self.open_if_changed().await?;
            return Ok(FollowOutcome::NoSnapshot);
        };
        if self.installed.as_ref() == Some(&version) && self.path.exists() {
            self.open_if_changed().await?;
            return Ok(FollowOutcome::Unchanged);
        }
        let tmp = tmp_sibling(&self.path, "restore.tmp");
        match fetch_errors_snapshot(store.as_ref(), &tmp, self.max_bytes, None).await? {
            FetchOutcome::Valid(snapshot) => {
                let dest = self.path.clone();
                let token = version.token().to_owned();
                let (issues, bytes) = (snapshot.issues, snapshot.bytes);
                let installed =
                    tokio::task::spawn_blocking(move || -> Result<(ErrorsDb, (u64, u64))> {
                        install_snapshot(&snapshot.path, &dest)?;
                        std::fs::write(version_sibling(&dest), token)
                            .context("recording installed errors snapshot version")?;
                        let db = ErrorsDb::open_read_only(&dest).with_context(|| {
                            format!("opening installed errors snapshot {}", dest.display())
                        })?;
                        Ok((db, file_identity(&dest)?))
                    })
                    .await
                    .context("joining errors snapshot install task")?;
                let (db, identity) = match installed {
                    Ok(installed) => installed,
                    Err(error) => {
                        let _ = tokio::fs::remove_file(&tmp).await;
                        return Err(error);
                    }
                };
                self.handle.replace(Some(db));
                self.opened_identity = Some(identity);
                self.installed = Some(version);
                Ok(FollowOutcome::Refreshed { issues, bytes })
            }
            FetchOutcome::NoSnapshot => {
                self.open_if_changed().await?;
                Ok(FollowOutcome::NoSnapshot)
            }
            FetchOutcome::VersionMismatch { found, expected } => {
                self.open_if_changed().await?;
                Ok(FollowOutcome::VersionMismatch { found, expected })
            }
            FetchOutcome::DeploymentMismatch => {
                anyhow::bail!(
                    "errors snapshot deployment check failed without an expected deployment"
                )
            }
        }
    }

    /// Open (or reopen) a read-only connection when the file exists and its
    /// identity differs from the one currently served.
    async fn open_if_changed(&mut self) -> Result<bool> {
        let path = self.path.clone();
        let current = self.opened_identity.filter(|_| self.handle.is_available());
        let opened =
            tokio::task::spawn_blocking(move || -> Result<Option<(ErrorsDb, (u64, u64))>> {
                let identity = match file_identity(&path) {
                    Ok(identity) => identity,
                    Err(error)
                        if error
                            .downcast_ref::<std::io::Error>()
                            .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound) =>
                    {
                        return Ok(None)
                    }
                    Err(error) => return Err(error),
                };
                if current == Some(identity) {
                    return Ok(None);
                }
                let db = ErrorsDb::open_read_only(&path)
                    .with_context(|| format!("opening errors database {}", path.display()))?;
                Ok(Some((db, identity)))
            })
            .await
            .context("joining errors database open task")??;
        Ok(match opened {
            Some((db, identity)) => {
                self.handle.replace(Some(db));
                self.opened_identity = Some(identity);
                true
            }
            None => false,
        })
    }
}

fn version_sibling(path: &Path) -> PathBuf {
    tmp_sibling(path, "snapshot-version")
}

fn file_identity(path: &Path) -> Result<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    let meta = std::fs::metadata(path).map_err(anyhow::Error::new)?;
    Ok((meta.dev(), meta.ino()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{save_errors_snapshot, DEFAULT_MAX_SNAPSHOT_BYTES};
    use object_store::memory::InMemory;
    use scry_block::AlwaysValid;

    async fn issue_count(handle: &SharedErrorsDb) -> Option<usize> {
        handle
            .read(|db| Ok(db.list_issues(100)?.len()))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn follows_snapshot_changes_and_skips_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let writer_path = dir.path().join("writer.sqlite");
        let mut writer = ErrorsDb::open(&writer_path, [1; 16]).unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let reader_path = dir.path().join("reader.sqlite");
        let handle = SharedErrorsDb::new();
        let mut follower = SnapshotFollower::new(
            reader_path.clone(),
            Some(store.clone()),
            handle.clone(),
            DEFAULT_MAX_SNAPSHOT_BYTES,
        );

        assert_eq!(follower.tick().await.unwrap(), FollowOutcome::NoSnapshot);
        assert!(!handle.is_available());

        save_errors_snapshot(&writer_path, store.as_ref(), &AlwaysValid)
            .await
            .unwrap();
        assert!(matches!(
            follower.tick().await.unwrap(),
            FollowOutcome::Refreshed { issues: 0, .. }
        ));
        assert_eq!(issue_count(&handle).await, Some(0));
        assert_eq!(follower.tick().await.unwrap(), FollowOutcome::Unchanged);

        crate::sqlite::tests::group_one_issue(&mut writer);
        save_errors_snapshot(&writer_path, store.as_ref(), &AlwaysValid)
            .await
            .unwrap();
        // Every clone of the handle (the query service holds one) observes
        // the swapped connection.
        let service_handle = handle.clone();
        assert!(matches!(
            follower.tick().await.unwrap(),
            FollowOutcome::Refreshed { issues: 1, .. }
        ));
        assert_eq!(issue_count(&service_handle).await, Some(1));

        // A restarted follower remembers the installed version.
        let mut restarted = SnapshotFollower::new(
            reader_path.clone(),
            Some(store.clone()),
            SharedErrorsDb::new(),
            DEFAULT_MAX_SNAPSHOT_BYTES,
        );
        assert_eq!(restarted.tick().await.unwrap(), FollowOutcome::Unchanged);
        assert_eq!(issue_count(restarted.handle()).await, Some(1));

        // With snapshots disabled the local file is served but never replaced.
        save_errors_snapshot(&writer_path, store.as_ref(), &AlwaysValid)
            .await
            .unwrap();
        let mut local_only = SnapshotFollower::new(
            reader_path,
            None,
            SharedErrorsDb::new(),
            DEFAULT_MAX_SNAPSHOT_BYTES,
        );
        assert_eq!(
            local_only.tick().await.unwrap(),
            FollowOutcome::Local { reopened: true }
        );
        assert_eq!(
            local_only.tick().await.unwrap(),
            FollowOutcome::Local { reopened: false }
        );
    }

    #[tokio::test]
    async fn shared_path_with_local_writer_is_never_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("errors.sqlite");
        let mut writer = ErrorsDb::open(&path, [1; 16]).unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // A (stale, empty) snapshot exists in the bucket.
        let other = dir.path().join("other.sqlite");
        drop(ErrorsDb::open(&other, [1; 16]).unwrap());
        save_errors_snapshot(&other, store.as_ref(), &AlwaysValid)
            .await
            .unwrap();

        crate::sqlite::tests::group_one_issue(&mut writer);
        let handle = SharedErrorsDb::new();
        let mut follower = SnapshotFollower::new(
            path.clone(),
            Some(store),
            handle.clone(),
            DEFAULT_MAX_SNAPSHOT_BYTES,
        );
        assert_eq!(
            follower.tick().await.unwrap(),
            FollowOutcome::Local { reopened: true }
        );
        assert_eq!(issue_count(&handle).await, Some(1));
        assert_eq!(
            follower.tick().await.unwrap(),
            FollowOutcome::Local { reopened: false }
        );

        // The writer replacing its file (a snapshot adoption, which removes
        // the stale WAL pair first) is followed.
        drop(writer);
        let replacement = dir.path().join("replacement.sqlite");
        drop(ErrorsDb::open(&replacement, [1; 16]).unwrap());
        crate::snapshot::install_snapshot(&replacement, &path).unwrap();
        assert_eq!(
            follower.tick().await.unwrap(),
            FollowOutcome::Local { reopened: true }
        );
        assert_eq!(issue_count(&handle).await, Some(0));
    }

    #[tokio::test]
    async fn version_mismatch_keeps_serving_the_installed_file() {
        let dir = tempfile::tempdir().unwrap();
        let writer_path = dir.path().join("writer.sqlite");
        drop(ErrorsDb::open(&writer_path, [1; 16]).unwrap());
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        save_errors_snapshot(&writer_path, store.as_ref(), &AlwaysValid)
            .await
            .unwrap();
        let reader_path = dir.path().join("reader.sqlite");
        let handle = SharedErrorsDb::new();
        let mut follower = SnapshotFollower::new(
            reader_path.clone(),
            Some(store.clone()),
            handle.clone(),
            DEFAULT_MAX_SNAPSHOT_BYTES,
        );
        follower.tick().await.unwrap();

        rusqlite::Connection::open(&writer_path)
            .unwrap()
            .pragma_update(None, "user_version", 99)
            .unwrap();
        save_errors_snapshot(&writer_path, store.as_ref(), &AlwaysValid)
            .await
            .unwrap();
        assert!(matches!(
            follower.tick().await.unwrap(),
            FollowOutcome::VersionMismatch { found: 99, .. }
        ));
        assert_eq!(issue_count(&handle).await, Some(0));
    }
}
