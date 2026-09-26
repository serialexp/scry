//! Lease-guarded error processing embedded in `scry ingest` (D-074).
//!
//! Exactly one ingester per bucket runs the errors pass: it holds the Valkey
//! lease [`ERRORS_LEASE_KEY`] (or, with `--allow-unfenced-maintenance`, the
//! single-process local lease). The holder keeps its lease across passes —
//! the Valkey guard renews itself at ttl/3 — and only a lost fence or a
//! restart moves it. The lease fence is handed to the engine, which checks it
//! before every durable publication and SQLite commit, and to the snapshot
//! producer, so only the current holder uploads the errors snapshot.
//!
//! Readiness fails closed: until the object store passes the conditional-write
//! capability probe (required by the immutable occurrence commits, see
//! `docs/design/error-monitoring.md`) and the deployment manifest resolves,
//! the worker does no error processing and retries each interval. Ingest,
//! convergence, and maintenance are unaffected.
//!
//! When an instance acquires the lease it adopts the bucket snapshot if that
//! snapshot contains more folded projection commits than its local database
//! (always, when it has none — the "restore on start" case), so a failover
//! does not publish a regressed snapshot while the new holder catches up.
//!
//! The worker is its own task, and the engine runs every SQLite, Parquet, and
//! fingerprinting step on the blocking pool, so a long errors pass never
//! delays the compaction and retention ticks of the maintenance loop.

use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use object_store::{path::Path as ObjectPath, ObjectStore};
use scry_cluster::{LeaseGuard, LeaseProvider};
use scry_errors::{
    engine::{reconcile_once, CatalogMode, ReconcileConfig},
    snapshot::{
        fetch_errors_snapshot, install_snapshot, projection_commit_count, save_errors_snapshot,
        tmp_sibling, FetchOutcome,
    },
    DeploymentId,
};
use tracing::{error, info, warn};

/// Valkey lease key guarding the project-wide errors pass.
pub(crate) const ERRORS_LEASE_KEY: &str = "lease/errors/project";

pub(crate) struct ErrorsWorkerConfig {
    pub errors_db: PathBuf,
    pub catalog_path: PathBuf,
    pub bucket: String,
    pub reconcile: ReconcileConfig,
    /// Completion-relative delay between passes.
    pub interval: Duration,
    /// Minimum time between snapshot uploads by the holder; zero disables.
    pub snapshot_interval: Duration,
    pub lease_ttl: Duration,
    pub max_snapshot_bytes: u64,
}

/// Prefix for the conditional-write capability probe objects.
const PROBE_PREFIX: &str = "_scry/probes/ingestd-errors";

/// What one [`ErrorsWorker::pass`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PassOutcome {
    /// Capability probe or deployment manifest not yet satisfied.
    NotReady,
    /// A peer holds the errors lease.
    PeerHolds,
    /// The lease backend is unreachable; no errors work without a lease.
    LeaseUnavailable,
    /// This instance holds the lease and ran a pass.
    Ran {
        succeeded: bool,
        snapshot_uploaded: bool,
    },
}

pub(crate) struct ErrorsWorker<L: LeaseProvider> {
    provider: L,
    store: Arc<dyn ObjectStore>,
    config: ErrorsWorkerConfig,
    deployment: Option<DeploymentId>,
    guard: Option<L::Guard>,
    last_snapshot: Option<Instant>,
    /// A pass changed occurrence or issue state since the last snapshot
    /// upload (or adoption). Idle passes never re-upload an unchanged database.
    unsnapshotted_changes: bool,
}

impl<L: LeaseProvider> ErrorsWorker<L> {
    pub(crate) fn new(
        provider: L,
        store: Arc<dyn ObjectStore>,
        config: ErrorsWorkerConfig,
    ) -> Self {
        Self {
            provider,
            store,
            config,
            deployment: None,
            guard: None,
            last_snapshot: None,
            unsnapshotted_changes: false,
        }
    }

    /// Run passes with a completion-relative delay, forever (the task is
    /// aborted at shutdown; the lease then expires or is dropped).
    pub(crate) async fn run(mut self) {
        info!(
            errors_db = %self.config.errors_db.display(),
            interval_secs = self.config.interval.as_secs(),
            lease = ERRORS_LEASE_KEY,
            "error processing worker started"
        );
        loop {
            self.pass().await;
            tokio::time::sleep(self.config.interval).await;
        }
    }

    pub(crate) async fn pass(&mut self) -> PassOutcome {
        let Some(deployment) = self.ensure_ready().await else {
            return PassOutcome::NotReady;
        };

        if let Some(guard) = self.guard.as_ref() {
            if let Err(lost) = guard.fence().check() {
                warn!(error = %lost, "errors lease lost; re-acquiring before further error processing");
                if let Some(guard) = self.guard.take() {
                    guard.release().await;
                }
            }
        }
        if self.guard.is_none() {
            match self
                .provider
                .try_acquire(ERRORS_LEASE_KEY, self.config.lease_ttl)
                .await
            {
                Ok(Some(guard)) => {
                    info!(lease = ERRORS_LEASE_KEY, "acquired errors lease");
                    // A just-adopted snapshot is the bucket copy. Otherwise the
                    // bucket copy is missing or behind this database, so the
                    // first due upload refreshes it even if no pass changes
                    // anything. Either way the first upload waits a full interval.
                    self.unsnapshotted_changes = !self.adopt_newer_snapshot(deployment).await;
                    self.last_snapshot = Some(Instant::now());
                    self.guard = Some(guard);
                }
                Ok(None) => return PassOutcome::PeerHolds,
                Err(error) => {
                    warn!(error = %format!("{error:#}"), "errors lease backend unavailable; error processing paused");
                    return PassOutcome::LeaseUnavailable;
                }
            }
        }
        let Some(fence) = self.guard.as_ref().map(LeaseGuard::fence) else {
            return PassOutcome::LeaseUnavailable;
        };

        let succeeded = match reconcile_once(
            CatalogMode::Converged {
                catalog_path: &self.config.catalog_path,
                bucket: &self.config.bucket,
            },
            self.store.clone(),
            &self.config.errors_db,
            deployment,
            &self.config.reconcile,
            fence.clone(),
        )
        .await
        {
            Ok(r) => {
                self.unsnapshotted_changes |= r.changed_state();
                if r.processed_blocks > 0
                    || r.fold.inserted > 0
                    || r.grouping.scanned > 0
                    || r.grouping.failures > 0
                {
                    info!(
                        blocks = r.processed_blocks,
                        covered = r.sources_already_covered,
                        occurrences = r.fold.inserted,
                        grouped = r.grouping.occurrences_grouped,
                        grouping_failures = r.grouping.failures,
                        grouping_drained = r.grouping.drained,
                        issues_created = r.grouping.issues_created,
                        issues_updated = r.grouping.issues_updated,
                        "errors pass completed"
                    );
                }
                true
            }
            Err(error) => {
                warn!(error = %format!("{error:#}"), "errors pass failed");
                false
            }
        };

        let snapshot_due = !self.config.snapshot_interval.is_zero()
            && self.unsnapshotted_changes
            && self
                .last_snapshot
                .is_none_or(|at| at.elapsed() >= self.config.snapshot_interval);
        let mut snapshot_uploaded = false;
        if succeeded && snapshot_due {
            match save_errors_snapshot(&self.config.errors_db, self.store.as_ref(), fence.as_ref())
                .await
            {
                Ok(report) => {
                    info!(bytes = report.bytes, "errors snapshot uploaded");
                    snapshot_uploaded = true;
                    self.unsnapshotted_changes = false;
                }
                Err(error) => warn!(error = %format!("{error:#}"), "errors snapshot failed"),
            }
            self.last_snapshot = Some(Instant::now());
        }
        PassOutcome::Ran {
            succeeded,
            snapshot_uploaded,
        }
    }

    /// Probe conditional writes and resolve the deployment, once. Fails
    /// closed: `None` until both succeed.
    async fn ensure_ready(&mut self) -> Option<DeploymentId> {
        if let Some(deployment) = self.deployment {
            return Some(deployment);
        }
        let probe = ObjectPath::from(format!("{PROBE_PREFIX}/{}", uuid::Uuid::now_v7()));
        if let Err(probe_error) =
            scry_objstore::probe_conditional_writes(self.store.as_ref(), &probe).await
        {
            error!(
                error = %format!("{probe_error:#}"),
                "object store failed the conditional-write capability probe; error processing stays disabled (retrying)"
            );
            return None;
        }
        let manifest = match scry_errors::manifest::ensure_deployment_manifest(
            self.store.as_ref(),
            None,
        )
        .await
        {
            Ok(manifest) => manifest,
            Err(manifest_error) => {
                warn!(error = %format!("{manifest_error:#}"), "deployment manifest unavailable; error processing waits");
                return None;
            }
        };
        match DeploymentId::parse(&manifest.deployment_id, "deployment_id") {
            Ok(deployment) => {
                info!(deployment_id = %manifest.deployment_id, "error processing ready");
                self.deployment = Some(deployment);
                Some(deployment)
            }
            Err(parse_error) => {
                error!(error = %parse_error, "deployment manifest identity is invalid; error processing disabled");
                None
            }
        }
    }

    /// Replace the local errors database with the bucket snapshot when the
    /// snapshot has folded more projection commits. Failures only cost
    /// catch-up time: the database rebuilds from committed projections.
    /// Returns whether the local database is now the bucket copy.
    async fn adopt_newer_snapshot(&self, deployment: DeploymentId) -> bool {
        let outcome = adopt_newer_snapshot(
            self.store.as_ref(),
            &self.config.errors_db,
            *deployment.as_bytes(),
            self.config.max_snapshot_bytes,
        )
        .await;
        match &outcome {
            Ok(Adoption::Adopted {
                projection_commits,
                bytes,
            }) => info!(
                projection_commits,
                bytes, "adopted errors snapshot from bucket"
            ),
            Ok(Adoption::KeptLocal { local, snapshot }) => info!(
                local_commits = local,
                snapshot_commits = snapshot,
                "local errors database is at least as complete as the bucket snapshot"
            ),
            Ok(Adoption::NoSnapshot) => {}
            Ok(Adoption::Incompatible) => {
                info!("bucket errors snapshot is for another schema or deployment; rebuilding locally")
            }
            Err(error) => warn!(
                error = %format!("{error:#}"),
                "errors snapshot adoption failed; rebuilding from committed projections"
            ),
        }
        matches!(outcome, Ok(Adoption::Adopted { .. }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Adoption {
    Adopted { projection_commits: u64, bytes: u64 },
    KeptLocal { local: u64, snapshot: u64 },
    NoSnapshot,
    Incompatible,
}

/// Whether a snapshot with `snapshot` folded commits should replace a local
/// database with `local` (None: missing or unmigrated).
pub(crate) fn should_adopt(snapshot: u64, local: Option<u64>) -> bool {
    local.is_none_or(|local| snapshot > local)
}

pub(crate) async fn adopt_newer_snapshot(
    store: &dyn ObjectStore,
    errors_db: &std::path::Path,
    deployment: [u8; 16],
    max_bytes: u64,
) -> Result<Adoption> {
    let tmp = tmp_sibling(errors_db, "adopt.tmp");
    let snapshot = match fetch_errors_snapshot(store, &tmp, max_bytes, Some(deployment)).await? {
        FetchOutcome::Valid(snapshot) => snapshot,
        FetchOutcome::NoSnapshot => return Ok(Adoption::NoSnapshot),
        FetchOutcome::VersionMismatch { .. } | FetchOutcome::DeploymentMismatch => {
            return Ok(Adoption::Incompatible)
        }
    };
    let dest = errors_db.to_path_buf();
    let outcome = tokio::task::spawn_blocking(move || -> Result<Adoption> {
        let local = if dest.exists() {
            projection_commit_count(&dest)?
        } else {
            None
        };
        if !should_adopt(snapshot.projection_commits, local) {
            let _ = std::fs::remove_file(&snapshot.path);
            return Ok(Adoption::KeptLocal {
                local: local.unwrap_or_default(),
                snapshot: snapshot.projection_commits,
            });
        }
        install_snapshot(&snapshot.path, &dest)?;
        Ok(Adoption::Adopted {
            projection_commits: snapshot.projection_commits,
            bytes: snapshot.bytes,
        })
    })
    .await
    .context("joining errors snapshot adoption task")?;
    if outcome.is_err() {
        let _ = tokio::fs::remove_file(&tmp).await;
    }
    outcome
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use scry_cluster::LocalLeaseProvider;
    use scry_errors::snapshot::head_errors_snapshot;

    use super::*;

    struct TempDir(PathBuf);
    impl TempDir {
        fn new(name: &str) -> Self {
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "scry-ingestd-{name}-{}-{unique}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn config(dir: &TempDir, db: &str) -> ErrorsWorkerConfig {
        let catalog_path = dir.0.join("catalog.sqlite");
        drop(scry_catalog::Catalog::open(&catalog_path, "bucket").unwrap());
        ErrorsWorkerConfig {
            errors_db: dir.0.join(db),
            catalog_path,
            bucket: "bucket".to_owned(),
            reconcile: ReconcileConfig::default(),
            interval: Duration::from_secs(1),
            snapshot_interval: Duration::from_secs(3600),
            lease_ttl: Duration::from_secs(30),
            max_snapshot_bytes: scry_errors::snapshot::DEFAULT_MAX_SNAPSHOT_BYTES,
        }
    }

    #[tokio::test]
    async fn only_the_lease_holder_processes_and_snapshots_and_failover_adopts() {
        let dir = TempDir::new("errors-worker");
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let provider = LocalLeaseProvider::new();
        let mut first =
            ErrorsWorker::new(provider.clone(), store.clone(), config(&dir, "a.sqlite"));
        let mut second =
            ErrorsWorker::new(provider.clone(), store.clone(), config(&dir, "b.sqlite"));

        assert_eq!(
            first.pass().await,
            PassOutcome::Ran {
                succeeded: true,
                snapshot_uploaded: false
            }
        );
        assert_eq!(second.pass().await, PassOutcome::PeerHolds);
        assert!(!dir.0.join("b.sqlite").exists());
        // The holder keeps its lease across passes.
        first.last_snapshot = None;
        assert_eq!(
            first.pass().await,
            PassOutcome::Ran {
                succeeded: true,
                snapshot_uploaded: true
            }
        );
        assert!(head_errors_snapshot(store.as_ref())
            .await
            .unwrap()
            .is_some());
        // An idle pass never re-uploads an unchanged database, even when due.
        first.last_snapshot = None;
        assert_eq!(
            first.pass().await,
            PassOutcome::Ran {
                succeeded: true,
                snapshot_uploaded: false
            }
        );

        // Failover: the next holder restores the snapshot before processing.
        drop(first);
        assert!(matches!(
            second.pass().await,
            PassOutcome::Ran {
                succeeded: true,
                ..
            }
        ));
        assert_eq!(
            projection_commit_count(&dir.0.join("b.sqlite")).unwrap(),
            Some(0)
        );
    }

    #[test]
    fn adoption_prefers_the_more_complete_database() {
        assert!(should_adopt(0, None));
        assert!(should_adopt(5, Some(4)));
        assert!(!should_adopt(4, Some(4)));
        assert!(!should_adopt(3, Some(4)));
    }

    #[tokio::test]
    async fn adoption_keeps_a_more_complete_local_database() {
        let dir = TempDir::new("errors-adopt");
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let snapshot_source = dir.0.join("source.sqlite");
        drop(scry_errors::sqlite::ErrorsDb::open(&snapshot_source, [1; 16]).unwrap());
        save_errors_snapshot(&snapshot_source, store.as_ref(), &scry_block::AlwaysValid)
            .await
            .unwrap();

        // Missing local database: adopted.
        let local = dir.0.join("local.sqlite");
        assert_eq!(
            adopt_newer_snapshot(store.as_ref(), &local, [1; 16], u64::MAX)
                .await
                .unwrap(),
            Adoption::Adopted {
                projection_commits: 0,
                bytes: std::fs::metadata(&local).unwrap().len()
            }
        );
        // Equal commit counts: the local database is kept.
        assert_eq!(
            adopt_newer_snapshot(store.as_ref(), &local, [1; 16], u64::MAX)
                .await
                .unwrap(),
            Adoption::KeptLocal {
                local: 0,
                snapshot: 0
            }
        );
        assert!(!tmp_sibling(&local, "adopt.tmp").exists());
        // Another deployment's snapshot is never adopted.
        assert_eq!(
            adopt_newer_snapshot(store.as_ref(), &dir.0.join("x.sqlite"), [2; 16], u64::MAX)
                .await
                .unwrap(),
            Adoption::Incompatible
        );
    }
}
