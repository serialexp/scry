//! Runtime guards and bounded occurrence reconciliation for `scry errors`.
//!
//! The reconciler projects bounded pages of authoritative server-stamped logs blocks
//! into immutable occurrence objects and folds verified projections into rebuildable
//! local SQLite.

pub use scry_errors::engine;
mod singleton;
mod status;

use std::{future::Future, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use object_store::{path::Path as ObjectPath, ObjectStore};
use scry_objstore::{open, probe_conditional_writes, ObjStoreConfig};
use tokio::sync::watch;
use tracing::{info, warn};
use uuid::Uuid;

pub use scry_errors::manifest::{
    ensure_deployment_manifest, read_deployment_manifest, require_deployment_manifest,
    validate_deployment_id, DeploymentManifest,
};
pub use singleton::SingletonLock;

/// CLI arguments for the `scry errors` role.
#[derive(Parser, Debug)]
#[command(about = "Bounded error-occurrence projection and reconciliation")]
pub struct Args {
    /// Optional deployment identity expected in this bucket's control manifest.
    /// Init always generates the candidate identity; serve never creates one.
    #[arg(long)]
    pub deployment_id: Option<String>,

    /// Rebuildable local occurrence projection.
    #[arg(long, default_value = "errors.sqlite")]
    pub errors_db: PathBuf,

    /// Owned rebuildable catalog of server-stamped logs raw inputs.
    #[arg(long, default_value = "catalog.sqlite")]
    pub catalog: PathBuf,

    /// Maximum source blocks and occurrence commits processed per bounded pass.
    #[arg(long, default_value_t = 128)]
    pub max_blocks: usize,

    /// Maximum source rows accepted in any one block.
    #[arg(long, default_value_t = 65_536)]
    pub max_source_rows: usize,

    /// Maximum compressed bytes read from any one source block.
    #[arg(long, default_value_t = 268_435_456)]
    pub max_source_bytes: usize,

    /// Maximum cumulative canonical raw-record bytes decoded per source block.
    #[arg(long, default_value_t = 134_217_728)]
    pub max_decoded_raw_bytes: usize,

    /// Maximum occurrence rows emitted by any one source block.
    #[arg(long, default_value_t = 4_096)]
    pub max_occurrence_rows: usize,

    /// Maximum bytes in one occurrence Parquet object.
    #[arg(long, default_value_t = 67_108_864)]
    pub max_projection_bytes: usize,

    /// Deterministic extractor generation included in projection identity.
    #[arg(long, default_value = engine::DEFAULT_EXTRACTOR_GENERATION)]
    pub extractor_generation: String,

    /// Coordination mode. Clustered processing remains unavailable until its
    /// lease-backed domain orchestration is wired.
    #[arg(long, value_enum, default_value_t = Mode::SingleWriter)]
    pub mode: Mode,

    /// Valkey URL reserved for clustered lease orchestration.
    #[arg(long, env = "SCRY_VALKEY_URL")]
    pub valkey_url: Option<String>,

    /// Optional local HTTP status endpoint (`/` and `/stats.json`).
    #[arg(long, num_args = 0..=1, default_missing_value = "127.0.0.1:4098")]
    pub stats_listen: Option<String>,

    /// Delay after each completed serve reconciliation pass (1..=86400 seconds).
    #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..=86_400))]
    pub reconcile_interval_secs: u64,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum Mode {
    /// One explicitly declared writer, protected by a local exclusive lock.
    SingleWriter,
    /// Projection reads only; does not acquire the writer lock.
    ReadOnly,
    /// Future multi-instance mode. Refuses startup until lease orchestration lands.
    Clustered,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Subcommand)]
pub enum Command {
    /// Probe storage and create or verify the deployment manifest, then exit.
    Init,
    /// Process one bounded source/commit page and fold it into SQLite, then exit.
    Reconcile,
    /// Reconcile repeatedly with a completion-relative delay until SIGINT or SIGTERM.
    Serve,
}

/// Prepared startup state kept alive for the lifetime of a domain engine.
///
/// Holding this value holds the no-Valkey writer lock. `store` and `errors_db` are
/// the explicit seams consumed by the occurrence reconciler.
pub struct RuntimeFoundation {
    pub store: Arc<dyn ObjectStore>,
    pub bucket: String,
    pub deployment: DeploymentManifest,
    pub errors_db: PathBuf,
    _singleton: Option<SingletonLock>,
}

/// Open and validate all correctness-bearing runtime dependencies.
pub async fn prepare(args: &Args) -> Result<RuntimeFoundation> {
    validate_args(args)?;

    let config =
        ObjStoreConfig::from_env().context("loading SCRY_OBJSTORE_* env for the errors role")?;
    let bucket = config.bucket.clone();
    let store = open(&config).await.context("opening error object store")?;

    let probe = ObjectPath::from(format!("_scry/probes/errorsd/{}", Uuid::now_v7()));
    probe_conditional_writes(store.as_ref(), &probe)
        .await
        .context("object store does not provide required conditional-write semantics")?;

    let deployment = match args.command {
        Command::Init => ensure_deployment_manifest(store.as_ref(), args.deployment_id.as_deref())
            .await
            .context("creating or verifying the bucket deployment manifest")?,
        Command::Reconcile | Command::Serve => {
            require_deployment_manifest(store.as_ref(), args.deployment_id.as_deref())
                .await
                .context("requiring the existing bucket deployment manifest")?
        }
    };

    let singleton = if args.command != Command::Init && args.mode == Mode::SingleWriter {
        Some(SingletonLock::acquire(&args.errors_db).with_context(|| {
            format!(
                "claiming single-writer ownership for {}",
                args.errors_db.display()
            )
        })?)
    } else {
        None
    };

    Ok(RuntimeFoundation {
        store,
        bucket,
        deployment,
        errors_db: args.errors_db.clone(),
        _singleton: singleton,
    })
}

fn validate_args(args: &Args) -> Result<()> {
    if let Some(deployment_id) = args.deployment_id.as_deref() {
        validate_deployment_id(deployment_id)?;
    }
    if !(1..=86_400).contains(&args.reconcile_interval_secs) {
        bail!("reconcile_interval_secs must be between 1 and 86400")
    }
    match args.mode {
        Mode::SingleWriter if args.valkey_url.is_some() => {
            bail!("--mode single-writer must not be combined with --valkey-url")
        }
        Mode::Clustered if args.valkey_url.is_none() => {
            bail!("--mode clustered requires --valkey-url")
        }
        Mode::Clustered => bail!(
            "clustered error processing is not available until lease-backed domain orchestration is wired"
        ),
        Mode::ReadOnly if args.command != Command::Init => {
            bail!("reconcile and serve require --mode single-writer")
        }
        Mode::ReadOnly | Mode::SingleWriter => Ok(()),
    }
}

/// Run initialization, bounded one-shot reconciliation, or reconciliation plus serve.
pub async fn run(args: Args) -> Result<()> {
    let foundation = prepare(&args).await?;
    info!(
        bucket = %foundation.bucket,
        deployment_id = %foundation.deployment.deployment_id,
        mode = ?args.mode,
        "errors runtime foundation ready"
    );

    if args.command == Command::Init {
        return Ok(());
    }

    let deployment_id =
        scry_errors::DeploymentId::parse(&foundation.deployment.deployment_id, "deployment_id")
            .context("parsing deployment manifest identity")?;
    let config = engine::ReconcileConfig {
        extractor_generation: args.extractor_generation.clone(),
        max_blocks: args.max_blocks,
        max_source_rows: args.max_source_rows,
        max_source_bytes: args.max_source_bytes,
        max_decoded_raw_bytes: args.max_decoded_raw_bytes,
        projection_limits: scry_errors::projection::ProjectionLimits {
            max_rows: args.max_occurrence_rows,
            max_bytes: args.max_projection_bytes,
            ..Default::default()
        },
        ..Default::default()
    };
    // Configuration errors are startup invariants. Serve only retries failures from an
    // otherwise valid reconciliation pass.
    engine::validate_config(&config)?;
    // Single-writer mode is guarded by the local exclusive lock held in
    // `foundation`, not by a distributed lease, so its fence is always valid.
    let fence: Arc<dyn scry_block::Fence> = Arc::new(scry_block::AlwaysValid);
    if args.command == Command::Reconcile {
        let report = engine::reconcile_once(
            engine::CatalogMode::Owned {
                catalog_path: &args.catalog,
                bucket: &foundation.bucket,
            },
            foundation.store.clone(),
            &foundation.errors_db,
            deployment_id,
            &config,
            fence,
        )
        .await?;
        info!(?report, "occurrence reconciliation completed");
        return Ok(());
    }

    let instance_id = Uuid::now_v7().to_string();
    let status = Arc::new(status::ErrorsStatus::new(
        instance_id.clone(),
        foundation.deployment.deployment_id.clone(),
        args.mode,
    ));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let signal_task = tokio::spawn({
        let shutdown_tx = shutdown_tx.clone();
        async move {
            shutdown_signal().await;
            info!("shutdown signalled; stopping errors reconciliation loop");
            let _ = shutdown_tx.send(true);
        }
    });
    let status_task = args.stats_listen.map(|listen| {
        let local: Arc<dyn scry_status::LocalStatus> = status.clone();
        let mut shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            let result =
                scry_status::serve_status(listen, local, None, instance_id.clone(), async move {
                    while !*shutdown.borrow() {
                        if shutdown.changed().await.is_err() {
                            break;
                        }
                    }
                })
                .await;
            if let Err(error) = result {
                warn!(%error, "errors status endpoint stopped");
            }
        })
    });

    let interval = Duration::from_secs(args.reconcile_interval_secs);
    run_reconcile_loop(interval, shutdown_rx, status, || {
        engine::reconcile_once(
            engine::CatalogMode::Owned {
                catalog_path: &args.catalog,
                bucket: &foundation.bucket,
            },
            foundation.store.clone(),
            &foundation.errors_db,
            deployment_id,
            &config,
            fence.clone(),
        )
    })
    .await;

    let _ = shutdown_tx.send(true);
    signal_task.abort();
    let _ = signal_task.await;
    if let Some(task) = status_task {
        task.await.context("joining errors status task")?;
    }
    drop(foundation);
    Ok(())
}

async fn run_reconcile_loop<F, Fut>(
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
    status: Arc<status::ErrorsStatus>,
    mut reconcile: F,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<engine::EngineReport>>,
{
    while !*shutdown.borrow() {
        match reconcile().await {
            Ok(report) => {
                status.record_success(report);
                info!(?report, "occurrence reconciliation completed");
            }
            Err(error) => {
                status.record_failure();
                warn!(%error, "occurrence reconciliation failed; retrying after interval");
            }
        }

        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            warn!(%error, "failed to install SIGINT handler");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => tokio::select! {
                _ = ctrl_c => {},
                _ = terminate.recv() => {},
            },
            Err(error) => {
                warn!(%error, "failed to install SIGTERM handler");
                ctrl_c.await;
            }
        }
    }

    #[cfg(not(unix))]
    ctrl_c.await;
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use scry_status::LocalStatus;

    use super::*;

    #[tokio::test]
    async fn serve_loop_retries_failures_and_stops_without_an_extra_pass() {
        let status = Arc::new(status::ErrorsStatus::new(
            "instance".to_owned(),
            "deployment".to_owned(),
            Mode::SingleWriter,
        ));
        let calls = Arc::new(AtomicUsize::new(0));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        run_reconcile_loop(Duration::from_millis(1), shutdown_rx, status.clone(), {
            let calls = calls.clone();
            move || {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                let shutdown_tx = shutdown_tx.clone();
                async move {
                    if call == 0 {
                        bail!("transient test failure")
                    }
                    let _ = shutdown_tx.send(true);
                    Ok(engine::EngineReport {
                        candidate_blocks: 3,
                        processed_blocks: 2,
                        occurrence_rows: 7,
                        sources_already_covered: 4,
                        grouping: scry_errors::sqlite::GroupingReport {
                            scanned: 5,
                            failures: 2,
                            drained: true,
                            ..Default::default()
                        },
                        ..Default::default()
                    })
                }
            }
        })
        .await;

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let snapshot = status.snapshot();
        assert_eq!(snapshot.data["reconcile_failures"], 1);
        assert_eq!(snapshot.data["reconcile_successes"], 1);
        assert_eq!(snapshot.data["last_report"]["candidate_blocks"], 3);
        assert_eq!(snapshot.data["last_report"]["processed_blocks"], 2);
        assert_eq!(snapshot.data["last_report"]["pending_blocks"], 1);
        assert_eq!(snapshot.data["last_report"]["occurrence_rows"], 7);
        assert_eq!(snapshot.data["last_report"]["sources_already_covered"], 4);
        assert_eq!(snapshot.data["last_report"]["grouping_scanned"], 5);
        assert_eq!(snapshot.data["last_report"]["grouping_failures"], 2);
        assert_eq!(snapshot.data["last_report"]["grouping_drained"], true);
        assert!(snapshot.data["last_failure_unix_ms"].as_u64().unwrap() > 0);
        assert!(snapshot.data["last_success_unix_ms"].as_u64().unwrap() > 0);
        assert!(snapshot.data["reconcile_lag_ms"].is_u64());
    }

    #[tokio::test]
    async fn serve_loop_honors_shutdown_while_waiting() {
        let status = Arc::new(status::ErrorsStatus::new(
            "instance".to_owned(),
            "deployment".to_owned(),
            Mode::SingleWriter,
        ));
        let calls = Arc::new(AtomicUsize::new(0));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let task = tokio::spawn(run_reconcile_loop(
            Duration::from_secs(60),
            shutdown_rx,
            status,
            {
                let calls = calls.clone();
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    std::future::ready(Ok(engine::EngineReport::default()))
                }
            },
        ));
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("loop should stop promptly")
            .expect("loop task should join");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
