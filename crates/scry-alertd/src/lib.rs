mod evaluator;
mod leases;
mod projection;
mod scheduler;
mod singleton;
mod targets;

use std::{path::PathBuf, str::FromStr, sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use axum::{
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use clap::{Parser, Subcommand, ValueEnum};
use object_store::ObjectStore;
use scry_alert::{
    canonical_json, rule_tombstone_key, sha256_hex, validate_monitor, AlertStore, AlertStoreError,
    AlertsDb, Monitor, MonitorId, MonitorSummary, RuleMutationKind, RuleMutationReceipt,
    RuleTombstone, ALERT_RECORD_SCHEMA_VERSION,
};
use scry_query::client::{QueryScalar, QueryWireClient, ScalarOutcome};
use serde::{Deserialize, Serialize};
use tokio::sync::{watch, Mutex, Semaphore};
use uuid::Uuid;

pub use evaluator::{
    evaluate_monitor_slot, EvaluationContext, EvaluationError, QueryWireExecutor, ScalarExecutor,
    SlotOutcome,
};
pub use leases::{Lease, Leases};
pub use scheduler::{SchedulerConfig, MAX_CONCURRENT_EVALUATIONS};

pub const TOKEN_ENV: &str = "SCRY_ALERTD_TOKEN";
const MAX_CONTROL_REQUEST_BODY_BYTES: usize = 256 * 1024;
const EVALUATION_LEASE_TTL: Duration = Duration::from_secs(45);
const RECONCILE_INTERVAL: Duration = Duration::from_secs(30);
const VALKEY_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const VALKEY_RETRY_MAX: Duration = Duration::from_secs(30);

#[derive(Parser, Debug)]
#[command(about = "Durable scalar alert evaluator and control service")]
pub struct Args {
    #[arg(long, default_value = "alerts.sqlite")]
    pub alerts_db: PathBuf,
    #[arg(long, value_enum, default_value_t = Mode::Clustered)]
    pub mode: Mode,
    #[arg(long, env = "SCRY_VALKEY_URL")]
    pub valkey_url: Option<String>,
    #[arg(long, env = "SCRY_VALKEY_NAMESPACE")]
    pub valkey_namespace: Option<String>,
    /// Allowlisted queryd targets as `ID=HOST:PORT`. Monitor records retain only IDs.
    /// Required by `serve`; maintenance commands do not connect to queryd.
    #[arg(long, value_name = "ID=ADDR")]
    pub queryd: Vec<String>,
    #[arg(long, default_value = "127.0.0.1:4400")]
    pub control_listen: String,
    #[arg(long, default_value_t = 64)]
    pub max_control_requests: usize,
    /// Lateness allowance: seconds after a slot ends before it is evaluated.
    ///
    /// Ingest makes a record queryable only once its block is flushed, which
    /// can take up to ingestd's `--block-max-age-secs` (default 60) plus
    /// upload time. A slot evaluated sooner would miss its own final records
    /// and, because an evaluated slot is never revised, keep that wrong
    /// result. Keep this above the ingest flush age plus upload latency.
    #[arg(
        long,
        value_name = "SECONDS",
        default_value_t = 90,
        value_parser = clap::value_parser!(u64).range(0..=3_600)
    )]
    pub evaluation_delay: u64,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum Mode {
    SingleWriter,
    Clustered,
}

#[derive(Clone, Debug, Subcommand)]
pub enum Command {
    /// Create/validate deployment control-storage prerequisites.
    Init {
        #[arg(long)]
        deployment_id: Option<String>,
    },
    /// Serve the authenticated private control API.
    Serve {
        #[arg(long)]
        deployment_id: Option<String>,
    },
    /// Re-encrypt referenced notification-target secrets with the current key.
    RotateTargetKey {
        #[arg(long)]
        deployment_id: Option<String>,
        #[arg(long)]
        dry_run: bool,
    },
}

pub async fn run(args: Args) -> Result<()> {
    if args.max_control_requests == 0 {
        bail!("--max-control-requests must be at least one");
    }
    let query_targets = Arc::new(parse_query_targets(&args.queryd)?);
    let object_config = scry_objstore::ObjStoreConfig::from_env()?;
    let store = scry_objstore::open(&object_config).await?;
    let probe = object_store::path::Path::from(format!("_scry/probes/alertd/{}", Uuid::new_v4()));
    scry_objstore::probe_conditional_writes(store.as_ref(), &probe)
        .await
        .context("probing alert control-store conditional writes")?;

    match args.command {
        Command::Init { deployment_id } => {
            let manifest = scry_objstore::manifest::ensure_deployment_manifest(
                store.as_ref(),
                deployment_id.as_deref(),
            )
            .await?;
            let deployment = Uuid::parse_str(&manifest.deployment_id)?;
            let _db = AlertsDb::open(&args.alerts_db, deployment)?;
            tracing::info!(deployment_id = %manifest.deployment_id, "alert control storage initialized");
            Ok(())
        }
        Command::Serve { ref deployment_id } => {
            if query_targets.is_empty() {
                bail!("serve requires at least one --queryd ID=ADDR target");
            }
            let token =
                std::env::var(TOKEN_ENV).with_context(|| format!("{TOKEN_ENV} must be set"))?;
            if token.len() < 32 {
                bail!("{TOKEN_ENV} must contain at least 32 bytes");
            }
            let keyring = Arc::new(targets::load_keyring()?);
            // Validate clustered configuration before touching local state;
            // the connection itself is established in the background.
            let valkey = match args.mode {
                Mode::SingleWriter => None,
                Mode::Clustered => {
                    let url = args
                        .valkey_url
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .context("clustered alert mode requires --valkey-url or SCRY_VALKEY_URL")?
                        .to_owned();
                    let keys = scry_valkey::Keyspace::resolve(args.valkey_namespace.as_deref())?;
                    Some((url, keys))
                }
            };
            let local_lock = match args.mode {
                Mode::SingleWriter => Some(singleton::SingletonLock::acquire(&args.alerts_db)?),
                Mode::Clustered => None,
            };
            let manifest = scry_objstore::manifest::require_deployment_manifest(
                store.as_ref(),
                deployment_id.as_deref(),
            )
            .await?;
            let deployment = Uuid::parse_str(&manifest.deployment_id)?;
            let db = Arc::new(Mutex::new(AlertsDb::open(&args.alerts_db, deployment)?));
            // Per-record failures are quarantined or retried; only a failed
            // listing prevents startup.
            projection::reconcile_all(store.as_ref(), &db, &manifest.deployment_id).await?;

            let shutdown = shutdown_channel();
            let leases = match valkey {
                None => Leases::local(),
                Some((url, keys)) => {
                    let (leases, cell) = Leases::valkey_pending();
                    tokio::spawn(connect_valkey_in_background(
                        url,
                        keys,
                        cell,
                        shutdown.clone(),
                    ));
                    leases
                }
            };
            let state = AppState {
                token: Arc::from(token),
                store: store.clone(),
                db: db.clone(),
                permits: Arc::new(Semaphore::new(args.max_control_requests)),
                target_mutations: Arc::new(Semaphore::new(1)),
                test_sends: Arc::new(Semaphore::new(8)),
                query_targets: query_targets.clone(),
                deployment_id: manifest.deployment_id.clone(),
                keyring,
                transport: Arc::new(targets::SecureWebhookTransport),
                leases: leases.clone(),
                _local_lock: Arc::new(local_lock),
            };
            let context = Arc::new(EvaluationContext {
                store,
                db,
                deployment_id: manifest.deployment_id,
                query_targets,
                executor: Arc::new(QueryWireExecutor),
                lease_ttl: EVALUATION_LEASE_TTL,
            });
            let config = SchedulerConfig {
                evaluation_delay: Duration::from_secs(args.evaluation_delay),
                max_concurrent: MAX_CONCURRENT_EVALUATIONS,
                shutdown_grace: scheduler::SHUTDOWN_GRACE,
            };
            serve(
                &args.control_listen,
                state,
                context,
                leases,
                config,
                shutdown,
            )
            .await
        }
        Command::RotateTargetKey {
            deployment_id,
            dry_run,
        } => {
            let keyring = targets::load_keyring()?;
            let manifest = scry_objstore::manifest::require_deployment_manifest(
                store.as_ref(),
                deployment_id.as_deref(),
            )
            .await?;
            let report =
                targets::rotate(store.as_ref(), &manifest.deployment_id, &keyring, dry_run).await?;
            tracing::info!(
                targets = report.targets,
                referenced = report.referenced,
                rotated = report.rotated,
                current = report.current,
                orphaned_skipped = ?report.orphaned,
                failed = report.failed.len(),
                dry_run,
                "notification-target key rotation pass complete"
            );
            for failure in &report.failed {
                tracing::error!(%failure, "notification-target secret not rotated");
            }
            if !report.failed.is_empty() {
                bail!(
                    "{} referenced notification-target secret(s) could not be rotated or verified",
                    report.failed.len()
                );
            }
            Ok(())
        }
    }
}

/// Serve the control API, the scheduler, and periodic reconciliation until
/// `shutdown`, then stop accepting requests and drain evaluations.
async fn serve(
    listen: &str,
    state: AppState,
    context: Arc<EvaluationContext>,
    leases: Leases,
    config: SchedulerConfig,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("binding alert control API {listen}"))?;
    let (stop_tx, stop_rx) = watch::channel(false);
    let scheduler = tokio::spawn(
        scheduler::Scheduler::new(context.clone(), leases, config).run(stop_rx.clone()),
    );
    let reconcile = tokio::spawn(reconcile_loop(context, RECONCILE_INTERVAL, stop_rx));
    tracing::info!(%listen, "scry alert control API ready");
    let served = axum::serve(listener, router(state))
        .with_graceful_shutdown(wait_for_shutdown(shutdown))
        .await
        .context("serving alert control API");
    let _ = stop_tx.send(true);
    if let Err(error) = scheduler.await {
        tracing::error!(error = %error, "alert scheduler task failed");
    }
    if let Err(error) = reconcile.await {
        tracing::error!(error = %error, "alert reconciliation task failed");
    }
    served
}

/// Periodic full reconciliation, independent of the scheduler so a slow
/// object-store listing never delays due evaluations.
async fn reconcile_loop(
    context: Arc<EvaluationContext>,
    period: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Startup already reconciled.
    interval.tick().await;
    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = shutdown.changed() => return,
        }
        tokio::select! {
            result = projection::reconcile_all(context.store.as_ref(), &context.db, &context.deployment_id) => {
                if let Err(error) = result {
                    tracing::warn!(error = %error, "alert projection reconciliation failed");
                }
            }
            _ = shutdown.changed() => return,
        }
    }
}

/// Connect to Valkey with capped exponential backoff and install the lease
/// provider once connected. Until then clustered evaluation fails closed
/// while the control API serves normally.
async fn connect_valkey_in_background(
    url: String,
    keys: scry_valkey::Keyspace,
    cell: Arc<std::sync::OnceLock<scry_valkey::ValkeyLeaseProvider>>,
    mut shutdown: watch::Receiver<bool>,
) {
    let holder = Uuid::new_v4();
    let mut backoff = Duration::from_secs(1);
    loop {
        match tokio::time::timeout(
            VALKEY_CONNECT_TIMEOUT,
            scry_valkey::ValkeyClient::connect(&url, holder, keys.clone()),
        )
        .await
        {
            Ok(Ok(client)) => {
                let _ = cell.set(scry_valkey::ValkeyLeaseProvider::new(client));
                tracing::info!("alert evaluation leases available");
                return;
            }
            Ok(Err(error)) => {
                tracing::warn!(error = %error, retry_in = ?backoff, "Valkey unavailable; alert evaluation paused");
            }
            Err(_) => {
                tracing::warn!(retry_in = ?backoff, "Valkey connect timed out; alert evaluation paused");
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = shutdown.changed() => return,
        }
        backoff = (backoff * 2).min(VALKEY_RETRY_MAX);
    }
}

/// SIGINT/SIGTERM fan-out. Mirrors `scry_server::shutdown` without making the
/// alert role depend on the server crate.
fn shutdown_channel() -> watch::Receiver<bool> {
    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        let ctrl_c = async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                tracing::warn!(error = %error, "failed to install SIGINT handler");
                std::future::pending::<()>().await;
            }
        };
        #[cfg(unix)]
        let terminate = async {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut signal) => {
                    signal.recv().await;
                }
                Err(error) => {
                    tracing::warn!(error = %error, "failed to install SIGTERM handler");
                    std::future::pending::<()>().await;
                }
            }
        };
        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();
        tokio::select! {
            _ = ctrl_c => {}
            _ = terminate => {}
        }
        tracing::info!("alert service shutdown requested");
        let _ = tx.send(true);
    });
    rx
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    while !*shutdown.borrow_and_update() {
        if shutdown.changed().await.is_err() {
            // The signal task never exits without sending; treat a dropped
            // sender as "never shut down" rather than an immediate stop.
            std::future::pending::<()>().await;
        }
    }
}

#[derive(Clone)]
struct AppState {
    token: Arc<str>,
    store: Arc<dyn ObjectStore>,
    db: Arc<Mutex<AlertsDb>>,
    permits: Arc<Semaphore>,
    target_mutations: Arc<Semaphore>,
    test_sends: Arc<Semaphore>,
    query_targets: Arc<Vec<QueryTarget>>,
    deployment_id: String,
    keyring: Arc<scry_alert::SecretKeyring>,
    transport: Arc<dyn targets::WebhookTransport>,
    leases: Leases,
    _local_lock: Arc<Option<singleton::SingletonLock>>,
}

#[derive(Clone, Debug)]
pub struct QueryTarget {
    pub id: String,
    pub address: String,
}

fn control_state_for_test(
    token: String,
    store: Arc<dyn ObjectStore>,
    db: AlertsDb,
    query_targets: Vec<QueryTarget>,
) -> Result<AppState> {
    let deployment_id = db.deployment_id().to_string();
    let keyring = targets::parse_keyring("test:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", None)?;
    Ok(AppState {
        token: Arc::from(token),
        store,
        db: Arc::new(Mutex::new(db)),
        permits: Arc::new(Semaphore::new(16)),
        target_mutations: Arc::new(Semaphore::new(1)),
        test_sends: Arc::new(Semaphore::new(4)),
        query_targets: Arc::new(query_targets),
        deployment_id,
        keyring: Arc::new(keyring),
        transport: Arc::new(targets::SecureWebhookTransport),
        leases: Leases::local(),
        _local_lock: Arc::new(None),
    })
}

pub async fn serve_control_for_test(
    listener: tokio::net::TcpListener,
    token: String,
    store: Arc<dyn ObjectStore>,
    db: AlertsDb,
    query_targets: Vec<QueryTarget>,
) -> Result<()> {
    let state = control_state_for_test(token, store, db, query_targets)?;
    axum::serve(listener, router(state))
        .await
        .context("serving test alert control API")
}

pub async fn serve_control_with_reconciliation_for_test(
    listener: tokio::net::TcpListener,
    token: String,
    store: Arc<dyn ObjectStore>,
    db: AlertsDb,
    reconciliation_interval: Duration,
) -> Result<()> {
    let state = control_state_for_test(token, store, db, Vec::new())?;
    let reconcile_state = state.clone();
    let reconcile = async move {
        let mut interval = tokio::time::interval(reconciliation_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if let Err(error) = projection::reconcile_all(
                reconcile_state.store.as_ref(),
                reconcile_state.db.as_ref(),
                &reconcile_state.deployment_id,
            )
            .await
            {
                tracing::warn!(error = %error, "test alert projection reconciliation failed");
            }
        }
    };
    tokio::select! {
        result = axum::serve(listener, router(state)) => result.context("serving test alert control API"),
        _ = reconcile => unreachable!("test reconciliation loop is infinite"),
    }
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/monitors", get(list_monitors).post(create_monitor))
        .route(
            "/v1/monitors/{id}",
            get(get_monitor).put(update_monitor).delete(delete_monitor),
        )
        .route("/v1/monitors/validate", post(validate_monitor_request))
        .route("/v1/monitors/test", post(test_monitor_request))
        .merge(targets::routes())
        .layer(DefaultBodyLimit::max(MAX_CONTROL_REQUEST_BODY_BYTES))
        .with_state(state)
}

#[derive(Deserialize)]
struct PageQuery {
    after: Option<String>,
    limit: Option<usize>,
}

#[derive(Serialize)]
struct MonitorList {
    monitors: Vec<MonitorSummaryDto>,
    next: Option<String>,
}

#[derive(Serialize)]
struct MonitorSummaryDto {
    monitor: Monitor,
    state: Option<scry_alert::AlertState>,
}

impl From<MonitorSummary> for MonitorSummaryDto {
    fn from(value: MonitorSummary) -> Self {
        let state = value
            .state
            .filter(|state| state.monitor_revision == value.monitor.revision);
        Self {
            monitor: value.monitor,
            state,
        }
    }
}

async fn list_monitors(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(page): Query<PageQuery>,
) -> Result<Json<MonitorList>, ApiError> {
    authenticate(&state, &headers)?;
    let _permit = admit_control(&state)?;
    let after = page.after.as_deref().map(parse_id).transpose()?;
    let limit = page.limit.unwrap_or(100).clamp(1, 500);
    let rows = state.db.lock().await.list_monitors(after, limit)?;
    let next = (rows.len() == limit)
        .then(|| rows.last().map(|row| row.monitor.id.to_string()))
        .flatten();
    Ok(Json(MonitorList {
        monitors: rows.into_iter().map(Into::into).collect(),
        next,
    }))
}

async fn get_monitor(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<MonitorSummaryDto>, ApiError> {
    authenticate(&state, &headers)?;
    let _permit = admit_control(&state)?;
    let id = parse_id(&id)?;
    state
        .db
        .lock()
        .await
        .get_monitor(id)?
        .map(MonitorSummaryDto::from)
        .map(Json)
        .ok_or(ApiError::NotFound)
}

async fn create_monitor(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(monitor): Json<Monitor>,
) -> Result<(StatusCode, Json<MonitorSummaryDto>), ApiError> {
    authenticate(&state, &headers)?;
    let _permit = admit_control(&state)?;
    let command_id = require_idempotency(&headers)?;
    if monitor.revision != 1 {
        return Err(ApiError::BadRequest(
            "new monitor revision must be one".into(),
        ));
    }
    persist_monitor(&state, monitor, command_id, RuleMutationKind::Create)
        .await
        .map(|body| (StatusCode::CREATED, Json(body)))
}

async fn update_monitor(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(monitor): Json<Monitor>,
) -> Result<Json<MonitorSummaryDto>, ApiError> {
    authenticate(&state, &headers)?;
    let _permit = admit_control(&state)?;
    let command_id = require_idempotency(&headers)?;
    let id = parse_id(&id)?;
    if id != monitor.id {
        return Err(ApiError::BadRequest(
            "path and body monitor IDs differ".into(),
        ));
    }
    let expected = expected_revision(&headers)?;
    if monitor.revision != expected.saturating_add(1) {
        return Err(ApiError::Conflict);
    }
    let durable_head = AlertStore::new(state.store.as_ref())
        .read_rule_head(id)
        .await?;
    if durable_head.value.deleted
        || (durable_head.value.revision != expected
            && durable_head.value.revision != monitor.revision)
    {
        return Err(ApiError::Conflict);
    }
    persist_monitor(&state, monitor, command_id, RuleMutationKind::Update)
        .await
        .map(Json)
}

/// Delete (tombstone) a monitor. A replayed command resumes exactly like a
/// target delete: success if the head already carries this command's
/// tombstone, otherwise finish the tombstone CAS from the expected revision,
/// otherwise a conflict.
async fn delete_monitor(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    authenticate(&state, &headers)?;
    let _permit = admit_control(&state)?;
    let command_id = require_idempotency(&headers)?;
    let id = parse_id(&id)?;
    let expected = expected_revision(&headers)?;
    let alert_store = AlertStore::new(state.store.as_ref());
    let staged = alert_store.read_tombstone(id, command_id).await?;
    if staged
        .as_ref()
        .is_some_and(|tombstone| tombstone.revision != expected)
    {
        return Err(ApiError::Conflict);
    }
    let head = alert_store.read_rule_head(id).await?;
    if let Some(tombstone) = &staged {
        if head.value.deleted
            && head.value.tombstone_key.as_deref()
                == Some(rule_tombstone_key(id, command_id).as_str())
        {
            state.db.lock().await.tombstone_monitor(
                id,
                tombstone.revision,
                tombstone.deleted_at_unix_nano,
            )?;
            return Ok(StatusCode::NO_CONTENT);
        }
    }
    if head.value.deleted || head.value.revision != expected {
        return Err(ApiError::Conflict);
    }
    let tombstone = staged.unwrap_or_else(|| RuleTombstone {
        schema_version: ALERT_RECORD_SCHEMA_VERSION,
        monitor_id: id,
        revision: expected,
        command_id: command_id.to_owned(),
        deleted_at_unix_nano: chrono::Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_default()
            .max(0) as u64,
    });
    alert_store.tombstone_rule(&tombstone, head.version).await?;
    state.db.lock().await.tombstone_monitor(
        id,
        tombstone.revision,
        tombstone.deleted_at_unix_nano,
    )?;
    Ok(StatusCode::NO_CONTENT)
}

async fn validate_monitor_request(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(monitor): Json<Monitor>,
) -> Result<StatusCode, ApiError> {
    authenticate(&state, &headers)?;
    let _permit = admit_control(&state)?;
    validate_monitor(&monitor).map_err(|error| ApiError::BadRequest(error.to_string()))?;
    require_known_target(&state, &monitor)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
struct TestEvaluation {
    outcome: &'static str,
    value: Option<f64>,
    error_class: Option<&'static str>,
}

async fn test_monitor_request(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(monitor): Json<Monitor>,
) -> Result<Json<TestEvaluation>, ApiError> {
    authenticate(&state, &headers)?;
    let _permit = admit_control(&state)?;
    validate_monitor(&monitor).map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let target = require_known_target(&state, &monitor)?;
    let now = chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_default()
        .max(0) as u64;
    let request = evaluator::scalar_request(&monitor, now, format!("alert-test:{}", monitor.id));
    let result = QueryWireClient::new(&target.address)
        .with_limits(evaluator::scalar_limits())
        .scalar(request)
        .await;
    let response = match result {
        Ok(ScalarOutcome::Value(QueryScalar::Number(value))) => TestEvaluation {
            outcome: "value",
            value: Some(value),
            error_class: None,
        },
        Ok(ScalarOutcome::Value(QueryScalar::Boolean(value))) => TestEvaluation {
            outcome: "value",
            value: Some(if value { 1.0 } else { 0.0 }),
            error_class: None,
        },
        Ok(ScalarOutcome::NoData) => TestEvaluation {
            outcome: "no_data",
            value: None,
            error_class: None,
        },
        Err(error) => TestEvaluation {
            outcome: "error",
            value: None,
            error_class: Some(evaluator::query_error_class(&error)),
        },
    };
    Ok(Json(response))
}

async fn persist_monitor(
    state: &AppState,
    monitor: Monitor,
    command_id: &str,
    kind: RuleMutationKind,
) -> Result<MonitorSummaryDto, ApiError> {
    validate_monitor(&monitor).map_err(|error| ApiError::BadRequest(error.to_string()))?;
    require_known_target(state, &monitor)?;
    let request_sha256 = sha256_hex(&canonical_json(&monitor).map_err(AlertStoreError::from)?);
    let receipt = RuleMutationReceipt {
        schema_version: ALERT_RECORD_SCHEMA_VERSION,
        command_id: command_id.to_owned(),
        kind,
        monitor_id: monitor.id,
        revision: monitor.revision,
        request_sha256,
    };
    let alert_store = AlertStore::new(state.store.as_ref());
    if let Some(existing) = alert_store.read_rule_mutation(command_id).await? {
        if existing != receipt {
            return Err(ApiError::Conflict);
        }
        let durable = alert_store.read_rule(monitor.id).await?;
        state.db.lock().await.fold_rule(&durable.value)?;
        return state
            .db
            .lock()
            .await
            .get_monitor(monitor.id)?
            .map(MonitorSummaryDto::from)
            .ok_or(ApiError::NotFound);
    }
    // The revision object is keyed by this command, so a revision left behind
    // by another command that never published cannot block this one.
    alert_store
        .create_rule_revision(&monitor, command_id)
        .await?;
    // Publish the command receipt only after the rule head is durable. A crash in
    // between is repaired by the idempotent immutable rule write on retry.
    alert_store.record_rule_mutation(&receipt).await?;
    state.db.lock().await.fold_rule(&monitor)?;
    state
        .db
        .lock()
        .await
        .get_monitor(monitor.id)?
        .map(MonitorSummaryDto::from)
        .ok_or(ApiError::NotFound)
}

fn require_known_target<'a>(
    state: &'a AppState,
    monitor: &Monitor,
) -> Result<&'a QueryTarget, ApiError> {
    state
        .query_targets
        .iter()
        .find(|target| target.id == monitor.query.target_id)
        .ok_or_else(|| ApiError::BadRequest("unknown query target ID".into()))
}

fn parse_query_targets(values: &[String]) -> Result<Vec<QueryTarget>> {
    let mut targets = Vec::with_capacity(values.len());
    for value in values {
        let (id, address) = value
            .split_once('=')
            .with_context(|| format!("queryd target `{value}` must be ID=ADDR"))?;
        if id.is_empty() || id.len() > 64 || address.is_empty() {
            bail!("invalid queryd target `{value}`");
        }
        if targets.iter().any(|target: &QueryTarget| target.id == id) {
            bail!("duplicate queryd target ID `{id}`");
        }
        targets.push(QueryTarget {
            id: id.to_owned(),
            address: address.to_owned(),
        });
    }
    Ok(targets)
}

fn parse_id(value: &str) -> Result<MonitorId, ApiError> {
    Uuid::from_str(value)
        .map(MonitorId)
        .map_err(|_| ApiError::BadRequest("invalid monitor ID".into()))
}

fn expected_revision(headers: &HeaderMap) -> Result<u64, ApiError> {
    headers
        .get(header::IF_MATCH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim_matches('"').parse::<u64>().ok())
        .ok_or_else(|| ApiError::BadRequest("If-Match revision is required".into()))
}

fn authenticate(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    let supplied = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or(ApiError::Unauthorized)?;
    if constant_time_eq(supplied.as_bytes(), state.token.as_bytes()) {
        Ok(())
    } else {
        Err(ApiError::Unauthorized)
    }
}

fn admit_control(state: &AppState) -> Result<tokio::sync::SemaphorePermit<'_>, ApiError> {
    state
        .permits
        .try_acquire()
        .map_err(|_| ApiError::Overloaded)
}

fn require_idempotency(headers: &HeaderMap) -> Result<&str, ApiError> {
    let value = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| ApiError::BadRequest("Idempotency-Key is required".into()))?;
    Uuid::parse_str(value)
        .map_err(|_| ApiError::BadRequest("Idempotency-Key must be a UUID".into()))?;
    Ok(value)
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut different = 0u8;
    for (&left, &right) in left.iter().zip(right) {
        different |= left ^ right;
    }
    different == 0
}

#[derive(Debug)]
enum ApiError {
    Unauthorized,
    NotFound,
    Conflict,
    Overloaded,
    Unavailable(String),
    BadRequest(String),
    /// A server-side fault the client cannot fix (keyring, local database,
    /// or a corrupt durable record).
    Internal(String),
    Database(scry_alert::AlertsDbError),
    Store(AlertStoreError),
}

impl From<scry_alert::AlertsDbError> for ApiError {
    fn from(value: scry_alert::AlertsDbError) -> Self {
        Self::Database(value)
    }
}

impl From<AlertStoreError> for ApiError {
    fn from(value: AlertStoreError) -> Self {
        Self::Store(value)
    }
}

impl ApiError {
    fn status_and_message(self) -> (StatusCode, String) {
        match self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized".to_owned()),
            Self::NotFound | Self::Store(AlertStoreError::Missing { .. }) => {
                (StatusCode::NOT_FOUND, "resource not found".to_owned())
            }
            Self::Conflict
            | Self::Store(AlertStoreError::Conflict { .. } | AlertStoreError::Collision { .. }) => {
                (
                    StatusCode::CONFLICT,
                    "resource revision conflict".to_owned(),
                )
            }
            Self::Overloaded => (
                StatusCode::SERVICE_UNAVAILABLE,
                "alert control API overloaded".to_owned(),
            ),
            Self::Unavailable(message) => (StatusCode::SERVICE_UNAVAILABLE, message),
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
            Self::Internal(message) => (StatusCode::INTERNAL_SERVER_ERROR, message),
            Self::Database(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
            // Only a failed object-store call is an upstream (gateway) error;
            // an invalid durable record is an internal fault.
            Self::Store(error @ AlertStoreError::Object { .. }) => {
                (StatusCode::BAD_GATEWAY, error.to_string())
            }
            Self::Store(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
        }
    }
}

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = self.status_and_message();
        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_errors_map_to_client_meaningful_statuses() {
        let path = || "p".to_owned();
        let status = |error| ApiError::Store(error).status_and_message().0;
        assert_eq!(
            status(AlertStoreError::Missing { path: path() }),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            status(AlertStoreError::Conflict { path: path() }),
            StatusCode::CONFLICT
        );
        assert_eq!(
            status(AlertStoreError::Collision { path: path() }),
            StatusCode::CONFLICT
        );
        assert_eq!(
            status(AlertStoreError::Corrupt {
                path: path(),
                message: "m"
            }),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            status(AlertStoreError::Oversized { path: path() }),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            status(AlertStoreError::Object {
                path: path(),
                source: object_store::Error::NotImplemented {
                    operation: "x".into(),
                    implementer: "y".into(),
                },
            }),
            StatusCode::BAD_GATEWAY
        );
    }

    #[test]
    fn evaluation_delay_defaults_above_the_ingest_flush_age() {
        let args = Args::try_parse_from(["alert", "serve"]).unwrap();
        assert_eq!(args.evaluation_delay, 90);
        assert!(
            args.evaluation_delay > 60,
            "must exceed ingestd's default --block-max-age-secs"
        );
        assert!(Args::try_parse_from(["alert", "--evaluation-delay", "3601", "serve"]).is_err());
    }
}
