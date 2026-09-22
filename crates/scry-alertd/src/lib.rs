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
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use scry_alert::{
    canonical_json, evaluate, sha256_hex, transition_key, validate_monitor, AlertState, AlertStore,
    AlertsDb, DurableObservation, Monitor, MonitorId, MonitorSummary, Observation,
    RuleMutationKind, RuleMutationReceipt, RuleTombstone, StateHead, TransitionRecord,
    ALERT_RECORD_SCHEMA_VERSION,
};
use scry_cluster::{LeaseGuard, LeaseProvider, LocalGuard, LocalLeaseProvider};
use scry_query::client::{
    QueryClientError, QueryDeadlines, QueryLimits, QueryScalar, QueryWireClient, ScalarOutcome,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Semaphore};
use uuid::Uuid;

pub const TOKEN_ENV: &str = "SCRY_ALERTD_TOKEN";
const MAX_CONTROL_REQUEST_BODY_BYTES: usize = 256 * 1024;

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
        Command::Serve { deployment_id } => {
            if query_targets.is_empty() {
                bail!("serve requires at least one --queryd ID=ADDR target");
            }
            let token =
                std::env::var(TOKEN_ENV).with_context(|| format!("{TOKEN_ENV} must be set"))?;
            if token.len() < 32 {
                bail!("{TOKEN_ENV} must contain at least 32 bytes");
            }
            let keyring = Arc::new(targets::load_keyring()?);
            let manifest = scry_objstore::manifest::require_deployment_manifest(
                store.as_ref(),
                deployment_id.as_deref(),
            )
            .await?;
            let deployment = Uuid::parse_str(&manifest.deployment_id)?;
            let mut db = AlertsDb::open(&args.alerts_db, deployment)?;
            rebuild_projection(store.as_ref(), &mut db).await?;
            targets::rebuild_projection(store.as_ref(), &mut db).await?;
            let state = AppState {
                token: Arc::from(token),
                store,
                db: Arc::new(Mutex::new(db)),
                permits: Arc::new(Semaphore::new(args.max_control_requests)),
                target_mutations: Arc::new(Semaphore::new(1)),
                test_sends: Arc::new(Semaphore::new(8)),
                query_targets,
                deployment_id: manifest.deployment_id,
                keyring,
                transport: Arc::new(targets::SecureWebhookTransport),
                delivery_leases: Arc::new(DeliveryLeases::new(LocalLeaseProvider::new())),
                _local_lock: Arc::new(None),
            };
            match args.mode {
                Mode::SingleWriter => {
                    let lock = singleton::SingletonLock::acquire(&args.alerts_db)?;
                    let mut state = state;
                    state._local_lock = Arc::new(Some(lock));
                    serve_with_scheduler(
                        &args.control_listen,
                        state,
                        scry_cluster::LocalLeaseProvider::new(),
                    )
                    .await
                }
                Mode::Clustered => {
                    let url = args
                        .valkey_url
                        .as_deref()
                        .filter(|value| !value.trim().is_empty())
                        .context("clustered alert mode requires --valkey-url or SCRY_VALKEY_URL")?;
                    let keys = scry_valkey::Keyspace::resolve(args.valkey_namespace.as_deref())?;
                    let client =
                        scry_valkey::ValkeyClient::connect(url, Uuid::new_v4(), keys).await?;
                    let provider = scry_valkey::ValkeyLeaseProvider::new(client);
                    let mut state = state;
                    state.delivery_leases = Arc::new(DeliveryLeases::Valkey(provider.clone()));
                    serve_with_scheduler(&args.control_listen, state, provider).await
                }
            }
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
            let (scanned, rotated) =
                targets::rotate(store.as_ref(), &manifest.deployment_id, &keyring, dry_run).await?;
            tracing::info!(
                scanned,
                rotated,
                dry_run,
                "notification-target key rotation pass complete"
            );
            Ok(())
        }
    }
}

struct ProjectionEntry {
    monitor_id: MonitorId,
    live: Option<ProjectionLiveEntry>,
}

struct ProjectionLiveEntry {
    monitor: Monitor,
    state: Option<(String, TransitionRecord, StateHead)>,
}

async fn load_projection(
    store: &dyn ObjectStore,
    deployment_id: Uuid,
) -> Result<Vec<ProjectionEntry>> {
    let prefix = object_store::path::Path::from("_scry/alerts/v1/rules");
    let mut listed = store.list(Some(&prefix));
    let mut heads = Vec::new();
    while let Some(meta) = listed.try_next().await? {
        if meta.location.as_ref().ends_with("/head.json") {
            heads.push(meta.location.to_string());
            if heads.len() > 100_000 {
                bail!("alert rule-head rebuild exceeds 100000 monitors");
            }
        }
    }
    heads.sort_unstable();
    let alert_store = AlertStore::new(store);
    let mut entries = Vec::with_capacity(heads.len());
    for key in heads {
        let Some(id) = key
            .strip_prefix("_scry/alerts/v1/rules/")
            .and_then(|tail| tail.strip_suffix("/head.json"))
            .and_then(|id| Uuid::parse_str(id).ok())
            .map(MonitorId)
        else {
            continue;
        };
        let rule_head = alert_store.read_rule_head(id).await?;
        if rule_head.value.schema_version != ALERT_RECORD_SCHEMA_VERSION
            || rule_head.value.monitor_id != id
        {
            bail!("alert rule head `{key}` does not match its object key");
        }
        if rule_head.value.deleted {
            entries.push(ProjectionEntry {
                monitor_id: id,
                live: None,
            });
            continue;
        }
        let monitor = alert_store.read_rule(id).await?.value;
        if monitor.id != id
            || monitor.revision != rule_head.value.revision
            || scry_alert::rule_revision_key(id, monitor.revision) != rule_head.value.revision_key
        {
            bail!("alert rule revision for `{id}` does not match its head");
        }
        validate_monitor(&monitor)?;
        let state = if let Some(head) = alert_store.read_state_head(id).await? {
            if head.value.monitor_id != id || head.value.deployment_id != deployment_id.to_string()
            {
                bail!("alert state head for `{id}` has invalid ownership");
            }
            let transition = alert_store
                .read_current_transition(&head.value)
                .await?
                .value;
            Some((head.value.transition_key.clone(), transition, head.value))
        } else {
            None
        };
        entries.push(ProjectionEntry {
            monitor_id: id,
            live: Some(ProjectionLiveEntry { monitor, state }),
        });
    }
    Ok(entries)
}

fn fold_projection(db: &mut AlertsDb, entries: Vec<ProjectionEntry>) -> Result<()> {
    for entry in entries {
        let Some(live) = entry.live else {
            db.delete_monitor(entry.monitor_id)?;
            continue;
        };
        db.fold_rule(&live.monitor)?;
        if let Some((key, transition, head)) = live.state {
            db.fold_transition(&key, &transition, &head)?;
        }
    }
    Ok(())
}

async fn rebuild_projection(store: &dyn ObjectStore, db: &mut AlertsDb) -> Result<()> {
    let entries = load_projection(store, db.deployment_id()).await?;
    fold_projection(db, entries)
}

async fn serve_with_scheduler<L>(listen: &str, state: AppState, provider: L) -> Result<()>
where
    L: LeaseProvider + Clone + Send + Sync + 'static,
    L::Guard: Send,
{
    let scheduler_state = state.clone();
    let scheduler = async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut passes_until_reconcile = 0u8;
        loop {
            interval.tick().await;
            if passes_until_reconcile == 0 {
                if let Err(error) = reconcile_projection(&scheduler_state).await {
                    tracing::warn!(error = %error, "alert projection reconciliation failed");
                }
                if let Err(error) = targets::reconcile_projection(
                    scheduler_state.store.as_ref(),
                    scheduler_state.db.as_ref(),
                )
                .await
                {
                    tracing::warn!(error = %error, "notification-target projection reconciliation failed");
                }
                passes_until_reconcile = 30;
            }
            passes_until_reconcile = passes_until_reconcile.saturating_sub(1);
            if let Err(error) = scheduler_pass(&provider, &scheduler_state).await {
                tracing::warn!(error = %error, "alert scheduler pass failed");
            }
        }
    };
    let app = router(state);
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("binding alert control API {listen}"))?;
    tracing::info!(%listen, "scry alert control API ready");
    tokio::select! {
        result = axum::serve(listener, app) => result.context("serving alert control API"),
        _ = scheduler => unreachable!("alert scheduler loop is infinite"),
    }
}

async fn reconcile_projection(state: &AppState) -> Result<()> {
    let deployment_id = state.db.lock().await.deployment_id();
    let entries = load_projection(state.store.as_ref(), deployment_id).await?;
    let mut db = state.db.lock().await;
    fold_projection(&mut db, entries)
}

async fn scheduler_pass<L: LeaseProvider>(provider: &L, state: &AppState) -> Result<()> {
    let now = chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_default()
        .max(0) as u64;
    const MAX_CONCURRENT_EVALUATIONS: usize = 8;
    let mut jobs = futures::stream::FuturesUnordered::new();
    let mut cursor = None;
    loop {
        let rules = state.db.lock().await.list_monitors(cursor, 500)?;
        if rules.is_empty() {
            break;
        }
        let page_len = rules.len();
        cursor = rules.last().map(|summary| summary.monitor.id);
        for summary in rules {
            let monitor = summary.monitor;
            let interval_nanos = monitor.every_seconds.saturating_mul(1_000_000_000);
            let Some(slot) = scry_alert::slot_at(
                monitor.id,
                now.saturating_sub(interval_nanos),
                monitor.every_seconds,
                monitor.jitter_seconds,
            ) else {
                continue;
            };
            if now < slot.execute_at_unix_nano
                || summary
                    .state
                    .as_ref()
                    .is_some_and(|current| current.last_slot_id >= slot.id)
            {
                continue;
            }
            let Some(target) = state
                .query_targets
                .iter()
                .find(|target| target.id == monitor.query.target_id)
                .cloned()
            else {
                tracing::warn!(monitor_id = %monitor.id, target = %monitor.query.target_id, "alert monitor references unknown target");
                continue;
            };
            while jobs.len() >= MAX_CONCURRENT_EVALUATIONS {
                if let Some((monitor_id, due_slot, Err(error))) = jobs.next().await {
                    tracing::warn!(%monitor_id, slot = due_slot, error = %error, "alert evaluation failed");
                }
            }
            let context = EvaluationContext {
                store: state.store.clone(),
                db: state.db.clone(),
                target,
                lease_ttl: Duration::from_secs(45),
            };
            jobs.push(async move {
                let result = evaluate_monitor_slot(
                    provider,
                    &context,
                    &monitor,
                    slot.id,
                    slot.end_unix_nano,
                )
                .await;
                (monitor.id, slot.id, result)
            });
        }
        if page_len < 500 {
            break;
        }
    }
    while let Some((monitor_id, slot, result)) = jobs.next().await {
        if let Err(error) = result {
            tracing::warn!(%monitor_id, slot, error = %error, "alert evaluation failed");
        }
    }
    Ok(())
}

#[derive(Clone)]
enum DeliveryLeases {
    Local(LocalLeaseProvider),
    Valkey(scry_valkey::ValkeyLeaseProvider),
}

enum DeliveryLease {
    Local(LocalGuard),
    Valkey(scry_valkey::ValkeyLease),
}

impl DeliveryLeases {
    fn new(provider: LocalLeaseProvider) -> Self {
        Self::Local(provider)
    }

    async fn try_acquire(&self, key: &str, ttl: Duration) -> Result<Option<DeliveryLease>> {
        match self {
            Self::Local(provider) => provider
                .try_acquire(key, ttl)
                .await
                .map(|guard| guard.map(DeliveryLease::Local)),
            Self::Valkey(provider) => provider
                .try_acquire(key, ttl)
                .await
                .map(|guard| guard.map(DeliveryLease::Valkey)),
        }
    }
}

impl DeliveryLease {
    fn fence(&self) -> Arc<dyn scry_block::Fence> {
        match self {
            Self::Local(guard) => guard.fence(),
            Self::Valkey(guard) => guard.fence(),
        }
    }

    async fn release(self) {
        match self {
            Self::Local(guard) => guard.release().await,
            Self::Valkey(guard) => guard.release().await,
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
    delivery_leases: Arc<DeliveryLeases>,
    _local_lock: Arc<Option<singleton::SingletonLock>>,
}

#[derive(Clone, Debug)]
pub struct QueryTarget {
    pub id: String,
    pub address: String,
}

pub struct EvaluationContext {
    pub store: Arc<dyn ObjectStore>,
    pub db: Arc<Mutex<AlertsDb>>,
    pub target: QueryTarget,
    pub lease_ttl: Duration,
}

/// Evaluate and durably commit one monitor slot while holding its cluster lease.
///
/// The generic lease provider keeps the engine testable with `LocalLeaseProvider`
/// while production passes `ValkeyLeaseProvider`.
pub async fn evaluate_monitor_slot<L: LeaseProvider>(
    provider: &L,
    context: &EvaluationContext,
    monitor: &Monitor,
    slot_id: u64,
    slot_end_unix_nano: u64,
) -> Result<Option<AlertState>> {
    if context.target.id != monitor.query.target_id {
        bail!(
            "query target `{}` does not match monitor target `{}`",
            context.target.id,
            monitor.query.target_id
        );
    }
    let key = format!("lease/alert/eval/{}", monitor.id);
    let Some(guard) = provider.try_acquire(&key, context.lease_ttl).await? else {
        return Ok(None);
    };
    let fence = guard.fence();
    let outcome = evaluate_under_guard(
        fence.as_ref(),
        context.store.clone(),
        context.db.clone(),
        &context.target,
        monitor,
        slot_id,
        slot_end_unix_nano,
    )
    .await;
    guard.release().await;
    outcome.map(Some)
}

async fn evaluate_under_guard(
    fence: &dyn scry_block::Fence,
    store: Arc<dyn ObjectStore>,
    db: Arc<Mutex<AlertsDb>>,
    target: &QueryTarget,
    monitor: &Monitor,
    slot_id: u64,
    slot_end_unix_nano: u64,
) -> Result<AlertState> {
    fence
        .check()
        .context("alert evaluation lease lost before query")?;
    let lookback = monitor.query.lookback_seconds.saturating_mul(1_000_000_000);
    let start = slot_end_unix_nano.saturating_sub(lookback);
    // Query wire bounds are inclusive. Exclude the slot end by sending end-1,
    // yielding the design's half-open [start, end) event-time window.
    let inclusive_end = slot_end_unix_nano.saturating_sub(1);
    let request = scry_query::QueryRequest {
        signal: signal_byte(monitor.query.signal),
        query: scry_query::Query {
            matchers: monitor
                .query
                .matchers
                .iter()
                .map(|matcher| (matcher.name.clone(), matcher.value.clone()))
                .collect(),
            ts_min: Some(start),
            ts_max: Some(inclusive_end),
            ..Default::default()
        },
        sql: Some(monitor.query.sql.clone()),
        limit: None,
        request_id: Some(format!("alert:{}:{slot_id}", monitor.id)),
        live: false,
    };
    let observation = if monitor.enabled {
        let client = QueryWireClient::new(&target.address)
            .with_deadlines(QueryDeadlines {
                connect: Duration::from_secs(5),
                write: Duration::from_secs(5),
                total: Duration::from_secs(30),
            })
            .with_limits(QueryLimits {
                max_frames: 64,
                max_bytes: 1024 * 1024,
                max_rows: 1,
            });
        match client.scalar(request).await {
            Ok(ScalarOutcome::Value(QueryScalar::Number(value))) => Observation::Value(value),
            Ok(ScalarOutcome::Value(QueryScalar::Boolean(value))) => {
                Observation::Value(if value { 1.0 } else { 0.0 })
            }
            Ok(ScalarOutcome::NoData) => Observation::NoData,
            Err(error) => Observation::Error {
                class: query_error_class(&error).to_owned(),
            },
        }
    } else {
        Observation::NoData
    };

    let alert_store = AlertStore::new(store.as_ref());
    let latest_rule = alert_store.read_rule(monitor.id).await?;
    if latest_rule.value.revision != monitor.revision {
        bail!("monitor revision changed during evaluation");
    }
    let current_head = alert_store.read_state_head(monitor.id).await?;
    let current_transition = if let Some(head) = &current_head {
        Some(
            alert_store
                .read_current_transition(&head.value)
                .await?
                .value,
        )
    } else {
        None
    };
    let prior = current_transition
        .as_ref()
        .map(|record| record.state.clone());
    let (next, _) = evaluate(
        monitor,
        prior.as_ref(),
        observation.clone(),
        slot_id,
        slot_end_unix_nano,
    );
    if prior
        .as_ref()
        .is_some_and(|state| state.last_slot_id >= slot_id)
    {
        if let (Some(head), Some(transition)) = (&current_head, &current_transition) {
            db.lock()
                .await
                .fold_transition(&head.value.transition_key, transition, &head.value)?;
        }
        return Ok(next);
    }

    fence
        .check()
        .context("alert evaluation lease lost before commit")?;
    let latest_rule = alert_store.read_rule(monitor.id).await?;
    if latest_rule.value.revision != monitor.revision {
        bail!("monitor revision changed before commit");
    }
    let transition_key = transition_key(monitor.id, next.transition_sequence, slot_id);
    let transition = TransitionRecord {
        schema_version: ALERT_RECORD_SCHEMA_VERSION,
        deployment_id: db.lock().await.deployment_id().to_string(),
        monitor_id: monitor.id,
        monitor_revision: monitor.revision,
        slot_id,
        evaluated_at_unix_nano: slot_end_unix_nano,
        observation: DurableObservation::from(observation),
        state: next.clone(),
        previous_transition_key: current_head
            .as_ref()
            .map(|head| head.value.transition_key.clone()),
        notification_intents: vec![],
    };
    let transition_bytes = canonical_json(&transition)?;
    let head = StateHead {
        schema_version: ALERT_RECORD_SCHEMA_VERSION,
        deployment_id: transition.deployment_id.clone(),
        monitor_id: monitor.id,
        transition_sequence: next.transition_sequence,
        transition_key: transition_key.clone(),
        transition_sha256: sha256_hex(&transition_bytes),
        updated_at_unix_nano: slot_end_unix_nano,
    };
    alert_store
        .commit_transition(
            &transition_key,
            &transition,
            &head,
            current_head.map(|head| head.version),
        )
        .await?;
    db.lock()
        .await
        .fold_transition(&transition_key, &transition, &head)?;
    Ok(next)
}

fn signal_byte(signal: scry_alert::Signal) -> u8 {
    match signal {
        scry_alert::Signal::Metrics => 1,
        scry_alert::Signal::Logs => 2,
        scry_alert::Signal::Traces => 3,
        scry_alert::Signal::Profiles => 4,
    }
}

fn query_error_class(error: &QueryClientError) -> &'static str {
    match error {
        QueryClientError::Resource { .. } => "resource",
        QueryClientError::Timeout { .. } => "timeout",
        QueryClientError::Transport(_) => "transport",
        QueryClientError::Stream { .. } => "query",
        QueryClientError::Protocol(_) => "protocol",
        QueryClientError::Bounds { .. } => "bounds",
    }
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
        delivery_leases: Arc::new(DeliveryLeases::new(LocalLeaseProvider::new())),
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
            if let Err(error) = targets::reconcile_projection(
                reconcile_state.store.as_ref(),
                reconcile_state.db.as_ref(),
            )
            .await
            {
                tracing::warn!(error = %error, "test notification-target reconciliation failed");
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
    let expected = headers
        .get(header::IF_MATCH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim_matches('"').parse::<u64>().ok())
        .ok_or_else(|| ApiError::BadRequest("If-Match revision is required".into()))?;
    if monitor.revision != expected.saturating_add(1) {
        return Err(ApiError::Conflict);
    }
    let durable_head = AlertStore::new(state.store.as_ref())
        .read_rule_head(id)
        .await
        .map_err(|error| match error {
            scry_alert::AlertStoreError::Missing { .. } => ApiError::NotFound,
            other => ApiError::Store(other),
        })?;
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

async fn delete_monitor(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    authenticate(&state, &headers)?;
    let _permit = admit_control(&state)?;
    let command_id = require_idempotency(&headers)?;
    let id = parse_id(&id)?;
    let expected = headers
        .get(header::IF_MATCH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim_matches('"').parse::<u64>().ok())
        .ok_or_else(|| ApiError::BadRequest("If-Match revision is required".into()))?;
    let alert_store = AlertStore::new(state.store.as_ref());
    if let Some(existing) = alert_store
        .read_tombstone(id, command_id)
        .await
        .map_err(ApiError::Store)?
    {
        return if existing.revision == expected {
            Ok(StatusCode::NO_CONTENT)
        } else {
            Err(ApiError::Conflict)
        };
    }
    let deleted_at = chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_default()
        .max(0) as u64;
    let durable_head = alert_store
        .read_rule_head(id)
        .await
        .map_err(ApiError::Store)?;
    if durable_head.value.revision != expected || durable_head.value.deleted {
        return Err(ApiError::Conflict);
    }
    alert_store
        .tombstone_rule(
            &RuleTombstone {
                schema_version: ALERT_RECORD_SCHEMA_VERSION,
                monitor_id: id,
                revision: expected,
                command_id: command_id.to_owned(),
                deleted_at_unix_nano: deleted_at,
            },
            durable_head.version,
        )
        .await
        .map_err(ApiError::Store)?;
    state.db.lock().await.delete_monitor(id)?;
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
    let lookback = monitor.query.lookback_seconds.saturating_mul(1_000_000_000);
    let request = scry_query::QueryRequest {
        signal: signal_byte(monitor.query.signal),
        query: scry_query::Query {
            matchers: monitor
                .query
                .matchers
                .iter()
                .map(|matcher| (matcher.name.clone(), matcher.value.clone()))
                .collect(),
            ts_min: Some(now.saturating_sub(lookback)),
            ts_max: Some(now.saturating_sub(1)),
            ..Default::default()
        },
        sql: Some(monitor.query.sql.clone()),
        limit: None,
        request_id: Some(format!("alert-test:{}", monitor.id)),
        live: false,
    };
    let result = QueryWireClient::new(&target.address)
        .with_limits(QueryLimits {
            max_frames: 64,
            max_bytes: 1024 * 1024,
            max_rows: 1,
        })
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
            error_class: Some(query_error_class(&error)),
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
    let request_sha256 = sha256_hex(
        &canonical_json(&monitor)
            .map_err(scry_alert::AlertStoreError::from)
            .map_err(ApiError::Store)?,
    );
    let receipt = RuleMutationReceipt {
        schema_version: ALERT_RECORD_SCHEMA_VERSION,
        command_id: command_id.to_owned(),
        kind,
        monitor_id: monitor.id,
        revision: monitor.revision,
        request_sha256,
    };
    let alert_store = AlertStore::new(state.store.as_ref());
    if let Some(existing) = alert_store
        .read_rule_mutation(command_id)
        .await
        .map_err(ApiError::Store)?
    {
        if existing != receipt {
            return Err(ApiError::Conflict);
        }
        let durable = alert_store
            .read_rule(monitor.id)
            .await
            .map_err(ApiError::Store)?;
        state.db.lock().await.fold_rule(&durable.value)?;
        return state
            .db
            .lock()
            .await
            .get_monitor(monitor.id)?
            .map(MonitorSummaryDto::from)
            .ok_or(ApiError::NotFound);
    }
    alert_store
        .create_rule_revision(&monitor)
        .await
        .map_err(ApiError::Store)?;
    // Publish the command receipt only after the rule head is durable. A crash in
    // between is repaired by the idempotent immutable rule write on retry.
    alert_store
        .record_rule_mutation(&receipt)
        .await
        .map_err(ApiError::Store)?;
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
    Database(scry_alert::AlertsDbError),
    Store(scry_alert::AlertStoreError),
}

impl From<scry_alert::AlertsDbError> for ApiError {
    fn from(value: scry_alert::AlertsDbError) -> Self {
        Self::Database(value)
    }
}

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized".to_owned()),
            Self::NotFound => (StatusCode::NOT_FOUND, "resource not found".to_owned()),
            Self::Conflict => (
                StatusCode::CONFLICT,
                "resource revision conflict".to_owned(),
            ),
            Self::Overloaded => (
                StatusCode::SERVICE_UNAVAILABLE,
                "alert control API overloaded".to_owned(),
            ),
            Self::Unavailable(message) => (StatusCode::SERVICE_UNAVAILABLE, message),
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
            Self::Database(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
            Self::Store(scry_alert::AlertStoreError::Conflict { .. }) => (
                StatusCode::CONFLICT,
                "resource revision conflict".to_owned(),
            ),
            Self::Store(error) => (StatusCode::BAD_GATEWAY, error.to_string()),
        };
        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}
