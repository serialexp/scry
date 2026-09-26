//! One monitor slot, evaluated and committed under the monitor's lease.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use object_store::ObjectStore;
use scry_alert::{
    evaluate, validate_monitor, AlertState, AlertStore, AlertStoreError, AlertsDb,
    DurableObservation, Monitor, MonitorId, Observation, RuleHead, StateHead, TransitionRecord,
    Versioned, STATE_RECORD_SCHEMA_VERSION,
};
use scry_cluster::{LeaseGuard, LeaseProvider};
use scry_query::client::{
    QueryClientError, QueryDeadlines, QueryLimits, QueryScalar, QueryWireClient, ScalarOutcome,
};
use tokio::sync::Mutex;

use crate::QueryTarget;

/// Executes a monitor's scalar query for one slot window.
///
/// Implementations classify every failure as [`Observation::Error`]; the
/// evaluator never sees transport errors. Injected so tests can drive the
/// evaluator without a queryd.
#[async_trait]
pub trait ScalarExecutor: Send + Sync {
    async fn execute(
        &self,
        target: &QueryTarget,
        monitor: &Monitor,
        slot_id: u64,
        slot_end_unix_nano: u64,
    ) -> Observation;
}

/// Production executor: queryd's query wire with bounded deadlines and
/// result limits, through ordinary queryd admission.
pub struct QueryWireExecutor;

#[async_trait]
impl ScalarExecutor for QueryWireExecutor {
    async fn execute(
        &self,
        target: &QueryTarget,
        monitor: &Monitor,
        slot_id: u64,
        slot_end_unix_nano: u64,
    ) -> Observation {
        let request = scalar_request(
            monitor,
            slot_end_unix_nano,
            format!("alert:{}:{slot_id}", monitor.id),
        );
        let client = QueryWireClient::new(&target.address)
            .with_deadlines(QueryDeadlines {
                connect: Duration::from_secs(5),
                write: Duration::from_secs(5),
                total: Duration::from_secs(30),
            })
            .with_limits(scalar_limits());
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
    }
}

pub(crate) fn scalar_limits() -> QueryLimits {
    QueryLimits {
        max_frames: 64,
        max_bytes: 1024 * 1024,
        max_rows: 1,
    }
}

/// The query for the half-open event-time window `[end - lookback, end)`.
/// Query-wire bounds are inclusive, so the request ends at `end - 1`.
pub(crate) fn scalar_request(
    monitor: &Monitor,
    end_unix_nano: u64,
    request_id: String,
) -> scry_query::QueryRequest {
    let lookback = monitor.query.lookback_seconds.saturating_mul(1_000_000_000);
    scry_query::QueryRequest {
        signal: signal_byte(monitor.query.signal),
        query: scry_query::Query {
            matchers: monitor
                .query
                .matchers
                .iter()
                .map(|matcher| (matcher.name.clone(), matcher.value.clone()))
                .collect(),
            ts_min: Some(end_unix_nano.saturating_sub(lookback)),
            ts_max: Some(end_unix_nano.saturating_sub(1)),
            ..Default::default()
        },
        sql: Some(monitor.query.sql.clone()),
        limit: None,
        request_id: Some(request_id),
        live: false,
    }
}

fn signal_byte(signal: scry_alert::Signal) -> u8 {
    match signal {
        scry_alert::Signal::Metrics => 1,
        scry_alert::Signal::Logs => 2,
        scry_alert::Signal::Traces => 3,
        scry_alert::Signal::Profiles => 4,
    }
}

pub(crate) fn query_error_class(error: &QueryClientError) -> &'static str {
    match error {
        QueryClientError::Resource { .. } => "resource",
        QueryClientError::Timeout { .. } => "timeout",
        QueryClientError::Transport(_) => "transport",
        QueryClientError::Stream { .. } => "query",
        QueryClientError::Protocol(_) => "protocol",
        QueryClientError::Bounds { .. } => "bounds",
    }
}

pub struct EvaluationContext {
    pub store: Arc<dyn ObjectStore>,
    pub db: Arc<Mutex<AlertsDb>>,
    pub deployment_id: String,
    /// Allowlisted queryd targets. A monitor's target is resolved per slot
    /// from the durable revision being evaluated.
    pub query_targets: Arc<Vec<QueryTarget>>,
    pub executor: Arc<dyn ScalarExecutor>,
    pub lease_ttl: Duration,
}

/// What happened to one requested slot.
#[derive(Clone, Debug, PartialEq)]
pub enum SlotOutcome {
    /// This call evaluated the slot and advanced the durable state head.
    Committed(AlertState),
    /// Durable state already covered the slot (a replay, or a peer got there
    /// first); it was folded into the local projection unchanged.
    AlreadyCovered(AlertState),
    /// The requested rule revision is no longer current (edited, deleted, or
    /// a newer revision already owns the state), or a concurrent writer won
    /// the state-head CAS. Nothing was committed; newer durable records were
    /// folded locally and a later tick evaluates the current revision.
    Superseded,
    /// Another holder owns the monitor's evaluation lease.
    Busy,
}

#[derive(Debug, thiserror::Error)]
pub enum EvaluationError {
    /// The lease backend could not be reached. No lease means no commit; the
    /// scheduler backs off rather than hammering the backend.
    #[error("alert evaluation lease backend unavailable: {0:#}")]
    LeaseUnavailable(anyhow::Error),
    #[error(transparent)]
    Failed(#[from] anyhow::Error),
}

impl From<AlertStoreError> for EvaluationError {
    fn from(error: AlertStoreError) -> Self {
        Self::Failed(error.into())
    }
}

impl From<scry_alert::AlertsDbError> for EvaluationError {
    fn from(error: scry_alert::AlertsDbError) -> Self {
        Self::Failed(error.into())
    }
}

/// Evaluate and durably commit one slot of `monitor` (the locally projected
/// revision the scheduler found due) while holding its evaluation lease.
pub async fn evaluate_monitor_slot<L: LeaseProvider>(
    provider: &L,
    context: &EvaluationContext,
    monitor: &Monitor,
    slot_id: u64,
    slot_end_unix_nano: u64,
) -> Result<SlotOutcome, EvaluationError> {
    let key = format!("lease/alert/eval/{}", monitor.id);
    let guard = match provider.try_acquire(&key, context.lease_ttl).await {
        Ok(Some(guard)) => guard,
        Ok(None) => return Ok(SlotOutcome::Busy),
        Err(error) => return Err(EvaluationError::LeaseUnavailable(error)),
    };
    let fence = guard.fence();
    let outcome = evaluate_under_guard(
        fence.as_ref(),
        context,
        monitor,
        slot_id,
        slot_end_unix_nano,
    )
    .await;
    guard.release().await;
    outcome
}

/// Design steps 2–7: re-read durable rule and state *before* querying, so a
/// stale, deleted, or already-covered slot never costs a query; then query,
/// compute, recheck revision and fence, and commit metadata-last.
async fn evaluate_under_guard(
    fence: &dyn scry_block::Fence,
    context: &EvaluationContext,
    monitor: &Monitor,
    slot_id: u64,
    slot_end_unix_nano: u64,
) -> Result<SlotOutcome, EvaluationError> {
    let alert_store = AlertStore::new(context.store.as_ref());
    fence
        .check()
        .map_err(|error| error.context("alert evaluation lease lost before rule read"))?;

    if !rule_is_current(&alert_store, context, monitor).await? {
        return Ok(SlotOutcome::Superseded);
    }
    let prior_head = alert_store
        .read_state_head(monitor.id, &context.deployment_id)
        .await?;
    if let Some(head) = &prior_head {
        let state = &head.value.state;
        if state.monitor_revision > monitor.revision {
            fold_state(context, monitor.id, state).await?;
            return Ok(SlotOutcome::Superseded);
        }
        if state.covers(monitor.revision, slot_id) {
            fold_state(context, monitor.id, state).await?;
            return Ok(SlotOutcome::AlreadyCovered(state.clone()));
        }
    }

    let observation = if monitor.enabled {
        let Some(target) = context
            .query_targets
            .iter()
            .find(|target| target.id == monitor.query.target_id)
        else {
            return Err(anyhow::anyhow!(
                "monitor {} references unknown query target `{}`",
                monitor.id,
                monitor.query.target_id
            )
            .into());
        };
        context
            .executor
            .execute(target, monitor, slot_id, slot_end_unix_nano)
            .await
    } else {
        // Disabled monitors are never queried; `evaluate` maps them to Disabled.
        Observation::NoData
    };

    let prior_state = prior_head.as_ref().map(|head| &head.value.state);
    let (next, change) = evaluate(
        monitor,
        prior_state,
        observation.clone(),
        slot_id,
        slot_end_unix_nano,
    );

    fence
        .check()
        .map_err(|error| error.context("alert evaluation lease lost before commit"))?;
    if !rule_is_current(&alert_store, context, monitor).await? {
        return Ok(SlotOutcome::Superseded);
    }

    let transition = change.transitioned.then(|| TransitionRecord {
        schema_version: STATE_RECORD_SCHEMA_VERSION,
        deployment_id: context.deployment_id.clone(),
        monitor_id: monitor.id,
        monitor_revision: monitor.revision,
        slot_id,
        evaluated_at_unix_nano: slot_end_unix_nano,
        observation: DurableObservation::from(observation),
        previous_status: change.previous,
        resumed: change.resumed,
        state: next.clone(),
        previous_transition_key: prior_head
            .as_ref()
            .map(|head| head.value.latest_transition.key(monitor.id)),
        notification_intents: vec![],
    });
    let latest_transition = match (&transition, &prior_head) {
        (Some(record), _) => record.reference().map_err(AlertStoreError::from)?,
        (None, Some(head)) => head.value.latest_transition.clone(),
        (None, None) => {
            return Err(anyhow::anyhow!("a first state must be a transition").into());
        }
    };
    let head = StateHead {
        schema_version: STATE_RECORD_SCHEMA_VERSION,
        deployment_id: context.deployment_id.clone(),
        monitor_id: monitor.id,
        state: next.clone(),
        latest_transition,
        updated_at_unix_nano: slot_end_unix_nano,
    };
    match alert_store
        .commit_state(
            &head,
            transition.as_ref(),
            prior_head.map(|head| head.version),
        )
        .await
    {
        Ok(()) => {}
        Err(AlertStoreError::Conflict { .. }) => {
            // A concurrent writer advanced the head (only possible after a
            // lease handover). Fold whatever won; this result is discarded.
            if let Some(winner) = alert_store
                .read_state_head(monitor.id, &context.deployment_id)
                .await?
            {
                fold_state(context, monitor.id, &winner.value.state).await?;
            }
            return Ok(SlotOutcome::Superseded);
        }
        Err(error) => return Err(error.into()),
    }
    fold_state(context, monitor.id, &next).await?;
    Ok(SlotOutcome::Committed(next))
}

/// Whether `monitor`'s revision is still the durable current revision. A
/// deleted or newer durable rule is folded into the local projection so the
/// scheduler stops scheduling the stale revision.
async fn rule_is_current(
    alert_store: &AlertStore<'_>,
    context: &EvaluationContext,
    monitor: &Monitor,
) -> Result<bool, EvaluationError> {
    let head: Versioned<RuleHead> = match alert_store.read_rule_head(monitor.id).await {
        Ok(head) => head,
        Err(AlertStoreError::Missing { .. }) => {
            tracing::warn!(monitor_id = %monitor.id, "alert rule head is missing; not evaluating");
            return Ok(false);
        }
        Err(error) => return Err(error.into()),
    };
    let head = head.value;
    if head.deleted {
        context.db.lock().await.tombstone_monitor(
            monitor.id,
            head.revision,
            head.updated_at_unix_nano,
        )?;
        return Ok(false);
    }
    if head.revision == monitor.revision {
        return Ok(true);
    }
    if head.revision < monitor.revision {
        return Err(anyhow::anyhow!(
            "durable rule head for {} is at revision {} behind the projected revision {}",
            monitor.id,
            head.revision,
            monitor.revision
        )
        .into());
    }
    let newer = alert_store.read_rule_revision(&head).await?.value;
    validate_monitor(&newer).map_err(anyhow::Error::from)?;
    context.db.lock().await.fold_rule(&newer)?;
    Ok(false)
}

async fn fold_state(
    context: &EvaluationContext,
    id: MonitorId,
    state: &AlertState,
) -> Result<(), EvaluationError> {
    context.db.lock().await.fold_state(id, state)?;
    Ok(())
}
