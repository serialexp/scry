use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::Duration,
};

use async_trait::async_trait;
use futures::TryStreamExt;
use object_store::{memory::InMemory, path::Path as ObjectPath, ObjectStore};
use scry_alert::{
    AlertStatus, AlertStore, AlertsDb, Comparator, ExecutionErrorPolicy, Monitor, MonitorId,
    NoDataPolicy, Observation, RuleTombstone, ScalarCondition, ScalarQuery, Signal,
    ALERT_RECORD_SCHEMA_VERSION, MONITOR_SCHEMA_VERSION,
};
use scry_alertd::{
    evaluate_monitor_slot, EvaluationContext, QueryTarget, ScalarExecutor, SlotOutcome,
};
use scry_cluster::LocalLeaseProvider;
use tokio::sync::Mutex;
use uuid::Uuid;

const S: u64 = 1_000_000_000;

/// Replays scripted observations and counts queries.
#[derive(Default)]
struct ScriptedExecutor {
    script: StdMutex<VecDeque<Observation>>,
    calls: AtomicUsize,
}

#[async_trait]
impl ScalarExecutor for ScriptedExecutor {
    async fn execute(&self, _: &QueryTarget, _: &Monitor, _: u64, _: u64) -> Observation {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.script
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected query")
    }
}

impl ScriptedExecutor {
    fn push(&self, observation: Observation) {
        self.script.lock().unwrap().push_back(observation);
    }
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

fn monitor(enabled: bool) -> Monitor {
    Monitor {
        schema_version: MONITOR_SCHEMA_VERSION,
        id: MonitorId::new(),
        revision: 1,
        name: "monitor".into(),
        enabled,
        query: ScalarQuery {
            target_id: "local".into(),
            signal: Signal::Metrics,
            matchers: vec![],
            lookback_seconds: 60,
            sql: "SELECT count(*) AS value FROM metrics".into(),
        },
        condition: ScalarCondition {
            comparator: Comparator::Gt,
            threshold: 10.0,
        },
        every_seconds: 60,
        jitter_seconds: 0,
        for_seconds: 0,
        recover_for_seconds: 0,
        no_data: NoDataPolicy::NoData,
        execution_error: ExecutionErrorPolicy::Error,
        labels: vec![],
        annotations: vec![],
        created_at_unix_nano: 1,
        updated_at_unix_nano: 1,
    }
}

struct Harness {
    store: Arc<dyn ObjectStore>,
    db: Arc<Mutex<AlertsDb>>,
    deployment: String,
    executor: Arc<ScriptedExecutor>,
    context: EvaluationContext,
    leases: LocalLeaseProvider,
}

impl Harness {
    async fn new(monitor: &Monitor) -> Self {
        let deployment = Uuid::new_v4();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        AlertStore::new(store.as_ref())
            .create_rule_revision(monitor, &Uuid::new_v4().to_string())
            .await
            .unwrap();
        let mut db = AlertsDb::open_in_memory(deployment).unwrap();
        db.fold_rule(monitor).unwrap();
        let db = Arc::new(Mutex::new(db));
        let executor = Arc::new(ScriptedExecutor::default());
        let context = EvaluationContext {
            store: store.clone(),
            db: db.clone(),
            deployment_id: deployment.to_string(),
            query_targets: Arc::new(vec![QueryTarget {
                id: "local".into(),
                address: "127.0.0.1:9".into(),
            }]),
            executor: executor.clone(),
            lease_ttl: Duration::from_secs(5),
        };
        Self {
            store,
            db,
            deployment: deployment.to_string(),
            executor,
            context,
            leases: LocalLeaseProvider::new(),
        }
    }

    async fn evaluate(&self, monitor: &Monitor, slot: u64) -> SlotOutcome {
        let end = (slot + 1) * monitor.every_seconds * S;
        evaluate_monitor_slot(&self.leases, &self.context, monitor, slot, end)
            .await
            .unwrap()
    }

    async fn transition_count(&self, id: MonitorId) -> usize {
        let prefix = ObjectPath::from(format!("_scry/alerts/v1/transitions/v2/{id}"));
        self.store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len()
    }

    async fn projected_state(&self, id: MonitorId) -> Option<scry_alert::AlertState> {
        self.db
            .lock()
            .await
            .get_monitor(id)
            .unwrap()
            .and_then(|summary| summary.state)
    }
}

fn committed(outcome: SlotOutcome) -> scry_alert::AlertState {
    match outcome {
        SlotOutcome::Committed(state) => state,
        other => panic!("expected a commit, got {other:?}"),
    }
}

#[tokio::test]
async fn disabled_slot_commits_once_and_repairs_projection_on_replay() {
    let monitor = monitor(false);
    let harness = Harness::new(&monitor).await;

    let first = committed(harness.evaluate(&monitor, 7).await);
    assert_eq!(first.status, AlertStatus::Disabled);
    assert_eq!(first.transition_sequence, 1);
    assert_eq!(harness.executor.calls(), 0, "disabled monitors never query");

    let alerts = AlertStore::new(harness.store.as_ref());
    let head = alerts
        .read_state_head(monitor.id, &harness.deployment)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(head.value.state, first);
    let transition = alerts.read_latest_transition(&head.value).await.unwrap();
    assert_eq!(transition.value.state, first);
    assert_eq!(transition.value.previous_status, None);

    // Lose the local projection, then replay the same slot.
    harness.db.lock().await.forget_monitor(monitor.id).unwrap();
    harness.db.lock().await.fold_rule(&monitor).unwrap();
    assert!(harness.projected_state(monitor.id).await.is_none());

    let replay = harness.evaluate(&monitor, 7).await;
    assert_eq!(replay, SlotOutcome::AlreadyCovered(first.clone()));
    assert_eq!(harness.projected_state(monitor.id).await, Some(first));
}

#[tokio::test]
async fn transitions_are_written_only_when_the_status_changes() {
    let monitor = monitor(true);
    let harness = Harness::new(&monitor).await;
    for value in [20.0, 30.0, 40.0, 5.0, 6.0] {
        harness.executor.push(Observation::Value(value));
    }

    let firing = committed(harness.evaluate(&monitor, 1).await);
    assert_eq!(
        (firing.status, firing.transition_sequence),
        (AlertStatus::Firing, 1)
    );
    for slot in [2, 3] {
        let still = committed(harness.evaluate(&monitor, slot).await);
        assert_eq!(
            (still.status, still.transition_sequence),
            (AlertStatus::Firing, 1)
        );
        assert_eq!(still.last_slot_id, slot, "the head advances every slot");
        assert_eq!(still.since_unix_nano, firing.since_unix_nano);
    }
    assert_eq!(harness.transition_count(monitor.id).await, 1);

    let resolved = committed(harness.evaluate(&monitor, 4).await);
    assert_eq!(
        (resolved.status, resolved.transition_sequence),
        (AlertStatus::Inactive, 2)
    );
    committed(harness.evaluate(&monitor, 5).await);
    assert_eq!(harness.transition_count(monitor.id).await, 2);

    let alerts = AlertStore::new(harness.store.as_ref());
    let head = alerts
        .read_state_head(monitor.id, &harness.deployment)
        .await
        .unwrap()
        .unwrap()
        .value;
    assert_eq!(head.state.last_slot_id, 5);
    assert_eq!(head.state.last_value, Some(6.0));
    let latest = alerts.read_latest_transition(&head).await.unwrap().value;
    assert_eq!(latest.slot_id, 4);
    assert_eq!(latest.previous_status, Some(AlertStatus::Firing));
    let first_key = latest.previous_transition_key.expect("causal chain");
    assert!(first_key.contains(&format!("{:020}-", 1)));
    assert_eq!(harness.executor.calls(), 5);
}

#[tokio::test]
async fn covered_slots_are_not_queried_again() {
    let monitor = monitor(true);
    let harness = Harness::new(&monitor).await;
    harness.executor.push(Observation::Value(1.0));
    let first = committed(harness.evaluate(&monitor, 3).await);
    assert_eq!(
        harness.evaluate(&monitor, 3).await,
        SlotOutcome::AlreadyCovered(first.clone())
    );
    assert_eq!(
        harness.evaluate(&monitor, 2).await,
        SlotOutcome::AlreadyCovered(first)
    );
    assert_eq!(harness.executor.calls(), 1);
}

#[tokio::test]
async fn a_stale_revision_is_superseded_before_querying() {
    let original = monitor(true);
    let harness = Harness::new(&original).await;
    let mut edited = original.clone();
    edited.revision = 2;
    edited.condition.threshold += 1.0;
    edited.updated_at_unix_nano = 2;
    AlertStore::new(harness.store.as_ref())
        .create_rule_revision(&edited, &Uuid::new_v4().to_string())
        .await
        .unwrap();

    assert_eq!(
        harness.evaluate(&original, 1).await,
        SlotOutcome::Superseded
    );
    assert_eq!(harness.executor.calls(), 0);
    let projected = harness
        .db
        .lock()
        .await
        .get_monitor(original.id)
        .unwrap()
        .unwrap();
    assert_eq!(projected.monitor, edited, "the newer revision is folded");
}

#[tokio::test]
async fn a_deleted_rule_is_not_evaluated_or_resurrected() {
    let monitor = monitor(true);
    let harness = Harness::new(&monitor).await;
    let alerts = AlertStore::new(harness.store.as_ref());
    let head = alerts.read_rule_head(monitor.id).await.unwrap();
    alerts
        .tombstone_rule(
            &RuleTombstone {
                schema_version: ALERT_RECORD_SCHEMA_VERSION,
                monitor_id: monitor.id,
                revision: 1,
                command_id: Uuid::new_v4().to_string(),
                deleted_at_unix_nano: 5,
            },
            head.version,
        )
        .await
        .unwrap();

    assert_eq!(harness.evaluate(&monitor, 1).await, SlotOutcome::Superseded);
    assert_eq!(harness.executor.calls(), 0);
    let mut db = harness.db.lock().await;
    assert!(db.get_monitor(monitor.id).unwrap().is_none());
    assert!(!db.fold_rule(&monitor).unwrap(), "no local resurrection");
}

#[tokio::test]
async fn an_interval_change_keeps_evaluating_and_the_old_revision_cannot_write() {
    let original = monitor(true);
    let harness = Harness::new(&original).await;
    harness.executor.push(Observation::Value(20.0));
    let firing = committed(harness.evaluate(&original, 1_000).await);
    assert_eq!(firing.status, AlertStatus::Firing);

    // Ten times longer interval: slot IDs become ten times smaller.
    let mut slower = original.clone();
    slower.revision = 2;
    slower.every_seconds = 600;
    AlertStore::new(harness.store.as_ref())
        .create_rule_revision(&slower, &Uuid::new_v4().to_string())
        .await
        .unwrap();
    harness.db.lock().await.fold_rule(&slower).unwrap();
    harness.executor.push(Observation::Value(25.0));
    let after = committed(harness.evaluate(&slower, 101).await);
    assert_eq!(
        (after.monitor_revision, after.last_slot_id),
        (2, 101),
        "a smaller slot of a newer revision is not stale"
    );
    assert_eq!(after.status, AlertStatus::Firing);
    assert_eq!(
        after.transition_sequence, 1,
        "no status change, no transition"
    );

    // A late evaluation of the old revision, even at a larger slot, loses.
    assert_eq!(
        harness.evaluate(&original, 5_000).await,
        SlotOutcome::Superseded
    );
    assert_eq!(harness.projected_state(original.id).await, Some(after));
    assert_eq!(harness.executor.calls(), 2);
}
