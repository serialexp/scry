use std::{sync::Arc, time::Duration};

use object_store::{memory::InMemory, ObjectStore};
use scry_alert::{
    AlertStatus, AlertStore, AlertsDb, Comparator, ExecutionErrorPolicy, Monitor, MonitorId,
    NoDataPolicy, ScalarCondition, ScalarQuery, Signal, MONITOR_SCHEMA_VERSION,
};
use scry_alertd::{evaluate_monitor_slot, EvaluationContext, QueryTarget};
use scry_cluster::LocalLeaseProvider;
use tokio::sync::Mutex;
use uuid::Uuid;

fn disabled_monitor() -> Monitor {
    Monitor {
        schema_version: MONITOR_SCHEMA_VERSION,
        id: MonitorId::new(),
        revision: 1,
        name: "disabled monitor".into(),
        enabled: false,
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

#[tokio::test]
async fn disabled_slot_commits_once_and_repairs_projection_on_replay() {
    let deployment = Uuid::new_v4();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let monitor = disabled_monitor();
    AlertStore::new(store.as_ref())
        .create_rule_revision(&monitor)
        .await
        .unwrap();
    let mut db = AlertsDb::open_in_memory(deployment).unwrap();
    db.fold_rule(&monitor).unwrap();
    let db = Arc::new(Mutex::new(db));
    let context = EvaluationContext {
        store: store.clone(),
        db: db.clone(),
        target: QueryTarget {
            id: "local".into(),
            address: "127.0.0.1:9".into(),
        },
        lease_ttl: Duration::from_secs(5),
    };
    let leases = LocalLeaseProvider::new();

    let first = evaluate_monitor_slot(&leases, &context, &monitor, 7, 420_000_000_000)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.status, AlertStatus::Disabled);
    assert_eq!(first.transition_sequence, 1);

    let head = AlertStore::new(store.as_ref())
        .read_state_head(monitor.id)
        .await
        .unwrap()
        .unwrap();
    let transition = AlertStore::new(store.as_ref())
        .read_current_transition(&head.value)
        .await
        .unwrap();
    assert_eq!(transition.value.state, first);

    db.lock().await.delete_monitor(monitor.id).unwrap();
    db.lock().await.fold_rule(&monitor).unwrap();
    assert!(db
        .lock()
        .await
        .get_monitor(monitor.id)
        .unwrap()
        .unwrap()
        .state
        .is_none());

    let replay = evaluate_monitor_slot(&leases, &context, &monitor, 7, 420_000_000_000)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replay, first);
    let repaired = db
        .lock()
        .await
        .get_monitor(monitor.id)
        .unwrap()
        .unwrap()
        .state
        .unwrap();
    assert_eq!(repaired, first);
}
