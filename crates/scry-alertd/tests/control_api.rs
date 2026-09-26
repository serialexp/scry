use std::sync::Arc;

use object_store::{memory::InMemory, ObjectStore, ObjectStoreExt};
use scry_alert::{
    rule_revision_key, rule_tombstone_key, AlertStore, AlertsDb, Comparator, ExecutionErrorPolicy,
    Monitor, MonitorId, NoDataPolicy, RuleTombstone, ScalarCondition, ScalarQuery, Signal,
    ALERT_RECORD_SCHEMA_VERSION, MONITOR_SCHEMA_VERSION,
};
use scry_alertd::{serve_control_for_test, QueryTarget};
use uuid::Uuid;

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

fn monitor() -> Monitor {
    Monitor {
        schema_version: MONITOR_SCHEMA_VERSION,
        id: MonitorId::new(),
        revision: 1,
        name: "high metric count".into(),
        enabled: true,
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
async fn authenticated_create_and_list_are_revisioned_and_bounded() {
    let deployment = Uuid::new_v4();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = AlertsDb::open_in_memory(deployment).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_control_for_test(
        listener,
        TOKEN.into(),
        store,
        db,
        vec![QueryTarget {
            id: "local".into(),
            address: "127.0.0.1:9".into(),
        }],
    ));
    let client = reqwest::Client::new();
    let rule = monitor();

    for authorization in [
        None,
        Some("Basic 0123456789abcdef0123456789abcdef"),
        Some("bearer 0123456789abcdef0123456789abcdef"),
        Some("Bearer 0123456789abcdef0123456789abcdee"),
        Some("Bearer"),
    ] {
        let mut request = client.get(format!("http://{address}/v1/monitors"));
        if let Some(value) = authorization {
            request = request.header(reqwest::header::AUTHORIZATION, value);
        }
        let unauthorized = request.send().await.unwrap();
        assert_eq!(
            unauthorized.status(),
            reqwest::StatusCode::UNAUTHORIZED,
            "authorization value {authorization:?} must be rejected"
        );
        assert_eq!(
            unauthorized.json::<serde_json::Value>().await.unwrap(),
            serde_json::json!({ "error": "unauthorized" })
        );
    }

    let command_id = Uuid::new_v4().to_string();
    let created = client
        .post(format!("http://{address}/v1/monitors"))
        .bearer_auth(TOKEN)
        .header("idempotency-key", &command_id)
        .json(&rule)
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), reqwest::StatusCode::CREATED);

    let replayed = client
        .post(format!("http://{address}/v1/monitors"))
        .bearer_auth(TOKEN)
        .header("idempotency-key", &command_id)
        .json(&rule)
        .send()
        .await
        .unwrap();
    assert_eq!(replayed.status(), reqwest::StatusCode::CREATED);

    let mut conflicting = rule.clone();
    conflicting.name = "different request".into();
    let reused = client
        .post(format!("http://{address}/v1/monitors"))
        .bearer_auth(TOKEN)
        .header("idempotency-key", &command_id)
        .json(&conflicting)
        .send()
        .await
        .unwrap();
    assert_eq!(reused.status(), reqwest::StatusCode::CONFLICT);

    let malformed_key = client
        .post(format!("http://{address}/v1/monitors"))
        .bearer_auth(TOKEN)
        .header("idempotency-key", "../nested")
        .json(&monitor())
        .send()
        .await
        .unwrap();
    assert_eq!(malformed_key.status(), reqwest::StatusCode::BAD_REQUEST);

    let listed: serde_json::Value = client
        .get(format!("http://{address}/v1/monitors?limit=1"))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed["monitors"].as_array().unwrap().len(), 1);
    assert_eq!(listed["monitors"][0]["monitor"]["id"], rule.id.to_string());

    let mut updated_rule = rule.clone();
    updated_rule.revision = 2;
    updated_rule.name = "updated rule".into();
    updated_rule.updated_at_unix_nano = 2;
    let update_command = Uuid::new_v4().to_string();
    let updated = client
        .put(format!("http://{address}/v1/monitors/{}", rule.id))
        .bearer_auth(TOKEN)
        .header("idempotency-key", &update_command)
        .header("if-match", "\"1\"")
        .json(&updated_rule)
        .send()
        .await
        .unwrap();
    assert_eq!(updated.status(), reqwest::StatusCode::OK);
    let replayed_update = client
        .put(format!("http://{address}/v1/monitors/{}", rule.id))
        .bearer_auth(TOKEN)
        .header("idempotency-key", &update_command)
        .header("if-match", "\"1\"")
        .json(&updated_rule)
        .send()
        .await
        .unwrap();
    assert_eq!(replayed_update.status(), reqwest::StatusCode::OK);

    let delete_command = Uuid::new_v4().to_string();
    let deleted = client
        .delete(format!("http://{address}/v1/monitors/{}", rule.id))
        .bearer_auth(TOKEN)
        .header("idempotency-key", &delete_command)
        .header("if-match", "\"2\"")
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), reqwest::StatusCode::NO_CONTENT);
    let replayed_delete = client
        .delete(format!("http://{address}/v1/monitors/{}", rule.id))
        .bearer_auth(TOKEN)
        .header("idempotency-key", &delete_command)
        .header("if-match", "\"2\"")
        .send()
        .await
        .unwrap();
    assert_eq!(replayed_delete.status(), reqwest::StatusCode::NO_CONTENT);

    server.abort();
}

struct Api {
    address: std::net::SocketAddr,
    client: reqwest::Client,
    store: Arc<dyn ObjectStore>,
    server: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Api {
    async fn start() -> Self {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = AlertsDb::open_in_memory(Uuid::new_v4()).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_control_for_test(
            listener,
            TOKEN.into(),
            store.clone(),
            db,
            vec![QueryTarget {
                id: "local".into(),
                address: "127.0.0.1:9".into(),
            }],
        ));
        Self {
            address,
            client: reqwest::Client::new(),
            store,
            server,
        }
    }

    async fn create(&self, rule: &Monitor) {
        let response = self
            .client
            .post(format!("http://{}/v1/monitors", self.address))
            .bearer_auth(TOKEN)
            .header("idempotency-key", Uuid::new_v4().to_string())
            .json(rule)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    }

    async fn update(&self, rule: &Monitor, command: &str, if_match: u64) -> reqwest::StatusCode {
        self.client
            .put(format!("http://{}/v1/monitors/{}", self.address, rule.id))
            .bearer_auth(TOKEN)
            .header("idempotency-key", command)
            .header("if-match", format!("\"{if_match}\""))
            .json(rule)
            .send()
            .await
            .unwrap()
            .status()
    }

    async fn delete(&self, id: MonitorId, command: &str, if_match: u64) -> reqwest::StatusCode {
        self.client
            .delete(format!("http://{}/v1/monitors/{id}", self.address))
            .bearer_auth(TOKEN)
            .header("idempotency-key", command)
            .header("if-match", format!("\"{if_match}\""))
            .send()
            .await
            .unwrap()
            .status()
    }

    async fn get(&self, id: MonitorId) -> reqwest::StatusCode {
        self.client
            .get(format!("http://{}/v1/monitors/{id}", self.address))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap()
            .status()
    }
}

#[tokio::test]
async fn delete_replay_resumes_a_staged_tombstone_and_statuses_are_meaningful() {
    use reqwest::StatusCode;

    let api = Api::start().await;
    let rule = monitor();
    api.create(&rule).await;

    // Missing monitors are 404, not an upstream error.
    let missing = MonitorId::new();
    assert_eq!(
        api.delete(missing, &Uuid::new_v4().to_string(), 1).await,
        StatusCode::NOT_FOUND
    );
    let mut ghost = monitor();
    ghost.id = missing;
    ghost.revision = 2;
    assert_eq!(
        api.update(&ghost, &Uuid::new_v4().to_string(), 1).await,
        StatusCode::NOT_FOUND
    );

    // A delete that staged its tombstone record but crashed before the head
    // CAS: a replay of the same command completes it.
    let command = Uuid::new_v4().to_string();
    AlertStore::new(api.store.as_ref())
        .create_record(
            &rule_tombstone_key(rule.id, &command),
            &RuleTombstone {
                schema_version: ALERT_RECORD_SCHEMA_VERSION,
                monitor_id: rule.id,
                revision: 1,
                command_id: command.clone(),
                deleted_at_unix_nano: 7,
            },
        )
        .await
        .unwrap();
    // The staged tombstone pins its revision.
    assert_eq!(api.delete(rule.id, &command, 2).await, StatusCode::CONFLICT);
    assert_eq!(
        api.delete(rule.id, &command, 1).await,
        StatusCode::NO_CONTENT
    );
    let head = AlertStore::new(api.store.as_ref())
        .read_rule_head(rule.id)
        .await
        .unwrap()
        .value;
    assert!(head.deleted);
    assert_eq!(
        head.tombstone_key.as_deref(),
        Some(rule_tombstone_key(rule.id, &command).as_str())
    );
    assert_eq!(api.get(rule.id).await, StatusCode::NOT_FOUND);
    // Replays stay successful; another command against the deleted head conflicts.
    assert_eq!(
        api.delete(rule.id, &command, 1).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        api.delete(rule.id, &Uuid::new_v4().to_string(), 1).await,
        StatusCode::CONFLICT
    );
    let mut revived = rule.clone();
    revived.revision = 2;
    assert_eq!(
        api.update(&revived, &Uuid::new_v4().to_string(), 1).await,
        StatusCode::CONFLICT
    );

    api.server.abort();
}

#[tokio::test]
async fn an_unpublished_revision_left_by_a_crashed_command_never_blocks_a_save() {
    use reqwest::StatusCode;

    let api = Api::start().await;
    let rule = monitor();
    api.create(&rule).await;

    // A command wrote revision 2 but crashed before advancing the head.
    let mut abandoned = rule.clone();
    abandoned.revision = 2;
    abandoned.name = "abandoned edit".into();
    api.store
        .put(
            &object_store::path::Path::from(rule_revision_key(rule.id, 2, "crashed-command")),
            serde_json::to_vec(&abandoned).unwrap().into(),
        )
        .await
        .unwrap();

    let mut edit = rule.clone();
    edit.revision = 2;
    edit.name = "the edit that should win".into();
    edit.updated_at_unix_nano = 2;
    let command = Uuid::new_v4().to_string();
    assert_eq!(api.update(&edit, &command, 1).await, StatusCode::OK);
    assert_eq!(api.update(&edit, &command, 1).await, StatusCode::OK);
    let durable = AlertStore::new(api.store.as_ref())
        .read_rule(rule.id)
        .await
        .unwrap()
        .value;
    assert_eq!(durable, edit);

    // The same revision from a different command is a conflict, not a 5xx.
    let mut late = edit.clone();
    late.name = "late competitor".into();
    assert_eq!(
        api.update(&late, &Uuid::new_v4().to_string(), 1).await,
        StatusCode::CONFLICT
    );

    api.server.abort();
}
