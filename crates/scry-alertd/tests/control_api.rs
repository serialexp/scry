use std::sync::Arc;

use object_store::{memory::InMemory, ObjectStore};
use scry_alert::{
    AlertsDb, Comparator, ExecutionErrorPolicy, Monitor, MonitorId, NoDataPolicy, ScalarCondition,
    ScalarQuery, Signal, MONITOR_SCHEMA_VERSION,
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
