use std::sync::Arc;

use object_store::{memory::InMemory, ObjectStore};
use scry_alert::AlertsDb;
use scry_alertd::{serve_control_for_test, serve_control_with_reconciliation_for_test};
use uuid::Uuid;

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

async fn server() -> (String, tokio::task::JoinHandle<anyhow::Result<()>>) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = AlertsDb::open_in_memory(Uuid::new_v4()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(serve_control_for_test(
        listener,
        TOKEN.into(),
        store,
        db,
        vec![],
    ));
    (format!("http://{address}"), task)
}

async fn reconciling_server(
    store: Arc<dyn ObjectStore>,
    deployment: Uuid,
) -> (String, tokio::task::JoinHandle<anyhow::Result<()>>) {
    let db = AlertsDb::open_in_memory(deployment).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(serve_control_with_reconciliation_for_test(
        listener,
        TOKEN.into(),
        store,
        db,
        std::time::Duration::from_millis(10),
    ));
    (format!("http://{address}"), task)
}

fn write(secret: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "schema_version": 1,
        "name": "pager",
        "enabled": true,
        "kind": {"type":"webhook", "url":"https://example.com/hook"},
        "timeout_ms": 1000,
        "headers": [{"name":"x-team", "value":"ops"}],
        "format": {"type":"builtin", "format_id":"generic_json"},
        "secret": secret
    })
}

#[tokio::test]
async fn crud_replay_conflict_and_redaction() {
    let (base, task) = server().await;
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .get(format!("{base}/v1/notification-targets"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );

    let command = Uuid::new_v4().to_string();
    let created = client
        .post(format!("{base}/v1/notification-targets"))
        .bearer_auth(TOKEN)
        .header("idempotency-key", &command)
        .json(&write(
            serde_json::json!({"action":"set","value":"top-secret"}),
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    let body: serde_json::Value = created.json().await.unwrap();
    assert_eq!(body["revision"], "1");
    assert_eq!(body["secret_configured"], true);
    let serialized = body.to_string();
    assert!(!serialized.contains("top-secret"));
    assert!(!serialized.contains("ciphertext"));
    let id = body["id"].as_str().unwrap();

    let replay: serde_json::Value = client
        .post(format!("{base}/v1/notification-targets"))
        .bearer_auth(TOKEN)
        .header("idempotency-key", &command)
        .json(&write(
            serde_json::json!({"action":"set","value":"top-secret"}),
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(replay["id"], id);

    let conflict = client
        .post(format!("{base}/v1/notification-targets"))
        .bearer_auth(TOKEN)
        .header("idempotency-key", &command)
        .json(&write(
            serde_json::json!({"action":"set","value":"different"}),
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(conflict.status(), 409);

    let list: serde_json::Value = client
        .get(format!("{base}/v1/notification-targets"))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list["targets"].as_array().unwrap().len(), 1);
    assert!(!list.to_string().contains("top-secret"));

    let update = client
        .put(format!("{base}/v1/notification-targets/{id}"))
        .bearer_auth(TOKEN)
        .header("idempotency-key", Uuid::new_v4().to_string())
        .header("if-match", "\"9\"")
        .json(&write(serde_json::json!({"action":"unchanged"})))
        .send()
        .await
        .unwrap();
    assert_eq!(update.status(), 409);
    task.abort();
}

#[tokio::test]
async fn two_instances_converge_created_updated_and_deleted_targets() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let deployment = Uuid::new_v4();
    let (first, first_task) = reconciling_server(store.clone(), deployment).await;
    let (second, second_task) = reconciling_server(store, deployment).await;
    let client = reqwest::Client::new();

    let created: serde_json::Value = client
        .post(format!("{first}/v1/notification-targets"))
        .bearer_auth(TOKEN)
        .header("idempotency-key", Uuid::new_v4().to_string())
        .json(&write(serde_json::json!({"action":"set","value":"secret"})))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = created["id"].as_str().unwrap();

    let await_target = |base: String, expected_revision: Option<&'static str>| {
        let client = client.clone();
        let id = id.to_owned();
        async move {
            tokio::time::timeout(std::time::Duration::from_secs(2), async move {
                loop {
                    let list: serde_json::Value = client
                        .get(format!("{base}/v1/notification-targets"))
                        .bearer_auth(TOKEN)
                        .send()
                        .await
                        .unwrap()
                        .json()
                        .await
                        .unwrap();
                    let found = list["targets"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|target| target["id"] == id);
                    if match (found, expected_revision) {
                        (Some(target), Some(revision)) => target["revision"] == revision,
                        (None, None) => true,
                        _ => false,
                    } {
                        return;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("peer projection did not converge");
        }
    };
    await_target(second.clone(), Some("1")).await;

    let mut updated = write(serde_json::json!({"action":"unchanged"}));
    updated["name"] = serde_json::json!("updated");
    assert_eq!(
        client
            .put(format!("{first}/v1/notification-targets/{id}"))
            .bearer_auth(TOKEN)
            .header("idempotency-key", Uuid::new_v4().to_string())
            .header("if-match", "\"1\"")
            .json(&updated)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    await_target(second.clone(), Some("2")).await;

    assert_eq!(
        client
            .delete(format!("{first}/v1/notification-targets/{id}"))
            .bearer_auth(TOKEN)
            .header("idempotency-key", Uuid::new_v4().to_string())
            .header("if-match", "\"2\"")
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
    await_target(second, None).await;
    first_task.abort();
    second_task.abort();
}

#[tokio::test]
async fn validation_and_preview_are_bounded_and_secret_free() {
    let (base, task) = server().await;
    let client = reqwest::Client::new();
    let invalid = client.post(format!("{base}/v1/notification-targets/validate")).bearer_auth(TOKEN)
        .json(&serde_json::json!({
            "schema_version":1,"name":"bad","enabled":true,
            "kind":{"type":"webhook","url":"https://127.0.0.1/hook"},"timeout_ms":1,
            "headers":[],"format":{"type":"builtin","format_id":"generic_json"},"secret":{"action":"set","value":"x"}
        })).send().await.unwrap();
    assert_eq!(invalid.status(), 400);

    let command = Uuid::new_v4().to_string();
    let body: serde_json::Value = client
        .post(format!("{base}/v1/notification-targets"))
        .bearer_auth(TOKEN)
        .header("idempotency-key", command)
        .json(&write(
            serde_json::json!({"action":"set","value":"top-secret"}),
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let preview: serde_json::Value = client
        .post(format!(
            "{base}/v1/notification-targets/{}/preview",
            body["id"].as_str().unwrap()
        ))
        .bearer_auth(TOKEN)
        .header("if-match", "\"1\"")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(preview["content_type"], "application/json");
    assert_eq!(preview["body_sha256"].as_str().unwrap().len(), 64);
    assert!(!preview.to_string().contains("top-secret"));
    task.abort();
}
