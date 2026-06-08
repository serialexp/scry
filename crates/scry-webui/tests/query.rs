//! `/api/query` relay tests: auth gate, byte-pipe round-trip, the 502 path when
//! the upstream `scry-queryd` is unreachable, the `/api/targets` listing, and
//! `X-Scry-Target` routing across multiple configured targets.

use axum::body::{to_bytes, Body};
use axum::http::{header, Request, StatusCode};
use axum::Router;
use axum_extra::extract::cookie::Key;
use scry_webui::{parse_targets, router, AppState};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tower::ServiceExt;

const PASSWORD: &str = "hunter2";

/// Header carrying the selected target id (mirrors `query::TARGET_HEADER`).
const TARGET_HEADER: &str = "x-scry-target";

/// Build a router whose byte-pipe dials the single `queryd` (id `default`).
/// Fixed key so a cookie minted by one cloned instance verifies in the next.
fn app(queryd: &str) -> Router {
    app_targets(&[queryd.to_string()])
}

/// Build a router over a parsed target allowlist (raw `--queryd` values).
fn app_targets(raw: &[String]) -> Router {
    let (targets, default) = parse_targets(raw).unwrap();
    let state = AppState::new(
        targets,
        default,
        PASSWORD.to_string(),
        Key::from(&[9u8; 64]),
        3600,
        false,
    );
    router(state)
}

/// Log in and return the `name=value` cookie pair to replay on later requests.
async fn login_cookie(app: &Router) -> String {
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(format!(r#"{{"password":"{PASSWORD}"}}"#)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    res.headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string()
}

/// A one-shot fake `scry-queryd`: accept one connection, read exactly
/// `expect_len` request bytes, write `response`, then close (so the relay's
/// `read_to_end` terminates). Returns the bound address.
async fn fake_queryd(expect_len: usize, response: Vec<u8>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; expect_len];
        sock.read_exact(&mut buf).await.unwrap();
        sock.write_all(&response).await.unwrap();
        // drop `sock` → EOF for the relay.
    });
    addr
}

#[tokio::test]
async fn query_requires_auth() {
    let res = app("127.0.0.1:1")
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/query")
                .body(Body::from(vec![1u8, 2, 3]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn query_pipes_bytes_through() {
    let request = b"framed-query-request".to_vec();
    let response = b"framed-query-response-arrow-ipc".to_vec();
    let addr = fake_queryd(request.len(), response.clone()).await;

    let app = app(&addr);
    let cookie = login_cookie(&app).await;

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/query")
                .header(header::COOKIE, cookie)
                .body(Body::from(request))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let got = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    assert_eq!(got.as_ref(), response.as_slice());
}

#[tokio::test]
async fn query_returns_502_when_queryd_down() {
    // Port 1 on loopback refuses immediately.
    let app = app("127.0.0.1:1");
    let cookie = login_cookie(&app).await;

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/query")
                .header(header::COOKIE, cookie)
                .body(Body::from(vec![0u8; 8]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_GATEWAY);
}

#[tokio::test]
async fn targets_requires_auth() {
    let res = app("127.0.0.1:1")
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/targets")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn targets_lists_configured_with_default() {
    let app = app_targets(&[
        "local=127.0.0.1:4101".into(),
        "gothab=127.0.0.1:4100".into(),
    ]);
    let cookie = login_cookie(&app).await;

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/targets")
                .header(header::COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["default"], "local");
    let targets = json["targets"].as_array().unwrap();
    assert_eq!(targets.len(), 2);
    assert_eq!(targets[0]["id"], "local");
    assert_eq!(targets[1]["id"], "gothab");
    // The raw address must never reach the browser.
    assert!(targets[0].get("addr").is_none());
}

#[tokio::test]
async fn query_routes_by_target_header() {
    // Two distinct fake upstreams; the header picks which one is dialed.
    let req_a = b"to-a".to_vec();
    let req_b = b"to-b".to_vec();
    let resp_a = b"resp-from-a".to_vec();
    let resp_b = b"resp-from-b".to_vec();
    let addr_a = fake_queryd(req_a.len(), resp_a.clone()).await;
    let addr_b = fake_queryd(req_b.len(), resp_b.clone()).await;

    let app = app_targets(&[format!("a={addr_a}"), format!("b={addr_b}")]);
    let cookie = login_cookie(&app).await;

    // Target b explicitly via the header.
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/query")
                .header(header::COOKIE, &cookie)
                .header(TARGET_HEADER, "b")
                .body(Body::from(req_b))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let got = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    assert_eq!(got.as_ref(), resp_b.as_slice());
}

#[tokio::test]
async fn query_defaults_when_header_absent() {
    // No header → the first declared target (a) is dialed.
    let req_a = b"to-a".to_vec();
    let resp_a = b"resp-from-a".to_vec();
    let addr_a = fake_queryd(req_a.len(), resp_a.clone()).await;

    let app = app_targets(&[format!("a={addr_a}"), "b=127.0.0.1:1".into()]);
    let cookie = login_cookie(&app).await;

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/query")
                .header(header::COOKIE, cookie)
                .body(Body::from(req_a))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let got = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    assert_eq!(got.as_ref(), resp_a.as_slice());
}

#[tokio::test]
async fn query_unknown_target_is_400() {
    let app = app_targets(&["a=127.0.0.1:4101".into(), "b=127.0.0.1:4100".into()]);
    let cookie = login_cookie(&app).await;

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/query")
                .header(header::COOKIE, cookie)
                .header(TARGET_HEADER, "nope")
                .body(Body::from(vec![0u8; 8]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}
