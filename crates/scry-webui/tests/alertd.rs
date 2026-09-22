use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{header, Request, StatusCode};
use axum_extra::extract::cookie::Key;
use scry_webui::{attach_alertd_targets, parse_targets, router, AppConfig, AppState, RelayLimits};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tower::ServiceExt;

const PASSWORD: &str = "hunter2";

fn state(alertd: Option<String>, insecure_cookie: bool) -> AppState {
    let (mut targets, default_target) = parse_targets(&["local=127.0.0.1:1".into()]).unwrap();
    if let Some(address) = alertd {
        attach_alertd_targets(&mut targets, &[format!("local={address}")]).unwrap();
    }
    AppState::new(AppConfig {
        targets,
        default_target,
        password: PASSWORD.into(),
        key: Key::from(&[3u8; 64]),
        session_ttl: 3600,
        insecure_cookie,
        alertd_token: Some("service-secret".into()),
        alertd_timeout: Duration::from_secs(2),
        max_alertd_requests: 2,
        limits: RelayLimits::default(),
    })
}

async fn login(state: &AppState) -> (String, String) {
    let response = router(state.clone())
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
    let cookie = response.headers()[header::SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    let response = router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/api/csrf")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    (cookie, json["csrfToken"].as_str().unwrap().to_string())
}

async fn fake_alertd() -> (String, tokio::task::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buffer = [0u8; 2048];
        loop {
            let count = socket.read(&mut buffer).await.unwrap();
            request.extend_from_slice(&buffer[..count]);
            if count == 0 || request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        socket
            .write_all(b"HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}")
            .await
            .unwrap();
        String::from_utf8(request).unwrap()
    });
    (address, task)
}

#[test]
fn alertd_target_parsing_rejects_unknown_duplicate_and_non_http_addresses() {
    let (mut targets, _) =
        parse_targets(&["one=127.0.0.1:1".into(), "two=127.0.0.1:2".into()]).unwrap();
    assert!(attach_alertd_targets(&mut targets, &["missing=http://127.0.0.1:3".into()]).is_err());
    assert!(attach_alertd_targets(&mut targets, &["http://127.0.0.1:3".into()]).is_err());
    assert!(attach_alertd_targets(&mut targets, &["one=127.0.0.1:3".into()]).is_err());
    attach_alertd_targets(&mut targets, &["one=http://127.0.0.1:3/".into()]).unwrap();
    assert!(attach_alertd_targets(&mut targets, &["one=http://127.0.0.1:4".into()]).is_err());
    assert_eq!(
        targets[0].alertd_addr.as_deref(),
        Some("http://127.0.0.1:3")
    );
}

#[tokio::test]
async fn alerts_require_auth_and_distinguish_unknown_from_unconfigured_targets() {
    let state = state(None, false);
    let unauthenticated = router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/api/v1/alerts/monitors")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
    let (cookie, _) = login(&state).await;
    let unconfigured = router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/api/v1/alerts/monitors")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unconfigured.status(), StatusCode::CONFLICT);
    let unknown = router(state)
        .oneshot(
            Request::builder()
                .uri("/api/v1/alerts/monitors")
                .header(header::COOKIE, cookie)
                .header("x-scry-target", "missing")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unknown.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn mutation_requires_csrf_and_is_disabled_in_insecure_mode() {
    for insecure in [false, true] {
        let state = state(Some("http://127.0.0.1:1".into()), insecure);
        let (cookie, _) = login(&state).await;
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/alerts/monitors")
                    .header(header::COOKIE, cookie)
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8_lossy(&body).contains(if insecure {
            "insecure-cookie"
        } else {
            "CSRF"
        }));
    }
}

#[tokio::test]
async fn proxy_supplies_service_bearer_and_preserves_upstream_status() {
    let (address, captured) = fake_alertd().await;
    let state = state(Some(address), false);
    let (cookie, csrf) = login(&state).await;
    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/alerts/notification-targets/target-id/test?dry=true")
                .header(header::COOKIE, cookie)
                .header(header::AUTHORIZATION, "Bearer browser-secret")
                .header(header::HOST, "scry.example")
                .header(header::ORIGIN, "https://scry.example")
                .header("sec-fetch-site", "same-origin")
                .header("x-scry-csrf", csrf)
                .header("idempotency-key", "request-1")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let request = captured.await.unwrap().to_ascii_lowercase();
    assert!(request.starts_with("post /v1/notification-targets/target-id/test?dry=true "));
    assert!(request.contains("authorization: bearer service-secret"));
    assert!(!request.contains("browser-secret"));
    assert!(!request.contains("cookie:"));
    assert!(request.contains("idempotency-key: request-1"));
}

#[tokio::test]
async fn upstream_connection_errors_map_to_bad_gateway() {
    let state = state(Some("http://127.0.0.1:1".into()), false);
    let (cookie, _) = login(&state).await;
    let response = router(state)
        .oneshot(
            Request::builder()
                .uri("/api/v1/alerts/monitors")
                .header(header::COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
}
