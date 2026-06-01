//! `/api/query` relay tests: auth gate, byte-pipe round-trip, and the 502 path
//! when the upstream `scry-queryd` is unreachable.

use axum::body::{to_bytes, Body};
use axum::http::{header, Request, StatusCode};
use axum::Router;
use axum_extra::extract::cookie::Key;
use scry_webui::{router, AppState};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tower::ServiceExt;

const PASSWORD: &str = "hunter2";

/// Build a router whose byte-pipe dials `queryd`. Fixed key so a cookie minted
/// by one cloned instance verifies in the next.
fn app(queryd: &str) -> Router {
    let state = AppState::new(
        queryd.to_string(),
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
