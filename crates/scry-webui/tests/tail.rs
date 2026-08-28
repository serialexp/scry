//! `/api/tail` relay tests: the auth gate, the two distinct rejection codes
//! (unknown target vs target with no live endpoint), that the relay dials the
//! *tail* address rather than the query one, that records stream out as the
//! upstream produces them (not buffered to EOF), that the tail pool is separate
//! from the query pool, and that dropping the response body closes the upstream
//! socket so queryd deregisters the subscription.
//!
//! The upstream here is a bare TCP socket, not a real queryd: this layer is a
//! byte-pipe and has no protocol knowledge to test. The tail sub-protocol
//! itself is covered end-to-end by `scripts/smoke-webui-tail.sh`.

use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{header, Request, StatusCode};
use axum::Router;
use axum_extra::extract::cookie::Key;
use http_body_util::BodyExt;
use scry_webui::{parse_targets, router, AppConfig, AppState, RelayLimits};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tower::ServiceExt;

const PASSWORD: &str = "hunter2";
const TARGET_HEADER: &str = "x-scry-target";

fn app_with(raw_queryd: &[String], raw_tail: &[String], limits: RelayLimits) -> Router {
    let (mut targets, default) = parse_targets(raw_queryd).unwrap();
    scry_webui::attach_tail_targets(&mut targets, raw_tail).unwrap();
    router(AppState::new(AppConfig {
        targets,
        default_target: default,
        password: PASSWORD.to_string(),
        key: Key::from(&[5u8; 64]),
        session_ttl: 3600,
        secure_cookie: false,
        limits,
    }))
}

fn limits() -> RelayLimits {
    RelayLimits {
        setup_timeout: Duration::from_secs(5),
        idle_timeout: Duration::from_secs(5),
        max_relays: 32,
        tail_idle_timeout: Some(Duration::from_secs(5)),
        max_tails: 8,
    }
}

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

fn tail_req(cookie: &str, target: Option<&str>, body: Vec<u8>) -> Request<Body> {
    let mut b = Request::builder()
        .method("POST")
        .uri("/api/tail")
        .header(header::COOKIE, cookie);
    if let Some(t) = target {
        b = b.header(TARGET_HEADER, t);
    }
    b.body(Body::from(body)).unwrap()
}

#[tokio::test]
async fn tail_requires_auth() {
    let app = app_with(
        &["127.0.0.1:1".to_string()],
        &["127.0.0.1:2".to_string()],
        limits(),
    );
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/tail")
                .body(Body::from(vec![0u8; 4]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn unknown_target_is_400_and_no_live_endpoint_is_409() {
    let app = app_with(
        &["a=127.0.0.1:1".to_string(), "b=127.0.0.1:2".to_string()],
        // Only `a` gets a tail address.
        &["a=127.0.0.1:3".to_string()],
        limits(),
    );
    let cookie = login_cookie(&app).await;

    let res = app
        .clone()
        .oneshot(tail_req(&cookie, Some("nope"), vec![1, 2, 3]))
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        StatusCode::BAD_REQUEST,
        "an id that names no target is a bad request"
    );

    let res = app
        .oneshot(tail_req(&cookie, Some("b"), vec![1, 2, 3]))
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        StatusCode::CONFLICT,
        "a real target with no --queryd-tail must be distinguishable from a typo"
    );
}

#[tokio::test]
async fn targets_report_which_ones_can_tail() {
    let app = app_with(
        &["a=127.0.0.1:1".to_string(), "b=127.0.0.1:2".to_string()],
        &["a=127.0.0.1:3".to_string()],
        limits(),
    );
    let cookie = login_cookie(&app).await;
    let res = app
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
    let targets = json["targets"].as_array().unwrap();
    assert_eq!(targets[0]["id"], "a");
    assert_eq!(targets[0]["live"], true);
    assert_eq!(targets[1]["id"], "b");
    assert_eq!(targets[1]["live"], false);
    // No address of any kind reaches the browser.
    assert!(
        !String::from_utf8_lossy(&body).contains("127.0.0.1"),
        "target addresses must not be serialized: {}",
        String::from_utf8_lossy(&body)
    );
}

/// The relay must dial the target's **tail** address, not its query address.
/// Both are live listeners here, so a mix-up shows up as the wrong greeting.
#[tokio::test]
async fn tail_dials_the_tail_address_not_the_query_address() {
    let query_sock = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let query_addr = query_sock.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let (mut sock, _) = query_sock.accept().await.unwrap();
        let _ = sock.write_all(b"QUERY-PORT").await;
    });

    let tail_sock = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tail_addr = tail_sock.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let (mut sock, _) = tail_sock.accept().await.unwrap();
        let mut got = vec![0u8; 9];
        sock.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"subscribe", "the client's bytes arrive verbatim");
        let _ = sock.write_all(b"TAIL-PORT").await;
    });

    let app = app_with(&[query_addr], &[tail_addr], limits());
    let cookie = login_cookie(&app).await;
    let res = app
        .oneshot(tail_req(&cookie, None, b"subscribe".to_vec()))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.as_ref(), b"TAIL-PORT");
}

/// A tail is a push stream: records must reach the browser as the upstream
/// emits them. If this buffered to EOF the UI would show nothing until the
/// subscription ended, which is the whole point of the feature.
#[tokio::test]
async fn records_stream_out_as_they_arrive() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut got = vec![0u8; 3];
        sock.read_exact(&mut got).await.unwrap();
        sock.write_all(b"record-1").await.unwrap();
        release_rx.await.unwrap();
        sock.write_all(b"record-2").await.unwrap();
    });

    let app = app_with(&["127.0.0.1:1".to_string()], &[addr], limits());
    let cookie = login_cookie(&app).await;
    let res = app
        .oneshot(tail_req(&cookie, None, b"sub".to_vec()))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let mut body = res.into_body();
    let first = tokio::time::timeout(Duration::from_secs(2), body.frame())
        .await
        .expect("first record arrived while the stream was still open")
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert_eq!(first.as_ref(), b"record-1");
    release_tx.send(()).unwrap();
    let rest = to_bytes(body, usize::MAX).await.unwrap();
    assert_eq!(rest.as_ref(), b"record-2");
}

/// Long-lived tails draw from their own pool. With `max_tails = 1` a second
/// subscription is refused while the first is open — and crucially the query
/// pool is untouched, so queries keep working.
#[tokio::test]
async fn tail_admission_is_separate_from_query_admission() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut got = vec![0u8; 3];
                if sock.read_exact(&mut got).await.is_ok() {
                    let _ = sock.write_all(b"hi").await;
                    // Hold the connection open like a real (quiet) tail.
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
            });
        }
    });

    let app = app_with(
        &[addr.clone()],
        &[addr],
        RelayLimits {
            max_tails: 1,
            ..limits()
        },
    );
    let cookie = login_cookie(&app).await;

    let held = app
        .clone()
        .oneshot(tail_req(&cookie, None, b"sub".to_vec()))
        .await
        .unwrap();
    assert_eq!(held.status(), StatusCode::OK);

    let refused = app
        .clone()
        .oneshot(tail_req(&cookie, None, b"sub".to_vec()))
        .await
        .unwrap();
    assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);

    // The query pool is a different semaphore, so queries still get through.
    let query = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/query")
                .header(header::COOKIE, &cookie)
                .body(Body::from(b"sub".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(query.status(), StatusCode::OK);

    // Dropping the held body releases the tail permit for the next subscriber.
    drop(held);
    let mut allowed = None;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let res = app
            .clone()
            .oneshot(tail_req(&cookie, None, b"sub".to_vec()))
            .await
            .unwrap();
        if res.status() == StatusCode::OK {
            allowed = Some(res);
            break;
        }
    }
    assert!(
        allowed.is_some(),
        "dropping the response body must release the tail permit"
    );
}

/// The browser closing the tab (dropping the body) has to reach queryd, or the
/// subscription and its fanned-in upstream connections leak.
#[tokio::test]
async fn dropping_the_body_closes_the_upstream_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (eof_tx, eof_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut got = vec![0u8; 3];
        sock.read_exact(&mut got).await.unwrap();
        sock.write_all(b"record-1").await.unwrap();
        // Now wait for our peer to hang up.
        let mut sink = Vec::new();
        let _ = sock.read_to_end(&mut sink).await;
        let _ = eof_tx.send(());
    });

    let app = app_with(&["127.0.0.1:1".to_string()], &[addr], limits());
    let cookie = login_cookie(&app).await;
    let res = app
        .oneshot(tail_req(&cookie, None, b"sub".to_vec()))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    drop(res);

    tokio::time::timeout(Duration::from_secs(2), eof_rx)
        .await
        .expect("upstream must observe EOF when the client goes away")
        .unwrap();
}

/// `tail_idle_timeout: None` is what `--tail-idle-timeout 0` configures: a tail
/// that has matched nothing yet must not be torn down for being quiet.
#[tokio::test]
async fn a_quiet_tail_is_not_killed_when_the_idle_timeout_is_disabled() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut got = vec![0u8; 3];
        sock.read_exact(&mut got).await.unwrap();
        // Silence far longer than any configured query idle timeout, then a record.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let _ = sock.write_all(b"late").await;
    });

    let app = app_with(
        &["127.0.0.1:1".to_string()],
        &[addr],
        RelayLimits {
            idle_timeout: Duration::from_millis(20),
            tail_idle_timeout: None,
            ..limits()
        },
    );
    let cookie = login_cookie(&app).await;
    let res = app
        .oneshot(tail_req(&cookie, None, b"sub".to_vec()))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.as_ref(), b"late");
}
