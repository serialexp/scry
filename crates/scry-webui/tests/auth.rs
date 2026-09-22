//! Auth surface tests: login / logout / me + signed versioned sessions and CSRF.
//! Exercised against the real `router` via `tower::ServiceExt::oneshot`.

use std::time::Duration;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum_extra::extract::cookie::Key;
use http_body_util::BodyExt;
use scry_webui::{parse_targets, router, AppConfig, AppState, RelayLimits};
use serde_json::Value;
use tower::ServiceExt;

const PASSWORD: &str = "hunter2";

fn state() -> AppState {
    state_with_insecure(false)
}

fn insecure_state() -> AppState {
    state_with_insecure(true)
}

fn state_with_insecure(insecure_cookie: bool) -> AppState {
    let (targets, default) = parse_targets(&["127.0.0.1:1".to_string()]).unwrap();
    AppState::new(AppConfig {
        targets,
        default_target: default,
        password: PASSWORD.to_string(),
        key: Key::from(&[7u8; 64]),
        session_ttl: 3600,
        insecure_cookie,
        alertd_token: None,
        alertd_timeout: Duration::from_secs(15),
        max_alertd_requests: 16,
        limits: RelayLimits {
            setup_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(30),
            ..RelayLimits::default()
        },
    })
}

fn login_req(password: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/api/login")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(format!(r#"{{"password":"{password}"}}"#)))
        .unwrap()
}

async fn login_cookie(app: AppState) -> String {
    let res = router(app).oneshot(login_req(PASSWORD)).await.unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    res.headers()
        .get(header::SET_COOKIE)
        .expect("login must set a cookie")
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn me_without_cookie_is_unauthorized() {
    let res = router(state())
        .oneshot(
            Request::builder()
                .uri("/api/me")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn login_wrong_password_is_unauthorized_and_sets_no_cookie() {
    let res = router(state()).oneshot(login_req("nope")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    assert!(res.headers().get(header::SET_COOKIE).is_none());
}

#[tokio::test]
async fn login_sets_secure_cookie_by_default_and_me_accepts_it() {
    let res = router(state()).oneshot(login_req(PASSWORD)).await.unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    let set_cookie = res
        .headers()
        .get(header::SET_COOKIE)
        .expect("login must set a cookie")
        .to_str()
        .unwrap()
        .to_string();
    assert!(set_cookie.contains("HttpOnly"), "cookie: {set_cookie}");
    assert!(
        set_cookie.contains("SameSite=Strict"),
        "cookie: {set_cookie}"
    );
    assert!(set_cookie.contains("Secure"), "cookie: {set_cookie}");

    let pair = set_cookie.split(';').next().unwrap();
    let res = router(state())
        .oneshot(
            Request::builder()
                .uri("/api/me")
                .header(header::COOKIE, pair)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn insecure_cookie_opt_out_omits_secure_attribute() {
    let res = router(insecure_state())
        .oneshot(login_req(PASSWORD))
        .await
        .unwrap();
    let set_cookie = res
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(!set_cookie.contains("Secure"), "cookie: {set_cookie}");
}

#[tokio::test]
async fn sessions_have_unique_csrf_nonces() {
    let first = login_cookie(state()).await;
    let second = login_cookie(state()).await;
    assert_ne!(
        first, second,
        "independent logins must mint unique sessions"
    );
}

#[tokio::test]
async fn csrf_endpoint_requires_authentication_and_returns_session_token() {
    let unauthorized = router(state())
        .oneshot(
            Request::builder()
                .uri("/api/csrf")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let cookie = login_cookie(state()).await;
    let response = router(state())
        .oneshot(
            Request::builder()
                .uri("/api/csrf")
                .header(header::COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let token = json["csrfToken"].as_str().expect("csrfToken string");
    assert!(
        token.len() >= 40,
        "token must encode a 256-bit random nonce"
    );
}

#[tokio::test]
async fn tampered_and_legacy_expiry_only_cookies_are_rejected() {
    for cookie in [
        "scry_session=9999999999",
        "scry_session=v1.9999999999.short",
    ] {
        let res = router(state())
            .oneshot(
                Request::builder()
                    .uri("/api/me")
                    .header(header::COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }
}
