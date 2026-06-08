//! Auth surface tests: login / logout / me + the signed session cookie.
//! Exercised against the real `router` via `tower::ServiceExt::oneshot`.

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum_extra::extract::cookie::Key;
use scry_webui::{parse_targets, router, AppState};
use tower::ServiceExt;

const PASSWORD: &str = "hunter2";

/// Fixed key so signed cookies issued by one router instance verify in another
/// (each `oneshot` consumes its router, so we rebuild between requests).
fn state() -> AppState {
    state_with_secure(false)
}

/// Same as `state()` but with `secure_cookie` enabled (HTTPS / TLS-proxy mode).
fn secure_state() -> AppState {
    state_with_secure(true)
}

fn state_with_secure(secure: bool) -> AppState {
    let (targets, default) = parse_targets(&["127.0.0.1:1".to_string()]).unwrap();
    AppState::new(
        targets,
        default,
        PASSWORD.to_string(),
        Key::from(&[7u8; 64]),
        3600,
        secure,
    )
}

fn login_req(password: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/api/login")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(format!(r#"{{"password":"{password}"}}"#)))
        .unwrap()
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
async fn login_sets_cookie_and_me_accepts_it() {
    // Log in with the correct password.
    let res = router(state()).oneshot(login_req(PASSWORD)).await.unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    let set_cookie = res
        .headers()
        .get(header::SET_COOKIE)
        .expect("login must set a cookie")
        .to_str()
        .unwrap()
        .to_string();
    // Cookie should be HttpOnly + SameSite=Strict, and NOT Secure by default
    // (plain-HTTP LAN deployment).
    assert!(set_cookie.contains("HttpOnly"), "cookie: {set_cookie}");
    assert!(
        set_cookie.contains("SameSite=Strict"),
        "cookie: {set_cookie}"
    );
    assert!(!set_cookie.contains("Secure"), "cookie: {set_cookie}");

    // Replay just the `name=value` pair on a fresh router (same key).
    let pair = set_cookie.split(';').next().unwrap().to_string();
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
async fn secure_cookie_sets_secure_attribute() {
    let res = secure_state();
    let res = router(res).oneshot(login_req(PASSWORD)).await.unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    let set_cookie = res
        .headers()
        .get(header::SET_COOKIE)
        .expect("login must set a cookie")
        .to_str()
        .unwrap()
        .to_string();
    assert!(set_cookie.contains("Secure"), "cookie: {set_cookie}");
}

#[tokio::test]
async fn tampered_cookie_is_rejected() {
    let res = router(state())
        .oneshot(
            Request::builder()
                .uri("/api/me")
                // Not a validly-signed value → verification fails → treated absent.
                .header(header::COOKIE, "scry_session=9999999999")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}
