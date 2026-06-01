//! Password → signed-cookie session.
//!
//! A single shared password (env `SCRY_WEBUI_PASSWORD`) gates the app. On a
//! successful `POST /api/login` we set a signed cookie whose value is the
//! session's absolute expiry (unix seconds). The cookie is signed by the
//! server's [`Key`](axum_extra::extract::cookie::Key) (derived from the
//! password), so a tampered cookie fails verification and reads as absent.
//!
//! The cookie is `HttpOnly` + `SameSite=Strict`. The `Secure` attribute is set
//! when [`AppState::secure_cookie`] is true — enable that (via
//! `--secure-cookie` / `SCRY_WEBUI_SECURE_COOKIE`) when the browser reaches
//! scry-webui over HTTPS, e.g. behind a TLS reverse proxy. It stays off by
//! default because scry-webui itself speaks plain HTTP, and a `Secure` cookie
//! is dropped by the browser over `http://`.

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use axum_extra::extract::cookie::{Cookie, SameSite, SignedCookieJar};
use serde::Deserialize;

use crate::AppState;

/// Name of the signed session cookie.
const COOKIE_NAME: &str = "scry_session";

#[derive(Deserialize)]
pub struct LoginBody {
    pub password: String,
}

/// `POST /api/login` — check the password; on match set the session cookie.
pub async fn login(
    State(state): State<AppState>,
    jar: SignedCookieJar,
    Json(body): Json<LoginBody>,
) -> Result<(SignedCookieJar, StatusCode), StatusCode> {
    if !ct_eq(body.password.as_bytes(), state.password().as_bytes()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let expiry = chrono::Utc::now().timestamp() + state.session_ttl();
    let cookie = Cookie::build((COOKIE_NAME, expiry.to_string()))
        .http_only(true)
        .same_site(SameSite::Strict)
        .secure(state.secure_cookie())
        .path("/")
        .max_age(time::Duration::seconds(state.session_ttl()))
        .build();
    Ok((jar.add(cookie), StatusCode::NO_CONTENT))
}

/// `POST /api/logout` — clear the session cookie.
pub async fn logout(jar: SignedCookieJar) -> (SignedCookieJar, StatusCode) {
    let cleared = Cookie::build((COOKIE_NAME, "")).path("/").build();
    (jar.remove(cleared), StatusCode::NO_CONTENT)
}

/// `GET /api/me` — 204 if the session is valid, else 401. The frontend uses
/// this to decide whether to show the login screen.
pub async fn me(jar: SignedCookieJar) -> StatusCode {
    if session_valid(&jar) {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::UNAUTHORIZED
    }
}

/// True iff the jar carries a signature-valid, unexpired session cookie.
/// `jar.get` only returns cookies that pass HMAC verification, so a missing or
/// tampered cookie reads as `None` → unauthenticated.
pub fn session_valid(jar: &SignedCookieJar) -> bool {
    match jar
        .get(COOKIE_NAME)
        .and_then(|c| c.value().parse::<i64>().ok())
    {
        Some(expiry) => expiry > chrono::Utc::now().timestamp(),
        None => false,
    }
}

/// Constant-time byte comparison (within equal length) to avoid a timing oracle
/// on the password. Length mismatch short-circuits — password length is not
/// sensitive.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
