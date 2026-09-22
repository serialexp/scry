//! Password → signed-cookie session and CSRF foundations.
//!
//! A single shared password (env `SCRY_WEBUI_PASSWORD`) gates the app. On a
//! successful `POST /api/login` we set a signed cookie containing a versioned
//! absolute expiry and a cryptographically random per-session nonce. The cookie
//! is signed by the server's [`Key`](axum_extra::extract::cookie::Key) (derived
//! from the password), so a tampered cookie fails verification and reads as
//! absent.
//!
//! The cookie is `HttpOnly` + `SameSite=Strict` + `Secure` by default. Direct
//! plain-HTTP development can explicitly opt out with `--insecure-cookie` or
//! `SCRY_WEBUI_INSECURE_COOKIE`.

use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::Json;
use axum_extra::extract::cookie::{Cookie, SameSite, SignedCookieJar};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use rand::{rngs::OsRng, RngCore};
use serde::{Deserialize, Serialize};

use crate::AppState;

/// Name of the signed session cookie.
const COOKIE_NAME: &str = "scry_session";
/// Version prefix for the signed cookie's internal payload.
const SESSION_VERSION: &str = "v1";
/// Header later mutation routes require to prove access to the session nonce.
pub const CSRF_HEADER: &str = "x-scry-csrf";
const NONCE_BYTES: usize = 32;

#[derive(Deserialize)]
pub struct LoginBody {
    pub password: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CsrfResponse {
    csrf_token: String,
}

#[derive(Debug)]
pub(crate) struct Session {
    nonce: [u8; NONCE_BYTES],
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
    let mut nonce = [0u8; NONCE_BYTES];
    OsRng.fill_bytes(&mut nonce);
    let value = format!(
        "{SESSION_VERSION}.{expiry}.{}",
        URL_SAFE_NO_PAD.encode(nonce)
    );
    let cookie = Cookie::build((COOKIE_NAME, value))
        .http_only(true)
        .same_site(SameSite::Strict)
        .secure(!state.insecure_cookie())
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
    if session(&jar).is_some() {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::UNAUTHORIZED
    }
}

/// `GET /api/csrf` — expose the authenticated session's nonce for subsequent
/// same-origin mutation requests. The session cookie remains HttpOnly.
pub async fn csrf(
    jar: SignedCookieJar,
) -> Result<([(header::HeaderName, &'static str); 1], Json<CsrfResponse>), StatusCode> {
    let session = session(&jar).ok_or(StatusCode::UNAUTHORIZED)?;
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(CsrfResponse {
            csrf_token: URL_SAFE_NO_PAD.encode(session.nonce),
        }),
    ))
}

/// True iff the jar carries a signature-valid, version-supported, unexpired
/// session cookie.
pub fn session_valid(jar: &SignedCookieJar) -> bool {
    session(jar).is_some()
}

/// Parse an authenticated session. `jar.get` only returns cookies that pass
/// HMAC verification, so a missing or tampered cookie reads as unauthenticated.
pub(crate) fn session(jar: &SignedCookieJar) -> Option<Session> {
    let cookie = jar.get(COOKIE_NAME)?;
    let mut fields = cookie.value().split('.');
    let version = fields.next()?;
    let expiry = fields.next()?.parse::<i64>().ok()?;
    let nonce = fields.next()?;
    if version != SESSION_VERSION
        || fields.next().is_some()
        || expiry <= chrono::Utc::now().timestamp()
    {
        return None;
    }
    Some(Session {
        nonce: decode_nonce(nonce)?,
    })
}

fn decode_nonce(nonce: &str) -> Option<[u8; NONCE_BYTES]> {
    let mut decoded = [0u8; NONCE_BYTES];
    let written = URL_SAFE_NO_PAD.decode_slice(nonce, &mut decoded).ok()?;
    (written == NONCE_BYTES).then_some(decoded)
}

/// Validate browser same-origin metadata. Mutation routes should call this
/// before acting; requiring both browser-controlled Fetch Metadata and an
/// `Origin` authority matching `Host` fails closed when either is absent.
pub fn same_origin(headers: &HeaderMap) -> bool {
    let fetch_site_matches = headers
        .get("sec-fetch-site")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == "same-origin");
    let Some(host) = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    let origin_matches = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<axum::http::Uri>().ok())
        .and_then(|origin| {
            origin
                .authority()
                .map(|authority| authority.as_str() == host)
        })
        .unwrap_or(false);
    fetch_site_matches && origin_matches
}

/// Validate the double-submit CSRF header against the nonce held inside the
/// signed, HttpOnly session cookie. The comparison is constant-time.
pub fn csrf_nonce_valid(headers: &HeaderMap, jar: &SignedCookieJar) -> bool {
    let Some(expected) = session(jar).map(|session| session.nonce) else {
        return false;
    };
    nonce_header_valid(headers, &expected)
}

fn nonce_header_valid(headers: &HeaderMap, expected: &[u8; NONCE_BYTES]) -> bool {
    headers
        .get(CSRF_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(decode_nonce)
        .is_some_and(|provided| ct_eq(&provided, expected))
}

/// Reusable complete guard for future browser mutation handlers.
pub fn mutation_request_valid(headers: &HeaderMap, jar: &SignedCookieJar) -> bool {
    same_origin(headers) && csrf_nonce_valid(headers, jar)
}

/// Constant-time byte comparison (within equal length) to avoid timing oracles.
/// Length mismatch short-circuits; password and nonce lengths are not sensitive.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_origin_requires_matching_origin_and_fetch_metadata() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, "scry.example:8443".parse().unwrap());
        headers.insert(header::ORIGIN, "https://scry.example:8443".parse().unwrap());
        headers.insert("sec-fetch-site", "same-origin".parse().unwrap());
        assert!(same_origin(&headers));

        headers.insert(header::ORIGIN, "https://evil.example".parse().unwrap());
        assert!(!same_origin(&headers));
        headers.remove(header::ORIGIN);
        assert!(!same_origin(&headers));
    }

    #[test]
    fn nonce_shape_is_strict() {
        assert!(decode_nonce(&URL_SAFE_NO_PAD.encode([42u8; NONCE_BYTES])).is_some());
        assert!(decode_nonce("not-a-session-nonce").is_none());
        assert!(decode_nonce(&URL_SAFE_NO_PAD.encode([42u8; NONCE_BYTES - 1])).is_none());
    }

    #[test]
    fn nonce_header_must_be_present_and_exact() {
        let expected = [42u8; NONCE_BYTES];
        let mut headers = HeaderMap::new();
        assert!(!nonce_header_valid(&headers, &expected));
        headers.insert(
            CSRF_HEADER,
            URL_SAFE_NO_PAD.encode([7u8; NONCE_BYTES]).parse().unwrap(),
        );
        assert!(!nonce_header_valid(&headers, &expected));
        headers.insert(
            CSRF_HEADER,
            URL_SAFE_NO_PAD.encode(expected).parse().unwrap(),
        );
        assert!(nonce_header_valid(&headers, &expected));
    }
}
