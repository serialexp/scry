//! scry-webui — a small web server that serves the SolidJS query app to a
//! browser and relays framed query requests to `scry-queryd`, gated by a simple
//! password → signed-cookie session.
//!
//! It is the browser counterpart to the Tauri desktop shell (`desktop/`): the
//! whole query wire protocol lives in TypeScript, and the server is a **dumb
//! byte-pipe** — `POST /api/query` writes the already-framed request bytes to
//! the configured upstream `scry-queryd`, reads the response to EOF, and hands
//! the raw bytes back. The server has zero protocol knowledge, exactly like the
//! Tauri `run_query` command it replaces.
//!
//! `POST /api/query` is the byte-pipe relay; see `query`.

pub mod assets;
pub mod auth;
pub mod query;

use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, FromRef};
use axum::routing::{get, post};
use axum::Router;
use axum_extra::extract::cookie::Key;

/// Shared, clone-cheap application state (mirrors `scry-gateway`'s pattern: a
/// `#[derive(Clone)]` handle over `Arc`-d internals).
#[derive(Clone)]
pub struct AppState(Arc<Inner>);

struct Inner {
    /// Upstream `scry-queryd` address the byte-pipe dials.
    queryd: String,
    /// The shared login password.
    password: String,
    /// Cookie-signing key (derived from the password).
    key: Key,
    /// Session lifetime in seconds.
    session_ttl: i64,
    /// Set the `Secure` attribute on the session cookie. Enable only when the
    /// browser reaches scry-webui over HTTPS (e.g. behind a TLS reverse proxy);
    /// a `Secure` cookie is dropped by the browser over plain `http://`.
    secure_cookie: bool,
}

impl AppState {
    pub fn new(
        queryd: String,
        password: String,
        key: Key,
        session_ttl: i64,
        secure_cookie: bool,
    ) -> Self {
        Self(Arc::new(Inner {
            queryd,
            password,
            key,
            session_ttl,
            secure_cookie,
        }))
    }

    pub fn queryd(&self) -> &str {
        &self.0.queryd
    }

    pub fn password(&self) -> &str {
        &self.0.password
    }

    pub fn session_ttl(&self) -> i64 {
        self.0.session_ttl
    }

    pub fn secure_cookie(&self) -> bool {
        self.0.secure_cookie
    }
}

/// `SignedCookieJar` extracts the signing key from app state via `FromRef`.
impl FromRef<AppState> for Key {
    fn from_ref(state: &AppState) -> Self {
        state.0.key.clone()
    }
}

/// Maximum request-body size for `/api/query`. The framed `QueryRequest` is
/// tiny (tens of bytes to a few KB); 8 MiB is generous headroom and well under
/// the wire's 32 MiB frame ceiling.
const API_BODY_LIMIT: usize = 8 * 1024 * 1024;

/// Build the application router: the `/api/*` surface plus the embedded SPA
/// served for every other path.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/login", post(auth::login))
        .route("/api/logout", post(auth::logout))
        .route("/api/me", get(auth::me))
        .route("/api/query", post(query::query))
        .layer(DefaultBodyLimit::max(API_BODY_LIMIT))
        .fallback(assets::serve)
        .with_state(state)
}
