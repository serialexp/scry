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

use anyhow::{bail, Context, Result};
use axum::extract::{DefaultBodyLimit, FromRef};
use axum::routing::{get, post};
use axum::Router;
use axum_extra::extract::cookie::Key;
use clap::Parser;
use tracing::info;

/// Env var carrying the shared login password (kept out of argv).
pub const PASSWORD_ENV: &str = "SCRY_WEBUI_PASSWORD";

/// CLI arguments for the `scry web` subcommand (formerly the `scry-webui` bin).
#[derive(Parser, Debug)]
#[command(about = "Browser query UI for the scry query daemon")]
pub struct Args {
    /// HTTP listen address.
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub listen: String,

    /// Upstream query-daemon target(s). Repeatable; each is `id=host:port`
    /// (e.g. `--queryd local=127.0.0.1:4101 --queryd gothab=127.0.0.1:4100`),
    /// and the first listed is the default. A single bare `host:port` is also
    /// accepted (id `default`). The browser selects a target by id — never a
    /// raw address — so the relay stays SSRF-safe. Defaults to
    /// `127.0.0.1:4100` when omitted.
    #[arg(long, value_name = "ID=ADDR")]
    pub queryd: Vec<String>,

    /// Session lifetime in seconds (default 1 day).
    #[arg(long, default_value_t = 86_400)]
    pub session_ttl: i64,

    /// Set the `Secure` attribute on the session cookie. Enable this only when
    /// the browser reaches scry-webui over HTTPS (e.g. behind a TLS reverse
    /// proxy such as Caddy); over plain `http://` a `Secure` cookie is dropped
    /// by the browser and login silently fails. Also via `SCRY_WEBUI_SECURE_COOKIE`
    /// (accepts 1/0/true/false/yes/no/on/off). Bare `--secure-cookie` ⇒ true.
    #[arg(
        long,
        env = "SCRY_WEBUI_SECURE_COOKIE",
        num_args = 0..=1,
        default_value_t = false,
        default_missing_value = "true",
        value_parser = clap::builder::BoolishValueParser::new(),
    )]
    pub secure_cookie: bool,
}

/// Serve the browser query UI and relay queries to the query daemon.
pub async fn run(args: Args) -> Result<()> {
    let password =
        std::env::var(PASSWORD_ENV).map_err(|_| anyhow::anyhow!("{PASSWORD_ENV} must be set"))?;
    if password.is_empty() {
        bail!("{PASSWORD_ENV} must not be empty");
    }

    // Derive a stable cookie-signing key from the password: sessions survive a
    // restart, and rotating the password naturally invalidates old sessions.
    let key = derive_key(&password);

    let (targets, default_target) =
        parse_targets(&args.queryd).context("parsing --queryd targets")?;
    let targets_desc = targets
        .iter()
        .map(|t| format!("{}={}", t.id, t.addr))
        .collect::<Vec<_>>()
        .join(", ");

    let state = AppState::new(
        targets,
        default_target.clone(),
        password,
        key,
        args.session_ttl,
        args.secure_cookie,
    );
    let app = router(state);

    let listener = tokio::net::TcpListener::bind(&args.listen)
        .await
        .with_context(|| format!("binding {}", args.listen))?;
    info!(
        listen = %args.listen,
        targets = %targets_desc,
        default = %default_target,
        session_ttl = args.session_ttl,
        "scry-webui ready"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .context("serving HTTP")?;

    Ok(())
}

/// Derive a 256-bit-class signing key from the password via HKDF
/// (`Key::derive_from`). That function requires ≥32 bytes of input material, so
/// we domain-separate with a fixed label and repeat to reach the floor — the
/// derivation is deterministic (key stable across restarts) and the entropy is
/// the password's, which is inherent to a single-password scheme.
fn derive_key(password: &str) -> Key {
    let mut material = format!("scry-webui-session-v1::{password}").into_bytes();
    while material.len() < 32 {
        let again = material.clone();
        material.extend_from_slice(&again);
    }
    Key::derive_from(&material)
}

/// One selectable upstream `scry-queryd` the byte-pipe may dial. The browser
/// picks a target by its `id` (never by raw address — that would be an SSRF
/// vector); the server maps the id back to `addr` from this allowlist.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct Target {
    /// Stable identifier the browser sends back (header `X-Scry-Target`).
    pub id: String,
    /// Human-friendly label for the UI dropdown.
    pub label: String,
    /// The `host:port` the relay actually dials. Not exposed to the browser.
    #[serde(skip)]
    pub addr: String,
}

/// Parse repeatable `--queryd` values into the target allowlist, returning the
/// targets (in declared order) and the default target id (the first one).
///
/// Each value is `id=addr` or a bare `addr`. A bare `addr` is only allowed as
/// the sole entry (id `"default"`, label = the addr); once there are several,
/// every entry must be named so the ids are unambiguous. Empty input falls back
/// to a single `127.0.0.1:4100` default. Ids must be unique and non-empty.
pub fn parse_targets(raw: &[String]) -> Result<(Vec<Target>, String)> {
    if raw.is_empty() {
        let addr = "127.0.0.1:4100".to_string();
        return Ok((
            vec![Target {
                id: "default".into(),
                label: addr.clone(),
                addr,
            }],
            "default".into(),
        ));
    }

    let mut targets: Vec<Target> = Vec::with_capacity(raw.len());
    for entry in raw {
        let entry = entry.trim();
        let target = match entry.split_once('=') {
            Some((id, addr)) => {
                let id = id.trim();
                let addr = addr.trim();
                if id.is_empty() || addr.is_empty() {
                    bail!("invalid --queryd '{entry}': expected 'id=host:port'");
                }
                Target {
                    id: id.to_string(),
                    label: id.to_string(),
                    addr: addr.to_string(),
                }
            }
            None => {
                // A bare address is only unambiguous as the only target.
                if raw.len() > 1 {
                    bail!(
                        "invalid --queryd '{entry}': name every target as 'id=host:port' \
                         when more than one is given"
                    );
                }
                Target {
                    id: "default".into(),
                    label: entry.to_string(),
                    addr: entry.to_string(),
                }
            }
        };
        if targets.iter().any(|t| t.id == target.id) {
            bail!("duplicate --queryd id '{}'", target.id);
        }
        targets.push(target);
    }

    let default = targets[0].id.clone();
    Ok((targets, default))
}

/// Shared, clone-cheap application state (mirrors `scry-gateway`'s pattern: a
/// `#[derive(Clone)]` handle over `Arc`-d internals).
#[derive(Clone)]
pub struct AppState(Arc<Inner>);

struct Inner {
    /// The selectable upstream `scry-queryd` targets (the allowlist).
    targets: Vec<Target>,
    /// Id of the target used when the browser sends no selection.
    default_target: String,
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
        targets: Vec<Target>,
        default_target: String,
        password: String,
        key: Key,
        session_ttl: i64,
        secure_cookie: bool,
    ) -> Self {
        Self(Arc::new(Inner {
            targets,
            default_target,
            password,
            key,
            session_ttl,
            secure_cookie,
        }))
    }

    pub fn targets(&self) -> &[Target] {
        &self.0.targets
    }

    pub fn default_target(&self) -> &str {
        &self.0.default_target
    }

    /// Resolve a browser-supplied target id to its upstream address. `None`/
    /// empty selects the default; an unknown id returns `None` (caller → 400).
    pub fn resolve_target(&self, id: Option<&str>) -> Option<&str> {
        let id = match id {
            Some(s) if !s.is_empty() => s,
            _ => self.default_target(),
        };
        self.0
            .targets
            .iter()
            .find(|t| t.id == id)
            .map(|t| t.addr.as_str())
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
        .route("/api/targets", get(query::targets))
        .route("/api/query", post(query::query))
        .layer(DefaultBodyLimit::max(API_BODY_LIMIT))
        .fallback(assets::serve)
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_falls_back_to_a_single_default() {
        let (targets, default) = parse_targets(&[]).unwrap();
        assert_eq!(default, "default");
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].id, "default");
        assert_eq!(targets[0].addr, "127.0.0.1:4100");
    }

    #[test]
    fn single_bare_addr_is_the_default() {
        let (targets, default) = parse_targets(&["127.0.0.1:4200".into()]).unwrap();
        assert_eq!(default, "default");
        assert_eq!(targets[0].addr, "127.0.0.1:4200");
        assert_eq!(targets[0].label, "127.0.0.1:4200");
    }

    #[test]
    fn named_targets_keep_order_and_first_is_default() {
        let (targets, default) = parse_targets(&[
            "local=127.0.0.1:4101".into(),
            "gothab=127.0.0.1:4100".into(),
        ])
        .unwrap();
        assert_eq!(default, "local");
        assert_eq!(
            targets.iter().map(|t| t.id.as_str()).collect::<Vec<_>>(),
            ["local", "gothab"]
        );
        assert_eq!(targets[1].addr, "127.0.0.1:4100");
    }

    #[test]
    fn bare_addr_rejected_when_multiple() {
        assert!(parse_targets(&["127.0.0.1:4101".into(), "g=127.0.0.1:4100".into()]).is_err());
    }

    #[test]
    fn duplicate_ids_rejected() {
        assert!(parse_targets(&["a=127.0.0.1:1".into(), "a=127.0.0.1:2".into()]).is_err());
    }

    #[test]
    fn empty_id_or_addr_rejected() {
        assert!(parse_targets(&["=127.0.0.1:1".into()]).is_err());
        assert!(parse_targets(&["a=".into()]).is_err());
    }

    #[test]
    fn resolve_target_maps_id_and_defaults() {
        let (targets, default) = parse_targets(&[
            "local=127.0.0.1:4101".into(),
            "gothab=127.0.0.1:4100".into(),
        ])
        .unwrap();
        let state = AppState::new(
            targets,
            default,
            "pw".into(),
            Key::from(&[7u8; 64]),
            60,
            false,
        );
        assert_eq!(state.resolve_target(Some("gothab")), Some("127.0.0.1:4100"));
        assert_eq!(state.resolve_target(Some("local")), Some("127.0.0.1:4101"));
        // Absent / empty → default (local).
        assert_eq!(state.resolve_target(None), Some("127.0.0.1:4101"));
        assert_eq!(state.resolve_target(Some("")), Some("127.0.0.1:4101"));
        // Unknown → None.
        assert_eq!(state.resolve_target(Some("nope")), None);
    }
}
