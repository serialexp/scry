//! scry-webui — a small web server that serves the SolidJS query app to a
//! browser and relays framed query requests to `scry-queryd`, gated by a simple
//! password → signed-cookie session.
//!
//! It is the browser counterpart to the Tauri desktop shell (`desktop/`): the
//! whole query wire protocol lives in TypeScript, and the server is a **dumb
//! byte-pipe** — `POST /api/query` writes the already-framed request bytes to
//! the configured upstream `scry-queryd` and streams the raw response to EOF
//! with HTTP backpressure. The server has zero protocol knowledge, exactly like
//! the Tauri `run_query` command it replaces.
//!
//! `POST /api/query` is the byte-pipe relay; see `query`. `POST /api/tail` is
//! the same pipe pointed at the target's queryd `--tail-listen` port, for the
//! UI's live log tail — a long-lived server-push stream rather than a
//! request/response, but from this server's side identical: write the client's
//! bytes, stream back whatever comes.

pub mod assets;
pub mod auth;
pub mod query;

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

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

    /// Live-tail address of a `--queryd` target, as `id=host:port` — the query
    /// daemon's `--tail-listen` port. Repeatable; the id must name a target
    /// declared with `--queryd` (a single bare `host:port` attaches to the sole
    /// target). queryd's tail listener is a *separate* port from its query port
    /// because the two binschema unions collide on their first byte, so a
    /// target needs both addresses to offer live tailing. Targets without one
    /// simply report `live: false` and the UI disables the Live toggle.
    #[arg(long, value_name = "ID=ADDR")]
    pub queryd_tail: Vec<String>,

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

    /// Deadline for connecting to queryd and writing the request, in seconds.
    /// A failure before response streaming starts returns 504.
    #[arg(long, default_value_t = 10)]
    pub relay_timeout: u64,

    /// Maximum idle interval between response bytes from queryd, in seconds.
    /// This is not a total response deadline: a large response may continue as
    /// long as it keeps making progress.
    #[arg(long, default_value_t = 30)]
    pub relay_idle_timeout: u64,

    /// Maximum query relays active at once. Excess requests return 503 rather
    /// than queueing sockets and response buffers without bound.
    #[arg(long, default_value_t = 32)]
    pub max_relays: usize,

    /// Maximum idle interval between bytes on a live-tail relay, in seconds.
    /// Deliberately *not* `--relay-idle-timeout`: a tail whose matchers select
    /// a quiet stream is legitimately silent for minutes, so the query path's
    /// 30s would kill working subscriptions. `0` disables the timeout (the UI
    /// reconnects on stream end either way).
    #[arg(long, default_value_t = 900)]
    pub tail_idle_timeout: u64,

    /// Maximum live-tail relays active at once. Separate from `--max-relays`
    /// on purpose: a tail holds its permit for its whole lifetime, so sharing
    /// the query pool would let a few open browser tabs starve queries.
    #[arg(long, default_value_t = 8)]
    pub max_tails: usize,
}

/// Serve the browser query UI and relay queries to the query daemon.
pub async fn run(args: Args) -> Result<()> {
    let password =
        std::env::var(PASSWORD_ENV).map_err(|_| anyhow::anyhow!("{PASSWORD_ENV} must be set"))?;
    if password.is_empty() {
        bail!("{PASSWORD_ENV} must not be empty");
    }
    if args.max_relays == 0 {
        bail!("--max-relays must be at least 1");
    }
    if args.max_tails == 0 {
        bail!("--max-tails must be at least 1");
    }

    // Derive a stable cookie-signing key from the password: sessions survive a
    // restart, and rotating the password naturally invalidates old sessions.
    let key = derive_key(&password);

    let (mut targets, default_target) =
        parse_targets(&args.queryd).context("parsing --queryd targets")?;
    attach_tail_targets(&mut targets, &args.queryd_tail)
        .context("parsing --queryd-tail addresses")?;
    let targets_desc = targets
        .iter()
        .map(|t| match &t.tail_addr {
            Some(tail) => format!("{}={} (tail {tail})", t.id, t.addr),
            None => format!("{}={}", t.id, t.addr),
        })
        .collect::<Vec<_>>()
        .join(", ");

    let state = AppState::new(AppConfig {
        targets,
        default_target: default_target.clone(),
        password,
        key,
        session_ttl: args.session_ttl,
        secure_cookie: args.secure_cookie,
        limits: RelayLimits {
            setup_timeout: Duration::from_secs(args.relay_timeout),
            idle_timeout: Duration::from_secs(args.relay_idle_timeout),
            max_relays: args.max_relays,
            // 0 means "no idle limit": a tail on a quiet stream is legitimately
            // silent, and the client reconnects when the stream does end.
            tail_idle_timeout: (args.tail_idle_timeout > 0)
                .then(|| Duration::from_secs(args.tail_idle_timeout)),
            max_tails: args.max_tails,
        },
    });
    let app = router(state);

    let listener = tokio::net::TcpListener::bind(&args.listen)
        .await
        .with_context(|| format!("binding {}", args.listen))?;
    info!(
        listen = %args.listen,
        targets = %targets_desc,
        default = %default_target,
        session_ttl = args.session_ttl,
        relay_setup_timeout_secs = args.relay_timeout,
        relay_idle_timeout_secs = args.relay_idle_timeout,
        max_relays = args.max_relays,
        tail_idle_timeout_secs = args.tail_idle_timeout,
        max_tails = args.max_tails,
        "scry-webui ready"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(scry_server::shutdown::wait(scry_server::shutdown::channel()))
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
///
/// Deliberately **not** `Serialize`: the browser gets a hand-built
/// `query::TargetInfo` carrying only `id`/`label`/`live`, so no future field
/// added here can leak an address by accident.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Target {
    /// Stable identifier the browser sends back (header `X-Scry-Target`).
    pub id: String,
    /// Human-friendly label for the UI dropdown.
    pub label: String,
    /// The `host:port` the relay actually dials for queries.
    pub addr: String,
    /// The `host:port` of this target's queryd `--tail-listen` port, when the
    /// operator configured one. `None` ⇒ this target cannot serve live tails.
    pub tail_addr: Option<String>,
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
                tail_addr: None,
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
                    tail_addr: None,
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
                    tail_addr: None,
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

/// Attach `--queryd-tail` addresses to already-parsed targets.
///
/// Each value is `id=addr`, naming a target from `--queryd`; a bare `addr` is
/// accepted only when there is exactly one target. An id that names no target
/// is a startup error rather than a silently-ignored flag — a typo'd id would
/// otherwise present as "live tailing just doesn't work" with nothing in the
/// logs to say why.
pub fn attach_tail_targets(targets: &mut [Target], raw: &[String]) -> Result<()> {
    for entry in raw {
        let entry = entry.trim();
        let (id, addr) = match entry.split_once('=') {
            Some((id, addr)) => (id.trim().to_string(), addr.trim()),
            None => {
                if targets.len() != 1 {
                    bail!(
                        "invalid --queryd-tail '{entry}': name the target as 'id=host:port' \
                         when more than one --queryd is configured"
                    );
                }
                (targets[0].id.clone(), entry)
            }
        };
        if id.is_empty() || addr.is_empty() {
            bail!("invalid --queryd-tail '{entry}': expected 'id=host:port'");
        }
        let Some(target) = targets.iter_mut().find(|t| t.id == id) else {
            bail!("--queryd-tail '{entry}' names unknown target id '{id}' (declare it with --queryd first)");
        };
        if target.tail_addr.is_some() {
            bail!("duplicate --queryd-tail for target id '{id}'");
        }
        target.tail_addr = Some(addr.to_string());
    }
    Ok(())
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
    /// Deadline for connecting and writing the request to queryd. Shared by the
    /// query and tail relays — both fail before any response header is sent.
    relay_timeout: Duration,
    /// Maximum silence between response chunks from queryd on a query relay.
    relay_idle_timeout: Duration,
    /// Maximum silence on a live-tail relay. `None` disables the timeout.
    tail_idle_timeout: Option<Duration>,
    /// Process-wide query-relay admission. A body stream owns its permit until
    /// EOF or client cancellation drops the body.
    relay_permits: Arc<Semaphore>,
    /// Live-tail admission, kept separate so long-lived tails cannot starve
    /// queries out of the pool.
    tail_permits: Arc<Semaphore>,
}

/// Admission and timeout limits for the two relay paths.
#[derive(Clone, Copy, Debug)]
pub struct RelayLimits {
    /// Connect + write deadline (both paths).
    pub setup_timeout: Duration,
    /// Maximum silence between chunks on a query relay.
    pub idle_timeout: Duration,
    /// Concurrent query relays.
    pub max_relays: usize,
    /// Maximum silence between chunks on a tail relay. `None` = no limit.
    pub tail_idle_timeout: Option<Duration>,
    /// Concurrent tail relays.
    pub max_tails: usize,
}

impl Default for RelayLimits {
    fn default() -> Self {
        Self {
            setup_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(30),
            max_relays: 32,
            tail_idle_timeout: Some(Duration::from_secs(900)),
            max_tails: 8,
        }
    }
}

/// Everything `AppState` needs at construction. A struct rather than a dozen
/// positional arguments, so adding a knob can't silently transpose two of them.
pub struct AppConfig {
    pub targets: Vec<Target>,
    pub default_target: String,
    pub password: String,
    pub key: Key,
    pub session_ttl: i64,
    pub secure_cookie: bool,
    pub limits: RelayLimits,
}

impl AppState {
    pub fn new(cfg: AppConfig) -> Self {
        Self(Arc::new(Inner {
            targets: cfg.targets,
            default_target: cfg.default_target,
            password: cfg.password,
            key: cfg.key,
            session_ttl: cfg.session_ttl,
            secure_cookie: cfg.secure_cookie,
            relay_timeout: cfg.limits.setup_timeout,
            relay_idle_timeout: cfg.limits.idle_timeout,
            tail_idle_timeout: cfg.limits.tail_idle_timeout,
            relay_permits: Arc::new(Semaphore::new(cfg.limits.max_relays.max(1))),
            tail_permits: Arc::new(Semaphore::new(cfg.limits.max_tails.max(1))),
        }))
    }

    pub fn targets(&self) -> &[Target] {
        &self.0.targets
    }

    pub fn default_target(&self) -> &str {
        &self.0.default_target
    }

    /// Look up a browser-supplied target id in the allowlist. `None`/empty
    /// selects the default; an unknown id returns `None` (caller → 400).
    pub fn find_target(&self, id: Option<&str>) -> Option<&Target> {
        let id = match id {
            Some(s) if !s.is_empty() => s,
            _ => self.default_target(),
        };
        self.0.targets.iter().find(|t| t.id == id)
    }

    /// Resolve a browser-supplied target id to its query address.
    pub fn resolve_target(&self, id: Option<&str>) -> Option<&str> {
        self.find_target(id).map(|t| t.addr.as_str())
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

    pub fn relay_timeout(&self) -> Duration {
        self.0.relay_timeout
    }

    pub fn relay_idle_timeout(&self) -> Duration {
        self.0.relay_idle_timeout
    }

    pub fn tail_idle_timeout(&self) -> Option<Duration> {
        self.0.tail_idle_timeout
    }

    pub fn relay_permits(&self) -> &Arc<Semaphore> {
        &self.0.relay_permits
    }

    pub fn tail_permits(&self) -> &Arc<Semaphore> {
        &self.0.tail_permits
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
        .route("/api/tail", post(query::tail))
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
        let state = AppState::new(AppConfig {
            targets,
            default_target: default,
            password: "pw".into(),
            key: Key::from(&[7u8; 64]),
            session_ttl: 60,
            secure_cookie: false,
            limits: RelayLimits::default(),
        });
        assert_eq!(state.resolve_target(Some("gothab")), Some("127.0.0.1:4100"));
        assert_eq!(state.resolve_target(Some("local")), Some("127.0.0.1:4101"));
        // Absent / empty → default (local).
        assert_eq!(state.resolve_target(None), Some("127.0.0.1:4101"));
        assert_eq!(state.resolve_target(Some("")), Some("127.0.0.1:4101"));
        // Unknown → None.
        assert_eq!(state.resolve_target(Some("nope")), None);
    }

    #[test]
    fn tail_addresses_attach_by_id() {
        let (mut targets, _) = parse_targets(&[
            "local=127.0.0.1:4101".into(),
            "gothab=127.0.0.1:4100".into(),
        ])
        .unwrap();
        attach_tail_targets(&mut targets, &["gothab=127.0.0.1:4200".into()]).unwrap();
        assert_eq!(
            targets[0].tail_addr, None,
            "untouched target stays live-less"
        );
        assert_eq!(targets[1].tail_addr.as_deref(), Some("127.0.0.1:4200"));
    }

    #[test]
    fn a_bare_tail_address_needs_exactly_one_target() {
        let (mut one, _) = parse_targets(&["127.0.0.1:4100".into()]).unwrap();
        attach_tail_targets(&mut one, &["127.0.0.1:4200".into()]).unwrap();
        assert_eq!(one[0].tail_addr.as_deref(), Some("127.0.0.1:4200"));

        let (mut two, _) =
            parse_targets(&["a=127.0.0.1:1".into(), "b=127.0.0.1:2".into()]).unwrap();
        assert!(attach_tail_targets(&mut two, &["127.0.0.1:4200".into()]).is_err());
    }

    /// A typo'd id must be loud at startup. Silently ignoring it would present
    /// as "live tailing just doesn't work" with nothing to explain why.
    #[test]
    fn tail_address_for_an_unknown_target_is_an_error() {
        let (mut targets, _) = parse_targets(&["local=127.0.0.1:4101".into()]).unwrap();
        let err = attach_tail_targets(&mut targets, &["locl=127.0.0.1:4200".into()])
            .expect_err("unknown id must fail")
            .to_string();
        assert!(err.contains("unknown target id"), "{err}");

        assert!(attach_tail_targets(&mut targets, &["local=".into()]).is_err());
        assert!(attach_tail_targets(&mut targets, &["=127.0.0.1:1".into()]).is_err());
    }

    #[test]
    fn a_target_cannot_take_two_tail_addresses() {
        let (mut targets, _) = parse_targets(&["local=127.0.0.1:4101".into()]).unwrap();
        assert!(attach_tail_targets(
            &mut targets,
            &["local=127.0.0.1:4200".into(), "local=127.0.0.1:4201".into()],
        )
        .is_err());
    }
}
