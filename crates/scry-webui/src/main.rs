//! `scry-webui` binary — serve the scry query UI to a browser and relay queries
//! to `scry-queryd`. See the crate-level docs in `lib.rs`.
//!
//! Run (after building the frontend with `bun run build` in `desktop/`):
//!
//! ```bash
//! SCRY_WEBUI_PASSWORD=secret \
//!   scry-webui --listen 127.0.0.1:8080 --queryd 127.0.0.1:4100
//! ```

use anyhow::{bail, Context, Result};
use axum_extra::extract::cookie::Key;
use clap::Parser;
use scry_webui::AppState;
use tracing::info;

/// Swap glibc's malloc for mimalloc, consistent with the other scry daemons.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Env var carrying the shared login password (kept out of argv).
const PASSWORD_ENV: &str = "SCRY_WEBUI_PASSWORD";

#[derive(Parser, Debug)]
#[command(
    name = "scry-webui",
    version,
    about = "Browser query UI for scry-queryd"
)]
struct Args {
    /// HTTP listen address.
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: String,

    /// Upstream `scry-queryd` address. The byte-pipe dials this; any
    /// browser-supplied address is ignored (SSRF-safe).
    #[arg(long, default_value = "127.0.0.1:4100")]
    queryd: String,

    /// Session lifetime in seconds (default 1 day).
    #[arg(long, default_value_t = 86_400)]
    session_ttl: i64,

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
    secure_cookie: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let password =
        std::env::var(PASSWORD_ENV).map_err(|_| anyhow::anyhow!("{PASSWORD_ENV} must be set"))?;
    if password.is_empty() {
        bail!("{PASSWORD_ENV} must not be empty");
    }

    // Derive a stable cookie-signing key from the password: sessions survive a
    // restart, and rotating the password naturally invalidates old sessions.
    let key = derive_key(&password);

    let state = AppState::new(
        args.queryd.clone(),
        password,
        key,
        args.session_ttl,
        args.secure_cookie,
    );
    let app = scry_webui::router(state);

    let listener = tokio::net::TcpListener::bind(&args.listen)
        .await
        .with_context(|| format!("binding {}", args.listen))?;
    info!(
        listen = %args.listen,
        queryd = %args.queryd,
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
