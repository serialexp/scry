//! `POST /api/query` — the dumb byte-pipe to `scry-queryd` + `GET /api/targets`.
//!
//! The whole query wire protocol (binschema framing, the `QueryFrame` union,
//! Arrow IPC decoding) lives in the TypeScript frontend. This handler has *zero*
//! protocol knowledge: it dials one of the server's configured upstream
//! `scry-queryd` targets, writes the already-framed request bytes, reads the
//! response to EOF, and hands the raw bytes back — exactly what the Tauri
//! `run_query` command does.
//!
//! Which upstream is dialed comes from the `X-Scry-Target` header — but only as
//! a target **id** the server resolves against its `--queryd` allowlist; a
//! client can never supply a raw address, so the relay stays SSRF-safe.
//! `scry-queryd` answers one request per connection and closes its write half,
//! which is what lets `read_to_end` terminate.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use axum_extra::extract::cookie::SignedCookieJar;
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::warn;

use crate::auth::session_valid;
use crate::{AppState, Target};

/// Header carrying the selected target **id** (from `/api/targets`). Never a
/// raw address — the server resolves it against the allowlist.
const TARGET_HEADER: &str = "x-scry-target";

/// `GET /api/targets` response: the selectable upstreams + the default id.
/// `Target`'s `addr` is `#[serde(skip)]`, so only `id` + `label` reach the
/// browser.
#[derive(Serialize)]
pub struct TargetsResponse {
    targets: Vec<Target>,
    default: String,
}

/// `GET /api/targets` — list the configured query targets. Auth-gated so the
/// names don't leak before login.
pub async fn targets(
    State(state): State<AppState>,
    jar: SignedCookieJar,
) -> Result<Json<TargetsResponse>, StatusCode> {
    if !session_valid(&jar) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(Json(TargetsResponse {
        targets: state.targets().to_vec(),
        default: state.default_target().to_string(),
    }))
}

/// Relay a framed query to the selected upstream daemon. 401 if unauthenticated,
/// 400 if the requested target id is unknown, 502 if the upstream can't be
/// reached or errors mid-exchange.
pub async fn query(
    State(state): State<AppState>,
    jar: SignedCookieJar,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Vec<u8>, StatusCode> {
    if !session_valid(&jar) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let requested = headers.get(TARGET_HEADER).and_then(|v| v.to_str().ok());
    let Some(addr) = state.resolve_target(requested) else {
        warn!(target = ?requested, "query for unknown target id");
        return Err(StatusCode::BAD_REQUEST);
    };
    // Own the addr before the await: `state` is borrowed and `relay` is async.
    let addr = addr.to_string();
    match relay(&addr, &body).await {
        Ok(resp) => Ok(resp),
        Err(e) => {
            warn!(queryd = %addr, error = %e, "query relay to scry-queryd failed");
            Err(StatusCode::BAD_GATEWAY)
        }
    }
}

/// Open a connection to `addr`, write the whole request, read the whole
/// response. Mirrors the desktop shell's `run_query` byte-pipe one-for-one.
async fn relay(addr: &str, request: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true).ok();
    stream.write_all(request).await?;
    stream.flush().await?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await?;
    Ok(buf)
}
