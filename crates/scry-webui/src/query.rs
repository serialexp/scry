//! `POST /api/query` and `POST /api/tail` — the dumb streaming byte-pipes to
//! `scry-queryd` + `GET /api/targets`.
//!
//! The query protocol remains entirely in the TypeScript client. This handler
//! connects and writes the already-framed request, then exposes queryd's TCP read
//! half as a backpressured HTTP body. It never accumulates a complete response.
//!
//! `/api/tail` is the same pipe pointed at the target's `--tail-listen` port
//! (queryd's live-tail front-door, D-053). The tail sub-protocol's handshake
//! looks interactive — Hello → HelloAck → Subscribe → records — but the client
//! **pipelines** Hello and Subscribe into the one request body, because both
//! the relay and the ingester read frames sequentially off a buffered reader
//! and neither cares that the second frame arrived early. That keeps this
//! server protocol-free: it still just writes bytes and streams bytes back.
//! It differs from `/api/query` only in its limits — its own admission
//! semaphore and a much longer (or absent) idle timeout, because a tail on a
//! quiet stream is legitimately silent for minutes.

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::Response;
use axum::Json;
use axum_extra::extract::cookie::SignedCookieJar;
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::OwnedSemaphorePermit;
use tracing::warn;

use crate::auth::session_valid;
use crate::{AppState, Target};

/// Header carrying the selected target **id** (from `/api/targets`). Never a
/// raw address — the server resolves it against the allowlist.
const TARGET_HEADER: &str = "x-scry-target";
const RELAY_CHUNK_BYTES: usize = 64 * 1024;

/// One target as the browser sees it. Built by hand from [`Target`] rather
/// than serializing it, so an address can never leak by someone adding a field
/// to the internal type.
#[derive(Serialize)]
pub struct TargetInfo {
    id: String,
    label: String,
    /// Whether this target has a configured `--queryd-tail` address, i.e.
    /// whether `/api/tail` will work for it. The UI disables its Live toggle
    /// when false instead of failing at subscribe time.
    live: bool,
}

impl From<&Target> for TargetInfo {
    fn from(t: &Target) -> Self {
        Self {
            id: t.id.clone(),
            label: t.label.clone(),
            live: t.tail_addr.is_some(),
        }
    }
}

/// `GET /api/targets` response: the selectable upstreams + the default id.
#[derive(Serialize)]
pub struct TargetsResponse {
    targets: Vec<TargetInfo>,
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
        targets: state.targets().iter().map(TargetInfo::from).collect(),
        default: state.default_target().to_string(),
    }))
}

/// Relay a framed query to the selected upstream daemon.
///
/// Setup failures are representable as HTTP statuses because they happen before
/// response headers are returned. Once streaming starts, an idle/read failure is
/// an HTTP body error; we never return a valid-looking truncated protocol stream.
pub async fn query(
    State(state): State<AppState>,
    jar: SignedCookieJar,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, StatusCode> {
    if !session_valid(&jar) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let requested = headers.get(TARGET_HEADER).and_then(|v| v.to_str().ok());
    let Some(addr) = state.resolve_target(requested) else {
        warn!(target = ?requested, "query for unknown target id");
        return Err(StatusCode::BAD_REQUEST);
    };
    let addr = addr.to_string();

    // Never queue HTTP requests behind active relays: queued request bodies and
    // sockets are another unbounded working set. The body stream owns the permit
    // until EOF, error, or browser cancellation drops it.
    let permit = state
        .relay_permits()
        .clone()
        .try_acquire_owned()
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    relay(
        addr,
        body,
        permit,
        state.relay_timeout(),
        Some(state.relay_idle_timeout()),
    )
    .await
}

/// Relay a live-tail subscription to the selected target's `--tail-listen`
/// address.
///
/// Byte-identical in mechanism to [`query`]; the differences are which address
/// is dialed, which admission pool is drawn from, and that the idle timeout is
/// long or absent. A target with no `--queryd-tail` configured answers **409**
/// — distinguishable from 400 (no such target) so the UI can say "this target
/// has no live endpoint" rather than "bad request".
pub async fn tail(
    State(state): State<AppState>,
    jar: SignedCookieJar,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, StatusCode> {
    if !session_valid(&jar) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let requested = headers.get(TARGET_HEADER).and_then(|v| v.to_str().ok());
    let Some(target) = state.find_target(requested) else {
        warn!(target = ?requested, "tail for unknown target id");
        return Err(StatusCode::BAD_REQUEST);
    };
    let Some(addr) = target.tail_addr.clone() else {
        warn!(target = %target.id, "tail for a target with no --queryd-tail address");
        return Err(StatusCode::CONFLICT);
    };

    let permit = state
        .tail_permits()
        .clone()
        .try_acquire_owned()
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    relay(
        addr,
        body,
        permit,
        state.relay_timeout(),
        state.tail_idle_timeout(),
    )
    .await
}

/// Connect, write the client's framed bytes, and hand back the upstream read
/// half as a streaming HTTP body. Shared by both relays.
async fn relay(
    addr: String,
    body: Bytes,
    permit: OwnedSemaphorePermit,
    setup_timeout: std::time::Duration,
    idle_timeout: Option<std::time::Duration>,
) -> Result<Response, StatusCode> {
    let setup = tokio::time::timeout(setup_timeout, connect_and_write(&addr, &body)).await;
    let stream = match setup {
        Ok(Ok(stream)) => stream,
        Ok(Err(e)) => {
            warn!(queryd = %addr, error = %e, "relay setup failed");
            return Err(StatusCode::BAD_GATEWAY);
        }
        Err(_) => {
            warn!(queryd = %addr, timeout_secs = setup_timeout.as_secs(), "relay setup timed out");
            return Err(StatusCode::GATEWAY_TIMEOUT);
        }
    };

    let response_stream = relay_stream(stream, idle_timeout, permit, addr);
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from_stream(response_stream))
        .expect("static relay response is valid"))
}

async fn connect_and_write(addr: &str, request: &[u8]) -> std::io::Result<TcpStream> {
    let mut stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true).ok();
    stream.write_all(request).await?;
    stream.flush().await?;
    Ok(stream)
}

/// Stream the upstream read half as HTTP body chunks. `idle_timeout` of `None`
/// waits indefinitely between chunks — correct for a live tail, where silence
/// means "nothing matched yet", not "stuck".
fn relay_stream(
    mut upstream: TcpStream,
    idle_timeout: Option<std::time::Duration>,
    permit: OwnedSemaphorePermit,
    addr: String,
) -> impl futures::Stream<Item = std::io::Result<Bytes>> + Send + 'static {
    async_stream::try_stream! {
        // Moving the permit into this generator ties admission exactly to body
        // lifetime. Dropping the HTTP body drops both permit and upstream socket.
        let _permit = permit;
        let mut buf = vec![0u8; RELAY_CHUNK_BYTES];
        loop {
            let read = match idle_timeout {
                Some(limit) => tokio::time::timeout(limit, upstream.read(&mut buf)).await,
                None => Ok(upstream.read(&mut buf).await),
            };
            match read {
                Ok(Ok(0)) => break,
                Ok(Ok(n)) => yield Bytes::copy_from_slice(&buf[..n]),
                Ok(Err(e)) => {
                    warn!(queryd = %addr, error = %e, "relay response read failed");
                    Err(e)?;
                }
                Err(_) => {
                    let secs = idle_timeout.map(|d| d.as_secs()).unwrap_or_default();
                    warn!(queryd = %addr, timeout_secs = secs, "relay response idle timeout");
                    Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "queryd response idle timeout",
                    ))?;
                }
            }
        }
    }
}
