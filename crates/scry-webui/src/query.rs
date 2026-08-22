//! `POST /api/query` — the dumb streaming byte-pipe to `scry-queryd` +
//! `GET /api/targets`.
//!
//! The query protocol remains entirely in the TypeScript client. This handler
//! connects and writes the already-framed request, then exposes queryd's TCP read
//! half as a backpressured HTTP body. It never accumulates a complete response.

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

    let setup_timeout = state.relay_timeout();
    let idle_timeout = state.relay_idle_timeout();
    let setup = tokio::time::timeout(setup_timeout, connect_and_write(&addr, &body)).await;
    let stream = match setup {
        Ok(Ok(stream)) => stream,
        Ok(Err(e)) => {
            warn!(queryd = %addr, error = %e, "query relay setup failed");
            return Err(StatusCode::BAD_GATEWAY);
        }
        Err(_) => {
            warn!(queryd = %addr, timeout_secs = setup_timeout.as_secs(), "query relay setup timed out");
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

fn relay_stream(
    mut upstream: TcpStream,
    idle_timeout: std::time::Duration,
    permit: OwnedSemaphorePermit,
    addr: String,
) -> impl futures::Stream<Item = std::io::Result<Bytes>> + Send + 'static {
    async_stream::try_stream! {
        // Moving the permit into this generator ties admission exactly to body
        // lifetime. Dropping the HTTP body drops both permit and upstream socket.
        let _permit = permit;
        let mut buf = vec![0u8; RELAY_CHUNK_BYTES];
        loop {
            let read = tokio::time::timeout(idle_timeout, upstream.read(&mut buf)).await;
            match read {
                Ok(Ok(0)) => break,
                Ok(Ok(n)) => yield Bytes::copy_from_slice(&buf[..n]),
                Ok(Err(e)) => {
                    warn!(queryd = %addr, error = %e, "query relay response read failed");
                    Err(e)?;
                }
                Err(_) => {
                    warn!(queryd = %addr, timeout_secs = idle_timeout.as_secs(), "query relay response idle timeout");
                    Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "queryd response idle timeout",
                    ))?;
                }
            }
        }
    }
}
