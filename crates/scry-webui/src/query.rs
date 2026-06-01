//! `POST /api/query` — the dumb byte-pipe to `scry-queryd`.
//!
//! The whole query wire protocol (binschema framing, the `QueryFrame` union,
//! Arrow IPC decoding) lives in the TypeScript frontend. This handler has *zero*
//! protocol knowledge: it dials the server's configured upstream `scry-queryd`,
//! writes the already-framed request bytes, reads the response to EOF, and hands
//! the raw bytes back — exactly what the Tauri `run_query` command does.
//!
//! The upstream address is the server's `--queryd`; any client-supplied address
//! is ignored (SSRF-safe). `scry-queryd` answers one request per connection and
//! closes its write half, which is what lets `read_to_end` terminate.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum_extra::extract::cookie::SignedCookieJar;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::warn;

use crate::auth::session_valid;
use crate::AppState;

/// Relay a framed query to the upstream daemon. 401 if unauthenticated, 502 if
/// the upstream can't be reached or errors mid-exchange.
pub async fn query(
    State(state): State<AppState>,
    jar: SignedCookieJar,
    body: Bytes,
) -> Result<Vec<u8>, StatusCode> {
    if !session_valid(&jar) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    match relay(state.queryd(), &body).await {
        Ok(resp) => Ok(resp),
        Err(e) => {
            warn!(queryd = state.queryd(), error = %e, "query relay to scry-queryd failed");
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
