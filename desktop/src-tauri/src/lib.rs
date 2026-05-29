//! Tauri shell for the scry desktop query app.
//!
//! This crate is deliberately a **dumb byte pipe**. The entire query
//! wire protocol — binschema framing, `QueryFrame` encode/decode, and
//! Arrow IPC decoding — lives in TypeScript (`src/protocol/*`,
//! `src/proto/*`). The only thing a browser can't do is open a raw TCP
//! socket, so that is the one and only job of this Rust side:
//!
//!   1. connect to the `scry-queryd` address the UI supplies,
//!   2. write the already-framed request bytes the TS client produced,
//!   3. read every response byte until the daemon closes the socket
//!      (one TCP connection per query — the daemon streams
//!      SchemaMsg → BatchMsg* → EndOfStream/StreamError then drops the
//!      connection), and
//!   4. hand the raw bytes back to TS, which de-frames and decodes them.
//!
//! Keeping protocol logic out of Rust is intentional: it makes the
//! TypeScript binding the single source of query-protocol truth on the
//! client, and means this shell never needs touching when the wire
//! schema evolves — only `scripts/gen-proto-ts.sh` re-runs.

use tauri::ipc::Response;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Open a TCP connection to `addr`, send the framed query request, and
/// return the full response byte stream.
///
/// `request` is the complete `[len:u32 BE][QueryFrame]` framing the TS
/// client built — we write it verbatim. The returned bytes are the
/// concatenation of every response frame (each itself length-prefixed);
/// TS splits and decodes them.
///
/// Returns the bytes as a [`tauri::ipc::Response`], which the IPC layer
/// delivers to JavaScript as an `ArrayBuffer` (no JSON number-array
/// round-trip — important for multi-MB Arrow payloads).
#[tauri::command]
async fn run_query(addr: String, request: Vec<u8>) -> Result<Response, String> {
    let mut stream = TcpStream::connect(&addr)
        .await
        .map_err(|e| format!("connect to {addr}: {e}"))?;
    stream.set_nodelay(true).ok();

    stream
        .write_all(&request)
        .await
        .map_err(|e| format!("write request: {e}"))?;
    stream
        .flush()
        .await
        .map_err(|e| format!("flush request: {e}"))?;

    // One connection per query: the daemon writes all response frames
    // then closes its write half, so reading to EOF yields the complete
    // response. The 32 MiB-per-frame ceiling is enforced TS-side when
    // de-framing; here we just accumulate.
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .await
        .map_err(|e| format!("read response: {e}"))?;

    Ok(Response::new(buf))
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![run_query])
        .run(tauri::generate_context!())
        .expect("error while running scry desktop application");
}
