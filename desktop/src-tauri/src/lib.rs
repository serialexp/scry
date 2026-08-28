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
//!
//! The live-tail commands ([`tail_start`] / [`tail_stop`]) are the same pipe
//! with the read half turned inside out: a tail has no end, so instead of
//! reading to EOF and returning, they push each chunk over a `Channel` as it
//! arrives. Still zero protocol knowledge — chunk boundaries are not frame
//! boundaries, and TS reassembles them.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use tauri::ipc::{Channel, InvokeResponseBody, Response};
use tauri::State;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Read buffer for a tail stream. Records are small; this only bounds how much
/// one `read` can hand over at once.
const TAIL_CHUNK_BYTES: usize = 64 * 1024;

/// Live tails in flight, so `tail_stop` can cancel one. Aborting the task drops
/// its socket, which the server sees as EOF and deregisters.
#[derive(Default)]
struct TailRegistry {
    next_id: AtomicU32,
    tasks: Mutex<HashMap<u32, tokio::task::JoinHandle<()>>>,
}

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

/// Open a TCP connection to `addr`'s live-tail port, write the framed
/// subscription, and stream every byte the server pushes back over `on_frame`
/// until the connection ends or [`tail_stop`] cancels it.
///
/// Returns a subscription id for `tail_stop`. Setup failures (connect/write)
/// are reported synchronously as an `Err`, so the UI can show why a
/// subscription never started; a failure *after* streaming begins ends the
/// stream, and the UI reconnects.
///
/// Each `Channel` message is an `ArrayBuffer` of whatever one socket read
/// produced — **not** a frame. A zero-length message is the end-of-stream
/// marker (a real read of length 0 means EOF, so it is never forwarded as
/// data, leaving the empty buffer free to mean exactly this).
#[tauri::command]
async fn tail_start(
    addr: String,
    request: Vec<u8>,
    on_frame: Channel<InvokeResponseBody>,
    registry: State<'_, Arc<TailRegistry>>,
) -> Result<u32, String> {
    let mut stream = TcpStream::connect(&addr)
        .await
        .map_err(|e| format!("connect to {addr}: {e}"))?;
    stream.set_nodelay(true).ok();
    stream
        .write_all(&request)
        .await
        .map_err(|e| format!("write subscribe: {e}"))?;
    stream
        .flush()
        .await
        .map_err(|e| format!("flush subscribe: {e}"))?;

    let registry = registry.inner().clone();
    let id = registry.next_id.fetch_add(1, Ordering::Relaxed);
    let bookkeeping = registry.clone();
    let handle = tokio::spawn(async move {
        let mut buf = vec![0u8; TAIL_CHUNK_BYTES];
        loop {
            match stream.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    // A send failure means the webview dropped the channel;
                    // there is no one left to receive, so stop reading.
                    if on_frame
                        .send(InvokeResponseBody::Raw(buf[..n].to_vec()))
                        .is_err()
                    {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        let _ = on_frame.send(InvokeResponseBody::Raw(Vec::new()));
        // Self-deregister so a long-running app doesn't accumulate finished
        // handles; `tail_stop` on an already-finished id is then a no-op.
        bookkeeping.tasks.lock().unwrap().remove(&id);
    });
    registry.tasks.lock().unwrap().insert(id, handle);
    Ok(id)
}

/// Cancel a live tail started by [`tail_start`]. Unknown//already-finished ids
/// are a no-op — the UI may stop a subscription that just ended on its own.
#[tauri::command]
fn tail_stop(id: u32, registry: State<'_, Arc<TailRegistry>>) {
    let handle = registry.tasks.lock().unwrap().remove(&id);
    if let Some(handle) = handle {
        handle.abort();
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .manage(Arc::new(TailRegistry::default()))
        .invoke_handler(tauri::generate_handler![run_query, tail_start, tail_stop])
        .run(tauri::generate_context!())
        .expect("error while running scry desktop application");
}
