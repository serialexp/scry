//! Transport abstraction for the query protocol.
//!
//! The protocol client only needs a way to send one framed request and
//! receive the full ordered response byte stream — the daemon's "one
//! connection per query" lifecycle makes that a clean request/response
//! shape. Keeping it behind an interface means the protocol logic is
//! transport-agnostic: the Tauri adapter below opens a native TCP
//! socket, but a future WebSocket bridge (for a pure-browser build)
//! would implement the same `Transport`.

import { invoke } from "@tauri-apps/api/core";

export interface Transport {
  /**
   * Send the already-framed `request` to `addr` and resolve with the
   * complete response byte stream. Rejects on connection/IO failure;
   * protocol-level `StreamError`s arrive *inside* the returned bytes and
   * are surfaced by the client, not here.
   */
  query(addr: string, request: Uint8Array): Promise<Uint8Array>;
}

/** Transport backed by the Rust `run_query` command (native TCP socket). */
export class TauriTransport implements Transport {
  async query(addr: string, request: Uint8Array): Promise<Uint8Array> {
    // The request frame is small (tens of bytes to a few KB), so passing
    // it as a JSON number array is fine. The *response* comes back as a
    // raw ArrayBuffer (the Rust command returns `tauri::ipc::Response`),
    // avoiding a number-array round-trip for multi-MB Arrow payloads.
    const res = await invoke<ArrayBuffer>("run_query", {
      addr,
      request: Array.from(request),
    });
    return new Uint8Array(res);
  }
}
