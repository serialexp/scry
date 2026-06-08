//! Transport abstraction for the query protocol.
//!
//! The protocol client only needs a way to send one framed request and
//! receive the full ordered response byte stream — the daemon's "one
//! connection per query" lifecycle makes that a clean request/response
//! shape. Keeping it behind an interface means the protocol logic is
//! transport-agnostic.
//!
//! Two implementations live alongside this interface, each in its own module so
//! the browser bundle never statically imports the Tauri API:
//!   - `transport-tauri.ts` — `TauriTransport`, a native TCP socket via the
//!     Rust `run_query` command (desktop app).
//!   - `transport-http.ts` — `HttpTransport`, a `fetch` to the `scry-webui`
//!     server's `/api/query` relay (browser).
//!
//! `store.ts` picks one at runtime via `getTransport()` (see `env.ts`).

export interface Transport {
  /**
   * Send the already-framed `request` to `addr` and resolve with the
   * complete response byte stream. Rejects on connection/IO failure;
   * protocol-level `StreamError`s arrive *inside* the returned bytes and
   * are surfaced by the client, not here.
   *
   * Note: `addr` means different things per transport. The desktop (Tauri)
   * transport dials it as a raw `host:port`. The HTTP transport treats it as a
   * target **id** sent in `X-Scry-Target`; the `scry-webui` server resolves the
   * id against its own `--queryd` allowlist (SSRF-safe — the browser never
   * supplies a raw address). Empty ⇒ the server's default target.
   */
  query(addr: string, request: Uint8Array): Promise<Uint8Array>;
}
