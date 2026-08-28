//! Transport abstraction for the query protocol.
//!
//! Two shapes of traffic share one interface:
//!
//!   - `query` — send one framed request, get the full ordered response byte
//!     stream. The daemon's "one connection per query" lifecycle makes this a
//!     clean request/response.
//!   - `tail` — send one framed subscription, then receive frames until the
//!     caller aborts or the server hangs up. Same bytes-in/bytes-out contract,
//!     but delivered incrementally, because a live tail has no end.
//!
//! Keeping both behind an interface means the protocol logic is
//! transport-agnostic. Two implementations live alongside it, each in its own
//! module so the browser bundle never statically imports the Tauri API:
//!   - `transport-tauri.ts` — native TCP sockets via Rust commands (desktop).
//!   - `transport-http.ts` — `fetch` to the `scry-webui` server's `/api/query`
//!     and `/api/tail` relays (browser).
//!
//! `store.ts` picks one at runtime via `getTransport()` (see `env.ts`).

/** Called once per complete frame body received on a tail stream. */
export type FrameHandler = (body: Uint8Array) => void;

/**
 * The selected target exists but has no live-tail endpoint configured, so no
 * subscription is possible. Distinct from a transport failure: nothing is
 * wrong, this deployment simply doesn't offer tailing here.
 */
export class LiveUnavailableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "LiveUnavailableError";
  }
}

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

  /**
   * Send the already-framed subscription `request` to `addr`'s **live-tail**
   * endpoint and invoke `onFrame` for every frame the server pushes back,
   * until `signal` aborts or the server closes the stream.
   *
   * Resolves when the stream ends normally (including on abort). Rejects on a
   * transport failure, or with `LiveUnavailableError` when this target has no
   * live endpoint. `addr` is interpreted exactly as in `query` — a raw
   * `host:port` under Tauri (the daemon's `--tail-listen` port), a target id in
   * the browser (the server holds the matching `--queryd-tail` address).
   */
  tail(
    addr: string,
    request: Uint8Array,
    onFrame: FrameHandler,
    signal: AbortSignal,
  ): Promise<void>;
}
