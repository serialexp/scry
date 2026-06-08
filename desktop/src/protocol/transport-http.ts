//! Browser transport: POST the framed request to the `scry-webui` server's
//! `/api/query` relay, which byte-pipes it to the selected upstream `scry-queryd`.
//!
//! The browser never supplies a raw address — that would be an SSRF vector.
//! Instead `addr` here is a target **id** from `/api/targets`, sent in the
//! `X-Scry-Target` header; the server resolves it against its own `--queryd`
//! allowlist and dials the matching address. An empty `addr` lets the server
//! pick its default target. The session cookie rides along automatically with
//! `credentials: "same-origin"`.

import type { Transport } from "./transport";

/** Thrown on a 401 so the UI can drop back to the login screen. */
export class UnauthorizedError extends Error {
  constructor() {
    super("session expired — please log in again");
    this.name = "UnauthorizedError";
  }
}

/** Transport backed by the `scry-webui` HTTP relay. */
export class HttpTransport implements Transport {
  async query(addr: string, request: Uint8Array): Promise<Uint8Array> {
    const headers: Record<string, string> = {
      "content-type": "application/octet-stream",
    };
    // `addr` is a target id here, not a raw address — forward it so the server
    // dials the right upstream. Empty ⇒ the server's default target.
    const target = addr.trim();
    if (target !== "") headers["x-scry-target"] = target;
    const res = await fetch("/api/query", {
      method: "POST",
      headers,
      // Send exactly the framed bytes (respecting byteOffset/byteLength). The
      // cast bridges a TS 5.7 lib lag: `Uint8Array` is now generic
      // (`Uint8Array<ArrayBufferLike>`) but DOM's `BodyInit` hasn't adopted the
      // type parameter — a Uint8Array is a valid fetch body at runtime.
      body: request as BodyInit,
      credentials: "same-origin",
    });
    if (res.status === 401) {
      throw new UnauthorizedError();
    }
    if (!res.ok) {
      // 502 == scry-queryd unreachable; anything else is unexpected.
      throw new Error(`query relay failed: HTTP ${res.status}`);
    }
    const buf = await res.arrayBuffer();
    return new Uint8Array(buf);
  }
}
