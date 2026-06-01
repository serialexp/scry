//! Browser transport: POST the framed request to the `scry-webui` server's
//! `/api/query` relay, which byte-pipes it to the upstream `scry-queryd`.
//!
//! The server dials its own configured `scry-queryd`, so the `addr` argument is
//! intentionally ignored here (the browser cannot — and must not — choose the
//! upstream; that would be an SSRF vector). The session cookie rides along
//! automatically with `credentials: "same-origin"`.

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
  async query(_addr: string, request: Uint8Array): Promise<Uint8Array> {
    const res = await fetch("/api/query", {
      method: "POST",
      headers: { "content-type": "application/octet-stream" },
      // Send exactly the framed bytes (respecting byteOffset/byteLength).
      body: request,
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
