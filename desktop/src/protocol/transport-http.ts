//! Browser transport: POST the framed request to the `scry-webui` server's
//! `/api/query` relay, which byte-pipes it to the selected upstream `scry-queryd`.
//!
//! The browser never supplies a raw address — that would be an SSRF vector.
//! Instead `addr` here is a target **id** from `/api/targets`, sent in the
//! `X-Scry-Target` header; the server resolves it against its own `--queryd`
//! allowlist and dials the matching address. An empty `addr` lets the server
//! pick its default target. The session cookie rides along automatically with
//! `credentials: "same-origin"`.

import { FrameStream } from "./framing";
import { LiveUnavailableError, type FrameHandler, type Transport } from "./transport";

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

  async tail(
    addr: string,
    request: Uint8Array,
    onFrame: FrameHandler,
    signal: AbortSignal,
  ): Promise<void> {
    const headers: Record<string, string> = {
      "content-type": "application/octet-stream",
    };
    const target = addr.trim();
    if (target !== "") headers["x-scry-target"] = target;

    let res: Response;
    try {
      res = await fetch("/api/tail", {
        method: "POST",
        headers,
        body: request as BodyInit,
        credentials: "same-origin",
        signal,
      });
    } catch (e) {
      // An abort during setup is an ordinary stop, not a failure.
      if (signal.aborted) return;
      throw e;
    }
    if (res.status === 401) throw new UnauthorizedError();
    if (res.status === 409) {
      throw new LiveUnavailableError(
        "this target has no live-tail endpoint configured (scry web needs --queryd-tail)",
      );
    }
    if (!res.ok) {
      throw new Error(`tail relay failed: HTTP ${res.status}`);
    }
    if (!res.body) {
      throw new Error("tail relay returned no response stream");
    }

    // Read the relay's chunks as they arrive. This is the whole reason the
    // tail path exists separately from `query`, which buffers to completion.
    const reader = res.body.getReader();
    const frames = new FrameStream();
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        if (!value) continue;
        for (const body of frames.push(value)) onFrame(body);
      }
    } catch (e) {
      if (signal.aborted) return;
      throw e;
    } finally {
      reader.releaseLock();
    }
  }
}
