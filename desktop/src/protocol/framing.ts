//! Length-prefixed framing — the TS mirror of `crates/proto/src/framing.rs`.
//!
//! Every wire message is `[len: u32 big-endian][body bytes]`. `len`
//! covers the body only; the prefix is not included.

import { MAX_FRAME_BYTES } from "./constants";

/** Prepend the big-endian u32 length prefix to a frame body. */
export function frame(body: Uint8Array): Uint8Array {
  if (body.length > MAX_FRAME_BYTES) {
    throw new Error(`frame too large: ${body.length} bytes, max ${MAX_FRAME_BYTES}`);
  }
  const out = new Uint8Array(4 + body.length);
  new DataView(out.buffer).setUint32(0, body.length, false); // big-endian
  out.set(body, 4);
  return out;
}

/**
 * Split a *complete* response buffer into frame bodies.
 *
 * The daemon streams every response frame then closes the socket, so by
 * the time the Tauri transport returns we hold the entire response and
 * can split it in one pass — no partial-frame reassembly needed. We
 * still validate lengths so a corrupt stream fails loudly rather than
 * silently truncating.
 */
export function deframe(buf: Uint8Array): Uint8Array[] {
  const frames: Uint8Array[] = [];
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  let off = 0;
  while (off + 4 <= buf.length) {
    const len = dv.getUint32(off, false); // big-endian
    off += 4;
    if (len > MAX_FRAME_BYTES) {
      throw new Error(`frame too large: ${len} bytes, max ${MAX_FRAME_BYTES}`);
    }
    if (off + len > buf.length) {
      throw new Error(`truncated frame: need ${len} bytes, have ${buf.length - off}`);
    }
    frames.push(buf.subarray(off, off + len));
    off += len;
  }
  if (off !== buf.length) {
    throw new Error(`trailing ${buf.length - off} bytes after final frame`);
  }
  return frames;
}

/**
 * Incremental de-framer for a *push* stream.
 *
 * `deframe` above works because a query response is complete by the time we
 * see it. A live tail never completes: frames arrive over minutes and a network
 * chunk can split anywhere — mid-body, or even between the four bytes of a
 * length prefix. This buffers the remainder and yields whole frames only.
 *
 * Returned frames are views into the accumulated buffer, so decode them before
 * the next `push` (which is what every caller does).
 */
export class FrameStream {
  #buf: Uint8Array = new Uint8Array(0);

  /** Feed one network chunk; returns every frame body it completed. */
  push(chunk: Uint8Array): Uint8Array[] {
    if (chunk.length === 0) return [];
    if (this.#buf.length === 0) {
      this.#buf = chunk;
    } else {
      const joined = new Uint8Array(this.#buf.length + chunk.length);
      joined.set(this.#buf, 0);
      joined.set(chunk, this.#buf.length);
      this.#buf = joined;
    }

    const frames: Uint8Array[] = [];
    let off = 0;
    for (;;) {
      if (this.#buf.length - off < 4) break;
      const dv = new DataView(this.#buf.buffer, this.#buf.byteOffset + off, 4);
      const len = dv.getUint32(0, false); // big-endian
      if (len > MAX_FRAME_BYTES) {
        throw new Error(`frame too large: ${len} bytes, max ${MAX_FRAME_BYTES}`);
      }
      if (this.#buf.length - off - 4 < len) break;
      frames.push(this.#buf.subarray(off + 4, off + 4 + len));
      off += 4 + len;
    }
    if (off > 0) this.#buf = this.#buf.subarray(off);
    return frames;
  }

  /** Bytes held back awaiting the rest of their frame. Non-zero at stream end
   *  means the peer was cut off mid-frame. */
  get pendingBytes(): number {
    return this.#buf.length;
  }
}
