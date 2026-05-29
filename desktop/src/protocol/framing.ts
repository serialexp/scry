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
