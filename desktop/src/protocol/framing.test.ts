//! Incremental de-framing. A live tail's chunk boundaries are wherever the
//! network put them, so every split has to be handled — including one that
//! lands inside a length prefix.

import { describe, expect, it } from "vitest";

import { FrameStream, deframe, frame } from "./framing";
import { MAX_FRAME_BYTES } from "./constants";

function body(n: number, fill: number): Uint8Array {
  return new Uint8Array(n).fill(fill);
}

function concat(parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.length;
  }
  return out;
}

describe("FrameStream", () => {
  it("yields whole frames from one chunk", () => {
    const stream = new FrameStream();
    const got = stream.push(concat([frame(body(3, 1)), frame(body(2, 2))]));
    expect(got.map((f) => Array.from(f))).toEqual([
      [1, 1, 1],
      [2, 2],
    ]);
    expect(stream.pendingBytes).toBe(0);
  });

  it("holds a partial frame until the rest arrives", () => {
    const stream = new FrameStream();
    const full = frame(body(8, 9));
    expect(stream.push(full.subarray(0, 6))).toEqual([]);
    expect(stream.pendingBytes).toBe(6);
    const got = stream.push(full.subarray(6));
    expect(got).toHaveLength(1);
    expect(Array.from(got[0]!)).toEqual(Array.from(body(8, 9)));
    expect(stream.pendingBytes).toBe(0);
  });

  it("handles a split inside the length prefix", () => {
    const stream = new FrameStream();
    const full = frame(body(4, 7));
    // Two of the four prefix bytes, then everything else.
    expect(stream.push(full.subarray(0, 2))).toEqual([]);
    const got = stream.push(full.subarray(2));
    expect(got).toHaveLength(1);
    expect(Array.from(got[0]!)).toEqual([7, 7, 7, 7]);
  });

  it("delivers frames one byte at a time", () => {
    const stream = new FrameStream();
    const wire = concat([frame(body(2, 1)), frame(body(3, 2))]);
    const seen: number[][] = [];
    for (const byte of wire) {
      for (const f of stream.push(Uint8Array.of(byte))) seen.push(Array.from(f));
    }
    expect(seen).toEqual([
      [1, 1],
      [2, 2, 2],
    ]);
  });

  it("ignores empty chunks", () => {
    const stream = new FrameStream();
    expect(stream.push(new Uint8Array(0))).toEqual([]);
    const got = stream.push(frame(body(1, 5)));
    expect(got).toHaveLength(1);
  });

  it("accepts a zero-length frame body", () => {
    const stream = new FrameStream();
    const got = stream.push(frame(new Uint8Array(0)));
    expect(got).toHaveLength(1);
    expect(got[0]!.length).toBe(0);
  });

  it("rejects an oversized declared length instead of buffering forever", () => {
    const stream = new FrameStream();
    const prefix = new Uint8Array(4);
    new DataView(prefix.buffer).setUint32(0, MAX_FRAME_BYTES + 1, false);
    expect(() => stream.push(prefix)).toThrow(/frame too large/);
  });

  it("agrees with the one-shot deframer", () => {
    const wire = concat([frame(body(5, 1)), frame(body(1, 2)), frame(body(9, 3))]);
    const stream = new FrameStream();
    const streamed: string[] = [];
    // Arbitrary, uneven chunking.
    for (let i = 0; i < wire.length; i += 7) {
      for (const f of stream.push(wire.subarray(i, i + 7))) {
        streamed.push(Array.from(f).join(","));
      }
    }
    expect(streamed).toEqual(deframe(wire).map((f) => Array.from(f).join(",")));
  });
});
