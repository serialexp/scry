import { describe, expect, it } from "vitest";

import {
  buildSpanTree,
  decodeFrameRow,
  decodeSpan,
  frameStats,
  layoutSpans,
  serviceHue,
  singleTraceId,
  type FrameRow,
  type Span,
} from "./traces";

/** Build a 16-byte trace id = session(8) ++ frame(8), big-endian. */
function traceIdBytes(session: bigint, frame: bigint): Uint8Array {
  const b = new Uint8Array(16);
  for (let i = 0; i < 8; i++) {
    b[7 - i] = Number((session >> BigInt(i * 8)) & 0xffn);
    b[15 - i] = Number((frame >> BigInt(i * 8)) & 0xffn);
  }
  return b;
}

// Minimal span factory; only the fields a given test cares about need overriding.
function span(p: Partial<Span> & Pick<Span, "spanId" | "start" | "end">): Span {
  return {
    traceId: "t0",
    parentSpanId: null,
    name: "span",
    kind: 1,
    service: "svc",
    statusCode: 0,
    statusMessage: "",
    attrs: [],
    resourceLabels: [],
    events: [],
    links: [],
    ...p,
  };
}

describe("buildSpanTree", () => {
  it("places children under their parent in pre-order, sorted by start", () => {
    const spans = [
      span({ spanId: "b", parentSpanId: "a", start: 30n, end: 40n }),
      span({ spanId: "a", parentSpanId: null, start: 0n, end: 100n }),
      span({ spanId: "c", parentSpanId: "a", start: 10n, end: 20n }),
    ];
    const placed = buildSpanTree(spans);
    expect(placed.map((s) => s.spanId)).toEqual(["a", "c", "b"]);
    expect(placed.map((s) => s.depth)).toEqual([0, 1, 1]);
  });

  it("nests grandchildren and increments depth", () => {
    const spans = [
      span({ spanId: "a", parentSpanId: null, start: 0n, end: 100n }),
      span({ spanId: "b", parentSpanId: "a", start: 10n, end: 90n }),
      span({ spanId: "c", parentSpanId: "b", start: 20n, end: 30n }),
    ];
    const placed = buildSpanTree(spans);
    expect(placed.map((s) => s.spanId)).toEqual(["a", "b", "c"]);
    expect(placed.map((s) => s.depth)).toEqual([0, 1, 2]);
  });

  it("treats a span whose parent is absent (partial result) as a root", () => {
    const spans = [
      span({ spanId: "child", parentSpanId: "missing", start: 50n, end: 60n }),
      span({ spanId: "early", parentSpanId: null, start: 10n, end: 20n }),
    ];
    const placed = buildSpanTree(spans);
    // Both are roots; ordered by start → early before child, both depth 0.
    expect(placed.map((s) => s.spanId)).toEqual(["early", "child"]);
    expect(placed.map((s) => s.depth)).toEqual([0, 0]);
  });

  it("orders multiple roots by start, ties broken by span id", () => {
    const spans = [
      span({ spanId: "z", parentSpanId: null, start: 5n, end: 6n }),
      span({ spanId: "a", parentSpanId: null, start: 5n, end: 6n }),
      span({ spanId: "m", parentSpanId: null, start: 1n, end: 2n }),
    ];
    expect(buildSpanTree(spans).map((s) => s.spanId)).toEqual(["m", "a", "z"]);
  });

  it("does not loop forever on a pathological cycle", () => {
    const spans = [
      span({ spanId: "a", parentSpanId: "b", start: 0n, end: 1n }),
      span({ spanId: "b", parentSpanId: "a", start: 0n, end: 1n }),
    ];
    // Neither is a true root (each parent is present), so nothing is emitted —
    // the cycle guard prevents an infinite walk regardless.
    expect(() => buildSpanTree(spans)).not.toThrow();
  });
});

describe("layoutSpans", () => {
  it("lays the root across the full window and nests children proportionally", () => {
    const spans = [
      span({ spanId: "a", start: 0n, end: 100n }),
      span({ spanId: "b", start: 25n, end: 75n }),
    ];
    const lay = layoutSpans(spans);
    expect(lay[0]).toEqual({ leftFrac: 0, widthFrac: 1 });
    expect(lay[1]).toEqual({ leftFrac: 0.25, widthFrac: 0.5 });
  });

  it("uses a window spanning min(start)..max(end) across all spans", () => {
    const spans = [
      span({ spanId: "a", start: 100n, end: 200n }),
      span({ spanId: "b", start: 200n, end: 300n }),
    ];
    const lay = layoutSpans(spans);
    // window = [100, 300], total = 200
    expect(lay[0]).toEqual({ leftFrac: 0, widthFrac: 0.5 });
    expect(lay[1]).toEqual({ leftFrac: 0.5, widthFrac: 0.5 });
  });

  it("lays every bar full-width when the window has zero duration", () => {
    const spans = [
      span({ spanId: "a", start: 42n, end: 42n }),
      span({ spanId: "b", start: 42n, end: 42n }),
    ];
    expect(layoutSpans(spans)).toEqual([
      { leftFrac: 0, widthFrac: 1 },
      { leftFrac: 0, widthFrac: 1 },
    ]);
  });
});

describe("singleTraceId", () => {
  it("returns null for an empty result", () => {
    expect(singleTraceId([])).toBeNull();
  });

  it("returns the lone trace id (hex) when every row shares it", () => {
    const rows = [{ trace_id: new Uint8Array([0xab, 0xcd]) }, { trace_id: new Uint8Array([0xab, 0xcd]) }];
    expect(singleTraceId(rows)).toBe("abcd");
  });

  it("returns null when two distinct trace ids are present", () => {
    const rows = [{ trace_id: new Uint8Array([0x01]) }, { trace_id: new Uint8Array([0x02]) }];
    expect(singleTraceId(rows)).toBeNull();
  });
});

describe("decodeSpan", () => {
  it("hexes binary ids, nulls an empty parent, and falls back the service name", () => {
    const s = decodeSpan({
      trace_id: new Uint8Array([0xde, 0xad]),
      span_id: new Uint8Array([0x01, 0x02]),
      parent_span_id: null,
      name: "GET /x",
      kind: 2,
      start_unix_nano: 5n,
      end_unix_nano: 9n,
      status_code: 2,
    });
    expect(s.traceId).toBe("dead");
    expect(s.spanId).toBe("0102");
    expect(s.parentSpanId).toBeNull();
    expect(s.service).toBe("(unknown)");
    expect(s.statusCode).toBe(2);
    expect(s.start).toBe(5n);
  });
});

describe("serviceHue", () => {
  it("is stable and in range for a given name", () => {
    const h = serviceHue("checkout");
    expect(h).toBe(serviceHue("checkout"));
    expect(h).toBeGreaterThanOrEqual(0);
    expect(h).toBeLessThan(360);
  });
});

describe("decodeFrameRow", () => {
  it("splits the trace id into session + frame and decodes durations", () => {
    const r = decodeFrameRow({
      trace_id: traceIdBytes(0xdeadbeefn, 1234n),
      t0: 1000n,
      t1: 1500n,
      dur_ns: 500n,
      spans: 7,
    });
    expect(r.session).toBe("00000000deadbeef");
    expect(r.frame).toBe(1234n);
    expect(r.traceId).toBe("00000000deadbeef00000000000004d2");
    expect(r.t0).toBe(1000n);
    expect(r.t1).toBe(1500n);
    expect(r.durNs).toBe(500n);
    expect(r.spans).toBe(7);
  });

  it("falls back to t1 - t0 when dur_ns is absent", () => {
    const r = decodeFrameRow({
      trace_id: traceIdBytes(1n, 2n),
      t0: 10n,
      t1: 90n,
      spans: 3,
    });
    expect(r.durNs).toBe(80n);
  });

  it("tolerates a hex-string trace id", () => {
    const r = decodeFrameRow({ trace_id: "0000000000000001000000000000000a", t0: 0n, t1: 0n });
    expect(r.session).toBe("0000000000000001");
    expect(r.frame).toBe(10n);
  });
});

describe("frameStats", () => {
  it("returns zeros for no frames", () => {
    expect(frameStats([])).toEqual({ count: 0, minNs: 0n, medianNs: 0n, p99Ns: 0n, maxNs: 0n });
  });

  it("computes count and the duration distribution", () => {
    const mk = (durNs: bigint): FrameRow => ({
      traceId: "",
      session: "",
      frame: 0n,
      t0: 0n,
      t1: durNs,
      durNs,
      spans: 1,
    });
    const rows = [mk(10n), mk(50n), mk(20n), mk(40n), mk(30n)];
    const s = frameStats(rows);
    expect(s.count).toBe(5);
    expect(s.minNs).toBe(10n);
    expect(s.maxNs).toBe(50n);
    // median (nearest-rank, p=0.5 of 5 → index 2 of sorted [10,20,30,40,50]).
    expect(s.medianNs).toBe(30n);
    expect(s.p99Ns).toBe(50n);
  });
});
