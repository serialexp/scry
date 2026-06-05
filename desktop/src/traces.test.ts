import { describe, expect, it } from "vitest";

import {
  buildSpanTree,
  decodeSpan,
  layoutSpans,
  serviceHue,
  singleTraceId,
  type Span,
} from "./traces";

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
