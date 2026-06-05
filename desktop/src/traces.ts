//! Pure, DOM-free logic for the traces waterfall view.
//!
//! Decodes Arrow row objects (`record.toJSON()`) into `Span`s, assembles the
//! parent/child tree in pre-order (so the rendered list reads top-down as a
//! call tree, Jaeger/Tempo style), and computes per-span bar geometry against
//! the trace's [t0, t1] window. Kept free of SolidJS / DOM so it is unit-tested
//! directly with vitest.

import { attrEntries, toHex } from "./format";

// ── types ───────────────────────────────────────────────────────────────

export interface SpanEvent {
  ts: bigint;
  name: string;
  attrs: [string, string][];
}

export interface SpanLink {
  traceId: string;
  spanId: string;
  attrs: [string, string][];
}

export interface Span {
  traceId: string;
  spanId: string;
  /** null = root span (no parent on the wire). */
  parentSpanId: string | null;
  name: string;
  kind: number;
  service: string;
  start: bigint;
  end: bigint;
  statusCode: number;
  statusMessage: string;
  attrs: [string, string][];
  resourceLabels: [string, string][];
  events: SpanEvent[];
  links: SpanLink[];
}

/** A span placed in the tree: pre-order index carries its nesting `depth`. */
export interface PlacedSpan extends Span {
  depth: number;
}

/** Bar geometry for one span, as fractions of the trace window [0, 1]. */
export interface SpanLayout {
  leftFrac: number;
  widthFrac: number;
}

// ── decoding ────────────────────────────────────────────────────────────

type Row = Record<string, unknown>;

function asBigInt(v: unknown): bigint {
  if (typeof v === "bigint") return v;
  if (v === null || v === undefined) return 0n;
  try {
    return BigInt(v as number | string);
  } catch {
    return 0n;
  }
}

/** Hex a FixedSizeBinary id cell; tolerate already-hex strings / null. */
function hexId(v: unknown): string {
  if (v instanceof Uint8Array) return toHex(v);
  if (v === null || v === undefined) return "";
  return String(v);
}

function decodeEvents(v: unknown): SpanEvent[] {
  if (!Array.isArray(v)) return [];
  return v.map((e) => {
    const o = (e ?? {}) as Row;
    return {
      ts: asBigInt(o.ts_unix_nano),
      name: String(o.name ?? ""),
      attrs: attrEntries(o.attributes),
    };
  });
}

function decodeLinks(v: unknown): SpanLink[] {
  if (!Array.isArray(v)) return [];
  return v.map((l) => {
    const o = (l ?? {}) as Row;
    return {
      traceId: hexId(o.trace_id),
      spanId: hexId(o.span_id),
      attrs: attrEntries(o.attributes),
    };
  });
}

/** Decode one Arrow row object into a `Span`. */
export function decodeSpan(o: Row): Span {
  const parent = hexId(o.parent_span_id);
  return {
    traceId: hexId(o.trace_id),
    spanId: hexId(o.span_id),
    parentSpanId: parent === "" ? null : parent,
    name: String(o.name ?? ""),
    kind: Number(o.kind ?? 0),
    service: String(o.service_name ?? "") || "(unknown)",
    start: asBigInt(o.start_unix_nano),
    end: asBigInt(o.end_unix_nano),
    statusCode: Number(o.status_code ?? 0),
    statusMessage: String(o.status_message ?? ""),
    attrs: attrEntries(o.attributes),
    resourceLabels: attrEntries(o.resource_labels),
    events: decodeEvents(o.events),
    links: decodeLinks(o.links),
  };
}

export function decodeSpans(rows: Row[]): Span[] {
  return rows.map(decodeSpan);
}

/**
 * The lone distinct trace id (hex) across the rows, or `null` when there are
 * zero or more than one — i.e. the result is not a single-trace lookup. Drives
 * the waterfall-vs-generic-table dispatch.
 */
export function singleTraceId(rows: Row[]): string | null {
  let id: string | null = null;
  for (const o of rows) {
    const t = hexId(o.trace_id);
    if (t === "") continue;
    if (id === null) id = t;
    else if (id !== t) return null;
  }
  return id;
}

// ── tree assembly ─────────────────────────────────────────────────────────

/**
 * Order spans in pre-order with a `depth`, building the parent/child tree.
 * Roots are spans with no parent, or whose parent is absent from the set
 * (partial results — an orphan is treated as a root). Siblings (and roots) are
 * ordered by start time, ties broken by span id for determinism.
 */
export function buildSpanTree(spans: Span[]): PlacedSpan[] {
  const byId = new Map<string, Span>();
  for (const s of spans) byId.set(s.spanId, s);

  const children = new Map<string, Span[]>();
  const roots: Span[] = [];
  for (const s of spans) {
    const isRoot = s.parentSpanId === null || !byId.has(s.parentSpanId);
    if (isRoot) {
      roots.push(s);
    } else {
      const list = children.get(s.parentSpanId!);
      if (list) list.push(s);
      else children.set(s.parentSpanId!, [s]);
    }
  }

  const cmp = (a: Span, b: Span): number => {
    if (a.start < b.start) return -1;
    if (a.start > b.start) return 1;
    return a.spanId < b.spanId ? -1 : a.spanId > b.spanId ? 1 : 0;
  };
  roots.sort(cmp);
  for (const list of children.values()) list.sort(cmp);

  const out: PlacedSpan[] = [];
  const seen = new Set<string>();
  const walk = (s: Span, depth: number): void => {
    if (seen.has(s.spanId)) return; // guard against pathological cycles
    seen.add(s.spanId);
    out.push({ ...s, depth });
    const kids = children.get(s.spanId);
    if (kids) for (const k of kids) walk(k, depth + 1);
  };
  for (const r of roots) walk(r, 0);
  return out;
}

// ── geometry ────────────────────────────────────────────────────────────

/** The [t0, t1] window covering every span (t0 = min start, t1 = max end). */
export function traceWindow(spans: Span[]): { t0: bigint; t1: bigint } {
  if (spans.length === 0) return { t0: 0n, t1: 0n };
  let t0 = spans[0]!.start;
  let t1 = spans[0]!.end;
  for (const s of spans) {
    if (s.start < t0) t0 = s.start;
    if (s.end > t1) t1 = s.end;
  }
  return { t0, t1 };
}

/**
 * Bar geometry per span as fractions of the trace window. A zero-width window
 * (all spans instantaneous / identical) lays every bar out full-width. Each
 * fraction is clamped to [0, 1].
 */
export function layoutSpans(spans: Span[]): SpanLayout[] {
  const { t0, t1 } = traceWindow(spans);
  const span = t1 - t0;
  if (span <= 0n) return spans.map(() => ({ leftFrac: 0, widthFrac: 1 }));
  const total = Number(span);
  const clamp = (x: number): number => (x < 0 ? 0 : x > 1 ? 1 : x);
  return spans.map((s) => {
    const left = clamp(Number(s.start - t0) / total);
    const width = clamp(Number(s.end - s.start) / total);
    return { leftFrac: left, widthFrac: width };
  });
}

// ── labels & colour ───────────────────────────────────────────────────────

const KIND_LABELS = ["UNSPEC", "INTERNAL", "SERVER", "CLIENT", "PRODUCER", "CONSUMER"];

/** OTel SpanKind number → short label. */
export function kindLabel(kind: number): string {
  return KIND_LABELS[kind] ?? `KIND_${kind}`;
}

/** OTel StatusCode → label + CSS-class suffix. 0=UNSET 1=OK 2=ERROR. */
export function statusLabel(code: number): { label: string; cls: string } {
  if (code === 2) return { label: "ERROR", cls: "err" };
  if (code === 1) return { label: "OK", cls: "ok" };
  return { label: "", cls: "unset" };
}

/** Stable hash of a service name → a hue (0–359) for bar colour. */
export function serviceHue(name: string): number {
  let h = 0;
  for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) | 0;
  return ((h % 360) + 360) % 360;
}
