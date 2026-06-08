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

// ── frames overview ───────────────────────────────────────────────────────
//
// `tracing-scry` (and any producer that follows the convention) mints a
// deterministic trace id of `session(8) ++ frame(8)`, big-endian: the high 8
// bytes identify a process run, the low 8 a frame counter. The frames-overview
// issues one aggregate row per trace — `{trace_id, t0, t1, dur_ns, spans}` — and
// this decodes each into a `FrameRow`, splitting the id back into session/frame
// so the UI can label and sort frames and drill into one by trace id.

/** One aggregated frame (= one trace) in the frames overview. */
export interface FrameRow {
  /** Full trace id, hex (32 chars) — drill target. */
  traceId: string;
  /** Session id: high 8 bytes, hex (16 chars). */
  session: string;
  /** Frame number: low 8 bytes, big-endian. */
  frame: bigint;
  /** Earliest span start in the frame (unix nanos). */
  t0: bigint;
  /** Latest span end in the frame (unix nanos). */
  t1: bigint;
  /** Frame duration (t1 - t0), nanos. */
  durNs: bigint;
  /** Number of spans in the frame. */
  spans: number;
}

/** Raw bytes of an id cell (FixedSizeBinary → Uint8Array); tolerate hex. */
function idBytes(v: unknown): Uint8Array {
  if (v instanceof Uint8Array) return v;
  if (typeof v === "string") {
    const clean = v.trim().replace(/^0x/i, "");
    if (clean.length % 2 === 0 && /^[0-9a-fA-F]*$/.test(clean)) {
      const out = new Uint8Array(clean.length / 2);
      for (let i = 0; i < out.length; i++) {
        out[i] = parseInt(clean.slice(i * 2, i * 2 + 2), 16);
      }
      return out;
    }
  }
  return new Uint8Array(0);
}

/** Read 8 big-endian bytes at `off` as an unsigned bigint. */
function beU64(b: Uint8Array, off: number): bigint {
  let x = 0n;
  for (let i = 0; i < 8; i++) x = (x << 8n) | BigInt(b[off + i] ?? 0);
  return x;
}

/** Decode one aggregate row into a `FrameRow`. */
export function decodeFrameRow(o: Row): FrameRow {
  const bytes = idBytes(o.trace_id);
  const t0 = asBigInt(o.t0);
  const t1 = asBigInt(o.t1);
  // Prefer the server-computed duration; fall back to t1 - t0.
  const durRaw = o.dur_ns;
  const durNs = durRaw === undefined || durRaw === null ? t1 - t0 : asBigInt(durRaw);
  return {
    traceId: bytes.length ? toHex(bytes) : "",
    session: bytes.length >= 8 ? toHex(bytes.slice(0, 8)) : "",
    frame: bytes.length >= 16 ? beU64(bytes, 8) : 0n,
    t0,
    t1,
    durNs,
    spans: Number(o.spans ?? 0),
  };
}

export function decodeFrameRows(rows: Row[]): FrameRow[] {
  return rows.map(decodeFrameRow);
}

/** Summary stats over a set of frames. Durations in nanos. */
export interface FrameStats {
  count: number;
  minNs: bigint;
  medianNs: bigint;
  p99Ns: bigint;
  maxNs: bigint;
}

/** Percentile (nearest-rank) of a pre-sortable bigint list. `p` in [0, 1]. */
function percentile(sorted: bigint[], p: number): bigint {
  if (sorted.length === 0) return 0n;
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil(p * sorted.length) - 1));
  return sorted[idx]!;
}

/** Compute count + duration distribution (min/median/p99/max) over frames. */
export function frameStats(rows: FrameRow[]): FrameStats {
  if (rows.length === 0) {
    return { count: 0, minNs: 0n, medianNs: 0n, p99Ns: 0n, maxNs: 0n };
  }
  const durs = rows.map((r) => r.durNs).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  return {
    count: rows.length,
    minNs: durs[0]!,
    medianNs: percentile(durs, 0.5),
    p99Ns: percentile(durs, 0.99),
    maxNs: durs[durs.length - 1]!,
  };
}
