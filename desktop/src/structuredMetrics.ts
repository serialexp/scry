//! Kind-aware structured metrics boundary.
//!
//! Structured v2 points deliberately do not expose a generic numeric `value`:
//! callers must discriminate `kind` before touching scalar, histogram, native
//! histogram, or summary data. This prevents an integer scalar (or a histogram
//! sum) from accidentally entering the legacy scalar chart aggregation path.

import type { Table } from "apache-arrow";
import type { TailMetricPointV2Output } from "./proto/generated-ingest";

export type LabelPairs = [string, string][];
export type ExactNumber =
  | { kind: "integer"; value: bigint }
  | { kind: "double"; value: number };
export type ExactCount =
  | { kind: "integer"; value: bigint }
  | { kind: "float"; value: number };

export interface StructuredMetricDescriptor {
  name: string;
  description: string;
  unit: string;
  metricKind: number;
  temporality: number;
  monotonic: boolean;
  resourceAttrs: LabelPairs;
  scopeName: string;
  scopeVersion: string;
  scopeAttrs: LabelPairs;
}

interface PointCommon {
  seriesFingerprint: bigint;
  labels: LabelPairs;
  descriptor: StructuredMetricDescriptor;
  startUnixNano: bigint;
  tsUnixNano: bigint;
  flags: number;
  attributes: LabelPairs;
  /** Kept losslessly for inspection; exemplar numbers and timestamps remain
   * bigint/number values supplied by the wire or Arrow decoder. */
  exemplars: unknown[];
}

export interface ScalarMetricPoint extends PointCommon {
  kind: "scalar";
  number: ExactNumber;
}
export interface HistogramMetricPoint extends PointCommon {
  kind: "histogram";
  count: bigint;
  sum: number | null;
  min: number | null;
  max: number | null;
  explicitBounds: number[];
  bucketCounts: bigint[];
}
export interface SparseBuckets {
  offset: number;
  deltas: number[];
  counts: ExactCount[];
}
export interface ExponentialHistogramMetricPoint extends PointCommon {
  kind: "exponential-histogram";
  count: ExactCount;
  sum: number | null;
  min: number | null;
  max: number | null;
  scale: number;
  zeroThreshold: number;
  zeroCount: ExactCount;
  positive: SparseBuckets;
  negative: SparseBuckets;
  customBounds: number[];
  resetHint: number;
}
export interface SummaryMetricPoint extends PointCommon {
  kind: "summary";
  count: bigint;
  sum: number;
  quantiles: { quantile: number; value: number }[];
}
export type StructuredMetricPoint =
  | ScalarMetricPoint
  | HistogramMetricPoint
  | ExponentialHistogramMetricPoint
  | SummaryMetricPoint;

const obj = (v: unknown): Record<string, unknown> =>
  v !== null && typeof v === "object"
    ? ((v as { toJSON?: () => unknown }).toJSON?.() as Record<string, unknown>) ??
      (v as Record<string, unknown>)
    : {};
const pick = (o: Record<string, unknown>, snake: string, camel: string): unknown =>
  o[snake] ?? o[camel];
const bigint = (v: unknown): bigint => typeof v === "bigint" ? v : BigInt(String(v ?? 0));
const number = (v: unknown): number => Number(v ?? 0);
const pairs = (v: unknown): LabelPairs => {
  if (!Array.isArray(v)) return [];
  return v.map((entry) => {
    if (Array.isArray(entry)) return [String(entry[0] ?? ""), String(entry[1] ?? "")];
    const p = obj(entry);
    return [String(p.key ?? ""), String(p.value ?? "")];
  });
};
const list = (v: unknown): unknown[] => {
  if (Array.isArray(v)) return v;
  if (v && typeof (v as { toArray?: unknown }).toArray === "function") {
    return Array.from((v as { toArray: () => Iterable<unknown> }).toArray());
  }
  if (v && typeof (v as { [Symbol.iterator]?: unknown })[Symbol.iterator] === "function") {
    return Array.from(v as Iterable<unknown>);
  }
  return [];
};

function descriptor(v: unknown, fallbackName = ""): StructuredMetricDescriptor {
  const d = obj(v);
  return {
    name: String(d.name ?? fallbackName),
    description: String(d.description ?? ""),
    unit: String(d.unit ?? ""),
    metricKind: number(pick(d, "metric_kind", "metricKind")),
    temporality: number(d.temporality),
    monotonic: number(d.monotonic) !== 0,
    resourceAttrs: pairs(pick(d, "resource_attrs", "resourceAttrs")),
    scopeName: String(pick(d, "scope_name", "scopeName") ?? ""),
    scopeVersion: String(pick(d, "scope_version", "scopeVersion") ?? ""),
    scopeAttrs: pairs(pick(d, "scope_attrs", "scopeAttrs")),
  };
}

function exactNumber(v: unknown): ExactNumber {
  const n = obj(v);
  const tagged = obj(n.value);
  const type = String(tagged.type ?? "");
  const value = obj(tagged.value);
  if (type === "IntegerValueV2") return { kind: "integer", value: bigint(value.value) };
  if (type === "DoubleValueV2") return { kind: "double", value: number(value.value) };
  const kind = number(n.kind);
  if (kind === 1 || n.integer !== undefined) return { kind: "integer", value: bigint(n.integer) };
  return { kind: "double", value: number(n.float) };
}

function exactCount(v: unknown): ExactCount {
  const n = obj(v);
  const tagged = obj(n.value);
  const type = String(tagged.type ?? "");
  const value = obj(tagged.value);
  if (type === "IntegerCountV2") return { kind: "integer", value: bigint(value.value) };
  if (type === "FloatCountV2") return { kind: "float", value: number(value.value) };
  const kind = number(n.kind);
  if (kind === 1 || n.integer !== undefined) return { kind: "integer", value: bigint(n.integer) };
  return { kind: "float", value: number(n.float) };
}

function sparse(v: unknown): SparseBuckets {
  const s = obj(v);
  return {
    offset: number(s.offset),
    deltas: list(s.deltas).map(number),
    counts: list(s.counts).map(exactCount),
  };
}

interface DecodeEnvelope {
  seriesFingerprint: bigint;
  labels: LabelPairs;
  descriptor: StructuredMetricDescriptor;
  topTs?: bigint;
}

function decodePoint(raw: unknown, env: DecodeEnvelope): StructuredMetricPoint | null {
  const wrapper = obj(raw);
  // Wire points are a tagged union. Historical Arrow points are a struct with
  // `kind` and one nullable child per kind.
  const tagged = obj(wrapper.value);
  const wireType = String(tagged.type ?? "");
  const p = wireType ? obj(tagged.value) : wrapper;
  const kind = wireType || number(pick(p, "kind", "kind"));
  const common: PointCommon = {
    ...env,
    startUnixNano: bigint(pick(p, "start_unix_nano", "startUnixNano")),
    tsUnixNano: env.topTs ?? bigint(pick(p, "ts_unix_nano", "tsUnixNano")),
    flags: number(p.flags),
    attributes: pairs(p.attributes),
    exemplars: list(p.exemplars),
  };
  if (kind === "ScalarPointV2" || kind === 1) {
    return { ...common, kind: "scalar", number: exactNumber(p.number ?? p.scalar) };
  }
  if (kind === "HistogramPointV2" || kind === 2) {
    const h = wireType ? p : obj(p.histogram);
    return {
      ...common, kind: "histogram", count: bigint(h.count),
      sum: number(h.has_sum) ? number(h.sum) : null,
      min: number(h.has_min) ? number(h.min) : null,
      max: number(h.has_max) ? number(h.max) : null,
      explicitBounds: list(h.explicit_bounds).map(number),
      bucketCounts: list(h.bucket_counts).map(bigint),
    };
  }
  if (kind === "ExponentialHistogramPointV2" || kind === 3) {
    const h = wireType ? p : obj(p.exponential_histogram);
    return {
      ...common, kind: "exponential-histogram", count: exactCount(h.count),
      sum: number(h.has_sum) ? number(h.sum) : null,
      min: number(h.has_min) ? number(h.min) : null,
      max: number(h.has_max) ? number(h.max) : null,
      scale: number(h.scale), zeroThreshold: number(h.zero_threshold),
      zeroCount: exactCount(h.zero_count), positive: sparse(h.positive), negative: sparse(h.negative),
      customBounds: list(h.custom_bounds).map(number), resetHint: number(h.reset_hint),
    };
  }
  if (kind === "SummaryPointV2" || kind === 4) {
    const s = wireType ? p : obj(p.summary);
    return {
      ...common, kind: "summary", count: bigint(s.count), sum: number(s.sum),
      quantiles: list(s.quantiles).map((q) => {
        const x = obj(q); return { quantile: number(x.quantile), value: number(x.value) };
      }),
    };
  }
  return null;
}

/** Losslessly map a capability-gated 0x55 frame into the UI model. */
export function structuredPointFromTail(value: TailMetricPointV2Output): StructuredMetricPoint {
  const point = decodePoint(value.point, {
    seriesFingerprint: value.series_fingerprint,
    labels: pairs(value.labels),
    descriptor: descriptor(value.descriptor),
  });
  if (!point) throw new Error("unknown structured metric point kind");
  return point;
}

/** Decode nested v3 metrics Arrow rows. Legacy scalar rows (`point = null`) are
 * ignored, making this safe to run beside the existing scalar table decoder. */
export function decodeStructuredMetricRows(table: Table, cap = 500): StructuredMetricPoint[] {
  const out: StructuredMetricPoint[] = [];
  for (const row of table.toArray()) {
    const r = obj(row);
    if (r.point == null) continue;
    const p = decodePoint(r.point, {
      seriesFingerprint: bigint(pick(r, "series_fingerprint", "seriesFingerprint")),
      labels: pairs(r.labels),
      descriptor: descriptor(r.descriptor, String(r.name ?? "")),
      topTs: bigint(pick(r, "ts_unix_nano", "tsUnixNano")),
    });
    if (p) out.push(p);
  }
  return out.length > cap ? out.slice(out.length - cap) : out;
}

/** Append without combining points. Series, timestamp, temporality and kind all
 * remain intact; unlike scalar chart buckets this performs no cross-series merge. */
export function appendStructuredCapped(
  previous: readonly StructuredMetricPoint[],
  incoming: readonly StructuredMetricPoint[],
  cap: number,
): StructuredMetricPoint[] {
  if (cap <= 0) return [];
  const start = Math.max(0, previous.length + incoming.length - cap);
  return [...previous, ...incoming].slice(start);
}

export function formatExact(v: ExactNumber | ExactCount): string {
  return typeof v.value === "bigint" ? v.value.toString() : String(v.value);
}
