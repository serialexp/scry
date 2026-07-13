//! Metrics time-series chart helpers: the decoded shape the `MetricsPanel`
//! renders, plus a small categorical palette. Bucket-step selection and range
//! snapping are shared with the log-volume path (`volume.ts`) — a metrics
//! chart is the same server-side `date_bin` downsample, only reduced by a
//! chosen aggregate (avg/sum/min/max/count) instead of a per-severity count.
//!
//! Series identity: metrics rows are keyed by an opaque `series_fingerprint`.
//! When the query opts into the D-058 label join (`with_labels`) the rows also
//! carry a `labels` map, letting us name a series by its label set; absent
//! that we fall back to the short fingerprint hex. This module owns only the
//! pure decode + types + palette — the store issues the queries and the panel
//! draws them.

import type { Table } from "apache-arrow";
import { attrEntries } from "./format";

/** Intra-bucket downsample reducer + cross-series aggregate for the chart. */
export type AggFn = "avg" | "sum" | "min" | "max" | "count";

/** All aggregation functions, in display order (drives the control `<select>`). */
export const AGG_FNS: AggFn[] = ["avg", "sum", "min", "max", "count"];

/** One line on the chart: a stable key, a display name, and per-bucket values
 *  (null where the series had no sample in that bucket, so uPlot breaks the
 *  line rather than interpolating across a gap). */
export interface MetricSeriesLine {
  /** Stable identity — fingerprint hex in grouped mode, `"agg"` for the single
   *  aggregated line. Used as the uPlot series key + palette index seed. */
  key: string;
  /** Legend label — resolved label set when available, else the key. */
  name: string;
  /** Per-bucket values, index-aligned to `MetricsChartData.buckets`. */
  points: (number | null)[];
}

/** Decoded metrics chart: a shared bucket axis (unix ms) + one line per series
 *  (aggregated mode = exactly one), plus the bucket width and truncation. */
export interface MetricsChartData {
  /** Bucket start times, unix **milliseconds**, ascending. */
  buckets: number[];
  /** One line per series (aggregated mode = exactly one). */
  series: MetricSeriesLine[];
  /** Bucket width in ms (axis label + cache key). */
  stepMs: number;
  /** Decoded aggregate rows (for the header). */
  total: number;
  /** Series dropped past the display cap (0 = none). */
  truncated: number;
}

/** Max series lines drawn before we truncate — keeps the legend legible and
 *  the canvas cheap. Grouped mode past this reports the drop in `truncated`. */
export const MAX_SERIES = 30;

/** A small categorical palette (token-aligned hues), assigned by series index
 *  and wrapping past its length. */
export const SERIES_PALETTE: string[] = [
  "#5eead4", // teal
  "#fbbf24", // amber
  "#a78bfa", // violet
  "#4ade80", // green
  "#f87171", // red
  "#60a5fa", // blue
  "#f472b6", // pink
  "#facc15", // yellow
  "#34d399", // emerald
  "#c084fc", // purple
  "#fb923c", // orange
  "#22d3ee", // cyan
];

/** Palette colour for series index `i` (wraps). */
export function seriesColor(i: number): string {
  return SERIES_PALETTE[i % SERIES_PALETTE.length]!;
}

/** Short, stable display form of a 64-bit fingerprint (low 32 bits as hex). */
export function fpHex(fp: bigint): string {
  const h = (fp & 0xffff_ffffn).toString(16).padStart(8, "0");
  return `#${h}`;
}

/** Coerce an unknown Arrow scalar (bigint / number / string) to bigint. */
function toBigInt(v: unknown): bigint {
  return typeof v === "bigint" ? v : BigInt((v as number | string | null) ?? 0);
}

/** Render a resolved label set as a compact Prometheus-ish legend string,
 *  dropping `__name__` (it's the chart's subject). Empty set → "". */
export function labelsToName(pairs: [string, string][]): string {
  const kept = pairs
    .filter(([k]) => k !== "__name__")
    .sort(([a], [b]) => a.localeCompare(b));
  if (kept.length === 0) return "";
  return `{${kept.map(([k, v]) => `${k}="${v}"`).join(", ")}}`;
}

/** Decode a metrics chart aggregate into gap-filled lines over a shared bucket
 *  axis. `grouped` selects the shape: aggregated (`{bucket_ns, v}`, one line)
 *  or per-series (`{bucket_ns, fp, v}`, one line per fingerprint). `names`
 *  optionally maps a series key (fp hex) to a resolved legend label. */
export function decodeMetricsChart(
  table: Table,
  stepMs: number,
  grouped: boolean,
  names?: Map<string, string>,
): MetricsChartData {
  const rows = table.toArray();
  // series key → (bucketMs → value)
  const bySeries = new Map<string, Map<number, number>>();
  const bucketSet = new Set<number>();
  let total = 0;

  for (const r of rows) {
    const o = (r?.toJSON?.() ?? {}) as Record<string, unknown>;
    const bucketMs = Number(toBigInt(o.bucket_ns) / 1_000_000n);
    const v = Number(o.v ?? 0);
    const key = grouped ? fpHex(toBigInt(o.fp)) : "agg";
    total += 1;
    bucketSet.add(bucketMs);
    let s = bySeries.get(key);
    if (!s) {
      s = new Map();
      bySeries.set(key, s);
    }
    s.set(bucketMs, v);
  }

  const buckets = Array.from(bucketSet).sort((a, b) => a - b);

  // Order series by peak value (desc) so the most prominent lines survive the
  // cap and take the first palette hues.
  const peak = (s: Map<number, number>): number => {
    let m = -Infinity;
    for (const v of s.values()) if (v > m) m = v;
    return m;
  };
  const keys = Array.from(bySeries.keys()).sort(
    (a, b) => peak(bySeries.get(b)!) - peak(bySeries.get(a)!),
  );
  const kept = keys.slice(0, MAX_SERIES);
  const truncated = keys.length - kept.length;

  const series: MetricSeriesLine[] = kept.map((key) => {
    const s = bySeries.get(key)!;
    const resolved = names?.get(key);
    const name = grouped ? (resolved && resolved !== "" ? resolved : key) : "value";
    return {
      key,
      name,
      points: buckets.map((b) => (s.has(b) ? s.get(b)! : null)),
    };
  });

  return { buckets, series, stepMs, total, truncated };
}

/** Decode a `SELECT series_fingerprint AS fp, labels …` result into a
 *  `fpHex → legend-label` map for the per-series legend. First value wins per
 *  fingerprint (a series' labels are constant), so a truncated scan still maps
 *  every fingerprint it saw. */
export function decodeSeriesNames(table: Table): Map<string, string> {
  const out = new Map<string, string>();
  for (const r of table.toArray()) {
    const o = (r?.toJSON?.() ?? {}) as Record<string, unknown>;
    const key = fpHex(toBigInt(o.fp ?? o.series_fingerprint));
    if (out.has(key)) continue;
    out.set(key, labelsToName(attrEntries(o.labels)));
  }
  return out;
}
