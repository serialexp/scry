//! Log-volume histogram helpers: bucket-step selection, range snapping, and
//! the decoded shape the `VolumePanel` renders.
//!
//! The volume graph is a count-over-time of log entries, split by severity,
//! respecting the current matchers + time range. It rides the existing query
//! wire via a `date_bin` aggregation (no protocol change), so this module owns
//! only the pure math: how wide a bucket to use for a given range, how to snap
//! a live "now" range to that bucket so repeated refreshes hit the queryd
//! result cache (identical bounds → identical request → cache hit), and the
//! `{ buckets, series }` shape after decoding the Arrow result.

/// Round bucket widths (milliseconds), smallest→largest. A query's step is the
/// smallest ladder rung that keeps the bucket count at/under `TARGET_BUCKETS`.
/// Round values keep `date_bin` origins on tidy boundaries and make the x-axis
/// legible.
export const STEP_LADDER_MS: number[] = [
  1_000, // 1s
  5_000, // 5s
  10_000, // 10s
  15_000, // 15s
  30_000, // 30s
  60_000, // 1m
  300_000, // 5m
  600_000, // 10m
  900_000, // 15m
  1_800_000, // 30m
  3_600_000, // 1h
  10_800_000, // 3h
  21_600_000, // 6h
  43_200_000, // 12h
  86_400_000, // 1d
];

/// Aim for ~this many buckets across the range — dense enough to show shape,
/// sparse enough that each bar is visible and the aggregate stays cheap.
export const TARGET_BUCKETS = 120;

/// Pick the bucket width (ms) for a range span: the smallest ladder rung whose
/// bucket count is ≤ `TARGET_BUCKETS`, clamped to the widest rung for very long
/// ranges. `spanMs ≤ 0` (missing/degenerate range) falls back to the smallest.
export function chooseStepMs(spanMs: number): number {
  if (!Number.isFinite(spanMs) || spanMs <= 0) return STEP_LADDER_MS[0];
  for (const step of STEP_LADDER_MS) {
    if (spanMs / step <= TARGET_BUCKETS) return step;
  }
  return STEP_LADDER_MS[STEP_LADDER_MS.length - 1];
}

/// DataFusion `INTERVAL` literal for a step, expressed in whole seconds (the
/// ladder is second-aligned, so this is exact). Used inside the volume SQL's
/// `date_bin(INTERVAL '…', …)`.
export function stepIntervalSql(stepMs: number): string {
  const sec = Math.max(1, Math.round(stepMs / 1000));
  return `INTERVAL '${sec} seconds'`;
}

/// Snap a live quick-range [now − span, now] so repeated refreshes within one
/// bucket reuse the exact same bounds — the key to the queryd result cache
/// hitting on a dashboard-style refresh. `tsMax` is floored to a bucket
/// boundary; `tsMin = tsMax − span`. Returns unix **nanoseconds** (the wire
/// unit), as BigInt.
export function snapQuickRangeNs(
  nowMs: number,
  spanMs: number,
): { tsMinNs: bigint; tsMaxNs: bigint } {
  const stepMs = chooseStepMs(spanMs);
  const snappedMaxMs = Math.floor(nowMs / stepMs) * stepMs;
  const tsMaxNs = BigInt(snappedMaxMs) * 1_000_000n;
  const tsMinNs = tsMaxNs - BigInt(spanMs) * 1_000_000n;
  return { tsMinNs, tsMaxNs };
}

/// One severity band of the histogram: a stable class (from the OTEL severity
/// bucketing) plus its per-bucket counts, index-aligned to `VolumeData.buckets`.
export interface VolumeSeries {
  /** Severity class label, e.g. "ERROR" / "INFO". */
  label: string;
  /** CSS class for the band color, e.g. "sev-error". */
  cls: string;
  /** Representative OTEL severity number for ordering (higher = more severe). */
  sev: number;
  /** Per-bucket counts, aligned to `VolumeData.buckets` (0 where absent). */
  counts: number[];
}

/// Decoded volume histogram: the shared x-axis (bucket start times, unix ms)
/// and one stacked band per severity class present in the range.
export interface VolumeData {
  /** Bucket start times, unix **milliseconds**, ascending. */
  buckets: number[];
  /** Severity bands, ordered least→most severe for a stable stack order. */
  series: VolumeSeries[];
  /** Total entries across all buckets/severities (for the header). */
  total: number;
  /** Bucket width in ms (for bar sizing + the axis label). */
  stepMs: number;
}
