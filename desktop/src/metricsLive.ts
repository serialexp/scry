//! Live metric samples, bucketed client-side and merged onto a stored chart
//! (D-065).
//!
//! # Why any of this is needed
//!
//! History and live arrive in different shapes. The chart's stored half is
//! **already aggregated**: queryd runs `date_bin(...)` + `avg/sum/min/max/count`
//! and returns one value per (series, bucket). The tail pushes **raw samples**
//! — a timestamp and a float. So the client has to do the server's bucketing
//! and reduction itself for the live half, and it has to do it the *same way*
//! or the newest bucket would visibly jump when a live value is replaced by
//! the stored one on the next refresh.
//!
//! # Accumulate, don't pre-reduce
//!
//! Each (series, bucket) keeps `{sum, count, min, max}` rather than a running
//! reduced value. That is what makes `avg` exact: you cannot average a stream
//! of averages without their counts, and a bucket keeps receiving samples for
//! as long as it is the newest one. From these four numbers every reducer the
//! UI offers is a closed-form read.
//!
//! # The seam: live owns only strictly-newer buckets
//!
//! History covers up to some newest bucket `H`. A live sample landing *inside*
//! `H` would give that bucket two competing values — the server's (computed
//! over the whole bucket) and ours (computed over the sliver of it we saw).
//! Ours would be wrong and would flicker. So live only ever contributes
//! buckets **strictly after** `H`; a sample at or before it is dropped. `H`
//! keeps its stored value until the next history refresh, at which point the
//! bucket is complete and correct.
//!
//! This mirrors the logs seam (`store.ts`), and it is best-effort for the same
//! reason: the tail is lossy by construction, so the live tip of the chart is
//! "what is happening right now", not an exact aggregate.

import {
  MAX_SERIES,
  fpHex,
  labelsToName,
  type AggFn,
  type MetricSeriesLine,
  type MetricsChartData,
} from "./metricsChart";
import type { TailSample } from "./protocol/tail";

/** Per-(series, bucket) accumulator. Enough state to answer any `AggFn`
 *  exactly, unlike a single running reduced value. */
export interface BucketAcc {
  sum: number;
  count: number;
  min: number;
  max: number;
}

/** Reduce one accumulator with `agg`. `count` is the sample count, which is
 *  exactly what the server's `count(value)` returns for the same bucket. */
export function reduceAcc(acc: BucketAcc, agg: AggFn): number {
  switch (agg) {
    case "avg":
      return acc.count === 0 ? 0 : acc.sum / acc.count;
    case "sum":
      return acc.sum;
    case "min":
      return acc.min;
    case "max":
      return acc.max;
    case "count":
      return acc.count;
  }
}

/** The bucket a timestamp falls in, as unix **ms**, floored to `stepMs`.
 *  Matches DataFusion's `date_bin` origin (the unix epoch). */
export function bucketOf(tsUnixNano: bigint, stepMs: number): number {
  const ms = Number(tsUnixNano / 1_000_000n);
  return Math.floor(ms / stepMs) * stepMs;
}

/** The legend/identity key for a live sample, matching what
 *  `decodeMetricsChart` builds for the stored half: the fingerprint hex when
 *  grouped, the single aggregate line otherwise. */
export function liveSeriesKey(sample: TailSample, grouped: boolean): string {
  return grouped ? fpHex(sample.seriesFingerprint) : "agg";
}

/**
 * A rolling set of live buckets: `seriesKey → bucketMs → BucketAcc`.
 *
 * Deliberately dumb and synchronous — the store owns when to feed it and when
 * to re-derive a chart, so all the ordering and throttling policy lives in one
 * place and this stays trivially testable.
 */
export class LiveBuckets {
  private series = new Map<string, Map<number, BucketAcc>>();
  /** Legend labels seen on the wire, so a series that appears *only* in the
   *  live half still gets a readable name instead of a bare fingerprint. */
  private names = new Map<string, string>();

  /** Number of distinct series currently held (for tests + diagnostics). */
  get seriesCount(): number {
    return this.series.size;
  }

  /** Drop everything — a new query, a signal change, a stopped stream. */
  clear(): void {
    this.series.clear();
    this.names.clear();
  }

  /**
   * Fold one sample in. `seamBucket` is the newest bucket the stored history
   * covers; a sample at or before it is **dropped**, because history already
   * accounts for that bucket and does so over the whole of it.
   *
   * Pass `null` when there is no history at all (an empty result): then every
   * bucket is live-owned, since nothing else claims one.
   */
  push(
    sample: TailSample,
    stepMs: number,
    grouped: boolean,
    seamBucket: number | null,
  ): boolean {
    if (stepMs <= 0) return false;
    const bucket = bucketOf(sample.tsUnixNano, stepMs);
    if (seamBucket !== null && bucket <= seamBucket) return false;

    const key = liveSeriesKey(sample, grouped);
    let buckets = this.series.get(key);
    if (!buckets) {
      buckets = new Map();
      this.series.set(key, buckets);
    }
    const acc = buckets.get(bucket);
    if (acc === undefined) {
      buckets.set(bucket, {
        sum: sample.value,
        count: 1,
        min: sample.value,
        max: sample.value,
      });
    } else {
      acc.sum += sample.value;
      acc.count += 1;
      if (sample.value < acc.min) acc.min = sample.value;
      if (sample.value > acc.max) acc.max = sample.value;
    }

    if (grouped && !this.names.has(key)) {
      const name = liveSampleName(sample);
      if (name !== "") this.names.set(key, name);
    }
    return true;
  }

  /** Forget buckets that start before `minBucketMs` — the chart's window has
   *  slid and they are off the left edge. Series left with no buckets are
   *  dropped so the legend doesn't accumulate ghosts. */
  evictBefore(minBucketMs: number): void {
    for (const [key, buckets] of this.series) {
      for (const b of Array.from(buckets.keys())) {
        if (b < minBucketMs) buckets.delete(b);
      }
      if (buckets.size === 0) {
        this.series.delete(key);
        this.names.delete(key);
      }
    }
  }

  /** A legend label observed on the wire for `key`, if any. */
  nameFor(key: string): string | undefined {
    return this.names.get(key);
  }

  /** Snapshot as plain maps, for merging and for tests. */
  snapshot(): Map<string, Map<number, BucketAcc>> {
    return this.series;
  }
}

/**
 * Build a legend label from a live sample's labels.
 *
 * Delegates to the stored half's `labelsToName` so a series named from the
 * wire is indistinguishable from one named by the `decodeSeriesNames` lookup.
 * They *can* both happen in one legend — `decodeSeriesNames` only resolves the
 * fingerprints its own (bounded) query returned, so a chart routinely mixes
 * resolved `{k="v"}` names with bare `#fp` fallbacks — and two spellings of the
 * same series would read as two different series.
 *
 * `__name__` is dropped, as it is there: it is the chart's subject, identical
 * on every line. Returns `""` when nothing distinguishing is left.
 */
export function liveSampleName(sample: TailSample): string {
  return labelsToName(sample.labels);
}

/**
 * Merge live buckets onto a stored chart, returning a new `MetricsChartData`.
 *
 * The stored half is never modified: every bucket it already has keeps its
 * server-computed value. Live contributes only buckets it owns (already
 * enforced on the way in by `LiveBuckets.push`), extending the axis to the
 * right and extending existing lines — or adding a line for a series that has
 * only appeared since the query ran.
 *
 * `history` may be `null` (no query has completed yet), in which case the
 * result is built entirely from live buckets.
 */
export function mergeLiveIntoChart(
  history: MetricsChartData | null,
  live: LiveBuckets,
  agg: AggFn,
  stepMs: number,
): MetricsChartData | null {
  const snapshot = live.snapshot();
  if (history === null && snapshot.size === 0) return null;

  const step = history?.stepMs ?? stepMs;
  const bucketSet = new Set<number>(history?.buckets ?? []);
  for (const buckets of snapshot.values()) {
    for (const b of buckets.keys()) bucketSet.add(b);
  }
  const allBuckets = Array.from(bucketSet).sort((a, b) => a - b);
  const bucketIndex = new Map<number, number>();
  allBuckets.forEach((b, i) => bucketIndex.set(b, i));

  // Start from the stored lines, re-indexed onto the widened axis.
  const lines = new Map<string, MetricSeriesLine>();
  for (const s of history?.series ?? []) {
    const points: (number | null)[] = new Array(allBuckets.length).fill(null);
    (history?.buckets ?? []).forEach((b, i) => {
      const at = bucketIndex.get(b);
      if (at !== undefined) points[at] = s.points[i] ?? null;
    });
    lines.set(s.key, { key: s.key, name: s.name, points });
  }

  // Overlay the live buckets.
  for (const [key, buckets] of snapshot) {
    let line = lines.get(key);
    if (!line) {
      // A series that exists only in the live half. Prefer a label seen on
      // the wire; fall back to the key so it is at least identifiable.
      line = {
        key,
        name: key === "agg" ? "value" : (live.nameFor(key) ?? key),
        points: new Array(allBuckets.length).fill(null),
      };
      lines.set(key, line);
    }
    for (const [b, acc] of buckets) {
      const at = bucketIndex.get(b);
      if (at !== undefined) line.points[at] = reduceAcc(acc, agg);
    }
  }

  // Keep the stored ordering (peak-sorted by `decodeMetricsChart`) and append
  // live-only series after it, so colours stay put as lines arrive.
  const ordered: MetricSeriesLine[] = [];
  for (const s of history?.series ?? []) {
    const l = lines.get(s.key);
    if (l) {
      ordered.push(l);
      lines.delete(s.key);
    }
  }
  for (const l of lines.values()) ordered.push(l);

  const kept = ordered.slice(0, MAX_SERIES);
  return {
    buckets: allBuckets,
    series: kept,
    stepMs: step,
    total: history?.total ?? 0,
    truncated: (history?.truncated ?? 0) + (ordered.length - kept.length),
  };
}
