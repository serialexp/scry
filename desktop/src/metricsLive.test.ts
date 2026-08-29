import { describe, expect, it } from "vitest";

import {
  LiveBuckets,
  bucketOf,
  liveSampleName,
  liveSeriesKey,
  mergeLiveIntoChart,
  reduceAcc,
} from "./metricsLive";
import type { MetricsChartData } from "./metricsChart";
import type { TailSample } from "./protocol/tail";

const STEP = 10_000; // 10s buckets

/** A sample at `tsMs` with `value`, on series `fp`. */
function sample(tsMs: number, value: number, fp = 1n, labels: [string, string][] = []): TailSample {
  return {
    tsUnixNano: BigInt(tsMs) * 1_000_000n,
    metricType: 2,
    seriesFingerprint: fp,
    value,
    labels,
  };
}

describe("bucketOf", () => {
  it("floors to the step, matching date_bin's epoch origin", () => {
    expect(bucketOf(0n, STEP)).toBe(0);
    expect(bucketOf(9_999n * 1_000_000n, STEP)).toBe(0);
    expect(bucketOf(10_000n * 1_000_000n, STEP)).toBe(10_000);
    expect(bucketOf(10_001n * 1_000_000n, STEP)).toBe(10_000);
    expect(bucketOf(19_999n * 1_000_000n, STEP)).toBe(10_000);
  });
});

describe("reduceAcc", () => {
  const acc = { sum: 10, count: 4, min: 1, max: 6 };

  it("computes every reducer from the accumulator", () => {
    expect(reduceAcc(acc, "sum")).toBe(10);
    expect(reduceAcc(acc, "count")).toBe(4);
    expect(reduceAcc(acc, "min")).toBe(1);
    expect(reduceAcc(acc, "max")).toBe(6);
    expect(reduceAcc(acc, "avg")).toBe(2.5);
  });

  it("avg is exact over the samples, not an average of averages", () => {
    // 1, 2, 6 → true mean 3. A running-mean implementation folding one sample
    // at a time would give ((1+2)/2 + 6)/2 = 3.75.
    const b = new LiveBuckets();
    for (const v of [1, 2, 6]) b.push(sample(0, v), STEP, false, null);
    const a = b.snapshot().get("agg")!.get(0)!;
    expect(reduceAcc(a, "avg")).toBe(3);
  });

  it("avg of an empty accumulator is 0, not NaN", () => {
    expect(reduceAcc({ sum: 0, count: 0, min: 0, max: 0 }, "avg")).toBe(0);
  });
});

describe("LiveBuckets.push — the seam", () => {
  it("drops samples at or before the newest history bucket", () => {
    const b = new LiveBuckets();
    // History covers up to bucket 10_000.
    expect(b.push(sample(5_000, 1), STEP, false, 10_000)).toBe(false);
    expect(b.push(sample(12_000, 1), STEP, false, 10_000)).toBe(false); // bucket 10_000 itself
    expect(b.push(sample(20_000, 1), STEP, false, 10_000)).toBe(true); // bucket 20_000
    expect(b.seriesCount).toBe(1);
    expect(Array.from(b.snapshot().get("agg")!.keys())).toEqual([20_000]);
  });

  it("keeps every bucket when there is no history at all", () => {
    const b = new LiveBuckets();
    expect(b.push(sample(0, 1), STEP, false, null)).toBe(true);
    expect(b.push(sample(50_000, 1), STEP, false, null)).toBe(true);
    expect(b.snapshot().get("agg")!.size).toBe(2);
  });

  it("accumulates min/max/sum/count within one bucket", () => {
    const b = new LiveBuckets();
    for (const v of [5, 1, 9, 3]) b.push(sample(1_000, v), STEP, false, null);
    expect(b.snapshot().get("agg")!.get(0)).toEqual({
      sum: 18,
      count: 4,
      min: 1,
      max: 9,
    });
  });

  it("separates series in grouped mode and merges them otherwise", () => {
    const grouped = new LiveBuckets();
    grouped.push(sample(0, 1, 1n), STEP, true, null);
    grouped.push(sample(0, 2, 2n), STEP, true, null);
    expect(grouped.seriesCount).toBe(2);

    const flat = new LiveBuckets();
    flat.push(sample(0, 1, 1n), STEP, false, null);
    flat.push(sample(0, 2, 2n), STEP, false, null);
    expect(flat.seriesCount).toBe(1);
    expect(flat.snapshot().get("agg")!.get(0)!.count).toBe(2);
  });

  it("refuses a non-positive step rather than dividing by zero", () => {
    const b = new LiveBuckets();
    expect(b.push(sample(0, 1), 0, false, null)).toBe(false);
    expect(b.seriesCount).toBe(0);
  });
});

describe("LiveBuckets.evictBefore", () => {
  it("drops buckets off the left edge and forgets emptied series", () => {
    const b = new LiveBuckets();
    b.push(sample(0, 1, 1n), STEP, true, null);
    b.push(sample(30_000, 2, 1n), STEP, true, null);
    b.push(sample(0, 3, 2n), STEP, true, null); // series 2 is entirely old

    b.evictBefore(20_000);

    expect(b.seriesCount).toBe(1);
    const remaining = b.snapshot().get(liveSeriesKey(sample(0, 0, 1n), true))!;
    expect(Array.from(remaining.keys())).toEqual([30_000]);
  });
});

describe("liveSampleName", () => {
  it("renders __name__ with its other labels", () => {
    expect(
      liveSampleName(sample(0, 1, 1n, [["__name__", "reqs"], ["job", "api"]])),
    ).toBe("reqs{job=api}");
  });

  it("handles a bare metric name and a nameless series", () => {
    expect(liveSampleName(sample(0, 1, 1n, [["__name__", "reqs"]]))).toBe("reqs");
    expect(liveSampleName(sample(0, 1, 1n, [["job", "api"]]))).toBe("job=api");
    expect(liveSampleName(sample(0, 1, 1n, []))).toBe("");
  });
});

describe("mergeLiveIntoChart", () => {
  const history: MetricsChartData = {
    buckets: [0, 10_000],
    series: [{ key: "agg", name: "value", points: [1, 2] }],
    stepMs: STEP,
    total: 2,
    truncated: 0,
  };

  it("returns null when there is neither history nor live data", () => {
    expect(mergeLiveIntoChart(null, new LiveBuckets(), "avg", STEP)).toBeNull();
  });

  it("returns history unchanged in value when there is no live data", () => {
    const merged = mergeLiveIntoChart(history, new LiveBuckets(), "avg", STEP)!;
    expect(merged.buckets).toEqual([0, 10_000]);
    expect(merged.series[0]!.points).toEqual([1, 2]);
  });

  it("extends the axis to the right and never rewrites a stored bucket", () => {
    const live = new LiveBuckets();
    live.push(sample(20_000, 7), STEP, false, 10_000);
    const merged = mergeLiveIntoChart(history, live, "avg", STEP)!;

    expect(merged.buckets).toEqual([0, 10_000, 20_000]);
    // Stored values intact; the live bucket appended.
    expect(merged.series[0]!.points).toEqual([1, 2, 7]);
  });

  it("builds a chart from live data alone when no query has completed", () => {
    const live = new LiveBuckets();
    live.push(sample(0, 4), STEP, false, null);
    live.push(sample(0, 6), STEP, false, null);
    const merged = mergeLiveIntoChart(null, live, "avg", STEP)!;
    expect(merged.buckets).toEqual([0]);
    expect(merged.series[0]!.points).toEqual([5]);
  });

  it("adds a line for a series that only exists live, keeping stored order", () => {
    const grouped: MetricsChartData = {
      buckets: [0],
      series: [{ key: "#00000001", name: "old", points: [1] }],
      stepMs: STEP,
      total: 1,
      truncated: 0,
    };
    const live = new LiveBuckets();
    live.push(sample(10_000, 5, 2n, [["__name__", "new"]]), STEP, true, 0);
    const merged = mergeLiveIntoChart(grouped, live, "max", STEP)!;

    expect(merged.series).toHaveLength(2);
    // The stored series stays first, so its palette colour doesn't shift.
    expect(merged.series[0]!.key).toBe("#00000001");
    expect(merged.series[0]!.points).toEqual([1, null]);
    expect(merged.series[1]!.name).toBe("new");
    expect(merged.series[1]!.points).toEqual([null, 5]);
  });

  it("applies the chosen reducer to live buckets", () => {
    const live = new LiveBuckets();
    for (const v of [2, 8]) live.push(sample(20_000, v), STEP, false, 10_000);
    const avg = mergeLiveIntoChart(history, live, "avg", STEP)!;
    const max = mergeLiveIntoChart(history, live, "max", STEP)!;
    const count = mergeLiveIntoChart(history, live, "count", STEP)!;
    expect(avg.series[0]!.points[2]).toBe(5);
    expect(max.series[0]!.points[2]).toBe(8);
    expect(count.series[0]!.points[2]).toBe(2);
  });
});
