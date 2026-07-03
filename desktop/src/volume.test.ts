import { describe, expect, it } from "vitest";

import {
  STEP_LADDER_MS,
  TARGET_BUCKETS,
  chooseStepMs,
  snapQuickRangeNs,
  stepIntervalSql,
} from "./volume";

describe("chooseStepMs", () => {
  it("keeps the bucket count at or under the target for each ladder rung", () => {
    for (const span of [
      5 * 60_000, // 5m
      15 * 60_000, // 15m
      60 * 60_000, // 1h
      6 * 60 * 60_000, // 6h
      24 * 60 * 60_000, // 24h
      7 * 24 * 60 * 60_000, // 7d
    ]) {
      const step = chooseStepMs(span);
      expect(span / step).toBeLessThanOrEqual(TARGET_BUCKETS);
    }
  });

  it("picks the smallest rung that satisfies the target (5m → 5s)", () => {
    // 5 minutes / 120 buckets = 2.5s, so the 5s rung is the smallest that fits.
    expect(chooseStepMs(5 * 60_000)).toBe(5_000);
  });

  it("clamps to the widest rung for very long ranges", () => {
    const widest = STEP_LADDER_MS[STEP_LADDER_MS.length - 1];
    expect(chooseStepMs(365 * 24 * 60 * 60_000)).toBe(widest);
  });

  it("falls back to the smallest rung for a degenerate span", () => {
    expect(chooseStepMs(0)).toBe(STEP_LADDER_MS[0]);
    expect(chooseStepMs(-1)).toBe(STEP_LADDER_MS[0]);
  });
});

describe("stepIntervalSql", () => {
  it("renders whole-second DataFusion INTERVAL literals", () => {
    expect(stepIntervalSql(5_000)).toBe("INTERVAL '5 seconds'");
    expect(stepIntervalSql(3_600_000)).toBe("INTERVAL '3600 seconds'");
  });
});

describe("snapQuickRangeNs", () => {
  it("snaps tsMax down to a bucket boundary and keeps the span exact", () => {
    const spanMs = 60 * 60_000; // 1h → step 30s (3600s/120)
    const step = chooseStepMs(spanMs);
    // A 'now' deliberately off a bucket boundary.
    const nowMs = 1_700_000_123_456;
    const { tsMinNs, tsMaxNs } = snapQuickRangeNs(nowMs, spanMs);
    const tsMaxMs = Number(tsMaxNs / 1_000_000n);
    // Snapped down to a multiple of the step.
    expect(tsMaxMs % step).toBe(0);
    expect(tsMaxMs).toBeLessThanOrEqual(nowMs);
    // Span preserved exactly.
    expect(Number((tsMaxNs - tsMinNs) / 1_000_000n)).toBe(spanMs);
  });

  it("is stable across refreshes within one bucket (cache-friendly)", () => {
    const spanMs = 60 * 60_000;
    const step = chooseStepMs(spanMs);
    // A bucket-aligned base so both samples fall inside the same window.
    const base = Math.floor(1_700_000_000_000 / step) * step;
    const a = snapQuickRangeNs(base + 1, spanMs);
    const b = snapQuickRangeNs(base + step - 1, spanMs);
    expect(a.tsMinNs).toBe(b.tsMinNs);
    expect(a.tsMaxNs).toBe(b.tsMaxNs);
  });
});
