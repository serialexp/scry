import { describe, expect, it } from "vitest";
import { tableFromArrays } from "apache-arrow";
import { appendStructuredCapped, decodeStructuredMetricRows, structuredPointFromTail } from "./structuredMetrics";

const descriptor = { id: 1, name: "latency", description: "", unit: "ms", metric_kind: 3, temporality: 2, monotonic: 0, resource_attrs: [], scope_name: "otel", scope_version: "1", scope_attrs: [] };
const common = { descriptor_id: 1, start_unix_nano: 1n, ts_unix_nano: 2n, flags: 0, attributes: [], exemplars: [] };

describe("structured metric boundary", () => {
  it("keeps wire integer scalars as bigint", () => {
    const point = structuredPointFromTail({ tag: 0x55, signal: 1, series_fingerprint: 0xffff_ffff_ffff_ffffn, labels: [], descriptor, point: { value: { type: "ScalarPointV2", value: { tag: 1, ...common, number: { value: { type: "IntegerValueV2", value: { tag: 1, value: -9_007_199_254_740_993n } } } } } } } as any);
    expect(point.kind).toBe("scalar");
    if (point.kind === "scalar") expect(point.number).toEqual({ kind: "integer", value: -9_007_199_254_740_993n });
    expect(point.seriesFingerprint).toBe(0xffff_ffff_ffff_ffffn);
  });

  it("preserves histogram counts and bounds without synthesizing a scalar", () => {
    const point = structuredPointFromTail({ tag: 0x55, signal: 1, series_fingerprint: 1n, labels: [], descriptor, point: { value: { type: "HistogramPointV2", value: { tag: 2, ...common, count: 9_007_199_254_740_993n, has_sum: 1, sum: 4.5, has_min: 0, min: 0, has_max: 1, max: 3, explicit_bounds: [1, 2], bucket_counts: [1n, 2n, 9_007_199_254_740_990n] } } } } as any);
    expect(point).toMatchObject({ kind: "histogram", count: 9_007_199_254_740_993n, sum: 4.5, min: null, max: 3, explicitBounds: [1, 2] });
    expect("value" in point).toBe(false);
  });

  it("decodes nested historical Arrow rows by point kind", () => {
    const table = tableFromArrays({ series_fingerprint: [7n], ts_unix_nano: [12n], descriptor: [{ ...descriptor, metric_kind: 4 }], point: [{ kind: 4, start_unix_nano: 2n, flags: 0, attributes: [], exemplars: [], scalar: null, histogram: null, exponential_histogram: null, summary: { count: 3n, sum: 6, quantiles: [{ quantile: 0.5, value: 2 }] } }] });
    expect(decodeStructuredMetricRows(table as any)).toMatchObject([{ kind: "summary", count: 3n, sum: 6, quantiles: [{ quantile: 0.5, value: 2 }], tsUnixNano: 12n }]);
  });

  it("bounds snapshots without merging series", () => {
    const points = [1n, 2n, 3n].map((fp) => structuredPointFromTail({ tag: 0x55, signal: 1, series_fingerprint: fp, labels: [], descriptor, point: { value: { type: "ScalarPointV2", value: { tag: 1, ...common, number: { value: { type: "DoubleValueV2", value: { tag: 2, value: Number(fp) } } } } } } } as any));
    expect(appendStructuredCapped(points.slice(0, 1), points.slice(1), 2).map((p) => p.seriesFingerprint)).toEqual([2n, 3n]);
  });
});
