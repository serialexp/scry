import { describe, expect, it } from "vitest";
import type { FleetInstance } from "../protocol/client";
import { fleetVersion } from "./Fleet";
import { fleetFields } from "./fleetFields";

function instance(role: string, data: Record<string, unknown>): FleetInstance {
  return {
    role,
    instance_id: `${role}-1`,
    addr: "127.0.0.1",
    now_unix_ms: Date.now(),
    uptime_secs: 1,
    rss_kib: 1,
    data,
  };
}

describe("fleetFields", () => {
  it("renders nested ingest compaction telemetry", () => {
    const fields = new Map(fleetFields(instance("ingest", {
      active_connections: 2,
      compaction: {
        enabled: true,
        grace_secs: 600,
        passes: 4,
        merges: 3,
        blocks_in: 24,
        bytes_out: 1_048_576,
        reaped: 16,
        reap_failed: 1,
        pass_failed: 2,
        partition_failed: 3,
      },
    })));

    expect(fields.get("compaction")).toBe("enabled");
    expect(fields.get("compaction grace")).toBe("10.0m");
    expect(fields.get("compaction merges")).toBe("3");
    expect(fields.get("compaction output")).toBe("1.0 MiB");
    expect(fields.get("compaction failures")).toBe("5");
  });

  it("renders compact resource telemetry", () => {
    const fields = new Map(fleetFields(instance("compact", {
      compaction: {
        enabled: true,
        resource_failed: 3,
        resources: {
          memory_budget_bytes: 1_073_741_824,
          datafusion_reserved_bytes: 134_217_728,
          datafusion_peak_bytes: 201_326_592,
          weighted_running_bytes: 268_435_456,
          weighted_peak_bytes: 402_653_184,
          weighted_waiters: 2,
          spill_used_bytes: 536_870_912,
          spill_peak_bytes: 805_306_368,
          spill_limit_bytes: 4_294_967_296,
          admissions: 12,
          rejected: 3,
        },
      },
    })));
    expect(fields.get("resource deferrals")).toBe("3");
    expect(fields.get("memory budget")).toBe("1.00 GiB");
    expect(fields.get("DataFusion reserved")).toBe("128.0 MiB");
    expect(fields.get("DataFusion peak")).toBe("192.0 MiB");
    expect(fields.get("merge memory peak")).toBe("384.0 MiB");
    expect(fields.get("merge memory waiters")).toBe("2");
    expect(fields.get("spill usage")).toBe("512.0 MiB / 4.00 GiB");
    expect(fields.get("spill peak")).toBe("768.0 MiB");
    expect(fields.get("resource rejected")).toBe("3");
  });

  it("renders nested query telemetry and cache rates", () => {
    const fields = new Map(fleetFields(instance("query", {
      queries_total: 10,
      query_errors_total: 2,
      avg_query_ms: 12.5,
      query_latency: { p95_ms_upper: 500, p99_ms_upper: 1_000 },
      query_ranges: { average_seconds: 7_200, max_seconds: 86_400, defaulted_total: 4, unbounded_start_total: 1 },
      memory_reserved_bytes: 1_048_576,
      memory_observed_peak_reserved_bytes: 2_097_152,
      admission: { waiting: 1, max_wait_ms: 25, timeouts_total: 2, rejected_total: 3 },
      recovery: { response_resets_total: 4, repair_attempts_total: 5, repair_failures_total: 1 },
      postings_cache: { hits: 9, misses: 1 },
      result_cache: { hits: 1, misses: 1 },
      label_suggestions: {
        resident_bytes_estimate: 65_536,
        names: 12,
        values: 345,
        saturated_labels: 2,
        fill_failures: 1,
      },
    })));

    expect(fields.get("latency p95 ≤")).toBe("500 ms");
    expect(fields.get("average range")).toBe("2.0h");
    expect(fields.get("maximum range")).toBe("1.0d");
    expect(fields.get("observed memory high-water")).toBe("2.0 MiB");
    expect(fields.get("label suggestions memory")).toBe("64.0 KiB");
    expect(fields.get("label suggestion names")).toBe("12");
    expect(fields.get("label suggestion values")).toBe("345");
    expect(fields.get("saturated labels")).toBe("2");
    expect(fields.get("label warm failures")).toBe("1");
    expect(fields.get("admission rejected")).toBe("5");
    expect(fields.get("response resets")).toBe("4");
    expect(fields.get("postings hit rate")).toBe("90%");
  });

  it("renders gateway inbound and per-sink forwarding stages", () => {
    const fields = new Map(fleetFields(instance("gateway", {
      inbound: { otlp_http: { accepted: 12, rejected: 2 } },
      records: { traces: 44 },
      sinks: {
        scry: {
          queue_depth: 3,
          queue_capacity: 16,
          signals: {
            traces: { enqueued: 12, dropped_full: 1, dropped_closed: 0, delivered: 10, failed: 1, partial_failure: 0, retries: 2 },
          },
        },
      },
    })));
    expect(fields.get("OTLP HTTP accepted")).toBe("12");
    expect(fields.get("OTLP HTTP rejected")).toBe("2");
    expect(fields.get("traces mapped")).toBe("44");
    expect(fields.get("scry queue")).toBe("3 / 16");
    expect(fields.get("scry queue dropped")).toBe("1");
    expect(fields.get("scry delivered")).toBe("10");
    expect(fields.get("scry retries")).toBe("2");
  });

  it("does not fabricate zeros for old fleet payloads", () => {
    const ingest = new Map(fleetFields(instance("ingest", { active_connections: 1 })));
    const query = new Map(fleetFields(instance("query", { queries_total: 1 })));
    expect(ingest.get("compaction")).toBe("—");
    expect(ingest.get("compaction failures")).toBe("—");
    expect(query.get("admission rejected")).toBe("—");
  });

  it("keeps scalar fallback fields for unknown roles", () => {
    expect(fleetFields(instance("future", { one: 1, nested: { ignored: true }, ok: true })))
      .toEqual([["one", "1"], ["ok", "yes"]]);
  });

  it("prefers the envelope version and supports legacy agent payloads", () => {
    expect(fleetVersion({ ...instance("agent", { version: "0.17.1" }), version: "0.18.0" }))
      .toBe("0.18.0");
    expect(fleetVersion(instance("agent", { version: "0.17.1" }))).toBe("0.17.1");
    expect(fleetVersion(instance("query", {}))).toBe("—");
  });
});

describe("catalog trend", () => {
  const catalog = (over: Record<string, unknown>) => ({
    catalog: {
      sampled: true,
      blocks: 348_112,
      rows: 9_000_000,
      lineage_rows: 12,
      by_level: [
        { level: 0, blocks: 12_345, rows: 1_000 },
        { level: 1, blocks: 900, rows: 2_000 },
      ],
      trend_window_secs: 3600,
      ...over,
    },
  });

  it("shows a shrinking catalog with an explicit minus sign", () => {
    const fields = new Map(fleetFields(instance("query", catalog({ blocks_per_hour: -441 }))));
    expect(fields.get("block trend")).toBe("−441/h");
    expect(fields.get("catalog blocks")).toBe("348,112");
    expect(fields.get("trend window")).toBe("1.0h");
  });

  it("shows a growing catalog with an explicit plus sign, scaled", () => {
    const fields = new Map(fleetFields(instance("query", catalog({ blocks_per_hour: 1234 }))));
    expect(fields.get("block trend")).toBe("+1.2k/h");
  });

  it("distinguishes measured-and-steady from not-measured", () => {
    const steady = new Map(fleetFields(instance("query", catalog({ blocks_per_hour: 0 }))));
    expect(steady.get("block trend")).toBe("steady");

    // blocks_per_hour absent: the gauge has not accumulated enough samples.
    const unmeasured = new Map(fleetFields(instance("query", catalog({}))));
    expect(unmeasured.get("block trend")).toBe("—");
  });

  it("renders an unsampled gauge as absent rather than an empty catalog", () => {
    const fields = new Map(fleetFields(instance("query", { catalog: { sampled: false } })));
    expect(fields.get("catalog blocks")).toBe("—");
    expect(fields.get("block trend")).toBe("—");
    expect(fields.get("level split")).toBe("—");
  });

  it("breaks blocks down by compaction level, one row per level", () => {
    const fields = new Map(fleetFields(instance("ingest", catalog({ blocks_per_hour: -10 }))));
    expect(fields.get("L0")).toBe("12,345 blocks");
    expect(fields.get("L1")).toBe("900 blocks");
  });

  it("shows per-signal breakdown when by_signal is present", () => {
    const fields = new Map(fleetFields(instance("query", {
      catalog: {
        sampled: true,
        blocks: 341_000,
        rows: 17_000_000_000,
        lineage_rows: 5,
        by_level: [],
        by_signal: [
          { signal: "logs", blocks: 338_000, rows: 16_900_000_000, bytes: 483_183_820_800 },
          { signal: "metrics", blocks: 2_800, rows: 100_000_000, bytes: 1_288_490_188 },
          { signal: "traces", blocks: 200, rows: 500_000, bytes: 83_886_080 },
        ],
      },
    })));
    expect(fields.get("logs")).toBe("338,000 blocks · 450.00 GiB");
    expect(fields.get("metrics")).toBe("2,800 blocks · 1.20 GiB");
    expect(fields.get("traces")).toBe("200 blocks · 80.0 MiB");
  });

  it("shows average block size per level when bytes are present", () => {
    const fields = new Map(fleetFields(instance("ingest", {
      catalog: {
        sampled: true,
        blocks: 1_100,
        rows: 50_000,
        lineage_rows: 2,
        by_level: [
          { level: 0, blocks: 1_000, rows: 10_000, bytes: 1_048_576_000 },
          { level: 1, blocks: 100, rows: 40_000, bytes: 1_048_576_000 },
        ],
        blocks_per_hour: -5,
      },
    })));
    // L0: 1,048,576,000 / 1000 = 1,048,576 = 1.0 MiB avg
    // L1: 1,048,576,000 / 100 = 10,485,760 = 10.0 MiB avg
    expect(fields.get("L0")).toBe("1,000 blocks · 1.0 MiB avg");
    expect(fields.get("L1")).toBe("100 blocks · 10.0 MiB avg");
  });
});

describe("ingest block balance", () => {
  it("keeps merge outputs on the created side and both reapers on the removed side", () => {
    const fields = new Map(fleetFields(instance("ingest", {
      blocks: {
        created: 5,
        uploaded: 3,
        merge_outputs: 2,
        reclaimed: 13,
        compaction_reaped: 8,
        retention_reaped: 5,
        net: -8,
      },
    })));

    expect(fields.get("blocks created")).toBe("5");
    expect(fields.get("  ↳ merge outputs")).toBe("2");
    expect(fields.get("blocks reclaimed")).toBe("13");
    expect(fields.get("  ↳ by retention")).toBe("5");
    expect(fields.get("net blocks (this instance)")).toBe("-8");
  });

  it("separates staged retention work from actually-reaped work", () => {
    const fields = new Map(fleetFields(instance("ingest", {
      retention: {
        passes: 4,
        reaped: 5,
        staged: 3,
        bytes_reaped: 1_048_576,
        reap_failed: 0,
        last_dry_run: false,
      },
    })));

    expect(fields.get("retention reaped")).toBe("5");
    expect(fields.get("retention staged")).toBe("3");
    expect(fields.get("retention freed")).toBe("1.0 MiB");
    expect(fields.get("retention mode")).toBe("applying");
    expect(fields.get("retention passes")).toBe("4");
  });

  it("says when retention is only dry-running, so a zero reap is not read as nothing to do", () => {
    const fields = new Map(fleetFields(instance("ingest", {
      retention: { passes: 9, reaped: 0, candidates: 120, last_dry_run: true },
    })));
    expect(fields.get("retention mode")).toBe("dry run");
    expect(fields.get("retention reaped")).toBe("0");
  });
});

describe("compact role card", () => {
  it("renders compaction-focused metrics and the catalog gauge", () => {
    const fields = new Map(fleetFields(instance("compact", {
      catalog: {
        sampled: true,
        blocks: 347_715,
        rows: 8_000_000,
        lineage_rows: 10,
        by_level: [
          { level: 0, blocks: 340_000, rows: 7_500_000 },
          { level: 1, blocks: 7_000, rows: 450_000 },
          { level: 2, blocks: 715, rows: 50_000 },
        ],
        blocks_per_hour: -2200,
        trend_window_secs: 1800,
        sampled_age_secs: 5,
      },
      blocks: {
        created: 100,
        uploaded: 0,
        merge_outputs: 100,
        reclaimed: 800,
        compaction_reaped: 800,
        retention_reaped: 0,
        net: -700,
      },
      compaction: {
        enabled: true,
        grace_secs: 600,
        passes: 12,
        merges: 100,
        blocks_in: 800,
        blocks_out: 100,
        bytes_out: 52_428_800,
        reaped: 700,
        reap_failed: 3,
        aborted: 1,
        pass_failed: 0,
        partition_failed: 2,
        lease_held: 5,
        lease_unavailable: 0,
        oversized: 4,
        last_pass_unix_ms: 1725100000000,
        last_pass_duration_ms: 4500,
        current_pass_planned: 211,
        current_pass_completed: 45,
      },
    })));

    // Catalog gauge
    expect(fields.get("catalog blocks")).toBe("347,715");
    expect(fields.get("block trend")).toBe("−2.2k/h");
    expect(fields.get("L0")).toBe("340,000 blocks");
    expect(fields.get("L1")).toBe("7,000 blocks");
    expect(fields.get("L2")).toBe("715 blocks");

    // Block balance (a compactor creates only merge outputs, reclaims only via compaction)
    expect(fields.get("blocks created")).toBe("100");
    expect(fields.get("  ↳ merge outputs")).toBe("100");
    expect(fields.get("blocks reclaimed")).toBe("800");
    expect(fields.get("  ↳ by compaction")).toBe("800");
    expect(fields.get("net blocks (this instance)")).toBe("-700");

    // Live progress
    expect(fields.get("current pass")).toBe("compacting 45 / 211");

    // Compaction stats
    expect(fields.get("compaction")).toBe("enabled");
    expect(fields.get("compaction merges")).toBe("100");
    expect(fields.get("blocks compacted")).toBe("800");
    expect(fields.get("compaction output")).toBe("50.0 MiB");
    expect(fields.get("lease held")).toBe("5");
    expect(fields.get("oversized partitions")).toBe("4");
    expect(fields.get("last pass duration")).toBe("4,500 ms");
    expect(fields.get("compaction failures")).toBe("2");
  });

  it("shows idle when no pass is running", () => {
    const fields = new Map(fleetFields(instance("compact", {
      compaction: {
        enabled: true,
        current_pass_planned: 0,
        current_pass_completed: 0,
      },
    })));
    expect(fields.get("current pass")).toBe("idle");
  });
});

describe("mixed-version fleet during a rollout", () => {
  it("still shows counts from a pre-gauge instance's flat keys", () => {
    const fields = new Map(fleetFields(instance("query", {
      catalog_blocks: 350_095,
      catalog_rows: 42,
      catalog_lineage_rows: 7,
    })));

    expect(fields.get("catalog blocks")).toBe("350,095");
    expect(fields.get("catalog rows")).toBe("42");
    expect(fields.get("lineage claims")).toBe("7");
    expect(fields.get("block trend")).toBe(
      "—",
      // An instance that never sampled has no trend to fall back to, and
      // inventing one would be worse than showing none.
    );
  });
});
