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
    })));

    expect(fields.get("latency p95 ≤")).toBe("500 ms");
    expect(fields.get("average range")).toBe("2.0h");
    expect(fields.get("maximum range")).toBe("1.0d");
    expect(fields.get("observed memory high-water")).toBe("2.0 MiB");
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

  it("breaks blocks down by compaction level", () => {
    const fields = new Map(fleetFields(instance("ingest", catalog({ blocks_per_hour: -10 }))));
    expect(fields.get("level split")).toBe("L0 12,345 · L1 900");
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
