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
