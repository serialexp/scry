import type { FleetInstance } from "../protocol/client";

export type FleetField = readonly [label: string, value: string];
type Data = Record<string, unknown>;

function object(value: unknown): Data {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Data
    : {};
}

function number(data: Data, key: string): number | null {
  const value = data[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function boolean(data: Data, key: string): boolean | null {
  const value = data[key];
  return typeof value === "boolean" ? value : null;
}

function count(value: number | null): string {
  return value === null ? "—" : value.toLocaleString();
}

function duration(seconds: number | null): string {
  if (seconds === null) return "—";
  if (seconds < 60) return `${seconds.toFixed(0)}s`;
  if (seconds < 3_600) return `${(seconds / 60).toFixed(1)}m`;
  if (seconds < 86_400) return `${(seconds / 3_600).toFixed(1)}h`;
  return `${(seconds / 86_400).toFixed(1)}d`;
}

function milliseconds(value: number | null): string {
  return value === null ? "—" : `${value.toLocaleString(undefined, { maximumFractionDigits: 1 })} ms`;
}

function bytes(value: number | null): string {
  if (value === null) return "—";
  if (value < 1024) return `${value.toFixed(0)} B`;
  const kib = value / 1024;
  if (kib < 1024) return `${kib.toFixed(1)} KiB`;
  const mib = kib / 1024;
  return mib < 1024 ? `${mib.toFixed(1)} MiB` : `${(mib / 1024).toFixed(2)} GiB`;
}

function hitRate(data: Data): string {
  const hits = number(data, "hits") ?? 0;
  const misses = number(data, "misses") ?? 0;
  const total = hits + misses;
  return total === 0 ? "—" : `${(hits * 100 / total).toFixed(0)}%`;
}

function genericFields(data: Data): FleetField[] {
  return Object.entries(data)
    .flatMap(([key, value]): FleetField[] => {
      if (typeof value === "boolean") return [[key.replaceAll("_", " "), value ? "yes" : "no"]];
      if (typeof value === "number") return [[key.replaceAll("_", " "), count(value)]];
      if (typeof value === "string") return [[key.replaceAll("_", " "), value]];
      return [];
    })
    .slice(0, 10);
}

function sumOrMissing(...values: Array<number | null>): number | null {
  const present = values.filter((value): value is number => value !== null);
  return present.length === 0 ? null : present.reduce((sum, value) => sum + value, 0);
}

function ingestFields(data: Data): FleetField[] {
  const hasCompaction = data.compaction !== null && typeof data.compaction === "object";
  const compaction = object(data.compaction);
  const lastPass = number(compaction, "last_pass_unix_ms");
  const enabled = boolean(compaction, "enabled");
  return [
    ["active connections", count(number(data, "active_connections"))],
    ["metric samples", count(number(data, "metric_samples"))],
    ["log entries", count(number(data, "log_entries"))],
    ["rejected", count(number(data, "rejected"))],
    ["compaction", !hasCompaction || enabled === null ? "—" : (enabled ? "enabled" : "disabled")],
    ["compaction grace", duration(number(compaction, "grace_secs"))],
    ["compaction passes", count(number(compaction, "passes"))],
    ["compaction merges", count(number(compaction, "merges"))],
    ["blocks compacted", count(number(compaction, "blocks_in"))],
    ["compaction output", bytes(number(compaction, "bytes_out"))],
    ["inputs reaped", count(number(compaction, "reaped"))],
    ["fenced aborts", count(number(compaction, "aborted"))],
    ["lease held", count(number(compaction, "lease_held"))],
    ["lease unavailable", count(number(compaction, "lease_unavailable"))],
    ["compaction failures", count(sumOrMissing(number(compaction, "pass_failed"), number(compaction, "partition_failed")))],
    ["reap failures", count(number(compaction, "reap_failed"))],
    ["last compaction", lastPass === null ? "—" : new Date(lastPass).toLocaleString()],
  ];
}

function gatewayFields(data: Data): FleetField[] {
  const inbound = object(data.inbound);
  const records = object(data.records);
  const sinks = object(data.sinks);
  const fields: FleetField[] = [];
  for (const [key, label] of [
    ["otlp_http", "OTLP HTTP"],
    ["otlp_grpc", "OTLP gRPC"],
    ["prom_remote_write_http", "remote-write HTTP"],
    ["pyroscope_http", "Pyroscope HTTP"],
    ["native_wire", "native wire"],
  ] as const) {
    const protocol = object(inbound[key]);
    fields.push([`${label} accepted`, count(number(protocol, "accepted"))]);
    fields.push([`${label} rejected`, count(number(protocol, "rejected"))]);
  }
  for (const signal of ["logs", "metrics", "traces", "profiles"] as const) {
    fields.push([`${signal} mapped`, count(number(records, signal))]);
  }
  for (const [name, raw] of Object.entries(sinks)) {
    const sink = object(raw);
    const signals = object(sink.signals);
    let enqueued: number | null = null;
    let dropped: number | null = null;
    let delivered: number | null = null;
    let failed: number | null = null;
    let retries: number | null = null;
    for (const rawSignal of Object.values(signals)) {
      const signal = object(rawSignal);
      enqueued = sumOrMissing(enqueued, number(signal, "enqueued"));
      dropped = sumOrMissing(dropped, number(signal, "dropped_full"), number(signal, "dropped_closed"));
      delivered = sumOrMissing(delivered, number(signal, "delivered"));
      failed = sumOrMissing(failed, number(signal, "failed"), number(signal, "partial_failure"));
      retries = sumOrMissing(retries, number(signal, "retries"));
    }
    fields.push([`${name} queue`, `${count(number(sink, "queue_depth"))} / ${count(number(sink, "queue_capacity"))}`]);
    fields.push([`${name} enqueued`, count(enqueued)]);
    fields.push([`${name} queue dropped`, count(dropped)]);
    fields.push([`${name} delivered`, count(delivered)]);
    fields.push([`${name} failed / partial`, count(failed)]);
    fields.push([`${name} retries`, count(retries)]);
  }
  return fields;
}

function queryFields(data: Data): FleetField[] {
  const latency = object(data.query_latency);
  const ranges = object(data.query_ranges);
  const admission = object(data.admission);
  const recovery = object(data.recovery);
  return [
    ["queries", count(number(data, "queries_total"))],
    ["in flight", count(number(data, "queries_in_flight"))],
    ["errors", count(number(data, "query_errors_total"))],
    ["average latency", milliseconds(number(data, "avg_query_ms"))],
    ["latency p95 ≤", milliseconds(number(latency, "p95_ms_upper"))],
    ["latency p99 ≤", milliseconds(number(latency, "p99_ms_upper"))],
    ["average range", duration(number(ranges, "average_seconds"))],
    ["maximum range", duration(number(ranges, "max_seconds"))],
    ["defaulted ranges", count(number(ranges, "defaulted_total"))],
    ["unbounded starts", count(number(ranges, "unbounded_start_total"))],
    ["memory reserved", bytes(number(data, "memory_reserved_bytes"))],
    ["observed memory high-water", bytes(number(data, "memory_observed_peak_reserved_bytes"))],
    ["admission waiting", count(number(admission, "waiting"))],
    ["admission max wait", milliseconds(number(admission, "max_wait_ms"))],
    ["admission rejected", count(sumOrMissing(number(admission, "timeouts_total"), number(admission, "rejected_total")))],
    ["response resets", count(number(recovery, "response_resets_total"))],
    ["repair attempts", count(number(recovery, "repair_attempts_total"))],
    ["repair failures", count(number(recovery, "repair_failures_total"))],
    ["postings hit rate", hitRate(object(data.postings_cache))],
    ["result hit rate", hitRate(object(data.result_cache))],
    ["catalog blocks", count(number(data, "catalog_blocks"))],
    ["lineage claims", count(number(data, "catalog_lineage_rows"))],
  ];
}

export function fleetFields(instance: FleetInstance): FleetField[] {
  if (instance.role === "ingest") return ingestFields(instance.data);
  if (instance.role === "query") return queryFields(instance.data);
  if (instance.role === "gateway") return gatewayFields(instance.data);
  return genericFields(instance.data);
}
