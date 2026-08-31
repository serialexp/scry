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

/** A signed rate of change per hour.
 *
 *  The sign is the entire reason this field exists — "is the catalog growing or
 *  shrinking" — so it is always explicit and never left for the reader to infer
 *  from a bare number.
 *
 *  Three distinct outcomes, deliberately not collapsed: `—` (no measurement
 *  yet), `steady` (measured, and not moving), and a signed rate. A daemon that
 *  has just started and one whose catalog is perfectly balanced call for very
 *  different reactions. */
function perHour(value: number | null): string {
  if (value === null) return "—";
  const rounded = Math.round(value);
  if (rounded === 0) return "steady";
  const sign = rounded > 0 ? "+" : "−";
  const magnitude = Math.abs(rounded);
  const scaled = magnitude >= 1000
    ? `${(magnitude / 1000).toFixed(1)}k`
    : `${magnitude}`;
  return `${sign}${scaled}/h`;
}

/** Blocks per compaction level, ascending: `L0 12,345 · L1 900 · L2 40`.
 *
 *  Worth its own row because a flat total hides the state that matters: L0
 *  climbing while the upper levels drain means ingest is outrunning merging,
 *  and the total can sit still through all of it. */
function levelSplit(catalog: Data): string {
  const raw = catalog.by_level;
  if (!Array.isArray(raw) || raw.length === 0) return "—";
  return raw
    .map((entry) => {
      const level = object(entry);
      const n = number(level, "level");
      return `L${n === null ? "?" : n} ${count(number(level, "blocks"))}`;
    })
    .join(" · ");
}

/** The sampled catalog gauge, shown identically on every role that has one.
 *
 *  The block/row/lineage counts fall back to the flat `catalog_*` keys a
 *  pre-gauge daemon publishes. Not for API compatibility — for rolling
 *  deploys, where old and new instances sit in the fleet together and this page
 *  is the thing you are watching the rollout with. The trend genuinely has no
 *  fallback: an old instance never measured one. */
function catalogFields(data: Data): FleetField[] {
  const catalog = object(data.catalog);
  return [
    ["catalog blocks", count(number(catalog, "blocks") ?? number(data, "catalog_blocks"))],
    // The count is sampled, not live. Showing its age keeps a minute-old
    // reading from being read as current — which matters most just after a
    // restart, when the first sample can predate the first block.
    ["reading age", duration(number(catalog, "sampled_age_secs"))],
    ["block trend", perHour(number(catalog, "blocks_per_hour"))],
    // Qualifies the trend: a rate over four minutes and a rate over an hour
    // deserve different amounts of trust.
    ["trend window", duration(number(catalog, "trend_window_secs"))],
    ["level split", levelSplit(catalog)],
    ["catalog rows", count(number(catalog, "rows") ?? number(data, "catalog_rows"))],
    [
      "lineage claims",
      count(number(catalog, "lineage_rows") ?? number(data, "catalog_lineage_rows")),
    ],
  ];
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
  const retention = object(data.retention);
  const balance = object(data.blocks);
  const lastPass = number(compaction, "last_pass_unix_ms");
  const lastRetention = number(retention, "last_pass_unix_ms");
  const enabled = boolean(compaction, "enabled");
  return [
    ["active connections", count(number(data, "active_connections"))],
    ["metric samples", count(number(data, "metric_samples"))],
    ["log entries", count(number(data, "log_entries"))],
    ["rejected", count(number(data, "rejected"))],
    ...catalogFields(data),
    // The balance is this instance's own cumulative flows, which is why it sits
    // beside the trend rather than being expected to equal it: the trend
    // measures the shared catalog, peers included.
    ["blocks created", count(number(balance, "created"))],
    ["  ↳ uploaded", count(number(balance, "uploaded"))],
    ["  ↳ merge outputs", count(number(balance, "merge_outputs"))],
    ["blocks reclaimed", count(number(balance, "reclaimed"))],
    ["  ↳ by compaction", count(number(balance, "compaction_reaped"))],
    ["  ↳ by retention", count(number(balance, "retention_reaped"))],
    ["net blocks (this instance)", count(number(balance, "net"))],
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
    ["retention passes", count(number(retention, "passes"))],
    // Retention is dry-run by default, so a pass that reaped nothing is
    // ambiguous unless the mode is shown next to it.
    ["retention mode", boolean(retention, "last_dry_run") === null
      ? "—"
      : (boolean(retention, "last_dry_run") ? "dry run" : "applying")],
    ["retention reaped", count(number(retention, "reaped"))],
    // Staged means soft-deleted and serving out its grace window: gone from the
    // live set, still occupying the bucket. Kept apart from reaped so freed
    // storage is never over-claimed.
    ["retention staged", count(number(retention, "staged"))],
    ["retention freed", bytes(number(retention, "bytes_reaped"))],
    ["retention reap failures", count(number(retention, "reap_failed"))],
    ["last retention", lastRetention === null ? "—" : new Date(lastRetention).toLocaleString()],
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
    ...catalogFields(data),
  ];
}

function compactFields(data: Data): FleetField[] {
  const compaction = object(data.compaction);
  const balance = object(data.blocks);
  const lastPass = number(compaction, "last_pass_unix_ms");
  const enabled = boolean(compaction, "enabled");
  return [
    ...catalogFields(data),
    // Balance: same shape as ingest, but a pure compactor creates only via
    // merges and reclaims only via compaction reaps — uploaded and retention
    // columns are always 0.
    ["blocks created", count(number(balance, "created"))],
    ["  ↳ merge outputs", count(number(balance, "merge_outputs"))],
    ["blocks reclaimed", count(number(balance, "reclaimed"))],
    ["  ↳ by compaction", count(number(balance, "compaction_reaped"))],
    ["net blocks (this instance)", count(number(balance, "net"))],
    ["compaction", enabled === null ? "—" : (enabled ? "enabled" : "disabled")],
    ["compaction grace", duration(number(compaction, "grace_secs"))],
    ["compaction passes", count(number(compaction, "passes"))],
    ["compaction merges", count(number(compaction, "merges"))],
    ["blocks compacted", count(number(compaction, "blocks_in"))],
    ["compaction output", bytes(number(compaction, "bytes_out"))],
    ["inputs reaped", count(number(compaction, "reaped"))],
    ["fenced aborts", count(number(compaction, "aborted"))],
    ["lease held", count(number(compaction, "lease_held"))],
    ["lease unavailable", count(number(compaction, "lease_unavailable"))],
    ["oversized partitions", count(number(compaction, "oversized"))],
    ["compaction failures", count(sumOrMissing(number(compaction, "pass_failed"), number(compaction, "partition_failed")))],
    ["reap failures", count(number(compaction, "reap_failed"))],
    ["last compaction", lastPass === null ? "—" : new Date(lastPass).toLocaleString()],
    ["last pass duration", milliseconds(number(compaction, "last_pass_duration_ms"))],
  ];
}

export function fleetFields(instance: FleetInstance): FleetField[] {
  if (instance.role === "ingest") return ingestFields(instance.data);
  if (instance.role === "query") return queryFields(instance.data);
  if (instance.role === "gateway") return gatewayFields(instance.data);
  if (instance.role === "compact") return compactFields(instance.data);
  return genericFields(instance.data);
}
