//! Application state — a single SolidJS store for the query form + run
//! status, plus a signal holding the last result table.
//!
//! Per the project's state convention we use a store rather than prop
//! drilling; components import `state` and the action functions directly.
//! The Arrow `Table` is deliberately kept in a `createSignal`, not the
//! store: stores deeply proxy their contents, and an Arrow table is an
//! opaque, getter-heavy object that must not be proxied.

import { createSignal } from "solid-js";
import { createStore } from "solid-js/store";
import type { Table } from "apache-arrow";

import { Signal, type SignalName } from "./protocol/constants";
import type { Transport } from "./protocol/transport";
import { UnauthorizedError } from "./protocol/transport-http";
import { isTauri } from "./env";
import {
  runQuery,
  QueryError,
  fetchLabelNames,
  fetchLabelValues,
  fetchFleetStatus,
  type QuerySpec,
  type MetaScope,
  type FleetInstance,
} from "./protocol/client";
import { LiveUnavailableError } from "./protocol/transport";
import {
  TailError,
  equalityMatcher,
  runTail,
} from "./protocol/tail";
import {
  MAX_LOG_ROWS,
  appendCapped,
  decodeLogRows,
  isAfterSeam,
  isLogTable,
  newestTs,
  tailRecordToLogRow,
  type LogRow,
} from "./logs";
import { severity, severityRank } from "./severity";
import {
  chooseStepMs,
  snapQuickRangeNs,
  stepIntervalSql,
  type VolumeData,
  type VolumeSeries,
} from "./volume";
import {
  decodeMetricsChart,
  decodeSeriesNames,
  type AggFn,
  type MetricsChartData,
} from "./metricsChart";

export type { AggFn } from "./metricsChart";

export interface MatcherRow {
  name: string;
  value: string;
}

export type RunStatus = "idle" | "running" | "done" | "error";

export interface FormState {
  /** `host:port` of the scry-queryd daemon (desktop/native transport only). */
  addr: string;
  /** Selected query target **id** (browser only; resolved server-side against
   *  the `--queryd` allowlist). Empty ⇒ the server's default target. */
  target: string;
  signal: SignalName;
  matchers: MatcherRow[];
  /** Inclusive lower time bound, unix nanos (raw text; empty = none). */
  tsMin: string;
  /** Inclusive upper time bound, unix nanos (raw text; empty = none). */
  tsMax: string;
  /** SQL against the registered table (empty = `SELECT *`). */
  sql: string;
  /** Row cap (raw text; empty/0 = no limit). Ignored when `sql` is set. */
  limit: string;
  /** Hex trace id (32 hex chars), traces signal only. */
  traceId: string;
  // ── run outcome (scalars only; the table lives in a signal) ──────
  status: RunStatus;
  error: string | null;
  rowCount: number;
  totalRows: bigint;
  elapsedMs: number;
}

const INITIAL_RANGE_MS = 15 * 60_000;
const initialRange = snapQuickRangeNs(Date.now(), INITIAL_RANGE_MS);

const INITIAL: FormState = {
  addr: "127.0.0.1:4100",
  target: "",
  signal: "Metrics",
  matchers: [{ name: "", value: "" }],
  // Start bounded: loading Explore must never ask queryd to discover fields or
  // scan data across the entire bucket. The user can still clear or widen it.
  tsMin: String(initialRange.tsMinNs),
  tsMax: String(initialRange.tsMaxNs),
  sql: "",
  limit: "1000",
  traceId: "",
  status: "idle",
  error: null,
  rowCount: 0,
  totalRows: 0n,
  elapsedMs: 0,
};

const [state, setState] = createStore<FormState>({ ...INITIAL });
const [resultTable, setResultTable] = createSignal<Table | null>(null);

/** How the current result should be rendered. `"frames"` is the traces
 *  frames-overview aggregate (one row per frame); `"default"` is everything
 *  else (the per-signal views + generic table). Set by the action that issued
 *  the query, so the view dispatch doesn't have to sniff column names. */
const [resultKind, setResultKind] = createSignal<"default" | "frames">("default");

export { state, resultTable, resultKind };

// ── Inspector selection ──────────────────────────────────────────────
//
// The Explore inspector rail shows the currently-selected result item. Only
// logs carry a purpose-built inspector for now; other signals clear it. Kept
// in a signal (not the store) so it can hold the raw label/attr tuples.

export interface InspectorLog {
  kind: "log";
  ts: bigint;
  sev: number;
  body: string;
  /** Stream labels (service identity). */
  labels: [string, string][];
  /** Per-entry attributes (stream, trace_id, …). */
  attrs: [string, string][];
}

export type InspectorItem = InspectorLog;

const [selected, setSelectedSig] = createSignal<InspectorItem | null>(null);
export { selected };

/** Set (or clear) the inspector selection. */
export function setSelected(item: InspectorItem | null): void {
  setSelectedSig(item);
}

// ── Quick time-range presets ─────────────────────────────────────────
//
// The query bar's range pills. Centralized here (not in a component) so the
// active preset is shared state: applying a preset stamps `activeRange`,
// while any manual ts edit clears it (see `setField`).

/** Quick time-range presets: label → span in milliseconds. */
export const QUICK_RANGES: { label: string; ms: number }[] = [
  { label: "5m", ms: 5 * 60_000 },
  { label: "15m", ms: 15 * 60_000 },
  { label: "1h", ms: 60 * 60_000 },
  { label: "6h", ms: 6 * 60 * 60_000 },
  { label: "24h", ms: 24 * 60 * 60_000 },
  { label: "7d", ms: 7 * 24 * 60 * 60_000 },
];

/** The label of the currently-applied quick range, or null when the bounds
 *  were set manually / cleared. Drives the range-pill active highlight. */
const [activeRange, setActiveRange] = createSignal<string | null>("15m");
export { activeRange };

/** Set ts_min/ts_max to [now - span, now] in unix nanoseconds, snapping the
 *  upper bound down to the range's bucket step so repeated refreshes within a
 *  bucket hit the queryd result cache. Stamps `activeRange` for the pill UI. */
export function applyQuickRange(ms: number, label: string): void {
  const { tsMinNs, tsMaxNs } = snapQuickRangeNs(Date.now(), ms);
  setState({ tsMin: String(tsMinNs), tsMax: String(tsMaxNs) });
  setActiveRange(label);
  void refreshLabels();
  if (state.signal === "Logs") void runLogVolume();
  if (state.signal === "Metrics") void runMetricsChart();
}

/** Clear both time bounds (and the active-range highlight). */
export function clearTimeRange(): void {
  setState({ tsMin: "", tsMax: "" });
  setActiveRange(null);
}

// ── Auth (browser only) ──────────────────────────────────────────────
//
// The desktop (Tauri) shell talks straight to the daemon over a native
// socket — no gate. The browser shell goes through `scry-webui`, which
// requires a password → signed-cookie session. `inBrowser` decides which.

/** True when running in a browser tab (vs the Tauri desktop window). */
export const inBrowser = !isTauri();

// `authed`: is there a usable session? Desktop is always authed. `authChecked`:
// has the initial `/api/me` probe completed (avoids a login-screen flash on a
// page load that already has a valid cookie)? Desktop needs no probe.
const [authed, setAuthed] = createSignal(!inBrowser);
const [authChecked, setAuthChecked] = createSignal(!inBrowser);
export { authed, authChecked };

/** One selectable upstream as exposed by `GET /api/targets` (browser only).
 *  Only `id` + `label` cross the wire — the raw address stays server-side. */
export interface TargetInfo {
  id: string;
  label: string;
  /** Whether `scry web` has a `--queryd-tail` address for this target, i.e.
   *  whether live tailing is possible against it. Absent on older servers. */
  live?: boolean;
}

// The target allowlist fetched from `scry-webui` after login (browser only).
const [targets, setTargets] = createSignal<TargetInfo[]>([]);
export { targets };

/** Fetch the configured query targets and seed the form selection with the
 *  server's default. Browser only; a no-op (and harmless) under Tauri. */
export async function fetchTargets(): Promise<void> {
  if (!inBrowser) return;
  try {
    const res = await fetch("/api/targets", { credentials: "same-origin" });
    if (!res.ok) return;
    const body = (await res.json()) as { targets: TargetInfo[]; default: string };
    setTargets(body.targets);
    // Seed the selection with the server default unless the user already picked
    // one that's still valid.
    const ids = new Set(body.targets.map((t) => t.id));
    if (!ids.has(state.target)) {
      setState("target", body.default ?? "");
    }
  } catch {
    // Leave targets empty; the relay still works against the server default.
  }
}

/** Probe the existing session cookie once on startup (browser only). */
export async function checkSession(): Promise<void> {
  if (!inBrowser) return;
  try {
    const res = await fetch("/api/me", { credentials: "same-origin" });
    const ok = res.status === 204;
    setAuthed(ok);
    if (ok) await fetchTargets();
  } catch {
    setAuthed(false);
  } finally {
    setAuthChecked(true);
  }
}

/** Attempt a login; returns true on success. */
export async function login(password: string): Promise<boolean> {
  const res = await fetch("/api/login", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ password }),
    credentials: "same-origin",
  });
  const ok = res.status === 204;
  setAuthed(ok);
  if (ok) await fetchTargets();
  return ok;
}

/** Clear the session and drop back to the login screen. */
export async function logout(): Promise<void> {
  try {
    await fetch("/api/logout", { method: "POST", credentials: "same-origin" });
  } finally {
    setAuthed(false);
  }
}

// ── Field + matcher mutators ─────────────────────────────────────────

export function setField<K extends keyof FormState>(key: K, value: FormState[K]): void {
  setState(key, value);
  // A manual time-bound edit means we're no longer on a named preset.
  if (key === "tsMin" || key === "tsMax") setActiveRange(null);
}

export function addMatcher(): void {
  setState("matchers", (m) => [...m, { name: "", value: "" }]);
}

export function removeMatcher(index: number): void {
  setState("matchers", (m) => (m.length <= 1 ? m : m.filter((_, i) => i !== index)));
}

export function setMatcher(index: number, field: keyof MatcherRow, value: string): void {
  setState("matchers", index, field, value);
}

/** Remove the matcher at `index` outright (unlike `removeMatcher`, which keeps
 *  a minimum of one row for the sidebar form). Collapses to a single blank row
 *  when the last matcher is deleted, so the store invariant (≥1 row) holds. */
export function deleteMatcher(index: number): void {
  setState("matchers", (m) => {
    const next = m.filter((_, i) => i !== index);
    return next.length === 0 ? [{ name: "", value: "" }] : next;
  });
}

/** Add (or fill) a `name=value` matcher from the label browser. Reuses the
 *  first fully-blank row if there is one, else appends; a no-op if the exact
 *  pair is already present. */
export function applyLabelMatcher(name: string, value: string): void {
  const rows = state.matchers;
  if (rows.some((m) => m.name === name && m.value === value)) return;
  const blank = rows.findIndex((m) => m.name.trim() === "" && m.value.trim() === "");
  if (blank >= 0) {
    setState("matchers", blank, { name, value });
  } else {
    setState("matchers", (m) => [...m, { name, value }]);
  }
}

// ── Run ──────────────────────────────────────────────────────────────

// Pick the transport for the current shell, lazily and once. The Tauri adapter
// statically imports `@tauri-apps/api`, so it's loaded via dynamic `import()`
// only when actually running under Tauri — keeping it out of the browser bundle.
let transportPromise: Promise<Transport> | null = null;

function getTransport(): Promise<Transport> {
  if (!transportPromise) {
    transportPromise = isTauri()
      ? import("./protocol/transport-tauri").then((m) => new m.TauriTransport())
      : import("./protocol/transport-http").then((m) => new m.HttpTransport());
  }
  return transportPromise;
}

// ── Fleet status ─────────────────────────────────────────────────────

export type FleetStatus = "idle" | "loading" | "ready" | "error";
const [fleetStatus, setFleetStatus] = createSignal<FleetStatus>("idle");
const [fleetInstances, setFleetInstances] = createSignal<FleetInstance[]>([]);
const [fleetError, setFleetError] = createSignal<string | null>(null);
const [fleetUpdatedAt, setFleetUpdatedAt] = createSignal<number | null>(null);
export { fleetStatus, fleetInstances, fleetError, fleetUpdatedAt };

/** Refresh the complete fleet through the selected queryd. Safe to call from a
 * timer: failures preserve the previous snapshot while exposing the error. */
export async function refreshFleet(): Promise<void> {
  setFleetStatus("loading");
  setFleetError(null);
  try {
    const transport = await getTransport();
    const instances = await fetchFleetStatus(transport, inBrowser ? state.target : state.addr);
    instances.sort(
      (a, b) => a.role.localeCompare(b.role) || a.instance_id.localeCompare(b.instance_id),
    );
    setFleetInstances(instances);
    setFleetUpdatedAt(Date.now());
    setFleetStatus("ready");
  } catch (err) {
    if (err instanceof UnauthorizedError) setAuthed(false);
    setFleetError(err instanceof Error ? err.message : String(err));
    setFleetStatus("error");
  }
}

function parseBigIntOpt(raw: string): bigint | undefined {
  const t = raw.trim();
  if (t === "") return undefined;
  let v: bigint;
  try {
    v = BigInt(t);
  } catch {
    throw new Error(`not an integer: "${raw}"`);
  }
  if (v < 0n) throw new Error(`must be non-negative: "${raw}"`);
  return v;
}

function parseHex16(hex: string): Uint8Array {
  const clean = hex.trim().replace(/^0x/i, "");
  if (clean.length !== 32 || !/^[0-9a-fA-F]+$/.test(clean)) {
    throw new Error("trace id must be exactly 32 hex chars (16 bytes)");
  }
  const out = new Uint8Array(16);
  for (let i = 0; i < 16; i++) {
    out[i] = parseInt(clean.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

/** Build a `QuerySpec` from the current form (throws on invalid input). */
function specFromForm(): QuerySpec {
  const sql = state.sql.trim();
  const matchers = state.matchers
    .map((m) => ({ name: m.name.trim(), value: m.value }))
    .filter((m) => m.name !== "");

  let traceId: Uint8Array | undefined;
  if (state.signal === "Traces" && state.traceId.trim() !== "") {
    traceId = parseHex16(state.traceId);
  }

  return {
    signal: Signal[state.signal],
    matchers,
    tsMin: parseBigIntOpt(state.tsMin),
    tsMax: parseBigIntOpt(state.tsMax),
    sql: sql === "" ? undefined : sql,
    // When SQL is present the server ignores the wire limit (express it
    // in the SQL); only send the limit for the default SELECT *.
    limit: sql === "" ? parseBigIntOpt(state.limit) : undefined,
    traceId,
    // While tailing, the history half should include the ingesters' in-flight
    // ring too (D-054) — otherwise the pane shows a gap between the newest
    // sealed block and the first pushed record.
    live: state.signal === "Logs" && liveActive() ? true : undefined,
  };
}

/** Run a pre-built spec, recording the result under `kind`. Shared by the
 *  form-driven query and the traces frames-overview / drill-in actions. */
async function runSpec(spec: QuerySpec, kind: "default" | "frames"): Promise<void> {
  setState({ status: "running", error: null });
  // A fresh result invalidates the inspector selection (it references old rows).
  setSelectedSig(null);
  try {
    const transport = await getTransport();
    // Desktop dials a raw `host:port`; browser sends a target *id* the server
    // resolves against its allowlist.
    const dest = inBrowser ? state.target.trim() : state.addr.trim();
    const res = await runQuery(transport, dest, spec);
    setResultKind(kind);
    setResultTable(res.table);
    setState({
      status: "done",
      error: null,
      rowCount: res.rowCount,
      totalRows: res.totalRows,
      elapsedMs: res.elapsedMs,
    });
  } catch (e) {
    setResultTable(null);
    // A 401 from the relay means our session lapsed mid-use: drop back to the
    // login screen rather than showing a cryptic query error.
    if (e instanceof UnauthorizedError) {
      setAuthed(false);
      setState({
        status: "error",
        error: "Session expired — please log in again.",
        rowCount: 0,
        totalRows: 0n,
      });
      return;
    }
    const message =
      e instanceof QueryError
        ? e.message
        : e instanceof Error
          ? e.message
          : String(e);
    setState({ status: "error", error: message, rowCount: 0, totalRows: 0n });
  }
}

// ── Label discoverability (D-050) ────────────────────────────────────
//
// "What can I match on?" answered from the daemon's label cache over the
// same transport as queries. Names load for the current signal + time
// window; values load lazily per name. Both caches reset when the scope
// (signal / time / target) changes, guarded by a monotonic sequence so a
// stale in-flight response can't clobber a newer scope.

export type LabelStatus = "idle" | "loading" | "ready" | "error";

const [labelNames, setLabelNames] = createSignal<string[]>([]);
const [labelStatus, setLabelStatus] = createSignal<LabelStatus>("idle");
const [labelError, setLabelError] = createSignal<string | null>(null);
const [labelValues, setLabelValues] = createSignal<Record<string, string[]>>({});
export { labelNames, labelStatus, labelError, labelValues };

/** Signals with a postings/promoted-column label surface. Profiles carry
 *  their labels inside the opaque pprof blob, so metadata is empty there. */
function signalHasLabels(sig: SignalName): boolean {
  return sig === "Metrics" || sig === "Logs" || sig === "Traces";
}

let metaKey = "";
let metaSeq = 0;

function currentMetaScope(): MetaScope {
  let tsMin: bigint | undefined;
  let tsMax: bigint | undefined;
  // Metadata is best-effort: an in-progress (invalid) time entry just means
  // "unbounded on that side" rather than an error.
  try {
    tsMin = parseBigIntOpt(state.tsMin);
  } catch {
    tsMin = undefined;
  }
  try {
    tsMax = parseBigIntOpt(state.tsMax);
  } catch {
    tsMax = undefined;
  }
  return { signal: Signal[state.signal], tsMin, tsMax };
}

function metaDest(): string {
  return inBrowser ? state.target.trim() : state.addr.trim();
}

function scopeKey(scope: MetaScope, dest: string): string {
  return `${dest}|${scope.signal}|${scope.tsMin ?? ""}|${scope.tsMax ?? ""}`;
}

/** Load the label names for the current signal + time window, resetting the
 *  per-name value cache. No-ops when the scope key is unchanged (unless
 *  `force`). Browser mode needs a session + a chosen target first. */
export async function refreshLabels(force = false): Promise<void> {
  if (!signalHasLabels(state.signal)) {
    metaKey = "";
    setLabelNames([]);
    setLabelValues({});
    setLabelStatus("idle");
    return;
  }
  const scope = currentMetaScope();
  const dest = metaDest();
  if (inBrowser && (!authed() || dest === "")) return;

  const key = scopeKey(scope, dest);
  if (!force && key === metaKey) return;
  metaKey = key;
  const seq = ++metaSeq;
  setLabelValues({});
  setLabelError(null);
  setLabelStatus("loading");
  try {
    const transport = await getTransport();
    const names = await fetchLabelNames(transport, dest, scope);
    if (seq !== metaSeq) return; // superseded by a newer scope
    setLabelNames(names);
    setLabelError(null);
    setLabelStatus("ready");
  } catch (e) {
    if (seq !== metaSeq) return;
    const message =
      e instanceof Error ? e.message : String(e);
    setLabelNames([]);
    setLabelError(message);
    setLabelStatus("error");
  }
}

/** Lazily fetch the distinct values for one label `name` under the current
 *  scope, caching them. No-op if already cached or the name is blank. */
export async function ensureLabelValues(name: string): Promise<void> {
  const n = name.trim();
  if (n === "" || !signalHasLabels(state.signal)) return;
  if (labelValues()[n] !== undefined) return;
  const scope = currentMetaScope();
  const dest = metaDest();
  if (inBrowser && (!authed() || dest === "")) return;
  const keyAtStart = metaKey || scopeKey(scope, dest);
  try {
    const transport = await getTransport();
    const values = await fetchLabelValues(transport, dest, scope, n);
    if ((metaKey || keyAtStart) !== keyAtStart) return; // scope changed under us
    setLabelValues((prev) => ({ ...prev, [n]: values }));
  } catch (e) {
    // Leave uncached so a later interaction can retry. Surface the failure too:
    // the metrics picker loads `__name__` without opening the fields strip, and
    // silently swallowing this made a dead queryd look like "no metrics".
    if ((metaKey || keyAtStart) !== keyAtStart) return;
    setLabelError(e instanceof Error ? e.message : String(e));
    setLabelStatus("error");
  }
}

// ── Per-value counts for label drill-down (Part C) ───────────────────
//
// When a label name is expanded in the browser, show how many entries each
// value accounts for *under the current matchers + range* — the Explore
// drill-down. Logs-only: it reads the synthesized `labels` map column
// (`labels['key']`), which metrics results don't carry yet. Counts reset
// whenever the query is (re)run, so they always reflect the active filters.

const [labelValueCounts, setLabelValueCounts] = createSignal<
  Record<string, Record<string, number>>
>({});
export { labelValueCounts };

/** Escape a label key for safe interpolation into `labels['…']`. */
function sqlStrLit(s: string): string {
  return s.replace(/'/g, "''");
}

/** Fetch per-value entry counts for one label `name` under the current
 *  matchers + range (logs only), caching them until the next query run. */
export async function ensureLabelValueCounts(name: string): Promise<void> {
  const n = name.trim();
  if (n === "" || state.signal !== "Logs") return;
  if (labelValueCounts()[n] !== undefined) return;

  let tsMin: bigint | undefined;
  let tsMax: bigint | undefined;
  let matchers: { name: string; value: string }[];
  try {
    tsMin = parseBigIntOpt(state.tsMin);
    tsMax = parseBigIntOpt(state.tsMax);
    matchers = state.matchers
      .map((m) => ({ name: m.name.trim(), value: m.value }))
      .filter((m) => m.name !== "");
  } catch {
    return;
  }
  const dest = metaDest();
  if (inBrowser && (!authed() || dest === "")) return;

  const sql =
    `SELECT labels['${sqlStrLit(n)}'] AS v, count(*) AS c ` +
    `FROM logs GROUP BY v ORDER BY c DESC`;
  try {
    const transport = await getTransport();
    const res = await runQuery(transport, dest, {
      signal: Signal.Logs,
      matchers,
      tsMin,
      tsMax,
      sql,
      requestId: "webui-label-counts",
    });
    const counts: Record<string, number> = {};
    for (const r of res.table.toArray()) {
      const o = (r?.toJSON?.() ?? {}) as Record<string, unknown>;
      const v = o.v;
      if (v === null || v === undefined) continue; // entries lacking the label
      counts[String(v)] = Number(o.c ?? 0);
    }
    setLabelValueCounts((prev) => ({ ...prev, [n]: counts }));
  } catch {
    // Leave uncached so a later expansion can retry.
  }
}

export async function runCurrentQuery(): Promise<void> {
  let spec: QuerySpec;
  try {
    spec = specFromForm();
  } catch (e) {
    setState({ status: "error", error: e instanceof Error ? e.message : String(e) });
    return;
  }
  // Counts are matcher-dependent; invalidate so an expanded name re-fetches
  // against the new filter set.
  setLabelValueCounts({});
  await runSpec(spec, "default");
  // For logs, refresh the volume histogram alongside the table using the same
  // matchers + range. Fire-and-forget: the graph is auxiliary, so a volume
  // failure must never fail the main query. It's cache-backed on the queryd,
  // so a repeated range is ~free.
  if (state.signal === "Logs") void runLogVolume();
  else clearVolume();
  // For metrics, refresh the time-series chart alongside the table.
  if (state.signal === "Metrics") void runMetricsChart();
  else clearMetricsChart();
}

/** The Explore drill-down loop: add a `name=value` matcher, then re-run the
 *  table + volume so the whole view refilters to the selected slice. */
export async function drillLabelValue(name: string, value: string): Promise<void> {
  applyLabelMatcher(name, value);
  await runCurrentQuery();
}

// ── Live tail (D-052/D-053) ──────────────────────────────────────────
//
// A subscription to the ingesters' logs tap, relayed by the query daemon's
// tail front-door, appended to the query result as records arrive.
//
// Order of operations matters. We subscribe *first* and only then run the
// history query, so nothing logged during the query is missed; the price is an
// overlap, which the history's newest timestamp (the "seam") resolves. Doing it
// the other way round would leave a real gap with nothing to reconstruct it
// from.
//
// Everything here is best-effort by construction: the server drops records
// under load rather than backpressuring ingest, and a reconnect after a dropped
// stream loses whatever happened in the gap. That is the tail's contract, not a
// defect in this client — for a complete answer, re-run the query.

/** Rolling ceiling on retained live rows. Bounds the tab, not the server. */
export const LIVE_ROW_CAP = 5000;
/** Coalesce arrivals into at most this many DOM updates per second. */
const LIVE_FLUSH_MS = 250;
/** Reconnect backoff bounds after a stream ends unexpectedly. */
const LIVE_RECONNECT_MIN_MS = 1000;
const LIVE_RECONNECT_MAX_MS = 8000;

export type LiveStatus =
  | "off"
  | "connecting"
  | "streaming"
  | "reconnecting"
  | "error";

const [liveStatus, setLiveStatus] = createSignal<LiveStatus>("off");
const [liveError, setLiveError] = createSignal<string | null>(null);
const [liveRows, setLiveRows] = createSignal<LogRow[]>([]);
const [liveDropped, setLiveDropped] = createSignal(0);
export { liveStatus, liveError, liveRows, liveDropped };

/** Is a live subscription wanted right now? (Distinct from `liveStatus`, which
 *  reports what the connection is actually doing.) */
export const liveActive = () => liveStatus() !== "off" && liveStatus() !== "error";

/** Why live tailing is not available for the current form, or `null` when it
 *  is. Drives the toggle's disabled state and its tooltip. */
export function liveUnavailableReason(): string | null {
  if (state.signal !== "Logs") {
    return "Live tailing is logs-only — the server has no tail for other signals.";
  }
  if (state.sql.trim() !== "") {
    return "Live tailing can't apply custom SQL; clear it to stream.";
  }
  if (inBrowser) {
    const t = targets().find((x) => x.id === state.target);
    // `live === undefined` is an older server that doesn't report the flag;
    // let the attempt happen and surface whatever it says.
    if (t && t.live === false) {
      return "This target has no live endpoint (scry web needs --queryd-tail).";
    }
  }
  return null;
}

// The in-flight subscription: an abort handle, the seam the history query
// established, and rows arrived before that seam was known.
let liveAbort: AbortController | null = null;
let liveSeam: bigint | null = null;
let liveSeamKnown = false;
let liveHeld: LogRow[] = [];
let livePending: LogRow[] = [];
let liveFlushTimer: ReturnType<typeof setTimeout> | null = null;
/** Guards against a stale subscription's callbacks landing after a restart. */
let liveGeneration = 0;

function flushLiveRows(): void {
  liveFlushTimer = null;
  if (livePending.length === 0) return;
  const batch = livePending;
  livePending = [];
  setLiveRows((prev) => {
    const next = appendCapped(prev, batch, LIVE_ROW_CAP);
    const lost = prev.length + batch.length - next.length;
    if (lost > 0) setLiveDropped((n) => n + lost);
    return next;
  });
}

function scheduleLiveFlush(): void {
  if (liveFlushTimer !== null) return;
  liveFlushTimer = setTimeout(flushLiveRows, LIVE_FLUSH_MS);
}

function admitLiveRow(row: LogRow): void {
  if (!liveSeamKnown) {
    // The history query is still running; we don't yet know what it will
    // cover. Hold the record rather than guess.
    liveHeld.push(row);
    return;
  }
  if (!isAfterSeam(row, liveSeam)) return;
  livePending.push(row);
  scheduleLiveFlush();
}

/** Called once the history query lands: release everything held behind it. */
function openLiveSeam(seam: bigint | null): void {
  liveSeam = seam;
  liveSeamKnown = true;
  const held = liveHeld;
  liveHeld = [];
  for (const row of held) {
    if (isAfterSeam(row, seam)) livePending.push(row);
  }
  if (livePending.length > 0) scheduleLiveFlush();
}

/** The matcher specs for a subscription — the query bar's tokens, in the
 *  grammar `scry-match` parses server-side. */
function liveMatchers(): string[] {
  return state.matchers
    .map((m) => ({ name: m.name.trim(), value: m.value }))
    .filter((m) => m.name !== "")
    .map((m) => equalityMatcher(m.name, m.value));
}

/** Connect, and keep reconnecting while the user still wants a live view. */
async function pumpLive(generation: number, signal: AbortSignal): Promise<void> {
  let backoff = LIVE_RECONNECT_MIN_MS;
  const transport = await getTransport();
  const dest = inBrowser ? state.target.trim() : state.addr.trim();

  while (!signal.aborted && generation === liveGeneration) {
    try {
      await runTail(
        transport,
        dest,
        { matchers: liveMatchers() },
        __APP_VERSION__,
        {
          onSubscribed: () => {
            if (generation === liveGeneration) {
              backoff = LIVE_RECONNECT_MIN_MS;
              setLiveStatus("streaming");
              setLiveError(null);
            }
          },
          onRecord: (rec) => {
            if (generation === liveGeneration) admitLiveRow(tailRecordToLogRow(rec));
          },
        },
        signal,
      );
    } catch (e) {
      if (signal.aborted || generation !== liveGeneration) return;
      if (e instanceof UnauthorizedError) {
        setAuthed(false);
        stopLive("Session expired — please log in again.");
        return;
      }
      // A protocol refusal or a missing endpoint will fail identically on every
      // retry, so say so and stop rather than reconnect-looping.
      if (e instanceof TailError || e instanceof LiveUnavailableError) {
        stopLive(e.message);
        return;
      }
      setLiveError(e instanceof Error ? e.message : String(e));
    }
    if (signal.aborted || generation !== liveGeneration) return;

    // The stream ended (server hang-up, relay timeout, network blip). Records
    // logged during the gap are gone — the tail has no replay.
    setLiveStatus("reconnecting");
    await new Promise((r) => setTimeout(r, backoff));
    backoff = Math.min(backoff * 2, LIVE_RECONNECT_MAX_MS);
  }
}

/**
 * Turn the live view on: subscribe, then run the history query underneath it.
 *
 * Safe to call when already live — it restarts cleanly, which is what the
 * matcher/target-changed path wants.
 */
export async function startLive(): Promise<void> {
  const reason = liveUnavailableReason();
  if (reason !== null) {
    setLiveStatus("error");
    setLiveError(reason);
    return;
  }

  stopLive();
  const generation = ++liveGeneration;
  const controller = new AbortController();
  liveAbort = controller;
  liveSeam = null;
  liveSeamKnown = false;
  liveHeld = [];
  livePending = [];
  setLiveRows([]);
  setLiveDropped(0);
  setLiveError(null);
  setLiveStatus("connecting");

  // Subscribe first — anything logged from here on is captured, even while the
  // history query below is still running.
  void pumpLive(generation, controller.signal);

  await runCurrentQuery();
  if (generation !== liveGeneration) return; // restarted while we waited
  const table = resultTable();
  const seam =
    table && isLogTable(table) ? newestTs(decodeLogRows(table, MAX_LOG_ROWS)) : null;
  openLiveSeam(seam);
}

/** Turn the live view off. `reason` (when given) explains an involuntary stop
 *  and puts the toggle in its error state; a plain stop clears it. */
export function stopLive(reason?: string): void {
  liveGeneration++;
  if (liveAbort) {
    liveAbort.abort();
    liveAbort = null;
  }
  if (liveFlushTimer !== null) {
    clearTimeout(liveFlushTimer);
    liveFlushTimer = null;
  }
  livePending = [];
  liveHeld = [];
  liveSeamKnown = false;
  liveSeam = null;
  if (reason !== undefined) {
    setLiveStatus("error");
    setLiveError(reason);
  } else {
    setLiveStatus("off");
    setLiveError(null);
  }
}

/** The Live pill's click handler. */
export function toggleLive(): void {
  if (liveActive()) stopLive();
  else void startLive();
}

// ── Auto-refresh (the non-logs answer to Live) ───────────────────────
//
// The server tails logs only, so the other three signals get the honest
// alternative: re-run the same query on a timer. Cheap for a repeated range
// (queryd caches results), and it is what a dashboard does anyway.
//
// A tick re-applies the *quick range* before re-running, so a "last 15m" view
// slides forward instead of freezing at the minute it was picked. With a
// manually-typed range there is nothing to slide, so we just re-run the same
// bounds — a refresh still picks up newly-flushed blocks inside them.

/** Auto-refresh choices for the query bar's select. `0` = off. */
export const REFRESH_INTERVALS: { label: string; ms: number }[] = [
  { label: "off", ms: 0 },
  { label: "5s", ms: 5_000 },
  { label: "10s", ms: 10_000 },
  { label: "30s", ms: 30_000 },
  { label: "1m", ms: 60_000 },
];

/** The span of a quick-range label, or `null` when it isn't one of ours.
 *  Pure — the slide step of an auto-refresh tick, isolated for testing. */
export function quickRangeMs(label: string | null): number | null {
  if (label === null) return null;
  const hit = QUICK_RANGES.find((r) => r.label === label);
  return hit ? hit.ms : null;
}

const [refreshMs, setRefreshMs] = createSignal(0);
export { refreshMs };

let refreshTimer: ReturnType<typeof setInterval> | null = null;

/** One auto-refresh tick. Exported for tests; the timer is what calls it. */
export function refreshTick(): void {
  // A run already in flight means the interval is shorter than the query takes.
  // Skipping (rather than queueing) keeps a slow query from stacking ticks.
  if (state.status === "running") return;
  // Logs with a live stream attached are already current; a re-run would reset
  // the seam and blank the pane for no gain.
  if (liveActive()) return;
  const ms = quickRangeMs(activeRange());
  if (ms !== null) applyQuickRange(ms, activeRange()!);
  void runCurrentQuery();
}

/** Set (or clear, with `0`) the auto-refresh period. */
export function setRefreshInterval(ms: number): void {
  setRefreshMs(ms);
  if (refreshTimer !== null) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
  if (ms > 0) refreshTimer = setInterval(refreshTick, ms);
}

// ── Logs volume histogram (Part B) ───────────────────────────────────
//
// A count-over-time of log entries split by severity, over the current
// matchers + range. Rides the query wire via a `date_bin` aggregation (no
// protocol change); the result lives in its own signal so the table view is
// untouched. Logs-only — metrics results carry no label/severity column yet.

export type VolumeStatus = "idle" | "loading" | "ready" | "empty" | "error";

const [volumeData, setVolumeData] = createSignal<VolumeData | null>(null);
const [volumeStatus, setVolumeStatus] = createSignal<VolumeStatus>("idle");
export { volumeData, volumeStatus };

/** Monotonic guard so a slow volume response can't clobber a newer one. */
let volumeSeq = 0;

function clearVolume(): void {
  volumeSeq++;
  setVolumeData(null);
  setVolumeStatus("idle");
}

/** Run the log-volume aggregation for the current form (matchers + range) and
 *  decode it into the `volumeData` signal. Requires an explicit [ts_min,
 *  ts_max] range (like Grafana Explore) so the bucket step is well-defined and
 *  the range is a closed, cacheable window. */
export async function runLogVolume(): Promise<void> {
  if (state.signal !== "Logs") {
    clearVolume();
    return;
  }

  let tsMin: bigint | undefined;
  let tsMax: bigint | undefined;
  let matchers: { name: string; value: string }[];
  try {
    tsMin = parseBigIntOpt(state.tsMin);
    tsMax = parseBigIntOpt(state.tsMax);
    matchers = state.matchers
      .map((m) => ({ name: m.name.trim(), value: m.value }))
      .filter((m) => m.name !== "");
  } catch {
    setVolumeStatus("error");
    setVolumeData(null);
    return;
  }

  // Need a bounded range to pick a bucket width. Without one, skip quietly —
  // the panel prompts the user to choose a range.
  if (tsMin === undefined || tsMax === undefined || tsMax <= tsMin) {
    clearVolume();
    setVolumeStatus("empty");
    return;
  }

  const spanMs = Number((tsMax - tsMin) / 1_000_000n);
  const stepMs = chooseStepMs(spanMs);
  const sql =
    `SELECT CAST(date_bin(${stepIntervalSql(stepMs)}, ` +
    `to_timestamp_nanos(ts_unix_nano)) AS BIGINT) AS bucket_ns, ` +
    `severity, count(*) AS n FROM logs GROUP BY bucket_ns, severity ORDER BY bucket_ns`;

  const seq = ++volumeSeq;
  setVolumeStatus("loading");
  try {
    const transport = await getTransport();
    const dest = inBrowser ? state.target.trim() : state.addr.trim();
    const res = await runQuery(transport, dest, {
      signal: Signal.Logs,
      matchers,
      tsMin,
      tsMax,
      sql,
      requestId: "webui-log-volume",
    });
    if (seq !== volumeSeq) return; // superseded by a newer request
    const decoded = decodeVolume(res.table, stepMs);
    setVolumeData(decoded);
    setVolumeStatus(decoded.buckets.length === 0 ? "empty" : "ready");
  } catch (e) {
    if (seq !== volumeSeq) return;
    if (e instanceof UnauthorizedError) {
      setAuthed(false);
      clearVolume();
      return;
    }
    setVolumeData(null);
    setVolumeStatus("error");
  }
}

/** Decode the `{bucket_ns, severity, n}` aggregate into stacked severity bands
 *  over a shared, gap-filled bucket axis. */
function decodeVolume(table: Table, stepMs: number): VolumeData {
  const rows = table.toArray();
  // bucket-ms → (sevClass → count)
  const byBucket = new Map<number, Map<string, number>>();
  const classMeta = new Map<string, { label: string; cls: string; sev: number }>();
  let total = 0;

  for (const r of rows) {
    const o = (r?.toJSON?.() ?? {}) as Record<string, unknown>;
    const bucketNs = BigInt((o.bucket_ns ?? 0) as bigint | number | string);
    const bucketMs = Number(bucketNs / 1_000_000n);
    const sevNum = Number(o.severity ?? 0);
    const n = Number(o.n ?? 0);
    const info = severity(sevNum);
    total += n;

    let bucket = byBucket.get(bucketMs);
    if (!bucket) {
      bucket = new Map();
      byBucket.set(bucketMs, bucket);
    }
    bucket.set(info.label, (bucket.get(info.label) ?? 0) + n);
    if (!classMeta.has(info.label)) {
      classMeta.set(info.label, {
        label: info.label,
        cls: info.cls,
        sev: severityRank(info.label),
      });
    }
  }

  const buckets = Array.from(byBucket.keys()).sort((a, b) => a - b);
  // Least→most severe so the stack order is stable (severe on top).
  const classes = Array.from(classMeta.values()).sort((a, b) => a.sev - b.sev);
  const series: VolumeSeries[] = classes.map((c) => ({
    label: c.label,
    cls: c.cls,
    sev: c.sev,
    counts: buckets.map((b) => byBucket.get(b)?.get(c.label) ?? 0),
  }));

  return { buckets, series, total, stepMs };
}

// ── Metrics time-series chart (Phase 1a) ─────────────────────────────
//
// A downsampled line chart for the Metrics signal — the counterpart to the
// logs volume histogram. The chosen metric is just the `__name__` matcher; the
// chart rides the same query wire via a server-side `date_bin` aggregation
// (no protocol change). Two modes: a single aggregated line (avg/sum/min/max/
// count across all matching series) or one line per series. In per-series mode
// the legend resolves fingerprints to their label set best-effort via the
// D-058 opt-in label join (`with_labels`), falling back to fingerprint hex.
// Metrics-only; the result lives in its own signals so the table view is
// untouched.

const [metricAgg, setMetricAggSig] = createSignal<AggFn>("avg");
const [metricGrouped, setMetricGroupedSig] = createSignal(false);
const [metricsChartData, setMetricsChartData] = createSignal<MetricsChartData | null>(null);
type MetricsChartStatus = VolumeStatus | "no-data";
const [metricsChartStatus, setMetricsChartStatus] = createSignal<MetricsChartStatus>("idle");
export { metricAgg, metricGrouped, metricsChartData, metricsChartStatus };

/** The metric currently charted — the value of the `__name__` matcher, or ""
 *  when none is set. (Metrics have no separate "metric" field; the chart's
 *  subject is a label matcher like any other.) */
export function selectedMetric(): string {
  const m = state.matchers.find((r) => r.name.trim() === "__name__");
  return m ? m.value.trim() : "";
}

/** Set (replace / add / clear) the `__name__` matcher that names the charted
 *  metric, then re-run the table + chart. */
export function setMetricName(name: string): void {
  const n = name.trim();
  const idx = state.matchers.findIndex((r) => r.name.trim() === "__name__");
  if (idx >= 0) {
    if (n === "") deleteMatcher(idx);
    else setState("matchers", idx, "value", n);
  } else if (n !== "") {
    applyLabelMatcher("__name__", n);
  }
  void runCurrentQuery();
}

/** Change the chart aggregation and re-run just the chart (the table is
 *  aggregation-independent). */
export function setMetricAgg(fn: AggFn): void {
  setMetricAggSig(fn);
  void runMetricsChart();
}

/** Toggle per-series vs single-aggregated and re-run just the chart. */
export function setMetricGrouped(on: boolean): void {
  setMetricGroupedSig(on);
  void runMetricsChart();
}

/** Monotonic guard so a slow metrics-chart response can't clobber a newer one. */
let metricsChartSeq = 0;

function clearMetricsChart(): void {
  metricsChartSeq++;
  setMetricsChartData(null);
  setMetricsChartStatus("idle");
}

/** Max rows scanned to resolve fingerprint→labels for the per-series legend.
 *  Bounds the cost; a metric's distinct series count is far below this in
 *  practice, and a truncated scan still maps every fingerprint it saw. */
const SERIES_NAME_SCAN_CAP = 20000n;

/** Best-effort fingerprint→label-set resolution for the per-series legend, via
 *  the D-058 opt-in label join. Returns an empty map on any failure — the
 *  chart then legends by fingerprint hex. */
async function resolveMetricSeriesNames(
  dest: string,
  matchers: { name: string; value: string }[],
  tsMin: bigint,
  tsMax: bigint,
): Promise<Map<string, string>> {
  try {
    const transport = await getTransport();
    const res = await runQuery(transport, dest, {
      signal: Signal.Metrics,
      matchers,
      tsMin,
      tsMax,
      sql: `SELECT series_fingerprint AS fp, labels FROM metrics LIMIT ${SERIES_NAME_SCAN_CAP}`,
      withLabels: true,
      requestId: "webui-metrics-series",
    });
    return decodeSeriesNames(res.table);
  } catch {
    return new Map();
  }
}

/** Run the metrics time-series aggregation for the current form (metric +
 *  matchers + range) and decode it into `metricsChartData`. Requires a chosen
 *  metric (`__name__`) and a bounded [ts_min, ts_max] range (like the volume
 *  panel) so the bucket step is well-defined and the window is cacheable. */
export async function runMetricsChart(): Promise<void> {
  if (state.signal !== "Metrics") {
    clearMetricsChart();
    return;
  }

  const metric = selectedMetric();
  let tsMin: bigint | undefined;
  let tsMax: bigint | undefined;
  let matchers: { name: string; value: string }[];
  try {
    tsMin = parseBigIntOpt(state.tsMin);
    tsMax = parseBigIntOpt(state.tsMax);
    matchers = state.matchers
      .map((m) => ({ name: m.name.trim(), value: m.value }))
      .filter((m) => m.name !== "");
  } catch {
    setMetricsChartData(null);
    setMetricsChartStatus("error");
    return;
  }

  // Need a metric to chart and a bounded range to pick a bucket width. Without
  // both, skip quietly — the panel prompts the user.
  if (metric === "" || tsMin === undefined || tsMax === undefined || tsMax <= tsMin) {
    clearMetricsChart();
    setMetricsChartStatus("empty");
    return;
  }

  const grouped = metricGrouped();
  const agg = metricAgg();
  // The aggregate doubles as the intra-bucket downsample reducer.
  const reducer = agg === "count" ? "count(value)" : `${agg}(value)`;
  const spanMs = Number((tsMax - tsMin) / 1_000_000n);
  const stepMs = chooseStepMs(spanMs);
  const bucket =
    `CAST(date_bin(${stepIntervalSql(stepMs)}, ` +
    `to_timestamp_nanos(ts_unix_nano)) AS BIGINT) AS bucket_ns`;
  const sql = grouped
    ? `SELECT ${bucket}, series_fingerprint AS fp, ${reducer} AS v ` +
      `FROM metrics GROUP BY fp, bucket_ns ORDER BY bucket_ns`
    : `SELECT ${bucket}, ${reducer} AS v ` +
      `FROM metrics GROUP BY bucket_ns ORDER BY bucket_ns`;

  const seq = ++metricsChartSeq;
  setMetricsChartStatus("loading");
  try {
    const transport = await getTransport();
    const dest = metaDest();
    const res = await runQuery(transport, dest, {
      signal: Signal.Metrics,
      matchers,
      tsMin,
      tsMax,
      sql,
      requestId: "webui-metrics-chart",
    });
    if (seq !== metricsChartSeq) return; // superseded

    // Per-series: resolve the legend labels best-effort. Never fails the chart.
    let names: Map<string, string> | undefined;
    if (grouped) {
      names = await resolveMetricSeriesNames(dest, matchers, tsMin, tsMax);
      if (seq !== metricsChartSeq) return;
    }

    const decoded = decodeMetricsChart(res.table, stepMs, grouped, names);
    setMetricsChartData(decoded);
    setMetricsChartStatus(decoded.buckets.length === 0 ? "no-data" : "ready");
  } catch (e) {
    if (seq !== metricsChartSeq) return;
    if (e instanceof UnauthorizedError) {
      setAuthed(false);
      clearMetricsChart();
      return;
    }
    setMetricsChartData(null);
    setMetricsChartStatus("error");
  }
}

/** Max frames the overview aggregate returns. Slowest-first, so the cap keeps
 *  the frames most worth looking at. */
const FRAMES_LIMIT = 5000;

/** Run the traces frames-overview: one aggregated row per trace (= per frame),
 *  carrying its [t0, t1] window, duration, and span count. Reuses the form's
 *  matchers (→ promoted columns) and time bounds; the slowest frames come
 *  first so the LIMIT keeps the interesting ones. */
export async function runFramesOverview(): Promise<void> {
  let matchers: { name: string; value: string }[];
  let tsMin: bigint | undefined;
  let tsMax: bigint | undefined;
  try {
    matchers = state.matchers
      .map((m) => ({ name: m.name.trim(), value: m.value }))
      .filter((m) => m.name !== "");
    tsMin = parseBigIntOpt(state.tsMin);
    tsMax = parseBigIntOpt(state.tsMax);
  } catch (e) {
    setState({ status: "error", error: e instanceof Error ? e.message : String(e) });
    return;
  }

  const sql =
    "SELECT trace_id, " +
    "MIN(start_unix_nano) AS t0, " +
    "MAX(end_unix_nano) AS t1, " +
    "MAX(end_unix_nano) - MIN(start_unix_nano) AS dur_ns, " +
    "COUNT(*) AS spans " +
    "FROM traces GROUP BY trace_id " +
    `ORDER BY dur_ns DESC LIMIT ${FRAMES_LIMIT}`;

  await runSpec(
    { signal: Signal.Traces, matchers, tsMin, tsMax, sql },
    "frames",
  );
}

/** Drill from the frames overview into one frame's waterfall: load every span
 *  for `traceIdHex` (a by-id lookup) and render the standard single-trace view.
 *  Reflects the selection in the form (trace-id field, SQL cleared). */
export async function drillIntoFrame(traceIdHex: string): Promise<void> {
  let traceId: Uint8Array;
  try {
    traceId = parseHex16(traceIdHex);
  } catch (e) {
    setState({ status: "error", error: e instanceof Error ? e.message : String(e) });
    return;
  }
  setState({ signal: "Traces", traceId: traceIdHex, sql: "" });

  let tsMin: bigint | undefined;
  let tsMax: bigint | undefined;
  try {
    tsMin = parseBigIntOpt(state.tsMin);
    tsMax = parseBigIntOpt(state.tsMax);
  } catch {
    tsMin = undefined;
    tsMax = undefined;
  }

  await runSpec(
    {
      signal: Signal.Traces,
      matchers: [],
      tsMin,
      tsMax,
      traceId,
      limit: parseBigIntOpt(state.limit),
    },
    "default",
  );
}
