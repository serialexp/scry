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
  type QuerySpec,
  type MetaScope,
} from "./protocol/client";
import { severity, severityRank } from "./severity";
import {
  chooseStepMs,
  stepIntervalSql,
  type VolumeData,
  type VolumeSeries,
} from "./volume";

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

const INITIAL: FormState = {
  addr: "127.0.0.1:4100",
  target: "",
  signal: "Metrics",
  matchers: [{ name: "", value: "" }],
  tsMin: "",
  tsMax: "",
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
  };
}

/** Run a pre-built spec, recording the result under `kind`. Shared by the
 *  form-driven query and the traces frames-overview / drill-in actions. */
async function runSpec(spec: QuerySpec, kind: "default" | "frames"): Promise<void> {
  setState({ status: "running", error: null });
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
const [labelValues, setLabelValues] = createSignal<Record<string, string[]>>({});
export { labelNames, labelStatus, labelValues };

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
  setLabelStatus("loading");
  try {
    const transport = await getTransport();
    const names = await fetchLabelNames(transport, dest, scope);
    if (seq !== metaSeq) return; // superseded by a newer scope
    setLabelNames(names);
    setLabelStatus("ready");
  } catch {
    if (seq !== metaSeq) return;
    setLabelNames([]);
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
  } catch {
    // Leave uncached so a later interaction can retry.
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
}

/** The Explore drill-down loop: add a `name=value` matcher, then re-run the
 *  table + volume so the whole view refilters to the selected slice. */
export async function drillLabelValue(name: string, value: string): Promise<void> {
  applyLabelMatcher(name, value);
  await runCurrentQuery();
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
