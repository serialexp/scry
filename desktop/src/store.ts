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
import { runQuery, QueryError, type QuerySpec } from "./protocol/client";

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

export async function runCurrentQuery(): Promise<void> {
  let spec: QuerySpec;
  try {
    spec = specFromForm();
  } catch (e) {
    setState({ status: "error", error: e instanceof Error ? e.message : String(e) });
    return;
  }
  await runSpec(spec, "default");
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
