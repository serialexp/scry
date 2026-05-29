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
import { TauriTransport } from "./protocol/transport";
import { runQuery, QueryError, type QuerySpec } from "./protocol/client";

export interface MatcherRow {
  name: string;
  value: string;
}

export type RunStatus = "idle" | "running" | "done" | "error";

export interface FormState {
  /** `host:port` of the scry-queryd daemon. */
  addr: string;
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

export { state, resultTable };

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

const transport = new TauriTransport();

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

export async function runCurrentQuery(): Promise<void> {
  let spec: QuerySpec;
  try {
    spec = specFromForm();
  } catch (e) {
    setState({ status: "error", error: e instanceof Error ? e.message : String(e) });
    return;
  }

  setState({ status: "running", error: null });
  try {
    const res = await runQuery(transport, state.addr.trim(), spec);
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
    const message =
      e instanceof QueryError
        ? e.message
        : e instanceof Error
          ? e.message
          : String(e);
    setState({ status: "error", error: message, rowCount: 0, totalRows: 0n });
  }
}
