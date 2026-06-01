//! Renders the decoded Arrow result table.
//!
//! Logs get a purpose-built reader view — the body is the payload, so it gets
//! the room; the timestamp and severity are compact, the labels ("attributes")
//! collapse behind a per-row expander, and a search box filters lines
//! client-side. ANSI colour/escape sequences are stripped from bodies. A "raw"
//! toggle drops back to the generic column table. Every other signal renders as
//! the generic (display-capped) HTML table.

import { For, Show, createMemo, createSignal, type Component, type JSX } from "solid-js";

import { state, resultTable } from "../store";

/** Cap rendered rows so a large result can't lock up the DOM. */
const MAX_DISPLAY_ROWS = 2000;

// ── value formatting (generic table) ─────────────────────────────────

function toHex(bytes: Uint8Array): string {
  let s = "";
  for (const b of bytes) s += b.toString(16).padStart(2, "0");
  return s;
}

function fmtCell(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (v instanceof Uint8Array) return toHex(v);
  if (typeof v === "bigint") return v.toString();
  if (typeof v === "object") {
    try {
      return JSON.stringify(v, (_k, val) => (typeof val === "bigint" ? val.toString() : val));
    } catch {
      return String(v);
    }
  }
  return String(v);
}

// ── log helpers ───────────────────────────────────────────────────────

// Canonical ansi-regex (chalk) — matches CSI/OSC colour & cursor sequences.
const ANSI_RE = new RegExp(
  [
    "[\\u001B\\u009B][[\\]()#;?]*(?:(?:(?:(?:;[-a-zA-Z\\d\\/#&.:=?%@~_]+)*|[a-zA-Z\\d]+(?:;[-a-zA-Z\\d\\/#&.:=?%@~_]*)*)?\\u0007)",
    "(?:(?:\\d{1,4}(?:;\\d{0,4})*)?[\\dA-PR-TZcf-nq-uy=><~]))",
  ].join("|"),
  "g",
);

/** Leftover C0/C1 control chars, keeping tab (\\t) and newline (\\n). */
const CTRL_RE = /[\u0000-\u0008\u000b-\u001f\u007f-\u009f]/g;

/** Strip ANSI escapes, then any leftover control chars except tab/newline. */
function stripAnsi(s: string): string {
  return s.replace(ANSI_RE, "").replace(CTRL_RE, "");
}

interface LogRow {
  ts: bigint;
  sev: number;
  body: string;
  attrs: [string, string][];
}

function attrVal(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "bigint") return v.toString();
  if (typeof v === "object") return fmtCell(v);
  return String(v);
}

/** Coerce the Arrow Map cell (object / Map / array-of-pairs) into entries. */
function attrEntries(v: unknown): [string, string][] {
  if (v === null || v === undefined) return [];
  if (v instanceof Map) {
    return [...v.entries()].map(([k, val]) => [String(k), attrVal(val)]);
  }
  if (Array.isArray(v)) {
    return v.map((e) =>
      Array.isArray(e)
        ? [String(e[0]), attrVal(e[1])]
        : [String((e as { key: unknown }).key), attrVal((e as { value: unknown }).value)],
    );
  }
  if (typeof v === "object") {
    return Object.entries(v as Record<string, unknown>).map(([k, val]) => [k, attrVal(val)]);
  }
  return [];
}

/** OTEL severity number → display label + CSS class. */
function severity(sev: number): { label: string; cls: string } {
  if (sev >= 21) return { label: "FATAL", cls: "sev-fatal" };
  if (sev >= 17) return { label: "ERROR", cls: "sev-error" };
  if (sev >= 13) return { label: "WARN", cls: "sev-warn" };
  if (sev >= 9) return { label: "INFO", cls: "sev-info" };
  if (sev >= 5) return { label: "DEBUG", cls: "sev-debug" };
  if (sev >= 1) return { label: "TRACE", cls: "sev-trace" };
  return { label: "—", cls: "sev-none" };
}

const pad = (n: number, w = 2): string => String(n).padStart(w, "0");

/** Unix-nanos → a compact local time plus a full ISO title. */
function fmtTs(ns: bigint): { short: string; full: string } {
  const d = new Date(Number(ns / 1_000_000n));
  if (Number.isNaN(d.getTime())) return { short: "—", full: "" };
  const short = `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`;
  return { short, full: d.toISOString() };
}

// ── component ──────────────────────────────────────────────────────────

const ResultsTable: Component = () => {
  const [filter, setFilter] = createSignal("");
  const [raw, setRaw] = createSignal(false);

  // Logs view (null unless the result carries the canonical log columns).
  const logs = createMemo(() => {
    const t = resultTable();
    if (!t) return null;
    const names = new Set(t.schema.fields.map((f) => f.name));
    if (!(names.has("body") && names.has("ts_unix_nano") && names.has("severity"))) {
      return null;
    }
    const all = t.toArray();
    const shown = Math.min(all.length, MAX_DISPLAY_ROWS);
    const rows: LogRow[] = [];
    for (let i = 0; i < shown; i++) {
      const o = (all[i]?.toJSON?.() ?? {}) as Record<string, unknown>;
      const tsRaw = o.ts_unix_nano;
      rows.push({
        ts: typeof tsRaw === "bigint" ? tsRaw : BigInt((tsRaw as number | string) ?? 0),
        sev: Number(o.severity ?? 0),
        body: stripAnsi(String(o.body ?? "")),
        attrs: attrEntries(o.attributes),
      });
    }
    return { rows, shown, total: t.numRows };
  });

  const filteredLogs = createMemo(() => {
    const lv = logs();
    if (!lv) return [];
    const q = filter().trim().toLowerCase();
    if (q === "") return lv.rows;
    return lv.rows.filter(
      (r) =>
        r.body.toLowerCase().includes(q) ||
        r.attrs.some(([k, v]) => k.toLowerCase().includes(q) || v.toLowerCase().includes(q)),
    );
  });

  // Generic table view (any signal).
  const table = createMemo(() => {
    const t = resultTable();
    if (!t) return null;
    const fields = t.schema.fields.map((f) => ({ name: f.name, type: String(f.type) }));
    const rows: string[][] = [];
    const all = t.toArray();
    const shown = Math.min(all.length, MAX_DISPLAY_ROWS);
    for (let i = 0; i < shown; i++) {
      const obj = all[i]?.toJSON?.() ?? {};
      rows.push(fields.map((f) => fmtCell(obj[f.name])));
    }
    return { fields, rows, shown, total: t.numRows };
  });

  // `total` is an accessor so the meta strip stays reactive across queries
  // (the surrounding component body runs only once).
  const metaCommon = (total: () => number): JSX.Element => (
    <>
      <span>
        <strong>{total().toLocaleString()}</strong> rows
      </span>
      <Show when={state.totalRows !== BigInt(total())}>
        <span class="warn" title="client-decoded rows differ from the server's reported count">
          ⚠ server reported {state.totalRows.toString()}
        </span>
      </Show>
      <span>{state.elapsedMs.toFixed(1)} ms</span>
    </>
  );

  return (
    <div class="results">
      <Show
        when={resultTable()}
        fallback={
          <div class="results-empty">
            <Show when={state.status === "idle"}>Run a query to see results.</Show>
            <Show when={state.status === "running"}>Querying…</Show>
            <Show when={state.status === "error"}>Query failed — see the error above.</Show>
          </div>
        }
      >
        {/* Logs reader view, unless the raw toggle is on. `lv` is an accessor
            (kept reactive) — read it in JSX positions, never captured. */}
        <Show when={!raw() && logs()}>
          {(lv) => (
            <>
              <div class="results-meta">
                {metaCommon(() => lv().total)}
                <input
                  class="log-search"
                  type="search"
                  placeholder="filter lines…"
                  value={filter()}
                  onInput={(e) => setFilter(e.currentTarget.value)}
                />
                <span>{filteredLogs().length.toLocaleString()} shown</span>
                <Show when={lv().shown < lv().total}>
                  <span class="warn">scanned first {lv().shown.toLocaleString()}</span>
                </Show>
                <label class="raw-toggle" title="show the underlying columns as a table">
                  <input type="checkbox" checked={raw()} onInput={(e) => setRaw(e.currentTarget.checked)} />
                  raw
                </label>
              </div>
              <div class="log-list">
                <For each={filteredLogs()}>
                  {(r) => {
                    const sev = severity(r.sev);
                    const ts = fmtTs(r.ts);
                    return (
                      <div class={`log-entry ${sev.cls}`}>
                        <div class="log-head">
                          <span class="log-ts" title={ts.full}>
                            {ts.short}
                          </span>
                          <span class={`log-sev ${sev.cls}`}>{sev.label}</span>
                          <span class="log-body">{r.body}</span>
                        </div>
                        <Show when={r.attrs.length > 0}>
                          <details class="log-attrs">
                            <summary>
                              {r.attrs.length} label{r.attrs.length === 1 ? "" : "s"}
                            </summary>
                            <div class="log-attr-chips">
                              <For each={r.attrs}>
                                {([k, v]) => (
                                  <span class="chip">
                                    <b>{k}</b>
                                    <span>{v}</span>
                                  </span>
                                )}
                              </For>
                            </div>
                          </details>
                        </Show>
                      </div>
                    );
                  }}
                </For>
              </div>
            </>
          )}
        </Show>

        {/* Generic table: every non-log signal, or logs in raw mode. Read the
            `table()` memo in JSX positions so it tracks new results. */}
        <Show when={(!logs() || raw()) && table()}>
          {(v) => (
            <>
              <div class="results-meta">
                {metaCommon(() => v().total)}
                <span>{v().fields.length} columns</span>
                <Show when={logs()}>
                  <label class="raw-toggle" title="back to the logs reader view">
                    <input type="checkbox" checked={raw()} onInput={(e) => setRaw(e.currentTarget.checked)} />
                    raw
                  </label>
                </Show>
                <Show when={v().shown < v().total}>
                  <span class="warn">showing first {v().shown.toLocaleString()}</span>
                </Show>
              </div>
              <div class="table-scroll">
                <table class="data-table">
                  <thead>
                    <tr>
                      <For each={v().fields}>
                        {(f) => (
                          <th title={f.type}>
                            <span class="col-name">{f.name}</span>
                            <span class="col-type">{f.type}</span>
                          </th>
                        )}
                      </For>
                    </tr>
                  </thead>
                  <tbody>
                    <For each={v().rows}>
                      {(row) => (
                        <tr>
                          <For each={row}>{(cell) => <td title={cell}>{cell}</td>}</For>
                        </tr>
                      )}
                    </For>
                  </tbody>
                </table>
              </div>
            </>
          )}
        </Show>
      </Show>
    </div>
  );
};

export default ResultsTable;
