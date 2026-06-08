//! Renders the decoded Arrow result table.
//!
//! Logs get a purpose-built reader view — the body is the payload, so it gets
//! the room; the timestamp and severity are compact, the labels ("attributes")
//! collapse behind a per-row expander, and a search box filters lines
//! client-side. ANSI colour/escape sequences are stripped from bodies. A "raw"
//! toggle drops back to the generic column table. Every other signal renders as
//! the generic (display-capped) HTML table.

import { For, Show, createMemo, createSignal, type Component, type JSX } from "solid-js";

import { attrEntries, fmtCell, fmtTs } from "../format";
import {
  buildSpanTree,
  decodeFrameRows,
  decodeSpans,
  frameStats,
  layoutSpans,
  singleTraceId,
} from "../traces";
import { state, resultTable, resultKind } from "../store";
import TracesView, { type TraceData } from "./TracesView";
import FramesView, { type FramesData } from "./FramesView";

/** Cap rendered rows so a large result can't lock up the DOM. */
const MAX_DISPLAY_ROWS = 2000;

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
  /** Stream labels (the service identity) — joined from the postings
   *  sidecar onto every row by the query engine. Shown as primary chips. */
  labels: [string, string][];
  /** Per-entry attributes (stream=stdout/stderr, trace_id, …). Secondary. */
  attrs: [string, string][];
}

/** Stream-label keys promoted to always-visible chips, in display order.
 *  These answer "which service / workload is this?" — the rest fold into
 *  the expander. */
const PRIMARY_LABEL_KEYS = [
  "container",
  "pod",
  "namespace",
  "node",
  "service",
  "k8s_app.kubernetes.io/name",
  "k8s_app.kubernetes.io/instance",
  "k8s_app.kubernetes.io/component",
];

/** Pick the identifying labels to show inline. Falls back to the first few
 *  labels when none of the curated keys are present. */
function primaryLabels(labels: [string, string][]): [string, string][] {
  if (labels.length === 0) return [];
  const byKey = new Map(labels);
  const out: [string, string][] = [];
  for (const k of PRIMARY_LABEL_KEYS) {
    const v = byKey.get(k);
    if (v !== undefined) out.push([k, v]);
  }
  return out.length > 0 ? out : labels.slice(0, 4);
}

/** Shorten a label key for chip display: drop the agent's `k8s_` prefix and
 *  collapse `app.kubernetes.io/name` → `name`. */
function shortKey(k: string): string {
  let s = k.startsWith("k8s_") ? k.slice(4) : k;
  const slash = s.lastIndexOf("/");
  if (slash >= 0) s = s.slice(slash + 1);
  return s;
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
        labels: attrEntries(o.labels),
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
    const matches = (k: string, v: string) =>
      k.toLowerCase().includes(q) || v.toLowerCase().includes(q);
    return lv.rows.filter(
      (r) =>
        r.body.toLowerCase().includes(q) ||
        r.labels.some(([k, v]) => matches(k, v)) ||
        r.attrs.some(([k, v]) => matches(k, v)),
    );
  });

  // Traces waterfall view: only when the result carries the canonical span
  // columns AND is a single-trace lookup (one distinct trace_id). Multi-trace
  // results fall through to the generic table.
  const traces = createMemo<TraceData | null>(() => {
    const t = resultTable();
    if (!t) return null;
    const names = new Set(t.schema.fields.map((f) => f.name));
    if (!(names.has("trace_id") && names.has("span_id") && names.has("start_unix_nano"))) {
      return null;
    }
    const all = t.toArray();
    const shown = Math.min(all.length, MAX_DISPLAY_ROWS);
    const raws: Record<string, unknown>[] = [];
    for (let i = 0; i < shown; i++) {
      raws.push((all[i]?.toJSON?.() ?? {}) as Record<string, unknown>);
    }
    const traceId = singleTraceId(raws);
    if (traceId === null) return null;
    const ordered = buildSpanTree(decodeSpans(raws));
    const layouts = layoutSpans(ordered);
    const rows = ordered.map((span, i) => ({ span, layout: layouts[i]! }));
    return { traceId, rows, shown, total: t.numRows };
  });

  // Frames-overview view: the aggregate-per-frame result. Driven by the
  // explicit `resultKind` flag the action set (not column sniffing), since the
  // aggregate shares columns with a generic table.
  const frames = createMemo<FramesData | null>(() => {
    if (resultKind() !== "frames") return null;
    const t = resultTable();
    if (!t) return null;
    const all = t.toArray();
    const shown = Math.min(all.length, MAX_DISPLAY_ROWS);
    const raws: Record<string, unknown>[] = [];
    for (let i = 0; i < shown; i++) {
      raws.push((all[i]?.toJSON?.() ?? {}) as Record<string, unknown>);
    }
    const rows = decodeFrameRows(raws);
    return { rows, stats: frameStats(rows), shown, total: t.numRows };
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
                    const primary = primaryLabels(r.labels);
                    const extra = r.labels.length + r.attrs.length;
                    return (
                      <div class={`log-entry ${sev.cls}`}>
                        <div class="log-head">
                          <span class="log-ts" title={ts.full}>
                            {ts.short}
                          </span>
                          <span class={`log-sev ${sev.cls}`}>{sev.label}</span>
                          <span class="log-body">{r.body}</span>
                        </div>
                        <Show when={primary.length > 0}>
                          <div class="log-labels">
                            <For each={primary}>
                              {([k, v]) => (
                                <span class="chip lbl" title={k}>
                                  <b>{shortKey(k)}</b>
                                  <span>{v}</span>
                                </span>
                              )}
                            </For>
                          </div>
                        </Show>
                        <Show when={extra > 0}>
                          <details class="log-attrs">
                            <summary>
                              {r.labels.length} label{r.labels.length === 1 ? "" : "s"} ·{" "}
                              {r.attrs.length} attr{r.attrs.length === 1 ? "" : "s"}
                            </summary>
                            <Show when={r.labels.length > 0}>
                              <div class="log-attr-chips">
                                <For each={r.labels}>
                                  {([k, v]) => (
                                    <span class="chip">
                                      <b>{k}</b>
                                      <span>{v}</span>
                                    </span>
                                  )}
                                </For>
                              </div>
                            </Show>
                            <Show when={r.attrs.length > 0}>
                              <div class="log-attr-chips">
                                <For each={r.attrs}>
                                  {([k, v]) => (
                                    <span class="chip attr">
                                      <b>{k}</b>
                                      <span>{v}</span>
                                    </span>
                                  )}
                                </For>
                              </div>
                            </Show>
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

        {/* Traces waterfall, unless the raw toggle is on. `tv` is a reactive
            accessor — passed through to TracesView, never captured here. */}
        <Show when={!raw() && traces()}>
          {(tv) => <TracesView data={tv} raw={raw} setRaw={setRaw} />}
        </Show>

        {/* Frames overview (traces aggregate). */}
        <Show when={!raw() && frames()}>
          {(fv) => <FramesView data={fv} raw={raw} setRaw={setRaw} />}
        </Show>

        {/* Generic table: any signal with no purpose-built view, or a
            log/trace/frames result in raw mode. Read the `table()` memo in JSX
            positions so it tracks new results. */}
        <Show when={((!logs() && !traces() && !frames()) || raw()) && table()}>
          {(v) => (
            <>
              <div class="results-meta">
                {metaCommon(() => v().total)}
                <span>{v().fields.length} columns</span>
                <Show when={logs() || traces() || frames()}>
                  <label class="raw-toggle" title="back to the purpose-built view">
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
