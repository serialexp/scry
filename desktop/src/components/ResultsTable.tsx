//! Renders the decoded Arrow result table.
//!
//! Logs get a purpose-built reader view — the body is the payload, so it gets
//! the room; the timestamp and severity are compact, the labels ("attributes")
//! collapse behind a per-row expander, and a search box filters lines
//! client-side. ANSI colour/escape sequences are stripped from bodies. A "raw"
//! toggle drops back to the generic column table. Every other signal renders as
//! the generic (display-capped) HTML table.

import { For, Show, createMemo, createSignal, type Component, type JSX } from "solid-js";

import { fmtCell, fmtTs } from "../format";
import {
  buildSpanTree,
  decodeFrameRows,
  decodeSpans,
  frameStats,
  layoutSpans,
  singleTraceId,
} from "../traces";
import { MAX_LOG_ROWS, decodeLogRows, isLogTable, mergeLogRows } from "../logs";
import {
  state,
  resultTable,
  resultKind,
  selected,
  setSelected,
  liveRows,
  liveActive,
} from "../store";
import { severity } from "../severity";
import TracesView, { type TraceData } from "./TracesView";
import FramesView, { type FramesData } from "./FramesView";

/** Cap rendered rows so a large result can't lock up the DOM. */
const MAX_DISPLAY_ROWS = MAX_LOG_ROWS;

// -- log helpers -------------------------------------------------------
//
// The row shape, its ANSI stripping and its decoders live in `../logs`: a
// live-tail record produces exactly the same row from a completely different
// wire, and the view must not be able to tell them apart.

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

/** The identifying service/workload for the compact grid's Service column:
 *  the first present primary label value. */
function serviceOf(labels: [string, string][]): string {
  const p = primaryLabels(labels);
  return p.length > 0 ? p[0]![1] : "";
}

/** A trace id from a log entry's attributes, if present (Trace column). */
function traceOf(attrs: [string, string][]): string {
  for (const [k, v] of attrs) {
    if (k === "trace_id" || k === "traceId" || k === "trace.id") return v;
  }
  return "";
}

// ── component ──────────────────────────────────────────────────────────

const ResultsTable: Component = () => {
  const [filter, setFilter] = createSignal("");
  const [raw, setRaw] = createSignal(false);

  // Logs view (null unless the result carries the canonical log columns).
  //
  // Two sources, one list: the queried history, then whatever the live
  // subscription has pushed since. `mergeLogRows` keeps the newest when the
  // pair overruns the display cap, so a running tail scrolls rather than
  // stopping dead at 2000 rows.
  const logs = createMemo(() => {
    const t = resultTable();
    if (!t || !isLogTable(t)) return null;
    const history = decodeLogRows(t, MAX_DISPLAY_ROWS);
    const live = liveActive() ? liveRows() : [];
    const rows = mergeLogRows(history, live, MAX_DISPLAY_ROWS);
    return {
      rows,
      shown: history.length,
      total: t.numRows,
      liveCount: live.length,
    };
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
                <Show when={lv().liveCount > 0}>
                  <span class="live-count" title="records pushed by the live subscription since the query ran">
                    +{lv().liveCount.toLocaleString()} live
                  </span>
                </Show>
                <Show when={lv().shown < lv().total}>
                  <span class="warn">scanned first {lv().shown.toLocaleString()}</span>
                </Show>
                <label class="raw-toggle" title="show the underlying columns as a table">
                  <input type="checkbox" checked={raw()} onInput={(e) => setRaw(e.currentTarget.checked)} />
                  raw
                </label>
              </div>
              <div class="logs-grid">
                <div class="logs-grid-head">
                  <span>Time</span>
                  <span>Level</span>
                  <span>Service</span>
                  <span>Message</span>
                  <span>Trace</span>
                </div>
                <div class="logs-grid-body">
                  <For each={filteredLogs()}>
                    {(r) => {
                      const sev = severity(r.sev);
                      const ts = fmtTs(r.ts);
                      const svc = serviceOf(r.labels);
                      const trace = traceOf(r.attrs);
                      const isSel = () => {
                        const s = selected();
                        return !!s && s.ts === r.ts && s.body === r.body;
                      };
                      return (
                        <button
                          type="button"
                          class={`log-row ${sev.cls}`}
                          classList={{ selected: isSel() }}
                          onClick={() =>
                            setSelected({
                              kind: "log",
                              ts: r.ts,
                              sev: r.sev,
                              body: r.body,
                              labels: r.labels,
                              attrs: r.attrs,
                            })
                          }
                        >
                          <span class="lg-ts" title={ts.full}>
                            {ts.short}
                          </span>
                          <span class="lg-level">
                            <span class={`log-sev ${sev.cls}`}>{sev.label}</span>
                          </span>
                          <span class="lg-svc" title={svc}>
                            {svc}
                          </span>
                          <span class="lg-msg">{r.body}</span>
                          <span class="lg-trace" title={trace}>
                            {trace ? trace.slice(0, 12) : ""}
                          </span>
                        </button>
                      );
                    }}
                  </For>
                </div>
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
