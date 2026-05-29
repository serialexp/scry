//! Renders the decoded Arrow result table: a header strip with row
//! counts + timing, then a (display-capped) HTML table. Reads the result
//! signal + run meta from the store directly.

import { For, Show, createMemo, type Component } from "solid-js";

import { state, resultTable } from "../store";

/** Cap rendered rows so a large result can't lock up the DOM. */
const MAX_DISPLAY_ROWS = 2000;

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

const ResultsTable: Component = () => {
  const view = createMemo(() => {
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

  return (
    <div class="results">
      <Show
        when={view()}
        fallback={
          <div class="results-empty">
            <Show when={state.status === "idle"}>Run a query to see results.</Show>
            <Show when={state.status === "running"}>Querying…</Show>
            <Show when={state.status === "error"}>Query failed — see the error above.</Show>
          </div>
        }
      >
        {(v) => (
          <>
            <div class="results-meta">
              <span>
                <strong>{v().total.toLocaleString()}</strong> rows
              </span>
              <Show when={state.totalRows !== BigInt(v().total)}>
                <span class="warn" title="client-decoded rows differ from the server's reported count">
                  ⚠ server reported {state.totalRows.toString()}
                </span>
              </Show>
              <span>{state.elapsedMs.toFixed(1)} ms</span>
              <span>{v().fields.length} columns</span>
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
    </div>
  );
};

export default ResultsTable;
