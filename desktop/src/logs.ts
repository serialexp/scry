//! The log row shape, shared by the two sources that produce one.
//!
//! A log line reaches the UI either as a row of an Arrow result table (a query
//! against stored blocks, possibly merged with the ingesters' in-flight ring)
//! or as a `TailRecord` pushed over a live subscription. They carry the same
//! information, so they become the same struct here and the view code never
//! has to care which one it is rendering.

import type { Table } from "apache-arrow";

import { attrEntries } from "./format";
import type { TailRecord } from "./protocol/tail";

/** Cap on rows decoded and rendered at once, so a large result can't lock up
 *  the DOM. Shared by the history decode and the live merge. */
export const MAX_LOG_ROWS = 2000;

export interface LogRow {
  ts: bigint;
  sev: number;
  body: string;
  /** Stream labels (the service identity) — joined from the postings
   *  sidecar onto every row by the query engine. Shown as primary chips. */
  labels: [string, string][];
  /** Per-entry attributes (stream=stdout/stderr, trace_id, …). Secondary. */
  attrs: [string, string][];
}

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
export function stripAnsi(s: string): string {
  return s.replace(ANSI_RE, "").replace(CTRL_RE, "");
}

/** Does this result table carry the canonical log columns? */
export function isLogTable(table: Table): boolean {
  const names = new Set(table.schema.fields.map((f) => f.name));
  return names.has("body") && names.has("ts_unix_nano") && names.has("severity");
}

/** Decode up to `limit` rows of a logs result table. */
export function decodeLogRows(table: Table, limit: number): LogRow[] {
  const all = table.toArray();
  const shown = Math.min(all.length, limit);
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
  return rows;
}

/** Convert a pushed live record into the same row shape. */
export function tailRecordToLogRow(rec: TailRecord): LogRow {
  return {
    ts: rec.tsUnixNano,
    sev: rec.severity,
    body: stripAnsi(rec.body),
    labels: rec.labels,
    attrs: rec.attrs,
  };
}

/** The newest timestamp in a set of rows, or `null` when there are none. */
export function newestTs(rows: LogRow[]): bigint | null {
  let max: bigint | null = null;
  for (const r of rows) {
    if (max === null || r.ts > max) max = r.ts;
  }
  return max;
}

/**
 * Merge the queried history with the live rows arrived since.
 *
 * History first, live appended in arrival order — the `tail -f` reading order,
 * and the order a live pane has to grow in. When the total exceeds `limit` the
 * **newest** rows win: with a live stream running, dropping the oldest is
 * obviously right, and it keeps the pane pinned to what is happening now.
 */
export function mergeLogRows(
  history: LogRow[],
  live: LogRow[],
  limit: number,
): LogRow[] {
  if (live.length === 0) return history.slice(0, limit);
  const merged = history.concat(live);
  return merged.length <= limit ? merged : merged.slice(merged.length - limit);
}

/**
 * Keep a live buffer bounded, dropping the oldest rows.
 *
 * A tail left open on a busy stream would otherwise grow without limit — the
 * browser tab, not the server, is what falls over first.
 */
export function appendCapped(buffer: LogRow[], incoming: LogRow[], cap: number): LogRow[] {
  if (incoming.length === 0) return buffer;
  const next = buffer.concat(incoming);
  return next.length <= cap ? next : next.slice(next.length - cap);
}

/**
 * Should this live record be shown, given where the history query ended?
 *
 * The subscription is opened *before* the history query runs, so the two
 * overlap: a record can be in both. The history's newest timestamp is the seam
 * — anything at or below it is already on screen. `seam === null` means the
 * history came back empty, so nothing can be a duplicate and everything shows.
 *
 * Equal timestamps are excluded rather than included: a duplicated line is a
 * visible lie about what was logged, while a dropped one at the exact seam
 * nanosecond is invisible and, for a best-effort tail, acceptable.
 */
export function isAfterSeam(row: LogRow, seam: bigint | null): boolean {
  return seam === null || row.ts > seam;
}
