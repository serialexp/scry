//! Headless end-to-end check for the traces waterfall data path.
//!
//! Logs into a running scry-webui, runs a real by-id trace lookup through the
//! same /api/query relay the browser uses, decodes the Arrow result, and runs
//! the production traces.ts functions on it — proving the column detector,
//! decode, and tree-build work against real Arrow `toJSON()` shapes (the bit
//! the unit tests stub). Not part of CI; a manual verification aid.
//!
//! Usage: bun scripts/trace-render-check.ts <baseUrl> <password>

import { runQuery, type QuerySpec } from "../src/protocol/client";
import type { Transport } from "../src/protocol/transport";
import { Signal } from "../src/protocol/constants";
import {
  buildSpanTree,
  decodeSpans,
  layoutSpans,
  singleTraceId,
} from "../src/traces";

const base = process.argv[2] ?? "http://127.0.0.1:18080";
const password = process.argv[3] ?? "trace123";

// Log in, capture the session cookie.
const login = await fetch(`${base}/api/login`, {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ password }),
});
if (login.status !== 204) throw new Error(`login failed: HTTP ${login.status}`);
const cookie = login.headers.get("set-cookie")?.split(";")[0] ?? "";
if (!cookie) throw new Error("no session cookie returned");

// Minimal Transport that rides the cookie (mirrors HttpTransport).
const transport: Transport = {
  async query(_addr, request) {
    const res = await fetch(`${base}/api/query`, {
      method: "POST",
      headers: { "content-type": "application/octet-stream", cookie },
      body: request as BodyInit,
    });
    if (!res.ok) throw new Error(`relay failed: HTTP ${res.status}`);
    return new Uint8Array(await res.arrayBuffer());
  },
};

const run = (spec: QuerySpec) => runQuery(transport, "", spec);
const rowsOf = (table: { toArray(): { toJSON(): unknown }[] }) =>
  table.toArray().map((r) => r.toJSON() as Record<string, unknown>);

// Step A: grab one trace id.
const idRes = await run({
  signal: Signal.Traces,
  matchers: [],
  sql: "SELECT trace_id FROM traces LIMIT 1",
});
const idRows = rowsOf(idRes.table);
const traceIdBytes = idRows[0]?.trace_id as Uint8Array;
const traceIdHex = Buffer.from(traceIdBytes).toString("hex");
console.log(`picked trace_id = ${traceIdHex}`);

// Step B: by-id lookup (the waterfall path).
const res = await run({
  signal: Signal.Traces,
  matchers: [],
  traceId: traceIdBytes,
});
const rows = rowsOf(res.table);
console.log(`by-id lookup returned ${rows.length} spans (server total ${res.totalRows})`);

// Run the production logic.
const single = singleTraceId(rows);
const spans = decodeSpans(rows);
const placed = buildSpanTree(spans);
const layouts = layoutSpans(placed);

console.log(`singleTraceId → ${single} (detector ${single ? "PASS: waterfall" : "FAIL: would fall back to table"})`);
console.log("\nwaterfall (pre-order, depth · kind · name · service · dur · [left,width]):");
for (let i = 0; i < placed.length; i++) {
  const s = placed[i]!;
  const l = layouts[i]!;
  const durMs = Number(s.end - s.start) / 1e6;
  console.log(
    `  ${"  ".repeat(s.depth)}d${s.depth} ${s.name} (${s.service}) ` +
      `${durMs.toFixed(2)}ms [${l.leftFrac.toFixed(2)},${l.widthFrac.toFixed(2)}]` +
      ` parent=${s.parentSpanId ?? "ROOT"}`,
  );
}

// Sanity assertions.
const roots = placed.filter((s) => s.depth === 0);
if (single === null) throw new Error("ASSERT FAIL: result was not detected as single-trace");
if (placed.length !== rows.length) throw new Error("ASSERT FAIL: tree dropped spans");
if (roots.length < 1) throw new Error("ASSERT FAIL: no root span");
if (layouts.some((l) => l.leftFrac < 0 || l.leftFrac + l.widthFrac > 1.0001))
  throw new Error("ASSERT FAIL: bar geometry out of [0,1]");
console.log(`\nOK: single trace, ${placed.length} spans, ${roots.length} root(s), geometry in range.`);
