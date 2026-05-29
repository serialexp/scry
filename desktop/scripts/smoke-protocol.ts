//! Headless end-to-end check of the TypeScript query protocol against a
//! live `scry-queryd`, without the Tauri GUI.
//!
//! It drives the *exact* production code path — `runQuery` from
//! `src/protocol/client.ts`, the generated binschema bindings, and the
//! Arrow IPC decode — but swaps the Tauri socket transport for a
//! `node:net` one (which mirrors the Rust shim: write the framed
//! request, read to EOF). This validates the part that actually carries
//! risk: that the generated TS decode + concatenated-Arrow-IPC parse
//! agree with the daemon's real bytes.
//!
//! Usage: bun run scripts/smoke-protocol.ts [host:port]

import net from "node:net";

import { runQuery, QueryError, type QuerySpec } from "../src/protocol/client";
import { Signal } from "../src/protocol/constants";
import type { Transport } from "../src/protocol/transport";

class NodeTcpTransport implements Transport {
  query(addr: string, request: Uint8Array): Promise<Uint8Array> {
    const idx = addr.lastIndexOf(":");
    const host = addr.slice(0, idx);
    const port = Number(addr.slice(idx + 1));
    return new Promise((resolve, reject) => {
      const chunks: Buffer[] = [];
      let done = false;
      const finish = () => {
        if (done) return;
        done = true;
        resolve(new Uint8Array(Buffer.concat(chunks)));
      };
      const sock = net.connect({ host, port }, () => {
        sock.write(Buffer.from(request));
      });
      sock.on("data", (d) => chunks.push(d));
      sock.on("end", finish);
      sock.on("close", finish);
      sock.on("error", reject);
    });
  }
}

const addr = process.argv[2] ?? "127.0.0.1:4100";
const transport = new NodeTcpTransport();
let failures = 0;

function jsonReplacer(_k: string, v: unknown): unknown {
  if (typeof v === "bigint") return v.toString();
  if (v instanceof Uint8Array) return Buffer.from(v).toString("hex");
  return v;
}

async function expectOk(label: string, spec: QuerySpec): Promise<void> {
  try {
    const r = await runQuery(transport, addr, spec);
    const cols = r.table.schema.fields.map((f) => `${f.name}:${f.type}`);
    console.log(
      `[ok]  ${label}: rows=${r.rowCount} serverRows=${r.totalRows} cols=${cols.length} elapsed=${r.elapsedMs.toFixed(1)}ms`,
    );
    console.log(`      columns: ${cols.join(", ")}`);
    const row0 = r.table.get(0);
    if (row0) {
      const s = JSON.stringify(row0.toJSON(), jsonReplacer);
      console.log(`      row0: ${s.length > 280 ? s.slice(0, 280) + "…" : s}`);
    }
    if (r.rowCount !== Number(r.totalRows)) {
      console.log(`[FAIL] ${label}: client rows ${r.rowCount} != server rows ${r.totalRows}`);
      failures++;
    }
  } catch (e) {
    console.log(`[FAIL] ${label}: ${e instanceof Error ? e.message : String(e)}`);
    failures++;
  }
}

async function expectError(label: string, spec: QuerySpec): Promise<void> {
  try {
    await runQuery(transport, addr, spec);
    console.log(`[FAIL] ${label}: expected a StreamError, query succeeded`);
    failures++;
  } catch (e) {
    if (e instanceof QueryError) {
      console.log(`[ok]  ${label}: surfaced ${e.message}`);
    } else {
      console.log(`[FAIL] ${label}: expected QueryError, got ${e instanceof Error ? e.message : String(e)}`);
      failures++;
    }
  }
}

console.log(`# protocol smoke against ${addr}`);
await expectOk("metrics SELECT * LIMIT 25", { signal: Signal.Metrics, matchers: [], limit: 25n });
await expectOk("metrics SQL (SELECT * FROM metrics LIMIT 5)", {
  signal: Signal.Metrics,
  matchers: [],
  sql: "SELECT * FROM metrics LIMIT 5",
});
await expectError("metrics bad SQL (expect StreamError)", {
  signal: Signal.Metrics,
  matchers: [],
  sql: "SELEKT nonsense FROM",
});

console.log(failures === 0 ? "\nPASS" : `\nFAIL (${failures})`);
process.exit(failures === 0 ? 0 : 1);
