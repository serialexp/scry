//! The scry query-protocol client, in TypeScript.
//!
//! Drives one query end to end: build a `QueryRequest` frame from a
//! high-level spec, hand it to a `Transport`, then de-frame and decode
//! the response — `SchemaMsg` + `BatchMsg*` carry Arrow IPC bytes which
//! we concatenate into a single Arrow stream and parse with
//! `apache-arrow`. `EndOfStream` gives the server's row count for a
//! cross-check; `StreamError` becomes a thrown `QueryError`.
//!
//! All wire knowledge lives here and in the generated `../proto`
//! bindings — nothing protocol-specific leaks into the UI.

import { tableFromIPC, type Table } from "apache-arrow";
import {
  QueryFrameEncoder,
  QueryFrameDecoder,
  type QueryFrameInput,
  type QueryRequestInput,
  type QueryRequestOutput,
  type SchemaMsgOutput,
  type BatchMsgOutput,
  type EndOfStreamOutput,
  type StreamErrorOutput,
} from "../proto/generated";
import { frame, deframe } from "./framing";
import { queryErrName } from "./constants";
import type { Transport } from "./transport";

// ── Generator-bug bridge ─────────────────────────────────────────────
//
// The binschema TS generator (0.6.x) declares `QueryFrame.msg` as a bare
// union (`QueryRequestOutput | SchemaMsgOutput | …`), but the emitted
// encoder/decoder actually use a tagged `{ type, value }` envelope at
// runtime (the encoder branches on `value.msg.type`; the decoder sets
// `value.msg = { type, value }`). The runtime contract is the correct
// one — the static type just doesn't reflect the tag. Until the
// generator is fixed, we model the real shape here and bridge with a
// single cast at each boundary. (Reported separately to the binschema
// repo; see desktop/README.md.)
type TaggedFrame =
  | { type: "QueryRequest"; value: QueryRequestOutput }
  | { type: "SchemaMsg"; value: SchemaMsgOutput }
  | { type: "BatchMsg"; value: BatchMsgOutput }
  | { type: "EndOfStream"; value: EndOfStreamOutput }
  | { type: "StreamError"; value: StreamErrorOutput };

/** High-level, ergonomic query description (the UI's vocabulary). */
export interface QuerySpec {
  /** Signal byte (see `Signal`). */
  signal: number;
  /** AND'd equality label matchers. */
  matchers: { name: string; value: string }[];
  /** Inclusive lower time bound (unix nanos). Omit for none. */
  tsMin?: bigint;
  /** Inclusive upper time bound (unix nanos). Omit for none. */
  tsMax?: bigint;
  /** SQL against the registered table for the signal. Omit for `SELECT *`. */
  sql?: string;
  /** Row cap. Omit / 0 = no limit. Ignored by the server when `sql` is set. */
  limit?: bigint;
  /** Caller-supplied correlation id for the daemon's logs. */
  requestId?: string;
  /** 16 raw bytes — traces by-id lookup. Omit for non-traces / no lookup. */
  traceId?: Uint8Array;
}

export interface QueryResult {
  /** The decoded Arrow table (schema + rows). */
  table: Table;
  /** Rows the client actually decoded. */
  rowCount: number;
  /** Rows the server reports it emitted (cross-check against `rowCount`). */
  totalRows: bigint;
  /** Wall-clock round-trip, milliseconds. */
  elapsedMs: number;
}

/** A protocol-level `StreamError` frame, surfaced as an exception. */
export class QueryError extends Error {
  constructor(
    public readonly code: number,
    public readonly serverMessage: string,
  ) {
    super(
      `${queryErrName(code)} (0x${code.toString(16).padStart(4, "0")}): ${serverMessage}`,
    );
    this.name = "QueryError";
  }
}

function buildRequestFrame(spec: QuerySpec): Uint8Array {
  const value: QueryRequestInput = {
    signal: spec.signal,
    matchers: spec.matchers.map((m) => ({ name: m.name, value: m.value })),
    ts_min_present: spec.tsMin !== undefined ? 1 : 0,
    ts_min: spec.tsMin ?? 0n,
    ts_max_present: spec.tsMax !== undefined ? 1 : 0,
    ts_max: spec.tsMax ?? 0n,
    sql: spec.sql ?? "",
    limit: spec.limit ?? 0n,
    request_id: spec.requestId ?? "",
    trace_id: spec.traceId ? Array.from(spec.traceId) : [],
  };
  // Cast: the runtime encoder wants the tagged `{ type, value }` shape
  // (see TaggedFrame note above), which the declared `QueryFrameInput`
  // type doesn't express.
  const frameInput = {
    msg: { type: "QueryRequest", value },
  } as unknown as QueryFrameInput;
  const body = new QueryFrameEncoder().encode(frameInput);
  return frame(body);
}

function concatChunks(chunks: Uint8Array[]): Uint8Array {
  const total = chunks.reduce((n, c) => n + c.length, 0);
  const out = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) {
    out.set(c, off);
    off += c.length;
  }
  return out;
}

/**
 * Run a query against `addr` over `transport`. Resolves with the decoded
 * table + counts, or rejects with a `QueryError` (protocol-level) or a
 * plain `Error` (transport/decoding failure).
 */
export async function runQuery(
  transport: Transport,
  addr: string,
  spec: QuerySpec,
): Promise<QueryResult> {
  const started = performance.now();
  const requestFrame = buildRequestFrame(spec);
  const responseBytes = await transport.query(addr, requestFrame);

  // Schema first, then any batch/dictionary messages — concatenated they
  // form a single Arrow IPC stream we can hand to `tableFromIPC`.
  const ipcChunks: Uint8Array[] = [];
  let totalRows = 0n;
  let sawTerminator = false;

  for (const body of deframe(responseBytes)) {
    const decoded = new QueryFrameDecoder(body).decode();
    // Cast: the decoder returns the tagged `{ type, value }` runtime
    // shape, not the bare union the type declares (see TaggedFrame note).
    const msg = (decoded as unknown as { msg: TaggedFrame }).msg;
    switch (msg.type) {
      case "SchemaMsg":
      case "BatchMsg":
        ipcChunks.push(Uint8Array.from(msg.value.ipc_bytes));
        break;
      case "EndOfStream":
        totalRows = msg.value.total_rows;
        sawTerminator = true;
        break;
      case "StreamError":
        throw new QueryError(msg.value.code, msg.value.message);
      default:
        // A `QueryRequest` from the server would be a protocol violation;
        // ignore anything unexpected rather than mis-decode.
        break;
    }
  }

  if (!sawTerminator) {
    throw new Error(
      "query stream ended without EndOfStream or StreamError (server closed early?)",
    );
  }
  if (ipcChunks.length === 0) {
    throw new Error("server sent no schema frame");
  }

  const table = tableFromIPC(concatChunks(ipcChunks));
  return {
    table,
    rowCount: table.numRows,
    totalRows,
    elapsedMs: performance.now() - started,
  };
}
