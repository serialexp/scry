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
  type LabelNamesRequestInput,
  type LabelValuesRequestInput,
  type LabelNamesResponseOutput,
  type LabelValuesResponseOutput,
  type FleetStatusRequestInput,
  type FleetStatusResponseOutput,
  type SchemaMsgOutput,
  type BatchMsgOutput,
  type ResponseSupersededOutput,
  type EndOfStreamOutput,
  type QueryStatsOutput,
  type StreamErrorOutput,
} from "../proto/generated";
import { frame, deframe } from "./framing";
import { QUERY_CAP_ATTEMPT_SUPERSESSION, queryErrName } from "./constants";
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
  | { type: "ResponseSuperseded"; value: ResponseSupersededOutput }
  | { type: "QueryStats"; value: QueryStatsOutput }
  | { type: "EndOfStream"; value: EndOfStreamOutput }
  | { type: "LabelNamesResponse"; value: LabelNamesResponseOutput }
  | { type: "LabelValuesResponse"; value: LabelValuesResponseOutput }
  | { type: "FleetStatusResponse"; value: FleetStatusResponseOutput }
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
  /** Full-text substring over log `body` (logs only). Omit / "" = absent. */
  bodyContains?: string;
  /** Merged history+live view (logs only, D-054). Omit / false = blocks only. */
  live?: boolean;
  /** Metrics only: request the synthesised `labels` `Map<Utf8,Utf8>` column
   *  (the D-058 fingerprint→label join). Omit / false = fingerprint-only. */
  withLabels?: boolean;
}

/** One phase of a query, in milliseconds. */
export interface TimingPhase {
  label: string;
  ms: number;
}

/**
 * Where a query's time went (D-066), assembled from the server's `QueryStats`
 * frame plus two measurements only the client can make.
 *
 * The three groups are **not** interchangeable and a UI must not mix them:
 *
 * - `phases` are sequential wall-clock spans of the server's timeline. They sum
 *   to `serverMs` exactly, because `other` is included as an explicit term
 *   rather than the remainder being smeared across the named phases.
 * - `transportMs` / `decodeMs` are the client's own halves — network plus Arrow
 *   decode — which the daemon cannot see.
 * - `datafusion` and the sidecar fetch totals are summed across partitions and
 *   concurrent fetches. They can legitimately exceed the phase that contains
 *   them, so they are reported apart and never drawn as timeline slices.
 */
export interface QueryTiming {
  /** Server-side total, including time queued before the request was read. */
  serverMs: number;
  /** Sequential server phases, `other` last. Only non-zero phases are listed. */
  phases: TimingPhase[];
  /** `elapsedMs − serverMs − decodeMs`: the network round trip. */
  transportMs: number;
  /** Time this client spent in `tableFromIPC`. */
  decodeMs: number;
  /** Whether the server answered from its result cache. */
  cacheHit: boolean;
  /** Response attempts, >1 when a block vanished mid-scan and forced a replan. */
  attempts: number;
  blocksScanned: number;
  blocksConsidered: number;
  bytesScanned: bigint;
  /** Object-store waits, summed over concurrent fetches — not timeline slices. */
  postingsFetchMs: number;
  bloomFetchMs: number;
  /** Summed across partitions — can exceed `execute`. Never a timeline slice. */
  datafusion: { openingMs: number; scanningMs: number; computeMs: number };
  /** Which daemon produced this. Empty if it didn't identify itself. */
  nodeId: string;
  /** Per-ingester fan-out for a `live` query; empty for an ordinary one. */
  liveNodes: { addr: string; ms: number; rows: bigint; ok: boolean }[];
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
  /**
   * Phase breakdown, when the server sent a `QueryStats` frame. `undefined`
   * against a daemon older than D-066 — the query still succeeds, so callers
   * must treat this as optional rather than assuming it.
   */
  timing?: QueryTiming;
}

/** Microseconds (server wire units) → milliseconds, at 3 decimal places. */
function usToMs(us: bigint | number): number {
  return Number(us) / 1000;
}

/**
 * Turn a raw `QueryStats` frame plus the client's own two measurements into a
 * `QueryTiming`.
 *
 * Exported for tests: the arithmetic here — specifically that the phases,
 * `other` included, sum back to `serverMs` — is the property that makes the
 * waterfall honest, and it deserves a test that does not need a live daemon.
 */
export function buildQueryTiming(
  s: QueryStatsOutput,
  elapsedMs: number,
  decodeMs: number,
): QueryTiming {
  const named: [string, bigint | number][] = [
    ["admission", s.admission_wait_us],
    ["catalog", s.catalog_us],
    ["cache-lookup", s.cache_lookup_us],
    ["live-fetch", s.live_fetch_us],
    ["register", s.register_us],
    ["plan", s.plan_us],
    ["execute", s.execute_us],
    ["serialize", s.serialize_us],
    ["write", s.write_us],
  ];
  const serverMs = usToMs(s.server_total_us);
  const namedMs = named.reduce((acc, [, us]) => acc + usToMs(us), 0);
  // The residual is a first-class phase, not a rounding error to hide. The
  // server measures its total independently of the parts, so anything the named
  // phases don't cover is real time that went somewhere unnamed — most often
  // reading the request off the socket, which happens before the phase timers
  // start. Clamped at 0 because the two measurements are independent and a
  // scheduling hiccup could in principle invert them by microseconds.
  const otherMs = Math.max(0, serverMs - namedMs);

  const phases: TimingPhase[] = named
    .filter(([, us]) => Number(us) > 0)
    .map(([label, us]) => ({ label, ms: usToMs(us) }));
  phases.push({ label: "other", ms: otherMs });

  return {
    serverMs,
    phases,
    // What is left after the server's own time and our Arrow decode is the
    // network. This is the half of "why did that take 9 seconds?" that the
    // daemon's logs cannot answer.
    transportMs: Math.max(0, elapsedMs - serverMs - decodeMs),
    decodeMs,
    cacheHit: Number(s.cache_hit) === 1,
    attempts: Number(s.attempts),
    blocksScanned: Number(s.blocks_scanned),
    blocksConsidered: Number(s.blocks_considered),
    bytesScanned: BigInt(s.bytes_scanned),
    postingsFetchMs: usToMs(s.postings_fetch_us),
    bloomFetchMs: usToMs(s.bloom_fetch_us),
    datafusion: {
      openingMs: usToMs(s.df_opening_us),
      scanningMs: usToMs(s.df_scanning_us),
      computeMs: usToMs(s.df_compute_us),
    },
    nodeId: s.node_id,
    liveNodes: (s.live_nodes ?? []).map((n) => ({
      addr: n.addr,
      ms: usToMs(n.elapsed_us),
      rows: BigInt(n.rows),
      ok: Number(n.ok) === 1,
    })),
  };
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
    body_contains: spec.bodyContains ?? "",
    live: spec.live ? 1 : 0,
    with_labels: spec.withLabels ? 1 : 0,
    capabilities: QUERY_CAP_ATTEMPT_SUPERSESSION,
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
  let ipcChunks: Uint8Array[] = [];
  let totalRows = 0n;
  let sawTerminator = false;
  let activeAttempt = 0;
  let awaitingSchema = true;
  let stats: QueryStatsOutput | undefined;
  const maxSupersededAttempts = 2;

  for (const body of deframe(responseBytes)) {
    if (sawTerminator) throw new Error("server sent a frame after EndOfStream");
    const decoded = new QueryFrameDecoder(body).decode();
    // Cast: the decoder returns the tagged `{ type, value }` runtime
    // shape, not the bare union the type declares (see TaggedFrame note).
    const msg = (decoded as unknown as { msg: TaggedFrame }).msg;
    switch (msg.type) {
      case "SchemaMsg":
        if (!awaitingSchema) throw new Error("duplicate schema in query attempt");
        ipcChunks.push(Uint8Array.from(msg.value.ipc_bytes));
        awaitingSchema = false;
        break;
      case "BatchMsg":
        if (awaitingSchema) throw new Error("batch received before schema");
        ipcChunks.push(Uint8Array.from(msg.value.ipc_bytes));
        break;
      case "ResponseSuperseded":
        if (
          awaitingSchema ||
          activeAttempt >= maxSupersededAttempts ||
          msg.value.superseded_attempt !== activeAttempt ||
          msg.value.next_attempt !== activeAttempt + 1
        ) {
          throw new Error("invalid ResponseSuperseded attempt transition");
        }
        // Strict reset: no Arrow bytes (including dictionaries) from the old
        // attempt survive, and the next frame is required to be a schema.
        ipcChunks = [];
        activeAttempt = msg.value.next_attempt;
        awaitingSchema = true;
        // A superseded attempt's timings describe work that was thrown away.
        stats = undefined;
        break;
      case "QueryStats":
        // Sent immediately before the terminator. Optional by design: an older
        // daemon simply never sends one and the query still succeeds.
        stats = msg.value;
        break;
      case "EndOfStream":
        if (awaitingSchema) throw new Error("EndOfStream received before schema");
        totalRows = msg.value.total_rows;
        sawTerminator = true;
        break;
      case "StreamError":
        throw new QueryError(msg.value.code, msg.value.message);
      default:
        throw new Error(`unexpected ${msg.type} frame in data-query response`);
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

  // Timed on its own: Arrow decode is client-side CPU, and folding it into the
  // round trip would make a big result look like a slow network.
  const decodeStarted = performance.now();
  const table = tableFromIPC(concatChunks(ipcChunks));
  const decodeMs = performance.now() - decodeStarted;
  if (BigInt(table.numRows) !== totalRows) {
    throw new Error(
      `query row-count mismatch: decoded ${table.numRows}, server reported ${totalRows}`,
    );
  }
  const elapsedMs = performance.now() - started;
  return {
    table,
    rowCount: table.numRows,
    totalRows,
    elapsedMs,
    timing: stats ? buildQueryTiming(stats, elapsedMs, decodeMs) : undefined,
  };
}

// ── Label metadata (discoverability) ─────────────────────────────────
//
// One request → one terminal response frame → close, over the same
// `Transport` the data query uses (so scry-webui's dumb byte-pipe relays
// it unchanged). Answers "what can I match on?" from the daemon's label
// cache; see the query schema's LabelNames/LabelValues variants and D-050.

/** Signal + optional time window scoping a metadata request. */
export interface MetaScope {
  signal: number;
  tsMin?: bigint;
  tsMax?: bigint;
}

function decodeMetaResponse(responseBytes: Uint8Array): TaggedFrame {
  for (const body of deframe(responseBytes)) {
    const decoded = new QueryFrameDecoder(body).decode();
    return (decoded as unknown as { msg: TaggedFrame }).msg;
  }
  throw new Error("metadata stream ended with no frame (server closed early?)");
}

/** Fetch the distinct, sorted label names matchable for `scope`. */
export async function fetchLabelNames(
  transport: Transport,
  addr: string,
  scope: MetaScope,
): Promise<string[]> {
  const value: LabelNamesRequestInput = {
    signal: scope.signal,
    ts_min_present: scope.tsMin !== undefined ? 1 : 0,
    ts_min: scope.tsMin ?? 0n,
    ts_max_present: scope.tsMax !== undefined ? 1 : 0,
    ts_max: scope.tsMax ?? 0n,
    capabilities: QUERY_CAP_ATTEMPT_SUPERSESSION,
  };
  const frameInput = {
    msg: { type: "LabelNamesRequest", value },
  } as unknown as QueryFrameInput;
  const requestFrame = frame(new QueryFrameEncoder().encode(frameInput));
  const responseBytes = await transport.query(addr, requestFrame);
  const msg = decodeMetaResponse(responseBytes);
  if (msg.type === "StreamError") throw new QueryError(msg.value.code, msg.value.message);
  if (msg.type === "LabelNamesResponse") return msg.value.names;
  throw new Error(`expected LabelNamesResponse, got ${msg.type}`);
}

/** Fetch the distinct, sorted values `name` takes for `scope`. */
export async function fetchLabelValues(
  transport: Transport,
  addr: string,
  scope: MetaScope,
  name: string,
): Promise<string[]> {
  const value: LabelValuesRequestInput = {
    signal: scope.signal,
    label_name: name,
    ts_min_present: scope.tsMin !== undefined ? 1 : 0,
    ts_min: scope.tsMin ?? 0n,
    ts_max_present: scope.tsMax !== undefined ? 1 : 0,
    ts_max: scope.tsMax ?? 0n,
    capabilities: QUERY_CAP_ATTEMPT_SUPERSESSION,
  };
  const frameInput = {
    msg: { type: "LabelValuesRequest", value },
  } as unknown as QueryFrameInput;
  const requestFrame = frame(new QueryFrameEncoder().encode(frameInput));
  const responseBytes = await transport.query(addr, requestFrame);
  const msg = decodeMetaResponse(responseBytes);
  if (msg.type === "StreamError") throw new QueryError(msg.value.code, msg.value.message);
  if (msg.type === "LabelValuesResponse") return msg.value.values;
  throw new Error(`expected LabelValuesResponse, got ${msg.type}`);
}

// ── Fleet status ─────────────────────────────────────────────────────

/** Common status envelope published by agents, ingestd, queryd, and gateways.
 * `data` remains role-specific so new counters can be added without a wire change. */
export interface FleetInstance {
  role: string;
  instance_id: string;
  addr: string;
  /** Absent only for status documents published by pre-version-field binaries. */
  version?: string;
  now_unix_ms: number;
  uptime_secs: number;
  rss_kib: number | null;
  data: Record<string, unknown>;
}

/** Fetch every currently-live status document from the selected queryd's
 * Valkey registry. The response is one terminal, non-Arrow frame. */
export async function fetchFleetStatus(
  transport: Transport,
  addr: string,
): Promise<FleetInstance[]> {
  const value: FleetStatusRequestInput = {};
  const frameInput = {
    msg: { type: "FleetStatusRequest", value },
  } as unknown as QueryFrameInput;
  const requestFrame = frame(new QueryFrameEncoder().encode(frameInput));
  const responseBytes = await transport.query(addr, requestFrame);
  const msg = decodeMetaResponse(responseBytes);
  if (msg.type === "StreamError") throw new QueryError(msg.value.code, msg.value.message);
  if (msg.type !== "FleetStatusResponse") {
    throw new Error(`expected FleetStatusResponse, got ${msg.type}`);
  }

  return msg.value.instances_json.map((json) => {
    const parsed = JSON.parse(json) as FleetInstance;
    if (
      !parsed ||
      typeof parsed.role !== "string" ||
      typeof parsed.instance_id !== "string" ||
      typeof parsed.addr !== "string" ||
      (parsed.version !== undefined && typeof parsed.version !== "string") ||
      typeof parsed.now_unix_ms !== "number" ||
      typeof parsed.uptime_secs !== "number" ||
      (parsed.rss_kib !== null && typeof parsed.rss_kib !== "number") ||
      !parsed.data ||
      typeof parsed.data !== "object" ||
      Array.isArray(parsed.data)
    ) {
      throw new Error("queryd returned an invalid fleet status document");
    }
    return parsed;
  });
}
