//! Hand-mirrored numeric constants from the scry query protocol.
//!
//! These mirror `crates/proto/src/constants.rs` (the Rust side mirrors
//! the schema by hand too — same contract). If a value changes in the
//! schema, update it here. Keep this the *only* place the TS client
//! hard-codes protocol numbers.

/** Target signal byte. Matches `scry_proto::constants::Signal`. */
export const Signal = {
  Metrics: 1,
  Logs: 2,
  Traces: 3,
  Profiles: 4,
} as const;

export type SignalName = keyof typeof Signal;
export type SignalByte = (typeof Signal)[SignalName];

export const SIGNAL_NAMES = Object.keys(Signal) as SignalName[];

/** Client supports strict reset-and-restart query response attempts. */
export const QUERY_CAP_ATTEMPT_SUPERSESSION = 0x0000_0001;

/** QUERY_ERR_* codes carried by a `StreamError` frame. */
export const QueryErrCode = {
  BAD_REQUEST: 0x0001,
  SQL_PARSE: 0x0002,
  PLAN: 0x0003,
  RESOURCES: 0x0004,
  LIVE_UNAVAILABLE: 0x0005,
  FLEET_UNAVAILABLE: 0x0006,
  INTERNAL: 0x00ff,
} as const;

const QUERY_ERR_NAMES: Record<number, string> = {
  [QueryErrCode.BAD_REQUEST]: "QUERY_ERR_BAD_REQUEST",
  [QueryErrCode.SQL_PARSE]: "QUERY_ERR_SQL_PARSE",
  [QueryErrCode.PLAN]: "QUERY_ERR_PLAN",
  [QueryErrCode.RESOURCES]: "QUERY_ERR_RESOURCES",
  [QueryErrCode.LIVE_UNAVAILABLE]: "QUERY_ERR_LIVE_UNAVAILABLE",
  [QueryErrCode.FLEET_UNAVAILABLE]: "QUERY_ERR_FLEET_UNAVAILABLE",
  [QueryErrCode.INTERNAL]: "QUERY_ERR_INTERNAL",
};

export function queryErrName(code: number): string {
  return QUERY_ERR_NAMES[code] ?? "QUERY_ERR_UNKNOWN";
}

/** Hard ceiling on a single framed message — mirrors `framing::MAX_FRAME_BYTES`. */
export const MAX_FRAME_BYTES = 32 * 1024 * 1024;

// ── Live tail (the ingest wire, not the query wire) ───────────────────
//
// The tail sub-protocol reuses the *ingest* `Frame` union (D-052/D-053), so
// these mirror the ingest half of `crates/proto/src/constants.rs`.

/** `Hello.protocol_version` — the only negotiation point on the ingest wire. */
export const PROTOCOL_VERSION_V0 = 0x0001;

/** `Hello.signals` bitmask. A tail client announces exactly the signal it is
 *  about to subscribe to — the handshake gates which signals a connection may
 *  carry, so announcing the wrong bit gets the subscription refused. */
export const SIGNAL_BIT_METRICS = 0x01;
export const SIGNAL_BIT_LOGS = 0x02;

/** The `Hello.signals` bit to announce when tailing `signal`. Only logs and
 *  metrics have an ingest tap (D-065). */
export function tailSignalBit(signal: SignalByte): number {
  return signal === Signal.Metrics ? SIGNAL_BIT_METRICS : SIGNAL_BIT_LOGS;
}

/** ERR_* codes carried by an ingest-wire `Error` frame. */
export const TailErrCode = {
  PROTOCOL_VERSION: 1,
  BAD_FRAMING: 2,
  HELLO_REQUIRED: 4,
  BAD_MATCHER: 8,
  /** No Valkey on the query daemon ⇒ no ingesters to discover, so it refuses
   *  rather than streaming nothing. */
  TAIL_UNAVAILABLE: 9,
  OVERLOADED: 10,
  INTERNAL: 255,
} as const;

const TAIL_ERR_NAMES: Record<number, string> = {
  [TailErrCode.PROTOCOL_VERSION]: "ERR_PROTOCOL_VERSION",
  [TailErrCode.BAD_FRAMING]: "ERR_BAD_FRAMING",
  [TailErrCode.HELLO_REQUIRED]: "ERR_HELLO_REQUIRED",
  [TailErrCode.BAD_MATCHER]: "ERR_BAD_MATCHER",
  [TailErrCode.TAIL_UNAVAILABLE]: "ERR_TAIL_UNAVAILABLE",
  [TailErrCode.OVERLOADED]: "ERR_OVERLOADED",
  [TailErrCode.INTERNAL]: "ERR_INTERNAL",
};

export function tailErrName(code: number): string {
  return TAIL_ERR_NAMES[code] ?? "ERR_UNKNOWN";
}
