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

/** QUERY_ERR_* codes carried by a `StreamError` frame. */
export const QueryErrCode = {
  BAD_REQUEST: 0x0001,
  SQL_PARSE: 0x0002,
  PLAN: 0x0003,
  RESOURCES: 0x0004,
  INTERNAL: 0x00ff,
} as const;

const QUERY_ERR_NAMES: Record<number, string> = {
  [QueryErrCode.BAD_REQUEST]: "QUERY_ERR_BAD_REQUEST",
  [QueryErrCode.SQL_PARSE]: "QUERY_ERR_SQL_PARSE",
  [QueryErrCode.PLAN]: "QUERY_ERR_PLAN",
  [QueryErrCode.RESOURCES]: "QUERY_ERR_RESOURCES",
  [QueryErrCode.INTERNAL]: "QUERY_ERR_INTERNAL",
};

export function queryErrName(code: number): string {
  return QUERY_ERR_NAMES[code] ?? "QUERY_ERR_UNKNOWN";
}

/** Hard ceiling on a single framed message — mirrors `framing::MAX_FRAME_BYTES`. */
export const MAX_FRAME_BYTES = 32 * 1024 * 1024;
