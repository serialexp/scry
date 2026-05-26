//! Numeric constants from `proto/ingest.schema.json`.
//!
//! Kept in sync by hand against the schema's `protocol.constants` block.
//! When the schema gains a new constant, mirror it here. The bindings are
//! generated from the binary structure only; constants are not.

/// Wire-format version. `(major << 8) | minor`.
pub const PROTOCOL_VERSION_V0: u16 = 0x0001;

// ── Hello.signals bitmask ──────────────────────────────────────────────
pub const SIGNAL_BIT_METRICS:  u8 = 0x01;
pub const SIGNAL_BIT_LOGS:     u8 = 0x02;
pub const SIGNAL_BIT_TRACES:   u8 = 0x04;
pub const SIGNAL_BIT_PROFILES: u8 = 0x08;

// ── Batch.signal ───────────────────────────────────────────────────────
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Metrics  = 1,
    Logs     = 2,
    Traces   = 3,
    Profiles = 4,
}

impl Signal {
    pub fn as_u8(self) -> u8 { self as u8 }

    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Signal::Metrics),
            2 => Some(Signal::Logs),
            3 => Some(Signal::Traces),
            4 => Some(Signal::Profiles),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Signal::Metrics  => "metrics",
            Signal::Logs     => "logs",
            Signal::Traces   => "traces",
            Signal::Profiles => "profiles",
        }
    }
}

/// Used in `FlowControl.signal` to mean "every signal".
pub const SIGNAL_ALL: u8 = 0xFF;

// ── Batch.compression ──────────────────────────────────────────────────
pub const COMPRESSION_NONE: u8 = 0;
pub const COMPRESSION_ZSTD: u8 = 1;

// ── BatchAck.status ────────────────────────────────────────────────────
pub const ACK_ACCEPTED:  u8 = 0;
pub const ACK_THROTTLED: u8 = 1;
pub const ACK_REJECTED:  u8 = 2;

// ── BatchAck.reason_code (when status == REJECTED) ─────────────────────
pub const REJECT_BAD_SCHEMA:             u16 = 1;
pub const REJECT_BAD_FINGERPRINT:        u16 = 2;
pub const REJECT_UNKNOWN_SERIES:         u16 = 3;
pub const REJECT_TIMESTAMP_OUT_OF_RANGE: u16 = 4;
pub const REJECT_BATCH_TOO_LARGE:        u16 = 5;
pub const REJECT_SIGNAL_NOT_ANNOUNCED:   u16 = 6;
pub const REJECT_CARDINALITY_LIMIT:      u16 = 7;
pub const REJECT_INTERNAL:               u16 = 99;

// ── Error.code (connection-killing) ────────────────────────────────────
pub const ERR_PROTOCOL_VERSION: u16 = 1;
pub const ERR_BAD_FRAMING:      u16 = 2;
pub const ERR_DUPLICATE_HELLO:  u16 = 3;
pub const ERR_HELLO_REQUIRED:   u16 = 4;
pub const ERR_SESSION_MISMATCH: u16 = 5;
pub const ERR_INFLIGHT_EXCEEDED: u16 = 6;
pub const ERR_AUTH:             u16 = 7;
pub const ERR_INTERNAL:         u16 = 255;

// ── Goodbye.reason_code ────────────────────────────────────────────────
pub const GOODBYE_NORMAL:          u16 = 0;
pub const GOODBYE_SERVER_DRAINING: u16 = 1;
pub const GOODBYE_AGENT_RELOAD:    u16 = 2;

// ── HelloAck defaults ──────────────────────────────────────────────────
pub const DEFAULT_SUGGESTED_BATCH_BYTES: u32 = 4 * 1024 * 1024;
pub const DEFAULT_MAX_BATCH_BYTES:       u32 = 16 * 1024 * 1024;
pub const DEFAULT_MAX_INFLIGHT_BATCHES:  u16 = 64;
