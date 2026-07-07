//! Numeric constants from `proto/{ingest,query}.schema.json`.
//!
//! Kept in sync by hand against the schemas. When a schema gains a new
//! constant, mirror it here. The binschema bindings are generated from
//! the binary structure only; constants are not.

/// Wire-format version. `(major << 8) | minor`.
pub const PROTOCOL_VERSION_V0: u16 = 0x0001;

// ── Hello.signals bitmask ──────────────────────────────────────────────
pub const SIGNAL_BIT_METRICS: u8 = 0x01;
pub const SIGNAL_BIT_LOGS: u8 = 0x02;
pub const SIGNAL_BIT_TRACES: u8 = 0x04;
pub const SIGNAL_BIT_PROFILES: u8 = 0x08;

// ── Batch.signal ───────────────────────────────────────────────────────
//
// `Dummy = 0xFE` is a v0.1-only placeholder used by the storage layer
// to exercise the pipeline before any real signal lands. The wire
// protocol carries `DummyBatch` records under this discriminator. The
// variant goes away when the first real signal arrives (no protocol
// version bump needed — earlier servers will simply reject 0xFE as
// REJECT_SIGNAL_NOT_ANNOUNCED).
//
// `0xFF` is reserved for `SIGNAL_ALL` in `FlowControl.signal` and must
// not be reused here.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Metrics = 1,
    Logs = 2,
    Traces = 3,
    Profiles = 4,
    Dummy = 0xFE,
}

impl Signal {
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Signal::Metrics),
            2 => Some(Signal::Logs),
            3 => Some(Signal::Traces),
            4 => Some(Signal::Profiles),
            0xFE => Some(Signal::Dummy),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Signal::Metrics => "metrics",
            Signal::Logs => "logs",
            Signal::Traces => "traces",
            Signal::Profiles => "profiles",
            Signal::Dummy => "dummy",
        }
    }
}

/// Used in `FlowControl.signal` to mean "every signal".
pub const SIGNAL_ALL: u8 = 0xFF;

// ── SeriesDictEntry.metric_type ─────────────────────────────────────────
// Free-form per-series type byte. It is not validated on the wire and lands
// only in the block meta sidecar (`series_types`), so it is advisory metadata
// for the query layer, not a correctness-bearing field. The values mirror the
// Prometheus metric-type enum. `UNKNOWN` is the right choice when the producer
// can't supply a type — notably Prometheus remote-write v1, whose sample stream
// carries no per-series type. Histograms/summaries are exploded into component
// series (`_bucket`/`_sum`/`_count`) upstream, so they appear here as the
// constituent series' types, not as a single HISTOGRAM series.
pub const METRIC_TYPE_UNKNOWN: u8 = 0;
pub const METRIC_TYPE_COUNTER: u8 = 1;
pub const METRIC_TYPE_GAUGE: u8 = 2;
pub const METRIC_TYPE_HISTOGRAM: u8 = 3;
pub const METRIC_TYPE_SUMMARY: u8 = 4;

// ── Batch.compression ──────────────────────────────────────────────────
pub const COMPRESSION_NONE: u8 = 0;
pub const COMPRESSION_ZSTD: u8 = 1;

// ── BatchAck.status ────────────────────────────────────────────────────
pub const ACK_ACCEPTED: u8 = 0;
pub const ACK_THROTTLED: u8 = 1;
pub const ACK_REJECTED: u8 = 2;

// ── BatchAck.reason_code (when status == REJECTED) ─────────────────────
pub const REJECT_BAD_SCHEMA: u16 = 1;
pub const REJECT_BAD_FINGERPRINT: u16 = 2;
pub const REJECT_UNKNOWN_SERIES: u16 = 3;
pub const REJECT_TIMESTAMP_OUT_OF_RANGE: u16 = 4;
pub const REJECT_BATCH_TOO_LARGE: u16 = 5;
pub const REJECT_SIGNAL_NOT_ANNOUNCED: u16 = 6;
pub const REJECT_CARDINALITY_LIMIT: u16 = 7;
pub const REJECT_INTERNAL: u16 = 99;

// ── Error.code (connection-killing) ────────────────────────────────────
pub const ERR_PROTOCOL_VERSION: u16 = 1;
pub const ERR_BAD_FRAMING: u16 = 2;
pub const ERR_DUPLICATE_HELLO: u16 = 3;
pub const ERR_HELLO_REQUIRED: u16 = 4;
pub const ERR_SESSION_MISMATCH: u16 = 5;
pub const ERR_INFLIGHT_EXCEEDED: u16 = 6;
pub const ERR_AUTH: u16 = 7;
/// A `Subscribe` frame carried a matcher spec that failed to parse.
pub const ERR_BAD_MATCHER: u16 = 8;
/// The queryd live-tail front-door cannot serve a subscription because it has
/// no Valkey to discover ingesters through (see D-053). A direct `--ingest`
/// tail does not hit this — only the queryd relay refuses.
pub const ERR_TAIL_UNAVAILABLE: u16 = 9;
pub const ERR_INTERNAL: u16 = 255;

// ── Goodbye.reason_code ────────────────────────────────────────────────
pub const GOODBYE_NORMAL: u16 = 0;
pub const GOODBYE_SERVER_DRAINING: u16 = 1;
pub const GOODBYE_AGENT_RELOAD: u16 = 2;

// ── HelloAck defaults ──────────────────────────────────────────────────
pub const DEFAULT_SUGGESTED_BATCH_BYTES: u32 = 4 * 1024 * 1024;
pub const DEFAULT_MAX_BATCH_BYTES: u32 = 16 * 1024 * 1024;
pub const DEFAULT_MAX_INFLIGHT_BATCHES: u16 = 64;

// ── Query protocol (proto/query.schema.json) ───────────────────────────
//
// StreamError.code values. The query daemon emits exactly one
// StreamError before closing the connection on the failure paths
// below; clients should treat receipt as terminal.

/// Decoding the client's `QueryRequest` frame failed, or the request
/// fields are invalid (e.g. a matcher with empty name).
pub const QUERY_ERR_BAD_REQUEST: u16 = 0x0001;

/// DataFusion's SQL parser rejected the request's `sql` text.
pub const QUERY_ERR_SQL_PARSE: u16 = 0x0002;

/// `create_physical_plan` (or earlier planning) failed for a reason
/// other than SQL parse — e.g. an unknown column reference, an
/// unsupported aggregate.
pub const QUERY_ERR_PLAN: u16 = 0x0003;

/// DataFusion's `MemoryPool` returned `ResourcesExhausted` while the
/// query was running. Daemon stays up; the next query starts with
/// the budget freshly available.
pub const QUERY_ERR_RESOURCES: u16 = 0x0004;

/// A `live` merged history+live query (D-054) was requested but the
/// server can't serve the live half: it has no Valkey connection, so it
/// can't discover the ingesters to fan in from. Per decision 3 the live
/// portion is refused outright rather than silently degraded to
/// blocks-only. The client may retry without `--live`.
pub const QUERY_ERR_LIVE_UNAVAILABLE: u16 = 0x0005;

/// Catch-all for any other server-side failure (catalog mutex
/// poisoned, unexpected DataFusion error, postings sidecar fetch
/// failure mid-query, …). Message field carries human-readable
/// context.
pub const QUERY_ERR_INTERNAL: u16 = 0x00FF;

/// Human-readable name for a query error code; used by the client to
/// format error messages without re-doing the match in the call site.
pub fn query_err_name(code: u16) -> &'static str {
    match code {
        QUERY_ERR_BAD_REQUEST => "QUERY_ERR_BAD_REQUEST",
        QUERY_ERR_SQL_PARSE => "QUERY_ERR_SQL_PARSE",
        QUERY_ERR_PLAN => "QUERY_ERR_PLAN",
        QUERY_ERR_RESOURCES => "QUERY_ERR_RESOURCES",
        QUERY_ERR_LIVE_UNAVAILABLE => "QUERY_ERR_LIVE_UNAVAILABLE",
        QUERY_ERR_INTERNAL => "QUERY_ERR_INTERNAL",
        _ => "QUERY_ERR_UNKNOWN",
    }
}
