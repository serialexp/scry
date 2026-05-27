//! Wire format for the Arrow Flight query daemon (`scry-queryd`).
//!
//! The Flight `Ticket` is opaque protobuf bytes the server interprets
//! however it wants. We serialise [`QueryRequest`] as JSON into those
//! bytes for three reasons:
//!
//! 1. **Debuggability.** `tcpdump`, `wireshark`, `tonic`'s tracing
//!    middleware — all of them surface the ticket payload, and JSON
//!    stays human-readable wherever it lands.
//! 2. **Mechanical serde.** `MetricsQuery` already derives the standard
//!    Serde traits; one more `derive` on a wrapper is the entire format
//!    spec.
//! 3. **No protobuf-IDL overhead for v0.3.** We don't yet need wire
//!    forwards/backwards compatibility versioning across mismatched
//!    client/server pairs; the daemon and CLI ship from the same
//!    workspace. Switch to bincode/protobuf later if a hot path ever
//!    shows JSON in a flamegraph (it won't — the ticket is bytes, not
//!    rows).
//!
//! The request shape mirrors what `scry-query`'s local-only CLI takes
//! today: a `MetricsQuery` (matchers + time bounds, postings preselect),
//! optional SQL against the already-narrowed `metrics` table, optional
//! row LIMIT, and a caller-supplied tracing correlation id.

use anyhow::{Context, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::MetricsQuery;

/// What the Arrow Flight client puts in the `Ticket`. The daemon's
/// `do_get` handler deserialises this, builds + executes the same
/// DataFusion plan the local CLI would, and streams `RecordBatch`es
/// back as `FlightData`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryRequest {
    /// AND'd equality matchers + time bounds. Resolved via postings
    /// sidecars at the server before scan() — identical to the
    /// local-CLI path.
    pub metrics_query: MetricsQuery,

    /// Optional SQL against the registered `metrics` table. The
    /// matcher / time-bound preselect still applies; the SQL text
    /// runs against the already-narrowed table. If absent the server
    /// runs `SELECT * FROM metrics` (with `limit` applied) so the
    /// remote-mode CLI's default matches the local-mode default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,

    /// Optional `LIMIT N`. Ignored when `sql` is set (express the
    /// limit in the SQL itself).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,

    /// Caller-supplied tracing correlation id. The server logs this
    /// under `request_id` in every event/span for the query so a
    /// trace search by id finds the whole life-cycle. Absent =
    /// server picks one (monotonic per-process integer).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

impl QueryRequest {
    /// JSON-encode into bytes suitable for an Arrow Flight `Ticket`.
    /// Cheap; the payload is a few hundred bytes for any realistic
    /// matcher list.
    pub fn to_ticket_bytes(&self) -> Result<Bytes> {
        serde_json::to_vec(self)
            .map(Bytes::from)
            .context("serialising QueryRequest to JSON")
    }

    /// Decode from the ticket bytes the server received. Errors with
    /// context so the daemon can surface a `Status::invalid_argument`
    /// with a meaningful message instead of an opaque parse failure.
    pub fn from_ticket_bytes(b: &[u8]) -> Result<Self> {
        serde_json::from_slice(b).context("deserialising QueryRequest from ticket JSON")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_minimal() {
        let req = QueryRequest::default();
        let bytes = req.to_ticket_bytes().unwrap();
        let back = QueryRequest::from_ticket_bytes(&bytes).unwrap();
        assert!(back.metrics_query.matchers.is_empty());
        assert_eq!(back.sql, None);
        assert_eq!(back.limit, None);
        assert_eq!(back.request_id, None);
    }

    #[test]
    fn roundtrip_populated() {
        let req = QueryRequest {
            metrics_query: MetricsQuery {
                matchers: vec![
                    ("__name__".into(), "scry_http_requests_total".into()),
                    ("env".into(), "prod".into()),
                ],
                ts_min: Some(1_700_000_000_000_000_000),
                ts_max: Some(1_700_000_001_000_000_000),
            },
            sql: Some("SELECT count(*) FROM metrics".into()),
            limit: Some(10),
            request_id: Some("test-42".into()),
        };
        let bytes = req.to_ticket_bytes().unwrap();
        let back = QueryRequest::from_ticket_bytes(&bytes).unwrap();
        assert_eq!(back.metrics_query.matchers, req.metrics_query.matchers);
        assert_eq!(back.metrics_query.ts_min, req.metrics_query.ts_min);
        assert_eq!(back.metrics_query.ts_max, req.metrics_query.ts_max);
        assert_eq!(back.sql, req.sql);
        assert_eq!(back.limit, req.limit);
        assert_eq!(back.request_id, req.request_id);
    }

    #[test]
    fn rejects_invalid_json() {
        let err = QueryRequest::from_ticket_bytes(b"not json").unwrap_err();
        assert!(err.to_string().contains("deserialising QueryRequest"));
    }
}
