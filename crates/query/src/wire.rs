//! Ergonomic in-memory mirror of the binschema-defined query
//! protocol's [`QueryRequest`](scry_proto::QueryRequest), plus the
//! conversion glue between this struct and the generated wire type.
//!
//! The generated `QueryRequestInput` / `QueryRequestOutput` types are
//! mechanical — every optional field is modelled as a `*_present: u8`
//! companion (binschema 0.5.x's Rust generator emits `NotImplemented`
//! for `optional` fields inside discriminated_union variants; see the
//! note in `proto/query.schema.json`). Working with those types
//! directly at every call site is awkward, so this module owns the
//! ergonomic shape — `Option<u64>`, `Option<String>` — that callers
//! use, and translates to/from the wire form on transmission.
//!
//! [`QueryRequest`] kept the same field set as the pre-step-5
//! `flight_proto::QueryRequest` so the CLI's flag plumbing didn't
//! have to change.

use crate::Query;
use scry_proto::{constants::Signal, Matcher, QueryRequestInput, QueryRequestOutput};
use serde::{Deserialize, Serialize};

/// What the query client sends at the start of every query connection.
/// Mirrors the schema's `QueryRequest` but with ergonomic Rust types:
/// `Option<T>` instead of present-bit companions, `Vec<(String, String)>`
/// matchers instead of a `Vec<Matcher>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
    /// Target signal byte. `1` = metrics, `2` = logs. Server fails
    /// with `QUERY_ERR_BAD_REQUEST` if it sees `0` (which the wire
    /// encoder accepts but the protocol forbids) or an unimplemented
    /// signal. Defaults to `Signal::Metrics` in
    /// [`QueryRequest::default`] so existing call sites that never
    /// set it continue to query metrics.
    pub signal: u8,

    /// AND'd equality matchers + time bounds, shared across signals
    /// (see [`crate::Query`]). Resolved via postings sidecars at the
    /// server before scan() — identical to the local-CLI path.
    pub query: Query,

    /// Optional SQL against the registered table for this signal
    /// (`metrics` or `logs`). The matcher / time-bound preselect
    /// still applies; the SQL text runs against the already-narrowed
    /// table. If absent the server runs `SELECT * FROM <signal>`
    /// (with `limit` applied) so the remote-mode CLI's default
    /// matches the local-mode default.
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

impl Default for QueryRequest {
    fn default() -> Self {
        Self {
            // Metrics has been the default signal since v0.2; logs
            // callers explicitly opt in via the CLI's `--signal logs`.
            signal: Signal::Metrics as u8,
            query: Query::default(),
            sql: None,
            limit: None,
            request_id: None,
        }
    }
}

impl QueryRequest {
    /// Convert to the binschema wire shape. Empty `sql` / `request_id`
    /// strings encode "absent" on the wire — both fields have empty
    /// string as a natural absent sentinel (the server already treats
    /// "" sql as "use default SELECT *"). `limit = 0` similarly means
    /// "no limit". Only the timestamp bounds need explicit present-bits
    /// since 0 is a valid ts value.
    pub fn to_wire(&self) -> QueryRequestInput {
        let matchers = self
            .query
            .matchers
            .iter()
            .map(|(k, v)| Matcher {
                name: k.clone(),
                value: v.clone(),
            })
            .collect();
        QueryRequestInput {
            signal: self.signal,
            matchers,
            ts_min_present: u8::from(self.query.ts_min.is_some()),
            ts_min: self.query.ts_min.unwrap_or(0),
            ts_max_present: u8::from(self.query.ts_max.is_some()),
            ts_max: self.query.ts_max.unwrap_or(0),
            sql: self.sql.clone().unwrap_or_default(),
            limit: self.limit.map(|n| n as u64).unwrap_or(0),
            request_id: self.request_id.clone().unwrap_or_default(),
        }
    }

    /// Decode from the binschema wire shape. Inverse of `to_wire`.
    pub fn from_wire(w: QueryRequestOutput) -> Self {
        let matchers = w
            .matchers
            .into_iter()
            .map(|m| (m.name, m.value))
            .collect();
        let ts_min = if w.ts_min_present != 0 {
            Some(w.ts_min)
        } else {
            None
        };
        let ts_max = if w.ts_max_present != 0 {
            Some(w.ts_max)
        } else {
            None
        };
        let sql = if w.sql.is_empty() { None } else { Some(w.sql) };
        let limit = if w.limit == 0 {
            None
        } else {
            Some(w.limit as usize)
        };
        let request_id = if w.request_id.is_empty() {
            None
        } else {
            Some(w.request_id)
        };
        Self {
            signal: w.signal,
            query: Query {
                matchers,
                ts_min,
                ts_max,
            },
            sql,
            limit,
            request_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(req: QueryRequest) {
        let wire = req.to_wire();
        let out = QueryRequestOutput {
            tag: 1,
            signal: wire.signal,
            matchers: wire.matchers,
            ts_min_present: wire.ts_min_present,
            ts_min: wire.ts_min,
            ts_max_present: wire.ts_max_present,
            ts_max: wire.ts_max,
            sql: wire.sql,
            limit: wire.limit,
            request_id: wire.request_id,
        };
        let back = QueryRequest::from_wire(out);
        assert_eq!(back.signal, req.signal);
        assert_eq!(back.query.matchers, req.query.matchers);
        assert_eq!(back.query.ts_min, req.query.ts_min);
        assert_eq!(back.query.ts_max, req.query.ts_max);
        assert_eq!(back.sql, req.sql);
        assert_eq!(back.limit, req.limit);
        assert_eq!(back.request_id, req.request_id);
    }

    #[test]
    fn empty_request() {
        roundtrip(QueryRequest::default());
    }

    #[test]
    fn populated_request_metrics() {
        roundtrip(QueryRequest {
            signal: Signal::Metrics as u8,
            query: Query {
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
        });
    }

    #[test]
    fn populated_request_logs() {
        roundtrip(QueryRequest {
            signal: Signal::Logs as u8,
            query: Query {
                matchers: vec![("service".into(), "api".into())],
                ts_min: Some(1_700_000_000_000_000_000),
                ts_max: None,
            },
            sql: Some("SELECT count(*) FROM logs".into()),
            limit: Some(100),
            request_id: Some("logs-1".into()),
        });
    }

    #[test]
    fn ts_zero_is_preserved_when_present() {
        // 0 is a legitimate ts_unix_nano (Unix epoch). The present
        // bit lets us distinguish "user supplied 0" from "unset".
        roundtrip(QueryRequest {
            query: Query {
                matchers: vec![],
                ts_min: Some(0),
                ts_max: None,
            },
            ..Default::default()
        });
    }
}
