//! `scry-gateway` library: a fan-out hub for observability data.
//!
//! The gateway accepts records over several inbound protocols and **fans each
//! one out to every configured downstream sink** (all in → all out, no routing
//! config — for anything more selective, run a second gateway). Inbound:
//!
//! - [`wire`]: the native binschema ingest listener — `scry-agent` and any other
//!   native producer point here.
//! - OTLP/HTTP protobuf or JSON, optionally gzip-compressed, for traces, logs,
//!   and scalar metrics (`POST /v1/{traces,logs,metrics}`).
//! - [`otlp_grpc`]: OTLP/gRPC traces, logs, and scalar metrics.
//! - [`loki_ingest`]: Loki JSON or raw-Snappy protobuf (`POST /loki/api/v1/push`).
//! - [`pyroscope`]: legacy Pyroscope profile ingest (`POST /ingest`).
//! - [`pyroscope_push`]: modern Pyroscope Push v1 Connect HTTP.
//! - [`promwrite`]: Prometheus remote-write (`POST /api/v1/write`, `/api/v1/push`).
//!
//! Each inbound path decodes its request into a typed `*Batch` and hands it to
//! [`AppState`] (in [`sink`]), which offers it best-effort to every
//! [`sink::SinkHandle`] whose signal mask accepts it. Sinks:
//!
//! - [`sink_scry`]: the scry ingest server (native wire) — accepts all signals.
//! - [`loki`]: Grafana Loki push — logs only.
//! - [`opensearch`]: OpenSearch `_bulk` — logs only.
//! - [`mimir`]: Mimir remote-write push — metrics only.
//!
//! The signal-mapping functions (`otlp::map_traces`, `promwrite::map_remote_write`,
//! `loki::to_push_request`, `opensearch::to_bulk_ndjson`, …) are pure and
//! unit-tested; the handlers and sink workers are thin shells over them.

pub mod cli;
pub mod loki;
pub mod loki_ingest;
pub mod metrics;
pub mod mimir;
pub mod opensearch;
pub mod otlp;
pub mod otlp_common;
pub mod otlp_grpc;
pub mod otlp_logs;
pub mod otlp_metrics;
pub mod prometheus_proto;
pub mod promwrite;
pub mod pyroscope;
pub mod pyroscope_push;
pub mod sink;
pub mod sink_scry;
pub mod status;
pub mod wire;

use axum::{extract::DefaultBodyLimit, routing::post, Router};

pub use sink::AppState;
pub use wire::serve_wire;

/// Build the axum router wiring every foreign-protocol route to its handler.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/traces", post(otlp::handle))
        .route("/v1/logs", post(otlp_logs::handle))
        .route("/v1/metrics", post(otlp_metrics::handle))
        .route("/loki/api/v1/push", post(loki_ingest::handle))
        .route("/ingest", post(pyroscope::handle))
        .route("/push.v1.PusherService/Push", post(pyroscope_push::handle))
        // Prometheus remote-write. /api/v1/write is the Prometheus/VM default
        // receiver path; /api/v1/push is the Mimir/Cortex alias — accept both.
        .route("/api/v1/write", post(promwrite::handle))
        .route("/api/v1/push", post(promwrite::handle))
        .layer(DefaultBodyLimit::max(otlp_common::MAX_OTLP_BODY_BYTES))
        .with_state(state)
}
