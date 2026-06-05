//! `scry-gateway` library: a fan-out hub for observability data.
//!
//! The gateway accepts records over several inbound protocols and **fans each
//! one out to every configured downstream sink** (all in → all out, no routing
//! config — for anything more selective, run a second gateway). Inbound:
//!
//! - [`wire`]: the native binschema ingest listener — `scry-agent` and any other
//!   native producer point here.
//! - [`otlp`]: OTLP/HTTP protobuf trace push (`POST /v1/traces`).
//! - [`pyroscope`]: legacy Pyroscope profile ingest (`POST /ingest`).
//! - [`promwrite`]: Prometheus remote-write (`POST /api/v1/write`, `/api/v1/push`).
//!
//! Each inbound path decodes its request into a typed `*Batch` and hands it to
//! [`AppState`] (in [`sink`]), which offers it best-effort to every
//! [`sink::SinkHandle`] whose signal mask accepts it. Sinks:
//!
//! - [`sink_scry`]: the scry ingest server (native wire) — accepts all signals.
//! - [`loki`]: Grafana Loki push — logs only.
//! - [`opensearch`]: OpenSearch `_bulk` — logs only.
//!
//! The signal-mapping functions (`otlp::map_traces`, `promwrite::map_remote_write`,
//! `loki::to_push_request`, `opensearch::to_bulk_ndjson`, …) are pure and
//! unit-tested; the handlers and sink workers are thin shells over them.

pub mod loki;
pub mod opensearch;
pub mod otlp;
pub mod promwrite;
pub mod pyroscope;
pub mod sink;
pub mod sink_scry;
pub mod wire;

use axum::{routing::post, Router};

pub use sink::AppState;
pub use wire::serve_wire;

/// Build the axum router wiring every foreign-protocol route to its handler.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/traces", post(otlp::handle))
        .route("/ingest", post(pyroscope::handle))
        // Prometheus remote-write. /api/v1/write is the Prometheus/VM default
        // receiver path; /api/v1/push is the Mimir/Cortex alias — accept both.
        .route("/api/v1/write", post(promwrite::handle))
        .route("/api/v1/push", post(promwrite::handle))
        .with_state(state)
}
