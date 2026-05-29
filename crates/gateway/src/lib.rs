//! `scry-gateway` library: foreign-protocol → binschema translation.
//!
//! The gateway terminates two foreign HTTP push protocols and forwards each
//! request to a scry ingest server as a binschema batch over one shared wire
//! [`Client`](scry_client::Client):
//!
//! - [`otlp`]: OTLP/HTTP protobuf trace push (`POST /v1/traces`).
//! - [`pyroscope`]: legacy Pyroscope profile ingest (`POST /ingest`).
//!
//! [`upstream`] owns the encode → zstd → frame → send path shared by both.
//! The mapping functions (`otlp::map_traces`, `pyroscope::parse_ingest_params`)
//! are pure and unit-tested; the HTTP handlers are thin shells over them.

pub mod otlp;
pub mod promwrite;
pub mod pyroscope;
pub mod upstream;

use axum::{
    routing::post,
    Router,
};

pub use upstream::AppState;

/// Build the axum router wiring both foreign-protocol routes to their handlers.
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
