//! scry-server — the ingest-server library.
//!
//! Owns the wire-protocol lifecycle (listener, handshake, per-batch
//! dispatch) and the per-signal generic [`Pipeline`] that turns
//! accepted batches into durable parquet blocks. Callers (`noise-sink`
//! today, the eventual single `scry` binary tomorrow) build one
//! [`Pipeline<B>`] per signal they want stored and hand them to
//! [`Server`] — that way the same pipelines can be shared with future
//! background uploaders or catalog-lookup code in a unified process.
//!
//! Example (sketch):
//!
//! ```ignore
//! use scry_block::DummyBlockBuilder;
//! let dummy_pipeline = Pipeline::<DummyBlockBuilder>::open(
//!     wal_dir, store, catalog, writer_uuid, decode::dummy,
//! ).await?;
//! let dummy_pipeline = std::sync::Arc::new(tokio::sync::Mutex::new(dummy_pipeline));
//! let server = Server::new(
//!     ServerConfig {
//!         listen_addr: "127.0.0.1:4000".into(),
//!         writer_id: "noise-sink-1".into(),
//!         writer_uuid,
//!     },
//!     Some(dummy_pipeline.clone()),
//!     None,
//!     None,
//! );
//! server.serve_with_shutdown(tokio::signal::ctrl_c()).await?;
//! ```

pub mod decode;
mod pipeline;
pub mod query_service;
mod server;
pub mod stats;

pub use pipeline::{DecodeFn, Pipeline};
pub use query_service::QueryService;
pub use server::{DummyPipeline, LogsPipeline, MetricsPipeline, Server, ServerConfig};
pub use stats::{serve_stats, ServerMetrics, StatsProvider, UploadStats};
