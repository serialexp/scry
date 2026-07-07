//! scry-server — the ingest-server library.
//!
//! Owns the wire-protocol lifecycle (listener, handshake, per-batch
//! dispatch) and the per-signal generic [`Pipeline`] that turns
//! accepted batches into durable parquet blocks. Callers (the `scry ingest`
//! subcommand of the single `scry` binary) build one
//! [`Pipeline<B>`] per signal they want stored and hand them to
//! [`Server`] — that way the same pipelines can be shared with future
//! background uploaders or catalog-lookup code in a unified process.
//!
//! Example (sketch):
//!
//! ```ignore
//! // Each signal is internally sharded across connections (see
//! // `ShardedPipeline`); the shards share one global upload semaphore.
//! let dummy = ShardedPipeline::open(
//!     INGEST_SHARDS, wal_dir, store, catalog, writer_uuid,
//!     decode::dummy, upload_sem, /* upload_stats */ None,
//! ).await?;
//! let server = Server::new(
//!     ServerConfig {
//!         listen_addr: "127.0.0.1:4000".into(),
//!         writer_id: "scry-ingest-1".into(),
//!         writer_uuid,
//!     },
//!     Some(dummy),
//!     None,
//!     None,
//!     None,
//!     None,
//! );
//! server.serve_with_shutdown(tokio::signal::ctrl_c()).await?;
//! ```

pub mod decode;
pub mod live_merge;
pub mod live_ring;
mod pipeline;
pub mod query_service;
mod server;
pub mod stats;
pub mod tail;

pub use live_merge::{fetch_live_from_ingester, LiveDiscovery};
pub use live_ring::{LiveLogRecord, LiveRing, RetainingLogsAppender};
pub use pipeline::{DecodeFn, Pipeline, ShardedPipeline, INGEST_SHARDS};
pub use query_service::QueryService;
pub use scry_block::BlockBuilderConfig;
pub use server::{
    DummyPipeline, DummyShards, LogsPipeline, LogsShards, MetricsPipeline, MetricsShards,
    ProfilesPipeline, ProfilesShards, Server, ServerConfig, TracesPipeline, TracesShards,
};
pub use stats::{serve_stats, ServerMetrics, StatsProvider, UploadStats};
pub use tail::{SubId, SubscriptionRegistry, TailItem, TappingLogsAppender};
