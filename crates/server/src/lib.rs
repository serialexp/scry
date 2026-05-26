//! scry-server — the ingest-server library.
//!
//! Owns the wire-protocol lifecycle (listener, handshake, per-batch
//! dispatch) and the process-scoped [`DummyPipeline`] that turns
//! accepted DummyBatches into durable parquet blocks. Callers
//! (`noise-sink` today, the eventual single `scry` binary tomorrow)
//! build a [`DummyPipeline`] separately and hand it to [`Server`] —
//! that way the same pipeline can be shared with future background
//! uploaders or catalog-lookup code in a unified process.
//!
//! Example (sketch):
//!
//! ```ignore
//! let pipeline = DummyPipeline::open(wal_dir, store, catalog, writer_uuid).await?;
//! let pipeline = std::sync::Arc::new(tokio::sync::Mutex::new(pipeline));
//! let server = Server::new(
//!     ServerConfig {
//!         listen_addr: "127.0.0.1:4000".into(),
//!         writer_id: "noise-sink-1".into(),
//!         writer_uuid,
//!     },
//!     Some(pipeline.clone()),
//! );
//! server.serve_with_shutdown(tokio::signal::ctrl_c()).await?;
//! ```

mod pipeline;
mod server;

pub use pipeline::DummyPipeline;
pub use server::{Server, ServerConfig};
