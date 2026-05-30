//! `scry-compact` — v0.8 size-tiered compaction (single-instance).
//!
//! Compaction merges the many small blocks a busy writer fans out (one
//! per WAL rotation per shard) into fewer, larger ones, so queries open
//! fewer objects and load less per-block metadata. Blocks live at a
//! `level`; a `(signal, date, level)` partition with at least `fanout`
//! blocks is merged into one block at `level + 1` (size-tiered, per
//! `ARCHITECTURE.md § Compaction`).
//!
//! This crate is the engine plus a thin CLI (`src/main.rs`). The standalone
//! [`compact_once`](engine::compact_once) entry point is single-instance: one
//! compactor, no lease. The v0.9 multi-instance daemon drives
//! [`compact_partition`](engine::compact_partition) instead, passing a
//! [`Fence`](scry_block::Fence) (the Valkey lease guard) so exactly one
//! instance commits a given partition's merge, and a
//! [`BlockEventSink`](scry_block::BlockEventSink) so peers converge. The
//! merge's `meta.json` PUT is the fenced commit point: a lost lease aborts
//! before it, leaving inputs intact (see [`merge_blocks`](merge::merge_blocks)).
//!
//! - [`policy`] — which blocks to merge ([`CompactConfig`],
//!   [`plan_merges`]).
//! - [`merge`] — read K inputs, stream-sort via DataFusion, rebuild
//!   sidecars, upload ([`merge_blocks`](merge::merge_blocks)).
//! - [`engine`] — the full per-merge lifecycle
//!   ([`compact_once`](engine::compact_once) /
//!   [`compact_partition`](engine::compact_partition)).

pub mod engine;
pub mod merge;
pub mod policy;

pub use engine::{compact_once, compact_partition, CompactReport, PartitionOutcome};
pub use policy::{plan_merges, CompactConfig, PlannedMerge};
