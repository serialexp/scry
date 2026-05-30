//! `scry-compact` — v0.8 size-tiered compaction (single-instance).
//!
//! Compaction merges the many small blocks a busy writer fans out (one
//! per WAL rotation per shard) into fewer, larger ones, so queries open
//! fewer objects and load less per-block metadata. Blocks live at a
//! `level`; a `(signal, date, level)` partition with at least `fanout`
//! blocks is merged into one block at `level + 1` (size-tiered, per
//! `ARCHITECTURE.md § Compaction`).
//!
//! This crate is the engine plus a thin CLI (`src/main.rs`). The engine
//! is single-instance: one compactor, no distributed lease (the
//! per-partition object-store lease for N instances is the documented
//! multi-instance follow-up). It is forward-compatible with that design
//! — immutability + content-addressing already make a stale-lease double
//! merge harmless.
//!
//! - [`policy`] — which blocks to merge ([`CompactConfig`],
//!   [`plan_merges`]).
//! - [`merge`] — read K inputs, stream-sort via DataFusion, rebuild
//!   sidecars, upload ([`merge_blocks`](merge::merge_blocks)).
//! - [`engine`] — the full per-merge lifecycle
//!   ([`compact_once`](engine::compact_once)).

pub mod engine;
pub mod merge;
pub mod policy;

pub use engine::{compact_once, CompactReport};
pub use policy::{plan_merges, CompactConfig, PlannedMerge};
