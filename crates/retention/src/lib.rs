//! `scry-retention` — v0.8 per-signal TTL retention (single-instance).
//!
//! Retention reclaims storage by deleting blocks whose data is entirely
//! past a per-signal age limit. It is the **delete tail of compaction's
//! lifecycle with no merge** — it reuses [`scry_block::delete_block_objects`]
//! and [`scry_catalog::Catalog::delete_blocks`], and the same
//! object-before-row ordering (the catalog is derived state).
//!
//! Two safety properties shape the design:
//!
//! - **Opt-in, no implicit deletion.** A signal is only eligible if a TTL
//!   is configured for it ([`RetentionConfig::ttl_for`]); a signal with no
//!   TTL is never touched.
//! - **Whole-block criterion.** A block is reaped only when its *newest*
//!   record (`ts_max_unix_nano`) is past the TTL, so a block still holding
//!   in-window data is never dropped.
//!
//! This crate is the engine plus a thin CLI (`src/main.rs`). Like
//! `scry-compact` it is single-instance (one reaper, no distributed lease
//! — the multi-instance coordination is the documented follow-up).
//!
//! - [`policy`] — which blocks are expired ([`RetentionConfig`],
//!   [`plan_reaping`]).
//! - [`engine`] — the dry-run / apply lifecycle
//!   ([`retain_once`](engine::retain_once)).

pub mod engine;
pub mod policy;

pub use engine::{retain_once, RetentionReport};
pub use policy::{plan_reaping, RetentionConfig};
