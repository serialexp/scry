//! `scry-cluster` — v0.9 multi-instance orchestration.
//!
//! When N identical `scry` instances share one bucket, two things need
//! coordinating: **catalog convergence** (every instance learns about blocks
//! its peers wrote/merged/reaped) and **maintenance exclusion** (exactly one
//! instance does destructive work — compaction, retention — on a given
//! partition at a time). This crate holds the Valkey-*agnostic* logic for
//! both, so it is fully unit-testable in-process; the Valkey transport lives
//! in `scry-valkey` and is injected via two traits.
//!
//! ## Convergence (three tiers, all converging on the bucket as truth)
//!
//! 1. **pub/sub apply** ([`apply_event`]) — low-latency hint: peers broadcast
//!    [`BlockEvent`](scry_block::BlockEvent)s and each applies them
//!    idempotently to its catalog.
//! 2. **incremental poll** ([`poll_once`]) — backstop for dropped events:
//!    list only what's newer than each `(signal, writer, date)` cursor.
//! 3. **full walk** ([`full_walk`]) — exhaustive periodic re-derivation that
//!    also discovers brand-new prefixes.
//!
//! Valkey is only ever a hint; with it absent the system stays correct
//! (convergence falls back to polling, and — no lease — maintenance pauses).
//!
//! ## Maintenance ([`run_compaction_pass`] / [`run_retention_pass`])
//!
//! Generic over [`LeaseProvider`]: production injects the Valkey lease, tests
//! inject [`LocalLeaseProvider`]. The acquired guard's
//! [`Fence`](scry_block::Fence) is threaded into the engines so a lost lease
//! aborts before any irreversible step.

pub mod consume;
pub mod lease;
pub mod maintain;
pub mod poll;

pub use consume::{apply_event, ApplyOutcome};
pub use lease::{LeaseGuard, LeaseProvider, LocalGuard, LocalLeaseProvider};
pub use maintain::{run_compaction_pass, run_retention_pass, RETENTION_LEASE_KEY};
pub use poll::{full_walk, poll_once, PollReport};
