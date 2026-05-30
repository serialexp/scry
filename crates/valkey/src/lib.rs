//! `scry-valkey` — the Valkey transport for scry's multi-instance
//! coordination (v0.9).
//!
//! This crate is the only one that talks to Valkey. It implements the two
//! transport-agnostic traits the rest of the system is written against:
//!
//! - [`ValkeyLeaseProvider`] : `scry_cluster::LeaseProvider` — exact mutual
//!   exclusion via `SET NX PX` + Lua compare-and-set renew/release (see
//!   [`lease`]). This replaces the object-store `If-None-Match` lease of
//!   D-013, which Garage cannot support.
//! - [`ValkeySink`] : `scry_block::BlockEventSink` — fans block lifecycle
//!   events to peers over pub/sub, non-blocking on the hot path (see [`sink`]).
//!
//! plus [`ValkeyClient`] (a connected handle + health watch) and the [`pubsub`]
//! subscribe side the convergence consumer reads from.
//!
//! Everything degrades safely: with `SCRY_VALKEY_URL` unset
//! ([`ValkeyClient::from_env`] returns `None`) the daemons run a correct
//! single-instance path — no pub/sub, and no lease means maintenance pauses
//! rather than risking an uncoordinated destructive race.

pub mod client;
pub mod lease;
pub mod pubsub;
pub mod sink;

pub use client::{ValkeyClient, VALKEY_URL_ENV};
pub use lease::{ValkeyLease, ValkeyLeaseProvider};
pub use pubsub::{channel_for, parse_envelope, publish_envelope, subscribe_blocks};
pub use sink::ValkeySink;
