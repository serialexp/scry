//! Durable alert-domain types and pure evaluation logic.
//!
//! Network clients, scheduling tasks, coordination backends, and operator-facing
//! services live in `scry-alertd`. This crate owns the versioned contracts and
//! deterministic behavior those adapters share.

pub mod model;
pub mod records;
pub mod schedule;
mod serde_u64;
pub mod sqlite;
pub mod state;
pub mod store;
pub mod validation;

pub use model::*;
pub use records::*;
pub use schedule::*;
pub use sqlite::*;
pub use state::*;
pub use store::*;
pub use validation::*;
