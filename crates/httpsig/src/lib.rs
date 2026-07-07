//! `scry-httpsig`: shared HTTP-client construction and AWS SigV4 request signing.
//!
//! Two small pieces that scry's OpenSearch-facing roles need in common and must
//! not duplicate:
//!
//! - [`build_http_client`] ([`client`]) — a `reqwest::Client` that optionally
//!   **adds** a custom CA certificate on top of the built-in webpki roots, for
//!   endpoints fronted by a private/internal CA.
//! - [`SigV4Signer`] + [`build_sigv4_signer`] ([`sigv4`]) — AWS SigV4 signing for
//!   Amazon OpenSearch Service (`es`) / Serverless (`aoss`), which reject
//!   unsigned requests.
//!
//! The **gateway's** OpenSearch *sink* and the **replay-opensearch** *reader*
//! both talk to (possibly AWS-managed, possibly private-CA) OpenSearch clusters,
//! so both depend on this leaf crate rather than one depending on the other.

pub mod client;
pub mod sigv4;

pub use client::build_http_client;
pub use sigv4::{build_sigv4_signer, SigV4Signer};
