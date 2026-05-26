//! scry ingest wire protocol.
//!
//! This crate exposes the generated binschema bindings for the agent ↔
//! server ingest protocol plus a small amount of hand-written glue:
//!
//! - [`framing`] — length-prefixed framing over an async stream.
//! - [`constants`] — the numeric constants from the schema (signals,
//!   ack statuses, reject/error codes), defined as `const` so call
//!   sites can match on them.
//! - [`fingerprint`] — xxh3-64 over canonically-sorted labels.
//!
//! The protocol design lives in `docs/ARCHITECTURE.md` (Ingest section)
//! and `proto/README.md`. The wire format itself is in
//! `proto/ingest.schema.json`; everything in [`generated`] is mechanically
//! derived from that file via `scripts/gen-proto.sh`.

#[allow(clippy::all)]
#[rustfmt::skip]
pub mod generated;

pub mod build;
pub mod constants;
pub mod fingerprint;
pub mod framing;

pub use generated::{
    Frame, FrameMsg,
    Hello, HelloInput, HelloOutput,
    HelloAck, HelloAckInput, HelloAckOutput,
    Batch, BatchInput, BatchOutput,
    BatchAck, BatchAckInput, BatchAckOutput,
    FlowControl, FlowControlInput, FlowControlOutput,
    Ping, PingInput, PingOutput,
    Pong, PongInput, PongOutput,
    Goodbye, GoodbyeInput, GoodbyeOutput,
    Error as ErrorMsg, ErrorInput, ErrorOutput,
    LabelPair,
    MetricsBatch, SeriesDictEntry, MetricSample,
    LogsBatch, LogStream, LogEntry,
    TracesBatch, ResourceEntry, ScopeEntry, Span, SpanEvent, SpanLink,
    ProfilesBatch, ProfileBlob,
    DummyBatch, DummyRecord,
};
