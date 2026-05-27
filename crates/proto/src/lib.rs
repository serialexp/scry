//! scry wire protocols (ingest + query).
//!
//! This crate exposes the generated binschema bindings for both of
//! scry's TCP wire protocols plus a small amount of hand-written glue:
//!
//! - [`generated`] — agent ↔ ingest-server protocol, from
//!   `proto/ingest.schema.json`.
//! - [`generated_query`] — client ↔ query-daemon protocol, from
//!   `proto/query.schema.json`.
//! - [`framing`] — length-prefixed framing over an async stream;
//!   generic over the framed type via the [`framing::Framed`] trait,
//!   so the same helpers serve both protocols.
//! - [`constants`] — numeric constants from both schemas (signals,
//!   ack statuses, reject / error codes, query error codes), defined
//!   as `const` so call sites can match on them.
//! - [`fingerprint`] — xxh3-64 over canonically-sorted labels (ingest).
//!
//! The protocol designs live in `docs/ARCHITECTURE.md`. The wire
//! formats themselves are in `proto/{ingest,query}.schema.json`;
//! everything in [`generated`] / [`generated_query`] is mechanically
//! derived from those files via `scripts/gen-proto.sh`.

#[allow(clippy::all)]
#[rustfmt::skip]
pub mod generated;

#[allow(clippy::all)]
#[rustfmt::skip]
pub mod generated_query;

pub mod build;
pub mod constants;
pub mod fingerprint;
pub mod framing;
pub mod streaming;

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

pub use generated_query::{
    QueryFrame, QueryFrameMsg,
    QueryRequest, QueryRequestInput, QueryRequestOutput,
    Matcher,
    SchemaMsg, SchemaMsgInput, SchemaMsgOutput,
    BatchMsg, BatchMsgInput, BatchMsgOutput,
    EndOfStream, EndOfStreamInput, EndOfStreamOutput,
    StreamError, StreamErrorInput, StreamErrorOutput,
};
