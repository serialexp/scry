//! scry wire protocols (ingest + query).
//!
//! This crate exposes the generated binschema bindings for both of
//! scry's TCP wire protocols plus a small amount of hand-written glue:
//!
//! - [`generated`] — agent ↔ ingest-server protocol, from
//!   `proto/ingest.schema.json`.
//! - [`generated_query`] — client ↔ query-daemon protocol, from
//!   `proto/query.schema.json`.
//! - [`generated_query_worker`] — private queryd ↔ queryd control protocol,
//!   from `proto/query-worker.schema.json`.
//! - [`framing`] — length-prefixed framing over an async stream;
//!   generic over the framed type via the [`framing::Framed`] trait,
//!   so the same helpers serve both protocols.
//! - [`constants`] — numeric constants from both schemas (signals,
//!   ack statuses, reject / error codes, query error codes), defined
//!   as `const` so call sites can match on them.
//! - [`fingerprint`] — xxh3-64 over canonically-sorted labels (ingest).
//!
//! The protocol designs live in `docs/ARCHITECTURE.md`. The wire formats
//! themselves are the three schemas under `proto/`; all `generated*` modules
//! are mechanically derived from them via `scripts/gen-proto.sh`.

#[allow(clippy::all)]
#[rustfmt::skip]
pub mod generated;

#[allow(clippy::all)]
#[rustfmt::skip]
pub mod generated_query;

#[allow(clippy::all)]
#[rustfmt::skip]
pub mod generated_query_worker;

pub mod build;
pub mod constants;
pub mod fingerprint;
pub mod framing;
pub mod logs_v2_encoder;
pub mod metrics_v2;
pub mod payload;
pub mod streaming;
pub mod streaming_logs_v2;
pub mod streaming_v2;

pub use generated::{
    AgentStatus, AgentStatusInput, AgentStatusOutput, Batch, BatchAck, BatchAckInput,
    BatchAckOutput, BatchInput, BatchOutput, DummyBatch, DummyRecord, Error as ErrorMsg,
    ErrorInput, ErrorOutput, FlowControl, FlowControlInput, FlowControlOutput, Frame, FrameMsg,
    Goodbye, GoodbyeInput, GoodbyeOutput, Hello, HelloAck, HelloAckInput, HelloAckOutput,
    HelloInput, HelloOutput, LabelPair, LogEntry, LogStream, LogsBatch, LogsBatchV2,
    LogsBatchV2Input, LogsBatchV2Output, MetricSample, MetricsBatch, OpaqueLogRecordV2, Ping,
    PingInput, PingOutput, Pong, PongInput, PongOutput, ProfileBlob, ProfilesBatch, ResourceEntry,
    ScopeEntry, SeriesDictEntry, Span, SpanEvent, SpanLink, TailMetricPointV2,
    TailMetricPointV2Input, TailMetricPointV2Output, TracesBatch,
};

pub use streaming_logs_v2::{
    decode_logs_batch_v2_into, validate_record as validate_logs_v2_record, AnyValue, AnyValueRef,
    ArrayIter as AnyValueArrayIter, ArrayRef as AnyValueArrayRef, CanonicalLogRecord,
    DecodeError as LogsV2DecodeError, DecodeLimits as LogsV2DecodeLimits, LogsV2Appender,
    MapIter as AnyValueMapIter, MapRef as AnyValueMapRef,
};

pub use generated_query::{
    BatchMsg, BatchMsgInput, BatchMsgOutput, EndOfStream, EndOfStreamInput, EndOfStreamOutput,
    FleetStatusRequest, FleetStatusRequestInput, FleetStatusRequestOutput, FleetStatusResponse,
    FleetStatusResponseInput, FleetStatusResponseOutput, LabelNamesRequest, LabelNamesRequestInput,
    LabelNamesRequestOutput, LabelNamesResponse, LabelNamesResponseInput, LabelNamesResponseOutput,
    LabelValuesRequest, LabelValuesRequestInput, LabelValuesRequestOutput, LabelValuesResponse,
    LabelValuesResponseInput, LabelValuesResponseOutput, LiveNodeTiming, Matcher, QueryFrame,
    QueryFrameMsg, QueryRequest, QueryRequestInput, QueryRequestOutput, QueryStats,
    QueryStatsInput, QueryStatsOutput, ResponseSuperseded, ResponseSupersededInput,
    ResponseSupersededOutput, SchemaMsg, SchemaMsgInput, SchemaMsgOutput, StreamError,
    StreamErrorInput, StreamErrorOutput,
};

pub use logs_v2_encoder::{
    encode_log_record_v2_from_source_into, encode_log_record_v2_into,
    encode_logs_batch_v2_from_sources_into, encode_logs_batch_v2_into,
    AnyValueAdapter as LogsV2AnyValueAdapter, AnyValueInput as LogsV2AnyValueInput,
    BorrowedAnyValue as LogsV2BorrowedAnyValue, EncodeError as LogsV2EncodeError,
    EncodeScratch as LogsV2EncodeScratch, KeyValueInput as LogsV2KeyValueInput, LogRecordInput,
    ScopeInput as LogsV2ScopeInput, SourceLogRecordInput as LogsV2SourceLogRecordInput,
    SourceScopeInput as LogsV2SourceScopeInput,
};

pub use generated_query_worker::{
    QueryWorkerFrame, QueryWorkerFrameMsg, WorkerAuthenticated, WorkerAuthenticatedInput,
    WorkerAuthenticatedOutput, WorkerBidDecline, WorkerBidDeclineInput, WorkerBidDeclineOutput,
    WorkerBidRequest, WorkerBidRequestInput, WorkerBidRequestOutput, WorkerBidResponse,
    WorkerBidResponseInput, WorkerBidResponseOutput, WorkerBlockLocality, WorkerBlockOffer,
    WorkerCancel, WorkerCancelAck, WorkerCancelAckInput, WorkerCancelAckOutput, WorkerCancelInput,
    WorkerCancelOutput, WorkerClientHello, WorkerClientHelloInput, WorkerClientHelloOutput,
    WorkerError, WorkerErrorInput, WorkerErrorOutput, WorkerRelease, WorkerReleaseAck,
    WorkerReleaseAckInput, WorkerReleaseAckOutput, WorkerReleaseInput, WorkerReleaseOutput,
    WorkerServerHello, WorkerServerHelloInput, WorkerServerHelloOutput,
};
