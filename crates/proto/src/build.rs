//! Ergonomic constructors for wire messages.
//!
//! Each helper takes the user-supplied fields, builds an `XxxInput`,
//! and converts it into the generated `XxxOutput` via `From`. The
//! `From` impl populates `const` fields (e.g. the message tag byte)
//! from the schema, so we don't hand-mirror tag values here.
//!
//! ## Computed-field caveat
//!
//! The schema has **no `computed` fields** today (`length_of`, `crc32_of`,
//! …). If we add any, `XxxInput → XxxOutput` will populate them with
//! whatever default the binschema codegen picks, which **may produce
//! incorrect on-the-wire values**. The encode path uses the runtime's
//! computed-field machinery during serialisation, so the bytes that go
//! out are still correct — but the in-memory `Output` struct returned
//! by `.into()` will have stale/default values for those fields, which
//! is misleading.
//!
//! If we ever add a `computed` field to a type that callers build via
//! this module, audit the corresponding constructor here and either
//! populate the computed slot explicitly (post-`.into()`) or stop using
//! `.into()` for that type. See:
//! <https://github.com/anthropics/binschema> Rust codegen notes.

use crate::generated::{
    AgentStatusInput, BatchAckInput, BatchInput, ErrorInput, FlowControlInput, Frame, FrameMsg,
    GoodbyeInput, HelloAckInput, HelloInput, LabelPair, LiveBatchInput, LiveQueryInput, LiveRecord,
    MatcherSpec, PingInput, PongInput, SubscribeInput, TailRecordInput, TailSampleInput,
};

pub struct HelloArgs<'a> {
    pub protocol_version: u16,
    pub agent_id: [u8; 16],
    pub agent_version: &'a str,
    pub hostname: &'a str,
    pub signals: u8,
    pub capabilities: u32,
    pub resource_attrs: Vec<LabelPair>,
}

pub fn hello(a: HelloArgs<'_>) -> Frame {
    Frame {
        msg: FrameMsg::Hello(
            HelloInput {
                protocol_version: a.protocol_version,
                agent_id: a.agent_id.to_vec(),
                agent_version: a.agent_version.into(),
                hostname: a.hostname.into(),
                signals: a.signals,
                capabilities: a.capabilities,
                resource_attrs: a.resource_attrs,
            }
            .into(),
        ),
    }
}

pub struct HelloAckArgs<'a> {
    pub protocol_version: u16,
    pub writer_id: &'a str,
    pub session_id: u64,
    pub capabilities: u32,
    pub suggested_batch_bytes: u32,
    pub max_batch_bytes: u32,
    pub max_inflight_batches: u16,
}

pub fn hello_ack(a: HelloAckArgs<'_>) -> Frame {
    Frame {
        msg: FrameMsg::HelloAck(
            HelloAckInput {
                protocol_version: a.protocol_version,
                writer_id: a.writer_id.into(),
                session_id: a.session_id,
                capabilities: a.capabilities,
                suggested_batch_bytes: a.suggested_batch_bytes,
                max_batch_bytes: a.max_batch_bytes,
                max_inflight_batches: a.max_inflight_batches,
            }
            .into(),
        ),
    }
}

pub struct AgentStatusArgs<'a> {
    pub session_id: u64,
    pub sequence: u64,
    pub snapshot_json: &'a str,
}

pub fn agent_status(a: AgentStatusArgs<'_>) -> Frame {
    Frame {
        msg: FrameMsg::AgentStatus(
            AgentStatusInput {
                session_id: a.session_id,
                sequence: a.sequence,
                snapshot_json: a.snapshot_json.into(),
            }
            .into(),
        ),
    }
}

pub struct BatchArgs {
    pub session_id: u64,
    pub batch_id: u64,
    pub signal: u8,
    pub ts_min_unix_nano: u64,
    pub ts_max_unix_nano: u64,
    pub record_count: u32,
    pub compression: u8,
    pub uncompressed_size: u32,
    pub payload: Vec<u8>,
}

pub fn batch(a: BatchArgs) -> Frame {
    Frame {
        msg: FrameMsg::Batch(
            BatchInput {
                session_id: a.session_id,
                batch_id: a.batch_id,
                signal: a.signal,
                ts_min_unix_nano: a.ts_min_unix_nano,
                ts_max_unix_nano: a.ts_max_unix_nano,
                record_count: a.record_count,
                compression: a.compression,
                uncompressed_size: a.uncompressed_size,
                payload: a.payload,
            }
            .into(),
        ),
    }
}

pub fn batch_ack(
    session_id: u64,
    batch_id: u64,
    status: u8,
    retry_after_ms: u32,
    reason_code: u16,
    message: &str,
) -> Frame {
    Frame {
        msg: FrameMsg::BatchAck(
            BatchAckInput {
                session_id,
                batch_id,
                status,
                retry_after_ms,
                reason_code,
                message: message.into(),
            }
            .into(),
        ),
    }
}

pub fn flow_control(
    session_id: u64,
    signal: u8,
    max_bytes_per_sec: u32,
    max_batches_inflight: u16,
    valid_for_ms: u32,
) -> Frame {
    Frame {
        msg: FrameMsg::FlowControl(
            FlowControlInput {
                session_id,
                signal,
                max_bytes_per_sec,
                max_batches_inflight,
                valid_for_ms,
            }
            .into(),
        ),
    }
}

pub fn ping(nonce: u64) -> Frame {
    Frame {
        msg: FrameMsg::Ping(PingInput { nonce }.into()),
    }
}

pub fn pong(nonce: u64) -> Frame {
    Frame {
        msg: FrameMsg::Pong(PongInput { nonce }.into()),
    }
}

pub fn goodbye(reason_code: u16, message: &str) -> Frame {
    Frame {
        msg: FrameMsg::Goodbye(
            GoodbyeInput {
                reason_code,
                message: message.into(),
            }
            .into(),
        ),
    }
}

pub fn error(code: u16, message: &str) -> Frame {
    Frame {
        msg: FrameMsg::Error(
            ErrorInput {
                code,
                message: message.into(),
            }
            .into(),
        ),
    }
}

/// A live-tail subscription request: match `signal` records against the
/// Prometheus-style matcher specs (`key=value`, `key=~re`, …). An empty
/// `matchers` slice subscribes to every record of that signal.
pub fn subscribe(signal: u8, matchers: &[String]) -> Frame {
    Frame {
        msg: FrameMsg::Subscribe(
            SubscribeInput {
                signal,
                matchers: matchers
                    .iter()
                    .map(|m| MatcherSpec { spec: m.clone() })
                    .collect(),
            }
            .into(),
        ),
    }
}

pub struct TailRecordArgs {
    pub signal: u8,
    pub ts_unix_nano: u64,
    pub severity: u8,
    pub labels: Vec<LabelPair>,
    pub body: String,
    pub attributes: Vec<LabelPair>,
}

/// A single live record streamed back to a subscriber. Best-effort: no
/// ordering, no completeness, and no relationship to stored blocks.
pub fn tail_record(a: TailRecordArgs) -> Frame {
    Frame {
        msg: FrameMsg::TailRecord(
            TailRecordInput {
                signal: a.signal,
                ts_unix_nano: a.ts_unix_nano,
                severity: a.severity,
                labels: a.labels,
                body: a.body,
                attributes: a.attributes,
            }
            .into(),
        ),
    }
}

pub struct TailSampleArgs {
    pub signal: u8,
    pub ts_unix_nano: u64,
    pub metric_type: u8,
    pub series_fingerprint: u64,
    pub value: f64,
    pub labels: Vec<LabelPair>,
}

/// A single live metric sample streamed back to a subscriber — the
/// [`tail_record`] counterpart for the metrics signal (D-065). Same
/// best-effort contract: no ordering, no completeness, no relationship to
/// stored blocks.
pub fn tail_metric_point_v2(
    signal: u8,
    series_fingerprint: u64,
    labels: Vec<crate::LabelPair>,
    descriptor: crate::generated::MetricDescriptorV2,
    point: crate::generated::MetricPointV2,
) -> Frame {
    Frame {
        msg: FrameMsg::TailMetricPointV2(
            crate::generated::TailMetricPointV2Input {
                signal,
                series_fingerprint,
                labels,
                descriptor,
                point,
            }
            .into(),
        ),
    }
}

pub fn tail_sample(a: TailSampleArgs) -> Frame {
    Frame {
        msg: FrameMsg::TailSample(
            TailSampleInput {
                signal: a.signal,
                ts_unix_nano: a.ts_unix_nano,
                metric_type: a.metric_type,
                series_fingerprint: a.series_fingerprint,
                value: a.value,
                labels: a.labels,
            }
            .into(),
        ),
    }
}

pub struct LiveQueryArgs {
    pub signal: u8,
    pub matchers: Vec<String>,
    /// 0 = no lower bound.
    pub ts_min_unix_nano: u64,
    /// 0 = no upper bound.
    pub ts_max_unix_nano: u64,
    /// Empty = no substring filter.
    pub body_contains: String,
}

/// A merged-query live snapshot request (D-054): the query daemon asks a
/// discovered ingester for its retained recent records matching these
/// predicates. The ingester replies with exactly one [`live_batch`].
pub fn live_query(a: LiveQueryArgs) -> Frame {
    Frame {
        msg: FrameMsg::LiveQuery(
            LiveQueryInput {
                signal: a.signal,
                matchers: a
                    .matchers
                    .iter()
                    .map(|m| MatcherSpec { spec: m.clone() })
                    .collect(),
                ts_min_unix_nano: a.ts_min_unix_nano,
                ts_max_unix_nano: a.ts_max_unix_nano,
                body_contains: a.body_contains,
            }
            .into(),
        ),
    }
}

/// An ingester's reply to a [`live_query`]: its retained recent records, each
/// tagged with the WAL `(shard, seg)` the merged query dedups against.
/// `writer_uuid` identifies this ingester's WAL instance.
pub fn live_batch(writer_uuid: [u8; 16], records: Vec<LiveRecord>) -> Frame {
    Frame {
        msg: FrameMsg::LiveBatch(
            LiveBatchInput {
                writer_uuid: writer_uuid.to_vec(),
                records,
            }
            .into(),
        ),
    }
}
