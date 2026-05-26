//! Ergonomic constructors for wire messages.
//!
//! The generated `XxxOutput` structs include the `const` tag field as a
//! public member, so direct struct-literal construction requires the
//! call site to know each message's tag byte. The helpers in this
//! module hide that detail — call sites can build messages without
//! reaching into schema-private constants.
//!
//! These constructors are the only place outside `generated.rs` that
//! knows tag values. If the schema's tag assignments ever shift, this
//! module is the single point of update.

use crate::generated::{
    BatchAckOutput, BatchOutput, ErrorOutput, FlowControlOutput, Frame, FrameMsg, GoodbyeOutput,
    HelloAckOutput, HelloOutput, LabelPair, PingOutput, PongOutput,
};

// Tag values, matched to `Frame.msg` discriminator in
// `proto/ingest.schema.json`. Kept private — callers should not be
// reaching for these directly; use the constructors below.
const TAG_HELLO:        u8 = 0x01;
const TAG_HELLO_ACK:    u8 = 0x02;
const TAG_BATCH:        u8 = 0x10;
const TAG_BATCH_ACK:    u8 = 0x11;
const TAG_FLOW_CONTROL: u8 = 0x20;
const TAG_PING:         u8 = 0x30;
const TAG_PONG:         u8 = 0x31;
const TAG_GOODBYE:      u8 = 0x40;
const TAG_ERROR:        u8 = 0xF0;

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
        msg: FrameMsg::Hello(HelloOutput {
            tag: TAG_HELLO,
            protocol_version: a.protocol_version,
            agent_id: a.agent_id.to_vec(),
            agent_version: a.agent_version.into(),
            hostname: a.hostname.into(),
            signals: a.signals,
            capabilities: a.capabilities,
            resource_attrs: a.resource_attrs,
        }),
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
        msg: FrameMsg::HelloAck(HelloAckOutput {
            tag: TAG_HELLO_ACK,
            protocol_version: a.protocol_version,
            writer_id: a.writer_id.into(),
            session_id: a.session_id,
            capabilities: a.capabilities,
            suggested_batch_bytes: a.suggested_batch_bytes,
            max_batch_bytes: a.max_batch_bytes,
            max_inflight_batches: a.max_inflight_batches,
        }),
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
        msg: FrameMsg::Batch(BatchOutput {
            tag: TAG_BATCH,
            session_id: a.session_id,
            batch_id: a.batch_id,
            signal: a.signal,
            ts_min_unix_nano: a.ts_min_unix_nano,
            ts_max_unix_nano: a.ts_max_unix_nano,
            record_count: a.record_count,
            compression: a.compression,
            uncompressed_size: a.uncompressed_size,
            payload: a.payload,
        }),
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
        msg: FrameMsg::BatchAck(BatchAckOutput {
            tag: TAG_BATCH_ACK,
            session_id,
            batch_id,
            status,
            retry_after_ms,
            reason_code,
            message: message.into(),
        }),
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
        msg: FrameMsg::FlowControl(FlowControlOutput {
            tag: TAG_FLOW_CONTROL,
            session_id,
            signal,
            max_bytes_per_sec,
            max_batches_inflight,
            valid_for_ms,
        }),
    }
}

pub fn ping(nonce: u64) -> Frame {
    Frame { msg: FrameMsg::Ping(PingOutput { tag: TAG_PING, nonce }) }
}

pub fn pong(nonce: u64) -> Frame {
    Frame { msg: FrameMsg::Pong(PongOutput { tag: TAG_PONG, nonce }) }
}

pub fn goodbye(reason_code: u16, message: &str) -> Frame {
    Frame {
        msg: FrameMsg::Goodbye(GoodbyeOutput {
            tag: TAG_GOODBYE,
            reason_code,
            message: message.into(),
        }),
    }
}

pub fn error(code: u16, message: &str) -> Frame {
    Frame {
        msg: FrameMsg::Error(ErrorOutput {
            tag: TAG_ERROR,
            code,
            message: message.into(),
        }),
    }
}
