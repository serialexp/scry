#![allow(non_camel_case_types)]
#![allow(dead_code)]
#![allow(unreachable_code)]

#[allow(unused_imports)]
use binschema_runtime::{BitStreamEncoder, BitStreamDecoder, Endianness, BitOrder, Result, BinSchemaError, EncodeContext, FieldValue};
#[allow(unused_imports)]
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum FrameMsg {
    Hello(HelloOutput),
    HelloAck(HelloAckOutput),
    Batch(BatchOutput),
    BatchAck(BatchAckOutput),
    FlowControl(FlowControlOutput),
    AgentStatus(AgentStatusOutput),
    Ping(PingOutput),
    Pong(PongOutput),
    Goodbye(GoodbyeOutput),
    Subscribe(SubscribeOutput),
    TailRecord(TailRecordOutput),
    LiveQuery(LiveQueryOutput),
    LiveBatch(LiveBatchOutput),
    TailSample(TailSampleOutput),
    TailMetricPointV2(TailMetricPointV2Output),
    Error(ErrorOutput),
}

impl FrameMsg {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        match self {
            FrameMsg::Hello(v) => {
                encoder.write_uint8(1);
                encoder.write_uint16(v.protocol_version, Endianness::BigEndian);
                for item in &v.agent_id {
                    encoder.write_uint8(*item);
                }
                encoder.write_uint8(v.agent_version.chars().count() as u8);
                let string_bytes: Vec<u8> = v.agent_version.chars().map(|c| c as u8).collect();
                for &b in string_bytes.iter() {
                    encoder.write_uint8(b);
                }
                encoder.write_uint8(v.hostname.len() as u8);
                let string_bytes: &[u8] = v.hostname.as_bytes();
                for &b in string_bytes.iter() {
                    encoder.write_uint8(b);
                }
                encoder.write_uint8(v.signals);
                encoder.write_uint32(v.capabilities, Endianness::BigEndian);
                encoder.write_uint16(v.resource_attrs.len() as u16, Endianness::BigEndian);
                for item in &v.resource_attrs {
                    item.encode_into(encoder)?;
                }
            }
            FrameMsg::HelloAck(v) => {
                encoder.write_uint8(2);
                encoder.write_uint16(v.protocol_version, Endianness::BigEndian);
                encoder.write_uint8(v.writer_id.chars().count() as u8);
                let string_bytes: Vec<u8> = v.writer_id.chars().map(|c| c as u8).collect();
                for &b in string_bytes.iter() {
                    encoder.write_uint8(b);
                }
                encoder.write_uint64(v.session_id, Endianness::BigEndian);
                encoder.write_uint32(v.capabilities, Endianness::BigEndian);
                encoder.write_uint32(v.suggested_batch_bytes, Endianness::BigEndian);
                encoder.write_uint32(v.max_batch_bytes, Endianness::BigEndian);
                encoder.write_uint16(v.max_inflight_batches, Endianness::BigEndian);
            }
            FrameMsg::Batch(v) => {
                encoder.write_uint8(16);
                encoder.write_uint64(v.session_id, Endianness::BigEndian);
                encoder.write_uint64(v.batch_id, Endianness::BigEndian);
                encoder.write_uint8(v.signal);
                encoder.write_uint64(v.ts_min_unix_nano, Endianness::BigEndian);
                encoder.write_uint64(v.ts_max_unix_nano, Endianness::BigEndian);
                encoder.write_uint32(v.record_count, Endianness::BigEndian);
                encoder.write_uint8(v.compression);
                encoder.write_uint32(v.uncompressed_size, Endianness::BigEndian);
                encoder.write_uint32(v.payload.len() as u32, Endianness::BigEndian);
                for item in &v.payload {
                    encoder.write_uint8(*item);
                }
            }
            FrameMsg::BatchAck(v) => {
                encoder.write_uint8(17);
                encoder.write_uint64(v.session_id, Endianness::BigEndian);
                encoder.write_uint64(v.batch_id, Endianness::BigEndian);
                encoder.write_uint8(v.status);
                encoder.write_uint32(v.retry_after_ms, Endianness::BigEndian);
                encoder.write_uint16(v.reason_code, Endianness::BigEndian);
                encoder.write_uint16(v.message.len() as u16, Endianness::BigEndian);
                let string_bytes: &[u8] = v.message.as_bytes();
                for &b in string_bytes.iter() {
                    encoder.write_uint8(b);
                }
            }
            FrameMsg::FlowControl(v) => {
                encoder.write_uint8(32);
                encoder.write_uint64(v.session_id, Endianness::BigEndian);
                encoder.write_uint8(v.signal);
                encoder.write_uint32(v.max_bytes_per_sec, Endianness::BigEndian);
                encoder.write_uint16(v.max_batches_inflight, Endianness::BigEndian);
                encoder.write_uint32(v.valid_for_ms, Endianness::BigEndian);
            }
            FrameMsg::AgentStatus(v) => {
                encoder.write_uint8(33);
                encoder.write_uint64(v.session_id, Endianness::BigEndian);
                encoder.write_uint64(v.sequence, Endianness::BigEndian);
                encoder.write_uint32(v.snapshot_json.len() as u32, Endianness::BigEndian);
                let string_bytes: &[u8] = v.snapshot_json.as_bytes();
                for &b in string_bytes.iter() {
                    encoder.write_uint8(b);
                }
            }
            FrameMsg::Ping(v) => {
                encoder.write_uint8(48);
                encoder.write_uint64(v.nonce, Endianness::BigEndian);
            }
            FrameMsg::Pong(v) => {
                encoder.write_uint8(49);
                encoder.write_uint64(v.nonce, Endianness::BigEndian);
            }
            FrameMsg::Goodbye(v) => {
                encoder.write_uint8(64);
                encoder.write_uint16(v.reason_code, Endianness::BigEndian);
                encoder.write_uint16(v.message.len() as u16, Endianness::BigEndian);
                let string_bytes: &[u8] = v.message.as_bytes();
                for &b in string_bytes.iter() {
                    encoder.write_uint8(b);
                }
            }
            FrameMsg::Subscribe(v) => {
                encoder.write_uint8(80);
                encoder.write_uint8(v.signal);
                encoder.write_uint16(v.matchers.len() as u16, Endianness::BigEndian);
                for item in &v.matchers {
                    item.encode_into(encoder)?;
                }
            }
            FrameMsg::TailRecord(v) => {
                encoder.write_uint8(81);
                encoder.write_uint8(v.signal);
                encoder.write_uint64(v.ts_unix_nano, Endianness::BigEndian);
                encoder.write_uint8(v.severity);
                encoder.write_uint16(v.labels.len() as u16, Endianness::BigEndian);
                for item in &v.labels {
                    item.encode_into(encoder)?;
                }
                encoder.write_uint32(v.body.len() as u32, Endianness::BigEndian);
                let string_bytes: &[u8] = v.body.as_bytes();
                for &b in string_bytes.iter() {
                    encoder.write_uint8(b);
                }
                encoder.write_uint16(v.attributes.len() as u16, Endianness::BigEndian);
                for item in &v.attributes {
                    item.encode_into(encoder)?;
                }
            }
            FrameMsg::LiveQuery(v) => {
                encoder.write_uint8(82);
                encoder.write_uint8(v.signal);
                encoder.write_uint16(v.matchers.len() as u16, Endianness::BigEndian);
                for item in &v.matchers {
                    item.encode_into(encoder)?;
                }
                encoder.write_uint64(v.ts_min_unix_nano, Endianness::BigEndian);
                encoder.write_uint64(v.ts_max_unix_nano, Endianness::BigEndian);
                encoder.write_uint32(v.body_contains.len() as u32, Endianness::BigEndian);
                let string_bytes: &[u8] = v.body_contains.as_bytes();
                for &b in string_bytes.iter() {
                    encoder.write_uint8(b);
                }
            }
            FrameMsg::LiveBatch(v) => {
                encoder.write_uint8(83);
                for item in &v.writer_uuid {
                    encoder.write_uint8(*item);
                }
                encoder.write_uint32(v.records.len() as u32, Endianness::BigEndian);
                for item in &v.records {
                    item.encode_into(encoder)?;
                }
            }
            FrameMsg::TailSample(v) => {
                encoder.write_uint8(84);
                encoder.write_uint8(v.signal);
                encoder.write_uint64(v.ts_unix_nano, Endianness::BigEndian);
                encoder.write_uint8(v.metric_type);
                encoder.write_uint64(v.series_fingerprint, Endianness::BigEndian);
                encoder.write_float64(v.value, Endianness::BigEndian);
                encoder.write_uint16(v.labels.len() as u16, Endianness::BigEndian);
                for item in &v.labels {
                    item.encode_into(encoder)?;
                }
            }
            FrameMsg::TailMetricPointV2(v) => {
                encoder.write_uint8(85);
                encoder.write_uint8(v.signal);
                encoder.write_uint64(v.series_fingerprint, Endianness::BigEndian);
                encoder.write_uint16(v.labels.len() as u16, Endianness::BigEndian);
                for item in &v.labels {
                    item.encode_into(encoder)?;
                }
                v.descriptor.encode_into(encoder)?;
                v.point.encode_into(encoder)?;
            }
            FrameMsg::Error(v) => {
                encoder.write_uint8(240);
                encoder.write_uint16(v.code, Endianness::BigEndian);
                encoder.write_uint16(v.message.len() as u16, Endianness::BigEndian);
                let string_bytes: &[u8] = v.message.as_bytes();
                for &b in string_bytes.iter() {
                    encoder.write_uint8(b);
                }
            }
        }
        Ok(())
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            FrameMsg::Hello(_) => "Hello",
            FrameMsg::HelloAck(_) => "HelloAck",
            FrameMsg::Batch(_) => "Batch",
            FrameMsg::BatchAck(_) => "BatchAck",
            FrameMsg::FlowControl(_) => "FlowControl",
            FrameMsg::AgentStatus(_) => "AgentStatus",
            FrameMsg::Ping(_) => "Ping",
            FrameMsg::Pong(_) => "Pong",
            FrameMsg::Goodbye(_) => "Goodbye",
            FrameMsg::Subscribe(_) => "Subscribe",
            FrameMsg::TailRecord(_) => "TailRecord",
            FrameMsg::LiveQuery(_) => "LiveQuery",
            FrameMsg::LiveBatch(_) => "LiveBatch",
            FrameMsg::TailSample(_) => "TailSample",
            FrameMsg::TailMetricPointV2(_) => "TailMetricPointV2",
            FrameMsg::Error(_) => "Error",
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        // Union type - try each variant in order until one succeeds
        let start_pos = decoder.position();
        if let Ok(v) = HelloOutput::decode_with_decoder(decoder) {
            return Ok(FrameMsg::Hello(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = HelloAckOutput::decode_with_decoder(decoder) {
            return Ok(FrameMsg::HelloAck(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = BatchOutput::decode_with_decoder(decoder) {
            return Ok(FrameMsg::Batch(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = BatchAckOutput::decode_with_decoder(decoder) {
            return Ok(FrameMsg::BatchAck(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = FlowControlOutput::decode_with_decoder(decoder) {
            return Ok(FrameMsg::FlowControl(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = AgentStatusOutput::decode_with_decoder(decoder) {
            return Ok(FrameMsg::AgentStatus(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = PingOutput::decode_with_decoder(decoder) {
            return Ok(FrameMsg::Ping(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = PongOutput::decode_with_decoder(decoder) {
            return Ok(FrameMsg::Pong(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = GoodbyeOutput::decode_with_decoder(decoder) {
            return Ok(FrameMsg::Goodbye(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = SubscribeOutput::decode_with_decoder(decoder) {
            return Ok(FrameMsg::Subscribe(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = TailRecordOutput::decode_with_decoder(decoder) {
            return Ok(FrameMsg::TailRecord(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = LiveQueryOutput::decode_with_decoder(decoder) {
            return Ok(FrameMsg::LiveQuery(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = LiveBatchOutput::decode_with_decoder(decoder) {
            return Ok(FrameMsg::LiveBatch(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = TailSampleOutput::decode_with_decoder(decoder) {
            return Ok(FrameMsg::TailSample(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = TailMetricPointV2Output::decode_with_decoder(decoder) {
            return Ok(FrameMsg::TailMetricPointV2(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = ErrorOutput::decode_with_decoder(decoder) {
            return Ok(FrameMsg::Error(v));
        }
        Err(binschema_runtime::BinSchemaError::InvalidVariant("no variant matched the input bytes".to_string()))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum MetricPointV2Value {
    ScalarPointV2(ScalarPointV2Output),
    HistogramPointV2(HistogramPointV2Output),
    ExponentialHistogramPointV2(ExponentialHistogramPointV2Output),
    SummaryPointV2(SummaryPointV2Output),
}

impl MetricPointV2Value {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        match self {
            MetricPointV2Value::ScalarPointV2(v) => {
                encoder.write_uint8(1);
                encoder.write_uint32(v.descriptor_id, Endianness::BigEndian);
                encoder.write_uint64(v.start_unix_nano, Endianness::BigEndian);
                encoder.write_uint64(v.ts_unix_nano, Endianness::BigEndian);
                encoder.write_uint32(v.flags, Endianness::BigEndian);
                encoder.write_uint16(v.attributes.len() as u16, Endianness::BigEndian);
                for item in &v.attributes {
                    item.encode_into(encoder)?;
                }
                encoder.write_uint16(v.exemplars.len() as u16, Endianness::BigEndian);
                for item in &v.exemplars {
                    item.encode_into(encoder)?;
                }
                v.number.encode_into(encoder)?;
            }
            MetricPointV2Value::HistogramPointV2(v) => {
                encoder.write_uint8(2);
                encoder.write_uint32(v.descriptor_id, Endianness::BigEndian);
                encoder.write_uint64(v.start_unix_nano, Endianness::BigEndian);
                encoder.write_uint64(v.ts_unix_nano, Endianness::BigEndian);
                encoder.write_uint32(v.flags, Endianness::BigEndian);
                encoder.write_uint16(v.attributes.len() as u16, Endianness::BigEndian);
                for item in &v.attributes {
                    item.encode_into(encoder)?;
                }
                encoder.write_uint16(v.exemplars.len() as u16, Endianness::BigEndian);
                for item in &v.exemplars {
                    item.encode_into(encoder)?;
                }
                encoder.write_uint64(v.count, Endianness::BigEndian);
                encoder.write_uint8(v.has_sum);
                encoder.write_float64(v.sum, Endianness::BigEndian);
                encoder.write_uint8(v.has_min);
                encoder.write_float64(v.min, Endianness::BigEndian);
                encoder.write_uint8(v.has_max);
                encoder.write_float64(v.max, Endianness::BigEndian);
                encoder.write_uint32(v.explicit_bounds.len() as u32, Endianness::BigEndian);
                for item in &v.explicit_bounds {
                    encoder.write_float64(*item, Endianness::BigEndian);
                }
                encoder.write_uint32(v.bucket_counts.len() as u32, Endianness::BigEndian);
                for item in &v.bucket_counts {
                    encoder.write_uint64(*item, Endianness::BigEndian);
                }
            }
            MetricPointV2Value::ExponentialHistogramPointV2(v) => {
                encoder.write_uint8(3);
                encoder.write_uint32(v.descriptor_id, Endianness::BigEndian);
                encoder.write_uint64(v.start_unix_nano, Endianness::BigEndian);
                encoder.write_uint64(v.ts_unix_nano, Endianness::BigEndian);
                encoder.write_uint32(v.flags, Endianness::BigEndian);
                encoder.write_uint16(v.attributes.len() as u16, Endianness::BigEndian);
                for item in &v.attributes {
                    item.encode_into(encoder)?;
                }
                encoder.write_uint16(v.exemplars.len() as u16, Endianness::BigEndian);
                for item in &v.exemplars {
                    item.encode_into(encoder)?;
                }
                v.count.encode_into(encoder)?;
                encoder.write_uint8(v.has_sum);
                encoder.write_float64(v.sum, Endianness::BigEndian);
                encoder.write_uint8(v.has_min);
                encoder.write_float64(v.min, Endianness::BigEndian);
                encoder.write_uint8(v.has_max);
                encoder.write_float64(v.max, Endianness::BigEndian);
                encoder.write_int32(v.scale, Endianness::BigEndian);
                encoder.write_float64(v.zero_threshold, Endianness::BigEndian);
                v.zero_count.encode_into(encoder)?;
                v.positive.encode_into(encoder)?;
                v.negative.encode_into(encoder)?;
                encoder.write_uint32(v.custom_bounds.len() as u32, Endianness::BigEndian);
                for item in &v.custom_bounds {
                    encoder.write_float64(*item, Endianness::BigEndian);
                }
                encoder.write_uint8(v.reset_hint);
            }
            MetricPointV2Value::SummaryPointV2(v) => {
                encoder.write_uint8(4);
                encoder.write_uint32(v.descriptor_id, Endianness::BigEndian);
                encoder.write_uint64(v.start_unix_nano, Endianness::BigEndian);
                encoder.write_uint64(v.ts_unix_nano, Endianness::BigEndian);
                encoder.write_uint32(v.flags, Endianness::BigEndian);
                encoder.write_uint16(v.attributes.len() as u16, Endianness::BigEndian);
                for item in &v.attributes {
                    item.encode_into(encoder)?;
                }
                encoder.write_uint16(v.exemplars.len() as u16, Endianness::BigEndian);
                for item in &v.exemplars {
                    item.encode_into(encoder)?;
                }
                encoder.write_uint64(v.count, Endianness::BigEndian);
                encoder.write_float64(v.sum, Endianness::BigEndian);
                encoder.write_uint16(v.quantiles.len() as u16, Endianness::BigEndian);
                for item in &v.quantiles {
                    item.encode_into(encoder)?;
                }
            }
        }
        Ok(())
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            MetricPointV2Value::ScalarPointV2(_) => "ScalarPointV2",
            MetricPointV2Value::HistogramPointV2(_) => "HistogramPointV2",
            MetricPointV2Value::ExponentialHistogramPointV2(_) => "ExponentialHistogramPointV2",
            MetricPointV2Value::SummaryPointV2(_) => "SummaryPointV2",
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        // Union type - try each variant in order until one succeeds
        let start_pos = decoder.position();
        if let Ok(v) = ScalarPointV2Output::decode_with_decoder(decoder) {
            return Ok(MetricPointV2Value::ScalarPointV2(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = HistogramPointV2Output::decode_with_decoder(decoder) {
            return Ok(MetricPointV2Value::HistogramPointV2(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = ExponentialHistogramPointV2Output::decode_with_decoder(decoder) {
            return Ok(MetricPointV2Value::ExponentialHistogramPointV2(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = SummaryPointV2Output::decode_with_decoder(decoder) {
            return Ok(MetricPointV2Value::SummaryPointV2(v));
        }
        Err(binschema_runtime::BinSchemaError::InvalidVariant("no variant matched the input bytes".to_string()))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum MetricNumberV2Value {
    IntegerValueV2(IntegerValueV2Output),
    DoubleValueV2(DoubleValueV2Output),
}

impl MetricNumberV2Value {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        match self {
            MetricNumberV2Value::IntegerValueV2(v) => {
                encoder.write_uint8(1);
                encoder.write_int64(v.value, Endianness::BigEndian);
            }
            MetricNumberV2Value::DoubleValueV2(v) => {
                encoder.write_uint8(2);
                encoder.write_float64(v.value, Endianness::BigEndian);
            }
        }
        Ok(())
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            MetricNumberV2Value::IntegerValueV2(_) => "IntegerValueV2",
            MetricNumberV2Value::DoubleValueV2(_) => "DoubleValueV2",
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        // Union type - try each variant in order until one succeeds
        let start_pos = decoder.position();
        if let Ok(v) = IntegerValueV2Output::decode_with_decoder(decoder) {
            return Ok(MetricNumberV2Value::IntegerValueV2(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = DoubleValueV2Output::decode_with_decoder(decoder) {
            return Ok(MetricNumberV2Value::DoubleValueV2(v));
        }
        Err(binschema_runtime::BinSchemaError::InvalidVariant("no variant matched the input bytes".to_string()))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum MetricCountV2Value {
    IntegerCountV2(IntegerCountV2Output),
    FloatCountV2(FloatCountV2Output),
}

impl MetricCountV2Value {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        match self {
            MetricCountV2Value::IntegerCountV2(v) => {
                encoder.write_uint8(1);
                encoder.write_uint64(v.value, Endianness::BigEndian);
            }
            MetricCountV2Value::FloatCountV2(v) => {
                encoder.write_uint8(2);
                encoder.write_float64(v.value, Endianness::BigEndian);
            }
        }
        Ok(())
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            MetricCountV2Value::IntegerCountV2(_) => "IntegerCountV2",
            MetricCountV2Value::FloatCountV2(_) => "FloatCountV2",
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        // Union type - try each variant in order until one succeeds
        let start_pos = decoder.position();
        if let Ok(v) = IntegerCountV2Output::decode_with_decoder(decoder) {
            return Ok(MetricCountV2Value::IntegerCountV2(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = FloatCountV2Output::decode_with_decoder(decoder) {
            return Ok(MetricCountV2Value::FloatCountV2(v));
        }
        Err(binschema_runtime::BinSchemaError::InvalidVariant("no variant matched the input bytes".to_string()))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Frame {
    pub msg: FrameMsg,
}

impl Frame {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        self.msg.encode_into(encoder)?;
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let msg = FrameMsg::decode_with_decoder(decoder)?;
        Ok(Self {
            msg,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct HelloInput {
    pub protocol_version: u16,
    pub agent_id: Vec<u8>,
    pub agent_version: std::string::String,
    pub hostname: std::string::String,
    pub signals: u8,
    pub capabilities: u32,
    pub resource_attrs: Vec<LabelPair>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HelloOutput {
    pub tag: u8,
    pub protocol_version: u16,
    pub agent_id: Vec<u8>,
    pub agent_version: std::string::String,
    pub hostname: std::string::String,
    pub signals: u8,
    pub capabilities: u32,
    pub resource_attrs: Vec<LabelPair>,
}

pub type Hello = HelloOutput;

impl HelloInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(1);
        encoder.write_u16_be(self.protocol_version);
        for item in &self.agent_id {
            encoder.write_byte(*item);
        }
        encoder.write_byte(self.agent_version.chars().count() as u8);
        let string_bytes: Vec<u8> = self.agent_version.chars().map(|c| c as u8).collect();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        encoder.write_byte(self.hostname.len() as u8);
        let string_bytes: &[u8] = self.hostname.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        encoder.write_byte(self.signals);
        encoder.write_u32_be(self.capabilities);
        encoder.write_u16_be(self.resource_attrs.len() as u16);
        for item in &self.resource_attrs {
            item.encode_into(encoder)?;
        }
        Ok(())
    }

}

impl HelloOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 1u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 1, got {}", tag)));
        }
        let protocol_version = decoder.read_u16_be()?;
        let mut agent_id = Vec::with_capacity(16);
        for _ in 0..16 {
            let item = decoder.read_byte()?;
            agent_id.push(item);
        }
        let length = decoder.read_byte()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let agent_version: std::string::String = bytes.iter().map(|&b| b as char).collect();
        let length = decoder.read_byte()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let hostname = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        let signals = decoder.read_byte()?;
        let capabilities = decoder.read_u32_be()?;
        let length = decoder.read_u16_be()? as usize;
        let mut resource_attrs = Vec::with_capacity(length);
        for _ in 0..length {
            let item = LabelPair::decode_with_decoder(decoder)?;
            resource_attrs.push(item);
        }
        Ok(Self {
            tag,
            protocol_version,
            agent_id,
            agent_version,
            hostname,
            signals,
            capabilities,
            resource_attrs,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        HelloInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        HelloInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<HelloOutput> for HelloInput {
    fn from(o: HelloOutput) -> Self {
        Self {
            protocol_version: o.protocol_version,
            agent_id: o.agent_id,
            agent_version: o.agent_version,
            hostname: o.hostname,
            signals: o.signals,
            capabilities: o.capabilities,
            resource_attrs: o.resource_attrs,
        }
    }
}

impl From<HelloInput> for HelloOutput {
    fn from(i: HelloInput) -> Self {
        Self {
            tag: 1u8,
            protocol_version: i.protocol_version,
            agent_id: i.agent_id,
            agent_version: i.agent_version,
            hostname: i.hostname,
            signals: i.signals,
            capabilities: i.capabilities,
            resource_attrs: i.resource_attrs,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct HelloAckInput {
    pub protocol_version: u16,
    pub writer_id: std::string::String,
    pub session_id: u64,
    pub capabilities: u32,
    pub suggested_batch_bytes: u32,
    pub max_batch_bytes: u32,
    pub max_inflight_batches: u16,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HelloAckOutput {
    pub tag: u8,
    pub protocol_version: u16,
    pub writer_id: std::string::String,
    pub session_id: u64,
    pub capabilities: u32,
    pub suggested_batch_bytes: u32,
    pub max_batch_bytes: u32,
    pub max_inflight_batches: u16,
}

pub type HelloAck = HelloAckOutput;

impl HelloAckInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(2);
        encoder.write_u16_be(self.protocol_version);
        encoder.write_byte(self.writer_id.chars().count() as u8);
        let string_bytes: Vec<u8> = self.writer_id.chars().map(|c| c as u8).collect();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        encoder.write_u64_be(self.session_id);
        encoder.write_u32_be(self.capabilities);
        encoder.write_u32_be(self.suggested_batch_bytes);
        encoder.write_u32_be(self.max_batch_bytes);
        encoder.write_u16_be(self.max_inflight_batches);
        Ok(())
    }

}

impl HelloAckOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 2u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 2, got {}", tag)));
        }
        let protocol_version = decoder.read_u16_be()?;
        let length = decoder.read_byte()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let writer_id: std::string::String = bytes.iter().map(|&b| b as char).collect();
        let session_id = decoder.read_u64_be()?;
        let capabilities = decoder.read_u32_be()?;
        let suggested_batch_bytes = decoder.read_u32_be()?;
        let max_batch_bytes = decoder.read_u32_be()?;
        let max_inflight_batches = decoder.read_u16_be()?;
        Ok(Self {
            tag,
            protocol_version,
            writer_id,
            session_id,
            capabilities,
            suggested_batch_bytes,
            max_batch_bytes,
            max_inflight_batches,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        HelloAckInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        HelloAckInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<HelloAckOutput> for HelloAckInput {
    fn from(o: HelloAckOutput) -> Self {
        Self {
            protocol_version: o.protocol_version,
            writer_id: o.writer_id,
            session_id: o.session_id,
            capabilities: o.capabilities,
            suggested_batch_bytes: o.suggested_batch_bytes,
            max_batch_bytes: o.max_batch_bytes,
            max_inflight_batches: o.max_inflight_batches,
        }
    }
}

impl From<HelloAckInput> for HelloAckOutput {
    fn from(i: HelloAckInput) -> Self {
        Self {
            tag: 2u8,
            protocol_version: i.protocol_version,
            writer_id: i.writer_id,
            session_id: i.session_id,
            capabilities: i.capabilities,
            suggested_batch_bytes: i.suggested_batch_bytes,
            max_batch_bytes: i.max_batch_bytes,
            max_inflight_batches: i.max_inflight_batches,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BatchInput {
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

#[derive(Debug, Clone, PartialEq)]
pub struct BatchOutput {
    pub tag: u8,
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

pub type Batch = BatchOutput;

impl BatchInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(16);
        encoder.write_u64_be(self.session_id);
        encoder.write_u64_be(self.batch_id);
        encoder.write_byte(self.signal);
        encoder.write_u64_be(self.ts_min_unix_nano);
        encoder.write_u64_be(self.ts_max_unix_nano);
        encoder.write_u32_be(self.record_count);
        encoder.write_byte(self.compression);
        encoder.write_u32_be(self.uncompressed_size);
        encoder.write_u32_be(self.payload.len() as u32);
        for item in &self.payload {
            encoder.write_byte(*item);
        }
        Ok(())
    }

}

impl BatchOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 16u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 16, got {}", tag)));
        }
        let session_id = decoder.read_u64_be()?;
        let batch_id = decoder.read_u64_be()?;
        let signal = decoder.read_byte()?;
        let ts_min_unix_nano = decoder.read_u64_be()?;
        let ts_max_unix_nano = decoder.read_u64_be()?;
        let record_count = decoder.read_u32_be()?;
        let compression = decoder.read_byte()?;
        let uncompressed_size = decoder.read_u32_be()?;
        let length = decoder.read_u32_be()? as usize;
        let mut payload = Vec::with_capacity(length);
        for _ in 0..length {
            let item = decoder.read_byte()?;
            payload.push(item);
        }
        Ok(Self {
            tag,
            session_id,
            batch_id,
            signal,
            ts_min_unix_nano,
            ts_max_unix_nano,
            record_count,
            compression,
            uncompressed_size,
            payload,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        BatchInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        BatchInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<BatchOutput> for BatchInput {
    fn from(o: BatchOutput) -> Self {
        Self {
            session_id: o.session_id,
            batch_id: o.batch_id,
            signal: o.signal,
            ts_min_unix_nano: o.ts_min_unix_nano,
            ts_max_unix_nano: o.ts_max_unix_nano,
            record_count: o.record_count,
            compression: o.compression,
            uncompressed_size: o.uncompressed_size,
            payload: o.payload,
        }
    }
}

impl From<BatchInput> for BatchOutput {
    fn from(i: BatchInput) -> Self {
        Self {
            tag: 16u8,
            session_id: i.session_id,
            batch_id: i.batch_id,
            signal: i.signal,
            ts_min_unix_nano: i.ts_min_unix_nano,
            ts_max_unix_nano: i.ts_max_unix_nano,
            record_count: i.record_count,
            compression: i.compression,
            uncompressed_size: i.uncompressed_size,
            payload: i.payload,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BatchAckInput {
    pub session_id: u64,
    pub batch_id: u64,
    pub status: u8,
    pub retry_after_ms: u32,
    pub reason_code: u16,
    pub message: std::string::String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BatchAckOutput {
    pub tag: u8,
    pub session_id: u64,
    pub batch_id: u64,
    pub status: u8,
    pub retry_after_ms: u32,
    pub reason_code: u16,
    pub message: std::string::String,
}

pub type BatchAck = BatchAckOutput;

impl BatchAckInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(17);
        encoder.write_u64_be(self.session_id);
        encoder.write_u64_be(self.batch_id);
        encoder.write_byte(self.status);
        encoder.write_u32_be(self.retry_after_ms);
        encoder.write_u16_be(self.reason_code);
        encoder.write_u16_be(self.message.len() as u16);
        let string_bytes: &[u8] = self.message.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        Ok(())
    }

}

impl BatchAckOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 17u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 17, got {}", tag)));
        }
        let session_id = decoder.read_u64_be()?;
        let batch_id = decoder.read_u64_be()?;
        let status = decoder.read_byte()?;
        let retry_after_ms = decoder.read_u32_be()?;
        let reason_code = decoder.read_u16_be()?;
        let length = decoder.read_u16_be()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let message = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        Ok(Self {
            tag,
            session_id,
            batch_id,
            status,
            retry_after_ms,
            reason_code,
            message,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        BatchAckInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        BatchAckInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<BatchAckOutput> for BatchAckInput {
    fn from(o: BatchAckOutput) -> Self {
        Self {
            session_id: o.session_id,
            batch_id: o.batch_id,
            status: o.status,
            retry_after_ms: o.retry_after_ms,
            reason_code: o.reason_code,
            message: o.message,
        }
    }
}

impl From<BatchAckInput> for BatchAckOutput {
    fn from(i: BatchAckInput) -> Self {
        Self {
            tag: 17u8,
            session_id: i.session_id,
            batch_id: i.batch_id,
            status: i.status,
            retry_after_ms: i.retry_after_ms,
            reason_code: i.reason_code,
            message: i.message,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FlowControlInput {
    pub session_id: u64,
    pub signal: u8,
    pub max_bytes_per_sec: u32,
    pub max_batches_inflight: u16,
    pub valid_for_ms: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FlowControlOutput {
    pub tag: u8,
    pub session_id: u64,
    pub signal: u8,
    pub max_bytes_per_sec: u32,
    pub max_batches_inflight: u16,
    pub valid_for_ms: u32,
}

pub type FlowControl = FlowControlOutput;

impl FlowControlInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(32);
        encoder.write_u64_be(self.session_id);
        encoder.write_byte(self.signal);
        encoder.write_u32_be(self.max_bytes_per_sec);
        encoder.write_u16_be(self.max_batches_inflight);
        encoder.write_u32_be(self.valid_for_ms);
        Ok(())
    }

}

impl FlowControlOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 32u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 32, got {}", tag)));
        }
        let session_id = decoder.read_u64_be()?;
        let signal = decoder.read_byte()?;
        let max_bytes_per_sec = decoder.read_u32_be()?;
        let max_batches_inflight = decoder.read_u16_be()?;
        let valid_for_ms = decoder.read_u32_be()?;
        Ok(Self {
            tag,
            session_id,
            signal,
            max_bytes_per_sec,
            max_batches_inflight,
            valid_for_ms,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        FlowControlInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        FlowControlInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<FlowControlOutput> for FlowControlInput {
    fn from(o: FlowControlOutput) -> Self {
        Self {
            session_id: o.session_id,
            signal: o.signal,
            max_bytes_per_sec: o.max_bytes_per_sec,
            max_batches_inflight: o.max_batches_inflight,
            valid_for_ms: o.valid_for_ms,
        }
    }
}

impl From<FlowControlInput> for FlowControlOutput {
    fn from(i: FlowControlInput) -> Self {
        Self {
            tag: 32u8,
            session_id: i.session_id,
            signal: i.signal,
            max_bytes_per_sec: i.max_bytes_per_sec,
            max_batches_inflight: i.max_batches_inflight,
            valid_for_ms: i.valid_for_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AgentStatusInput {
    pub session_id: u64,
    pub sequence: u64,
    pub snapshot_json: std::string::String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AgentStatusOutput {
    pub tag: u8,
    pub session_id: u64,
    pub sequence: u64,
    pub snapshot_json: std::string::String,
}

pub type AgentStatus = AgentStatusOutput;

impl AgentStatusInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(33);
        encoder.write_u64_be(self.session_id);
        encoder.write_u64_be(self.sequence);
        encoder.write_u32_be(self.snapshot_json.len() as u32);
        let string_bytes: &[u8] = self.snapshot_json.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        Ok(())
    }

}

impl AgentStatusOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 33u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 33, got {}", tag)));
        }
        let session_id = decoder.read_u64_be()?;
        let sequence = decoder.read_u64_be()?;
        let length = decoder.read_u32_be()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let snapshot_json = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        Ok(Self {
            tag,
            session_id,
            sequence,
            snapshot_json,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        AgentStatusInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        AgentStatusInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<AgentStatusOutput> for AgentStatusInput {
    fn from(o: AgentStatusOutput) -> Self {
        Self {
            session_id: o.session_id,
            sequence: o.sequence,
            snapshot_json: o.snapshot_json,
        }
    }
}

impl From<AgentStatusInput> for AgentStatusOutput {
    fn from(i: AgentStatusInput) -> Self {
        Self {
            tag: 33u8,
            session_id: i.session_id,
            sequence: i.sequence,
            snapshot_json: i.snapshot_json,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PingInput {
    pub nonce: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PingOutput {
    pub tag: u8,
    pub nonce: u64,
}

pub type Ping = PingOutput;

impl PingInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(48);
        encoder.write_u64_be(self.nonce);
        Ok(())
    }

}

impl PingOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 48u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 48, got {}", tag)));
        }
        let nonce = decoder.read_u64_be()?;
        Ok(Self {
            tag,
            nonce,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        PingInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        PingInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<PingOutput> for PingInput {
    fn from(o: PingOutput) -> Self {
        Self {
            nonce: o.nonce,
        }
    }
}

impl From<PingInput> for PingOutput {
    fn from(i: PingInput) -> Self {
        Self {
            tag: 48u8,
            nonce: i.nonce,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PongInput {
    pub nonce: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PongOutput {
    pub tag: u8,
    pub nonce: u64,
}

pub type Pong = PongOutput;

impl PongInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(49);
        encoder.write_u64_be(self.nonce);
        Ok(())
    }

}

impl PongOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 49u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 49, got {}", tag)));
        }
        let nonce = decoder.read_u64_be()?;
        Ok(Self {
            tag,
            nonce,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        PongInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        PongInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<PongOutput> for PongInput {
    fn from(o: PongOutput) -> Self {
        Self {
            nonce: o.nonce,
        }
    }
}

impl From<PongInput> for PongOutput {
    fn from(i: PongInput) -> Self {
        Self {
            tag: 49u8,
            nonce: i.nonce,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct GoodbyeInput {
    pub reason_code: u16,
    pub message: std::string::String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GoodbyeOutput {
    pub tag: u8,
    pub reason_code: u16,
    pub message: std::string::String,
}

pub type Goodbye = GoodbyeOutput;

impl GoodbyeInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(64);
        encoder.write_u16_be(self.reason_code);
        encoder.write_u16_be(self.message.len() as u16);
        let string_bytes: &[u8] = self.message.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        Ok(())
    }

}

impl GoodbyeOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 64u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 64, got {}", tag)));
        }
        let reason_code = decoder.read_u16_be()?;
        let length = decoder.read_u16_be()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let message = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        Ok(Self {
            tag,
            reason_code,
            message,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        GoodbyeInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        GoodbyeInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<GoodbyeOutput> for GoodbyeInput {
    fn from(o: GoodbyeOutput) -> Self {
        Self {
            reason_code: o.reason_code,
            message: o.message,
        }
    }
}

impl From<GoodbyeInput> for GoodbyeOutput {
    fn from(i: GoodbyeInput) -> Self {
        Self {
            tag: 64u8,
            reason_code: i.reason_code,
            message: i.message,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ErrorInput {
    pub code: u16,
    pub message: std::string::String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ErrorOutput {
    pub tag: u8,
    pub code: u16,
    pub message: std::string::String,
}

pub type Error = ErrorOutput;

impl ErrorInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(240);
        encoder.write_u16_be(self.code);
        encoder.write_u16_be(self.message.len() as u16);
        let string_bytes: &[u8] = self.message.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        Ok(())
    }

}

impl ErrorOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 240u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 240, got {}", tag)));
        }
        let code = decoder.read_u16_be()?;
        let length = decoder.read_u16_be()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let message = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        Ok(Self {
            tag,
            code,
            message,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        ErrorInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        ErrorInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<ErrorOutput> for ErrorInput {
    fn from(o: ErrorOutput) -> Self {
        Self {
            code: o.code,
            message: o.message,
        }
    }
}

impl From<ErrorInput> for ErrorOutput {
    fn from(i: ErrorInput) -> Self {
        Self {
            tag: 240u8,
            code: i.code,
            message: i.message,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubscribeInput {
    pub signal: u8,
    pub matchers: Vec<MatcherSpec>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubscribeOutput {
    pub tag: u8,
    pub signal: u8,
    pub matchers: Vec<MatcherSpec>,
}

pub type Subscribe = SubscribeOutput;

impl SubscribeInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(80);
        encoder.write_byte(self.signal);
        encoder.write_u16_be(self.matchers.len() as u16);
        for item in &self.matchers {
            item.encode_into(encoder)?;
        }
        Ok(())
    }

}

impl SubscribeOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 80u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 80, got {}", tag)));
        }
        let signal = decoder.read_byte()?;
        let length = decoder.read_u16_be()? as usize;
        let mut matchers = Vec::with_capacity(length);
        for _ in 0..length {
            let item = MatcherSpec::decode_with_decoder(decoder)?;
            matchers.push(item);
        }
        Ok(Self {
            tag,
            signal,
            matchers,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        SubscribeInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        SubscribeInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<SubscribeOutput> for SubscribeInput {
    fn from(o: SubscribeOutput) -> Self {
        Self {
            signal: o.signal,
            matchers: o.matchers,
        }
    }
}

impl From<SubscribeInput> for SubscribeOutput {
    fn from(i: SubscribeInput) -> Self {
        Self {
            tag: 80u8,
            signal: i.signal,
            matchers: i.matchers,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatcherSpec {
    pub spec: std::string::String,
}

impl MatcherSpec {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_u16_be(self.spec.len() as u16);
        let string_bytes: &[u8] = self.spec.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let length = decoder.read_u16_be()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let spec = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        Ok(Self {
            spec,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TailRecordInput {
    pub signal: u8,
    pub ts_unix_nano: u64,
    pub severity: u8,
    pub labels: Vec<LabelPair>,
    pub body: std::string::String,
    pub attributes: Vec<LabelPair>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TailRecordOutput {
    pub tag: u8,
    pub signal: u8,
    pub ts_unix_nano: u64,
    pub severity: u8,
    pub labels: Vec<LabelPair>,
    pub body: std::string::String,
    pub attributes: Vec<LabelPair>,
}

pub type TailRecord = TailRecordOutput;

impl TailRecordInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(81);
        encoder.write_byte(self.signal);
        encoder.write_u64_be(self.ts_unix_nano);
        encoder.write_byte(self.severity);
        encoder.write_u16_be(self.labels.len() as u16);
        for item in &self.labels {
            item.encode_into(encoder)?;
        }
        encoder.write_u32_be(self.body.len() as u32);
        let string_bytes: &[u8] = self.body.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        encoder.write_u16_be(self.attributes.len() as u16);
        for item in &self.attributes {
            item.encode_into(encoder)?;
        }
        Ok(())
    }

}

impl TailRecordOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 81u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 81, got {}", tag)));
        }
        let signal = decoder.read_byte()?;
        let ts_unix_nano = decoder.read_u64_be()?;
        let severity = decoder.read_byte()?;
        let length = decoder.read_u16_be()? as usize;
        let mut labels = Vec::with_capacity(length);
        for _ in 0..length {
            let item = LabelPair::decode_with_decoder(decoder)?;
            labels.push(item);
        }
        let length = decoder.read_u32_be()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let body = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        let length = decoder.read_u16_be()? as usize;
        let mut attributes = Vec::with_capacity(length);
        for _ in 0..length {
            let item = LabelPair::decode_with_decoder(decoder)?;
            attributes.push(item);
        }
        Ok(Self {
            tag,
            signal,
            ts_unix_nano,
            severity,
            labels,
            body,
            attributes,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        TailRecordInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        TailRecordInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<TailRecordOutput> for TailRecordInput {
    fn from(o: TailRecordOutput) -> Self {
        Self {
            signal: o.signal,
            ts_unix_nano: o.ts_unix_nano,
            severity: o.severity,
            labels: o.labels,
            body: o.body,
            attributes: o.attributes,
        }
    }
}

impl From<TailRecordInput> for TailRecordOutput {
    fn from(i: TailRecordInput) -> Self {
        Self {
            tag: 81u8,
            signal: i.signal,
            ts_unix_nano: i.ts_unix_nano,
            severity: i.severity,
            labels: i.labels,
            body: i.body,
            attributes: i.attributes,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TailSampleInput {
    pub signal: u8,
    pub ts_unix_nano: u64,
    pub metric_type: u8,
    pub series_fingerprint: u64,
    pub value: f64,
    pub labels: Vec<LabelPair>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TailSampleOutput {
    pub tag: u8,
    pub signal: u8,
    pub ts_unix_nano: u64,
    pub metric_type: u8,
    pub series_fingerprint: u64,
    pub value: f64,
    pub labels: Vec<LabelPair>,
}

pub type TailSample = TailSampleOutput;

impl TailSampleInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(84);
        encoder.write_byte(self.signal);
        encoder.write_u64_be(self.ts_unix_nano);
        encoder.write_byte(self.metric_type);
        encoder.write_u64_be(self.series_fingerprint);
        encoder.write_u64_be((self.value).to_bits());
        encoder.write_u16_be(self.labels.len() as u16);
        for item in &self.labels {
            item.encode_into(encoder)?;
        }
        Ok(())
    }

}

impl TailSampleOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 84u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 84, got {}", tag)));
        }
        let signal = decoder.read_byte()?;
        let ts_unix_nano = decoder.read_u64_be()?;
        let metric_type = decoder.read_byte()?;
        let series_fingerprint = decoder.read_u64_be()?;
        let value = f64::from_bits(decoder.read_u64_be()?);
        let length = decoder.read_u16_be()? as usize;
        let mut labels = Vec::with_capacity(length);
        for _ in 0..length {
            let item = LabelPair::decode_with_decoder(decoder)?;
            labels.push(item);
        }
        Ok(Self {
            tag,
            signal,
            ts_unix_nano,
            metric_type,
            series_fingerprint,
            value,
            labels,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        TailSampleInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        TailSampleInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<TailSampleOutput> for TailSampleInput {
    fn from(o: TailSampleOutput) -> Self {
        Self {
            signal: o.signal,
            ts_unix_nano: o.ts_unix_nano,
            metric_type: o.metric_type,
            series_fingerprint: o.series_fingerprint,
            value: o.value,
            labels: o.labels,
        }
    }
}

impl From<TailSampleInput> for TailSampleOutput {
    fn from(i: TailSampleInput) -> Self {
        Self {
            tag: 84u8,
            signal: i.signal,
            ts_unix_nano: i.ts_unix_nano,
            metric_type: i.metric_type,
            series_fingerprint: i.series_fingerprint,
            value: i.value,
            labels: i.labels,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TailMetricPointV2Input {
    pub signal: u8,
    pub series_fingerprint: u64,
    pub labels: Vec<LabelPair>,
    pub descriptor: MetricDescriptorV2,
    pub point: MetricPointV2,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TailMetricPointV2Output {
    pub tag: u8,
    pub signal: u8,
    pub series_fingerprint: u64,
    pub labels: Vec<LabelPair>,
    pub descriptor: MetricDescriptorV2,
    pub point: MetricPointV2,
}

pub type TailMetricPointV2 = TailMetricPointV2Output;

impl TailMetricPointV2Input {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into_with_context(&mut encoder, &EncodeContext::new())?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        self.encode_into_with_context(encoder, &EncodeContext::new())
    }

    pub fn encode_with_context(&self, ctx: &EncodeContext) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into_with_context(&mut encoder, ctx)?;
        Ok(encoder.finish())
    }

    pub fn encode_into_with_context(&self, encoder: &mut BitStreamEncoder, ctx: &EncodeContext) -> Result<()> {

        // Build parent context for nested struct encoding
        let mut parent_fields: HashMap<std::string::String, FieldValue> = HashMap::new();
        parent_fields.insert("signal".to_string(), FieldValue::U8(self.signal));
        parent_fields.insert("series_fingerprint".to_string(), FieldValue::U64(self.series_fingerprint));
        // Collect items with sub-field values for typed array 'labels'
        {
            let mut items_data: Vec<(std::string::String, HashMap<std::string::String, FieldValue>)> = Vec::new();
            for item in &self.labels {
                let item_bytes = item.encode()?;
                let mut item_fields: HashMap<std::string::String, FieldValue> = HashMap::new();
                item_fields.insert("_encoded_size".to_string(), FieldValue::U64(item_bytes.len() as u64));
                item_fields.insert("key".to_string(), FieldValue::String(item.key.clone()));
                item_fields.insert("value".to_string(), FieldValue::String(item.value.clone()));
                items_data.push(("LabelPair".to_string(), item_fields));
            }
            parent_fields.insert("labels".to_string(), FieldValue::Items(items_data));
        }
        let child_ctx = ctx.extend_with_parent(parent_fields);
        let _ = &child_ctx; // Used by nested struct encoding
        encoder.write_byte(85);
        encoder.write_byte(self.signal);
        encoder.write_u64_be(self.series_fingerprint);
        encoder.write_u16_be(self.labels.len() as u16);
        for item in &self.labels {
            item.encode_into(encoder)?;
        }
        // Encode nested struct descriptor
        self.descriptor.encode_into(encoder)?;
        // Encode nested struct point
        self.point.encode_into(encoder)?;
        Ok(())
    }

}

impl TailMetricPointV2Output {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 85u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 85, got {}", tag)));
        }
        let signal = decoder.read_byte()?;
        let series_fingerprint = decoder.read_u64_be()?;
        let length = decoder.read_u16_be()? as usize;
        let mut labels = Vec::with_capacity(length);
        for _ in 0..length {
            let item = LabelPair::decode_with_decoder(decoder)?;
            labels.push(item);
        }
        let descriptor = MetricDescriptorV2::decode_with_decoder(decoder)?;
        let point = MetricPointV2::decode_with_decoder(decoder)?;
        Ok(Self {
            tag,
            signal,
            series_fingerprint,
            labels,
            descriptor,
            point,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        TailMetricPointV2Input::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        TailMetricPointV2Input::from(self.clone()).encode_into(encoder)
    }
    pub fn encode_with_context(&self, ctx: &EncodeContext) -> Result<Vec<u8>> {
        TailMetricPointV2Input::from(self.clone()).encode_with_context(ctx)
    }
    pub fn encode_into_with_context(&self, encoder: &mut BitStreamEncoder, ctx: &EncodeContext) -> Result<()> {
        TailMetricPointV2Input::from(self.clone()).encode_into_with_context(encoder, ctx)
    }
}

impl From<TailMetricPointV2Output> for TailMetricPointV2Input {
    fn from(o: TailMetricPointV2Output) -> Self {
        Self {
            signal: o.signal,
            series_fingerprint: o.series_fingerprint,
            labels: o.labels,
            descriptor: o.descriptor,
            point: o.point,
        }
    }
}

impl From<TailMetricPointV2Input> for TailMetricPointV2Output {
    fn from(i: TailMetricPointV2Input) -> Self {
        Self {
            tag: 85u8,
            signal: i.signal,
            series_fingerprint: i.series_fingerprint,
            labels: i.labels,
            descriptor: i.descriptor,
            point: i.point,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LiveQueryInput {
    pub signal: u8,
    pub matchers: Vec<MatcherSpec>,
    pub ts_min_unix_nano: u64,
    pub ts_max_unix_nano: u64,
    pub body_contains: std::string::String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LiveQueryOutput {
    pub tag: u8,
    pub signal: u8,
    pub matchers: Vec<MatcherSpec>,
    pub ts_min_unix_nano: u64,
    pub ts_max_unix_nano: u64,
    pub body_contains: std::string::String,
}

pub type LiveQuery = LiveQueryOutput;

impl LiveQueryInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(82);
        encoder.write_byte(self.signal);
        encoder.write_u16_be(self.matchers.len() as u16);
        for item in &self.matchers {
            item.encode_into(encoder)?;
        }
        encoder.write_u64_be(self.ts_min_unix_nano);
        encoder.write_u64_be(self.ts_max_unix_nano);
        encoder.write_u32_be(self.body_contains.len() as u32);
        let string_bytes: &[u8] = self.body_contains.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        Ok(())
    }

}

impl LiveQueryOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 82u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 82, got {}", tag)));
        }
        let signal = decoder.read_byte()?;
        let length = decoder.read_u16_be()? as usize;
        let mut matchers = Vec::with_capacity(length);
        for _ in 0..length {
            let item = MatcherSpec::decode_with_decoder(decoder)?;
            matchers.push(item);
        }
        let ts_min_unix_nano = decoder.read_u64_be()?;
        let ts_max_unix_nano = decoder.read_u64_be()?;
        let length = decoder.read_u32_be()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let body_contains = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        Ok(Self {
            tag,
            signal,
            matchers,
            ts_min_unix_nano,
            ts_max_unix_nano,
            body_contains,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        LiveQueryInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        LiveQueryInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<LiveQueryOutput> for LiveQueryInput {
    fn from(o: LiveQueryOutput) -> Self {
        Self {
            signal: o.signal,
            matchers: o.matchers,
            ts_min_unix_nano: o.ts_min_unix_nano,
            ts_max_unix_nano: o.ts_max_unix_nano,
            body_contains: o.body_contains,
        }
    }
}

impl From<LiveQueryInput> for LiveQueryOutput {
    fn from(i: LiveQueryInput) -> Self {
        Self {
            tag: 82u8,
            signal: i.signal,
            matchers: i.matchers,
            ts_min_unix_nano: i.ts_min_unix_nano,
            ts_max_unix_nano: i.ts_max_unix_nano,
            body_contains: i.body_contains,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LiveBatchInput {
    pub writer_uuid: Vec<u8>,
    pub records: Vec<LiveRecord>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LiveBatchOutput {
    pub tag: u8,
    pub writer_uuid: Vec<u8>,
    pub records: Vec<LiveRecord>,
}

pub type LiveBatch = LiveBatchOutput;

impl LiveBatchInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(83);
        for item in &self.writer_uuid {
            encoder.write_byte(*item);
        }
        encoder.write_u32_be(self.records.len() as u32);
        for item in &self.records {
            item.encode_into(encoder)?;
        }
        Ok(())
    }

}

impl LiveBatchOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 83u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 83, got {}", tag)));
        }
        let mut writer_uuid = Vec::with_capacity(16);
        for _ in 0..16 {
            let item = decoder.read_byte()?;
            writer_uuid.push(item);
        }
        let length = decoder.read_u32_be()? as usize;
        let mut records = Vec::with_capacity(length);
        for _ in 0..length {
            let item = LiveRecord::decode_with_decoder(decoder)?;
            records.push(item);
        }
        Ok(Self {
            tag,
            writer_uuid,
            records,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        LiveBatchInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        LiveBatchInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<LiveBatchOutput> for LiveBatchInput {
    fn from(o: LiveBatchOutput) -> Self {
        Self {
            writer_uuid: o.writer_uuid,
            records: o.records,
        }
    }
}

impl From<LiveBatchInput> for LiveBatchOutput {
    fn from(i: LiveBatchInput) -> Self {
        Self {
            tag: 83u8,
            writer_uuid: i.writer_uuid,
            records: i.records,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LiveRecord {
    pub wal_shard: u32,
    pub wal_seg: u64,
    pub ts_unix_nano: u64,
    pub severity: u8,
    pub labels: Vec<LabelPair>,
    pub body: std::string::String,
    pub attributes: Vec<LabelPair>,
}

impl LiveRecord {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_u32_be(self.wal_shard);
        encoder.write_u64_be(self.wal_seg);
        encoder.write_u64_be(self.ts_unix_nano);
        encoder.write_byte(self.severity);
        encoder.write_u16_be(self.labels.len() as u16);
        for item in &self.labels {
            item.encode_into(encoder)?;
        }
        encoder.write_u32_be(self.body.len() as u32);
        let string_bytes: &[u8] = self.body.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        encoder.write_u16_be(self.attributes.len() as u16);
        for item in &self.attributes {
            item.encode_into(encoder)?;
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let wal_shard = decoder.read_u32_be()?;
        let wal_seg = decoder.read_u64_be()?;
        let ts_unix_nano = decoder.read_u64_be()?;
        let severity = decoder.read_byte()?;
        let length = decoder.read_u16_be()? as usize;
        let mut labels = Vec::with_capacity(length);
        for _ in 0..length {
            let item = LabelPair::decode_with_decoder(decoder)?;
            labels.push(item);
        }
        let length = decoder.read_u32_be()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let body = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        let length = decoder.read_u16_be()? as usize;
        let mut attributes = Vec::with_capacity(length);
        for _ in 0..length {
            let item = LabelPair::decode_with_decoder(decoder)?;
            attributes.push(item);
        }
        Ok(Self {
            wal_shard,
            wal_seg,
            ts_unix_nano,
            severity,
            labels,
            body,
            attributes,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LabelPair {
    pub key: std::string::String,
    pub value: std::string::String,
}

impl LabelPair {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(self.key.len() as u8);
        let string_bytes: &[u8] = self.key.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        encoder.write_u16_be(self.value.len() as u16);
        let string_bytes: &[u8] = self.value.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let length = decoder.read_byte()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let key = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        let length = decoder.read_u16_be()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let value = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        Ok(Self {
            key,
            value,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricsBatch {
    pub series: Vec<SeriesDictEntry>,
    pub samples: Vec<MetricSample>,
}

impl MetricsBatch {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_u32_be(self.series.len() as u32);
        for item in &self.series {
            item.encode_into(encoder)?;
        }
        encoder.write_u32_be(self.samples.len() as u32);
        for item in &self.samples {
            item.encode_into(encoder)?;
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let length = decoder.read_u32_be()? as usize;
        let mut series = Vec::with_capacity(length);
        for _ in 0..length {
            let item = SeriesDictEntry::decode_with_decoder(decoder)?;
            series.push(item);
        }
        let length = decoder.read_u32_be()? as usize;
        let mut samples = Vec::with_capacity(length);
        for _ in 0..length {
            let item = MetricSample::decode_with_decoder(decoder)?;
            samples.push(item);
        }
        Ok(Self {
            series,
            samples,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SeriesDictEntry {
    pub fingerprint: u64,
    pub metric_type: u8,
    pub labels: Vec<LabelPair>,
}

impl SeriesDictEntry {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_u64_be(self.fingerprint);
        encoder.write_byte(self.metric_type);
        encoder.write_u16_be(self.labels.len() as u16);
        for item in &self.labels {
            item.encode_into(encoder)?;
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let fingerprint = decoder.read_u64_be()?;
        let metric_type = decoder.read_byte()?;
        let length = decoder.read_u16_be()? as usize;
        let mut labels = Vec::with_capacity(length);
        for _ in 0..length {
            let item = LabelPair::decode_with_decoder(decoder)?;
            labels.push(item);
        }
        Ok(Self {
            fingerprint,
            metric_type,
            labels,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricSample {
    pub fingerprint: u64,
    pub ts_unix_nano: u64,
    pub value: f64,
}

impl MetricSample {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_u64_be(self.fingerprint);
        encoder.write_u64_be(self.ts_unix_nano);
        encoder.write_u64_be((self.value).to_bits());
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let fingerprint = decoder.read_u64_be()?;
        let ts_unix_nano = decoder.read_u64_be()?;
        let value = f64::from_bits(decoder.read_u64_be()?);
        Ok(Self {
            fingerprint,
            ts_unix_nano,
            value,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogsBatch {
    pub streams: Vec<LogStream>,
}

impl LogsBatch {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_u32_be(self.streams.len() as u32);
        for item in &self.streams {
            item.encode_into(encoder)?;
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let length = decoder.read_u32_be()? as usize;
        let mut streams = Vec::with_capacity(length);
        for _ in 0..length {
            let item = LogStream::decode_with_decoder(decoder)?;
            streams.push(item);
        }
        Ok(Self {
            streams,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogStream {
    pub fingerprint: u64,
    pub labels: Vec<LabelPair>,
    pub entries: Vec<LogEntry>,
}

impl LogStream {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_u64_be(self.fingerprint);
        encoder.write_u16_be(self.labels.len() as u16);
        for item in &self.labels {
            item.encode_into(encoder)?;
        }
        encoder.write_u32_be(self.entries.len() as u32);
        for item in &self.entries {
            item.encode_into(encoder)?;
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let fingerprint = decoder.read_u64_be()?;
        let length = decoder.read_u16_be()? as usize;
        let mut labels = Vec::with_capacity(length);
        for _ in 0..length {
            let item = LabelPair::decode_with_decoder(decoder)?;
            labels.push(item);
        }
        let length = decoder.read_u32_be()? as usize;
        let mut entries = Vec::with_capacity(length);
        for _ in 0..length {
            let item = LogEntry::decode_with_decoder(decoder)?;
            entries.push(item);
        }
        Ok(Self {
            fingerprint,
            labels,
            entries,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogEntry {
    pub ts_unix_nano: u64,
    pub severity: u8,
    pub body: std::string::String,
    pub attributes: Vec<LabelPair>,
}

impl LogEntry {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_u64_be(self.ts_unix_nano);
        encoder.write_byte(self.severity);
        encoder.write_u32_be(self.body.len() as u32);
        let string_bytes: &[u8] = self.body.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        encoder.write_u16_be(self.attributes.len() as u16);
        for item in &self.attributes {
            item.encode_into(encoder)?;
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let ts_unix_nano = decoder.read_u64_be()?;
        let severity = decoder.read_byte()?;
        let length = decoder.read_u32_be()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let body = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        let length = decoder.read_u16_be()? as usize;
        let mut attributes = Vec::with_capacity(length);
        for _ in 0..length {
            let item = LabelPair::decode_with_decoder(decoder)?;
            attributes.push(item);
        }
        Ok(Self {
            ts_unix_nano,
            severity,
            body,
            attributes,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TracesBatch {
    pub resources: Vec<ResourceEntry>,
    pub scopes: Vec<ScopeEntry>,
    pub spans: Vec<Span>,
}

impl TracesBatch {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_u16_be(self.resources.len() as u16);
        for item in &self.resources {
            item.encode_into(encoder)?;
        }
        encoder.write_u16_be(self.scopes.len() as u16);
        for item in &self.scopes {
            item.encode_into(encoder)?;
        }
        encoder.write_u32_be(self.spans.len() as u32);
        for item in &self.spans {
            item.encode_into(encoder)?;
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let length = decoder.read_u16_be()? as usize;
        let mut resources = Vec::with_capacity(length);
        for _ in 0..length {
            let item = ResourceEntry::decode_with_decoder(decoder)?;
            resources.push(item);
        }
        let length = decoder.read_u16_be()? as usize;
        let mut scopes = Vec::with_capacity(length);
        for _ in 0..length {
            let item = ScopeEntry::decode_with_decoder(decoder)?;
            scopes.push(item);
        }
        let length = decoder.read_u32_be()? as usize;
        let mut spans = Vec::with_capacity(length);
        for _ in 0..length {
            let item = Span::decode_with_decoder(decoder)?;
            spans.push(item);
        }
        Ok(Self {
            resources,
            scopes,
            spans,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResourceEntry {
    pub labels: Vec<LabelPair>,
}

impl ResourceEntry {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_u16_be(self.labels.len() as u16);
        for item in &self.labels {
            item.encode_into(encoder)?;
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let length = decoder.read_u16_be()? as usize;
        let mut labels = Vec::with_capacity(length);
        for _ in 0..length {
            let item = LabelPair::decode_with_decoder(decoder)?;
            labels.push(item);
        }
        Ok(Self {
            labels,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScopeEntry {
    pub name: std::string::String,
    pub version: std::string::String,
}

impl ScopeEntry {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(self.name.len() as u8);
        let string_bytes: &[u8] = self.name.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        encoder.write_byte(self.version.chars().count() as u8);
        let string_bytes: Vec<u8> = self.version.chars().map(|c| c as u8).collect();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let length = decoder.read_byte()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let name = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        let length = decoder.read_byte()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let version: std::string::String = bytes.iter().map(|&b| b as char).collect();
        Ok(Self {
            name,
            version,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Span {
    pub resource_idx: u16,
    pub scope_idx: u16,
    pub trace_id: Vec<u8>,
    pub span_id: Vec<u8>,
    pub parent_span_id: Option<Vec<u8>>,
    pub name: std::string::String,
    pub kind: u8,
    pub start_unix_nano: u64,
    pub end_unix_nano: u64,
    pub status_code: u8,
    pub status_message: std::string::String,
    pub attributes: Vec<LabelPair>,
    pub events: Vec<SpanEvent>,
    pub links: Vec<SpanLink>,
}

impl Span {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_u16_be(self.resource_idx);
        encoder.write_u16_be(self.scope_idx);
        for item in &self.trace_id {
            encoder.write_byte(*item);
        }
        for item in &self.span_id {
            encoder.write_byte(*item);
        }
        if let Some(ref v) = self.parent_span_id {
            encoder.write_uint8(1);
            for b in v.iter() {
                encoder.write_byte(*b);
            }
        } else {
            encoder.write_uint8(0);
        }
        encoder.write_u16_be(self.name.len() as u16);
        let string_bytes: &[u8] = self.name.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        encoder.write_byte(self.kind);
        encoder.write_u64_be(self.start_unix_nano);
        encoder.write_u64_be(self.end_unix_nano);
        encoder.write_byte(self.status_code);
        encoder.write_u16_be(self.status_message.len() as u16);
        let string_bytes: &[u8] = self.status_message.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        encoder.write_u16_be(self.attributes.len() as u16);
        for item in &self.attributes {
            item.encode_into(encoder)?;
        }
        encoder.write_u16_be(self.events.len() as u16);
        for item in &self.events {
            item.encode_into(encoder)?;
        }
        encoder.write_byte(self.links.len() as u8);
        for item in &self.links {
            item.encode_into(encoder)?;
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let resource_idx = decoder.read_u16_be()?;
        let scope_idx = decoder.read_u16_be()?;
        let mut trace_id = Vec::with_capacity(16);
        for _ in 0..16 {
            let item = decoder.read_byte()?;
            trace_id.push(item);
        }
        let mut span_id = Vec::with_capacity(8);
        for _ in 0..8 {
            let item = decoder.read_byte()?;
            span_id.push(item);
        }
        let has_value = decoder.read_uint8()? != 0;
        let parent_span_id = if has_value {
            {
                let mut buf: Vec<u8> = Vec::with_capacity(8);
                for _ in 0..8 {
                    buf.push(decoder.read_byte()?);
                }
                Some(buf)
            }
        } else {
            None
        };
        let length = decoder.read_u16_be()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let name = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        let kind = decoder.read_byte()?;
        let start_unix_nano = decoder.read_u64_be()?;
        let end_unix_nano = decoder.read_u64_be()?;
        let status_code = decoder.read_byte()?;
        let length = decoder.read_u16_be()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let status_message = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        let length = decoder.read_u16_be()? as usize;
        let mut attributes = Vec::with_capacity(length);
        for _ in 0..length {
            let item = LabelPair::decode_with_decoder(decoder)?;
            attributes.push(item);
        }
        let length = decoder.read_u16_be()? as usize;
        let mut events = Vec::with_capacity(length);
        for _ in 0..length {
            let item = SpanEvent::decode_with_decoder(decoder)?;
            events.push(item);
        }
        let length = decoder.read_byte()? as usize;
        let mut links = Vec::with_capacity(length);
        for _ in 0..length {
            let item = SpanLink::decode_with_decoder(decoder)?;
            links.push(item);
        }
        Ok(Self {
            resource_idx,
            scope_idx,
            trace_id,
            span_id,
            parent_span_id,
            name,
            kind,
            start_unix_nano,
            end_unix_nano,
            status_code,
            status_message,
            attributes,
            events,
            links,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SpanEvent {
    pub ts_unix_nano: u64,
    pub name: std::string::String,
    pub attributes: Vec<LabelPair>,
}

impl SpanEvent {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_u64_be(self.ts_unix_nano);
        encoder.write_u16_be(self.name.len() as u16);
        let string_bytes: &[u8] = self.name.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        encoder.write_byte(self.attributes.len() as u8);
        for item in &self.attributes {
            item.encode_into(encoder)?;
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let ts_unix_nano = decoder.read_u64_be()?;
        let length = decoder.read_u16_be()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let name = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        let length = decoder.read_byte()? as usize;
        let mut attributes = Vec::with_capacity(length);
        for _ in 0..length {
            let item = LabelPair::decode_with_decoder(decoder)?;
            attributes.push(item);
        }
        Ok(Self {
            ts_unix_nano,
            name,
            attributes,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SpanLink {
    pub trace_id: Vec<u8>,
    pub span_id: Vec<u8>,
    pub attributes: Vec<LabelPair>,
}

impl SpanLink {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        for item in &self.trace_id {
            encoder.write_byte(*item);
        }
        for item in &self.span_id {
            encoder.write_byte(*item);
        }
        encoder.write_byte(self.attributes.len() as u8);
        for item in &self.attributes {
            item.encode_into(encoder)?;
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let mut trace_id = Vec::with_capacity(16);
        for _ in 0..16 {
            let item = decoder.read_byte()?;
            trace_id.push(item);
        }
        let mut span_id = Vec::with_capacity(8);
        for _ in 0..8 {
            let item = decoder.read_byte()?;
            span_id.push(item);
        }
        let length = decoder.read_byte()? as usize;
        let mut attributes = Vec::with_capacity(length);
        for _ in 0..length {
            let item = LabelPair::decode_with_decoder(decoder)?;
            attributes.push(item);
        }
        Ok(Self {
            trace_id,
            span_id,
            attributes,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProfilesBatch {
    pub samples: Vec<ProfileBlob>,
}

impl ProfilesBatch {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_u32_be(self.samples.len() as u32);
        for item in &self.samples {
            item.encode_into(encoder)?;
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let length = decoder.read_u32_be()? as usize;
        let mut samples = Vec::with_capacity(length);
        for _ in 0..length {
            let item = ProfileBlob::decode_with_decoder(decoder)?;
            samples.push(item);
        }
        Ok(Self {
            samples,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProfileBlob {
    pub ts_unix_nano: u64,
    pub duration_nano: u64,
    pub labels: Vec<LabelPair>,
    pub format: u8,
    pub data: Vec<u8>,
}

impl ProfileBlob {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_u64_be(self.ts_unix_nano);
        encoder.write_u64_be(self.duration_nano);
        encoder.write_u16_be(self.labels.len() as u16);
        for item in &self.labels {
            item.encode_into(encoder)?;
        }
        encoder.write_byte(self.format);
        encoder.write_u32_be(self.data.len() as u32);
        for item in &self.data {
            encoder.write_byte(*item);
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let ts_unix_nano = decoder.read_u64_be()?;
        let duration_nano = decoder.read_u64_be()?;
        let length = decoder.read_u16_be()? as usize;
        let mut labels = Vec::with_capacity(length);
        for _ in 0..length {
            let item = LabelPair::decode_with_decoder(decoder)?;
            labels.push(item);
        }
        let format = decoder.read_byte()?;
        let length = decoder.read_u32_be()? as usize;
        let mut data = Vec::with_capacity(length);
        for _ in 0..length {
            let item = decoder.read_byte()?;
            data.push(item);
        }
        Ok(Self {
            ts_unix_nano,
            duration_nano,
            labels,
            format,
            data,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DummyBatch {
    pub records: Vec<DummyRecord>,
}

impl DummyBatch {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_u32_be(self.records.len() as u32);
        for item in &self.records {
            item.encode_into(encoder)?;
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let length = decoder.read_u32_be()? as usize;
        let mut records = Vec::with_capacity(length);
        for _ in 0..length {
            let item = DummyRecord::decode_with_decoder(decoder)?;
            records.push(item);
        }
        Ok(Self {
            records,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DummyRecord {
    pub ts_unix_nano: u64,
    pub key: std::string::String,
    pub value: Vec<u8>,
}

impl DummyRecord {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_u64_be(self.ts_unix_nano);
        encoder.write_u16_be(self.key.len() as u16);
        let string_bytes: &[u8] = self.key.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        encoder.write_u32_be(self.value.len() as u32);
        for item in &self.value {
            encoder.write_byte(*item);
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let ts_unix_nano = decoder.read_u64_be()?;
        let length = decoder.read_u16_be()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let key = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        let length = decoder.read_u32_be()? as usize;
        let mut value = Vec::with_capacity(length);
        for _ in 0..length {
            let item = decoder.read_byte()?;
            value.push(item);
        }
        Ok(Self {
            ts_unix_nano,
            key,
            value,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricsBatchV2Input {
    pub descriptors: Vec<MetricDescriptorV2>,
    pub points: Vec<MetricPointV2>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricsBatchV2Output {
    pub magic: u32,
    pub descriptors: Vec<MetricDescriptorV2>,
    pub points: Vec<MetricPointV2>,
}

pub type MetricsBatchV2 = MetricsBatchV2Output;

impl MetricsBatchV2Input {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_u32_be(1397568000);
        encoder.write_u32_be(self.descriptors.len() as u32);
        for item in &self.descriptors {
            item.encode_into(encoder)?;
        }
        encoder.write_u32_be(self.points.len() as u32);
        for item in &self.points {
            item.encode_into(encoder)?;
        }
        Ok(())
    }

}

impl MetricsBatchV2Output {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let magic = decoder.read_u32_be()?;
        if magic != 1397568000u32 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 1397568000, got {}", magic)));
        }
        let length = decoder.read_u32_be()? as usize;
        let mut descriptors = Vec::with_capacity(length);
        for _ in 0..length {
            let item = MetricDescriptorV2::decode_with_decoder(decoder)?;
            descriptors.push(item);
        }
        let length = decoder.read_u32_be()? as usize;
        let mut points = Vec::with_capacity(length);
        for _ in 0..length {
            let item = MetricPointV2::decode_with_decoder(decoder)?;
            points.push(item);
        }
        Ok(Self {
            magic,
            descriptors,
            points,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        MetricsBatchV2Input::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        MetricsBatchV2Input::from(self.clone()).encode_into(encoder)
    }
}

impl From<MetricsBatchV2Output> for MetricsBatchV2Input {
    fn from(o: MetricsBatchV2Output) -> Self {
        Self {
            descriptors: o.descriptors,
            points: o.points,
        }
    }
}

impl From<MetricsBatchV2Input> for MetricsBatchV2Output {
    fn from(i: MetricsBatchV2Input) -> Self {
        Self {
            magic: 1397568000u32,
            descriptors: i.descriptors,
            points: i.points,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricDescriptorV2 {
    pub id: u32,
    pub name: std::string::String,
    pub description: std::string::String,
    pub unit: std::string::String,
    pub metric_kind: u8,
    pub temporality: u8,
    pub monotonic: u8,
    pub resource_attrs: Vec<LabelPair>,
    pub scope_name: std::string::String,
    pub scope_version: std::string::String,
    pub scope_attrs: Vec<LabelPair>,
}

impl MetricDescriptorV2 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_u32_be(self.id);
        encoder.write_u16_be(self.name.len() as u16);
        let string_bytes: &[u8] = self.name.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        encoder.write_u16_be(self.description.len() as u16);
        let string_bytes: &[u8] = self.description.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        encoder.write_u16_be(self.unit.len() as u16);
        let string_bytes: &[u8] = self.unit.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        encoder.write_byte(self.metric_kind);
        encoder.write_byte(self.temporality);
        encoder.write_byte(self.monotonic);
        encoder.write_u16_be(self.resource_attrs.len() as u16);
        for item in &self.resource_attrs {
            item.encode_into(encoder)?;
        }
        encoder.write_byte(self.scope_name.len() as u8);
        let string_bytes: &[u8] = self.scope_name.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        encoder.write_byte(self.scope_version.len() as u8);
        let string_bytes: &[u8] = self.scope_version.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        encoder.write_u16_be(self.scope_attrs.len() as u16);
        for item in &self.scope_attrs {
            item.encode_into(encoder)?;
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let id = decoder.read_u32_be()?;
        let length = decoder.read_u16_be()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let name = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        let length = decoder.read_u16_be()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let description = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        let length = decoder.read_u16_be()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let unit = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        let metric_kind = decoder.read_byte()?;
        let temporality = decoder.read_byte()?;
        let monotonic = decoder.read_byte()?;
        let length = decoder.read_u16_be()? as usize;
        let mut resource_attrs = Vec::with_capacity(length);
        for _ in 0..length {
            let item = LabelPair::decode_with_decoder(decoder)?;
            resource_attrs.push(item);
        }
        let length = decoder.read_byte()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let scope_name = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        let length = decoder.read_byte()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let scope_version = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        let length = decoder.read_u16_be()? as usize;
        let mut scope_attrs = Vec::with_capacity(length);
        for _ in 0..length {
            let item = LabelPair::decode_with_decoder(decoder)?;
            scope_attrs.push(item);
        }
        Ok(Self {
            id,
            name,
            description,
            unit,
            metric_kind,
            temporality,
            monotonic,
            resource_attrs,
            scope_name,
            scope_version,
            scope_attrs,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricPointV2 {
    pub value: MetricPointV2Value,
}

impl MetricPointV2 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        self.value.encode_into(encoder)?;
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let value = MetricPointV2Value::decode_with_decoder(decoder)?;
        Ok(Self {
            value,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScalarPointV2Input {
    pub descriptor_id: u32,
    pub start_unix_nano: u64,
    pub ts_unix_nano: u64,
    pub flags: u32,
    pub attributes: Vec<LabelPair>,
    pub exemplars: Vec<MetricExemplarV2>,
    pub number: MetricNumberV2,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScalarPointV2Output {
    pub tag: u8,
    pub descriptor_id: u32,
    pub start_unix_nano: u64,
    pub ts_unix_nano: u64,
    pub flags: u32,
    pub attributes: Vec<LabelPair>,
    pub exemplars: Vec<MetricExemplarV2>,
    pub number: MetricNumberV2,
}

pub type ScalarPointV2 = ScalarPointV2Output;

impl ScalarPointV2Input {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into_with_context(&mut encoder, &EncodeContext::new())?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        self.encode_into_with_context(encoder, &EncodeContext::new())
    }

    pub fn encode_with_context(&self, ctx: &EncodeContext) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into_with_context(&mut encoder, ctx)?;
        Ok(encoder.finish())
    }

    pub fn encode_into_with_context(&self, encoder: &mut BitStreamEncoder, ctx: &EncodeContext) -> Result<()> {

        // Build parent context for nested struct encoding
        let mut parent_fields: HashMap<std::string::String, FieldValue> = HashMap::new();
        parent_fields.insert("descriptor_id".to_string(), FieldValue::U32(self.descriptor_id));
        parent_fields.insert("start_unix_nano".to_string(), FieldValue::U64(self.start_unix_nano));
        parent_fields.insert("ts_unix_nano".to_string(), FieldValue::U64(self.ts_unix_nano));
        parent_fields.insert("flags".to_string(), FieldValue::U32(self.flags));
        // Collect items with sub-field values for typed array 'attributes'
        {
            let mut items_data: Vec<(std::string::String, HashMap<std::string::String, FieldValue>)> = Vec::new();
            for item in &self.attributes {
                let item_bytes = item.encode()?;
                let mut item_fields: HashMap<std::string::String, FieldValue> = HashMap::new();
                item_fields.insert("_encoded_size".to_string(), FieldValue::U64(item_bytes.len() as u64));
                item_fields.insert("key".to_string(), FieldValue::String(item.key.clone()));
                item_fields.insert("value".to_string(), FieldValue::String(item.value.clone()));
                items_data.push(("LabelPair".to_string(), item_fields));
            }
            parent_fields.insert("attributes".to_string(), FieldValue::Items(items_data));
        }
        // Collect items with sub-field values for typed array 'exemplars'
        {
            let mut items_data: Vec<(std::string::String, HashMap<std::string::String, FieldValue>)> = Vec::new();
            for _ in &self.exemplars {
                let item_bytes = Vec::<u8>::new(); // Items need context, skip encoding for now
                let mut item_fields: HashMap<std::string::String, FieldValue> = HashMap::new();
                item_fields.insert("_encoded_size".to_string(), FieldValue::U64(item_bytes.len() as u64));
                items_data.push(("MetricExemplarV2".to_string(), item_fields));
            }
            parent_fields.insert("exemplars".to_string(), FieldValue::Items(items_data));
        }
        let child_ctx = ctx.extend_with_parent(parent_fields);
        let _ = &child_ctx; // Used by nested struct encoding
        encoder.write_byte(1);
        encoder.write_u32_be(self.descriptor_id);
        encoder.write_u64_be(self.start_unix_nano);
        encoder.write_u64_be(self.ts_unix_nano);
        encoder.write_u32_be(self.flags);
        encoder.write_u16_be(self.attributes.len() as u16);
        for item in &self.attributes {
            item.encode_into(encoder)?;
        }
        encoder.write_u16_be(self.exemplars.len() as u16);
        for item in &self.exemplars {
            item.encode_into_with_context(encoder, &child_ctx)?;
        }
        // Encode nested struct number
        self.number.encode_into(encoder)?;
        Ok(())
    }

}

impl ScalarPointV2Output {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 1u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 1, got {}", tag)));
        }
        let descriptor_id = decoder.read_u32_be()?;
        let start_unix_nano = decoder.read_u64_be()?;
        let ts_unix_nano = decoder.read_u64_be()?;
        let flags = decoder.read_u32_be()?;
        let length = decoder.read_u16_be()? as usize;
        let mut attributes = Vec::with_capacity(length);
        for _ in 0..length {
            let item = LabelPair::decode_with_decoder(decoder)?;
            attributes.push(item);
        }
        let length = decoder.read_u16_be()? as usize;
        let mut exemplars = Vec::with_capacity(length);
        for _ in 0..length {
            let item = MetricExemplarV2::decode_with_decoder(decoder)?;
            exemplars.push(item);
        }
        let number = MetricNumberV2::decode_with_decoder(decoder)?;
        Ok(Self {
            tag,
            descriptor_id,
            start_unix_nano,
            ts_unix_nano,
            flags,
            attributes,
            exemplars,
            number,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        ScalarPointV2Input::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        ScalarPointV2Input::from(self.clone()).encode_into(encoder)
    }
    pub fn encode_with_context(&self, ctx: &EncodeContext) -> Result<Vec<u8>> {
        ScalarPointV2Input::from(self.clone()).encode_with_context(ctx)
    }
    pub fn encode_into_with_context(&self, encoder: &mut BitStreamEncoder, ctx: &EncodeContext) -> Result<()> {
        ScalarPointV2Input::from(self.clone()).encode_into_with_context(encoder, ctx)
    }
}

impl From<ScalarPointV2Output> for ScalarPointV2Input {
    fn from(o: ScalarPointV2Output) -> Self {
        Self {
            descriptor_id: o.descriptor_id,
            start_unix_nano: o.start_unix_nano,
            ts_unix_nano: o.ts_unix_nano,
            flags: o.flags,
            attributes: o.attributes,
            exemplars: o.exemplars,
            number: o.number,
        }
    }
}

impl From<ScalarPointV2Input> for ScalarPointV2Output {
    fn from(i: ScalarPointV2Input) -> Self {
        Self {
            tag: 1u8,
            descriptor_id: i.descriptor_id,
            start_unix_nano: i.start_unix_nano,
            ts_unix_nano: i.ts_unix_nano,
            flags: i.flags,
            attributes: i.attributes,
            exemplars: i.exemplars,
            number: i.number,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct HistogramPointV2Input {
    pub descriptor_id: u32,
    pub start_unix_nano: u64,
    pub ts_unix_nano: u64,
    pub flags: u32,
    pub attributes: Vec<LabelPair>,
    pub exemplars: Vec<MetricExemplarV2>,
    pub count: u64,
    pub has_sum: u8,
    pub sum: f64,
    pub has_min: u8,
    pub min: f64,
    pub has_max: u8,
    pub max: f64,
    pub explicit_bounds: Vec<f64>,
    pub bucket_counts: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HistogramPointV2Output {
    pub tag: u8,
    pub descriptor_id: u32,
    pub start_unix_nano: u64,
    pub ts_unix_nano: u64,
    pub flags: u32,
    pub attributes: Vec<LabelPair>,
    pub exemplars: Vec<MetricExemplarV2>,
    pub count: u64,
    pub has_sum: u8,
    pub sum: f64,
    pub has_min: u8,
    pub min: f64,
    pub has_max: u8,
    pub max: f64,
    pub explicit_bounds: Vec<f64>,
    pub bucket_counts: Vec<u64>,
}

pub type HistogramPointV2 = HistogramPointV2Output;

impl HistogramPointV2Input {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into_with_context(&mut encoder, &EncodeContext::new())?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        self.encode_into_with_context(encoder, &EncodeContext::new())
    }

    pub fn encode_with_context(&self, ctx: &EncodeContext) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into_with_context(&mut encoder, ctx)?;
        Ok(encoder.finish())
    }

    pub fn encode_into_with_context(&self, encoder: &mut BitStreamEncoder, ctx: &EncodeContext) -> Result<()> {

        // Build parent context for nested struct encoding
        let mut parent_fields: HashMap<std::string::String, FieldValue> = HashMap::new();
        parent_fields.insert("descriptor_id".to_string(), FieldValue::U32(self.descriptor_id));
        parent_fields.insert("start_unix_nano".to_string(), FieldValue::U64(self.start_unix_nano));
        parent_fields.insert("ts_unix_nano".to_string(), FieldValue::U64(self.ts_unix_nano));
        parent_fields.insert("flags".to_string(), FieldValue::U32(self.flags));
        // Collect items with sub-field values for typed array 'attributes'
        {
            let mut items_data: Vec<(std::string::String, HashMap<std::string::String, FieldValue>)> = Vec::new();
            for item in &self.attributes {
                let item_bytes = item.encode()?;
                let mut item_fields: HashMap<std::string::String, FieldValue> = HashMap::new();
                item_fields.insert("_encoded_size".to_string(), FieldValue::U64(item_bytes.len() as u64));
                item_fields.insert("key".to_string(), FieldValue::String(item.key.clone()));
                item_fields.insert("value".to_string(), FieldValue::String(item.value.clone()));
                items_data.push(("LabelPair".to_string(), item_fields));
            }
            parent_fields.insert("attributes".to_string(), FieldValue::Items(items_data));
        }
        // Collect items with sub-field values for typed array 'exemplars'
        {
            let mut items_data: Vec<(std::string::String, HashMap<std::string::String, FieldValue>)> = Vec::new();
            for _ in &self.exemplars {
                let item_bytes = Vec::<u8>::new(); // Items need context, skip encoding for now
                let mut item_fields: HashMap<std::string::String, FieldValue> = HashMap::new();
                item_fields.insert("_encoded_size".to_string(), FieldValue::U64(item_bytes.len() as u64));
                items_data.push(("MetricExemplarV2".to_string(), item_fields));
            }
            parent_fields.insert("exemplars".to_string(), FieldValue::Items(items_data));
        }
        parent_fields.insert("count".to_string(), FieldValue::U64(self.count));
        parent_fields.insert("has_sum".to_string(), FieldValue::U8(self.has_sum));
        parent_fields.insert("sum".to_string(), FieldValue::F64(self.sum));
        parent_fields.insert("has_min".to_string(), FieldValue::U8(self.has_min));
        parent_fields.insert("min".to_string(), FieldValue::F64(self.min));
        parent_fields.insert("has_max".to_string(), FieldValue::U8(self.has_max));
        parent_fields.insert("max".to_string(), FieldValue::F64(self.max));
        let child_ctx = ctx.extend_with_parent(parent_fields);
        let _ = &child_ctx; // Used by nested struct encoding
        encoder.write_byte(2);
        encoder.write_u32_be(self.descriptor_id);
        encoder.write_u64_be(self.start_unix_nano);
        encoder.write_u64_be(self.ts_unix_nano);
        encoder.write_u32_be(self.flags);
        encoder.write_u16_be(self.attributes.len() as u16);
        for item in &self.attributes {
            item.encode_into(encoder)?;
        }
        encoder.write_u16_be(self.exemplars.len() as u16);
        for item in &self.exemplars {
            item.encode_into_with_context(encoder, &child_ctx)?;
        }
        encoder.write_u64_be(self.count);
        encoder.write_byte(self.has_sum);
        encoder.write_u64_be((self.sum).to_bits());
        encoder.write_byte(self.has_min);
        encoder.write_u64_be((self.min).to_bits());
        encoder.write_byte(self.has_max);
        encoder.write_u64_be((self.max).to_bits());
        encoder.write_u32_be(self.explicit_bounds.len() as u32);
        for item in &self.explicit_bounds {
            encoder.write_u64_be((*item).to_bits());
        }
        encoder.write_u32_be(self.bucket_counts.len() as u32);
        for item in &self.bucket_counts {
            encoder.write_u64_be(*item);
        }
        Ok(())
    }

}

impl HistogramPointV2Output {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 2u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 2, got {}", tag)));
        }
        let descriptor_id = decoder.read_u32_be()?;
        let start_unix_nano = decoder.read_u64_be()?;
        let ts_unix_nano = decoder.read_u64_be()?;
        let flags = decoder.read_u32_be()?;
        let length = decoder.read_u16_be()? as usize;
        let mut attributes = Vec::with_capacity(length);
        for _ in 0..length {
            let item = LabelPair::decode_with_decoder(decoder)?;
            attributes.push(item);
        }
        let length = decoder.read_u16_be()? as usize;
        let mut exemplars = Vec::with_capacity(length);
        for _ in 0..length {
            let item = MetricExemplarV2::decode_with_decoder(decoder)?;
            exemplars.push(item);
        }
        let count = decoder.read_u64_be()?;
        let has_sum = decoder.read_byte()?;
        let sum = f64::from_bits(decoder.read_u64_be()?);
        let has_min = decoder.read_byte()?;
        let min = f64::from_bits(decoder.read_u64_be()?);
        let has_max = decoder.read_byte()?;
        let max = f64::from_bits(decoder.read_u64_be()?);
        let length = decoder.read_u32_be()? as usize;
        let mut explicit_bounds = Vec::with_capacity(length);
        for _ in 0..length {
            let item = f64::from_bits(decoder.read_u64_be()?);
            explicit_bounds.push(item);
        }
        let length = decoder.read_u32_be()? as usize;
        let mut bucket_counts = Vec::with_capacity(length);
        for _ in 0..length {
            let item = decoder.read_u64_be()?;
            bucket_counts.push(item);
        }
        Ok(Self {
            tag,
            descriptor_id,
            start_unix_nano,
            ts_unix_nano,
            flags,
            attributes,
            exemplars,
            count,
            has_sum,
            sum,
            has_min,
            min,
            has_max,
            max,
            explicit_bounds,
            bucket_counts,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        HistogramPointV2Input::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        HistogramPointV2Input::from(self.clone()).encode_into(encoder)
    }
    pub fn encode_with_context(&self, ctx: &EncodeContext) -> Result<Vec<u8>> {
        HistogramPointV2Input::from(self.clone()).encode_with_context(ctx)
    }
    pub fn encode_into_with_context(&self, encoder: &mut BitStreamEncoder, ctx: &EncodeContext) -> Result<()> {
        HistogramPointV2Input::from(self.clone()).encode_into_with_context(encoder, ctx)
    }
}

impl From<HistogramPointV2Output> for HistogramPointV2Input {
    fn from(o: HistogramPointV2Output) -> Self {
        Self {
            descriptor_id: o.descriptor_id,
            start_unix_nano: o.start_unix_nano,
            ts_unix_nano: o.ts_unix_nano,
            flags: o.flags,
            attributes: o.attributes,
            exemplars: o.exemplars,
            count: o.count,
            has_sum: o.has_sum,
            sum: o.sum,
            has_min: o.has_min,
            min: o.min,
            has_max: o.has_max,
            max: o.max,
            explicit_bounds: o.explicit_bounds,
            bucket_counts: o.bucket_counts,
        }
    }
}

impl From<HistogramPointV2Input> for HistogramPointV2Output {
    fn from(i: HistogramPointV2Input) -> Self {
        Self {
            tag: 2u8,
            descriptor_id: i.descriptor_id,
            start_unix_nano: i.start_unix_nano,
            ts_unix_nano: i.ts_unix_nano,
            flags: i.flags,
            attributes: i.attributes,
            exemplars: i.exemplars,
            count: i.count,
            has_sum: i.has_sum,
            sum: i.sum,
            has_min: i.has_min,
            min: i.min,
            has_max: i.has_max,
            max: i.max,
            explicit_bounds: i.explicit_bounds,
            bucket_counts: i.bucket_counts,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExponentialHistogramPointV2Input {
    pub descriptor_id: u32,
    pub start_unix_nano: u64,
    pub ts_unix_nano: u64,
    pub flags: u32,
    pub attributes: Vec<LabelPair>,
    pub exemplars: Vec<MetricExemplarV2>,
    pub count: MetricCountV2,
    pub has_sum: u8,
    pub sum: f64,
    pub has_min: u8,
    pub min: f64,
    pub has_max: u8,
    pub max: f64,
    pub scale: i32,
    pub zero_threshold: f64,
    pub zero_count: MetricCountV2,
    pub positive: SparseBucketsV2,
    pub negative: SparseBucketsV2,
    pub custom_bounds: Vec<f64>,
    pub reset_hint: u8,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExponentialHistogramPointV2Output {
    pub tag: u8,
    pub descriptor_id: u32,
    pub start_unix_nano: u64,
    pub ts_unix_nano: u64,
    pub flags: u32,
    pub attributes: Vec<LabelPair>,
    pub exemplars: Vec<MetricExemplarV2>,
    pub count: MetricCountV2,
    pub has_sum: u8,
    pub sum: f64,
    pub has_min: u8,
    pub min: f64,
    pub has_max: u8,
    pub max: f64,
    pub scale: i32,
    pub zero_threshold: f64,
    pub zero_count: MetricCountV2,
    pub positive: SparseBucketsV2,
    pub negative: SparseBucketsV2,
    pub custom_bounds: Vec<f64>,
    pub reset_hint: u8,
}

pub type ExponentialHistogramPointV2 = ExponentialHistogramPointV2Output;

impl ExponentialHistogramPointV2Input {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into_with_context(&mut encoder, &EncodeContext::new())?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        self.encode_into_with_context(encoder, &EncodeContext::new())
    }

    pub fn encode_with_context(&self, ctx: &EncodeContext) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into_with_context(&mut encoder, ctx)?;
        Ok(encoder.finish())
    }

    pub fn encode_into_with_context(&self, encoder: &mut BitStreamEncoder, ctx: &EncodeContext) -> Result<()> {

        // Build parent context for nested struct encoding
        let mut parent_fields: HashMap<std::string::String, FieldValue> = HashMap::new();
        parent_fields.insert("descriptor_id".to_string(), FieldValue::U32(self.descriptor_id));
        parent_fields.insert("start_unix_nano".to_string(), FieldValue::U64(self.start_unix_nano));
        parent_fields.insert("ts_unix_nano".to_string(), FieldValue::U64(self.ts_unix_nano));
        parent_fields.insert("flags".to_string(), FieldValue::U32(self.flags));
        // Collect items with sub-field values for typed array 'attributes'
        {
            let mut items_data: Vec<(std::string::String, HashMap<std::string::String, FieldValue>)> = Vec::new();
            for item in &self.attributes {
                let item_bytes = item.encode()?;
                let mut item_fields: HashMap<std::string::String, FieldValue> = HashMap::new();
                item_fields.insert("_encoded_size".to_string(), FieldValue::U64(item_bytes.len() as u64));
                item_fields.insert("key".to_string(), FieldValue::String(item.key.clone()));
                item_fields.insert("value".to_string(), FieldValue::String(item.value.clone()));
                items_data.push(("LabelPair".to_string(), item_fields));
            }
            parent_fields.insert("attributes".to_string(), FieldValue::Items(items_data));
        }
        // Collect items with sub-field values for typed array 'exemplars'
        {
            let mut items_data: Vec<(std::string::String, HashMap<std::string::String, FieldValue>)> = Vec::new();
            for _ in &self.exemplars {
                let item_bytes = Vec::<u8>::new(); // Items need context, skip encoding for now
                let mut item_fields: HashMap<std::string::String, FieldValue> = HashMap::new();
                item_fields.insert("_encoded_size".to_string(), FieldValue::U64(item_bytes.len() as u64));
                items_data.push(("MetricExemplarV2".to_string(), item_fields));
            }
            parent_fields.insert("exemplars".to_string(), FieldValue::Items(items_data));
        }
        parent_fields.insert("has_sum".to_string(), FieldValue::U8(self.has_sum));
        parent_fields.insert("sum".to_string(), FieldValue::F64(self.sum));
        parent_fields.insert("has_min".to_string(), FieldValue::U8(self.has_min));
        parent_fields.insert("min".to_string(), FieldValue::F64(self.min));
        parent_fields.insert("has_max".to_string(), FieldValue::U8(self.has_max));
        parent_fields.insert("max".to_string(), FieldValue::F64(self.max));
        parent_fields.insert("scale".to_string(), FieldValue::I32(self.scale));
        parent_fields.insert("zero_threshold".to_string(), FieldValue::F64(self.zero_threshold));
        parent_fields.insert("reset_hint".to_string(), FieldValue::U8(self.reset_hint));
        let child_ctx = ctx.extend_with_parent(parent_fields);
        let _ = &child_ctx; // Used by nested struct encoding
        encoder.write_byte(3);
        encoder.write_u32_be(self.descriptor_id);
        encoder.write_u64_be(self.start_unix_nano);
        encoder.write_u64_be(self.ts_unix_nano);
        encoder.write_u32_be(self.flags);
        encoder.write_u16_be(self.attributes.len() as u16);
        for item in &self.attributes {
            item.encode_into(encoder)?;
        }
        encoder.write_u16_be(self.exemplars.len() as u16);
        for item in &self.exemplars {
            item.encode_into_with_context(encoder, &child_ctx)?;
        }
        // Encode nested struct count
        self.count.encode_into(encoder)?;
        encoder.write_byte(self.has_sum);
        encoder.write_u64_be((self.sum).to_bits());
        encoder.write_byte(self.has_min);
        encoder.write_u64_be((self.min).to_bits());
        encoder.write_byte(self.has_max);
        encoder.write_u64_be((self.max).to_bits());
        encoder.write_u32_be(self.scale as u32);
        encoder.write_u64_be((self.zero_threshold).to_bits());
        // Encode nested struct zero_count
        self.zero_count.encode_into(encoder)?;
        // Encode nested struct positive
        self.positive.encode_into(encoder)?;
        // Encode nested struct negative
        self.negative.encode_into(encoder)?;
        encoder.write_u32_be(self.custom_bounds.len() as u32);
        for item in &self.custom_bounds {
            encoder.write_u64_be((*item).to_bits());
        }
        encoder.write_byte(self.reset_hint);
        Ok(())
    }

}

impl ExponentialHistogramPointV2Output {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 3u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 3, got {}", tag)));
        }
        let descriptor_id = decoder.read_u32_be()?;
        let start_unix_nano = decoder.read_u64_be()?;
        let ts_unix_nano = decoder.read_u64_be()?;
        let flags = decoder.read_u32_be()?;
        let length = decoder.read_u16_be()? as usize;
        let mut attributes = Vec::with_capacity(length);
        for _ in 0..length {
            let item = LabelPair::decode_with_decoder(decoder)?;
            attributes.push(item);
        }
        let length = decoder.read_u16_be()? as usize;
        let mut exemplars = Vec::with_capacity(length);
        for _ in 0..length {
            let item = MetricExemplarV2::decode_with_decoder(decoder)?;
            exemplars.push(item);
        }
        let count = MetricCountV2::decode_with_decoder(decoder)?;
        let has_sum = decoder.read_byte()?;
        let sum = f64::from_bits(decoder.read_u64_be()?);
        let has_min = decoder.read_byte()?;
        let min = f64::from_bits(decoder.read_u64_be()?);
        let has_max = decoder.read_byte()?;
        let max = f64::from_bits(decoder.read_u64_be()?);
        let scale = decoder.read_u32_be()? as i32;
        let zero_threshold = f64::from_bits(decoder.read_u64_be()?);
        let zero_count = MetricCountV2::decode_with_decoder(decoder)?;
        let positive = SparseBucketsV2::decode_with_decoder(decoder)?;
        let negative = SparseBucketsV2::decode_with_decoder(decoder)?;
        let length = decoder.read_u32_be()? as usize;
        let mut custom_bounds = Vec::with_capacity(length);
        for _ in 0..length {
            let item = f64::from_bits(decoder.read_u64_be()?);
            custom_bounds.push(item);
        }
        let reset_hint = decoder.read_byte()?;
        Ok(Self {
            tag,
            descriptor_id,
            start_unix_nano,
            ts_unix_nano,
            flags,
            attributes,
            exemplars,
            count,
            has_sum,
            sum,
            has_min,
            min,
            has_max,
            max,
            scale,
            zero_threshold,
            zero_count,
            positive,
            negative,
            custom_bounds,
            reset_hint,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        ExponentialHistogramPointV2Input::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        ExponentialHistogramPointV2Input::from(self.clone()).encode_into(encoder)
    }
    pub fn encode_with_context(&self, ctx: &EncodeContext) -> Result<Vec<u8>> {
        ExponentialHistogramPointV2Input::from(self.clone()).encode_with_context(ctx)
    }
    pub fn encode_into_with_context(&self, encoder: &mut BitStreamEncoder, ctx: &EncodeContext) -> Result<()> {
        ExponentialHistogramPointV2Input::from(self.clone()).encode_into_with_context(encoder, ctx)
    }
}

impl From<ExponentialHistogramPointV2Output> for ExponentialHistogramPointV2Input {
    fn from(o: ExponentialHistogramPointV2Output) -> Self {
        Self {
            descriptor_id: o.descriptor_id,
            start_unix_nano: o.start_unix_nano,
            ts_unix_nano: o.ts_unix_nano,
            flags: o.flags,
            attributes: o.attributes,
            exemplars: o.exemplars,
            count: o.count,
            has_sum: o.has_sum,
            sum: o.sum,
            has_min: o.has_min,
            min: o.min,
            has_max: o.has_max,
            max: o.max,
            scale: o.scale,
            zero_threshold: o.zero_threshold,
            zero_count: o.zero_count,
            positive: o.positive,
            negative: o.negative,
            custom_bounds: o.custom_bounds,
            reset_hint: o.reset_hint,
        }
    }
}

impl From<ExponentialHistogramPointV2Input> for ExponentialHistogramPointV2Output {
    fn from(i: ExponentialHistogramPointV2Input) -> Self {
        Self {
            tag: 3u8,
            descriptor_id: i.descriptor_id,
            start_unix_nano: i.start_unix_nano,
            ts_unix_nano: i.ts_unix_nano,
            flags: i.flags,
            attributes: i.attributes,
            exemplars: i.exemplars,
            count: i.count,
            has_sum: i.has_sum,
            sum: i.sum,
            has_min: i.has_min,
            min: i.min,
            has_max: i.has_max,
            max: i.max,
            scale: i.scale,
            zero_threshold: i.zero_threshold,
            zero_count: i.zero_count,
            positive: i.positive,
            negative: i.negative,
            custom_bounds: i.custom_bounds,
            reset_hint: i.reset_hint,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SummaryPointV2Input {
    pub descriptor_id: u32,
    pub start_unix_nano: u64,
    pub ts_unix_nano: u64,
    pub flags: u32,
    pub attributes: Vec<LabelPair>,
    pub exemplars: Vec<MetricExemplarV2>,
    pub count: u64,
    pub sum: f64,
    pub quantiles: Vec<QuantileValueV2>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SummaryPointV2Output {
    pub tag: u8,
    pub descriptor_id: u32,
    pub start_unix_nano: u64,
    pub ts_unix_nano: u64,
    pub flags: u32,
    pub attributes: Vec<LabelPair>,
    pub exemplars: Vec<MetricExemplarV2>,
    pub count: u64,
    pub sum: f64,
    pub quantiles: Vec<QuantileValueV2>,
}

pub type SummaryPointV2 = SummaryPointV2Output;

impl SummaryPointV2Input {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into_with_context(&mut encoder, &EncodeContext::new())?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        self.encode_into_with_context(encoder, &EncodeContext::new())
    }

    pub fn encode_with_context(&self, ctx: &EncodeContext) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into_with_context(&mut encoder, ctx)?;
        Ok(encoder.finish())
    }

    pub fn encode_into_with_context(&self, encoder: &mut BitStreamEncoder, ctx: &EncodeContext) -> Result<()> {

        // Build parent context for nested struct encoding
        let mut parent_fields: HashMap<std::string::String, FieldValue> = HashMap::new();
        parent_fields.insert("descriptor_id".to_string(), FieldValue::U32(self.descriptor_id));
        parent_fields.insert("start_unix_nano".to_string(), FieldValue::U64(self.start_unix_nano));
        parent_fields.insert("ts_unix_nano".to_string(), FieldValue::U64(self.ts_unix_nano));
        parent_fields.insert("flags".to_string(), FieldValue::U32(self.flags));
        // Collect items with sub-field values for typed array 'attributes'
        {
            let mut items_data: Vec<(std::string::String, HashMap<std::string::String, FieldValue>)> = Vec::new();
            for item in &self.attributes {
                let item_bytes = item.encode()?;
                let mut item_fields: HashMap<std::string::String, FieldValue> = HashMap::new();
                item_fields.insert("_encoded_size".to_string(), FieldValue::U64(item_bytes.len() as u64));
                item_fields.insert("key".to_string(), FieldValue::String(item.key.clone()));
                item_fields.insert("value".to_string(), FieldValue::String(item.value.clone()));
                items_data.push(("LabelPair".to_string(), item_fields));
            }
            parent_fields.insert("attributes".to_string(), FieldValue::Items(items_data));
        }
        // Collect items with sub-field values for typed array 'exemplars'
        {
            let mut items_data: Vec<(std::string::String, HashMap<std::string::String, FieldValue>)> = Vec::new();
            for _ in &self.exemplars {
                let item_bytes = Vec::<u8>::new(); // Items need context, skip encoding for now
                let mut item_fields: HashMap<std::string::String, FieldValue> = HashMap::new();
                item_fields.insert("_encoded_size".to_string(), FieldValue::U64(item_bytes.len() as u64));
                items_data.push(("MetricExemplarV2".to_string(), item_fields));
            }
            parent_fields.insert("exemplars".to_string(), FieldValue::Items(items_data));
        }
        parent_fields.insert("count".to_string(), FieldValue::U64(self.count));
        parent_fields.insert("sum".to_string(), FieldValue::F64(self.sum));
        // Collect items with sub-field values for typed array 'quantiles'
        {
            let mut items_data: Vec<(std::string::String, HashMap<std::string::String, FieldValue>)> = Vec::new();
            for item in &self.quantiles {
                let item_bytes = item.encode()?;
                let mut item_fields: HashMap<std::string::String, FieldValue> = HashMap::new();
                item_fields.insert("_encoded_size".to_string(), FieldValue::U64(item_bytes.len() as u64));
                item_fields.insert("quantile".to_string(), FieldValue::F64(item.quantile));
                item_fields.insert("value".to_string(), FieldValue::F64(item.value));
                items_data.push(("QuantileValueV2".to_string(), item_fields));
            }
            parent_fields.insert("quantiles".to_string(), FieldValue::Items(items_data));
        }
        let child_ctx = ctx.extend_with_parent(parent_fields);
        let _ = &child_ctx; // Used by nested struct encoding
        encoder.write_byte(4);
        encoder.write_u32_be(self.descriptor_id);
        encoder.write_u64_be(self.start_unix_nano);
        encoder.write_u64_be(self.ts_unix_nano);
        encoder.write_u32_be(self.flags);
        encoder.write_u16_be(self.attributes.len() as u16);
        for item in &self.attributes {
            item.encode_into(encoder)?;
        }
        encoder.write_u16_be(self.exemplars.len() as u16);
        for item in &self.exemplars {
            item.encode_into_with_context(encoder, &child_ctx)?;
        }
        encoder.write_u64_be(self.count);
        encoder.write_u64_be((self.sum).to_bits());
        encoder.write_u16_be(self.quantiles.len() as u16);
        for item in &self.quantiles {
            item.encode_into(encoder)?;
        }
        Ok(())
    }

}

impl SummaryPointV2Output {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 4u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 4, got {}", tag)));
        }
        let descriptor_id = decoder.read_u32_be()?;
        let start_unix_nano = decoder.read_u64_be()?;
        let ts_unix_nano = decoder.read_u64_be()?;
        let flags = decoder.read_u32_be()?;
        let length = decoder.read_u16_be()? as usize;
        let mut attributes = Vec::with_capacity(length);
        for _ in 0..length {
            let item = LabelPair::decode_with_decoder(decoder)?;
            attributes.push(item);
        }
        let length = decoder.read_u16_be()? as usize;
        let mut exemplars = Vec::with_capacity(length);
        for _ in 0..length {
            let item = MetricExemplarV2::decode_with_decoder(decoder)?;
            exemplars.push(item);
        }
        let count = decoder.read_u64_be()?;
        let sum = f64::from_bits(decoder.read_u64_be()?);
        let length = decoder.read_u16_be()? as usize;
        let mut quantiles = Vec::with_capacity(length);
        for _ in 0..length {
            let item = QuantileValueV2::decode_with_decoder(decoder)?;
            quantiles.push(item);
        }
        Ok(Self {
            tag,
            descriptor_id,
            start_unix_nano,
            ts_unix_nano,
            flags,
            attributes,
            exemplars,
            count,
            sum,
            quantiles,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        SummaryPointV2Input::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        SummaryPointV2Input::from(self.clone()).encode_into(encoder)
    }
    pub fn encode_with_context(&self, ctx: &EncodeContext) -> Result<Vec<u8>> {
        SummaryPointV2Input::from(self.clone()).encode_with_context(ctx)
    }
    pub fn encode_into_with_context(&self, encoder: &mut BitStreamEncoder, ctx: &EncodeContext) -> Result<()> {
        SummaryPointV2Input::from(self.clone()).encode_into_with_context(encoder, ctx)
    }
}

impl From<SummaryPointV2Output> for SummaryPointV2Input {
    fn from(o: SummaryPointV2Output) -> Self {
        Self {
            descriptor_id: o.descriptor_id,
            start_unix_nano: o.start_unix_nano,
            ts_unix_nano: o.ts_unix_nano,
            flags: o.flags,
            attributes: o.attributes,
            exemplars: o.exemplars,
            count: o.count,
            sum: o.sum,
            quantiles: o.quantiles,
        }
    }
}

impl From<SummaryPointV2Input> for SummaryPointV2Output {
    fn from(i: SummaryPointV2Input) -> Self {
        Self {
            tag: 4u8,
            descriptor_id: i.descriptor_id,
            start_unix_nano: i.start_unix_nano,
            ts_unix_nano: i.ts_unix_nano,
            flags: i.flags,
            attributes: i.attributes,
            exemplars: i.exemplars,
            count: i.count,
            sum: i.sum,
            quantiles: i.quantiles,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricNumberV2 {
    pub value: MetricNumberV2Value,
}

impl MetricNumberV2 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        self.value.encode_into(encoder)?;
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let value = MetricNumberV2Value::decode_with_decoder(decoder)?;
        Ok(Self {
            value,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricCountV2 {
    pub value: MetricCountV2Value,
}

impl MetricCountV2 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        self.value.encode_into(encoder)?;
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let value = MetricCountV2Value::decode_with_decoder(decoder)?;
        Ok(Self {
            value,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct IntegerValueV2Input {
    pub value: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IntegerValueV2Output {
    pub tag: u8,
    pub value: i64,
}

pub type IntegerValueV2 = IntegerValueV2Output;

impl IntegerValueV2Input {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(1);
        encoder.write_u64_be(self.value as u64);
        Ok(())
    }

}

impl IntegerValueV2Output {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 1u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 1, got {}", tag)));
        }
        let value = decoder.read_u64_be()? as i64;
        Ok(Self {
            tag,
            value,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        IntegerValueV2Input::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        IntegerValueV2Input::from(self.clone()).encode_into(encoder)
    }
}

impl From<IntegerValueV2Output> for IntegerValueV2Input {
    fn from(o: IntegerValueV2Output) -> Self {
        Self {
            value: o.value,
        }
    }
}

impl From<IntegerValueV2Input> for IntegerValueV2Output {
    fn from(i: IntegerValueV2Input) -> Self {
        Self {
            tag: 1u8,
            value: i.value,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DoubleValueV2Input {
    pub value: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DoubleValueV2Output {
    pub tag: u8,
    pub value: f64,
}

pub type DoubleValueV2 = DoubleValueV2Output;

impl DoubleValueV2Input {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(2);
        encoder.write_u64_be((self.value).to_bits());
        Ok(())
    }

}

impl DoubleValueV2Output {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 2u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 2, got {}", tag)));
        }
        let value = f64::from_bits(decoder.read_u64_be()?);
        Ok(Self {
            tag,
            value,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        DoubleValueV2Input::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        DoubleValueV2Input::from(self.clone()).encode_into(encoder)
    }
}

impl From<DoubleValueV2Output> for DoubleValueV2Input {
    fn from(o: DoubleValueV2Output) -> Self {
        Self {
            value: o.value,
        }
    }
}

impl From<DoubleValueV2Input> for DoubleValueV2Output {
    fn from(i: DoubleValueV2Input) -> Self {
        Self {
            tag: 2u8,
            value: i.value,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct IntegerCountV2Input {
    pub value: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IntegerCountV2Output {
    pub tag: u8,
    pub value: u64,
}

pub type IntegerCountV2 = IntegerCountV2Output;

impl IntegerCountV2Input {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(1);
        encoder.write_u64_be(self.value);
        Ok(())
    }

}

impl IntegerCountV2Output {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 1u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 1, got {}", tag)));
        }
        let value = decoder.read_u64_be()?;
        Ok(Self {
            tag,
            value,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        IntegerCountV2Input::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        IntegerCountV2Input::from(self.clone()).encode_into(encoder)
    }
}

impl From<IntegerCountV2Output> for IntegerCountV2Input {
    fn from(o: IntegerCountV2Output) -> Self {
        Self {
            value: o.value,
        }
    }
}

impl From<IntegerCountV2Input> for IntegerCountV2Output {
    fn from(i: IntegerCountV2Input) -> Self {
        Self {
            tag: 1u8,
            value: i.value,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FloatCountV2Input {
    pub value: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FloatCountV2Output {
    pub tag: u8,
    pub value: f64,
}

pub type FloatCountV2 = FloatCountV2Output;

impl FloatCountV2Input {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(2);
        encoder.write_u64_be((self.value).to_bits());
        Ok(())
    }

}

impl FloatCountV2Output {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 2u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 2, got {}", tag)));
        }
        let value = f64::from_bits(decoder.read_u64_be()?);
        Ok(Self {
            tag,
            value,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        FloatCountV2Input::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        FloatCountV2Input::from(self.clone()).encode_into(encoder)
    }
}

impl From<FloatCountV2Output> for FloatCountV2Input {
    fn from(o: FloatCountV2Output) -> Self {
        Self {
            value: o.value,
        }
    }
}

impl From<FloatCountV2Input> for FloatCountV2Output {
    fn from(i: FloatCountV2Input) -> Self {
        Self {
            tag: 2u8,
            value: i.value,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SparseBucketsV2 {
    pub offset: i32,
    pub deltas: Vec<i32>,
    pub counts: Vec<MetricCountV2>,
}

impl SparseBucketsV2 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_u32_be(self.offset as u32);
        encoder.write_u32_be(self.deltas.len() as u32);
        for item in &self.deltas {
            encoder.write_u32_be(*item as u32);
        }
        encoder.write_u32_be(self.counts.len() as u32);
        for item in &self.counts {
            item.encode_into(encoder)?;
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let offset = decoder.read_u32_be()? as i32;
        let length = decoder.read_u32_be()? as usize;
        let mut deltas = Vec::with_capacity(length);
        for _ in 0..length {
            let item = decoder.read_u32_be()? as i32;
            deltas.push(item);
        }
        let length = decoder.read_u32_be()? as usize;
        let mut counts = Vec::with_capacity(length);
        for _ in 0..length {
            let item = MetricCountV2::decode_with_decoder(decoder)?;
            counts.push(item);
        }
        Ok(Self {
            offset,
            deltas,
            counts,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct QuantileValueV2 {
    pub quantile: f64,
    pub value: f64,
}

impl QuantileValueV2 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_u64_be((self.quantile).to_bits());
        encoder.write_u64_be((self.value).to_bits());
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let quantile = f64::from_bits(decoder.read_u64_be()?);
        let value = f64::from_bits(decoder.read_u64_be()?);
        Ok(Self {
            quantile,
            value,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricExemplarV2 {
    pub ts_unix_nano: u64,
    pub number: MetricNumberV2,
    pub filtered_attrs: Vec<LabelPair>,
    pub trace_id: Vec<u8>,
    pub span_id: Vec<u8>,
}

impl MetricExemplarV2 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into_with_context(&mut encoder, &EncodeContext::new())?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        self.encode_into_with_context(encoder, &EncodeContext::new())
    }

    pub fn encode_with_context(&self, ctx: &EncodeContext) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into_with_context(&mut encoder, ctx)?;
        Ok(encoder.finish())
    }

    pub fn encode_into_with_context(&self, encoder: &mut BitStreamEncoder, ctx: &EncodeContext) -> Result<()> {

        // Build parent context for nested struct encoding
        let mut parent_fields: HashMap<std::string::String, FieldValue> = HashMap::new();
        parent_fields.insert("ts_unix_nano".to_string(), FieldValue::U64(self.ts_unix_nano));
        // Collect items with sub-field values for typed array 'filtered_attrs'
        {
            let mut items_data: Vec<(std::string::String, HashMap<std::string::String, FieldValue>)> = Vec::new();
            for item in &self.filtered_attrs {
                let item_bytes = item.encode()?;
                let mut item_fields: HashMap<std::string::String, FieldValue> = HashMap::new();
                item_fields.insert("_encoded_size".to_string(), FieldValue::U64(item_bytes.len() as u64));
                item_fields.insert("key".to_string(), FieldValue::String(item.key.clone()));
                item_fields.insert("value".to_string(), FieldValue::String(item.value.clone()));
                items_data.push(("LabelPair".to_string(), item_fields));
            }
            parent_fields.insert("filtered_attrs".to_string(), FieldValue::Items(items_data));
        }
        let child_ctx = ctx.extend_with_parent(parent_fields);
        let _ = &child_ctx; // Used by nested struct encoding
        encoder.write_u64_be(self.ts_unix_nano);
        // Encode nested struct number
        self.number.encode_into(encoder)?;
        encoder.write_u16_be(self.filtered_attrs.len() as u16);
        for item in &self.filtered_attrs {
            item.encode_into(encoder)?;
        }
        for item in &self.trace_id {
            encoder.write_byte(*item);
        }
        for item in &self.span_id {
            encoder.write_byte(*item);
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let ts_unix_nano = decoder.read_u64_be()?;
        let number = MetricNumberV2::decode_with_decoder(decoder)?;
        let length = decoder.read_u16_be()? as usize;
        let mut filtered_attrs = Vec::with_capacity(length);
        for _ in 0..length {
            let item = LabelPair::decode_with_decoder(decoder)?;
            filtered_attrs.push(item);
        }
        let mut trace_id = Vec::with_capacity(16);
        for _ in 0..16 {
            let item = decoder.read_byte()?;
            trace_id.push(item);
        }
        let mut span_id = Vec::with_capacity(8);
        for _ in 0..8 {
            let item = decoder.read_byte()?;
            span_id.push(item);
        }
        Ok(Self {
            ts_unix_nano,
            number,
            filtered_attrs,
            trace_id,
            span_id,
        })
    }
}
