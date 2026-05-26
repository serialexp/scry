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
    Ping(PingOutput),
    Pong(PongOutput),
    Goodbye(GoodbyeOutput),
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
            FrameMsg::Ping(_) => "Ping",
            FrameMsg::Pong(_) => "Pong",
            FrameMsg::Goodbye(_) => "Goodbye",
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
        if let Ok(v) = ErrorOutput::decode_with_decoder(decoder) {
            return Ok(FrameMsg::Error(v));
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
