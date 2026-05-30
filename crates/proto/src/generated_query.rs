#![allow(non_camel_case_types)]
#![allow(dead_code)]
#![allow(unreachable_code)]

#[allow(unused_imports)]
use binschema_runtime::{BitStreamEncoder, BitStreamDecoder, Endianness, BitOrder, Result, BinSchemaError, EncodeContext, FieldValue};
#[allow(unused_imports)]
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum QueryFrameMsg {
    QueryRequest(QueryRequestOutput),
    SchemaMsg(SchemaMsgOutput),
    BatchMsg(BatchMsgOutput),
    EndOfStream(EndOfStreamOutput),
    StreamError(StreamErrorOutput),
}

impl QueryFrameMsg {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        match self {
            QueryFrameMsg::QueryRequest(v) => {
                encoder.write_uint8(1);
                encoder.write_uint8(v.signal);
                encoder.write_uint16(v.matchers.len() as u16, Endianness::BigEndian);
                for item in &v.matchers {
                    item.encode_into(encoder)?;
                }
                encoder.write_uint8(v.ts_min_present);
                encoder.write_uint64(v.ts_min, Endianness::BigEndian);
                encoder.write_uint8(v.ts_max_present);
                encoder.write_uint64(v.ts_max, Endianness::BigEndian);
                encoder.write_uint32(v.sql.len() as u32, Endianness::BigEndian);
                let string_bytes: &[u8] = v.sql.as_bytes();
                for &b in string_bytes.iter() {
                    encoder.write_uint8(b);
                }
                encoder.write_uint64(v.limit, Endianness::BigEndian);
                encoder.write_uint16(v.request_id.chars().count() as u16, Endianness::BigEndian);
                let string_bytes: Vec<u8> = v.request_id.chars().map(|c| c as u8).collect();
                for &b in string_bytes.iter() {
                    encoder.write_uint8(b);
                }
                encoder.write_uint16(v.trace_id.len() as u16, Endianness::BigEndian);
                for item in &v.trace_id {
                    encoder.write_uint8(*item);
                }
                encoder.write_uint32(v.body_contains.len() as u32, Endianness::BigEndian);
                let string_bytes: &[u8] = v.body_contains.as_bytes();
                for &b in string_bytes.iter() {
                    encoder.write_uint8(b);
                }
            }
            QueryFrameMsg::SchemaMsg(v) => {
                encoder.write_uint8(16);
                encoder.write_uint32(v.ipc_bytes.len() as u32, Endianness::BigEndian);
                for item in &v.ipc_bytes {
                    encoder.write_uint8(*item);
                }
            }
            QueryFrameMsg::BatchMsg(v) => {
                encoder.write_uint8(17);
                encoder.write_uint32(v.ipc_bytes.len() as u32, Endianness::BigEndian);
                for item in &v.ipc_bytes {
                    encoder.write_uint8(*item);
                }
            }
            QueryFrameMsg::EndOfStream(v) => {
                encoder.write_uint8(31);
                encoder.write_uint64(v.total_rows, Endianness::BigEndian);
            }
            QueryFrameMsg::StreamError(v) => {
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
            QueryFrameMsg::QueryRequest(_) => "QueryRequest",
            QueryFrameMsg::SchemaMsg(_) => "SchemaMsg",
            QueryFrameMsg::BatchMsg(_) => "BatchMsg",
            QueryFrameMsg::EndOfStream(_) => "EndOfStream",
            QueryFrameMsg::StreamError(_) => "StreamError",
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        // Union type - try each variant in order until one succeeds
        let start_pos = decoder.position();
        if let Ok(v) = QueryRequestOutput::decode_with_decoder(decoder) {
            return Ok(QueryFrameMsg::QueryRequest(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = SchemaMsgOutput::decode_with_decoder(decoder) {
            return Ok(QueryFrameMsg::SchemaMsg(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = BatchMsgOutput::decode_with_decoder(decoder) {
            return Ok(QueryFrameMsg::BatchMsg(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = EndOfStreamOutput::decode_with_decoder(decoder) {
            return Ok(QueryFrameMsg::EndOfStream(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = StreamErrorOutput::decode_with_decoder(decoder) {
            return Ok(QueryFrameMsg::StreamError(v));
        }
        Err(binschema_runtime::BinSchemaError::InvalidVariant("no variant matched the input bytes".to_string()))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryFrame {
    pub msg: QueryFrameMsg,
}

impl QueryFrame {
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
        let msg = QueryFrameMsg::decode_with_decoder(decoder)?;
        Ok(Self {
            msg,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryRequestInput {
    pub signal: u8,
    pub matchers: Vec<Matcher>,
    pub ts_min_present: u8,
    pub ts_min: u64,
    pub ts_max_present: u8,
    pub ts_max: u64,
    pub sql: std::string::String,
    pub limit: u64,
    pub request_id: std::string::String,
    pub trace_id: Vec<u8>,
    pub body_contains: std::string::String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryRequestOutput {
    pub tag: u8,
    pub signal: u8,
    pub matchers: Vec<Matcher>,
    pub ts_min_present: u8,
    pub ts_min: u64,
    pub ts_max_present: u8,
    pub ts_max: u64,
    pub sql: std::string::String,
    pub limit: u64,
    pub request_id: std::string::String,
    pub trace_id: Vec<u8>,
    pub body_contains: std::string::String,
}

pub type QueryRequest = QueryRequestOutput;

impl QueryRequestInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(1);
        encoder.write_byte(self.signal);
        encoder.write_u16_be(self.matchers.len() as u16);
        for item in &self.matchers {
            item.encode_into(encoder)?;
        }
        encoder.write_byte(self.ts_min_present);
        encoder.write_u64_be(self.ts_min);
        encoder.write_byte(self.ts_max_present);
        encoder.write_u64_be(self.ts_max);
        encoder.write_u32_be(self.sql.len() as u32);
        let string_bytes: &[u8] = self.sql.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        encoder.write_u64_be(self.limit);
        encoder.write_u16_be(self.request_id.chars().count() as u16);
        let string_bytes: Vec<u8> = self.request_id.chars().map(|c| c as u8).collect();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        encoder.write_u16_be(self.trace_id.len() as u16);
        for item in &self.trace_id {
            encoder.write_byte(*item);
        }
        encoder.write_u32_be(self.body_contains.len() as u32);
        let string_bytes: &[u8] = self.body_contains.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        Ok(())
    }

}

impl QueryRequestOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 1u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 1, got {}", tag)));
        }
        let signal = decoder.read_byte()?;
        let length = decoder.read_u16_be()? as usize;
        let mut matchers = Vec::with_capacity(length);
        for _ in 0..length {
            let item = Matcher::decode_with_decoder(decoder)?;
            matchers.push(item);
        }
        let ts_min_present = decoder.read_byte()?;
        let ts_min = decoder.read_u64_be()?;
        let ts_max_present = decoder.read_byte()?;
        let ts_max = decoder.read_u64_be()?;
        let length = decoder.read_u32_be()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let sql = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        let limit = decoder.read_u64_be()?;
        let length = decoder.read_u16_be()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let request_id: std::string::String = bytes.iter().map(|&b| b as char).collect();
        let length = decoder.read_u16_be()? as usize;
        let mut trace_id = Vec::with_capacity(length);
        for _ in 0..length {
            let item = decoder.read_byte()?;
            trace_id.push(item);
        }
        let length = decoder.read_u32_be()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let body_contains = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        Ok(Self {
            tag,
            signal,
            matchers,
            ts_min_present,
            ts_min,
            ts_max_present,
            ts_max,
            sql,
            limit,
            request_id,
            trace_id,
            body_contains,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        QueryRequestInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        QueryRequestInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<QueryRequestOutput> for QueryRequestInput {
    fn from(o: QueryRequestOutput) -> Self {
        Self {
            signal: o.signal,
            matchers: o.matchers,
            ts_min_present: o.ts_min_present,
            ts_min: o.ts_min,
            ts_max_present: o.ts_max_present,
            ts_max: o.ts_max,
            sql: o.sql,
            limit: o.limit,
            request_id: o.request_id,
            trace_id: o.trace_id,
            body_contains: o.body_contains,
        }
    }
}

impl From<QueryRequestInput> for QueryRequestOutput {
    fn from(i: QueryRequestInput) -> Self {
        Self {
            tag: 1u8,
            signal: i.signal,
            matchers: i.matchers,
            ts_min_present: i.ts_min_present,
            ts_min: i.ts_min,
            ts_max_present: i.ts_max_present,
            ts_max: i.ts_max,
            sql: i.sql,
            limit: i.limit,
            request_id: i.request_id,
            trace_id: i.trace_id,
            body_contains: i.body_contains,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Matcher {
    pub name: std::string::String,
    pub value: std::string::String,
}

impl Matcher {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_u16_be(self.name.len() as u16);
        let string_bytes: &[u8] = self.name.as_bytes();
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
        let length = decoder.read_u16_be()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let name = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        let length = decoder.read_u16_be()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let value = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        Ok(Self {
            name,
            value,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaMsgInput {
    pub ipc_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaMsgOutput {
    pub tag: u8,
    pub ipc_bytes: Vec<u8>,
}

pub type SchemaMsg = SchemaMsgOutput;

impl SchemaMsgInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(16);
        encoder.write_u32_be(self.ipc_bytes.len() as u32);
        for item in &self.ipc_bytes {
            encoder.write_byte(*item);
        }
        Ok(())
    }

}

impl SchemaMsgOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 16u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 16, got {}", tag)));
        }
        let length = decoder.read_u32_be()? as usize;
        let mut ipc_bytes = Vec::with_capacity(length);
        for _ in 0..length {
            let item = decoder.read_byte()?;
            ipc_bytes.push(item);
        }
        Ok(Self {
            tag,
            ipc_bytes,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        SchemaMsgInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        SchemaMsgInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<SchemaMsgOutput> for SchemaMsgInput {
    fn from(o: SchemaMsgOutput) -> Self {
        Self {
            ipc_bytes: o.ipc_bytes,
        }
    }
}

impl From<SchemaMsgInput> for SchemaMsgOutput {
    fn from(i: SchemaMsgInput) -> Self {
        Self {
            tag: 16u8,
            ipc_bytes: i.ipc_bytes,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BatchMsgInput {
    pub ipc_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BatchMsgOutput {
    pub tag: u8,
    pub ipc_bytes: Vec<u8>,
}

pub type BatchMsg = BatchMsgOutput;

impl BatchMsgInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(17);
        encoder.write_u32_be(self.ipc_bytes.len() as u32);
        for item in &self.ipc_bytes {
            encoder.write_byte(*item);
        }
        Ok(())
    }

}

impl BatchMsgOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 17u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 17, got {}", tag)));
        }
        let length = decoder.read_u32_be()? as usize;
        let mut ipc_bytes = Vec::with_capacity(length);
        for _ in 0..length {
            let item = decoder.read_byte()?;
            ipc_bytes.push(item);
        }
        Ok(Self {
            tag,
            ipc_bytes,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        BatchMsgInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        BatchMsgInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<BatchMsgOutput> for BatchMsgInput {
    fn from(o: BatchMsgOutput) -> Self {
        Self {
            ipc_bytes: o.ipc_bytes,
        }
    }
}

impl From<BatchMsgInput> for BatchMsgOutput {
    fn from(i: BatchMsgInput) -> Self {
        Self {
            tag: 17u8,
            ipc_bytes: i.ipc_bytes,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct EndOfStreamInput {
    pub total_rows: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EndOfStreamOutput {
    pub tag: u8,
    pub total_rows: u64,
}

pub type EndOfStream = EndOfStreamOutput;

impl EndOfStreamInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(31);
        encoder.write_u64_be(self.total_rows);
        Ok(())
    }

}

impl EndOfStreamOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 31u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 31, got {}", tag)));
        }
        let total_rows = decoder.read_u64_be()?;
        Ok(Self {
            tag,
            total_rows,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        EndOfStreamInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        EndOfStreamInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<EndOfStreamOutput> for EndOfStreamInput {
    fn from(o: EndOfStreamOutput) -> Self {
        Self {
            total_rows: o.total_rows,
        }
    }
}

impl From<EndOfStreamInput> for EndOfStreamOutput {
    fn from(i: EndOfStreamInput) -> Self {
        Self {
            tag: 31u8,
            total_rows: i.total_rows,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamErrorInput {
    pub code: u16,
    pub message: std::string::String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamErrorOutput {
    pub tag: u8,
    pub code: u16,
    pub message: std::string::String,
}

pub type StreamError = StreamErrorOutput;

impl StreamErrorInput {
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

impl StreamErrorOutput {
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
        StreamErrorInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        StreamErrorInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<StreamErrorOutput> for StreamErrorInput {
    fn from(o: StreamErrorOutput) -> Self {
        Self {
            code: o.code,
            message: o.message,
        }
    }
}

impl From<StreamErrorInput> for StreamErrorOutput {
    fn from(i: StreamErrorInput) -> Self {
        Self {
            tag: 240u8,
            code: i.code,
            message: i.message,
        }
    }
}
