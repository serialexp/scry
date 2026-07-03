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
    LabelNamesRequest(LabelNamesRequestOutput),
    LabelValuesRequest(LabelValuesRequestOutput),
    SchemaMsg(SchemaMsgOutput),
    BatchMsg(BatchMsgOutput),
    EndOfStream(EndOfStreamOutput),
    LabelNamesResponse(LabelNamesResponseOutput),
    LabelValuesResponse(LabelValuesResponseOutput),
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
            QueryFrameMsg::LabelNamesRequest(v) => {
                encoder.write_uint8(2);
                encoder.write_uint8(v.signal);
                encoder.write_uint8(v.ts_min_present);
                encoder.write_uint64(v.ts_min, Endianness::BigEndian);
                encoder.write_uint8(v.ts_max_present);
                encoder.write_uint64(v.ts_max, Endianness::BigEndian);
            }
            QueryFrameMsg::LabelValuesRequest(v) => {
                encoder.write_uint8(3);
                encoder.write_uint8(v.signal);
                encoder.write_uint16(v.label_name.len() as u16, Endianness::BigEndian);
                let string_bytes: &[u8] = v.label_name.as_bytes();
                for &b in string_bytes.iter() {
                    encoder.write_uint8(b);
                }
                encoder.write_uint8(v.ts_min_present);
                encoder.write_uint64(v.ts_min, Endianness::BigEndian);
                encoder.write_uint8(v.ts_max_present);
                encoder.write_uint64(v.ts_max, Endianness::BigEndian);
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
            QueryFrameMsg::LabelNamesResponse(v) => {
                encoder.write_uint8(32);
                encoder.write_uint32(v.names.len() as u32, Endianness::BigEndian);
                for item in &v.names {
                    encoder.write_uint16(item.len() as u16, Endianness::BigEndian);
                    for b in item.as_bytes() {
                        encoder.write_uint8(*b);
                    }
                }
            }
            QueryFrameMsg::LabelValuesResponse(v) => {
                encoder.write_uint8(33);
                encoder.write_uint32(v.values.len() as u32, Endianness::BigEndian);
                for item in &v.values {
                    encoder.write_uint16(item.len() as u16, Endianness::BigEndian);
                    for b in item.as_bytes() {
                        encoder.write_uint8(*b);
                    }
                }
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
            QueryFrameMsg::LabelNamesRequest(_) => "LabelNamesRequest",
            QueryFrameMsg::LabelValuesRequest(_) => "LabelValuesRequest",
            QueryFrameMsg::SchemaMsg(_) => "SchemaMsg",
            QueryFrameMsg::BatchMsg(_) => "BatchMsg",
            QueryFrameMsg::EndOfStream(_) => "EndOfStream",
            QueryFrameMsg::LabelNamesResponse(_) => "LabelNamesResponse",
            QueryFrameMsg::LabelValuesResponse(_) => "LabelValuesResponse",
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
        if let Ok(v) = LabelNamesRequestOutput::decode_with_decoder(decoder) {
            return Ok(QueryFrameMsg::LabelNamesRequest(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = LabelValuesRequestOutput::decode_with_decoder(decoder) {
            return Ok(QueryFrameMsg::LabelValuesRequest(v));
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
        if let Ok(v) = LabelNamesResponseOutput::decode_with_decoder(decoder) {
            return Ok(QueryFrameMsg::LabelNamesResponse(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = LabelValuesResponseOutput::decode_with_decoder(decoder) {
            return Ok(QueryFrameMsg::LabelValuesResponse(v));
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
pub struct LabelNamesRequestInput {
    pub signal: u8,
    pub ts_min_present: u8,
    pub ts_min: u64,
    pub ts_max_present: u8,
    pub ts_max: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LabelNamesRequestOutput {
    pub tag: u8,
    pub signal: u8,
    pub ts_min_present: u8,
    pub ts_min: u64,
    pub ts_max_present: u8,
    pub ts_max: u64,
}

pub type LabelNamesRequest = LabelNamesRequestOutput;

impl LabelNamesRequestInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(2);
        encoder.write_byte(self.signal);
        encoder.write_byte(self.ts_min_present);
        encoder.write_u64_be(self.ts_min);
        encoder.write_byte(self.ts_max_present);
        encoder.write_u64_be(self.ts_max);
        Ok(())
    }

}

impl LabelNamesRequestOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 2u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 2, got {}", tag)));
        }
        let signal = decoder.read_byte()?;
        let ts_min_present = decoder.read_byte()?;
        let ts_min = decoder.read_u64_be()?;
        let ts_max_present = decoder.read_byte()?;
        let ts_max = decoder.read_u64_be()?;
        Ok(Self {
            tag,
            signal,
            ts_min_present,
            ts_min,
            ts_max_present,
            ts_max,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        LabelNamesRequestInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        LabelNamesRequestInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<LabelNamesRequestOutput> for LabelNamesRequestInput {
    fn from(o: LabelNamesRequestOutput) -> Self {
        Self {
            signal: o.signal,
            ts_min_present: o.ts_min_present,
            ts_min: o.ts_min,
            ts_max_present: o.ts_max_present,
            ts_max: o.ts_max,
        }
    }
}

impl From<LabelNamesRequestInput> for LabelNamesRequestOutput {
    fn from(i: LabelNamesRequestInput) -> Self {
        Self {
            tag: 2u8,
            signal: i.signal,
            ts_min_present: i.ts_min_present,
            ts_min: i.ts_min,
            ts_max_present: i.ts_max_present,
            ts_max: i.ts_max,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LabelValuesRequestInput {
    pub signal: u8,
    pub label_name: std::string::String,
    pub ts_min_present: u8,
    pub ts_min: u64,
    pub ts_max_present: u8,
    pub ts_max: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LabelValuesRequestOutput {
    pub tag: u8,
    pub signal: u8,
    pub label_name: std::string::String,
    pub ts_min_present: u8,
    pub ts_min: u64,
    pub ts_max_present: u8,
    pub ts_max: u64,
}

pub type LabelValuesRequest = LabelValuesRequestOutput;

impl LabelValuesRequestInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(3);
        encoder.write_byte(self.signal);
        encoder.write_u16_be(self.label_name.len() as u16);
        let string_bytes: &[u8] = self.label_name.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        encoder.write_byte(self.ts_min_present);
        encoder.write_u64_be(self.ts_min);
        encoder.write_byte(self.ts_max_present);
        encoder.write_u64_be(self.ts_max);
        Ok(())
    }

}

impl LabelValuesRequestOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 3u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 3, got {}", tag)));
        }
        let signal = decoder.read_byte()?;
        let length = decoder.read_u16_be()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let label_name = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        let ts_min_present = decoder.read_byte()?;
        let ts_min = decoder.read_u64_be()?;
        let ts_max_present = decoder.read_byte()?;
        let ts_max = decoder.read_u64_be()?;
        Ok(Self {
            tag,
            signal,
            label_name,
            ts_min_present,
            ts_min,
            ts_max_present,
            ts_max,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        LabelValuesRequestInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        LabelValuesRequestInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<LabelValuesRequestOutput> for LabelValuesRequestInput {
    fn from(o: LabelValuesRequestOutput) -> Self {
        Self {
            signal: o.signal,
            label_name: o.label_name,
            ts_min_present: o.ts_min_present,
            ts_min: o.ts_min,
            ts_max_present: o.ts_max_present,
            ts_max: o.ts_max,
        }
    }
}

impl From<LabelValuesRequestInput> for LabelValuesRequestOutput {
    fn from(i: LabelValuesRequestInput) -> Self {
        Self {
            tag: 3u8,
            signal: i.signal,
            label_name: i.label_name,
            ts_min_present: i.ts_min_present,
            ts_min: i.ts_min,
            ts_max_present: i.ts_max_present,
            ts_max: i.ts_max,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LabelNamesResponseInput {
    pub names: Vec<std::string::String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LabelNamesResponseOutput {
    pub tag: u8,
    pub names: Vec<std::string::String>,
}

pub type LabelNamesResponse = LabelNamesResponseOutput;

impl LabelNamesResponseInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(32);
        encoder.write_u32_be(self.names.len() as u32);
        for item in &self.names {
            encoder.write_u16_be(item.len() as u16);
            for b in item.as_bytes() {
                encoder.write_byte(*b);
            }
        }
        Ok(())
    }

}

impl LabelNamesResponseOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 32u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 32, got {}", tag)));
        }
        let length = decoder.read_u32_be()? as usize;
        let mut names = Vec::with_capacity(length);
        for _ in 0..length {
            let str_len = decoder.read_u16_be()? as usize;
            let str_bytes = decoder.read_bytes_vec(str_len)?;
            let item = std::string::String::from_utf8(str_bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
            names.push(item);
        }
        Ok(Self {
            tag,
            names,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        LabelNamesResponseInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        LabelNamesResponseInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<LabelNamesResponseOutput> for LabelNamesResponseInput {
    fn from(o: LabelNamesResponseOutput) -> Self {
        Self {
            names: o.names,
        }
    }
}

impl From<LabelNamesResponseInput> for LabelNamesResponseOutput {
    fn from(i: LabelNamesResponseInput) -> Self {
        Self {
            tag: 32u8,
            names: i.names,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LabelValuesResponseInput {
    pub values: Vec<std::string::String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LabelValuesResponseOutput {
    pub tag: u8,
    pub values: Vec<std::string::String>,
}

pub type LabelValuesResponse = LabelValuesResponseOutput;

impl LabelValuesResponseInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(33);
        encoder.write_u32_be(self.values.len() as u32);
        for item in &self.values {
            encoder.write_u16_be(item.len() as u16);
            for b in item.as_bytes() {
                encoder.write_byte(*b);
            }
        }
        Ok(())
    }

}

impl LabelValuesResponseOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 33u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 33, got {}", tag)));
        }
        let length = decoder.read_u32_be()? as usize;
        let mut values = Vec::with_capacity(length);
        for _ in 0..length {
            let str_len = decoder.read_u16_be()? as usize;
            let str_bytes = decoder.read_bytes_vec(str_len)?;
            let item = std::string::String::from_utf8(str_bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
            values.push(item);
        }
        Ok(Self {
            tag,
            values,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        LabelValuesResponseInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        LabelValuesResponseInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<LabelValuesResponseOutput> for LabelValuesResponseInput {
    fn from(o: LabelValuesResponseOutput) -> Self {
        Self {
            values: o.values,
        }
    }
}

impl From<LabelValuesResponseInput> for LabelValuesResponseOutput {
    fn from(i: LabelValuesResponseInput) -> Self {
        Self {
            tag: 33u8,
            values: i.values,
        }
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
