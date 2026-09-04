#![allow(non_camel_case_types)]
#![allow(dead_code)]
#![allow(unreachable_code)]

#[allow(unused_imports)]
use binschema_runtime::{BitStreamEncoder, BitStreamDecoder, Endianness, BitOrder, Result, BinSchemaError, EncodeContext, FieldValue};
#[allow(unused_imports)]
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum QueryWorkerFrameMsg {
    WorkerClientHello(WorkerClientHelloOutput),
    WorkerServerHello(WorkerServerHelloOutput),
    WorkerAuthenticated(WorkerAuthenticatedOutput),
    WorkerBidRequest(WorkerBidRequestOutput),
    WorkerBidResponse(WorkerBidResponseOutput),
    WorkerBidDecline(WorkerBidDeclineOutput),
    WorkerRelease(WorkerReleaseOutput),
    WorkerReleaseAck(WorkerReleaseAckOutput),
    WorkerCancel(WorkerCancelOutput),
    WorkerCancelAck(WorkerCancelAckOutput),
    WorkerError(WorkerErrorOutput),
}

impl QueryWorkerFrameMsg {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        match self {
            QueryWorkerFrameMsg::WorkerClientHello(v) => {
                encoder.write_uint8(1);
                encoder.write_uint16(v.protocol_version, Endianness::BigEndian);
                for item in &v.coordinator_id {
                    encoder.write_uint8(*item);
                }
                for item in &v.expected_worker_id {
                    encoder.write_uint8(*item);
                }
                encoder.write_uint16(v.deployment.len() as u16, Endianness::BigEndian);
                let string_bytes: &[u8] = v.deployment.as_bytes();
                for &b in string_bytes.iter() {
                    encoder.write_uint8(b);
                }
                encoder.write_uint64(v.timestamp_unix_ms, Endianness::BigEndian);
                for item in &v.nonce {
                    encoder.write_uint8(*item);
                }
                encoder.write_uint8(v.key_id.chars().count() as u8);
                let string_bytes: Vec<u8> = v.key_id.chars().map(|c| c as u8).collect();
                for &b in string_bytes.iter() {
                    encoder.write_uint8(b);
                }
                for item in &v.mac {
                    encoder.write_uint8(*item);
                }
            }
            QueryWorkerFrameMsg::WorkerServerHello(v) => {
                encoder.write_uint8(2);
                encoder.write_uint16(v.protocol_version, Endianness::BigEndian);
                for item in &v.worker_id {
                    encoder.write_uint8(*item);
                }
                for item in &v.coordinator_nonce {
                    encoder.write_uint8(*item);
                }
                for item in &v.worker_nonce {
                    encoder.write_uint8(*item);
                }
                encoder.write_uint64(v.timestamp_unix_ms, Endianness::BigEndian);
                encoder.write_uint8(v.key_id.chars().count() as u8);
                let string_bytes: Vec<u8> = v.key_id.chars().map(|c| c as u8).collect();
                for &b in string_bytes.iter() {
                    encoder.write_uint8(b);
                }
                for item in &v.mac {
                    encoder.write_uint8(*item);
                }
            }
            QueryWorkerFrameMsg::WorkerAuthenticated(v) => {
                encoder.write_uint8(3);
                encoder.write_uint64(v.sequence, Endianness::BigEndian);
                encoder.write_uint32(v.payload.len() as u32, Endianness::BigEndian);
                for item in &v.payload {
                    encoder.write_uint8(*item);
                }
                for item in &v.mac {
                    encoder.write_uint8(*item);
                }
            }
            QueryWorkerFrameMsg::WorkerBidRequest(v) => {
                encoder.write_uint8(16);
                for item in &v.coordinator_id {
                    encoder.write_uint8(*item);
                }
                encoder.write_uint32(v.query_attempt, Endianness::BigEndian);
                for item in &v.offer_id {
                    encoder.write_uint8(*item);
                }
                encoder.write_uint64(v.deadline_unix_ms, Endianness::BigEndian);
                encoder.write_uint8(v.signal);
                encoder.write_uint16(v.required_columns.len() as u16, Endianness::BigEndian);
                for item in &v.required_columns {
                    encoder.write_uint8(item.len() as u8);
                    for b in item.as_bytes() {
                        encoder.write_uint8(*b);
                    }
                }
                encoder.write_uint8(v.requires_postings);
                encoder.write_uint8(v.requires_bloom);
                encoder.write_uint16(v.blocks.len() as u16, Endianness::BigEndian);
                for item in &v.blocks {
                    item.encode_into(encoder)?;
                }
                encoder.write_uint8(v.operation);
                encoder.write_uint64(v.estimated_output_bytes, Endianness::BigEndian);
                encoder.write_uint32(v.memory_units, Endianness::BigEndian);
                for item in &v.block_set_digest {
                    encoder.write_uint8(*item);
                }
            }
            QueryWorkerFrameMsg::WorkerBidResponse(v) => {
                encoder.write_uint8(17);
                for item in &v.offer_id {
                    encoder.write_uint8(*item);
                }
                for item in &v.worker_id {
                    encoder.write_uint8(*item);
                }
                encoder.write_uint64(v.locality_generation, Endianness::BigEndian);
                encoder.write_uint16(v.locality.len() as u16, Endianness::BigEndian);
                for item in &v.locality {
                    item.encode_into(encoder)?;
                }
                for item in &v.reservation_token {
                    encoder.write_uint8(*item);
                }
                encoder.write_uint64(v.reservation_expires_unix_ms, Endianness::BigEndian);
                encoder.write_uint32(v.estimated_start_delay_ms, Endianness::BigEndian);
                encoder.write_uint16(v.available_fragment_slots, Endianness::BigEndian);
                encoder.write_uint16(v.memory_pressure_per_mille, Endianness::BigEndian);
            }
            QueryWorkerFrameMsg::WorkerBidDecline(v) => {
                encoder.write_uint8(18);
                for item in &v.offer_id {
                    encoder.write_uint8(*item);
                }
                encoder.write_uint16(v.reason, Endianness::BigEndian);
                encoder.write_uint16(v.message.len() as u16, Endianness::BigEndian);
                let string_bytes: &[u8] = v.message.as_bytes();
                for &b in string_bytes.iter() {
                    encoder.write_uint8(b);
                }
            }
            QueryWorkerFrameMsg::WorkerRelease(v) => {
                encoder.write_uint8(32);
                for item in &v.coordinator_id {
                    encoder.write_uint8(*item);
                }
                for item in &v.reservation_token {
                    encoder.write_uint8(*item);
                }
            }
            QueryWorkerFrameMsg::WorkerReleaseAck(v) => {
                encoder.write_uint8(33);
                for item in &v.reservation_token {
                    encoder.write_uint8(*item);
                }
                encoder.write_uint8(v.released);
            }
            QueryWorkerFrameMsg::WorkerCancel(v) => {
                encoder.write_uint8(48);
                for item in &v.coordinator_id {
                    encoder.write_uint8(*item);
                }
                encoder.write_uint32(v.query_attempt, Endianness::BigEndian);
                for item in &v.fragment_id {
                    encoder.write_uint8(*item);
                }
                encoder.write_uint32(v.fragment_attempt, Endianness::BigEndian);
            }
            QueryWorkerFrameMsg::WorkerCancelAck(v) => {
                encoder.write_uint8(49);
                for item in &v.fragment_id {
                    encoder.write_uint8(*item);
                }
                encoder.write_uint32(v.fragment_attempt, Endianness::BigEndian);
                encoder.write_uint8(v.cancelled);
            }
            QueryWorkerFrameMsg::WorkerError(v) => {
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
            QueryWorkerFrameMsg::WorkerClientHello(_) => "WorkerClientHello",
            QueryWorkerFrameMsg::WorkerServerHello(_) => "WorkerServerHello",
            QueryWorkerFrameMsg::WorkerAuthenticated(_) => "WorkerAuthenticated",
            QueryWorkerFrameMsg::WorkerBidRequest(_) => "WorkerBidRequest",
            QueryWorkerFrameMsg::WorkerBidResponse(_) => "WorkerBidResponse",
            QueryWorkerFrameMsg::WorkerBidDecline(_) => "WorkerBidDecline",
            QueryWorkerFrameMsg::WorkerRelease(_) => "WorkerRelease",
            QueryWorkerFrameMsg::WorkerReleaseAck(_) => "WorkerReleaseAck",
            QueryWorkerFrameMsg::WorkerCancel(_) => "WorkerCancel",
            QueryWorkerFrameMsg::WorkerCancelAck(_) => "WorkerCancelAck",
            QueryWorkerFrameMsg::WorkerError(_) => "WorkerError",
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        // Union type - try each variant in order until one succeeds
        let start_pos = decoder.position();
        if let Ok(v) = WorkerClientHelloOutput::decode_with_decoder(decoder) {
            return Ok(QueryWorkerFrameMsg::WorkerClientHello(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = WorkerServerHelloOutput::decode_with_decoder(decoder) {
            return Ok(QueryWorkerFrameMsg::WorkerServerHello(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = WorkerAuthenticatedOutput::decode_with_decoder(decoder) {
            return Ok(QueryWorkerFrameMsg::WorkerAuthenticated(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = WorkerBidRequestOutput::decode_with_decoder(decoder) {
            return Ok(QueryWorkerFrameMsg::WorkerBidRequest(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = WorkerBidResponseOutput::decode_with_decoder(decoder) {
            return Ok(QueryWorkerFrameMsg::WorkerBidResponse(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = WorkerBidDeclineOutput::decode_with_decoder(decoder) {
            return Ok(QueryWorkerFrameMsg::WorkerBidDecline(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = WorkerReleaseOutput::decode_with_decoder(decoder) {
            return Ok(QueryWorkerFrameMsg::WorkerRelease(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = WorkerReleaseAckOutput::decode_with_decoder(decoder) {
            return Ok(QueryWorkerFrameMsg::WorkerReleaseAck(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = WorkerCancelOutput::decode_with_decoder(decoder) {
            return Ok(QueryWorkerFrameMsg::WorkerCancel(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = WorkerCancelAckOutput::decode_with_decoder(decoder) {
            return Ok(QueryWorkerFrameMsg::WorkerCancelAck(v));
        }
        decoder.seek(start_pos)?;
        if let Ok(v) = WorkerErrorOutput::decode_with_decoder(decoder) {
            return Ok(QueryWorkerFrameMsg::WorkerError(v));
        }
        Err(binschema_runtime::BinSchemaError::InvalidVariant("no variant matched the input bytes".to_string()))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryWorkerFrame {
    pub msg: QueryWorkerFrameMsg,
}

impl QueryWorkerFrame {
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
        let msg = QueryWorkerFrameMsg::decode_with_decoder(decoder)?;
        Ok(Self {
            msg,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerClientHelloInput {
    pub protocol_version: u16,
    pub coordinator_id: Vec<u8>,
    pub expected_worker_id: Vec<u8>,
    pub deployment: std::string::String,
    pub timestamp_unix_ms: u64,
    pub nonce: Vec<u8>,
    pub key_id: std::string::String,
    pub mac: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerClientHelloOutput {
    pub tag: u8,
    pub protocol_version: u16,
    pub coordinator_id: Vec<u8>,
    pub expected_worker_id: Vec<u8>,
    pub deployment: std::string::String,
    pub timestamp_unix_ms: u64,
    pub nonce: Vec<u8>,
    pub key_id: std::string::String,
    pub mac: Vec<u8>,
}

pub type WorkerClientHello = WorkerClientHelloOutput;

impl WorkerClientHelloInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(1);
        encoder.write_u16_be(self.protocol_version);
        for item in &self.coordinator_id {
            encoder.write_byte(*item);
        }
        for item in &self.expected_worker_id {
            encoder.write_byte(*item);
        }
        encoder.write_u16_be(self.deployment.len() as u16);
        let string_bytes: &[u8] = self.deployment.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        encoder.write_u64_be(self.timestamp_unix_ms);
        for item in &self.nonce {
            encoder.write_byte(*item);
        }
        encoder.write_byte(self.key_id.chars().count() as u8);
        let string_bytes: Vec<u8> = self.key_id.chars().map(|c| c as u8).collect();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        for item in &self.mac {
            encoder.write_byte(*item);
        }
        Ok(())
    }

}

impl WorkerClientHelloOutput {
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
        let mut coordinator_id = Vec::with_capacity(16);
        for _ in 0..16 {
            let item = decoder.read_byte()?;
            coordinator_id.push(item);
        }
        let mut expected_worker_id = Vec::with_capacity(16);
        for _ in 0..16 {
            let item = decoder.read_byte()?;
            expected_worker_id.push(item);
        }
        let length = decoder.read_u16_be()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let deployment = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        let timestamp_unix_ms = decoder.read_u64_be()?;
        let mut nonce = Vec::with_capacity(32);
        for _ in 0..32 {
            let item = decoder.read_byte()?;
            nonce.push(item);
        }
        let length = decoder.read_byte()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let key_id: std::string::String = bytes.iter().map(|&b| b as char).collect();
        let mut mac = Vec::with_capacity(32);
        for _ in 0..32 {
            let item = decoder.read_byte()?;
            mac.push(item);
        }
        Ok(Self {
            tag,
            protocol_version,
            coordinator_id,
            expected_worker_id,
            deployment,
            timestamp_unix_ms,
            nonce,
            key_id,
            mac,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        WorkerClientHelloInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        WorkerClientHelloInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<WorkerClientHelloOutput> for WorkerClientHelloInput {
    fn from(o: WorkerClientHelloOutput) -> Self {
        Self {
            protocol_version: o.protocol_version,
            coordinator_id: o.coordinator_id,
            expected_worker_id: o.expected_worker_id,
            deployment: o.deployment,
            timestamp_unix_ms: o.timestamp_unix_ms,
            nonce: o.nonce,
            key_id: o.key_id,
            mac: o.mac,
        }
    }
}

impl From<WorkerClientHelloInput> for WorkerClientHelloOutput {
    fn from(i: WorkerClientHelloInput) -> Self {
        Self {
            tag: 1u8,
            protocol_version: i.protocol_version,
            coordinator_id: i.coordinator_id,
            expected_worker_id: i.expected_worker_id,
            deployment: i.deployment,
            timestamp_unix_ms: i.timestamp_unix_ms,
            nonce: i.nonce,
            key_id: i.key_id,
            mac: i.mac,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerServerHelloInput {
    pub protocol_version: u16,
    pub worker_id: Vec<u8>,
    pub coordinator_nonce: Vec<u8>,
    pub worker_nonce: Vec<u8>,
    pub timestamp_unix_ms: u64,
    pub key_id: std::string::String,
    pub mac: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerServerHelloOutput {
    pub tag: u8,
    pub protocol_version: u16,
    pub worker_id: Vec<u8>,
    pub coordinator_nonce: Vec<u8>,
    pub worker_nonce: Vec<u8>,
    pub timestamp_unix_ms: u64,
    pub key_id: std::string::String,
    pub mac: Vec<u8>,
}

pub type WorkerServerHello = WorkerServerHelloOutput;

impl WorkerServerHelloInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(2);
        encoder.write_u16_be(self.protocol_version);
        for item in &self.worker_id {
            encoder.write_byte(*item);
        }
        for item in &self.coordinator_nonce {
            encoder.write_byte(*item);
        }
        for item in &self.worker_nonce {
            encoder.write_byte(*item);
        }
        encoder.write_u64_be(self.timestamp_unix_ms);
        encoder.write_byte(self.key_id.chars().count() as u8);
        let string_bytes: Vec<u8> = self.key_id.chars().map(|c| c as u8).collect();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        for item in &self.mac {
            encoder.write_byte(*item);
        }
        Ok(())
    }

}

impl WorkerServerHelloOutput {
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
        let mut worker_id = Vec::with_capacity(16);
        for _ in 0..16 {
            let item = decoder.read_byte()?;
            worker_id.push(item);
        }
        let mut coordinator_nonce = Vec::with_capacity(32);
        for _ in 0..32 {
            let item = decoder.read_byte()?;
            coordinator_nonce.push(item);
        }
        let mut worker_nonce = Vec::with_capacity(32);
        for _ in 0..32 {
            let item = decoder.read_byte()?;
            worker_nonce.push(item);
        }
        let timestamp_unix_ms = decoder.read_u64_be()?;
        let length = decoder.read_byte()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let key_id: std::string::String = bytes.iter().map(|&b| b as char).collect();
        let mut mac = Vec::with_capacity(32);
        for _ in 0..32 {
            let item = decoder.read_byte()?;
            mac.push(item);
        }
        Ok(Self {
            tag,
            protocol_version,
            worker_id,
            coordinator_nonce,
            worker_nonce,
            timestamp_unix_ms,
            key_id,
            mac,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        WorkerServerHelloInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        WorkerServerHelloInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<WorkerServerHelloOutput> for WorkerServerHelloInput {
    fn from(o: WorkerServerHelloOutput) -> Self {
        Self {
            protocol_version: o.protocol_version,
            worker_id: o.worker_id,
            coordinator_nonce: o.coordinator_nonce,
            worker_nonce: o.worker_nonce,
            timestamp_unix_ms: o.timestamp_unix_ms,
            key_id: o.key_id,
            mac: o.mac,
        }
    }
}

impl From<WorkerServerHelloInput> for WorkerServerHelloOutput {
    fn from(i: WorkerServerHelloInput) -> Self {
        Self {
            tag: 2u8,
            protocol_version: i.protocol_version,
            worker_id: i.worker_id,
            coordinator_nonce: i.coordinator_nonce,
            worker_nonce: i.worker_nonce,
            timestamp_unix_ms: i.timestamp_unix_ms,
            key_id: i.key_id,
            mac: i.mac,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerAuthenticatedInput {
    pub sequence: u64,
    pub payload: Vec<u8>,
    pub mac: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerAuthenticatedOutput {
    pub tag: u8,
    pub sequence: u64,
    pub payload: Vec<u8>,
    pub mac: Vec<u8>,
}

pub type WorkerAuthenticated = WorkerAuthenticatedOutput;

impl WorkerAuthenticatedInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(3);
        encoder.write_u64_be(self.sequence);
        encoder.write_u32_be(self.payload.len() as u32);
        for item in &self.payload {
            encoder.write_byte(*item);
        }
        for item in &self.mac {
            encoder.write_byte(*item);
        }
        Ok(())
    }

}

impl WorkerAuthenticatedOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 3u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 3, got {}", tag)));
        }
        let sequence = decoder.read_u64_be()?;
        let length = decoder.read_u32_be()? as usize;
        let mut payload = Vec::with_capacity(length);
        for _ in 0..length {
            let item = decoder.read_byte()?;
            payload.push(item);
        }
        let mut mac = Vec::with_capacity(32);
        for _ in 0..32 {
            let item = decoder.read_byte()?;
            mac.push(item);
        }
        Ok(Self {
            tag,
            sequence,
            payload,
            mac,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        WorkerAuthenticatedInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        WorkerAuthenticatedInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<WorkerAuthenticatedOutput> for WorkerAuthenticatedInput {
    fn from(o: WorkerAuthenticatedOutput) -> Self {
        Self {
            sequence: o.sequence,
            payload: o.payload,
            mac: o.mac,
        }
    }
}

impl From<WorkerAuthenticatedInput> for WorkerAuthenticatedOutput {
    fn from(i: WorkerAuthenticatedInput) -> Self {
        Self {
            tag: 3u8,
            sequence: i.sequence,
            payload: i.payload,
            mac: i.mac,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerBlockOffer {
    pub uuid: Vec<u8>,
    pub estimated_scan_bytes: u64,
}

impl WorkerBlockOffer {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        for item in &self.uuid {
            encoder.write_byte(*item);
        }
        encoder.write_u64_be(self.estimated_scan_bytes);
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let mut uuid = Vec::with_capacity(16);
        for _ in 0..16 {
            let item = decoder.read_byte()?;
            uuid.push(item);
        }
        let estimated_scan_bytes = decoder.read_u64_be()?;
        Ok(Self {
            uuid,
            estimated_scan_bytes,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerBlockLocality {
    pub uuid: Vec<u8>,
    pub locality: u8,
}

impl WorkerBlockLocality {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        for item in &self.uuid {
            encoder.write_byte(*item);
        }
        encoder.write_byte(self.locality);
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let mut uuid = Vec::with_capacity(16);
        for _ in 0..16 {
            let item = decoder.read_byte()?;
            uuid.push(item);
        }
        let locality = decoder.read_byte()?;
        Ok(Self {
            uuid,
            locality,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerBidRequestInput {
    pub coordinator_id: Vec<u8>,
    pub query_attempt: u32,
    pub offer_id: Vec<u8>,
    pub deadline_unix_ms: u64,
    pub signal: u8,
    pub required_columns: Vec<std::string::String>,
    pub requires_postings: u8,
    pub requires_bloom: u8,
    pub blocks: Vec<WorkerBlockOffer>,
    pub operation: u8,
    pub estimated_output_bytes: u64,
    pub memory_units: u32,
    pub block_set_digest: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerBidRequestOutput {
    pub tag: u8,
    pub coordinator_id: Vec<u8>,
    pub query_attempt: u32,
    pub offer_id: Vec<u8>,
    pub deadline_unix_ms: u64,
    pub signal: u8,
    pub required_columns: Vec<std::string::String>,
    pub requires_postings: u8,
    pub requires_bloom: u8,
    pub blocks: Vec<WorkerBlockOffer>,
    pub operation: u8,
    pub estimated_output_bytes: u64,
    pub memory_units: u32,
    pub block_set_digest: Vec<u8>,
}

pub type WorkerBidRequest = WorkerBidRequestOutput;

impl WorkerBidRequestInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(16);
        for item in &self.coordinator_id {
            encoder.write_byte(*item);
        }
        encoder.write_u32_be(self.query_attempt);
        for item in &self.offer_id {
            encoder.write_byte(*item);
        }
        encoder.write_u64_be(self.deadline_unix_ms);
        encoder.write_byte(self.signal);
        encoder.write_u16_be(self.required_columns.len() as u16);
        for item in &self.required_columns {
            encoder.write_byte(item.len() as u8);
            for b in item.as_bytes() {
                encoder.write_byte(*b);
            }
        }
        encoder.write_byte(self.requires_postings);
        encoder.write_byte(self.requires_bloom);
        encoder.write_u16_be(self.blocks.len() as u16);
        for item in &self.blocks {
            item.encode_into(encoder)?;
        }
        encoder.write_byte(self.operation);
        encoder.write_u64_be(self.estimated_output_bytes);
        encoder.write_u32_be(self.memory_units);
        for item in &self.block_set_digest {
            encoder.write_byte(*item);
        }
        Ok(())
    }

}

impl WorkerBidRequestOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 16u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 16, got {}", tag)));
        }
        let mut coordinator_id = Vec::with_capacity(16);
        for _ in 0..16 {
            let item = decoder.read_byte()?;
            coordinator_id.push(item);
        }
        let query_attempt = decoder.read_u32_be()?;
        let mut offer_id = Vec::with_capacity(16);
        for _ in 0..16 {
            let item = decoder.read_byte()?;
            offer_id.push(item);
        }
        let deadline_unix_ms = decoder.read_u64_be()?;
        let signal = decoder.read_byte()?;
        let length = decoder.read_u16_be()? as usize;
        let mut required_columns = Vec::with_capacity(length);
        for _ in 0..length {
            let str_len = decoder.read_byte()? as usize;
            let str_bytes = decoder.read_bytes_vec(str_len)?;
            let item = std::string::String::from_utf8(str_bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
            required_columns.push(item);
        }
        let requires_postings = decoder.read_byte()?;
        let requires_bloom = decoder.read_byte()?;
        let length = decoder.read_u16_be()? as usize;
        let mut blocks = Vec::with_capacity(length);
        for _ in 0..length {
            let item = WorkerBlockOffer::decode_with_decoder(decoder)?;
            blocks.push(item);
        }
        let operation = decoder.read_byte()?;
        let estimated_output_bytes = decoder.read_u64_be()?;
        let memory_units = decoder.read_u32_be()?;
        let mut block_set_digest = Vec::with_capacity(32);
        for _ in 0..32 {
            let item = decoder.read_byte()?;
            block_set_digest.push(item);
        }
        Ok(Self {
            tag,
            coordinator_id,
            query_attempt,
            offer_id,
            deadline_unix_ms,
            signal,
            required_columns,
            requires_postings,
            requires_bloom,
            blocks,
            operation,
            estimated_output_bytes,
            memory_units,
            block_set_digest,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        WorkerBidRequestInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        WorkerBidRequestInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<WorkerBidRequestOutput> for WorkerBidRequestInput {
    fn from(o: WorkerBidRequestOutput) -> Self {
        Self {
            coordinator_id: o.coordinator_id,
            query_attempt: o.query_attempt,
            offer_id: o.offer_id,
            deadline_unix_ms: o.deadline_unix_ms,
            signal: o.signal,
            required_columns: o.required_columns,
            requires_postings: o.requires_postings,
            requires_bloom: o.requires_bloom,
            blocks: o.blocks,
            operation: o.operation,
            estimated_output_bytes: o.estimated_output_bytes,
            memory_units: o.memory_units,
            block_set_digest: o.block_set_digest,
        }
    }
}

impl From<WorkerBidRequestInput> for WorkerBidRequestOutput {
    fn from(i: WorkerBidRequestInput) -> Self {
        Self {
            tag: 16u8,
            coordinator_id: i.coordinator_id,
            query_attempt: i.query_attempt,
            offer_id: i.offer_id,
            deadline_unix_ms: i.deadline_unix_ms,
            signal: i.signal,
            required_columns: i.required_columns,
            requires_postings: i.requires_postings,
            requires_bloom: i.requires_bloom,
            blocks: i.blocks,
            operation: i.operation,
            estimated_output_bytes: i.estimated_output_bytes,
            memory_units: i.memory_units,
            block_set_digest: i.block_set_digest,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerBidResponseInput {
    pub offer_id: Vec<u8>,
    pub worker_id: Vec<u8>,
    pub locality_generation: u64,
    pub locality: Vec<WorkerBlockLocality>,
    pub reservation_token: Vec<u8>,
    pub reservation_expires_unix_ms: u64,
    pub estimated_start_delay_ms: u32,
    pub available_fragment_slots: u16,
    pub memory_pressure_per_mille: u16,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerBidResponseOutput {
    pub tag: u8,
    pub offer_id: Vec<u8>,
    pub worker_id: Vec<u8>,
    pub locality_generation: u64,
    pub locality: Vec<WorkerBlockLocality>,
    pub reservation_token: Vec<u8>,
    pub reservation_expires_unix_ms: u64,
    pub estimated_start_delay_ms: u32,
    pub available_fragment_slots: u16,
    pub memory_pressure_per_mille: u16,
}

pub type WorkerBidResponse = WorkerBidResponseOutput;

impl WorkerBidResponseInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(17);
        for item in &self.offer_id {
            encoder.write_byte(*item);
        }
        for item in &self.worker_id {
            encoder.write_byte(*item);
        }
        encoder.write_u64_be(self.locality_generation);
        encoder.write_u16_be(self.locality.len() as u16);
        for item in &self.locality {
            item.encode_into(encoder)?;
        }
        for item in &self.reservation_token {
            encoder.write_byte(*item);
        }
        encoder.write_u64_be(self.reservation_expires_unix_ms);
        encoder.write_u32_be(self.estimated_start_delay_ms);
        encoder.write_u16_be(self.available_fragment_slots);
        encoder.write_u16_be(self.memory_pressure_per_mille);
        Ok(())
    }

}

impl WorkerBidResponseOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 17u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 17, got {}", tag)));
        }
        let mut offer_id = Vec::with_capacity(16);
        for _ in 0..16 {
            let item = decoder.read_byte()?;
            offer_id.push(item);
        }
        let mut worker_id = Vec::with_capacity(16);
        for _ in 0..16 {
            let item = decoder.read_byte()?;
            worker_id.push(item);
        }
        let locality_generation = decoder.read_u64_be()?;
        let length = decoder.read_u16_be()? as usize;
        let mut locality = Vec::with_capacity(length);
        for _ in 0..length {
            let item = WorkerBlockLocality::decode_with_decoder(decoder)?;
            locality.push(item);
        }
        let mut reservation_token = Vec::with_capacity(32);
        for _ in 0..32 {
            let item = decoder.read_byte()?;
            reservation_token.push(item);
        }
        let reservation_expires_unix_ms = decoder.read_u64_be()?;
        let estimated_start_delay_ms = decoder.read_u32_be()?;
        let available_fragment_slots = decoder.read_u16_be()?;
        let memory_pressure_per_mille = decoder.read_u16_be()?;
        Ok(Self {
            tag,
            offer_id,
            worker_id,
            locality_generation,
            locality,
            reservation_token,
            reservation_expires_unix_ms,
            estimated_start_delay_ms,
            available_fragment_slots,
            memory_pressure_per_mille,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        WorkerBidResponseInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        WorkerBidResponseInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<WorkerBidResponseOutput> for WorkerBidResponseInput {
    fn from(o: WorkerBidResponseOutput) -> Self {
        Self {
            offer_id: o.offer_id,
            worker_id: o.worker_id,
            locality_generation: o.locality_generation,
            locality: o.locality,
            reservation_token: o.reservation_token,
            reservation_expires_unix_ms: o.reservation_expires_unix_ms,
            estimated_start_delay_ms: o.estimated_start_delay_ms,
            available_fragment_slots: o.available_fragment_slots,
            memory_pressure_per_mille: o.memory_pressure_per_mille,
        }
    }
}

impl From<WorkerBidResponseInput> for WorkerBidResponseOutput {
    fn from(i: WorkerBidResponseInput) -> Self {
        Self {
            tag: 17u8,
            offer_id: i.offer_id,
            worker_id: i.worker_id,
            locality_generation: i.locality_generation,
            locality: i.locality,
            reservation_token: i.reservation_token,
            reservation_expires_unix_ms: i.reservation_expires_unix_ms,
            estimated_start_delay_ms: i.estimated_start_delay_ms,
            available_fragment_slots: i.available_fragment_slots,
            memory_pressure_per_mille: i.memory_pressure_per_mille,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerBidDeclineInput {
    pub offer_id: Vec<u8>,
    pub reason: u16,
    pub message: std::string::String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerBidDeclineOutput {
    pub tag: u8,
    pub offer_id: Vec<u8>,
    pub reason: u16,
    pub message: std::string::String,
}

pub type WorkerBidDecline = WorkerBidDeclineOutput;

impl WorkerBidDeclineInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(18);
        for item in &self.offer_id {
            encoder.write_byte(*item);
        }
        encoder.write_u16_be(self.reason);
        encoder.write_u16_be(self.message.len() as u16);
        let string_bytes: &[u8] = self.message.as_bytes();
        for &b in string_bytes.iter() {
            encoder.write_byte(b);
        }
        Ok(())
    }

}

impl WorkerBidDeclineOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 18u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 18, got {}", tag)));
        }
        let mut offer_id = Vec::with_capacity(16);
        for _ in 0..16 {
            let item = decoder.read_byte()?;
            offer_id.push(item);
        }
        let reason = decoder.read_u16_be()?;
        let length = decoder.read_u16_be()? as usize;
        let bytes = decoder.read_bytes_vec(length)?;
        let message = std::string::String::from_utf8(bytes).map_err(|_| binschema_runtime::BinSchemaError::InvalidUtf8)?;
        Ok(Self {
            tag,
            offer_id,
            reason,
            message,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        WorkerBidDeclineInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        WorkerBidDeclineInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<WorkerBidDeclineOutput> for WorkerBidDeclineInput {
    fn from(o: WorkerBidDeclineOutput) -> Self {
        Self {
            offer_id: o.offer_id,
            reason: o.reason,
            message: o.message,
        }
    }
}

impl From<WorkerBidDeclineInput> for WorkerBidDeclineOutput {
    fn from(i: WorkerBidDeclineInput) -> Self {
        Self {
            tag: 18u8,
            offer_id: i.offer_id,
            reason: i.reason,
            message: i.message,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerReleaseInput {
    pub coordinator_id: Vec<u8>,
    pub reservation_token: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerReleaseOutput {
    pub tag: u8,
    pub coordinator_id: Vec<u8>,
    pub reservation_token: Vec<u8>,
}

pub type WorkerRelease = WorkerReleaseOutput;

impl WorkerReleaseInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(32);
        for item in &self.coordinator_id {
            encoder.write_byte(*item);
        }
        for item in &self.reservation_token {
            encoder.write_byte(*item);
        }
        Ok(())
    }

}

impl WorkerReleaseOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 32u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 32, got {}", tag)));
        }
        let mut coordinator_id = Vec::with_capacity(16);
        for _ in 0..16 {
            let item = decoder.read_byte()?;
            coordinator_id.push(item);
        }
        let mut reservation_token = Vec::with_capacity(32);
        for _ in 0..32 {
            let item = decoder.read_byte()?;
            reservation_token.push(item);
        }
        Ok(Self {
            tag,
            coordinator_id,
            reservation_token,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        WorkerReleaseInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        WorkerReleaseInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<WorkerReleaseOutput> for WorkerReleaseInput {
    fn from(o: WorkerReleaseOutput) -> Self {
        Self {
            coordinator_id: o.coordinator_id,
            reservation_token: o.reservation_token,
        }
    }
}

impl From<WorkerReleaseInput> for WorkerReleaseOutput {
    fn from(i: WorkerReleaseInput) -> Self {
        Self {
            tag: 32u8,
            coordinator_id: i.coordinator_id,
            reservation_token: i.reservation_token,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerReleaseAckInput {
    pub reservation_token: Vec<u8>,
    pub released: u8,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerReleaseAckOutput {
    pub tag: u8,
    pub reservation_token: Vec<u8>,
    pub released: u8,
}

pub type WorkerReleaseAck = WorkerReleaseAckOutput;

impl WorkerReleaseAckInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(33);
        for item in &self.reservation_token {
            encoder.write_byte(*item);
        }
        encoder.write_byte(self.released);
        Ok(())
    }

}

impl WorkerReleaseAckOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 33u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 33, got {}", tag)));
        }
        let mut reservation_token = Vec::with_capacity(32);
        for _ in 0..32 {
            let item = decoder.read_byte()?;
            reservation_token.push(item);
        }
        let released = decoder.read_byte()?;
        Ok(Self {
            tag,
            reservation_token,
            released,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        WorkerReleaseAckInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        WorkerReleaseAckInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<WorkerReleaseAckOutput> for WorkerReleaseAckInput {
    fn from(o: WorkerReleaseAckOutput) -> Self {
        Self {
            reservation_token: o.reservation_token,
            released: o.released,
        }
    }
}

impl From<WorkerReleaseAckInput> for WorkerReleaseAckOutput {
    fn from(i: WorkerReleaseAckInput) -> Self {
        Self {
            tag: 33u8,
            reservation_token: i.reservation_token,
            released: i.released,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerCancelInput {
    pub coordinator_id: Vec<u8>,
    pub query_attempt: u32,
    pub fragment_id: Vec<u8>,
    pub fragment_attempt: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerCancelOutput {
    pub tag: u8,
    pub coordinator_id: Vec<u8>,
    pub query_attempt: u32,
    pub fragment_id: Vec<u8>,
    pub fragment_attempt: u32,
}

pub type WorkerCancel = WorkerCancelOutput;

impl WorkerCancelInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(48);
        for item in &self.coordinator_id {
            encoder.write_byte(*item);
        }
        encoder.write_u32_be(self.query_attempt);
        for item in &self.fragment_id {
            encoder.write_byte(*item);
        }
        encoder.write_u32_be(self.fragment_attempt);
        Ok(())
    }

}

impl WorkerCancelOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 48u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 48, got {}", tag)));
        }
        let mut coordinator_id = Vec::with_capacity(16);
        for _ in 0..16 {
            let item = decoder.read_byte()?;
            coordinator_id.push(item);
        }
        let query_attempt = decoder.read_u32_be()?;
        let mut fragment_id = Vec::with_capacity(16);
        for _ in 0..16 {
            let item = decoder.read_byte()?;
            fragment_id.push(item);
        }
        let fragment_attempt = decoder.read_u32_be()?;
        Ok(Self {
            tag,
            coordinator_id,
            query_attempt,
            fragment_id,
            fragment_attempt,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        WorkerCancelInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        WorkerCancelInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<WorkerCancelOutput> for WorkerCancelInput {
    fn from(o: WorkerCancelOutput) -> Self {
        Self {
            coordinator_id: o.coordinator_id,
            query_attempt: o.query_attempt,
            fragment_id: o.fragment_id,
            fragment_attempt: o.fragment_attempt,
        }
    }
}

impl From<WorkerCancelInput> for WorkerCancelOutput {
    fn from(i: WorkerCancelInput) -> Self {
        Self {
            tag: 48u8,
            coordinator_id: i.coordinator_id,
            query_attempt: i.query_attempt,
            fragment_id: i.fragment_id,
            fragment_attempt: i.fragment_attempt,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerCancelAckInput {
    pub fragment_id: Vec<u8>,
    pub fragment_attempt: u32,
    pub cancelled: u8,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerCancelAckOutput {
    pub tag: u8,
    pub fragment_id: Vec<u8>,
    pub fragment_attempt: u32,
    pub cancelled: u8,
}

pub type WorkerCancelAck = WorkerCancelAckOutput;

impl WorkerCancelAckInput {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = BitStreamEncoder::new(BitOrder::MsbFirst);
        self.encode_into(&mut encoder)?;
        Ok(encoder.finish())
    }

    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        encoder.write_byte(49);
        for item in &self.fragment_id {
            encoder.write_byte(*item);
        }
        encoder.write_u32_be(self.fragment_attempt);
        encoder.write_byte(self.cancelled);
        Ok(())
    }

}

impl WorkerCancelAckOutput {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = BitStreamDecoder::new(bytes, BitOrder::MsbFirst);
        Self::decode_with_decoder(&mut decoder)
    }

    pub fn decode_with_decoder(decoder: &mut BitStreamDecoder) -> Result<Self> {
        let tag = decoder.read_byte()?;
        if tag != 49u8 {
            return Err(binschema_runtime::BinSchemaError::InvalidVariant(format!("expected 49, got {}", tag)));
        }
        let mut fragment_id = Vec::with_capacity(16);
        for _ in 0..16 {
            let item = decoder.read_byte()?;
            fragment_id.push(item);
        }
        let fragment_attempt = decoder.read_u32_be()?;
        let cancelled = decoder.read_byte()?;
        Ok(Self {
            tag,
            fragment_id,
            fragment_attempt,
            cancelled,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        WorkerCancelAckInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        WorkerCancelAckInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<WorkerCancelAckOutput> for WorkerCancelAckInput {
    fn from(o: WorkerCancelAckOutput) -> Self {
        Self {
            fragment_id: o.fragment_id,
            fragment_attempt: o.fragment_attempt,
            cancelled: o.cancelled,
        }
    }
}

impl From<WorkerCancelAckInput> for WorkerCancelAckOutput {
    fn from(i: WorkerCancelAckInput) -> Self {
        Self {
            tag: 49u8,
            fragment_id: i.fragment_id,
            fragment_attempt: i.fragment_attempt,
            cancelled: i.cancelled,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerErrorInput {
    pub code: u16,
    pub message: std::string::String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerErrorOutput {
    pub tag: u8,
    pub code: u16,
    pub message: std::string::String,
}

pub type WorkerError = WorkerErrorOutput;

impl WorkerErrorInput {
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

impl WorkerErrorOutput {
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
        WorkerErrorInput::from(self.clone()).encode()
    }
    pub fn encode_into(&self, encoder: &mut BitStreamEncoder) -> Result<()> {
        WorkerErrorInput::from(self.clone()).encode_into(encoder)
    }
}

impl From<WorkerErrorOutput> for WorkerErrorInput {
    fn from(o: WorkerErrorOutput) -> Self {
        Self {
            code: o.code,
            message: o.message,
        }
    }
}

impl From<WorkerErrorInput> for WorkerErrorOutput {
    fn from(i: WorkerErrorInput) -> Self {
        Self {
            tag: 240u8,
            code: i.code,
            message: i.message,
        }
    }
}
