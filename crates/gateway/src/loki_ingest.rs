//! Inbound Grafana Loki push protocol (JSON and raw-Snappy protobuf).

use std::collections::BTreeMap;

use axum::{
    body::Bytes,
    extract::State,
    http::{header, HeaderMap, StatusCode},
};
use prost::Message;
use scry_proto::{
    fingerprint::fingerprint,
    generated::{LogEntry, LogStream, LogsBatch},
    LabelPair,
};

use crate::{
    loki::{LokiPushRequest, LokiValue},
    otlp_common::MAX_OTLP_BODY_BYTES,
    sink::AppState,
};

#[derive(Clone, PartialEq, Message)]
pub struct ProtoPushRequest {
    #[prost(message, repeated, tag = "1")]
    pub streams: Vec<ProtoStream>,
}
#[derive(Clone, PartialEq, Message)]
pub struct ProtoStream {
    #[prost(string, tag = "1")]
    pub labels: String,
    #[prost(message, repeated, tag = "2")]
    pub entries: Vec<ProtoEntry>,
    #[prost(uint64, tag = "3")]
    pub hash: u64,
}
#[derive(Clone, PartialEq, Message)]
pub struct ProtoEntry {
    #[prost(message, optional, tag = "1")]
    pub timestamp: Option<ProtoTimestamp>,
    #[prost(string, tag = "2")]
    pub line: String,
    #[prost(message, repeated, tag = "3")]
    pub structured_metadata: Vec<ProtoLabelPair>,
}
#[derive(Clone, PartialEq, Message)]
pub struct ProtoTimestamp {
    #[prost(int64, tag = "1")]
    pub seconds: i64,
    #[prost(int32, tag = "2")]
    pub nanos: i32,
}
#[derive(Clone, PartialEq, Message)]
pub struct ProtoLabelPair {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

pub async fn handle(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, (StatusCode, String)> {
    let result = decode(&headers, &body);
    let batch = result.inspect_err(|_| {
        if let Some(metrics) = state.metrics() {
            metrics.inbound_rejected(crate::metrics::Inbound::LokiHttp);
        }
    })?;
    if let Some(metrics) = state.metrics() {
        metrics.inbound_accepted(crate::metrics::Inbound::LokiHttp);
    }
    state.offer_logs(batch);
    Ok(StatusCode::NO_CONTENT)
}

pub fn decode(headers: &HeaderMap, body: &[u8]) -> Result<LogsBatch, (StatusCode, String)> {
    if body.len() > MAX_OTLP_BODY_BYTES {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            "Loki request exceeds 32 MiB".into(),
        ));
    }
    let media = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/x-protobuf")
        .split(';')
        .next()
        .unwrap_or_default()
        .trim();
    match media {
        "application/json" => {
            let request: LokiPushRequest = serde_json::from_slice(body).map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("Loki JSON decode failed: {e}"),
                )
            })?;
            map_json(request)
        }
        "application/x-protobuf" | "application/protobuf" => {
            let encoding = headers
                .get(header::CONTENT_ENCODING)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("snappy");
            let raw = match encoding {
                "snappy" => {
                    let expanded = snap::raw::decompress_len(body).map_err(|e| {
                        (
                            StatusCode::BAD_REQUEST,
                            format!("Loki Snappy header failed: {e}"),
                        )
                    })?;
                    if expanded > MAX_OTLP_BODY_BYTES {
                        return Err((
                            StatusCode::PAYLOAD_TOO_LARGE,
                            "expanded Loki request exceeds 32 MiB".into(),
                        ));
                    }
                    snap::raw::Decoder::new()
                        .decompress_vec(body)
                        .map_err(|e| {
                            (
                                StatusCode::BAD_REQUEST,
                                format!("Loki Snappy decode failed: {e}"),
                            )
                        })?
                }
                "identity" | "" => body.to_vec(),
                _ => {
                    return Err((
                        StatusCode::UNSUPPORTED_MEDIA_TYPE,
                        format!("unsupported Loki Content-Encoding {encoding}"),
                    ))
                }
            };
            let request = ProtoPushRequest::decode(raw.as_slice()).map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("Loki protobuf decode failed: {e}"),
                )
            })?;
            map_proto(request)
        }
        _ => Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            format!("unsupported Loki Content-Type {media}"),
        )),
    }
}

fn map_json(request: LokiPushRequest) -> Result<LogsBatch, (StatusCode, String)> {
    let mut streams = Vec::with_capacity(request.streams.len());
    for stream in request.streams {
        let labels = stream
            .stream
            .into_iter()
            .map(|(key, value)| LabelPair { key, value })
            .collect::<Vec<_>>();
        let fp = fingerprint(&labels);
        let mut entries = Vec::with_capacity(stream.values.len());
        for value in stream.values {
            entries.push(json_entry(value)?);
        }
        streams.push(LogStream {
            fingerprint: fp,
            labels,
            entries,
        });
    }
    Ok(LogsBatch { streams })
}

fn json_entry(value: LokiValue) -> Result<LogEntry, (StatusCode, String)> {
    let ts_unix_nano = value.ts_unix_nano.parse().map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            "invalid Loki nanosecond timestamp".into(),
        )
    })?;
    Ok(LogEntry {
        ts_unix_nano,
        severity: 0,
        body: value.line,
        attributes: value
            .metadata
            .into_iter()
            .map(|(key, value)| LabelPair { key, value })
            .collect(),
    })
}

fn map_proto(request: ProtoPushRequest) -> Result<LogsBatch, (StatusCode, String)> {
    let mut streams = Vec::with_capacity(request.streams.len());
    for stream in request.streams {
        let labels = parse_label_set(&stream.labels)
            .map_err(|message| (StatusCode::BAD_REQUEST, message))?;
        let fp = fingerprint(&labels);
        let mut entries = Vec::with_capacity(stream.entries.len());
        for entry in stream.entries {
            let timestamp = entry.timestamp.ok_or_else(|| {
                (
                    StatusCode::BAD_REQUEST,
                    "Loki protobuf entry has no timestamp".into(),
                )
            })?;
            if timestamp.seconds < 0 || !(0..1_000_000_000).contains(&timestamp.nanos) {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "invalid Loki protobuf timestamp".into(),
                ));
            }
            let ts_unix_nano = (timestamp.seconds as u64)
                .checked_mul(1_000_000_000)
                .and_then(|value| value.checked_add(timestamp.nanos as u64))
                .ok_or_else(|| {
                    (
                        StatusCode::BAD_REQUEST,
                        "Loki protobuf timestamp overflow".into(),
                    )
                })?;
            entries.push(LogEntry {
                ts_unix_nano,
                severity: 0,
                body: entry.line,
                attributes: entry
                    .structured_metadata
                    .into_iter()
                    .map(|pair| LabelPair {
                        key: pair.name,
                        value: pair.value,
                    })
                    .collect(),
            });
        }
        streams.push(LogStream {
            fingerprint: fp,
            labels,
            entries,
        });
    }
    Ok(LogsBatch { streams })
}

pub fn parse_label_set(input: &str) -> Result<Vec<LabelPair>, String> {
    let input = input.trim();
    if !input.starts_with('{') || !input.ends_with('}') {
        return Err("Loki labels must be enclosed in braces".into());
    }
    let bytes = input.as_bytes();
    let mut pos = 1usize;
    let end = bytes.len() - 1;
    let mut labels = BTreeMap::new();
    loop {
        while pos < end && (bytes[pos].is_ascii_whitespace() || bytes[pos] == b',') {
            pos += 1;
        }
        if pos == end {
            break;
        }
        let key_start = pos;
        while pos < end && (bytes[pos].is_ascii_alphanumeric() || bytes[pos] == b'_') {
            pos += 1;
        }
        if pos == key_start {
            return Err("invalid Loki label name".into());
        }
        let key = &input[key_start..pos];
        while pos < end && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos >= end || bytes[pos] != b'=' {
            return Err("expected '=' after Loki label name".into());
        }
        pos += 1;
        while pos < end && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos >= end || bytes[pos] != b'"' {
            return Err("expected quoted Loki label value".into());
        }
        pos += 1;
        let mut value = String::new();
        let mut closed = false;
        while pos < end {
            match bytes[pos] {
                b'"' => {
                    pos += 1;
                    closed = true;
                    break;
                }
                b'\\' => {
                    pos += 1;
                    if pos >= end {
                        return Err("unterminated Loki label escape".into());
                    }
                    value.push(match bytes[pos] {
                        b'n' => '\n',
                        b'\\' => '\\',
                        b'"' => '"',
                        _ => return Err("unsupported Loki label escape".into()),
                    });
                    pos += 1;
                }
                _ => {
                    let character = input[pos..]
                        .chars()
                        .next()
                        .ok_or_else(|| "invalid UTF-8 Loki label value".to_string())?;
                    value.push(character);
                    pos += character.len_utf8();
                }
            }
        }
        if !closed {
            return Err("unterminated Loki label value".into());
        }
        labels.insert(key.to_string(), value);
        while pos < end && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos < end && bytes[pos] != b',' {
            return Err("expected comma between Loki labels".into());
        }
    }
    Ok(labels
        .into_iter()
        .map(|(key, value)| LabelPair { key, value })
        .collect())
}

pub fn sample_proto_request(entries: usize) -> ProtoPushRequest {
    let mut values = Vec::with_capacity(entries);
    for index in 0..entries {
        values.push(ProtoEntry {
            timestamp: Some(ProtoTimestamp {
                seconds: 1_700_300_000,
                nanos: index as i32,
            }),
            line: format!("loki proto {index}"),
            structured_metadata: vec![ProtoLabelPair {
                name: "source".into(),
                value: "smoke".into(),
            }],
        });
    }
    ProtoPushRequest {
        streams: vec![ProtoStream {
            labels: r#"{service="loki-proto",env="smoke"}"#.into(),
            entries: values,
            hash: 0,
        }],
    }
}

pub fn encode_proto_snappy(request: &ProtoPushRequest) -> Vec<u8> {
    snap::raw::Encoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("fixture Snappy encoding")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_escaped_labels() {
        let labels =
            parse_label_set(r#"{service="a\\b",message="say \"hi\"",line="a\nb",city="Zürich"}"#)
                .unwrap();
        assert_eq!(labels.len(), 4);
        assert_eq!(
            labels.iter().find(|l| l.key == "line").unwrap().value,
            "a\nb"
        );
        assert_eq!(
            labels.iter().find(|l| l.key == "city").unwrap().value,
            "Zürich"
        );
    }
    #[test]
    fn decodes_snappy_protobuf() {
        let request = sample_proto_request(2);
        let body = encode_proto_snappy(&request);
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "application/x-protobuf".parse().unwrap(),
        );
        headers.insert(header::CONTENT_ENCODING, "snappy".parse().unwrap());
        let batch = decode(&headers, &body).unwrap();
        assert_eq!(batch.streams[0].entries.len(), 2);
    }
}
