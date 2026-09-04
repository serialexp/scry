//! Modern Pyroscope Push v1 unary Connect receiver.

use std::io::{Read, Write};

use axum::{
    body::Bytes,
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use prost::Message;
use scry_proto::{
    generated::{ProfileBlob, ProfilesBatch},
    LabelPair,
};
use serde::{Deserialize, Serialize};

use crate::{otlp_common::MAX_OTLP_BODY_BYTES, sink::AppState};

#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PushRequest {
    #[prost(message, repeated, tag = "1")]
    pub series: Vec<RawProfileSeries>,
}
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct RawProfileSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<ProfileLabel>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<RawSample>,
}
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ProfileLabel {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct RawSample {
    #[prost(bytes = "vec", tag = "1")]
    #[serde(rename = "rawProfile", with = "base64_bytes")]
    pub raw_profile: Vec<u8>,
    #[prost(string, tag = "2")]
    #[serde(rename = "ID", alias = "id")]
    pub id: String,
}
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct PushResponse {}

#[derive(Clone, PartialEq, Message)]
struct PprofMetadata {
    #[prost(message, repeated, tag = "1")]
    sample_type: Vec<PprofValueType>,
    #[prost(string, repeated, tag = "6")]
    string_table: Vec<String>,
    #[prost(int64, tag = "9")]
    time_nanos: i64,
    #[prost(int64, tag = "10")]
    duration_nanos: i64,
}

#[derive(Clone, PartialEq, Message)]
struct PprofValueType {
    #[prost(int64, tag = "1")]
    r#type: i64,
    #[prost(int64, tag = "2")]
    unit: i64,
}

mod base64_bytes {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(bytes))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let value = String::deserialize(deserializer)?;
        STANDARD.decode(value).map_err(serde::de::Error::custom)
    }
}

pub async fn handle(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, (StatusCode, String)> {
    let decoded = (|| {
        let json = content_is_json(&headers)?;
        let body = decode_http_content(&headers, &body)?;
        let request = if json {
            serde_json::from_slice(&body).map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("Pyroscope Push JSON decode failed: {e}"),
                )
            })?
        } else {
            PushRequest::decode(body.as_slice()).map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("Pyroscope Push protobuf decode failed: {e}"),
                )
            })?
        };
        let batch = map_request(request).map_err(|message| (StatusCode::BAD_REQUEST, message))?;
        Ok::<_, (StatusCode, String)>((json, batch))
    })();
    let (json, batch) = decoded.inspect_err(|_| {
        if let Some(metrics) = state.metrics() {
            metrics.inbound_rejected(crate::metrics::Inbound::PyroscopePushHttp);
        }
    })?;
    if let Some(metrics) = state.metrics() {
        metrics.inbound_accepted(crate::metrics::Inbound::PyroscopePushHttp);
    }
    state.offer_profiles(batch);
    let response = PushResponse::default();
    if json {
        Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            serde_json::to_vec(&response).unwrap(),
        )
            .into_response())
    } else {
        Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/proto")],
            response.encode_to_vec(),
        )
            .into_response())
    }
}

fn content_is_json(headers: &HeaderMap) -> Result<bool, (StatusCode, String)> {
    let media = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/proto")
        .split(';')
        .next()
        .unwrap_or_default();
    match media {
        "application/json" => Ok(true),
        "application/proto" | "application/x-protobuf" | "application/protobuf" => Ok(false),
        _ => Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            format!("unsupported Pyroscope Push Content-Type {media}"),
        )),
    }
}

fn decode_http_content(headers: &HeaderMap, body: &[u8]) -> Result<Vec<u8>, (StatusCode, String)> {
    if body.len() > MAX_OTLP_BODY_BYTES {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            "Pyroscope Push request exceeds 32 MiB".into(),
        ));
    }
    match headers
        .get(header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("identity")
    {
        "identity" | "" => Ok(body.to_vec()),
        "gzip" => read_gzip_bounded(body).map_err(|e| (StatusCode::BAD_REQUEST, e)),
        value => Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            format!("unsupported Pyroscope Push Content-Encoding {value}"),
        )),
    }
}

pub fn map_request(request: PushRequest) -> Result<ProfilesBatch, String> {
    let total = request
        .series
        .iter()
        .map(|series| series.samples.len())
        .sum();
    let mut samples = Vec::with_capacity(total);
    for series in request.series {
        let labels = series
            .labels
            .into_iter()
            .map(|label| LabelPair {
                key: label.name,
                value: label.value,
            })
            .collect::<Vec<_>>();
        for sample in series.samples {
            let raw = maybe_unzip(&sample.raw_profile)?;
            let metadata = PprofMetadata::decode(raw.as_slice())
                .map_err(|e| format!("invalid pprof profile: {e}"))?;
            if metadata.string_table.first().map(String::as_str) != Some("")
                || metadata.sample_type.is_empty()
            {
                return Err(
                    "pprof requires an empty string-table entry zero and a sample type".into(),
                );
            }
            if metadata.time_nanos < 0 || metadata.duration_nanos < 0 {
                return Err("pprof timestamp and duration must be non-negative".into());
            }
            let data = gzip(&raw)?;
            samples.push(ProfileBlob {
                ts_unix_nano: metadata.time_nanos as u64,
                duration_nano: metadata.duration_nanos as u64,
                labels: labels.clone(),
                format: 1,
                data,
            });
        }
    }
    Ok(ProfilesBatch { samples })
}

fn maybe_unzip(bytes: &[u8]) -> Result<Vec<u8>, String> {
    if bytes.starts_with(&[0x1f, 0x8b]) {
        read_gzip_bounded(bytes)
    } else if bytes.len() <= MAX_OTLP_BODY_BYTES {
        Ok(bytes.to_vec())
    } else {
        Err("pprof profile exceeds 32 MiB".into())
    }
}
fn read_gzip_bounded(bytes: &[u8]) -> Result<Vec<u8>, String> {
    let mut decoder = flate2::read::GzDecoder::new(bytes);
    let mut out = Vec::new();
    decoder
        .by_ref()
        .take(MAX_OTLP_BODY_BYTES as u64 + 1)
        .read_to_end(&mut out)
        .map_err(|e| format!("gzip decode failed: {e}"))?;
    if out.len() > MAX_OTLP_BODY_BYTES {
        Err("expanded data exceeds 32 MiB".into())
    } else {
        Ok(out)
    }
}
fn gzip(bytes: &[u8]) -> Result<Vec<u8>, String> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).map_err(|e| e.to_string())?;
    encoder.finish().map_err(|e| e.to_string())
}

pub fn sample_request(samples: usize) -> PushRequest {
    let mut values = Vec::with_capacity(samples);
    for index in 0..samples {
        let profile = PprofMetadata {
            sample_type: vec![PprofValueType { r#type: 1, unit: 2 }],
            string_table: vec![String::new(), "cpu".into(), "nanoseconds".into()],
            time_nanos: 1_700_400_000_000_000_000 + index as i64,
            duration_nanos: 10_000_000_000,
        };
        values.push(RawSample {
            raw_profile: profile.encode_to_vec(),
            id: format!("sample-{index}"),
        });
    }
    PushRequest {
        series: vec![RawProfileSeries {
            labels: vec![ProfileLabel {
                name: "service.name".into(),
                value: "pyroscope-push".into(),
            }],
            samples: values,
        }],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_metadata_and_normalizes_gzip() {
        let mapped = map_request(sample_request(2)).unwrap();
        assert_eq!(mapped.samples.len(), 2);
        assert_eq!(mapped.samples[0].duration_nano, 10_000_000_000);
        assert!(mapped.samples[0].data.starts_with(&[0x1f, 0x8b]));
    }
    #[test]
    fn rejects_malformed_pprof() {
        let mut request = sample_request(1);
        request.series[0].samples[0].raw_profile = vec![0xff];
        assert!(map_request(request).is_err());
    }
}
