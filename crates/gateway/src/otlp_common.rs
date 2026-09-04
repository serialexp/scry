//! Shared OTLP attribute and HTTP transport helpers.

use std::io::Read;

use axum::{
    body::Bytes,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use prost::Message;
use scry_proto::LabelPair;
use serde::{de::DeserializeOwned, Serialize};

/// Keep both compressed and expanded requests below the native wire's 32 MiB cap.
pub const MAX_OTLP_BODY_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OtlpEncoding {
    Protobuf,
    Json,
}

pub fn decode_request<T>(
    headers: &HeaderMap,
    body: Bytes,
) -> Result<(T, OtlpEncoding), (StatusCode, String)>
where
    T: Message + Default + DeserializeOwned,
{
    if body.len() > MAX_OTLP_BODY_BYTES {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            "compressed OTLP request exceeds 32 MiB".into(),
        ));
    }
    let decoded = decode_content(headers, &body)?;
    let encoding = request_encoding(headers)?;
    let request = match encoding {
        OtlpEncoding::Protobuf => T::decode(decoded.as_slice()).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("OTLP protobuf decode failed: {e}"),
            )
        })?,
        OtlpEncoding::Json => serde_json::from_slice(&decoded).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("OTLP JSON decode failed: {e}"),
            )
        })?,
    };
    Ok((request, encoding))
}

pub fn encode_response<T>(value: &T, encoding: OtlpEncoding) -> Response
where
    T: Message + Serialize,
{
    match encoding {
        OtlpEncoding::Protobuf => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/x-protobuf")],
            value.encode_to_vec(),
        )
            .into_response(),
        OtlpEncoding::Json => match serde_json::to_vec(value) {
            Ok(body) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                body,
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("OTLP JSON encode failed: {e}"),
            )
                .into_response(),
        },
    }
}

fn request_encoding(headers: &HeaderMap) -> Result<OtlpEncoding, (StatusCode, String)> {
    let Some(value) = headers.get(header::CONTENT_TYPE) else {
        return Ok(OtlpEncoding::Protobuf);
    };
    let media = value
        .to_str()
        .map_err(|_| {
            (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "invalid Content-Type".into(),
            )
        })?
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    match media.as_str() {
        "application/x-protobuf" | "application/protobuf" | "application/octet-stream" => {
            Ok(OtlpEncoding::Protobuf)
        }
        "application/json" => Ok(OtlpEncoding::Json),
        _ => Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            format!("unsupported OTLP Content-Type {media}"),
        )),
    }
}

fn decode_content(headers: &HeaderMap, body: &[u8]) -> Result<Vec<u8>, (StatusCode, String)> {
    let encoding = headers
        .get(header::CONTENT_ENCODING)
        .map(|v| v.to_str())
        .transpose()
        .map_err(|_| {
            (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "invalid Content-Encoding".into(),
            )
        })?
        .unwrap_or("identity")
        .trim()
        .to_ascii_lowercase();
    match encoding.as_str() {
        "" | "identity" => Ok(body.to_vec()),
        "gzip" => {
            let mut decoder = flate2::read::GzDecoder::new(body);
            let mut out = Vec::new();
            decoder
                .by_ref()
                .take(MAX_OTLP_BODY_BYTES as u64 + 1)
                .read_to_end(&mut out)
                .map_err(|e| {
                    (
                        StatusCode::BAD_REQUEST,
                        format!("OTLP gzip decode failed: {e}"),
                    )
                })?;
            if out.len() > MAX_OTLP_BODY_BYTES {
                return Err((
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "expanded OTLP request exceeds 32 MiB".into(),
                ));
            }
            Ok(out)
        }
        _ => Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            format!("unsupported OTLP Content-Encoding {encoding}"),
        )),
    }
}

pub fn kv_to_labels(attrs: &[KeyValue]) -> Vec<LabelPair> {
    let mut labels = Vec::with_capacity(attrs.len());
    for kv in attrs {
        labels.push(LabelPair {
            key: kv.key.clone(),
            value: kv
                .value
                .as_ref()
                .map(anyvalue_to_string)
                .unwrap_or_default(),
        });
    }
    labels
}

pub fn anyvalue_to_string(value: &AnyValue) -> String {
    use any_value::Value;
    match &value.value {
        Some(Value::StringValue(s)) => s.clone(),
        Some(Value::BoolValue(v)) => v.to_string(),
        Some(Value::IntValue(v)) => v.to_string(),
        Some(Value::DoubleValue(v)) => v.to_string(),
        Some(Value::BytesValue(v)) => hex_lower(v),
        Some(Value::ArrayValue(array)) => {
            let mut out = String::from("[");
            for (index, item) in array.values.iter().enumerate() {
                if index != 0 {
                    out.push(',');
                }
                out.push_str(&anyvalue_to_string(item));
            }
            out.push(']');
            out
        }
        Some(Value::KvlistValue(list)) => {
            let mut out = String::from("{");
            for (index, item) in list.values.iter().enumerate() {
                if index != 0 {
                    out.push(',');
                }
                out.push_str(&item.key);
                out.push('=');
                if let Some(value) = &item.value {
                    out.push_str(&anyvalue_to_string(value));
                }
            }
            out.push('}');
            out
        }
        _ => String::new(),
    }
}

pub fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

pub fn insert_if_absent(labels: &mut Vec<LabelPair>, key: &str, value: &str) {
    if !value.is_empty() && !labels.iter().any(|label| label.key == key) {
        labels.push(LabelPair {
            key: key.into(),
            value: value.into(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use flate2::{write::GzEncoder, Compression};
    use std::io::Write;

    #[test]
    fn gzip_decoding_is_bounded_and_content_type_is_dispatched() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        );
        headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(br#"{}"#).unwrap();
        let body = encoder.finish().unwrap();
        assert_eq!(request_encoding(&headers).unwrap(), OtlpEncoding::Json);
        assert_eq!(decode_content(&headers, &body).unwrap(), br#"{}"#);
    }

    #[test]
    fn hex_does_not_allocate_per_byte() {
        assert_eq!(hex_lower(&[0, 0xab, 0xff]), "00abff");
    }
}
