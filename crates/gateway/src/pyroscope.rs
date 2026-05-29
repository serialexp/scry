//! Legacy Pyroscope profile ingest: `POST /ingest`.
//!
//! Shape (confirmed from `serialexp/pyroscope-bun`):
//! `POST /ingest?from=<unix-sec>&until=<unix-sec>&name=<app>&spyName=<spy>[&sampleRate=<n>]`
//! with a `multipart/form-data` body whose `profile` field is a gzipped pprof.
//! The pprof bytes are stored verbatim as an opaque `ProfileBlob` (`format = 1`,
//! pprof_gz) — the gateway never parses pprof.
//!
//! `name` may carry inline labels Pyroscope-style: `app{key=value,key2=value2}`.

use std::collections::HashMap;

use axum::{
    extract::{Multipart, Query, State},
    http::StatusCode,
};
use scry_proto::{
    generated::{ProfileBlob, ProfilesBatch},
    LabelPair,
};

use crate::upstream::{self, AppState};

/// Profile format byte: gzipped pprof. Mirrors the wire `ProfileBlob.format`
/// semantics documented in `scry_proto::streaming`.
const FORMAT_PPROF_GZ: u8 = 1;

/// Parsed metadata extracted from the `/ingest` query string.
#[derive(Debug, PartialEq)]
pub struct IngestMeta {
    pub ts_unix_nano: u64,
    pub duration_nano: u64,
    pub labels: Vec<LabelPair>,
}

/// Handle one Pyroscope `/ingest` push.
pub async fn handle(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
    mut multipart: Multipart,
) -> Result<StatusCode, (StatusCode, String)> {
    let meta = parse_ingest_params(&params);

    let mut data: Option<Vec<u8>> = None;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("multipart read failed: {e}")))?
    {
        if field.name() == Some("profile") {
            let bytes = field
                .bytes()
                .await
                .map_err(|e| (StatusCode::BAD_REQUEST, format!("reading 'profile' field failed: {e}")))?;
            data = Some(bytes.to_vec());
        }
    }

    let data = data.ok_or((
        StatusCode::BAD_REQUEST,
        "missing multipart 'profile' field".to_string(),
    ))?;

    let blob = ProfileBlob {
        ts_unix_nano: meta.ts_unix_nano,
        duration_nano: meta.duration_nano,
        labels: meta.labels,
        format: FORMAT_PPROF_GZ,
        data,
    };

    upstream::send_profiles(&state, ProfilesBatch { samples: vec![blob] })
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("upstream send failed: {e}")))?;

    Ok(StatusCode::OK)
}

/// Pure parse of the `/ingest` query params into [`IngestMeta`]. Tolerant:
/// missing/garbled timestamps default to 0, an absent `name` yields no service
/// label. `from`/`until` are unix **seconds** on the wire and converted to ns.
pub fn parse_ingest_params(params: &HashMap<String, String>) -> IngestMeta {
    let from_sec = params.get("from").and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
    let until_sec = params.get("until").and_then(|s| s.parse::<u64>().ok()).unwrap_or(from_sec);

    let ts_unix_nano = from_sec.saturating_mul(1_000_000_000);
    let duration_nano = until_sec.saturating_sub(from_sec).saturating_mul(1_000_000_000);

    let mut labels: Vec<LabelPair> = Vec::new();
    if let Some(name) = params.get("name") {
        let (app, inline) = parse_app_name(name);
        if !app.is_empty() {
            labels.push(LabelPair { key: "service.name".into(), value: app });
        }
        labels.extend(inline);
    }
    if let Some(spy) = params.get("spyName") {
        labels.push(LabelPair { key: "spy_name".into(), value: spy.clone() });
    }
    if let Some(rate) = params.get("sampleRate") {
        labels.push(LabelPair { key: "sample_rate".into(), value: rate.clone() });
    }

    IngestMeta { ts_unix_nano, duration_nano, labels }
}

/// Split a Pyroscope app name `app{k=v,k2=v2}` into the base app and its inline
/// labels. A name without braces yields the whole string as the app and no
/// labels.
fn parse_app_name(name: &str) -> (String, Vec<LabelPair>) {
    let Some(open) = name.find('{') else {
        return (name.to_string(), Vec::new());
    };
    let app = name[..open].to_string();
    let close = name.rfind('}').unwrap_or(name.len());
    let inner = if close > open { &name[open + 1..close] } else { "" };

    let mut labels = Vec::new();
    for pair in inner.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        if let Some((k, v)) = pair.split_once('=') {
            labels.push(LabelPair {
                key: k.trim().to_string(),
                value: v.trim().trim_matches('"').to_string(),
            });
        }
    }
    (app, labels)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn parses_timestamps_and_labels() {
        let p = params(&[
            ("from", "1700000000"),
            ("until", "1700000010"),
            ("name", "myapp{env=prod,region=eu}"),
            ("spyName", "bunspy"),
            ("sampleRate", "100"),
        ]);
        let meta = parse_ingest_params(&p);
        assert_eq!(meta.ts_unix_nano, 1_700_000_000_000_000_000);
        assert_eq!(meta.duration_nano, 10_000_000_000);
        assert_eq!(
            meta.labels,
            vec![
                LabelPair { key: "service.name".into(), value: "myapp".into() },
                LabelPair { key: "env".into(), value: "prod".into() },
                LabelPair { key: "region".into(), value: "eu".into() },
                LabelPair { key: "spy_name".into(), value: "bunspy".into() },
                LabelPair { key: "sample_rate".into(), value: "100".into() },
            ]
        );
    }

    #[test]
    fn name_without_braces_is_app_only() {
        let (app, labels) = parse_app_name("plain.cpu");
        assert_eq!(app, "plain.cpu");
        assert!(labels.is_empty());
    }

    #[test]
    fn tolerates_missing_timestamps() {
        let meta = parse_ingest_params(&params(&[("name", "x")]));
        assert_eq!(meta.ts_unix_nano, 0);
        assert_eq!(meta.duration_nano, 0);
        assert_eq!(meta.labels, vec![LabelPair { key: "service.name".into(), value: "x".into() }]);
    }
}
