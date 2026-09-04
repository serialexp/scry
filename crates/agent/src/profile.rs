//! Kubernetes pprof target model and bounded CPU-profile puller.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use futures::StreamExt;
use prost::Message;
use scry_proto::{generated::ProfileBlob, LabelPair};

/// A single pod-selected regular container and its pprof endpoint.
#[derive(Debug, Clone, PartialEq)]
pub struct ProfileTarget {
    pub key: String,
    pub url: String,
    pub labels: Vec<LabelPair>,
    /// Runtime-qualified ID (`containerd://…`, `cri-o://…`) retained for a future
    /// process-sampling backend. HTTP pulling does not use it.
    pub container_id: Option<String>,
}

#[derive(Debug, Default)]
pub struct ProfileStats {
    pub pull_failures: AtomicU64,
    pub backpressure_drops: AtomicU64,
}

/// Reconcile pod-discovered targets into one non-overlapping pull task per target.
/// A changed target value replaces its task even when its stable key is unchanged.
#[allow(clippy::too_many_arguments)]
pub fn spawn_scheduler(
    targets: crate::discovery::ProfileTargetRegistry,
    http: reqwest::Client,
    interval: Duration,
    duration: Duration,
    max_body_bytes: usize,
    reconcile_interval: Duration,
    static_labels: Vec<LabelPair>,
    tx: tokio::sync::mpsc::Sender<ProfileBlob>,
    stats: Arc<ProfileStats>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut active: HashMap<String, (ProfileTarget, tokio::task::JoinHandle<()>)> =
            HashMap::new();
        loop {
            let mut desired = targets.read().await.clone();
            for target in desired.values_mut() {
                if !static_labels.is_empty() {
                    target
                        .labels
                        .retain(|label| !static_labels.iter().any(|fixed| fixed.key == label.key));
                    target.labels.extend_from_slice(&static_labels);
                }
            }
            active.retain(|key, (target, handle)| {
                if desired.get(key) == Some(target) {
                    true
                } else {
                    handle.abort();
                    false
                }
            });
            for (key, target) in desired {
                if active.contains_key(&key) {
                    continue;
                }
                let handle = spawn_pull_task(
                    target.clone(),
                    http.clone(),
                    interval,
                    duration,
                    max_body_bytes,
                    tx.clone(),
                    stats.clone(),
                    shutdown.clone(),
                );
                active.insert(key, (target, handle));
            }
            tokio::select! {
                _ = tokio::time::sleep(reconcile_interval) => {}
                _ = shutdown.changed() => if *shutdown.borrow() { break; },
            }
        }
        for (_, (_, handle)) in active {
            handle.abort();
        }
    })
}

#[allow(clippy::too_many_arguments)]
fn spawn_pull_task(
    target: ProfileTarget,
    http: reqwest::Client,
    interval: Duration,
    duration: Duration,
    max_body_bytes: usize,
    tx: tokio::sync::mpsc::Sender<ProfileBlob>,
    stats: Arc<ProfileStats>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => match pull_once(&http, &target, duration, max_body_bytes).await {
                    Ok(blob) => if tx.try_send(blob).is_err() {
                        stats.backpressure_drops.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(target = %target.key, "dropping profile because outbound queue is full");
                    },
                    Err(error) => {
                        stats.pull_failures.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(target = %target.key, %error, "pprof pull failed");
                    }
                },
                _ = shutdown.changed() => if *shutdown.borrow() { break; },
            }
        }
    })
}

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

pub async fn pull_once(
    http: &reqwest::Client,
    target: &ProfileTarget,
    duration: Duration,
    max_body_bytes: usize,
) -> Result<ProfileBlob> {
    let started_ns = now_unix_nano();
    let started = Instant::now();
    let seconds = duration.as_secs().max(1);
    let separator = if target.url.contains('?') { '&' } else { '?' };
    let url = format!("{}{separator}seconds={seconds}", target.url);
    let response = http
        .get(&url)
        .send()
        .await
        .context("sending pprof request")?;
    let status = response.status();
    if !status.is_success() {
        bail!("pprof target returned HTTP {status}");
    }
    if response
        .content_length()
        .is_some_and(|n| n > max_body_bytes as u64)
    {
        bail!("pprof response exceeds {max_body_bytes} bytes");
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("reading pprof response")?;
        if body.len().saturating_add(chunk.len()) > max_body_bytes {
            bail!("pprof response exceeds {max_body_bytes} bytes");
        }
        body.extend_from_slice(&chunk);
    }
    normalize_profile(target, &body, started_ns, started.elapsed(), max_body_bytes)
}

fn normalize_profile(
    target: &ProfileTarget,
    body: &[u8],
    fallback_ts: u64,
    fallback_duration: Duration,
    max_body_bytes: usize,
) -> Result<ProfileBlob> {
    let raw = if body.starts_with(&[0x1f, 0x8b]) {
        let mut decoder = GzDecoder::new(body);
        let mut raw = Vec::new();
        decoder
            .by_ref()
            .take(max_body_bytes as u64 + 1)
            .read_to_end(&mut raw)
            .context("decoding gzipped pprof")?;
        if raw.len() > max_body_bytes {
            bail!("expanded pprof exceeds {max_body_bytes} bytes");
        }
        raw
    } else {
        body.to_vec()
    };
    let metadata = PprofMetadata::decode(raw.as_slice()).context("decoding pprof")?;
    if metadata.string_table.first().map(String::as_str) != Some("")
        || metadata.sample_type.is_empty()
    {
        bail!("pprof requires empty string-table entry zero and a sample type");
    }
    if metadata.time_nanos < 0 || metadata.duration_nanos < 0 {
        bail!("pprof timestamp and duration must be non-negative");
    }
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&raw).context("compressing pprof")?;
    let data = encoder.finish().context("finishing pprof gzip")?;
    if data.len() > max_body_bytes {
        bail!("normalized pprof exceeds {max_body_bytes} bytes");
    }
    Ok(ProfileBlob {
        ts_unix_nano: if metadata.time_nanos == 0 {
            fallback_ts
        } else {
            metadata.time_nanos as u64
        },
        duration_nano: if metadata.duration_nanos == 0 {
            fallback_duration.as_nanos() as u64
        } else {
            metadata.duration_nanos as u64
        },
        labels: target.labels.clone(),
        format: 1,
        data,
    })
}

pub fn now_unix_nano() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target() -> ProfileTarget {
        ProfileTarget {
            key: "uid/api".into(),
            url: "http://localhost/profile".into(),
            labels: vec![LabelPair {
                key: "pod".into(),
                value: "api-1".into(),
            }],
            container_id: Some("containerd://abc".into()),
        }
    }

    fn raw_profile(time_nanos: i64, duration_nanos: i64) -> Vec<u8> {
        PprofMetadata {
            sample_type: vec![PprofValueType { r#type: 1, unit: 2 }],
            string_table: vec![String::new(), "cpu".into(), "nanoseconds".into()],
            time_nanos,
            duration_nanos,
        }
        .encode_to_vec()
    }

    #[test]
    fn normalizes_raw_pprof_and_uses_embedded_time() {
        let blob = normalize_profile(
            &target(),
            &raw_profile(123, 456),
            10,
            Duration::from_secs(2),
            1024,
        )
        .unwrap();
        assert_eq!(
            (blob.ts_unix_nano, blob.duration_nano, blob.format),
            (123, 456, 1)
        );
        assert!(blob.data.starts_with(&[0x1f, 0x8b]));
        let mut decoded = Vec::new();
        GzDecoder::new(blob.data.as_slice())
            .read_to_end(&mut decoded)
            .unwrap();
        assert_eq!(decoded, raw_profile(123, 456));
    }

    #[test]
    fn normalizes_gzip_and_falls_back_for_zero_time() {
        let raw = raw_profile(0, 0);
        let mut gz = GzEncoder::new(Vec::new(), Compression::default());
        gz.write_all(&raw).unwrap();
        let gz = gz.finish().unwrap();
        let blob = normalize_profile(&target(), &gz, 99, Duration::from_millis(3), 1024).unwrap();
        assert_eq!(blob.ts_unix_nano, 99);
        assert_eq!(blob.duration_nano, 3_000_000);
    }

    #[test]
    fn rejects_malformed_and_expanded_oversize_profiles() {
        assert!(normalize_profile(&target(), b"nope", 0, Duration::ZERO, 1024).is_err());
        let raw = raw_profile(1, 1);
        assert!(normalize_profile(&target(), &raw, 0, Duration::ZERO, raw.len() - 1).is_err());
    }

    #[tokio::test]
    async fn pull_fetches_expected_duration_query_and_normalizes_body() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let raw = raw_profile(123, 456);
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 2048];
            let read = stream.read(&mut request).await.unwrap();
            let request = String::from_utf8_lossy(&request[..read]);
            assert!(request.starts_with("GET /profile?existing=yes&seconds=7 HTTP/1.1"));
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        raw.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            stream.write_all(&raw).await.unwrap();
        });
        let mut target = target();
        target.url = format!("http://{addr}/profile?existing=yes");
        let blob = pull_once(
            &reqwest::Client::new(),
            &target,
            Duration::from_secs(7),
            1024,
        )
        .await
        .unwrap();
        assert_eq!(blob.ts_unix_nano, 123);
        assert!(blob.data.starts_with(&[0x1f, 0x8b]));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn pull_rejects_declared_oversize_before_reading_body() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 1024];
            let _ = stream.read(&mut request).await.unwrap();
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2048\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
        });
        let mut target = target();
        target.url = format!("http://{addr}/profile");
        let error = pull_once(
            &reqwest::Client::new(),
            &target,
            Duration::from_secs(1),
            1024,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("exceeds"));
        server.await.unwrap();
    }
}
