//! Random batch generators per signal. Each `make_*` returns a fully
//! built [`Frame`] (Batch variant) whose payload is zstd-compressed
//! according to the schema. Records, labels, and timestamps are all
//! randomised but stay within plausible ranges so the sink's decoders
//! exercise realistic paths.

use binschema_runtime::BinSchemaError;
use rand::distributions::{Alphanumeric, Distribution};
use rand::Rng;
use rand_distr::Normal;
use scry_proto::{
    build,
    constants::{Signal, COMPRESSION_ZSTD},
    fingerprint::fingerprint,
    generated::{
        DummyBatch, DummyRecord, LogEntry, LogStream, LogsBatch, MetricSample, MetricsBatch,
        ProfileBlob, ProfilesBatch, ResourceEntry, ScopeEntry, SeriesDictEntry, Span, SpanEvent,
        SpanLink, TracesBatch,
    },
    Frame, LabelPair,
};
use std::time::{SystemTime, UNIX_EPOCH};

const ZSTD_LEVEL: i32 = 3;

pub fn make_batch<R: Rng>(rng: &mut R, signal: Signal, session_id: u64, batch_id: u64) -> Frame {
    let now_ns = unix_nanos_now();
    let (record_count, payload_uncompressed, ts_min, ts_max) = match signal {
        Signal::Metrics => render_metrics(rng, now_ns),
        Signal::Logs => render_logs(rng, now_ns),
        Signal::Traces => render_traces(rng, now_ns),
        Signal::Profiles => render_profiles(rng, now_ns),
        Signal::Dummy => render_dummy(rng, now_ns),
    };

    let payload = zstd::encode_all(payload_uncompressed.as_slice(), ZSTD_LEVEL)
        .expect("zstd encode_all is infallible on Vec input");

    build::batch(build::BatchArgs {
        session_id,
        batch_id,
        signal: signal.as_u8(),
        ts_min_unix_nano: ts_min,
        ts_max_unix_nano: ts_max,
        record_count,
        compression: COMPRESSION_ZSTD,
        uncompressed_size: payload_uncompressed.len() as u32,
        payload,
    })
}

fn unix_nanos_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn rand_string<R: Rng>(rng: &mut R, len: usize) -> String {
    (0..len).map(|_| Alphanumeric.sample(rng) as char).collect()
}

fn labels_for<R: Rng>(rng: &mut R, base_name: &str) -> Vec<LabelPair> {
    let host_idx: u32 = rng.gen_range(1..=8);
    let env = ["prod", "stage", "dev"][rng.gen_range(0..3)];
    vec![
        LabelPair {
            key: "__name__".into(),
            value: base_name.into(),
        },
        LabelPair {
            key: "host".into(),
            value: format!("host-{host_idx}"),
        },
        LabelPair {
            key: "env".into(),
            value: env.into(),
        },
        LabelPair {
            key: "region".into(),
            value: "eu-central".into(),
        },
    ]
}

// ── Metrics ────────────────────────────────────────────────────────────

fn render_metrics<R: Rng>(rng: &mut R, now_ns: u64) -> (u32, Vec<u8>, u64, u64) {
    // 8 series, 50 samples each = 400 records.
    let metric_names = [
        "scry_cpu_seconds_total",
        "scry_http_requests_total",
        "scry_gc_pause_seconds",
        "scry_disk_used_bytes",
        "scry_mem_rss_bytes",
        "scry_open_fds",
        "scry_request_latency_ms",
        "scry_errors_total",
    ];

    let mut series: Vec<SeriesDictEntry> = Vec::with_capacity(metric_names.len());
    for name in &metric_names {
        let labels = labels_for(rng, name);
        series.push(SeriesDictEntry {
            fingerprint: fingerprint(&labels),
            metric_type: rng.gen_range(1..=2), // counter / gauge
            labels,
        });
    }

    let samples_per_series = 50u32;
    let total_samples = (series.len() as u32) * samples_per_series;
    let mut samples = Vec::with_capacity(total_samples as usize);
    let dist = Normal::new(0.0, 5.0).unwrap();
    let mut ts_min = u64::MAX;
    let mut ts_max = 0u64;

    for s in &series {
        let mut value: f64 = rng.gen_range(0.0..1000.0);
        for i in 0..samples_per_series {
            // 50 samples spaced 100 ms apart, ending "now".
            let ts = now_ns - ((samples_per_series - 1 - i) as u64) * 100 * 1_000_000;
            value += dist.sample(rng);
            samples.push(MetricSample {
                fingerprint: s.fingerprint,
                ts_unix_nano: ts,
                value,
            });
            ts_min = ts_min.min(ts);
            ts_max = ts_max.max(ts);
        }
    }

    let payload = MetricsBatch { series, samples };
    let bytes = encode(&payload, |b, e| b.encode_into(e));
    (total_samples, bytes, ts_min, ts_max)
}

// ── Logs ───────────────────────────────────────────────────────────────

fn render_logs<R: Rng>(rng: &mut R, now_ns: u64) -> (u32, Vec<u8>, u64, u64) {
    // 3 streams, 20 lines each = 60 records.
    let services = ["api", "worker", "scheduler"];
    let mut streams = Vec::with_capacity(services.len());
    let mut total: u32 = 0;
    let mut ts_min = u64::MAX;
    let mut ts_max = 0u64;

    for svc in services {
        let labels = vec![
            LabelPair {
                key: "service".into(),
                value: svc.into(),
            },
            LabelPair {
                key: "host".into(),
                value: format!("host-{}", rng.gen_range(1..=4)),
            },
            LabelPair {
                key: "env".into(),
                value: "prod".into(),
            },
        ];
        let mut entries = Vec::with_capacity(20);
        for i in 0..20 {
            let ts = now_ns - ((19 - i) as u64) * 50 * 1_000_000;
            ts_min = ts_min.min(ts);
            ts_max = ts_max.max(ts);
            entries.push(LogEntry {
                ts_unix_nano: ts,
                severity: [5u8, 9, 13, 17][rng.gen_range(0..4)],
                body: format!(
                    "request {} processed in {}ms",
                    rand_string(rng, 8),
                    rng.gen_range(1..500)
                ),
                attributes: vec![
                    LabelPair {
                        key: "trace_id".into(),
                        value: rand_string(rng, 32).to_lowercase(),
                    },
                    LabelPair {
                        key: "status".into(),
                        value: [200u16, 201, 400, 500][rng.gen_range(0..4)].to_string(),
                    },
                ],
            });
            total += 1;
        }
        streams.push(LogStream {
            fingerprint: fingerprint(&labels),
            labels,
            entries,
        });
    }

    let payload = LogsBatch { streams };
    let bytes = encode(&payload, |b, e| b.encode_into(e));
    (total, bytes, ts_min, ts_max)
}

// ── Traces ─────────────────────────────────────────────────────────────

fn render_traces<R: Rng>(rng: &mut R, now_ns: u64) -> (u32, Vec<u8>, u64, u64) {
    // 5 traces, 4 spans each = 20 spans.
    let resources = vec![
        ResourceEntry {
            labels: vec![
                LabelPair {
                    key: "service.name".into(),
                    value: "api".into(),
                },
                LabelPair {
                    key: "service.namespace".into(),
                    value: "shop".into(),
                },
                LabelPair {
                    key: "deployment.environment".into(),
                    value: "prod".into(),
                },
                LabelPair {
                    key: "host".into(),
                    value: "host-1".into(),
                },
            ],
        },
        ResourceEntry {
            labels: vec![
                LabelPair {
                    key: "service.name".into(),
                    value: "worker".into(),
                },
                LabelPair {
                    key: "service.namespace".into(),
                    value: "shop".into(),
                },
                LabelPair {
                    key: "deployment.environment".into(),
                    value: "staging".into(),
                },
                LabelPair {
                    key: "host".into(),
                    value: "host-2".into(),
                },
            ],
        },
    ];
    let scopes = vec![
        ScopeEntry {
            name: "scry.spewer".into(),
            version: "0.1.0".into(),
        },
        ScopeEntry {
            name: "scry.spewer.tokio".into(),
            version: "1.0".into(),
        },
    ];

    let mut spans: Vec<Span> = Vec::with_capacity(20);
    let mut ts_min = u64::MAX;
    let mut ts_max = 0u64;

    for _trace in 0..5 {
        let trace_id: [u8; 16] = rng.gen();
        let root_span_id: [u8; 8] = rng.gen();
        let trace_start = now_ns - rng.gen_range(0..10_000_000_000u64);

        for span_idx in 0..4 {
            let span_id: [u8; 8] = if span_idx == 0 {
                root_span_id
            } else {
                rng.gen()
            };
            let parent = if span_idx == 0 {
                None
            } else {
                Some(root_span_id.to_vec())
            };
            let dur_ns: u64 = rng.gen_range(100_000..50_000_000);
            let start = trace_start + (span_idx as u64) * 1_000_000;
            let end = start + dur_ns;
            ts_min = ts_min.min(start);
            ts_max = ts_max.max(end);
            spans.push(Span {
                resource_idx: rng.gen_range(0..resources.len() as u16),
                scope_idx: rng.gen_range(0..scopes.len() as u16),
                trace_id: trace_id.to_vec(),
                span_id: span_id.to_vec(),
                parent_span_id: parent,
                name: format!("op.{}", rand_string(rng, 6)),
                kind: rng.gen_range(1..=5),
                start_unix_nano: start,
                end_unix_nano: end,
                status_code: if rng.gen_bool(0.05) { 2 } else { 1 },
                status_message: String::new(),
                attributes: vec![
                    LabelPair {
                        key: "http.method".into(),
                        value: ["GET", "POST", "PUT"][rng.gen_range(0..3)].into(),
                    },
                    LabelPair {
                        key: "http.status".into(),
                        value: [200u16, 404, 500][rng.gen_range(0..3)].to_string(),
                    },
                ],
                events: if rng.gen_bool(0.3) {
                    vec![SpanEvent {
                        ts_unix_nano: start + dur_ns / 2,
                        name: "checkpoint".into(),
                        attributes: vec![],
                    }]
                } else {
                    vec![]
                },
                links: vec![SpanLink {
                    trace_id: trace_id.to_vec(),
                    span_id: root_span_id.to_vec(),
                    attributes: vec![],
                }],
            });
        }
    }

    let total = spans.len() as u32;
    let payload = TracesBatch {
        resources,
        scopes,
        spans,
    };
    let bytes = encode(&payload, |b, e| b.encode_into(e));
    (total, bytes, ts_min, ts_max)
}

// ── Profiles ───────────────────────────────────────────────────────────

fn render_profiles<R: Rng>(rng: &mut R, now_ns: u64) -> (u32, Vec<u8>, u64, u64) {
    // 1 blob per batch — profiles are heavy enough that one is plenty.
    let blob_size = rng.gen_range(8_000..32_000);
    let mut data = vec![0u8; blob_size];
    rng.fill(&mut data[..]);

    let blob = ProfileBlob {
        ts_unix_nano: now_ns,
        duration_nano: rng.gen_range(1_000_000_000..30_000_000_000), // 1-30 s
        labels: vec![
            LabelPair {
                key: "service".into(),
                value: "noise-spewer".into(),
            },
            LabelPair {
                key: "profile.type".into(),
                value: ["cpu", "heap", "goroutine"][rng.gen_range(0..3)].into(),
            },
        ],
        format: 1, // pprof_gz (placeholder; it's actually random bytes for the spewer)
        data,
    };

    let payload = ProfilesBatch {
        samples: vec![blob],
    };
    let bytes = encode(&payload, |b, e| b.encode_into(e));
    (1, bytes, now_ns, now_ns)
}

// ── Dummy (v0.1-only) ──────────────────────────────────────────────────

fn render_dummy<R: Rng>(rng: &mut R, now_ns: u64) -> (u32, Vec<u8>, u64, u64) {
    // 256 records per batch, 50 ms apart, random key/value. Small enough
    // to keep the WAL/block builder honest under load; large enough that
    // a single batch produces a non-trivial parquet row group.
    const N: u32 = 256;
    let mut records = Vec::with_capacity(N as usize);
    let mut ts_min = u64::MAX;
    let mut ts_max = 0u64;
    for i in 0..N {
        let ts = now_ns - ((N - 1 - i) as u64) * 50 * 1_000_000;
        ts_min = ts_min.min(ts);
        ts_max = ts_max.max(ts);
        let value_len = rng.gen_range(8..64);
        let mut value = vec![0u8; value_len];
        rng.fill(&mut value[..]);
        records.push(DummyRecord {
            ts_unix_nano: ts,
            key: format!("k.{}", rand_string(rng, 6)),
            value,
        });
    }
    let payload = DummyBatch { records };
    let bytes = encode(&payload, |b, e| b.encode_into(e));
    (N, bytes, ts_min, ts_max)
}

// ── Encode helper ──────────────────────────────────────────────────────

fn encode<T, F>(value: &T, encode_into: F) -> Vec<u8>
where
    F: Fn(&T, &mut binschema_runtime::BitStreamEncoder) -> Result<(), BinSchemaError>,
{
    let mut encoder =
        binschema_runtime::BitStreamEncoder::new(binschema_runtime::BitOrder::MsbFirst);
    encode_into(value, &mut encoder).expect("payload encode is infallible for well-formed inputs");
    encoder.finish()
}
