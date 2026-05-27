//! Streaming decoders that bypass the per-record heap allocations the
//! generated `*::decode_with_decoder` paths perform.
//!
//! The generated `DummyBatch::decode_with_decoder` (and its `MetricsBatch`
//! sibling) produces a fully materialised `Vec<…Record>` where every
//! record carries owned `String` / `Vec<u8>` fields. At ingest rates of
//! a few thousand batches per second that's millions of tiny mallocs
//! per second, which glibc's arena keeps around as high-water-mark
//! slack. See `crates/block/src/dummy.rs` and CLAUDE.md § Performance
//! for the broader picture.
//!
//! This module gives callers a way to walk the same wire format
//! without ever materialising the intermediate Vec/String/Vec<u8>:
//! they implement a per-signal appender trait (typically backed by CSR
//! buffers that absorb each record with `extend_from_slice`), and the
//! signal-specific `decode_*_batch_into` entry point reads the encoded
//! payload, handing the appender borrowed slices straight out of the
//! source buffer.
//!
//! The wire format is mirrored from the generated `encode_into`; if
//! the schema's representation of any batch ever changes, both this
//! file and the generated decoder must be updated in lockstep. The
//! `streaming_matches_generated` integration tests guard that
//! agreement: they round-trip a known batch through both paths and
//! assert identical results.

use binschema_runtime::{BinSchemaError, BitOrder, BitStreamDecoder};

/// Consumer for streaming `DummyBatch` decode. Each call hands the
/// implementation borrowed slices of the wire payload; if it wants
/// to retain them past the call, it must copy them (this is the
/// entire point — let the implementation copy directly into its
/// destination buffer, with no intermediate `String` / `Vec<u8>`).
pub trait DummyAppender {
    fn append_raw(&mut self, ts_unix_nano: u64, key: &[u8], value: &[u8]);
}

/// Decode a `DummyBatch` payload into `appender`. Returns the record
/// count on success.
///
/// Performance: every per-record allocation that the generated
/// `DummyBatch::decode_with_decoder` performed is gone. The only
/// allocation cost left is the byte-level memcpys the appender does
/// internally (e.g. `extend_from_slice` into a CSR buffer), which we
/// can't avoid because parquet eventually needs the bytes in one
/// contiguous place anyway.
pub fn decode_dummy_batch_into<A: DummyAppender>(
    payload: &[u8],
    appender: &mut A,
) -> Result<usize, BinSchemaError> {
    let mut dec = BitStreamDecoder::new(payload, BitOrder::MsbFirst);
    let count = dec.read_u32_be()? as usize;
    for _ in 0..count {
        let ts = dec.read_u64_be()?;

        let key_len = dec.read_u16_be()? as usize;
        let key_start = dec.position();
        let key_end = key_start
            .checked_add(key_len)
            .ok_or(BinSchemaError::UnexpectedEof)?;
        if key_end > payload.len() {
            return Err(BinSchemaError::UnexpectedEof);
        }
        let key = &payload[key_start..key_end];
        dec.seek(key_end)?;

        let value_len = dec.read_u32_be()? as usize;
        let value_start = dec.position();
        let value_end = value_start
            .checked_add(value_len)
            .ok_or(BinSchemaError::UnexpectedEof)?;
        if value_end > payload.len() {
            return Err(BinSchemaError::UnexpectedEof);
        }
        let value = &payload[value_start..value_end];
        dec.seek(value_end)?;

        appender.append_raw(ts, key, value);
    }
    Ok(count)
}

// ── Metrics ────────────────────────────────────────────────────────────

/// Consumer for streaming `MetricsBatch` decode.
///
/// Wire shape (mirrored from `generated::MetricsBatch::encode_into`):
///
/// ```text
/// series_count: u32 BE
/// for each series:
///     fingerprint: u64 BE
///     metric_type: u8
///     label_count: u16 BE
///     for each label:
///         key_len:   u8                bytes…
///         value_len: u16 BE            bytes…
/// sample_count: u32 BE
/// for each sample:
///     fingerprint:  u64 BE
///     ts_unix_nano: u64 BE
///     value:        f64 BE (bits stored as u64 BE)
/// ```
///
/// Two call-shapes per batch:
///
/// - [`observe_series`](MetricsAppender::observe_series) once per
///   series. The labels are passed as an owned `Vec<(Vec<u8>, Vec<u8>)>`
///   because (a) it's the cold path — the spewer sends ~8 series per
///   batch versus ~400 samples — and (b) handing the appender owned
///   bytes lets it dedup-and-stash without a separate "did I see this
///   fingerprint already" round-trip back to the decoder.
/// - [`append_sample`](MetricsAppender::append_sample) once per sample.
///   Pure value types; zero allocation in the hot path.
pub trait MetricsAppender {
    fn observe_series(
        &mut self,
        fingerprint: u64,
        metric_type: u8,
        labels: Vec<(Vec<u8>, Vec<u8>)>,
    );
    fn append_sample(&mut self, fingerprint: u64, ts_unix_nano: u64, value: f64);
}

/// Decode a `MetricsBatch` payload into `appender`. Returns
/// `(series_count, sample_count)` on success.
///
/// Performance characteristics:
/// - The series-dictionary block does N small allocations per series
///   (one `Vec` of pairs, two `Vec<u8>` per label). At spewer-shaped
///   workloads (8 series × 3 labels per batch) that's ~25 mallocs per
///   batch — negligible next to zstd decompression's allocations.
/// - The sample block is entirely allocation-free: fixed-width fields
///   read straight out of the payload bytes.
pub fn decode_metrics_batch_into<A: MetricsAppender>(
    payload: &[u8],
    appender: &mut A,
) -> Result<(usize, usize), BinSchemaError> {
    let mut dec = BitStreamDecoder::new(payload, BitOrder::MsbFirst);
    let series_count = dec.read_u32_be()? as usize;
    for _ in 0..series_count {
        let fingerprint = dec.read_u64_be()?;
        let metric_type = dec.read_byte()?;
        let label_count = dec.read_u16_be()? as usize;
        let mut labels: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(label_count);
        for _ in 0..label_count {
            // Key: u8 length + bytes.
            let key_len = dec.read_byte()? as usize;
            let key_start = dec.position();
            let key_end = key_start
                .checked_add(key_len)
                .ok_or(BinSchemaError::UnexpectedEof)?;
            if key_end > payload.len() {
                return Err(BinSchemaError::UnexpectedEof);
            }
            let key = payload[key_start..key_end].to_vec();
            dec.seek(key_end)?;

            // Value: u16 BE length + bytes.
            let value_len = dec.read_u16_be()? as usize;
            let value_start = dec.position();
            let value_end = value_start
                .checked_add(value_len)
                .ok_or(BinSchemaError::UnexpectedEof)?;
            if value_end > payload.len() {
                return Err(BinSchemaError::UnexpectedEof);
            }
            let value = payload[value_start..value_end].to_vec();
            dec.seek(value_end)?;

            labels.push((key, value));
        }
        appender.observe_series(fingerprint, metric_type, labels);
    }

    let sample_count = dec.read_u32_be()? as usize;
    for _ in 0..sample_count {
        let fingerprint = dec.read_u64_be()?;
        let ts = dec.read_u64_be()?;
        let value = f64::from_bits(dec.read_u64_be()?);
        appender.append_sample(fingerprint, ts, value);
    }
    Ok((series_count, sample_count))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated::{DummyBatch, DummyRecord};

    #[derive(Default, Debug, PartialEq)]
    struct Collected {
        records: Vec<(u64, Vec<u8>, Vec<u8>)>,
    }

    impl DummyAppender for Collected {
        fn append_raw(&mut self, ts: u64, key: &[u8], value: &[u8]) {
            self.records.push((ts, key.to_vec(), value.to_vec()));
        }
    }

    /// Encode a hand-built `DummyBatch` through the generated path,
    /// then decode it through the streaming path, and assert the
    /// streaming consumer sees exactly the same records.
    #[test]
    fn streaming_matches_generated() {
        let batch = DummyBatch {
            records: vec![
                DummyRecord {
                    ts_unix_nano: 100,
                    key: "first".into(),
                    value: vec![1, 2, 3],
                },
                DummyRecord {
                    ts_unix_nano: 200,
                    key: "second-with-a-longer-key".into(),
                    value: (0..255u16).map(|b| b as u8).collect(),
                },
                DummyRecord {
                    // Empty key + empty value — make sure the zero-length
                    // path doesn't trip the bounds check.
                    ts_unix_nano: 300,
                    key: "".into(),
                    value: vec![],
                },
            ],
        };
        let payload = batch.encode().expect("encode");

        let mut collected = Collected::default();
        let n = decode_dummy_batch_into(&payload, &mut collected).expect("decode");
        assert_eq!(n, batch.records.len());
        assert_eq!(collected.records.len(), batch.records.len());
        for (got, want) in collected.records.iter().zip(batch.records.iter()) {
            assert_eq!(got.0, want.ts_unix_nano);
            assert_eq!(&got.1, want.key.as_bytes());
            assert_eq!(&got.2, &want.value);
        }
    }

    /// Empty batch — record_count = 0, no record bytes. Should
    /// produce zero appends and not error.
    #[test]
    fn streaming_handles_empty_batch() {
        let batch = DummyBatch { records: vec![] };
        let payload = batch.encode().expect("encode");
        let mut collected = Collected::default();
        let n = decode_dummy_batch_into(&payload, &mut collected).expect("decode");
        assert_eq!(n, 0);
        assert!(collected.records.is_empty());
    }

    /// Truncated payload — claim N records but cut the bytes short.
    /// Should error, not panic.
    #[test]
    fn streaming_rejects_truncated_payload() {
        let batch = DummyBatch {
            records: vec![DummyRecord {
                ts_unix_nano: 42,
                key: "k".into(),
                value: vec![7, 7, 7],
            }],
        };
        let mut payload = batch.encode().expect("encode");
        let truncated_len = payload.len() - 2;
        payload.truncate(truncated_len);

        let mut collected = Collected::default();
        let result = decode_dummy_batch_into(&payload, &mut collected);
        assert!(result.is_err(), "expected error, got {:?}", result);
    }

    // ── Metrics ─────────────────────────────────────────────────────

    use crate::generated::{LabelPair, MetricSample, MetricsBatch, SeriesDictEntry};

    #[derive(Default, Debug, PartialEq)]
    struct MetricsCollected {
        series: Vec<(u64, u8, Vec<(Vec<u8>, Vec<u8>)>)>,
        samples: Vec<(u64, u64, f64)>,
    }

    impl MetricsAppender for MetricsCollected {
        fn observe_series(
            &mut self,
            fingerprint: u64,
            metric_type: u8,
            labels: Vec<(Vec<u8>, Vec<u8>)>,
        ) {
            self.series.push((fingerprint, metric_type, labels));
        }
        fn append_sample(&mut self, fingerprint: u64, ts: u64, value: f64) {
            self.samples.push((fingerprint, ts, value));
        }
    }

    #[test]
    fn metrics_streaming_matches_generated() {
        let batch = MetricsBatch {
            series: vec![
                SeriesDictEntry {
                    fingerprint: 0xCAFE_BABE,
                    metric_type: 2, // gauge
                    labels: vec![
                        LabelPair { key: "__name__".into(), value: "cpu_usage".into() },
                        LabelPair { key: "host".into(),     value: "host-1".into() },
                    ],
                },
                SeriesDictEntry {
                    fingerprint: 0xDEAD_BEEF,
                    metric_type: 1, // counter
                    labels: vec![
                        LabelPair { key: "__name__".into(), value: "http_requests_total".into() },
                        LabelPair { key: "service".into(),  value: "api".into() },
                        LabelPair { key: "status".into(),   value: "200".into() },
                    ],
                },
            ],
            samples: vec![
                MetricSample { fingerprint: 0xCAFE_BABE, ts_unix_nano: 1_000, value:  3.14 },
                MetricSample { fingerprint: 0xCAFE_BABE, ts_unix_nano: 2_000, value:  6.28 },
                MetricSample { fingerprint: 0xDEAD_BEEF, ts_unix_nano: 1_500, value: 42.0  },
                MetricSample { fingerprint: 0xDEAD_BEEF, ts_unix_nano: 2_500, value: 43.0  },
            ],
        };
        let payload = batch.encode().expect("encode");

        let mut collected = MetricsCollected::default();
        let (n_series, n_samples) =
            decode_metrics_batch_into(&payload, &mut collected).expect("decode");
        assert_eq!(n_series, batch.series.len());
        assert_eq!(n_samples, batch.samples.len());
        assert_eq!(collected.series.len(), batch.series.len());
        for (got, want) in collected.series.iter().zip(batch.series.iter()) {
            assert_eq!(got.0, want.fingerprint);
            assert_eq!(got.1, want.metric_type);
            assert_eq!(got.2.len(), want.labels.len());
            for (gl, wl) in got.2.iter().zip(want.labels.iter()) {
                assert_eq!(&gl.0, wl.key.as_bytes());
                assert_eq!(&gl.1, wl.value.as_bytes());
            }
        }
        for (got, want) in collected.samples.iter().zip(batch.samples.iter()) {
            assert_eq!(got.0, want.fingerprint);
            assert_eq!(got.1, want.ts_unix_nano);
            assert_eq!(got.2, want.value);
        }
    }

    #[test]
    fn metrics_streaming_handles_empty_batch() {
        let batch = MetricsBatch { series: vec![], samples: vec![] };
        let payload = batch.encode().expect("encode");
        let mut collected = MetricsCollected::default();
        let (n_series, n_samples) =
            decode_metrics_batch_into(&payload, &mut collected).expect("decode");
        assert_eq!(n_series, 0);
        assert_eq!(n_samples, 0);
        assert!(collected.series.is_empty());
        assert!(collected.samples.is_empty());
    }

    #[test]
    fn metrics_streaming_rejects_truncated_payload() {
        let batch = MetricsBatch {
            series: vec![SeriesDictEntry {
                fingerprint: 1,
                metric_type: 2,
                labels: vec![LabelPair { key: "k".into(), value: "v".into() }],
            }],
            samples: vec![
                MetricSample { fingerprint: 1, ts_unix_nano: 1, value: 1.0 },
                MetricSample { fingerprint: 1, ts_unix_nano: 2, value: 2.0 },
            ],
        };
        let mut payload = batch.encode().expect("encode");
        // Drop the trailing bytes mid-sample.
        let truncated_len = payload.len() - 4;
        payload.truncate(truncated_len);

        let mut collected = MetricsCollected::default();
        let result = decode_metrics_batch_into(&payload, &mut collected);
        assert!(result.is_err(), "expected error, got {:?}", result);
    }
}
