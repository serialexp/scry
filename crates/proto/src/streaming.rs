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

// ── Logs ───────────────────────────────────────────────────────────────

/// Consumer for streaming `LogsBatch` decode.
///
/// Wire shape (mirrored from `generated::LogsBatch::encode_into`,
/// `LogStream::encode_into`, and `LogEntry::encode_into`):
///
/// ```text
/// stream_count: u32 BE
/// for each stream:
///     fingerprint:  u64 BE
///     label_count:  u16 BE
///     for each label:
///         key_len:   u8                bytes…
///         value_len: u16 BE            bytes…
///     entry_count:  u32 BE
///     for each entry:
///         ts_unix_nano: u64 BE
///         severity:     u8
///         body_len:     u32 BE         bytes…   (utf8; not validated here)
///         attr_count:   u16 BE
///         for each attr:
///             key_len:   u8            bytes…
///             value_len: u16 BE        bytes…
/// ```
///
/// Two call shapes per batch:
///
/// - [`observe_stream`](LogsAppender::observe_stream) once per stream.
///   Stream-level labels mirror metrics' series labels: cold path
///   (~3 streams per batch in the noise-spewer workload), so we
///   hand the appender owned bytes — same trade-off as
///   `MetricsAppender::observe_series`.
/// - [`append_entry`](LogsAppender::append_entry) once per log
///   entry. `body` and per-entry `attributes` are also owned; the
///   body can be large (a log line is typically hundreds of bytes)
///   but it's one allocation per entry, not per attribute, and the
///   appender's destination is typically a `Vec<String>` anyway.
///   No UTF-8 validation in this path — the appender decides
///   whether to error / coerce / pass through (Parquet's Utf8
///   writer requires valid UTF-8; `String::from_utf8_lossy` is
///   the conventional cheap coercion).
pub trait LogsAppender {
    fn observe_stream(&mut self, fingerprint: u64, labels: Vec<(Vec<u8>, Vec<u8>)>);

    fn append_entry(
        &mut self,
        fingerprint: u64,
        ts_unix_nano: u64,
        severity: u8,
        body: Vec<u8>,
        attributes: Vec<(Vec<u8>, Vec<u8>)>,
    );
}

/// Decode a `LogsBatch` payload into `appender`. Returns the total
/// entry count across all streams on success.
///
/// Performance characteristics:
/// - The stream-dictionary block does small allocations per stream
///   label (one `Vec<u8>` per key and value). At spewer-shaped
///   workloads (3 streams × 3 labels) that's ~18 mallocs per batch
///   — negligible.
/// - The entry block does N allocations for the body Vec<u8> plus
///   M per-attribute Vec<u8> pairs (typically 2 attrs per entry =
///   5 allocations per entry). At 60 entries per batch that's
///   ~300 mallocs per batch — still cheaper than the zstd
///   decompression cost on the same payload. The win versus the
///   generated path is that we never materialise an intermediate
///   `LogsBatch` Vec — entries flow straight into the builder's
///   CSR buffers without a per-batch parent Vec / String pool.
pub fn decode_logs_batch_into<A: LogsAppender>(
    payload: &[u8],
    appender: &mut A,
) -> Result<usize, BinSchemaError> {
    let mut dec = BitStreamDecoder::new(payload, BitOrder::MsbFirst);
    let stream_count = dec.read_u32_be()? as usize;
    let mut total_entries: usize = 0;

    for _ in 0..stream_count {
        let fingerprint = dec.read_u64_be()?;
        let label_count = dec.read_u16_be()? as usize;
        let mut labels: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(label_count);
        for _ in 0..label_count {
            let (k, v) = read_label_pair(&mut dec, payload)?;
            labels.push((k, v));
        }
        appender.observe_stream(fingerprint, labels);

        let entry_count = dec.read_u32_be()? as usize;
        for _ in 0..entry_count {
            let ts = dec.read_u64_be()?;
            let severity = dec.read_byte()?;

            // body: u32 BE length + bytes (utf8 on the wire,
            // not validated here — see trait doc).
            let body_len = dec.read_u32_be()? as usize;
            let body_start = dec.position();
            let body_end = body_start
                .checked_add(body_len)
                .ok_or(BinSchemaError::UnexpectedEof)?;
            if body_end > payload.len() {
                return Err(BinSchemaError::UnexpectedEof);
            }
            let body = payload[body_start..body_end].to_vec();
            dec.seek(body_end)?;

            let attr_count = dec.read_u16_be()? as usize;
            let mut attrs: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(attr_count);
            for _ in 0..attr_count {
                let (k, v) = read_label_pair(&mut dec, payload)?;
                attrs.push((k, v));
            }

            appender.append_entry(fingerprint, ts, severity, body, attrs);
            total_entries += 1;
        }
    }

    Ok(total_entries)
}

/// Shared LabelPair reader for the metrics + logs streaming
/// decoders. Wire shape: `u8 key_len + key bytes + u16 BE value_len
/// + value bytes`. Mirrors the generated `LabelPair::encode_into`.
fn read_label_pair(
    dec: &mut BitStreamDecoder<'_>,
    payload: &[u8],
) -> Result<(Vec<u8>, Vec<u8>), BinSchemaError> {
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

    Ok((key, value))
}

/// Read `len` fixed bytes as a slice borrowed straight out of `payload`
/// (no copy), advancing the decoder. Used for the fixed-width id fields
/// in spans (`trace_id`/`span_id`/`parent_span_id`).
fn read_fixed<'p>(
    dec: &mut BitStreamDecoder<'_>,
    payload: &'p [u8],
    len: usize,
) -> Result<&'p [u8], BinSchemaError> {
    let start = dec.position();
    let end = start
        .checked_add(len)
        .ok_or(BinSchemaError::UnexpectedEof)?;
    if end > payload.len() {
        return Err(BinSchemaError::UnexpectedEof);
    }
    dec.seek(end)?;
    Ok(&payload[start..end])
}

/// Read a `u16 BE`-length-prefixed byte run as a borrowed slice.
fn read_str_u16<'p>(
    dec: &mut BitStreamDecoder<'_>,
    payload: &'p [u8],
) -> Result<&'p [u8], BinSchemaError> {
    let len = dec.read_u16_be()? as usize;
    read_fixed(dec, payload, len)
}

/// Read a `u8`-length-prefixed byte run as a borrowed slice.
fn read_str_u8<'p>(
    dec: &mut BitStreamDecoder<'_>,
    payload: &'p [u8],
) -> Result<&'p [u8], BinSchemaError> {
    let len = dec.read_byte()? as usize;
    read_fixed(dec, payload, len)
}

/// Read a `u8`-count run of `LabelPair`s as owned `(Vec<u8>, Vec<u8>)`s.
/// Span events/links use a `u8` attribute count (vs the `u16` on the span
/// itself); this is the cold path either way.
// The `Vec<(Vec<u8>, Vec<u8>)>` label-pair shape is the crate-wide
// convention (see the appender traits + `DecodedEvent`/`DecodedLink`); it
// reads clearly here, so opt this helper out of `type_complexity` rather
// than introduce a one-off alias that diverges from those signatures.
#[allow(clippy::type_complexity)]
fn read_attrs_u8(
    dec: &mut BitStreamDecoder<'_>,
    payload: &[u8],
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, BinSchemaError> {
    let count = dec.read_byte()? as usize;
    let mut attrs = Vec::with_capacity(count);
    for _ in 0..count {
        attrs.push(read_label_pair(dec, payload)?);
    }
    Ok(attrs)
}

// ── Traces ───────────────────────────────────────────────────────────────

/// A span's nested event, materialised during streaming decode and handed
/// to the appender by reference inside a [`DecodedSpan`].
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedEvent {
    pub ts_unix_nano: u64,
    pub name: Vec<u8>,
    pub attributes: Vec<(Vec<u8>, Vec<u8>)>,
}

/// A span's nested link.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedLink {
    pub trace_id: Vec<u8>,
    pub span_id: Vec<u8>,
    pub attributes: Vec<(Vec<u8>, Vec<u8>)>,
}

/// One fully-resolved span handed to a [`TracesAppender`]. Scalar fields
/// and the fixed-width ids are borrowed straight out of the wire payload
/// (no copy); the per-batch `resources`/`scopes` dictionaries are resolved
/// by the decoder so the appender sees denormalised resource labels +
/// scope name/version and never has to carry the dictionary itself. Nested
/// `events`/`links` are owned (cold path — typically ≤1 of each per span).
pub struct DecodedSpan<'a> {
    pub trace_id: &'a [u8],
    pub span_id: &'a [u8],
    pub parent_span_id: Option<&'a [u8]>,
    pub resource_labels: &'a [(Vec<u8>, Vec<u8>)],
    pub scope_name: &'a [u8],
    pub scope_version: &'a [u8],
    pub name: &'a [u8],
    pub kind: u8,
    pub start_unix_nano: u64,
    pub end_unix_nano: u64,
    pub status_code: u8,
    pub status_message: &'a [u8],
    pub attributes: &'a [(Vec<u8>, Vec<u8>)],
    pub events: &'a [DecodedEvent],
    pub links: &'a [DecodedLink],
}

/// Consumer for streaming `TracesBatch` decode.
///
/// Wire shape (mirrored from `generated::{TracesBatch, ResourceEntry,
/// ScopeEntry, Span, SpanEvent, SpanLink}::encode_into`):
///
/// ```text
/// resource_count: u16 BE
/// for each resource:
///     label_count: u16 BE  → LabelPair…       (u8 key-len, u16 BE value-len)
/// scope_count: u16 BE
/// for each scope:
///     name:    u8-len bytes (utf8)
///     version: u8-len bytes (ascii)
/// span_count: u32 BE
/// for each span:
///     resource_idx: u16 BE
///     scope_idx:    u16 BE
///     trace_id:     16 bytes
///     span_id:      8 bytes
///     parent_span_id: u8 present-flag (+ 8 bytes if 1)
///     name:           u16-len bytes (utf8)
///     kind:           u8
///     start_unix_nano: u64 BE
///     end_unix_nano:   u64 BE
///     status_code:     u8
///     status_message:  u16-len bytes (utf8)
///     attr_count:  u16 BE → LabelPair…
///     event_count: u16 BE → SpanEvent…  (ts u64, name u16-len, attr u8-count LabelPair…)
///     link_count:  u8     → SpanLink…   (trace_id[16], span_id[8], attr u8-count LabelPair…)
/// ```
///
/// Unlike metrics/logs there's no `observe_*` dictionary call: the
/// decoder resolves `resource_idx`/`scope_idx` against the per-batch
/// dictionaries and hands the appender self-contained spans. A span whose
/// `resource_idx`/`scope_idx` is out of range is treated as a malformed
/// batch (`UnexpectedEof`-class error), same severity as a truncated read.
pub trait TracesAppender {
    fn append_span(&mut self, span: &DecodedSpan<'_>);
}

/// Decode a `TracesBatch` payload into `appender`. Returns the span count.
pub fn decode_traces_batch_into<A: TracesAppender>(
    payload: &[u8],
    appender: &mut A,
) -> Result<usize, BinSchemaError> {
    let mut dec = BitStreamDecoder::new(payload, BitOrder::MsbFirst);

    // Resource dictionary.
    let resource_count = dec.read_u16_be()? as usize;
    let mut resources: Vec<Vec<(Vec<u8>, Vec<u8>)>> = Vec::with_capacity(resource_count);
    for _ in 0..resource_count {
        let label_count = dec.read_u16_be()? as usize;
        let mut labels = Vec::with_capacity(label_count);
        for _ in 0..label_count {
            labels.push(read_label_pair(&mut dec, payload)?);
        }
        resources.push(labels);
    }

    // Scope dictionary (owned name/version bytes — referenced for the
    // whole batch, so a copy out of the payload is the right call).
    let scope_count = dec.read_u16_be()? as usize;
    let mut scopes: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(scope_count);
    for _ in 0..scope_count {
        let name = read_str_u8(&mut dec, payload)?.to_vec();
        let version = read_str_u8(&mut dec, payload)?.to_vec();
        scopes.push((name, version));
    }

    let span_count = dec.read_u32_be()? as usize;
    for _ in 0..span_count {
        let resource_idx = dec.read_u16_be()? as usize;
        let scope_idx = dec.read_u16_be()? as usize;
        let trace_id = read_fixed(&mut dec, payload, 16)?;
        let span_id = read_fixed(&mut dec, payload, 8)?;
        let has_parent = dec.read_byte()? != 0;
        let parent_span_id = if has_parent {
            Some(read_fixed(&mut dec, payload, 8)?)
        } else {
            None
        };
        let name = read_str_u16(&mut dec, payload)?;
        let kind = dec.read_byte()?;
        let start_unix_nano = dec.read_u64_be()?;
        let end_unix_nano = dec.read_u64_be()?;
        let status_code = dec.read_byte()?;
        let status_message = read_str_u16(&mut dec, payload)?;

        let attr_count = dec.read_u16_be()? as usize;
        let mut attributes = Vec::with_capacity(attr_count);
        for _ in 0..attr_count {
            attributes.push(read_label_pair(&mut dec, payload)?);
        }

        let event_count = dec.read_u16_be()? as usize;
        let mut events = Vec::with_capacity(event_count);
        for _ in 0..event_count {
            let ts_unix_nano = dec.read_u64_be()?;
            let ename = read_str_u16(&mut dec, payload)?.to_vec();
            let eattrs = read_attrs_u8(&mut dec, payload)?;
            events.push(DecodedEvent {
                ts_unix_nano,
                name: ename,
                attributes: eattrs,
            });
        }

        let link_count = dec.read_byte()? as usize;
        let mut links = Vec::with_capacity(link_count);
        for _ in 0..link_count {
            let ltrace = read_fixed(&mut dec, payload, 16)?.to_vec();
            let lspan = read_fixed(&mut dec, payload, 8)?.to_vec();
            let lattrs = read_attrs_u8(&mut dec, payload)?;
            links.push(DecodedLink {
                trace_id: ltrace,
                span_id: lspan,
                attributes: lattrs,
            });
        }

        // Resolve the per-batch dictionaries. An out-of-range index is a
        // malformed batch — reject it rather than silently dropping
        // resource/scope context.
        let resource_labels = resources
            .get(resource_idx)
            .map(|v| v.as_slice())
            .ok_or(BinSchemaError::UnexpectedEof)?;
        let (scope_name, scope_version) = scopes
            .get(scope_idx)
            .map(|(n, v)| (n.as_slice(), v.as_slice()))
            .ok_or(BinSchemaError::UnexpectedEof)?;

        appender.append_span(&DecodedSpan {
            trace_id,
            span_id,
            parent_span_id,
            resource_labels,
            scope_name,
            scope_version,
            name,
            kind,
            start_unix_nano,
            end_unix_nano,
            status_code,
            status_message,
            attributes: &attributes,
            events: &events,
            links: &links,
        });
    }

    Ok(span_count)
}

// ── Profiles ───────────────────────────────────────────────────────────

/// Consumer for streaming `ProfilesBatch` decode.
///
/// Wire shape (mirrored from `generated::{ProfilesBatch, ProfileBlob}::
/// encode_into`):
///
/// ```text
/// blob_count: u32 BE
/// for each blob:
///     ts_unix_nano:  u64 BE
///     duration_nano: u64 BE
///     label_count:   u16 BE → LabelPair…
///     format:        u8
///     data_len:      u32 BE  bytes…   (opaque pprof; stored verbatim)
/// ```
///
/// The pprof `data` blob is handed over owned — one allocation per blob,
/// and the builder needs the bytes in one contiguous place for the parquet
/// `Binary` column anyway. Profiles are low-volume (≈1 blob/batch), so
/// there's no hot path here to optimise.
pub trait ProfilesAppender {
    fn append_blob(
        &mut self,
        ts_unix_nano: u64,
        duration_nano: u64,
        labels: Vec<(Vec<u8>, Vec<u8>)>,
        format: u8,
        data: Vec<u8>,
    );
}

/// Decode a `ProfilesBatch` payload into `appender`. Returns the blob count.
pub fn decode_profiles_batch_into<A: ProfilesAppender>(
    payload: &[u8],
    appender: &mut A,
) -> Result<usize, BinSchemaError> {
    let mut dec = BitStreamDecoder::new(payload, BitOrder::MsbFirst);
    let blob_count = dec.read_u32_be()? as usize;
    for _ in 0..blob_count {
        let ts_unix_nano = dec.read_u64_be()?;
        let duration_nano = dec.read_u64_be()?;
        let label_count = dec.read_u16_be()? as usize;
        let mut labels = Vec::with_capacity(label_count);
        for _ in 0..label_count {
            labels.push(read_label_pair(&mut dec, payload)?);
        }
        let format = dec.read_byte()?;
        let data_len = dec.read_u32_be()? as usize;
        let data = read_fixed(&mut dec, payload, data_len)?.to_vec();
        appender.append_blob(ts_unix_nano, duration_nano, labels, format, data);
    }
    Ok(blob_count)
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
                        LabelPair {
                            key: "__name__".into(),
                            value: "cpu_usage".into(),
                        },
                        LabelPair {
                            key: "host".into(),
                            value: "host-1".into(),
                        },
                    ],
                },
                SeriesDictEntry {
                    fingerprint: 0xDEAD_BEEF,
                    metric_type: 1, // counter
                    labels: vec![
                        LabelPair {
                            key: "__name__".into(),
                            value: "http_requests_total".into(),
                        },
                        LabelPair {
                            key: "service".into(),
                            value: "api".into(),
                        },
                        LabelPair {
                            key: "status".into(),
                            value: "200".into(),
                        },
                    ],
                },
            ],
            samples: vec![
                MetricSample {
                    fingerprint: 0xCAFE_BABE,
                    ts_unix_nano: 1_000,
                    value: 3.25,
                },
                MetricSample {
                    fingerprint: 0xCAFE_BABE,
                    ts_unix_nano: 2_000,
                    value: 6.5,
                },
                MetricSample {
                    fingerprint: 0xDEAD_BEEF,
                    ts_unix_nano: 1_500,
                    value: 42.0,
                },
                MetricSample {
                    fingerprint: 0xDEAD_BEEF,
                    ts_unix_nano: 2_500,
                    value: 43.0,
                },
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
        let batch = MetricsBatch {
            series: vec![],
            samples: vec![],
        };
        let payload = batch.encode().expect("encode");
        let mut collected = MetricsCollected::default();
        let (n_series, n_samples) =
            decode_metrics_batch_into(&payload, &mut collected).expect("decode");
        assert_eq!(n_series, 0);
        assert_eq!(n_samples, 0);
        assert!(collected.series.is_empty());
        assert!(collected.samples.is_empty());
    }

    // ── Logs ────────────────────────────────────────────────────────

    use crate::generated::{LogEntry, LogStream, LogsBatch};

    // Type aliases to keep the per-row tuples (and `clippy::type_complexity`)
    // legible. The test only needs structural equality; named struct fields
    // would be marginally clearer but the tuple shape lines up 1:1 with the
    // appender trait's argument order, which is the easier invariant to read
    // against `decode_logs_batch_into`.
    type CollectedStream = (u64, Vec<(Vec<u8>, Vec<u8>)>);
    type CollectedEntry = (u64, u64, u8, Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>);

    #[derive(Default, Debug, PartialEq)]
    struct LogsCollected {
        streams: Vec<CollectedStream>,
        entries: Vec<CollectedEntry>,
    }

    impl LogsAppender for LogsCollected {
        fn observe_stream(&mut self, fingerprint: u64, labels: Vec<(Vec<u8>, Vec<u8>)>) {
            self.streams.push((fingerprint, labels));
        }
        fn append_entry(
            &mut self,
            fingerprint: u64,
            ts_unix_nano: u64,
            severity: u8,
            body: Vec<u8>,
            attributes: Vec<(Vec<u8>, Vec<u8>)>,
        ) {
            self.entries
                .push((fingerprint, ts_unix_nano, severity, body, attributes));
        }
    }

    #[test]
    fn logs_streaming_matches_generated() {
        let batch = LogsBatch {
            streams: vec![
                LogStream {
                    fingerprint: 0x1111_2222,
                    labels: vec![
                        LabelPair {
                            key: "service".into(),
                            value: "api".into(),
                        },
                        LabelPair {
                            key: "env".into(),
                            value: "prod".into(),
                        },
                    ],
                    entries: vec![
                        LogEntry {
                            ts_unix_nano: 1_000,
                            severity: 9,
                            body: "GET /healthz 200".into(),
                            attributes: vec![
                                LabelPair {
                                    key: "status".into(),
                                    value: "200".into(),
                                },
                                LabelPair {
                                    key: "trace_id".into(),
                                    value: "abc123".into(),
                                },
                            ],
                        },
                        LogEntry {
                            ts_unix_nano: 2_000,
                            severity: 17,
                            body: "POST /pay 500 (db timeout)".into(),
                            attributes: vec![LabelPair {
                                key: "status".into(),
                                value: "500".into(),
                            }],
                        },
                    ],
                },
                LogStream {
                    fingerprint: 0x3333_4444,
                    labels: vec![LabelPair {
                        key: "service".into(),
                        value: "worker".into(),
                    }],
                    entries: vec![LogEntry {
                        // Zero-length body + zero attributes — the edge case
                        // the dummy test guards for its own format.
                        ts_unix_nano: 3_000,
                        severity: 5,
                        body: "".into(),
                        attributes: vec![],
                    }],
                },
            ],
        };
        let payload = batch.encode().expect("encode");

        let mut collected = LogsCollected::default();
        let total = decode_logs_batch_into(&payload, &mut collected).expect("decode");
        assert_eq!(total, 3, "two streams: 2 + 1 entries");
        assert_eq!(collected.streams.len(), batch.streams.len());

        // Stream-level checks.
        for (got, want) in collected.streams.iter().zip(batch.streams.iter()) {
            assert_eq!(got.0, want.fingerprint);
            assert_eq!(got.1.len(), want.labels.len());
            for (gl, wl) in got.1.iter().zip(want.labels.iter()) {
                assert_eq!(&gl.0, wl.key.as_bytes());
                assert_eq!(&gl.1, wl.value.as_bytes());
            }
        }

        // Entry-level checks — flatten the expected batch the same
        // way the streaming decoder emits them (stream-by-stream).
        let want_entries: Vec<_> = batch
            .streams
            .iter()
            .flat_map(|s| s.entries.iter().map(move |e| (s.fingerprint, e)))
            .collect();
        assert_eq!(collected.entries.len(), want_entries.len());
        for (got, (want_fp, want_e)) in collected.entries.iter().zip(want_entries.iter()) {
            assert_eq!(got.0, *want_fp);
            assert_eq!(got.1, want_e.ts_unix_nano);
            assert_eq!(got.2, want_e.severity);
            assert_eq!(&got.3, want_e.body.as_bytes());
            assert_eq!(got.4.len(), want_e.attributes.len());
            for (ga, wa) in got.4.iter().zip(want_e.attributes.iter()) {
                assert_eq!(&ga.0, wa.key.as_bytes());
                assert_eq!(&ga.1, wa.value.as_bytes());
            }
        }
    }

    #[test]
    fn logs_streaming_handles_empty_batch() {
        let batch = LogsBatch { streams: vec![] };
        let payload = batch.encode().expect("encode");
        let mut collected = LogsCollected::default();
        let n = decode_logs_batch_into(&payload, &mut collected).expect("decode");
        assert_eq!(n, 0);
        assert!(collected.streams.is_empty());
        assert!(collected.entries.is_empty());
    }

    #[test]
    fn logs_streaming_rejects_truncated_payload() {
        let batch = LogsBatch {
            streams: vec![LogStream {
                fingerprint: 1,
                labels: vec![LabelPair {
                    key: "k".into(),
                    value: "v".into(),
                }],
                entries: vec![LogEntry {
                    ts_unix_nano: 1,
                    severity: 9,
                    body: "hello".into(),
                    attributes: vec![],
                }],
            }],
        };
        let mut payload = batch.encode().expect("encode");
        let truncated_len = payload.len() - 3;
        payload.truncate(truncated_len);

        let mut collected = LogsCollected::default();
        let result = decode_logs_batch_into(&payload, &mut collected);
        assert!(result.is_err(), "expected error, got {:?}", result);
    }

    #[test]
    fn metrics_streaming_rejects_truncated_payload() {
        let batch = MetricsBatch {
            series: vec![SeriesDictEntry {
                fingerprint: 1,
                metric_type: 2,
                labels: vec![LabelPair {
                    key: "k".into(),
                    value: "v".into(),
                }],
            }],
            samples: vec![
                MetricSample {
                    fingerprint: 1,
                    ts_unix_nano: 1,
                    value: 1.0,
                },
                MetricSample {
                    fingerprint: 1,
                    ts_unix_nano: 2,
                    value: 2.0,
                },
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

    // ── Traces ──────────────────────────────────────────────────────

    use crate::generated::{
        ProfileBlob, ProfilesBatch, ResourceEntry, ScopeEntry, Span, SpanEvent, SpanLink,
        TracesBatch,
    };

    /// Owned mirror of a `DecodedSpan` — `append_span` borrows, so the
    /// collector copies everything out for later structural comparison.
    #[derive(Debug, PartialEq)]
    struct OwnedSpan {
        trace_id: Vec<u8>,
        span_id: Vec<u8>,
        parent_span_id: Option<Vec<u8>>,
        resource_labels: Vec<(Vec<u8>, Vec<u8>)>,
        scope_name: Vec<u8>,
        scope_version: Vec<u8>,
        name: Vec<u8>,
        kind: u8,
        start_unix_nano: u64,
        end_unix_nano: u64,
        status_code: u8,
        status_message: Vec<u8>,
        attributes: Vec<(Vec<u8>, Vec<u8>)>,
        events: Vec<DecodedEvent>,
        links: Vec<DecodedLink>,
    }

    #[derive(Default)]
    struct TracesCollected {
        spans: Vec<OwnedSpan>,
    }

    impl TracesAppender for TracesCollected {
        fn append_span(&mut self, s: &DecodedSpan<'_>) {
            self.spans.push(OwnedSpan {
                trace_id: s.trace_id.to_vec(),
                span_id: s.span_id.to_vec(),
                parent_span_id: s.parent_span_id.map(|p| p.to_vec()),
                resource_labels: s.resource_labels.to_vec(),
                scope_name: s.scope_name.to_vec(),
                scope_version: s.scope_version.to_vec(),
                name: s.name.to_vec(),
                kind: s.kind,
                start_unix_nano: s.start_unix_nano,
                end_unix_nano: s.end_unix_nano,
                status_code: s.status_code,
                status_message: s.status_message.to_vec(),
                attributes: s.attributes.to_vec(),
                events: s.events.to_vec(),
                links: s.links.to_vec(),
            });
        }
    }

    fn sample_traces_batch() -> TracesBatch {
        TracesBatch {
            resources: vec![
                ResourceEntry {
                    labels: vec![
                        LabelPair {
                            key: "service.name".into(),
                            value: "api".into(),
                        },
                        LabelPair {
                            key: "host".into(),
                            value: "host-1".into(),
                        },
                    ],
                },
                ResourceEntry {
                    labels: vec![LabelPair {
                        key: "service.name".into(),
                        value: "worker".into(),
                    }],
                },
            ],
            scopes: vec![
                ScopeEntry {
                    name: "scry.spewer".into(),
                    version: "0.1.0".into(),
                },
                ScopeEntry {
                    name: "tokio".into(),
                    version: "1.0".into(),
                },
            ],
            spans: vec![
                Span {
                    resource_idx: 0,
                    scope_idx: 1,
                    trace_id: (0..16u8).collect(),
                    span_id: (0..8u8).collect(),
                    parent_span_id: None,
                    name: "root".into(),
                    kind: 2,
                    start_unix_nano: 1_000,
                    end_unix_nano: 2_000,
                    status_code: 1,
                    status_message: "ok".into(),
                    attributes: vec![
                        LabelPair {
                            key: "http.method".into(),
                            value: "GET".into(),
                        },
                        LabelPair {
                            key: "http.status".into(),
                            value: "200".into(),
                        },
                    ],
                    events: vec![SpanEvent {
                        ts_unix_nano: 1_500,
                        name: "checkpoint".into(),
                        attributes: vec![LabelPair {
                            key: "k".into(),
                            value: "v".into(),
                        }],
                    }],
                    links: vec![SpanLink {
                        trace_id: (16..32u8).collect(),
                        span_id: (8..16u8).collect(),
                        attributes: vec![],
                    }],
                },
                Span {
                    // Child span: parent set, no events, two links, empty
                    // attrs — exercises the optional + zero-count paths.
                    resource_idx: 1,
                    scope_idx: 0,
                    trace_id: (0..16u8).collect(),
                    span_id: (8..16u8).collect(),
                    parent_span_id: Some((0..8u8).collect()),
                    name: "child".into(),
                    kind: 3,
                    start_unix_nano: 1_200,
                    end_unix_nano: 1_800,
                    status_code: 2,
                    status_message: String::new(),
                    attributes: vec![],
                    events: vec![],
                    links: vec![
                        SpanLink {
                            trace_id: (32..48u8).collect(),
                            span_id: (0..8u8).collect(),
                            attributes: vec![],
                        },
                        SpanLink {
                            trace_id: (48..64u8).collect(),
                            span_id: (8..16u8).collect(),
                            attributes: vec![LabelPair {
                                key: "rel".into(),
                                value: "follows".into(),
                            }],
                        },
                    ],
                },
            ],
        }
    }

    #[test]
    fn traces_streaming_matches_generated() {
        let batch = sample_traces_batch();
        let payload = batch.encode().expect("encode");

        let mut collected = TracesCollected::default();
        let n = decode_traces_batch_into(&payload, &mut collected).expect("decode");
        assert_eq!(n, batch.spans.len());
        assert_eq!(collected.spans.len(), batch.spans.len());

        for (got, want) in collected.spans.iter().zip(batch.spans.iter()) {
            assert_eq!(&got.trace_id, &want.trace_id);
            assert_eq!(&got.span_id, &want.span_id);
            assert_eq!(
                got.parent_span_id.as_deref(),
                want.parent_span_id.as_deref()
            );
            assert_eq!(&got.name, want.name.as_bytes());
            assert_eq!(got.kind, want.kind);
            assert_eq!(got.start_unix_nano, want.start_unix_nano);
            assert_eq!(got.end_unix_nano, want.end_unix_nano);
            assert_eq!(got.status_code, want.status_code);
            assert_eq!(&got.status_message, want.status_message.as_bytes());

            // Resolved dictionaries.
            let want_res = &batch.resources[want.resource_idx as usize].labels;
            assert_eq!(got.resource_labels.len(), want_res.len());
            for (gl, wl) in got.resource_labels.iter().zip(want_res.iter()) {
                assert_eq!(&gl.0, wl.key.as_bytes());
                assert_eq!(&gl.1, wl.value.as_bytes());
            }
            let want_scope = &batch.scopes[want.scope_idx as usize];
            assert_eq!(&got.scope_name, want_scope.name.as_bytes());
            assert_eq!(&got.scope_version, want_scope.version.as_bytes());

            // Span attributes.
            assert_eq!(got.attributes.len(), want.attributes.len());
            for (ga, wa) in got.attributes.iter().zip(want.attributes.iter()) {
                assert_eq!(&ga.0, wa.key.as_bytes());
                assert_eq!(&ga.1, wa.value.as_bytes());
            }

            // Nested events.
            assert_eq!(got.events.len(), want.events.len());
            for (ge, we) in got.events.iter().zip(want.events.iter()) {
                assert_eq!(ge.ts_unix_nano, we.ts_unix_nano);
                assert_eq!(&ge.name, we.name.as_bytes());
                assert_eq!(ge.attributes.len(), we.attributes.len());
                for (ga, wa) in ge.attributes.iter().zip(we.attributes.iter()) {
                    assert_eq!(&ga.0, wa.key.as_bytes());
                    assert_eq!(&ga.1, wa.value.as_bytes());
                }
            }

            // Nested links.
            assert_eq!(got.links.len(), want.links.len());
            for (gl, wl) in got.links.iter().zip(want.links.iter()) {
                assert_eq!(&gl.trace_id, &wl.trace_id);
                assert_eq!(&gl.span_id, &wl.span_id);
                assert_eq!(gl.attributes.len(), wl.attributes.len());
            }
        }
    }

    #[test]
    fn traces_streaming_handles_empty_batch() {
        let batch = TracesBatch {
            resources: vec![],
            scopes: vec![],
            spans: vec![],
        };
        let payload = batch.encode().expect("encode");
        let mut collected = TracesCollected::default();
        let n = decode_traces_batch_into(&payload, &mut collected).expect("decode");
        assert_eq!(n, 0);
        assert!(collected.spans.is_empty());
    }

    #[test]
    fn traces_streaming_rejects_truncated_payload() {
        let batch = sample_traces_batch();
        let mut payload = batch.encode().expect("encode");
        let truncated_len = payload.len() - 5;
        payload.truncate(truncated_len);
        let mut collected = TracesCollected::default();
        let result = decode_traces_batch_into(&payload, &mut collected);
        assert!(result.is_err(), "expected error, got {:?}", result);
    }

    // ── Profiles ────────────────────────────────────────────────────

    // Tuple shape lines up 1:1 with `ProfilesAppender::append_blob`'s
    // argument order (ts, duration, labels, format, data) — same legibility
    // tradeoff (and `type_complexity` dodge) as `CollectedEntry` above.
    type CollectedBlob = (u64, u64, Vec<(Vec<u8>, Vec<u8>)>, u8, Vec<u8>);

    #[derive(Default)]
    struct ProfilesCollected {
        blobs: Vec<CollectedBlob>,
    }

    impl ProfilesAppender for ProfilesCollected {
        fn append_blob(
            &mut self,
            ts_unix_nano: u64,
            duration_nano: u64,
            labels: Vec<(Vec<u8>, Vec<u8>)>,
            format: u8,
            data: Vec<u8>,
        ) {
            self.blobs
                .push((ts_unix_nano, duration_nano, labels, format, data));
        }
    }

    fn sample_profiles_batch() -> ProfilesBatch {
        ProfilesBatch {
            samples: vec![
                ProfileBlob {
                    ts_unix_nano: 42,
                    duration_nano: 10_000_000_000,
                    labels: vec![
                        LabelPair {
                            key: "service".into(),
                            value: "api".into(),
                        },
                        LabelPair {
                            key: "profile.type".into(),
                            value: "cpu".into(),
                        },
                    ],
                    format: 1,
                    data: (0..255u16).map(|b| b as u8).collect(),
                },
                ProfileBlob {
                    // Empty labels + empty data — zero-count edge case.
                    ts_unix_nano: 99,
                    duration_nano: 0,
                    labels: vec![],
                    format: 2,
                    data: vec![],
                },
            ],
        }
    }

    #[test]
    fn profiles_streaming_matches_generated() {
        let batch = sample_profiles_batch();
        let payload = batch.encode().expect("encode");

        let mut collected = ProfilesCollected::default();
        let n = decode_profiles_batch_into(&payload, &mut collected).expect("decode");
        assert_eq!(n, batch.samples.len());
        assert_eq!(collected.blobs.len(), batch.samples.len());

        for (got, want) in collected.blobs.iter().zip(batch.samples.iter()) {
            assert_eq!(got.0, want.ts_unix_nano);
            assert_eq!(got.1, want.duration_nano);
            assert_eq!(got.2.len(), want.labels.len());
            for (gl, wl) in got.2.iter().zip(want.labels.iter()) {
                assert_eq!(&gl.0, wl.key.as_bytes());
                assert_eq!(&gl.1, wl.value.as_bytes());
            }
            assert_eq!(got.3, want.format);
            assert_eq!(&got.4, &want.data);
        }
    }

    #[test]
    fn profiles_streaming_handles_empty_batch() {
        let batch = ProfilesBatch { samples: vec![] };
        let payload = batch.encode().expect("encode");
        let mut collected = ProfilesCollected::default();
        let n = decode_profiles_batch_into(&payload, &mut collected).expect("decode");
        assert_eq!(n, 0);
        assert!(collected.blobs.is_empty());
    }

    #[test]
    fn profiles_streaming_rejects_truncated_payload() {
        let batch = sample_profiles_batch();
        let mut payload = batch.encode().expect("encode");
        let truncated_len = payload.len() - 10;
        payload.truncate(truncated_len);
        let mut collected = ProfilesCollected::default();
        let result = decode_profiles_batch_into(&payload, &mut collected);
        assert!(result.is_err(), "expected error, got {:?}", result);
    }
}
