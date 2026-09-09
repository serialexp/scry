//! Block builder for log entries — the v0.4 second-real-signal.
//!
//! Per `ARCHITECTURE.md § Logs` (and the v0.4 plan locked with Bart),
//! a logs block consists of three objects in the bucket, mirroring
//! the metrics layout one-for-one:
//!
//! - `<block>.parquet` — `(stream_fingerprint, ts_unix_nano,
//!   severity, body, attributes Map<Utf8,Utf8>)` sorted by
//!   `(stream_fingerprint, ts)`. The intra-block sort makes parquet
//!   row-group min/max stats on the fingerprint column an aggressive
//!   pruning lever once a query has resolved its target stream
//!   fingerprint set — same shape as metrics' postings-driven
//!   pushdown.
//! - `<block>.postings.parquet` — `(label_name, label_value,
//!   stream_fingerprints LIST<u64>)` sorted by `(label_name,
//!   label_value)`. Built from `LogStream.labels` only (service,
//!   host, env — the *stream-level* labels). Per-entry attributes
//!   (`trace_id`, `status`, …) are queryable via SQL on the Map
//!   column but not pushdown-eligible at the postings layer; same
//!   reason as metrics' labels-on-samples decision.
//! - `<block>.meta.json` — the catalog's source of truth for block
//!   existence; carries `has_postings`/`postings_size_bytes` plus
//!   the per-block `all_fingerprints` list that empty-matcher
//!   queries materialise without scanning the postings file.
//!
//! Wire input is `LogsBatch { streams: Vec<LogStream { entries:
//! Vec<LogEntry> }> }` (see `scry_proto::generated`). Each batch
//! re-sends whatever portion of its stream dictionary the agent
//! considered active; we dedup by fingerprint server-side, same as
//! metrics. The hot ingest path is per-entry (one fingerprint, one
//! ts, one severity byte, one body string, plus an attribute map);
//! stream ingestion is amortised across many entries.
//!
//! Body / substring search (`body LIKE '%pat%'`) works against the
//! column with no index — full column scan after time-range / label
//! pruning. Real substring search is the tantivy-backed phase
//! later; the column is here today so SQL queries can still answer
//! "show me errors mentioning 'database'" cheaply enough for v0.
//!
//! ## CSR layout
//!
//! Hot-path entry storage uses five parallel `Vec`s instead of
//! `Vec<LogEntry>` so the data lives in column-shaped memory —
//! matches Arrow's internal layout, which lets `from_iter_values`
//! walk each column as a single contiguous memcpy at parquet-encode
//! time. Same lesson as `dummy.rs` / `metrics.rs`; see CLAUDE.md
//! § Performance.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    ArrayRef, BinaryArray, FixedSizeBinaryBuilder, MapBuilder, StringArray, StringBuilder,
    UInt16Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::{path::Path, ObjectStore};
use parquet::arrow::ArrowWriter;
use scry_proto::streaming::LogsAppender;
use scry_proto::{CanonicalLogRecord, LogsV2Appender};
use std::hash::Hasher;
use twox_hash::XxHash64;
use uuid::Uuid;

use crate::{
    block_path, BlockBuilder, BlockBuilderConfig, BlockMeta, BodyBloomBuilder, EncodedBlock,
};

const SIGNAL: &str = "logs";
const SCHEMA_VERSION_V1: u32 = 1;
const SCHEMA_VERSION_V2: u32 = 2;
const SCOPE_NAME_LABEL: &str = "otel.scope.name";
const SCOPE_VERSION_LABEL: &str = "otel.scope.version";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LogsSchema {
    V1,
    V2,
}

impl LogsSchema {
    fn version(self) -> u32 {
        match self {
            Self::V1 => SCHEMA_VERSION_V1,
            Self::V2 => SCHEMA_VERSION_V2,
        }
    }
}

fn attributes_field() -> Field {
    let entries_field = Arc::new(Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Utf8, true),
        ])),
        false,
    ));
    Field::new(
        "attributes",
        DataType::Map(entries_field, /*keys_sorted=*/ false),
        false,
    )
}

/// Exact physical schema of historical logs v1 parquet files.
pub fn logs_physical_schema_v1() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("stream_fingerprint", DataType::UInt64, false),
        Field::new("ts_unix_nano", DataType::UInt64, false),
        Field::new("severity", DataType::UInt8, false),
        Field::new("body", DataType::Utf8, false),
        attributes_field(),
    ]))
}

/// Exact physical schema of logs v2 parquet files.
///
/// [`LogsBlockBuilder`] selects this schema when its first batch callback is the
/// canonical logs-v2 appender; legacy callbacks continue to select v1.
pub fn logs_physical_schema_v2() -> SchemaRef {
    let mut fields: Vec<Field> = logs_physical_schema_v1()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields.extend([
        Field::new("observed_ts_unix_nano", DataType::UInt64, true),
        Field::new("severity_text", DataType::Utf8, true),
        Field::new("event_name", DataType::Utf8, true),
        Field::new("trace_id", DataType::FixedSizeBinary(16), true),
        Field::new("span_id", DataType::FixedSizeBinary(8), true),
        Field::new("trace_flags", DataType::UInt8, true),
        Field::new("raw_record_version", DataType::UInt16, true),
        Field::new("raw_record", DataType::Binary, true),
    ]);
    Arc::new(Schema::new(fields))
}

/// One unique stream accumulated for this block. Owned labels for
/// the same reason as `metrics::OwnedSeries`: we dedup by
/// fingerprint and the wire payload is dropped after decode, so the
/// bytes have to live somewhere if the postings build wants them at
/// finish time.
struct OwnedStream {
    fingerprint: u64,
    labels: Vec<(String, String)>,
}

/// In-memory logs block under construction.
pub struct LogsBlockBuilder {
    writer_id: Uuid,
    block_uuid: Option<Uuid>,
    cfg: BlockBuilderConfig,
    // Per-entry column-shaped storage (hot path). One `Vec` per
    // physical parquet column.
    fingerprints: Vec<u64>,
    ts: Vec<u64>,
    severities: Vec<u8>,
    // Bodies are kept as owned `String` because they're already
    // UTF-8-coerced on the way in (via `from_utf8_lossy` in the
    // appender) and the parquet `Utf8` writer needs `&str`. A
    // single `String` per entry plus its small header is one
    // malloc/entry — cheaper than the CSR offset+buffer dance
    // because parquet doesn't accept the CSR shape for nullable
    // string columns directly.
    bodies: Vec<String>,
    // Per-entry attribute maps. Two-deep `Vec` because each entry
    // has a small unique attribute set (trace_id, status, …) and
    // Arrow's MapBuilder walks them per-row at finish time. Same
    // tradeoff as metrics' per-series label storage.
    attributes: Vec<Vec<(String, String)>>,
    // V2-only projected fidelity columns. They stay empty for a v1 block.
    observed_ts: Vec<Option<u64>>,
    severity_texts: Vec<Option<String>>,
    event_names: Vec<Option<String>>,
    trace_ids: Vec<Option<[u8; 16]>>,
    span_ids: Vec<Option<[u8; 8]>>,
    trace_flags: Vec<Option<u8>>,
    raw_record_versions: Vec<Option<u16>>,
    raw_records: Vec<Option<Vec<u8>>>,
    schema: Option<LogsSchema>,
    // Reused while rendering typed values. Each completed String moves into its
    // durable column and this scratch retains capacity whenever no move is needed.
    text_scratch: String,
    // Reused while assembling each record's canonical stream labels. The vector's
    // allocation is retained when the stream was already observed; for a new
    // stream its allocation becomes the owned dictionary entry.
    label_scratch: Vec<(String, String)>,
    // Per-stream dedup. `stream_seen` cheaply rejects duplicates;
    // `stream_dict` keeps them in insertion order for stable
    // postings output (mostly a test ergonomic; query correctness
    // doesn't care).
    stream_seen: HashSet<u64>,
    stream_dict: Vec<OwnedStream>,
    bytes_est: u64,
    ts_min: u64,
    ts_max: u64,
}

impl LogsBlockBuilder {
    pub fn main_schema() -> SchemaRef {
        // The no-record default remains v1 for compatibility. Encoding selects
        // from the first appender callback recorded in `schema`.
        logs_physical_schema_v1()
    }

    pub fn postings_schema() -> SchemaRef {
        // Identical schema to metrics' postings sidecar; the shared
        // `postings` module is the single source of truth and the
        // query-side resolver reads it by column position.
        crate::postings::postings_schema()
    }

    pub fn row_count(&self) -> u64 {
        self.fingerprints.len() as u64
    }
}

impl BlockBuilder for LogsBlockBuilder {
    const SIGNAL: &'static str = SIGNAL;

    fn new(writer_id: Uuid, cfg: BlockBuilderConfig) -> Self {
        Self {
            writer_id,
            block_uuid: None,
            cfg,
            fingerprints: Vec::with_capacity(4096),
            ts: Vec::with_capacity(4096),
            severities: Vec::with_capacity(4096),
            bodies: Vec::with_capacity(4096),
            attributes: Vec::with_capacity(4096),
            observed_ts: Vec::with_capacity(4096),
            severity_texts: Vec::with_capacity(4096),
            event_names: Vec::with_capacity(4096),
            trace_ids: Vec::with_capacity(4096),
            span_ids: Vec::with_capacity(4096),
            trace_flags: Vec::with_capacity(4096),
            raw_record_versions: Vec::with_capacity(4096),
            raw_records: Vec::with_capacity(4096),
            schema: None,
            text_scratch: String::new(),
            label_scratch: Vec::new(),
            stream_seen: HashSet::with_capacity(256),
            stream_dict: Vec::with_capacity(256),
            bytes_est: 0,
            ts_min: u64::MAX,
            ts_max: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.fingerprints.is_empty()
    }

    fn should_close(&self) -> bool {
        self.row_count() >= self.cfg.max_rows || self.bytes_est >= self.cfg.target_bytes
    }

    fn content_schema_key(&self) -> Option<u64> {
        self.schema.map(|schema| schema.version() as u64)
    }

    fn merge(&mut self, other: &mut Self) {
        if other.is_empty() {
            // A dictionary-only v1 batch or empty v2 batch may still have selected
            // a schema and populated stream state. Empty merges transfer nothing.
            other.reset();
            return;
        }
        assert!(
            self.is_empty() || self.schema == other.schema,
            "cannot merge heterogeneous logs schemas"
        );
        if self.is_empty() {
            // Schema selection can precede the first row (v1 dictionaries and v2
            // begin_batch). No dictionary-only state belongs in the adopted block.
            self.stream_seen.clear();
            self.stream_dict.clear();
            self.schema = other.schema;
        } else {
            for incoming in &other.stream_dict {
                if let Some(existing) = self
                    .stream_dict
                    .iter()
                    .find(|stream| stream.fingerprint == incoming.fingerprint)
                {
                    assert_eq!(
                        existing.labels, incoming.labels,
                        "stream fingerprint collision with different exact labels"
                    );
                }
            }
        }
        // Entry columns: a bulk move each — `append` drains `other`'s
        // vec and keeps its capacity for reuse.
        self.fingerprints.append(&mut other.fingerprints);
        self.ts.append(&mut other.ts);
        self.severities.append(&mut other.severities);
        self.bodies.append(&mut other.bodies);
        self.attributes.append(&mut other.attributes);
        self.observed_ts.append(&mut other.observed_ts);
        self.severity_texts.append(&mut other.severity_texts);
        self.event_names.append(&mut other.event_names);
        self.trace_ids.append(&mut other.trace_ids);
        self.span_ids.append(&mut other.span_ids);
        self.trace_flags.append(&mut other.trace_flags);
        self.raw_record_versions
            .append(&mut other.raw_record_versions);
        self.raw_records.append(&mut other.raw_records);

        // Stream dictionary: dedup against the *shared* builder's
        // `stream_seen` (same policy as metrics' series dedup).
        for s in other.stream_dict.drain(..) {
            if self.stream_seen.insert(s.fingerprint) {
                self.stream_dict.push(s);
            }
        }
        other.stream_seen.clear();

        self.bytes_est = self
            .bytes_est
            .checked_add(other.bytes_est)
            .expect("logs merged size accounting overflow");
        self.ts_min = self.ts_min.min(other.ts_min);
        self.ts_max = self.ts_max.max(other.ts_max);

        other.bytes_est = 0;
        other.ts_min = u64::MAX;
        other.ts_max = 0;
        other.schema = None;
    }

    fn reset(&mut self) {
        self.fingerprints.clear();
        self.ts.clear();
        self.severities.clear();
        self.bodies.clear();
        self.attributes.clear();
        self.observed_ts.clear();
        self.severity_texts.clear();
        self.event_names.clear();
        self.trace_ids.clear();
        self.span_ids.clear();
        self.trace_flags.clear();
        self.raw_record_versions.clear();
        self.raw_records.clear();
        self.schema = None;
        self.text_scratch.clear();
        self.label_scratch.clear();
        self.stream_seen.clear();
        self.stream_dict.clear();
        self.bytes_est = 0;
        self.ts_min = u64::MAX;
        self.ts_max = 0;
    }

    fn set_compression_level(&mut self, level: i32) {
        self.cfg.compression_level = level;
    }

    fn set_wal_seg_max(&mut self, seg: u64) {
        self.cfg.wal_seg_max = Some(seg);
    }

    fn set_wal_shard(&mut self, shard: u32) {
        self.cfg.wal_shard = Some(shard);
    }

    fn set_block_uuid(&mut self, uuid: Uuid) {
        self.block_uuid = Some(uuid);
    }

    fn finish_and_upload(
        self,
        store: &dyn ObjectStore,
    ) -> impl std::future::Future<Output = Result<Option<BlockMeta>>> + Send {
        self.finish_and_upload_impl(store)
    }
}

impl LogsAppender for LogsBlockBuilder {
    fn observe_stream(&mut self, fingerprint: u64, labels: Vec<(Vec<u8>, Vec<u8>)>) {
        assert!(
            self.schema.is_none() || self.schema == Some(LogsSchema::V1),
            "cannot mix logs v1 and v2 callbacks"
        );
        self.schema = Some(LogsSchema::V1);
        if !self.stream_seen.insert(fingerprint) {
            // Already accumulated this stream in an earlier batch.
            // Wire spec assumes labels are identical across batches
            // for a given fingerprint (the fingerprint is over the
            // labels); we trust that without re-verifying.
            return;
        }
        // Coerce label bytes to UTF-8 (lossy on invalid input —
        // a misbehaving agent shouldn't poison ingest). Same
        // policy as `MetricsBlockBuilder::observe_series`.
        let owned: Vec<(String, String)> = labels
            .into_iter()
            .map(|(k, v)| {
                (
                    String::from_utf8_lossy(&k).into_owned(),
                    String::from_utf8_lossy(&v).into_owned(),
                )
            })
            .collect();
        self.stream_dict.push(OwnedStream {
            fingerprint,
            labels: owned,
        });
    }

    fn append_entry(
        &mut self,
        fingerprint: u64,
        ts_unix_nano: u64,
        severity: u8,
        body: Vec<u8>,
        attributes: Vec<(Vec<u8>, Vec<u8>)>,
    ) {
        assert!(
            self.schema.is_none() || self.schema == Some(LogsSchema::V1),
            "cannot mix logs v1 and v2 records"
        );
        self.schema = Some(LogsSchema::V1);
        self.ts_min = self.ts_min.min(ts_unix_nano);
        self.ts_max = self.ts_max.max(ts_unix_nano);
        // Same UTF-8 coercion as the stream labels — parquet's Utf8
        // writer demands valid UTF-8 and a bad byte from an agent
        // shouldn't fail the whole block. `from_utf8_lossy` is
        // alloc-free in the common (valid) case: it returns
        // `Cow::Borrowed`, and `into_owned()` copies only when we
        // actually replaced bad bytes.
        let body_str = String::from_utf8_lossy(&body).into_owned();
        let attrs: Vec<(String, String)> = attributes
            .into_iter()
            .map(|(k, v)| {
                (
                    String::from_utf8_lossy(&k).into_owned(),
                    String::from_utf8_lossy(&v).into_owned(),
                )
            })
            .collect();
        // 17 fixed bytes (fp + ts + severity) + body + attrs. Like
        // metrics, this is for "stop accumulating" pacing, not
        // exact accounting; real compressed size after zstd is
        // much smaller.
        let attr_bytes: usize = attrs.iter().map(|(k, v)| k.len() + v.len()).sum();
        self.bytes_est += 17 + body_str.len() as u64 + attr_bytes as u64;
        self.fingerprints.push(fingerprint);
        self.ts.push(ts_unix_nano);
        self.severities.push(severity);
        self.bodies.push(body_str);
        self.attributes.push(attrs);
    }
}

impl LogsV2Appender for LogsBlockBuilder {
    fn begin_batch(&mut self, _record_count: u32) -> Result<(), String> {
        if self.schema.is_some() && self.schema != Some(LogsSchema::V2) {
            return Err("cannot mix logs v1 and v2 records".into());
        }
        self.schema = Some(LogsSchema::V2);
        Ok(())
    }

    fn record(&mut self, record: CanonicalLogRecord<'_>) -> Result<(), String> {
        if self.schema.is_some() && self.schema != Some(LogsSchema::V2) {
            return Err("cannot mix logs v1 and v2 records".into());
        }
        let ts = if record.time_unix_nano != 0 {
            record.time_unix_nano
        } else if record.observed_time_unix_nano != 0 {
            record.observed_time_unix_nano
        } else {
            return Err("event and observed timestamps are both zero".into());
        };
        let severity = u8::try_from(record.severity_number)
            .map_err(|_| "severity_number does not fit u8".to_string())?;
        let flags = u8::try_from(record.trace_flags)
            .map_err(|_| "trace_flags does not fit u8".to_string())?;
        let resource = record
            .resource_attributes
            .map_iter()
            .ok_or_else(|| "resource_attributes is not a map".to_string())?;
        let attrs_iter = record
            .attributes
            .map_iter()
            .ok_or_else(|| "attributes is not a map".to_string())?;

        self.label_scratch.clear();
        self.text_scratch.clear();
        let mut has_scope_name = false;
        let mut has_scope_version = false;
        for (key, value) in resource {
            has_scope_name |= key == SCOPE_NAME_LABEL;
            has_scope_version |= key == SCOPE_VERSION_LABEL;
            value.write_typed_canonical_text(&mut self.text_scratch);
            self.label_scratch
                .push((key.to_owned(), std::mem::take(&mut self.text_scratch)));
        }
        if !has_scope_name {
            if let Some(name) = record.scope_name.filter(|value| !value.is_empty()) {
                self.label_scratch
                    .push((SCOPE_NAME_LABEL.into(), name.to_owned()));
            }
        }
        if !has_scope_version {
            if let Some(version) = record.scope_version.filter(|value| !value.is_empty()) {
                self.label_scratch
                    .push((SCOPE_VERSION_LABEL.into(), version.to_owned()));
            }
        }
        self.label_scratch.sort_unstable();
        for pair in self.label_scratch.windows(2) {
            if pair[0].0 == pair[1].0 {
                return Err(format!("duplicate stream label {}", pair[0].0));
            }
        }
        // Logs-v2 identity is length-delimited: canonical strings may contain
        // NUL, so the legacy `key\0value\0` byte stream is ambiguous here.
        let mut hasher = XxHash64::with_seed(0);
        for (key, value) in &self.label_scratch {
            hasher.write(&(key.len() as u64).to_be_bytes());
            hasher.write(key.as_bytes());
            hasher.write(&(value.len() as u64).to_be_bytes());
            hasher.write(value.as_bytes());
        }
        let fingerprint = hasher.finish();
        let stream_is_new = !self.stream_seen.contains(&fingerprint);
        if !stream_is_new {
            let existing = self
                .stream_dict
                .iter()
                .find(|stream| stream.fingerprint == fingerprint)
                .expect("stream_seen and stream_dict remain in sync");
            if existing.labels != self.label_scratch {
                return Err("stream fingerprint collision with different exact labels".into());
            }
        }

        let mut attrs = Vec::with_capacity(attrs_iter.len());
        for (key, value) in attrs_iter {
            value.write_typed_canonical_text(&mut self.text_scratch);
            attrs.push((key.to_owned(), std::mem::take(&mut self.text_scratch)));
        }
        record.body.write_canonical_text(&mut self.text_scratch);
        let body = std::mem::take(&mut self.text_scratch);

        self.schema = Some(LogsSchema::V2);
        self.ts_min = self.ts_min.min(ts);
        self.ts_max = self.ts_max.max(ts);
        let label_bytes: usize = self
            .label_scratch
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum();
        let attr_bytes: usize = attrs
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum();
        self.bytes_est = self
            .bytes_est
            .checked_add(
                54u64
                    .checked_add(body.len() as u64)
                    .and_then(|n| n.checked_add(label_bytes as u64))
                    .and_then(|n| n.checked_add(attr_bytes as u64))
                    .and_then(|n| n.checked_add(record.severity_text.len() as u64))
                    .and_then(|n| n.checked_add(record.event_name.len() as u64))
                    .and_then(|n| n.checked_add(record.encoded.len() as u64))
                    .ok_or_else(|| "logs v2 size accounting overflow".to_string())?,
            )
            .ok_or_else(|| "logs v2 block size accounting overflow".to_string())?;

        self.fingerprints.push(fingerprint);
        self.ts.push(ts);
        self.severities.push(severity);
        self.bodies.push(body);
        self.attributes.push(attrs);
        // V2 represents the exact observed timestamp, including zero. NULL is
        // reserved for historical v1 rows normalized into the v2 query schema.
        self.observed_ts.push(Some(record.observed_time_unix_nano));
        // V2 always represents these fields, including a present empty string.
        // NULL is reserved for v1 rows normalized into the stable v2 schema.
        self.severity_texts
            .push(Some(record.severity_text.to_owned()));
        self.event_names.push(Some(record.event_name.to_owned()));
        self.trace_ids.push(record.trace_id.copied());
        self.span_ids.push(record.span_id.copied());
        self.trace_flags.push(Some(flags));
        self.raw_record_versions
            .push(Some(scry_proto::constants::LOGS_RAW_VERSION_V1));
        self.raw_records.push(Some(record.encoded.to_vec()));
        if stream_is_new {
            assert!(self.stream_seen.insert(fingerprint));
            self.stream_dict.push(OwnedStream {
                fingerprint,
                labels: std::mem::take(&mut self.label_scratch),
            });
        } else {
            self.label_scratch.clear();
        }
        Ok(())
    }
}

impl LogsBlockBuilder {
    /// Body of [`BlockBuilder::finish_and_upload`]. Split out for
    /// the `mut self` rebinding ergonomic — see `dummy.rs` /
    /// `metrics.rs` for the same pattern.
    async fn finish_and_upload_impl(self, store: &dyn ObjectStore) -> Result<Option<BlockMeta>> {
        if self.is_empty() {
            return Ok(None);
        }
        // Offload the CPU-heavy encode (sort + Arrow build + zstd +
        // postings) onto the blocking pool so it doesn't monopolise an
        // async worker thread; the PUTs run back here on the async side.
        let enc = tokio::task::spawn_blocking(move || self.encode())
            .await
            .context("join logs encode task")??;
        crate::put_block_objects(store, enc.puts).await?;
        let meta = enc.meta;
        tracing::info!(
            block_uuid = %meta.uuid,
            row_count = meta.row_count,
            stream_count = meta.all_fingerprints.as_ref().map_or(0, |v| v.len()),
            byte_size = meta.byte_size,
            postings_size = meta.postings_size_bytes.unwrap_or(0),
            ts_min = meta.ts_min_unix_nano,
            ts_max = meta.ts_max_unix_nano,
            "logs block uploaded"
        );
        Ok(Some(meta))
    }

    /// Encode buffered log entries into the main + postings parquet and
    /// the JSON sidecar. Pure CPU, no I/O — runs on the blocking pool
    /// via `spawn_blocking`. The async `finish_and_upload_impl` performs
    /// the PUTs.
    fn encode(mut self) -> Result<EncodedBlock> {
        let n = self.fingerprints.len();

        // ── Main parquet ───────────────────────────────────────────
        //
        // Sort permutation over (stream_fingerprint, ts) ascending.
        // Same shape as metrics: the postings index pays off because
        // sorted rows let parquet's row-group min/max stats on the
        // fingerprint column skip most groups once a query has
        // resolved its target fingerprint set.
        let row_count = u32::try_from(n).context("logs block row count exceeds u32 indexing")?;
        let mut order: Vec<u32> = (0..row_count).collect();
        order.sort_by_key(|&i| (self.fingerprints[i as usize], self.ts[i as usize]));

        let schema = self.schema.expect("non-empty logs builder has a schema");
        let main_schema = match schema {
            LogsSchema::V1 => logs_physical_schema_v1(),
            LogsSchema::V2 => logs_physical_schema_v2(),
        };
        let fp_arr: ArrayRef = Arc::new(UInt64Array::from_iter_values(
            order.iter().map(|&i| self.fingerprints[i as usize]),
        ));
        let ts_arr: ArrayRef = Arc::new(UInt64Array::from_iter_values(
            order.iter().map(|&i| self.ts[i as usize]),
        ));
        let sev_arr: ArrayRef = Arc::new(UInt8Array::from_iter_values(
            order.iter().map(|&i| self.severities[i as usize]),
        ));
        let body_arr: ArrayRef = Arc::new(StringArray::from_iter_values(
            order.iter().map(|&i| self.bodies[i as usize].as_str()),
        ));
        // Attributes: walk the permutation, emit each entry's
        // attribute map. MapBuilder's defaults
        // ("entries"/"keys"/"values") match the schema field names
        // declared in `main_schema()` above.
        let mut attr_builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for &i in order.iter() {
            for (k, v) in &self.attributes[i as usize] {
                attr_builder.keys().append_value(k);
                attr_builder.values().append_value(v);
            }
            attr_builder
                .append(true)
                .context("MapBuilder::append (attributes)")?;
        }
        let attr_arr: ArrayRef = Arc::new(attr_builder.finish());

        // ── Body bloom (full-text skip sidecar) ────────────────────
        //
        // Byte-trigram bloom over every body, built from the complete
        // set so it's sized optimally for this block's distinct-gram
        // count. Order is irrelevant (a bloom is a set), so we build
        // straight from `self.bodies` before the buffers are released.
        // Keep auxiliary full-text indexing inside the same approximate memory
        // envelope as the retained block. A pathological set of distinct grams
        // may omit the optional bloom, but never risks an ingest-time OOM; exact
        // body filtering remains the query backstop.
        let bloom_budget = usize::try_from(self.cfg.target_bytes)
            .unwrap_or(usize::MAX)
            .max(1);
        let mut bloom_builder = BodyBloomBuilder::new(self.cfg.bloom_ngram);
        let mut bloom_complete = true;
        for body in &self.bodies {
            if !bloom_builder.add_body_bounded(body, bloom_budget) {
                bloom_complete = false;
                break;
            }
        }
        let body_bloom = bloom_complete
            .then(|| bloom_builder.finish_bounded(self.cfg.bloom_target_fpr, bloom_budget))
            .flatten();
        let bloom_bytes = body_bloom.map(|bloom| Bytes::from(bloom.to_bytes()));
        let bloom_size = bloom_bytes.as_ref().map(|bytes| bytes.len() as u64);

        let mut columns = vec![fp_arr, ts_arr, sev_arr, body_arr, attr_arr];
        if schema == LogsSchema::V2 {
            columns.push(Arc::new(UInt64Array::from_iter(
                order.iter().map(|&i| self.observed_ts[i as usize]),
            )));
            columns.push(Arc::new(StringArray::from_iter(
                order
                    .iter()
                    .map(|&i| self.severity_texts[i as usize].as_deref()),
            )));
            columns.push(Arc::new(StringArray::from_iter(
                order
                    .iter()
                    .map(|&i| self.event_names[i as usize].as_deref()),
            )));
            let mut trace_ids = FixedSizeBinaryBuilder::with_capacity(n, 16);
            let mut span_ids = FixedSizeBinaryBuilder::with_capacity(n, 8);
            for &i in &order {
                match self.trace_ids[i as usize] {
                    Some(value) => trace_ids.append_value(value)?,
                    None => trace_ids.append_null(),
                }
                match self.span_ids[i as usize] {
                    Some(value) => span_ids.append_value(value)?,
                    None => span_ids.append_null(),
                }
            }
            columns.push(Arc::new(trace_ids.finish()));
            columns.push(Arc::new(span_ids.finish()));
            columns.push(Arc::new(UInt8Array::from_iter(
                order.iter().map(|&i| self.trace_flags[i as usize]),
            )));
            columns.push(Arc::new(UInt16Array::from_iter(
                order.iter().map(|&i| self.raw_record_versions[i as usize]),
            )));
            columns.push(Arc::new(BinaryArray::from_iter(
                order
                    .iter()
                    .map(|&i| self.raw_records[i as usize].as_deref()),
            )));
        }

        drop(order);
        // Release source buffers — Arrow now owns column copies.
        self.fingerprints = Vec::new();
        self.ts = Vec::new();
        self.severities = Vec::new();
        self.bodies = Vec::new();
        self.attributes = Vec::new();

        let main_batch = RecordBatch::try_new(main_schema.clone(), columns)
            .context("constructing logs main RecordBatch")?;

        let props = self.cfg.main_writer_props()?;
        let mut main_buf: Vec<u8> = Vec::with_capacity(self.bytes_est as usize);
        {
            let mut w = ArrowWriter::try_new(&mut main_buf, main_schema, Some(props.clone()))
                .context("ArrowWriter::try_new (logs main)")?;
            w.write(&main_batch)
                .context("ArrowWriter::write (logs main)")?;
            w.close().context("ArrowWriter::close (logs main)")?;
        }
        let main_bytes = Bytes::from(main_buf);
        let byte_size = main_bytes.len() as u64;

        // ── Postings parquet ───────────────────────────────────────
        //
        // Same shape as metrics: HashMap-built inverted index over
        // stream labels, sorted on output. The cost analysis from
        // metrics applies one-for-one — see the TODO there about
        // dictionary-interning if cardinality ever climbs.
        let postings = self.build_postings();
        let postings_props = self.cfg.postings_writer_props()?;
        let postings_bytes = crate::postings::encode_postings(&postings, &postings_props)?;
        let postings_size = postings_bytes.len() as u64;

        // ── Sidecar JSON ───────────────────────────────────────────
        let block_uuid = self.block_uuid.unwrap_or_else(Uuid::now_v7);
        let all_fingerprints: Vec<u64> = self.stream_dict.iter().map(|s| s.fingerprint).collect();
        let meta = BlockMeta {
            uuid: block_uuid,
            signal: SIGNAL.to_string(),
            writer_id: self.writer_id,
            ts_min_unix_nano: self.ts_min,
            ts_max_unix_nano: self.ts_max,
            row_count: n as u64,
            byte_size,
            schema_version: schema.version(),
            level: 0,
            compacted_from: Vec::new(),
            producer_version: env!("CARGO_PKG_VERSION").to_string(),
            label_fingerprint_bloom: None,
            has_postings: true,
            postings_size_bytes: Some(postings_size),
            // Logs have no per-fingerprint type metadata (no
            // counter-vs-gauge equivalent). `series_types` stays
            // `None`; queries that need the full fingerprint set use
            // `all_fingerprints` instead — the signal-agnostic shape
            // that drives `scry_query::postings::resolve_fingerprints`.
            series_types: None,
            all_fingerprints: Some(all_fingerprints),
            has_body_bloom: bloom_bytes.is_some(),
            body_bloom_size_bytes: bloom_size,
            wal_seg_max: self.cfg.wal_seg_max,
            wal_shard: self.cfg.wal_shard,
        };
        let meta_bytes =
            Bytes::from(serde_json::to_vec_pretty(&meta).context("serialising logs BlockMeta")?);

        // ── Upload order: main → postings → meta ───────────────────
        //
        // Same ordering invariant as metrics: the meta.json sidecar
        // is the "block exists" signal for catalog reconcile. The
        // only durable persistence ordering that matters is "meta
        // last."
        let main_path = Path::from(block_path(
            SIGNAL,
            self.ts_min,
            self.writer_id,
            block_uuid,
            "parquet",
        ));
        let postings_path = Path::from(block_path(
            SIGNAL,
            self.ts_min,
            self.writer_id,
            block_uuid,
            "postings.parquet",
        ));
        let bloom_path = Path::from(block_path(
            SIGNAL,
            self.ts_min,
            self.writer_id,
            block_uuid,
            "body.bloom",
        ));
        let meta_path = Path::from(block_path(
            SIGNAL,
            self.ts_min,
            self.writer_id,
            block_uuid,
            "meta.json",
        ));

        let mut puts = vec![(main_path, main_bytes), (postings_path, postings_bytes)];
        if let Some(bloom_bytes) = bloom_bytes {
            puts.push((bloom_path, bloom_bytes));
        }
        puts.push((meta_path, meta_bytes));
        Ok(EncodedBlock { meta, puts })
    }

    /// Walk the stream dictionary, building
    /// `Vec<((name, value), sorted fingerprints)>` keyed in
    /// lexicographic `(name, value)` order for the postings
    /// parquet. Mirrors `MetricsBlockBuilder::build_postings`.
    fn build_postings(&self) -> Vec<((String, String), Vec<u64>)> {
        use std::collections::HashMap;
        let mut inv: HashMap<(String, String), Vec<u64>> = HashMap::new();
        for stream in &self.stream_dict {
            for (k, v) in &stream.labels {
                inv.entry((k.clone(), v.clone()))
                    .or_default()
                    .push(stream.fingerprint);
            }
        }
        let mut entries: Vec<((String, String), Vec<u64>)> = inv.into_iter().collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        for (_, fps) in entries.iter_mut() {
            fps.sort_unstable();
            fps.dedup();
        }
        entries
    }
}

#[cfg(test)]
mod schema_tests {
    use super::*;
    use arrow::array::{Array, FixedSizeBinaryArray, MapArray};

    #[test]
    fn schemas_are_exact_and_default_schema_is_v1() {
        let v1 = logs_physical_schema_v1();
        let v2 = logs_physical_schema_v2();
        assert_eq!(LogsBlockBuilder::main_schema(), v1);
        assert_eq!(v1.fields().len(), 5);
        assert_eq!(v2.fields().len(), 13);
        let expected = [
            ("stream_fingerprint", DataType::UInt64, false),
            ("ts_unix_nano", DataType::UInt64, false),
            ("severity", DataType::UInt8, false),
            ("body", DataType::Utf8, false),
            ("attributes", v1.field(4).data_type().clone(), false),
            ("observed_ts_unix_nano", DataType::UInt64, true),
            ("severity_text", DataType::Utf8, true),
            ("event_name", DataType::Utf8, true),
            ("trace_id", DataType::FixedSizeBinary(16), true),
            ("span_id", DataType::FixedSizeBinary(8), true),
            ("trace_flags", DataType::UInt8, true),
            ("raw_record_version", DataType::UInt16, true),
            ("raw_record", DataType::Binary, true),
        ];
        for (field, (name, ty, nullable)) in v2.fields().iter().zip(expected) {
            assert_eq!(field.name(), name);
            assert_eq!(field.data_type(), &ty);
            assert_eq!(field.is_nullable(), nullable);
        }
        assert!(v2.fields()[..5]
            .iter()
            .zip(v1.fields())
            .all(|(left, right)| left == right));
    }

    fn put_string(out: &mut Vec<u8>, value: &str) {
        out.extend_from_slice(&(value.len() as u32).to_be_bytes());
        out.extend_from_slice(value.as_bytes());
    }

    fn put_value_string(out: &mut Vec<u8>, value: &str) {
        out.push(1);
        put_string(out, value);
    }

    fn put_map(out: &mut Vec<u8>, entries: &[(&str, &str)]) {
        out.push(7);
        out.extend_from_slice(&(entries.len() as u32).to_be_bytes());
        for (key, value) in entries {
            put_string(out, key);
            put_value_string(out, value);
        }
    }

    fn v2_payload() -> (Vec<u8>, Vec<u8>) {
        let mut record = Vec::new();
        record.extend_from_slice(&0u16.to_be_bytes());
        put_string(&mut record, "resource/v1");
        record.extend_from_slice(&0u32.to_be_bytes());
        put_map(&mut record, &[("service", "api")]);
        record.push(1);
        put_string(&mut record, "scope");
        put_string(&mut record, "1.0");
        record.extend_from_slice(&0u32.to_be_bytes());
        put_map(&mut record, &[]);
        put_string(&mut record, "scope/v1");
        record.extend_from_slice(&0u64.to_be_bytes());
        record.extend_from_slice(&42u64.to_be_bytes());
        record.extend_from_slice(&17i32.to_be_bytes());
        put_string(&mut record, "ERROR");
        put_string(&mut record, "exception");
        // Typed body and attributes exercise canonical-text projection.
        record.push(3);
        record.extend_from_slice(&(-9i64).to_be_bytes());
        record.extend_from_slice(&0u32.to_be_bytes());
        put_map(&mut record, &[("answer", "yes")]);
        record.extend_from_slice(&1u32.to_be_bytes());
        record.push(16);
        record.extend_from_slice(&[1; 16]);
        record.push(8);
        record.extend_from_slice(&[2; 8]);

        let mut payload = Vec::new();
        payload.extend_from_slice(&scry_proto::constants::LOGS_BATCH_V2_MAGIC.to_be_bytes());
        payload.extend_from_slice(&scry_proto::constants::LOGS_RAW_VERSION_V1.to_be_bytes());
        payload.extend_from_slice(&1u32.to_be_bytes());
        payload.extend_from_slice(&(record.len() as u32).to_be_bytes());
        payload.extend_from_slice(&record);
        (payload, record)
    }

    #[test]
    fn v2_selects_schema_projects_and_preserves_raw() {
        let (payload, raw) = v2_payload();
        let mut builder = LogsBlockBuilder::new(Uuid::nil(), BlockBuilderConfig::default());
        scry_proto::decode_logs_batch_v2_into(
            &payload,
            scry_proto::LogsV2DecodeLimits::default(),
            &mut builder,
        )
        .unwrap();
        assert_eq!(builder.content_schema_key(), Some(2));

        let encoded = builder.encode().unwrap();
        assert_eq!(encoded.meta.schema_version, 2);
        assert_eq!(encoded.meta.ts_min_unix_nano, 42);
        assert_eq!(encoded.meta.ts_max_unix_nano, 42);
        let postings = encoded
            .puts
            .iter()
            .find(|(path, _)| path.as_ref().ends_with("postings.parquet"))
            .unwrap();
        let decoded_postings = crate::postings::decode_postings(postings.1.clone()).unwrap();
        assert_eq!(
            decoded_postings
                .iter()
                .map(|((name, value), _)| (name.as_str(), value.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("otel.scope.name", "scope"),
                ("otel.scope.version", "1.0"),
                // Identity-bearing typed resource values retain their type.
                ("service", "\"api\""),
            ]
        );
        assert!(decoded_postings.iter().all(
            |(_, fingerprints)| fingerprints == &encoded.meta.all_fingerprints.clone().unwrap()
        ));

        let main = encoded
            .puts
            .iter()
            .find(|(path, _)| {
                path.as_ref().ends_with(".parquet") && !path.as_ref().ends_with("postings.parquet")
            })
            .unwrap();
        let reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(main.1.clone())
                .unwrap()
                .build()
                .unwrap();
        let batch = reader.into_iter().next().unwrap().unwrap();
        assert_eq!(batch.schema(), logs_physical_schema_v2());
        assert_eq!(batch.num_columns(), 13);
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            42
        );
        assert_eq!(
            batch
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "-9"
        );
        let attrs = batch.column(4).as_any().downcast_ref::<MapArray>().unwrap();
        let entries = attrs.value(0);
        let entries = entries
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();
        assert_eq!(
            entries
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "answer"
        );
        assert_eq!(
            entries
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "\"yes\""
        );
        assert_eq!(
            batch
                .column(5)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            42
        );
        assert_eq!(
            batch
                .column(6)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "ERROR"
        );
        assert_eq!(
            batch
                .column(7)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "exception"
        );
        assert_eq!(
            batch
                .column(8)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(0),
            &[1; 16]
        );
        assert_eq!(
            batch
                .column(9)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(0),
            &[2; 8]
        );
        assert_eq!(
            batch
                .column(10)
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .value(0),
            1
        );
        assert_eq!(
            batch
                .column(11)
                .as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .value(0),
            scry_proto::constants::LOGS_RAW_VERSION_V1
        );
        assert_eq!(
            batch
                .column(12)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            raw
        );
        for column in batch.columns().iter().skip(5) {
            assert_eq!(column.null_count(), 0);
        }
    }

    #[test]
    fn v2_schema_resets_after_drain() {
        let (payload, _) = v2_payload();
        let mut source = LogsBlockBuilder::new(Uuid::nil(), BlockBuilderConfig::default());
        let mut target = LogsBlockBuilder::new(Uuid::nil(), BlockBuilderConfig::default());
        scry_proto::decode_logs_batch_v2_into(&payload, Default::default(), &mut source).unwrap();
        target.merge(&mut source);
        assert_eq!(target.content_schema_key(), Some(2));
        assert_eq!(source.content_schema_key(), None);
        source.append_entry(1, 1, 1, b"v1".to_vec(), vec![]);
        assert_eq!(source.content_schema_key(), Some(1));
    }

    #[test]
    fn empty_merge_resets_selected_schema_and_dictionary_state() {
        let mut target = LogsBlockBuilder::new(Uuid::nil(), BlockBuilderConfig::default());
        let mut source = LogsBlockBuilder::new(Uuid::nil(), BlockBuilderConfig::default());
        source.observe_stream(7, vec![(b"service".to_vec(), b"orphan".to_vec())]);
        assert!(source.is_empty());
        assert_eq!(source.content_schema_key(), Some(1));
        target.merge(&mut source);
        assert_eq!(source.content_schema_key(), None);
        assert!(source.stream_dict.is_empty());
        assert!(target.stream_dict.is_empty());

        LogsV2Appender::begin_batch(&mut source, 0).unwrap();
        assert_eq!(source.content_schema_key(), Some(2));
        target.merge(&mut source);
        assert_eq!(source.content_schema_key(), None);
        assert_eq!(target.content_schema_key(), None);
    }

    #[test]
    fn v2_selection_and_invalid_projection_reset_cleanly() {
        let mut empty_payload = Vec::new();
        empty_payload.extend_from_slice(&scry_proto::constants::LOGS_BATCH_V2_MAGIC.to_be_bytes());
        empty_payload.extend_from_slice(&scry_proto::constants::LOGS_RAW_VERSION_V1.to_be_bytes());
        empty_payload.extend_from_slice(&0u32.to_be_bytes());
        let mut builder = LogsBlockBuilder::new(Uuid::nil(), BlockBuilderConfig::default());
        scry_proto::decode_logs_batch_v2_into(&empty_payload, Default::default(), &mut builder)
            .unwrap();
        assert!(builder.is_empty());
        assert_eq!(builder.content_schema_key(), Some(2));
        builder.reset();
        assert_eq!(builder.content_schema_key(), None);

        let (payload, _) = v2_payload();
        let mut timestamp_and_severity = Vec::new();
        timestamp_and_severity.extend_from_slice(&42u64.to_be_bytes());
        timestamp_and_severity.extend_from_slice(&17i32.to_be_bytes());
        let severity_offset = payload
            .windows(timestamp_and_severity.len())
            .position(|window| window == timestamp_and_severity)
            .unwrap()
            + 8;
        let mut invalid = payload;
        invalid[severity_offset..severity_offset + 4].copy_from_slice(&256i32.to_be_bytes());
        let err = scry_proto::decode_logs_batch_v2_into(&invalid, Default::default(), &mut builder)
            .unwrap_err();
        assert!(err.to_string().contains("severity_number does not fit u8"));
        assert!(builder.is_empty());
        assert_eq!(builder.content_schema_key(), Some(2));
        builder.reset();
        assert_eq!(builder.content_schema_key(), None);
    }
}
