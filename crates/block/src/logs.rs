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
    ArrayRef, ListArray, MapBuilder, StringArray, StringBuilder, UInt64Array, UInt64Builder,
    UInt8Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::{path::Path, ObjectStore, ObjectStoreExt};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use scry_proto::streaming::LogsAppender;
use uuid::Uuid;

use crate::{block_path, BlockBuilder, BlockBuilderConfig, BlockMeta, EncodedBlock};

const SIGNAL: &str = "logs";
const SCHEMA_VERSION: u32 = 1;

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
        // Map<Utf8,Utf8>: the canonical Arrow Map layout used by
        // MapBuilder<StringBuilder, StringBuilder>. Field names
        // ("entries"/"keys"/"values") match MapBuilder's defaults
        // so the schema and the builder agree without overrides.
        //
        // `values` is declared nullable to match MapBuilder's default
        // (its inner StringBuilder accepts nulls). Our writer never
        // actually emits a null value — `append_entry` UTF-8-coerces
        // every attribute string — but the column type must match the
        // builder's output type exactly or `RecordBatch::try_new`
        // rejects the batch.
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", DataType::Utf8, false),
                Field::new("values", DataType::Utf8, true),
            ])),
            false,
        ));
        Arc::new(Schema::new(vec![
            Field::new("stream_fingerprint", DataType::UInt64, false),
            Field::new("ts_unix_nano", DataType::UInt64, false),
            Field::new("severity", DataType::UInt8, false),
            Field::new("body", DataType::Utf8, false),
            Field::new(
                "attributes",
                DataType::Map(entries_field, /*keys_sorted=*/ false),
                false,
            ),
        ]))
    }

    pub fn postings_schema() -> SchemaRef {
        // Identical schema to metrics' postings sidecar; the
        // postings cache and query-side resolver are signal-agnostic
        // and rely on this shape exactly.
        let inner = Field::new("item", DataType::UInt64, false);
        Arc::new(Schema::new(vec![
            Field::new("label_name", DataType::Utf8, false),
            Field::new("label_value", DataType::Utf8, false),
            Field::new(
                "stream_fingerprints",
                DataType::List(Arc::new(inner)),
                false,
            ),
        ]))
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
            cfg,
            fingerprints: Vec::with_capacity(4096),
            ts: Vec::with_capacity(4096),
            severities: Vec::with_capacity(4096),
            bodies: Vec::with_capacity(4096),
            attributes: Vec::with_capacity(4096),
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

    fn merge(&mut self, other: &mut Self) {
        // Entry columns: a bulk move each — `append` drains `other`'s
        // vec and keeps its capacity for reuse.
        self.fingerprints.append(&mut other.fingerprints);
        self.ts.append(&mut other.ts);
        self.severities.append(&mut other.severities);
        self.bodies.append(&mut other.bodies);
        self.attributes.append(&mut other.attributes);

        // Stream dictionary: dedup against the *shared* builder's
        // `stream_seen` (same policy as metrics' series dedup).
        for s in other.stream_dict.drain(..) {
            if self.stream_seen.insert(s.fingerprint) {
                self.stream_dict.push(s);
            }
        }
        other.stream_seen.clear();

        self.bytes_est += other.bytes_est;
        self.ts_min = self.ts_min.min(other.ts_min);
        self.ts_max = self.ts_max.max(other.ts_max);

        other.bytes_est = 0;
        other.ts_min = u64::MAX;
        other.ts_max = 0;
    }

    fn reset(&mut self) {
        self.fingerprints.clear();
        self.ts.clear();
        self.severities.clear();
        self.bodies.clear();
        self.attributes.clear();
        self.stream_seen.clear();
        self.stream_dict.clear();
        self.bytes_est = 0;
        self.ts_min = u64::MAX;
        self.ts_max = 0;
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

impl LogsBlockBuilder {
    /// Body of [`BlockBuilder::finish_and_upload`]. Split out for
    /// the `mut self` rebinding ergonomic — see `dummy.rs` /
    /// `metrics.rs` for the same pattern.
    async fn finish_and_upload_impl(
        self,
        store: &dyn ObjectStore,
    ) -> Result<Option<BlockMeta>> {
        if self.is_empty() {
            return Ok(None);
        }
        // Offload the CPU-heavy encode (sort + Arrow build + zstd +
        // postings) onto the blocking pool so it doesn't monopolise an
        // async worker thread; the PUTs run back here on the async side.
        let enc = tokio::task::spawn_blocking(move || self.encode())
            .await
            .context("join logs encode task")??;
        for (path, bytes) in enc.puts {
            store
                .put(&path, bytes.into())
                .await
                .with_context(|| format!("upload {path}"))?;
        }
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
        let mut order: Vec<u32> = (0..n as u32).collect();
        order.sort_by_key(|&i| (self.fingerprints[i as usize], self.ts[i as usize]));

        let main_schema = Self::main_schema();
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
        let mut attr_builder = MapBuilder::new(
            None,
            StringBuilder::new(),
            StringBuilder::new(),
        );
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

        drop(order);
        // Release source buffers — Arrow now owns column copies.
        self.fingerprints = Vec::new();
        self.ts = Vec::new();
        self.severities = Vec::new();
        self.bodies = Vec::new();
        self.attributes = Vec::new();

        let main_batch = RecordBatch::try_new(
            main_schema.clone(),
            vec![fp_arr, ts_arr, sev_arr, body_arr, attr_arr],
        )
        .context("constructing logs main RecordBatch")?;

        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
            .set_max_row_group_row_count(Some(self.cfg.row_group_size))
            .build();
        let mut main_buf: Vec<u8> = Vec::with_capacity(self.bytes_est as usize);
        {
            let mut w = ArrowWriter::try_new(&mut main_buf, main_schema, Some(props.clone()))
                .context("ArrowWriter::try_new (logs main)")?;
            w.write(&main_batch).context("ArrowWriter::write (logs main)")?;
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
        let postings_bytes = self.encode_postings(postings, &props)?;
        let postings_size = postings_bytes.len() as u64;

        // ── Sidecar JSON ───────────────────────────────────────────
        let block_uuid = Uuid::now_v7();
        let all_fingerprints: Vec<u64> =
            self.stream_dict.iter().map(|s| s.fingerprint).collect();
        let meta = BlockMeta {
            uuid: block_uuid,
            signal: SIGNAL.to_string(),
            writer_id: self.writer_id,
            ts_min_unix_nano: self.ts_min,
            ts_max_unix_nano: self.ts_max,
            row_count: n as u64,
            byte_size,
            schema_version: SCHEMA_VERSION,
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
        };
        let meta_bytes = Bytes::from(
            serde_json::to_vec_pretty(&meta).context("serialising logs BlockMeta")?,
        );

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
        let meta_path = Path::from(block_path(
            SIGNAL,
            self.ts_min,
            self.writer_id,
            block_uuid,
            "meta.json",
        ));

        Ok(EncodedBlock {
            meta,
            puts: vec![
                (main_path, main_bytes),
                (postings_path, postings_bytes),
                (meta_path, meta_bytes),
            ],
        })
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

    fn encode_postings(
        &self,
        entries: Vec<((String, String), Vec<u64>)>,
        props: &WriterProperties,
    ) -> Result<Bytes> {
        let schema = Self::postings_schema();
        if entries.is_empty() {
            // Write an empty parquet so the file is always present
            // when has_postings=true. Mirrors metrics behaviour;
            // query side detects empty by row count.
            let empty_main = RecordBatch::new_empty(schema.clone());
            let mut buf: Vec<u8> = Vec::new();
            let mut w = ArrowWriter::try_new(&mut buf, schema, Some(props.clone()))
                .context("ArrowWriter::try_new (empty logs postings)")?;
            w.write(&empty_main).context("ArrowWriter::write (empty logs postings)")?;
            w.close().context("ArrowWriter::close (empty logs postings)")?;
            return Ok(Bytes::from(buf));
        }

        let names: StringArray =
            entries.iter().map(|((k, _), _)| Some(k.as_str())).collect();
        let values: StringArray =
            entries.iter().map(|((_, v), _)| Some(v.as_str())).collect();

        let total_fps: usize = entries.iter().map(|(_, fps)| fps.len()).sum();
        let mut values_builder = UInt64Builder::with_capacity(total_fps);
        let mut offsets: Vec<i32> = Vec::with_capacity(entries.len() + 1);
        let mut running: i32 = 0;
        offsets.push(running);
        for (_, fps) in entries.iter() {
            for &fp in fps {
                values_builder.append_value(fp);
            }
            running = running
                .checked_add(fps.len() as i32)
                .expect("logs postings offset overflow (i32); see LargeListArray TODO in metrics.rs");
            offsets.push(running);
        }
        debug_assert!(running >= 0);
        let values_array = Arc::new(values_builder.finish());
        let offset_buf = OffsetBuffer::new(offsets.into());
        let field = match Self::postings_schema().field(2).data_type() {
            DataType::List(f) => f.clone(),
            other => {
                anyhow::bail!("logs postings schema column 2 should be List, found {other:?}")
            }
        };
        let list = ListArray::new(field, offset_buf, values_array, None);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(names), Arc::new(values), Arc::new(list)],
        )
        .context("constructing logs postings RecordBatch")?;

        let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
        let mut w = ArrowWriter::try_new(&mut buf, schema, Some(props.clone()))
            .context("ArrowWriter::try_new (logs postings)")?;
        w.write(&batch).context("ArrowWriter::write (logs postings)")?;
        w.close().context("ArrowWriter::close (logs postings)")?;
        Ok(Bytes::from(buf))
    }
}
