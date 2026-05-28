//! Block builder for metrics samples — the v0.2 first-real-signal.
//!
//! Per `ARCHITECTURE.md § Metrics`, a metrics block consists of three
//! objects in the bucket:
//!
//! - `<block>.parquet` — `(series_fingerprint, ts_unix_nano, value)`
//!   sorted by `(series_fingerprint, ts)`. The intra-block sort makes
//!   parquet row-group min/max stats on the fingerprint column an
//!   aggressive pruning lever once a query has resolved its target
//!   fingerprint set.
//! - `<block>.postings.parquet` — `(label_name, label_value,
//!   series_fingerprints LIST<u64>)` sorted by `(label_name,
//!   label_value)`. This is the inverted index that turns a
//!   `metric{service="api", env="prod"}` predicate into a small
//!   fingerprint set without scanning the main parquet.
//! - `<block>.meta.json` — the catalog's source of truth for block
//!   existence; carries `has_postings`/`postings_size_bytes` plus the
//!   per-block `series_types` map (since the canonical postings schema
//!   has nowhere to encode counter-vs-gauge intent).
//!
//! Wire input is `MetricsBatch { series: Vec<SeriesDictEntry>, samples:
//! Vec<MetricSample> }` (see `scry_proto::generated`). Each batch
//! re-sends whatever portion of its series dictionary the agent
//! considered active; we dedup by fingerprint server-side. The hot
//! ingest path is per-sample (3 × u64 + 1 × f64 = 24 bytes); series
//! ingestion is amortised across many samples.
//!
//! ## CSR layout
//!
//! Hot-path sample storage uses three parallel `Vec`s instead of
//! `Vec<MetricSample>` so the data lives in column-shaped memory —
//! matches Arrow's internal layout, which lets `from_iter_values` walk
//! each column as a single contiguous memcpy at parquet-encode time.
//! Same lesson as `crates/block/src/dummy.rs`; see CLAUDE.md
//! § Performance.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    ArrayRef, Float64Array, ListArray, StringArray, UInt64Array, UInt64Builder,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::{path::Path, ObjectStore, ObjectStoreExt};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use scry_proto::streaming::MetricsAppender;
use uuid::Uuid;

use crate::{block_path, BlockBuilder, BlockBuilderConfig, BlockMeta, EncodedBlock};

const SIGNAL: &str = "metrics";
const SCHEMA_VERSION: u32 = 1;

/// One unique series accumulated for this block. Owned labels because
/// we dedup by fingerprint and the wire payload is dropped after
/// decode — we have to copy the bytes somewhere if we want them at
/// finish time, and the postings build later needs them as
/// `&str` for Arrow `StringArray::from_iter_values`.
struct OwnedSeries {
    fingerprint: u64,
    metric_type: u8,
    labels: Vec<(String, String)>,
}

/// In-memory metrics block under construction.
pub struct MetricsBlockBuilder {
    writer_id: Uuid,
    cfg: BlockBuilderConfig,
    // Per-sample column-shaped storage (hot path).
    fingerprints: Vec<u64>,
    ts: Vec<u64>,
    values: Vec<f64>,
    // Per-series dedup. `series_seen` cheaply rejects duplicates;
    // `series_dict` keeps them in insertion order (mostly for stable
    // postings output during tests — order doesn't matter to query
    // correctness).
    series_seen: HashSet<u64>,
    series_dict: Vec<OwnedSeries>,
    bytes_est: u64,
    ts_min: u64,
    ts_max: u64,
}

impl MetricsBlockBuilder {
    pub fn main_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("series_fingerprint", DataType::UInt64, false),
            Field::new("ts_unix_nano", DataType::UInt64, false),
            Field::new("value", DataType::Float64, false),
        ]))
    }

    pub fn postings_schema() -> SchemaRef {
        // List items are non-nullable u64 fingerprints.
        let inner = Field::new("item", DataType::UInt64, false);
        Arc::new(Schema::new(vec![
            Field::new("label_name", DataType::Utf8, false),
            Field::new("label_value", DataType::Utf8, false),
            Field::new(
                "series_fingerprints",
                DataType::List(Arc::new(inner)),
                false,
            ),
        ]))
    }

    pub fn row_count(&self) -> u64 {
        self.fingerprints.len() as u64
    }
}

impl BlockBuilder for MetricsBlockBuilder {
    const SIGNAL: &'static str = SIGNAL;

    fn new(writer_id: Uuid, cfg: BlockBuilderConfig) -> Self {
        Self {
            writer_id,
            cfg,
            fingerprints: Vec::with_capacity(4096),
            ts: Vec::with_capacity(4096),
            values: Vec::with_capacity(4096),
            series_seen: HashSet::with_capacity(256),
            series_dict: Vec::with_capacity(256),
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
        // Sample columns: a bulk move each — `append` drains `other`'s
        // vec and keeps its capacity for reuse.
        self.fingerprints.append(&mut other.fingerprints);
        self.ts.append(&mut other.ts);
        self.values.append(&mut other.values);

        // Series dictionary: dedup against the *shared* builder's
        // `series_seen` so cross-batch dedup scope matches decoding
        // straight into the shared builder. A fingerprint already
        // accumulated here is dropped (labels are assumed identical for
        // a given fingerprint — same trust as `observe_series`).
        for s in other.series_dict.drain(..) {
            if self.series_seen.insert(s.fingerprint) {
                self.series_dict.push(s);
            }
        }
        other.series_seen.clear();

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
        self.values.clear();
        self.series_seen.clear();
        self.series_dict.clear();
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

impl MetricsAppender for MetricsBlockBuilder {
    fn observe_series(
        &mut self,
        fingerprint: u64,
        metric_type: u8,
        labels: Vec<(Vec<u8>, Vec<u8>)>,
    ) {
        if !self.series_seen.insert(fingerprint) {
            // Already accumulated this series in an earlier batch.
            // Wire spec assumes the labels are identical across
            // batches for a given fingerprint (hash is over the
            // labels); we trust that without re-verifying.
            return;
        }
        // Convert wire bytes to UTF-8 Strings. Label keys/values are
        // strings on the wire (per `LabelPair`'s encode); if the agent
        // sent invalid UTF-8 we substitute U+FFFD rather than failing
        // the whole block. A misbehaving agent shouldn't poison
        // ingest.
        let owned: Vec<(String, String)> = labels
            .into_iter()
            .map(|(k, v)| {
                (
                    String::from_utf8_lossy(&k).into_owned(),
                    String::from_utf8_lossy(&v).into_owned(),
                )
            })
            .collect();
        self.series_dict.push(OwnedSeries {
            fingerprint,
            metric_type,
            labels: owned,
        });
    }

    fn append_sample(&mut self, fingerprint: u64, ts_unix_nano: u64, value: f64) {
        self.ts_min = self.ts_min.min(ts_unix_nano);
        self.ts_max = self.ts_max.max(ts_unix_nano);
        // Each sample is 24 bytes on disk after parquet encoding
        // (3 × 8). Real compressed size is much smaller, but the
        // estimate is for "stop accumulating" pacing, not exact
        // accounting.
        self.bytes_est += 24;
        self.fingerprints.push(fingerprint);
        self.ts.push(ts_unix_nano);
        self.values.push(value);
    }
}

impl MetricsBlockBuilder {
    /// Body of [`BlockBuilder::finish_and_upload`]. Split out for the
    /// `mut self` rebinding ergonomic — see `dummy.rs` for the same
    /// pattern.
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
            .context("join metrics encode task")??;
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
            series_count = meta.series_types.as_ref().map_or(0, |v| v.len()),
            byte_size = meta.byte_size,
            postings_size = meta.postings_size_bytes.unwrap_or(0),
            ts_min = meta.ts_min_unix_nano,
            ts_max = meta.ts_max_unix_nano,
            "metrics block uploaded"
        );
        Ok(Some(meta))
    }

    /// Encode buffered samples into the main + postings parquet and the
    /// JSON sidecar. Pure CPU, no I/O — runs on the blocking pool via
    /// `spawn_blocking`. The async `finish_and_upload_impl` performs the
    /// PUTs.
    fn encode(mut self) -> Result<EncodedBlock> {
        let n = self.fingerprints.len();

        // ── Main parquet ───────────────────────────────────────────
        //
        // Sort permutation over (fingerprint, ts) ascending. The
        // intra-block sort is what makes the postings index pay off
        // at query time: with sorted rows, parquet's row-group
        // min/max stats on the fingerprint column let queriers skip
        // most groups once they've resolved the fingerprint set from
        // postings.
        let mut order: Vec<u32> = (0..n as u32).collect();
        order.sort_by_key(|&i| (self.fingerprints[i as usize], self.ts[i as usize]));

        let main_schema = Self::main_schema();
        let fp_arr: ArrayRef = Arc::new(UInt64Array::from_iter_values(
            order.iter().map(|&i| self.fingerprints[i as usize]),
        ));
        let ts_arr: ArrayRef = Arc::new(UInt64Array::from_iter_values(
            order.iter().map(|&i| self.ts[i as usize]),
        ));
        let val_arr: ArrayRef = Arc::new(Float64Array::from_iter_values(
            order.iter().map(|&i| self.values[i as usize]),
        ));
        drop(order);
        // Release source buffers — Arrow now owns column copies.
        self.fingerprints = Vec::new();
        self.ts = Vec::new();
        self.values = Vec::new();

        let main_batch = RecordBatch::try_new(main_schema.clone(), vec![fp_arr, ts_arr, val_arr])
            .context("constructing metrics main RecordBatch")?;

        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
            .set_max_row_group_row_count(Some(self.cfg.row_group_size))
            .build();
        let mut main_buf: Vec<u8> = Vec::with_capacity(self.bytes_est as usize);
        {
            let mut w = ArrowWriter::try_new(&mut main_buf, main_schema, Some(props.clone()))
                .context("ArrowWriter::try_new (metrics main)")?;
            w.write(&main_batch).context("ArrowWriter::write (metrics main)")?;
            w.close().context("ArrowWriter::close (metrics main)")?;
        }
        let main_bytes = Bytes::from(main_buf);
        let byte_size = main_bytes.len() as u64;

        // ── Postings parquet ───────────────────────────────────────
        //
        // Build the inverted index as HashMap<(name,value), Vec<u64>>
        // (cheap inserts), then sort each fingerprint vec at write
        // time and walk the outer keys in sorted order. BTreeMap would
        // give sortedness for free but is slower per insert; the
        // dominant cost here is the outer hash inserts × N_series ×
        // N_labels_per_series, not the final sort.
        //
        // TODO(v0.3): At the architecture's 60M-active-series ceiling
        // this transiently allocates ~50k (String,String) entries
        // (~2.5 MB). If real workloads ever approach that, intern
        // label names/values into a per-block dictionary and key the
        // postings map by integer IDs instead.
        let postings = self.build_postings();
        let postings_bytes = self.encode_postings(postings, &props)?;
        let postings_size = postings_bytes.len() as u64;

        // ── Sidecar JSON ───────────────────────────────────────────
        let block_uuid = Uuid::now_v7();
        let series_types: Vec<(u64, u8)> = self
            .series_dict
            .iter()
            .map(|s| (s.fingerprint, s.metric_type))
            .collect();
        // `all_fingerprints` is the signal-agnostic view of
        // `series_types`. Cheap to derive (one u64 per series) and
        // lets `scry_query::postings::resolve_fingerprints` handle
        // empty-matcher queries without a metrics-specific branch.
        let all_fingerprints: Vec<u64> = series_types.iter().map(|(fp, _)| *fp).collect();
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
            series_types: Some(series_types),
            all_fingerprints: Some(all_fingerprints),
        };
        let meta_bytes = Bytes::from(
            serde_json::to_vec_pretty(&meta).context("serialising metrics BlockMeta")?,
        );

        // ── Upload order: main → postings → meta ───────────────────
        //
        // Same ordering invariant as dummy: the meta.json sidecar is
        // the "block exists" signal for catalog reconcile. If we
        // crash after main+postings but before meta, the orphaned
        // parquets stay until retention; if we crash after main but
        // before postings, reconcile sees no meta and skips them.
        // The only durable persistence ordering that matters is
        // "meta last."
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

    /// Walk the series dictionary, building
    /// `Vec<((name, value), sorted fingerprints)>` keyed in lexicographic
    /// `(name, value)` order for the postings parquet.
    fn build_postings(&self) -> Vec<((String, String), Vec<u64>)> {
        use std::collections::HashMap;
        let mut inv: HashMap<(String, String), Vec<u64>> = HashMap::new();
        for series in &self.series_dict {
            for (k, v) in &series.labels {
                inv.entry((k.clone(), v.clone()))
                    .or_default()
                    .push(series.fingerprint);
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
            // when has_postings=true. Cheap; query path can detect
            // empty by row count.
            let empty_main = RecordBatch::new_empty(schema.clone());
            let mut buf: Vec<u8> = Vec::new();
            let mut w = ArrowWriter::try_new(&mut buf, schema, Some(props.clone()))
                .context("ArrowWriter::try_new (empty postings)")?;
            w.write(&empty_main).context("ArrowWriter::write (empty postings)")?;
            w.close().context("ArrowWriter::close (empty postings)")?;
            return Ok(Bytes::from(buf));
        }

        // Build the three columns. The ListArray uses i32 offsets;
        // at our 60M-series architecture ceiling each fingerprint
        // list maxes at ~thousands of u64s and the cumulative offset
        // stays well under i32::MAX. Still, debug_assert the running
        // offset just in case real workloads ever stretch that.
        //
        // TODO(v0.3): If we ever ship a deployment where postings
        // cardinality could push past 2.1B entries per block, switch
        // to LargeListArray (i64 offsets). At v0.2 scale we're nowhere
        // close.
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
                .expect("postings offset overflow (i32); see LargeListArray TODO");
            offsets.push(running);
        }
        debug_assert!(running >= 0);
        let values_array = Arc::new(values_builder.finish());
        let offset_buf = OffsetBuffer::new(offsets.into());
        let field = match Self::postings_schema().field(2).data_type() {
            DataType::List(f) => f.clone(),
            other => {
                anyhow::bail!("postings schema column 2 should be List, found {other:?}")
            }
        };
        let list = ListArray::new(field, offset_buf, values_array, None);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(names), Arc::new(values), Arc::new(list)],
        )
        .context("constructing postings RecordBatch")?;

        let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
        let mut w = ArrowWriter::try_new(&mut buf, schema, Some(props.clone()))
            .context("ArrowWriter::try_new (postings)")?;
        w.write(&batch).context("ArrowWriter::write (postings)")?;
        w.close().context("ArrowWriter::close (postings)")?;
        Ok(Bytes::from(buf))
    }
}
