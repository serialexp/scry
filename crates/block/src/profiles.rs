//! Block builder for profiles — the v0.6 fourth signal, storage half.
//!
//! Per the v0.5/v0.6 storage plan locked with Bart, a profiles block is
//! the simplest of the four signals: **one parquet row per
//! `ProfileBlob`**, with the pprof bytes stored *verbatim* in an opaque
//! `Binary` column. No postings sidecar, no pprof parsing. The
//! flat-vs-nested "one row per resolved sample" question the architecture
//! defers (ARCHITECTURE.md § Deferred) is a *query*-phase decision (v0.6);
//! storage just needs to durably keep the blob and its labels so the
//! flamegraph aggregator can decode it later without re-ingest.
//!
//! - `<block>.parquet` — `(ts_unix_nano, duration_nano, labels
//!   Map<Utf8,Utf8>, format UInt8, data Binary)` sorted by
//!   `ts_unix_nano`. The `(profile_type, time_range)` query shape the
//!   docs describe is served by the block's `ts_min`/`ts_max` stats plus
//!   a `labels['profile.type']` predicate — no inverted index needed.
//! - `<block>.meta.json` — `has_postings:false`, no postings file.
//!
//! Wire input is `ProfilesBatch { samples: Vec<ProfileBlob> }` (see
//! `scry_proto::generated`). Profiles are low-volume (≈1 blob/batch), so
//! there's no hot path here; the per-blob `Vec<u8>` data + `Vec` of label
//! pairs are fine without the CSR gymnastics metrics/logs need.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, BinaryArray, MapBuilder, StringBuilder, UInt64Array, UInt8Array};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::{path::Path, ObjectStore, ObjectStoreExt};
use parquet::arrow::ArrowWriter;
use scry_proto::streaming::ProfilesAppender;
use uuid::Uuid;

use crate::{block_path, BlockBuilder, BlockBuilderConfig, BlockMeta, EncodedBlock};

const SIGNAL: &str = "profiles";
const SCHEMA_VERSION: u32 = 1;

/// In-memory profiles block under construction. One parallel `Vec` per
/// physical parquet column.
pub struct ProfilesBlockBuilder {
    writer_id: Uuid,
    cfg: BlockBuilderConfig,
    ts: Vec<u64>,
    durations: Vec<u64>,
    labels: Vec<Vec<(String, String)>>,
    formats: Vec<u8>,
    data: Vec<Vec<u8>>,
    bytes_est: u64,
    ts_min: u64,
    ts_max: u64,
}

impl ProfilesBlockBuilder {
    pub fn main_schema() -> SchemaRef {
        // Map<Utf8,Utf8> in the canonical MapBuilder layout (field names
        // "entries"/"keys"/"values"); `values` nullable to match the
        // builder's output type exactly. Same shape as logs' attributes.
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", DataType::Utf8, false),
                Field::new("values", DataType::Utf8, true),
            ])),
            false,
        ));
        Arc::new(Schema::new(vec![
            Field::new("ts_unix_nano", DataType::UInt64, false),
            Field::new("duration_nano", DataType::UInt64, false),
            Field::new(
                "labels",
                DataType::Map(entries_field, /*keys_sorted=*/ false),
                false,
            ),
            Field::new("format", DataType::UInt8, false),
            Field::new("data", DataType::Binary, false),
        ]))
    }

    pub fn row_count(&self) -> u64 {
        self.ts.len() as u64
    }
}

impl BlockBuilder for ProfilesBlockBuilder {
    const SIGNAL: &'static str = SIGNAL;

    fn new(writer_id: Uuid, cfg: BlockBuilderConfig) -> Self {
        Self {
            writer_id,
            cfg,
            ts: Vec::with_capacity(256),
            durations: Vec::with_capacity(256),
            labels: Vec::with_capacity(256),
            formats: Vec::with_capacity(256),
            data: Vec::with_capacity(256),
            bytes_est: 0,
            ts_min: u64::MAX,
            ts_max: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.ts.is_empty()
    }

    fn should_close(&self) -> bool {
        self.row_count() >= self.cfg.max_rows || self.bytes_est >= self.cfg.target_bytes
    }

    fn merge(&mut self, other: &mut Self) {
        self.ts.append(&mut other.ts);
        self.durations.append(&mut other.durations);
        self.labels.append(&mut other.labels);
        self.formats.append(&mut other.formats);
        self.data.append(&mut other.data);

        self.bytes_est += other.bytes_est;
        self.ts_min = self.ts_min.min(other.ts_min);
        self.ts_max = self.ts_max.max(other.ts_max);

        other.bytes_est = 0;
        other.ts_min = u64::MAX;
        other.ts_max = 0;
    }

    fn reset(&mut self) {
        self.ts.clear();
        self.durations.clear();
        self.labels.clear();
        self.formats.clear();
        self.data.clear();
        self.bytes_est = 0;
        self.ts_min = u64::MAX;
        self.ts_max = 0;
    }

    fn set_compression_level(&mut self, level: i32) {
        self.cfg.compression_level = level;
    }

    fn finish_and_upload(
        self,
        store: &dyn ObjectStore,
    ) -> impl std::future::Future<Output = Result<Option<BlockMeta>>> + Send {
        self.finish_and_upload_impl(store)
    }
}

impl ProfilesAppender for ProfilesBlockBuilder {
    fn append_blob(
        &mut self,
        ts_unix_nano: u64,
        duration_nano: u64,
        labels: Vec<(Vec<u8>, Vec<u8>)>,
        format: u8,
        data: Vec<u8>,
    ) {
        self.ts_min = self.ts_min.min(ts_unix_nano);
        self.ts_max = self.ts_max.max(ts_unix_nano);
        // UTF-8-coerce label bytes (lossy on bad input — a misbehaving
        // agent shouldn't poison the block). Same policy as logs/metrics.
        let owned: Vec<(String, String)> = labels
            .into_iter()
            .map(|(k, v)| {
                (
                    String::from_utf8_lossy(&k).into_owned(),
                    String::from_utf8_lossy(&v).into_owned(),
                )
            })
            .collect();
        let label_bytes: usize = owned.iter().map(|(k, v)| k.len() + v.len()).sum();
        // 17 fixed bytes (ts + duration + format) + label bytes + blob.
        self.bytes_est += 17 + label_bytes as u64 + data.len() as u64;
        self.ts.push(ts_unix_nano);
        self.durations.push(duration_nano);
        self.labels.push(owned);
        self.formats.push(format);
        self.data.push(data);
    }
}

impl ProfilesBlockBuilder {
    async fn finish_and_upload_impl(self, store: &dyn ObjectStore) -> Result<Option<BlockMeta>> {
        if self.is_empty() {
            return Ok(None);
        }
        let enc = tokio::task::spawn_blocking(move || self.encode())
            .await
            .context("join profiles encode task")??;
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
            byte_size = meta.byte_size,
            ts_min = meta.ts_min_unix_nano,
            ts_max = meta.ts_max_unix_nano,
            "profiles block uploaded"
        );
        Ok(Some(meta))
    }

    fn encode(mut self) -> Result<EncodedBlock> {
        let n = self.ts.len();

        // Sort permutation by ts ascending.
        let mut order: Vec<u32> = (0..n as u32).collect();
        order.sort_by_key(|&i| self.ts[i as usize]);

        let schema = Self::main_schema();
        let ts_arr: ArrayRef = Arc::new(UInt64Array::from_iter_values(
            order.iter().map(|&i| self.ts[i as usize]),
        ));
        let dur_arr: ArrayRef = Arc::new(UInt64Array::from_iter_values(
            order.iter().map(|&i| self.durations[i as usize]),
        ));
        let fmt_arr: ArrayRef = Arc::new(UInt8Array::from_iter_values(
            order.iter().map(|&i| self.formats[i as usize]),
        ));
        let data_arr: ArrayRef = Arc::new(BinaryArray::from_iter_values(
            order.iter().map(|&i| self.data[i as usize].as_slice()),
        ));

        let mut label_builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for &i in order.iter() {
            for (k, v) in &self.labels[i as usize] {
                label_builder.keys().append_value(k);
                label_builder.values().append_value(v);
            }
            label_builder
                .append(true)
                .context("MapBuilder::append (profiles labels)")?;
        }
        let label_arr: ArrayRef = Arc::new(label_builder.finish());

        drop(order);
        self.ts = Vec::new();
        self.durations = Vec::new();
        self.labels = Vec::new();
        self.formats = Vec::new();
        self.data = Vec::new();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![ts_arr, dur_arr, label_arr, fmt_arr, data_arr],
        )
        .context("constructing profiles main RecordBatch")?;

        let props = self.cfg.main_writer_props()?;
        let mut buf: Vec<u8> = Vec::with_capacity(self.bytes_est as usize);
        {
            let mut w = ArrowWriter::try_new(&mut buf, schema, Some(props))
                .context("ArrowWriter::try_new (profiles main)")?;
            w.write(&batch)
                .context("ArrowWriter::write (profiles main)")?;
            w.close().context("ArrowWriter::close (profiles main)")?;
        }
        let parquet_bytes = Bytes::from(buf);
        let byte_size = parquet_bytes.len() as u64;

        let block_uuid = Uuid::now_v7();
        let parquet_path = Path::from(block_path(
            SIGNAL,
            self.ts_min,
            self.writer_id,
            block_uuid,
            "parquet",
        ));
        let meta_path = Path::from(block_path(
            SIGNAL,
            self.ts_min,
            self.writer_id,
            block_uuid,
            "meta.json",
        ));

        let meta = BlockMeta {
            uuid: block_uuid,
            signal: SIGNAL.to_string(),
            writer_id: self.writer_id,
            ts_min_unix_nano: self.ts_min,
            ts_max_unix_nano: self.ts_max,
            row_count: n as u64,
            byte_size,
            schema_version: SCHEMA_VERSION,
            level: 0,
            producer_version: env!("CARGO_PKG_VERSION").to_string(),
            label_fingerprint_bloom: None,
            // Profiles query by (type, time), served by block stats — no
            // inverted index (per D-025 + the v0.6 storage plan).
            has_postings: false,
            postings_size_bytes: None,
            series_types: None,
            all_fingerprints: None,
            has_body_bloom: false,
            body_bloom_size_bytes: None,
        };
        let meta_bytes = Bytes::from(
            serde_json::to_vec_pretty(&meta).context("serialising profiles BlockMeta")?,
        );

        // Upload order: parquet first, meta.json last (the sidecar is the
        // catalog's "block exists" signal).
        Ok(EncodedBlock {
            meta,
            puts: vec![(parquet_path, parquet_bytes), (meta_path, meta_bytes)],
        })
    }
}
