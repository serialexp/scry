//! Block builder for `DummyRecord` — the v0.1-only record shape.
//!
//! Records are buffered in arrow builders, sorted by `ts_unix_nano`
//! on close, written out as a single-row-group parquet (zstd), and
//! uploaded to object storage along with a JSON sidecar. Reflects
//! `ARCHITECTURE.md § The block builder`, scoped to one signal.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, BinaryArray, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::{path::Path, ObjectStore};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use scry_proto::DummyRecord;
use uuid::Uuid;

use crate::{block_path, BlockBuilderConfig, BlockMeta};

const SIGNAL: &str = "dummy";
const SCHEMA_VERSION: u32 = 1;

/// Buffered records being assembled into a single block.
///
/// The struct owns three column-shaped `Vec`s rather than per-record
/// `RecordBatch` constructions — every `append` is a few pushes, no
/// allocations beyond the underlying buffer growth. The arrow arrays
/// are built once at `finish_and_upload` time.
pub struct DummyBlockBuilder {
    writer_id: Uuid,
    cfg: BlockBuilderConfig,
    ts: Vec<u64>,
    keys: Vec<String>,
    values: Vec<Vec<u8>>,
    bytes_est: u64,
    ts_min: u64,
    ts_max: u64,
}

impl DummyBlockBuilder {
    pub fn new(writer_id: Uuid, cfg: BlockBuilderConfig) -> Self {
        Self {
            writer_id,
            cfg,
            ts: Vec::with_capacity(4096),
            keys: Vec::with_capacity(4096),
            values: Vec::with_capacity(4096),
            bytes_est: 0,
            ts_min: u64::MAX,
            ts_max: 0,
        }
    }

    pub fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("ts_unix_nano", DataType::UInt64, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Binary, false),
        ]))
    }

    pub fn row_count(&self) -> u64 {
        self.ts.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.ts.is_empty()
    }

    /// Should the caller close this block now? Returns true once
    /// either the row-count or byte-estimate cap is reached.
    pub fn should_close(&self) -> bool {
        self.row_count() >= self.cfg.max_rows || self.bytes_est >= self.cfg.target_bytes
    }

    /// Append one record. Cheap — Vec pushes only.
    pub fn append(&mut self, rec: DummyRecord) {
        self.ts_min = self.ts_min.min(rec.ts_unix_nano);
        self.ts_max = self.ts_max.max(rec.ts_unix_nano);
        // Rough byte estimate so should_close has something to fire
        // on. 8 (u64) + key + value + a handful of bytes of overhead.
        self.bytes_est += 16 + rec.key.len() as u64 + rec.value.len() as u64;
        self.ts.push(rec.ts_unix_nano);
        self.keys.push(rec.key);
        self.values.push(rec.value);
    }

    /// Close the block: serialise to parquet, upload it and a
    /// metadata sidecar, return the [`BlockMeta`] for catalog
    /// insertion. Consumes `self`.
    pub async fn finish_and_upload(
        mut self,
        store: &dyn ObjectStore,
    ) -> Result<Option<BlockMeta>> {
        if self.is_empty() {
            return Ok(None);
        }

        // Sort by ts ascending. We carry value/key permutation with
        // an index vector so we don't have to clone strings into
        // (ts, key, value) tuples just to sort.
        let n = self.ts.len();
        let mut order: Vec<u32> = (0..n as u32).collect();
        order.sort_by_key(|&i| self.ts[i as usize]);

        let ts_sorted: Vec<u64> = order.iter().map(|&i| self.ts[i as usize]).collect();
        // Move out of the source vecs so we don't clone. We replace
        // with empty Vecs since we're consumed at the end anyway.
        let keys = std::mem::take(&mut self.keys);
        let values = std::mem::take(&mut self.values);
        let mut keys_sorted: Vec<String> = Vec::with_capacity(n);
        let mut values_sorted: Vec<Vec<u8>> = Vec::with_capacity(n);
        // Indirection by index: rebuild in sorted order. Strings and
        // value Vecs are moved out of the originals via swap_remove,
        // but that breaks indices — so we use the simpler clone path
        // here (each block is bounded; this is not a hot loop on a
        // per-record basis).
        for &i in &order {
            keys_sorted.push(keys[i as usize].clone());
            values_sorted.push(values[i as usize].clone());
        }
        drop(keys);
        drop(values);

        let schema = Self::schema();
        let ts_arr: ArrayRef = Arc::new(UInt64Array::from(ts_sorted));
        let key_arr: ArrayRef = Arc::new(StringArray::from(keys_sorted));
        let val_arr: ArrayRef = Arc::new(BinaryArray::from_iter_values(
            values_sorted.iter().map(|v| v.as_slice()),
        ));
        let batch = RecordBatch::try_new(schema.clone(), vec![ts_arr, key_arr, val_arr])
            .context("constructing parquet RecordBatch")?;

        // Encode to in-memory parquet. v0.1 blocks are small enough
        // (≤ 128 MiB) that an in-RAM buffer is fine; switching to
        // streaming multipart upload is a v0.2+ concern.
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
            .set_max_row_group_size(1024 * 1024) // ~1M rows; row-group bytes are roughly compressed payload
            .build();
        let mut buf: Vec<u8> = Vec::with_capacity(self.bytes_est as usize);
        {
            let mut w = ArrowWriter::try_new(&mut buf, schema, Some(props))
                .context("ArrowWriter::try_new")?;
            w.write(&batch).context("ArrowWriter::write")?;
            w.close().context("ArrowWriter::close")?;
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
            producer_version: env!("CARGO_PKG_VERSION").to_string(),
            label_fingerprint_bloom: None,
        };
        let meta_bytes = Bytes::from(
            serde_json::to_vec_pretty(&meta).context("serialising BlockMeta")?,
        );

        // Upload the parquet first; the sidecar is the catalog's
        // signal that the parquet exists. If we crash between the two
        // uploads, the parquet is orphaned and the reconciler will
        // either ignore it (no sidecar) or a future writer will
        // overwrite it with its own block on retry (different UUID,
        // so no overwrite in practice — the orphan stays until
        // retention sweeps it).
        store
            .put(&parquet_path, parquet_bytes.into())
            .await
            .with_context(|| format!("upload parquet {parquet_path}"))?;
        store
            .put(&meta_path, meta_bytes.into())
            .await
            .with_context(|| format!("upload sidecar {meta_path}"))?;

        tracing::info!(
            block_uuid = %block_uuid,
            row_count = n,
            byte_size,
            ts_min = self.ts_min,
            ts_max = self.ts_max,
            "block uploaded"
        );

        Ok(Some(meta))
    }
}
