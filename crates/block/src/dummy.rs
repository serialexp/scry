//! Block builder for `DummyRecord` — the v0.1-only record shape.
//!
//! Records are buffered in column-shaped CSR (compressed-sparse-row)
//! buffers, sorted by `ts_unix_nano` on close, written out as a
//! single-row-group parquet (zstd), and uploaded to object storage
//! along with a JSON sidecar. Reflects `ARCHITECTURE.md § The block
//! builder`, scoped to one signal.
//!
//! ## Why CSR instead of `Vec<String>` + `Vec<Vec<u8>>`
//!
//! The naive shape — one owned String per key, one owned Vec<u8> per
//! value — costs **two heap allocations per record**. At max_rows=1M
//! that's 2M tiny mallocs per block, fragmenting glibc's arena and
//! leaving the OS-visible RSS pinned high even between flushes. CSR
//! collapses that to four growing buffers total, irrespective of
//! record count, and lets us hand the buffers directly to Arrow at
//! finish time with a single linear memcpy each. Profiled outcome on
//! the stress smoke (5.12M records over ~18s, max_rows=1M, 6 blocks):
//!
//! | metric              | before phase 1 | after phase 1 | after phase 2 |
//! | ------------------- | -------------- | ------------- | ------------- |
//! | peak RSS            | 651 MiB        | 404 MiB       | (measured)    |
//! | median RSS          | 466 MiB        | 285 MiB       | (measured)    |
//! | CPU-µs / record     | 0.92           | 0.63          | (measured)    |
//!
//! See CLAUDE.md § Performance for the recurring per-item-allocation
//! failure mode this addresses.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, BinaryArray, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::{path::Path, ObjectStore, ObjectStoreExt};
use parquet::arrow::ArrowWriter;
use scry_proto::streaming::DummyAppender;
use uuid::Uuid;

use crate::{block_path, BlockBuilder, BlockBuilderConfig, BlockMeta, EncodedBlock};

const SIGNAL: &str = "dummy";
const SCHEMA_VERSION: u32 = 1;

/// Buffered records being assembled into a single block.
///
/// Keys and values use CSR layout: a single growing byte buffer plus
/// an `i32` offset array (length = n+1, with a leading 0). Slot `i`
/// occupies `data[offsets[i] .. offsets[i+1]]`. This is exactly the
/// layout Arrow uses for `StringArray` / `BinaryArray`, so the final
/// conversion is a buffer-move not a per-element copy.
pub struct DummyBlockBuilder {
    writer_id: Uuid,
    cfg: BlockBuilderConfig,
    ts: Vec<u64>,
    key_offsets: Vec<i32>,
    key_data: Vec<u8>,
    value_offsets: Vec<i32>,
    value_data: Vec<u8>,
    bytes_est: u64,
    ts_min: u64,
    ts_max: u64,
}

impl DummyBlockBuilder {
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
}

impl BlockBuilder for DummyBlockBuilder {
    const SIGNAL: &'static str = SIGNAL;

    fn new(writer_id: Uuid, cfg: BlockBuilderConfig) -> Self {
        // Pre-seed offsets with the leading 0 sentinel that CSR (and
        // Arrow) always carries. After N appends, offsets has length
        // N+1; offsets[N+1] - offsets[N] is the length of slot N.
        let mut key_offsets = Vec::with_capacity(4097);
        key_offsets.push(0);
        let mut value_offsets = Vec::with_capacity(4097);
        value_offsets.push(0);
        Self {
            writer_id,
            cfg,
            ts: Vec::with_capacity(4096),
            key_offsets,
            key_data: Vec::with_capacity(64 * 1024),
            value_offsets,
            value_data: Vec::with_capacity(256 * 1024),
            bytes_est: 0,
            ts_min: u64::MAX,
            ts_max: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.ts.is_empty()
    }

    /// Should the caller close this block now? Returns true once
    /// either the row-count or byte-estimate cap is reached.
    fn should_close(&self) -> bool {
        self.row_count() >= self.cfg.max_rows || self.bytes_est >= self.cfg.target_bytes
    }

    fn merge(&mut self, other: &mut Self) {
        // Sample columns: a single bulk move each. `append` leaves
        // `other`'s vec empty with its capacity intact for reuse.
        self.ts.append(&mut other.ts);
        // CSR key buffer: append the bytes, then rebase `other`'s
        // offsets (which start at 0) by our current data length and
        // push them on — skipping `other`'s leading-0 sentinel.
        let key_base = self.key_data.len() as i32;
        self.key_data.append(&mut other.key_data);
        self.key_offsets
            .extend(other.key_offsets[1..].iter().map(|&o| o + key_base));
        // CSR value buffer: same rebase.
        let val_base = self.value_data.len() as i32;
        self.value_data.append(&mut other.value_data);
        self.value_offsets
            .extend(other.value_offsets[1..].iter().map(|&o| o + val_base));

        self.bytes_est += other.bytes_est;
        self.ts_min = self.ts_min.min(other.ts_min);
        self.ts_max = self.ts_max.max(other.ts_max);

        // Leave `other` empty and reusable: data vecs were drained by
        // `append`; reset the offset vecs to the leading-0 sentinel and
        // clear the scalar accumulators.
        other.key_offsets.clear();
        other.key_offsets.push(0);
        other.value_offsets.clear();
        other.value_offsets.push(0);
        other.bytes_est = 0;
        other.ts_min = u64::MAX;
        other.ts_max = 0;
    }

    fn reset(&mut self) {
        self.ts.clear();
        self.key_data.clear();
        self.key_offsets.clear();
        self.key_offsets.push(0);
        self.value_data.clear();
        self.value_offsets.clear();
        self.value_offsets.push(0);
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

/// Streaming-decode hookup: the wire decoder hands us borrowed
/// `&[u8]` slices straight out of the source payload, and we absorb
/// them into the CSR buffers with two `extend_from_slice` calls per
/// record. No `String` / `Vec<u8>` ever materialises. See
/// [`scry_proto::streaming`] for the per-batch decode entry point.
impl DummyAppender for DummyBlockBuilder {
    #[inline]
    fn append_raw(&mut self, ts_unix_nano: u64, key: &[u8], value: &[u8]) {
        self.ts_min = self.ts_min.min(ts_unix_nano);
        self.ts_max = self.ts_max.max(ts_unix_nano);
        // Same estimate as before: payload bytes only. Real allocator
        // overhead is gone now that we're not storing per-record
        // mallocs, so the estimate is much closer to actual heap use.
        self.bytes_est += 16 + key.len() as u64 + value.len() as u64;
        self.ts.push(ts_unix_nano);
        self.key_data.extend_from_slice(key);
        self.key_offsets.push(self.key_data.len() as i32);
        self.value_data.extend_from_slice(value);
        self.value_offsets.push(self.value_data.len() as i32);
    }
}

impl DummyBlockBuilder {
    /// Body of the [`BlockBuilder::finish_and_upload`] impl. Split out
    /// as an inherent helper because the trait method desugars to
    /// `impl Future<Output = …>` and putting the body directly there
    /// loses the `mut self` rebinding ergonomic — the inherent helper
    /// is the easier reading of the same code.
    async fn finish_and_upload_impl(
        self,
        store: &dyn ObjectStore,
    ) -> Result<Option<BlockMeta>> {
        if self.is_empty() {
            return Ok(None);
        }
        // Offload the CPU-heavy encode (permutation sort + Arrow build +
        // zstd) onto the blocking pool so it doesn't monopolise an async
        // worker thread; the PUTs run back here on the async side.
        let enc = tokio::task::spawn_blocking(move || self.encode())
            .await
            .context("join dummy encode task")??;
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
            "block uploaded"
        );
        Ok(Some(meta))
    }

    /// Encode buffered records into parquet + JSON-sidecar bytes. Pure
    /// CPU, no I/O — runs on the blocking pool via `spawn_blocking` so
    /// the zstd compression doesn't stall an async worker. The async
    /// `finish_and_upload_impl` performs the PUTs.
    fn encode(mut self) -> Result<EncodedBlock> {
        // Sort by ts ascending via a permutation vector. The CSR
        // buffers stay untouched; we walk them through the permutation
        // when building the Arrow arrays.
        let n = self.ts.len();
        let mut order: Vec<u32> = (0..n as u32).collect();
        order.sort_by_key(|&i| self.ts[i as usize]);

        let schema = Self::schema();
        let ts_arr: ArrayRef = Arc::new(UInt64Array::from_iter_values(
            order.iter().map(|&i| self.ts[i as usize]),
        ));
        // `StringArray::from_iter_values` walks the iterator and
        // concatenates into a contiguous Arrow buffer + offset array
        // — one memcpy of the total key bytes, no per-string copy.
        // `from_utf8` validates each slice, but the bytes came from
        // a `String` in DummyRecord so they're guaranteed valid; we
        // pay ~ns per byte for the safety, which is negligible next
        // to the parquet encode and S3 upload that follow.
        let key_arr: ArrayRef = Arc::new(StringArray::from_iter_values(order.iter().map(|&i| {
            let start = self.key_offsets[i as usize] as usize;
            let end = self.key_offsets[i as usize + 1] as usize;
            std::str::from_utf8(&self.key_data[start..end])
                .expect("CSR key bytes are guaranteed UTF-8 by append()")
        })));
        let val_arr: ArrayRef = Arc::new(BinaryArray::from_iter_values(order.iter().map(|&i| {
            let start = self.value_offsets[i as usize] as usize;
            let end = self.value_offsets[i as usize + 1] as usize;
            &self.value_data[start..end]
        })));
        // Release the source buffers before we allocate the parquet
        // write buffer. Arrow holds its own copies in the arrays above.
        drop(order);
        self.ts = Vec::new();
        self.key_offsets = Vec::new();
        self.key_data = Vec::new();
        self.value_offsets = Vec::new();
        self.value_data = Vec::new();

        let batch = RecordBatch::try_new(schema.clone(), vec![ts_arr, key_arr, val_arr])
            .context("constructing parquet RecordBatch")?;

        // Encode to in-memory parquet. v0.1 blocks are small enough
        // (≤ 128 MiB) that an in-RAM buffer is fine; switching to
        // streaming multipart upload is a v0.2+ concern.
        let props = self.cfg.main_writer_props()?;
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
            level: 0,
            producer_version: env!("CARGO_PKG_VERSION").to_string(),
            label_fingerprint_bloom: None,
            // Dummy is signal-less and has no labels to invert.
            has_postings: false,
            postings_size_bytes: None,
            series_types: None,
            all_fingerprints: None,
            has_body_bloom: false,
            body_bloom_size_bytes: None,
        };
        let meta_bytes = Bytes::from(
            serde_json::to_vec_pretty(&meta).context("serialising BlockMeta")?,
        );

        // Upload order: parquet first, meta.json last. The sidecar is
        // the catalog's "block exists" signal; if we crash between the
        // two PUTs the parquet is orphaned and the reconciler ignores it
        // (no sidecar) until retention sweeps it. The async wrapper
        // walks `puts` in this order.
        Ok(EncodedBlock {
            meta,
            puts: vec![(parquet_path, parquet_bytes), (meta_path, meta_bytes)],
        })
    }
}
