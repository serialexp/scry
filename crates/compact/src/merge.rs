//! The merge engine: read K input blocks, stream-sort them through
//! DataFusion, and write one merged block (main parquet + rebuilt
//! sidecars + meta) at the next level up.
//!
//! Per `ARCHITECTURE.md § Compaction § Per-merge sequence`, the merged
//! main parquet is the K inputs read back and re-sorted by the signal's
//! sort key — `ORDER BY` over a DataFusion union of the input parquets,
//! which streams (and spills to disk under memory pressure) so a merge
//! never has to hold the whole partition in RAM. Sidecars are rebuilt:
//!
//! - **postings** (metrics/logs): the union of the inputs' postings,
//!   re-sorted/deduped — read back with `scry_block::postings`.
//! - **body bloom** (logs): re-accumulated from the merged body column
//!   during the same streaming pass, via [`BodyBloomBuilder`].
//! - **all_fingerprints** (metrics/logs): the distinct fingerprint
//!   column, accumulated during the streaming pass.
//! - **series_types** (metrics): unioned from the inputs' `meta.json`.
//!
//! Output is content-addressed under a compactor `writer_id`; uploads go
//! `main → [postings] → [bloom] → meta.json` so the meta sidecar (the
//! "block exists" signal for reconcile) lands last.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Array, ListArray, StringArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::{col, ParquetReadOptions, SessionConfig, SessionContext};
use futures::{Stream, StreamExt, TryStreamExt};
use object_store::buffered::BufWriter as ObjectStoreWriter;
use object_store::{path::Path as ObjPath, ObjectStore, ObjectStoreExt};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use parquet::arrow::AsyncArrowWriter;
use scry_block::postings::{postings_record_batch, postings_schema, PostingsEntry};
use scry_block::{
    block_path, compacted_ancestor_closure, BlockBuilderConfig, BlockMeta, BodyBloomBuilder, Fence,
};
use scry_catalog::CatalogEntry;
use uuid::Uuid;

/// Per-signal knobs the merge needs: the sort key (so the merged block
/// keeps the same intra-block ordering its readers prune on), and which
/// sidecars to rebuild.
struct OutputCleanupGuard {
    store: Arc<dyn ObjectStore>,
    paths: Vec<ObjPath>,
    armed: Arc<AtomicBool>,
}

impl OutputCleanupGuard {
    fn new(store: Arc<dyn ObjectStore>, paths: Vec<ObjPath>) -> Self {
        Self {
            store,
            paths,
            armed: Arc::new(AtomicBool::new(true)),
        }
    }

    fn disarm(&self) {
        self.armed.store(false, Ordering::Release);
    }

    fn is_armed(&self) -> bool {
        self.armed.load(Ordering::Acquire)
    }
}

impl Drop for OutputCleanupGuard {
    fn drop(&mut self) {
        if !self.armed.swap(false, Ordering::AcqRel) {
            return;
        }
        let store = self.store.clone();
        let paths = self.paths.clone();
        // Async cleanup cannot run directly in Drop. Spawn it on the merge's
        // Tokio runtime so cancellation still removes completed pre-commit
        // objects; crash cleanup remains the object-store lifecycle's job.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                cleanup_paths(&store, &paths).await;
            });
        }
    }
}

struct SignalSpec {
    /// Columns the merged main parquet is ordered by, ascending — must
    /// match the block builder's sort for this signal.
    sort_cols: &'static [&'static str],
    /// Column carrying the per-row fingerprint (metrics/logs). Drives
    /// both `all_fingerprints` and (presence of) the postings rebuild.
    fp_col: Option<&'static str>,
    /// Body column for the full-text bloom (logs only).
    body_col: Option<&'static str>,
    /// Whether this signal carries a `series_types` map (metrics only).
    has_series_types: bool,
}

type PostingsStream = Pin<
    Box<dyn Stream<Item = std::result::Result<RecordBatch, parquet::errors::ParquetError>> + Send>,
>;

struct PostingsCursor {
    stream: PostingsStream,
    batch: Option<RecordBatch>,
    row: usize,
    current: Option<PostingsEntry>,
}

impl PostingsCursor {
    async fn advance(&mut self) -> Result<()> {
        loop {
            if let Some(batch) = &self.batch {
                if self.row < batch.num_rows() {
                    let names = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .context("postings col 0 not Utf8")?;
                    let values = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .context("postings col 1 not Utf8")?;
                    let lists = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .context("postings col 2 not List")?;
                    let fps = lists.value(self.row);
                    let fps = fps
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .context("postings fingerprint list not UInt64")?;
                    self.current = Some((
                        (
                            names.value(self.row).to_owned(),
                            values.value(self.row).to_owned(),
                        ),
                        fps.values().to_vec(),
                    ));
                    self.row += 1;
                    return Ok(());
                }
            }
            self.batch = self
                .stream
                .try_next()
                .await
                .context("read postings batch")?;
            self.row = 0;
            if self.batch.is_none() {
                self.current = None;
                return Ok(());
            }
        }
    }
}

fn spec_for(signal: &str) -> Result<SignalSpec> {
    Ok(match signal {
        "logs" => SignalSpec {
            sort_cols: &["stream_fingerprint", "ts_unix_nano"],
            fp_col: Some("stream_fingerprint"),
            body_col: Some("body"),
            has_series_types: false,
        },
        "metrics" => SignalSpec {
            sort_cols: &["series_fingerprint", "ts_unix_nano"],
            fp_col: Some("series_fingerprint"),
            body_col: None,
            has_series_types: true,
        },
        "traces" => SignalSpec {
            sort_cols: &["trace_id", "start_unix_nano"],
            fp_col: None,
            body_col: None,
            has_series_types: false,
        },
        "profiles" => SignalSpec {
            sort_cols: &["ts_unix_nano"],
            fp_col: None,
            body_col: None,
            has_series_types: false,
        },
        other => anyhow::bail!("compaction not supported for signal {other:?}"),
    })
}

/// Merge `inputs` (all same signal + level) into one block at
/// `out_level`, written under `writer_id`. Returns the merged
/// [`BlockMeta`] (already uploaded, meta sidecar last) on success, or
/// `Ok(None)` if the `fence` reported the lease lost during the merge —
/// see the commit-point fence below. Does **not** touch the catalog — the
/// engine does that.
///
/// ## Commit-point fence
///
/// A merge can run for minutes (DataFusion sort over the K inputs). In a
/// multi-instance deployment the lease guarding this partition can be lost
/// mid-merge (a renewal failed; a peer took over). Blocks are addressed by
/// random UUID, **not** content hash, so two instances merging the same
/// partition produce two *distinct* blocks with identical rows — a
/// double-count a later merge would union, not dedupe. The fence makes a
/// double-merge benign: `reconcile_from_bucket` keys on `meta.json`, so a
/// block with no `meta.json` is invisible. We therefore upload the data
/// objects (`main → [postings] → [bloom]`) first, then **check the fence
/// immediately before the `meta.json` PUT**. If the lease was lost we skip
/// `meta.json` and return `Ok(None)`: the uploaded data objects are harmless
/// leaked bytes (reclaimable by a future orphan-GC / full walk), there is no
/// catalog row, no events, and the inputs are untouched for the rightful
/// lease holder to re-merge.
#[allow(clippy::too_many_arguments)]
pub async fn merge_blocks(
    store: Arc<dyn ObjectStore>,
    bucket: &str,
    signal: &str,
    inputs: &[CatalogEntry],
    out_level: u32,
    writer_id: Uuid,
    block_cfg: &BlockBuilderConfig,
    fence: &dyn Fence,
    resources: &Arc<crate::resource::CompactResources>,
    admitted_non_df_bytes: u64,
) -> Result<Option<BlockMeta>> {
    anyhow::ensure!(!inputs.is_empty(), "merge_blocks called with no inputs");
    let block_uuid = Uuid::now_v7();
    let ts_min = inputs
        .iter()
        .map(|entry| entry.meta.ts_min_unix_nano)
        .min()
        .expect("non-empty");
    let staged_paths = ["body.bloom", "postings.parquet", "parquet"]
        .into_iter()
        .map(|suffix| ObjPath::from(block_path(signal, ts_min, writer_id, block_uuid, suffix)))
        .collect();
    let cleanup = OutputCleanupGuard::new(store.clone(), staged_paths);
    let result = merge_blocks_inner(
        store.clone(),
        bucket,
        signal,
        inputs,
        out_level,
        writer_id,
        block_uuid,
        block_cfg,
        fence,
        resources,
        admitted_non_df_bytes,
        &cleanup,
    )
    .await;

    match &result {
        Ok(Some(_)) => cleanup.disarm(),
        Ok(None) => {
            cleanup_paths(&store, &cleanup.paths).await;
            cleanup.disarm();
        }
        Err(_) if cleanup.is_armed() => {
            cleanup_paths(&store, &cleanup.paths).await;
            cleanup.disarm();
        }
        Err(_) => {}
    }
    result
}

#[allow(clippy::too_many_arguments)]
async fn merge_blocks_inner(
    store: Arc<dyn ObjectStore>,
    bucket: &str,
    signal: &str,
    inputs: &[CatalogEntry],
    out_level: u32,
    writer_id: Uuid,
    block_uuid: Uuid,
    block_cfg: &BlockBuilderConfig,
    fence: &dyn Fence,
    resources: &Arc<crate::resource::CompactResources>,
    admitted_non_df_bytes: u64,
    cleanup: &OutputCleanupGuard,
) -> Result<Option<BlockMeta>> {
    let spec = spec_for(signal)?;

    // Catalog entries intentionally omit sidecar-only fields. Fetch every
    // durable input meta exactly once for all signals, validate that it is the
    // sidecar requested by the catalog entry, and reuse it for every metadata
    // concern below (series types and ancestry).
    let mut input_metas = Vec::with_capacity(inputs.len());
    for entry in inputs {
        let fetched = fetch_meta(&store, &entry.meta).await?;
        anyhow::ensure!(
            fetched.uuid == entry.meta.uuid,
            "input meta UUID mismatch: requested {}, fetched {}",
            entry.meta.uuid,
            fetched.uuid
        );
        anyhow::ensure!(
            fetched.signal == signal,
            "input {} has signal {:?}, expected {signal:?}",
            fetched.uuid,
            fetched.signal
        );
        input_metas.push(fetched);
    }
    anyhow::ensure!(
        input_metas.len() == inputs.len(),
        "not all input metadata sidecars were loaded"
    );
    let compacted_from = compacted_ancestor_closure(block_uuid, &input_metas)
        .context("validate compacted ancestry")?;

    // Time bounds and schema version come straight from the inputs — the
    // merge is lossless, so min/max ts and the schema version are exact.
    let ts_min = inputs
        .iter()
        .map(|e| e.meta.ts_min_unix_nano)
        .min()
        .expect("non-empty");
    let ts_max = inputs
        .iter()
        .map(|e| e.meta.ts_max_unix_nano)
        .max()
        .expect("non-empty");
    let schema_version = inputs[0].meta.schema_version;

    // ── DataFusion: union the input main parquets, sort by the signal
    //    key, stream the result. ────────────────────────────────────
    // Disable Utf8View: DataFusion otherwise reads parquet string
    // columns back as `Utf8View`, which (a) breaks the body-column
    // downcast below and (b) would change the merged block's schema away
    // from the `Utf8` a freshly-written block uses. The merged block must
    // be schema-identical to an L0 block so every reader treats it the
    // same.
    let mut session_cfg = SessionConfig::new();
    session_cfg
        .options_mut()
        .execution
        .parquet
        .schema_force_view_types = false;
    let ctx = SessionContext::new_with_config_rt(session_cfg, resources.runtime_env());
    let url = ObjectStoreUrl::parse(format!("s3://{bucket}"))
        .map_err(|e| anyhow::anyhow!("parse object store url: {e}"))?;
    ctx.runtime_env()
        .register_object_store(url.as_ref(), store.clone());

    let paths: Vec<String> = inputs
        .iter()
        .map(|e| {
            format!(
                "s3://{bucket}/{}",
                block_path(
                    &e.meta.signal,
                    e.meta.ts_min_unix_nano,
                    e.meta.writer_id,
                    e.meta.uuid,
                    "parquet",
                )
            )
        })
        .collect();

    let df = ctx
        .read_parquet(paths, ParquetReadOptions::default())
        .await
        .map_err(crate::resource::CompactResources::classify_datafusion)
        .context("read_parquet over input blocks")?;
    let sort_exprs: Vec<_> = spec
        .sort_cols
        .iter()
        .map(|c| col(*c).sort(true, false))
        .collect();
    let df = df
        .sort(sort_exprs)
        .map_err(crate::resource::CompactResources::classify_datafusion)
        .context("sort merged inputs")?;
    let mut stream = df
        .execute_stream()
        .await
        .map_err(crate::resource::CompactResources::classify_datafusion)
        .context("execute merge stream")?;
    let out_schema = stream.schema();

    // ── Streaming pass: write main parquet, accumulate sidecar state. ─
    let fp_idx = match spec.fp_col {
        Some(name) => Some(out_schema.index_of(name).context("fp column missing")?),
        None => None,
    };
    let body_idx = match spec.body_col {
        Some(name) => Some(out_schema.index_of(name).context("body column missing")?),
        None => None,
    };
    let mut fp_set: Option<HashSet<u64>> = fp_idx.map(|_| HashSet::new());
    let mut bloom_builder = body_idx.map(|_| BodyBloomBuilder::new(block_cfg.bloom_ngram));

    let main_props = block_cfg.main_writer_props()?;
    let main_path = ObjPath::from(block_path(signal, ts_min, writer_id, block_uuid, "parquet"));
    let output_buffer = resources.config().output_buffer_bytes;
    let main_sink =
        ObjectStoreWriter::with_capacity(store.clone(), main_path.clone(), output_buffer)
            .with_max_concurrency(1);
    let mut writer = AsyncArrowWriter::try_new(main_sink, out_schema.clone(), Some(main_props))
        .context("AsyncArrowWriter::try_new (merged main)")?;
    let mut row_count: u64 = 0;

    let write_result: Result<()> = async {
        while let Some(batch) = stream.next().await {
            let batch = batch
                .map_err(crate::resource::CompactResources::classify_datafusion)
                .context("reading merged batch")?;
            row_count += batch.num_rows() as u64;

            if let (Some(set), Some(idx)) = (fp_set.as_mut(), fp_idx) {
                let arr = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .context("fingerprint column is not UInt64")?;
                let fingerprint_budget = admitted_non_df_bytes / 6;
                for v in arr.iter().flatten() {
                    if !set.contains(&v)
                        && (set.len() as u64).saturating_add(1).saturating_mul(32)
                            > fingerprint_budget
                    {
                        return Err(crate::resource::ResourceError::SidecarLimit {
                            component: "all fingerprints",
                            budget_bytes: fingerprint_budget,
                        }
                        .into());
                    }
                    set.insert(v);
                }
            }
            if let (Some(bb), Some(idx)) = (bloom_builder.as_mut(), body_idx) {
                let arr = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .context("body column is not Utf8")?;
                // Leave room in the admitted non-DF allocation for the parquet
                // writer, upload chunk, fingerprints, and final bloom bitset.
                let bloom_budget = admitted_non_df_bytes / 3;
                for i in 0..arr.len() {
                    if arr.is_valid(i) && !bb.add_body_bounded(arr.value(i), bloom_budget as usize)
                    {
                        return Err(crate::resource::ResourceError::SidecarLimit {
                            component: "body bloom n-grams",
                            budget_bytes: bloom_budget,
                        }
                        .into());
                    }
                }
            }
            writer.write(&batch).await.context("write merged batch")?;
            if writer.in_progress_size() >= resources.config().parquet_writer_memory_bytes {
                writer
                    .flush()
                    .await
                    .context("flush merged parquet row group")?;
            }
        }
        writer.finish().await.context("close merged main parquet")?;
        Ok(())
    }
    .await;
    if let Err(error) = write_result {
        let mut sink = writer.into_inner();
        if let Err(abort_error) = sink.abort().await {
            tracing::warn!(%abort_error, "failed to abort merged parquet multipart upload");
        }
        return Err(error);
    }
    let byte_size = store
        .head(&main_path)
        .await
        .context("head merged main parquet")?
        .size;

    // ── Postings (metrics/logs): bounded k-way union of sorted inputs. ─
    let (has_postings, postings_size_bytes) = if spec.fp_col.is_some() {
        let path = ObjPath::from(block_path(
            signal,
            ts_min,
            writer_id,
            block_uuid,
            "postings.parquet",
        ));
        let size = merge_postings_streaming(
            store.clone(),
            inputs,
            path,
            block_cfg,
            resources,
            admitted_non_df_bytes / 3,
        )
        .await?;
        (true, Some(size))
    } else {
        (false, None)
    };

    // ── Body bloom (logs): finalise the streamed accumulator. ────────
    let (has_body_bloom, body_bloom_size_bytes) = if let Some(bb) = bloom_builder {
        let bloom = bb.finish(block_cfg.bloom_target_fpr);
        let bytes = Bytes::from(bloom.to_bytes());
        let size = bytes.len() as u64;
        let path = ObjPath::from(block_path(
            signal,
            ts_min,
            writer_id,
            block_uuid,
            "body.bloom",
        ));
        store
            .put(&path, bytes.into())
            .await
            .with_context(|| format!("upload merged object {path}"))?;
        (true, Some(size))
    } else {
        (false, None)
    };

    // ── series_types (metrics): union from input sidecars. ───────────
    let series_types = if spec.has_series_types {
        let mut map: HashMap<u64, u8> = HashMap::new();
        let series_types_budget = admitted_non_df_bytes / 6;
        for meta in &input_metas {
            if let Some(types) = &meta.series_types {
                for &(fp, t) in types {
                    if !map.contains_key(&fp)
                        && (map.len() as u64).saturating_add(1).saturating_mul(32)
                            > series_types_budget
                    {
                        return Err(crate::resource::ResourceError::SidecarLimit {
                            component: "series types",
                            budget_bytes: series_types_budget,
                        }
                        .into());
                    }
                    map.entry(fp).or_insert(t);
                }
            }
        }
        let mut v: Vec<(u64, u8)> = map.into_iter().collect();
        v.sort_by_key(|(fp, _)| *fp);
        Some(v)
    } else {
        None
    };

    let all_fingerprints = fp_set.map(|set| {
        let mut v: Vec<u64> = set.into_iter().collect();
        v.sort_unstable();
        v
    });

    let meta = BlockMeta {
        uuid: block_uuid,
        signal: signal.to_string(),
        writer_id,
        ts_min_unix_nano: ts_min,
        ts_max_unix_nano: ts_max,
        row_count,
        byte_size,
        schema_version,
        level: out_level,
        compacted_from,
        producer_version: env!("CARGO_PKG_VERSION").to_string(),
        label_fingerprint_bloom: None,
        has_postings,
        postings_size_bytes,
        series_types,
        all_fingerprints,
        has_body_bloom,
        body_bloom_size_bytes,
        // A compacted block merges inputs spanning many WAL segments (and
        // potentially many writers/shards) and is never the live seam (its
        // records are long durable), so it carries no per-writer watermark.
        // The persistent `wal_watermarks` high-water already advanced when
        // the L0 inputs were first inserted; a merged `None` never
        // regresses it. Same for the shard discriminator.
        wal_seg_max: None,
        wal_shard: None,
    };
    let meta_bytes = Bytes::from(serde_json::to_vec(&meta).context("serialise merged meta")?);
    let meta_budget = admitted_non_df_bytes / 6;
    if meta_bytes.len() as u64 > meta_budget {
        return Err(crate::resource::ResourceError::SidecarLimit {
            component: "meta.json",
            budget_bytes: meta_budget,
        }
        .into());
    }

    // Main and sidecars have now been uploaded in durability order. None carry
    // the "block exists" signal on their own — reconcile keys on meta.json — so
    // they are safe to write before the commit point.

    // Commit-point fence: the merge may have taken minutes. If the lease was
    // lost in the meantime, abort *before* writing meta.json. Without the
    // meta sidecar the block is invisible to reconcile, the inputs stay
    // intact, and the only residue is the leaked data objects above.
    if let Err(e) = fence.check() {
        tracing::warn!(
            block_uuid = %block_uuid,
            signal,
            out_level,
            error = %e,
            "lease lost during merge; skipping meta.json commit (block aborted)"
        );
        return Ok(None);
    }

    // Commit: meta.json last (durability invariant — the "block exists" signal).
    // From the first attempt onward the result can be ambiguous: a store may
    // persist the object and lose the response. Never roll back data objects
    // after this point; reconciliation provides idempotent completion.
    cleanup.disarm();
    let meta_path = ObjPath::from(block_path(
        signal,
        ts_min,
        writer_id,
        block_uuid,
        "meta.json",
    ));
    store
        .put(&meta_path, meta_bytes.into())
        .await
        .with_context(|| format!("upload merged meta {meta_path}"))?;

    tracing::info!(
        block_uuid = %meta.uuid,
        signal,
        out_level,
        inputs = inputs.len(),
        row_count = meta.row_count,
        byte_size = meta.byte_size,
        "merged block uploaded"
    );
    Ok(Some(meta))
}

/// K-way merge the already-sorted input postings streams into bounded output
/// batches. Only one input record batch, one current row per input, and one
/// output batch are retained at a time. A single pathological postings row is
/// rejected against the permit-relative budget, though parquet decoding itself
/// may allocate that row before it can be inspected.
async fn merge_postings_streaming(
    store: Arc<dyn ObjectStore>,
    inputs: &[CatalogEntry],
    output_path: ObjPath,
    block_cfg: &BlockBuilderConfig,
    resources: &Arc<crate::resource::CompactResources>,
    postings_budget: u64,
) -> Result<u64> {
    let mut cursors = Vec::new();
    for entry in inputs.iter().filter(|entry| entry.meta.has_postings) {
        let path = ObjPath::from(block_path(
            &entry.meta.signal,
            entry.meta.ts_min_unix_nano,
            entry.meta.writer_id,
            entry.meta.uuid,
            "postings.parquet",
        ));
        let meta = store
            .head(&path)
            .await
            .with_context(|| format!("HEAD input postings {path}"))?;
        let reader =
            ParquetObjectReader::new(store.clone(), path.clone()).with_file_size(meta.size);
        let stream = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .with_context(|| format!("open input postings {path}"))?
            .with_batch_size(1024)
            .build()
            .with_context(|| format!("build input postings stream {path}"))?;
        let mut cursor = PostingsCursor {
            stream: Box::pin(stream),
            batch: None,
            row: 0,
            current: None,
        };
        cursor.advance().await?;
        cursors.push(cursor);
    }

    let props = block_cfg.postings_writer_props()?;
    let sink = ObjectStoreWriter::with_capacity(
        store.clone(),
        output_path.clone(),
        resources.config().output_buffer_bytes,
    )
    .with_max_concurrency(1);
    let mut writer = AsyncArrowWriter::try_new(sink, postings_schema(), Some(props))
        .context("create merged postings writer")?;
    let limit = postings_budget.max(1);
    let batch_limit = resources
        .config()
        .parquet_writer_memory_bytes
        .min(limit as usize)
        .max(1);
    let result: Result<()> = async {
        let mut heap = BinaryHeap::new();
        for (index, cursor) in cursors.iter().enumerate() {
            if let Some((key, _)) = &cursor.current {
                heap.push(Reverse((key.clone(), index)));
            }
        }
        let mut output = Vec::<PostingsEntry>::new();
        let mut output_bytes = 0usize;
        while let Some(Reverse((key, index))) = heap.pop() {
            let mut fps = Vec::new();
            let mut matching = vec![index];
            while let Some(Reverse((next, _))) = heap.peek() {
                if next != &key {
                    break;
                }
                let Reverse((_, i)) = heap.pop().expect("peeked heap entry");
                matching.push(i);
            }
            for i in matching {
                let (_, values) = cursors[i].current.take().context("missing postings row")?;
                let requested = fps.len().saturating_add(values.len()).saturating_mul(8) as u64;
                if requested > limit {
                    return Err(crate::resource::ResourceError::RequestTooLarge {
                        requested_bytes: requested,
                        budget_bytes: limit,
                    }
                    .into());
                }
                fps.extend(values);
                cursors[i].advance().await?;
                if let Some((next, _)) = &cursors[i].current {
                    heap.push(Reverse((next.clone(), i)));
                }
            }
            fps.sort_unstable();
            fps.dedup();
            output_bytes =
                output_bytes.saturating_add(key.0.len() + key.1.len() + fps.len() * 8 + 16);
            output.push((key, fps));
            if output_bytes >= batch_limit {
                writer.write(&postings_record_batch(&output)?).await?;
                output.clear();
                output_bytes = 0;
                if writer.in_progress_size() >= batch_limit {
                    writer.flush().await?;
                }
            }
        }
        if !output.is_empty() || cursors.is_empty() {
            writer.write(&postings_record_batch(&output)?).await?;
        }
        writer.finish().await?;
        Ok(())
    }
    .await;
    if let Err(error) = result {
        let mut sink = writer.into_inner();
        if let Err(abort_error) = sink.abort().await {
            tracing::warn!(%abort_error, "failed to abort postings multipart upload");
        }
        return Err(error);
    }
    Ok(store.head(&output_path).await?.size)
}

async fn cleanup_paths(store: &Arc<dyn ObjectStore>, paths: &[ObjPath]) {
    for path in paths {
        if let Err(error) = store.delete(path).await {
            // A missing object is the common case when failure happened before
            // that stage. ObjectStore has no portable not-found predicate, so
            // retain this as debug-only diagnostics rather than masking failure.
            tracing::debug!(%path, %error, "failed to clean uncommitted compaction object");
        }
    }
}

/// Fetch and parse a block's `meta.json` sidecar from the bucket.
async fn fetch_meta(store: &Arc<dyn ObjectStore>, meta: &BlockMeta) -> Result<BlockMeta> {
    let p = block_path(
        &meta.signal,
        meta.ts_min_unix_nano,
        meta.writer_id,
        meta.uuid,
        "meta.json",
    );
    let bytes = store
        .get(&ObjPath::from(p))
        .await
        .context("get input meta.json")?
        .bytes()
        .await
        .context("read input meta.json body")?;
    serde_json::from_slice(&bytes).context("parse input meta.json")
}
