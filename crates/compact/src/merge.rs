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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Array, StringArray, UInt64Array};
use bytes::Bytes;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::{col, ParquetReadOptions, SessionConfig, SessionContext};
use futures::StreamExt;
use object_store::{path::Path as ObjPath, ObjectStore, ObjectStoreExt};
use parquet::arrow::ArrowWriter;
use scry_block::postings::{decode_postings, encode_postings, merge_postings};
use scry_block::{block_path, BlockBuilderConfig, BlockMeta, BodyBloomBuilder, Fence};
use scry_catalog::CatalogEntry;
use uuid::Uuid;

/// Per-signal knobs the merge needs: the sort key (so the merged block
/// keeps the same intra-block ordering its readers prune on), and which
/// sidecars to rebuild.
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
) -> Result<Option<BlockMeta>> {
    anyhow::ensure!(!inputs.is_empty(), "merge_blocks called with no inputs");
    let spec = spec_for(signal)?;

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
    let ctx = SessionContext::new_with_config(session_cfg);
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
        .context("read_parquet over input blocks")?;
    let sort_exprs: Vec<_> = spec
        .sort_cols
        .iter()
        .map(|c| col(*c).sort(true, false))
        .collect();
    let df = df.sort(sort_exprs).context("sort merged inputs")?;
    let mut stream = df.execute_stream().await.context("execute merge stream")?;
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
    let mut main_buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut main_buf, out_schema.clone(), Some(main_props))
        .context("ArrowWriter::try_new (merged main)")?;
    let mut row_count: u64 = 0;

    while let Some(batch) = stream.next().await {
        let batch = batch.context("reading merged batch")?;
        row_count += batch.num_rows() as u64;

        if let (Some(set), Some(idx)) = (fp_set.as_mut(), fp_idx) {
            let arr = batch
                .column(idx)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .context("fingerprint column is not UInt64")?;
            for v in arr.iter().flatten() {
                set.insert(v);
            }
        }
        if let (Some(bb), Some(idx)) = (bloom_builder.as_mut(), body_idx) {
            let arr = batch
                .column(idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("body column is not Utf8")?;
            for i in 0..arr.len() {
                if arr.is_valid(i) {
                    bb.add_body(arr.value(i));
                }
            }
        }
        writer.write(&batch).context("write merged batch")?;
    }
    writer.close().context("close merged main parquet")?;
    let main_bytes = Bytes::from(main_buf);
    let byte_size = main_bytes.len() as u64;

    let block_uuid = Uuid::now_v7();
    let mut puts: Vec<(ObjPath, Bytes)> = Vec::new();
    puts.push((
        ObjPath::from(block_path(signal, ts_min, writer_id, block_uuid, "parquet")),
        main_bytes,
    ));

    // ── Postings (metrics/logs): union the inputs' postings. ─────────
    let (has_postings, postings_size_bytes) = if spec.fp_col.is_some() {
        let mut sets = Vec::with_capacity(inputs.len());
        for e in inputs {
            if !e.meta.has_postings {
                continue;
            }
            let p = block_path(
                &e.meta.signal,
                e.meta.ts_min_unix_nano,
                e.meta.writer_id,
                e.meta.uuid,
                "postings.parquet",
            );
            let bytes = store
                .get(&ObjPath::from(p))
                .await
                .context("get input postings")?
                .bytes()
                .await
                .context("read input postings body")?;
            sets.push(decode_postings(bytes).context("decode input postings")?);
        }
        let merged = merge_postings(sets);
        let props = block_cfg.postings_writer_props()?;
        let bytes = encode_postings(&merged, &props).context("encode merged postings")?;
        let size = bytes.len() as u64;
        puts.push((
            ObjPath::from(block_path(
                signal,
                ts_min,
                writer_id,
                block_uuid,
                "postings.parquet",
            )),
            bytes,
        ));
        (true, Some(size))
    } else {
        (false, None)
    };

    // ── Body bloom (logs): finalise the streamed accumulator. ────────
    let (has_body_bloom, body_bloom_size_bytes) = if let Some(bb) = bloom_builder {
        let bloom = bb.finish(block_cfg.bloom_target_fpr);
        let bytes = Bytes::from(bloom.to_bytes());
        let size = bytes.len() as u64;
        puts.push((
            ObjPath::from(block_path(signal, ts_min, writer_id, block_uuid, "body.bloom")),
            bytes,
        ));
        (true, Some(size))
    } else {
        (false, None)
    };

    // ── series_types (metrics): union from input sidecars. ───────────
    let series_types = if spec.has_series_types {
        let mut map: HashMap<u64, u8> = HashMap::new();
        for e in inputs {
            let meta = fetch_meta(&store, &e.meta).await?;
            if let Some(types) = meta.series_types {
                for (fp, t) in types {
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
        producer_version: env!("CARGO_PKG_VERSION").to_string(),
        label_fingerprint_bloom: None,
        has_postings,
        postings_size_bytes,
        series_types,
        all_fingerprints,
        has_body_bloom,
        body_bloom_size_bytes,
    };
    let meta_bytes = Bytes::from(serde_json::to_vec_pretty(&meta).context("serialise merged meta")?);

    // Upload the data objects first (main → [postings] → [bloom]). These
    // carry no "block exists" signal on their own — reconcile keys on
    // meta.json — so they are safe to write before the commit point.
    for (path, bytes) in puts {
        store
            .put(&path, bytes.into())
            .await
            .with_context(|| format!("upload merged object {path}"))?;
    }

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
    let meta_path = ObjPath::from(block_path(signal, ts_min, writer_id, block_uuid, "meta.json"));
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

