//! Postings-driven fingerprint resolution.
//!
//! For each block, we fetch `<block>.postings.parquet` once and walk
//! its rows to find every `(label_name, label_value)` row that matches
//! *any* of the query's AND'd matchers. Once we have one fingerprint
//! set per matcher, we intersect them. An empty intersection means the
//! block is fully pruned: the caller skips the main parquet entirely.
//!
//! Why fetch + scan in-process rather than pushing the filter into
//! parquet: the postings parquet is sorted by `(label_name,
//! label_value)`, so a proper RowFilter pushdown would skip most of
//! the file at the parquet level. At v0.2 scale (postings ≤ a few MB
//! per block per the architecture) the in-process scan is fast enough
//! and a fraction of the engineering cost. RowFilter pushdown is the
//! v0.3 optimisation.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Array, ListArray, StringArray, UInt64Array};
use futures::TryStreamExt;
use object_store::{path::Path as ObjPath, ObjectStore, ObjectStoreExt};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use scry_block::{block_path, BlockMeta};

use crate::postings_cache::PostingsIndex;

/// Resolve the AND'd matcher set to the fingerprint set that overlaps
/// every matcher in this block.
///
/// Returns:
/// - `Ok(None)`         — at least one matcher had zero hits, so the
///                        AND'd intersection is empty. Caller can skip
///                        the main parquet entirely.
/// - `Ok(Some(set))`    — non-empty intersection; pass to `scan_block`.
///
/// Special case: an empty `matchers` list returns the block's full
/// fingerprint set, derived from `meta.all_fingerprints` (which every
/// real signal's block builder populates — metrics fills it from the
/// series dictionary, logs from the stream dictionary). We avoid the
/// postings scan because the postings file is keyed by `(label_name,
/// label_value)` and has no natural "all fingerprints" row, while the
/// sidecar already carries the complete list.
///
/// Signal-agnostic: the block's signal name lives on the [`BlockMeta`]
/// itself (filled in by the catalog hydration from the `signal`
/// column), so the same code path serves metrics and logs without
/// branching.
pub async fn resolve_fingerprints(
    store: Arc<dyn ObjectStore>,
    meta: &BlockMeta,
    matchers: &[(String, String)],
) -> Result<Option<HashSet<u64>>> {
    if matchers.is_empty() {
        // The catalog row carries a summary of the sidecar but not
        // `all_fingerprints` itself (it's variable-length, only used
        // for the empty-matcher path). Fetch the full sidecar JSON
        // to recover the per-block fingerprint list. Empty-matcher
        // queries are diagnostic ("scan everything") rather than
        // hot, so the extra GET is fine.
        let sidecar_path = ObjPath::from(block_path(
            &meta.signal,
            meta.ts_min_unix_nano,
            meta.writer_id,
            meta.uuid,
            "meta.json",
        ));
        let bytes = store
            .get(&sidecar_path)
            .await
            .with_context(|| format!("GET sidecar {sidecar_path}"))?
            .bytes()
            .await
            .with_context(|| format!("read sidecar body {sidecar_path}"))?;
        let full_meta: BlockMeta = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse sidecar {sidecar_path}"))?;
        let Some(all) = full_meta.all_fingerprints.as_ref() else {
            anyhow::bail!(
                "block {} ({}) sidecar has no all_fingerprints; cannot resolve empty matcher set",
                meta.uuid,
                meta.signal
            );
        };
        let set: HashSet<u64> = all.iter().copied().collect();
        return Ok(if set.is_empty() { None } else { Some(set) });
    }

    let index = fetch_and_parse_postings(store, meta).await?;
    Ok(intersect_matchers(&index, matchers))
}

/// Fetch `<block>.postings.parquet` from object storage and decode it
/// into a fully-indexed in-memory [`PostingsIndex`]. The expensive
/// step (GET + parquet decode + `StringArray`/`ListArray` downcasts +
/// `Vec<u64>` materialisation) — split out so the `PostingsCache` can
/// invoke it on a miss and stash the result.
///
/// Re-running this against the same block returns identical data
/// (blocks are immutable), which is why caching the result is sound
/// without invalidation.
pub async fn fetch_and_parse_postings(
    store: Arc<dyn ObjectStore>,
    meta: &BlockMeta,
) -> Result<PostingsIndex> {
    let path = ObjPath::from(block_path(
        &meta.signal,
        meta.ts_min_unix_nano,
        meta.writer_id,
        meta.uuid,
        "postings.parquet",
    ));

    // ParquetObjectReader pulls bytes from object storage on demand;
    // for the postings parquet we end up reading the whole file (a
    // few MB at most), but the same abstraction is what makes the
    // main-parquet scan's byte-range pruning possible.
    //
    // We HEAD first and pass `with_file_size` so the reader uses
    // bounded range requests instead of S3 suffix-range probes; some
    // S3-compatibles (notably older Garage builds) are quirky about
    // suffix ranges, and one extra HEAD is cheaper than retry math.
    let object_meta = store
        .head(&path)
        .await
        .with_context(|| format!("HEAD postings parquet {path}"))?;
    let reader = ParquetObjectReader::new(store, path.clone()).with_file_size(object_meta.size);
    let mut stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .with_context(|| format!("opening postings parquet {path}"))?
        .build()
        .with_context(|| format!("building postings reader {path}"))?;

    let mut entries: std::collections::HashMap<
        String,
        std::collections::HashMap<String, Arc<Vec<u64>>>,
    > = std::collections::HashMap::new();

    while let Some(batch) = stream.try_next().await.context("postings batch")? {
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("postings col 0 not StringArray")?;
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("postings col 1 not StringArray")?;
        let lists = batch
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .context("postings col 2 not ListArray")?;

        for row in 0..batch.num_rows() {
            let name = names.value(row);
            let value = values.value(row);
            let fps_arr = lists.value(row);
            let fps = fps_arr
                .as_any()
                .downcast_ref::<UInt64Array>()
                .context("postings list inner not UInt64Array")?;
            let vec: Vec<u64> = fps.values().iter().copied().collect();
            entries
                .entry(name.to_string())
                .or_default()
                .insert(value.to_string(), Arc::new(vec));
        }
    }

    Ok(PostingsIndex::new(entries))
}

/// Intersect AND'd matchers against an already-loaded
/// [`PostingsIndex`]. Returns `None` if any matcher's posting list is
/// missing or empty — that's the "block fully pruned" signal.
///
/// Same algorithm as the original inline loop: seed with the
/// smallest list, retain in-place against each other matcher's set,
/// short-circuit on an empty accumulator.
pub fn intersect_matchers(
    index: &PostingsIndex,
    matchers: &[(String, String)],
) -> Option<HashSet<u64>> {
    let mut per_matcher: Vec<&Arc<Vec<u64>>> = Vec::with_capacity(matchers.len());
    for (name, value) in matchers {
        match index.lookup(name, value) {
            Some(fps) if !fps.is_empty() => per_matcher.push(fps),
            _ => return None,
        }
    }
    if per_matcher.is_empty() {
        // Caller passes non-empty matchers; reaching here means every
        // matcher matched but contributed an empty Vec — equivalent
        // to "no candidate fingerprints".
        return None;
    }

    let smallest_idx = per_matcher
        .iter()
        .enumerate()
        .min_by_key(|(_, v)| v.len())
        .map(|(i, _)| i)
        .expect("non-empty");
    let mut acc: HashSet<u64> = per_matcher[smallest_idx].iter().copied().collect();
    for (i, fps) in per_matcher.iter().enumerate() {
        if i == smallest_idx {
            continue;
        }
        let other: HashSet<u64> = fps.iter().copied().collect();
        acc.retain(|fp| other.contains(fp));
        if acc.is_empty() {
            return None;
        }
    }
    Some(acc)
}
