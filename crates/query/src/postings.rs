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
/// fingerprint set, derived from `meta.series_types` (which the
/// metrics block builder populates). We avoid the postings scan
/// because the postings file is keyed by `(label_name, label_value)`
/// and has no natural "all series" row, while the sidecar already
/// carries the complete fingerprint list.
pub async fn resolve_fingerprints(
    store: Arc<dyn ObjectStore>,
    meta: &BlockMeta,
    matchers: &[(String, String)],
) -> Result<Option<HashSet<u64>>> {
    if matchers.is_empty() {
        // The catalog row carries a summary of the sidecar but not
        // `series_types` itself (it's variable-length, only needed
        // for type-aware queries). For the empty-matcher path we
        // fetch the full sidecar JSON to recover the per-block
        // fingerprint list. Empty-matcher queries are diagnostic
        // ("scan everything") rather than hot, so the extra GET is
        // fine.
        let sidecar_path = ObjPath::from(block_path(
            "metrics",
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
        let Some(types) = full_meta.series_types.as_ref() else {
            anyhow::bail!(
                "metrics block {} sidecar has no series_types; cannot resolve empty matcher set",
                meta.uuid
            );
        };
        let set: HashSet<u64> = types.iter().map(|(fp, _)| *fp).collect();
        return Ok(if set.is_empty() { None } else { Some(set) });
    }

    let path = ObjPath::from(block_path(
        "metrics",
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
    let reader =
        ParquetObjectReader::new(store, path.clone()).with_file_size(object_meta.size);
    let mut stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .with_context(|| format!("opening postings parquet {path}"))?
        .build()
        .with_context(|| format!("building postings reader {path}"))?;

    // One Vec<u64> per matcher, in the same order as `matchers`. We
    // collect into Vecs first (cheap appends) and convert to HashSet
    // only at intersection time — saves a hash insert per fingerprint
    // for matchers whose row never appears.
    let mut per_matcher: Vec<Vec<u64>> = vec![Vec::new(); matchers.len()];

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
            // Linear scan of matchers per row. With N matchers ≤ ~10
            // and postings rows in the thousands, this is far below
            // the cost of the parquet decode itself. A HashMap-keyed
            // lookup over `(name, value)` would shave it but adds
            // allocator churn we don't need.
            for (i, (mname, mvalue)) in matchers.iter().enumerate() {
                if name == mname && value == mvalue {
                    let fps_arr = lists.value(row);
                    let fps = fps_arr
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .context("postings list inner not UInt64Array")?;
                    per_matcher[i].reserve(fps.len());
                    for j in 0..fps.len() {
                        per_matcher[i].push(fps.value(j));
                    }
                }
            }
        }
    }

    // Intersect. At v0.2 scale (≤ thousands of fingerprints per
    // matcher) HashSet is simpler than the sorted-merge approach;
    // the asymptotic difference doesn't matter.
    if per_matcher.iter().any(|v| v.is_empty()) {
        // At least one matcher matched zero postings rows → AND is
        // empty → block fully pruned.
        return Ok(None);
    }

    // Seed with the smallest matcher's set; intersect each remaining
    // matcher in place. Avoids materialising every matcher's full
    // set when one of them is selective (the common case for queries
    // like `__name__=foo AND env=prod` where `__name__` is the
    // narrowest predicate).
    let smallest_idx = per_matcher
        .iter()
        .enumerate()
        .min_by_key(|(_, v)| v.len())
        .map(|(i, _)| i)
        .expect("matchers non-empty");
    let mut acc: HashSet<u64> = per_matcher[smallest_idx].iter().copied().collect();
    for (i, fps) in per_matcher.iter().enumerate() {
        if i == smallest_idx {
            continue;
        }
        let other: HashSet<u64> = fps.iter().copied().collect();
        acc.retain(|fp| other.contains(fp));
        if acc.is_empty() {
            return Ok(None);
        }
    }
    Ok(Some(acc))
}
