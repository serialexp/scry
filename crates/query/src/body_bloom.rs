//! Query-side body-bloom skip for the logs full-text path.
//!
//! Each logs block carries a `<uuid>.body.bloom` sidecar — a byte-trigram
//! bloom over every log body (built inline at seal; see
//! [`scry_block::bloom`]). When a query asks for `body_contains`, we fetch
//! the bloom and ask whether the pattern *could* occur in the block. If it
//! definitely can't, the block is dropped before any postings/parquet I/O.
//!
//! The bloom has one-sided error: a `false` from [`scry_block::BodyBloom::contains_pattern`]
//! is authoritative ("not here"), a `true` is "maybe" and is verified by
//! the exact `contains(body, pat)` predicate during the scan. So a stale or
//! missing bloom can only ever cost a wasted scan, never a lost result —
//! which is why fetch failures here are logged and treated as "keep the
//! block," not as query errors.

use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::{path::Path as ObjPath, ObjectStore, ObjectStoreExt};
use scry_block::{block_path, BlockMeta, BodyBloom};

/// Fetch and parse a block's `body.bloom` sidecar. The sidecar is a small
/// raw blob (header + bitset), so this is a single whole-object GET — no
/// parquet machinery. `Ok(None)` means the bytes were present but didn't
/// parse as a bloom (bad magic / version / truncation); callers treat that
/// the same as a transport error: keep the block and scan it.
pub async fn fetch_body_bloom(
    store: Arc<dyn ObjectStore>,
    meta: &BlockMeta,
) -> Result<Option<BodyBloom>> {
    let path = ObjPath::from(block_path(
        &meta.signal,
        meta.ts_min_unix_nano,
        meta.writer_id,
        meta.uuid,
        "body.bloom",
    ));
    let bytes = store
        .get(&path)
        .await
        .with_context(|| format!("GET body bloom {path}"))?
        .bytes()
        .await
        .with_context(|| format!("read body bloom body {path}"))?;
    Ok(BodyBloom::from_bytes(&bytes))
}

/// Decide whether a candidate block can be skipped for a `body_contains`
/// query. Returns `true` when the block's bloom authoritatively rules the
/// pattern out. Any failure to obtain a usable bloom returns `false`
/// (keep the block) — correctness over speed.
pub async fn block_excluded_by_bloom(
    store: Arc<dyn ObjectStore>,
    meta: &BlockMeta,
    pattern: &str,
) -> bool {
    if !meta.has_body_bloom {
        return false;
    }
    match fetch_body_bloom(store, meta).await {
        Ok(Some(bloom)) => !bloom.contains_pattern(pattern),
        Ok(None) => {
            tracing::warn!(block = %meta.uuid, "body bloom unparseable; scanning block");
            false
        }
        Err(e) => {
            tracing::warn!(block = %meta.uuid, error = %e, "body bloom fetch failed; scanning block");
            false
        }
    }
}
