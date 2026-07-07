//! Bucket polling: the source-of-truth backstop behind pub/sub.
//!
//! Pub/sub (the convergence consumer) is a low-latency hint that can drop
//! events. Polling re-derives the catalog from the bucket — the actual source
//! of truth — in two modes:
//!
//! - [`poll_once`] — **incremental**: for each known poll cursor
//!   `(signal, writer_id, date)`, list only the objects newer than the
//!   cursor's high-water UUID (`list_with_offset`, exclusive). A healthy
//!   pub/sub stream keeps cursors at the head, so a healthy poll lists nothing
//!   — cheap enough to run every few seconds when degraded. This catches
//!   blocks dropped by pub/sub for prefixes the catalog already tracks.
//! - [`full_walk`] — **exhaustive**: list every `*.meta.json` in the bucket
//!   and upsert it, seeding cursors for prefixes no event/poll has discovered
//!   yet (a brand-new writer or date). Runs on a long jittered interval
//!   (~30 min) as the ultimate backstop.
//!
//! Both share [`fetch_and_apply`], which inserts (idempotent `INSERT OR
//! IGNORE`) and advances cursors to the max UUID seen per prefix. Cursors only
//! advance (monotonic), so re-listing already-known blocks is a no-op.

use std::collections::HashMap;

use anyhow::{Context, Result};
use futures::StreamExt;
use object_store::{path::Path as ObjPath, ObjectStore, ObjectStoreExt};
use scry_block::BlockMeta;
use scry_catalog::{date_dir, CatalogHandle};
use uuid::Uuid;

/// Outcome of a poll / walk pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PollReport {
    /// Cursors examined (incremental poll) — 0 for a full walk.
    pub cursors: usize,
    /// `*.meta.json` objects observed across all prefixes this pass.
    pub seen: usize,
    /// Blocks newly inserted into the catalog (previously unknown).
    pub inserted: usize,
    /// Sidecars that failed to parse — counted, logged, skipped.
    pub failed: usize,
}

/// Build the object-store prefix for a `(signal, date, writer_id)` partition:
/// `<signal>/<yyyy>/<mm>/<dd>/<writer_id>/`. Matches `scry_block::block_path`'s
/// layout (`%Y/%m/%d`), derived here from the `yyyy-mm-dd` cursor date.
fn partition_prefix(signal: &str, date: &str, writer_id: Uuid) -> String {
    format!("{signal}/{}/{writer_id}/", date.replace('-', "/"))
}

/// Incrementally poll every known cursor for blocks newer than its
/// high-water UUID. Cursors are discovered by the convergence consumer
/// (on `Created`) and by [`full_walk`]; this only backstops known prefixes.
pub async fn poll_once<C, S>(store: &S, catalog: &C, bucket: &str) -> Result<PollReport>
where
    C: CatalogHandle,
    S: ObjectStore + ?Sized,
{
    let cursors = catalog.with(|c| c.list_cursors()).context("list cursors")?;
    let mut report = PollReport {
        cursors: cursors.len(),
        ..Default::default()
    };

    for (signal, writer_id, date) in cursors {
        let prefix = partition_prefix(&signal, &date, writer_id);
        let high = catalog
            .with(|c| c.get_cursor(&signal, writer_id, &date))
            .with_context(|| format!("get_cursor {signal}/{date}/{writer_id}"))?;

        // start-after the cursor's UUID stem (exclusive). Re-includes that
        // UUID's own sibling objects (harmless idempotent inserts) and every
        // newer block. `None` shouldn't happen (a listed cursor has a value),
        // but if it does, fall back to listing the whole prefix.
        let offset = match high {
            Some(uuid) => ObjPath::from(format!("{prefix}{uuid}")),
            None => ObjPath::from(prefix.clone()),
        };
        let prefix_path = ObjPath::from(prefix.as_str());

        let locations = collect_meta_locations_with_offset(store, &prefix_path, &offset).await?;
        fetch_and_apply(store, catalog, bucket, locations, &mut report).await?;
    }

    Ok(report)
}

/// Exhaustively walk the bucket: list every `*.meta.json`, upsert it, and
/// seed/advance cursors. The ultimate backstop — discovers prefixes no event
/// or incremental poll has seen.
pub async fn full_walk<C, S>(store: &S, catalog: &C, bucket: &str) -> Result<PollReport>
where
    C: CatalogHandle,
    S: ObjectStore + ?Sized,
{
    let mut report = PollReport::default();
    let locations = collect_meta_locations(store, None).await?;
    fetch_and_apply(store, catalog, bucket, locations, &mut report).await?;
    Ok(report)
}

/// List a prefix and return the locations of every `*.meta.json` object.
async fn collect_meta_locations<S>(store: &S, prefix: Option<&ObjPath>) -> Result<Vec<ObjPath>>
where
    S: ObjectStore + ?Sized,
{
    let mut stream = store.list(prefix);
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        let meta = item.context("listing bucket objects")?;
        let loc = meta.location.as_ref();
        // `_catalog/` is reserved for catalog snapshots (D-055), not blocks.
        if loc.starts_with("_catalog/") {
            continue;
        }
        if loc.ends_with(".meta.json") {
            out.push(meta.location);
        }
    }
    Ok(out)
}

/// Like [`collect_meta_locations`] but only objects strictly after `offset`.
async fn collect_meta_locations_with_offset<S>(
    store: &S,
    prefix: &ObjPath,
    offset: &ObjPath,
) -> Result<Vec<ObjPath>>
where
    S: ObjectStore + ?Sized,
{
    let mut stream = store.list_with_offset(Some(prefix), offset);
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        let meta = item.context("listing bucket objects (offset)")?;
        let loc = meta.location.as_ref();
        // `_catalog/` is reserved for catalog snapshots (D-055), not blocks.
        if loc.starts_with("_catalog/") {
            continue;
        }
        if loc.ends_with(".meta.json") {
            out.push(meta.location);
        }
    }
    Ok(out)
}

/// Fetch each meta.json, parse it, `insert_block` (idempotent), and advance
/// the per-prefix cursor to the max UUID seen. Parse failures are counted and
/// skipped — one bad sidecar never aborts the pass. Updates `report` in place.
async fn fetch_and_apply<C, S>(
    store: &S,
    catalog: &C,
    _bucket: &str,
    locations: Vec<ObjPath>,
    report: &mut PollReport,
) -> Result<()>
where
    C: CatalogHandle,
    S: ObjectStore + ?Sized,
{
    // Highest UUID seen per (signal, writer_id, date) this pass, so we issue
    // one monotonic cursor advance per prefix at the end.
    let mut high: HashMap<(String, Uuid, String), Uuid> = HashMap::new();

    for loc in locations {
        report.seen += 1;
        let bytes = match store.get(&loc).await {
            Ok(r) => r.bytes().await.context("read meta.json body")?,
            // A peer may have deleted the block between list and get — fine.
            Err(object_store::Error::NotFound { .. }) => continue,
            Err(e) => return Err(e).with_context(|| format!("get {loc}")),
        };
        let meta: BlockMeta = match serde_json::from_slice(&bytes) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(location = %loc, error = %e, "skipping unparseable meta.json");
                report.failed += 1;
                continue;
            }
        };

        let inserted = catalog
            .with(|c| c.insert_block(&meta))
            .context("poll insert_block")?;
        if inserted {
            report.inserted += 1;
        }

        let key = (
            meta.signal.clone(),
            meta.writer_id,
            date_dir(meta.ts_min_unix_nano),
        );
        high.entry(key)
            .and_modify(|u| {
                if meta.uuid > *u {
                    *u = meta.uuid;
                }
            })
            .or_insert(meta.uuid);
    }

    for ((signal, writer_id, date), uuid) in high {
        catalog
            .with(|c| c.advance_cursor(&signal, writer_id, &date, uuid))
            .context("poll advance_cursor")?;
    }

    Ok(())
}
