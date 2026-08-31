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
//! - [`full_walk`] — **exhaustive**: list every `*.meta.json` in the bucket,
//!   upsert the ones the catalog doesn't already have, and seed cursors for
//!   prefixes no event/poll has discovered yet (a brand-new writer or date).
//!   Runs on a long interval as the ultimate backstop.
//!
//! Both share [`fetch_and_apply`], which inserts (idempotent `INSERT OR
//! IGNORE`) and advances cursors to the max UUID seen per prefix. Cursors only
//! advance (monotonic), so re-listing already-known blocks is a no-op.
//!
//! **The walk costs a LIST, not a GET per block** (D-066). The block UUID is
//! in the object key, so a listed sidecar whose UUID the catalog already has
//! is skipped without being fetched. Before this, a converged deployment paid
//! one GET per block on every pass to learn nothing: gothab's 346k-block
//! bucket took 15-20 hours per walk on a 30-minute timer, so the walk ran
//! permanently and starved live queries of object-store throughput.

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
    /// Sidecars **not fetched at all** because the catalog already had a row
    /// for the UUID in the object key. In a converged deployment this is very
    /// nearly the whole listing, and it is the difference between a walk that
    /// costs one GET per block and one that costs none.
    pub skipped: usize,
    /// Sidecars whose GET failed for a reason other than `NotFound` — counted,
    /// logged, and skipped. These do **not** advance a cursor, so the next pass
    /// retries them.
    pub fetch_failed: usize,
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

        // No known-set filter here: the offset listing already narrows this to
        // the handful of objects newer than the cursor, so loading a
        // catalog-sized UUID set once per cursor — every few seconds — would
        // cost far more than the GETs it saved.
        let locations = collect_meta_locations_with_offset(store, &prefix_path, &offset).await?;
        fetch_and_apply(store, catalog, bucket, locations, None, &mut report).await?;
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
    full_walk_with_grace(store, catalog, bucket, Duration::ZERO).await
}

pub async fn full_walk_with_grace<C, S>(
    store: &S,
    catalog: &C,
    bucket: &str,
    reap_grace: Duration,
) -> Result<PollReport>
where
    C: CatalogHandle,
    S: ObjectStore + ?Sized,
{
    let mut report = PollReport::default();
    tracing::info!("catalog full-walk starting bucket listing");
    let locations = collect_meta_locations(store, None).await?;
    let total = locations.len();

    // One catalog-sized read, then the listing is filtered against it: a walk
    // over a converged bucket should cost a LIST and essentially no GETs. This
    // is the difference between a backstop and a permanent background load —
    // see D-066.
    let known = catalog
        .with(|c| c.known_block_uuids())
        .context("load known block uuids")?;
    tracing::info!(
        meta_objects = total,
        known_blocks = known.len(),
        "catalog full-walk listing complete; fetching sidecars"
    );
    fetch_and_apply(store, catalog, bucket, locations, Some(&known), &mut report).await?;
    tracing::info!(
        seen = report.seen,
        skipped = report.skipped,
        inserted = report.inserted,
        failed = report.failed,
        fetch_failed = report.fetch_failed,
        "catalog full-walk complete"
    );
    let eligible = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        + reap_grace)
        .as_nanos() as u64;
    catalog
        .with(|c| c.stage_unstaged_superseded(eligible))
        .context("stage full-walk superseded reaps")?;
    Ok(report)
}

/// Reconcile one `(signal, date)` compaction partition from bucket truth.
/// Called only after taking that partition's lease, before validating a plan,
/// so a prior holder that committed `meta.json` but crashed before publishing
/// cannot cause the same inputs to be merged into a duplicate output.
pub async fn reconcile_partition<C, S>(
    store: &S,
    catalog: &C,
    bucket: &str,
    signal: &str,
    date: &str,
    reap_grace: Duration,
) -> Result<PollReport>
where
    C: CatalogHandle,
    S: ObjectStore + ?Sized,
{
    let prefix = ObjPath::from(format!("{signal}/{}/", date.replace('-', "/")));
    let locations = collect_meta_locations(store, Some(&prefix)).await?;
    let mut report = PollReport::default();
    // Filtered by what the catalog already holds, exactly as D-066 taught the
    // full walk. The LIST is still authoritative — it discovers every committed
    // `meta.json` in the prefix, including one a crashed peer wrote — but the
    // GETs skip sidecars the catalog already has. On a converged catalog this
    // turns ~3,900 GETs into approximately zero, dropping the per-partition
    // cost from ~50 s to the LIST time (~2 s).
    //
    // This was previously `None` ("deliberately unfiltered") out of caution.
    // The filter is safe because `known_block_uuids()` returns every row in
    // `blocks` (no liveness filter), so a superseded or soft-deleted block is
    // still "known" and never re-fetched — matching the full walk's guarantee.
    let known = catalog
        .with(|c| c.known_block_uuids())
        .context("load known block uuids for partition reconcile")?;
    fetch_and_apply(store, catalog, bucket, locations, Some(&known), &mut report).await?;
    let eligible = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        + reap_grace)
        .as_nanos() as u64;
    catalog
        .with(|c| c.stage_unstaged_superseded(eligible))
        .context("stage partition superseded reaps")?;
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

/// Maximum concurrent sidecar GETs in a convergence pass.
///
/// Sized so a *cold* walk (empty catalog, every sidecar genuinely unknown) is
/// not serialised on object-store round-trip latency, while staying well short
/// of turning a background backstop into a load generator. The walk shares its
/// object store with live queries, and D-066 exists precisely because that
/// sharing went wrong.
const SIDECAR_FETCH_CONCURRENCY: usize = 16;

/// A block's identity as carried by its object key, no GET required.
struct BlockKey {
    signal: String,
    date: String,
    writer_id: Uuid,
    uuid: Uuid,
}

/// Parse `<signal>/<yyyy>/<mm>/<dd>/<writer_id>/<block_uuid>.meta.json` — the
/// inverse of [`scry_block::block_path`].
///
/// This is what lets a walk answer "do I already know this block?" without
/// paying a GET: the key carries the UUID *and* every field the cursor
/// bookkeeping needs. The path's `yyyy/mm/dd` is derived from the block's
/// `ts_min_unix_nano` at write time, so it agrees with `date_dir(ts_min)` by
/// construction.
///
/// Strict on purpose: anything that doesn't match the exact shape returns
/// `None` and is then treated as *unknown*, so the walk falls back to fetching
/// it. A parser that guessed could silently skip a real block forever.
fn parse_block_key(loc: &ObjPath) -> Option<BlockKey> {
    let stem = loc.as_ref().strip_suffix(".meta.json")?;
    let parts: Vec<&str> = stem.split('/').collect();
    let &[signal, yyyy, mm, dd, writer_id, uuid] = parts.as_slice() else {
        return None;
    };
    if signal.is_empty() || yyyy.len() != 4 || mm.len() != 2 || dd.len() != 2 {
        return None;
    }
    if !yyyy
        .bytes()
        .chain(mm.bytes())
        .chain(dd.bytes())
        .all(|b| b.is_ascii_digit())
    {
        return None;
    }
    Some(BlockKey {
        signal: signal.to_string(),
        date: format!("{yyyy}-{mm}-{dd}"),
        writer_id: Uuid::parse_str(writer_id).ok()?,
        uuid: Uuid::parse_str(uuid).ok()?,
    })
}

/// Record `uuid` as the pass's high-water mark for its prefix, keeping the max.
fn bump_high(
    high: &mut HashMap<(String, Uuid, String), Uuid>,
    signal: String,
    writer_id: Uuid,
    date: String,
    uuid: Uuid,
) {
    high.entry((signal, writer_id, date))
        .and_modify(|u| {
            if uuid > *u {
                *u = uuid;
            }
        })
        .or_insert(uuid);
}

/// Fetch each meta.json, parse it, `insert_block` (idempotent), and advance
/// the per-prefix cursor to the max UUID seen. Updates `report` in place.
///
/// `known` is the set of block UUIDs the catalog already has a row for. When
/// supplied, any listed object whose key parses to a UUID in that set is
/// **not fetched**: the catalog can learn nothing from a sidecar it already
/// has, and the key alone carries the cursor bookkeeping. `None` disables the
/// filter, which is what the incremental poll wants — it lists from a cursor
/// offset and so sees only a handful of objects, not worth loading a
/// catalog-sized set to filter.
///
/// Failures never abort the pass: a sidecar that won't parse, or a GET that
/// fails for any reason other than `NotFound`, is counted, logged and skipped.
/// A cursor is only ever advanced past a block the catalog actually holds, so
/// a skipped failure is retried on the next pass rather than lost.
async fn fetch_and_apply<C, S>(
    store: &S,
    catalog: &C,
    _bucket: &str,
    locations: Vec<ObjPath>,
    known: Option<&std::collections::HashSet<Uuid>>,
    report: &mut PollReport,
) -> Result<()>
where
    C: CatalogHandle,
    S: ObjectStore + ?Sized,
{
    // Highest UUID seen per (signal, writer_id, date) this pass, so we issue
    // one monotonic cursor advance per prefix at the end.
    let mut high: HashMap<(String, Uuid, String), Uuid> = HashMap::new();
    // Prefixes where something in this pass did *not* make it into the
    // catalog. Their cursor is left alone entirely.
    //
    // Without this, a pass that skips one block and succeeds on a later one in
    // the same prefix advances the cursor *past the gap*: UUIDv7 is monotonic,
    // so `high` becomes the later block and the incremental poll — which lists
    // from the cursor — can never see the skipped one again. Only a full walk
    // would recover it. That is the difference between "retry next pass" and
    // "silently lost until the backstop happens to run".
    let mut poisoned: std::collections::HashSet<(String, Uuid, String)> =
        std::collections::HashSet::new();
    let total = locations.len();
    let mut last_progress = Instant::now();

    // Split the listing into "already in the catalog" and "must be fetched".
    // A skipped block still advances its cursor — it is in the catalog, which
    // is exactly the condition the cursor asserts.
    let mut to_fetch: Vec<ObjPath> = Vec::with_capacity(locations.len());
    for loc in locations {
        match (known, parse_block_key(&loc)) {
            (Some(known), Some(key)) if known.contains(&key.uuid) => {
                report.seen += 1;
                report.skipped += 1;
                bump_high(&mut high, key.signal, key.writer_id, key.date, key.uuid);
            }
            _ => to_fetch.push(loc),
        }
    }

    let mut fetches = futures::stream::iter(to_fetch.into_iter().map(|loc| async move {
        let res = match store.get(&loc).await {
            Ok(r) => r.bytes().await,
            Err(e) => Err(e),
        };
        (loc, res)
    }))
    .buffer_unordered(SIDECAR_FETCH_CONCURRENCY);

    while let Some((loc, res)) = fetches.next().await {
        report.seen += 1;
        let bytes = match res {
            Ok(b) => b,
            // A peer may have deleted the block between list and get. That is
            // the block being *gone*, not us failing to read it, so it leaves
            // no gap and must not poison the prefix.
            Err(object_store::Error::NotFound { .. }) => continue,
            Err(e) => {
                // Anything else is transient as far as we can tell from here.
                // Aborting would discard every cursor advance this pass has
                // earned, which on a large bucket is hours of work thrown away
                // for one flaky GET.
                report.fetch_failed += 1;
                poison(&mut poisoned, &loc);
                tracing::warn!(location = %loc, error = %e, "sidecar fetch failed; continuing pass");
                continue;
            }
        };
        let meta: BlockMeta = match serde_json::from_slice(&bytes) {
            Ok(m) => m,
            Err(e) => {
                report.failed += 1;
                poison(&mut poisoned, &loc);
                tracing::warn!(location = %loc, error = %e, "skipping unparseable meta.json");
                continue;
            }
        };

        let inserted = catalog
            .with(|c| c.insert_block(&meta))
            .context("poll insert_block")?;
        if inserted {
            report.inserted += 1;
        }

        bump_high(
            &mut high,
            meta.signal.clone(),
            meta.writer_id,
            date_dir(meta.ts_min_unix_nano),
            meta.uuid,
        );

        if last_progress.elapsed() >= Duration::from_secs(10) {
            tracing::info!(
                processed = report.seen,
                total,
                inserted = report.inserted,
                skipped = report.skipped,
                failed = report.failed,
                fetch_failed = report.fetch_failed,
                "catalog sidecar fetch progress"
            );
            last_progress = Instant::now();
        }
    }

    for (prefix, uuid) in high {
        if poisoned.contains(&prefix) {
            let (signal, writer_id, date) = &prefix;
            tracing::warn!(
                %signal, %writer_id, %date,
                "holding cursor: a block in this prefix was not applied this pass"
            );
            continue;
        }
        let (signal, writer_id, date) = prefix;
        catalog
            .with(|c| c.advance_cursor(&signal, writer_id, &date, uuid))
            .context("poll advance_cursor")?;
    }

    Ok(())
}

/// Mark `loc`'s prefix as one whose cursor must not move this pass.
///
/// A location whose key doesn't parse has no cursor to hold back — cursors are
/// keyed on `(signal, writer_id, date)`, all three of which come from the key —
/// so there is nothing to poison and nothing to lose.
fn poison(poisoned: &mut std::collections::HashSet<(String, Uuid, String)>, loc: &ObjPath) {
    if let Some(key) = parse_block_key(loc) {
        poisoned.insert((key.signal, key.writer_id, key.date));
    }
}
