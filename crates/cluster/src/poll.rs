//! Bucket polling: the source-of-truth backstop behind pub/sub.
//!
//! Pub/sub (the convergence consumer) is a low-latency hint that can drop
//! events. Polling re-derives the catalog from the bucket — the actual source
//! of truth — in two modes:
//!
//! - [`poll_once`] — **incremental**: for each known poll cursor
//!   `(signal, writer_id, date)`, list only the objects whose UUIDv7 time is
//!   after `min(cursor time, now − lookback)` (`list_with_offset`). The
//!   look-back exists because blocks do not commit in UUID order: concurrent
//!   uploads finish out of order, a retried upload keeps the UUID it was
//!   encoded with, and a compaction output's UUID is minted when its merge
//!   *starts*. Listing only past the high-water UUID would skip every such
//!   block until the next full walk. Listed keys the catalog already has are
//!   filtered by primary-key probe, so a converged poll costs one LIST per
//!   prefix and no GETs; a prefix whose cursor is older than the window (a
//!   finished day) still lists only past its cursor. This catches blocks
//!   dropped by pub/sub for prefixes the catalog already tracks.
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

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use futures::StreamExt;
use object_store::{path::Path as ObjPath, ObjectStore, ObjectStoreExt};
use scry_block::BlockMeta;
use scry_catalog::{date_dir, CatalogHandle};
use scry_storage_layout::is_reserved_control_key;
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

/// Default [`poll_once`] look-back: how late, relative to its UUIDv7
/// timestamp, a block may commit and still be found by the next incremental
/// poll rather than the next full walk.
///
/// Fifteen minutes covers an L0 upload that exhausts the object-store client's
/// retry budget (three minutes by default) several times over. Compaction
/// merges can run longer; a merge whose commit lags its start by more than
/// this relies on its `Superseded` event and, failing that, the full walk.
pub const DEFAULT_POLL_LOOKBACK: Duration = Duration::from_secs(15 * 60);

/// Incrementally poll every known cursor for blocks committed since the last
/// pass. Cursors are discovered by the convergence consumer (on `Created`)
/// and by [`full_walk`]; this only backstops known prefixes.
///
/// Each prefix is listed from `min(cursor time, now − lookback)` (see the
/// module docs); a cursor that is not UUIDv7 — only a catalog written before
/// [`scry_catalog::Catalog::advance_cursor`] ignored WAL-recovery UUIDs can
/// hold one — lists its whole prefix once, after which the cursor is v7.
pub async fn poll_once<C, S>(
    store: &S,
    catalog: &C,
    bucket: &str,
    lookback: Duration,
) -> Result<PollReport>
where
    C: CatalogHandle,
    S: ObjectStore + ?Sized,
{
    let heads = catalog
        .with(|c| c.list_cursor_heads())
        .context("list cursors")?;
    let mut report = PollReport {
        cursors: heads.len(),
        ..Default::default()
    };
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let window_start_ms = now_ms.saturating_sub(lookback.as_millis() as u64);
    // Reused across prefixes: the listed keys the catalog already holds.
    let mut known: HashSet<Uuid> = HashSet::new();

    for head in heads {
        let prefix = partition_prefix(&head.signal, &head.date, head.writer_id);
        let prefix_path = ObjPath::from(prefix.as_str());
        let locations = match lookback_offset(&prefix, head.highest, window_start_ms) {
            Some(offset) => {
                collect_meta_locations_with_offset(store, &prefix_path, &offset).await?
            }
            None => collect_meta_locations(store, Some(&prefix_path)).await?,
        };

        // Re-listing the window returns mostly blocks the catalog already
        // has. Probe each by primary key — one lock, no allocation per key —
        // rather than loading a catalog-sized UUID set every few seconds.
        known.clear();
        catalog
            .with(|c| -> Result<()> {
                for loc in &locations {
                    if let Some(key) = parse_block_key(loc) {
                        if c.has_block(key.uuid)? {
                            known.insert(key.uuid);
                        }
                    }
                }
                Ok(())
            })
            .context("probe listed blocks against the catalog")?;
        fetch_and_apply(store, catalog, bucket, locations, Some(&known), &mut report).await?;
    }

    Ok(report)
}

/// The `list_with_offset` start for one prefix: every key whose UUIDv7 time
/// is at or after `min(cursor time, window_start_ms)` sorts after it.
///
/// `None` when the cursor is not UUIDv7 and so carries no usable time: the
/// caller lists the whole prefix.
fn lookback_offset(prefix: &str, cursor: Uuid, window_start_ms: u64) -> Option<ObjPath> {
    if cursor.get_version_num() != 7 {
        return None;
    }
    let (secs, nanos) = cursor.get_timestamp()?.to_unix();
    let cursor_ms = secs * 1_000 + u64::from(nanos) / 1_000_000;
    let start_ms = cursor_ms.min(window_start_ms);
    // The smallest UUID with a 48-bit timestamp of `start_ms`: every v7 UUID
    // minted at or after that millisecond sorts after it, as hex text too.
    let mut bytes = [0u8; 16];
    bytes[..6].copy_from_slice(&start_ms.to_be_bytes()[2..]);
    Some(ObjPath::from(format!(
        "{prefix}{}",
        Uuid::from_bytes(bytes)
    )))
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
    // The caller compacts on the strength of this reconcile. A committed
    // sidecar that could not be fetched may be exactly the prior holder's
    // output whose inputs now look live; merging them again would create a
    // second live copy of their rows. An unparseable sidecar is different:
    // it is invisible to every reader, so its inputs are the only live copy.
    anyhow::ensure!(
        report.fetch_failed == 0,
        "partition reconcile could not fetch {} committed sidecar(s); refusing to compact \
         {signal}/{date} against an incomplete catalog",
        report.fetch_failed
    );
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
        // Classify before suffix checks: control products may legitimately use
        // `*.meta.json`, but they are never telemetry block sidecars.
        if is_reserved_control_key(loc) {
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
        if is_reserved_control_key(loc) {
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
/// Borrowed from the key: parsing a listed key allocates nothing.
struct BlockKey<'a> {
    /// `<signal>/<yyyy>/<mm>/<dd>/<writer_id>/` — the object prefix, and so
    /// the poll cursor, this block belongs to.
    prefix: &'a str,
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
fn parse_block_key(loc: &ObjPath) -> Option<BlockKey<'_>> {
    let stem = loc.as_ref().strip_suffix(".meta.json")?;
    let (dir, uuid) = stem.rsplit_once('/')?;
    let prefix = &stem[..=dir.len()];
    parse_prefix(prefix)?;
    Some(BlockKey {
        prefix,
        uuid: Uuid::parse_str(uuid).ok()?,
    })
}

/// The parts of a `<signal>/<yyyy>/<mm>/<dd>/<writer_id>/` prefix.
struct PrefixParts<'a> {
    signal: &'a str,
    yyyy: &'a str,
    mm: &'a str,
    dd: &'a str,
    writer_id: Uuid,
}

/// Strictly parse a partition prefix, trailing `/` included.
fn parse_prefix(prefix: &str) -> Option<PrefixParts<'_>> {
    let mut parts = prefix.strip_suffix('/')?.split('/');
    let (signal, yyyy, mm, dd, writer_id) = (
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
    );
    if parts.next().is_some() {
        return None;
    }
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
    Some(PrefixParts {
        signal,
        yyyy,
        mm,
        dd,
        writer_id: Uuid::parse_str(writer_id).ok()?,
    })
}

/// Record `uuid` as the pass's high-water mark for `prefix`, keeping the max.
///
/// Only UUIDv7 counts, matching `Catalog::advance_cursor`: a WAL-recovery
/// block's v4-shaped UUID usually sorts above every v7 in its prefix, and as
/// the pass maximum it would hide the real v7 head from the cursor advance.
/// Allocates only the first time a prefix is seen in a pass.
fn bump_high(high: &mut HashMap<String, Uuid>, prefix: &str, uuid: Uuid) {
    if uuid.get_version_num() != 7 {
        return;
    }
    match high.get_mut(prefix) {
        Some(current) if uuid > *current => *current = uuid,
        Some(_) => {}
        None => {
            high.insert(prefix.to_owned(), uuid);
        }
    }
}

/// Fetch each meta.json, parse it, `insert_block` (idempotent), and advance
/// the per-prefix cursor to the max UUID seen. Updates `report` in place.
///
/// `known` is the set of block UUIDs the catalog already has a row for. When
/// supplied, any listed object whose key parses to a UUID in that set is
/// **not fetched**: the catalog can learn nothing from a sidecar it already
/// has, and the key alone carries the cursor bookkeeping. The full walk and
/// partition reconcile pass the whole catalog's set; the incremental poll
/// passes the subset of its listing that it probed as known. `None` fetches
/// everything.
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
    known: Option<&HashSet<Uuid>>,
    report: &mut PollReport,
) -> Result<()>
where
    C: CatalogHandle,
    S: ObjectStore + ?Sized,
{
    // Highest UUID seen per `<signal>/<yyyy>/<mm>/<dd>/<writer_id>/` prefix
    // this pass, so we issue one monotonic cursor advance per prefix at the
    // end. Keyed by the prefix text so a listed key is matched without
    // allocating; an owned key is made once per prefix.
    let mut high: HashMap<String, Uuid> = HashMap::new();
    // Prefixes where something in this pass did *not* make it into the
    // catalog. Their cursor is left alone entirely.
    //
    // Without this, a pass that skips one block and succeeds on a later one in
    // the same prefix advances the cursor *past the gap*: UUIDv7 is monotonic,
    // so `high` becomes the later block and the incremental poll — which lists
    // from near the cursor — may never see the skipped one again. Only a full
    // walk would recover it. That is the difference between "retry next pass"
    // and "silently lost until the backstop happens to run".
    let mut poisoned: HashSet<String> = HashSet::new();
    let total = locations.len();
    let mut last_progress = Instant::now();

    // Split the listing into "already in the catalog" and "must be fetched".
    // A skipped block still advances its cursor — it is in the catalog, which
    // is exactly the condition the cursor asserts.
    let mut to_fetch: Vec<ObjPath> = Vec::with_capacity(locations.len());
    for loc in locations {
        let skip = match (known, parse_block_key(&loc)) {
            (Some(known), Some(key)) if known.contains(&key.uuid) => {
                bump_high(&mut high, key.prefix, key.uuid);
                true
            }
            _ => false,
        };
        if skip {
            report.seen += 1;
            report.skipped += 1;
        } else {
            to_fetch.push(loc);
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
                if matches!(
                    e,
                    object_store::Error::PermissionDenied { .. }
                        | object_store::Error::Unauthenticated { .. }
                ) {
                    // Not transient: it recurs every pass until an operator
                    // fixes the credentials, so say so rather than letting it
                    // read like a flaky GET.
                    tracing::error!(
                        location = %loc,
                        error = %e,
                        "object store denied a sidecar GET the listing returned; the \
                         credentials need s3:GetObject on the bucket — this block stays \
                         invisible until they do"
                    );
                } else {
                    tracing::warn!(location = %loc, error = %e, "sidecar fetch failed; continuing pass");
                }
                continue;
            }
        };
        let mut meta: BlockMeta = match serde_json::from_slice(&bytes) {
            Ok(m) => m,
            Err(e) => {
                report.failed += 1;
                poison(&mut poisoned, &loc);
                tracing::warn!(location = %loc, error = %e, "skipping unparseable meta.json");
                continue;
            }
        };
        meta.meta_json_size_bytes = Some(bytes.len() as u64);

        let inserted = catalog
            .with(|c| c.insert_block(&meta))
            .context("poll insert_block")?;
        if inserted {
            report.inserted += 1;
        }

        // The cursor key comes from the sidecar itself, which agrees with
        // the object key by construction. This path already paid a GET, so
        // building the prefix text here is not a per-listed-key cost.
        bump_high(
            &mut high,
            &partition_prefix(
                &meta.signal,
                &date_dir(meta.ts_min_unix_nano),
                meta.writer_id,
            ),
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
            tracing::warn!(
                %prefix,
                "holding cursor: a block in this prefix was not applied this pass"
            );
            continue;
        }
        // Every prefix in `high` came from a parsed key or from
        // `partition_prefix`, so it parses; a sidecar whose signal contained
        // a `/` would not, and has no cursor it could name.
        let Some(parts) = parse_prefix(&prefix) else {
            tracing::warn!(%prefix, "not advancing a cursor for an unparseable prefix");
            continue;
        };
        let date = format!("{}-{}-{}", parts.yyyy, parts.mm, parts.dd);
        catalog
            .with(|c| c.advance_cursor(parts.signal, parts.writer_id, &date, uuid))
            .context("poll advance_cursor")?;
    }

    Ok(())
}

/// Mark `loc`'s prefix as one whose cursor must not move this pass.
///
/// A location whose key doesn't parse has no cursor to hold back — cursors are
/// keyed on `(signal, writer_id, date)`, all three of which come from the key —
/// so there is nothing to poison and nothing to lose.
fn poison(poisoned: &mut HashSet<String>, loc: &ObjPath) {
    if let Some(key) = parse_block_key(loc) {
        if !poisoned.contains(key.prefix) {
            poisoned.insert(key.prefix.to_owned());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PREFIX: &str = "logs/2026/05/30/0190a000-0000-7000-8000-000000000001/";

    fn v7_at_ms(ms: u64) -> Uuid {
        Uuid::new_v7(uuid::Timestamp::from_unix(
            uuid::NoContext,
            ms / 1_000,
            ((ms % 1_000) * 1_000_000) as u32,
        ))
    }

    fn key(uuid: Uuid) -> String {
        format!("{PREFIX}{uuid}.meta.json")
    }

    #[test]
    fn offset_starts_at_the_earlier_of_cursor_and_window() {
        let cursor = v7_at_ms(1_000_000);
        // Window start before the cursor: list from the window.
        let from_window = lookback_offset(PREFIX, cursor, 900_000).unwrap();
        assert!(key(v7_at_ms(900_000)).as_str() > from_window.as_ref());
        assert!(key(v7_at_ms(899_999)).as_str() < from_window.as_ref());
        // Window start after the cursor (a finished prefix): list from the
        // cursor's own millisecond, never later.
        let from_cursor = lookback_offset(PREFIX, cursor, 2_000_000).unwrap();
        assert!(key(cursor).as_str() > from_cursor.as_ref());
        assert!(key(v7_at_ms(1_000_001)).as_str() > from_cursor.as_ref());
        assert!(key(v7_at_ms(999_999)).as_str() < from_cursor.as_ref());
    }

    #[test]
    fn a_non_v7_cursor_lists_the_whole_prefix() {
        let v4 = Uuid::parse_str("ffffffff-ffff-4fff-bfff-ffffffffffff").unwrap();
        assert!(lookback_offset(PREFIX, v4, 0).is_none());
    }

    #[test]
    fn block_keys_parse_strictly_and_borrow_their_prefix() {
        let uuid = v7_at_ms(5);
        let loc = ObjPath::from(key(uuid));
        let parsed = parse_block_key(&loc).unwrap();
        assert_eq!(parsed.prefix, PREFIX);
        assert_eq!(parsed.uuid, uuid);
        let parts = parse_prefix(parsed.prefix).unwrap();
        assert_eq!(
            (parts.signal, parts.yyyy, parts.mm, parts.dd),
            ("logs", "2026", "05", "30")
        );

        for bad in [
            format!("logs/2026/05/{uuid}.meta.json"),
            format!("logs/2026/5/30/{uuid}/{uuid}.meta.json"),
            format!("logs/2026/05/3x/{uuid}/{uuid}.meta.json"),
            format!("a/logs/2026/05/30/{uuid}/{uuid}.meta.json"),
            format!("{PREFIX}not-a-uuid.meta.json"),
            format!("{PREFIX}{uuid}.parquet"),
        ] {
            assert!(
                parse_block_key(&ObjPath::from(bad.as_str())).is_none(),
                "{bad}"
            );
        }
    }

    #[test]
    fn only_v7_uuids_raise_the_high_water_mark() {
        let mut high = HashMap::new();
        let low = v7_at_ms(1);
        let v4 = Uuid::parse_str("ffffffff-ffff-4fff-bfff-ffffffffffff").unwrap();
        bump_high(&mut high, PREFIX, v4);
        assert!(high.is_empty());
        bump_high(&mut high, PREFIX, low);
        bump_high(&mut high, PREFIX, v4);
        assert_eq!(high.get(PREFIX), Some(&low));
        let later = v7_at_ms(2);
        bump_high(&mut high, PREFIX, later);
        bump_high(&mut high, PREFIX, low);
        assert_eq!(high.get(PREFIX), Some(&later));
    }
}
