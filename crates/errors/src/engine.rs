//! Bounded server-stamped logs schema-v3 occurrence projection and reconciliation.
//!
//! # Threading
//!
//! `reconcile_once` is async but never runs SQLite, Parquet encode/decode, or
//! OCC1 fingerprinting on the async worker threads: every such step runs in
//! `spawn_blocking` (the SQLite connection moves into the blocking task and
//! back). A host that runs other periodic work on the same runtime — ingestd's
//! compaction and retention ticks — is therefore not stalled by a long pass.
//!
//! # Fencing
//!
//! The caller passes the [`Fence`] of the lease that authorizes it to write
//! (`AlwaysValid` for a single writer). It is checked immediately before each
//! durable object publication and before each SQLite commit, so an instance that
//! lost the lease mid-pass stops before its next write.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{bail, ensure, Context, Result};
use arrow::array::{Array, BinaryArray, UInt16Array, UInt64Array};
use bytes::Bytes;
use futures::StreamExt;
use object_store::{path::Path as ObjectPath, GetOptions, ObjectStore, ObjectStoreExt};
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ProjectionMask};
use scry_block::{block_path, Fence};
use scry_catalog::{Catalog, CatalogEntry};

use crate::{
    extract,
    fingerprint::{fingerprint_from_decoded, FINGERPRINT_VERSION},
    issue::derive_issue_id,
    occ1,
    projection::{
        decode_occurrences, encode_occurrences, occurrence_keys, OccurrenceCommit,
        ProjectionLimits, ProjectionRow, OCCURRENCE_PROJECTION_PREFIX,
    },
    publication::{publish_occurrence_projection, PublicationOutcome},
    sqlite::{
        ErrorsDb, FoldReport, GroupedOccurrence, GroupingReport, ReconcileCursor, SqliteError,
        UngroupedOccurrence,
    },
    DeploymentId, Error as ExtractionError, Limits as ExtractionLimits, Occurrence, Scratch,
};
use scry_proto::{
    constants::LOGS_RAW_VERSION_V2, validate_logs_record_version, LogsV2DecodeLimits,
};
use scry_storage_layout::is_reserved_control_key;

pub const DEFAULT_EXTRACTOR_GENERATION: &str = "occurrence-v1";
/// Grouping generation of the current fingerprint algorithm. It must change
/// whenever [`FINGERPRINT_VERSION`] changes; see `fingerprint.rs`.
pub const DEFAULT_GROUPING_GENERATION: &str = "fp-v2";
const _: () = assert!(
    FINGERPRINT_VERSION == 2,
    "update DEFAULT_GROUPING_GENERATION"
);

/// Controls how the reconciliation engine accesses the block catalog.
pub enum CatalogMode<'a> {
    /// Standalone: open a read-write catalog at `catalog_path` and reconcile
    /// its raw metadata from S3 before selecting source blocks. Used by the
    /// standalone `scry errors` role which owns its own catalog.
    Owned {
        catalog_path: &'a Path,
        bucket: &'a str,
    },
    /// Embedded in another role (e.g. `scry ingest`): the catalog is already
    /// converged by the host's convergence loop. Open it read-only and skip
    /// `reconcile_raw_meta_page`. The bucket identity is still needed for
    /// source-block validation.
    Converged {
        catalog_path: &'a Path,
        bucket: &'a str,
    },
}
const OCCURRENCE_CURSOR_PARTITION: &str = "occurrence-commits-v1";
const RAW_META_CURSOR_PARTITION: &str = "raw-logs-meta-v1";
const RAW_LOGS_CURSOR_PARTITION: &str = "raw-logs";
const MAX_COMMIT_BYTES: usize = 16 * 1024;
const MAX_RAW_META_BYTES: usize = 1024 * 1024;
const RAW_OBJECTS_PER_BLOCK: usize = 4;

#[derive(Debug, Clone)]
pub struct ReconcileConfig {
    pub extractor_generation: String,
    pub grouping_generation: String,
    pub max_blocks: usize,
    pub max_source_rows: usize,
    pub max_source_bytes: usize,
    pub max_decoded_raw_bytes: usize,
    pub source_batch_rows: usize,
    pub projection_limits: ProjectionLimits,
    pub extraction_limits: ExtractionLimits,
    /// Occurrences grouped per SQLite transaction.
    pub grouping_page_rows: usize,
    /// Upper bound on occurrences grouped in one pass (a pass stops after the
    /// page that reaches it).
    pub grouping_max_rows_per_pass: usize,
    /// Wall-clock budget for grouping in one pass, checked between pages.
    pub grouping_time_budget: Duration,
}

impl Default for ReconcileConfig {
    fn default() -> Self {
        Self {
            extractor_generation: DEFAULT_EXTRACTOR_GENERATION.to_owned(),
            grouping_generation: DEFAULT_GROUPING_GENERATION.to_owned(),
            max_blocks: 128,
            max_source_rows: 65_536,
            max_source_bytes: 256 * 1024 * 1024,
            max_decoded_raw_bytes: 128 * 1024 * 1024,
            source_batch_rows: 1_024,
            projection_limits: ProjectionLimits::default(),
            extraction_limits: ExtractionLimits::default(),
            grouping_page_rows: 1_024,
            grouping_max_rows_per_pass: 262_144,
            grouping_time_budget: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EngineReport {
    pub candidate_blocks: usize,
    pub processed_blocks: usize,
    /// Candidate source blocks skipped because `source_coverage` already records
    /// a projection of the configured extractor generation.
    pub sources_already_covered: usize,
    pub occurrence_rows: usize,
    pub skipped_records: usize,
    pub raw_meta_quarantined: usize,
    pub source_blocks_quarantined: usize,
    pub occurrence_commits_quarantined: usize,
    /// Listed commit markers skipped because they are already folded.
    pub commits_already_folded: usize,
    /// Projection commits folded into the database by this pass, whether
    /// published by this pass or by another writer.
    pub commits_folded: usize,
    pub publications_created: usize,
    pub publications_existing: usize,
    pub fold: FoldReport,
    pub grouping: GroupingReport,
}

impl EngineReport {
    fn absorb_fold(&mut self, fold: FoldReport) {
        self.commits_folded += 1;
        self.fold.inserted += fold.inserted;
        self.fold.exact_duplicates += fold.exact_duplicates;
        self.fold.collisions += fold.collisions;
    }

    /// Whether the pass changed occurrence or issue state. Snapshot producers
    /// skip uploads while this stays false; reconcile cursors alone are not
    /// counted, since a restored copy re-derives them by listing.
    pub fn changed_state(&self) -> bool {
        self.commits_folded > 0 || self.grouping.scanned > 0
    }
}

/// The caller's lease was lost; the pass stopped before its next durable write.
#[derive(Debug, thiserror::Error)]
#[error("errors lease lost: {0}")]
pub struct LeaseLost(String);

fn check_lease(fence: &dyn Fence) -> Result<()> {
    fence
        .check()
        .map_err(|error| anyhow::Error::new(LeaseLost(format!("{error:#}"))))
}

struct ExtractedRow {
    occurrence: Occurrence,
    occurred_at: u64,
    observed_at: Option<u64>,
    received_at: u64,
    ordinal: u64,
}

/// An [`ErrorsDb`] whose every operation runs on the blocking thread pool.
struct BlockingDb {
    db: Option<ErrorsDb>,
}

impl BlockingDb {
    async fn open(path: &Path, deployment_id: DeploymentId, max_rows: usize) -> Result<Self> {
        let path = path.to_path_buf();
        let db = tokio::task::spawn_blocking(move || -> Result<ErrorsDb> {
            let mut db = ErrorsDb::open(&path, *deployment_id.as_bytes())
                .with_context(|| format!("opening errors database {}", path.display()))?;
            db.set_max_rows_per_transaction(max_rows);
            Ok(db)
        })
        .await
        .context("errors database open task panicked")??;
        Ok(Self { db: Some(db) })
    }

    async fn run<T, F>(&mut self, f: F) -> Result<T>
    where
        F: FnOnce(&mut ErrorsDb) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let mut db = self
            .db
            .take()
            .context("errors database unavailable after a failed blocking task")?;
        let (db, result) = tokio::task::spawn_blocking(move || {
            let result = f(&mut db);
            (db, result)
        })
        .await
        .context("errors database task panicked")?;
        self.db = Some(db);
        result
    }

    async fn cursor(&mut self, partition: &'static str) -> Result<Option<ReconcileCursor>> {
        self.run(move |db| Ok(db.reconcile_cursor(partition)?))
            .await
    }

    async fn set_cursor(&mut self, cursor: ReconcileCursor) -> Result<()> {
        self.run(move |db| Ok(db.set_reconcile_cursor(&cursor)?))
            .await
    }
}

/// Reconciles a bounded page of live server-stamped logs catalog entries in
/// deterministic order, after first folding a bounded page of committed occurrence
/// objects, then groups a budgeted amount of the ungrouped backlog.
///
/// Processing is intentionally serial. This is the single-writer implementation and
/// therefore has an explicit concurrency bound of one source block.
pub async fn reconcile_once(
    catalog_mode: CatalogMode<'_>,
    store: Arc<dyn ObjectStore>,
    errors_db_path: &Path,
    deployment_id: DeploymentId,
    config: &ReconcileConfig,
    fence: Arc<dyn Fence>,
) -> Result<EngineReport> {
    let (catalog_path, bucket, owned) = match &catalog_mode {
        CatalogMode::Owned {
            catalog_path,
            bucket,
        } => (*catalog_path, *bucket, true),
        CatalogMode::Converged {
            catalog_path,
            bucket,
        } => (*catalog_path, *bucket, false),
    };
    validate_config(config)?;
    ensure!(
        !bucket.is_empty(),
        "source catalog bucket identity must not be empty"
    );
    let mut db = BlockingDb::open(
        errors_db_path,
        deployment_id,
        config.projection_limits.max_rows,
    )
    .await?;
    let mut report = EngineReport::default();

    // Committed occurrence objects are independently authoritative. Do this before
    // consulting the raw catalog so a lost catalog (and lost raw blocks) cannot stop
    // rebuilding the disposable SQLite projection.
    fold_occurrence_page(store.as_ref(), config, &mut db, &fence, &mut report).await?;

    // In Owned mode, reconcile a bounded page of raw metadata from S3 into
    // the catalog before selecting source blocks. In Converged mode (e.g.
    // embedded in ingestd), the host's convergence loop already keeps the
    // catalog current, so we skip this step.
    if owned {
        reconcile_raw_meta_page(
            store.as_ref(),
            config,
            &mut db,
            catalog_path,
            bucket,
            &mut report,
        )
        .await?;
    }

    // Bounded, cursor-filtered selection: the SQL query pushes signal, schema version,
    // ordering, cursor, and LIMIT into SQLite so memory and startup work are O(max_blocks)
    // rather than O(catalog). When the cursor passes the last entry the page is empty
    // and we wrap to the beginning.
    //
    // list_source_blocks is live-only. Resolve only this bounded page as a defensive
    // lineage check so a stale catalog can never make this worker guess a terminal. An
    // immutable object that cannot be processed is quarantined for this cycle; moving
    // the cursor prevents it from wedging later keys, while cyclic wrap makes it
    // retryable.
    let cursor = db.cursor(RAW_LOGS_CURSOR_PARTITION).await?;
    let (candidates, terminal_consistent) = select_source_page(
        catalog_path.to_path_buf(),
        bucket.to_owned(),
        owned,
        cursor.map(|c| c.highest_key),
        config.max_blocks,
    )
    .await?;
    ensure!(
        candidates.iter().all(|entry| entry.bucket == bucket),
        "source catalog contains a live logs block for a different bucket"
    );
    report.candidate_blocks = candidates.len();

    // Blocks already covered for this extractor generation are skipped without any
    // object-store request; otherwise every wrap would re-read and re-verify every
    // live block forever.
    let covered: Vec<bool> = {
        let ids: Vec<[u8; 16]> = candidates.iter().map(|e| *e.meta.uuid.as_bytes()).collect();
        let generation = config.extractor_generation.clone();
        db.run(move |db| {
            ids.iter()
                .map(|id| Ok(db.is_source_covered(id, &generation)?))
                .collect()
        })
        .await?
    };

    for ((entry, &consistent), &already_covered) in
        candidates.iter().zip(&terminal_consistent).zip(&covered)
    {
        if already_covered {
            report.sources_already_covered += 1;
        } else {
            let result = if consistent {
                process_block(
                    entry,
                    store.clone(),
                    deployment_id,
                    config,
                    &mut db,
                    &fence,
                    &mut report,
                )
                .await
            } else {
                Err(anyhow::anyhow!("inconsistent terminal resolution"))
            };
            if let Err(error) = result {
                if is_pass_fatal(&error) {
                    return Err(error);
                }
                report.source_blocks_quarantined += 1;
                tracing::warn!(
                    object_kind = "raw_logs_block",
                    block_uuid = %entry.meta.uuid,
                    error = %format!("{error:#}"),
                    "quarantining immutable object for this reconciliation cycle"
                );
            }
        }
        db.set_cursor(ReconcileCursor {
            partition: RAW_LOGS_CURSOR_PARTITION.to_owned(),
            highest_key: source_entry_key(entry),
            updated_at_unix_nano: entry.meta.ts_max_unix_nano,
        })
        .await?;
    }

    // Grouping phase: compute fingerprints for ungrouped occurrences and fold
    // into issue summaries. Runs after occurrence extraction so newly committed
    // occurrences are available in `errors.sqlite`.
    group_backlog(&mut db, deployment_id, config, &fence, &mut report).await?;

    Ok(report)
}

/// Resolve one bounded page of candidate source blocks and their terminal
/// consistency on the blocking pool (the catalog is synchronous SQLite).
async fn select_source_page(
    catalog_path: PathBuf,
    bucket: String,
    owned: bool,
    after: Option<String>,
    max_blocks: usize,
) -> Result<(Vec<CatalogEntry>, Vec<bool>)> {
    tokio::task::spawn_blocking(move || -> Result<(Vec<CatalogEntry>, Vec<bool>)> {
        let catalog = if owned {
            Catalog::open(&catalog_path, &bucket).with_context(|| {
                format!("opening owned source catalog {}", catalog_path.display())
            })?
        } else {
            Catalog::open_read_only(&catalog_path).with_context(|| {
                format!(
                    "opening converged source catalog {}",
                    catalog_path.display()
                )
            })?
        };
        let mut candidates = catalog
            .list_source_blocks("logs", 3, after.as_deref(), max_blocks)
            .context("listing live logs-v3 source blocks")?;
        if candidates.is_empty() && after.is_some() {
            candidates = catalog
                .list_source_blocks("logs", 3, None, max_blocks)
                .context("listing live logs-v3 source blocks (wrap)")?;
        }
        let consistent = candidates
            .iter()
            .map(|entry| {
                catalog
                    .resolve_terminal(entry.meta.uuid)
                    .map(|res| matches!(res, scry_catalog::TerminalResolution::Unique(uuid) if uuid == entry.meta.uuid))
                    .unwrap_or(false)
            })
            .collect();
        Ok((candidates, consistent))
    })
    .await
    .context("source catalog task panicked")?
}

fn source_entry_key(entry: &CatalogEntry) -> String {
    format!("{}/{}", entry.date, entry.meta.uuid)
}

/// Rebuild a bounded portion of the errors role's raw-input catalog from committed
/// logs sidecars. Restricting the listing to `logs/` avoids unrelated signals; the
/// shared storage-layout classifier excludes control keys, and catalog insertion
/// retains the established liveness and lineage semantics.
///
/// Split into two phases — async listing/fetch then blocking catalog insertion.
async fn reconcile_raw_meta_page(
    store: &dyn ObjectStore,
    config: &ReconcileConfig,
    db: &mut BlockingDb,
    catalog_path: &Path,
    bucket: &str,
    report: &mut EngineReport,
) -> Result<()> {
    let prefix = ObjectPath::from("logs/");
    let cursor = db.cursor(RAW_META_CURSOR_PARTITION).await?;
    let offset = cursor
        .as_ref()
        .filter(|cursor| !cursor.highest_key.is_empty())
        .map(|cursor| ObjectPath::from(cursor.highest_key.as_str()));
    let mut listing = match offset.as_ref() {
        Some(offset) => store.list_with_offset(Some(&prefix), offset),
        None => store.list(Some(&prefix)),
    };
    let max_list_objects = config.max_blocks.saturating_mul(RAW_OBJECTS_PER_BLOCK);
    let mut scanned = 0usize;
    // Phase 1 (async): list objects and fetch/validate metadata into owned structs.
    let mut validated_metas: Vec<scry_block::BlockMeta> = Vec::new();
    let mut last_location = None;
    let mut exhausted = false;

    while validated_metas.len() < config.max_blocks && scanned < max_list_objects {
        let Some(item) = listing.next().await else {
            exhausted = true;
            break;
        };
        let object = item.context("listing raw logs metadata")?;
        scanned += 1;
        let key = object.location.as_ref();
        if is_reserved_control_key(key) {
            last_location = Some(object.location);
            continue;
        }
        if !key.ends_with(".meta.json") {
            last_location = Some(object.location);
            continue;
        }
        if object.size > MAX_RAW_META_BYTES as u64 {
            report.raw_meta_quarantined += 1;
            tracing::warn!(path = %key, size = object.size, "quarantining oversized raw logs sidecar");
            last_location = Some(object.location);
            continue;
        }
        let bytes =
            match read_bounded_object(store, key, MAX_RAW_META_BYTES, Some(object.size)).await {
                Ok(bytes) => bytes,
                Err(error) => {
                    if is_pass_fatal(&error) {
                        return Err(error);
                    }
                    report.raw_meta_quarantined += 1;
                    tracing::warn!(path = %key, "quarantining unreadable raw logs sidecar");
                    last_location = Some(object.location);
                    continue;
                }
            };
        let meta: scry_block::BlockMeta = match serde_json::from_slice(&bytes) {
            Ok(meta) => meta,
            Err(_) => {
                report.raw_meta_quarantined += 1;
                tracing::warn!(path = %key, "quarantining malformed raw logs sidecar");
                last_location = Some(object.location);
                continue;
            }
        };
        if meta.signal != "logs" {
            report.raw_meta_quarantined += 1;
            tracing::warn!(path = %key, "quarantining mislabeled raw logs sidecar");
            last_location = Some(object.location);
            continue;
        }
        let expected_key = block_path(
            "logs",
            meta.ts_min_unix_nano,
            meta.writer_id,
            meta.uuid,
            "meta.json",
        );
        if key != expected_key {
            report.raw_meta_quarantined += 1;
            tracing::warn!(path = %key, "quarantining raw logs sidecar whose metadata identity does not match its key");
            last_location = Some(object.location);
            continue;
        }
        validated_metas.push(meta);
        last_location = Some(object.location);
    }

    // Phase 2 (blocking): open the catalog and insert validated metadata.
    let catalog_path = catalog_path.to_path_buf();
    let bucket = bucket.to_owned();
    let quarantined = tokio::task::spawn_blocking(move || -> Result<usize> {
        let catalog = Catalog::open(&catalog_path, &bucket).with_context(|| {
            format!(
                "opening owned source catalog {} for raw-meta insertion",
                catalog_path.display()
            )
        })?;
        let mut quarantined = 0;
        for meta in &validated_metas {
            if catalog.insert_block(meta).is_err() {
                quarantined += 1;
                tracing::warn!(
                    block_uuid = %meta.uuid,
                    "quarantining invalid raw logs sidecar metadata"
                );
            }
        }
        Ok(quarantined)
    })
    .await
    .context("raw metadata catalog task panicked")??;
    report.raw_meta_quarantined += quarantined;

    if let Some(last_location) = last_location {
        db.set_cursor(ReconcileCursor {
            partition: RAW_META_CURSOR_PARTITION.to_owned(),
            highest_key: last_location.to_string(),
            updated_at_unix_nano: 0,
        })
        .await?;
    }
    if exhausted && offset.is_some() {
        db.set_cursor(ReconcileCursor {
            partition: RAW_META_CURSOR_PARTITION.to_owned(),
            highest_key: String::new(),
            updated_at_unix_nano: 0,
        })
        .await?;
    }
    Ok(())
}

async fn fold_occurrence_page(
    store: &dyn ObjectStore,
    config: &ReconcileConfig,
    db: &mut BlockingDb,
    fence: &Arc<dyn Fence>,
    report: &mut EngineReport,
) -> Result<()> {
    let prefix = ObjectPath::from(format!("{OCCURRENCE_PROJECTION_PREFIX}/"));
    let cursor = db.cursor(OCCURRENCE_CURSOR_PARTITION).await?;
    let offset = cursor
        .as_ref()
        .filter(|cursor| !cursor.highest_key.is_empty())
        .map(|cursor| ObjectPath::from(cursor.highest_key.as_str()));
    let mut listing = match offset.as_ref() {
        Some(offset) => store.list_with_offset(Some(&prefix), offset),
        None => store.list(Some(&prefix)),
    };
    let max_list_objects = config.max_blocks.saturating_mul(2);
    let mut keys = Vec::with_capacity(config.max_blocks);
    let mut scanned = 0usize;
    let mut last_location = None;
    let mut exhausted = false;
    while keys.len() < config.max_blocks && scanned < max_list_objects {
        let Some(item) = listing.next().await else {
            exhausted = true;
            break;
        };
        let meta = item.context("listing committed occurrence projections")?;
        scanned += 1;
        last_location = Some(meta.location.clone());
        if meta.location.as_ref().ends_with(".commit.json") {
            keys.push(meta.location.to_string());
        }
    }
    // ObjectStore promises ordered listings, but explicit sorting keeps folding order
    // deterministic across implementations and documents the SQLite contract.
    keys.sort();
    let listed_commits = keys.len();
    // A commit already recorded in `projection_commits` was verified and folded
    // atomically with its rows; skip it before any GET.
    let keys: Vec<String> = db
        .run(move |db| {
            let mut pending = Vec::with_capacity(keys.len());
            for key in keys {
                if !db.has_projection_commit(&key)? {
                    pending.push(key);
                }
            }
            Ok(pending)
        })
        .await?;
    report.commits_already_folded += listed_commits - keys.len();
    for key in &keys {
        if let Err(error) = fold_occurrence_commit(store, key, config, db, fence, report).await {
            if is_pass_fatal(&error) {
                return Err(error);
            }
            report.occurrence_commits_quarantined += 1;
            tracing::warn!(
                path = %key,
                object_kind = "occurrence_commit",
                error = %format!("{error:#}"),
                "quarantining immutable object for this reconciliation cycle"
            );
        }
    }
    if let Some(last_location) = last_location {
        db.set_cursor(ReconcileCursor {
            partition: OCCURRENCE_CURSOR_PARTITION.to_owned(),
            highest_key: last_location.to_string(),
            updated_at_unix_nano: 0,
        })
        .await?;
    }
    if exhausted && offset.is_some() {
        db.set_cursor(ReconcileCursor {
            partition: OCCURRENCE_CURSOR_PARTITION.to_owned(),
            highest_key: String::new(),
            updated_at_unix_nano: 0,
        })
        .await?;
    }
    Ok(())
}

/// Read, verify, and fold one committed occurrence projection. Returns the
/// number of rows it contains.
async fn fold_occurrence_commit(
    store: &dyn ObjectStore,
    commit_key: &str,
    config: &ReconcileConfig,
    db: &mut BlockingDb,
    fence: &Arc<dyn Fence>,
    report: &mut EngineReport,
) -> Result<usize> {
    let commit_json = read_bounded_object(store, commit_key, MAX_COMMIT_BYTES, None).await?;
    let commit: OccurrenceCommit = serde_json::from_slice(&commit_json)
        .with_context(|| format!("decoding occurrence commit {commit_key}"))?;
    let canonical = commit
        .canonical_json()
        .context("encoding canonical occurrence commit")?;
    ensure!(
        canonical.as_slice() == commit_json.as_ref(),
        "occurrence commit {commit_key} is not canonical JSON"
    );
    let suffix = commit_key
        .strip_prefix(&format!("{OCCURRENCE_PROJECTION_PREFIX}/"))
        .context("occurrence commit outside projection prefix")?;
    let date = suffix
        .get(..10)
        .context("occurrence commit lacks date partition")?
        .to_owned();
    let expected = occurrence_keys(
        &date,
        commit.source_log_block_uuid,
        &commit.extractor_generation,
    )?;
    ensure!(
        expected.commit == commit_key && expected.data == commit.data_key,
        "occurrence commit key or data key is inconsistent"
    );
    ensure!(
        commit.row_count <= config.projection_limits.max_rows as u64,
        "occurrence commit row count exceeds limit"
    );
    let data = read_bounded_object(
        store,
        &commit.data_key,
        config.projection_limits.max_bytes,
        Some(commit.data_size_bytes),
    )
    .await?;
    let limits = config.projection_limits;
    let commit_key = commit_key.to_owned();
    let fence = fence.clone();
    let (fold, rows) = db
        .run(move |db| {
            let verified = OccurrenceCommit::new(
                commit.source_log_block_uuid,
                commit.extractor_generation.clone(),
                commit.data_key.clone(),
                &data,
                commit.row_count,
                commit.committed_at_unix_nano,
            )?;
            ensure!(
                verified == commit,
                "occurrence projection digest or size does not match commit"
            );
            let rows = decode_occurrences(data, limits)?;
            ensure!(
                rows.len() as u64 == commit.row_count,
                "occurrence projection row count does not match commit"
            );
            let fold = db.fold_committed(
                &date,
                &commit_key,
                &commit,
                &commit_json,
                rows.iter().map(|row| row.as_row().as_sqlite_row()),
                fence.as_ref(),
            )?;
            Ok((fold, rows.len()))
        })
        .await?;
    report.absorb_fold(fold);
    Ok(rows)
}

pub fn validate_config(config: &ReconcileConfig) -> Result<()> {
    ensure!(config.max_blocks > 0, "max_blocks must be nonzero");
    ensure!(
        config.max_source_rows > 0,
        "max_source_rows must be nonzero"
    );
    ensure!(
        config.max_source_bytes > 0,
        "max_source_bytes must be nonzero"
    );
    ensure!(
        config.max_decoded_raw_bytes > 0,
        "max_decoded_raw_bytes must be nonzero"
    );
    ensure!(
        config.source_batch_rows > 0,
        "source_batch_rows must be nonzero"
    );
    ensure!(
        config.projection_limits.max_rows > 0,
        "projection max_rows must be nonzero"
    );
    ensure!(
        config.grouping_page_rows > 0,
        "grouping_page_rows must be nonzero"
    );
    ensure!(
        config.grouping_max_rows_per_pass > 0,
        "grouping_max_rows_per_pass must be nonzero"
    );
    ensure!(
        !config.grouping_generation.is_empty(),
        "grouping_generation must not be empty"
    );
    // occurrence_keys performs the authoritative generation grammar validation.
    occurrence_keys(
        "2000-01-01",
        uuid::Uuid::from_u128(1),
        &config.extractor_generation,
    )?;
    Ok(())
}

async fn process_block(
    entry: &CatalogEntry,
    store: Arc<dyn ObjectStore>,
    deployment_id: DeploymentId,
    config: &ReconcileConfig,
    db: &mut BlockingDb,
    fence: &Arc<dyn Fence>,
    report: &mut EngineReport,
) -> Result<()> {
    ensure!(
        entry.meta.row_count <= config.max_source_rows as u64,
        "source block {} row count {} exceeds limit {}",
        entry.meta.uuid,
        entry.meta.row_count,
        config.max_source_rows
    );
    ensure!(
        entry.meta.byte_size <= config.max_source_bytes as u64,
        "source block {} byte size {} exceeds limit {}",
        entry.meta.uuid,
        entry.meta.byte_size,
        config.max_source_bytes
    );
    let keys = occurrence_keys(&entry.date, entry.meta.uuid, &config.extractor_generation)?;

    // A projection that is already published (by an earlier pass whose local fold
    // was lost, or by a peer) is authoritative: fold it as committed instead of
    // re-encoding the source. This skips the source read and parquet encode, and
    // means an encoder whose output differs from an existing object's bytes can
    // never turn into an immutable-object collision.
    match store.head(&ObjectPath::from(keys.commit.as_str())).await {
        Ok(_) => {
            let rows =
                fold_occurrence_commit(store.as_ref(), &keys.commit, config, db, fence, report)
                    .await?;
            report.publications_existing += 1;
            report.processed_blocks += 1;
            report.occurrence_rows += rows;
            return Ok(());
        }
        Err(object_store::Error::NotFound { .. }) => {}
        Err(error) => {
            return Err(anyhow::Error::new(error).context(format!("HEAD {}", keys.commit)));
        }
    }

    let source_key = block_path(
        "logs",
        entry.meta.ts_min_unix_nano,
        entry.meta.writer_id,
        entry.meta.uuid,
        "parquet",
    );
    let source = read_bounded_object(
        store.as_ref(),
        &source_key,
        config.max_source_bytes,
        Some(entry.meta.byte_size),
    )
    .await?;
    let (data, row_count, skipped) = {
        let entry = entry.clone();
        let config = config.clone();
        tokio::task::spawn_blocking(move || -> Result<(Bytes, usize, usize)> {
            let (extracted, skipped) = extract_source(&source, &entry, deployment_id, &config)?;
            ensure!(
                extracted.len() <= config.projection_limits.max_rows,
                "source block {} produced {} occurrences, exceeding projection limit {}",
                entry.meta.uuid,
                extracted.len(),
                config.projection_limits.max_rows
            );
            let source_uuid = *entry.meta.uuid.as_bytes();
            let rows: Vec<_> = extracted
                .iter()
                .map(|row| {
                    ProjectionRow::from_occurrence(
                        &row.occurrence,
                        row.occurred_at,
                        row.observed_at,
                        row.received_at,
                        &source_uuid,
                        row.ordinal,
                    )
                })
                .collect();
            let data = encode_occurrences(&rows, config.projection_limits)?;
            Ok((data, rows.len(), skipped))
        })
        .await
        .context("occurrence extraction task panicked")??
    };
    // Source metadata is immutable and supplies a deterministic timestamp. Wall-clock
    // time would make retries produce conflicting commit bytes at the same key.
    let commit = OccurrenceCommit::new(
        entry.meta.uuid,
        config.extractor_generation.clone(),
        keys.data.clone(),
        &data,
        row_count as u64,
        entry.meta.ts_max_unix_nano,
    )?;
    let commit_json = Bytes::from(commit.canonical_json()?);
    check_lease(fence.as_ref())?;
    match publish_occurrence_projection(store.clone(), &keys, data, commit_json.clone()).await? {
        PublicationOutcome::Created => report.publications_created += 1,
        PublicationOutcome::ExistingIdentical => report.publications_existing += 1,
        PublicationOutcome::Collision => {
            bail!(
                "immutable occurrence projection collision at {}",
                keys.commit
            )
        }
    }

    // Read both objects behind the visibility marker. Publication verifies an object
    // after ambiguous/existing creates; this read also verifies a newly created marker
    // before its metadata is trusted by SQLite.
    let verified_commit_json = read_bounded_object(
        store.as_ref(),
        &keys.commit,
        MAX_COMMIT_BYTES,
        Some(commit_json.len() as u64),
    )
    .await?;
    ensure!(
        verified_commit_json == commit_json,
        "published commit marker differs from the expected canonical bytes"
    );
    let verified_commit: OccurrenceCommit = serde_json::from_slice(&verified_commit_json)
        .context("decoding published commit marker")?;
    ensure!(
        verified_commit == commit,
        "published commit marker has different metadata"
    );
    let verified_data = read_bounded_object(
        store.as_ref(),
        &keys.data,
        config.projection_limits.max_bytes,
        Some(commit.data_size_bytes),
    )
    .await?;
    let limits = config.projection_limits;
    let date = entry.date.clone();
    let fence = fence.clone();
    let (fold, verified_rows) = db
        .run(move |db| {
            ensure!(
                OccurrenceCommit::new(
                    commit.source_log_block_uuid,
                    commit.extractor_generation.clone(),
                    commit.data_key.clone(),
                    &verified_data,
                    commit.row_count,
                    commit.committed_at_unix_nano,
                )? == commit,
                "published projection digest or size does not match its commit"
            );
            let rows = decode_occurrences(verified_data, limits)?;
            ensure!(
                rows.len() as u64 == commit.row_count,
                "published projection row count does not match its commit"
            );
            let fold = db.fold_committed(
                &date,
                &keys.commit,
                &verified_commit,
                &verified_commit_json,
                rows.iter().map(|row| row.as_row().as_sqlite_row()),
                fence.as_ref(),
            )?;
            Ok((fold, rows.len()))
        })
        .await?;
    report.absorb_fold(fold);
    report.processed_blocks += 1;
    report.occurrence_rows += verified_rows;
    report.skipped_records += skipped;
    Ok(())
}

/// Errors that end the whole pass rather than quarantining one immutable
/// object: an unavailable object store (every later object would fail the same
/// way), a lost lease, and a failing local database (not the object's fault).
fn is_pass_fatal(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<object_store::Error>()
            .is_some_and(|error| !matches!(error, object_store::Error::NotFound { .. }))
            || cause.downcast_ref::<LeaseLost>().is_some()
            || cause.downcast_ref::<tokio::task::JoinError>().is_some()
            || cause.downcast_ref::<SqliteError>().is_some_and(|error| {
                matches!(
                    error,
                    SqliteError::FenceLost(_)
                        | SqliteError::Sqlite(_)
                        | SqliteError::GenerationConflict { .. }
                        | SqliteError::UnsupportedSchema { .. }
                )
            })
    })
}

async fn read_bounded_object(
    store: &dyn ObjectStore,
    key: &str,
    max_bytes: usize,
    expected_bytes: Option<u64>,
) -> Result<Bytes> {
    let path = ObjectPath::from(key);
    let metadata = store
        .head(&path)
        .await
        .with_context(|| format!("HEAD {key}"))?;
    ensure!(
        metadata.size <= max_bytes as u64,
        "object {key} size {} exceeds limit {max_bytes}",
        metadata.size
    );
    if let Some(expected) = expected_bytes {
        ensure!(
            metadata.size == expected,
            "object {key} size differs from metadata"
        );
    }
    let result = store
        .get_opts(
            &path,
            GetOptions {
                range: Some((0..metadata.size).into()),
                ..Default::default()
            },
        )
        .await
        .with_context(|| format!("GET {key}"))?;
    let bytes = result
        .bytes()
        .await
        .with_context(|| format!("reading {key}"))?;
    ensure!(
        bytes.len() as u64 == metadata.size,
        "short or oversized read of {key}"
    );
    Ok(bytes)
}

fn extract_source(
    source: &Bytes,
    entry: &CatalogEntry,
    deployment_id: DeploymentId,
    config: &ReconcileConfig,
) -> Result<(Vec<ExtractedRow>, usize)> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(source.clone())
        .context("opening logs-v2 parquet")?;
    let metadata_rows = u64::try_from(builder.metadata().file_metadata().num_rows())
        .context("negative parquet row count")?;
    ensure!(
        metadata_rows == entry.meta.row_count,
        "parquet row count differs from catalog"
    );
    ensure!(
        metadata_rows <= config.max_source_rows as u64,
        "parquet row limit exceeded"
    );
    let schema = builder.schema();
    let ts_index = require_field(
        schema.as_ref(),
        "ts_unix_nano",
        &arrow::datatypes::DataType::UInt64,
    )?;
    let observed_index = require_field(
        schema.as_ref(),
        "observed_ts_unix_nano",
        &arrow::datatypes::DataType::UInt64,
    )?;
    let version_index = require_field(
        schema.as_ref(),
        "raw_record_version",
        &arrow::datatypes::DataType::UInt16,
    )?;
    let raw_index = require_field(
        schema.as_ref(),
        "raw_record",
        &arrow::datatypes::DataType::Binary,
    )?;
    let received_index = require_field(
        schema.as_ref(),
        "received_ts_unix_nano",
        &arrow::datatypes::DataType::UInt64,
    )?;
    // Decode only the five columns needed by the extractor. In particular, avoid
    // materializing the duplicated body/attribute projections beside `raw_record`.
    let projection = ProjectionMask::roots(
        builder.parquet_schema(),
        [
            ts_index,
            observed_index,
            version_index,
            raw_index,
            received_index,
        ],
    );
    let reader = builder
        .with_projection(projection)
        .with_batch_size(config.source_batch_rows)
        .build()
        .context("building logs-v2 parquet reader")?;

    let mut output = Vec::new();
    let mut scratch = Scratch::with_capacity(16 * 1024);
    let mut ordinal = 0_u64;
    let mut decoded_raw_bytes = 0usize;
    let mut projected_canonical_bytes = 0usize;
    let mut skipped = 0usize;
    for batch in reader {
        let batch = batch.context("reading logs-v2 parquet batch")?;
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context("invalid ts column")?;
        let observed = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context("invalid observed timestamp column")?;
        let versions = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .context("invalid raw version column")?;
        let raw = batch
            .column(3)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context("invalid raw record column")?;
        let received = batch
            .column(4)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context("invalid received timestamp column")?;
        for index in 0..batch.num_rows() {
            ensure!(
                !versions.is_null(index) && !raw.is_null(index) && !received.is_null(index),
                "logs-v3 row lacks canonical raw record or receipt timestamp"
            );
            ensure!(
                versions.value(index) == LOGS_RAW_VERSION_V2,
                "unsupported logs raw-record version {}",
                versions.value(index)
            );
            decoded_raw_bytes = decoded_raw_bytes
                .checked_add(raw.value_length(index) as usize)
                .context("decoded raw byte count overflow")?;
            ensure!(
                decoded_raw_bytes <= config.max_decoded_raw_bytes,
                "decoded raw records exceed byte limit"
            );
            let record = validate_logs_record_version(
                raw.value(index),
                LOGS_RAW_VERSION_V2,
                LogsV2DecodeLimits::default(),
            )
            .context("validating canonical logs-v2 raw record")?;
            let received_at = record
                .received_time_unix_nano
                .context("server-stamped raw record lacks receipt timestamp")?;
            ensure!(received_at != 0, "server receipt timestamp must be nonzero");
            ensure!(
                received.value(index) == received_at,
                "raw record receipt timestamp differs from parquet column"
            );
            match extract(
                record,
                deployment_id,
                config.extraction_limits,
                &mut scratch,
            ) {
                Ok(occurrence) => {
                    ensure!(
                        output.len() < config.projection_limits.max_rows,
                        "source block {} produced more than {} occurrences",
                        entry.meta.uuid,
                        config.projection_limits.max_rows
                    );
                    projected_canonical_bytes = projected_canonical_bytes
                        .checked_add(occurrence.canonical.len())
                        .context("projected canonical byte count overflow")?;
                    ensure!(
                        projected_canonical_bytes <= config.projection_limits.max_bytes,
                        "source block {} occurrence canonical bytes exceed projection byte limit",
                        entry.meta.uuid
                    );
                    let occurred_at = ts.value(index);
                    let observed_at = (!observed.is_null(index)).then(|| observed.value(index));
                    output.push(ExtractedRow {
                        occurrence,
                        occurred_at,
                        observed_at,
                        received_at,
                        ordinal,
                    });
                }
                Err(error) if ineligible(&error) => skipped += 1,
                Err(error) => return Err(error).context("extracting occurrence"),
            }
            ordinal += 1;
        }
    }
    ensure!(
        ordinal == metadata_rows,
        "decoded parquet row count differs from metadata"
    );
    Ok((output, skipped))
}

fn require_field(
    schema: &arrow::datatypes::Schema,
    name: &str,
    data_type: &arrow::datatypes::DataType,
) -> Result<usize> {
    let index = schema
        .index_of(name)
        .with_context(|| format!("missing {name} column"))?;
    ensure!(
        schema.field(index).data_type() == data_type,
        "wrong type for {name} column"
    );
    Ok(index)
}

fn ineligible(error: &ExtractionError) -> bool {
    matches!(
        error,
        ExtractionError::NotExceptionEvent
            | ExtractionError::MissingExceptionContent
            | ExtractionError::MissingAttribute(_)
            | ExtractionError::AttributeType(_)
            | ExtractionError::EmptyAttribute(_)
            | ExtractionError::InvalidUuid(_)
            | ExtractionError::IdentityLimit
            | ExtractionError::CanonicalLimit
            | ExtractionError::LengthOverflow
    )
}

/// Fingerprint one stored occurrence. The OCC1 bytes are decoded once and feed
/// both the fingerprint and the severity. Identity fields inside the canonical
/// bytes must match the row they are stored under; a mismatch or a decode error
/// is a durable grouping failure, never a silent skip.
pub fn group_occurrence(
    deployment_id: &[u8; 16],
    occurrence: &UngroupedOccurrence<'_>,
) -> std::result::Result<GroupedOccurrence, String> {
    let decoded = occ1::decode(occurrence.canonical)
        .map_err(|error| format!("OCC1 decode failed: {error}"))?;
    if decoded.deployment_id != deployment_id
        || decoded.app_identity_digest != occurrence.app_identity_sha256
        || decoded.event_id != occurrence.event_id
    {
        return Err("OCC1 identity does not match its occurrence row".to_owned());
    }
    let fingerprint = fingerprint_from_decoded(&decoded, occurrence.app_identity_sha256);
    let issue_id = derive_issue_id(
        deployment_id,
        occurrence.app_identity_sha256,
        fingerprint.version,
        &fingerprint.digest,
    );
    Ok(GroupedOccurrence {
        issue_id: *issue_id.as_bytes(),
        fingerprint_version: fingerprint.version,
        fingerprint_digest: fingerprint.digest,
        grouping_quality: fingerprint.quality as u8,
        title: fingerprint.title,
        severity: decoded.severity_number,
    })
}

/// Group the ungrouped backlog page by page until it drains or the per-pass
/// row or time budget is spent. Each page is its own fenced transaction.
async fn group_backlog(
    db: &mut BlockingDb,
    deployment_id: DeploymentId,
    config: &ReconcileConfig,
    fence: &Arc<dyn Fence>,
    report: &mut EngineReport,
) -> Result<()> {
    let started = Instant::now();
    let deployment = *deployment_id.as_bytes();
    loop {
        let generation = config.grouping_generation.clone();
        let page_rows = config.grouping_page_rows;
        let fence = fence.clone();
        let page = db
            .run(move |db| {
                Ok(
                    db.group_page(&generation, page_rows, fence.as_ref(), |occurrence| {
                        group_occurrence(&deployment, occurrence)
                    })?,
                )
            })
            .await
            .context("grouping occurrences")?;
        report.grouping.absorb(page);
        if page.drained
            || report.grouping.scanned >= config.grouping_max_rows_per_pass
            || started.elapsed() >= config.grouping_time_budget
        {
            break;
        }
    }
    if report.grouping.failures > 0 {
        tracing::warn!(
            failures = report.grouping.failures,
            generation = %config.grouping_generation,
            "occurrences could not be fingerprinted; recorded in grouping_failures"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use object_store::{memory::InMemory, PutPayload};
    use rusqlite::Connection;
    use scry_block::{logs::LogsBlockBuilder, AlwaysValid, BlockBuilder, BlockBuilderConfig};
    use scry_proto::{
        decode_logs_batch_v2_into, encode_logs_batch_v2_into, redact_and_stamp_logs_v2,
        LogRecordInput, LogsV2AnyValueInput as Value, LogsV2KeyValueInput as KeyValue,
        LogsV2StampScratch,
    };
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::*;

    fn valid() -> Arc<dyn Fence> {
        Arc::new(AlwaysValid)
    }

    struct Fixture {
        directory: tempfile::TempDir,
        store: Arc<dyn ObjectStore>,
        deployment_id: DeploymentId,
        stamped: Vec<u8>,
        writer_id: Uuid,
    }

    fn fixture() -> Fixture {
        let resource = [KeyValue {
            key: "service.name",
            value: Value::String("checkout"),
        }];
        let attributes = [
            KeyValue {
                key: "exception.message",
                value: Value::String("connection refused"),
            },
            KeyValue {
                key: "exception.type",
                value: Value::String("DatabaseError"),
            },
            KeyValue {
                key: "scry.event.id",
                value: Value::String("00000000-0000-0000-0000-000000000044"),
            },
        ];
        let record = LogRecordInput {
            resource_schema_url: "",
            resource_dropped_attributes_count: 0,
            resource_attributes: &resource,
            scope: None,
            scope_schema_url: "",
            time_unix_nano: 1_700_000_000_000_000_000,
            observed_time_unix_nano: 1_700_000_000_000_000_100,
            severity: 17,
            severity_text: "ERROR",
            event_name: "exception",
            body: Value::String("request failed"),
            dropped_attributes_count: 0,
            attributes: &attributes,
            trace_flags: 1,
            trace_id: Some(&[5; 16]),
            span_id: Some(&[6; 8]),
        };
        let mut payload = Vec::new();
        encode_logs_batch_v2_into(&[record], Default::default(), &mut payload).unwrap();
        let mut stamp_scratch = LogsV2StampScratch::default();
        let stamped = redact_and_stamp_logs_v2(
            &payload,
            1_700_000_000_000_000_200,
            Default::default(),
            &mut stamp_scratch,
        )
        .unwrap()
        .to_vec();
        let deployment = Uuid::from_u128(0x33);
        Fixture {
            directory: tempdir().unwrap(),
            store: Arc::new(InMemory::new()),
            deployment_id: DeploymentId::parse(&deployment.to_string(), "deployment_id").unwrap(),
            stamped,
            writer_id: Uuid::from_u128(0x11),
        }
    }

    async fn upload_block(fixture: &Fixture, block_id: Uuid) -> scry_block::BlockMeta {
        let mut builder = LogsBlockBuilder::new(fixture.writer_id, BlockBuilderConfig::default());
        builder.set_block_uuid(block_id);
        decode_logs_batch_v2_into(&fixture.stamped, Default::default(), &mut builder).unwrap();
        builder
            .finish_and_upload(fixture.store.as_ref())
            .await
            .unwrap()
            .unwrap()
    }

    async fn put(store: &dyn ObjectStore, key: &str, bytes: impl Into<Bytes>) {
        store
            .put(&ObjectPath::from(key), PutPayload::from(bytes.into()))
            .await
            .unwrap();
    }

    async fn reconcile(
        fixture: &Fixture,
        catalog_path: &Path,
        errors_path: &Path,
        config: &ReconcileConfig,
    ) -> Result<EngineReport> {
        reconcile_once(
            CatalogMode::Owned {
                catalog_path,
                bucket: "test-bucket",
            },
            fixture.store.clone(),
            errors_path,
            fixture.deployment_id,
            config,
            valid(),
        )
        .await
    }

    #[tokio::test]
    async fn malformed_sidecar_before_good_advances_and_wraps() {
        let fixture = fixture();
        upload_block(&fixture, Uuid::from_u128(0x22)).await;
        put(
            fixture.store.as_ref(),
            "logs/0000/00/00/00000000-0000-0000-0000-000000000000/00000000-0000-0000-0000-000000000000.meta.json",
            Bytes::from_static(b"not-json"),
        )
        .await;
        let config = ReconcileConfig {
            max_blocks: 1,
            ..Default::default()
        };
        let catalog_path = fixture.directory.path().join("catalog.sqlite");
        let errors_path = fixture.directory.path().join("errors.sqlite");
        let first = reconcile(&fixture, &catalog_path, &errors_path, &config)
            .await
            .unwrap();
        assert_eq!(first.raw_meta_quarantined, 1);
        // The raw metadata walker may scan several objects per block, so the valid
        // sidecar is admitted and processed in the same pass after the poison key.
        assert_eq!(first.processed_blocks, 1);
        let second = reconcile(&fixture, &catalog_path, &errors_path, &config)
            .await
            .unwrap();
        // The wrapped page offers the same block again; coverage skips it.
        assert_eq!(second.processed_blocks, 0);
        assert_eq!(second.sources_already_covered, 1);
        assert_eq!(
            Catalog::open(&catalog_path, "test-bucket")
                .unwrap()
                .list_blocks()
                .unwrap()
                .len(),
            1
        );
        let mut retried_after_wrap = false;
        for _ in 0..8 {
            let report = reconcile(&fixture, &catalog_path, &errors_path, &config)
                .await
                .unwrap();
            if report.raw_meta_quarantined == 1 {
                retried_after_wrap = true;
                break;
            }
        }
        assert!(
            retried_after_wrap,
            "poison sidecar was not retried after wrap"
        );
    }

    #[tokio::test]
    async fn bad_source_parquet_before_good_does_not_block_good() {
        let fixture = fixture();
        let bad_id = Uuid::from_u128(0x20);
        let mut bad = upload_block(&fixture, bad_id).await;
        let bad_key = block_path(
            "logs",
            bad.ts_min_unix_nano,
            bad.writer_id,
            bad.uuid,
            "parquet",
        );
        put(
            fixture.store.as_ref(),
            &bad_key,
            Bytes::from_static(b"bad parquet"),
        )
        .await;
        bad.byte_size = 11;
        let catalog_path = fixture.directory.path().join("catalog.sqlite");
        let catalog = Catalog::open(&catalog_path, "test-bucket").unwrap();
        catalog.insert_block(&bad).unwrap();
        let good = upload_block(&fixture, Uuid::from_u128(0x21)).await;
        catalog.insert_block(&good).unwrap();
        drop(catalog);
        let config = ReconcileConfig {
            max_blocks: 2,
            ..Default::default()
        };
        let report = reconcile(
            &fixture,
            &catalog_path,
            &fixture.directory.path().join("errors.sqlite"),
            &config,
        )
        .await
        .unwrap();
        assert_eq!(report.source_blocks_quarantined, 1);
        assert_eq!(report.processed_blocks, 1);
        assert_eq!(report.occurrence_rows, 1);
    }

    #[tokio::test]
    async fn bad_commit_and_data_before_good_do_not_block_good_and_wrap() {
        let fixture = fixture();
        let good_meta = upload_block(&fixture, Uuid::from_u128(0x30)).await;
        let config = ReconcileConfig {
            max_blocks: 1,
            ..Default::default()
        };
        let catalog_path = fixture.directory.path().join("catalog.sqlite");
        let source_errors = fixture.directory.path().join("source.sqlite");
        reconcile(&fixture, &catalog_path, &source_errors, &config)
            .await
            .unwrap();
        let date = first_date(&good_meta);
        let malformed =
            occurrence_keys(&date, Uuid::from_u128(1), DEFAULT_EXTRACTOR_GENERATION).unwrap();
        put(
            fixture.store.as_ref(),
            &malformed.commit,
            Bytes::from_static(b"bad commit"),
        )
        .await;
        let bad_data_keys =
            occurrence_keys(&date, Uuid::from_u128(2), DEFAULT_EXTRACTOR_GENERATION).unwrap();
        put(
            fixture.store.as_ref(),
            &bad_data_keys.data,
            Bytes::from_static(b"bad data"),
        )
        .await;
        let bad_commit = OccurrenceCommit::new(
            Uuid::from_u128(2),
            DEFAULT_EXTRACTOR_GENERATION.to_owned(),
            bad_data_keys.data.clone(),
            b"different data",
            0,
            1,
        )
        .unwrap();
        put(
            fixture.store.as_ref(),
            &bad_data_keys.commit,
            Bytes::from(bad_commit.canonical_json().unwrap()),
        )
        .await;
        let rebuilt = fixture.directory.path().join("rebuilt.sqlite");
        let empty_catalog = fixture.directory.path().join("empty-catalog.sqlite");
        let mut quarantined = 0usize;
        let mut inserted = 0usize;
        for _ in 0..8 {
            let report = reconcile(&fixture, &empty_catalog, &rebuilt, &config)
                .await
                .unwrap();
            quarantined += report.occurrence_commits_quarantined;
            inserted += report.fold.inserted;
            if inserted == 1 && quarantined >= 2 {
                break;
            }
        }
        assert_eq!(
            inserted, 1,
            "valid commit was not folded after poison objects"
        );
        assert!(quarantined >= 2, "both poison commits must be quarantined");

        let mut retried_after_wrap = false;
        for _ in 0..8 {
            let report = reconcile(&fixture, &empty_catalog, &rebuilt, &config)
                .await
                .unwrap();
            if report.occurrence_commits_quarantined != 0 {
                retried_after_wrap = true;
                break;
            }
        }
        assert!(
            retried_after_wrap,
            "poison commit was not retried after wrap"
        );
    }

    #[tokio::test]
    async fn uncataloged_logs_v2_block_is_discovered_published_and_folded_idempotently() {
        let directory = tempdir().unwrap();
        let catalog_path = directory.path().join("catalog.sqlite");
        let errors_path = directory.path().join("errors.sqlite");
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer_id = Uuid::from_u128(0x11);
        let block_id = Uuid::from_u128(0x22);
        let deployment = Uuid::from_u128(0x33);
        let event_id = "00000000-0000-0000-0000-000000000044";
        let resource = [
            KeyValue {
                key: "service.name",
                value: Value::String("checkout"),
            },
            KeyValue {
                key: "service.namespace",
                value: Value::String("payments"),
            },
        ];
        let attributes = [
            KeyValue {
                key: "exception.message",
                value: Value::String("connection refused"),
            },
            KeyValue {
                key: "exception.type",
                value: Value::String("DatabaseError"),
            },
            KeyValue {
                key: "scry.event.id",
                value: Value::String(event_id),
            },
        ];
        let record = LogRecordInput {
            resource_schema_url: "",
            resource_dropped_attributes_count: 0,
            resource_attributes: &resource,
            scope: None,
            scope_schema_url: "",
            time_unix_nano: 1_700_000_000_000_000_000,
            observed_time_unix_nano: 1_700_000_000_000_000_100,
            severity: 21,
            severity_text: "ERROR",
            event_name: "exception",
            body: Value::String("request failed"),
            dropped_attributes_count: 0,
            attributes: &attributes,
            trace_flags: 1,
            trace_id: Some(&[5; 16]),
            span_id: Some(&[6; 8]),
        };
        let mut payload = Vec::new();
        encode_logs_batch_v2_into(&[record], Default::default(), &mut payload).unwrap();
        let received_at = 1_700_000_000_000_000_200;
        let mut stamp_scratch = LogsV2StampScratch::default();
        let stamped = redact_and_stamp_logs_v2(
            &payload,
            received_at,
            Default::default(),
            &mut stamp_scratch,
        )
        .unwrap();
        let mut builder = LogsBlockBuilder::new(writer_id, BlockBuilderConfig::default());
        builder.set_block_uuid(block_id);
        decode_logs_batch_v2_into(stamped, Default::default(), &mut builder).unwrap();
        let meta = builder
            .finish_and_upload(store.as_ref())
            .await
            .unwrap()
            .unwrap();
        assert!(!catalog_path.exists(), "test must start without a catalog");

        // Exercise the inclusive row/block/batch boundaries rather than only the
        // roomy production defaults.
        let config = ReconcileConfig {
            max_blocks: 1,
            max_source_rows: 1,
            source_batch_rows: 1,
            projection_limits: ProjectionLimits {
                max_rows: 1,
                ..Default::default()
            },
            ..Default::default()
        };
        let deployment_id = DeploymentId::parse(&deployment.to_string(), "deployment_id").unwrap();
        let run = |catalog_path: PathBuf, errors_path: PathBuf, store: Arc<dyn ObjectStore>| {
            let config = config.clone();
            async move {
                reconcile_once(
                    CatalogMode::Owned {
                        catalog_path: &catalog_path,
                        bucket: "test-bucket",
                    },
                    store,
                    &errors_path,
                    deployment_id,
                    &config,
                    valid(),
                )
                .await
                .unwrap()
            }
        };
        let first = run(catalog_path.clone(), errors_path.clone(), store.clone()).await;
        assert_eq!(first.candidate_blocks, 1);
        assert_eq!(first.processed_blocks, 1);
        assert_eq!(first.occurrence_rows, 1);
        assert_eq!(first.publications_created, 1);
        assert_eq!(first.fold.inserted, 1);
        // Grouping phase: the occurrence should be fingerprinted and create one issue.
        assert_eq!(first.grouping.occurrences_grouped, 1);
        assert_eq!(first.grouping.issues_created, 1);
        assert_eq!(first.commits_folded, 1);
        assert!(first.changed_state());

        let keys =
            occurrence_keys(&first_date(&meta), block_id, DEFAULT_EXTRACTOR_GENERATION).unwrap();
        store
            .head(&ObjectPath::from(keys.data.as_str()))
            .await
            .unwrap();
        store
            .head(&ObjectPath::from(keys.commit.as_str()))
            .await
            .unwrap();
        let projected = store
            .get(&ObjectPath::from(keys.data.as_str()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let rows = decode_occurrences(projected, config.projection_limits).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].event_id,
            *Uuid::parse_str(event_id).unwrap().as_bytes()
        );
        assert_eq!(rows[0].source_log_block_uuid, *block_id.as_bytes());
        assert_eq!(rows[0].source_row_ordinal, 0);

        let second = run(catalog_path.clone(), errors_path.clone(), store.clone()).await;
        // Nothing is re-read: the commit is already folded and the wrapped source
        // block is already covered.
        assert_eq!(second.commits_already_folded, 1);
        assert_eq!(second.sources_already_covered, 1);
        assert_eq!(second.processed_blocks, 0);
        assert_eq!(second.publications_existing, 0);
        assert_eq!(second.fold, FoldReport::default());
        assert_eq!(second.commits_folded, 0);
        assert!(
            !second.changed_state(),
            "an idle pass must not dirty snapshots"
        );

        let connection = Connection::open(&errors_path).unwrap();
        for table in [
            "occurrences",
            "projection_commits",
            "source_coverage",
            "issues",
            "occurrence_issues",
        ] {
            let count: u64 = connection
                .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 1, "unexpected count in {table}");
        }
        // The issue carries the exception type title and the decoded severity.
        let (title, severity): (String, i64) = connection
            .query_row("SELECT title, max_severity FROM issues", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .unwrap();
        assert_eq!(title, "DatabaseError");
        assert_eq!(severity, 21);
        drop(connection);

        // More live blocks than max_blocks advance deterministically instead of making
        // the whole pass fail. Reuse the already stamped immutable payload.
        let catalog = Catalog::open(&catalog_path, "test-bucket").unwrap();
        for block_id in [Uuid::from_u128(0x23), Uuid::from_u128(0x24)] {
            let mut builder = LogsBlockBuilder::new(writer_id, BlockBuilderConfig::default());
            builder.set_block_uuid(block_id);
            decode_logs_batch_v2_into(stamped, Default::default(), &mut builder).unwrap();
            let added = builder
                .finish_and_upload(store.as_ref())
                .await
                .unwrap()
                .unwrap();
            catalog.insert_block(&added).unwrap();
        }
        drop(catalog);
        let page_two = run(catalog_path.clone(), errors_path.clone(), store.clone()).await;
        // candidate_blocks is the bounded page size, not the total catalog count.
        assert_eq!(page_two.candidate_blocks, 1);
        assert_eq!(page_two.processed_blocks, 1);
        let page_three = run(catalog_path.clone(), errors_path.clone(), store.clone()).await;
        assert_eq!(page_three.candidate_blocks, 1);
        assert_eq!(page_three.processed_blocks, 1);

        // The local database is disposable: committed projection objects alone rebuild
        // it even when both the raw catalog and source block are unavailable.
        let rebuilt_path = directory.path().join("rebuilt-errors.sqlite");
        let missing_catalog = directory.path().join("missing-catalog.sqlite");
        let raw_keys: Vec<_> = store
            .list(Some(&ObjectPath::from("logs/")))
            .map(|item| item.unwrap().location)
            .collect()
            .await;
        for key in raw_keys {
            store.delete(&key).await.unwrap();
        }
        let rebuilt = run(missing_catalog, rebuilt_path.clone(), store).await;
        assert_eq!(rebuilt.processed_blocks, 0);
        assert_eq!(rebuilt.fold.inserted, 1);
        let rebuilt_db = Connection::open(rebuilt_path).unwrap();
        let count: u64 = rebuilt_db
            .query_row("SELECT count(*) FROM occurrences", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    fn first_date(meta: &scry_block::BlockMeta) -> String {
        let catalog = tempdir().unwrap();
        let db = Catalog::open(&catalog.path().join("catalog.sqlite"), "unused").unwrap();
        db.insert_block(meta).unwrap();
        db.list_blocks().unwrap().remove(0).date
    }

    struct LostFence;
    impl Fence for LostFence {
        fn check(&self) -> Result<()> {
            bail!("lease lost")
        }
    }

    #[tokio::test]
    async fn lost_lease_stops_before_publication_and_fold() {
        let fixture = fixture();
        let meta = upload_block(&fixture, Uuid::from_u128(0x40)).await;
        let catalog_path = fixture.directory.path().join("catalog.sqlite");
        let errors_path = fixture.directory.path().join("errors.sqlite");
        let error = reconcile_once(
            CatalogMode::Owned {
                catalog_path: &catalog_path,
                bucket: "test-bucket",
            },
            fixture.store.clone(),
            &errors_path,
            fixture.deployment_id,
            &ReconcileConfig::default(),
            Arc::new(LostFence),
        )
        .await
        .unwrap_err();
        assert!(is_pass_fatal(&error), "{error:#}");
        let keys =
            occurrence_keys(&first_date(&meta), meta.uuid, DEFAULT_EXTRACTOR_GENERATION).unwrap();
        assert!(matches!(
            fixture
                .store
                .head(&ObjectPath::from(keys.data.as_str()))
                .await,
            Err(object_store::Error::NotFound { .. })
        ));
        let count: u64 = Connection::open(&errors_path)
            .unwrap()
            .query_row("SELECT count(*) FROM occurrences", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    /// A projection published before the writer properties were pinned (or by
    /// any encoder whose bytes differ) is folded as committed, not re-encoded
    /// into an immutable-object collision.
    #[tokio::test]
    async fn existing_projection_with_legacy_bytes_is_folded_not_republished() {
        let fixture = fixture();
        let meta = upload_block(&fixture, Uuid::from_u128(0x50)).await;
        let catalog_path = fixture.directory.path().join("catalog.sqlite");
        let catalog = Catalog::open(&catalog_path, "test-bucket").unwrap();
        catalog.insert_block(&meta).unwrap();
        let entry = catalog.list_blocks().unwrap().remove(0);
        drop(catalog);
        let config = ReconcileConfig::default();

        // Publish the block's projection with parquet's default properties.
        let source_key = block_path(
            "logs",
            meta.ts_min_unix_nano,
            meta.writer_id,
            meta.uuid,
            "parquet",
        );
        let source = fixture
            .store
            .get(&ObjectPath::from(source_key.as_str()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let (extracted, _) =
            extract_source(&source, &entry, fixture.deployment_id, &config).unwrap();
        let uuid = *meta.uuid.as_bytes();
        let rows: Vec<_> = extracted
            .iter()
            .map(|row| {
                ProjectionRow::from_occurrence(
                    &row.occurrence,
                    row.occurred_at,
                    row.observed_at,
                    row.received_at,
                    &uuid,
                    row.ordinal,
                )
            })
            .collect();
        let legacy = crate::projection::encode_occurrences_with(
            &rows,
            config.projection_limits,
            parquet::file::properties::WriterProperties::builder().build(),
        )
        .unwrap();
        assert_ne!(
            legacy,
            encode_occurrences(&rows, config.projection_limits).unwrap()
        );
        let keys = occurrence_keys(&entry.date, meta.uuid, DEFAULT_EXTRACTOR_GENERATION).unwrap();
        let commit = OccurrenceCommit::new(
            meta.uuid,
            DEFAULT_EXTRACTOR_GENERATION.to_owned(),
            keys.data.clone(),
            &legacy,
            rows.len() as u64,
            meta.ts_max_unix_nano,
        )
        .unwrap();
        put(fixture.store.as_ref(), &keys.data, legacy).await;
        put(
            fixture.store.as_ref(),
            &keys.commit,
            Bytes::from(commit.canonical_json().unwrap()),
        )
        .await;

        let errors_path = fixture.directory.path().join("errors.sqlite");
        let mut db = BlockingDb::open(&errors_path, fixture.deployment_id, 4_096)
            .await
            .unwrap();
        let mut report = EngineReport::default();
        process_block(
            &entry,
            fixture.store.clone(),
            fixture.deployment_id,
            &config,
            &mut db,
            &valid(),
            &mut report,
        )
        .await
        .unwrap();
        assert_eq!(report.publications_existing, 1);
        assert_eq!(report.publications_created, 0);
        assert_eq!(report.fold.inserted, 1);
        assert_eq!(report.occurrence_rows, 1);
    }

    #[tokio::test]
    async fn grouping_drains_several_pages_in_one_pass_within_budget() {
        let fixture = fixture();
        let catalog_path = fixture.directory.path().join("catalog.sqlite");
        let catalog = Catalog::open(&catalog_path, "test-bucket").unwrap();
        for id in 0x60..0x65 {
            let meta = upload_block(&fixture, Uuid::from_u128(id)).await;
            catalog.insert_block(&meta).unwrap();
        }
        drop(catalog);
        // Each block carries the same event ID, so only the first inserts; use
        // distinct occurrences by folding them directly instead.
        let errors_path = fixture.directory.path().join("errors.sqlite");
        {
            let deployment = *fixture.deployment_id.as_bytes();
            let mut db = ErrorsDb::open(&errors_path, deployment).unwrap();
            insert_distinct_occurrences(&mut db, &fixture, 7);
        }
        let config = ReconcileConfig {
            grouping_page_rows: 2,
            ..Default::default()
        };
        let report = reconcile(&fixture, &catalog_path, &errors_path, &config)
            .await
            .unwrap();
        assert_eq!(report.grouping.scanned, 7 + 1);
        assert!(report.grouping.drained);

        // A row budget stops the pass early; the next pass resumes.
        let errors_path = fixture.directory.path().join("budgeted.sqlite");
        {
            let deployment = *fixture.deployment_id.as_bytes();
            let mut db = ErrorsDb::open(&errors_path, deployment).unwrap();
            insert_distinct_occurrences(&mut db, &fixture, 7);
        }
        let config = ReconcileConfig {
            grouping_page_rows: 2,
            grouping_max_rows_per_pass: 3,
            ..Default::default()
        };
        let first = reconcile(&fixture, &catalog_path, &errors_path, &config)
            .await
            .unwrap();
        assert_eq!(first.grouping.scanned, 4);
        assert!(!first.grouping.drained);
        let second = reconcile(&fixture, &catalog_path, &errors_path, &config)
            .await
            .unwrap();
        assert_eq!(second.grouping.scanned, 4);
        assert!(second.grouping.drained);
    }

    /// Fold `count` occurrences with distinct event IDs and real OCC1 bytes.
    fn insert_distinct_occurrences(db: &mut ErrorsDb, fixture: &Fixture, count: u8) {
        use sha2::{Digest, Sha256};
        let source = Uuid::from_u128(0x99);
        let keys = occurrence_keys("2026-09-10", source, DEFAULT_EXTRACTOR_GENERATION).unwrap();
        let commit = OccurrenceCommit::new(
            source,
            DEFAULT_EXTRACTOR_GENERATION.to_owned(),
            keys.data,
            b"data",
            u64::from(count),
            1,
        )
        .unwrap();
        let json = commit.canonical_json().unwrap();
        let occurrences: Vec<Occurrence> = (0..count)
            .map(|i| {
                let event = format!("00000000-0000-0000-0000-0000000001{i:02x}");
                occurrence_with_event(fixture, &event)
            })
            .collect();
        let digests: Vec<[u8; 32]> = occurrences
            .iter()
            .map(|o| Sha256::digest(&o.canonical).into())
            .collect();
        let rows: Vec<_> = occurrences
            .iter()
            .zip(&digests)
            .map(|(o, digest)| crate::sqlite::OccurrenceRow {
                deployment_id: o.deployment_id.as_bytes(),
                app_id: &o.app.app_id,
                app_identity_sha256: &o.app.digest,
                event_id: o.event_id.as_bytes(),
                occurred_at_unix_nano: 1,
                observed_at_unix_nano: None,
                received_at_unix_nano: 1,
                trace_id: None,
                span_id: None,
                trace_flags: 0,
                canonical_version: 1,
                scrub_policy_version: 1,
                canonical_sha256: digest,
                canonical: &o.canonical,
                source_log_block_uuid: source.as_bytes(),
                source_row_ordinal: 0,
            })
            .collect();
        db.fold_committed(
            "2026-09-10",
            &keys.commit,
            &commit,
            &json,
            rows,
            &AlwaysValid,
        )
        .unwrap();
    }

    fn occurrence_with_event(fixture: &Fixture, event: &str) -> Occurrence {
        let resource = [KeyValue {
            key: "service.name",
            value: Value::String("checkout"),
        }];
        let attributes = [
            KeyValue {
                key: "exception.type",
                value: Value::String("DatabaseError"),
            },
            KeyValue {
                key: "scry.event.id",
                value: Value::String(event),
            },
        ];
        let record = LogRecordInput {
            resource_schema_url: "",
            resource_dropped_attributes_count: 0,
            resource_attributes: &resource,
            scope: None,
            scope_schema_url: "",
            time_unix_nano: 1,
            observed_time_unix_nano: 1,
            severity: 17,
            severity_text: "ERROR",
            event_name: "exception",
            body: Value::String(""),
            dropped_attributes_count: 0,
            attributes: &attributes,
            trace_flags: 0,
            trace_id: None,
            span_id: None,
        };
        let mut encoded = Vec::new();
        scry_proto::encode_log_record_v2_into(&record, LogsV2DecodeLimits::default(), &mut encoded)
            .unwrap();
        let record =
            scry_proto::validate_logs_v2_record(&encoded, LogsV2DecodeLimits::default()).unwrap();
        extract(
            record,
            fixture.deployment_id,
            ExtractionLimits::default(),
            &mut Scratch::default(),
        )
        .unwrap()
    }

    #[test]
    fn group_occurrence_uses_decoded_severity_and_rejects_bad_rows() {
        use crate::occ1::tests::make_occurrence_with;
        let occ = make_occurrence_with(Some("TypeError"), Some("boom 12345"), 21);
        let deployment = *occ.deployment_id.as_bytes();
        let row = UngroupedOccurrence {
            fold_seq: 1,
            app_identity_sha256: &occ.app.digest,
            event_id: occ.event_id.as_bytes(),
            occurred_at_unix_nano: 1,
            received_at_unix_nano: 1,
            canonical: &occ.canonical,
        };
        let grouped = group_occurrence(&deployment, &row).unwrap();
        assert_eq!(grouped.severity, 21);
        assert_eq!(grouped.fingerprint_version, FINGERPRINT_VERSION);
        assert_eq!(grouped.title, "TypeError");

        let garbage = UngroupedOccurrence {
            canonical: b"not occ1",
            ..row
        };
        assert!(group_occurrence(&deployment, &garbage)
            .unwrap_err()
            .contains("OCC1 decode failed"));
        let wrong_event = UngroupedOccurrence {
            event_id: &[0xAB; 16],
            ..row
        };
        assert!(group_occurrence(&deployment, &wrong_event)
            .unwrap_err()
            .contains("identity"));
    }

    #[test]
    fn rejects_zero_bounds_and_invalid_generation() {
        let mut config = ReconcileConfig {
            max_blocks: 0,
            ..Default::default()
        };
        assert!(validate_config(&config)
            .unwrap_err()
            .to_string()
            .contains("max_blocks"));
        config.max_blocks = 1;
        config.grouping_page_rows = 0;
        assert!(validate_config(&config)
            .unwrap_err()
            .to_string()
            .contains("grouping_page_rows"));
        config.grouping_page_rows = 1;
        config.extractor_generation = "INVALID".to_owned();
        assert!(validate_config(&config)
            .unwrap_err()
            .to_string()
            .contains("extractor generation"));
    }
}
