//! Rebuildable SQLite fold for the occurrence projection and issue grouping.
//!
//! Every public write is one bounded transaction. Digests are accelerators only:
//! duplicate decisions compare the exact canonical bytes after digest equality.
//!
//! # Grouping model (schema v5)
//!
//! Each newly inserted occurrence receives a strictly increasing `fold_seq`.
//! Grouping for a generation walks `fold_seq` from a persisted per-generation
//! cursor, so each pass reads only occurrences it has not seen, and late-arriving
//! occurrences (old `occurred_at`, new `fold_seq`) are never skipped. A grouping
//! page, its issue aggregates, its failure records, and the cursor advance commit
//! in one transaction. Within a page, occurrences of one issue are aggregated in
//! memory and written with a single issue upsert (all aggregates are
//! order-independent), so a hot issue costs one row rewrite per page rather than
//! one per occurrence.
//!
//! An occurrence whose OCC1 bytes cannot be fingerprinted is recorded in
//! `grouping_failures` and the cursor moves past it, so one poison row cannot wedge
//! grouping; the failures stay queryable per generation.
//!
//! `occurrence_issues` is keyed by the full occurrence identity plus
//! `grouping_generation`, so several generations can coexist. Readers list issues
//! of the *active* generation recorded in `metadata`. The first generation ever
//! grouped becomes active immediately; a later generation becomes active only once
//! its cursor has drained the backlog, so a regroup never shows a half-built list.
//!
//! Issue and per-issue ordering timestamps are the occurrence time clamped to at
//! most [`CLOCK_SKEW_ALLOWANCE_NANOS`] after the server receipt time, so a client
//! with a fast clock cannot pin an issue or occurrence to the top of a list.

use std::collections::HashMap;
use std::path::Path;

use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use scry_block::Fence;

use crate::projection::{occurrence_keys, OccurrenceCommit};
use crate::quarantine::{collision_id, CollisionMirror};

pub const ERRORS_SCHEMA_VERSION: u32 = 5;
pub const DEFAULT_MAX_ROWS_PER_TRANSACTION: usize = 4_096;
/// Hard server-side cap on rows returned by one issue-list or occurrence-list
/// read. Larger requested limits are clamped; see `error-monitoring-ui.md`.
pub const MAX_ISSUE_PAGE_ROWS: usize = 1_000;
/// How far an occurrence's own timestamp may lead its server receipt time before
/// it is clamped for issue `first_seen`/`last_seen` and latest-occurrence order.
pub const CLOCK_SKEW_ALLOWANCE_NANOS: u64 = 5 * 60 * 1_000_000_000;
/// Page cache of a writable connection, in KiB. The SQLite default (2 MiB)
/// thrashes once the database outgrows it: at one million occurrences the
/// fold rate falls from ~150k to ~67k rows/s (see `tests/scale.rs`). Allocated
/// lazily, so small databases never reach it.
const WRITER_CACHE_KIB: i64 = 64 * 1024;
/// Longest stored grouping-failure reason, in bytes.
const MAX_FAILURE_REASON_BYTES: usize = 256;
pub(crate) const DEPLOYMENT_METADATA_KEY: &str = "deployment_id";
const ACTIVE_GENERATION_METADATA_KEY: &str = "active_grouping_generation";

#[derive(Debug, thiserror::Error)]
pub enum SqliteError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("write contains {actual} rows, exceeding transaction limit {limit}")]
    TransactionLimit { actual: usize, limit: usize },
    #[error("{field} has length {actual}; expected {expected}")]
    InvalidLength {
        field: &'static str,
        expected: usize,
        actual: usize,
    },
    #[error("projection commit identity already has different exact metadata")]
    CommitConflict,
    #[error(
        "errors database belongs to deployment {actual:?}, not expected deployment {expected:?}"
    )]
    DeploymentMismatch {
        expected: [u8; 16],
        actual: [u8; 16],
    },
    #[error("errors database has schema version {found}; this binary supports {supported}")]
    UnsupportedSchema { found: u32, supported: u32 },
    #[error("errors database has no bound deployment")]
    MissingDeployment,
    #[error(
        "issue already belongs to grouping generation {existing:?}, not {requested:?}; \
         a new grouping generation requires a new fingerprint version"
    )]
    GenerationConflict { existing: String, requested: String },
    #[error("lease fence lost before commit: {0}")]
    FenceLost(String),
}

/// Borrowed input accepted from an extractor or a Parquet reader.
#[derive(Debug, Clone, Copy)]
pub struct OccurrenceRow<'a> {
    pub deployment_id: &'a [u8],
    pub app_id: &'a [u8],
    pub app_identity_sha256: &'a [u8],
    pub event_id: &'a [u8],
    pub occurred_at_unix_nano: u64,
    pub observed_at_unix_nano: Option<u64>,
    pub received_at_unix_nano: u64,
    pub trace_id: Option<&'a [u8]>,
    pub span_id: Option<&'a [u8]>,
    pub trace_flags: u32,
    pub canonical_version: u16,
    pub scrub_policy_version: u16,
    pub canonical_sha256: &'a [u8],
    pub canonical: &'a [u8],
    pub source_log_block_uuid: &'a [u8],
    pub source_row_ordinal: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FoldReport {
    pub inserted: usize,
    pub exact_duplicates: usize,
    pub collisions: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileCursor {
    pub partition: String,
    pub highest_key: String,
    pub updated_at_unix_nano: u64,
}

/// One occurrence offered to the grouping callback of [`ErrorsDb::group_page`].
/// Borrows directly from the SQLite row; nothing is copied.
#[derive(Debug, Clone, Copy)]
pub struct UngroupedOccurrence<'a> {
    pub fold_seq: u64,
    pub app_identity_sha256: &'a [u8; 32],
    pub event_id: &'a [u8; 16],
    pub occurred_at_unix_nano: u64,
    pub received_at_unix_nano: u64,
    pub canonical: &'a [u8],
}

/// The grouping callback's result for one occurrence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupedOccurrence {
    pub issue_id: [u8; 16],
    pub fingerprint_version: u16,
    pub fingerprint_digest: [u8; 32],
    pub grouping_quality: u8,
    /// Display title; callers bound it (see `fingerprint::MAX_TITLE_BYTES`).
    pub title: String,
    pub severity: i32,
}

/// An issue summary for API responses.
///
/// Timestamps are nanoseconds since the Unix epoch serialized as JSON numbers.
/// JavaScript clients parse them into IEEE doubles, which keep about 256 ns of
/// precision at current epoch values; clients use them for display and ordering
/// only, never as identities.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct IssueSummary {
    pub issue_id: String,
    pub app_id: String,
    pub title: String,
    pub grouping_quality: u8,
    pub first_seen_unix_nano: u64,
    pub last_seen_unix_nano: u64,
    pub occurrence_count: u64,
    pub max_severity: i32,
    pub fingerprint_version: u16,
}

/// An occurrence summary for API responses — the columns available directly
/// on the `occurrences` table without OCC1 blob decode. See [`IssueSummary`] for
/// timestamp precision.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct OccurrenceSummary {
    pub event_id: String,
    pub occurred_at_unix_nano: u64,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
}

/// Report from grouping one or more pages.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GroupingReport {
    /// Issues created, counted once per page that created them.
    pub issues_created: usize,
    /// Existing issues that received occurrences, counted once per page: a page
    /// folds all of its occurrences of one issue into a single upsert.
    pub issues_updated: usize,
    pub occurrences_grouped: usize,
    pub duplicate_skipped: usize,
    /// Occurrences recorded in `grouping_failures` and skipped.
    pub failures: usize,
    /// Occurrences read from the backlog (grouped, duplicate, or failed).
    pub scanned: usize,
    /// True when no ungrouped occurrence remained after the last page.
    pub drained: bool,
}

impl GroupingReport {
    pub fn absorb(&mut self, page: GroupingReport) {
        self.issues_created += page.issues_created;
        self.issues_updated += page.issues_updated;
        self.occurrences_grouped += page.occurrences_grouped;
        self.duplicate_skipped += page.duplicate_skipped;
        self.failures += page.failures;
        self.scanned += page.scanned;
        self.drained = page.drained;
    }
}

/// One recorded grouping failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupingFailure {
    pub fold_seq: u64,
    pub event_id: [u8; 16],
    pub reason: String,
}

/// Grouping progress for one generation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GroupingProgress {
    /// Highest `fold_seq` grouped (or failed) for the generation.
    pub cursor: u64,
    /// Highest `fold_seq` assigned to any occurrence.
    pub head: u64,
    /// Number of recorded grouping failures for the generation.
    pub failures: u64,
}

// Hot read queries are constants so tests can EXPLAIN exactly what runs.
const SQL_LIST_ISSUES: &str = "SELECT issue_id, app_id, title, grouping_quality,
            first_seen_unix_nano, last_seen_unix_nano,
            occurrence_count, max_severity, fingerprint_version
     FROM issues
     WHERE deployment_id = ?1 AND grouping_generation = ?2
     ORDER BY last_seen_unix_nano DESC, issue_id
     LIMIT ?3";
const SQL_GET_ISSUE: &str = "SELECT issue_id, app_id, title, grouping_quality,
            first_seen_unix_nano, last_seen_unix_nano,
            occurrence_count, max_severity, fingerprint_version
     FROM issues
     WHERE deployment_id = ?1 AND issue_id = ?2";
const SQL_LIST_OCCURRENCES_FOR_ISSUE: &str =
    "SELECT o.event_id, o.occurred_at_unix_nano, o.trace_id, o.span_id
     FROM occurrence_issues oi
     JOIN occurrences o
       ON o.deployment_id = oi.deployment_id
      AND o.app_id = oi.app_id
      AND o.app_identity_sha256 = oi.app_identity_sha256
      AND o.event_id = oi.event_id
     WHERE oi.deployment_id = ?1 AND oi.issue_id = ?2
     ORDER BY oi.sort_at_unix_nano DESC, oi.event_id DESC
     LIMIT ?3";
const SQL_UNGROUPED_PAGE: &str = "SELECT fold_seq, app_id, app_identity_sha256, event_id,
            occurred_at_unix_nano, received_at_unix_nano, canonical
     FROM occurrences
     WHERE fold_seq > ?1
     ORDER BY fold_seq
     LIMIT ?2";

pub struct ErrorsDb {
    conn: Connection,
    deployment_id: [u8; 16],
    max_rows_per_transaction: usize,
    /// Per-page issue aggregates of [`ErrorsDb::group_page`]; cleared, not
    /// reallocated, for each page.
    pending_issues: HashMap<[u8; 16], PendingIssue>,
}

/// One issue's aggregate over the occurrences of a single grouping page,
/// written with one upsert when the page ends.
#[derive(Debug)]
struct PendingIssue {
    app_id: [u8; 16],
    app_identity_sha256: [u8; 32],
    fingerprint_version: u16,
    fingerprint_digest: [u8; 32],
    grouping_quality: u8,
    /// Title of the page's first occurrence; used only if the issue is new.
    title: String,
    first_seen: i64,
    /// The latest occurrence by (clamped time, event ID); its time is `last_seen`.
    latest_sort_at: i64,
    latest_event_id: [u8; 16],
    max_severity: i32,
    occurrences: i64,
}

impl ErrorsDb {
    pub fn open(path: &Path, deployment_id: [u8; 16]) -> Result<Self, SqliteError> {
        Self::from_connection(Connection::open(path)?, deployment_id)
    }

    pub fn open_in_memory(deployment_id: [u8; 16]) -> Result<Self, SqliteError> {
        Self::from_connection(Connection::open_in_memory()?, deployment_id)
    }

    /// Open an existing errors database **read-only**. Skips migration; the
    /// schema version must match this binary and the bound deployment is read
    /// from the database. WAL mode allows concurrent readers alongside the
    /// single writer.
    pub fn open_read_only(path: &Path) -> Result<Self, SqliteError> {
        let flags =
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let conn = Connection::open_with_flags(path, flags)?;
        let found: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
        if found != i64::from(ERRORS_SCHEMA_VERSION) {
            return Err(SqliteError::UnsupportedSchema {
                found: found as u32,
                supported: ERRORS_SCHEMA_VERSION,
            });
        }
        let deployment: Option<Vec<u8>> = conn
            .query_row(
                "SELECT value FROM metadata WHERE key = ?1",
                [DEPLOYMENT_METADATA_KEY],
                |row| row.get(0),
            )
            .optional()?;
        let deployment = deployment.ok_or(SqliteError::MissingDeployment)?;
        let deployment_id: [u8; 16] =
            deployment
                .as_slice()
                .try_into()
                .map_err(|_| SqliteError::InvalidLength {
                    field: "metadata.deployment_id",
                    expected: 16,
                    actual: deployment.len(),
                })?;
        Ok(Self {
            conn,
            deployment_id,
            max_rows_per_transaction: DEFAULT_MAX_ROWS_PER_TRANSACTION,
            pending_issues: HashMap::new(),
        })
    }

    fn from_connection(mut conn: Connection, deployment_id: [u8; 16]) -> Result<Self, SqliteError> {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        // Negative cache_size is KiB.
        conn.pragma_update(None, "cache_size", -WRITER_CACHE_KIB)?;
        migrate(&mut conn)?;
        bind_deployment(&mut conn, deployment_id)?;
        Ok(Self {
            conn,
            deployment_id,
            max_rows_per_transaction: DEFAULT_MAX_ROWS_PER_TRANSACTION,
            pending_issues: HashMap::new(),
        })
    }

    pub fn deployment_id(&self) -> [u8; 16] {
        self.deployment_id
    }

    pub fn set_max_rows_per_transaction(&mut self, limit: usize) {
        self.max_rows_per_transaction = limit;
    }

    /// Whether `projection_key` (a commit marker key) has already been folded.
    /// Lets the reconciler skip the object-store GETs of a committed projection.
    pub fn has_projection_commit(&self, projection_key: &str) -> Result<bool, SqliteError> {
        Ok(self
            .conn
            .prepare_cached(
                "SELECT 1 FROM projection_commits WHERE occurrence_projection_key = ?1",
            )?
            .exists([projection_key])?)
    }

    /// Whether a source log block is covered by a folded projection of the given
    /// extractor generation.
    pub fn is_source_covered(
        &self,
        source_log_block_uuid: &[u8; 16],
        extractor_generation: &str,
    ) -> Result<bool, SqliteError> {
        Ok(self
            .conn
            .prepare_cached(
                "SELECT 1 FROM source_coverage
                 WHERE source_log_block_uuid = ?1 AND extractor_generation = ?2",
            )?
            .exists(params![
                source_log_block_uuid.as_slice(),
                extractor_generation
            ])?)
    }

    /// Atomically records a committed generation, source coverage, and its rows.
    ///
    /// `projection_key` is the commit marker key and therefore the durable generation
    /// identity. Callers must feed generations in deterministic key order. `fence`
    /// is checked immediately before the commit; a lost lease rolls back.
    pub fn fold_committed<'a, I>(
        &mut self,
        source_date: &str,
        projection_key: &str,
        commit: &OccurrenceCommit,
        commit_json: &[u8],
        rows: I,
        fence: &dyn Fence,
    ) -> Result<FoldReport, SqliteError>
    where
        I: IntoIterator<Item = OccurrenceRow<'a>>,
    {
        let expected_keys = occurrence_keys(
            source_date,
            commit.source_log_block_uuid,
            &commit.extractor_generation,
        )
        .map_err(|_| SqliteError::CommitConflict)?;
        if expected_keys.commit != projection_key || expected_keys.data != commit.data_key {
            return Err(SqliteError::CommitConflict);
        }
        let rows = rows.into_iter();
        let (lower, upper) = rows.size_hint();
        if lower > self.max_rows_per_transaction
            || upper.is_some_and(|upper| upper > self.max_rows_per_transaction)
        {
            return Err(SqliteError::TransactionLimit {
                actual: upper.unwrap_or(lower),
                limit: self.max_rows_per_transaction,
            });
        }

        // Validate the complete bounded batch before opening the transaction. In
        // particular, a late row from another deployment must not follow any write.
        let mut validated_rows = Vec::with_capacity(upper.unwrap_or(lower));
        for row in rows {
            if validated_rows.len() == self.max_rows_per_transaction {
                return Err(SqliteError::TransactionLimit {
                    actual: validated_rows.len() + 1,
                    limit: self.max_rows_per_transaction,
                });
            }
            validate_row(&row, self.deployment_id)?;
            validated_rows.push(row);
        }

        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        ensure_commit(&tx, projection_key, commit, commit_json)?;
        let mut report = FoldReport::default();
        let mut next_fold_seq: i64 = tx.query_row(
            "SELECT COALESCE(MAX(fold_seq), 0) + 1 FROM occurrences",
            [],
            |row| row.get(0),
        )?;
        for row in validated_rows {
            fold_row(&tx, projection_key, row, &mut next_fold_seq, &mut report)?;
        }
        tx.execute(
            "INSERT INTO source_coverage (
                 source_log_block_uuid, source_date, extractor_generation,
                 occurrence_projection_key, covered_at_unix_nano
             ) VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(source_log_block_uuid, extractor_generation) DO UPDATE SET
                 source_date = excluded.source_date,
                 occurrence_projection_key = excluded.occurrence_projection_key,
                 covered_at_unix_nano = excluded.covered_at_unix_nano",
            params![
                commit.source_log_block_uuid.as_bytes().as_slice(),
                source_date,
                commit.extractor_generation,
                projection_key,
                to_sql_i64(commit.committed_at_unix_nano),
            ],
        )?;
        check_fence(fence)?;
        tx.commit()?;
        Ok(report)
    }

    pub fn set_reconcile_cursor(&mut self, cursor: &ReconcileCursor) -> Result<(), SqliteError> {
        self.conn.execute(
            "INSERT INTO reconcile_cursors(partition, highest_key, updated_at_unix_nano)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(partition) DO UPDATE SET
                 highest_key = excluded.highest_key,
                 updated_at_unix_nano = excluded.updated_at_unix_nano",
            params![
                cursor.partition,
                cursor.highest_key,
                to_sql_i64(cursor.updated_at_unix_nano)
            ],
        )?;
        Ok(())
    }

    pub fn reconcile_cursor(
        &self,
        partition: &str,
    ) -> Result<Option<ReconcileCursor>, SqliteError> {
        Ok(self
            .conn
            .query_row(
                "SELECT highest_key, updated_at_unix_nano FROM reconcile_cursors
                 WHERE partition = ?1",
                [partition],
                |row| {
                    Ok(ReconcileCursor {
                        partition: partition.to_owned(),
                        highest_key: row.get(0)?,
                        updated_at_unix_nano: from_sql_i64(row.get(1)?),
                    })
                },
            )
            .optional()?)
    }

    /// Group at most `limit` occurrences after the generation's cursor, in one
    /// transaction.
    ///
    /// `group` fingerprints one borrowed occurrence; an `Err(reason)` records a
    /// durable grouping failure and moves on. Aggregates, failures, the cursor
    /// advance and (when the backlog drains) generation activation commit
    /// together, after `fence` confirms the caller still holds its lease.
    pub fn group_page<F>(
        &mut self,
        generation: &str,
        limit: usize,
        fence: &dyn Fence,
        mut group: F,
    ) -> Result<GroupingReport, SqliteError>
    where
        F: FnMut(&UngroupedOccurrence<'_>) -> Result<GroupedOccurrence, String>,
    {
        let deployment = self.deployment_id;
        let pending = &mut self.pending_issues;
        pending.clear();
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let cursor = grouping_cursor(&tx, generation)?;
        let mut report = GroupingReport::default();
        let mut last_seq = cursor;
        {
            let mut select = tx.prepare_cached(SQL_UNGROUPED_PAGE)?;
            let mut rows = select.query(params![cursor as i64, limit as i64])?;
            while let Some(row) = rows.next()? {
                let fold_seq = row.get::<_, i64>(0)? as u64;
                let app_id = fixed::<16>(blob(row, 1)?, "occurrences.app_id")?;
                let occurrence = UngroupedOccurrence {
                    fold_seq,
                    app_identity_sha256: fixed::<32>(
                        blob(row, 2)?,
                        "occurrences.app_identity_sha256",
                    )?,
                    event_id: fixed::<16>(blob(row, 3)?, "occurrences.event_id")?,
                    occurred_at_unix_nano: from_sql_i64(row.get(4)?),
                    received_at_unix_nano: from_sql_i64(row.get(5)?),
                    canonical: blob(row, 6)?,
                };
                match group(&occurrence) {
                    Ok(grouped) => map_occurrence(
                        &tx,
                        deployment,
                        generation,
                        app_id,
                        &occurrence,
                        grouped,
                        pending,
                        &mut report,
                    )?,
                    Err(reason) => {
                        record_failure(&tx, generation, app_id, &occurrence, &reason)?;
                        report.failures += 1;
                    }
                }
                last_seq = fold_seq;
                report.scanned += 1;
            }
        }
        for (issue_id, issue) in pending.drain() {
            upsert_issue(&tx, deployment, generation, &issue_id, &issue, &mut report)?;
        }
        // A full page may have consumed exactly the rest of the backlog; one
        // index probe distinguishes that from a remaining backlog so a
        // regrouped generation activates without an extra empty page.
        report.drained = report.scanned < limit
            || !tx
                .prepare_cached("SELECT EXISTS(SELECT 1 FROM occurrences WHERE fold_seq > ?1)")?
                .query_row(params![last_seq as i64], |row| row.get::<_, bool>(0))?;
        if last_seq != cursor {
            tx.prepare_cached(
                "INSERT INTO grouping_cursors(grouping_generation, last_fold_seq)
                 VALUES (?1, ?2)
                 ON CONFLICT(grouping_generation) DO UPDATE SET
                     last_fold_seq = excluded.last_fold_seq",
            )?
            .execute(params![generation, last_seq as i64])?;
        }
        let active = active_generation(&tx)?;
        if active.as_deref() != Some(generation) && (active.is_none() || report.drained) {
            tx.execute(
                "INSERT INTO metadata(key, value) VALUES (?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                params![ACTIVE_GENERATION_METADATA_KEY, generation.as_bytes()],
            )?;
        }
        check_fence(fence)?;
        tx.commit()?;
        Ok(report)
    }

    /// The grouping generation readers currently list issues from.
    pub fn active_grouping_generation(&self) -> Result<Option<String>, SqliteError> {
        active_generation(&self.conn)
    }

    pub fn grouping_progress(&self, generation: &str) -> Result<GroupingProgress, SqliteError> {
        let cursor = grouping_cursor(&self.conn, generation)?;
        let head: i64 = self.conn.query_row(
            "SELECT COALESCE(MAX(fold_seq), 0) FROM occurrences",
            [],
            |row| row.get(0),
        )?;
        let failures: i64 = self.conn.query_row(
            "SELECT count(*) FROM grouping_failures WHERE grouping_generation = ?1",
            [generation],
            |row| row.get(0),
        )?;
        Ok(GroupingProgress {
            cursor,
            head: head as u64,
            failures: failures as u64,
        })
    }

    /// Recorded grouping failures of a generation, oldest first.
    pub fn grouping_failures(
        &self,
        generation: &str,
        limit: usize,
    ) -> Result<Vec<GroupingFailure>, SqliteError> {
        let mut stmt = self.conn.prepare(
            "SELECT fold_seq, event_id, reason FROM grouping_failures
             WHERE grouping_generation = ?1 ORDER BY fold_seq LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![generation, limit as i64], |row| {
            Ok(GroupingFailure {
                fold_seq: row.get::<_, i64>(0)? as u64,
                event_id: row.get(1)?,
                reason: row.get(2)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, rusqlite::Error>>()?)
    }

    /// List issues of the active grouping generation ordered by `last_seen`
    /// descending, then issue ID. `limit` is clamped to [`MAX_ISSUE_PAGE_ROWS`].
    /// Suitable for read-only connections.
    pub fn list_issues(&self, limit: usize) -> Result<Vec<IssueSummary>, SqliteError> {
        let Some(generation) = self.active_grouping_generation()? else {
            return Ok(Vec::new());
        };
        let limit = limit.min(MAX_ISSUE_PAGE_ROWS);
        let mut stmt = self.conn.prepare_cached(SQL_LIST_ISSUES)?;
        let rows = stmt.query_map(
            params![self.deployment_id.as_slice(), generation, limit as i64],
            issue_from_row,
        )?;
        Ok(rows.collect::<Result<Vec<_>, rusqlite::Error>>()?)
    }

    /// Fetch a single issue of this deployment by its 16-byte ID (a primary-key
    /// lookup). Returns `None` if not found.
    pub fn get_issue(&self, issue_id: &[u8; 16]) -> Result<Option<IssueSummary>, SqliteError> {
        Ok(self
            .conn
            .prepare_cached(SQL_GET_ISSUE)?
            .query_row(
                params![self.deployment_id.as_slice(), issue_id.as_slice()],
                issue_from_row,
            )
            .optional()?)
    }

    /// List occurrences of an issue, newest first by skew-clamped occurrence time
    /// with the event ID as a deterministic tie-break. Served by the
    /// `occurrence_issues_by_issue` index. `limit` is clamped to
    /// [`MAX_ISSUE_PAGE_ROWS`].
    pub fn list_occurrences_for_issue(
        &self,
        issue_id: &[u8; 16],
        limit: usize,
    ) -> Result<Vec<OccurrenceSummary>, SqliteError> {
        let limit = limit.min(MAX_ISSUE_PAGE_ROWS);
        let mut stmt = self.conn.prepare_cached(SQL_LIST_OCCURRENCES_FOR_ISSUE)?;
        let rows = stmt.query_map(
            params![
                self.deployment_id.as_slice(),
                issue_id.as_slice(),
                limit as i64
            ],
            |row| {
                let event_id: [u8; 16] = row.get(0)?;
                let trace_id = row.get_ref(2)?.as_blob_or_null()?;
                let span_id = row.get_ref(3)?.as_blob_or_null()?;
                Ok(OccurrenceSummary {
                    event_id: uuid::Uuid::from_bytes(event_id).to_string(),
                    occurred_at_unix_nano: from_sql_i64(row.get(1)?),
                    trace_id: trace_id.map(hex),
                    span_id: span_id.map(hex),
                })
            },
        )?;
        Ok(rows.collect::<Result<Vec<_>, rusqlite::Error>>()?)
    }

    /// `EXPLAIN QUERY PLAN` output for each hot query, joined per query by
    /// `" | "`. Used by tests and benchmarks to prove index use.
    pub fn explain_hot_queries(&self) -> Result<Vec<(&'static str, String)>, SqliteError> {
        let dummy16 = [0_u8; 16];
        let mut out = Vec::new();
        for (name, sql, params) in [
            (
                "list_issues",
                SQL_LIST_ISSUES,
                vec![
                    rusqlite::types::Value::Blob(dummy16.to_vec()),
                    rusqlite::types::Value::Text("g".into()),
                    rusqlite::types::Value::Integer(10),
                ],
            ),
            (
                "get_issue",
                SQL_GET_ISSUE,
                vec![
                    rusqlite::types::Value::Blob(dummy16.to_vec()),
                    rusqlite::types::Value::Blob(dummy16.to_vec()),
                ],
            ),
            (
                "list_occurrences_for_issue",
                SQL_LIST_OCCURRENCES_FOR_ISSUE,
                vec![
                    rusqlite::types::Value::Blob(dummy16.to_vec()),
                    rusqlite::types::Value::Blob(dummy16.to_vec()),
                    rusqlite::types::Value::Integer(10),
                ],
            ),
            (
                "ungrouped_page",
                SQL_UNGROUPED_PAGE,
                vec![
                    rusqlite::types::Value::Integer(0),
                    rusqlite::types::Value::Integer(10),
                ],
            ),
        ] {
            let mut stmt = self.conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
            let details = stmt
                .query_map(rusqlite::params_from_iter(params), |row| {
                    row.get::<_, String>(3)
                })?
                .collect::<Result<Vec<_>, _>>()?;
            out.push((name, details.join(" | ")));
        }
        Ok(out)
    }

    #[cfg(test)]
    fn counts(&self) -> (u64, u64, u64) {
        let count = |table: &str| {
            self.conn
                .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
                .unwrap()
        };
        (
            count("occurrences"),
            count("occurrence_collisions"),
            count("projection_commits"),
        )
    }
}

/// Current schema. Every statement is idempotent so it can run on a fresh
/// database and after the explicit upgrade steps in [`migrate`].
const SCHEMA_V5: &str = "
     CREATE TABLE IF NOT EXISTS metadata (
         key TEXT PRIMARY KEY,
         value BLOB NOT NULL
     ) STRICT, WITHOUT ROWID;
     CREATE TABLE IF NOT EXISTS projection_commits (
         occurrence_projection_key TEXT PRIMARY KEY,
         source_log_block_uuid BLOB NOT NULL CHECK(length(source_log_block_uuid) = 16),
         extractor_generation TEXT NOT NULL,
         data_key TEXT NOT NULL,
         data_sha256 TEXT NOT NULL CHECK(length(data_sha256) = 64),
         data_size_bytes INTEGER NOT NULL,
         row_count INTEGER NOT NULL,
         committed_at_unix_nano INTEGER NOT NULL,
         commit_json BLOB NOT NULL
     ) STRICT;
     CREATE TABLE IF NOT EXISTS source_coverage (
         source_log_block_uuid BLOB NOT NULL CHECK(length(source_log_block_uuid) = 16),
         source_date TEXT NOT NULL,
         extractor_generation TEXT NOT NULL,
         occurrence_projection_key TEXT NOT NULL REFERENCES projection_commits(occurrence_projection_key),
         covered_at_unix_nano INTEGER NOT NULL,
         PRIMARY KEY(source_log_block_uuid, extractor_generation)
     ) STRICT, WITHOUT ROWID;
     CREATE TABLE IF NOT EXISTS occurrences (
         deployment_id BLOB NOT NULL CHECK(length(deployment_id) = 16),
         app_id BLOB NOT NULL CHECK(length(app_id) = 16),
         app_identity_sha256 BLOB NOT NULL CHECK(length(app_identity_sha256) = 32),
         event_id BLOB NOT NULL CHECK(length(event_id) = 16),
         occurred_at_unix_nano INTEGER NOT NULL,
         observed_at_unix_nano INTEGER,
         received_at_unix_nano INTEGER NOT NULL,
         trace_id BLOB CHECK(trace_id IS NULL OR length(trace_id) = 16),
         span_id BLOB CHECK(span_id IS NULL OR length(span_id) = 8),
         trace_flags INTEGER NOT NULL,
         canonical_version INTEGER NOT NULL,
         scrub_policy_version INTEGER NOT NULL,
         canonical_sha256 BLOB NOT NULL CHECK(length(canonical_sha256) = 32),
         canonical BLOB NOT NULL,
         occurrence_projection_key TEXT NOT NULL REFERENCES projection_commits(occurrence_projection_key),
         source_log_block_uuid BLOB NOT NULL CHECK(length(source_log_block_uuid) = 16),
         source_row_ordinal INTEGER NOT NULL,
         fold_seq INTEGER NOT NULL DEFAULT 0,
         PRIMARY KEY(deployment_id, app_id, app_identity_sha256, event_id)
     ) STRICT, WITHOUT ROWID;
     CREATE UNIQUE INDEX IF NOT EXISTS occurrences_by_fold_seq
         ON occurrences(fold_seq);
     CREATE TABLE IF NOT EXISTS occurrence_collisions (
         collision_id BLOB PRIMARY KEY CHECK(length(collision_id) = 32),
         deployment_id BLOB NOT NULL CHECK(length(deployment_id) = 16),
         app_id BLOB NOT NULL CHECK(length(app_id) = 16),
         event_id BLOB NOT NULL CHECK(length(event_id) = 16),
         winner_sha256 BLOB NOT NULL CHECK(length(winner_sha256) = 32),
         winner_canonical BLOB NOT NULL,
         contender_sha256 BLOB NOT NULL CHECK(length(contender_sha256) = 32),
         contender_canonical BLOB NOT NULL,
         contender_projection_key TEXT NOT NULL,
         contender_source_log_block_uuid BLOB NOT NULL CHECK(length(contender_source_log_block_uuid) = 16),
         contender_source_row_ordinal INTEGER NOT NULL
     ) STRICT, WITHOUT ROWID;
     CREATE TABLE IF NOT EXISTS reconcile_cursors (
         partition TEXT PRIMARY KEY,
         highest_key TEXT NOT NULL,
         updated_at_unix_nano INTEGER NOT NULL
     ) STRICT, WITHOUT ROWID;
     CREATE TABLE IF NOT EXISTS issues (
         deployment_id BLOB NOT NULL CHECK(length(deployment_id) = 16),
         issue_id BLOB NOT NULL CHECK(length(issue_id) = 16),
         grouping_generation TEXT NOT NULL,
         app_id BLOB NOT NULL CHECK(length(app_id) = 16),
         app_identity_sha256 BLOB NOT NULL CHECK(length(app_identity_sha256) = 32),
         fingerprint_version INTEGER NOT NULL,
         fingerprint_digest BLOB NOT NULL CHECK(length(fingerprint_digest) = 32),
         grouping_quality INTEGER NOT NULL,
         title TEXT NOT NULL,
         first_seen_unix_nano INTEGER NOT NULL,
         last_seen_unix_nano INTEGER NOT NULL,
         occurrence_count INTEGER NOT NULL,
         max_severity INTEGER NOT NULL,
         latest_event_id BLOB NOT NULL CHECK(length(latest_event_id) = 16),
         PRIMARY KEY (deployment_id, issue_id)
     ) STRICT, WITHOUT ROWID;
     CREATE INDEX IF NOT EXISTS issues_by_generation_last_seen
         ON issues(deployment_id, grouping_generation, last_seen_unix_nano DESC, issue_id);
     CREATE TABLE IF NOT EXISTS occurrence_issues (
         deployment_id BLOB NOT NULL CHECK(length(deployment_id) = 16),
         app_id BLOB NOT NULL CHECK(length(app_id) = 16),
         app_identity_sha256 BLOB NOT NULL CHECK(length(app_identity_sha256) = 32),
         event_id BLOB NOT NULL CHECK(length(event_id) = 16),
         grouping_generation TEXT NOT NULL,
         issue_id BLOB NOT NULL CHECK(length(issue_id) = 16),
         fingerprint_version INTEGER NOT NULL,
         fingerprint_digest BLOB NOT NULL CHECK(length(fingerprint_digest) = 32),
         sort_at_unix_nano INTEGER NOT NULL,
         PRIMARY KEY (deployment_id, app_id, app_identity_sha256, event_id, grouping_generation)
     ) STRICT, WITHOUT ROWID;
     CREATE INDEX IF NOT EXISTS occurrence_issues_by_issue
         ON occurrence_issues(deployment_id, issue_id, sort_at_unix_nano, event_id);
     CREATE TABLE IF NOT EXISTS grouping_cursors (
         grouping_generation TEXT PRIMARY KEY,
         last_fold_seq INTEGER NOT NULL
     ) STRICT, WITHOUT ROWID;
     CREATE TABLE IF NOT EXISTS grouping_failures (
         grouping_generation TEXT NOT NULL,
         fold_seq INTEGER NOT NULL,
         app_id BLOB NOT NULL CHECK(length(app_id) = 16),
         app_identity_sha256 BLOB NOT NULL CHECK(length(app_identity_sha256) = 32),
         event_id BLOB NOT NULL CHECK(length(event_id) = 16),
         reason TEXT NOT NULL,
         PRIMARY KEY (grouping_generation, fold_seq)
     ) STRICT, WITHOUT ROWID;
";

fn migrate(conn: &mut Connection) -> Result<(), SqliteError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
             version INTEGER PRIMARY KEY,
             applied_at_unix INTEGER NOT NULL
         ) STRICT;",
    )?;
    let applied: i64 = tx.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
        [],
        |row| row.get(0),
    )?;
    if applied > i64::from(ERRORS_SCHEMA_VERSION) {
        return Err(SqliteError::UnsupportedSchema {
            found: applied as u32,
            supported: ERRORS_SCHEMA_VERSION,
        });
    }
    if applied < 5 {
        // v5 re-keys the derived grouping tables (generation and full app identity
        // in the occurrence mapping, generation in the issue listing index) and
        // introduces fold sequencing. Pre-v5 grouping rows were produced with known
        // defects (hardcoded severity, first_seen never lowered, occurrences of
        // distinct app identities sharing one mapping row) and by fp-v1, which
        // grouping no longer uses, so they are discarded rather than converted;
        // fp-v2 regroups every occurrence from `occurrences`.
        tx.execute_batch(
            "DROP TABLE IF EXISTS occurrence_issues;
             DROP TABLE IF EXISTS issues;
             DROP INDEX IF EXISTS occurrences_time;",
        )?;
        let occurrences_exists: bool = tx
            .prepare("SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'occurrences'")?
            .exists([])?;
        if occurrences_exists {
            let has_fold_seq: bool = tx
                .prepare("SELECT 1 FROM pragma_table_info('occurrences') WHERE name = 'fold_seq'")?
                .exists([])?;
            if !has_fold_seq {
                // Existing rows get a deterministic sequence in occurrence order;
                // everything folded later sorts after them.
                tx.execute_batch(
                    "ALTER TABLE occurrences ADD COLUMN fold_seq INTEGER NOT NULL DEFAULT 0;
                     UPDATE occurrences SET fold_seq = ranked.seq
                     FROM (
                         SELECT deployment_id AS d, app_id AS a, app_identity_sha256 AS s,
                                event_id AS e,
                                row_number() OVER (
                                    ORDER BY occurred_at_unix_nano, event_id,
                                             deployment_id, app_id, app_identity_sha256
                                ) AS seq
                         FROM occurrences
                     ) AS ranked
                     WHERE occurrences.deployment_id = ranked.d
                       AND occurrences.app_id = ranked.a
                       AND occurrences.app_identity_sha256 = ranked.s
                       AND occurrences.event_id = ranked.e;",
                )?;
            }
        }
    }
    tx.execute_batch(SCHEMA_V5)?;
    for version in 1..=ERRORS_SCHEMA_VERSION {
        tx.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, applied_at_unix)
             VALUES (?1, unixepoch())",
            [version],
        )?;
    }
    tx.commit()?;
    // Stamp user_version so snapshot restore can version-check.
    // PRAGMA writes cannot run inside a transaction.
    conn.pragma_update(None, "user_version", ERRORS_SCHEMA_VERSION)?;
    Ok(())
}

fn bind_deployment(conn: &mut Connection, expected: [u8; 16]) -> Result<(), SqliteError> {
    let tx = conn.transaction()?;
    let existing: Option<Vec<u8>> = tx
        .query_row(
            "SELECT value FROM metadata WHERE key = ?1",
            [DEPLOYMENT_METADATA_KEY],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(existing) = existing {
        let actual: [u8; 16] =
            existing
                .as_slice()
                .try_into()
                .map_err(|_| SqliteError::InvalidLength {
                    field: "metadata.deployment_id",
                    expected: 16,
                    actual: existing.len(),
                })?;
        if actual != expected {
            return Err(SqliteError::DeploymentMismatch { expected, actual });
        }
    } else {
        // A database created before deployment metadata existed may already contain
        // rows. Bind it only if every such row belongs to the expected deployment.
        let conflicting_deployment: Option<Vec<u8>> = tx
            .query_row(
                "SELECT deployment_id FROM occurrences
                 WHERE deployment_id != ?1 LIMIT 1",
                [expected.as_slice()],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(conflicting_deployment) = conflicting_deployment {
            let actual: [u8; 16] = conflicting_deployment.as_slice().try_into().map_err(|_| {
                SqliteError::InvalidLength {
                    field: "occurrences.deployment_id",
                    expected: 16,
                    actual: conflicting_deployment.len(),
                }
            })?;
            return Err(SqliteError::DeploymentMismatch { expected, actual });
        }
        tx.execute(
            "INSERT INTO metadata(key, value) VALUES (?1, ?2)",
            params![DEPLOYMENT_METADATA_KEY, expected.as_slice()],
        )?;
    }
    tx.commit()?;
    Ok(())
}

fn check_fence(fence: &dyn Fence) -> Result<(), SqliteError> {
    fence
        .check()
        .map_err(|error| SqliteError::FenceLost(format!("{error:#}")))
}

fn grouping_cursor(conn: &Connection, generation: &str) -> Result<u64, SqliteError> {
    Ok(conn
        .prepare_cached(
            "SELECT last_fold_seq FROM grouping_cursors WHERE grouping_generation = ?1",
        )?
        .query_row([generation], |row| row.get::<_, i64>(0))
        .optional()?
        .unwrap_or(0) as u64)
}

fn active_generation(conn: &Connection) -> Result<Option<String>, SqliteError> {
    let value: Option<Vec<u8>> = conn
        .prepare_cached("SELECT value FROM metadata WHERE key = ?1")?
        .query_row([ACTIVE_GENERATION_METADATA_KEY], |row| row.get(0))
        .optional()?;
    Ok(value.map(|bytes| String::from_utf8_lossy(&bytes).into_owned()))
}

fn ensure_commit(
    tx: &Transaction<'_>,
    projection_key: &str,
    commit: &OccurrenceCommit,
    commit_json: &[u8],
) -> Result<(), SqliteError> {
    let existing: Option<Vec<u8>> = tx
        .query_row(
            "SELECT commit_json FROM projection_commits WHERE occurrence_projection_key = ?1",
            [projection_key],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(existing) = existing {
        return if existing == commit_json {
            Ok(())
        } else {
            Err(SqliteError::CommitConflict)
        };
    }
    tx.execute(
        "INSERT INTO projection_commits VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            projection_key,
            commit.source_log_block_uuid.as_bytes().as_slice(),
            commit.extractor_generation,
            commit.data_key,
            commit.data_sha256,
            to_sql_i64(commit.data_size_bytes),
            to_sql_i64(commit.row_count),
            to_sql_i64(commit.committed_at_unix_nano),
            commit_json,
        ],
    )?;
    Ok(())
}

fn validate_row(row: &OccurrenceRow<'_>, expected_deployment: [u8; 16]) -> Result<(), SqliteError> {
    for (field, bytes, expected) in [
        ("deployment_id", row.deployment_id, 16),
        ("app_id", row.app_id, 16),
        ("app_identity_sha256", row.app_identity_sha256, 32),
        ("event_id", row.event_id, 16),
        ("canonical_sha256", row.canonical_sha256, 32),
        ("source_log_block_uuid", row.source_log_block_uuid, 16),
    ] {
        if bytes.len() != expected {
            return Err(SqliteError::InvalidLength {
                field,
                expected,
                actual: bytes.len(),
            });
        }
    }
    for (field, bytes, expected) in [("trace_id", row.trace_id, 16), ("span_id", row.span_id, 8)] {
        if let Some(bytes) = bytes {
            if bytes.len() != expected {
                return Err(SqliteError::InvalidLength {
                    field,
                    expected,
                    actual: bytes.len(),
                });
            }
        }
    }
    if row.deployment_id != expected_deployment {
        let actual = row.deployment_id.try_into().expect("length checked above");
        return Err(SqliteError::DeploymentMismatch {
            expected: expected_deployment,
            actual,
        });
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn fold_row(
    tx: &Transaction<'_>,
    projection_key: &str,
    row: OccurrenceRow<'_>,
    next_fold_seq: &mut i64,
    report: &mut FoldReport,
) -> Result<(), SqliteError> {
    let existing: Option<(Vec<u8>, Vec<u8>)> = tx
        .prepare_cached(
            "SELECT canonical_sha256, canonical FROM occurrences
             WHERE deployment_id = ?1 AND app_id = ?2
               AND app_identity_sha256 = ?3 AND event_id = ?4",
        )?
        .query_row(
            params![
                row.deployment_id,
                row.app_id,
                row.app_identity_sha256,
                row.event_id
            ],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    if let Some((winner_sha256, winner_canonical)) = existing {
        // Digest equality is only the fast path into the authoritative byte compare.
        if winner_sha256.as_slice() == row.canonical_sha256
            && winner_canonical.as_slice() == row.canonical
        {
            report.exact_duplicates += 1;
            return Ok(());
        }
        let winner_digest: &[u8; 32] = winner_sha256
            .as_slice()
            .try_into()
            .expect("database CHECK enforces digest length");
        let contender_digest: &[u8; 32] = row
            .canonical_sha256
            .try_into()
            .expect("validated digest length");
        let deployment: &[u8; 16] = row.deployment_id.try_into().expect("validated ID length");
        let app: &[u8; 16] = row.app_id.try_into().expect("validated ID length");
        let event: &[u8; 16] = row.event_id.try_into().expect("validated ID length");
        let collision = CollisionMirror {
            deployment_id: deployment,
            app_id: app,
            event_id: event,
            winner_sha256: winner_digest,
            winner_canonical: &winner_canonical,
            contender_sha256: contender_digest,
            contender_canonical: row.canonical,
        };
        tx.execute(
            "INSERT OR IGNORE INTO occurrence_collisions VALUES
             (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                collision_id(&collision).as_slice(),
                row.deployment_id,
                row.app_id,
                row.event_id,
                winner_sha256,
                winner_canonical,
                row.canonical_sha256,
                row.canonical,
                projection_key,
                row.source_log_block_uuid,
                to_sql_i64(row.source_row_ordinal),
            ],
        )?;
        report.collisions += 1;
        return Ok(());
    }

    tx.prepare_cached(
        "INSERT INTO occurrences (
             deployment_id, app_id, app_identity_sha256, event_id,
             occurred_at_unix_nano, observed_at_unix_nano, received_at_unix_nano,
             trace_id, span_id, trace_flags, canonical_version, scrub_policy_version,
             canonical_sha256, canonical, occurrence_projection_key,
             source_log_block_uuid, source_row_ordinal, fold_seq
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16,
                   ?17, ?18)",
    )?
    .execute(params![
        row.deployment_id,
        row.app_id,
        row.app_identity_sha256,
        row.event_id,
        to_sql_i64(row.occurred_at_unix_nano),
        row.observed_at_unix_nano.map(to_sql_i64),
        to_sql_i64(row.received_at_unix_nano),
        row.trace_id,
        row.span_id,
        i64::from(row.trace_flags),
        i64::from(row.canonical_version),
        i64::from(row.scrub_policy_version),
        row.canonical_sha256,
        row.canonical,
        projection_key,
        row.source_log_block_uuid,
        to_sql_i64(row.source_row_ordinal),
        *next_fold_seq,
    ])?;
    *next_fold_seq += 1;
    report.inserted += 1;
    Ok(())
}

/// The ordering timestamp of an occurrence: its own time, but never more than
/// [`CLOCK_SKEW_ALLOWANCE_NANOS`] after the server received it.
pub fn clamped_occurrence_time(occurred_at_unix_nano: u64, received_at_unix_nano: u64) -> u64 {
    occurred_at_unix_nano.min(received_at_unix_nano.saturating_add(CLOCK_SKEW_ALLOWANCE_NANOS))
}

/// Record the occurrence → issue mapping and fold the occurrence into the page's
/// pending aggregate for its issue.
#[allow(clippy::too_many_arguments)]
fn map_occurrence(
    tx: &Transaction<'_>,
    deployment: [u8; 16],
    generation: &str,
    app_id: &[u8; 16],
    occurrence: &UngroupedOccurrence<'_>,
    grouped: GroupedOccurrence,
    pending: &mut HashMap<[u8; 16], PendingIssue>,
    report: &mut GroupingReport,
) -> Result<(), SqliteError> {
    let sort_at = to_sql_i64(clamped_occurrence_time(
        occurrence.occurred_at_unix_nano,
        occurrence.received_at_unix_nano,
    ));
    // The mapping row is the idempotence guard: a re-offered occurrence of this
    // generation changes nothing.
    let mapped = tx
        .prepare_cached(
            "INSERT INTO occurrence_issues
                 (deployment_id, app_id, app_identity_sha256, event_id, grouping_generation,
                  issue_id, fingerprint_version, fingerprint_digest, sort_at_unix_nano)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT DO NOTHING",
        )?
        .execute(params![
            deployment.as_slice(),
            app_id.as_slice(),
            occurrence.app_identity_sha256.as_slice(),
            occurrence.event_id.as_slice(),
            generation,
            grouped.issue_id.as_slice(),
            i64::from(grouped.fingerprint_version),
            grouped.fingerprint_digest.as_slice(),
            sort_at,
        ])?;
    if mapped == 0 {
        report.duplicate_skipped += 1;
        return Ok(());
    }
    report.occurrences_grouped += 1;

    match pending.entry(grouped.issue_id) {
        std::collections::hash_map::Entry::Occupied(mut entry) => {
            let issue = entry.get_mut();
            issue.occurrences += 1;
            issue.first_seen = issue.first_seen.min(sort_at);
            issue.max_severity = issue.max_severity.max(grouped.severity);
            if (sort_at, occurrence.event_id) > (issue.latest_sort_at, &issue.latest_event_id) {
                issue.latest_sort_at = sort_at;
                issue.latest_event_id = *occurrence.event_id;
            }
        }
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(PendingIssue {
                app_id: *app_id,
                app_identity_sha256: *occurrence.app_identity_sha256,
                fingerprint_version: grouped.fingerprint_version,
                fingerprint_digest: grouped.fingerprint_digest,
                grouping_quality: grouped.grouping_quality,
                title: grouped.title,
                first_seen: sort_at,
                latest_sort_at: sort_at,
                latest_event_id: *occurrence.event_id,
                max_severity: grouped.severity,
                occurrences: 1,
            });
        }
    }
    Ok(())
}

/// Create an issue from, or fold into it, one page's aggregate. The latest
/// occurrence is the maximum of (clamped time, event ID), so ties resolve
/// deterministically regardless of grouping order. The WHERE guard refuses to
/// merge generations.
fn upsert_issue(
    tx: &Transaction<'_>,
    deployment: [u8; 16],
    generation: &str,
    issue_id: &[u8; 16],
    issue: &PendingIssue,
    report: &mut GroupingReport,
) -> Result<(), SqliteError> {
    let count: Option<i64> = tx
        .prepare_cached(
            "INSERT INTO issues
                 (deployment_id, issue_id, grouping_generation, app_id, app_identity_sha256,
                  fingerprint_version, fingerprint_digest, grouping_quality, title,
                  first_seen_unix_nano, last_seen_unix_nano, occurrence_count,
                  max_severity, latest_event_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
             ON CONFLICT(deployment_id, issue_id) DO UPDATE SET
                 occurrence_count = occurrence_count + excluded.occurrence_count,
                 first_seen_unix_nano = MIN(first_seen_unix_nano, excluded.first_seen_unix_nano),
                 last_seen_unix_nano = MAX(last_seen_unix_nano, excluded.last_seen_unix_nano),
                 max_severity = MAX(max_severity, excluded.max_severity),
                 latest_event_id = CASE
                     WHEN (excluded.last_seen_unix_nano, excluded.latest_event_id)
                          > (last_seen_unix_nano, latest_event_id)
                     THEN excluded.latest_event_id ELSE latest_event_id END
             WHERE grouping_generation = excluded.grouping_generation
             RETURNING occurrence_count",
        )?
        .query_row(
            params![
                deployment.as_slice(),
                issue_id.as_slice(),
                generation,
                issue.app_id.as_slice(),
                issue.app_identity_sha256.as_slice(),
                i64::from(issue.fingerprint_version),
                issue.fingerprint_digest.as_slice(),
                i64::from(issue.grouping_quality),
                issue.title,
                issue.first_seen,
                issue.latest_sort_at,
                issue.occurrences,
                i64::from(issue.max_severity),
                issue.latest_event_id.as_slice(),
            ],
            |row| row.get(0),
        )
        .optional()?;
    match count {
        // The stored count equals this page's contribution only for a new issue.
        Some(count) if count == issue.occurrences => report.issues_created += 1,
        Some(_) => report.issues_updated += 1,
        None => {
            let existing: String = tx.query_row(
                "SELECT grouping_generation FROM issues WHERE deployment_id = ?1 AND issue_id = ?2",
                params![deployment.as_slice(), issue_id.as_slice()],
                |row| row.get(0),
            )?;
            return Err(SqliteError::GenerationConflict {
                existing,
                requested: generation.to_owned(),
            });
        }
    }
    Ok(())
}

fn record_failure(
    tx: &Transaction<'_>,
    generation: &str,
    app_id: &[u8; 16],
    occurrence: &UngroupedOccurrence<'_>,
    reason: &str,
) -> Result<(), SqliteError> {
    let mut cut = reason.len().min(MAX_FAILURE_REASON_BYTES);
    while !reason.is_char_boundary(cut) {
        cut -= 1;
    }
    tx.prepare_cached(
        "INSERT INTO grouping_failures
             (grouping_generation, fold_seq, app_id, app_identity_sha256, event_id, reason)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT DO NOTHING",
    )?
    .execute(params![
        generation,
        occurrence.fold_seq as i64,
        app_id.as_slice(),
        occurrence.app_identity_sha256.as_slice(),
        occurrence.event_id.as_slice(),
        &reason[..cut],
    ])?;
    Ok(())
}

fn issue_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<IssueSummary> {
    let issue_id: [u8; 16] = row.get(0)?;
    let app_id: [u8; 16] = row.get(1)?;
    Ok(IssueSummary {
        issue_id: uuid::Uuid::from_bytes(issue_id).to_string(),
        app_id: uuid::Uuid::from_bytes(app_id).to_string(),
        title: row.get(2)?,
        grouping_quality: row.get::<_, i64>(3)? as u8,
        first_seen_unix_nano: from_sql_i64(row.get(4)?),
        last_seen_unix_nano: from_sql_i64(row.get(5)?),
        occurrence_count: row.get::<_, i64>(6)? as u64,
        max_severity: row.get::<_, i64>(7)? as i32,
        fingerprint_version: row.get::<_, i64>(8)? as u16,
    })
}

fn blob<'r>(row: &'r rusqlite::Row<'_>, index: usize) -> Result<&'r [u8], SqliteError> {
    Ok(row
        .get_ref(index)?
        .as_blob()
        .map_err(rusqlite::Error::from)?)
}

fn fixed<'a, const N: usize>(
    bytes: &'a [u8],
    field: &'static str,
) -> Result<&'a [u8; N], SqliteError> {
    bytes.try_into().map_err(|_| SqliteError::InvalidLength {
        field,
        expected: N,
        actual: bytes.len(),
    })
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(DIGITS[usize::from(byte >> 4)] as char);
        out.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    out
}

/// SQLite INTEGER is signed; this mapping preserves the complete u64 ordering and bits.
fn to_sql_i64(value: u64) -> i64 {
    (value ^ (1_u64 << 63)) as i64
}

fn from_sql_i64(value: i64) -> u64 {
    (value as u64) ^ (1_u64 << 63)
}

#[cfg(test)]
pub(crate) mod tests {
    use scry_block::AlwaysValid;
    use sha2::{Digest, Sha256};
    use uuid::Uuid;

    use super::*;
    use crate::projection::{occurrence_keys, OccurrenceCommit};

    const DEPLOYMENT: [u8; 16] = [1; 16];
    const OTHER_DEPLOYMENT: [u8; 16] = [9; 16];
    const APP: [u8; 16] = [2; 16];
    const APP_DIGEST: [u8; 32] = [3; 32];
    const GEN: &str = "fp-test";

    fn memory_db() -> ErrorsDb {
        ErrorsDb::open_in_memory(DEPLOYMENT).unwrap()
    }

    fn temporary_db_path(test_name: &str) -> std::path::PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "scry-errors-{test_name}-{}-{unique}.sqlite",
            std::process::id()
        ))
    }

    fn remove_db(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    fn fixture<'a>(canonical: &'a [u8], digest: &'a [u8; 32]) -> OccurrenceRow<'a> {
        OccurrenceRow {
            deployment_id: &DEPLOYMENT,
            app_id: &APP,
            app_identity_sha256: &APP_DIGEST,
            event_id: &[4; 16],
            occurred_at_unix_nano: u64::MAX,
            observed_at_unix_nano: None,
            received_at_unix_nano: 7,
            trace_id: None,
            span_id: None,
            trace_flags: 1,
            canonical_version: 1,
            scrub_policy_version: 1,
            canonical_sha256: digest,
            canonical,
            source_log_block_uuid: &[5; 16],
            source_row_ordinal: 9,
        }
    }

    fn commit() -> (String, OccurrenceCommit, Vec<u8>) {
        let source = Uuid::from_bytes([5; 16]);
        let keys = occurrence_keys("2026-09-10", source, "v1").unwrap();
        let commit = OccurrenceCommit::new(source, "v1", keys.data, b"data", 1, 8).unwrap();
        let json = commit.canonical_json().unwrap();
        (keys.commit, commit, json)
    }

    /// Insert one occurrence per `(event byte, occurred_at, received_at)`.
    fn insert_occurrences(db: &mut ErrorsDb, events: &[([u8; 16], u64, u64)]) {
        let (key, commit, json) = commit();
        let canonical = b"occ".to_vec();
        let digest: [u8; 32] = Sha256::digest(&canonical).into();
        let rows: Vec<_> = events
            .iter()
            .map(|(event, occurred, received)| OccurrenceRow {
                event_id: event,
                occurred_at_unix_nano: *occurred,
                received_at_unix_nano: *received,
                ..fixture(&canonical, &digest)
            })
            .collect();
        db.fold_committed("2026-09-10", &key, &commit, &json, rows, &AlwaysValid)
            .unwrap();
    }

    /// A grouping callback that assigns every occurrence to the issue whose ID
    /// is the first byte of its event ID repeated, with severity from byte 1.
    fn by_event_prefix(occurrence: &UngroupedOccurrence<'_>) -> Result<GroupedOccurrence, String> {
        if occurrence.event_id[0] == 0xEE {
            return Err("poison".to_owned());
        }
        Ok(GroupedOccurrence {
            issue_id: [occurrence.event_id[0]; 16],
            fingerprint_version: 2,
            fingerprint_digest: [occurrence.event_id[0]; 32],
            grouping_quality: 0,
            title: format!("issue-{:02x}", occurrence.event_id[0]),
            severity: i32::from(occurrence.event_id[1]),
        })
    }

    fn event(issue: u8, severity: u8, unique: u8) -> [u8; 16] {
        let mut id = [0_u8; 16];
        id[0] = issue;
        id[1] = severity;
        id[15] = unique;
        id
    }

    /// Fold and group one occurrence into a database bound to `[1; 16]`,
    /// creating exactly one issue. For tests of other modules.
    pub(crate) fn group_one_issue(db: &mut ErrorsDb) {
        insert_occurrences(db, &[(event(0xA0, 17, 1), 1_000, 1_000)]);
        let report = db
            .group_page(GEN, 100, &AlwaysValid, by_event_prefix)
            .unwrap();
        assert_eq!(report.issues_created, 1);
    }

    #[test]
    fn exact_bytes_decide_duplicate_even_after_equal_digest() {
        let mut db = memory_db();
        let (key, commit, json) = commit();
        let digest: [u8; 32] = Sha256::digest(b"same").into();
        let fold = |db: &mut ErrorsDb, canonical: &[u8]| {
            db.fold_committed(
                "2026-09-10",
                &key,
                &commit,
                &json,
                [fixture(canonical, &digest)],
                &AlwaysValid,
            )
            .unwrap()
        };
        let first = fold(&mut db, b"same");
        let duplicate = fold(&mut db, b"same");
        // Artificial equal digest with different bytes proves bytes remain authoritative.
        let collision = fold(&mut db, b"other");
        assert_eq!(first.inserted, 1);
        assert_eq!(duplicate.exact_duplicates, 1);
        assert_eq!(collision.collisions, 1);
        assert_eq!(db.counts(), (1, 1, 1));
    }

    #[test]
    fn same_compact_app_id_with_distinct_full_digests_are_separate_identities() {
        let mut db = memory_db();
        let (key, commit, json) = commit();
        let digest: [u8; 32] = Sha256::digest(b"same").into();
        let first = fixture(b"same", &digest);
        let mut second = fixture(b"same", &digest);
        second.app_identity_sha256 = &[8; 32];

        let report = db
            .fold_committed(
                "2026-09-10",
                &key,
                &commit,
                &json,
                [first, second],
                &AlwaysValid,
            )
            .unwrap();

        assert_eq!(report.inserted, 2);
        assert_eq!(report.exact_duplicates, 0);
        assert_eq!(report.collisions, 0);
        assert_eq!(db.counts(), (2, 0, 1));
    }

    #[test]
    fn oversized_batch_rolls_back_commit_and_rows() {
        let mut db = memory_db();
        db.set_max_rows_per_transaction(1);
        let (key, commit, json) = commit();
        let digest: [u8; 32] = Sha256::digest(b"same").into();
        let result = db.fold_committed(
            "2026-09-10",
            &key,
            &commit,
            &json,
            [fixture(b"same", &digest), fixture(b"same", &digest)],
            &AlwaysValid,
        );
        assert!(matches!(result, Err(SqliteError::TransactionLimit { .. })));
        assert_eq!(db.counts(), (0, 0, 0));
    }

    #[test]
    fn conflicting_commit_exact_bytes_are_rejected() {
        let mut db = memory_db();
        let (key, commit, json) = commit();
        db.fold_committed("2026-09-10", &key, &commit, &json, [], &AlwaysValid)
            .unwrap();
        assert!(matches!(
            db.fold_committed("2026-09-10", &key, &commit, b"different", [], &AlwaysValid),
            Err(SqliteError::CommitConflict)
        ));
    }

    struct LostFence;
    impl Fence for LostFence {
        fn check(&self) -> anyhow::Result<()> {
            anyhow::bail!("lease lost")
        }
    }

    #[test]
    fn lost_fence_rolls_back_fold_and_grouping() {
        let mut db = memory_db();
        let (key, commit, json) = commit();
        let digest: [u8; 32] = Sha256::digest(b"same").into();
        let result = db.fold_committed(
            "2026-09-10",
            &key,
            &commit,
            &json,
            [fixture(b"same", &digest)],
            &LostFence,
        );
        assert!(matches!(result, Err(SqliteError::FenceLost(_))));
        assert_eq!(db.counts(), (0, 0, 0));
        assert!(!db.has_projection_commit(&key).unwrap());

        insert_occurrences(&mut db, &[(event(0xA0, 17, 1), 10, 10)]);
        let result = db.group_page(GEN, 10, &LostFence, by_event_prefix);
        assert!(matches!(result, Err(SqliteError::FenceLost(_))));
        assert_eq!(db.grouping_progress(GEN).unwrap().cursor, 0);
        assert!(db.active_grouping_generation().unwrap().is_none());
    }

    #[test]
    fn projection_commit_and_source_coverage_are_queryable() {
        let mut db = memory_db();
        let (key, commit, json) = commit();
        assert!(!db.has_projection_commit(&key).unwrap());
        assert!(!db.is_source_covered(&[5; 16], "v1").unwrap());
        db.fold_committed("2026-09-10", &key, &commit, &json, [], &AlwaysValid)
            .unwrap();
        assert!(db.has_projection_commit(&key).unwrap());
        assert!(db.is_source_covered(&[5; 16], "v1").unwrap());
        assert!(!db.is_source_covered(&[5; 16], "v2").unwrap());
        assert!(!db.is_source_covered(&[6; 16], "v1").unwrap());
    }

    #[test]
    fn cursor_round_trips_full_u64_range() {
        let mut db = memory_db();
        let cursor = ReconcileCursor {
            partition: "2026-09".into(),
            highest_key: "z".into(),
            updated_at_unix_nano: u64::MAX,
        };
        db.set_reconcile_cursor(&cursor).unwrap();
        assert_eq!(db.reconcile_cursor("2026-09").unwrap(), Some(cursor));
    }

    #[test]
    fn reopening_with_another_deployment_is_rejected() {
        let path = temporary_db_path("reopen");
        ErrorsDb::open(&path, DEPLOYMENT).unwrap();

        let result = ErrorsDb::open(&path, OTHER_DEPLOYMENT);
        assert!(matches!(
            result,
            Err(SqliteError::DeploymentMismatch {
                expected: OTHER_DEPLOYMENT,
                actual: DEPLOYMENT,
            })
        ));
        ErrorsDb::open(&path, DEPLOYMENT).unwrap();
        remove_db(&path);
    }

    #[test]
    fn unbound_empty_database_is_bound_on_open() {
        let path = temporary_db_path("unbound");
        Connection::open(&path)
            .unwrap()
            .execute_batch(
                "CREATE TABLE schema_migrations (
                     version INTEGER PRIMARY KEY,
                     applied_at_unix INTEGER NOT NULL
                 ) STRICT;",
            )
            .unwrap();

        ErrorsDb::open(&path, DEPLOYMENT).unwrap();
        assert!(matches!(
            ErrorsDb::open(&path, OTHER_DEPLOYMENT),
            Err(SqliteError::DeploymentMismatch { .. })
        ));
        remove_db(&path);
    }

    #[test]
    fn newer_schema_is_refused() {
        let path = temporary_db_path("newer");
        ErrorsDb::open(&path, DEPLOYMENT).unwrap();
        Connection::open(&path)
            .unwrap()
            .execute(
                "INSERT INTO schema_migrations(version, applied_at_unix) VALUES (?1, 0)",
                [ERRORS_SCHEMA_VERSION + 1],
            )
            .unwrap();
        assert!(matches!(
            ErrorsDb::open(&path, DEPLOYMENT),
            Err(SqliteError::UnsupportedSchema { .. })
        ));
        remove_db(&path);
    }

    /// A v4 database keeps its occurrences (with a fold sequence in occurrence
    /// order) and loses only the derived grouping tables.
    #[test]
    fn v4_database_is_upgraded_in_place() {
        let path = temporary_db_path("v4upgrade");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE schema_migrations (
                     version INTEGER PRIMARY KEY, applied_at_unix INTEGER NOT NULL) STRICT;
                 INSERT INTO schema_migrations VALUES (1,0),(2,0),(3,0),(4,0);
                 CREATE TABLE metadata (key TEXT PRIMARY KEY, value BLOB NOT NULL) STRICT, WITHOUT ROWID;
                 CREATE TABLE projection_commits (
                     occurrence_projection_key TEXT PRIMARY KEY,
                     source_log_block_uuid BLOB NOT NULL, extractor_generation TEXT NOT NULL,
                     data_key TEXT NOT NULL, data_sha256 TEXT NOT NULL,
                     data_size_bytes INTEGER NOT NULL, row_count INTEGER NOT NULL,
                     committed_at_unix_nano INTEGER NOT NULL, commit_json BLOB NOT NULL) STRICT;
                 CREATE TABLE occurrences (
                     deployment_id BLOB NOT NULL, app_id BLOB NOT NULL,
                     app_identity_sha256 BLOB NOT NULL, event_id BLOB NOT NULL,
                     occurred_at_unix_nano INTEGER NOT NULL, observed_at_unix_nano INTEGER,
                     received_at_unix_nano INTEGER NOT NULL, trace_id BLOB, span_id BLOB,
                     trace_flags INTEGER NOT NULL, canonical_version INTEGER NOT NULL,
                     scrub_policy_version INTEGER NOT NULL, canonical_sha256 BLOB NOT NULL,
                     canonical BLOB NOT NULL, occurrence_projection_key TEXT NOT NULL,
                     source_log_block_uuid BLOB NOT NULL, source_row_ordinal INTEGER NOT NULL,
                     PRIMARY KEY(deployment_id, app_id, app_identity_sha256, event_id)
                 ) STRICT, WITHOUT ROWID;
                 CREATE INDEX occurrences_time ON occurrences(occurred_at_unix_nano DESC, event_id);
                 CREATE TABLE issues (issue_id BLOB NOT NULL, deployment_id BLOB NOT NULL,
                     PRIMARY KEY (deployment_id, issue_id)) STRICT, WITHOUT ROWID;
                 CREATE TABLE occurrence_issues (deployment_id BLOB NOT NULL,
                     app_id BLOB NOT NULL, event_id BLOB NOT NULL,
                     PRIMARY KEY (deployment_id, app_id, event_id)) STRICT, WITHOUT ROWID;",
            )
            .unwrap();
            for (event, occurred) in [(0xB0_u8, 30_u64), (0xA0, 10), (0xC0, 20)] {
                conn.execute(
                    "INSERT INTO occurrences VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?5, NULL, NULL,
                         1, 1, 1, ?6, x'00', 'k', ?2, 0)",
                    params![
                        DEPLOYMENT.as_slice(),
                        APP.as_slice(),
                        APP_DIGEST.as_slice(),
                        [event; 16].as_slice(),
                        to_sql_i64(occurred),
                        [0_u8; 32].as_slice(),
                    ],
                )
                .unwrap();
            }
        }
        let mut db = ErrorsDb::open(&path, DEPLOYMENT).unwrap();
        let mut order = Vec::new();
        db.group_page(GEN, 10, &AlwaysValid, |occurrence| {
            order.push((occurrence.fold_seq, occurrence.event_id[0]));
            by_event_prefix(occurrence)
        })
        .unwrap();
        assert_eq!(order, vec![(1, 0xA0), (2, 0xC0), (3, 0xB0)]);
        assert_eq!(db.list_issues(10).unwrap().len(), 3);
        let version: u32 = db
            .conn
            .pragma_query_value(None, "user_version", |r| r.get(0))
            .unwrap();
        assert_eq!(version, ERRORS_SCHEMA_VERSION);
        drop(db);
        remove_db(&path);
    }

    #[test]
    fn deployment_mismatch_in_late_row_precedes_all_transaction_writes() {
        let mut db = memory_db();
        let (key, commit, json) = commit();
        let digest: [u8; 32] = Sha256::digest(b"same").into();
        let first = fixture(b"same", &digest);
        let mut wrong_deployment = fixture(b"same", &digest);
        wrong_deployment.deployment_id = &OTHER_DEPLOYMENT;

        assert!(matches!(
            db.fold_committed(
                "2026-09-10",
                &key,
                &commit,
                &json,
                [first, wrong_deployment],
                &AlwaysValid,
            ),
            Err(SqliteError::DeploymentMismatch {
                expected: DEPLOYMENT,
                actual: OTHER_DEPLOYMENT,
            })
        ));
        assert_eq!(db.counts(), (0, 0, 0));
    }

    #[test]
    fn grouping_creates_and_updates_issues_idempotently() {
        let mut db = memory_db();
        insert_occurrences(
            &mut db,
            &[
                (event(0xA0, 17, 1), 1_000, 1_000),
                (event(0xA0, 21, 2), 2_000, 2_000),
                (event(0xB0, 9, 3), 1_500, 1_500),
            ],
        );
        let report = db
            .group_page(GEN, 100, &AlwaysValid, by_event_prefix)
            .unwrap();
        // Both occurrences of 0xA0 fold into one upsert within the page.
        assert_eq!(report.issues_created, 2);
        assert_eq!(report.issues_updated, 0);
        assert_eq!(report.occurrences_grouped, 3);
        assert!(report.drained);

        // The cursor has passed everything: nothing is offered again.
        let again = db
            .group_page(GEN, 100, &AlwaysValid, by_event_prefix)
            .unwrap();
        assert_eq!(again.scanned, 0);

        let issue = db.get_issue(&[0xA0; 16]).unwrap().unwrap();
        assert_eq!(issue.occurrence_count, 2);
        assert_eq!(issue.first_seen_unix_nano, 1_000);
        assert_eq!(issue.last_seen_unix_nano, 2_000);
        assert_eq!(issue.max_severity, 21);
        assert_eq!(issue.title, "issue-a0");
        assert!(db.get_issue(&[0xFF; 16]).unwrap().is_none());
    }

    #[test]
    fn late_occurrence_lowers_first_seen_and_is_not_skipped() {
        let mut db = memory_db();
        insert_occurrences(&mut db, &[(event(0xA0, 17, 1), 5_000, 5_000)]);
        db.group_page(GEN, 100, &AlwaysValid, by_event_prefix)
            .unwrap();
        // Folded later but occurred earlier than everything already grouped.
        insert_occurrences(&mut db, &[(event(0xA0, 3, 2), 1_000, 6_000)]);
        let report = db
            .group_page(GEN, 100, &AlwaysValid, by_event_prefix)
            .unwrap();
        assert_eq!(report.occurrences_grouped, 1);
        let issue = db.get_issue(&[0xA0; 16]).unwrap().unwrap();
        assert_eq!(issue.first_seen_unix_nano, 1_000);
        assert_eq!(issue.last_seen_unix_nano, 5_000);
        assert_eq!(issue.max_severity, 17);
    }

    #[test]
    fn latest_event_ties_break_on_event_id_regardless_of_order() {
        for order in [[1_u8, 2], [2, 1]] {
            let mut db = memory_db();
            for unique in order {
                insert_occurrences(&mut db, &[(event(0xA0, 17, unique), 1_000, 1_000)]);
            }
            db.group_page(GEN, 100, &AlwaysValid, by_event_prefix)
                .unwrap();
            let latest: [u8; 16] = db
                .conn
                .query_row("SELECT latest_event_id FROM issues", [], |r| r.get(0))
                .unwrap();
            assert_eq!(latest, event(0xA0, 17, 2));
        }
    }

    #[test]
    fn issue_aggregates_do_not_depend_on_page_size() {
        type IssueRow = (Vec<u8>, u64, u64, i64, i64, Vec<u8>, String);
        let events = [
            (event(0xA0, 17, 1), 5_000, 5_000),
            (event(0xB0, 9, 2), 1_500, 1_500),
            (event(0xA0, 21, 3), 2_000, 2_000),
            (event(0xA0, 3, 4), 5_000, 5_000),
            (event(0xB0, 12, 5), 900, 900),
            (event(0xA0, 17, 6), 1_000, 1_000),
            (event(0xB0, 1, 7), 1_500, 1_500),
        ];
        let rows_for = |page_rows: usize| {
            let mut db = memory_db();
            insert_occurrences(&mut db, &events);
            let mut total = GroupingReport::default();
            loop {
                let page = db
                    .group_page(GEN, page_rows, &AlwaysValid, by_event_prefix)
                    .unwrap();
                total.absorb(page);
                if page.drained {
                    break;
                }
            }
            assert_eq!(total.occurrences_grouped, events.len());
            let mut stmt = db
                .conn
                .prepare(
                    "SELECT issue_id, first_seen_unix_nano, last_seen_unix_nano,
                            occurrence_count, max_severity, latest_event_id, title
                     FROM issues ORDER BY issue_id",
                )
                .unwrap();
            let rows: Vec<IssueRow> = stmt
                .query_map([], |r| {
                    Ok((
                        r.get(0)?,
                        from_sql_i64(r.get(1)?),
                        from_sql_i64(r.get(2)?),
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                    ))
                })
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            (rows, total)
        };
        let (single, single_report) = rows_for(1);
        assert_eq!(single_report.issues_created, 2);
        assert_eq!(single_report.issues_updated, events.len() - 2);
        assert_eq!(
            single,
            vec![
                // Ties on time break on the event ID bytes: (A0, 17, 1) > (A0, 3, 4).
                (
                    vec![0xA0; 16],
                    1_000,
                    5_000,
                    4,
                    21,
                    event(0xA0, 17, 1).to_vec(),
                    "issue-a0".into()
                ),
                (
                    vec![0xB0; 16],
                    900,
                    1_500,
                    3,
                    12,
                    event(0xB0, 9, 2).to_vec(),
                    "issue-b0".into()
                ),
            ]
        );
        for page_rows in [2, 3, 100] {
            assert_eq!(rows_for(page_rows).0, single, "page size {page_rows}");
        }
    }

    #[test]
    fn future_skewed_occurrence_is_clamped_for_ordering() {
        let mut db = memory_db();
        let received = 1_000_000;
        let far_future = received + 10 * CLOCK_SKEW_ALLOWANCE_NANOS;
        insert_occurrences(
            &mut db,
            &[
                (event(0xA0, 17, 1), far_future, received),
                (event(0xA0, 17, 2), received + 1, received + 1 + 10),
            ],
        );
        db.group_page(GEN, 100, &AlwaysValid, by_event_prefix)
            .unwrap();
        let issue = db.get_issue(&[0xA0; 16]).unwrap().unwrap();
        assert_eq!(
            issue.last_seen_unix_nano,
            received + CLOCK_SKEW_ALLOWANCE_NANOS
        );
        // Ordering uses the clamped time, but the summary reports the
        // occurrence's own timestamp.
        let occurrences = db.list_occurrences_for_issue(&[0xA0; 16], 10).unwrap();
        assert_eq!(
            occurrences[0].event_id,
            Uuid::from_bytes(event(0xA0, 17, 1)).to_string()
        );
        assert_eq!(occurrences[0].occurred_at_unix_nano, far_future);
    }

    #[test]
    fn grouping_pages_resume_from_cursor() {
        let mut db = memory_db();
        let events: Vec<_> = (0..10_u8)
            .map(|i| (event(0xA0, 1, i), 1_000 - u64::from(i), 1_000))
            .collect();
        insert_occurrences(&mut db, &events);
        let mut seen = Vec::new();
        loop {
            let page = db
                .group_page(GEN, 3, &AlwaysValid, |occurrence| {
                    seen.push(occurrence.fold_seq);
                    by_event_prefix(occurrence)
                })
                .unwrap();
            if page.drained {
                break;
            }
        }
        assert_eq!(seen, (1..=10).collect::<Vec<u64>>());
        assert_eq!(
            db.get_issue(&[0xA0; 16]).unwrap().unwrap().occurrence_count,
            10
        );
        let progress = db.grouping_progress(GEN).unwrap();
        assert_eq!((progress.cursor, progress.head), (10, 10));
    }

    #[test]
    fn poison_occurrence_is_recorded_and_skipped() {
        let mut db = memory_db();
        insert_occurrences(
            &mut db,
            &[
                (event(0xEE, 1, 1), 1_000, 1_000),
                (event(0xA0, 1, 2), 2_000, 2_000),
            ],
        );
        let report = db
            .group_page(GEN, 100, &AlwaysValid, by_event_prefix)
            .unwrap();
        assert_eq!(report.failures, 1);
        assert_eq!(report.occurrences_grouped, 1);
        let failures = db.grouping_failures(GEN, 10).unwrap();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].event_id, event(0xEE, 1, 1));
        assert_eq!(failures[0].reason, "poison");
        assert_eq!(db.grouping_progress(GEN).unwrap().failures, 1);
        // The poison row is not re-offered.
        let again = db
            .group_page(GEN, 100, &AlwaysValid, by_event_prefix)
            .unwrap();
        assert_eq!(again.scanned, 0);
    }

    #[test]
    fn same_event_id_under_distinct_app_identities_groups_separately() {
        let mut db = memory_db();
        let (key, commit, json) = commit();
        let digest: [u8; 32] = Sha256::digest(b"same").into();
        let first = fixture(b"same", &digest);
        let mut second = fixture(b"same", &digest);
        second.app_identity_sha256 = &[8; 32];
        db.fold_committed(
            "2026-09-10",
            &key,
            &commit,
            &json,
            [first, second],
            &AlwaysValid,
        )
        .unwrap();
        let report = db
            .group_page(GEN, 100, &AlwaysValid, |occurrence| {
                Ok(GroupedOccurrence {
                    issue_id: [occurrence.app_identity_sha256[0]; 16],
                    fingerprint_version: 2,
                    fingerprint_digest: [0; 32],
                    grouping_quality: 0,
                    title: "t".to_owned(),
                    severity: 1,
                })
            })
            .unwrap();
        assert_eq!(report.occurrences_grouped, 2);
        assert_eq!(report.duplicate_skipped, 0);
        assert_eq!(
            db.list_occurrences_for_issue(&[3; 16], 10).unwrap().len(),
            1
        );
        assert_eq!(
            db.list_occurrences_for_issue(&[8; 16], 10).unwrap().len(),
            1
        );
    }

    #[test]
    fn generations_coexist_and_listing_follows_the_active_one() {
        let mut db = memory_db();
        insert_occurrences(
            &mut db,
            &[
                (event(0xA0, 1, 1), 1_000, 1_000),
                (event(0xB0, 1, 2), 2_000, 2_000),
            ],
        );
        db.group_page("fp-old", 100, &AlwaysValid, by_event_prefix)
            .unwrap();
        assert_eq!(
            db.active_grouping_generation().unwrap().as_deref(),
            Some("fp-old")
        );
        let regroup = |occurrence: &UngroupedOccurrence<'_>| {
            let mut grouped = by_event_prefix(occurrence)?;
            grouped.issue_id = [occurrence.event_id[0] + 1; 16];
            Ok(grouped)
        };
        // A partial regroup does not switch readers to the new generation.
        let partial = db.group_page("fp-new", 1, &AlwaysValid, regroup).unwrap();
        assert!(!partial.drained);
        assert_eq!(
            db.active_grouping_generation().unwrap().as_deref(),
            Some("fp-old")
        );
        let listed: Vec<_> = db
            .list_issues(10)
            .unwrap()
            .into_iter()
            .map(|i| i.title)
            .collect();
        assert_eq!(listed, vec!["issue-b0", "issue-a0"]);
        // Draining activates it; old issues remain but are no longer listed.
        let rest = db.group_page("fp-new", 100, &AlwaysValid, regroup).unwrap();
        assert!(rest.drained);
        assert_eq!(
            db.active_grouping_generation().unwrap().as_deref(),
            Some("fp-new")
        );
        let listed: Vec<_> = db
            .list_issues(10)
            .unwrap()
            .into_iter()
            .map(|i| i.issue_id)
            .collect();
        assert_eq!(
            listed,
            vec![
                Uuid::from_bytes([0xB1; 16]).to_string(),
                Uuid::from_bytes([0xA1; 16]).to_string()
            ]
        );
        assert!(db.get_issue(&[0xA0; 16]).unwrap().is_some());
    }

    #[test]
    fn reusing_an_issue_id_across_generations_is_refused() {
        let mut db = memory_db();
        insert_occurrences(&mut db, &[(event(0xA0, 1, 1), 1_000, 1_000)]);
        db.group_page("fp-a", 100, &AlwaysValid, by_event_prefix)
            .unwrap();
        let result = db.group_page("fp-b", 100, &AlwaysValid, by_event_prefix);
        assert!(matches!(
            result,
            Err(SqliteError::GenerationConflict { .. })
        ));
        // Rolled back: the new generation's cursor did not move.
        assert_eq!(db.grouping_progress("fp-b").unwrap().cursor, 0);
    }

    #[test]
    fn list_issues_orders_by_last_seen_then_issue_id_and_clamps_limit() {
        let mut db = memory_db();
        insert_occurrences(
            &mut db,
            &[
                (event(0xA0, 10, 1), 1_000, 1_000),
                (event(0xB0, 20, 2), 5_000, 5_000),
                (event(0xC0, 20, 3), 5_000, 5_000),
            ],
        );
        db.group_page(GEN, 100, &AlwaysValid, by_event_prefix)
            .unwrap();
        let issues = db.list_issues(10).unwrap();
        let titles: Vec<_> = issues.iter().map(|i| i.title.as_str()).collect();
        assert_eq!(titles, vec!["issue-b0", "issue-c0", "issue-a0"]);
        assert_eq!(issues[0].max_severity, 20);
        assert_eq!(issues[0].last_seen_unix_nano, 5_000);
        assert_eq!(db.list_issues(1).unwrap()[0].title, "issue-b0");
        assert_eq!(db.list_issues(usize::MAX).unwrap().len(), 3);
    }

    #[test]
    fn list_occurrences_orders_newest_first_with_event_tie_break() {
        let mut db = memory_db();
        insert_occurrences(
            &mut db,
            &[
                (event(0xA0, 1, 1), 1_000, 1_000),
                (event(0xA0, 1, 3), 2_000, 2_000),
                (event(0xA0, 1, 2), 2_000, 2_000),
                (event(0xB0, 1, 4), 9_000, 9_000),
            ],
        );
        db.group_page(GEN, 100, &AlwaysValid, by_event_prefix)
            .unwrap();
        let listed: Vec<_> = db
            .list_occurrences_for_issue(&[0xA0; 16], 10)
            .unwrap()
            .into_iter()
            .map(|o| (o.event_id, o.occurred_at_unix_nano))
            .collect();
        let id = |unique| Uuid::from_bytes(event(0xA0, 1, unique)).to_string();
        assert_eq!(listed, vec![(id(3), 2_000), (id(2), 2_000), (id(1), 1_000)]);
        assert_eq!(
            db.list_occurrences_for_issue(&[0xA0; 16], 1).unwrap().len(),
            1
        );
        assert!(db
            .list_occurrences_for_issue(&[0xFF; 16], 10)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn hot_queries_use_indexes_without_temp_sorts() {
        let db = memory_db();
        for (name, plan) in db.explain_hot_queries().unwrap() {
            assert!(
                !plan.contains("USE TEMP B-TREE"),
                "{name} sorts in a temp b-tree: {plan}"
            );
            match name {
                "list_issues" => assert!(
                    plan.contains("USING INDEX issues_by_generation_last_seen"),
                    "{plan}"
                ),
                "get_issue" => assert!(plan.contains("USING PRIMARY KEY"), "{plan}"),
                "list_occurrences_for_issue" => {
                    assert!(
                        plan.contains("USING COVERING INDEX occurrence_issues_by_issue")
                            || plan.contains("USING INDEX occurrence_issues_by_issue"),
                        "{plan}"
                    );
                    assert!(plan.contains("o USING PRIMARY KEY"), "{plan}");
                }
                "ungrouped_page" => {
                    assert!(plan.contains("occurrences_by_fold_seq"), "{plan}")
                }
                other => panic!("unexpected query {other}"),
            }
        }
    }

    #[test]
    fn open_read_only_reads_issues_and_binds_deployment() {
        let path = temporary_db_path("readonly");
        {
            let mut db = ErrorsDb::open(&path, DEPLOYMENT).unwrap();
            insert_occurrences(&mut db, &[(event(0xA0, 15, 1), 3_000, 3_000)]);
            db.group_page(GEN, 100, &AlwaysValid, by_event_prefix)
                .unwrap();
        }

        let ro = ErrorsDb::open_read_only(&path).unwrap();
        assert_eq!(ro.deployment_id(), DEPLOYMENT);
        let issues = ro.list_issues(10).unwrap();
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].title, "issue-a0");
        assert_eq!(issues[0].last_seen_unix_nano, 3_000);
        assert_eq!(issues[0].max_severity, 15);
        assert_eq!(issues[0].fingerprint_version, 2);
        assert!(Uuid::parse_str(&issues[0].issue_id).is_ok());
        assert!(ro.get_issue(&[0xA0; 16]).unwrap().is_some());
        assert_eq!(
            ro.list_occurrences_for_issue(&[0xA0; 16], 10)
                .unwrap()
                .len(),
            1
        );
        drop(ro);
        remove_db(&path);
    }

    #[test]
    fn open_read_only_refuses_other_schema_versions() {
        let path = temporary_db_path("readonly-version");
        ErrorsDb::open(&path, DEPLOYMENT).unwrap();
        Connection::open(&path)
            .unwrap()
            .pragma_update(None, "user_version", ERRORS_SCHEMA_VERSION - 1)
            .unwrap();
        assert!(matches!(
            ErrorsDb::open_read_only(&path),
            Err(SqliteError::UnsupportedSchema { .. })
        ));
        remove_db(&path);
    }

    #[test]
    fn hex_formats_lowercase_pairs() {
        assert_eq!(hex(&[0x00, 0x0f, 0xa0, 0xff]), "000fa0ff");
    }
}
