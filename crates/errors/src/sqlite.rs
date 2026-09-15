//! Rebuildable SQLite fold for the occurrence projection.
//!
//! Every public write is one bounded transaction. Digests are accelerators only:
//! duplicate decisions compare the exact canonical bytes after digest equality.

use std::path::Path;

use rusqlite::{params, Connection, OptionalExtension, Transaction};

use crate::projection::{occurrence_keys, OccurrenceCommit};
use crate::quarantine::{collision_id, CollisionMirror};

pub const ERRORS_SCHEMA_VERSION: u32 = 3;
pub const DEFAULT_MAX_ROWS_PER_TRANSACTION: usize = 4_096;
const DEPLOYMENT_METADATA_KEY: &str = "deployment_id";

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

/// An occurrence that has not yet been grouped for a given generation.
#[derive(Debug, Clone)]
pub struct UngroupedOccurrence {
    pub deployment_id: Vec<u8>,
    pub app_id: Vec<u8>,
    pub app_identity_sha256: Vec<u8>,
    pub event_id: Vec<u8>,
    pub occurred_at_unix_nano: u64,
    pub received_at_unix_nano: u64,
    pub canonical: Vec<u8>,
    pub canonical_sha256: Vec<u8>,
}

/// Input for folding one occurrence's grouping result into the issue tables.
#[derive(Debug, Clone)]
pub struct GroupingRow {
    pub deployment_id: Vec<u8>,
    pub app_id: Vec<u8>,
    pub app_identity_sha256: Vec<u8>,
    pub event_id: Vec<u8>,
    pub issue_id: Vec<u8>,
    pub fingerprint_version: u16,
    pub fingerprint_digest: Vec<u8>,
    pub grouping_quality: u8,
    pub grouping_generation: String,
    pub title: String,
    pub occurred_at_unix_nano: u64,
    pub severity: i32,
}

/// An issue summary for API responses.
#[derive(Debug, Clone, serde::Serialize)]
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

/// Report from a grouping fold pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GroupingReport {
    pub issues_created: usize,
    pub issues_updated: usize,
    pub occurrences_grouped: usize,
    pub duplicate_skipped: usize,
}

pub struct ErrorsDb {
    conn: Connection,
    deployment_id: [u8; 16],
    max_rows_per_transaction: usize,
}

impl ErrorsDb {
    pub fn open(path: &Path, deployment_id: [u8; 16]) -> Result<Self, SqliteError> {
        Self::from_connection(Connection::open(path)?, deployment_id)
    }

    pub fn open_in_memory(deployment_id: [u8; 16]) -> Result<Self, SqliteError> {
        Self::from_connection(Connection::open_in_memory()?, deployment_id)
    }

    /// Open an existing errors database **read-only**. Skips migration and
    /// deployment binding since a read-only connection cannot write. WAL mode
    /// allows concurrent readers alongside the single writer.
    pub fn open_read_only(path: &Path) -> Result<Self, SqliteError> {
        let flags =
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let conn = Connection::open_with_flags(path, flags)?;
        Ok(Self {
            conn,
            deployment_id: [0; 16],
            max_rows_per_transaction: DEFAULT_MAX_ROWS_PER_TRANSACTION,
        })
    }

    fn from_connection(mut conn: Connection, deployment_id: [u8; 16]) -> Result<Self, SqliteError> {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        migrate(&conn)?;
        bind_deployment(&mut conn, deployment_id)?;
        Ok(Self {
            conn,
            deployment_id,
            max_rows_per_transaction: DEFAULT_MAX_ROWS_PER_TRANSACTION,
        })
    }

    pub fn set_max_rows_per_transaction(&mut self, limit: usize) {
        self.max_rows_per_transaction = limit;
    }

    /// Atomically records a committed generation, source coverage, and its rows.
    ///
    /// `projection_key` is the commit marker key and therefore the durable generation
    /// identity. Callers must feed generations in deterministic key order.
    pub fn fold_committed<'a, I>(
        &mut self,
        source_date: &str,
        projection_key: &str,
        commit: &OccurrenceCommit,
        commit_json: &[u8],
        rows: I,
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

        let tx = self.conn.transaction()?;
        ensure_commit(&tx, projection_key, commit, commit_json)?;
        let mut report = FoldReport::default();
        for row in validated_rows {
            fold_row(&tx, projection_key, row, &mut report)?;
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
        tx.commit()?;
        Ok(report)
    }

    pub fn set_reconcile_cursor(&mut self, cursor: &ReconcileCursor) -> Result<(), SqliteError> {
        let tx = self.conn.transaction()?;
        tx.execute(
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
        tx.commit()?;
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

    /// Read a bounded page of occurrences not yet grouped for the given
    /// generation, ordered by `(occurred_at_unix_nano, event_id)` after an
    /// optional cursor.
    pub fn ungrouped_occurrences(
        &self,
        grouping_generation: &str,
        after_cursor: Option<(&[u8], &[u8])>, // (occurred_at_unix_nano as [u8;8], event_id)
        limit: usize,
    ) -> Result<Vec<UngroupedOccurrence>, SqliteError> {
        let (cursor_ts, cursor_eid): (Vec<u8>, Vec<u8>) = match after_cursor {
            Some((ts, eid)) => (ts.to_vec(), eid.to_vec()),
            None => (to_sql_i64(0).to_be_bytes().to_vec(), vec![0; 16]),
        };
        let mut stmt = self.conn.prepare(
            "SELECT o.deployment_id, o.app_id, o.app_identity_sha256,
                    o.event_id, o.occurred_at_unix_nano, o.received_at_unix_nano,
                    o.canonical, o.canonical_sha256
             FROM occurrences o
             LEFT JOIN occurrence_issues oi
                 ON o.deployment_id = oi.deployment_id
                AND o.app_id = oi.app_id
                AND o.event_id = oi.event_id
                AND oi.grouping_generation = ?1
             WHERE oi.event_id IS NULL
               AND (o.occurred_at_unix_nano, o.event_id) > (?2, ?3)
             ORDER BY o.occurred_at_unix_nano, o.event_id
             LIMIT ?4",
        )?;
        let rows = stmt.query_map(
            params![
                grouping_generation,
                i64::from_be_bytes(cursor_ts.try_into().unwrap_or([0; 8])),
                cursor_eid,
                limit as i64
            ],
            |row| {
                Ok(UngroupedOccurrence {
                    deployment_id: row.get(0)?,
                    app_id: row.get(1)?,
                    app_identity_sha256: row.get(2)?,
                    event_id: row.get(3)?,
                    occurred_at_unix_nano: from_sql_i64(row.get(4)?),
                    received_at_unix_nano: from_sql_i64(row.get(5)?),
                    canonical: row.get(6)?,
                    canonical_sha256: row.get(7)?,
                })
            },
        )?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    /// Atomically fold a batch of grouping results into the issue tables.
    pub fn fold_grouped(&mut self, results: &[GroupingRow]) -> Result<GroupingReport, SqliteError> {
        let tx = self.conn.transaction()?;
        let mut report = GroupingReport::default();
        for result in results {
            fold_issue(&tx, result, &mut report)?;
        }
        tx.commit()?;
        Ok(report)
    }

    /// List issues ordered by `last_seen` descending, limited to `limit` rows.
    /// Suitable for read-only connections.
    pub fn list_issues(&self, limit: usize) -> Result<Vec<IssueSummary>, SqliteError> {
        let mut stmt = self.conn.prepare(
            "SELECT issue_id, app_id, title, grouping_quality,
                    first_seen_unix_nano, last_seen_unix_nano,
                    occurrence_count, max_severity, fingerprint_version
             FROM issues
             ORDER BY last_seen_unix_nano DESC, issue_id
             LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |row| {
            let issue_id: Vec<u8> = row.get(0)?;
            let app_id: Vec<u8> = row.get(1)?;
            Ok(IssueSummary {
                issue_id: uuid::Uuid::from_slice(&issue_id)
                    .map(|u| u.to_string())
                    .unwrap_or_else(|_| issue_id.iter().map(|b| format!("{b:02x}")).collect()),
                app_id: uuid::Uuid::from_slice(&app_id)
                    .map(|u| u.to_string())
                    .unwrap_or_else(|_| app_id.iter().map(|b| format!("{b:02x}")).collect()),
                title: row.get(2)?,
                grouping_quality: {
                    let v: i64 = row.get(3)?;
                    v as u8
                },
                first_seen_unix_nano: from_sql_i64(row.get(4)?),
                last_seen_unix_nano: from_sql_i64(row.get(5)?),
                occurrence_count: {
                    let v: i64 = row.get(6)?;
                    v as u64
                },
                max_severity: {
                    let v: i64 = row.get(7)?;
                    v as i32
                },
                fingerprint_version: {
                    let v: i64 = row.get(8)?;
                    v as u16
                },
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
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

fn migrate(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        "BEGIN IMMEDIATE;
         CREATE TABLE IF NOT EXISTS schema_migrations (
             version INTEGER PRIMARY KEY,
             applied_at_unix INTEGER NOT NULL
         ) STRICT;
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
             PRIMARY KEY(deployment_id, app_id, app_identity_sha256, event_id)
         ) STRICT, WITHOUT ROWID;
         CREATE INDEX IF NOT EXISTS occurrences_time
             ON occurrences(occurred_at_unix_nano DESC, event_id);
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
             issue_id BLOB NOT NULL CHECK(length(issue_id) = 16),
             deployment_id BLOB NOT NULL CHECK(length(deployment_id) = 16),
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
             grouping_generation TEXT NOT NULL,
             PRIMARY KEY (deployment_id, issue_id)
         ) STRICT, WITHOUT ROWID;
         CREATE INDEX IF NOT EXISTS issues_last_seen
             ON issues(last_seen_unix_nano DESC, issue_id);
         CREATE TABLE IF NOT EXISTS occurrence_issues (
             deployment_id BLOB NOT NULL CHECK(length(deployment_id) = 16),
             app_id BLOB NOT NULL CHECK(length(app_id) = 16),
             event_id BLOB NOT NULL CHECK(length(event_id) = 16),
             issue_id BLOB NOT NULL CHECK(length(issue_id) = 16),
             fingerprint_version INTEGER NOT NULL,
             fingerprint_digest BLOB NOT NULL CHECK(length(fingerprint_digest) = 32),
             grouping_generation TEXT NOT NULL,
             PRIMARY KEY (deployment_id, app_id, event_id)
         ) STRICT, WITHOUT ROWID;
         INSERT OR IGNORE INTO schema_migrations(version, applied_at_unix)
             VALUES (1, unixepoch());
         INSERT OR IGNORE INTO schema_migrations(version, applied_at_unix)
             VALUES (2, unixepoch());
         INSERT OR IGNORE INTO schema_migrations(version, applied_at_unix)
             VALUES (3, unixepoch());
         COMMIT;",
    )?;
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
    report: &mut FoldReport,
) -> Result<(), SqliteError> {
    let existing: Option<(Vec<u8>, Vec<u8>)> = tx
        .query_row(
            "SELECT canonical_sha256, canonical FROM occurrences
             WHERE deployment_id = ?1 AND app_id = ?2
               AND app_identity_sha256 = ?3 AND event_id = ?4",
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

    tx.execute(
        "INSERT INTO occurrences VALUES
         (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
        params![
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
        ],
    )?;
    report.inserted += 1;
    Ok(())
}

fn fold_issue(
    tx: &Transaction<'_>,
    row: &GroupingRow,
    report: &mut GroupingReport,
) -> Result<(), SqliteError> {
    // Check if this occurrence is already grouped for this generation.
    let already: bool = tx
        .query_row(
            "SELECT 1 FROM occurrence_issues
             WHERE deployment_id = ?1 AND app_id = ?2 AND event_id = ?3",
            params![
                row.deployment_id.as_slice(),
                row.app_id.as_slice(),
                row.event_id.as_slice(),
            ],
            |_| Ok(true),
        )
        .optional()?
        .unwrap_or(false);
    if already {
        report.duplicate_skipped += 1;
        return Ok(());
    }

    // Insert the occurrence→issue mapping.
    tx.execute(
        "INSERT OR IGNORE INTO occurrence_issues
             (deployment_id, app_id, event_id, issue_id,
              fingerprint_version, fingerprint_digest, grouping_generation)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            row.deployment_id.as_slice(),
            row.app_id.as_slice(),
            row.event_id.as_slice(),
            row.issue_id.as_slice(),
            row.fingerprint_version as i64,
            row.fingerprint_digest.as_slice(),
            row.grouping_generation,
        ],
    )?;
    report.occurrences_grouped += 1;

    // Upsert the issue: create if new, update aggregates if existing.
    let ts = to_sql_i64(row.occurred_at_unix_nano);
    let updated = tx.execute(
        "UPDATE issues SET
             occurrence_count = occurrence_count + 1,
             last_seen_unix_nano = MAX(last_seen_unix_nano, ?1),
             max_severity = MAX(max_severity, ?2),
             latest_event_id = CASE WHEN ?1 > last_seen_unix_nano
                                    THEN ?3 ELSE latest_event_id END
         WHERE deployment_id = ?4 AND issue_id = ?5",
        params![
            ts,
            row.severity as i64,
            row.event_id.as_slice(),
            row.deployment_id.as_slice(),
            row.issue_id.as_slice(),
        ],
    )?;
    if updated == 0 {
        tx.execute(
            "INSERT INTO issues
                 (issue_id, deployment_id, app_id, app_identity_sha256,
                  fingerprint_version, fingerprint_digest, grouping_quality,
                  title, first_seen_unix_nano, last_seen_unix_nano,
                  occurrence_count, max_severity, latest_event_id,
                  grouping_generation)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9, 1, ?10, ?11, ?12)",
            params![
                row.issue_id.as_slice(),
                row.deployment_id.as_slice(),
                row.app_id.as_slice(),
                row.app_identity_sha256.as_slice(),
                row.fingerprint_version as i64,
                row.fingerprint_digest.as_slice(),
                row.grouping_quality as i64,
                row.title,
                ts,
                row.severity as i64,
                row.event_id.as_slice(),
                row.grouping_generation,
            ],
        )?;
        report.issues_created += 1;
    } else {
        report.issues_updated += 1;
    }
    Ok(())
}

/// SQLite INTEGER is signed; this mapping preserves the complete u64 ordering and bits.
fn to_sql_i64(value: u64) -> i64 {
    (value ^ (1_u64 << 63)) as i64
}

fn from_sql_i64(value: i64) -> u64 {
    (value as u64) ^ (1_u64 << 63)
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};
    use uuid::Uuid;

    use super::*;
    use crate::projection::{occurrence_keys, OccurrenceCommit};

    const DEPLOYMENT: [u8; 16] = [1; 16];
    const OTHER_DEPLOYMENT: [u8; 16] = [9; 16];

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

    fn fixture<'a>(canonical: &'a [u8], digest: &'a [u8; 32]) -> OccurrenceRow<'a> {
        OccurrenceRow {
            deployment_id: &DEPLOYMENT,
            app_id: &[2; 16],
            app_identity_sha256: &[3; 32],
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

    #[test]
    fn exact_bytes_decide_duplicate_even_after_equal_digest() {
        let mut db = memory_db();
        let (key, commit, json) = commit();
        let digest: [u8; 32] = Sha256::digest(b"same").into();
        let first = db
            .fold_committed(
                "2026-09-10",
                &key,
                &commit,
                &json,
                [fixture(b"same", &digest)],
            )
            .unwrap();
        let duplicate = db
            .fold_committed(
                "2026-09-10",
                &key,
                &commit,
                &json,
                [fixture(b"same", &digest)],
            )
            .unwrap();
        // Artificial equal digest with different bytes proves bytes remain authoritative.
        let collision = db
            .fold_committed(
                "2026-09-10",
                &key,
                &commit,
                &json,
                [fixture(b"other", &digest)],
            )
            .unwrap();
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
            .fold_committed("2026-09-10", &key, &commit, &json, [first, second])
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
        );
        assert!(matches!(result, Err(SqliteError::TransactionLimit { .. })));
        assert_eq!(db.counts(), (0, 0, 0));
    }

    #[test]
    fn conflicting_commit_exact_bytes_are_rejected() {
        let mut db = memory_db();
        let (key, commit, json) = commit();
        db.fold_committed("2026-09-10", &key, &commit, &json, [])
            .unwrap();
        assert!(matches!(
            db.fold_committed("2026-09-10", &key, &commit, b"different", []),
            Err(SqliteError::CommitConflict)
        ));
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
        std::fs::remove_file(path).unwrap();
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
        std::fs::remove_file(path).unwrap();
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
            ),
            Err(SqliteError::DeploymentMismatch {
                expected: DEPLOYMENT,
                actual: OTHER_DEPLOYMENT,
            })
        ));
        assert_eq!(db.counts(), (0, 0, 0));
    }

    #[test]
    fn grouping_fold_creates_and_updates_issues_idempotently() {
        let mut db = memory_db();

        let row1 = GroupingRow {
            deployment_id: DEPLOYMENT.to_vec(),
            app_id: vec![0x42; 16],
            app_identity_sha256: vec![0x43; 32],
            event_id: vec![0x01; 16],
            issue_id: vec![0xA0; 16],
            fingerprint_version: 1,
            fingerprint_digest: vec![0xF0; 32],
            grouping_quality: 0,
            grouping_generation: "fp-v1".to_owned(),
            title: "TypeError".to_owned(),
            occurred_at_unix_nano: 1_000,
            severity: 17,
        };

        // First occurrence creates the issue.
        let r1 = db.fold_grouped(std::slice::from_ref(&row1)).unwrap();
        assert_eq!(r1.issues_created, 1);
        assert_eq!(r1.occurrences_grouped, 1);
        assert_eq!(r1.issues_updated, 0);

        // Same occurrence again is a duplicate.
        let r2 = db.fold_grouped(std::slice::from_ref(&row1)).unwrap();
        assert_eq!(r2.duplicate_skipped, 1);
        assert_eq!(r2.issues_created, 0);
        assert_eq!(r2.occurrences_grouped, 0);

        // Second occurrence for the same issue updates aggregates.
        let row2 = GroupingRow {
            event_id: vec![0x02; 16],
            occurred_at_unix_nano: 2_000,
            severity: 21,
            ..row1.clone()
        };
        let r3 = db.fold_grouped(&[row2]).unwrap();
        assert_eq!(r3.issues_created, 0);
        assert_eq!(r3.issues_updated, 1);
        assert_eq!(r3.occurrences_grouped, 1);

        // Verify the issue aggregates.
        let (count, last_seen, max_sev): (i64, i64, i64) = db
            .conn
            .query_row(
                "SELECT occurrence_count, last_seen_unix_nano, max_severity
                 FROM issues WHERE deployment_id = ?1 AND issue_id = ?2",
                params![DEPLOYMENT.as_slice(), vec![0xA0_u8; 16].as_slice()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(count, 2);
        assert_eq!(from_sql_i64(last_seen), 2_000);
        assert_eq!(max_sev, 21);

        // A different issue (different fingerprint) is separate.
        let row3 = GroupingRow {
            event_id: vec![0x03; 16],
            issue_id: vec![0xB0; 16],
            fingerprint_digest: vec![0xF1; 32],
            title: "ValueError".to_owned(),
            ..row1
        };
        let r4 = db.fold_grouped(&[row3]).unwrap();
        assert_eq!(r4.issues_created, 1);

        // Total: 2 issues, 3 occurrence_issues rows.
        let issue_count: i64 = db
            .conn
            .query_row("SELECT count(*) FROM issues", [], |r| r.get(0))
            .unwrap();
        assert_eq!(issue_count, 2);
        let oi_count: i64 = db
            .conn
            .query_row("SELECT count(*) FROM occurrence_issues", [], |r| r.get(0))
            .unwrap();
        assert_eq!(oi_count, 3);
    }

    #[test]
    fn list_issues_returns_issues_ordered_by_last_seen_desc() {
        let mut db = memory_db();

        // Insert two issues with different last_seen timestamps.
        let early_issue = GroupingRow {
            deployment_id: DEPLOYMENT.to_vec(),
            app_id: vec![0x42; 16],
            app_identity_sha256: vec![0x43; 32],
            event_id: vec![0x01; 16],
            issue_id: vec![0xA0; 16],
            fingerprint_version: 1,
            fingerprint_digest: vec![0xF0; 32],
            grouping_quality: 0,
            grouping_generation: "fp-v1".to_owned(),
            title: "EarlyError".to_owned(),
            occurred_at_unix_nano: 1_000,
            severity: 10,
        };
        let late_issue = GroupingRow {
            event_id: vec![0x02; 16],
            issue_id: vec![0xB0; 16],
            fingerprint_digest: vec![0xF1; 32],
            title: "LateError".to_owned(),
            occurred_at_unix_nano: 5_000,
            severity: 20,
            ..early_issue.clone()
        };

        db.fold_grouped(std::slice::from_ref(&early_issue)).unwrap();
        db.fold_grouped(std::slice::from_ref(&late_issue)).unwrap();

        let issues = db.list_issues(10).unwrap();
        assert_eq!(issues.len(), 2);
        // Most recent first.
        assert_eq!(issues[0].title, "LateError");
        assert_eq!(issues[0].occurrence_count, 1);
        assert_eq!(issues[0].max_severity, 20);
        assert_eq!(issues[0].last_seen_unix_nano, 5_000);
        assert_eq!(issues[1].title, "EarlyError");
        assert_eq!(issues[1].occurrence_count, 1);

        // Limit works.
        let limited = db.list_issues(1).unwrap();
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].title, "LateError");
    }

    #[test]
    fn open_read_only_can_read_issues_written_by_primary() {
        let path = temporary_db_path("readonly");
        {
            let mut db = ErrorsDb::open(&path, DEPLOYMENT).unwrap();
            let row = GroupingRow {
                deployment_id: DEPLOYMENT.to_vec(),
                app_id: vec![0x42; 16],
                app_identity_sha256: vec![0x43; 32],
                event_id: vec![0x01; 16],
                issue_id: vec![0xA0; 16],
                fingerprint_version: 1,
                fingerprint_digest: vec![0xF0; 32],
                grouping_quality: 0,
                grouping_generation: "fp-v1".to_owned(),
                title: "ReadOnlyTest".to_owned(),
                occurred_at_unix_nano: 3_000,
                severity: 15,
            };
            db.fold_grouped(std::slice::from_ref(&row)).unwrap();
        }

        let ro = ErrorsDb::open_read_only(&path).unwrap();
        let issues = ro.list_issues(10).unwrap();
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].title, "ReadOnlyTest");
        assert_eq!(issues[0].last_seen_unix_nano, 3_000);
        assert_eq!(issues[0].max_severity, 15);
        assert_eq!(issues[0].fingerprint_version, 1);

        // Verify issue_id is UUID-formatted.
        assert!(Uuid::parse_str(&issues[0].issue_id).is_ok());

        std::fs::remove_file(&path).unwrap();
        // WAL/SHM cleanup.
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }
}
