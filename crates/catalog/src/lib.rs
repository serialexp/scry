//! SQLite-backed catalog of scry blocks.
//!
//! The catalog is **derived state** (`ARCHITECTURE.md § The catalog`):
//! the source of truth for "which blocks exist" is the object-storage
//! bucket. The catalog is just a queryable mirror of the sidecars,
//! kept up to date in two ways:
//!
//! - **Online**: writers call [`Catalog::insert_block`] after each
//!   successful upload. This is the hot path during normal operation.
//! - **Offline**: [`Catalog::reconcile_from_bucket`] walks the bucket
//!   and upserts every sidecar it finds. Used at startup, after a
//!   crash, or by `scry-list` to bootstrap an empty catalog from a
//!   shared bucket.
//!
//! ## v0.1 scope
//!
//! The on-disk schema is **the** full schema from
//! `ARCHITECTURE.md § The catalog § Schema`, minus the `buckets`
//! table (one bucket in v0.1, recorded as plain text on each row).
//! Fields that aren't populated yet — `fingerprint`, `superseded_by`,
//! `deleted_at`, `postings_size_bytes`, `has_postings` — stay in the
//! schema as nullables so v0.2+ doesn't need a migration. Indices
//! match the architecture spec exactly.

use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use object_store::{path::Path as ObjPath, ObjectStore, ObjectStoreExt};
use rusqlite::{params, Connection, OptionalExtension};
use scry_block::BlockMeta;
use uuid::Uuid;

pub mod snapshot;
pub use snapshot::{
    restore_snapshot, save_snapshot, RestoreOutcome, SaveReport, CATALOG_SCHEMA_VERSION,
    SNAPSHOT_KEY,
};

/// A catalog row, joining the block sidecar with the per-instance
/// bookkeeping fields (`bucket`, `date`, `level`). Returned by
/// [`Catalog::list_blocks`] and [`Catalog::get_block`].
#[derive(Debug, Clone)]
pub struct CatalogEntry {
    pub meta: BlockMeta,
    pub bucket: String,
    /// `yyyy-mm-dd` UTC of `meta.ts_min_unix_nano`. Stored explicitly
    /// so query planners can prune by date without recomputing.
    pub date: String,
    /// Compaction level. 0 for freshly-written blocks; bumps on merge.
    /// All v0.1 blocks are level 0.
    pub level: u32,
}

/// Report returned by [`Catalog::reconcile_from_bucket`].
#[derive(Debug, Clone, Default)]
pub struct ReconcileReport {
    /// Total `*.meta.json` objects observed in the bucket.
    pub seen: usize,
    /// Catalog rows newly inserted (UUID was previously unknown).
    pub inserted: usize,
    /// Sidecars whose UUID was already in the catalog and were left
    /// alone. Blocks are immutable, so we never overwrite.
    pub already_present: usize,
    /// Sidecars that failed to parse — counted, logged, and skipped.
    /// A noisy bucket shouldn't fail reconcile.
    pub failed: usize,
}

/// Result of resolving a block UUID through durable compaction ancestry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalResolution {
    /// The UUID itself, or exactly one known descendant, is currently live.
    Unique(Uuid),
    /// No live block currently claims to represent this UUID.
    None,
    /// Contradictory compactions produced incomparable live descendants.
    Fork(Vec<Uuid>),
}

/// A superseded input whose immutable objects still need physical deletion.
#[derive(Debug, Clone)]
pub struct PendingReap {
    pub entry: CatalogEntry,
    pub output_uuid: Uuid,
    pub eligible_at_unix_nano: u64,
}

pub struct Catalog {
    conn: Connection,
    bucket: String,
}

impl Catalog {
    /// Open (or create) the catalog at `path`. Schema is initialised
    /// idempotently on first open and on every subsequent open
    /// (`CREATE TABLE IF NOT EXISTS`). `bucket` is the logical bucket
    /// name recorded against every block this instance writes.
    pub fn open(path: &Path, bucket: impl Into<String>) -> Result<Self> {
        let conn = Connection::open(path)
            .with_context(|| format!("opening sqlite at {}", path.display()))?;
        // WAL mode keeps reads and the occasional write from blocking
        // each other. Synchronous=NORMAL is the standard pairing.
        conn.pragma_update(None, "journal_mode", "WAL")
            .context("set journal_mode=WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")
            .context("set synchronous=NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .context("set foreign_keys=ON")?;
        let cat = Self {
            conn,
            bucket: bucket.into(),
        };
        cat.init_schema()?;
        Ok(cat)
    }

    /// Bucket associated with this catalog instance. New inserts are
    /// recorded against this name; reconcile reads the same bucket via
    /// the [`ObjectStore`] passed in.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    fn init_schema(&self) -> Result<()> {
        // D-054 added per-block WAL watermark columns after catalogs already
        // existed in production. `CREATE TABLE IF NOT EXISTS` does not evolve an
        // existing `blocks` table, so migrate that v1 shape before any query or
        // insert can reference the new columns. D-061 adds pointer-independent
        // logical liveness and persistent pending-reap state. These additive
        // columns are safe on old local catalogs; the obsolete superseded_by FK
        // is removed by `migrate_blocks_v3` below.
        self.add_column_if_missing("blocks", "wal_seg_max", "INTEGER")?;
        self.add_column_if_missing("blocks", "wal_shard", "INTEGER")?;
        self.add_column_if_missing("blocks", "superseded", "INTEGER NOT NULL DEFAULT 0")?;
        self.add_column_if_missing("blocks", "reap_output_uuid", "TEXT")?;
        self.add_column_if_missing("blocks", "reap_eligible_at", "INTEGER")?;
        self.migrate_blocks_v3()?;

        // The DDL matches ARCHITECTURE.md § The catalog § Schema with
        // the `buckets` table omitted (one bucket in v0.1) and the
        // `blocks.bucket REFERENCES buckets(name)` FK relaxed to plain
        // TEXT. Both come back when multi-bucket lands; nothing about
        // the v0.1 rows needs to change for that migration.
        self.conn
            .execute_batch(
                r#"
            CREATE TABLE IF NOT EXISTS blocks (
              uuid                TEXT PRIMARY KEY,
              bucket              TEXT NOT NULL,
              signal              TEXT NOT NULL,
              date                TEXT NOT NULL,
              writer_id           TEXT NOT NULL,
              level               INTEGER NOT NULL DEFAULT 0,
              ts_min              INTEGER NOT NULL,
              ts_max              INTEGER NOT NULL,
              row_count           INTEGER NOT NULL,
              byte_size           INTEGER NOT NULL,
              postings_size_bytes INTEGER,
              has_postings        INTEGER NOT NULL DEFAULT 0,
              body_bloom_size_bytes INTEGER,
              has_body_bloom      INTEGER NOT NULL DEFAULT 0,
              schema_version      INTEGER NOT NULL,
              fingerprint         BLOB,
              -- Legacy diagnostic pointer. Logical liveness is the independent
              -- `superseded` bit so deleting an intermediate output cannot trip
              -- a foreign key or resurrect its ancestors.
              superseded_by       TEXT,
              superseded          INTEGER NOT NULL DEFAULT 0,
              deleted_at          INTEGER,
              reap_output_uuid    TEXT,
              reap_eligible_at    INTEGER,
              -- D-054 dedup watermark: highest WAL segment this block
              -- durably contains, and the ingest shard that wrote it. NULL
              -- for pre-D-054 / compacted blocks (round-trips the sidecar
              -- losslessly; the authoritative high-water lives in
              -- wal_watermarks below).
              wal_seg_max         INTEGER,
              wal_shard           INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_blocks_query
              ON blocks(signal, date, ts_min, ts_max)
              WHERE deleted_at IS NULL AND superseded = 0;

            CREATE INDEX IF NOT EXISTS idx_blocks_compact
              ON blocks(bucket, signal, date, level)
              WHERE deleted_at IS NULL AND superseded = 0;

            -- Durable, order-independent compaction claims. No foreign keys:
            -- both ancestors and intermediate descendants are intentionally
            -- removable while the claim remains useful for stale-peer repair.
            CREATE TABLE IF NOT EXISTS block_lineage (
              ancestor_uuid   TEXT NOT NULL,
              descendant_uuid TEXT NOT NULL,
              signal          TEXT NOT NULL,
              date            TEXT NOT NULL,
              observed_at     INTEGER NOT NULL,
              PRIMARY KEY (ancestor_uuid, descendant_uuid)
            ) WITHOUT ROWID;

            CREATE INDEX IF NOT EXISTS idx_block_lineage_descendant
              ON block_lineage(descendant_uuid);

            CREATE INDEX IF NOT EXISTS idx_blocks_pending_reap
              ON blocks(reap_eligible_at)
              WHERE superseded = 1 AND reap_eligible_at IS NOT NULL;

            -- Per-(signal, writer, date) high-water mark for incremental
            -- ListObjects polling (ARCHITECTURE.md § Cursor-driven polling).
            -- `highest_uuid` is the lexically-greatest (== newest, since
            -- block UUIDs are v7 time-sortable) block UUID this instance has
            -- ingested for the partition; the next poll lists start-after it.
            CREATE TABLE IF NOT EXISTS poll_cursors (
              signal       TEXT NOT NULL,
              writer_id    TEXT NOT NULL,
              date         TEXT NOT NULL,
              highest_uuid TEXT NOT NULL,
              PRIMARY KEY (signal, writer_id, date)
            );

            -- Persistent, monotonic per-WAL-instance segment high-water
            -- (D-054). Keyed by the WAL instance `(writer_id, signal,
            -- shard)`; `seg_max` is the greatest WAL segment durably
            -- committed to a block for that instance. Advanced atomically
            -- with `insert_block` (and by convergence `apply_event` for
            -- peers' blocks), never decremented by supersede/delete, so it
            -- survives compaction (which drops per-block watermarks). The
            -- merged history+live query keeps a live record tagged
            -- `(writer, shard, seg)` iff `seg > seg_max` — the exact seam
            -- between "already in a block" and "still only in flight".
            CREATE TABLE IF NOT EXISTS wal_watermarks (
              writer_id TEXT NOT NULL,
              signal    TEXT NOT NULL,
              shard     INTEGER NOT NULL,
              seg_max   INTEGER NOT NULL,
              PRIMARY KEY (writer_id, signal, shard)
            );

            -- Label cache: a materialized view over the authoritative per-block
            -- postings sidecars, warmed lazily by the metadata handler (D-050).
            -- NOT a source of truth — every row is reconstructable by scanning
            -- the block's postings. Keyed by block_uuid so it expires with the
            -- block lifecycle (reaped in delete_blocks). `block_labels_warmed`
            -- records that a block has been scanned even when it carries zero
            -- labels, so a label-less block isn't rescanned on every request.
            CREATE TABLE IF NOT EXISTS block_labels (
              block_uuid  TEXT NOT NULL,
              label_name  TEXT NOT NULL,
              label_value TEXT NOT NULL,
              PRIMARY KEY (block_uuid, label_name, label_value)
            ) WITHOUT ROWID;

            CREATE INDEX IF NOT EXISTS idx_block_labels_name
              ON block_labels(label_name);

            CREATE TABLE IF NOT EXISTS block_labels_warmed (
              block_uuid TEXT PRIMARY KEY
            );
            "#,
            )
            .context("initialising catalog schema")?;
        // Stamp the catalog schema version so a snapshot restored by a
        // different binary can be version-checked before use (D-055). Bump
        // `CATALOG_SCHEMA_VERSION` whenever the DDL above changes.
        self.conn
            .pragma_update(None, "user_version", snapshot::CATALOG_SCHEMA_VERSION)
            .context("stamping PRAGMA user_version")?;
        Ok(())
    }

    /// Add one column to an existing table when upgrading an older on-disk
    /// catalog. SQLite has no `ADD COLUMN IF NOT EXISTS`, so inspect
    /// `PRAGMA table_info` first; fresh catalogs skip both ALTERs because the
    /// complete table definition above already contains the columns.
    fn add_column_if_missing(&self, table: &str, column: &str, sql_type: &str) -> Result<()> {
        let mut stmt = self
            .conn
            .prepare(&format!("PRAGMA table_info({table})"))
            .with_context(|| format!("reading {table} columns"))?;
        let columns = stmt
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if !columns.iter().any(|name| name == column) && !columns.is_empty() {
            self.conn
                .execute_batch(&format!(
                    "ALTER TABLE {table} ADD COLUMN {column} {sql_type}"
                ))
                .with_context(|| format!("adding {table}.{column}"))?;
        }
        Ok(())
    }

    /// Rebuild pre-D-061 `blocks` tables to remove the self-referential
    /// `superseded_by` foreign key. An intermediate output is itself deleted by
    /// a later compaction while older peers may still retain rows pointing at
    /// it; liveness therefore cannot depend on that row's lifetime.
    fn migrate_blocks_v3(&self) -> Result<()> {
        let has_blocks: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='blocks')",
            [],
            |r| r.get(0),
        )?;
        if !has_blocks {
            return Ok(());
        }
        let has_fk = self
            .conn
            .prepare("PRAGMA foreign_key_list(blocks)")?
            .query([])?
            .next()?
            .is_some();
        if !has_fk {
            return Ok(());
        }

        self.conn.pragma_update(None, "foreign_keys", "OFF")?;
        let result = self.conn.execute_batch(
            r#"
            BEGIN IMMEDIATE;
            CREATE TABLE blocks_v3 (
              uuid TEXT PRIMARY KEY, bucket TEXT NOT NULL, signal TEXT NOT NULL,
              date TEXT NOT NULL, writer_id TEXT NOT NULL, level INTEGER NOT NULL DEFAULT 0,
              ts_min INTEGER NOT NULL, ts_max INTEGER NOT NULL, row_count INTEGER NOT NULL,
              byte_size INTEGER NOT NULL, postings_size_bytes INTEGER,
              has_postings INTEGER NOT NULL DEFAULT 0, body_bloom_size_bytes INTEGER,
              has_body_bloom INTEGER NOT NULL DEFAULT 0, schema_version INTEGER NOT NULL,
              fingerprint BLOB, superseded_by TEXT, superseded INTEGER NOT NULL DEFAULT 0,
              deleted_at INTEGER, reap_output_uuid TEXT, reap_eligible_at INTEGER,
              wal_seg_max INTEGER, wal_shard INTEGER
            );
            INSERT INTO blocks_v3 SELECT
              uuid, bucket, signal, date, writer_id, level, ts_min, ts_max,
              row_count, byte_size, postings_size_bytes, has_postings,
              body_bloom_size_bytes, has_body_bloom, schema_version, fingerprint,
              superseded_by, CASE WHEN superseded_by IS NULL THEN superseded ELSE 1 END,
              deleted_at,
              COALESCE(reap_output_uuid, superseded_by),
              CASE WHEN superseded_by IS NOT NULL AND deleted_at IS NULL
                   THEN COALESCE(reap_eligible_at, 0)
                   ELSE reap_eligible_at END,
              wal_seg_max, wal_shard
            FROM blocks;
            DROP TABLE blocks;
            ALTER TABLE blocks_v3 RENAME TO blocks;
            COMMIT;
            "#,
        );
        if result.is_err() {
            // `execute_batch` can stop after BEGIN. Roll back before restoring
            // FK enforcement so a failed startup never returns a connection in
            // a half-migrated transaction.
            let _ = self.conn.execute_batch("ROLLBACK");
        }
        self.conn.pragma_update(None, "foreign_keys", "ON")?;
        result.context("migrating blocks table to pointer-independent liveness")?;
        Ok(())
    }

    /// Insert a block sidecar into the catalog. Idempotent: if the
    /// UUID is already present (e.g. a writer's online insert raced
    /// with the reconcile loop), the existing row is preserved
    /// untouched.
    ///
    /// Returns `true` if the row was newly inserted, `false` if it
    /// was already present.
    pub fn insert_block(&self, meta: &BlockMeta) -> Result<bool> {
        let date = format_date(meta.ts_min_unix_nano);
        // Insert the block row and advance the WAL high-water in one
        // transaction (D-054): the block becoming queryable and the
        // watermark that dedups it against still-in-flight live records must
        // be atomic, or a crash between the two writes would leave a block
        // visible whose records the live path can't recognise as durable →
        // a double across the seam. `unchecked_transaction` gives us a tx
        // over the shared `&self` connection.
        let tx = self
            .conn
            .unchecked_transaction()
            .context("begin insert_block transaction")?;
        let rows = tx
            .execute(
                r#"
            INSERT OR IGNORE INTO blocks (
              uuid, bucket, signal, date, writer_id, level,
              ts_min, ts_max, row_count, byte_size,
              postings_size_bytes, has_postings,
              body_bloom_size_bytes, has_body_bloom,
              schema_version, fingerprint, superseded_by, superseded, deleted_at,
              reap_output_uuid, reap_eligible_at, wal_seg_max, wal_shard
            ) VALUES (
              ?1, ?2, ?3, ?4, ?5, ?16,
              ?6, ?7, ?8, ?9,
              ?10, ?11,
              ?12, ?13,
              ?14, ?15, NULL,
              CASE WHEN EXISTS (
                SELECT 1 FROM block_lineage l
                JOIN blocks d ON d.uuid = l.descendant_uuid
                WHERE l.ancestor_uuid = ?1
                  AND d.deleted_at IS NULL AND d.superseded = 0
              ) THEN 1 ELSE 0 END,
              NULL, NULL, NULL, ?17, ?18
            )
            "#,
                params![
                    meta.uuid.to_string(),
                    self.bucket,
                    meta.signal,
                    date,
                    meta.writer_id.to_string(),
                    // SQLite stores INTEGER as i64; ts is u64 nanos. The
                    // value fits comfortably into i64 until year 2262, so
                    // a direct cast is fine for the next ~236 years.
                    meta.ts_min_unix_nano as i64,
                    meta.ts_max_unix_nano as i64,
                    meta.row_count as i64,
                    meta.byte_size as i64,
                    meta.postings_size_bytes.map(|v| v as i64),
                    if meta.has_postings { 1i64 } else { 0i64 },
                    meta.body_bloom_size_bytes.map(|v| v as i64),
                    if meta.has_body_bloom { 1i64 } else { 0i64 },
                    meta.schema_version as i64,
                    meta.label_fingerprint_bloom.as_deref(),
                    meta.level as i64,
                    meta.wal_seg_max.map(|v| v as i64),
                    meta.wal_shard.map(|v| v as i64),
                ],
            )
            .context("INSERT OR IGNORE block")?;
        // Advance the high-water unconditionally when the block carries a
        // watermark — even if the block row was already present (rows==0),
        // because the UPSERT is monotonic-max, so re-advancing is a no-op
        // that also self-heals a watermark table lagging behind blocks.
        if let (Some(seg), Some(shard)) = (meta.wal_seg_max, meta.wal_shard) {
            advance_watermark_in(&tx, &meta.writer_id.to_string(), &meta.signal, shard, seg)?;
        }
        apply_lineage_in(&tx, meta, &date)?;
        tx.commit().context("commit insert_block transaction")?;
        Ok(rows > 0)
    }

    /// Advance the durable WAL segment high-water for the instance
    /// `(writer_id, signal, shard)` to `seg_max`, but **only if greater**
    /// than the stored value — a monotonic high-water (mirrors
    /// [`advance_cursor`]). This is the value the merged history+live query
    /// dedups against (D-054): a live record tagged `(writer, shard, seg)`
    /// is kept iff `seg > seg_max`. Called from `insert_block` (local
    /// writes) and convergence `apply_event` (peers' blocks) so a
    /// query-only catalog carries every instance's high-water.
    pub fn advance_watermark(
        &self,
        writer_id: Uuid,
        signal: &str,
        shard: u32,
        seg_max: u64,
    ) -> Result<()> {
        advance_watermark_in(&self.conn, &writer_id.to_string(), signal, shard, seg_max)
    }

    /// Read the durable WAL segment high-water for `(writer_id, signal,
    /// shard)`. `None` when no block for that instance has been seen — the
    /// dedup treats it as `0` (covers nothing, so every live record is
    /// kept).
    pub fn get_watermark(&self, writer_id: Uuid, signal: &str, shard: u32) -> Result<Option<u64>> {
        let v: Option<i64> = self
            .conn
            .query_row(
                "SELECT seg_max FROM wal_watermarks \
                 WHERE writer_id = ?1 AND signal = ?2 AND shard = ?3",
                params![writer_id.to_string(), signal, shard as i64],
                |r| r.get(0),
            )
            .optional()
            .context("SELECT wal_watermark")?;
        Ok(v.map(|v| v as u64))
    }

    /// List every **live** block — not deleted and not superseded by a
    /// compaction merge — ordered by `(date, ts_min)`. This is the set
    /// queries read from: the moment the compactor sets `superseded_by`
    /// on an input (pointing at its merged replacement), that input
    /// drops out here, so a query never double-counts a merged block
    /// against its still-present-but-superseded inputs during the
    /// grace window before the input objects are deleted.
    pub fn list_blocks(&self) -> Result<Vec<CatalogEntry>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT uuid, bucket, signal, date, writer_id, level,
                   ts_min, ts_max, row_count, byte_size,
                   schema_version, fingerprint,
                   has_postings, postings_size_bytes,
                   has_body_bloom, body_bloom_size_bytes,
                   wal_seg_max, wal_shard
            FROM blocks
            WHERE deleted_at IS NULL AND superseded = 0
            ORDER BY date, ts_min, uuid
            "#,
        )?;
        let rows = stmt.query_map([], row_to_entry)?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    /// Look up a single block by UUID. Returns `None` if no such row.
    pub fn get_block(&self, uuid: Uuid) -> Result<Option<CatalogEntry>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT uuid, bucket, signal, date, writer_id, level,
                   ts_min, ts_max, row_count, byte_size,
                   schema_version, fingerprint,
                   has_postings, postings_size_bytes,
                   has_body_bloom, body_bloom_size_bytes,
                   wal_seg_max, wal_shard
            FROM blocks
            WHERE uuid = ?1
            "#,
        )?;
        let res = stmt
            .query_row(params![uuid.to_string()], row_to_entry)
            .optional()?;
        Ok(res)
    }

    /// Count of logical live blocks (queryable, neither retained nor superseded).
    pub fn block_count(&self) -> Result<usize> {
        let n: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM blocks WHERE deleted_at IS NULL AND superseded = 0",
            [],
            |r| r.get(0),
        )?;
        Ok(n as usize)
    }

    /// Total logical-live row count. Uses the same predicate as `list_blocks` so
    /// status reporting cannot double-count inputs pending physical reaping.
    pub fn live_row_count(&self) -> Result<u64> {
        let n: i64 = self.conn.query_row(
            "SELECT COALESCE(SUM(row_count), 0) FROM blocks \
             WHERE deleted_at IS NULL AND superseded = 0",
            [],
            |r| r.get(0),
        )?;
        Ok(n as u64)
    }

    /// Number of durable ancestry claims retained in the rebuildable lineage
    /// index. Exposed for growth monitoring while pruning policy is staged.
    pub fn lineage_row_count(&self) -> Result<u64> {
        let n: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM block_lineage", [], |r| r.get(0))?;
        Ok(n as u64)
    }

    /// Prune rebuildable lineage claims for one authoritatively reconciled
    /// partition. A caller must only invoke this after a stable object-store
    /// listing: every UUID in `present_descendants` has a committed `meta.json`,
    /// and any omitted descendant is no longer durable bucket truth.
    ///
    /// Current compacted sidecars contain their full transitive closure, so an
    /// extant terminal directly claims every represented ancestor. Removing
    /// edges to absent intermediate outputs therefore cannot break resolution.
    pub fn prune_lineage_partition(
        &self,
        signal: &str,
        date: &str,
        present_descendants: &[Uuid],
    ) -> Result<usize> {
        let present: std::collections::HashSet<String> =
            present_descendants.iter().map(Uuid::to_string).collect();
        let tx = self.conn.unchecked_transaction()?;
        let stale = {
            let mut stmt = tx.prepare(
                "SELECT ancestor_uuid, descendant_uuid FROM block_lineage \
                 WHERE signal = ?1 AND date = ?2",
            )?;
            let rows = stmt.query_map(params![signal, date], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
                .into_iter()
                .filter(|(_, descendant)| !present.contains(descendant))
                .collect::<Vec<_>>()
        };
        let mut deleted = 0usize;
        {
            let mut stmt = tx.prepare_cached(
                "DELETE FROM block_lineage WHERE ancestor_uuid = ?1 AND descendant_uuid = ?2",
            )?;
            for (ancestor, descendant) in stale {
                deleted += stmt.execute(params![ancestor, descendant])?;
            }
        }
        tx.commit().context("commit partition lineage pruning")?;
        Ok(deleted)
    }

    /// Mark a set of input blocks as superseded by a freshly-written
    /// compaction output (`merged`). Sets `superseded_by = merged` on
    /// every input UUID. After this returns the inputs no longer appear
    /// in [`list_blocks`], so queries read the merged block instead.
    ///
    /// `merged` must already be inserted (the `superseded_by` foreign
    /// key references `blocks(uuid)`); the compactor inserts the merged
    /// block before calling this. Runs in a single transaction so the
    /// supersede flips atomically — a query either sees all inputs or
    /// none of them, never a half-merged partition.
    /// Atomically from a shared-catalog caller's point of view, publish a
    /// replacement and stage its direct inputs for physical cleanup. The method
    /// is invoked inside one `CatalogHandle::with` closure, so peers cannot list
    /// between output insertion and logical supersession.
    pub fn apply_compaction(
        &self,
        output: &BlockMeta,
        direct_inputs: &[Uuid],
        reap_eligible_at_unix_nano: u64,
    ) -> Result<bool> {
        let inserted = self.insert_block(output)?;
        self.stage_superseded(direct_inputs, output.uuid, reap_eligible_at_unix_nano)?;
        Ok(inserted)
    }

    pub fn mark_superseded(&self, inputs: &[Uuid], merged: Uuid) -> Result<()> {
        self.stage_superseded(inputs, merged, 0)
    }

    /// Mark inputs logically superseded and persist their physical-reap work.
    /// Pointer-independent `superseded` is authoritative; `superseded_by` is
    /// retained only for operator diagnostics and old catalog readers.
    pub fn stage_superseded(
        &self,
        inputs: &[Uuid],
        merged: Uuid,
        eligible_at_unix_nano: u64,
    ) -> Result<()> {
        let merged_str = merged.to_string();
        let tx = self.conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "UPDATE blocks SET superseded_by = ?1, superseded = 1, \
                 reap_output_uuid = ?1, reap_eligible_at = \
                   MAX(COALESCE(reap_eligible_at, ?2), ?2) WHERE uuid = ?3",
            )?;
            for input in inputs {
                stmt.execute(params![
                    merged_str,
                    eligible_at_unix_nano.min(i64::MAX as u64) as i64,
                    input.to_string()
                ])
                .context("stage superseded input")?;
            }
        }
        tx.commit().context("commit stage_superseded")?;
        Ok(())
    }

    /// Superseded inputs whose deferred grace has elapsed. Their full catalog
    /// metadata remains available until object deletion has completed.
    pub fn list_pending_reaps(&self, now_unix_nano: u64) -> Result<Vec<PendingReap>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT uuid, bucket, signal, date, writer_id, level,
                   ts_min, ts_max, row_count, byte_size,
                   schema_version, fingerprint,
                   has_postings, postings_size_bytes,
                   has_body_bloom, body_bloom_size_bytes,
                   wal_seg_max, wal_shard,
                   reap_output_uuid, reap_eligible_at
            FROM blocks
            WHERE superseded = 1 AND reap_eligible_at IS NOT NULL
              AND reap_eligible_at <= ?1
            ORDER BY reap_eligible_at, date, uuid
            "#,
        )?;
        let rows = stmt.query_map(params![now_unix_nano.min(i64::MAX as u64) as i64], |row| {
            let entry = row_to_entry(row)?;
            let output: String = row.get(18)?;
            let eligible: i64 = row.get(19)?;
            let output_uuid = Uuid::parse_str(&output).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    18,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;
            Ok(PendingReap {
                entry,
                output_uuid,
                eligible_at_unix_nano: eligible as u64,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("list pending reaps")
    }

    /// Resolve `uuid` to the unique maximal live descendant represented by the
    /// lineage graph. Multiple incomparable terminals are a corruption/fork and
    /// must never be guessed between.
    pub fn resolve_terminal(&self, uuid: Uuid) -> Result<TerminalResolution> {
        let mut stmt = self.conn.prepare(
            r#"
            WITH RECURSIVE descendants(uuid) AS (
              SELECT descendant_uuid FROM block_lineage WHERE ancestor_uuid = ?1
              UNION
              SELECT l.descendant_uuid FROM block_lineage l
              JOIN descendants d ON l.ancestor_uuid = d.uuid
            )
            SELECT DISTINCT b.uuid
            FROM descendants d JOIN blocks b ON b.uuid = d.uuid
            WHERE b.deleted_at IS NULL AND b.superseded = 0
            ORDER BY b.uuid
            "#,
        )?;
        let ids = stmt
            .query_map(params![uuid.to_string()], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?
            .into_iter()
            .map(|s| Uuid::parse_str(&s).context("parse resolved descendant UUID"))
            .collect::<Result<Vec<_>>>()?;
        match ids.as_slice() {
            [] => {
                let live: bool = self.conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM blocks WHERE uuid=?1 \
                     AND deleted_at IS NULL AND superseded=0)",
                    params![uuid.to_string()],
                    |r| r.get(0),
                )?;
                Ok(if live {
                    TerminalResolution::Unique(uuid)
                } else {
                    TerminalResolution::None
                })
            }
            [only] => Ok(TerminalResolution::Unique(*only)),
            _ => Ok(TerminalResolution::Fork(ids)),
        }
    }

    /// Drop a set of block rows from the catalog. Called by the
    /// compactor *after* the input objects have been deleted from the
    /// bucket (the catalog is derived state — the row only goes once
    /// the bucket truth is gone). Runs in one transaction.
    ///
    /// Safe to call on superseded inputs: nothing references an input's
    /// UUID (the `superseded_by` FK points *from* the input *to* the
    /// still-present merged block, not the other way round).
    pub fn delete_blocks(&self, uuids: &[Uuid]) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare_cached("DELETE FROM blocks WHERE uuid = ?1")?;
            // Reap the derived label cache alongside the block row so it stays
            // bounded to live blocks (the cache's expiry mechanism, D-050).
            let mut lbl = tx.prepare_cached("DELETE FROM block_labels WHERE block_uuid = ?1")?;
            let mut warm =
                tx.prepare_cached("DELETE FROM block_labels_warmed WHERE block_uuid = ?1")?;
            for uuid in uuids {
                let id = uuid.to_string();
                stmt.execute(params![id]).context("DELETE block row")?;
                lbl.execute(params![id]).context("DELETE block_labels")?;
                warm.execute(params![id])
                    .context("DELETE block_labels_warmed")?;
            }
        }
        tx.commit().context("commit delete_blocks")?;
        Ok(())
    }

    /// Record a block's distinct `(label_name, label_value)` pairs into the
    /// label cache and mark the block **warmed**. Idempotent (`INSERT OR
    /// IGNORE`); safe with an empty slice — the warmed marker is still written
    /// so a label-less block is not rescanned on every metadata request. One
    /// transaction. The cache is a materialized view over postings (D-050); the
    /// caller supplies pairs enumerated from the block's `PostingsIndex`.
    pub fn upsert_block_labels(&self, uuid: Uuid, pairs: &[(String, String)]) -> Result<()> {
        let id = uuid.to_string();
        let tx = self.conn.unchecked_transaction()?;
        {
            tx.prepare_cached("INSERT OR IGNORE INTO block_labels_warmed(block_uuid) VALUES (?1)")?
                .execute(params![id])
                .context("mark block warmed")?;
            let mut ins = tx.prepare_cached(
                "INSERT OR IGNORE INTO block_labels(block_uuid, label_name, label_value) \
                 VALUES (?1, ?2, ?3)",
            )?;
            for (name, value) in pairs {
                ins.execute(params![id, name, value])
                    .context("insert block label")?;
            }
        }
        tx.commit().context("commit upsert_block_labels")?;
        Ok(())
    }

    /// The subset of `candidates` whose labels are already cached (warmed), so
    /// the metadata handler only pays a postings scan for the cold remainder.
    pub fn warmed_blocks(&self, candidates: &[Uuid]) -> Result<HashSet<Uuid>> {
        let mut out = HashSet::new();
        let mut stmt = self
            .conn
            .prepare_cached("SELECT 1 FROM block_labels_warmed WHERE block_uuid = ?1")?;
        for uuid in candidates {
            if stmt.exists(params![uuid.to_string()])? {
                out.insert(*uuid);
            }
        }
        Ok(out)
    }

    /// Distinct label **names** across the given (warmed) blocks, sorted.
    /// Empty input → empty output.
    pub fn distinct_label_names(&self, blocks: &[Uuid]) -> Result<Vec<String>> {
        if blocks.is_empty() {
            return Ok(Vec::new());
        }
        let placeholders = vec!["?"; blocks.len()].join(",");
        let sql = format!(
            "SELECT DISTINCT label_name FROM block_labels \
             WHERE block_uuid IN ({placeholders}) ORDER BY label_name"
        );
        let ids: Vec<String> = blocks.iter().map(Uuid::to_string).collect();
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(ids.iter()), |r| {
                r.get::<_, String>(0)
            })?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("distinct_label_names")?;
        Ok(rows)
    }

    /// Distinct **values** for `name` across the given (warmed) blocks, sorted.
    /// Empty input → empty output.
    pub fn distinct_label_values(&self, name: &str, blocks: &[Uuid]) -> Result<Vec<String>> {
        if blocks.is_empty() {
            return Ok(Vec::new());
        }
        let placeholders = vec!["?"; blocks.len()].join(",");
        let sql = format!(
            "SELECT DISTINCT label_value FROM block_labels \
             WHERE label_name = ? AND block_uuid IN ({placeholders}) ORDER BY label_value"
        );
        let mut binds: Vec<String> = Vec::with_capacity(blocks.len() + 1);
        binds.push(name.to_string());
        binds.extend(blocks.iter().map(Uuid::to_string));
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(binds.iter()), |r| {
                r.get::<_, String>(0)
            })?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("distinct_label_values")?;
        Ok(rows)
    }

    /// Soft-delete a set of blocks: stamp `deleted_at` with
    /// `deleted_at_unix_nano`. Because [`list_blocks`](Self::list_blocks)
    /// filters `deleted_at IS NULL`, a marked block drops out of the live
    /// set immediately — so the retention reaper can mark expired blocks,
    /// let queries stop listing them, wait an optional grace window, and
    /// only then delete their objects + rows. At grace 0 this step is
    /// skipped and [`delete_blocks`](Self::delete_blocks) is called
    /// directly. Runs in one transaction.
    pub fn mark_deleted(&self, uuids: &[Uuid], deleted_at_unix_nano: u64) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        {
            let mut stmt =
                tx.prepare_cached("UPDATE blocks SET deleted_at = ?1 WHERE uuid = ?2")?;
            for uuid in uuids {
                stmt.execute(params![deleted_at_unix_nano as i64, uuid.to_string()])
                    .context("UPDATE deleted_at")?;
            }
        }
        tx.commit().context("commit mark_deleted")?;
        Ok(())
    }

    /// The highest block UUID this instance has ingested for
    /// `(signal, writer_id, date)`, or `None` if the partition has never
    /// been polled. The incremental poller lists `start-after` this value
    /// (see [`advance_cursor`](Self::advance_cursor)).
    pub fn get_cursor(&self, signal: &str, writer_id: Uuid, date: &str) -> Result<Option<Uuid>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT highest_uuid FROM poll_cursors \
             WHERE signal = ?1 AND writer_id = ?2 AND date = ?3",
        )?;
        let res: Option<String> = stmt
            .query_row(params![signal, writer_id.to_string(), date], |r| r.get(0))
            .optional()?;
        match res {
            None => Ok(None),
            Some(s) => {
                let u = Uuid::parse_str(&s).with_context(|| format!("parsing cursor uuid {s}"))?;
                Ok(Some(u))
            }
        }
    }

    /// Advance the cursor for `(signal, writer_id, date)` to `uuid`, but
    /// **only if `uuid` is lexically greater** than the stored value — a
    /// monotonic high-water mark. UUID v7 strings sort by creation time, so
    /// "lexically greater" means "newer". This is what lets pub/sub and
    /// polling converge on the same state: whichever path observes a block
    /// first advances the cursor; the slower path's advance is a no-op.
    ///
    /// Implemented as an UPSERT whose `DO UPDATE` is gated on
    /// `excluded.highest_uuid > poll_cursors.highest_uuid`, so an
    /// out-of-order (older) observation can never roll the cursor backward.
    pub fn advance_cursor(
        &self,
        signal: &str,
        writer_id: Uuid,
        date: &str,
        uuid: Uuid,
    ) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO poll_cursors (signal, writer_id, date, highest_uuid) \
                 VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT(signal, writer_id, date) DO UPDATE SET \
                   highest_uuid = excluded.highest_uuid \
                 WHERE excluded.highest_uuid > poll_cursors.highest_uuid",
                params![signal, writer_id.to_string(), date, uuid.to_string()],
            )
            .context("UPSERT poll_cursor")?;
        Ok(())
    }

    /// Every known cursor as `(signal, writer_id, date)`. Used by the
    /// reconnect full sweep to re-poll every partition's tail.
    pub fn list_cursors(&self) -> Result<Vec<(String, Uuid, String)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT signal, writer_id, date FROM poll_cursors")?;
        let rows = stmt.query_map([], |r| {
            let signal: String = r.get(0)?;
            let writer_id_str: String = r.get(1)?;
            let date: String = r.get(2)?;
            Ok((signal, writer_id_str, date))
        })?;
        let mut out = Vec::new();
        for r in rows {
            let (signal, writer_id_str, date) = r?;
            let writer_id = Uuid::parse_str(&writer_id_str)
                .with_context(|| format!("parsing cursor writer_id {writer_id_str}"))?;
            out.push((signal, writer_id, date));
        }
        Ok(out)
    }

    /// Walk the bucket, fetch every `*.meta.json`, parse it as a
    /// [`BlockMeta`], and `INSERT OR IGNORE` into the catalog. Used to
    /// bootstrap an empty catalog and to re-derive after corruption.
    ///
    /// Sidecars that fail to parse are logged and counted in
    /// [`ReconcileReport::failed`] but do not abort the reconcile;
    /// one bad sidecar shouldn't poison the rest of the bucket.
    pub async fn reconcile_from_bucket(&self, store: &dyn ObjectStore) -> Result<ReconcileReport> {
        let mut report = ReconcileReport::default();
        let mut stream = store.list(None);
        while let Some(item) = stream.next().await {
            let obj = match item {
                Ok(o) => o,
                Err(e) => {
                    tracing::warn!(error = %e, "list error during reconcile, continuing");
                    continue;
                }
            };
            let path_str = obj.location.as_ref();
            // The `_catalog/` prefix is reserved for catalog snapshots (D-055),
            // never a block sidecar — skip it before any suffix check.
            if path_str.starts_with("_catalog/") {
                continue;
            }
            if !path_str.ends_with(".meta.json") {
                continue;
            }
            report.seen += 1;

            let bytes = match store.get(&obj.location).await {
                Ok(g) => match g.bytes().await {
                    Ok(b) => b,
                    Err(e) => {
                        report.failed += 1;
                        tracing::warn!(path = %path_str, error = %e, "sidecar get-body failed");
                        continue;
                    }
                },
                Err(e) => {
                    report.failed += 1;
                    tracing::warn!(path = %path_str, error = %e, "sidecar get failed");
                    continue;
                }
            };
            let meta: BlockMeta = match serde_json::from_slice(&bytes) {
                Ok(m) => m,
                Err(e) => {
                    report.failed += 1;
                    tracing::warn!(path = %path_str, error = %e, "sidecar JSON parse failed");
                    continue;
                }
            };
            match self.insert_block(&meta) {
                Ok(true) => report.inserted += 1,
                Ok(false) => report.already_present += 1,
                Err(e) => {
                    report.failed += 1;
                    tracing::warn!(path = %path_str, error = %e, "catalog insert failed");
                }
            }
        }
        tracing::info!(
            seen = report.seen,
            inserted = report.inserted,
            already_present = report.already_present,
            failed = report.failed,
            "reconcile complete"
        );
        Ok(report)
    }
}

/// Short-lived, synchronous access to a [`Catalog`] for one operation.
///
/// The compaction/retention engines run a long lifecycle that interleaves
/// quick catalog mutations (`insert_block`, `mark_superseded`,
/// `delete_blocks`, …) with minutes-long async work (the DataFusion merge,
/// object-store DELETEs). In a multi-instance daemon the catalog is shared
/// behind a `Mutex` with the convergence consumer and the query path, so the
/// lock **must not** be held across an `.await`.
///
/// This trait expresses exactly that discipline: [`with`](CatalogHandle::with)
/// hands a `&Catalog` to a closure that does one synchronous call and returns
/// before any await. The single-instance path passes a `&Catalog` (the impl is
/// a no-op pass-through); the daemon passes a `&Mutex<Catalog>` (the impl locks
/// for the duration of the closure only). The engines are generic over the
/// handle, so one routine serves both without duplicating the lifecycle and
/// without ever leaking a lock across an await point.
pub trait CatalogHandle {
    /// Run `f` against the catalog and return its result. For a locked handle
    /// the lock is acquired before `f` and released as `f` returns — `f` is
    /// synchronous, so this can never straddle an `.await`.
    fn with<R>(&self, f: impl FnOnce(&Catalog) -> R) -> R;
}

impl CatalogHandle for Catalog {
    #[inline]
    fn with<R>(&self, f: impl FnOnce(&Catalog) -> R) -> R {
        f(self)
    }
}

impl CatalogHandle for std::sync::Mutex<Catalog> {
    #[inline]
    fn with<R>(&self, f: impl FnOnce(&Catalog) -> R) -> R {
        f(&self.lock().expect("catalog mutex poisoned"))
    }
}

/// The `yyyy-mm-dd` UTC partition date a block with this `ts_min_unix_nano`
/// belongs to — the `date` column value and the date component of both the
/// object-storage path and a poll cursor's key. Exposed so the cluster's
/// convergence/poll code can derive a block's cursor key from its meta
/// without re-deriving the calendar math.
pub fn date_dir(ts_unix_nano: u64) -> String {
    format_date(ts_unix_nano)
}

fn format_date(ts_unix_nano: u64) -> String {
    let secs = (ts_unix_nano / 1_000_000_000) as i64;
    DateTime::<Utc>::from_timestamp(secs, 0)
        .unwrap_or_else(Utc::now)
        .format("%Y-%m-%d")
        .to_string()
}

fn apply_lineage_in(conn: &Connection, meta: &BlockMeta, date: &str) -> Result<()> {
    if meta.compacted_from.is_empty() {
        return Ok(());
    }
    let descendant = meta.uuid.to_string();
    let observed_at = Utc::now().timestamp_nanos_opt().unwrap_or_default();
    for ancestor in &meta.compacted_from {
        let ancestor = ancestor.to_string();
        conn.execute(
            "INSERT OR IGNORE INTO block_lineage \
             (ancestor_uuid, descendant_uuid, signal, date, observed_at) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![ancestor, descendant, meta.signal, date, observed_at],
        )?;
        conn.execute(
            "UPDATE blocks SET superseded = 1 WHERE uuid = ?1 AND uuid <> ?2",
            params![ancestor, descendant],
        )?;
    }
    Ok(())
}

/// Monotonic-max UPSERT into `wal_watermarks`, shared by the `&self`
/// [`Catalog::advance_watermark`] and the transactional `insert_block`
/// path. Generic over anything that derefs to a `Connection` (a
/// `Connection` or a `Transaction`) so both callers reuse one statement.
/// The `DO UPDATE` is gated on `excluded.seg_max > wal_watermarks.seg_max`,
/// so an out-of-order (older) observation can never roll the high-water
/// backward — exactly the `advance_cursor` idiom.
fn advance_watermark_in(
    conn: &Connection,
    writer_id: &str,
    signal: &str,
    shard: u32,
    seg_max: u64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO wal_watermarks (writer_id, signal, shard, seg_max) \
         VALUES (?1, ?2, ?3, ?4) \
         ON CONFLICT(writer_id, signal, shard) DO UPDATE SET \
           seg_max = excluded.seg_max \
         WHERE excluded.seg_max > wal_watermarks.seg_max",
        params![writer_id, signal, shard as i64, seg_max as i64],
    )
    .context("UPSERT wal_watermark")?;
    Ok(())
}

fn row_to_entry(row: &rusqlite::Row<'_>) -> rusqlite::Result<CatalogEntry> {
    let uuid_str: String = row.get(0)?;
    let bucket: String = row.get(1)?;
    let signal: String = row.get(2)?;
    let date: String = row.get(3)?;
    let writer_id_str: String = row.get(4)?;
    let level: i64 = row.get(5)?;
    let ts_min: i64 = row.get(6)?;
    let ts_max: i64 = row.get(7)?;
    let row_count: i64 = row.get(8)?;
    let byte_size: i64 = row.get(9)?;
    let schema_version: i64 = row.get(10)?;
    let fingerprint: Option<Vec<u8>> = row.get(11)?;
    let has_postings_raw: i64 = row.get(12)?;
    let postings_size_bytes: Option<i64> = row.get(13)?;
    let has_body_bloom_raw: i64 = row.get(14)?;
    let body_bloom_size_bytes: Option<i64> = row.get(15)?;
    let wal_seg_max: Option<i64> = row.get(16)?;
    let wal_shard: Option<i64> = row.get(17)?;

    let uuid = Uuid::parse_str(&uuid_str).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
    })?;
    let writer_id = Uuid::parse_str(&writer_id_str).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
    })?;

    Ok(CatalogEntry {
        meta: BlockMeta {
            uuid,
            signal,
            writer_id,
            ts_min_unix_nano: ts_min as u64,
            ts_max_unix_nano: ts_max as u64,
            row_count: row_count as u64,
            byte_size: byte_size as u64,
            schema_version: schema_version as u32,
            level: level as u32,
            // The producer_version isn't worth round-tripping through
            // the catalog — sidecar JSON has it. Empty string here is
            // the conventional "unknown" sentinel.
            producer_version: String::new(),
            label_fingerprint_bloom: fingerprint,
            has_postings: has_postings_raw != 0,
            postings_size_bytes: postings_size_bytes.map(|v| v as u64),
            // series_types lives only in the sidecar JSON; not promoted
            // to a catalog column because the catalog query patterns
            // don't filter on it. Callers that want type metadata go
            // through `reconcile_from_bucket` / read the sidecar.
            series_types: None,
            // Likewise: the full fingerprint list lives only in the
            // sidecar. Callers that hit the empty-matcher fallback
            // read the sidecar (see `scry_query::postings`).
            all_fingerprints: None,
            has_body_bloom: has_body_bloom_raw != 0,
            body_bloom_size_bytes: body_bloom_size_bytes.map(|v| v as u64),
            wal_seg_max: wal_seg_max.map(|v| v as u64),
            wal_shard: wal_shard.map(|v| v as u32),
            compacted_from: Vec::new(),
        },
        bucket,
        date,
        level: level as u32,
    })
}

/// Build a canonical `Path` for a block sidecar. Convenience for the
/// reconciler when a caller wants to validate object existence.
pub fn sidecar_path_for(entry: &CatalogEntry) -> ObjPath {
    ObjPath::from(scry_block::block_path(
        &entry.meta.signal,
        entry.meta.ts_min_unix_nano,
        entry.meta.writer_id,
        entry.meta.uuid,
        "meta.json",
    ))
}

// Catalog wraps a !Sync rusqlite::Connection. Spell out the required
// trait bounds so misuse fails at compile time, not at runtime.
// rusqlite::Connection is Send (it pins the underlying SQLite handle
// to the thread that opened it via Mutex), so Catalog is Send too;
// we just don't claim Sync.
const _ASSERT_SEND: fn() = || {
    fn is_send<T: Send>() {}
    is_send::<Catalog>();
};
