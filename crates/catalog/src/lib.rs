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

use std::path::Path;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use object_store::{path::Path as ObjPath, ObjectStore, ObjectStoreExt};
use rusqlite::{params, Connection, OptionalExtension};
use scry_block::BlockMeta;
use uuid::Uuid;

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
        // The DDL matches ARCHITECTURE.md § The catalog § Schema with
        // the `buckets` table omitted (one bucket in v0.1) and the
        // `blocks.bucket REFERENCES buckets(name)` FK relaxed to plain
        // TEXT. Both come back when multi-bucket lands; nothing about
        // the v0.1 rows needs to change for that migration.
        self.conn.execute_batch(
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
              superseded_by       TEXT REFERENCES blocks(uuid),
              deleted_at          INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_blocks_query
              ON blocks(signal, date, ts_min, ts_max)
              WHERE deleted_at IS NULL;

            CREATE INDEX IF NOT EXISTS idx_blocks_compact
              ON blocks(bucket, signal, date, level)
              WHERE deleted_at IS NULL;
            "#,
        )
        .context("initialising catalog schema")?;
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
        let rows = self.conn.execute(
            r#"
            INSERT OR IGNORE INTO blocks (
              uuid, bucket, signal, date, writer_id, level,
              ts_min, ts_max, row_count, byte_size,
              postings_size_bytes, has_postings,
              body_bloom_size_bytes, has_body_bloom,
              schema_version, fingerprint, superseded_by, deleted_at
            ) VALUES (
              ?1, ?2, ?3, ?4, ?5, 0,
              ?6, ?7, ?8, ?9,
              ?10, ?11,
              ?12, ?13,
              ?14, ?15, NULL, NULL
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
            ],
        )
        .context("INSERT OR IGNORE block")?;
        Ok(rows > 0)
    }

    /// List every non-deleted block, ordered by `(date, ts_min)`.
    pub fn list_blocks(&self) -> Result<Vec<CatalogEntry>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT uuid, bucket, signal, date, writer_id, level,
                   ts_min, ts_max, row_count, byte_size,
                   schema_version, fingerprint,
                   has_postings, postings_size_bytes,
                   has_body_bloom, body_bloom_size_bytes
            FROM blocks
            WHERE deleted_at IS NULL
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
                   has_body_bloom, body_bloom_size_bytes
            FROM blocks
            WHERE uuid = ?1
            "#,
        )?;
        let res = stmt
            .query_row(params![uuid.to_string()], row_to_entry)
            .optional()?;
        Ok(res)
    }

    /// Count of non-deleted blocks. Cheap; uses the partial index.
    pub fn block_count(&self) -> Result<usize> {
        let n: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM blocks WHERE deleted_at IS NULL",
            [],
            |r| r.get(0),
        )?;
        Ok(n as usize)
    }

    /// Walk the bucket, fetch every `*.meta.json`, parse it as a
    /// [`BlockMeta`], and `INSERT OR IGNORE` into the catalog. Used to
    /// bootstrap an empty catalog and to re-derive after corruption.
    ///
    /// Sidecars that fail to parse are logged and counted in
    /// [`ReconcileReport::failed`] but do not abort the reconcile;
    /// one bad sidecar shouldn't poison the rest of the bucket.
    pub async fn reconcile_from_bucket(
        &self,
        store: &dyn ObjectStore,
    ) -> Result<ReconcileReport> {
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

fn format_date(ts_unix_nano: u64) -> String {
    let secs = (ts_unix_nano / 1_000_000_000) as i64;
    DateTime::<Utc>::from_timestamp(secs, 0)
        .unwrap_or_else(Utc::now)
        .format("%Y-%m-%d")
        .to_string()
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

    let uuid = Uuid::parse_str(&uuid_str)
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e)))?;
    let writer_id = Uuid::parse_str(&writer_id_str)
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e)))?;

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

