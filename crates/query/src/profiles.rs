//! DataFusion `TableProvider` over profiles blocks, plus the register-
//! helpers that match `lib.rs`'s metrics/logs shape.
//!
//! Logically, the "profiles" table is the row-wise union of every
//! profiles block currently in the catalog whose time range overlaps
//! the query window. Each block contributes its `<block>.parquet`,
//! whose schema is exactly
//! [`scry_block::ProfilesBlockBuilder::main_schema`] — one row per
//! profile blob (`ts_unix_nano`, `duration_nano`, `labels`
//! `Map<Utf8,Utf8>`, `format` UInt8, `data` Binary), sorted by
//! `ts_unix_nano`.
//!
//! ## Retrieval only (v0.6)
//!
//! This vertical is **retrieval**: select profile rows by time (and,
//! via `--sql`, by label) and stream the rows back, including the raw
//! pprof `data` blob — a loss-free round-trip, like logs. **Flamegraph
//! aggregation is deliberately out of scope** (see `docs/decisions.md`
//! D-034): Grafana's flamegraph panel renders *pre-aggregated* data —
//! the Pyroscope/Phlare backend parses pprof and merges stacks
//! server-side, the UI never parses raw pprof — so aggregation is
//! backend/query work that belongs to a later stage, once something
//! consumes it.
//!
//! ## No postings — predicates instead
//!
//! Like traces, profiles blocks carry **no postings sidecar**. Time
//! filters become row-filter predicates pushed into `ParquetSource`;
//! label filtering rides `--sql` against the `labels` Map (no promoted
//! columns at v0.6). A bare `--matcher` is rejected up front rather
//! than silently ignored.

use std::any::Any;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DFSchema, Result as DfResult, ScalarValue};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::memory::DataSourceExec;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{col, Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::expressions::lit as physical_lit;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use object_store::ObjectStore;
use scry_block::{block_path, ProfilesBlockBuilder};
use scry_catalog::{Catalog, CatalogEntry};

use crate::postings_cache::PostingsCache;
use crate::table::{object_store_url_for, time_overlaps};
use crate::Query;

/// Default name the [`ProfilesTable`] is registered under in a
/// `SessionContext`. The CLI and the query daemon agree on this so
/// users can write `SELECT … FROM profiles …` without thinking about it.
pub const PROFILES_TABLE_NAME: &str = "profiles";

/// The Arrow schema of a profiles block's main parquet. Reused verbatim
/// from the block writer ([`ProfilesBlockBuilder::main_schema`]) so the
/// registered table type can never drift from the on-disk parquet type.
fn profiles_schema() -> SchemaRef {
    ProfilesBlockBuilder::main_schema()
}

/// One catalog row for a profiles block. No postings ⇒ no fingerprint
/// set; the parquet predicate (time bounds, built in
/// [`ProfilesTable::scan`]) plus any `--sql` carry all selectivity.
#[derive(Debug, Clone)]
pub struct ProfilesBlockEntry {
    pub entry: CatalogEntry,
}

/// `TableProvider` for the profiles signal. One instance per call to
/// [`register_profiles_table`] — carries a snapshot of catalog rows
/// plus the query's time bounds, so `scan()` stays pure CPU.
pub struct ProfilesTable {
    schema: SchemaRef,
    object_store_url: ObjectStoreUrl,
    blocks: Vec<ProfilesBlockEntry>,
    ts_min: Option<u64>,
    ts_max: Option<u64>,
}

impl std::fmt::Debug for ProfilesTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProfilesTable")
            .field("blocks", &self.blocks.len())
            .field("ts_min", &self.ts_min)
            .field("ts_max", &self.ts_max)
            .finish()
    }
}

impl ProfilesTable {
    pub fn new(
        bucket: &str,
        blocks: Vec<ProfilesBlockEntry>,
        ts_min: Option<u64>,
        ts_max: Option<u64>,
    ) -> DfResult<Self> {
        Ok(Self {
            schema: profiles_schema(),
            object_store_url: object_store_url_for(bucket)?,
            blocks,
            ts_min,
            ts_max,
        })
    }

    pub fn object_store_url(&self) -> &ObjectStoreUrl {
        &self.object_store_url
    }

    pub fn blocks(&self) -> &[ProfilesBlockEntry] {
        &self.blocks
    }
}

#[async_trait]
impl TableProvider for ProfilesTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // ── Per-block plan emission ────────────────────────────────
        //
        // Same structural shape as `TracesTable::scan`, minus the
        // promoted-column / trace-id predicates (profiles have none at
        // v0.6). We push DataFusion-supplied `filters` (from --sql) and
        // the time bounds over `ts_unix_nano`.
        let df_schema = DFSchema::try_from(self.schema())?;

        let make_branch = |file_path: String, file_size: u64| -> DfResult<Arc<dyn ExecutionPlan>> {
            let mut block_filters: Vec<Expr> = filters.to_vec();

            if let Some(min) = self.ts_min {
                block_filters.push(
                    col("ts_unix_nano").gt_eq(Expr::Literal(ScalarValue::UInt64(Some(min)), None)),
                );
            }
            if let Some(max) = self.ts_max {
                block_filters.push(
                    col("ts_unix_nano").lt_eq(Expr::Literal(ScalarValue::UInt64(Some(max)), None)),
                );
            }

            let predicate = conjunction(block_filters)
                .map(|p| state.create_physical_expr(p, &df_schema))
                .transpose()?
                .unwrap_or_else(|| physical_lit(true));

            let source = Arc::new(
                ParquetSource::new(self.schema())
                    .with_predicate(predicate)
                    .with_pushdown_filters(true),
            );
            let builder = FileScanConfigBuilder::new(self.object_store_url.clone(), source)
                .with_projection_indices(projection.cloned())?
                .with_limit(limit)
                .with_file(PartitionedFile::new(file_path, file_size));

            Ok(DataSourceExec::from_data_source(builder.build()))
        };

        if self.blocks.is_empty() {
            // No overlapping blocks. Emit a single empty
            // `DataSourceExec` so `SELECT count(*) FROM profiles`
            // returns 0 cleanly. Mirrors metrics/logs/traces behaviour.
            let source = Arc::new(ParquetSource::new(self.schema()).with_pushdown_filters(true));
            let builder = FileScanConfigBuilder::new(self.object_store_url.clone(), source)
                .with_projection_indices(projection.cloned())?
                .with_limit(limit);
            return Ok(DataSourceExec::from_data_source(builder.build()));
        }

        let mut branches: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(self.blocks.len());
        for block in &self.blocks {
            let meta = &block.entry.meta;
            let path = block_path(
                &meta.signal,
                meta.ts_min_unix_nano,
                meta.writer_id,
                meta.uuid,
                "parquet",
            );
            branches.push(make_branch(path, meta.byte_size)?);
        }

        UnionExec::try_new(branches)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

// ── Catalog + register helpers ────────────────────────────────────

/// Synchronous step: list catalog blocks, filter by `signal="profiles"`
/// and the query's time bounds. Pure compute over the connection;
/// callers wrapping the catalog in a mutex (the query daemon) can drop
/// the guard before doing async work.
pub fn list_profiles_candidates(catalog: &Catalog, q: &Query) -> Result<Vec<CatalogEntry>> {
    Ok(catalog
        .list_blocks()
        .context("listing blocks from catalog")?
        .into_iter()
        .filter(|e| e.meta.signal == "profiles")
        .filter(|e| time_overlaps(&e.meta, q.ts_min, q.ts_max))
        .collect())
}

/// Async step: take the already-narrowed catalog list (per
/// [`list_profiles_candidates`]) and produce a ready-to-register
/// [`ProfilesTable`]. No postings resolve — the `cache` argument is
/// accepted only for signature symmetry with the metrics/logs path
/// (so the query daemon's dispatch stays uniform) and is ignored.
///
/// Label matchers aren't pushed at v0.6 (no postings, no promoted
/// columns); a non-empty `q.matchers` is rejected with a pointer to
/// `--sql` rather than silently ignored (which would over-return rows).
pub async fn build_profiles_table_from_candidates(
    candidates: Vec<CatalogEntry>,
    _store: Arc<dyn ObjectStore>,
    _cache: Option<&PostingsCache>,
    q: &Query,
) -> Result<ProfilesTable> {
    anyhow::ensure!(
        q.matchers.is_empty(),
        "profiles label matchers aren't supported yet; \
         filter the `labels` Map via --sql instead"
    );

    let bucket = match candidates.first() {
        Some(first) => {
            let b = first.bucket.clone();
            anyhow::ensure!(
                candidates.iter().all(|e| e.bucket == b),
                "profiles blocks span multiple buckets; multi-bucket queries not yet supported"
            );
            b
        }
        None => {
            // No overlapping blocks. Return an empty table so
            // `SELECT count(*) FROM profiles` still works.
            return Ok(ProfilesTable::new("", Vec::new(), q.ts_min, q.ts_max)?);
        }
    };

    let blocks: Vec<ProfilesBlockEntry> = candidates
        .into_iter()
        .map(|entry| ProfilesBlockEntry { entry })
        .collect();

    ProfilesTable::new(&bucket, blocks, q.ts_min, q.ts_max)
        .map_err(|e| anyhow::anyhow!("constructing ProfilesTable: {e}"))
}

/// Build the table (catalog narrow only) and register it on `ctx`
/// under `"profiles"`. Also registers `store` against the table's
/// `ObjectStoreUrl` so DataFusion can route reads.
pub async fn register_profiles_table(
    ctx: &SessionContext,
    catalog: &Catalog,
    store: Arc<dyn ObjectStore>,
    q: &Query,
) -> Result<()> {
    let candidates = list_profiles_candidates(catalog, q)?;
    register_profiles_table_from_candidates(ctx, candidates, store, None, q).await
}

/// Same as [`register_profiles_table`] but accepts pre-listed catalog
/// entries — for callers that take the catalog lock for the sync
/// `list_profiles_candidates` call themselves (the query daemon, where
/// the `Catalog` lives behind a `Mutex`).
pub async fn register_profiles_table_from_candidates(
    ctx: &SessionContext,
    candidates: Vec<CatalogEntry>,
    store: Arc<dyn ObjectStore>,
    cache: Option<&PostingsCache>,
    q: &Query,
) -> Result<()> {
    let table = build_profiles_table_from_candidates(candidates, store.clone(), cache, q).await?;
    let url: &url::Url = table.object_store_url().as_ref();
    ctx.runtime_env().register_object_store(url, store);
    ctx.register_table(PROFILES_TABLE_NAME, Arc::new(table))
        .map_err(|e| anyhow::anyhow!("register profiles table: {e}"))?;
    Ok(())
}

/// One-shot convenience for the local (no-daemon) path: build a fresh
/// `SessionContext`, register the profiles table, return the `profiles`
/// `DataFrame`. Mirrors `metrics_query`/`logs_query`.
pub async fn profiles_query(
    catalog: &Catalog,
    store: Arc<dyn ObjectStore>,
    q: &Query,
) -> Result<datafusion::prelude::DataFrame> {
    let ctx = SessionContext::new();
    register_profiles_table(&ctx, catalog, store, q).await?;
    ctx.table(PROFILES_TABLE_NAME)
        .await
        .with_context(|| format!("looking up table {PROFILES_TABLE_NAME}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The query-side schema must be byte-identical to the block
    /// writer's, or DataFusion errors at scan time on a type mismatch.
    #[test]
    fn schema_matches_block_writer() {
        assert_eq!(profiles_schema(), ProfilesBlockBuilder::main_schema());
        // The opaque pprof blob is carried as Binary and must survive
        // a `SELECT *` round-trip untouched.
        let f = profiles_schema().field_with_name("data").unwrap().clone();
        assert_eq!(
            f.data_type(),
            &datafusion::arrow::datatypes::DataType::Binary
        );
    }

    /// Label matchers aren't pushable at v0.6 — they must be rejected
    /// (with a pointer to --sql) rather than silently ignored, which
    /// would over-return rows and break the loss-free guarantee.
    #[tokio::test]
    async fn label_matcher_is_rejected() {
        let q = Query {
            matchers: vec![("service".into(), "api".into())],
            ..Default::default()
        };
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let err = build_profiles_table_from_candidates(Vec::new(), store, None, &q)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("--sql"), "got: {err}");
    }
}
