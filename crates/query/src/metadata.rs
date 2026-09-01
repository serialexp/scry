//! Label discoverability: "what can I match on?" answered without a data scan.
//!
//! This is the shared core behind both the `scry query` daemon's
//! `LabelNamesRequest` / `LabelValuesRequest` wire handlers and the `scry get
//! --list-label-names` / `--label-values` CLI mode. Keeping one implementation
//! means the daemon and the one-shot CLI can never disagree about what labels a
//! signal exposes.
//!
//! **Model (D-050).** The label truth lives in the per-block postings sidecars
//! (`label_name, label_value, fingerprints`). Enumerating them on every request
//! would defeat the point, so we maintain a **materialized view** in the catalog
//! (`block_labels` / `block_labels_warmed`, keyed by block UUID) that is warmed
//! lazily: the first metadata request that sees a cold block fetches its
//! postings (through the shared [`PostingsCache`]), enumerates the pairs, and
//! upserts them. The cache is derived data, not a source of truth — it is
//! reaped with the block in `delete_blocks`, so it "expires" on block lifecycle,
//! and a fanned-out multi-instance deployment is a non-issue (each instance
//! warms its own catalog cache; a cold instance is only slower on the first
//! hit, never wrong).
//!
//! **Per-signal fidelity.** Metrics + logs get full-fidelity discovery from
//! postings. Traces carry no postings — the matchable labels are the promoted
//! resource columns ([`TRACE_PROMOTED_LABELS`]); names are that static set and
//! values come from a cheap `SELECT DISTINCT` over the candidate trace blocks.
//! Profiles carry their labels inside the opaque pprof blob, so metadata is
//! empty (the query form directs users to SQL there).
//!
//! **Locking discipline.** `rusqlite::Connection` (and therefore [`Catalog`])
//! is `!Sync`; callers wrap it in a `std::sync::Mutex`. The catalog lock is
//! never held across an `.await`: [`warm_label_cache`] locks to list candidates
//! + read the warmed set, then for each cold block projects only
//!   `(label_name,label_value)` from its postings sidecar without the lock and
//!   briefly re-locks to persist that block before moving to the next.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use tokio::sync::Semaphore;

use arrow::array::StringArray;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionConfig;
use futures::{StreamExt, TryStreamExt};
use object_store::{path::Path as ObjPath, ObjectStore, ObjectStoreExt};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use parquet::arrow::ProjectionMask;
use scry_block::block_path;
use scry_catalog::{Catalog, CatalogEntry};
use scry_proto::constants::{Signal, QUERY_ERR_BAD_REQUEST, QUERY_ERR_INTERNAL};
use uuid::Uuid;

use crate::traces::{
    list_traces_candidates, register_traces_table_from_candidates, TRACES_TABLE_NAME,
};
use crate::{
    list_metrics_candidates, logs::list_logs_candidates, promoted_column_for, PostingsCache, Query,
    TRACE_PROMOTED_LABELS,
};

/// A metadata failure: a `QUERY_ERR_*` code plus human context. The daemon maps
/// this into a `StreamError` frame; the CLI prints it and exits non-zero.
pub type MetaError = (u16, String);

/// Bounds for the reusable, process-local label metadata view.
#[derive(Clone, Debug)]
pub struct LabelMetadataConfig {
    /// Maximum number of projected postings sidecars read concurrently.
    pub read_parallelism: usize,
    /// Values retained for an ordinary label.
    pub values_per_label: usize,
    /// Values retained for the metric-name label (`__name__`).
    pub metric_names: usize,
}

impl Default for LabelMetadataConfig {
    fn default() -> Self {
        Self {
            read_parallelism: 16,
            values_per_label: 1_000,
            metric_names: 10_000,
        }
    }
}

/// Cheap, eventually-consistent coordinator counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LabelMetadataStats {
    /// Estimated retained heap for the suggestion indexes. This is neither RSS
    /// nor SQLite size; it counts string capacity plus conservative container
    /// node overhead.
    pub resident_bytes_estimate: usize,
    pub names: usize,
    pub values: usize,
    pub saturated_labels: usize,
    pub blocks_warmed: u64,
    pub projected_reads: u64,
    pub cache_hits: u64,
    pub fills_in_flight: usize,
    pub fill_failures: u64,
}

#[derive(Default)]
struct MetadataView {
    names: HashMap<u8, BTreeSet<String>>,
    values: HashMap<(u8, String), BTreeSet<String>>,
    saturated: HashSet<(u8, String)>,
    blocks: HashSet<Uuid>,
    names_count: usize,
    values_count: usize,
}

/// Reusable label metadata materialized view. Clones share the same view.
///
/// Suggestions are global (the union of everything merged or warmed), sorted,
/// and value lists are bounded by [`LabelMetadataConfig`]. The per-UUID flight
/// lock is process-wide, including across independently-created coordinators.
pub struct LabelMetadataCoordinator {
    config: LabelMetadataConfig,
    view: Mutex<MetadataView>,
    fill_permits: Arc<Semaphore>,
    resident_bytes_estimate: AtomicUsize,
    blocks_warmed: AtomicU64,
    projected_reads: AtomicU64,
    cache_hits: AtomicU64,
    fills_in_flight: AtomicUsize,
    fill_failures: AtomicU64,
}

impl LabelMetadataCoordinator {
    pub fn new(config: LabelMetadataConfig) -> Self {
        let read_parallelism = config.read_parallelism.max(1);
        Self {
            config,
            view: Mutex::new(MetadataView::default()),
            fill_permits: Arc::new(Semaphore::new(read_parallelism)),
            resident_bytes_estimate: AtomicUsize::new(0),
            blocks_warmed: AtomicU64::new(0),
            projected_reads: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            fills_in_flight: AtomicUsize::new(0),
            fill_failures: AtomicU64::new(0),
        }
    }

    pub fn config(&self) -> &LabelMetadataConfig {
        &self.config
    }

    /// Merge one already-warmed block loaded from persistent storage. Recording
    /// the UUID as well as its pairs is important: otherwise the first client
    /// after startup would revisit every persisted recent block just to learn
    /// that it was already warm. Empty pairs still mark a label-less block.
    pub fn merge_persisted_block<I, N, V>(&self, signal: Signal, uuid: Uuid, pairs: I)
    where
        I: IntoIterator<Item = (N, V)>,
        N: Into<String>,
        V: Into<String>,
    {
        self.merge_persisted_pairs(signal, pairs);
        let inserted = self
            .view
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .blocks
            .insert(uuid);
        if inserted {
            self.blocks_warmed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Merge pairs loaded by a caller from persistent storage (also useful for
    /// tests and incremental contributions whose block identity is unavailable).
    pub fn merge_persisted_pairs<I, N, V>(&self, signal: Signal, pairs: I)
    where
        I: IntoIterator<Item = (N, V)>,
        N: Into<String>,
        V: Into<String>,
    {
        let mut view = self.view.lock().unwrap_or_else(|e| e.into_inner());
        for (name, value) in pairs {
            let name = name.into();
            let value = value.into();
            let signal_byte = signal.as_u8();
            if view
                .names
                .entry(signal_byte)
                .or_default()
                .insert(name.clone())
            {
                view.names_count += 1;
                self.resident_bytes_estimate.fetch_add(
                    name.capacity() + estimated_tree_node_overhead(),
                    Ordering::Relaxed,
                );
            }
            let limit = if signal == Signal::Metrics && name == "__name__" {
                self.config.metric_names
            } else {
                self.config.values_per_label
            };
            let key = (signal_byte, name);
            if !view.values.contains_key(&key) {
                self.resident_bytes_estimate.fetch_add(
                    key.1.capacity() + 2 * size_of::<usize>() + size_of::<BTreeSet<String>>(),
                    Ordering::Relaxed,
                );
            }
            let mut added = false;
            let saturated = {
                let values = view.values.entry(key.clone()).or_default();
                if values.contains(&value) {
                    continue;
                }
                // Keep the lexicographically smallest bounded set, making results
                // deterministic regardless of completion order.
                if values.len() < limit {
                    self.resident_bytes_estimate.fetch_add(
                        value.capacity() + estimated_tree_node_overhead(),
                        Ordering::Relaxed,
                    );
                    values.insert(value);
                    added = true;
                    false
                } else {
                    if limit > 0 && values.last().is_some_and(|last| value < *last) {
                        let removed = values.pop_last().expect("non-empty bounded set");
                        self.resident_bytes_estimate
                            .fetch_sub(removed.capacity(), Ordering::Relaxed);
                        self.resident_bytes_estimate
                            .fetch_add(value.capacity(), Ordering::Relaxed);
                        values.insert(value);
                    }
                    true
                }
            };
            if added {
                view.values_count += 1;
            }
            if saturated {
                view.saturated.insert(key);
            }
        }
    }

    pub fn label_names(&self, signal: Signal) -> Vec<String> {
        self.view
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .names
            .get(&signal.as_u8())
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn label_values(&self, signal: Signal, name: &str) -> Vec<String> {
        self.view
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values
            .get(&(signal.as_u8(), name.to_owned()))
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn estimated_resident_bytes(&self) -> usize {
        self.resident_bytes_estimate.load(Ordering::Relaxed)
    }

    pub fn stats(&self) -> LabelMetadataStats {
        let view = self.view.lock().unwrap_or_else(|e| e.into_inner());
        LabelMetadataStats {
            resident_bytes_estimate: self.estimated_resident_bytes(),
            names: view.names_count,
            values: view.values_count,
            saturated_labels: view.saturated.len(),
            blocks_warmed: self.blocks_warmed.load(Ordering::Relaxed),
            projected_reads: self.projected_reads.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            fills_in_flight: self.fills_in_flight.load(Ordering::Relaxed),
            fill_failures: self.fill_failures.load(Ordering::Relaxed),
        }
    }

    /// Warm supplied catalog candidates with bounded parallel projected reads,
    /// then return the coordinator's global sorted label-name suggestions.
    pub async fn warm_candidates(
        &self,
        catalog: &Mutex<Catalog>,
        store: Arc<dyn ObjectStore>,
        signal: Signal,
        candidates: &[CatalogEntry],
    ) -> Result<Vec<String>, MetaError> {
        let parallelism = self.config.read_parallelism.max(1);
        futures::stream::iter(candidates.iter().cloned().map(|entry| {
            let store = store.clone();
            async move { self.warm_one(catalog, store, signal, entry).await }
        }))
        .buffer_unordered(parallelism)
        .try_collect::<Vec<_>>()
        .await?;
        Ok(self.label_names(signal))
    }

    async fn warm_one(
        &self,
        catalog: &Mutex<Catalog>,
        store: Arc<dyn ObjectStore>,
        signal: Signal,
        entry: CatalogEntry,
    ) -> Result<(), MetaError> {
        let uuid = entry.meta.uuid;
        if self
            .view
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .blocks
            .contains(&uuid)
        {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        let flight = uuid_flight(uuid);
        let _guard = flight.lock().await;
        if self
            .view
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .blocks
            .contains(&uuid)
        {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        let persisted = lock(catalog)?
            .warmed_blocks(&[uuid])
            .map_err(|e| (QUERY_ERR_INTERNAL, format!("warmed_blocks: {e:#}")))?
            .contains(&uuid);
        let pairs = if persisted {
            persisted_pairs(catalog, uuid)?
        } else if entry.meta.has_postings {
            let _permit = self
                .fill_permits
                .acquire()
                .await
                .expect("label metadata fill semaphore is never closed");
            self.projected_reads.fetch_add(1, Ordering::Relaxed);
            self.fills_in_flight.fetch_add(1, Ordering::Relaxed);
            let result = fetch_label_pairs(store, &entry.meta).await;
            self.fills_in_flight.fetch_sub(1, Ordering::Relaxed);
            result.map_err(|e| {
                self.fill_failures.fetch_add(1, Ordering::Relaxed);
                (
                    QUERY_ERR_INTERNAL,
                    format!("metadata postings {uuid}: {e:#}"),
                )
            })?
        } else {
            Vec::new()
        };

        // Persist before publishing anything in memory: callers never observe
        // partial success, and a failed/cancelled flight remains retryable.
        if !persisted {
            lock(catalog)?
                .upsert_block_labels(uuid, &pairs)
                .map_err(|e| {
                    (
                        QUERY_ERR_INTERNAL,
                        format!("upsert_block_labels {uuid}: {e:#}"),
                    )
                })?;
        }
        self.merge_persisted_pairs(signal, pairs);
        self.view
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .blocks
            .insert(uuid);
        self.blocks_warmed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

impl Default for LabelMetadataCoordinator {
    fn default() -> Self {
        Self::new(LabelMetadataConfig::default())
    }
}

fn estimated_tree_node_overhead() -> usize {
    // BTreeSet stores each String alongside links/occupancy metadata. Rust does
    // not expose the allocator's exact node layout, so use a stable conservative
    // estimate and label the resulting status gauge explicitly as an estimate.
    size_of::<String>() + 3 * size_of::<usize>()
}

fn uuid_flight(uuid: Uuid) -> Arc<tokio::sync::Mutex<()>> {
    static FLIGHTS: OnceLock<Mutex<HashMap<Uuid, Weak<tokio::sync::Mutex<()>>>>> = OnceLock::new();
    let mut flights = FLIGHTS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if let Some(lock) = flights.get(&uuid).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(tokio::sync::Mutex::new(()));
    flights.insert(uuid, Arc::downgrade(&lock));
    lock
}

fn persisted_pairs(
    catalog: &Mutex<Catalog>,
    uuid: Uuid,
) -> Result<Vec<(String, String)>, MetaError> {
    let guard = lock(catalog)?;
    let names = guard
        .distinct_label_names(&[uuid])
        .map_err(|e| (QUERY_ERR_INTERNAL, format!("distinct_label_names: {e:#}")))?;
    let mut pairs = Vec::new();
    for name in names {
        let values = guard
            .distinct_label_values(&name, &[uuid])
            .map_err(|e| (QUERY_ERR_INTERNAL, format!("distinct_label_values: {e:#}")))?;
        pairs.extend(values.into_iter().map(|value| (name.clone(), value)));
    }
    Ok(pairs)
}

/// Distinct, sorted label names for a signal over an optional `[ts_min, ts_max]`
/// window. See the module docs for per-signal fidelity.
pub async fn collect_label_names(
    catalog: &Mutex<Catalog>,
    store: Arc<dyn ObjectStore>,
    postings: &PostingsCache,
    runtime_env: Arc<RuntimeEnv>,
    signal: Signal,
    q: &Query,
) -> Result<Vec<String>, MetaError> {
    // runtime_env is only needed by the traces DISTINCT (values) path; names
    // never run SQL. Kept in the signature so both collect_* share one shape.
    let _ = runtime_env;
    match signal {
        Signal::Metrics | Signal::Logs => {
            let uuids = warm_label_cache(catalog, store, postings, signal, q).await?;
            let guard = lock(catalog)?;
            guard
                .distinct_label_names(&uuids)
                .map_err(|e| (QUERY_ERR_INTERNAL, format!("distinct_label_names: {e:#}")))
        }
        Signal::Traces => Ok(TRACE_PROMOTED_LABELS
            .iter()
            .map(|s| s.to_string())
            .collect()),
        Signal::Profiles => Ok(Vec::new()),
        other => Err((
            QUERY_ERR_BAD_REQUEST,
            format!("signal {other:?} has no label metadata"),
        )),
    }
}

/// Distinct, sorted values for one label name over a signal + optional window.
pub async fn collect_label_values(
    catalog: &Mutex<Catalog>,
    store: Arc<dyn ObjectStore>,
    postings: &PostingsCache,
    runtime_env: Arc<RuntimeEnv>,
    signal: Signal,
    name: &str,
    q: &Query,
) -> Result<Vec<String>, MetaError> {
    match signal {
        Signal::Metrics | Signal::Logs => {
            let uuids = warm_label_cache(catalog, store, postings, signal, q).await?;
            let guard = lock(catalog)?;
            guard
                .distinct_label_values(name, &uuids)
                .map_err(|e| (QUERY_ERR_INTERNAL, format!("distinct_label_values: {e:#}")))
        }
        Signal::Traces => trace_label_values(catalog, store, postings, runtime_env, name, q).await,
        Signal::Profiles => Ok(Vec::new()),
        other => Err((
            QUERY_ERR_BAD_REQUEST,
            format!("signal {other:?} has no label metadata"),
        )),
    }
}

/// Ensure the label cache is warm for every candidate block of a metrics/logs
/// metadata query, returning the candidate block UUIDs (the set the answer is
/// unioned over). Cold blocks are fetched, enumerated, and upserted. A fetch
/// failure fails the whole provisional metadata attempt: returning a partial
/// union would silently hide labels, while the daemon can repair a peer deletion
/// and retry from a fresh candidate listing.
async fn warm_label_cache(
    catalog: &Mutex<Catalog>,
    store: Arc<dyn ObjectStore>,
    _postings: &PostingsCache,
    signal: Signal,
    q: &Query,
) -> Result<Vec<Uuid>, MetaError> {
    // Phase 1 — list candidates + which are already warm (one lock).
    let (candidates, warm) = {
        let guard = lock(catalog)?;
        let candidates: Vec<CatalogEntry> = match signal {
            Signal::Metrics => list_metrics_candidates(&guard, q),
            Signal::Logs => list_logs_candidates(&guard, q),
            other => {
                return Err((
                    QUERY_ERR_INTERNAL,
                    format!("BUG: warm_label_cache called for {other:?}"),
                ))
            }
        }
        .map_err(|e| (QUERY_ERR_INTERNAL, format!("list candidates: {e:#}")))?;
        let uuids: Vec<Uuid> = candidates.iter().map(|c| c.meta.uuid).collect();
        let warm = guard
            .warmed_blocks(&uuids)
            .map_err(|e| (QUERY_ERR_INTERNAL, format!("warmed_blocks: {e:#}")))?;
        (candidates, warm)
    };

    let all_uuids: Vec<Uuid> = candidates.iter().map(|c| c.meta.uuid).collect();

    // Preserve the exact one-shot API while using an ephemeral coordinator for
    // bounded parallel reads and the process-wide UUID single-flight registry.
    // Already-persisted candidates need no object-store read.
    let cold: Vec<CatalogEntry> = candidates
        .iter()
        .filter(|entry| !warm.contains(&entry.meta.uuid))
        .cloned()
        .collect();
    LabelMetadataCoordinator::default()
        .warm_candidates(catalog, store, signal, &cold)
        .await?;

    Ok(all_uuids)
}

/// Read only the two scalar columns metadata discovery needs from one postings
/// sidecar. In particular, do **not** decode or retain the `fingerprints`
/// `List<u64>` column: on high-cardinality metrics that column is nearly the
/// entire sidecar and materialising it through [`PostingsCache`] made a small
/// field-list request consume hundreds of MiB.
async fn fetch_label_pairs(
    store: Arc<dyn ObjectStore>,
    meta: &scry_block::BlockMeta,
) -> anyhow::Result<Vec<(String, String)>> {
    let path = ObjPath::from(block_path(
        &meta.signal,
        meta.ts_min_unix_nano,
        meta.writer_id,
        meta.uuid,
        "postings.parquet",
    ));
    let object_meta = store.head(&path).await.map_err(anyhow::Error::from)?;
    let reader = ParquetObjectReader::new(store, path.clone()).with_file_size(object_meta.size);
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    let projection = ProjectionMask::leaves(builder.parquet_schema(), [0, 1]);
    let mut stream = builder.with_projection(projection).build()?;
    let mut pairs = Vec::new();

    while let Some(batch) = stream.try_next().await? {
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("postings col 0 not StringArray"))?;
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("postings col 1 not StringArray"))?;
        pairs.reserve(batch.num_rows());
        for row in 0..batch.num_rows() {
            pairs.push((names.value(row).to_owned(), values.value(row).to_owned()));
        }
    }
    Ok(pairs)
}

/// Distinct values of a promoted trace label, via a `SELECT DISTINCT` over the
/// candidate trace blocks. Unknown (non-promoted) names return empty.
async fn trace_label_values(
    catalog: &Mutex<Catalog>,
    store: Arc<dyn ObjectStore>,
    postings: &PostingsCache,
    runtime_env: Arc<RuntimeEnv>,
    name: &str,
    q: &Query,
) -> Result<Vec<String>, MetaError> {
    let Some(col) = promoted_column_for(name) else {
        return Ok(Vec::new());
    };
    let candidates: Vec<CatalogEntry> = {
        let guard = lock(catalog)?;
        list_traces_candidates(&guard, q)
            .map_err(|e| (QUERY_ERR_INTERNAL, format!("list_traces_candidates: {e:#}")))?
    };
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime_env);
    register_traces_table_from_candidates(&ctx, candidates, store, Some(postings), q)
        .await
        .map_err(|e| (QUERY_ERR_INTERNAL, format!("register traces table: {e:#}")))?;
    // arrow_cast normalises Utf8/Utf8View to a plain Utf8 output column so the
    // downcast below is unconditional; DISTINCT + ORDER BY dedupes + sorts.
    let sql = format!(
        "SELECT DISTINCT arrow_cast({col}, 'Utf8') AS v FROM {TRACES_TABLE_NAME} \
         WHERE {col} IS NOT NULL AND {col} <> '' ORDER BY v"
    );
    let df = ctx
        .sql(&sql)
        .await
        .map_err(|e| (QUERY_ERR_INTERNAL, format!("traces distinct sql: {e:#}")))?;
    let batches = df.collect().await.map_err(|e| {
        (
            QUERY_ERR_INTERNAL,
            format!("traces distinct collect: {e:#}"),
        )
    })?;
    let mut out = Vec::new();
    for batch in batches {
        use datafusion::arrow::array::Array;
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .ok_or_else(|| {
                (
                    QUERY_ERR_INTERNAL,
                    "traces distinct: expected Utf8 column".to_string(),
                )
            })?;
        for i in 0..arr.len() {
            if arr.is_valid(i) {
                out.push(arr.value(i).to_string());
            }
        }
    }
    Ok(out)
}

/// Build a candidate-selection [`Query`] from a metadata request's time bounds.
/// Metadata requests carry no matchers/sql — only the signal + an optional
/// `[ts_min, ts_max]` window (the `*_present` companion is the binschema-optional
/// convention). Reusing the same candidate path as a data query means a metadata
/// answer covers exactly the blocks a query over the same window would touch.
pub fn meta_query(ts_min: Option<u64>, ts_max: Option<u64>) -> Query {
    Query {
        matchers: Vec::new(),
        ts_min,
        ts_max,
        trace_id: None,
        body_contains: None,
        with_labels: false,
    }
}

fn lock(catalog: &Mutex<Catalog>) -> Result<std::sync::MutexGuard<'_, Catalog>, MetaError> {
    catalog
        .lock()
        .map_err(|e| (QUERY_ERR_INTERNAL, format!("catalog mutex poisoned: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suggestion_values_are_bounded_sorted_and_report_saturation() {
        let cache = LabelMetadataCoordinator::new(LabelMetadataConfig {
            read_parallelism: 2,
            values_per_label: 2,
            metric_names: 3,
        });
        cache.merge_persisted_pairs(
            Signal::Metrics,
            [
                ("env", "z"),
                ("env", "a"),
                ("env", "m"),
                ("__name__", "z_metric"),
                ("__name__", "a_metric"),
                ("__name__", "m_metric"),
                ("__name__", "b_metric"),
            ],
        );

        assert_eq!(cache.label_names(Signal::Metrics), vec!["__name__", "env"]);
        assert_eq!(cache.label_values(Signal::Metrics, "env"), vec!["a", "m"]);
        assert_eq!(
            cache.label_values(Signal::Metrics, "__name__"),
            vec!["a_metric", "b_metric", "m_metric"]
        );
        let stats = cache.stats();
        assert_eq!(stats.names, 2);
        assert_eq!(stats.values, 5);
        assert_eq!(stats.saturated_labels, 2);
        assert!(stats.resident_bytes_estimate > 0);
    }

    #[test]
    fn metric_name_limit_does_not_apply_to_logs() {
        let cache = LabelMetadataCoordinator::new(LabelMetadataConfig {
            read_parallelism: 1,
            values_per_label: 1,
            metric_names: 10,
        });
        cache.merge_persisted_pairs(Signal::Logs, [("__name__", "z"), ("__name__", "a")]);
        assert_eq!(cache.label_values(Signal::Logs, "__name__"), vec!["a"]);
        assert_eq!(cache.stats().saturated_labels, 1);
    }

    #[test]
    fn defaults_match_queryd_autocomplete_budget() {
        let config = LabelMetadataConfig::default();
        assert_eq!(config.read_parallelism, 16);
        assert_eq!(config.values_per_label, 1_000);
        assert_eq!(config.metric_names, 10_000);
    }
}
