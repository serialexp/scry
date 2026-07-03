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
//! + read the warmed set (phase 1), drops the guard for the async postings
//! fetches (phase 2), then re-locks to upsert (phase 3).

use std::sync::{Arc, Mutex};

use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionConfig;
use object_store::ObjectStore;
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
/// unioned over). Cold blocks are fetched, enumerated, and upserted; a block
/// whose postings can't be fetched (a peer deleted it) is skipped this round —
/// not marked warmed — so a later request retries it.
async fn warm_label_cache(
    catalog: &Mutex<Catalog>,
    store: Arc<dyn ObjectStore>,
    postings: &PostingsCache,
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

    // Phase 2 — fetch + enumerate cold blocks (no catalog lock held).
    let mut warmed_pairs: Vec<(Uuid, Vec<(String, String)>)> = Vec::new();
    for entry in &candidates {
        if warm.contains(&entry.meta.uuid) {
            continue;
        }
        // No postings ⇒ nothing enumerable; still mark warmed (empty) so it
        // isn't refetched every request.
        if !entry.meta.has_postings {
            warmed_pairs.push((entry.meta.uuid, Vec::new()));
            continue;
        }
        match postings.get_or_fetch(store.clone(), &entry.meta).await {
            Ok(idx) => {
                let mut pairs = Vec::with_capacity(idx.entry_count());
                for lname in idx.label_names() {
                    for value in idx.label_values(lname) {
                        pairs.push((lname.to_string(), value.to_string()));
                    }
                }
                warmed_pairs.push((entry.meta.uuid, pairs));
            }
            Err(e) => {
                tracing::warn!(uuid = %entry.meta.uuid, error = %e,
                    "metadata: postings fetch failed; skipping block this round");
            }
        }
    }

    // Phase 3 — persist the newly warmed blocks (one lock).
    if !warmed_pairs.is_empty() {
        let guard = lock(catalog)?;
        for (uuid, pairs) in &warmed_pairs {
            if let Err(e) = guard.upsert_block_labels(*uuid, pairs) {
                tracing::warn!(uuid = %uuid, error = %e, "metadata: upsert_block_labels failed");
            }
        }
    }

    Ok(all_uuids)
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
    }
}

fn lock(catalog: &Mutex<Catalog>) -> Result<std::sync::MutexGuard<'_, Catalog>, MetaError> {
    catalog
        .lock()
        .map_err(|e| (QUERY_ERR_INTERNAL, format!("catalog mutex poisoned: {e}")))
}
