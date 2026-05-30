//! Compaction pass driver: plan → merge → supersede → (grace) → delete.
//!
//! `compact_once` runs a single pass over the catalog's live blocks. For
//! each planned merge it executes the full
//! `ARCHITECTURE.md § Compaction § Per-merge sequence` lifecycle:
//!
//! 1. Merge the inputs into one block at the next level (uploaded, meta
//!    last).
//! 2. Insert the merged block into the catalog.
//! 3. Mark the inputs `superseded_by = merged` — **at this point queries
//!    read the merged block, not the inputs** (the query path filters
//!    `superseded_by IS NULL`).
//! 4. Wait the configured grace period (default 0 single-instance).
//! 5. Delete the input objects from the bucket.
//! 6. Drop the input catalog rows.
//!
//! The catalog is derived state, so the bucket truth (step 5) is removed
//! before the catalog rows (step 6). If a merge fails partway, the
//! immutable + content-addressed design means the worst case is an
//! orphaned merged block that the next pass treats as just another input
//! at its level — correctness is never at risk.

use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::ObjectStore;
use scry_block::{delete_block_objects, BlockBuilderConfig};
use scry_catalog::Catalog;
use uuid::Uuid;

use crate::merge::merge_blocks;
use crate::policy::{plan_merges, CompactConfig};

/// Outcome of one [`compact_once`] pass.
#[derive(Debug, Clone, Default)]
pub struct CompactReport {
    /// Number of merges executed.
    pub merges: usize,
    /// Total input blocks consumed across all merges.
    pub blocks_in: usize,
    /// Merged blocks produced (one per merge).
    pub blocks_out: usize,
    /// On-disk bytes of the merged main parquets produced.
    pub bytes_out: u64,
}

/// Run a single compaction pass. Returns a report; an empty report
/// (`merges == 0`) means no partition had enough blocks to compact.
pub async fn compact_once(
    store: Arc<dyn ObjectStore>,
    catalog: &Catalog,
    bucket: &str,
    cfg: &CompactConfig,
    block_cfg: &BlockBuilderConfig,
) -> Result<CompactReport> {
    let live = catalog.list_blocks().context("list live blocks")?;
    let plans = plan_merges(&live, cfg);
    let mut report = CompactReport::default();

    // One compactor identity for this pass — block paths are
    // content-addressed under it (`<signal>/.../<writer_id>/<uuid>`).
    let writer_id = Uuid::now_v7();

    for plan in plans {
        let input_uuids: Vec<Uuid> = plan.inputs.iter().map(|e| e.meta.uuid).collect();
        tracing::info!(
            signal = %plan.signal,
            date = %plan.date,
            input_level = plan.input_level,
            output_level = plan.output_level(),
            inputs = plan.inputs.len(),
            "compacting partition"
        );

        // 1. Merge → upload (meta last).
        let merged = merge_blocks(
            store.clone(),
            bucket,
            &plan.signal,
            &plan.inputs,
            plan.output_level(),
            writer_id,
            block_cfg,
        )
        .await
        .with_context(|| format!("merging {} {} blocks", plan.inputs.len(), plan.signal))?;

        // 2. Insert the merged block.
        catalog
            .insert_block(&merged)
            .context("insert merged block")?;

        // 3. Supersede the inputs — queries now skip them.
        catalog
            .mark_superseded(&input_uuids, merged.uuid)
            .context("mark inputs superseded")?;

        // 4. Grace period (single-instance default is 0).
        if !cfg.grace.is_zero() {
            tokio::time::sleep(cfg.grace).await;
        }

        // 5. Delete the input objects from the bucket.
        for input in &plan.inputs {
            delete_block_objects(store.as_ref(), &input.meta)
                .await
                .context("delete superseded input objects")?;
        }

        // 6. Drop the input catalog rows.
        catalog
            .delete_blocks(&input_uuids)
            .context("delete superseded input rows")?;

        report.merges += 1;
        report.blocks_in += plan.inputs.len();
        report.blocks_out += 1;
        report.bytes_out += merged.byte_size;
    }

    Ok(report)
}
