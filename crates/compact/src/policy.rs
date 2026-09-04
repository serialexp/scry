//! Compaction policy — which blocks to merge, into what.
//!
//! Size-tiered (`ARCHITECTURE.md § Compaction § Compaction policy`):
//! blocks live at a `level`, and when a `(signal, schema version, date, level)`
//! partition accumulates at least `fanout` blocks we merge the `fanout`
//! **smallest** of them into one block at `level + 1`. Size-tiered (vs
//! LevelDB-style levelled) keeps write amplification low — each byte is
//! rewritten ~`log_fanout(total)` times — which suits append-mostly
//! observability data.
//!
//! This planner emits **one** merge per qualifying partition per pass
//! (the `fanout` smallest blocks). Repeated passes — `--once` invoked
//! again, or the `--watch` loop — converge a backlog; a single pass is
//! intentionally bounded and predictable.

use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use scry_catalog::CatalogEntry;
use uuid::Uuid;

/// Tunables for a compaction pass.
#[derive(Debug, Clone)]
pub struct CompactConfig {
    /// Minimum blocks in a partition to trigger a merge, and the number
    /// merged per pass. Architecture default is 8.
    pub fanout: usize,
    /// Don't compact blocks at or above this level (L3 is the practical
    /// ceiling — past it individual parquet files get large enough that
    /// random-access reads suffer). Default 3.
    pub max_level: u32,
    /// Delay between marking inputs superseded and deleting their
    /// objects. The query side skips superseded blocks immediately, so
    /// single-instance correctness doesn't need a wait; a non-zero grace
    /// guards against any concurrent reader still mid-scan. Default 0 for
    /// the one-shot tool.
    pub grace: Duration,
    /// If set, only compact this signal; otherwise every signal.
    pub signal_filter: Option<String>,
    /// Maximum partitions to merge concurrently. Each partition's merge is
    /// independent (different blocks, different lease key), so there is no
    /// data-level reason for serial execution — the serial loop was just the
    /// first thing that worked. Default 1 preserves the old behaviour for
    /// anyone who does not opt in.
    pub parallelism: usize,
}

impl Default for CompactConfig {
    fn default() -> Self {
        Self {
            fanout: 8,
            max_level: 3,
            grace: Duration::ZERO,
            signal_filter: None,
            parallelism: 1,
        }
    }
}

impl CompactConfig {
    /// Validate that this policy can encode every output's complete ancestry.
    ///
    /// A block at level `max_level` represents `fanout + fanout² + …`
    /// ancestors. Sidecars deliberately cap that closure, so rejecting an
    /// incompatible policy at startup avoids a compactor that only fails after
    /// it has already built a deep merge tree.
    ///
    /// This models a **uniform** tree — every block at level `n` built by
    /// merging exactly `fanout` blocks at `n-1` under *this* config. Blocks
    /// already in the bucket may have been built under a different `--fanout`,
    /// so passing here does not prove existing partitions are mergeable; see
    /// [`validate_against_catalog`] for the check against real ancestry.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.fanout >= 2, "compaction fanout must be at least 2");
        anyhow::ensure!(
            self.max_level >= 1,
            "compaction max level must be at least 1"
        );

        let mut represented = 0usize;
        let mut width = 1usize;
        for _ in 0..self.max_level {
            width = width.checked_mul(self.fanout).ok_or_else(|| {
                anyhow::anyhow!("compaction fanout/level ancestry size overflows usize")
            })?;
            represented = represented.checked_add(width).ok_or_else(|| {
                anyhow::anyhow!("compaction fanout/level ancestry size overflows usize")
            })?;
            anyhow::ensure!(
                represented <= scry_block::MAX_COMPACTED_ANCESTORS,
                "compaction fanout {} through level {} requires {} ancestors, exceeding the sidecar limit of {}",
                self.fanout,
                self.max_level,
                represented,
                scry_block::MAX_COMPACTED_ANCESTORS
            );
        }
        Ok(())
    }
}

/// One planned merge: the inputs (already chosen, the `fanout` smallest
/// in their partition) and the level their merged output lands at.
#[derive(Debug, Clone)]
pub struct PlannedMerge {
    pub signal: String,
    pub date: String,
    /// Level of the inputs. Output is `input_level + 1`.
    pub input_level: u32,
    pub inputs: Vec<CatalogEntry>,
}

impl PlannedMerge {
    pub fn output_level(&self) -> u32 {
        self.input_level + 1
    }
}

/// A partition that had enough blocks to merge, but whose output could not
/// encode its own ancestor closure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OversizedPartition {
    pub signal: String,
    pub date: String,
    pub input_level: u32,
    /// Distinct ancestors the merged output would have had to record.
    pub projected_ancestors: usize,
}

/// The result of planning one pass.
#[derive(Debug, Clone, Default)]
pub struct CompactionPlan {
    /// Merges to execute this pass.
    pub merges: Vec<PlannedMerge>,
    /// Qualifying partitions deliberately **not** merged because the output's
    /// `compacted_from` closure would exceed
    /// [`scry_block::MAX_COMPACTED_ANCESTORS`].
    pub oversized: Vec<OversizedPartition>,
}

/// Number of distinct UUIDs a merge of `inputs` would have to record as its
/// complete transitive ancestry — each input plus each input's own closure.
///
/// Mirrors what [`scry_block::compacted_ancestor_closure`] builds, so the
/// planner can decline a merge the block layer would refuse. Cheaper than the
/// real thing (no validation, no sort) because the planner only needs the size.
pub fn projected_ancestry_len(inputs: &[CatalogEntry]) -> usize {
    let mut closure: HashSet<Uuid> = HashSet::new();
    for e in inputs {
        closure.insert(e.meta.uuid);
        closure.extend(e.meta.compacted_from.iter().copied());
    }
    closure.len()
}

/// Check the configured policy against blocks that **already exist**, rather
/// than against the uniform tree [`CompactConfig::validate`] models.
///
/// Returns the partitions whose next merge cannot be encoded. Intended as a
/// startup warning: changing `--compact-fanout` between runs can leave a
/// partition whose real ancestry (built under the old fanout) overflows under
/// the new one, and it is far better to say so at boot than to have that
/// partition quietly never compact again.
pub fn validate_against_catalog(
    blocks: &[CatalogEntry],
    cfg: &CompactConfig,
) -> Vec<OversizedPartition> {
    plan_merges(blocks, cfg).oversized
}

/// Plan merges over the live block set. `blocks` should be the catalog's
/// live rows ([`scry_catalog::Catalog::list_blocks`]); they are grouped
/// by `(signal, schema_version, date, level)` and any partition with `>= fanout`
/// blocks below `max_level` yields a merge of its `fanout` smallest blocks.
///
/// A partition whose merge would produce an un-encodable ancestor closure is
/// reported in [`CompactionPlan::oversized`] instead of being planned. It used
/// to be planned anyway and then fail inside `merge_blocks` — which returned
/// `Err` for the *whole pass*, so one stuck partition stopped every other
/// partition from compacting, forever.
pub fn plan_merges(blocks: &[CatalogEntry], cfg: &CompactConfig) -> CompactionPlan {
    // Deterministic grouping order (BTreeMap) so a pass is reproducible
    // and tests/logs are stable.
    let mut groups: BTreeMap<(String, u32, String, u32), Vec<CatalogEntry>> = BTreeMap::new();
    for b in blocks {
        if let Some(filter) = &cfg.signal_filter {
            if &b.meta.signal != filter {
                continue;
            }
        }
        if b.level >= cfg.max_level {
            continue;
        }
        groups
            .entry((
                b.meta.signal.clone(),
                b.meta.schema_version,
                b.date.clone(),
                b.level,
            ))
            .or_default()
            .push(b.clone());
    }

    let mut plan = CompactionPlan::default();
    for ((signal, _schema_version, date, level), mut entries) in groups {
        if entries.len() < cfg.fanout {
            continue;
        }
        // Pick the `fanout` smallest by on-disk size — merging the
        // smallest first is what keeps the size tiers tight and write
        // amplification bounded.
        entries.sort_by_key(|e| e.meta.byte_size);
        entries.truncate(cfg.fanout);

        // Decline rather than plan a merge the block layer will refuse.
        let projected = projected_ancestry_len(&entries);
        if projected > scry_block::MAX_COMPACTED_ANCESTORS {
            plan.oversized.push(OversizedPartition {
                signal,
                date,
                input_level: level,
                projected_ancestors: projected,
            });
            continue;
        }

        plan.merges.push(PlannedMerge {
            signal,
            date,
            input_level: level,
            inputs: entries,
        });
    }
    plan
}

#[cfg(test)]
mod tests {
    use super::*;
    use scry_block::BlockMeta;
    use uuid::Uuid;

    fn entry(signal: &str, level: u32, bytes: u64, ts: u64) -> CatalogEntry {
        CatalogEntry {
            meta: BlockMeta {
                uuid: Uuid::now_v7(),
                signal: signal.to_string(),
                writer_id: Uuid::now_v7(),
                ts_min_unix_nano: ts,
                ts_max_unix_nano: ts + 1,
                row_count: 1,
                byte_size: bytes,
                schema_version: 1,
                level,
                compacted_from: Vec::new(),
                producer_version: String::new(),
                label_fingerprint_bloom: None,
                has_postings: false,
                postings_size_bytes: None,
                series_types: None,
                all_fingerprints: None,
                has_body_bloom: false,
                body_bloom_size_bytes: None,
                wal_seg_max: None,
                wal_shard: None,
            },
            bucket: "b".into(),
            date: "2026-05-30".into(),
            level,
        }
    }

    #[test]
    fn below_fanout_does_nothing() {
        let blocks = vec![entry("logs", 0, 10, 1), entry("logs", 0, 20, 2)];
        let cfg = CompactConfig {
            fanout: 3,
            ..Default::default()
        };
        assert!(plan_merges(&blocks, &cfg).merges.is_empty());
    }

    #[test]
    fn picks_fanout_smallest_into_next_level() {
        let blocks = vec![
            entry("logs", 0, 100, 1),
            entry("logs", 0, 10, 2),
            entry("logs", 0, 50, 3),
            entry("logs", 0, 20, 4),
        ];
        let cfg = CompactConfig {
            fanout: 2,
            ..Default::default()
        };
        let plans = plan_merges(&blocks, &cfg).merges;
        assert_eq!(plans.len(), 1);
        let p = &plans[0];
        assert_eq!(p.input_level, 0);
        assert_eq!(p.output_level(), 1);
        let sizes: Vec<u64> = p.inputs.iter().map(|e| e.meta.byte_size).collect();
        assert_eq!(sizes, vec![10, 20], "two smallest selected");
    }

    #[test]
    fn mixed_schema_versions_are_never_planned_together() {
        let mut blocks = vec![
            entry("metrics", 0, 10, 1),
            entry("metrics", 0, 20, 2),
            entry("metrics", 0, 30, 3),
            entry("metrics", 0, 40, 4),
        ];
        blocks[2].meta.schema_version = 2;
        blocks[3].meta.schema_version = 2;
        let cfg = CompactConfig {
            fanout: 2,
            ..Default::default()
        };

        let plans = plan_merges(&blocks, &cfg).merges;
        assert_eq!(plans.len(), 2);
        for plan in plans {
            let version = plan.inputs[0].meta.schema_version;
            assert!(plan
                .inputs
                .iter()
                .all(|entry| entry.meta.schema_version == version));
        }
    }

    #[test]
    fn mixed_versions_do_not_collectively_reach_fanout() {
        let mut blocks = vec![entry("metrics", 0, 10, 1), entry("metrics", 0, 20, 2)];
        blocks[1].meta.schema_version = 2;
        let cfg = CompactConfig {
            fanout: 2,
            ..Default::default()
        };
        assert!(plan_merges(&blocks, &cfg).merges.is_empty());
    }

    #[test]
    fn respects_max_level_and_signal_filter() {
        let mut blocks = vec![
            entry("logs", 0, 10, 1),
            entry("logs", 0, 10, 2),
            entry("metrics", 0, 10, 1),
            entry("metrics", 0, 10, 2),
        ];
        // Two level-3 logs blocks must be ignored at max_level=3.
        blocks.push(entry("logs", 3, 10, 3));
        blocks.push(entry("logs", 3, 10, 4));
        let cfg = CompactConfig {
            fanout: 2,
            max_level: 3,
            signal_filter: Some("logs".into()),
            ..Default::default()
        };
        let plans = plan_merges(&blocks, &cfg).merges;
        assert_eq!(plans.len(), 1, "only the level-0 logs partition qualifies");
        assert_eq!(plans[0].signal, "logs");
        assert_eq!(plans[0].input_level, 0);
    }

    /// An entry carrying `n` synthetic ancestors, as a block merged under some
    /// earlier `--compact-fanout` would.
    fn entry_with_ancestors(signal: &str, level: u32, bytes: u64, n: usize) -> CatalogEntry {
        let mut e = entry(signal, level, bytes, 1);
        let mut ancestors: Vec<Uuid> = (0..n).map(|_| Uuid::now_v7()).collect();
        ancestors.sort_unstable();
        e.meta.compacted_from = ancestors;
        e
    }

    #[test]
    fn projected_ancestry_counts_inputs_plus_their_closures() {
        let a = entry_with_ancestors("logs", 1, 10, 3);
        let b = entry_with_ancestors("logs", 1, 10, 4);
        // 2 inputs + 3 + 4 distinct ancestors.
        assert_eq!(projected_ancestry_len(&[a, b]), 9);
    }

    #[test]
    fn projected_ancestry_deduplicates_shared_ancestors() {
        let a = entry_with_ancestors("logs", 1, 10, 3);
        let mut b = entry_with_ancestors("logs", 1, 10, 0);
        // b already contains a's ancestry (as happens when lineage overlaps).
        b.meta.compacted_from = a.meta.compacted_from.clone();
        assert_eq!(projected_ancestry_len(&[a, b]), 5, "2 inputs + 3 shared");
    }

    #[test]
    fn a_partition_whose_ancestry_would_overflow_is_declined_not_planned() {
        // Each input already carries a closure near the cap, so merging two of
        // them cannot encode the result. This is the fanout-changed-between-runs
        // shape: the blocks exist, they qualify, the output is un-encodable.
        let big = scry_block::MAX_COMPACTED_ANCESTORS / 2;
        let blocks = vec![
            entry_with_ancestors("logs", 1, 10, big),
            entry_with_ancestors("logs", 1, 20, big),
        ];
        let cfg = CompactConfig {
            fanout: 2,
            ..Default::default()
        };
        let plan = plan_merges(&blocks, &cfg);
        assert!(plan.merges.is_empty(), "must not plan an impossible merge");
        assert_eq!(plan.oversized.len(), 1);
        let o = &plan.oversized[0];
        assert_eq!(o.signal, "logs");
        assert_eq!(o.input_level, 1);
        assert_eq!(o.projected_ancestors, 2 * big + 2);
        assert!(o.projected_ancestors > scry_block::MAX_COMPACTED_ANCESTORS);
    }

    #[test]
    fn one_stuck_partition_does_not_block_the_others() {
        // The actual regression: an un-encodable partition used to fail the
        // whole pass, so healthy partitions stopped compacting too.
        let big = scry_block::MAX_COMPACTED_ANCESTORS / 2;
        let blocks = vec![
            entry_with_ancestors("logs", 1, 10, big),
            entry_with_ancestors("logs", 1, 20, big),
            entry("metrics", 0, 10, 1),
            entry("metrics", 0, 20, 2),
        ];
        let cfg = CompactConfig {
            fanout: 2,
            ..Default::default()
        };
        let plan = plan_merges(&blocks, &cfg);
        assert_eq!(plan.oversized.len(), 1, "logs L1 declined");
        assert_eq!(plan.merges.len(), 1, "metrics L0 still planned");
        assert_eq!(plan.merges[0].signal, "metrics");
    }

    #[test]
    fn a_partition_exactly_at_the_cap_is_still_planned() {
        // Boundary: the block layer's check is `> MAX`, so `== MAX` must merge.
        let each = (scry_block::MAX_COMPACTED_ANCESTORS - 2) / 2;
        let blocks = vec![
            entry_with_ancestors("logs", 1, 10, each),
            entry_with_ancestors("logs", 1, 20, each),
        ];
        let cfg = CompactConfig {
            fanout: 2,
            ..Default::default()
        };
        let plan = plan_merges(&blocks, &cfg);
        assert_eq!(projected_ancestry_len(&blocks), 2 * each + 2);
        assert!(2 * each + 2 <= scry_block::MAX_COMPACTED_ANCESTORS);
        assert_eq!(plan.merges.len(), 1);
        assert!(plan.oversized.is_empty());
    }

    #[test]
    fn catalog_validation_sees_what_uniform_tree_validation_misses() {
        let big = scry_block::MAX_COMPACTED_ANCESTORS / 2;
        let blocks = vec![
            entry_with_ancestors("logs", 1, 10, big),
            entry_with_ancestors("logs", 1, 20, big),
        ];
        let cfg = CompactConfig {
            fanout: 2,
            ..Default::default()
        };
        // The config itself is fine — a uniform fanout-2 tree is tiny.
        cfg.validate().unwrap();
        // But the blocks that actually exist are not mergeable under it.
        let stuck = validate_against_catalog(&blocks, &cfg);
        assert_eq!(stuck.len(), 1);
        assert_eq!(stuck[0].signal, "logs");
    }

    #[test]
    fn validates_ancestry_capacity_and_basic_bounds() {
        CompactConfig::default().validate().unwrap();

        let too_shallow = CompactConfig {
            max_level: 0,
            ..Default::default()
        };
        assert!(too_shallow.validate().is_err());

        let no_reduction = CompactConfig {
            fanout: 1,
            ..Default::default()
        };
        assert!(no_reduction.validate().is_err());

        let oversized = CompactConfig {
            fanout: 9,
            max_level: 3,
            ..Default::default()
        };
        let error = oversized.validate().unwrap_err().to_string();
        assert!(error.contains("exceeding the sidecar limit"), "{error}");
    }
}
