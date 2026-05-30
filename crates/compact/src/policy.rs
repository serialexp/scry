//! Compaction policy — which blocks to merge, into what.
//!
//! Size-tiered (`ARCHITECTURE.md § Compaction § Compaction policy`):
//! blocks live at a `level`, and when a `(signal, date, level)`
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

use std::collections::BTreeMap;
use std::time::Duration;

use scry_catalog::CatalogEntry;

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
}

impl Default for CompactConfig {
    fn default() -> Self {
        Self {
            fanout: 8,
            max_level: 3,
            grace: Duration::ZERO,
            signal_filter: None,
        }
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

/// Plan merges over the live block set. `blocks` should be the catalog's
/// live rows ([`scry_catalog::Catalog::list_blocks`]); they are grouped
/// by `(signal, date, level)` and any partition with `>= fanout` blocks
/// below `max_level` yields a merge of its `fanout` smallest blocks.
pub fn plan_merges(blocks: &[CatalogEntry], cfg: &CompactConfig) -> Vec<PlannedMerge> {
    // Deterministic grouping order (BTreeMap) so a pass is reproducible
    // and tests/logs are stable.
    let mut groups: BTreeMap<(String, String, u32), Vec<CatalogEntry>> = BTreeMap::new();
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
            .entry((b.meta.signal.clone(), b.date.clone(), b.level))
            .or_default()
            .push(b.clone());
    }

    let mut plans = Vec::new();
    for ((signal, date, level), mut entries) in groups {
        if entries.len() < cfg.fanout {
            continue;
        }
        // Pick the `fanout` smallest by on-disk size — merging the
        // smallest first is what keeps the size tiers tight and write
        // amplification bounded.
        entries.sort_by_key(|e| e.meta.byte_size);
        entries.truncate(cfg.fanout);
        plans.push(PlannedMerge {
            signal,
            date,
            input_level: level,
            inputs: entries,
        });
    }
    plans
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
                producer_version: String::new(),
                label_fingerprint_bloom: None,
                has_postings: false,
                postings_size_bytes: None,
                series_types: None,
                all_fingerprints: None,
                has_body_bloom: false,
                body_bloom_size_bytes: None,
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
        assert!(plan_merges(&blocks, &cfg).is_empty());
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
        let plans = plan_merges(&blocks, &cfg);
        assert_eq!(plans.len(), 1);
        let p = &plans[0];
        assert_eq!(p.input_level, 0);
        assert_eq!(p.output_level(), 1);
        let sizes: Vec<u64> = p.inputs.iter().map(|e| e.meta.byte_size).collect();
        assert_eq!(sizes, vec![10, 20], "two smallest selected");
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
        let plans = plan_merges(&blocks, &cfg);
        assert_eq!(plans.len(), 1, "only the level-0 logs partition qualifies");
        assert_eq!(plans[0].signal, "logs");
        assert_eq!(plans[0].input_level, 0);
    }
}
