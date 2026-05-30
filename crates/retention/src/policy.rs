//! Retention policy — which blocks are expired, given per-signal TTLs.
//!
//! Age-based (vs compaction's count-based policy): a block is reaped when
//! its newest record is older than the TTL configured for its signal. The
//! `now` instant is passed in, not read internally, so the policy is a
//! pure function and unit-testable without clock games.

use std::collections::BTreeMap;
use std::time::Duration;

use scry_catalog::CatalogEntry;

/// Tunables for a retention pass.
///
/// TTL lookup is **opt-in**: [`ttl_for`](Self::ttl_for) returns a per-signal
/// override if present, else the global `default_ttl`, else `None` — and a
/// `None` signal is never reaped. So with no `default_ttl` and no overrides,
/// nothing is eligible; a signal is only touched once you explicitly give it
/// (or all signals) a TTL.
#[derive(Debug, Clone, Default)]
pub struct RetentionConfig {
    /// TTL applied to every signal that has no explicit override. `None`
    /// means "no blanket default" — only overridden signals are eligible.
    pub default_ttl: Option<Duration>,
    /// Per-signal TTL overrides, keyed by signal name (`"logs"`, …).
    pub overrides: BTreeMap<String, Duration>,
    /// Delay between soft-deleting expired blocks (so queries stop listing
    /// them) and removing their objects. 0 is safe single-instance; a
    /// non-zero grace guards a concurrent reader mid-scan.
    pub grace: Duration,
    /// `false` (default) = dry-run: report candidates, touch nothing.
    /// `true` = actually delete.
    pub apply: bool,
}

impl RetentionConfig {
    /// The TTL governing `signal`, or `None` if the signal is not eligible
    /// for retention (no override and no blanket default).
    pub fn ttl_for(&self, signal: &str) -> Option<Duration> {
        self.overrides.get(signal).copied().or(self.default_ttl)
    }

    /// Whether *any* TTL is configured. The CLI rejects a run where this is
    /// false — a pass that can't reap anything is surely a mistake.
    pub fn any_ttl_configured(&self) -> bool {
        self.default_ttl.is_some() || !self.overrides.is_empty()
    }
}

/// Select the blocks whose data is entirely past their signal's TTL.
///
/// `blocks` should be the catalog's live rows
/// ([`scry_catalog::Catalog::list_blocks`]). A block is selected iff its
/// signal has a configured TTL and its newest record
/// (`ts_max_unix_nano`) is strictly older than `now_unix_nano - ttl`. The
/// result is sorted deterministically (signal, date, uuid) so a pass is
/// reproducible and logs/tests are stable.
pub fn plan_reaping(
    blocks: &[CatalogEntry],
    cfg: &RetentionConfig,
    now_unix_nano: u64,
) -> Vec<CatalogEntry> {
    let mut reap: Vec<CatalogEntry> = blocks
        .iter()
        .filter(|b| match cfg.ttl_for(&b.meta.signal) {
            Some(ttl) => {
                // Saturating throughout: a TTL larger than `now` yields
                // cutoff 0, so nothing is reaped (ts_max is always ≥ 0).
                let ttl_nanos = u64::try_from(ttl.as_nanos()).unwrap_or(u64::MAX);
                let cutoff = now_unix_nano.saturating_sub(ttl_nanos);
                b.meta.ts_max_unix_nano < cutoff
            }
            None => false,
        })
        .cloned()
        .collect();

    reap.sort_by(|a, b| {
        a.meta
            .signal
            .cmp(&b.meta.signal)
            .then_with(|| a.date.cmp(&b.date))
            .then_with(|| a.meta.uuid.cmp(&b.meta.uuid))
    });
    reap
}

#[cfg(test)]
mod tests {
    use super::*;
    use scry_block::BlockMeta;
    use uuid::Uuid;

    const DAY_NANOS: u64 = 24 * 3600 * 1_000_000_000;

    fn entry(signal: &str, ts_max: u64) -> CatalogEntry {
        CatalogEntry {
            meta: BlockMeta {
                uuid: Uuid::now_v7(),
                signal: signal.to_string(),
                writer_id: Uuid::now_v7(),
                ts_min_unix_nano: ts_max.saturating_sub(1),
                ts_max_unix_nano: ts_max,
                row_count: 1,
                byte_size: 100,
                schema_version: 1,
                level: 0,
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
            level: 0,
        }
    }

    fn cfg(default_days: Option<u64>, overrides: &[(&str, u64)]) -> RetentionConfig {
        RetentionConfig {
            default_ttl: default_days.map(|d| Duration::from_nanos(d * DAY_NANOS)),
            overrides: overrides
                .iter()
                .map(|(s, d)| (s.to_string(), Duration::from_nanos(d * DAY_NANOS)))
                .collect(),
            grace: Duration::ZERO,
            apply: false,
        }
    }

    #[test]
    fn no_ttl_configured_reaps_nothing() {
        let now = 100 * DAY_NANOS;
        let blocks = vec![entry("logs", 1), entry("metrics", 1)];
        let c = cfg(None, &[]);
        assert!(!c.any_ttl_configured());
        assert!(plan_reaping(&blocks, &c, now).is_empty());
    }

    #[test]
    fn fully_aged_reaped_recent_kept() {
        let now = 100 * DAY_NANOS;
        // old: ts_max 90 days ago (> 7d) → reaped; recent: 1 day ago → kept.
        let old = entry("logs", now - 90 * DAY_NANOS);
        let recent = entry("logs", now - DAY_NANOS);
        let blocks = vec![old.clone(), recent.clone()];
        let reaped = plan_reaping(&blocks, &cfg(None, &[("logs", 7)]), now);
        assert_eq!(reaped.len(), 1);
        assert_eq!(reaped[0].meta.uuid, old.meta.uuid);
    }

    #[test]
    fn partially_aged_block_kept() {
        // ts_max is exactly at the cutoff boundary: `<` is strict, so a
        // block whose newest record is exactly `ttl` old is NOT reaped.
        let now = 100 * DAY_NANOS;
        let boundary = entry("logs", now - 7 * DAY_NANOS); // ts_max == cutoff
        let reaped = plan_reaping(&[boundary], &cfg(None, &[("logs", 7)]), now);
        assert!(reaped.is_empty(), "block exactly at the cutoff is kept");
    }

    #[test]
    fn signal_without_ttl_never_reaped_even_when_old() {
        let now = 100 * DAY_NANOS;
        // metrics is ancient but has no TTL; only logs is configured.
        let metrics = entry("metrics", 1);
        let logs = entry("logs", now - 90 * DAY_NANOS);
        let reaped = plan_reaping(&[metrics, logs.clone()], &cfg(None, &[("logs", 7)]), now);
        assert_eq!(reaped.len(), 1);
        assert_eq!(reaped[0].meta.signal, "logs");
    }

    #[test]
    fn override_beats_global_default() {
        let now = 100 * DAY_NANOS;
        // global 30d, logs override 1d. A logs block 5 days old is past the
        // 1d override (reaped) but a metrics block 5 days old is within 30d
        // (kept).
        let logs = entry("logs", now - 5 * DAY_NANOS);
        let metrics = entry("metrics", now - 5 * DAY_NANOS);
        let reaped = plan_reaping(
            &[logs.clone(), metrics],
            &cfg(Some(30), &[("logs", 1)]),
            now,
        );
        assert_eq!(reaped.len(), 1);
        assert_eq!(reaped[0].meta.signal, "logs");
    }

    #[test]
    fn huge_ttl_reaps_nothing() {
        let now = 100 * DAY_NANOS;
        let blocks = vec![entry("logs", 1)];
        // 100_000 days ≫ now → saturating cutoff 0 → nothing reaped.
        let reaped = plan_reaping(&blocks, &cfg(None, &[("logs", 100_000)]), now);
        assert!(reaped.is_empty());
    }
}
