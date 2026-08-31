//! Catalog size, and — the point of the module — which way it is moving.
//!
//! Compaction and retention remove blocks; ingest and compaction's own merge
//! outputs create them. Whether the catalog is growing or shrinking is the
//! difference between two rates, and no counter in the process answered it: the
//! block count was a *level* with no slope, so the only way to get a trend was
//! to read the number twice, by hand, hours apart.
//!
//! This samples the catalog on a timer, keeps a bounded ring of readings, and
//! reports the endpoint-to-endpoint slope over the retained window alongside
//! the window's own length so the figure can be judged.
//!
//! Two deliberate properties:
//!
//! 1. **It never touches the shared catalog mutex.** The sampler opens its own
//!    read-only connection ([`Catalog::open_read_only`]) from the catalog path.
//!    A full scan of a large `blocks` table is slow enough that running it
//!    under the mutex ingest writes and queries contend for would stall real
//!    work for the least urgent reader in the process. It is also what lets the
//!    status path stop scanning entirely: the status snapshot reads a cached
//!    struct, no SQL.
//! 2. **A slope it cannot justify is `None`, not zero.** Fewer than two
//!    readings, or a window too short to mean anything, reports absent. Zero
//!    means "measured, and steady" — a distinction an operator staring at a
//!    backlog needs, since "not moving" and "not measured" call for opposite
//!    reactions.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use scry_catalog::{Catalog, LevelStats};
use tracing::{debug, warn};

/// How often to scan the catalog.
///
/// A full aggregate over a large `blocks` table is not free, and nothing
/// downstream reacts faster than a human refreshing a status page. One minute
/// keeps the cost negligible while still filling the ring's hour-long window
/// with enough points to see a trend.
pub const CATALOG_GAUGE_INTERVAL: Duration = Duration::from_secs(60);

/// Readings retained for the slope. At [`CATALOG_GAUGE_INTERVAL`] this is a
/// one-hour window — long enough that a single compaction pass landing does not
/// dominate the rate, short enough to still show a change of direction.
const RING_CAPACITY: usize = 60;

/// The shortest window a rate may be computed over. Two samples a few seconds
/// apart extrapolate to an enormous and entirely fictional per-hour figure;
/// below this the gauge reports no slope rather than a confident wrong one.
const MIN_SLOPE_WINDOW: Duration = Duration::from_secs(120);

#[derive(Clone, Copy, Debug)]
struct Sample {
    at_unix_ms: u64,
    blocks: u64,
}

/// One complete scan of the catalog.
#[derive(Clone, Debug)]
struct Reading {
    blocks: u64,
    rows: u64,
    lineage_rows: u64,
    by_level: Vec<LevelStats>,
    at_unix_ms: u64,
}

#[derive(Default)]
struct GaugeState {
    /// `None` until the first scan succeeds.
    current: Option<Reading>,
    ring: VecDeque<Sample>,
    /// Scans that failed since the last success. Surfaced because a gauge that
    /// has quietly stopped updating looks exactly like one reporting a catalog
    /// that has stopped changing.
    consecutive_failures: u64,
}

/// Periodically-sampled catalog size and trend. Share via `Arc`; construct with
/// [`CatalogGauge::new`] and drive with [`CatalogGauge::spawn`].
pub struct CatalogGauge {
    path: PathBuf,
    state: Mutex<GaugeState>,
}

impl CatalogGauge {
    pub fn new(path: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            path,
            state: Mutex::new(GaugeState::default()),
        })
    }

    /// Scan the catalog once and fold the result into the ring.
    ///
    /// Blocking (SQLite); call from `spawn_blocking` or a blocking context. A
    /// failure is counted and logged, never propagated — a status gauge must
    /// not be able to take down the daemon carrying it.
    fn sample_blocking(&self) {
        let read = Catalog::open_read_only(&self.path)
            .and_then(|cat| Ok((cat.live_block_stats()?, cat.lineage_row_count()?)));
        let (stats, lineage_rows) = match read {
            Ok(v) => v,
            Err(error) => {
                let mut state = self.lock();
                state.consecutive_failures += 1;
                let failures = state.consecutive_failures;
                drop(state);
                // Noisy on the first failure, quiet afterwards: a catalog that
                // is briefly unreadable during a restore is expected, a gauge
                // stuck for an hour is not, and both would otherwise log at the
                // same rate forever.
                if failures == 1 {
                    warn!(error = %error, "catalog gauge sample failed");
                } else {
                    debug!(error = %error, failures, "catalog gauge sample still failing");
                }
                return;
            }
        };

        let at_unix_ms = unix_ms_now();
        let mut state = self.lock();
        state.consecutive_failures = 0;
        state.current = Some(Reading {
            blocks: stats.blocks,
            rows: stats.rows,
            lineage_rows,
            by_level: stats.by_level,
            at_unix_ms,
        });
        state.ring.push_back(Sample {
            at_unix_ms,
            blocks: stats.blocks,
        });
        while state.ring.len() > RING_CAPACITY {
            state.ring.pop_front();
        }
    }

    /// Drive the gauge until the process ends.
    ///
    /// Samples immediately, then every `interval`. The immediate first scan
    /// matters: without it a freshly-started daemon reports no catalog size at
    /// all for a full interval, which reads as "broken" exactly when someone is
    /// checking whether a restart worked.
    ///
    /// Sleeps *after* each scan rather than on a fixed-rate ticker, so a scan
    /// that overruns the interval idles instead of immediately re-running —
    /// the same scheduling correction D-066 made to the full walk.
    pub fn spawn(self: Arc<Self>, interval: Duration) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let gauge = self.clone();
                if tokio::task::spawn_blocking(move || gauge.sample_blocking())
                    .await
                    .is_err()
                {
                    warn!("catalog gauge sampling task panicked; gauge will retry");
                }
                tokio::time::sleep(interval).await;
            }
        })
    }

    /// Net change in live blocks per hour over the retained window, or `None`
    /// when the window is too short or too sparse to support a figure.
    fn blocks_per_hour(state: &GaugeState) -> Option<f64> {
        let oldest = state.ring.front()?;
        let newest = state.ring.back()?;
        let span_ms = newest.at_unix_ms.checked_sub(oldest.at_unix_ms)?;
        if span_ms < MIN_SLOPE_WINDOW.as_millis() as u64 {
            return None;
        }
        // Signed: the whole question is which direction this is going, so the
        // subtraction happens in f64 rather than saturating at zero in u64.
        let delta = newest.blocks as f64 - oldest.blocks as f64;
        Some(delta * 3_600_000.0 / span_ms as f64)
    }

    /// The gauge as it appears inside a status snapshot's `data`.
    ///
    /// Reads the cached struct only — no SQL, no catalog mutex — which is what
    /// makes it safe to call on every status heartbeat.
    pub fn snapshot_json(&self) -> serde_json::Value {
        let state = self.lock();
        let Some(current) = state.current.as_ref() else {
            return serde_json::json!({
                "sampled": false,
                "sample_failures": state.consecutive_failures,
            });
        };
        let window_secs = match (state.ring.front(), state.ring.back()) {
            (Some(oldest), Some(newest)) => {
                newest.at_unix_ms.saturating_sub(oldest.at_unix_ms) as f64 / 1000.0
            }
            _ => 0.0,
        };
        serde_json::json!({
            "sampled": true,
            "blocks": current.blocks,
            "rows": current.rows,
            "lineage_rows": current.lineage_rows,
            "sampled_at_unix_ms": current.at_unix_ms,
            // How old this reading is, by the sampling instance's *own* clock.
            // Computed here rather than left to the reader to derive: a viewer
            // subtracting its own `Date.now()` from our timestamp would be
            // comparing across two machines' clocks. Without this the number
            // above is unfalsifiable — a count taken 59 seconds ago looks
            // exactly like a live one, which on a freshly-started daemon means
            // a real catalog reads as empty.
            "sampled_age_secs": unix_ms_now().saturating_sub(current.at_unix_ms) as f64 / 1000.0,
            "by_level": current
                .by_level
                .iter()
                .map(|l| serde_json::json!({
                    "level": l.level,
                    "blocks": l.blocks,
                    "rows": l.rows,
                }))
                .collect::<Vec<_>>(),
            // Absent rather than 0.0 when unjustifiable — see the module docs.
            "blocks_per_hour": Self::blocks_per_hour(&state),
            "trend_window_secs": window_secs,
            "trend_samples": state.ring.len(),
            "sample_failures": state.consecutive_failures,
        })
    }

    /// Live block count as of the last successful scan, for callers that want
    /// the bare number rather than the whole envelope.
    pub fn blocks(&self) -> Option<u64> {
        self.lock().current.as_ref().map(|c| c.blocks)
    }

    /// A poisoned gauge mutex must not propagate: the lock is held only for
    /// short infallible struct updates, so a poisoning means some *other*
    /// thread panicked, and losing the status page on top of that helps nobody.
    fn lock(&self) -> std::sync::MutexGuard<'_, GaugeState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gauge() -> Arc<CatalogGauge> {
        CatalogGauge::new(PathBuf::from("/nonexistent/catalog.sqlite"))
    }

    fn push(g: &CatalogGauge, at_unix_ms: u64, blocks: u64) {
        let mut state = g.lock();
        state.current = Some(Reading {
            blocks,
            rows: blocks * 10,
            lineage_rows: 0,
            by_level: Vec::new(),
            at_unix_ms,
        });
        state.ring.push_back(Sample { at_unix_ms, blocks });
        while state.ring.len() > RING_CAPACITY {
            state.ring.pop_front();
        }
    }

    #[test]
    fn an_unsampled_gauge_reports_absent_not_zero() {
        let g = gauge();
        let json = g.snapshot_json();
        assert_eq!(json["sampled"], serde_json::json!(false));
        assert!(json.get("blocks").is_none(), "no invented block count");
        assert_eq!(g.blocks(), None);
    }

    #[test]
    fn one_sample_yields_no_slope() {
        let g = gauge();
        push(&g, 1_000_000, 500);
        let json = g.snapshot_json();
        assert_eq!(json["sampled"], serde_json::json!(true));
        assert_eq!(json["blocks"], serde_json::json!(500));
        assert!(
            json["blocks_per_hour"].is_null(),
            "a single reading cannot imply a rate"
        );
    }

    #[test]
    fn a_window_shorter_than_the_floor_yields_no_slope() {
        let g = gauge();
        push(&g, 0, 1000);
        // 60s apart: a real delta, but extrapolating it to an hour would be a
        // guess dressed up as a measurement.
        push(&g, 60_000, 900);
        assert!(g.snapshot_json()["blocks_per_hour"].is_null());
    }

    #[test]
    fn a_shrinking_catalog_reports_a_negative_rate() {
        let g = gauge();
        push(&g, 0, 10_000);
        push(&g, 3_600_000, 9_000);
        let rate = g.snapshot_json()["blocks_per_hour"].as_f64().unwrap();
        assert!(
            (rate - -1000.0).abs() < 1e-6,
            "1000 blocks lost over exactly one hour, got {rate}"
        );
    }

    #[test]
    fn a_growing_catalog_reports_a_positive_rate() {
        let g = gauge();
        push(&g, 0, 1_000);
        push(&g, 1_800_000, 1_500);
        let rate = g.snapshot_json()["blocks_per_hour"].as_f64().unwrap();
        assert!(
            (rate - 1000.0).abs() < 1e-6,
            "500 blocks gained over half an hour, got {rate}"
        );
    }

    #[test]
    fn a_steady_catalog_reports_zero_which_is_not_absent() {
        let g = gauge();
        push(&g, 0, 4_242);
        push(&g, 3_600_000, 4_242);
        let json = g.snapshot_json();
        assert_eq!(
            json["blocks_per_hour"].as_f64(),
            Some(0.0),
            "measured-and-steady must be distinguishable from not-measured"
        );
        assert!(!json["blocks_per_hour"].is_null());
    }

    #[test]
    fn the_ring_stays_bounded_and_the_window_slides() {
        let g = gauge();
        for i in 0..(RING_CAPACITY as u64 * 3) {
            push(&g, i * 60_000, 1_000 + i);
        }
        let json = g.snapshot_json();
        assert_eq!(
            json["trend_samples"].as_u64().unwrap(),
            RING_CAPACITY as u64,
            "old readings are evicted rather than accumulating forever"
        );
        // The slope now describes only the retained window, not all of history.
        let window = json["trend_window_secs"].as_f64().unwrap();
        assert!(
            (window - (RING_CAPACITY as f64 - 1.0) * 60.0).abs() < 1e-6,
            "window covers the retained samples only, got {window}"
        );
        let rate = json["blocks_per_hour"].as_f64().unwrap();
        assert!(
            (rate - 60.0).abs() < 1e-6,
            "one block per minute, got {rate}"
        );
    }

    /// A sampled reading must carry its own age, so a stale count cannot pass
    /// for a live one. Without this the gauge is unfalsifiable: a freshly
    /// booted daemon reports the catalog it saw before the first block landed,
    /// and an empty-looking catalog is indistinguishable from an empty one.
    #[test]
    fn a_reading_reports_how_old_it_is() {
        let g = gauge();
        let now = unix_ms_now();
        push(&g, now - 45_000, 1_234);
        let age = g.snapshot_json()["sampled_age_secs"].as_f64().unwrap();
        assert!(
            (40.0..60.0).contains(&age),
            "a 45s-old reading should report ~45s, got {age}"
        );
    }

    #[test]
    fn a_failed_scan_is_counted_and_leaves_the_last_good_reading_intact() {
        let g = gauge();
        push(&g, 0, 777);
        // The path does not exist, so this scan cannot succeed.
        g.sample_blocking();
        let json = g.snapshot_json();
        assert_eq!(json["sample_failures"], serde_json::json!(1));
        assert_eq!(
            json["blocks"],
            serde_json::json!(777),
            "a failed scan must not erase what was last known"
        );
    }
}
