//! In-memory cache for parsed per-block body blooms (the v0.7 full-text
//! skip index).
//!
//! Structurally a sibling of [`crate::postings_cache`]: a byte-budgeted,
//! LRU-evicted, single-flight cache keyed by block UUID. The motivation is
//! the same — blocks are immutable, so once a query has fetched and parsed a
//! block's `body.bloom` sidecar there is no reason to ever fetch it again.
//! The daemon, which serves many queries against a warm block set, is the
//! prime beneficiary; the one-shot CLI passes `None` and takes the direct
//! fetch path in [`crate::body_bloom`].
//!
//! ## Why a separate cache from postings?
//!
//! The two sidecars are unrelated objects with different value types
//! ([`PostingsIndex`](crate::postings_cache::PostingsIndex) vs.
//! [`BodyBloom`]), different access patterns (postings resolve to a
//! fingerprint set; the bloom resolves to a single skip decision), and
//! wildly different sizes (blooms are ~2% of body size — tens to hundreds of
//! KB, vs. postings' "few MB"). Sharing one LRU would let large postings
//! evict cheap blooms and muddy the byte accounting. They get independent
//! budgets instead. The locking / single-flight / eviction machinery is
//! deliberately a near-verbatim copy of `postings_cache` so the two stay
//! easy to read side by side.
//!
//! ## Correctness
//!
//! The cache only ever *accelerates* the body-bloom skip; it never changes
//! its result. A fetch failure, an unparseable sidecar, or a missing bloom
//! all resolve to "keep the block" (scan it) exactly as the direct path
//! does — the bloom's one-sided error means a wrong "keep" only costs a
//! wasted scan, never a lost match.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use hashlink::LinkedHashMap;
use object_store::ObjectStore;
use scry_block::{BlockMeta, BodyBloom};
use tokio::sync::OnceCell;
use tracing::warn;
use uuid::Uuid;

use crate::body_bloom::fetch_body_bloom;

/// Default byte budget for the bloom cache: 64 MiB. Blooms run ~2% of body
/// size (tens to hundreds of KB per block at the default 1% FPR), so this
/// holds hundreds of blocks comfortably — far more than postings' 256 MiB
/// needs to for its larger sidecars.
pub const DEFAULT_BUDGET_BYTES: usize = 64 * 1024 * 1024;

/// Bloom cache configuration. Read from env via
/// [`BloomCacheConfig::from_env`] or built directly.
#[derive(Debug, Clone, Copy)]
pub struct BloomCacheConfig {
    /// Byte budget; entries beyond this evict LRU-first. A single entry
    /// larger than the budget is accepted anyway (zero caching is worse
    /// than zero eviction) and a warning is logged.
    pub budget_bytes: usize,
}

impl Default for BloomCacheConfig {
    fn default() -> Self {
        Self {
            budget_bytes: DEFAULT_BUDGET_BYTES,
        }
    }
}

impl BloomCacheConfig {
    /// Build from `SCRY_BLOOM_CACHE_BYTES`. Missing/empty = default.
    pub fn from_env() -> Result<Self> {
        let mut cfg = Self::default();
        if let Ok(v) = std::env::var("SCRY_BLOOM_CACHE_BYTES") {
            let v = v.trim();
            if !v.is_empty() {
                cfg.budget_bytes = v
                    .parse()
                    .with_context(|| format!("SCRY_BLOOM_CACHE_BYTES={v}"))?;
            }
        }
        Ok(cfg)
    }
}

/// Point-in-time snapshot of cache counters. `delta` is per-query
/// reporting; the gauges (`bytes_in`, `entries`, `budget_bytes`) carry
/// through unchanged.
#[derive(Debug, Clone, Copy, Default)]
pub struct BloomCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    /// Fetches that failed and left the slot empty for a future retry.
    pub fetch_errors: u64,
    pub bytes_in: usize,
    pub entries: usize,
    pub budget_bytes: usize,
}

impl BloomCacheStats {
    pub fn delta(self, prior: Self) -> Self {
        Self {
            hits: self.hits.saturating_sub(prior.hits),
            misses: self.misses.saturating_sub(prior.misses),
            evictions: self.evictions.saturating_sub(prior.evictions),
            fetch_errors: self.fetch_errors.saturating_sub(prior.fetch_errors),
            bytes_in: self.bytes_in,
            entries: self.entries,
            budget_bytes: self.budget_bytes,
        }
    }
}

/// A cached bloom slot. `None` inside the resolved cell means "the block
/// has no usable bloom" (no sidecar, or it failed to parse) — a valid,
/// cacheable answer that means "never skip this block". `Some(bloom)` is a
/// parsed filter. Weighted by the bloom's byte length (a `None` answer is
/// near-free but still occupies a slot).
type SlotValue = Arc<Option<BodyBloom>>;

/// One slot in the LRU. The `cell` is shared with all concurrent resolvers
/// waiting on the same block; `weight` is set once the fetch completes
/// (0 = still loading, eviction-immune).
struct CacheSlot {
    cell: Arc<OnceCell<SlotValue>>,
    weight: Arc<std::sync::atomic::AtomicUsize>,
}

impl CacheSlot {
    fn new_loading() -> Self {
        Self {
            cell: Arc::new(OnceCell::new()),
            weight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }
}

struct LruState {
    map: LinkedHashMap<Uuid, CacheSlot>,
    bytes_in: usize,
}

/// Body-bloom cache: per-block, byte-budgeted, LRU-evicted, single-flight.
pub struct BloomCache {
    state: Mutex<LruState>,
    budget_bytes: usize,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    fetch_errors: AtomicU64,
}

/// Per-slot weight estimate. A loaded bloom weighs its byte length plus a
/// small fixed overhead for the slot/Arc bookkeeping; an absent bloom
/// (`None`) still costs the overhead so the budget can't be defeated by a
/// flood of blooms-less blocks.
const SLOT_OVERHEAD_BYTES: usize = 64;

fn slot_weight(v: &SlotValue) -> usize {
    SLOT_OVERHEAD_BYTES + v.as_ref().as_ref().map(|b| b.byte_len()).unwrap_or(0)
}

impl BloomCache {
    pub fn new(cfg: BloomCacheConfig) -> Self {
        Self {
            state: Mutex::new(LruState {
                map: LinkedHashMap::new(),
                bytes_in: 0,
            }),
            budget_bytes: cfg.budget_bytes,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            fetch_errors: AtomicU64::new(0),
        }
    }

    /// Convenience for the common shape.
    pub fn with_budget_bytes(budget_bytes: usize) -> Self {
        Self::new(BloomCacheConfig { budget_bytes })
    }

    /// Take a snapshot for telemetry.
    pub fn stats(&self) -> BloomCacheStats {
        let state = self.state.lock().expect("bloom cache mutex poisoned");
        BloomCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            fetch_errors: self.fetch_errors.load(Ordering::Relaxed),
            bytes_in: state.bytes_in,
            entries: state.map.len(),
            budget_bytes: self.budget_bytes,
        }
    }

    /// Decide whether a candidate block can be skipped for a
    /// `body_contains` query, using the cache. Same contract as
    /// [`crate::body_bloom::block_excluded_by_bloom`]: `true` means the
    /// block's bloom authoritatively rules the pattern out (skip it); any
    /// failure to obtain a usable bloom returns `false` (keep + scan).
    ///
    /// Blocks whose `meta.has_body_bloom` is false short-circuit to `false`
    /// without touching the cache — there is nothing to fetch.
    pub async fn block_excluded(
        &self,
        store: Arc<dyn ObjectStore>,
        meta: &BlockMeta,
        pattern: &str,
    ) -> bool {
        if !meta.has_body_bloom {
            return false;
        }
        match self.get_or_fetch(store, meta).await {
            Ok(slot) => match slot.as_ref() {
                Some(bloom) => !bloom.contains_pattern(pattern),
                None => {
                    // Cached "no usable bloom" answer — keep + scan.
                    false
                }
            },
            Err(e) => {
                warn!(block = %meta.uuid, error = %e, "body bloom fetch failed; scanning block");
                false
            }
        }
    }

    /// Core single-flight + LRU logic. Returns the cached slot value:
    /// `Some(bloom)` if the sidecar parsed, `None` if it was missing /
    /// unparseable (cached so we don't re-fetch a known-bad block).
    /// Only transport errors propagate as `Err`.
    pub async fn get_or_fetch(
        &self,
        store: Arc<dyn ObjectStore>,
        meta: &BlockMeta,
    ) -> Result<SlotValue> {
        let uuid = meta.uuid;

        // ── Phase 1: look up or insert the slot under the mutex ─────
        let (cell, weight, was_present) = {
            let mut state = self.state.lock().expect("bloom cache mutex poisoned");
            if state.map.contains_key(&uuid) {
                state.map.to_back(&uuid);
                let slot = state.map.get(&uuid).unwrap();
                (slot.cell.clone(), slot.weight.clone(), true)
            } else {
                let slot = CacheSlot::new_loading();
                let cell = slot.cell.clone();
                let weight = slot.weight.clone();
                state.map.insert(uuid, slot);
                (cell, weight, false)
            }
        };

        if was_present {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }

        // ── Phase 2: initialize (or await existing initialization) ──
        let store_for_fetch = store.clone();
        let meta_for_fetch = meta.clone();
        let init_result = cell
            .get_or_try_init(|| async move {
                fetch_body_bloom(store_for_fetch, &meta_for_fetch)
                    .await
                    .map(Arc::new)
            })
            .await;

        let value = match init_result {
            Ok(v) => v.clone(),
            Err(e) => {
                self.fetch_errors.fetch_add(1, Ordering::Relaxed);
                let mut state = self.state.lock().expect("bloom cache mutex poisoned");
                if weight.load(Ordering::Relaxed) == 0 {
                    state.map.remove(&uuid);
                }
                return Err(e);
            }
        };

        // ── Phase 3: account for the load + run eviction ────────────
        let new_weight = slot_weight(&value);
        let prev = weight.swap(new_weight, Ordering::AcqRel);
        if prev == 0 {
            if new_weight > self.budget_bytes {
                warn!(
                    block_uuid = %uuid,
                    weight_bytes = new_weight,
                    budget_bytes = self.budget_bytes,
                    "bloom cache entry exceeds total budget; accepted anyway"
                );
            }
            let mut state = self.state.lock().expect("bloom cache mutex poisoned");
            state.bytes_in = state.bytes_in.saturating_add(new_weight);
            self.evict_to_budget(&mut state, /* protect_uuid */ uuid);
        }
        Ok(value)
    }

    /// Pop LRU entries from the head until `bytes_in <= budget_bytes` or we
    /// run out of evictable entries. Loading slots (weight==0) and the
    /// just-inserted entry (`protect_uuid`) are skipped.
    fn evict_to_budget(&self, state: &mut LruState, protect_uuid: Uuid) {
        while state.bytes_in > self.budget_bytes && state.map.len() > 1 {
            let victim_uuid: Option<Uuid> = state.map.iter().find_map(|(uuid, slot)| {
                if *uuid == protect_uuid {
                    return None;
                }
                let w = slot.weight.load(Ordering::Relaxed);
                if w == 0 {
                    return None; // still loading; skip
                }
                Some(*uuid)
            });
            let Some(uuid) = victim_uuid else {
                break;
            };
            if let Some(slot) = state.map.remove(&uuid) {
                let w = slot.weight.load(Ordering::Relaxed);
                state.bytes_in = state.bytes_in.saturating_sub(w);
                self.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

impl std::fmt::Debug for BloomCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.stats();
        f.debug_struct("BloomCache")
            .field("entries", &s.entries)
            .field("bytes_in", &s.bytes_in)
            .field("budget_bytes", &s.budget_bytes)
            .field("hits", &s.hits)
            .field("misses", &s.misses)
            .field("evictions", &s.evictions)
            .field("fetch_errors", &s.fetch_errors)
            .finish()
    }
}

// ─── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    fn bloom_for(bodies: &[&str]) -> SlotValue {
        Arc::new(Some(BodyBloom::build_from_bodies(
            bodies.iter().copied(),
            3,
            0.01,
        )))
    }

    /// Force a slot to a pre-built value via the same phases a real
    /// `get_or_fetch` would, but synchronously and without I/O.
    async fn install(cache: &BloomCache, uuid: Uuid, value: SlotValue) {
        let (cell, weight, was_present) = {
            let mut state = cache.state.lock().unwrap();
            if state.map.contains_key(&uuid) {
                state.map.to_back(&uuid);
                let slot = state.map.get(&uuid).unwrap();
                (slot.cell.clone(), slot.weight.clone(), true)
            } else {
                let slot = CacheSlot::new_loading();
                let cell = slot.cell.clone();
                let weight = slot.weight.clone();
                state.map.insert(uuid, slot);
                (cell, weight, false)
            }
        };
        if was_present {
            cache.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            cache.misses.fetch_add(1, Ordering::Relaxed);
        }
        let _ = cell
            .get_or_try_init(|| async { Ok::<_, anyhow::Error>(value.clone()) })
            .await
            .unwrap();
        let new_weight = slot_weight(&value);
        let prev = weight.swap(new_weight, Ordering::AcqRel);
        if prev == 0 {
            let mut state = cache.state.lock().unwrap();
            state.bytes_in += new_weight;
            cache.evict_to_budget(&mut state, uuid);
        }
    }

    #[tokio::test]
    async fn cold_then_warm() {
        let cache = BloomCache::with_budget_bytes(64 * 1024 * 1024);
        let uuid = Uuid::new_v4();
        let v = bloom_for(&["connection refused", "timeout waiting"]);
        install(&cache, uuid, v.clone()).await;
        install(&cache, uuid, v.clone()).await;
        install(&cache, uuid, v).await;
        let s = cache.stats();
        assert_eq!(s.misses, 1);
        assert_eq!(s.hits, 2);
        assert_eq!(s.entries, 1);
    }

    #[tokio::test]
    async fn lru_evicts_oldest() {
        let a_val = bloom_for(&["alpha bravo charlie"]);
        let one = slot_weight(&a_val);
        let cache = BloomCache::with_budget_bytes(one + one / 2);
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let c = Uuid::new_v4();
        install(&cache, a, a_val).await;
        install(&cache, b, bloom_for(&["delta echo foxtrot"])).await;
        assert_eq!(cache.stats().entries, 1);
        assert_eq!(cache.stats().evictions, 1);
        install(&cache, c, bloom_for(&["golf hotel india"])).await;
        assert_eq!(cache.stats().entries, 1);
        assert_eq!(cache.stats().evictions, 2);
        let state = cache.state.lock().unwrap();
        assert!(state.map.contains_key(&c));
        assert!(!state.map.contains_key(&a));
        assert!(!state.map.contains_key(&b));
    }

    #[tokio::test]
    async fn none_value_is_cached_and_weighted() {
        // A block with no usable bloom caches a `None` answer; it still
        // occupies a slot (overhead-weighted) so repeated queries don't
        // re-fetch a known-bad block.
        let cache = BloomCache::with_budget_bytes(64 * 1024);
        let uuid = Uuid::new_v4();
        install(&cache, uuid, Arc::new(None)).await;
        install(&cache, uuid, Arc::new(None)).await;
        let s = cache.stats();
        assert_eq!(s.entries, 1);
        assert_eq!(s.misses, 1);
        assert_eq!(s.hits, 1);
        assert!(s.bytes_in >= SLOT_OVERHEAD_BYTES);
    }

    #[test]
    fn config_from_env() {
        std::env::remove_var("SCRY_BLOOM_CACHE_BYTES");
        let cfg = BloomCacheConfig::from_env().unwrap();
        assert_eq!(cfg.budget_bytes, DEFAULT_BUDGET_BYTES);

        std::env::set_var("SCRY_BLOOM_CACHE_BYTES", "1048576");
        let cfg = BloomCacheConfig::from_env().unwrap();
        assert_eq!(cfg.budget_bytes, 1_048_576);

        std::env::set_var("SCRY_BLOOM_CACHE_BYTES", "not a number");
        assert!(BloomCacheConfig::from_env().is_err());
        std::env::remove_var("SCRY_BLOOM_CACHE_BYTES");
    }
}
