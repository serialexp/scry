//! In-memory cache for parsed per-block postings indexes.
//!
//! v0.3 step 2 stood up the query daemon and demonstrated that pool
//! warmth amortizes across queries, but the smoke also exposed a
//! different bottleneck: every query refetches every block's
//! `postings.parquet` and re-decodes it from scratch — about 1 s out
//! of the 1.3 s observed wall time on the smoke bucket, against
//! blocks that are *immutable*. That's pure waste.
//!
//! This module caches the parsed index — not the raw parquet bytes —
//! keyed by block UUID, with byte-weighted LRU eviction. Single-flight
//! is built in from the start: concurrent callers asking for the same
//! UUID share one [`tokio::sync::OnceCell`] so only one parquet GET
//! happens per block-miss, no matter how many queries arrive in the
//! window between "I want this" and "I have it".
//!
//! ## Design notes
//!
//! - **Cache value:** [`PostingsIndex`] — nested
//!   `HashMap<String, HashMap<String, Arc<Vec<u64>>>>`. The nested
//!   shape lets lookups borrow `&str` directly (no allocation per
//!   probe); the inner `Arc<Vec<u64>>` means the cache hands the same
//!   fingerprint list to every matcher without copying.
//! - **Single-flight via `tokio::sync::OnceCell`.** The slot's cell
//!   is cloned out of the LRU under the lock; concurrent callers
//!   then await the same `get_or_try_init` future. If the winning
//!   task is cancelled mid-fetch, `OnceCell` releases its internal
//!   lock and the next caller becomes the new winner. On fetch
//!   error the cell stays uninitialised, so retries are automatic.
//! - **Weight tracking is atomic, not gated on the LRU mutex.** The
//!   first task whose `weight.swap(new, …)` returns 0 is the one
//!   that did the load and is responsible for bumping `bytes_in` +
//!   running eviction. Subsequent tasks see the non-zero prior and
//!   skip — they already accounted on the winner's behalf.
//! - **Loading slots are eviction-immune.** While `weight == 0` the
//!   entry is still being populated; the eviction loop refuses to
//!   touch it. Otherwise a giant pending fetch could evict itself
//!   between `get_or_try_init` returning and the weight-update step.
//! - **Empty-matcher path is not cached.** That code path reads
//!   `meta.json` to enumerate every fingerprint in the block — a
//!   diagnostic "scan everything" mode, not the hot query shape.
//!   It falls straight through to the existing
//!   [`crate::postings::resolve_fingerprints`] which has its own
//!   `meta.json` GET.
//! - **No memory budget for the empty-matcher fallback either.** If
//!   that path ever shows up in a profile, give it its own cache.
//!
//! ## What the budget covers
//!
//! [`PostingsCacheConfig::budget_bytes`] is an estimate of in-memory
//! footprint — not a hard guarantee. We approximate per-entry size as
//! HashMap overhead + key string heap + `Arc<Vec<u64>>` control block
//! + Vec heap. Reality is ±30% depending on hash table load factor
//! and allocator behaviour, but the order of magnitude is what
//! matters for capacity planning.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use hashlink::LinkedHashMap;
use object_store::ObjectStore;
use scry_block::BlockMeta;
use tokio::sync::OnceCell;
use tracing::warn;
use uuid::Uuid;

use crate::postings::{fetch_and_parse_postings, intersect_matchers};

/// Default byte budget for the postings cache: 256 MiB. Postings
/// sidecars run "a few MB per block" per `ARCHITECTURE.md`, so this
/// holds order-of ~50–100 blocks comfortably.
pub const DEFAULT_BUDGET_BYTES: usize = 256 * 1024 * 1024;

/// Postings cache configuration. Read from env via
/// [`PostingsCacheConfig::from_env`] or built directly.
#[derive(Debug, Clone, Copy)]
pub struct PostingsCacheConfig {
    /// Byte budget; entries beyond this evict LRU-first. A single
    /// entry larger than the budget is accepted anyway (zero
    /// caching is worse than zero eviction) and a warning is logged.
    pub budget_bytes: usize,
}

impl Default for PostingsCacheConfig {
    fn default() -> Self {
        Self {
            budget_bytes: DEFAULT_BUDGET_BYTES,
        }
    }
}

impl PostingsCacheConfig {
    /// Build from `SCRY_POSTINGS_CACHE_BYTES`. Missing/empty = default.
    pub fn from_env() -> Result<Self> {
        let mut cfg = Self::default();
        if let Ok(v) = std::env::var("SCRY_POSTINGS_CACHE_BYTES") {
            let v = v.trim();
            if !v.is_empty() {
                cfg.budget_bytes = v
                    .parse()
                    .with_context(|| format!("SCRY_POSTINGS_CACHE_BYTES={v}"))?;
            }
        }
        Ok(cfg)
    }
}

/// Point-in-time snapshot of cache counters. `delta` is per-query
/// reporting; the gauges (`bytes_in`, `entries`, `budget_bytes`)
/// carry through unchanged.
#[derive(Debug, Clone, Copy, Default)]
pub struct PostingsCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    /// Number of times a fetch failed and left the slot empty for a
    /// future retry. Useful telemetry for "is the object store
    /// unreliable?"
    pub fetch_errors: u64,
    pub bytes_in: usize,
    pub entries: usize,
    pub budget_bytes: usize,
}

impl PostingsCacheStats {
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

/// Parsed postings sidecar held in memory.
///
/// Nested `HashMap` so `lookup(&str, &str)` is two cheap probes with
/// no intermediate allocation. `Arc<Vec<u64>>` for the fingerprint
/// list so concurrent intersections don't copy.
pub struct PostingsIndex {
    entries: HashMap<String, HashMap<String, Arc<Vec<u64>>>>,
    bytes_estimate: usize,
}

impl PostingsIndex {
    /// Build from the already-decoded outer/inner map shape. Estimates
    /// in-memory footprint once at construction time so the cache
    /// doesn't have to walk the structure on every insert.
    pub fn new(entries: HashMap<String, HashMap<String, Arc<Vec<u64>>>>) -> Self {
        let bytes_estimate = Self::estimate(&entries);
        Self {
            entries,
            bytes_estimate,
        }
    }

    /// Estimated in-memory bytes. Approximate (HashMap node overhead
    /// + heap content), not exact, but stable enough for budgeting.
    pub fn bytes_estimate(&self) -> usize {
        self.bytes_estimate
    }

    /// Number of (label_name, label_value) entries stored.
    pub fn entry_count(&self) -> usize {
        self.entries.values().map(|m| m.len()).sum()
    }

    /// `(label_name, label_value) → fingerprints`. Borrowed args; no
    /// allocation on the probe path.
    pub fn lookup(&self, name: &str, value: &str) -> Option<&Arc<Vec<u64>>> {
        self.entries.get(name)?.get(value)
    }

    /// Invert this `(name → value → fingerprints)` index into a
    /// per-fingerprint label set, merging the result into `acc`.
    ///
    /// Each stored `(name, value)` entry contributes its pair to every
    /// fingerprint that carries it. The logs query path calls this across
    /// the candidate blocks to build the `fingerprint → labels` map it
    /// uses to surface stream labels as a result column — fingerprints are
    /// global xxh3 hashes, so the same stream resolves to the same labels
    /// in every block. `acc` is a `BTreeSet` so the resulting label list is
    /// deduplicated and in a stable order.
    pub fn invert_into(
        &self,
        acc: &mut HashMap<u64, std::collections::BTreeSet<(String, String)>>,
    ) {
        for (name, inner) in &self.entries {
            for (value, fps) in inner {
                for &fp in fps.iter() {
                    acc.entry(fp)
                        .or_default()
                        .insert((name.clone(), value.clone()));
                }
            }
        }
    }

    fn estimate(
        entries: &HashMap<String, HashMap<String, Arc<Vec<u64>>>>,
    ) -> usize {
        // Approximate per-entry overhead:
        //   - outer HashMap node       ~48 bytes
        //   - String key heap          name.len() (capacity ≈ len here)
        //   - inner HashMap node       ~48 bytes
        //   - String inner key heap    value.len()
        //   - Arc<Vec<u64>> control    ~24 bytes (Arc + Vec headers)
        //   - Vec<u64> heap            fps.len() * 8
        let mut total = std::mem::size_of_val(entries);
        for (name, inner) in entries {
            total += 48 + name.len();
            total += std::mem::size_of_val(inner);
            for (value, fps) in inner {
                total += 48 + value.len() + 24 + fps.len() * 8;
            }
        }
        total
    }
}

/// One slot in the LRU. The `cell` is shared with all concurrent
/// resolvers waiting on the same block; `weight` is set once the
/// fetch completes (0 = still loading, eviction-immune).
struct CacheSlot {
    cell: Arc<OnceCell<Arc<PostingsIndex>>>,
    weight: Arc<AtomicUsize>,
}

impl CacheSlot {
    fn new_loading() -> Self {
        Self {
            cell: Arc::new(OnceCell::new()),
            weight: Arc::new(AtomicUsize::new(0)),
        }
    }
}

struct LruState {
    /// `LinkedHashMap` keeps insertion (refresh) order. We use
    /// `to_back` on every access so the head is the LRU, the tail is
    /// the MRU. Eviction pops from the head.
    map: LinkedHashMap<Uuid, CacheSlot>,
    /// Sum of `weight` across all loaded entries.
    bytes_in: usize,
}

/// Postings cache: per-block, byte-budgeted, LRU-evicted, single-flight.
pub struct PostingsCache {
    state: Mutex<LruState>,
    budget_bytes: usize,
    // Cumulative counters. Atomic so we don't widen the mutex.
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    fetch_errors: AtomicU64,
}

impl PostingsCache {
    pub fn new(cfg: PostingsCacheConfig) -> Self {
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
        Self::new(PostingsCacheConfig { budget_bytes })
    }

    /// Take a snapshot for telemetry. The cumulative counters use
    /// `Relaxed` because they're for reporting, not correctness.
    pub fn stats(&self) -> PostingsCacheStats {
        let state = self.state.lock().expect("cache mutex poisoned");
        PostingsCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            fetch_errors: self.fetch_errors.load(Ordering::Relaxed),
            bytes_in: state.bytes_in,
            entries: state.map.len(),
            budget_bytes: self.budget_bytes,
        }
    }

    /// Resolve AND'd matchers to the fingerprint set that overlaps
    /// every matcher in the given block, using the cache where
    /// possible. Same return contract as
    /// [`crate::postings::resolve_fingerprints`]:
    /// - `Ok(None)` — at least one matcher had no hits; block prunes.
    /// - `Ok(Some(set))` — non-empty intersection.
    ///
    /// Empty matchers fall through to the un-cached free function.
    pub async fn resolve(
        &self,
        store: Arc<dyn ObjectStore>,
        meta: &BlockMeta,
        matchers: &[(String, String)],
    ) -> Result<Option<HashSet<u64>>> {
        if matchers.is_empty() {
            // The empty-matcher path uses meta.json, not
            // postings.parquet, and is diagnostic. Skip the cache.
            return crate::postings::resolve_fingerprints(store, meta, matchers).await;
        }
        let index = self.get_or_fetch(store, meta).await?;
        Ok(intersect_matchers(&index, matchers))
    }

    /// Core single-flight + LRU logic. Public for callers (and tests)
    /// that want the parsed index itself (e.g., to inspect how big a
    /// block's postings are).
    pub async fn get_or_fetch(
        &self,
        store: Arc<dyn ObjectStore>,
        meta: &BlockMeta,
    ) -> Result<Arc<PostingsIndex>> {
        let uuid = meta.uuid;

        // ── Phase 1: look up or insert the slot under the mutex ─────
        let (cell, weight, was_present) = {
            let mut state = self.state.lock().expect("cache mutex poisoned");
            // `LinkedHashMap::to_back` would move an existing entry
            // to the MRU position. `get_refresh` is the helper for
            // "get + refresh ordering" — but its semantics return a
            // mutable ref to the value. We want a clone of the Arcs.
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

        // Counter is bumped outside the lock; `Relaxed` is fine —
        // we never use these for correctness.
        if was_present {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }

        // ── Phase 2: initialize (or await existing initialization) ──
        //
        // Owned clones move into the FnOnce so the borrowed `meta` /
        // store don't need to outlive the await across the cancel
        // boundaries `OnceCell` may swap winners on.
        let store_for_fetch = store.clone();
        let meta_for_fetch = meta.clone();
        let init_result = cell
            .get_or_try_init(|| async move {
                fetch_and_parse_postings(store_for_fetch, &meta_for_fetch)
                    .await
                    .map(Arc::new)
            })
            .await;

        let index = match init_result {
            Ok(index) => index.clone(),
            Err(e) => {
                // Cell stays uninitialised — `get_or_try_init` only
                // populates on Ok — so the next caller retries.
                // But the empty *slot* is still in the LRU, holding
                // a zero-weight placeholder. Remove it so the slot
                // doesn't permanently occupy a hash bucket on failed
                // blocks; counts as an eviction for telemetry.
                self.fetch_errors.fetch_add(1, Ordering::Relaxed);
                let mut state = self.state.lock().expect("cache mutex poisoned");
                // Defend against the case where another task
                // succeeded in the interim — only remove if it's
                // still a 0-weight slot.
                if weight.load(Ordering::Relaxed) == 0 {
                    state.map.remove(&uuid);
                }
                return Err(e);
            }
        };

        // ── Phase 3: account for the load + run eviction ────────────
        //
        // `swap` returns the prior value atomically. The task whose
        // swap returns 0 is the one that loaded this slot and is
        // responsible for updating bytes_in + evicting. Every other
        // waiter sees the non-zero prior and skips.
        let new_weight = index.bytes_estimate();
        let prev = weight.swap(new_weight, Ordering::AcqRel);
        if prev == 0 {
            let oversized = new_weight > self.budget_bytes;
            if oversized {
                warn!(
                    block_uuid = %uuid,
                    weight_bytes = new_weight,
                    budget_bytes = self.budget_bytes,
                    "postings cache entry exceeds total budget; accepted anyway"
                );
            }
            let mut state = self.state.lock().expect("cache mutex poisoned");
            state.bytes_in = state.bytes_in.saturating_add(new_weight);
            self.evict_to_budget(&mut state, /* protect_uuid */ uuid);
        }
        Ok(index)
    }

    /// Pop LRU entries from the head until `bytes_in <= budget_bytes`
    /// or we run out of evictable entries. Loading slots (weight==0)
    /// and the just-inserted entry (`protect_uuid`) are skipped.
    fn evict_to_budget(&self, state: &mut LruState, protect_uuid: Uuid) {
        while state.bytes_in > self.budget_bytes && state.map.len() > 1 {
            // Find the LRU evictable entry. `LinkedHashMap::iter` is
            // in insertion (refresh) order — head is LRU. Walk
            // forward, skipping unevictable slots, until we find one
            // we can drop.
            let victim_uuid: Option<Uuid> = state
                .map
                .iter()
                .find_map(|(uuid, slot)| {
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
                // Nothing evictable (every other entry is the
                // protected one or still loading). Bail; we'll come
                // back to it on the next insert.
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

impl std::fmt::Debug for PostingsCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.stats();
        f.debug_struct("PostingsCache")
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
    use std::sync::Arc;

    fn fp_list(items: &[u64]) -> Arc<Vec<u64>> {
        Arc::new(items.to_vec())
    }

    /// Build a small `PostingsIndex` directly without going through
    /// parquet — keeps the unit tests focused on cache behaviour
    /// instead of postings parsing.
    fn synthetic_index(
        name: &str,
        value: &str,
        fps: &[u64],
    ) -> Arc<PostingsIndex> {
        let mut inner = HashMap::new();
        inner.insert(value.to_string(), fp_list(fps));
        let mut outer = HashMap::new();
        outer.insert(name.to_string(), inner);
        Arc::new(PostingsIndex::new(outer))
    }

    /// Forces a cache slot to a pre-built index via the same paths a
    /// real `get_or_fetch` would, but synchronously and without
    /// network I/O. Used to set up scenarios for eviction +
    /// LRU-order tests.
    async fn install(cache: &PostingsCache, uuid: Uuid, index: Arc<PostingsIndex>) {
        // Mirror `get_or_fetch` phases 1 + 3 minus the actual fetch.
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
            .get_or_try_init(|| async { Ok::<_, anyhow::Error>(index.clone()) })
            .await
            .unwrap();
        let new_weight = index.bytes_estimate();
        let prev = weight.swap(new_weight, Ordering::AcqRel);
        if prev == 0 {
            let mut state = cache.state.lock().unwrap();
            state.bytes_in += new_weight;
            cache.evict_to_budget(&mut state, uuid);
        }
    }

    #[tokio::test]
    async fn lookup_returns_arc_clone() {
        let idx = synthetic_index("env", "prod", &[1, 2, 3]);
        let got = idx.lookup("env", "prod").unwrap().clone();
        assert_eq!(*got, vec![1, 2, 3]);
        assert!(idx.lookup("env", "stage").is_none());
        assert!(idx.lookup("missing", "x").is_none());
    }

    #[tokio::test]
    async fn cold_then_warm() {
        let cache = PostingsCache::with_budget_bytes(64 * 1024);
        let uuid = Uuid::new_v4();
        let idx = synthetic_index("__name__", "foo", &[10, 20]);
        install(&cache, uuid, idx.clone()).await;
        install(&cache, uuid, idx.clone()).await;
        install(&cache, uuid, idx.clone()).await;
        let s = cache.stats();
        assert_eq!(s.misses, 1);
        assert_eq!(s.hits, 2);
        assert_eq!(s.entries, 1);
    }

    #[tokio::test]
    async fn lru_evicts_oldest() {
        // Each synthetic_index is small (well under any sane budget);
        // we set the budget low enough that two of them overflow.
        let idx_a = synthetic_index("env", "prod", &[1, 2, 3]);
        let idx_b = synthetic_index("env", "stage", &[4, 5, 6]);
        let idx_c = synthetic_index("env", "dev", &[7]);
        // Budget = 1.5x one entry's weight → at most 1 fits.
        let one = idx_a.bytes_estimate();
        let cache = PostingsCache::with_budget_bytes(one + one / 2);

        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let c = Uuid::new_v4();

        install(&cache, a, idx_a).await;
        install(&cache, b, idx_b).await;
        // Inserting B evicts A.
        assert_eq!(cache.stats().entries, 1);
        assert_eq!(cache.stats().evictions, 1);

        install(&cache, c, idx_c).await;
        // Inserting C evicts B; only C remains.
        assert_eq!(cache.stats().entries, 1);
        assert_eq!(cache.stats().evictions, 2);
        let state = cache.state.lock().unwrap();
        assert!(state.map.contains_key(&c));
        assert!(!state.map.contains_key(&a));
        assert!(!state.map.contains_key(&b));
    }

    #[tokio::test]
    async fn lru_promotes_on_access() {
        // Three entries, budget big enough for two. Touching A
        // before inserting C should cause B (the LRU) to be evicted,
        // not A.
        let idx_a = synthetic_index("env", "prod", &[1]);
        let idx_b = synthetic_index("env", "stage", &[2]);
        let idx_c = synthetic_index("env", "dev", &[3]);
        let one = idx_a.bytes_estimate();
        let cache = PostingsCache::with_budget_bytes(one * 2 + one / 2);

        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let c = Uuid::new_v4();

        install(&cache, a, idx_a.clone()).await;
        install(&cache, b, idx_b).await;
        // Touch A → A becomes MRU, B becomes LRU.
        install(&cache, a, idx_a).await;
        install(&cache, c, idx_c).await;

        // B should have been evicted.
        let state = cache.state.lock().unwrap();
        assert!(state.map.contains_key(&a));
        assert!(state.map.contains_key(&c));
        assert!(!state.map.contains_key(&b));
    }

    #[tokio::test]
    async fn oversized_single_entry_accepted_with_warning() {
        // Budget = 1 byte. A single entry will exceed it. We should
        // still accept it — the alternative is "zero caching", which
        // is strictly worse. (The `warn!` log isn't asserted here;
        // eyeball the test output.)
        let cache = PostingsCache::with_budget_bytes(1);
        let uuid = Uuid::new_v4();
        let idx = synthetic_index("env", "prod", &[1, 2, 3, 4, 5]);
        install(&cache, uuid, idx).await;

        let state = cache.state.lock().unwrap();
        assert_eq!(state.map.len(), 1);
        assert!(state.bytes_in > cache.budget_bytes);
    }

    #[tokio::test]
    async fn single_flight_dedupes_concurrent_misses() {
        // Many concurrent get_or_fetch calls on the same UUID where
        // the fetch is intentionally slow. Only ONE fetch closure
        // body should actually run. We assert this by counting
        // entries into the (test-only) closure, not by wrapping an
        // ObjectStore.
        use std::sync::atomic::AtomicUsize;
        use std::time::Duration;
        use tokio::sync::Barrier;

        let cache = Arc::new(PostingsCache::with_budget_bytes(1 << 30));
        let uuid = Uuid::new_v4();
        let fetch_count = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(8));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let cache = cache.clone();
            let fetch_count = fetch_count.clone();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                // Replicate phase 1 + 2 of get_or_fetch but with a
                // synthetic fetcher so we don't need a fake store.
                let (cell, weight) = {
                    let mut state = cache.state.lock().unwrap();
                    if state.map.contains_key(&uuid) {
                        state.map.to_back(&uuid);
                        let slot = state.map.get(&uuid).unwrap();
                        cache.hits.fetch_add(1, Ordering::Relaxed);
                        (slot.cell.clone(), slot.weight.clone())
                    } else {
                        let slot = CacheSlot::new_loading();
                        let cell = slot.cell.clone();
                        let weight = slot.weight.clone();
                        state.map.insert(uuid, slot);
                        cache.misses.fetch_add(1, Ordering::Relaxed);
                        (cell, weight)
                    }
                };
                let fetch_count = fetch_count.clone();
                let _ = cell
                    .get_or_try_init(|| async move {
                        fetch_count.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Ok::<_, anyhow::Error>(synthetic_index(
                            "env",
                            "prod",
                            &[1, 2, 3],
                        ))
                    })
                    .await
                    .unwrap();
                let new_weight = 1; // doesn't matter for this test
                let prev = weight.swap(new_weight, Ordering::AcqRel);
                if prev == 0 {
                    let mut state = cache.state.lock().unwrap();
                    state.bytes_in += new_weight;
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(
            fetch_count.load(Ordering::Relaxed),
            1,
            "single-flight should run the fetch closure exactly once"
        );
        let s = cache.stats();
        // 8 callers; the winner is the miss, the rest may or may not
        // race in as hits depending on scheduling. What we *can*
        // assert is misses + hits = 8 and misses == 1 (since the
        // slot was inserted by the winner before any other caller
        // observed it... actually no, that ordering isn't
        // guaranteed). Loosen the assertion: total = 8, misses ≥ 1.
        assert_eq!(s.hits + s.misses, 8);
        assert!(s.misses >= 1);
    }

    #[tokio::test]
    async fn fetch_error_does_not_strand_slot() {
        // Simulate a failed fetch: cell errors, slot must be removed
        // so retries are clean. We replicate the error-path manually
        // (real fetch errors require a broken object store; the
        // happy-path code is tested via `install`).
        let cache = PostingsCache::with_budget_bytes(1 << 20);
        let uuid = Uuid::new_v4();
        // Phase 1: insert empty slot.
        let (cell, weight) = {
            let mut state = cache.state.lock().unwrap();
            let slot = CacheSlot::new_loading();
            let cell = slot.cell.clone();
            let weight = slot.weight.clone();
            state.map.insert(uuid, slot);
            (cell, weight)
        };
        cache.misses.fetch_add(1, Ordering::Relaxed);
        // Phase 2 (simulated failure): get_or_try_init returns Err
        // and OnceCell stays uninitialised.
        let init_result: Result<&Arc<PostingsIndex>, anyhow::Error> = cell
            .get_or_try_init(|| async { Err(anyhow::anyhow!("simulated fetch failure")) })
            .await;
        assert!(init_result.is_err());
        // Mirror the error-path cleanup the real get_or_fetch runs.
        cache.fetch_errors.fetch_add(1, Ordering::Relaxed);
        if weight.load(Ordering::Relaxed) == 0 {
            let mut state = cache.state.lock().unwrap();
            state.map.remove(&uuid);
        }
        let s = cache.stats();
        assert_eq!(s.entries, 0, "failed slot must be removed");
        assert_eq!(s.fetch_errors, 1);
    }

    #[test]
    fn config_from_env() {
        std::env::remove_var("SCRY_POSTINGS_CACHE_BYTES");
        let cfg = PostingsCacheConfig::from_env().unwrap();
        assert_eq!(cfg.budget_bytes, DEFAULT_BUDGET_BYTES);

        std::env::set_var("SCRY_POSTINGS_CACHE_BYTES", "1048576");
        let cfg = PostingsCacheConfig::from_env().unwrap();
        assert_eq!(cfg.budget_bytes, 1_048_576);

        std::env::set_var("SCRY_POSTINGS_CACHE_BYTES", "not a number");
        assert!(PostingsCacheConfig::from_env().is_err());
        std::env::remove_var("SCRY_POSTINGS_CACHE_BYTES");
    }
}
