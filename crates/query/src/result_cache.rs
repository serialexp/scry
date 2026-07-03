//! In-memory cache for whole query responses, keyed by request⊕candidate-set.
//!
//! The query daemon exists because blocks are immutable, so per-block sidecars
//! (postings, blooms) are worth caching after the first hit. This module takes
//! the idea one level up: cache the **entire serialized response** (the
//! concatenated `SchemaMsg` + `BatchMsg…` + `EndOfStream` frame bytes, or a
//! single metadata response frame) so a repeated request — the shape a
//! dashboard produces when it re-polls the same panel — is answered straight
//! from memory without touching DataFusion or the object store at all.
//!
//! ## Correctness: the candidate-set is part of the key
//!
//! A cached response is only valid while the query would still read the *same
//! blocks*. Rather than wire explicit invalidation into ingest / compaction /
//! retention, the caller folds a hash of the **candidate block-UUID set** into
//! the cache key (see `query_service::cache_key`). Any event that changes which
//! blocks a range touches — a new block landing, a compaction merging inputs, a
//! retention reap — changes the candidate set, hence the key, hence it's an
//! automatic miss. A *closed past range* has a stable candidate set and stays
//! cached. No TTL is required for correctness; a caller may still choose to fold
//! a coarse time bucket in for a memory backstop.
//!
//! ## Why simpler than [`crate::postings_cache::PostingsCache`]
//!
//! - **No single-flight.** A query response is recomputable and a duplicate
//!   concurrent miss is rare and cheap (it just recomputes); we don't hold a
//!   slot across the whole scan. `get` / `insert` are plain LRU operations.
//! - **Value is opaque bytes.** `Arc<[u8]>` — the already-framed wire bytes —
//!   so a hit is a single `write_all`.
//! - **Oversized entries are skipped, not accepted.** Unlike postings (where
//!   zero-caching a block is strictly worse), a query result larger than the
//!   whole budget is simply not cached — the caller already streams it, and
//!   admitting it would thrash the LRU. The caller additionally caps per-entry
//!   size so only small aggregation / metadata results are ever offered.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use hashlink::LinkedHashMap;

/// Default byte budget for the query result cache: 256 MiB. Aggregation /
/// metadata responses are small (KBs), so this holds many thousands of them.
pub const DEFAULT_QUERY_CACHE_BYTES: usize = 256 * 1024 * 1024;

/// Default per-entry cap the *caller* enforces while buffering a response:
/// 8 MiB. Above this a response is streamed but never cached (keeps large log
/// dumps out of the cache; keeps the dashboard-shaped small results in). Held
/// here as the shared default; `QueryResultCache` itself only knows the total
/// budget.
pub const DEFAULT_QUERY_CACHE_ENTRY_BYTES: usize = 8 * 1024 * 1024;

/// Fixed per-entry bookkeeping overhead added to the payload length when
/// accounting a cached response (LinkedHashMap node + `Arc<[u8]>` control
/// block + key). Approximate; the payload dominates.
const ENTRY_OVERHEAD_BYTES: usize = 64;

/// Point-in-time snapshot of cache counters. `delta` gives per-query reporting;
/// the gauges (`bytes_in`, `entries`, `budget_bytes`) carry through unchanged.
#[derive(Debug, Clone, Copy, Default)]
pub struct QueryResultCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub evictions: u64,
    pub bytes_in: usize,
    pub entries: usize,
    pub budget_bytes: usize,
}

impl QueryResultCacheStats {
    pub fn delta(self, prior: Self) -> Self {
        Self {
            hits: self.hits.saturating_sub(prior.hits),
            misses: self.misses.saturating_sub(prior.misses),
            inserts: self.inserts.saturating_sub(prior.inserts),
            evictions: self.evictions.saturating_sub(prior.evictions),
            bytes_in: self.bytes_in,
            entries: self.entries,
            budget_bytes: self.budget_bytes,
        }
    }
}

struct Entry {
    bytes: Arc<[u8]>,
    weight: usize,
}

struct LruState {
    /// Insertion (refresh) order; head = LRU, tail = MRU. `to_back` on a hit
    /// promotes; eviction pops the head.
    map: LinkedHashMap<u128, Entry>,
    bytes_in: usize,
}

/// Whole-response cache: byte-budgeted, LRU-evicted. Keyed by a 128-bit hash
/// the caller builds from the normalized request plus the candidate block set.
pub struct QueryResultCache {
    state: Mutex<LruState>,
    budget_bytes: usize,
    hits: AtomicU64,
    misses: AtomicU64,
    inserts: AtomicU64,
    evictions: AtomicU64,
}

impl QueryResultCache {
    /// Build with an explicit byte budget. A budget of `0` disables the cache:
    /// [`get`](Self::get) always misses (without counting) and
    /// [`insert`](Self::insert) is a no-op.
    pub fn with_budget_bytes(budget_bytes: usize) -> Self {
        Self {
            state: Mutex::new(LruState {
                map: LinkedHashMap::new(),
                bytes_in: 0,
            }),
            budget_bytes,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    /// Whether the cache admits entries at all (budget > 0).
    pub fn enabled(&self) -> bool {
        self.budget_bytes > 0
    }

    pub fn budget_bytes(&self) -> usize {
        self.budget_bytes
    }

    /// Snapshot for telemetry. Cumulative counters are `Relaxed` (reporting,
    /// not correctness).
    pub fn stats(&self) -> QueryResultCacheStats {
        let state = self.state.lock().expect("result cache mutex poisoned");
        QueryResultCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            inserts: self.inserts.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            bytes_in: state.bytes_in,
            entries: state.map.len(),
            budget_bytes: self.budget_bytes,
        }
    }

    /// Look up a cached response, promoting it to MRU on a hit. Returns the
    /// shared response bytes, ready to `write_all` to the socket. Counts a
    /// hit / miss unless the cache is disabled (then always `None`, uncounted).
    pub fn get(&self, key: u128) -> Option<Arc<[u8]>> {
        if !self.enabled() {
            return None;
        }
        let mut state = self.state.lock().expect("result cache mutex poisoned");
        if state.map.contains_key(&key) {
            state.map.to_back(&key);
            let bytes = state.map.get(&key).expect("present").bytes.clone();
            drop(state);
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(bytes)
        } else {
            drop(state);
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Admit a fully-buffered response under `key`. No-op if the cache is
    /// disabled or the entry alone would exceed the whole budget (query
    /// results are recomputable, so admitting a budget-buster only thrashes).
    /// Re-inserting an existing key refreshes its bytes + ordering.
    pub fn insert(&self, key: u128, bytes: Arc<[u8]>) {
        if !self.enabled() {
            return;
        }
        let weight = bytes.len().saturating_add(ENTRY_OVERHEAD_BYTES);
        if weight > self.budget_bytes {
            return;
        }
        let mut state = self.state.lock().expect("result cache mutex poisoned");
        // Replace an existing entry's accounting if the key is already present.
        if let Some(old) = state.map.remove(&key) {
            state.bytes_in = state.bytes_in.saturating_sub(old.weight);
        }
        state.bytes_in = state.bytes_in.saturating_add(weight);
        state.map.insert(key, Entry { bytes, weight });
        self.inserts.fetch_add(1, Ordering::Relaxed);
        self.evict_to_budget(&mut state);
    }

    /// Pop LRU entries (head) until within budget. The just-inserted entry sits
    /// at the tail (MRU) so it's evicted last; the `len() > 1` guard means a
    /// lone over-budget entry can't evict itself (and `insert` already refuses
    /// entries larger than the whole budget, so this can't loop forever).
    fn evict_to_budget(&self, state: &mut LruState) {
        while state.bytes_in > self.budget_bytes && state.map.len() > 1 {
            if let Some((_, victim)) = state.map.pop_front() {
                state.bytes_in = state.bytes_in.saturating_sub(victim.weight);
                self.evictions.fetch_add(1, Ordering::Relaxed);
            } else {
                break;
            }
        }
    }
}

impl std::fmt::Debug for QueryResultCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.stats();
        f.debug_struct("QueryResultCache")
            .field("entries", &s.entries)
            .field("bytes_in", &s.bytes_in)
            .field("budget_bytes", &s.budget_bytes)
            .field("hits", &s.hits)
            .field("misses", &s.misses)
            .field("inserts", &s.inserts)
            .field("evictions", &s.evictions)
            .finish()
    }
}

/// 128-bit content hash used to build cache keys. xxh3-128 — fast and wide
/// enough that a collision (which would serve a wrong cached response) is
/// astronomically unlikely across any realistic working set. The caller feeds
/// a canonical byte serialization of the normalized request + candidate set.
pub fn hash128(bytes: &[u8]) -> u128 {
    twox_hash::xxh3::hash128(bytes)
}

// ─── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(n: usize, fill: u8) -> Arc<[u8]> {
        vec![fill; n].into()
    }

    #[test]
    fn hit_after_insert_and_miss_when_absent() {
        let cache = QueryResultCache::with_budget_bytes(1 << 20);
        assert!(cache.get(1).is_none()); // miss
        cache.insert(1, bytes(100, 0xAB));
        let got = cache.get(1).expect("hit");
        assert_eq!(got.len(), 100);
        assert_eq!(got[0], 0xAB);
        assert!(cache.get(2).is_none()); // miss

        let s = cache.stats();
        assert_eq!(s.hits, 1);
        assert_eq!(s.misses, 2);
        assert_eq!(s.inserts, 1);
        assert_eq!(s.entries, 1);
    }

    #[test]
    fn disabled_cache_never_stores_or_counts() {
        let cache = QueryResultCache::with_budget_bytes(0);
        assert!(!cache.enabled());
        assert!(cache.get(1).is_none());
        cache.insert(1, bytes(10, 0));
        assert!(cache.get(1).is_none());
        let s = cache.stats();
        assert_eq!(s.hits, 0);
        assert_eq!(s.misses, 0);
        assert_eq!(s.inserts, 0);
        assert_eq!(s.entries, 0);
    }

    #[test]
    fn lru_evicts_oldest_over_budget() {
        // Budget fits ~2 entries of 1000 bytes (+overhead).
        let each = 1000 + ENTRY_OVERHEAD_BYTES;
        let cache = QueryResultCache::with_budget_bytes(each * 2 + each / 2);
        cache.insert(1, bytes(1000, 1));
        cache.insert(2, bytes(1000, 2));
        assert_eq!(cache.stats().entries, 2);
        // Third insert overflows → evicts key 1 (LRU).
        cache.insert(3, bytes(1000, 3));
        assert_eq!(cache.stats().entries, 2);
        assert_eq!(cache.stats().evictions, 1);
        assert!(cache.get(1).is_none());
        assert!(cache.get(2).is_some());
        assert!(cache.get(3).is_some());
    }

    #[test]
    fn get_promotes_to_mru() {
        let each = 1000 + ENTRY_OVERHEAD_BYTES;
        let cache = QueryResultCache::with_budget_bytes(each * 2 + each / 2);
        cache.insert(1, bytes(1000, 1));
        cache.insert(2, bytes(1000, 2));
        // Touch key 1 → it becomes MRU, key 2 becomes LRU.
        assert!(cache.get(1).is_some());
        cache.insert(3, bytes(1000, 3));
        // Key 2 should have been evicted, not key 1.
        assert!(cache.get(1).is_some());
        assert!(cache.get(3).is_some());
        assert!(cache.get(2).is_none());
    }

    #[test]
    fn oversized_entry_is_skipped() {
        let cache = QueryResultCache::with_budget_bytes(500);
        cache.insert(1, bytes(1000, 1)); // larger than the whole budget
        assert_eq!(cache.stats().entries, 0);
        assert_eq!(cache.stats().inserts, 0);
        assert!(cache.get(1).is_none());
    }

    #[test]
    fn reinsert_refreshes_bytes_without_double_counting() {
        let cache = QueryResultCache::with_budget_bytes(1 << 20);
        cache.insert(1, bytes(100, 1));
        cache.insert(1, bytes(200, 2));
        let s = cache.stats();
        assert_eq!(s.entries, 1);
        assert_eq!(s.bytes_in, 200 + ENTRY_OVERHEAD_BYTES);
        assert_eq!(cache.get(1).unwrap().len(), 200);
    }

    #[test]
    fn hash128_is_stable_and_discriminates() {
        assert_eq!(hash128(b"scry"), hash128(b"scry"));
        assert_ne!(hash128(b"scry"), hash128(b"scr"));
    }
}
