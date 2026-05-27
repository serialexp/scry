//! Buffer pool for HTTP response bodies.
//!
//! ## Why this exists
//!
//! DWARF-resolved profiling of `scry-query` against the smoke bucket
//! (`flamegraphs/20260527T025744Z-selective.deep.svg`) showed ~30 %
//! of wall time in kernel page-fault servicing — `clear_page_erms`,
//! `do_anonymous_page`, `alloc_anon_folio` — under a stack that
//! always ended in `object_store::util::collect_bytes` →
//! `Vec::extend_from_slice` inside `ObjectStore::get_range`. Each
//! per-range fetch was allocating a fresh `Vec<u8>` sized to the
//! range; once over glibc's ~128 KB threshold those go straight to
//! `mmap`, the kernel zeroes the pages on first write, then they
//! get `munmap`'d on Drop. A single query issues hundreds of such
//! fetches; every one paid full page-zero cost.
//!
//! Switching to mimalloc didn't help — for a one-shot CLI process,
//! the allocator can't amortize across queries that don't exist.
//! Within a *single* query though, we have ≤ 10 in-flight fetches
//! (object_store's `OBJECT_STORE_COALESCE_PARALLEL`) and many
//! sequential ones after that. Pooling ≈ 16 buffers means only the
//! first few fetches in a query pay full kernel-allocation cost; the
//! rest reuse already-faulted pages. The pages stay resident across
//! the entire query, no `munmap`, no re-zero.
//!
//! ## Mechanics
//!
//! - `BufPool` holds a `Mutex<Vec<Vec<u8>>>` of free buffers, LIFO
//!   for cache locality.
//! - `checkout(min_cap)` returns a `Vec<u8>` with `capacity ≥ min_cap`,
//!   either reused or freshly allocated.
//! - [`PooledBuf`] wraps the borrowed `Vec` + a back-pointer to the
//!   pool; on Drop it calls `clear()` (preserves capacity) and
//!   returns the buffer to the pool.
//! - [`PooledBuf`] implements `AsRef<[u8]>`, so we can hand the
//!   underlying bytes to `bytes::Bytes::from_owner(...)` and the
//!   `Bytes` keeps the buffer alive until its last clone drops.
//!
//! Pool capacity is bounded: when the pool is full, a new return
//! replaces the smallest-capacity buffer (so the pool stabilises to
//! the *N largest* sizes ever seen, which is what parquet's read
//! pattern wants — row groups dominate, footers and postings are
//! small and cheap to allocate fresh).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Default initial maximum number of buffers the pool retains. Chosen
/// so it covers `object_store::OBJECT_STORE_COALESCE_PARALLEL = 10`
/// parallel in-flight fetches plus headroom for sequential ones,
/// without pinning so much RSS that it competes with the parquet
/// decoder's own buffers. Tweak via [`BufPoolConfig`] if a workload
/// shape demands more or fewer.
pub const DEFAULT_POOL_CAPACITY: usize = 16;

/// Hard ceiling for autoscale growth. Far above what any sane query
/// should need; meant to stop pathological growth, not constrain
/// normal use.
pub const DEFAULT_POOL_MAX_CAPACITY: usize = 128;

/// Default per-buffer warmup size. Matches the rough working-set
/// size of a single block fetch on the smoke bucket (≈10 MiB of
/// coalesced parquet ranges).
pub const DEFAULT_POOL_WARMUP_SIZE: usize = 10 * 1024 * 1024;

/// When autoscale fires (peak in-flight exceeded current capacity),
/// grow by this many slots so the next burst has headroom without
/// triggering again immediately.
pub const DEFAULT_POOL_AUTOSCALE_HEADROOM: usize = 4;

/// Construction config for [`BufPool`].
///
/// All fields have sensible defaults via [`BufPoolConfig::default`];
/// override the ones that matter for your workload:
///
/// - One-shot CLI: set `warmup_count` so the very first query skips
///   page-fault cost (no second query exists to amortize against).
/// - Long-running daemon: leave `warmup_count` at 0; autoscale + the
///   organic LIFO churn warm the pool naturally after a query or two.
#[derive(Debug, Clone)]
pub struct BufPoolConfig {
    /// Starting cap on the free list. Buffers returned after the cap
    /// is reached either evict a smaller one or get dropped.
    pub initial_capacity: usize,
    /// Hard ceiling autoscale will never cross. Bound on RSS the pool
    /// can hold.
    pub max_capacity: usize,
    /// At construction, allocate this many buffers of `warmup_size`
    /// bytes, force-fault their pages, and park them in the free list.
    /// 0 = no synchronous warmup (cold start).
    pub warmup_count: usize,
    /// Capacity (in bytes) of each warmup buffer. Should match the
    /// typical per-fetch coalesced range size for the workload.
    pub warmup_size: usize,
    /// When `peak_in_flight` exceeds the current capacity, grow
    /// capacity by this many slots (clamped to `max_capacity`). 0
    /// disables autoscale.
    pub autoscale_headroom: usize,
}

/// Point-in-time snapshot of [`BufPool`] counters. All `Copy`, all
/// `pub` for cheap subtraction in delta reporting.
#[derive(Debug, Clone, Copy, Default)]
pub struct PoolStats {
    pub allocs: usize,
    pub reuses: usize,
    pub misses: usize,
    pub in_flight: usize,
    pub peak_in_flight: usize,
    pub grows: usize,
    pub free_count: usize,
    pub capacity: usize,
}

impl PoolStats {
    /// Field-wise `self - other` for the cumulative counters. Fields
    /// that aren't cumulative (in_flight, free_count, capacity,
    /// peak_in_flight) carry through `self`'s value unchanged — the
    /// "delta" of an instantaneous gauge is meaningless.
    pub fn delta(self, other: Self) -> Self {
        Self {
            allocs: self.allocs.saturating_sub(other.allocs),
            reuses: self.reuses.saturating_sub(other.reuses),
            misses: self.misses.saturating_sub(other.misses),
            grows: self.grows.saturating_sub(other.grows),
            in_flight: self.in_flight,
            peak_in_flight: self.peak_in_flight,
            free_count: self.free_count,
            capacity: self.capacity,
        }
    }
}

impl Default for BufPoolConfig {
    fn default() -> Self {
        Self {
            initial_capacity: DEFAULT_POOL_CAPACITY,
            max_capacity: DEFAULT_POOL_MAX_CAPACITY,
            warmup_count: 0,
            warmup_size: DEFAULT_POOL_WARMUP_SIZE,
            autoscale_headroom: DEFAULT_POOL_AUTOSCALE_HEADROOM,
        }
    }
}

/// Pool of reusable `Vec<u8>` buffers.
///
/// Cheap to clone — internally an `Arc<Mutex<...>>`. All clones share
/// the same free list, which is what you want: pool a single
/// per-process pool, not per-store.
#[derive(Clone)]
pub struct BufPool {
    inner: Arc<Inner>,
}

struct Inner {
    free: Mutex<Vec<Vec<u8>>>,
    /// Cap on `free.len()`. `Mutex`-guarded together with `free` so
    /// autoscale can grow it without racing on the eviction decision.
    /// (Stored separately from `free` capacity for explicit semantics
    /// — `free.capacity()` is just a Vec implementation detail.)
    capacity: Mutex<usize>,
    /// Hard ceiling autoscale won't cross.
    max_capacity: usize,
    autoscale_headroom: usize,

    // ── Diagnostic counters ──────────────────────────────────────
    //
    // Atomic so checkout/checkin don't need to widen the mutex
    // critical section just to update them. Relaxed ordering — the
    // values are read for human reporting, never for correctness.
    allocs: AtomicUsize,
    reuses: AtomicUsize,
    misses: AtomicUsize, // checkin dropped (pool full + smaller buffer)
    in_flight: AtomicUsize,
    peak_in_flight: AtomicUsize,
    grows: AtomicUsize,
}

impl BufPool {
    /// Build a pool with default configuration. Equivalent to
    /// `BufPool::with_config(BufPoolConfig::default())`.
    pub fn new() -> Self {
        Self::with_config(BufPoolConfig::default())
    }

    /// Build a pool with a hard capacity cap and no autoscale. Use
    /// this for tests or callers that want a known-stable cap; reach
    /// for [`BufPool::with_config`] when you want warmup or autoscale.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_config(BufPoolConfig {
            initial_capacity: capacity,
            max_capacity: capacity,
            warmup_count: 0,
            warmup_size: 0,
            autoscale_headroom: 0,
        })
    }

    /// Build a pool from a config. If `warmup_count > 0`, this
    /// synchronously allocates `warmup_count` buffers of `warmup_size`
    /// bytes each and force-faults every page in them before parking
    /// them in the free list — so subsequent checkouts hand back
    /// already-resident memory.
    ///
    /// The faulting itself is the point. `Vec::with_capacity` only
    /// reserves virtual address space; the kernel doesn't back the
    /// pages with physical frames until you write to them. We do that
    /// write explicitly via `resize(N, 0)`, then `clear()` to bring
    /// `len` back to 0 without releasing the capacity.
    pub fn with_config(cfg: BufPoolConfig) -> Self {
        let initial = cfg.initial_capacity.max(1);
        let max = cfg.max_capacity.max(initial);
        let mut free: Vec<Vec<u8>> = Vec::with_capacity(initial.max(cfg.warmup_count));

        if cfg.warmup_count > 0 && cfg.warmup_size > 0 {
            for _ in 0..cfg.warmup_count {
                let mut buf: Vec<u8> = Vec::with_capacity(cfg.warmup_size);
                // Touch every page. `resize` writes 0 across the
                // length, which forces the kernel to back each 4 KiB
                // page with a physical frame and zero it once. Without
                // this the pages stay un-faulted until first real use
                // — which defeats the purpose of warmup.
                buf.resize(cfg.warmup_size, 0);
                buf.clear();
                free.push(buf);
            }
        }

        // If warmup overshot initial_capacity, raise it to fit; we
        // never want to immediately drop pages we just paid to fault.
        let capacity = initial.max(free.len()).min(max);

        Self {
            inner: Arc::new(Inner {
                free: Mutex::new(free),
                capacity: Mutex::new(capacity),
                max_capacity: max,
                autoscale_headroom: cfg.autoscale_headroom,
                allocs: AtomicUsize::new(0),
                reuses: AtomicUsize::new(0),
                misses: AtomicUsize::new(0),
                in_flight: AtomicUsize::new(0),
                peak_in_flight: AtomicUsize::new(0),
                grows: AtomicUsize::new(0),
            }),
        }
    }

    /// Number of free buffers currently parked in the pool.
    pub fn free_count(&self) -> usize {
        self.inner.free.lock().unwrap().len()
    }

    /// Current free-list capacity (may have grown past
    /// `initial_capacity` via autoscale).
    pub fn capacity(&self) -> usize {
        *self.inner.capacity.lock().unwrap()
    }

    /// Hard ceiling autoscale will not cross.
    pub fn max_capacity(&self) -> usize {
        self.inner.max_capacity
    }

    /// Fresh allocations the pool has handed out (LIFO scan miss
    /// fell through to `Vec::with_capacity`).
    pub fn allocs(&self) -> usize {
        self.inner.allocs.load(Ordering::Relaxed)
    }

    /// Pool hits — checkouts satisfied by an already-allocated Vec.
    pub fn reuses(&self) -> usize {
        self.inner.reuses.load(Ordering::Relaxed)
    }

    /// Buffers dropped on checkin because the pool was full and the
    /// returning buffer wasn't bigger than the existing pool members.
    pub fn misses(&self) -> usize {
        self.inner.misses.load(Ordering::Relaxed)
    }

    /// Currently-checked-out buffer count.
    pub fn in_flight(&self) -> usize {
        self.inner.in_flight.load(Ordering::Relaxed)
    }

    /// Highest concurrent in-flight count ever observed.
    pub fn peak_in_flight(&self) -> usize {
        self.inner.peak_in_flight.load(Ordering::Relaxed)
    }

    /// Number of times autoscale fired (capacity grew).
    pub fn grows(&self) -> usize {
        self.inner.grows.load(Ordering::Relaxed)
    }

    /// Take a snapshot of all six counters in one go. Useful for
    /// per-query delta reporting — capture at request start, subtract
    /// at request end, log the delta. Note that this is race-y under
    /// concurrent queries (two overlapping queries each see the other's
    /// allocs in their deltas); fine for v0.3 single-query development,
    /// per-query-tagged counters are a later v0.3.x item.
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            allocs: self.allocs(),
            reuses: self.reuses(),
            misses: self.misses(),
            in_flight: self.in_flight(),
            peak_in_flight: self.peak_in_flight(),
            grows: self.grows(),
            free_count: self.free_count(),
            capacity: self.capacity(),
        }
    }

    /// Check out a buffer with `capacity() >= min_cap`. Reuses a
    /// pooled buffer when one fits; allocates a fresh `Vec` when not.
    ///
    /// Strategy: LIFO scan from the top of the free list (most
    /// recently returned, hottest in cache). Take the first buffer
    /// whose capacity is enough — we don't try to find the smallest
    /// sufficient one because (a) the free list is short, (b) "first
    /// fit from the top" preserves LIFO temperature for the buffers
    /// we leave behind.
    ///
    /// Also bumps `in_flight` and, if it exceeds the current capacity,
    /// triggers autoscale (synchronously raises the cap; does NOT
    /// allocate + warm a new buffer here, because that would stall
    /// the checkout on ~10 ms of page-zeroing). The cap rises so the
    /// NEXT round of checkins lands more buffers in the pool instead
    /// of getting dropped.
    fn checkout(&self, min_cap: usize) -> Vec<u8> {
        // ── In-flight bookkeeping + autoscale trigger ─────────────
        let new_in_flight = self.inner.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        // High-water update; relaxed CAS loop is fine — concurrent
        // updaters all converge on the max eventually.
        let mut peak = self.inner.peak_in_flight.load(Ordering::Relaxed);
        while new_in_flight > peak {
            match self.inner.peak_in_flight.compare_exchange_weak(
                peak,
                new_in_flight,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => peak = observed,
            }
        }
        if self.inner.autoscale_headroom > 0 {
            // Only grow when peak truly exceeds current cap. Reading
            // capacity under its own mutex keeps this consistent with
            // checkin's eviction decision.
            let mut cap_guard = self.inner.capacity.lock().unwrap();
            if new_in_flight > *cap_guard && *cap_guard < self.inner.max_capacity {
                let target = (*cap_guard + self.inner.autoscale_headroom)
                    .min(self.inner.max_capacity);
                if target > *cap_guard {
                    *cap_guard = target;
                    self.inner.grows.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        // ── Free-list scan ────────────────────────────────────────
        let mut guard = self.inner.free.lock().unwrap();
        if let Some(idx) = guard.iter().rposition(|v| v.capacity() >= min_cap) {
            let buf = guard.swap_remove(idx);
            drop(guard);
            self.inner.reuses.fetch_add(1, Ordering::Relaxed);
            return buf;
        }
        drop(guard);
        // None big enough — allocate fresh. We *don't* try to grow a
        // pooled buffer in-place because the realloc would itself
        // page-fault, and we'd lose the buffer's previous capacity.
        self.inner.allocs.fetch_add(1, Ordering::Relaxed);
        Vec::with_capacity(min_cap)
    }

    /// Return a buffer to the pool. Capacity is preserved (we only
    /// `clear()`, not drop). When the pool is full, the smallest
    /// existing buffer is evicted to make room — i.e. we keep the N
    /// largest buffers we've seen.
    fn checkin(&self, mut buf: Vec<u8>) {
        // In-flight decrement happens regardless of whether the
        // buffer ends up parked or dropped — once it's in this
        // method, it's no longer "checked out."
        self.inner.in_flight.fetch_sub(1, Ordering::Relaxed);

        buf.clear();
        let cap = *self.inner.capacity.lock().unwrap();
        let mut guard = self.inner.free.lock().unwrap();
        if guard.len() < cap {
            guard.push(buf);
            return;
        }
        // Pool is at capacity. Find the smallest buffer; if ours is
        // bigger, evict it. Otherwise drop ours.
        let (min_idx, min_cap) = guard
            .iter()
            .enumerate()
            .map(|(i, v)| (i, v.capacity()))
            .min_by_key(|&(_, c)| c)
            .expect("pool capacity > 0 implies guard non-empty here");
        if min_cap < buf.capacity() {
            guard.swap_remove(min_idx);
            guard.push(buf);
        } else {
            // Drop `buf` — pool already holds bigger buffers.
            drop(guard);
            self.inner.misses.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Default for BufPool {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for BufPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let free_count = self.inner.free.lock().unwrap().len();
        let cap = *self.inner.capacity.lock().unwrap();
        f.debug_struct("BufPool")
            .field("free_count", &free_count)
            .field("capacity", &cap)
            .field("max_capacity", &self.inner.max_capacity)
            .field("in_flight", &self.in_flight())
            .field("peak_in_flight", &self.peak_in_flight())
            .finish()
    }
}

/// RAII handle for a checked-out pool buffer.
///
/// On Drop returns the `Vec<u8>` to the pool (capacity preserved).
/// Implements `AsRef<[u8]>` so it can back a [`bytes::Bytes`] via
/// `Bytes::from_owner(pooled_buf)` — `Bytes` will keep `PooledBuf`
/// alive until its last clone drops, at which point the buffer
/// returns to the pool automatically.
pub struct PooledBuf {
    // `Option` lets us `take()` the Vec out in Drop without `unsafe`
    // (workspace forbids `unsafe_code`). Niche optimisation on Vec's
    // non-null pointer means `Option<Vec<u8>>` is the same size as
    // `Vec<u8>`, so no runtime overhead.
    //
    // Invariant: `buf` is `Some` for the entire lifetime of the
    // value except inside `Drop::drop`. Every accessor below assumes
    // this — `unreachable_unchecked`-free, just unwrap.
    buf: Option<Vec<u8>>,
    pool: BufPool,
}

impl PooledBuf {
    /// Check out a buffer from `pool` sized to fit at least `min_cap`
    /// bytes. The buffer's length starts at 0; use the inner Vec to
    /// fill it (via [`PooledBuf::extend_from_slice`] or
    /// [`PooledBuf::as_mut_vec`]).
    pub fn checkout(pool: &BufPool, min_cap: usize) -> Self {
        Self {
            buf: Some(pool.checkout(min_cap)),
            pool: pool.clone(),
        }
    }

    /// Append bytes to the buffer. Mirrors `Vec::extend_from_slice`.
    pub fn extend_from_slice(&mut self, slice: &[u8]) {
        self.buf
            .as_mut()
            .expect("buf is Some until Drop")
            .extend_from_slice(slice);
    }

    /// Mutable access to the inner Vec, for cases the simple
    /// `extend_from_slice` API doesn't cover.
    pub fn as_mut_vec(&mut self) -> &mut Vec<u8> {
        self.buf.as_mut().expect("buf is Some until Drop")
    }

    /// Number of bytes currently written to the buffer.
    pub fn len(&self) -> usize {
        self.buf.as_ref().expect("buf is Some until Drop").len()
    }

    /// True when no bytes have been written.
    pub fn is_empty(&self) -> bool {
        self.buf.as_ref().expect("buf is Some until Drop").is_empty()
    }

    /// Capacity of the inner buffer. Useful for tests.
    #[cfg(test)]
    pub fn capacity(&self) -> usize {
        self.buf.as_ref().expect("buf is Some until Drop").capacity()
    }

    /// Stable pointer to the underlying allocation. Used in tests to
    /// assert pool reuse hit the same allocation. Not part of the
    /// public stable API.
    #[cfg(test)]
    pub fn as_ptr(&self) -> *const u8 {
        self.buf.as_ref().expect("buf is Some until Drop").as_ptr()
    }
}

impl AsRef<[u8]> for PooledBuf {
    fn as_ref(&self) -> &[u8] {
        self.buf.as_ref().expect("buf is Some until Drop")
    }
}

impl Drop for PooledBuf {
    fn drop(&mut self) {
        if let Some(buf) = self.buf.take() {
            self.pool.checkin(buf);
        }
    }
}

impl std::fmt::Debug for PooledBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let buf = self.buf.as_ref();
        f.debug_struct("PooledBuf")
            .field("len", &buf.map(|v| v.len()).unwrap_or(0))
            .field("cap", &buf.map(|v| v.capacity()).unwrap_or(0))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkout_allocates_when_pool_empty() {
        let pool = BufPool::new();
        assert_eq!(pool.free_count(), 0);
        let buf = PooledBuf::checkout(&pool, 1024);
        assert!(buf.capacity() >= 1024);
        // Pool still empty until we drop.
        assert_eq!(pool.free_count(), 0);
        drop(buf);
        assert_eq!(pool.free_count(), 1, "Drop returns buffer to pool");
    }

    #[test]
    fn checkout_reuses_returned_buffer() {
        let pool = BufPool::new();
        let mut buf = PooledBuf::checkout(&pool, 4096);
        let original_capacity = buf.capacity();
        let original_ptr = buf.as_ptr();
        buf.extend_from_slice(&[1, 2, 3, 4]);
        drop(buf);

        let buf2 = PooledBuf::checkout(&pool, 4096);
        assert_eq!(
            buf2.capacity(),
            original_capacity,
            "reused buffer preserves capacity"
        );
        assert_eq!(
            buf2.as_ptr(),
            original_ptr,
            "reused buffer is the same allocation"
        );
        assert_eq!(buf2.len(), 0, "reused buffer is cleared");
    }

    #[test]
    fn checkout_skips_buffer_too_small() {
        let pool = BufPool::new();
        // Park a tiny one and a large one.
        drop(PooledBuf::checkout(&pool, 128));
        drop(PooledBuf::checkout(&pool, 16 * 1024));
        assert_eq!(pool.free_count(), 2);

        // Request bigger than the small one — must get the big one
        // (or allocate fresh, but we know the big one fits).
        let buf = PooledBuf::checkout(&pool, 8 * 1024);
        assert!(buf.capacity() >= 8 * 1024);
        // Pool should still hold the tiny one.
        assert_eq!(pool.free_count(), 1);
    }

    #[test]
    fn pool_evicts_smallest_when_full() {
        let pool = BufPool::with_capacity(3);
        // Fill the pool with three increasing-sized buffers.
        for cap in [1024, 2048, 4096] {
            drop(PooledBuf::checkout(&pool, cap));
        }
        assert_eq!(pool.free_count(), 3);

        // Return a bigger buffer — smallest (1024) should be evicted.
        drop(PooledBuf::checkout(&pool, 8192));
        assert_eq!(pool.free_count(), 3);

        // Now the pool contains buffers of cap 2048, 4096, 8192.
        // Request 4096 — should reuse one with cap ≥ 4096.
        let buf = PooledBuf::checkout(&pool, 4096);
        assert!(buf.capacity() >= 4096);
    }

    #[test]
    fn pool_does_not_grow_past_capacity() {
        let pool = BufPool::with_capacity(2);
        // Three same-sized buffers in flight, then dropped in order.
        let bufs: Vec<_> = (0..3)
            .map(|_| PooledBuf::checkout(&pool, 1024))
            .collect();
        assert_eq!(pool.free_count(), 0);
        drop(bufs);
        assert_eq!(pool.free_count(), 2, "third drop dropped to ground");
    }

    #[test]
    fn extend_then_as_ref_returns_only_written_bytes() {
        let pool = BufPool::new();
        let mut buf = PooledBuf::checkout(&pool, 1024);
        buf.extend_from_slice(b"hello");
        assert_eq!(buf.as_ref(), b"hello");
        assert_eq!(buf.len(), 5);
    }

    #[test]
    fn pool_is_clonable_and_shared() {
        let pool = BufPool::new();
        let pool2 = pool.clone();
        drop(PooledBuf::checkout(&pool, 1024));
        assert_eq!(pool2.free_count(), 1, "clones share state");
    }

    #[test]
    fn warmup_pre_populates_pool() {
        let pool = BufPool::with_config(BufPoolConfig {
            initial_capacity: 4,
            max_capacity: 4,
            warmup_count: 4,
            warmup_size: 64 * 1024,
            autoscale_headroom: 0,
        });
        assert_eq!(pool.free_count(), 4, "all warmup buffers parked");
        // Every warmup buffer has the requested capacity.
        let buf = PooledBuf::checkout(&pool, 64 * 1024);
        assert!(buf.capacity() >= 64 * 1024);
        // No alloc happened — it came from the warmed-up pool.
        assert_eq!(pool.allocs(), 0, "warm checkout did not allocate");
        assert_eq!(pool.reuses(), 1);
    }

    #[test]
    fn warmup_overshoot_raises_capacity() {
        // Warmup count > initial_capacity: we must not drop the
        // buffers we just paid to fault. Capacity rises to fit.
        let pool = BufPool::with_config(BufPoolConfig {
            initial_capacity: 2,
            max_capacity: 8,
            warmup_count: 5,
            warmup_size: 1024,
            autoscale_headroom: 0,
        });
        assert_eq!(pool.free_count(), 5);
        assert_eq!(pool.capacity(), 5, "capacity raised to fit warmup");
    }

    #[test]
    fn warmup_capped_at_max_capacity() {
        // warmup_count exceeds max_capacity — capacity caps at max,
        // and the extras still get parked (we don't refuse to warm).
        // The cap takes effect on the *next* checkin instead.
        let pool = BufPool::with_config(BufPoolConfig {
            initial_capacity: 2,
            max_capacity: 3,
            warmup_count: 5,
            warmup_size: 1024,
            autoscale_headroom: 0,
        });
        // All five buffers are parked (we already paid to fault them).
        assert_eq!(pool.free_count(), 5);
        // Capacity is clamped to max_capacity, NOT raised past it.
        assert_eq!(pool.capacity(), 3);
    }

    #[test]
    fn autoscale_grows_capacity_under_burst() {
        let pool = BufPool::with_config(BufPoolConfig {
            initial_capacity: 2,
            max_capacity: 16,
            warmup_count: 0,
            warmup_size: 0,
            autoscale_headroom: 4,
        });
        assert_eq!(pool.capacity(), 2);
        // Hold 5 buffers in flight — exceeds initial cap of 2. The
        // 3rd checkout (in_flight=3 > cap=2) triggers a grow to
        // min(2+4, 16) = 6. Subsequent checkouts won't re-trigger
        // because they fit under the new cap.
        let bufs: Vec<_> = (0..5).map(|_| PooledBuf::checkout(&pool, 1024)).collect();
        assert_eq!(pool.in_flight(), 5);
        assert_eq!(pool.peak_in_flight(), 5);
        assert!(pool.capacity() >= 5, "capacity grew to fit burst");
        assert!(pool.grows() >= 1);
        // All 5 return to the pool now; none should be dropped.
        drop(bufs);
        assert_eq!(pool.free_count(), 5);
        assert_eq!(pool.misses(), 0, "no checkin drops after autoscale");
        assert_eq!(pool.in_flight(), 0);
    }

    #[test]
    fn autoscale_respects_max_capacity() {
        let pool = BufPool::with_config(BufPoolConfig {
            initial_capacity: 2,
            max_capacity: 4,
            warmup_count: 0,
            warmup_size: 0,
            autoscale_headroom: 100,
        });
        // 6 in flight; cap should grow only up to max=4.
        let bufs: Vec<_> = (0..6).map(|_| PooledBuf::checkout(&pool, 1024)).collect();
        assert_eq!(pool.capacity(), 4);
        drop(bufs);
        assert_eq!(pool.free_count(), 4);
        assert_eq!(
            pool.misses(),
            2,
            "buffers past max_capacity get dropped on checkin"
        );
    }

    #[test]
    fn autoscale_disabled_when_headroom_zero() {
        let pool = BufPool::with_config(BufPoolConfig {
            initial_capacity: 2,
            max_capacity: 16,
            warmup_count: 0,
            warmup_size: 0,
            autoscale_headroom: 0,
        });
        let bufs: Vec<_> = (0..5).map(|_| PooledBuf::checkout(&pool, 1024)).collect();
        assert_eq!(pool.capacity(), 2, "cap unchanged with headroom=0");
        assert_eq!(pool.grows(), 0);
        drop(bufs);
        // Only 2 parked, the rest dropped.
        assert_eq!(pool.free_count(), 2);
        assert_eq!(pool.misses(), 3);
    }

    #[test]
    fn in_flight_and_peak_track_correctly() {
        let pool = BufPool::new();
        assert_eq!(pool.in_flight(), 0);
        let a = PooledBuf::checkout(&pool, 1024);
        let b = PooledBuf::checkout(&pool, 1024);
        let c = PooledBuf::checkout(&pool, 1024);
        assert_eq!(pool.in_flight(), 3);
        assert_eq!(pool.peak_in_flight(), 3);
        drop(c);
        drop(b);
        assert_eq!(pool.in_flight(), 1);
        // Peak is sticky.
        assert_eq!(pool.peak_in_flight(), 3);
        drop(a);
        assert_eq!(pool.in_flight(), 0);
        assert_eq!(pool.peak_in_flight(), 3);
    }
}
