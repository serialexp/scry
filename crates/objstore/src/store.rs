//! [`PooledStore`]: an `ObjectStore` newtype that drains response
//! bodies into reusable pool buffers.
//!
//! The trait surface delegates to the inner store for everything
//! except the *read* path. We override:
//!
//! - [`PooledStore::get_ranges`] — re-implements `coalesce_ranges`
//!   with a pool-aware per-range fetch (instead of letting the
//!   default impl call `self.get_range` per merged range, which
//!   eventually hits `collect_bytes` → fresh `Vec<u8>`).
//! - `get_opts` is left as a delegate. We can't pool through
//!   `get_opts` cleanly because its `GetResult` exposes a raw stream
//!   that the caller may consume in arbitrary ways; the pool wants
//!   to own the drain.
//!
//! `ObjectStoreExt::get_range` lives on a separate extension trait
//! (auto-implemented for any `ObjectStore`), so we can't override it
//! through trait dispatch. That's fine for our purposes — the
//! parquet + DataFusion hot path goes through `get_ranges`, which is
//! on the base trait and *is* overridden here.
//!
//! ## Single-chunk fast path
//!
//! When the inner store returns a body in exactly one chunk (e.g.
//! `InMemory` for tests, or any cached GET), we skip the pool
//! entirely and pass that `Bytes` through. Copying a `Bytes` into a
//! pool buffer just to copy back out would be pure waste.
//!
//! ## Why we re-implement coalescing
//!
//! `object_store`'s default `get_ranges` calls `coalesce_ranges` with
//! `|range| self.get_range(location, range)` — and `get_range`'s
//! default impl is `get_opts(...).bytes().await`, which is where
//! `collect_bytes` allocates the fresh `Vec`. By overriding both
//! `get_ranges` *and* `get_range` to route through our pool-aware
//! fetch, every read path lands in the same place.

use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, TryStreamExt};
use object_store::{
    coalesce_ranges, path::Path, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, Result, OBJECT_STORE_COALESCE_DEFAULT,
};

use crate::pool::{BufPool, PooledBuf};

/// Wraps an `Arc<dyn ObjectStore>` and routes range fetches through
/// a [`BufPool`] so per-fetch buffers get reused across the lifetime
/// of the process.
///
/// One pool per `PooledStore` instance. If you build several
/// `PooledStore`s (e.g. one per bucket) and want them to share a
/// pool, construct them via [`PooledStore::with_pool`] passing the
/// same `BufPool` clone.
pub struct PooledStore {
    inner: Arc<dyn ObjectStore>,
    pool: BufPool,
}

impl PooledStore {
    /// Wrap `inner` with a fresh default-sized buffer pool.
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            pool: BufPool::new(),
        }
    }

    /// Wrap `inner` with an externally-managed `pool`. Use this when
    /// you want several `PooledStore`s to share buffer memory.
    pub fn with_pool(inner: Arc<dyn ObjectStore>, pool: BufPool) -> Self {
        Self { inner, pool }
    }

    /// Borrow the underlying pool — exposed for tests and metrics.
    pub fn pool(&self) -> &BufPool {
        &self.pool
    }

    /// Borrow the wrapped store, for direct calls that should bypass
    /// the pooling path (rarely needed; the trait impl already
    /// delegates everything except the read path).
    pub fn inner(&self) -> &Arc<dyn ObjectStore> {
        &self.inner
    }

    /// Single-range pool-aware fetch. Issues `get_opts` with the
    /// range, drains the result stream into a pooled buffer, returns
    /// a `Bytes` whose lifetime keeps the pool buffer checked out
    /// until the last clone drops.
    async fn fetch_pooled(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        let len = (range.end - range.start) as usize;
        let opts = GetOptions {
            range: Some(range.into()),
            ..Default::default()
        };
        let result = self.inner.get_opts(location, opts).await?;
        let mut stream = result.into_stream();

        // Single-chunk fast path: zero-copy. Matches what
        // `object_store::util::collect_bytes` does.
        let first = stream.try_next().await?.unwrap_or_default();
        let Some(second) = stream.try_next().await? else {
            return Ok(first);
        };

        // Multi-chunk: drain into a pool-owned Vec sized for the
        // requested range (the size is exact when the server honours
        // the range, which S3/Garage do).
        let mut buf = PooledBuf::checkout(&self.pool, len);
        buf.extend_from_slice(&first);
        buf.extend_from_slice(&second);
        while let Some(chunk) = stream.try_next().await? {
            buf.extend_from_slice(&chunk);
        }

        // `Bytes::from_owner` keeps the PooledBuf alive until the
        // last Bytes clone drops; PooledBuf::Drop then returns the
        // Vec to the pool with capacity preserved.
        Ok(Bytes::from_owner(buf))
    }
}

impl std::fmt::Debug for PooledStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledStore")
            .field("pool", &self.pool)
            .finish_non_exhaustive()
    }
}

// `Display` is required by ObjectStore for error reporting.
impl std::fmt::Display for PooledStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PooledStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for PooledStore {
    // ── Writes / metadata: pure passthrough ───────────────────────

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        // `get_opts` returns a streaming GetResult — pooling here
        // would mean owning the drain, which `get_opts` doesn't.
        // The pool path runs inside our `get_range` / `get_ranges`
        // overrides instead.
        self.inner.get_opts(location, options).await
    }

    // ── Reads: pool-aware ─────────────────────────────────────────

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        // We could just delegate to the default `get_ranges`, which
        // would call our pool-aware `get_range` per merged range —
        // but the default's `coalesce_ranges` slices the merged
        // `Bytes` back out for each caller-requested sub-range, and
        // each slice keeps the whole `Bytes` (and therefore the
        // PooledBuf) alive until *every* slice is dropped. That's
        // exactly the lifetime we want, so re-implementing here is
        // just for clarity + a single call site to mine in profiles.
        coalesce_ranges(
            ranges,
            |range| self.fetch_pooled(location, range),
            OBJECT_STORE_COALESCE_DEFAULT,
        )
        .await
    }

    // ── List / delete / copy / rename: passthrough ────────────────

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use object_store::{memory::InMemory, ObjectStoreExt, PutPayload};

    /// Smoke test: PooledStore wrapping InMemory satisfies the
    /// ObjectStore contract end-to-end (put → get_ranges → contents).
    /// The single-chunk fast path is exercised here since InMemory
    /// returns its bytes in one shot.
    #[tokio::test]
    async fn passes_through_inmemory() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = PooledStore::new(inner);
        let path = Path::from("k");

        store
            .put(&path, PutPayload::from_static(b"hello world"))
            .await
            .unwrap();

        // Use the trait method directly (`get_ranges`) since that's
        // what we override; `ObjectStoreExt::get_range` is the
        // delegate path and isn't what we're testing.
        let got = store.get_ranges(&path, &[0..5]).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].as_ref(), b"hello");

        let got_full = store.get_ranges(&path, &[0..11]).await.unwrap();
        assert_eq!(got_full[0].as_ref(), b"hello world");
    }

    /// When the body comes back in one chunk we must NOT pull a
    /// buffer from the pool — that would be a wasted copy.
    #[tokio::test]
    async fn single_chunk_fetches_skip_pool() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = PooledStore::new(inner);
        store
            .put(&Path::from("k"), PutPayload::from_static(b"single"))
            .await
            .unwrap();

        let pool_before = store.pool().free_count();
        let _b = store.get_ranges(&Path::from("k"), &[0..6]).await.unwrap();
        let pool_after = store.pool().free_count();

        assert_eq!(
            pool_before, pool_after,
            "single-chunk path must not touch the pool"
        );
    }

    /// Multi-chunk drain: feed a custom inner store that splits
    /// responses into N chunks, verify the resulting Bytes is correct
    /// AND that dropping it returns a buffer to the pool.
    #[tokio::test]
    async fn multi_chunk_drains_into_pool() {
        let inner = Arc::new(ChunkingStore::new(8)) as Arc<dyn ObjectStore>;
        let store = PooledStore::new(inner);
        let path = Path::from("k");
        store
            .put(&path, PutPayload::from_static(b"abcdefghijklmnop"))
            .await
            .unwrap();

        assert_eq!(store.pool().free_count(), 0);
        let got = store.get_ranges(&path, &[0..16]).await.unwrap();
        assert_eq!(got[0].as_ref(), b"abcdefghijklmnop");
        // Bytes is still alive → pool buffer is checked out.
        assert_eq!(store.pool().free_count(), 0);
        drop(got);
        assert_eq!(
            store.pool().free_count(),
            1,
            "dropping the last Bytes ref returns the buffer to the pool"
        );
    }

    /// Repeated multi-chunk fetches must reuse the *same* underlying
    /// allocation — that's the whole point of the pool.
    #[tokio::test]
    async fn repeated_multi_chunk_fetches_reuse_allocation() {
        let inner = Arc::new(ChunkingStore::new(4)) as Arc<dyn ObjectStore>;
        let store = PooledStore::new(inner);
        let path = Path::from("k");
        store
            .put(&path, PutPayload::from_static(b"12345678"))
            .await
            .unwrap();

        // First fetch: drains into a fresh allocation, drops back.
        let first = store.get_ranges(&path, &[0..8]).await.unwrap();
        let first_ptr = first[0].as_ptr();
        drop(first);
        assert_eq!(store.pool().free_count(), 1);

        // Second fetch must reuse it.
        let second = store.get_ranges(&path, &[0..8]).await.unwrap();
        assert_eq!(
            second[0].as_ptr(),
            first_ptr,
            "second fetch reuses the first's buffer"
        );
    }

    // ── ChunkingStore: helper that forces multi-chunk responses ────
    //
    // InMemory always returns a single chunk, which is great for the
    // fast-path test but useless for exercising the drain. This
    // wrapper splits get responses into fixed-size chunks so we can
    // assert the pool path end-to-end.

    use futures::{stream, StreamExt};
    use object_store::GetResultPayload;

    struct ChunkingStore {
        inner: InMemory,
        chunk_size: usize,
    }

    impl ChunkingStore {
        fn new(chunk_size: usize) -> Self {
            Self {
                inner: InMemory::new(),
                chunk_size,
            }
        }
    }

    impl std::fmt::Display for ChunkingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ChunkingStore({})", self.chunk_size)
        }
    }

    impl std::fmt::Debug for ChunkingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ChunkingStore")
                .field("chunk_size", &self.chunk_size)
                .finish()
        }
    }

    #[async_trait]
    impl ObjectStore for ChunkingStore {
        async fn put_opts(
            &self,
            l: &Path,
            p: PutPayload,
            o: PutOptions,
        ) -> Result<PutResult> {
            self.inner.put_opts(l, p, o).await
        }
        async fn put_multipart_opts(
            &self,
            l: &Path,
            o: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(l, o).await
        }
        async fn get_opts(&self, l: &Path, o: GetOptions) -> Result<GetResult> {
            let mut r = self.inner.get_opts(l, o).await?;
            // Replace the payload stream with a chunked version.
            let bytes = std::mem::replace(
                &mut r.payload,
                GetResultPayload::Stream(stream::empty().boxed()),
            );
            let body = match bytes {
                GetResultPayload::Stream(s) => {
                    use futures::TryStreamExt;
                    let collected: Vec<Bytes> = s.try_collect().await?;
                    let mut joined = Vec::new();
                    for b in collected {
                        joined.extend_from_slice(&b);
                    }
                    joined
                }
                #[allow(unreachable_patterns)]
                _ => panic!("unexpected payload kind in test (InMemory always yields Stream)"),
            };
            let chunks: Vec<Result<Bytes>> = body
                .chunks(self.chunk_size)
                .map(|c| Ok(Bytes::copy_from_slice(c)))
                .collect();
            r.payload = GetResultPayload::Stream(stream::iter(chunks).boxed());
            Ok(r)
        }
        fn delete_stream(
            &self,
            l: BoxStream<'static, Result<Path>>,
        ) -> BoxStream<'static, Result<Path>> {
            self.inner.delete_stream(l)
        }
        fn list(&self, p: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list(p)
        }
        fn list_with_offset(
            &self,
            p: Option<&Path>,
            o: &Path,
        ) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list_with_offset(p, o)
        }
        async fn list_with_delimiter(&self, p: Option<&Path>) -> Result<ListResult> {
            self.inner.list_with_delimiter(p).await
        }
        async fn copy_opts(&self, f: &Path, t: &Path, o: CopyOptions) -> Result<()> {
            self.inner.copy_opts(f, t, o).await
        }
        async fn rename_opts(&self, f: &Path, t: &Path, o: RenameOptions) -> Result<()> {
            self.inner.rename_opts(f, t, o).await
        }
    }
}
