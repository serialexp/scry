//! `EvictOnNotFound` — an [`ObjectStore`] decorator that turns a peer's
//! deletion into a recoverable, observable event instead of a hard query
//! failure.
//!
//! In a multi-instance deployment (v0.9), one instance can hard-delete a block
//! (compaction reaping a superseded input, retention reaping an expired block)
//! that a peer still has a catalog row for — its convergence simply hasn't
//! caught up yet. When that peer plans a query, it lists the now-dead block and
//! hands it to DataFusion, whose scan then fails with `NotFound` on the
//! `GET`.
//!
//! This wrapper intercepts exactly that case: on a `NotFound` from a read, it
//! parses the block UUID out of the object path and records it in a shared set,
//! then returns the error unchanged (so the in-flight plan still fails fast).
//! The query driver inspects [`EvictOnNotFound::take_evicted`] afterwards: if
//! anything was recorded it `delete_blocks`-es those stale rows from the local
//! catalog and re-plans **once**. The bucket is the source of truth, so
//! dropping a row we just proved is gone is always safe; the convergence
//! consumer / poller would have removed it anyway, this just makes the racing
//! query self-heal instead of erroring.
//!
//! Only reads are intercepted (`get_opts` / `get_ranges` — the two entry points
//! DataFusion's parquet reader uses). Writes/lists/copies pass straight
//! through; a query path never issues them.

use std::collections::HashSet;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    path::Path, CopyOptions, Error as OsError, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result as OsResult,
};
use uuid::Uuid;

/// Wraps an object store, recording the UUIDs of blocks that read back
/// `NotFound` so the query driver can evict their stale catalog rows and
/// re-plan. Cheap to clone (everything is behind `Arc`).
#[derive(Clone)]
pub struct EvictOnNotFound {
    inner: Arc<dyn ObjectStore>,
    evicted: Arc<Mutex<HashSet<Uuid>>>,
}

impl EvictOnNotFound {
    /// Wrap `inner`. Start with an empty eviction set.
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            evicted: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Drain and return the set of block UUIDs that have read back `NotFound`
    /// since the last drain. The driver calls this after a query attempt: a
    /// non-empty result means "these rows are stale; delete them and re-plan".
    pub fn take_evicted(&self) -> Vec<Uuid> {
        let mut g = self.evicted.lock().expect("eviction set poisoned");
        g.drain().collect()
    }

    /// Whether anything has been evicted (without draining). Cheap predicate
    /// for the driver's "should I re-plan?" check.
    pub fn has_evictions(&self) -> bool {
        !self.evicted.lock().expect("eviction set poisoned").is_empty()
    }

    /// If `result` is `NotFound`, parse the block UUID from `location` and
    /// record it. The error is left untouched for the caller to propagate.
    fn note_if_missing<T>(&self, location: &Path, result: &OsResult<T>) {
        if let Err(OsError::NotFound { .. }) = result {
            if let Some(uuid) = block_uuid_from_path(location) {
                self.evicted
                    .lock()
                    .expect("eviction set poisoned")
                    .insert(uuid);
                tracing::debug!(%uuid, location = %location, "block object 404'd; queued for catalog eviction");
            } else {
                tracing::warn!(location = %location, "404 on a path with no parseable block UUID; cannot evict");
            }
        }
    }
}

/// Parse the block UUID from an object path. Block objects are keyed
/// `<signal>/<yyyy>/<mm>/<dd>/<writer_id>/<block_uuid>.<ext>` (parquet,
/// meta.json, postings, body.bloom), so the UUID is the filename up to its
/// first `.`. Returns `None` if the last segment isn't a UUID (e.g. an
/// unexpected path), so the caller can log rather than evict the wrong row.
fn block_uuid_from_path(location: &Path) -> Option<Uuid> {
    let filename = location.filename()?;
    let stem = filename.split('.').next()?;
    Uuid::parse_str(stem).ok()
}

impl std::fmt::Display for EvictOnNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EvictOnNotFound({})", self.inner)
    }
}

impl std::fmt::Debug for EvictOnNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EvictOnNotFound")
            .field("inner", &format_args!("{}", self.inner))
            .finish()
    }
}

#[async_trait]
impl ObjectStore for EvictOnNotFound {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, opts: GetOptions) -> OsResult<GetResult> {
        let result = self.inner.get_opts(location, opts).await;
        self.note_if_missing(location, &result);
        result
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OsResult<Vec<Bytes>> {
        let result = self.inner.get_ranges(location, ranges).await;
        self.note_if_missing(location, &result);
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<Path>>,
    ) -> BoxStream<'static, OsResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> OsResult<()> {
        self.inner.copy_opts(from, to, opts).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, opts: RenameOptions) -> OsResult<()> {
        self.inner.rename_opts(from, to, opts).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{memory::InMemory, ObjectStoreExt};

    #[test]
    fn parses_block_uuid_from_canonical_paths() {
        let uuid = Uuid::now_v7();
        let writer = Uuid::now_v7();
        for ext in ["parquet", "meta.json", "postings", "body.bloom"] {
            let p = Path::from(format!("metrics/2026/05/31/{writer}/{uuid}.{ext}"));
            assert_eq!(block_uuid_from_path(&p), Some(uuid), "ext={ext}");
        }
    }

    #[test]
    fn non_uuid_filename_yields_none() {
        let p = Path::from("metrics/2026/05/31/writer/not-a-uuid.parquet");
        assert_eq!(block_uuid_from_path(&p), None);
    }

    #[tokio::test]
    async fn records_uuid_on_notfound_get() {
        let store = EvictOnNotFound::new(Arc::new(InMemory::new()));
        let uuid = Uuid::now_v7();
        let missing = Path::from(format!("metrics/2026/05/31/{}/{uuid}.parquet", Uuid::now_v7()));

        assert!(!store.has_evictions());
        let r = store.get_opts(&missing, GetOptions::default()).await;
        assert!(matches!(r, Err(OsError::NotFound { .. })));
        assert!(store.has_evictions());
        assert_eq!(store.take_evicted(), vec![uuid]);
        // Draining clears it.
        assert!(!store.has_evictions());
    }

    #[tokio::test]
    async fn present_object_does_not_record() {
        let inner = InMemory::new();
        let writer = Uuid::now_v7();
        let uuid = Uuid::now_v7();
        let p = Path::from(format!("metrics/2026/05/31/{writer}/{uuid}.parquet"));
        inner.put(&p, PutPayload::from_static(b"hi")).await.unwrap();

        let store = EvictOnNotFound::new(Arc::new(inner));
        let got = store.get_opts(&p, GetOptions::default()).await;
        assert!(got.is_ok());
        assert!(!store.has_evictions());
    }
}
