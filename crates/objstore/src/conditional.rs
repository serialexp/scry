//! Conditional object writes and a runtime capability probe.
//!
//! `object_store` exposes conditional writes through [`PutMode`].  These
//! helpers keep callers from accidentally falling back to the default
//! overwrite mode, and [`probe_conditional_writes`] verifies that a backend
//! actually enforces (rather than merely accepts) both preconditions.

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use object_store::{
    path::Path, Error, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, PutResult,
    UpdateVersion,
};

/// Options for an atomic create-if-absent write.
pub fn create_options() -> PutOptions {
    PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    }
}

/// Options for an atomic compare-and-swap write.
pub fn update_options(version: UpdateVersion) -> PutOptions {
    PutOptions {
        mode: PutMode::Update(version),
        ..Default::default()
    }
}

/// Atomically create `location`, failing with [`Error::AlreadyExists`] if it
/// already exists.
pub async fn put_create(
    store: &dyn ObjectStore,
    location: &Path,
    payload: PutPayload,
) -> object_store::Result<PutResult> {
    store.put_opts(location, payload, create_options()).await
}

/// Atomically replace `location` only if `version` is still current.
pub async fn put_update(
    store: &dyn ObjectStore,
    location: &Path,
    payload: PutPayload,
    version: UpdateVersion,
) -> object_store::Result<PutResult> {
    store
        .put_opts(location, payload, update_options(version))
        .await
}

/// Verify atomic conditional-create and versioned-update support.
///
/// `prefix` is a disposable probe namespace. The function uses four children
/// below it and attempts to delete all four before returning, including after
/// a failed check. Callers should nevertheless use a fresh prefix: cleanup can
/// itself fail because of credentials or transport errors.
///
/// The probe verifies all of the properties needed by control-plane writers:
///
/// * a second create fails and leaves the first value intact;
/// * an update with a stale version fails and leaves the current value intact;
/// * an update with the current version succeeds;
/// * repeated concurrent creates have exactly one winner; and
/// * two updates racing on one current version have exactly one winner.
///
/// A successful probe also requires successful cleanup. If a semantic check
/// fails, that original failure is returned after cleanup has been attempted.
pub async fn probe_conditional_writes(store: &dyn ObjectStore, prefix: &Path) -> Result<()> {
    let create_path = prefix.clone().join("create");
    let update_path = prefix.clone().join("update");
    let race_path = prefix.clone().join("race");
    let update_race_path = prefix.clone().join("update-race");

    let check = async {
        let first = Bytes::from_static(b"conditional-probe:first");
        let second = Bytes::from_static(b"conditional-probe:second");
        put_create(store, &create_path, first.clone().into())
            .await
            .context("initial conditional create failed")?;
        let err = match put_create(store, &create_path, second.into()).await {
            Ok(_) => bail!("backend accepted a second PutMode::Create"),
            Err(error) => error,
        };
        if !matches!(err, Error::AlreadyExists { .. }) {
            bail!("second PutMode::Create returned the wrong error: {err}");
        }
        let retained = store
            .get(&create_path)
            .await
            .context("read after rejected create failed")?
            .bytes()
            .await
            .context("collect after rejected create failed")?;
        if retained != first {
            bail!("rejected PutMode::Create changed the stored object");
        }

        // Create v1, then use an unconditional write solely to manufacture a
        // newer version. This lets us test a stale update before the successful
        // current-version update.
        let v1 = put_create(
            store,
            &update_path,
            Bytes::from_static(b"conditional-probe:v1").into(),
        )
        .await
        .context("conditional update setup create failed")?;
        let current = Bytes::from_static(b"conditional-probe:current");
        let v2 = store
            .put(&update_path, current.clone().into())
            .await
            .context("conditional update setup overwrite failed")?;
        let stale_err = match put_update(
            store,
            &update_path,
            Bytes::from_static(b"conditional-probe:stale").into(),
            v1.into(),
        )
        .await
        {
            Ok(_) => bail!("backend accepted a stale PutMode::Update"),
            Err(error) => error,
        };
        if !matches!(stale_err, Error::Precondition { .. }) {
            bail!("stale PutMode::Update returned the wrong error: {stale_err}");
        }
        let retained = store
            .get(&update_path)
            .await
            .context("read after rejected update failed")?
            .bytes()
            .await
            .context("collect after rejected update failed")?;
        if retained != current {
            bail!("rejected PutMode::Update changed the stored object");
        }
        let updated = Bytes::from_static(b"conditional-probe:updated");
        put_update(store, &update_path, updated.clone().into(), v2.into())
            .await
            .context("PutMode::Update with the current version failed")?;
        let stored = store
            .get(&update_path)
            .await
            .context("read after current-version update failed")?
            .bytes()
            .await
            .context("collect after current-version update failed")?;
        if stored != updated {
            bail!("successful PutMode::Update did not store the new value");
        }

        let left = Bytes::from_static(b"conditional-probe:left");
        let right = Bytes::from_static(b"conditional-probe:right");
        for attempt in 0..16 {
            if attempt != 0 {
                store
                    .delete(&race_path)
                    .await
                    .context("reset concurrent-create probe failed")?;
            }
            let (left_result, right_result) = futures::join!(
                put_create(store, &race_path, left.clone().into()),
                put_create(store, &race_path, right.clone().into()),
            );
            let winner = match (left_result, right_result) {
                (Ok(_), Err(Error::AlreadyExists { .. })) => &left,
                (Err(Error::AlreadyExists { .. }), Ok(_)) => &right,
                (left_result, right_result) => bail!(
                    "concurrent PutMode::Create attempt {attempt} must have exactly one winner; left={left_result:?}, right={right_result:?}"
                ),
            };
            let stored = store
                .get(&race_path)
                .await
                .context("read after concurrent creates failed")?
                .bytes()
                .await
                .context("collect after concurrent creates failed")?;
            if stored != *winner {
                bail!("concurrent create winner does not match the stored value");
            }
        }

        let base = put_create(
            store,
            &update_race_path,
            Bytes::from_static(b"conditional-probe:update-race-base").into(),
        )
        .await
        .context("concurrent-update setup create failed")?;
        let version: UpdateVersion = base.into();
        let update_left = Bytes::from_static(b"conditional-probe:update-left");
        let update_right = Bytes::from_static(b"conditional-probe:update-right");
        let (left_result, right_result) = futures::join!(
            put_update(
                store,
                &update_race_path,
                update_left.clone().into(),
                version.clone(),
            ),
            put_update(
                store,
                &update_race_path,
                update_right.clone().into(),
                version,
            ),
        );
        let winner = match (left_result, right_result) {
            (Ok(_), Err(Error::Precondition { .. })) => update_left,
            (Err(Error::Precondition { .. }), Ok(_)) => update_right,
            (left_result, right_result) => bail!(
                "concurrent PutMode::Update must have exactly one winner; left={left_result:?}, right={right_result:?}"
            ),
        };
        let stored = store
            .get(&update_race_path)
            .await
            .context("read after concurrent updates failed")?
            .bytes()
            .await
            .context("collect after concurrent updates failed")?;
        if stored != winner {
            bail!("concurrent update winner does not match the stored value");
        }

        Ok(())
    }
    .await;

    // Never short-circuit cleanup: every path gets an attempted delete.
    let mut cleanup_error = None;
    for path in [&create_path, &update_path, &race_path, &update_race_path] {
        if let Err(error) = store.delete(path).await {
            if !matches!(error, Error::NotFound { .. }) && cleanup_error.is_none() {
                cleanup_error = Some(anyhow!(error).context(format!("cleanup of {path} failed")));
            }
        }
    }

    match check {
        Err(error) => Err(error),
        Ok(()) => cleanup_error.map_or(Ok(()), Err),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::{
        memory::InMemory, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload,
        ObjectMeta, PutMultipartOptions, RenameOptions,
    };

    use super::*;

    #[test]
    fn option_helpers_select_conditional_modes() {
        assert_eq!(create_options().mode, PutMode::Create);
        let version = UpdateVersion {
            e_tag: Some("etag".into()),
            version: Some("version".into()),
        };
        assert_eq!(
            update_options(version.clone()).mode,
            PutMode::Update(version)
        );
    }

    #[tokio::test]
    async fn helpers_enforce_create_and_update_with_in_memory() {
        let store = InMemory::new();
        let path = Path::from("conditional/helper");
        let created = put_create(&store, &path, Bytes::from_static(b"one").into())
            .await
            .unwrap();
        assert!(matches!(
            put_create(&store, &path, Bytes::from_static(b"two").into()).await,
            Err(Error::AlreadyExists { .. })
        ));
        put_update(
            &store,
            &path,
            Bytes::from_static(b"two").into(),
            created.into(),
        )
        .await
        .unwrap();
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap(),
            "two"
        );
    }

    #[tokio::test]
    async fn in_memory_passes_probe_and_is_cleaned_up() {
        let store = InMemory::new();
        let prefix = Path::from("conditional/probe");
        probe_conditional_writes(&store, &prefix).await.unwrap();
        for child in ["create", "update", "race", "update-race"] {
            assert!(matches!(
                store.get(&prefix.clone().join(child)).await,
                Err(Error::NotFound { .. })
            ));
        }
    }

    #[tokio::test]
    async fn pooled_store_preserves_conditional_put_options() {
        let store = crate::PooledStore::new(Arc::new(InMemory::new()));
        probe_conditional_writes(&store, &Path::from("conditional/pooled"))
            .await
            .unwrap();
    }

    /// Deliberately strips every write precondition, like a backend that
    /// accepts conditional headers but treats them as ordinary overwrites.
    #[derive(Debug)]
    struct IgnoresPreconditions {
        inner: InMemory,
        deleted: Arc<Mutex<Vec<Path>>>,
    }

    impl std::fmt::Display for IgnoresPreconditions {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("IgnoresPreconditions")
        }
    }

    #[async_trait]
    impl ObjectStore for IgnoresPreconditions {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            mut options: PutOptions,
        ) -> object_store::Result<PutResult> {
            options.mode = PutMode::Overwrite;
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            use futures::StreamExt;
            let deleted = Arc::clone(&self.deleted);
            let locations = locations.inspect(move |result| {
                if let Ok(path) = result {
                    deleted.lock().unwrap().push(path.clone());
                }
            });
            self.inner.delete_stream(Box::pin(locations))
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }

        async fn rename_opts(
            &self,
            from: &Path,
            to: &Path,
            options: RenameOptions,
        ) -> object_store::Result<()> {
            self.inner.rename_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn probe_rejects_ignored_preconditions_and_attempts_all_cleanup() {
        let deleted = Arc::new(Mutex::new(Vec::new()));
        let store = IgnoresPreconditions {
            inner: InMemory::new(),
            deleted: Arc::clone(&deleted),
        };
        let prefix = Path::from("conditional/ignored");
        let error = probe_conditional_writes(&store, &prefix).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("accepted a second PutMode::Create"));

        let deleted = deleted.lock().unwrap();
        assert_eq!(deleted.len(), 4);
        for child in ["create", "update", "race", "update-race"] {
            assert!(deleted.contains(&prefix.clone().join(child)));
        }
    }
}
