//! Conditional object writes and a runtime capability probe.
//!
//! `object_store` exposes conditional writes through [`PutMode`].  These
//! helpers keep callers from accidentally falling back to the default
//! overwrite mode, and [`probe_conditional_writes`] verifies that a backend
//! actually enforces (rather than merely accepts) both preconditions.

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use object_store::{
    path::Path, Error, GetOptions, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutOptions,
    PutPayload, PutResult, UpdateVersion,
};

/// Options for an atomic create-if-absent write.
pub fn create_options() -> PutOptions {
    PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    }
}

/// Options for an atomic compare-and-swap write.
///
/// The ETag is sent without its surrounding double quotes. S3 returns ETags
/// quoted (`"abc…"`) and `object_store` forwards them verbatim as `If-Match`,
/// but Hetzner Object Storage (Ceph RGW) compares a PutObject `If-Match`
/// against the bare ETag literally: a quoted, *current* ETag is rejected with
/// 412, while the unquoted form is accepted and a wrong unquoted ETag is still
/// rejected. AWS S3 documents the unquoted form, so it is sent everywhere.
/// Conditional GETs are unaffected and keep the ETag as returned.
pub fn update_options(mut version: UpdateVersion) -> PutOptions {
    if let Some(e_tag) = version.e_tag.as_mut() {
        if let Some(bare) = unquote_etag(e_tag) {
            *e_tag = bare.to_owned();
        }
    }
    PutOptions {
        mode: PutMode::Update(version),
        ..Default::default()
    }
}

/// The ETag inside one pair of surrounding double quotes, if it is quoted.
fn unquote_etag(e_tag: &str) -> Option<&str> {
    e_tag
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
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

/// How many times each concurrent race is repeated. A backend that serializes
/// conditional writes only most of the time must not pass by luck.
const RACE_ROUNDS: usize = 16;

/// Child keys the probe writes below its prefix, all deleted during cleanup.
const PROBE_CHILDREN: [&str; 4] = ["create", "update", "race", "update-race"];

/// Child key the probe reads but never writes: it must report `NotFound`.
const PROBE_MISSING_CHILD: &str = "missing";

/// Verify atomic conditional-create and versioned-update support.
///
/// `prefix` is a disposable probe namespace. The function writes four children
/// below it (and reads a fifth that it never writes) and attempts to delete
/// all four before returning, including after a failed check. Callers should
/// nevertheless use a fresh prefix: cleanup can itself fail because of
/// credentials or transport errors.
///
/// The probe verifies all of the properties needed by control-plane writers:
///
/// * HEAD and GET of a key that does not exist report `NotFound`. S3 answers
///   403 instead when the credentials lack `s3:ListBucket`, which would turn
///   every "missing means absent" decision into a hard failure, so that case
///   fails closed with an explanation;
/// * a second create fails and leaves the first value intact;
/// * an update with a stale version fails and leaves the current value intact;
/// * an update with the current version succeeds whether that version came
///   from a PUT response, a HEAD, or a GET (production compare-and-swap reads
///   the version with HEAD/GET, and some backends format those ETags
///   differently from PUT responses);
/// * a GET conditioned on the current ETag succeeds and one conditioned on a
///   stale ETag fails with `Precondition`;
/// * repeated concurrent creates have exactly one winner; and
/// * repeated pairs of updates racing on one HEAD-derived version each have
///   exactly one winner.
///
/// A successful probe also requires successful cleanup. If a semantic check
/// fails, that original failure is returned after cleanup has been attempted.
pub async fn probe_conditional_writes(store: &dyn ObjectStore, prefix: &Path) -> Result<()> {
    let [create_path, update_path, race_path, update_race_path] =
        PROBE_CHILDREN.map(|child| prefix.clone().join(child));
    let missing_path = prefix.clone().join(PROBE_MISSING_CHILD);

    let check = async {
        check_missing_is_not_found(store, &missing_path).await?;
        check_create(store, &create_path).await?;
        check_update(store, &update_path).await?;
        check_create_race(store, &race_path).await?;
        check_update_race(store, &update_race_path).await
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

/// HEAD and GET of a never-written key must both report `NotFound`.
async fn check_missing_is_not_found(store: &dyn ObjectStore, path: &Path) -> Result<()> {
    match store.head(path).await {
        Err(Error::NotFound { .. }) => {}
        Ok(_) => {
            bail!("HEAD of never-written probe key {path} succeeded; use a fresh probe prefix")
        }
        Err(error) => return Err(absence_error("HEAD", path, error)),
    }
    match store.get(path).await {
        Err(Error::NotFound { .. }) => Ok(()),
        Ok(_) => bail!("GET of never-written probe key {path} succeeded; use a fresh probe prefix"),
        Err(error) => Err(absence_error("GET", path, error)),
    }
}

/// Explain a missing key that did not read back as `NotFound`.
fn absence_error(operation: &str, path: &Path, error: Error) -> anyhow::Error {
    if matches!(error, Error::PermissionDenied { .. }) {
        anyhow!(error).context(format!(
            "{operation} of missing key {path} was denied (HTTP 403) instead of reporting \
             NotFound (HTTP 404). S3-compatible stores hide absence behind 403 when the \
             credentials lack s3:ListBucket on the bucket; scry treats NotFound as \"absent\", \
             so grant s3:ListBucket (and s3:GetObject) to these credentials"
        ))
    } else {
        anyhow!(error).context(format!(
            "{operation} of missing key {path} did not report NotFound"
        ))
    }
}

async fn check_create(store: &dyn ObjectStore, path: &Path) -> Result<()> {
    let first = Bytes::from_static(b"conditional-probe:first");
    let second = Bytes::from_static(b"conditional-probe:second");
    put_create(store, path, first.clone().into())
        .await
        .context("initial conditional create failed")?;
    let err = match put_create(store, path, second.into()).await {
        Ok(_) => bail!("backend accepted a second PutMode::Create"),
        Err(error) => error,
    };
    if !matches!(err, Error::AlreadyExists { .. }) {
        bail!("second PutMode::Create returned the wrong error: {err}");
    }
    expect_stored(
        store,
        path,
        &first,
        "rejected PutMode::Create changed the stored object",
    )
    .await
}

/// Stale and current compare-and-swap with versions taken from a PUT response,
/// a HEAD, and a GET, plus conditional GET with current and stale ETags.
async fn check_update(store: &dyn ObjectStore, path: &Path) -> Result<()> {
    // Create v1, then use an unconditional write solely to manufacture a
    // newer version. This lets us test a stale update before the successful
    // current-version update.
    let v1 = put_create(
        store,
        path,
        Bytes::from_static(b"conditional-probe:v1").into(),
    )
    .await
    .context("conditional update setup create failed")?;
    let current = Bytes::from_static(b"conditional-probe:current");
    let v2 = store
        .put(path, current.clone().into())
        .await
        .context("conditional update setup overwrite failed")?;
    expect_stale_update(store, path, v1.into(), &current, "a PUT-derived").await?;
    let updated = Bytes::from_static(b"conditional-probe:updated");
    put_update(store, path, updated.clone().into(), v2.into())
        .await
        .context("PutMode::Update with the current PUT-derived version failed")?;
    expect_stored(
        store,
        path,
        &updated,
        "successful PutMode::Update did not store the new value",
    )
    .await?;

    // HEAD-derived: how production compare-and-swap usually learns a version.
    let head_version = update_version(
        store
            .head(path)
            .await
            .context("HEAD for compare-and-swap failed")?,
        "HEAD",
    )?;
    let head_cas = Bytes::from_static(b"conditional-probe:head-cas");
    put_update(store, path, head_cas.clone().into(), head_version.clone())
        .await
        .context("PutMode::Update with the current HEAD-derived version failed")?;
    expect_stored(
        store,
        path,
        &head_cas,
        "PutMode::Update with a HEAD-derived version did not store the new value",
    )
    .await?;
    expect_stale_update(store, path, head_version, &head_cas, "a HEAD-derived").await?;

    // GET-derived: a read-modify-write takes the version from the body's GET.
    let got = store
        .get(path)
        .await
        .context("GET for compare-and-swap failed")?;
    let get_version = update_version(got.meta.clone(), "GET")?;
    let body = got
        .bytes()
        .await
        .context("collect GET for compare-and-swap failed")?;
    if body != head_cas {
        bail!("GET for compare-and-swap returned unexpected bytes");
    }
    let get_cas = Bytes::from_static(b"conditional-probe:get-cas");
    put_update(store, path, get_cas.clone().into(), get_version.clone())
        .await
        .context("PutMode::Update with the current GET-derived version failed")?;
    expect_stored(
        store,
        path,
        &get_cas,
        "PutMode::Update with a GET-derived version did not store the new value",
    )
    .await?;
    let stale_e_tag = get_version.e_tag.clone();
    expect_stale_update(store, path, get_version, &get_cas, "a GET-derived").await?;

    // Conditional GET: the current ETag reads, a stale one is refused.
    let current_e_tag = store
        .head(path)
        .await
        .context("HEAD for conditional GET failed")?
        .e_tag;
    let conditional = store
        .get_opts(
            path,
            GetOptions {
                if_match: current_e_tag,
                ..Default::default()
            },
        )
        .await
        .context("GET with the current If-Match ETag failed")?
        .bytes()
        .await
        .context("collect GET with the current If-Match ETag failed")?;
    if conditional != get_cas {
        bail!("GET with the current If-Match ETag returned unexpected bytes");
    }
    match store
        .get_opts(
            path,
            GetOptions {
                if_match: stale_e_tag,
                ..Default::default()
            },
        )
        .await
    {
        Ok(_) => bail!("backend served a GET whose If-Match ETag is stale"),
        Err(Error::Precondition { .. }) => Ok(()),
        Err(error) => bail!("GET with a stale If-Match ETag returned the wrong error: {error}"),
    }
}

async fn check_create_race(store: &dyn ObjectStore, path: &Path) -> Result<()> {
    let left = Bytes::from_static(b"conditional-probe:left");
    let right = Bytes::from_static(b"conditional-probe:right");
    for round in 0..RACE_ROUNDS {
        if round != 0 {
            store
                .delete(path)
                .await
                .context("reset concurrent-create probe failed")?;
        }
        let (left_result, right_result) = futures::join!(
            put_create(store, path, left.clone().into()),
            put_create(store, path, right.clone().into()),
        );
        let winner = match (left_result, right_result) {
            (Ok(_), Err(Error::AlreadyExists { .. })) => &left,
            (Err(Error::AlreadyExists { .. }), Ok(_)) => &right,
            (left_result, right_result) => bail!(
                "concurrent PutMode::Create round {round} must have exactly one winner; left={left_result:?}, right={right_result:?}"
            ),
        };
        expect_stored(
            store,
            path,
            winner,
            "concurrent create winner does not match the stored value",
        )
        .await?;
    }
    Ok(())
}

/// Each round reads the current version with HEAD — the production path — and
/// races two updates against it.
async fn check_update_race(store: &dyn ObjectStore, path: &Path) -> Result<()> {
    put_create(
        store,
        path,
        Bytes::from_static(b"conditional-probe:update-race-base").into(),
    )
    .await
    .context("concurrent-update setup create failed")?;
    for round in 0..RACE_ROUNDS {
        // Bodies must differ from every earlier round: S3 ETags are content
        // digests, so a winner that rewrote the previous round's bytes would
        // leave the ETag unchanged and the loser's If-Match still current.
        let left = Bytes::from(format!("conditional-probe:update-left-{round}"));
        let right = Bytes::from(format!("conditional-probe:update-right-{round}"));
        let version = update_version(
            store
                .head(path)
                .await
                .context("HEAD before concurrent updates failed")?,
            "HEAD",
        )?;
        let (left_result, right_result) = futures::join!(
            put_update(store, path, left.clone().into(), version.clone()),
            put_update(store, path, right.clone().into(), version),
        );
        let winner = match (left_result, right_result) {
            (Ok(_), Err(Error::Precondition { .. })) => &left,
            (Err(Error::Precondition { .. }), Ok(_)) => &right,
            (left_result, right_result) => bail!(
                "concurrent PutMode::Update round {round} must have exactly one winner; left={left_result:?}, right={right_result:?}"
            ),
        };
        expect_stored(
            store,
            path,
            winner,
            "concurrent update winner does not match the stored value",
        )
        .await?;
    }
    Ok(())
}

/// The compare-and-swap version carried by a HEAD/GET response.
fn update_version(meta: ObjectMeta, source: &str) -> Result<UpdateVersion> {
    if meta.e_tag.is_none() && meta.version.is_none() {
        bail!("{source} returned neither an ETag nor a version; compare-and-swap is impossible");
    }
    Ok(UpdateVersion {
        e_tag: meta.e_tag,
        version: meta.version,
    })
}

/// An update with `stale` must fail with `Precondition` and keep `current`.
async fn expect_stale_update(
    store: &dyn ObjectStore,
    path: &Path,
    stale: UpdateVersion,
    current: &Bytes,
    source: &str,
) -> Result<()> {
    let body = Bytes::from_static(b"conditional-probe:stale");
    match put_update(store, path, body.into(), stale).await {
        Ok(_) => bail!("backend accepted a PutMode::Update with {source} stale version"),
        Err(Error::Precondition { .. }) => {}
        Err(error) => {
            bail!("PutMode::Update with {source} stale version returned the wrong error: {error}")
        }
    }
    expect_stored(
        store,
        path,
        current,
        "rejected PutMode::Update changed the stored object",
    )
    .await
}

/// Read `path` and fail with `mismatch` unless it holds exactly `expected`.
async fn expect_stored(
    store: &dyn ObjectStore,
    path: &Path,
    expected: &Bytes,
    mismatch: &str,
) -> Result<()> {
    let stored = store
        .get(path)
        .await
        .with_context(|| format!("read of {path} failed"))?
        .bytes()
        .await
        .with_context(|| format!("collect of {path} failed"))?;
    if stored != *expected {
        bail!("{mismatch}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::{
        memory::InMemory, CopyOptions, GetResult, ListResult, MultipartUpload, PutMultipartOptions,
        RenameOptions,
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

    #[test]
    fn update_options_send_unquoted_etag() {
        let quoted = UpdateVersion {
            e_tag: Some("\"b1946ac92492d2347c6235b4d2611184\"".into()),
            version: Some("v1".into()),
        };
        assert_eq!(
            update_options(quoted).mode,
            PutMode::Update(UpdateVersion {
                e_tag: Some("b1946ac92492d2347c6235b4d2611184".into()),
                version: Some("v1".into()),
            })
        );
        // Anything not wrapped in exactly one pair of quotes passes through.
        for e_tag in ["\"", "\"abc", "abc\"", "W/\"abc\"", ""] {
            let version = UpdateVersion {
                e_tag: Some(e_tag.into()),
                version: None,
            };
            assert_eq!(
                update_options(version.clone()).mode,
                PutMode::Update(version),
                "{e_tag:?}"
            );
        }
        let versionless = UpdateVersion {
            e_tag: None,
            version: Some("v1".into()),
        };
        assert_eq!(
            update_options(versionless.clone()).mode,
            PutMode::Update(versionless)
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
        for child in PROBE_CHILDREN.into_iter().chain([PROBE_MISSING_CHILD]) {
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

    /// A deliberate deviation from S3 semantics, injected by [`QuirkyStore`].
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Quirk {
        /// Accepts write preconditions but treats them as plain overwrites.
        IgnoresWritePreconditions,
        /// Reports absent keys as 403, like S3 without `s3:ListBucket`.
        ForbidsMissingKeys,
        /// Ceph RGW / Hetzner: ETags come back quoted from every response,
        /// a quoted PutObject `If-Match` is rejected even when current, and
        /// conditional GETs accept either form.
        RgwEtagQuoting,
        /// HEAD reports an ETag that does not match what writes compare.
        HeadEtagMismatch,
        /// Serves GETs regardless of `If-Match`.
        IgnoresReadPreconditions,
    }

    #[derive(Debug)]
    struct QuirkyStore {
        inner: InMemory,
        quirk: Quirk,
        deleted: Arc<Mutex<Vec<Path>>>,
    }

    impl QuirkyStore {
        fn new(quirk: Quirk) -> Self {
            Self {
                inner: InMemory::new(),
                quirk,
                deleted: Arc::default(),
            }
        }
    }

    impl std::fmt::Display for QuirkyStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "QuirkyStore({:?})", self.quirk)
        }
    }

    fn quote(e_tag: &mut Option<String>) {
        if let Some(e_tag) = e_tag.as_mut() {
            *e_tag = format!("\"{e_tag}\"");
        }
    }

    #[async_trait]
    impl ObjectStore for QuirkyStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            mut options: PutOptions,
        ) -> object_store::Result<PutResult> {
            match (&options.mode, self.quirk) {
                (_, Quirk::IgnoresWritePreconditions) => options.mode = PutMode::Overwrite,
                (PutMode::Update(version), Quirk::RgwEtagQuoting)
                    if version.e_tag.as_deref().is_some_and(|e| e.starts_with('"')) =>
                {
                    return Err(Error::Precondition {
                        path: location.to_string(),
                        source: "quoted If-Match never matches".into(),
                    });
                }
                _ => {}
            }
            let mut result = self.inner.put_opts(location, payload, options).await?;
            if self.quirk == Quirk::RgwEtagQuoting {
                quote(&mut result.e_tag);
            }
            Ok(result)
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
            mut options: GetOptions,
        ) -> object_store::Result<GetResult> {
            match self.quirk {
                Quirk::IgnoresReadPreconditions => options.if_match = None,
                Quirk::RgwEtagQuoting => {
                    if let Some(e_tag) = options.if_match.as_mut() {
                        if let Some(bare) = unquote_etag(e_tag) {
                            *e_tag = bare.to_owned();
                        }
                    }
                }
                _ => {}
            }
            let head = options.head;
            let mut result = match self.inner.get_opts(location, options).await {
                Err(Error::NotFound { path, source })
                    if self.quirk == Quirk::ForbidsMissingKeys =>
                {
                    return Err(Error::PermissionDenied { path, source });
                }
                other => other?,
            };
            match self.quirk {
                Quirk::RgwEtagQuoting => quote(&mut result.meta.e_tag),
                Quirk::HeadEtagMismatch if head => {
                    if let Some(e_tag) = result.meta.e_tag.as_mut() {
                        e_tag.push_str("-head");
                    }
                }
                _ => {}
            }
            Ok(result)
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

    /// Run the probe against `quirk`, returning its error text (`None` on a
    /// pass) after asserting that every written child was cleaned up.
    async fn probe_with(quirk: Quirk) -> Option<String> {
        let store = QuirkyStore::new(quirk);
        let prefix = Path::from("conditional/quirk");
        let result = probe_conditional_writes(&store, &prefix).await;
        // Presence, not count: a probe that reaches the create race also
        // deletes between its rounds.
        let deleted = store.deleted.lock().unwrap();
        for child in PROBE_CHILDREN {
            assert!(deleted.contains(&prefix.clone().join(child)), "{quirk:?}");
        }
        result.err().map(|error| format!("{error:#}"))
    }

    #[tokio::test]
    async fn probe_rejects_ignored_preconditions_and_attempts_all_cleanup() {
        let error = probe_with(Quirk::IgnoresWritePreconditions)
            .await
            .expect("probe must fail");
        assert!(
            error.contains("accepted a second PutMode::Create"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn probe_fails_closed_when_missing_keys_read_back_forbidden() {
        let error = probe_with(Quirk::ForbidsMissingKeys)
            .await
            .expect("probe must fail");
        assert!(error.contains("HTTP 403"), "{error}");
        assert!(error.contains("s3:ListBucket"), "{error}");
    }

    #[tokio::test]
    async fn probe_passes_with_rgw_style_etag_quoting() {
        // Every compare-and-swap in the probe — PUT-, HEAD- and GET-derived,
        // and the racing rounds — must survive a backend that rejects quoted
        // PutObject If-Match. Only `update_options` unquoting makes this pass.
        assert_eq!(probe_with(Quirk::RgwEtagQuoting).await, None);
    }

    #[tokio::test]
    async fn probe_rejects_head_etags_that_writes_do_not_honour() {
        let error = probe_with(Quirk::HeadEtagMismatch)
            .await
            .expect("probe must fail");
        assert!(error.contains("HEAD-derived version failed"), "{error}");
    }

    #[tokio::test]
    async fn probe_rejects_ignored_get_preconditions() {
        let error = probe_with(Quirk::IgnoresReadPreconditions)
            .await
            .expect("probe must fail");
        assert!(error.contains("If-Match ETag is stale"), "{error}");
    }
}
