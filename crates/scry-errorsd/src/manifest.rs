use anyhow::{bail, ensure, Context, Result};
use object_store::{path::Path, Error, GetOptions, ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// One fixed key prevents independent products from accidentally sharing a bucket.
pub const DEPLOYMENT_MANIFEST_PATH: &str = "_scry/deployment/v1/manifest.json";
pub const DEPLOYMENT_MANIFEST_SCHEMA: u32 = 1;
const MAX_MANIFEST_BYTES: u64 = 16 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentManifest {
    pub schema_version: u32,
    pub deployment_id: String,
}

impl DeploymentManifest {
    fn new(deployment_id: Uuid) -> Self {
        Self {
            schema_version: DEPLOYMENT_MANIFEST_SCHEMA,
            deployment_id: deployment_id.to_string(),
        }
    }
}

pub(crate) fn validate_deployment_id(value: &str) -> Result<Uuid> {
    let parsed = Uuid::parse_str(value).context("deployment ID must be a UUID")?;
    if parsed.is_nil() {
        bail!("deployment ID must not be the nil UUID");
    }
    if parsed.to_string() != value {
        bail!("deployment ID must be a canonical lowercase UUID");
    }
    Ok(parsed)
}

/// Create the fixed manifest atomically, or read and verify the existing object.
///
/// A fresh random UUID is proposed for creation. Conditional creation selects
/// exactly one proposal; losing racers read and validate the stored winner. An
/// expected identity only constrains the winner and is never creation input.
pub async fn ensure_deployment_manifest(
    store: &dyn ObjectStore,
    expected_deployment_id: Option<&str>,
) -> Result<DeploymentManifest> {
    let expected = expected_deployment_id
        .map(validate_deployment_id)
        .transpose()?;
    let proposed = DeploymentManifest::new(Uuid::new_v4());
    let bytes = serde_json::to_vec(&proposed).context("serializing deployment manifest")?;
    let path = Path::from(DEPLOYMENT_MANIFEST_PATH);

    let actual = match scry_objstore::put_create(store, &path, PutPayload::from(bytes)).await {
        Ok(_) => proposed,
        Err(Error::AlreadyExists { .. }) => read_deployment_manifest(store).await?,
        Err(error) => {
            return Err(error).context("conditional create of deployment manifest failed")
        }
    };
    if let Some(expected) = expected {
        verify_manifest(&actual, expected)?;
    }
    Ok(actual)
}

/// Require and verify an existing manifest without creating deployment identity.
pub async fn require_deployment_manifest(
    store: &dyn ObjectStore,
    expected_deployment_id: Option<&str>,
) -> Result<DeploymentManifest> {
    let expected = expected_deployment_id
        .map(validate_deployment_id)
        .transpose()?;
    let manifest = read_deployment_manifest(store).await?;
    if let Some(expected) = expected {
        verify_manifest(&manifest, expected)?;
    }
    Ok(manifest)
}

/// Read and structurally validate the fixed bucket deployment manifest.
pub async fn read_deployment_manifest(store: &dyn ObjectStore) -> Result<DeploymentManifest> {
    let path = Path::from(DEPLOYMENT_MANIFEST_PATH);
    let metadata = store
        .head(&path)
        .await
        .context("reading deployment manifest metadata")?;
    if metadata.size > MAX_MANIFEST_BYTES {
        bail!(
            "deployment manifest is {} bytes; maximum is {MAX_MANIFEST_BYTES}",
            metadata.size
        );
    }
    let result = store
        .get_opts(
            &path,
            GetOptions {
                range: Some((0..metadata.size).into()),
                ..Default::default()
            },
        )
        .await
        .context("reading deployment manifest")?;
    let bytes = result
        .bytes()
        .await
        .context("collecting deployment manifest")?;
    ensure!(
        bytes.len() as u64 == metadata.size,
        "deployment manifest read returned {} bytes; expected {}",
        bytes.len(),
        metadata.size
    );
    let manifest: DeploymentManifest =
        serde_json::from_slice(&bytes).context("decoding deployment manifest")?;
    validate_deployment_id(&manifest.deployment_id)?;
    if manifest.schema_version != DEPLOYMENT_MANIFEST_SCHEMA {
        bail!(
            "deployment manifest schema mismatch: expected {}, found {}",
            DEPLOYMENT_MANIFEST_SCHEMA,
            manifest.schema_version
        );
    }
    Ok(manifest)
}

fn verify_manifest(manifest: &DeploymentManifest, expected_deployment_id: Uuid) -> Result<()> {
    if manifest.deployment_id != expected_deployment_id.to_string() {
        bail!(
            "bucket deployment mismatch: expected `{expected_deployment_id}`, found `{}`",
            manifest.deployment_id
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::{memory::InMemory, ObjectStore};

    use super::*;

    const DEPLOYMENT_A: &str = "018f47a2-8b6c-7def-8123-456789abcdef";
    const DEPLOYMENT_B: &str = "018f47a2-8b6c-7def-8123-456789abcdee";

    #[tokio::test]
    async fn init_without_expected_id_creates_canonical_uuid_manifest() {
        let store = InMemory::new();
        let created = ensure_deployment_manifest(&store, None).await.unwrap();
        assert_eq!(
            validate_deployment_id(&created.deployment_id)
                .unwrap()
                .to_string(),
            created.deployment_id
        );
        assert_eq!(read_deployment_manifest(&store).await.unwrap(), created);

        let listed = store
            .list_with_delimiter(Some(&Path::from("_scry/deployment/v1")))
            .await
            .unwrap();
        assert_eq!(listed.objects.len(), 1);
        assert_eq!(
            listed.objects[0].location.as_ref(),
            DEPLOYMENT_MANIFEST_PATH
        );
    }

    #[tokio::test]
    async fn concurrent_init_callers_return_the_same_winner() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let left_store = Arc::clone(&store);
        let right_store = Arc::clone(&store);
        let (left, right) = tokio::join!(
            async move { ensure_deployment_manifest(left_store.as_ref(), None).await },
            async move { ensure_deployment_manifest(right_store.as_ref(), None).await },
        );
        let left = left.unwrap();
        let right = right.unwrap();
        assert_eq!(left, right);
        assert_eq!(
            read_deployment_manifest(store.as_ref()).await.unwrap(),
            left
        );
    }

    #[tokio::test]
    async fn expected_existing_manifest_is_idempotent() {
        let store = InMemory::new();
        let created = ensure_deployment_manifest(&store, None).await.unwrap();
        ensure_deployment_manifest(&store, Some(&created.deployment_id))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn supplied_identity_is_only_an_expectation_not_the_created_identity() {
        let store = InMemory::new();
        let error = ensure_deployment_manifest(&store, Some(DEPLOYMENT_A))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("bucket deployment mismatch"));
        let winner = read_deployment_manifest(&store).await.unwrap();
        assert_ne!(winner.deployment_id, DEPLOYMENT_A);
        validate_deployment_id(&winner.deployment_id).unwrap();
    }

    #[tokio::test]
    async fn existing_different_deployment_fails_without_overwrite() {
        let store = InMemory::new();
        let created = ensure_deployment_manifest(&store, None).await.unwrap();
        let error = ensure_deployment_manifest(&store, Some(DEPLOYMENT_B))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("bucket deployment mismatch"));
        assert_eq!(read_deployment_manifest(&store).await.unwrap(), created);
    }

    #[tokio::test]
    async fn serve_requires_existing_manifest_without_creating_one() {
        let store = InMemory::new();
        let error = require_deployment_manifest(&store, None).await.unwrap_err();
        assert!(error.to_string().contains("reading deployment manifest"));
        assert!(matches!(
            store.head(&Path::from(DEPLOYMENT_MANIFEST_PATH)).await,
            Err(Error::NotFound { .. })
        ));
    }

    #[test]
    fn rejects_invalid_noncanonical_and_nil_deployment_ids() {
        for invalid in [
            "deployment-a",
            "018F47A2-8B6C-7DEF-8123-456789ABCDEF",
            "018f47a28b6c7def8123456789abcdef",
            "{018f47a2-8b6c-7def-8123-456789abcdef}",
            "00000000-0000-0000-0000-000000000000",
        ] {
            assert!(
                validate_deployment_id(invalid).is_err(),
                "accepted {invalid}"
            );
        }
        validate_deployment_id(DEPLOYMENT_A).unwrap();
    }

    #[tokio::test]
    async fn rejects_noncanonical_id_stored_in_manifest() {
        let store = InMemory::new();
        let path = Path::from(DEPLOYMENT_MANIFEST_PATH);
        store
            .put(
                &path,
                r#"{"schema_version":1,"deployment_id":"018F47A2-8B6C-7DEF-8123-456789ABCDEF"}"#
                    .into(),
            )
            .await
            .unwrap();
        let error = read_deployment_manifest(&store).await.unwrap_err();
        assert!(error.to_string().contains("canonical lowercase UUID"));
    }

    #[tokio::test]
    async fn rejects_manifest_larger_than_the_bounded_read() {
        let store = InMemory::new();
        let path = Path::from(DEPLOYMENT_MANIFEST_PATH);
        store
            .put(&path, vec![b'x'; MAX_MANIFEST_BYTES as usize + 1].into())
            .await
            .unwrap();
        let error = read_deployment_manifest(&store).await.unwrap_err();
        assert!(error.to_string().contains("maximum is 16384"));
    }

    #[tokio::test]
    async fn rejects_unknown_schema_without_replacing_it() {
        let store = InMemory::new();
        let path = Path::from(DEPLOYMENT_MANIFEST_PATH);
        store
            .put(
                &path,
                format!(r#"{{"schema_version":2,"deployment_id":"{DEPLOYMENT_A}"}}"#).into(),
            )
            .await
            .unwrap();
        let error = ensure_deployment_manifest(&store, Some(DEPLOYMENT_A))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("schema mismatch"));
    }
}
