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

pub fn validate_deployment_id(value: &str) -> Result<Uuid> {
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

    let actual = match crate::put_create(store, &path, PutPayload::from(bytes)).await {
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
    async fn create_read_and_require_manifest() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let created = ensure_deployment_manifest(store.as_ref(), None)
            .await
            .unwrap();
        validate_deployment_id(&created.deployment_id).unwrap();
        assert_eq!(
            require_deployment_manifest(store.as_ref(), Some(&created.deployment_id))
                .await
                .unwrap(),
            created
        );
    }

    #[tokio::test]
    async fn expected_id_constrains_existing_winner_not_creation() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let created = ensure_deployment_manifest(store.as_ref(), None)
            .await
            .unwrap();
        let wrong = if created.deployment_id == DEPLOYMENT_A {
            DEPLOYMENT_B
        } else {
            DEPLOYMENT_A
        };
        let error = ensure_deployment_manifest(store.as_ref(), Some(wrong))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("bucket deployment mismatch"));
    }

    #[test]
    fn deployment_id_must_be_canonical_non_nil_uuid() {
        assert!(validate_deployment_id(DEPLOYMENT_A).is_ok());
        assert!(validate_deployment_id("00000000-0000-0000-0000-000000000000").is_err());
        assert!(validate_deployment_id("018F47A2-8B6C-7DEF-8123-456789ABCDEF").is_err());
    }
}
