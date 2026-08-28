//! Thin wrapper around [`object_store`] for scry.
//!
//! Responsibilities deliberately kept narrow:
//!
//! - **Config struct** so the rest of the codebase doesn't have to know
//!   which env vars or which builder methods the apache crate wants.
//! - **Factory** that returns an `Arc<dyn ObjectStore>` for an
//!   S3-compatible backend (Garage in dev, real S3 / R2 / Hetzner in
//!   production), pre-wrapped in [`PooledStore`] so per-fetch buffers
//!   get reused across the lifetime of the process (see `pool.rs`
//!   for the motivation: DWARF profiling showed ~30% of query wall
//!   time in kernel page-fault servicing for fresh response Vecs).
//! - **`PooledStore` + `BufPool`** as a reusable `ObjectStore`
//!   adapter, in case future code wants to wrap a non-S3 store too.
//!
//! Everything else — `put`, `get`, `list`, `delete`, multipart, range
//! reads — is reached by calling the underlying `dyn ObjectStore`
//! directly. We are not in the business of re-exporting that surface.
//!
//! ## Conditional PUT
//!
//! Real S3, R2, and minio all support `If-None-Match: *` for safe
//! retry of block uploads. Garage 1.0.x silently overwrites — the
//! header is accepted but not honored. v0.1 of scry doesn't depend on
//! this: blocks are addressed by UUID v7, and a single writer never
//! issues two PUTs to the same path. When we move to a real S3-class
//! backend (or Garage gains support), a `put_if_absent` helper around
//! `PutMode::Create` is the place to add it back.

mod pool;
mod store;

pub use pool::{
    BufPool, BufPoolConfig, PoolStats, PooledBuf, DEFAULT_POOL_AUTOSCALE_HEADROOM,
    DEFAULT_POOL_CAPACITY, DEFAULT_POOL_MAX_CAPACITY, DEFAULT_POOL_MAX_RETAINED_BYTES,
    DEFAULT_POOL_WARMUP_SIZE,
};
pub use store::PooledStore;

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use object_store::{
    aws::{AmazonS3Builder, AmazonS3ConfigKey, AwsCredential, AwsCredentialProvider},
    client::CredentialProvider,
    ObjectStore,
};

/// Connection details for an S3-compatible bucket.
///
/// Reading these from the environment (`SCRY_OBJSTORE_*`) and from a
/// future config file are both layered on top — this struct is just
/// the parameter set the factory needs.
#[derive(Debug, Clone)]
pub struct ObjStoreConfig {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    /// Explicit credentials for non-AWS S3-compatible services. When absent,
    /// the AWS default credential chain is used (environment, shared profile,
    /// web identity/IRSA, ECS task or EKS Pod Identity, then EC2 IMDS).
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    /// Optional session token paired with explicit temporary credentials.
    pub session_token: Option<String>,
    /// Path-style (true, `endpoint/bucket/key`) vs virtual-hosted
    /// (false, `bucket.endpoint/key`). Garage and most homelab S3s
    /// want path-style; AWS prefers virtual-hosted but accepts either.
    pub path_style: bool,
}

impl ObjStoreConfig {
    /// Read bucket connection details from `SCRY_OBJSTORE_*`.
    ///
    /// Explicit `SCRY_OBJSTORE_ACCESS_KEY_ID` and
    /// `SCRY_OBJSTORE_SECRET_ACCESS_KEY` are optional but must be supplied as a
    /// pair. If omitted, the object-store factory resolves credentials through
    /// the standard AWS chain. `SCRY_OBJSTORE_SESSION_TOKEN` is accepted for
    /// explicit temporary credentials.
    pub fn from_env() -> Result<Self> {
        fn get(key: &str) -> Result<String> {
            std::env::var(key).with_context(|| format!("env var {key} not set"))
        }
        fn optional(key: &str) -> Option<String> {
            std::env::var(key).ok().filter(|value| !value.is_empty())
        }

        let access_key_id = optional("SCRY_OBJSTORE_ACCESS_KEY_ID");
        let secret_access_key = optional("SCRY_OBJSTORE_SECRET_ACCESS_KEY");
        let session_token = optional("SCRY_OBJSTORE_SESSION_TOKEN");
        validate_explicit_credentials(
            access_key_id.as_deref(),
            secret_access_key.as_deref(),
            session_token.as_deref(),
        )?;

        Ok(Self {
            endpoint: get("SCRY_OBJSTORE_ENDPOINT")?,
            region: get("SCRY_OBJSTORE_REGION")?,
            bucket: get("SCRY_OBJSTORE_BUCKET")?,
            access_key_id,
            secret_access_key,
            session_token,
            // Default to path-style; the env can override via
            // SCRY_OBJSTORE_PATH_STYLE=false for real AWS.
            path_style: std::env::var("SCRY_OBJSTORE_PATH_STYLE")
                .map(|v| v != "false")
                .unwrap_or(true),
        })
    }
}

fn validate_explicit_credentials(
    access_key_id: Option<&str>,
    secret_access_key: Option<&str>,
    session_token: Option<&str>,
) -> Result<()> {
    match (access_key_id, secret_access_key, session_token) {
        (Some(_), Some(_), _) | (None, None, None) => Ok(()),
        (None, None, Some(_)) => bail!(
            "env var SCRY_OBJSTORE_SESSION_TOKEN requires explicit SCRY_OBJSTORE_ACCESS_KEY_ID and SCRY_OBJSTORE_SECRET_ACCESS_KEY"
        ),
        _ => bail!(
            "SCRY_OBJSTORE_ACCESS_KEY_ID and SCRY_OBJSTORE_SECRET_ACCESS_KEY must be set together"
        ),
    }
}

impl BufPoolConfig {
    /// Read pool knobs from `SCRY_OBJSTORE_POOL_*` env vars, falling
    /// back to defaults for any that aren't set. Sizes are in MiB
    /// for ergonomics; counts are dimensionless.
    ///
    /// - `SCRY_OBJSTORE_POOL_WARMUP_COUNT` (default 0 — opt-in)
    /// - `SCRY_OBJSTORE_POOL_WARMUP_SIZE_MIB` (default 10)
    /// - `SCRY_OBJSTORE_POOL_INITIAL_CAPACITY` (default 16)
    /// - `SCRY_OBJSTORE_POOL_MAX_CAPACITY` (default 128)
    /// - `SCRY_OBJSTORE_POOL_MAX_RETAINED_MIB` (default 256)
    /// - `SCRY_OBJSTORE_POOL_AUTOSCALE_HEADROOM` (default 4)
    pub fn from_env() -> Result<Self> {
        fn parse<T: std::str::FromStr>(key: &str, default: T) -> Result<T>
        where
            T::Err: std::fmt::Display,
        {
            match std::env::var(key) {
                Ok(v) => v
                    .parse::<T>()
                    .map_err(|e| anyhow::anyhow!("env var {key}=`{v}` failed to parse: {e}")),
                Err(_) => Ok(default),
            }
        }
        let warmup_size_mib: usize = parse(
            "SCRY_OBJSTORE_POOL_WARMUP_SIZE_MIB",
            DEFAULT_POOL_WARMUP_SIZE / (1024 * 1024),
        )?;
        let max_retained_mib: usize = parse(
            "SCRY_OBJSTORE_POOL_MAX_RETAINED_MIB",
            DEFAULT_POOL_MAX_RETAINED_BYTES / (1024 * 1024),
        )?;
        Ok(Self {
            initial_capacity: parse("SCRY_OBJSTORE_POOL_INITIAL_CAPACITY", DEFAULT_POOL_CAPACITY)?,
            max_capacity: parse("SCRY_OBJSTORE_POOL_MAX_CAPACITY", DEFAULT_POOL_MAX_CAPACITY)?,
            max_retained_bytes: max_retained_mib.checked_mul(1024 * 1024).context(
                "SCRY_OBJSTORE_POOL_MAX_RETAINED_MIB overflows usize when converted to bytes",
            )?,
            warmup_count: parse("SCRY_OBJSTORE_POOL_WARMUP_COUNT", 0)?,
            warmup_size: warmup_size_mib * 1024 * 1024,
            autoscale_headroom: parse(
                "SCRY_OBJSTORE_POOL_AUTOSCALE_HEADROOM",
                DEFAULT_POOL_AUTOSCALE_HEADROOM,
            )?,
        })
    }
}

#[derive(Debug)]
struct AwsSdkCredentialProvider {
    inner: SharedCredentialsProvider,
}

#[async_trait]
impl CredentialProvider for AwsSdkCredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let credential = self.inner.provide_credentials().await.map_err(|source| {
            object_store::Error::Generic {
                store: "S3",
                source: Box::new(source),
            }
        })?;
        Ok(Arc::new(AwsCredential {
            key_id: credential.access_key_id().to_owned(),
            secret_key: credential.secret_access_key().to_owned(),
            token: credential.session_token().map(ToOwned::to_owned),
        }))
    }
}

async fn credential_provider(cfg: &ObjStoreConfig) -> Result<AwsCredentialProvider> {
    if let (Some(access_key_id), Some(secret_access_key)) =
        (&cfg.access_key_id, &cfg.secret_access_key)
    {
        return Ok(Arc::new(
            object_store::client::StaticCredentialProvider::new(AwsCredential {
                key_id: access_key_id.clone(),
                secret_key: secret_access_key.clone(),
                token: cfg.session_token.clone(),
            }),
        ));
    }

    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(cfg.region.clone()))
        .load()
        .await;
    let provider = sdk_config.credentials_provider().context(
        "AWS default credential chain is unavailable; configure an AWS profile, workload/instance role, or explicit SCRY_OBJSTORE_* credentials",
    )?;
    Ok(Arc::new(AwsSdkCredentialProvider { inner: provider }))
}

/// Build an `Arc<dyn ObjectStore>` for the given config.
pub async fn open(cfg: &ObjStoreConfig) -> Result<Arc<dyn ObjectStore>> {
    let (store, _pool) = open_with_pool(cfg).await?;
    Ok(store)
}

/// Like [`open`], but also returns a handle to the buffer pool the
/// returned store routes through. The pool uses default configuration
/// — no warmup, default capacity, autoscale enabled. Reach for
/// [`open_with_pool_config`] when you want to drive those from env or
/// CLI flags.
///
/// The pool is internally `Arc<...>` so cloning is cheap and shared:
/// the returned handle and the store both reference the same pool.
pub async fn open_with_pool(cfg: &ObjStoreConfig) -> Result<(Arc<dyn ObjectStore>, BufPool)> {
    open_with_pool_config(cfg, BufPoolConfig::default()).await
}

/// Like [`open_with_pool`] but takes an explicit [`BufPoolConfig`].
///
/// Callers that want env-driven defaults can pass
/// `BufPoolConfig::from_env()?` here; CLI binaries typically build a
/// `BufPoolConfig`, override any flag-set fields, and pass the result.
pub async fn open_with_pool_config(
    cfg: &ObjStoreConfig,
    pool_cfg: BufPoolConfig,
) -> Result<(Arc<dyn ObjectStore>, BufPool)> {
    let allow_http = cfg.endpoint.starts_with("http://");
    let credentials = credential_provider(cfg).await?;
    let s3 = AmazonS3Builder::new()
        .with_config(AmazonS3ConfigKey::Endpoint, &cfg.endpoint)
        .with_config(AmazonS3ConfigKey::Region, &cfg.region)
        .with_config(AmazonS3ConfigKey::Bucket, &cfg.bucket)
        .with_credentials(credentials)
        .with_config(
            AmazonS3ConfigKey::VirtualHostedStyleRequest,
            if cfg.path_style { "false" } else { "true" },
        )
        .with_allow_http(allow_http)
        .build()
        .with_context(|| format!("building S3 client for {}", cfg.endpoint))?;

    // Wrap in `PooledStore` so range-fetch response bodies drain into
    // reusable `Vec<u8>` buffers — sidesteps the per-fetch `mmap` +
    // page-zero cost that DWARF profiling pinned at ~30 % of query
    // wall on the smoke bucket. See `pool.rs` for the gory details.
    let pool = BufPool::with_config(pool_cfg);
    let pooled = PooledStore::with_pool(Arc::new(s3), pool.clone());
    Ok((Arc::new(pooled), pool))
}

#[cfg(test)]
mod tests {
    use super::validate_explicit_credentials;

    #[test]
    fn accepts_default_chain_without_explicit_credentials() {
        validate_explicit_credentials(None, None, None).unwrap();
    }

    #[test]
    fn accepts_explicit_permanent_and_temporary_credentials() {
        validate_explicit_credentials(Some("key"), Some("secret"), None).unwrap();
        validate_explicit_credentials(Some("key"), Some("secret"), Some("token")).unwrap();
    }

    #[tokio::test]
    async fn explicit_temporary_credentials_reach_object_store_provider() {
        let cfg = super::ObjStoreConfig {
            endpoint: "https://s3.example.com".into(),
            region: "test-1".into(),
            bucket: "bucket".into(),
            access_key_id: Some("key".into()),
            secret_access_key: Some("secret".into()),
            session_token: Some("token".into()),
            path_style: false,
        };
        let provider = super::credential_provider(&cfg).await.unwrap();
        let credential = provider.get_credential().await.unwrap();
        assert_eq!(credential.key_id, "key");
        assert_eq!(credential.secret_key, "secret");
        assert_eq!(credential.token.as_deref(), Some("token"));
    }

    #[test]
    fn rejects_partial_explicit_credentials() {
        assert!(validate_explicit_credentials(Some("key"), None, None).is_err());
        assert!(validate_explicit_credentials(None, Some("secret"), None).is_err());
        assert!(validate_explicit_credentials(None, None, Some("token")).is_err());
        assert!(validate_explicit_credentials(Some("key"), None, Some("token")).is_err());
    }
}
