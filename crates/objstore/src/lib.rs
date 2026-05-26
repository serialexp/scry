//! Thin wrapper around [`object_store`] for scry.
//!
//! Responsibilities deliberately kept narrow:
//!
//! - **Config struct** so the rest of the codebase doesn't have to know
//!   which env vars or which builder methods the apache crate wants.
//! - **Factory** that returns an `Arc<dyn ObjectStore>` for an
//!   S3-compatible backend (Garage in dev, real S3 / R2 / Hetzner in
//!   production).
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

use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::{
    aws::{AmazonS3Builder, AmazonS3ConfigKey},
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
    pub access_key_id: String,
    pub secret_access_key: String,
    /// Path-style (true, `endpoint/bucket/key`) vs virtual-hosted
    /// (false, `bucket.endpoint/key`). Garage and most homelab S3s
    /// want path-style; AWS prefers virtual-hosted but accepts either.
    pub path_style: bool,
}

impl ObjStoreConfig {
    /// Read config from the `SCRY_OBJSTORE_*` environment variables.
    /// Convenient for tests and dev binaries that source
    /// `docker/garage/.env`.
    pub fn from_env() -> Result<Self> {
        fn get(key: &str) -> Result<String> {
            std::env::var(key).with_context(|| format!("env var {key} not set"))
        }
        Ok(Self {
            endpoint: get("SCRY_OBJSTORE_ENDPOINT")?,
            region: get("SCRY_OBJSTORE_REGION")?,
            bucket: get("SCRY_OBJSTORE_BUCKET")?,
            access_key_id: get("SCRY_OBJSTORE_ACCESS_KEY_ID")?,
            secret_access_key: get("SCRY_OBJSTORE_SECRET_ACCESS_KEY")?,
            // Default to path-style; the env can override via
            // SCRY_OBJSTORE_PATH_STYLE=false for real AWS later.
            path_style: std::env::var("SCRY_OBJSTORE_PATH_STYLE")
                .map(|v| v != "false")
                .unwrap_or(true),
        })
    }
}

/// Build an `Arc<dyn ObjectStore>` for the given config.
///
/// The apache crate's builder takes a flat key-value bag. We set the
/// minimum needed to talk to a self-hosted S3 (Garage): endpoint,
/// region, bucket, credentials, path-style, and allow plain HTTP
/// (Garage is HTTP on localhost during dev).
pub fn open(cfg: &ObjStoreConfig) -> Result<Arc<dyn ObjectStore>> {
    let allow_http = cfg.endpoint.starts_with("http://");
    let store = AmazonS3Builder::new()
        .with_config(AmazonS3ConfigKey::Endpoint, &cfg.endpoint)
        .with_config(AmazonS3ConfigKey::Region, &cfg.region)
        .with_config(AmazonS3ConfigKey::Bucket, &cfg.bucket)
        .with_config(AmazonS3ConfigKey::AccessKeyId, &cfg.access_key_id)
        .with_config(AmazonS3ConfigKey::SecretAccessKey, &cfg.secret_access_key)
        .with_config(
            AmazonS3ConfigKey::VirtualHostedStyleRequest,
            if cfg.path_style { "false" } else { "true" },
        )
        .with_allow_http(allow_http)
        .build()
        .with_context(|| format!("building S3 client for {}", cfg.endpoint))?;
    Ok(Arc::new(store))
}

