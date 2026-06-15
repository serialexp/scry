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
    DEFAULT_POOL_CAPACITY, DEFAULT_POOL_MAX_CAPACITY, DEFAULT_POOL_WARMUP_SIZE,
};
pub use store::PooledStore;

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

impl BufPoolConfig {
    /// Read pool knobs from `SCRY_OBJSTORE_POOL_*` env vars, falling
    /// back to defaults for any that aren't set. Sizes are in MiB
    /// for ergonomics; counts are dimensionless.
    ///
    /// - `SCRY_OBJSTORE_POOL_WARMUP_COUNT` (default 0 — opt-in)
    /// - `SCRY_OBJSTORE_POOL_WARMUP_SIZE_MIB` (default 10)
    /// - `SCRY_OBJSTORE_POOL_INITIAL_CAPACITY` (default 16)
    /// - `SCRY_OBJSTORE_POOL_MAX_CAPACITY` (default 128)
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
        Ok(Self {
            initial_capacity: parse("SCRY_OBJSTORE_POOL_INITIAL_CAPACITY", DEFAULT_POOL_CAPACITY)?,
            max_capacity: parse("SCRY_OBJSTORE_POOL_MAX_CAPACITY", DEFAULT_POOL_MAX_CAPACITY)?,
            warmup_count: parse("SCRY_OBJSTORE_POOL_WARMUP_COUNT", 0)?,
            warmup_size: warmup_size_mib * 1024 * 1024,
            autoscale_headroom: parse(
                "SCRY_OBJSTORE_POOL_AUTOSCALE_HEADROOM",
                DEFAULT_POOL_AUTOSCALE_HEADROOM,
            )?,
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
    let (store, _pool) = open_with_pool(cfg)?;
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
pub fn open_with_pool(cfg: &ObjStoreConfig) -> Result<(Arc<dyn ObjectStore>, BufPool)> {
    open_with_pool_config(cfg, BufPoolConfig::default())
}

/// Like [`open_with_pool`] but takes an explicit [`BufPoolConfig`].
///
/// Callers that want env-driven defaults can pass
/// `BufPoolConfig::from_env()?` here; CLI binaries typically build a
/// `BufPoolConfig`, override any flag-set fields, and pass the result.
pub fn open_with_pool_config(
    cfg: &ObjStoreConfig,
    pool_cfg: BufPoolConfig,
) -> Result<(Arc<dyn ObjectStore>, BufPool)> {
    let allow_http = cfg.endpoint.starts_with("http://");
    let s3 = AmazonS3Builder::new()
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

    // Wrap in `PooledStore` so range-fetch response bodies drain into
    // reusable `Vec<u8>` buffers — sidesteps the per-fetch `mmap` +
    // page-zero cost that DWARF profiling pinned at ~30 % of query
    // wall on the smoke bucket. See `pool.rs` for the gory details.
    let pool = BufPool::with_config(pool_cfg);
    let pooled = PooledStore::with_pool(Arc::new(s3), pool.clone());
    Ok((Arc::new(pooled), pool))
}
