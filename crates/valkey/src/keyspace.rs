//! Per-deployment key namespacing.
//!
//! Every key and channel scry uses lives under a single configurable
//! namespace, defaulting to `scry`:
//!
//! ```text
//! <ns>/lease/…            leases (compaction partitions, retention)
//! <ns>/blocks/<signal>    block-event pub/sub channels
//! <ns>/tail/ingesters/…   the tail-address registry (D-053)
//! <ns>/deleted/…          the staged-deletions registry (D-063)
//! <ns>/status/…           the fleet status registry (D-057)
//! <ns>/status-owner/…     its owner fences
//! ```
//!
//! Two scry deployments pointed at one Valkey — a staging and a production
//! cluster, or two buckets sharing a cache — would otherwise share every one
//! of these. That is not a cosmetic collision: they would contend for each
//! other's leases (so one deployment's compaction blocks the other's), see
//! each other's instances in their fleet and tail views, and, worst,
//! **converge each other's block events** — one deployment's staged deletion
//! would hide a same-UUID block in the other. UUIDs make same-UUID collisions
//! vanishingly unlikely, but the lease and registry keys are *not* UUID-keyed,
//! and those collide by construction.
//!
//! The namespace is chosen once at startup ([`Keyspace::resolve`]) and carried
//! by [`ValkeyClient`](crate::ValkeyClient), so every key-building site reads
//! it from the connection it is already holding rather than from a global.
//!
//! Note that `scry-cluster` builds **logical** lease keys (`lease/retention`,
//! `lease/compact/…`) with no namespace at all — it is deliberately
//! Valkey-agnostic. [`ValkeyLeaseProvider`](crate::ValkeyLeaseProvider)
//! prefixes them on the way out, which is also why a namespace may not contain
//! anything that would change the meaning of a `SCAN MATCH` pattern.

use std::sync::Arc;

use anyhow::{bail, Result};
use uuid::Uuid;

/// Environment variable naming the deployment namespace. Unset ⇒
/// [`DEFAULT_NAMESPACE`].
pub const NAMESPACE_ENV: &str = "SCRY_VALKEY_NAMESPACE";

/// The namespace used when nothing is configured — what every existing
/// deployment already has in its keys.
pub const DEFAULT_NAMESPACE: &str = "scry";

/// Longest accepted namespace. Arbitrary but small: this rides on every key.
const MAX_NAMESPACE_LEN: usize = 64;

/// Builds every Valkey key and channel name for one deployment. Cheap to
/// clone (holds an `Arc<str>`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Keyspace {
    /// The namespace, verbatim (no trailing separator).
    namespace: Arc<str>,
}

impl Default for Keyspace {
    fn default() -> Self {
        Self {
            namespace: Arc::from(DEFAULT_NAMESPACE),
        }
    }
}

impl Keyspace {
    /// Validate `namespace` and build the keyspace.
    ///
    /// Accepts `[A-Za-z0-9_.:-]{1,64}`. The character set is deliberately
    /// narrow: `*`, `?` and `[` are glob metacharacters in `SCAN MATCH`, and
    /// `/` is our own separator — either would make a prefix scan match keys
    /// it does not own, or miss keys it does.
    pub fn new(namespace: &str) -> Result<Self> {
        if namespace.is_empty() {
            bail!("Valkey namespace must not be empty");
        }
        if namespace.len() > MAX_NAMESPACE_LEN {
            bail!(
                "Valkey namespace {namespace:?} is longer than {MAX_NAMESPACE_LEN} characters; \
                 it rides on every key"
            );
        }
        if let Some(bad) = namespace
            .chars()
            .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-')))
        {
            bail!(
                "Valkey namespace {namespace:?} contains {bad:?}; only \
                 letters, digits, and _ . : - are allowed (a glob or separator \
                 character would break prefix scans)"
            );
        }
        Ok(Self {
            namespace: Arc::from(namespace),
        })
    }

    /// Resolve the namespace from an explicit setting (a CLI flag), else
    /// [`NAMESPACE_ENV`], else [`DEFAULT_NAMESPACE`].
    pub fn resolve(explicit: Option<&str>) -> Result<Self> {
        let from_env = std::env::var(NAMESPACE_ENV).ok();
        let chosen = explicit
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .or_else(|| from_env.as_deref().map(str::trim).filter(|s| !s.is_empty()))
            .unwrap_or(DEFAULT_NAMESPACE);
        Self::new(chosen)
    }

    /// The namespace, without any separator.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Prefix a **logical** lease key (e.g. `lease/retention`), as produced by
    /// the Valkey-agnostic `scry-cluster`.
    pub fn lease(&self, logical_key: &str) -> String {
        format!("{}/{logical_key}", self.namespace)
    }

    /// The pub/sub channel block events for `signal` are published on.
    pub fn blocks_channel(&self, signal: &str) -> String {
        format!("{}/blocks/{signal}", self.namespace)
    }

    /// Prefix of the tail-address registry (D-053).
    pub fn tail_prefix(&self) -> String {
        format!("{}/tail/ingesters/", self.namespace)
    }

    /// The tail-registry key for one ingester.
    pub fn tail(&self, writer_uuid: Uuid) -> String {
        format!("{}{writer_uuid}", self.tail_prefix())
    }

    /// Prefix of the staged-deletions registry (D-063).
    pub fn staged_prefix(&self) -> String {
        format!("{}/deleted/", self.namespace)
    }

    /// The staged-deletions key for one block.
    pub fn staged(&self, block_uuid: Uuid) -> String {
        format!("{}{block_uuid}", self.staged_prefix())
    }

    /// Recover a block uuid from a staged-deletions key. `None` for a key that
    /// is not ours (another namespace, another registry) or whose suffix does
    /// not parse — a `SCAN` can only ever narrow, never guarantee.
    pub fn staged_uuid(&self, key: &str) -> Option<Uuid> {
        key.strip_prefix(&self.staged_prefix())?.parse().ok()
    }

    /// Prefix of the fleet status registry (D-057).
    pub fn status_prefix(&self) -> String {
        format!("{}/status/", self.namespace)
    }

    /// The status key for one instance id (already-encoded).
    pub fn status(&self, instance_id: &str) -> String {
        format!("{}{instance_id}", self.status_prefix())
    }

    /// The owner-fence key paired with [`status`](Self::status).
    pub fn status_owner(&self, instance_id: &str) -> String {
        format!("{}/status-owner/{instance_id}", self.namespace)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_namespace_reproduces_the_historical_keys() {
        let k = Keyspace::default();
        assert_eq!(k.lease("lease/retention"), "scry/lease/retention");
        assert_eq!(k.blocks_channel("logs"), "scry/blocks/logs");
        assert_eq!(k.tail_prefix(), "scry/tail/ingesters/");
        assert_eq!(k.staged_prefix(), "scry/deleted/");
        assert_eq!(k.status_prefix(), "scry/status/");
        assert_eq!(k.status_owner("a"), "scry/status-owner/a");
    }

    #[test]
    fn two_namespaces_share_no_key() {
        let a = Keyspace::new("prod").unwrap();
        let b = Keyspace::new("staging").unwrap();
        let uuid = Uuid::now_v7();
        assert_ne!(a.lease("lease/retention"), b.lease("lease/retention"));
        assert_ne!(a.blocks_channel("logs"), b.blocks_channel("logs"));
        assert_ne!(a.staged(uuid), b.staged(uuid));
        assert_ne!(a.tail(uuid), b.tail(uuid));
        assert_ne!(a.status("i"), b.status("i"));
    }

    #[test]
    fn staged_key_round_trips_and_rejects_foreign_keys() {
        let k = Keyspace::new("prod").unwrap();
        let uuid = Uuid::now_v7();
        assert_eq!(k.staged_uuid(&k.staged(uuid)), Some(uuid));
        // Another deployment's entry must not be adopted as ours.
        let other = Keyspace::new("staging").unwrap();
        assert_eq!(k.staged_uuid(&other.staged(uuid)), None);
        // Nor another registry's, nor a malformed suffix.
        assert_eq!(k.staged_uuid(&k.tail(uuid)), None);
        assert_eq!(k.staged_uuid("prod/deleted/not-a-uuid"), None);
    }

    #[test]
    fn glob_and_separator_characters_are_rejected() {
        for bad in ["", "pro*d", "prod/sub", "pro?d", "pro[d]", "prod key", "🙂"] {
            assert!(
                Keyspace::new(bad).is_err(),
                "namespace {bad:?} must be rejected"
            );
        }
        assert!(Keyspace::new(&"x".repeat(MAX_NAMESPACE_LEN + 1)).is_err());
        for ok in ["scry", "prod-1", "team.a", "ns:2", "A_b"] {
            assert!(
                Keyspace::new(ok).is_ok(),
                "namespace {ok:?} must be allowed"
            );
        }
    }

    #[test]
    fn resolve_prefers_the_explicit_setting_and_ignores_blanks() {
        assert_eq!(Keyspace::resolve(Some("prod")).unwrap().namespace(), "prod");
        assert_eq!(
            Keyspace::resolve(Some("  prod ")).unwrap().namespace(),
            "prod"
        );
        // A blank flag is "unset", not an invalid namespace.
        assert_eq!(
            Keyspace::resolve(Some("   ")).unwrap().namespace(),
            DEFAULT_NAMESPACE
        );
    }
}
