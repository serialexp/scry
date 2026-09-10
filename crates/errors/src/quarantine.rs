//! Deterministic values used to mirror occurrence identity collisions locally.
//!
//! Quarantine persistence in this crate is rebuildable SQLite state. A future
//! object-store quarantine publisher can consume these values without changing the
//! fold's definition; it is deliberately not implemented here.

use sha2::{Digest, Sha256};

const COLLISION_DOMAIN: &[u8] = b"scry.error.occurrence-collision.v1\0";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollisionMirror<'a> {
    pub deployment_id: &'a [u8; 16],
    pub app_id: &'a [u8; 16],
    pub event_id: &'a [u8; 16],
    pub winner_sha256: &'a [u8; 32],
    pub winner_canonical: &'a [u8],
    pub contender_sha256: &'a [u8; 32],
    pub contender_canonical: &'a [u8],
}

/// Stable identity for one exact winner/contender pair.
///
/// Length prefixes make the digest type-injective. Exact bytes remain mandatory in
/// the mirror table; this digest is only an index and idempotency accelerator.
pub fn collision_id(collision: &CollisionMirror<'_>) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(COLLISION_DOMAIN);
    hash.update(collision.deployment_id);
    hash.update(collision.app_id);
    hash.update(collision.event_id);
    hash.update(collision.winner_sha256);
    hash_len_bytes(&mut hash, collision.winner_canonical);
    hash.update(collision.contender_sha256);
    hash_len_bytes(&mut hash, collision.contender_canonical);
    hash.finalize().into()
}

fn hash_len_bytes(hash: &mut Sha256, bytes: &[u8]) {
    hash.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    hash.update(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collision_identity_includes_exact_bytes_not_only_digests() {
        let deployment = [1; 16];
        let app = [2; 16];
        let event = [3; 16];
        let digest = [4; 32];
        let first = CollisionMirror {
            deployment_id: &deployment,
            app_id: &app,
            event_id: &event,
            winner_sha256: &digest,
            winner_canonical: b"winner",
            contender_sha256: &digest,
            contender_canonical: b"one",
        };
        let second = CollisionMirror {
            contender_canonical: b"two",
            ..first.clone()
        };
        assert_ne!(collision_id(&first), collision_id(&second));
        assert_eq!(collision_id(&first), collision_id(&first));
    }
}
