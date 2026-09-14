//! Deterministic issue identity derived from deployment, application, and
//! fingerprint — not a random ID.

use sha2::{Digest, Sha256};

const ISSUE_DOMAIN_TAG: &[u8] = b"scry.issue.identity.v1\0";

/// A 16-byte deterministic issue identifier. Compact enough for display and
/// indexing; collision-authoritative checks use the full fingerprint digest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IssueId(pub [u8; 16]);

impl IssueId {
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl std::fmt::Display for IssueId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", uuid::Uuid::from_bytes(self.0))
    }
}

/// Derive a deterministic issue ID from deployment, application, and fingerprint.
///
/// The identity is `SHA-256(domain_tag || deployment_id || app_identity_digest ||
/// fingerprint_version || fingerprint_digest)[..16]`.
pub fn derive_issue_id(
    deployment_id: &[u8; 16],
    app_identity_digest: &[u8; 32],
    fingerprint_version: u16,
    fingerprint_digest: &[u8; 32],
) -> IssueId {
    let mut hasher = Sha256::new();
    hasher.update(ISSUE_DOMAIN_TAG);
    hasher.update(deployment_id);
    hasher.update(app_identity_digest);
    hasher.update(fingerprint_version.to_be_bytes());
    hasher.update(fingerprint_digest);
    let full: [u8; 32] = hasher.finalize().into();
    let mut id = [0_u8; 16];
    id.copy_from_slice(&full[..16]);
    IssueId(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_and_distinct() {
        let deployment = [0x11_u8; 16];
        let app = [0x22_u8; 32];
        let fp = [0x33_u8; 32];

        let id1 = derive_issue_id(&deployment, &app, 1, &fp);
        let id2 = derive_issue_id(&deployment, &app, 1, &fp);
        assert_eq!(id1, id2, "same inputs produce same ID");

        // Different fingerprint → different issue
        let fp2 = [0x44_u8; 32];
        let id3 = derive_issue_id(&deployment, &app, 1, &fp2);
        assert_ne!(id1, id3);

        // Different app → different issue
        let app2 = [0x55_u8; 32];
        let id4 = derive_issue_id(&deployment, &app2, 1, &fp);
        assert_ne!(id1, id4);

        // Different deployment → different issue
        let deployment2 = [0x66_u8; 16];
        let id5 = derive_issue_id(&deployment2, &app, 1, &fp);
        assert_ne!(id1, id5);

        // Different fingerprint version → different issue
        let id6 = derive_issue_id(&deployment, &app, 2, &fp);
        assert_ne!(id1, id6);
    }

    #[test]
    fn display_is_uuid_formatted() {
        let id = IssueId([
            0x01, 0x8f, 0x1f, 0x8e, 0x7b, 0x2c, 0x7a, 0x91, 0x81, 0x23, 0x01, 0x23, 0x45, 0x67,
            0x89, 0xab,
        ]);
        let s = id.to_string();
        assert_eq!(s.len(), 36); // UUID format
        assert!(s.contains('-'));
    }
}
