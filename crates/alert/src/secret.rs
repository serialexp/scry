use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chacha20poly1305::{
    aead::{Aead, Payload},
    KeyInit, XChaCha20Poly1305, XNonce,
};
use rand::{rngs::OsRng, RngCore};
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, Zeroizing};

use crate::{LogicalSecretId, NotificationTargetId};

pub const SECRET_ENVELOPE_VERSION: u8 = 1;
pub const SECRET_KEY_BYTES: usize = 32;
pub const MAX_SECRET_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EncryptedSecretEnvelope {
    pub version: u8,
    pub key_id: String,
    pub nonce_base64url: String,
    pub ciphertext_base64url: String,
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum SecretCryptoError {
    #[error(
        "secret key must be exactly 43 canonical unpadded base64url characters encoding 32 bytes"
    )]
    InvalidKey,
    #[error("secret is empty or exceeds {MAX_SECRET_BYTES} bytes")]
    InvalidSecret,
    #[error("unsupported secret envelope version or malformed envelope")]
    InvalidEnvelope,
    #[error("no configured key matches envelope key id `{0}`")]
    UnknownKey(String),
    #[error("secret authentication failed")]
    Authentication,
}

#[derive(Zeroize)]
#[zeroize(drop)]
pub struct SecretKey {
    bytes: [u8; SECRET_KEY_BYTES],
}
impl SecretKey {
    pub fn parse_base64url(input: &str) -> Result<Self, SecretCryptoError> {
        if input.len() != 43 || input.contains('=') {
            return Err(SecretCryptoError::InvalidKey);
        }
        let decoded = Zeroizing::new(
            URL_SAFE_NO_PAD
                .decode(input)
                .map_err(|_| SecretCryptoError::InvalidKey)?,
        );
        if decoded.len() != SECRET_KEY_BYTES || URL_SAFE_NO_PAD.encode(decoded.as_slice()) != input
        {
            return Err(SecretCryptoError::InvalidKey);
        }
        let mut bytes = [0; SECRET_KEY_BYTES];
        bytes.copy_from_slice(decoded.as_slice());
        Ok(Self { bytes })
    }
}

pub struct SecretKeySlot {
    pub id: String,
    pub key: SecretKey,
}
pub struct SecretKeyring {
    current: SecretKeySlot,
    previous: Option<SecretKeySlot>,
}
impl SecretKeyring {
    pub fn new(
        current_id: String,
        current: SecretKey,
        previous: Option<(String, SecretKey)>,
    ) -> Self {
        Self {
            current: SecretKeySlot {
                id: current_id,
                key: current,
            },
            previous: previous.map(|(id, key)| SecretKeySlot { id, key }),
        }
    }
    pub fn current_key_id(&self) -> &str {
        &self.current.id
    }
    pub fn encrypt(
        &self,
        binding: &SecretBinding<'_>,
        plaintext: &[u8],
    ) -> Result<EncryptedSecretEnvelope, SecretCryptoError> {
        if plaintext.is_empty() || plaintext.len() > MAX_SECRET_BYTES {
            return Err(SecretCryptoError::InvalidSecret);
        }
        let mut nonce = [0u8; 24];
        OsRng.fill_bytes(&mut nonce);
        let aad = binding.aad();
        let cipher = XChaCha20Poly1305::new((&self.current.key.bytes).into());
        let ciphertext = cipher
            .encrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: plaintext,
                    aad: aad.as_bytes(),
                },
            )
            .map_err(|_| SecretCryptoError::Authentication)?;
        Ok(EncryptedSecretEnvelope {
            version: SECRET_ENVELOPE_VERSION,
            key_id: self.current.id.clone(),
            nonce_base64url: URL_SAFE_NO_PAD.encode(nonce),
            ciphertext_base64url: URL_SAFE_NO_PAD.encode(ciphertext),
        })
    }
    pub fn decrypt(
        &self,
        binding: &SecretBinding<'_>,
        envelope: &EncryptedSecretEnvelope,
    ) -> Result<Zeroizing<Vec<u8>>, SecretCryptoError> {
        if envelope.version != SECRET_ENVELOPE_VERSION {
            return Err(SecretCryptoError::InvalidEnvelope);
        }
        let slot = if envelope.key_id == self.current.id {
            &self.current
        } else if let Some(slot) = self.previous.as_ref().filter(|s| s.id == envelope.key_id) {
            slot
        } else {
            return Err(SecretCryptoError::UnknownKey(envelope.key_id.clone()));
        };
        let nonce = URL_SAFE_NO_PAD
            .decode(&envelope.nonce_base64url)
            .map_err(|_| SecretCryptoError::InvalidEnvelope)?;
        let ciphertext = URL_SAFE_NO_PAD
            .decode(&envelope.ciphertext_base64url)
            .map_err(|_| SecretCryptoError::InvalidEnvelope)?;
        if nonce.len() != 24
            || URL_SAFE_NO_PAD.encode(&nonce) != envelope.nonce_base64url
            || URL_SAFE_NO_PAD.encode(&ciphertext) != envelope.ciphertext_base64url
        {
            return Err(SecretCryptoError::InvalidEnvelope);
        }
        let aad = binding.aad();
        let cipher = XChaCha20Poly1305::new((&slot.key.bytes).into());
        let plaintext = cipher
            .decrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: &ciphertext,
                    aad: aad.as_bytes(),
                },
            )
            .map_err(|_| SecretCryptoError::Authentication)?;
        if plaintext.is_empty() || plaintext.len() > MAX_SECRET_BYTES {
            return Err(SecretCryptoError::InvalidSecret);
        }
        Ok(Zeroizing::new(plaintext))
    }
}

pub struct SecretBinding<'a> {
    pub deployment_id: &'a str,
    pub target_id: NotificationTargetId,
    pub logical_secret_id: LogicalSecretId,
    pub generation: u64,
}
impl SecretBinding<'_> {
    fn aad(&self) -> String {
        format!(
            "scry-alert-secret-v1\0{}\0{}\0{}\0{}",
            self.deployment_id, self.target_id, self.logical_secret_id, self.generation
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn key(byte: u8) -> SecretKey {
        SecretKey::parse_base64url(&URL_SAFE_NO_PAD.encode([byte; 32])).unwrap()
    }
    fn binding<'a>(deployment: &'a str) -> SecretBinding<'a> {
        SecretBinding {
            deployment_id: deployment,
            target_id: NotificationTargetId(Uuid::nil()),
            logical_secret_id: LogicalSecretId(Uuid::from_u128(1)),
            generation: 2,
        }
    }
    use uuid::Uuid;
    #[test]
    fn strict_key_parsing() {
        assert!(
            SecretKey::parse_base64url(&format!("{}=", URL_SAFE_NO_PAD.encode([1; 32]))).is_err()
        );
        assert!(SecretKey::parse_base64url("not-a-key").is_err());
    }
    #[test]
    fn roundtrip_and_aad_binding() {
        let ring = SecretKeyring::new("k2".into(), key(2), None);
        let e = ring.encrypt(&binding("d1"), b"secret").unwrap();
        assert_eq!(&*ring.decrypt(&binding("d1"), &e).unwrap(), b"secret");
        assert_eq!(
            ring.decrypt(&binding("d2"), &e).unwrap_err(),
            SecretCryptoError::Authentication
        );
    }
    #[test]
    fn every_aad_component_is_authenticated_and_ciphertext_tampering_fails() {
        let ring = SecretKeyring::new("k".into(), key(3), None);
        let original = binding("deployment");
        let mut envelope = ring.encrypt(&original, b"secret").unwrap();
        let other_target = SecretBinding {
            target_id: NotificationTargetId(Uuid::from_u128(4)),
            ..original
        };
        assert_eq!(
            ring.decrypt(&other_target, &envelope).unwrap_err(),
            SecretCryptoError::Authentication
        );
        let other_logical = SecretBinding {
            logical_secret_id: LogicalSecretId(Uuid::from_u128(5)),
            ..original
        };
        assert_eq!(
            ring.decrypt(&other_logical, &envelope).unwrap_err(),
            SecretCryptoError::Authentication
        );
        let other_generation = SecretBinding {
            generation: 3,
            ..original
        };
        assert_eq!(
            ring.decrypt(&other_generation, &envelope).unwrap_err(),
            SecretCryptoError::Authentication
        );
        let replacement = if envelope.ciphertext_base64url.starts_with('A') {
            "B"
        } else {
            "A"
        };
        envelope
            .ciphertext_base64url
            .replace_range(0..1, replacement);
        assert_eq!(
            ring.decrypt(&original, &envelope).unwrap_err(),
            SecretCryptoError::Authentication
        );
    }

    #[test]
    fn previous_key_decrypts() {
        let old = SecretKeyring::new("k1".into(), key(1), None);
        let e = old.encrypt(&binding("d"), b"secret").unwrap();
        let rotated = SecretKeyring::new("k2".into(), key(2), Some(("k1".into(), key(1))));
        assert_eq!(&*rotated.decrypt(&binding("d"), &e).unwrap(), b"secret");
    }
}
