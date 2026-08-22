//! Bounded decoding for ingest `Batch.payload` bytes.
//!
//! The sender-declared `uncompressed_size` is useful for admission control, but
//! it is not trustworthy: a tiny compressed payload can expand far beyond it.
//! Decode through a hard byte limit and require the declaration to match the
//! actual output before handing bytes to a schema decoder.

use std::io::Read;

use crate::constants::{COMPRESSION_NONE, COMPRESSION_ZSTD};

/// Why an ingest payload could not be safely decoded.
#[derive(Debug, thiserror::Error)]
pub enum PayloadDecodeError {
    #[error("decoded payload exceeds the {max_bytes}-byte limit")]
    TooLarge { max_bytes: usize },
    #[error("uncompressed_size mismatch: declared {declared}, actual {actual}")]
    SizeMismatch { declared: usize, actual: usize },
    #[error("unknown compression codec {0}")]
    UnknownCompression(u8),
    #[error("zstd decompression failed: {0}")]
    Zstd(#[source] std::io::Error),
}

/// Decode a batch payload with a hard output bound and validate its declared
/// uncompressed size.
///
/// At most `max_bytes + 1` bytes are ever accumulated; the extra byte detects
/// an oversized stream without trusting `declared_size`.
pub fn decode_batch_payload(
    payload: &[u8],
    compression: u8,
    declared_size: u32,
    max_bytes: usize,
) -> Result<Vec<u8>, PayloadDecodeError> {
    let declared = declared_size as usize;
    if declared > max_bytes {
        return Err(PayloadDecodeError::TooLarge { max_bytes });
    }

    let decoded = match compression {
        COMPRESSION_NONE => {
            if payload.len() > max_bytes {
                return Err(PayloadDecodeError::TooLarge { max_bytes });
            }
            payload.to_vec()
        }
        COMPRESSION_ZSTD => {
            let decoder =
                zstd::stream::read::Decoder::new(payload).map_err(PayloadDecodeError::Zstd)?;
            let mut limited = decoder.take(max_bytes as u64 + 1);
            let mut out = Vec::with_capacity(declared.min(max_bytes));
            limited
                .read_to_end(&mut out)
                .map_err(PayloadDecodeError::Zstd)?;
            if out.len() > max_bytes {
                return Err(PayloadDecodeError::TooLarge { max_bytes });
            }
            out
        }
        other => return Err(PayloadDecodeError::UnknownCompression(other)),
    };

    if decoded.len() != declared {
        return Err(PayloadDecodeError::SizeMismatch {
            declared,
            actual: decoded.len(),
        });
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_valid_zstd_within_limit() {
        let raw = b"bounded payload";
        let compressed = zstd::encode_all(raw.as_slice(), 1).unwrap();
        assert_eq!(
            decode_batch_payload(&compressed, COMPRESSION_ZSTD, raw.len() as u32, 1024).unwrap(),
            raw
        );
    }

    #[test]
    fn rejects_zstd_expansion_past_limit() {
        let raw = vec![b'x'; 4096];
        let compressed = zstd::encode_all(raw.as_slice(), 1).unwrap();
        assert!(compressed.len() < 128);
        assert!(matches!(
            decode_batch_payload(&compressed, COMPRESSION_ZSTD, 32, 64),
            Err(PayloadDecodeError::TooLarge { max_bytes: 64 })
        ));
    }

    #[test]
    fn rejects_declared_size_mismatch() {
        assert!(matches!(
            decode_batch_payload(b"abc", COMPRESSION_NONE, 2, 64),
            Err(PayloadDecodeError::SizeMismatch {
                declared: 2,
                actual: 3
            })
        ));
    }

    #[test]
    fn rejects_unknown_compression() {
        assert!(matches!(
            decode_batch_payload(b"", 99, 0, 64),
            Err(PayloadDecodeError::UnknownCompression(99))
        ));
    }
}
