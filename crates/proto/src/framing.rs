//! Length-prefixed framing over an async byte stream.
//!
//! Every wire message is `[len: u32 big-endian][Frame bytes]`. `len`
//! covers the Frame bytes; the length prefix itself is not included.
//!
//! No silent truncation: a `len` above [`MAX_FRAME_BYTES`] causes the
//! reader to error out before allocating, so a corrupt or malicious
//! peer cannot trick us into reserving gigabytes.

use crate::generated::Frame;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Hard ceiling on a single framed message (32 MiB). Larger than the
/// schema's `DEFAULT_MAX_BATCH_BYTES` (16 MiB) so a server is free to
/// negotiate up to 16 MiB while still rejecting clearly-bogus framing.
pub const MAX_FRAME_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum FrameError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("frame too large: {got} bytes, max {max}")]
    TooLarge { got: usize, max: usize },

    #[error("decode: {0}")]
    Decode(binschema_runtime::BinSchemaError),

    #[error("encode: {0}")]
    Encode(binschema_runtime::BinSchemaError),
}

/// Read one frame from `r`. Returns the decoded [`Frame`] on success.
///
/// Returns an `Io` error with [`std::io::ErrorKind::UnexpectedEof`] on
/// graceful peer close *before* a length prefix has been read; readers
/// that want to distinguish "clean close" from "broken peer" should
/// match on that case.
pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> Result<Frame, FrameError> {
    let len = r.read_u32().await? as usize;
    if len > MAX_FRAME_BYTES {
        return Err(FrameError::TooLarge { got: len, max: MAX_FRAME_BYTES });
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    Frame::decode(&buf).map_err(FrameError::Decode)
}

/// Write one frame to `w`. Caller is responsible for flushing; this
/// does not flush so callers can batch multiple frames into one syscall
/// via `BufWriter`.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    w: &mut W,
    frame: &Frame,
) -> Result<(), FrameError> {
    let bytes = frame.encode().map_err(FrameError::Encode)?;
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(FrameError::TooLarge { got: bytes.len(), max: MAX_FRAME_BYTES });
    }
    w.write_u32(bytes.len() as u32).await?;
    w.write_all(&bytes).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build;
    use crate::generated::FrameMsg;

    #[tokio::test]
    async fn roundtrip_ping() {
        let mut buf = Vec::new();
        let frame = build::ping(0xDEAD_BEEF_CAFE_F00D);
        write_frame(&mut buf, &frame).await.unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let back = read_frame(&mut cursor).await.unwrap();
        match back.msg {
            FrameMsg::Ping(p) => assert_eq!(p.nonce, 0xDEAD_BEEF_CAFE_F00D),
            other => panic!("expected Ping, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn rejects_oversized_frame() {
        let mut buf = Vec::new();
        // u32 length = MAX_FRAME_BYTES + 1, no body
        buf.extend_from_slice(&((MAX_FRAME_BYTES + 1) as u32).to_be_bytes());
        let mut cursor = std::io::Cursor::new(buf);
        let err = read_frame(&mut cursor).await.unwrap_err();
        match err {
            FrameError::TooLarge { got, max } => {
                assert_eq!(got, MAX_FRAME_BYTES + 1);
                assert_eq!(max, MAX_FRAME_BYTES);
            }
            other => panic!("expected TooLarge, got {:?}", other),
        }
    }
}
