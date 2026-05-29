//! Length-prefixed framing over an async byte stream.
//!
//! Every wire message is `[len: u32 big-endian][body bytes]`. `len`
//! covers the body bytes; the length prefix itself is not included.
//! Same framing for both the ingest [`Frame`] and the query
//! [`QueryFrame`] — the [`Framed`] trait abstracts the encode/decode
//! pair so the helpers are generic over the framed type.
//!
//! No silent truncation: a `len` above [`MAX_FRAME_BYTES`] causes the
//! reader to error out before allocating, so a corrupt or malicious
//! peer cannot trick us into reserving gigabytes.

use crate::generated::Frame;
use crate::generated_query::QueryFrame;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Hard ceiling on a single framed message (32 MiB). Larger than the
/// schema's `DEFAULT_MAX_BATCH_BYTES` (16 MiB) so a server is free to
/// negotiate up to 16 MiB while still rejecting clearly-bogus framing.
/// The same ceiling applies to query response frames — an Arrow IPC
/// record-batch larger than 32 MiB would be a planner anomaly we'd
/// rather see fail loudly than silently fragment.
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

/// Encode/decode pair the framing helpers operate on. Both binschema-
/// generated top-level types (`Frame`, `QueryFrame`) already expose
/// these methods; the trait is pure indirection so a single pair of
/// `read_frame` / `write_frame` helpers serves both protocols.
///
/// Generic over `T: Framed` rather than `dyn Framed` so the trait
/// stays object-unsafe-friendly and the generated methods inline at
/// the call site.
pub trait Framed: Sized {
    fn encode(&self) -> binschema_runtime::Result<Vec<u8>>;
    fn decode(bytes: &[u8]) -> binschema_runtime::Result<Self>;
}

impl Framed for Frame {
    fn encode(&self) -> binschema_runtime::Result<Vec<u8>> {
        Frame::encode(self)
    }
    fn decode(bytes: &[u8]) -> binschema_runtime::Result<Self> {
        Frame::decode(bytes)
    }
}

impl Framed for QueryFrame {
    fn encode(&self) -> binschema_runtime::Result<Vec<u8>> {
        QueryFrame::encode(self)
    }
    fn decode(bytes: &[u8]) -> binschema_runtime::Result<Self> {
        QueryFrame::decode(bytes)
    }
}

/// Read one frame from `r`. Returns the decoded `T` on success.
///
/// Returns an `Io` error with [`std::io::ErrorKind::UnexpectedEof`] on
/// graceful peer close *before* a length prefix has been read; readers
/// that want to distinguish "clean close" from "broken peer" should
/// match on that case.
pub async fn read_frame<T: Framed, R: AsyncRead + Unpin>(r: &mut R) -> Result<T, FrameError> {
    let len = r.read_u32().await? as usize;
    if len > MAX_FRAME_BYTES {
        return Err(FrameError::TooLarge {
            got: len,
            max: MAX_FRAME_BYTES,
        });
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    T::decode(&buf).map_err(FrameError::Decode)
}

/// Write one frame to `w`. Caller is responsible for flushing; this
/// does not flush so callers can batch multiple frames into one syscall
/// via `BufWriter`.
pub async fn write_frame<T: Framed, W: AsyncWrite + Unpin>(
    w: &mut W,
    frame: &T,
) -> Result<(), FrameError> {
    let bytes = T::encode(frame).map_err(FrameError::Encode)?;
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(FrameError::TooLarge {
            got: bytes.len(),
            max: MAX_FRAME_BYTES,
        });
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
        let back: Frame = read_frame(&mut cursor).await.unwrap();
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
        let err = read_frame::<Frame, _>(&mut cursor).await.unwrap_err();
        match err {
            FrameError::TooLarge { got, max } => {
                assert_eq!(got, MAX_FRAME_BYTES + 1);
                assert_eq!(max, MAX_FRAME_BYTES);
            }
            other => panic!("expected TooLarge, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn roundtrip_query_request() {
        use crate::generated_query::{Matcher, QueryFrameMsg, QueryRequestInput};

        let req = QueryRequestInput {
            // v0.4: explicit signal byte (1 = metrics).
            signal: crate::constants::Signal::Metrics as u8,
            matchers: vec![Matcher {
                name: "__name__".into(),
                value: "scry_http_requests_total".into(),
            }],
            ts_min_present: 0,
            ts_min: 0,
            ts_max_present: 1,
            ts_max: 1_700_000_000_000_000_000,
            sql: String::new(),
            limit: 0,
            request_id: String::new(),
            // Empty = absent (traces-only by-id lookup).
            trace_id: Vec::new(),
        };
        let frame = QueryFrame {
            msg: QueryFrameMsg::QueryRequest(req.clone().into()),
        };

        let mut buf = Vec::new();
        write_frame(&mut buf, &frame).await.unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let back: QueryFrame = read_frame(&mut cursor).await.unwrap();
        match back.msg {
            QueryFrameMsg::QueryRequest(q) => {
                assert_eq!(q.matchers.len(), 1);
                assert_eq!(q.matchers[0].name, "__name__");
                assert_eq!(q.matchers[0].value, "scry_http_requests_total");
                assert_eq!(q.ts_min_present, 0);
                assert_eq!(q.ts_max_present, 1);
                assert_eq!(q.ts_max, 1_700_000_000_000_000_000);
                assert_eq!(q.limit, 0);
            }
            other => panic!("expected QueryRequest, got {:?}", other),
        }
    }
}
