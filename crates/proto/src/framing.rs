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
    use crate::QueryFrameMsg;

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
    async fn roundtrip_agent_status() {
        let mut buf = Vec::new();
        let frame = build::agent_status(build::AgentStatusArgs {
            session_id: 42,
            sequence: 7,
            snapshot_json: r#"{"role":"agent"}"#,
        });
        write_frame(&mut buf, &frame).await.unwrap();
        assert_eq!(buf[4], 0x21, "AgentStatus keeps its assigned wire tag");

        let mut cursor = std::io::Cursor::new(buf);
        let back: Frame = read_frame(&mut cursor).await.unwrap();
        match back.msg {
            FrameMsg::AgentStatus(s) => {
                assert_eq!(s.session_id, 42);
                assert_eq!(s.sequence, 7);
                assert_eq!(s.snapshot_json, r#"{"role":"agent"}"#);
            }
            other => panic!("expected AgentStatus, got {:?}", other),
        }
    }

    /// D-065: a metric sample rides its own frame rather than a widened
    /// `TailRecord`. The value is an f64 on the wire, so this pins both the
    /// assigned tag and that the float survives the round trip bit-for-bit
    /// (a `f64` compared with `==` — any lossy re-encode would show up here).
    #[tokio::test]
    async fn roundtrip_tail_sample() {
        let mut buf = Vec::new();
        let frame = build::tail_sample(build::TailSampleArgs {
            signal: crate::constants::Signal::Metrics as u8,
            ts_unix_nano: 1_700_000_000_123_456_789,
            metric_type: 2,
            series_fingerprint: 0x0123_4567_89AB_CDEF,
            // Not representable in binary32 — catches an f32 narrowing.
            value: 0.1 + 0.2,
            labels: vec![
                crate::generated::LabelPair {
                    key: "__name__".to_string(),
                    value: "http_requests_total".to_string(),
                },
                crate::generated::LabelPair {
                    key: "job".to_string(),
                    value: "api".to_string(),
                },
            ],
        });
        write_frame(&mut buf, &frame).await.unwrap();
        assert_eq!(buf[4], 0x54, "TailSample keeps its assigned wire tag");

        let mut cursor = std::io::Cursor::new(buf);
        let back: Frame = read_frame(&mut cursor).await.unwrap();
        match back.msg {
            FrameMsg::TailSample(s) => {
                assert_eq!(s.signal, crate::constants::Signal::Metrics as u8);
                assert_eq!(s.ts_unix_nano, 1_700_000_000_123_456_789);
                assert_eq!(s.metric_type, 2);
                assert_eq!(s.series_fingerprint, 0x0123_4567_89AB_CDEF);
                assert_eq!(s.value, 0.1 + 0.2);
                assert_eq!(s.labels.len(), 2);
                assert_eq!(s.labels[0].key, "__name__");
                assert_eq!(s.labels[0].value, "http_requests_total");
                assert_eq!(s.labels[1].key, "job");
            }
            other => panic!("expected TailSample, got {:?}", other),
        }
    }

    /// The union is decoded by trying variants in order, each guarded by its
    /// const tag byte. Adding `TailSample` (0x54) next to `TailRecord` (0x51)
    /// must not make a logs record decode as a sample or vice versa.
    #[tokio::test]
    async fn tail_record_and_tail_sample_do_not_alias() {
        let mut buf = Vec::new();
        write_frame(
            &mut buf,
            &build::tail_record(build::TailRecordArgs {
                signal: crate::constants::Signal::Logs as u8,
                ts_unix_nano: 5,
                severity: 9,
                labels: vec![],
                body: "hello".to_string(),
                attributes: vec![],
            }),
        )
        .await
        .unwrap();
        let mut cursor = std::io::Cursor::new(buf);
        let back: Frame = read_frame(&mut cursor).await.unwrap();
        assert!(
            matches!(back.msg, FrameMsg::TailRecord(ref r) if r.body == "hello"),
            "a TailRecord must still decode as a TailRecord, got {:?}",
            back.msg
        );
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
            // Empty = absent (logs-only full-text substring).
            body_contains: String::new(),
            // 0 = blocks-only (no merged history+live view).
            live: 0,
            // 0 = fingerprint-only (no opt-in metrics label join).
            with_labels: 0,
            capabilities: crate::constants::QUERY_CAP_ATTEMPT_SUPERSESSION,
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

    #[tokio::test]
    async fn query_superseded_frame_round_trips_with_assigned_tag() {
        let frame = QueryFrame {
            msg: QueryFrameMsg::ResponseSuperseded(
                crate::ResponseSupersededInput {
                    superseded_attempt: 3,
                    next_attempt: 4,
                    reason: crate::constants::QUERY_SUPERSEDED_REASON_SUPERSEDED_BLOCK_DISAPPEARED,
                }
                .into(),
            ),
        };
        let mut buf = Vec::new();
        write_frame(&mut buf, &frame).await.unwrap();
        assert_eq!(buf[4], 0x12);
        let mut cursor = std::io::Cursor::new(buf);
        match read_frame::<QueryFrame, _>(&mut cursor).await.unwrap().msg {
            QueryFrameMsg::ResponseSuperseded(reset) => {
                assert_eq!(reset.superseded_attempt, 3);
                assert_eq!(reset.next_attempt, 4);
                assert_eq!(reset.reason, 1);
            }
            other => panic!("expected ResponseSuperseded, got {other:?}"),
        }
    }

    /// A `QueryStats` frame carries every phase plus the live fan-out list, and
    /// sits *before* `EndOfStream` without disturbing it — a client's
    /// "read until terminator" loop must still see exactly one terminator, and
    /// must see it last.
    #[tokio::test]
    async fn query_stats_frame_round_trips_ahead_of_the_terminator() {
        let stats = crate::QueryStatsInput {
            server_total_us: 9_868_300,
            admission_wait_us: 5_600_000,
            catalog_us: 1_200,
            cache_lookup_us: 40,
            live_fetch_us: 0,
            register_us: 250_000,
            plan_us: 3_100,
            execute_us: 3_900_000,
            serialize_us: 12_000,
            write_us: 800,
            postings_fetch_us: 210_000,
            bloom_fetch_us: 0,
            // Summed across DataFusion partitions, so this legitimately exceeds
            // `execute_us`. It is a detail metric, never a timeline slice.
            df_opening_us: 5_500_000,
            df_scanning_us: 7_100_000,
            df_compute_us: 900_000,
            cache_hit: 0,
            attempts: 1,
            blocks_considered: 27,
            blocks_scanned: 27,
            bytes_scanned: 680_000,
            node_id: "queryd-0".into(),
            live_nodes: vec![
                crate::LiveNodeTiming {
                    addr: "10.0.0.1:4000".into(),
                    elapsed_us: 4_200,
                    rows: 13,
                    ok: 1,
                },
                crate::LiveNodeTiming {
                    addr: "10.0.0.2:4000".into(),
                    elapsed_us: 1_000_000,
                    rows: 0,
                    ok: 0,
                },
            ],
        };

        let mut buf = Vec::new();
        write_frame(
            &mut buf,
            &QueryFrame {
                msg: QueryFrameMsg::QueryStats(stats.clone().into()),
            },
        )
        .await
        .unwrap();
        // Tag byte lives right after the u32 length prefix.
        assert_eq!(buf[4], 0x1E);

        write_frame(
            &mut buf,
            &QueryFrame {
                msg: QueryFrameMsg::EndOfStream(crate::EndOfStreamInput { total_rows: 13 }.into()),
            },
        )
        .await
        .unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        match read_frame::<QueryFrame, _>(&mut cursor).await.unwrap().msg {
            QueryFrameMsg::QueryStats(s) => {
                assert_eq!(s.server_total_us, 9_868_300);
                assert_eq!(s.admission_wait_us, 5_600_000);
                assert_eq!(s.execute_us, 3_900_000);
                assert_eq!(s.postings_fetch_us, 210_000);
                assert_eq!(s.df_scanning_us, 7_100_000);
                assert_eq!(s.cache_hit, 0);
                assert_eq!(s.blocks_considered, 27);
                assert_eq!(s.bytes_scanned, 680_000);
                assert_eq!(s.node_id, "queryd-0");
                assert_eq!(s.live_nodes.len(), 2);
                assert_eq!(s.live_nodes[0].addr, "10.0.0.1:4000");
                assert_eq!(s.live_nodes[0].rows, 13);
                assert_eq!(s.live_nodes[1].ok, 0);
            }
            other => panic!("expected QueryStats, got {other:?}"),
        }
        match read_frame::<QueryFrame, _>(&mut cursor).await.unwrap().msg {
            QueryFrameMsg::EndOfStream(eos) => assert_eq!(eos.total_rows, 13),
            other => panic!("expected EndOfStream after QueryStats, got {other:?}"),
        }
    }

    /// An empty `live_nodes` array is the common case (a non-live query) and
    /// must not be confused with the frame ending early.
    #[tokio::test]
    async fn query_stats_frame_round_trips_with_no_live_nodes() {
        let stats = crate::QueryStatsInput {
            server_total_us: 2_100,
            admission_wait_us: 30,
            catalog_us: 900,
            cache_lookup_us: 60,
            live_fetch_us: 0,
            register_us: 0,
            plan_us: 0,
            execute_us: 0,
            serialize_us: 0,
            write_us: 1_000,
            postings_fetch_us: 0,
            bloom_fetch_us: 0,
            df_opening_us: 0,
            df_scanning_us: 0,
            df_compute_us: 0,
            // A cache hit: it reports its own small numbers, not the
            // originating miss's — which is the whole reason this is a
            // separate frame rather than fields on the cached terminator.
            cache_hit: 1,
            attempts: 1,
            blocks_considered: 0,
            blocks_scanned: 0,
            bytes_scanned: 0,
            node_id: String::new(),
            live_nodes: Vec::new(),
        };

        let mut buf = Vec::new();
        write_frame(
            &mut buf,
            &QueryFrame {
                msg: QueryFrameMsg::QueryStats(stats.into()),
            },
        )
        .await
        .unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        match read_frame::<QueryFrame, _>(&mut cursor).await.unwrap().msg {
            QueryFrameMsg::QueryStats(s) => {
                assert_eq!(s.cache_hit, 1);
                assert_eq!(s.server_total_us, 2_100);
                assert!(s.live_nodes.is_empty());
                assert_eq!(s.node_id, "");
            }
            other => panic!("expected QueryStats, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn query_fleet_status_frames_round_trip() {
        let request = QueryFrame {
            msg: QueryFrameMsg::FleetStatusRequest(crate::FleetStatusRequestInput {}.into()),
        };
        let response = QueryFrame {
            msg: QueryFrameMsg::FleetStatusResponse(
                crate::FleetStatusResponseInput {
                    instances_json: vec!["{\"role\":\"agent\"}".into()],
                }
                .into(),
            ),
        };

        let mut buf = Vec::new();
        write_frame(&mut buf, &request).await.unwrap();
        write_frame(&mut buf, &response).await.unwrap();
        let mut cursor = std::io::Cursor::new(buf);
        assert!(matches!(
            read_frame::<QueryFrame, _>(&mut cursor).await.unwrap().msg,
            QueryFrameMsg::FleetStatusRequest(_)
        ));
        match read_frame::<QueryFrame, _>(&mut cursor).await.unwrap().msg {
            QueryFrameMsg::FleetStatusResponse(status) => {
                assert_eq!(status.instances_json, ["{\"role\":\"agent\"}"]);
            }
            other => panic!("expected FleetStatusResponse, got {other:?}"),
        }
    }
}
