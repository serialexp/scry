//! A hand-rolled ingest wire client that surfaces per-batch **ack status** and
//! **latency**.
//!
//! `scry_client::Client` deliberately hides ack outcomes — it collapses every
//! `BatchAck` to a bare inflight credit. The replay bench needs the opposite: it
//! must *see* `ACK_THROTTLED` / `ACK_REJECTED` (that's the knee signal driving
//! the auto-ramp) and measure ack latency. So this is the spewer's proven
//! connect → Hello → write-loop-with-inflight-flow-control pattern
//! (`crates/noise-spewer`), specialized to report each ack back to the caller.

use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use scry_proto::{
    build,
    constants::{COMPRESSION_ZSTD, GOODBYE_NORMAL, PROTOCOL_VERSION_V0, SIGNAL_BIT_LOGS},
    framing::{read_frame, write_frame},
    generated::FrameMsg,
    Frame, LabelPair,
};
use tokio::{
    io::{AsyncWriteExt, BufReader, BufWriter},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::mpsc,
    task::JoinHandle,
    time::Instant,
};
use tracing::info;

const ZSTD_LEVEL: i32 = 3;

/// One observed batch acknowledgement.
#[derive(Debug, Clone, Copy)]
pub struct Ack {
    pub batch_id: u64,
    /// `ACK_ACCEPTED` / `ACK_THROTTLED` / `ACK_REJECTED`.
    pub status: u8,
    pub latency: std::time::Duration,
}

/// A raw ack from the reader task (no latency yet — the send side owns the
/// send-time map).
struct RawAck {
    batch_id: u64,
    status: u8,
}

struct ConnectParams {
    target: String,
    agent_id: [u8; 16],
    hostname: String,
}

/// A connected ingest session that reports ack status + latency.
pub struct WireSender {
    params: ConnectParams,
    wr: BufWriter<OwnedWriteHalf>,
    ack_rx: mpsc::Receiver<RawAck>,
    reader: JoinHandle<()>,
    sent_at: HashMap<u64, Instant>,
    inflight: usize,
    max_inflight: usize,
    session_id: u64,
    next_batch_id: u64,
}

impl WireSender {
    pub async fn connect(target: &str, agent_id: [u8; 16], hostname: &str) -> Result<Self> {
        let params = ConnectParams {
            target: target.to_string(),
            agent_id,
            hostname: hostname.to_string(),
        };
        let est = Self::handshake(&params).await?;
        Ok(Self {
            params,
            wr: est.wr,
            ack_rx: est.ack_rx,
            reader: est.reader,
            sent_at: HashMap::new(),
            inflight: 0,
            max_inflight: est.max_inflight,
            session_id: est.session_id,
            next_batch_id: 0,
        })
    }

    async fn handshake(params: &ConnectParams) -> Result<Established> {
        let stream = TcpStream::connect(&params.target)
            .await
            .with_context(|| format!("connecting to {}", params.target))?;
        stream.set_nodelay(true)?;
        let (rd, wr) = stream.into_split();
        let mut rd = BufReader::new(rd);
        let mut wr = BufWriter::new(wr);

        write_frame(
            &mut wr,
            &build::hello(build::HelloArgs {
                protocol_version: PROTOCOL_VERSION_V0,
                agent_id: params.agent_id,
                agent_version: env!("CARGO_PKG_VERSION"),
                hostname: &params.hostname,
                signals: SIGNAL_BIT_LOGS,
                capabilities: 0,
                resource_attrs: vec![LabelPair {
                    key: "service".into(),
                    value: "scry-replay-opensearch".into(),
                }],
            }),
        )
        .await?;
        wr.flush().await?;

        let hello_ack = match read_frame::<Frame, _>(&mut rd).await?.msg {
            FrameMsg::HelloAck(a) => a,
            FrameMsg::Error(e) => {
                bail!(
                    "server rejected handshake: code={} msg={:?}",
                    e.code,
                    e.message
                )
            }
            other => bail!("expected HelloAck, got {other:?}"),
        };
        info!(
            session_id = hello_ack.session_id,
            max_inflight = hello_ack.max_inflight_batches,
            "replay handshake complete"
        );

        let session_id = hello_ack.session_id;
        let max_inflight = hello_ack.max_inflight_batches.max(1) as usize;
        let (ack_tx, ack_rx) = mpsc::channel::<RawAck>(4096);
        let reader = tokio::spawn(reader_loop(rd, ack_tx));

        Ok(Established {
            wr,
            ack_rx,
            reader,
            max_inflight,
            session_id,
        })
    }

    /// Re-establish the session against the (possibly restarted) server.
    pub async fn reconnect(&mut self) -> Result<()> {
        self.reader.abort();
        let est = Self::handshake(&self.params).await?;
        self.wr = est.wr;
        self.ack_rx = est.ack_rx;
        self.reader = est.reader;
        self.max_inflight = est.max_inflight;
        self.session_id = est.session_id;
        self.inflight = 0;
        self.sent_at.clear();
        Ok(())
    }

    pub fn max_inflight(&self) -> usize {
        self.max_inflight
    }

    pub fn inflight(&self) -> usize {
        self.inflight
    }

    /// Send one logs batch: zstd-compress `payload`, frame it, write it. Blocks
    /// on the inflight budget, draining acks (returned to the caller for stats)
    /// while it waits. Returns the acks reclaimed during the send.
    pub async fn send_logs_batch(
        &mut self,
        record_count: u32,
        ts_min: u64,
        ts_max: u64,
        payload: &[u8],
    ) -> Result<Vec<Ack>> {
        let mut acks = Vec::new();
        // Block for an inflight slot, collecting acks as they arrive.
        while self.inflight >= self.max_inflight {
            match self.ack_rx.recv().await {
                Some(raw) => acks.push(self.record_ack(raw)),
                None => bail!("ingest server closed connection (reader gone)"),
            }
        }
        // Opportunistically reclaim already-queued acks; surface a dead reader.
        loop {
            match self.ack_rx.try_recv() {
                Ok(raw) => acks.push(self.record_ack(raw)),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    bail!("ingest server closed connection (reader gone)")
                }
            }
        }

        let compressed =
            zstd::encode_all(payload, ZSTD_LEVEL).context("zstd-compressing logs batch payload")?;
        let batch_id = self.next_batch_id;
        self.next_batch_id += 1;
        let frame = build::batch(build::BatchArgs {
            session_id: self.session_id,
            batch_id,
            signal: SIGNAL_BIT_LOGS,
            ts_min_unix_nano: ts_min,
            ts_max_unix_nano: ts_max,
            record_count,
            compression: COMPRESSION_ZSTD,
            uncompressed_size: payload.len() as u32,
            payload: compressed,
        });
        write_frame(&mut self.wr, &frame).await?;
        self.wr.flush().await?;
        self.sent_at.insert(batch_id, Instant::now());
        self.inflight += 1;
        Ok(acks)
    }

    fn record_ack(&mut self, raw: RawAck) -> Ack {
        self.inflight = self.inflight.saturating_sub(1);
        let latency = self
            .sent_at
            .remove(&raw.batch_id)
            .map(|t| t.elapsed())
            .unwrap_or_default();
        Ack {
            batch_id: raw.batch_id,
            status: raw.status,
            latency,
        }
    }

    /// Wait for all outstanding acks to drain (used at end-of-stream before
    /// Goodbye), returning them for final stats.
    pub async fn drain(&mut self) -> Vec<Ack> {
        let mut acks = Vec::new();
        while self.inflight > 0 {
            match self.ack_rx.recv().await {
                Some(raw) => acks.push(self.record_ack(raw)),
                None => break,
            }
        }
        acks
    }

    /// Send a graceful Goodbye and wait for the reader to finish.
    pub async fn shutdown(mut self, reason: &str) -> Result<()> {
        write_frame(&mut self.wr, &build::goodbye(GOODBYE_NORMAL, reason)).await?;
        self.wr.flush().await?;
        drop(self.wr);
        let _ = self.reader.await;
        Ok(())
    }
}

struct Established {
    wr: BufWriter<OwnedWriteHalf>,
    ack_rx: mpsc::Receiver<RawAck>,
    reader: JoinHandle<()>,
    max_inflight: usize,
    session_id: u64,
}

/// Drain server frames, forwarding each `BatchAck` (with its status) to the
/// send side.
async fn reader_loop(mut rd: BufReader<OwnedReadHalf>, ack_tx: mpsc::Sender<RawAck>) {
    loop {
        match read_frame::<Frame, _>(&mut rd).await {
            Ok(f) => match f.msg {
                FrameMsg::BatchAck(a) => {
                    if ack_tx
                        .send(RawAck {
                            batch_id: a.batch_id,
                            status: a.status,
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                FrameMsg::Goodbye(_) | FrameMsg::Error(_) => break,
                _ => {}
            },
            Err(_) => break,
        }
    }
}
