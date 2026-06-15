//! Reusable wire-transport client for a scry ingest server.
//!
//! This is the signal-agnostic half of an ingest producer's main loop:
//! connect and handshake, a background reader task that turns each `BatchAck`
//! into an inflight credit, and `send_batch` with inflight flow control. Callers feed it
//! already-built `Frame`s (Batch variants); the client knows nothing about any
//! particular signal — the producer announces which signals it will send via the
//! `signals` bitmask at [`Client::connect`].
//!
//! Used by both `scry-agent` (logs) and `scry-gateway` (traces + profiles).

use anyhow::{bail, Context, Result};
use scry_proto::{
    build,
    constants::{ACK_ACCEPTED, GOODBYE_NORMAL, PROTOCOL_VERSION_V0},
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
};
use tracing::{info, warn};

/// The immutable parameters of a connection, retained so the session can be
/// re-established transparently after the server restarts or the link drops.
#[derive(Clone)]
struct ConnectParams {
    addr: String,
    agent_id: [u8; 16],
    hostname: String,
    signals: u8,
    resource_attrs: Vec<LabelPair>,
}

/// The per-connection pieces replaced wholesale on every (re)connect.
struct Established {
    wr: BufWriter<OwnedWriteHalf>,
    ack_rx: mpsc::Receiver<()>,
    max_inflight: usize,
    session_id: u64,
    reader: JoinHandle<()>,
}

/// A connected, handshaken ingest session. Drop or call [`Client::shutdown`]
/// to end it.
///
/// The client retains its [`ConnectParams`] so [`Client::reconnect`] can
/// re-run the handshake against a restarted server. Each reconnect yields a
/// **new** server-assigned `session_id`, which the server validates on every
/// batch — so callers must send via [`Client::send_batch_stamped`], which
/// stamps the current session id into the frame on each attempt.
pub struct Client {
    params: ConnectParams,
    wr: BufWriter<OwnedWriteHalf>,
    ack_rx: mpsc::Receiver<()>,
    max_inflight: usize,
    inflight: usize,
    session_id: u64,
    reader: JoinHandle<()>,
}

impl Client {
    /// Connect to `addr`, perform the Hello/HelloAck handshake announcing the
    /// given `signals` bitmask (`SIGNAL_BIT_*` from `scry_proto::constants`,
    /// OR-combined), and spawn the ack-draining reader task.
    pub async fn connect(
        addr: &str,
        agent_id: [u8; 16],
        hostname: &str,
        signals: u8,
        resource_attrs: Vec<LabelPair>,
    ) -> Result<Self> {
        let params = ConnectParams {
            addr: addr.to_string(),
            agent_id,
            hostname: hostname.to_string(),
            signals,
            resource_attrs,
        };
        let est = Self::handshake(&params).await?;
        Ok(Self {
            params,
            wr: est.wr,
            ack_rx: est.ack_rx,
            max_inflight: est.max_inflight,
            inflight: 0,
            session_id: est.session_id,
            reader: est.reader,
        })
    }

    /// Open a fresh TCP connection, run the Hello/HelloAck handshake, and spawn
    /// the ack-draining reader task. Used by both [`Client::connect`] and
    /// [`Client::reconnect`].
    async fn handshake(params: &ConnectParams) -> Result<Established> {
        let stream = TcpStream::connect(&params.addr)
            .await
            .with_context(|| format!("connecting to {}", params.addr))?;
        stream.set_nodelay(true)?;
        info!(addr = %params.addr, "connected to ingest server");

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
                signals: params.signals,
                capabilities: 0,
                resource_attrs: params.resource_attrs.clone(),
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
            writer_id = %hello_ack.writer_id,
            session_id = hello_ack.session_id,
            max_inflight = hello_ack.max_inflight_batches,
            "handshake complete"
        );

        let session_id = hello_ack.session_id;
        let max_inflight = hello_ack.max_inflight_batches.max(1) as usize;

        let (ack_tx, ack_rx) = mpsc::channel::<()>(1024);
        let reader = tokio::spawn(reader_loop(rd, ack_tx));

        Ok(Established {
            wr,
            ack_rx,
            max_inflight,
            session_id,
            reader,
        })
    }

    /// Re-establish the session against the (possibly restarted) server,
    /// replacing the write half, reader task, session id, and inflight budget.
    /// A single attempt — callers own the retry/backoff policy. On success the
    /// new server-assigned `session_id` is live and the inflight count is reset
    /// to zero (the old connection's unacked batches are gone with it).
    pub async fn reconnect(&mut self) -> Result<()> {
        // Tear down the old reader task; its read half (and thus the old TCP
        // connection) drops with it.
        self.reader.abort();
        let est = Self::handshake(&self.params).await?;
        self.wr = est.wr;
        self.ack_rx = est.ack_rx;
        self.max_inflight = est.max_inflight;
        self.session_id = est.session_id;
        self.reader = est.reader;
        self.inflight = 0;
        Ok(())
    }

    /// The session id assigned by the server; callers stamp it into each
    /// `BatchArgs.session_id`.
    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    /// Stamp the current session id into a built Batch frame and send it.
    ///
    /// The server assigns a fresh session id on every (re)connect and rejects
    /// any batch whose `session_id` doesn't match the live session
    /// (`ERR_SESSION_MISMATCH`). Building a frame and then reconnecting would
    /// leave it carrying a stale id; this re-stamps in place (zero-copy — the
    /// payload `Vec` is untouched) immediately before the write, so it is
    /// always correct against the *current* session. Callers that may
    /// reconnect between building and sending must use this rather than
    /// [`Client::send_batch`].
    pub async fn send_batch_stamped(&mut self, frame: &mut Frame) -> Result<()> {
        if let FrameMsg::Batch(b) = &mut frame.msg {
            b.session_id = self.session_id;
        }
        self.send_batch(frame).await
    }

    /// Send one already-built Batch frame, blocking on the inflight budget
    /// when the server hasn't acked enough prior batches yet.
    pub async fn send_batch(&mut self, frame: &Frame) -> Result<()> {
        // Block until we have an inflight slot, draining acks as they arrive.
        while self.inflight >= self.max_inflight {
            if self.ack_rx.recv().await.is_none() {
                bail!("ingest server closed connection (reader gone)");
            }
            self.inflight = self.inflight.saturating_sub(1);
        }
        // Opportunistically reclaim any acks already queued. A disconnected
        // reader (server closed the connection) surfaces here even while we're
        // under the inflight cap — without this, a low-volume producer that
        // never blocks on the budget would keep writing into a half-open
        // socket and only notice much later. Treat it as a lost connection so
        // the caller can reconnect.
        loop {
            match self.ack_rx.try_recv() {
                Ok(()) => self.inflight = self.inflight.saturating_sub(1),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    bail!("ingest server closed connection (reader gone)");
                }
            }
        }

        write_frame(&mut self.wr, frame).await?;
        self.wr.flush().await?;
        self.inflight += 1;
        Ok(())
    }

    /// Send a graceful Goodbye with the given operator-log `reason` text, flush,
    /// and wait for the reader to drain.
    pub async fn shutdown(mut self, reason: &str) -> Result<()> {
        write_frame(&mut self.wr, &build::goodbye(GOODBYE_NORMAL, reason)).await?;
        self.wr.flush().await?;
        drop(self.wr);
        let _ = self.reader.await;
        info!(unacked = self.inflight, "ingest session closed");
        Ok(())
    }
}

/// Drain server-initiated frames. Each `BatchAck` releases one inflight
/// credit; everything else is logged and ignored (the client does not answer
/// Ping/FlowControl yet).
async fn reader_loop(mut rd: BufReader<OwnedReadHalf>, ack_tx: mpsc::Sender<()>) {
    loop {
        match read_frame::<Frame, _>(&mut rd).await {
            Ok(f) => match f.msg {
                FrameMsg::BatchAck(a) => {
                    if a.status != ACK_ACCEPTED {
                        warn!(
                            batch_id = a.batch_id,
                            status = a.status,
                            reason_code = a.reason_code,
                            msg = %a.message,
                            "non-accepted batch ack"
                        );
                    }
                    if ack_tx.send(()).await.is_err() {
                        break;
                    }
                }
                FrameMsg::Goodbye(g) => {
                    info!(reason = g.reason_code, msg = %g.message, "server goodbye");
                    break;
                }
                FrameMsg::Error(e) => {
                    warn!(code = e.code, msg = %e.message, "server error frame");
                    break;
                }
                _ => {}
            },
            Err(e) => {
                info!(error = %e, "reader done");
                break;
            }
        }
    }
}
