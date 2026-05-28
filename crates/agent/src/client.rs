//! Reusable wire-transport client for a scry ingest server.
//!
//! This is the signal-agnostic half of `noise-spewer`'s main loop lifted
//! into a small struct: connect + handshake, a background reader task that
//! turns each `BatchAck` into an inflight credit, and `send_batch` with the
//! same inflight flow control the spewer used. The agent feeds it
//! already-built `Frame`s (Batch variants); the client knows nothing about
//! logs specifically.

use anyhow::{bail, Context, Result};
use scry_proto::{
    build,
    constants::{ACK_ACCEPTED, GOODBYE_NORMAL, PROTOCOL_VERSION_V0, SIGNAL_BIT_LOGS},
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

/// A connected, handshaken ingest session. Drop or call [`Client::shutdown`]
/// to end it.
pub struct Client {
    wr: BufWriter<OwnedWriteHalf>,
    ack_rx: mpsc::Receiver<()>,
    max_inflight: usize,
    inflight: usize,
    session_id: u64,
    reader: JoinHandle<()>,
}

impl Client {
    /// Connect to `addr`, perform the Hello/HelloAck handshake announcing the
    /// logs signal, and spawn the ack-draining reader task.
    pub async fn connect(
        addr: &str,
        agent_id: [u8; 16],
        hostname: &str,
        resource_attrs: Vec<LabelPair>,
    ) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .with_context(|| format!("connecting to {addr}"))?;
        stream.set_nodelay(true)?;
        info!(addr, "connected to ingest server");

        let (rd, wr) = stream.into_split();
        let mut rd = BufReader::new(rd);
        let mut wr = BufWriter::new(wr);

        write_frame(
            &mut wr,
            &build::hello(build::HelloArgs {
                protocol_version: PROTOCOL_VERSION_V0,
                agent_id,
                agent_version: env!("CARGO_PKG_VERSION"),
                hostname,
                signals: SIGNAL_BIT_LOGS,
                capabilities: 0,
                resource_attrs,
            }),
        )
        .await?;
        wr.flush().await?;

        let hello_ack = match read_frame::<Frame, _>(&mut rd).await?.msg {
            FrameMsg::HelloAck(a) => a,
            FrameMsg::Error(e) => {
                bail!("server rejected handshake: code={} msg={:?}", e.code, e.message)
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

        Ok(Self {
            wr,
            ack_rx,
            max_inflight,
            inflight: 0,
            session_id,
            reader,
        })
    }

    /// The session id assigned by the server; callers stamp it into each
    /// `BatchArgs.session_id`.
    pub fn session_id(&self) -> u64 {
        self.session_id
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
        // Opportunistically reclaim any acks already queued.
        while self.ack_rx.try_recv().is_ok() {
            self.inflight = self.inflight.saturating_sub(1);
        }

        write_frame(&mut self.wr, frame).await?;
        self.wr.flush().await?;
        self.inflight += 1;
        Ok(())
    }

    /// Send a graceful Goodbye, flush, and wait for the reader to drain.
    pub async fn shutdown(mut self) -> Result<()> {
        write_frame(&mut self.wr, &build::goodbye(GOODBYE_NORMAL, "agent shutdown")).await?;
        self.wr.flush().await?;
        drop(self.wr);
        let _ = self.reader.await;
        info!(unacked = self.inflight, "ingest session closed");
        Ok(())
    }
}

/// Drain server-initiated frames. Each `BatchAck` releases one inflight
/// credit; everything else is logged and ignored (the agent does not answer
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
