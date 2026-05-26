//! noise-spewer — TCP client that connects to a scry ingest server and
//! emits randomised batches across all four signals. Useful for end-to-
//! end protocol exercise long before any real agent code exists.
//!
//! Run:
//!   noise-spewer --addr 127.0.0.1:4000 --rate 100 --duration 30s
//!
//! Run it concurrently with `noise-sink` for a full loop:
//!   noise-sink   --listen 127.0.0.1:4000 &
//!   noise-spewer --addr   127.0.0.1:4000 --duration 5s

use anyhow::{Context, Result, bail};
use clap::Parser;
use rand::SeedableRng;
use scry_proto::{
    LabelPair, build,
    constants::{
        ACK_ACCEPTED, ACK_REJECTED, ACK_THROTTLED, GOODBYE_NORMAL, PROTOCOL_VERSION_V0,
        SIGNAL_BIT_LOGS, SIGNAL_BIT_METRICS, SIGNAL_BIT_PROFILES, SIGNAL_BIT_TRACES, Signal,
    },
    framing::{read_frame, write_frame},
    generated::FrameMsg,
};
use std::time::Duration;
use tokio::{
    io::{AsyncWriteExt, BufReader, BufWriter},
    net::TcpStream,
    sync::mpsc,
    time::{Instant, sleep_until},
};
use tracing::{info, warn};
use uuid::Uuid;

mod gen;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Server address (host:port).
    #[arg(long, default_value = "127.0.0.1:4000")]
    addr: String,

    /// Target batches per second across all signals.
    #[arg(long, default_value_t = 10)]
    rate: u32,

    /// How long to run before sending Goodbye and exiting.
    #[arg(long, value_parser = parse_duration, default_value = "30s")]
    duration: Duration,

    /// Optional cap on batches sent (overrides --duration if reached first).
    #[arg(long)]
    max_batches: Option<u64>,

    /// Comma-separated signals to emit. Default: all.
    #[arg(long, default_value = "metrics,logs,traces,profiles")]
    signals: String,

    /// Random seed; if unset we pick one and log it.
    #[arg(long)]
    seed: Option<u64>,
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    // Tiny subset: integer + suffix.
    let (num, unit) = s
        .trim()
        .find(|c: char| c.is_alphabetic())
        .map(|i| (&s[..i], &s[i..]))
        .unwrap_or((s, "s"));
    let n: u64 = num.trim().parse().map_err(|_| format!("bad number in {s:?}"))?;
    let dur = match unit.trim() {
        "ms"        => Duration::from_millis(n),
        "s" | ""    => Duration::from_secs(n),
        "m"         => Duration::from_secs(n * 60),
        "h"         => Duration::from_secs(n * 3600),
        other       => return Err(format!("unknown duration unit {other:?}")),
    };
    Ok(dur)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let seed = args.seed.unwrap_or_else(rand::random);
    info!(seed, "noise-spewer starting");

    let (signal_mask, dummy_enabled) = parse_signal_selection(&args.signals)?;
    let mut signals = expand_mask(signal_mask);
    if dummy_enabled {
        signals.push(Signal::Dummy);
    }
    if signals.is_empty() {
        bail!("at least one signal must be enabled (--signals=metrics,…)");
    }

    let stream = TcpStream::connect(&args.addr)
        .await
        .with_context(|| format!("connecting to {}", args.addr))?;
    stream.set_nodelay(true)?;
    info!(addr = %args.addr, "connected");

    let (rd, wr) = stream.into_split();
    let mut rd = BufReader::new(rd);
    let mut wr = BufWriter::new(wr);

    // ── Handshake ──────────────────────────────────────────────────────
    let agent_uuid = Uuid::now_v7();
    let agent_id_bytes = *agent_uuid.as_bytes();
    let hostname = hostname_string();

    write_frame(
        &mut wr,
        &build::hello(build::HelloArgs {
            protocol_version: PROTOCOL_VERSION_V0,
            agent_id: agent_id_bytes,
            agent_version: env!("CARGO_PKG_VERSION"),
            hostname: &hostname,
            signals: signal_mask,
            capabilities: 0,
            resource_attrs: vec![
                LabelPair { key: "service".into(), value: "noise-spewer".into() },
                LabelPair { key: "host".into(), value: hostname.clone() },
            ],
        }),
    )
    .await?;
    wr.flush().await?;

    let hello_ack = match read_frame(&mut rd).await?.msg {
        FrameMsg::HelloAck(a) => a,
        FrameMsg::Error(e) => {
            bail!("server rejected handshake: code={} msg={:?}", e.code, e.message)
        }
        other => bail!("expected HelloAck, got {}", short_msg_name(&other)),
    };
    info!(
        writer_id = %hello_ack.writer_id,
        session_id = hello_ack.session_id,
        suggested_batch_bytes = hello_ack.suggested_batch_bytes,
        max_inflight = hello_ack.max_inflight_batches,
        "handshake complete"
    );

    // ── Reader task: drain BatchAcks (server-initiated; we count them
    // back into the writer's inflight budget). Anything else gets logged
    // and ignored; the spewer is intentionally dumb about FlowControl /
    // Ping for now. ────────────────────────────────────────────────────
    let (ack_tx, mut ack_rx) = mpsc::channel::<()>(1024);
    let reader_handle = tokio::spawn(async move {
        loop {
            match read_frame(&mut rd).await {
                Ok(f) => match f.msg {
                    FrameMsg::BatchAck(a) => {
                        if a.status != ACK_ACCEPTED {
                            warn!(
                                batch_id = a.batch_id,
                                status = ack_name(a.status),
                                reason_code = a.reason_code,
                                msg = %a.message,
                                "non-accepted ack"
                            );
                        }
                        if ack_tx.send(()).await.is_err() {
                            break;
                        }
                    }
                    FrameMsg::Ping(p) => {
                        tracing::debug!(nonce = p.nonce, "server ping (spewer does not answer)");
                    }
                    FrameMsg::Goodbye(g) => {
                        info!(reason = g.reason_code, msg = %g.message, "server goodbye");
                        break;
                    }
                    FrameMsg::Error(e) => {
                        warn!(code = e.code, msg = %e.message, "server error frame");
                        break;
                    }
                    other => {
                        tracing::debug!(kind = short_msg_name(&other), "ignoring unexpected server frame");
                    }
                },
                Err(e) => {
                    info!(error = %e, "reader done");
                    break;
                }
            }
        }
    });

    // ── Writer loop ────────────────────────────────────────────────────
    let session_id = hello_ack.session_id;
    let max_inflight = hello_ack.max_inflight_batches.max(1) as usize;
    let mut inflight: usize = 0;
    let mut batch_id: u64 = 0;

    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);

    let start = Instant::now();
    let deadline = start + args.duration;
    let tick = if args.rate == 0 {
        Duration::from_millis(100)
    } else {
        Duration::from_micros(1_000_000 / args.rate as u64)
    };
    let mut next_send = start;

    loop {
        if Instant::now() >= deadline {
            break;
        }
        if let Some(cap) = args.max_batches {
            if batch_id >= cap {
                break;
            }
        }

        while inflight >= max_inflight {
            if ack_rx.recv().await.is_none() {
                break;
            }
            inflight = inflight.saturating_sub(1);
        }
        while ack_rx.try_recv().is_ok() {
            inflight = inflight.saturating_sub(1);
        }

        let signal = signals[(batch_id as usize) % signals.len()];
        let frame = gen::make_batch(&mut rng, signal, session_id, batch_id);
        write_frame(&mut wr, &frame).await?;
        wr.flush().await?;
        inflight += 1;
        batch_id += 1;

        next_send += tick;
        let now = Instant::now();
        if next_send > now {
            sleep_until(next_send).await;
        } else {
            next_send = now;
        }
    }

    info!(batches_sent = batch_id, "duration reached, sending goodbye");
    write_frame(&mut wr, &build::goodbye(GOODBYE_NORMAL, "spewer done")).await?;
    wr.flush().await?;
    drop(wr);

    let _ = reader_handle.await;
    info!(unacked = inflight, "done");
    Ok(())
}

/// Parse the comma-separated `--signals` list into (Hello.signals bitmask,
/// dummy-enabled). `dummy` is recognised but has no bitmask bit — it's a
/// v0.1-only sentinel that doesn't appear in the wire-level signals field.
fn parse_signal_selection(s: &str) -> Result<(u8, bool)> {
    let mut mask = 0u8;
    let mut dummy = false;
    for sig in s.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
        match sig {
            "metrics"  => mask |= SIGNAL_BIT_METRICS,
            "logs"     => mask |= SIGNAL_BIT_LOGS,
            "traces"   => mask |= SIGNAL_BIT_TRACES,
            "profiles" => mask |= SIGNAL_BIT_PROFILES,
            "dummy"    => dummy = true,
            other      => bail!("unknown signal {other:?}"),
        }
    }
    Ok((mask, dummy))
}

fn expand_mask(mask: u8) -> Vec<Signal> {
    let mut out = Vec::new();
    if mask & SIGNAL_BIT_METRICS  != 0 { out.push(Signal::Metrics); }
    if mask & SIGNAL_BIT_LOGS     != 0 { out.push(Signal::Logs); }
    if mask & SIGNAL_BIT_TRACES   != 0 { out.push(Signal::Traces); }
    if mask & SIGNAL_BIT_PROFILES != 0 { out.push(Signal::Profiles); }
    out
}

fn hostname_string() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|s| s.trim().to_string())
        })
        .unwrap_or_else(|| "unknown".into())
}

fn ack_name(s: u8) -> &'static str {
    match s {
        ACK_ACCEPTED  => "accepted",
        ACK_THROTTLED => "throttled",
        ACK_REJECTED  => "rejected",
        _ => "unknown",
    }
}

fn short_msg_name(m: &FrameMsg) -> &'static str {
    match m {
        FrameMsg::Hello(_)       => "Hello",
        FrameMsg::HelloAck(_)    => "HelloAck",
        FrameMsg::Batch(_)       => "Batch",
        FrameMsg::BatchAck(_)    => "BatchAck",
        FrameMsg::FlowControl(_) => "FlowControl",
        FrameMsg::Ping(_)        => "Ping",
        FrameMsg::Pong(_)        => "Pong",
        FrameMsg::Goodbye(_)     => "Goodbye",
        FrameMsg::Error(_)       => "Error",
    }
}
