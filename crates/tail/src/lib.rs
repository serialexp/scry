//! `scry tail` — live log tailing.
//!
//! Opens one TCP connection per `--ingest` server, performs the native
//! handshake, sends a `Subscribe`, and prints every `TailRecord` the server
//! sprays back until interrupted (Ctrl-C) or the servers hang up.
//!
//! This is a **separate, best-effort surface**: the records here come
//! straight off the ingest hot path *before* they are sealed into a block,
//! so they are "roughly now" — lossy under load, unordered across servers,
//! and never deduplicated against or merged with stored history. It answers
//! "what is happening right now?", not "give me a complete, correct log."
//! See `crates/server/src/tail.rs` and D-050.

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::Parser;
use scry_proto::{
    build,
    constants::{Signal, PROTOCOL_VERSION_V0, SIGNAL_BIT_LOGS},
    framing::{read_frame, write_frame, FrameError},
    Frame, FrameMsg, LabelPair,
};
use tokio::{
    io::{AsyncWriteExt, BufReader, BufWriter},
    net::TcpStream,
    sync::{mpsc, oneshot},
};

// NOTE: `scry tail` prints records to **stdout** — that's its data stream. All
// diagnostics go to **stderr** via `eprintln!` (not `tracing`, which the shared
// `scry` subscriber routes to stdout and would interleave with records).

/// `scry tail` — stream live log records matching a set of label matchers.
#[derive(Parser, Debug)]
pub struct Args {
    /// Query daemon to tail through, as `host:port` (its `--tail-listen`
    /// address). The **primary path**: queryd discovers the live ingesters via
    /// Valkey and fans their records back. Repeatable. Requires queryd to have
    /// Valkey configured, else it refuses.
    #[arg(long = "queryd", value_name = "HOST:PORT")]
    pub queryd: Vec<String>,

    /// Ingest server to tail **directly**, as `host:port`. For debugging one
    /// ingester with no Valkey/queryd in the loop. Repeatable; combined with
    /// any `--queryd` endpoints. Records from all endpoints interleave onto
    /// stdout as they arrive.
    #[arg(long = "ingest", value_name = "HOST:PORT")]
    pub ingest: Vec<String>,

    /// Label matchers (Prometheus-style): `key=value`, `key!=value`,
    /// `key=~regex`, `key!~regex`. A stream is tailed only if it matches
    /// ALL matchers (logical AND). Omit to tail everything.
    #[arg(value_name = "MATCHER")]
    pub matchers: Vec<String>,

    /// Signal to tail. Only `logs` is supported today.
    #[arg(long, default_value = "logs")]
    pub signal: String,
}

/// One formatted line ready to print, tagged with which server it came from.
struct Line {
    origin: String,
    text: String,
}

pub async fn run(args: Args) -> Result<()> {
    let signal = match args.signal.as_str() {
        "logs" => Signal::Logs,
        other => bail!("unsupported --signal {other:?}: only 'logs' is supported today"),
    };

    // Fail fast on a bad matcher before opening any connections — the
    // server re-validates, but a local parse gives a cleaner error.
    scry_match::LabelFilter::parse(&args.matchers).context("parsing matchers")?;

    // `--queryd` (front-door) and `--ingest` (direct) endpoints are
    // wire-identical from the client's side — both speak the tail sub-protocol,
    // queryd just happens to relay. Merge into one dial set.
    let mut endpoints: Vec<String> = Vec::with_capacity(args.queryd.len() + args.ingest.len());
    endpoints.extend(args.queryd);
    endpoints.extend(args.ingest);
    if endpoints.is_empty() {
        bail!("provide at least one endpoint: --queryd HOST:PORT (via the query daemon) or --ingest HOST:PORT (direct)");
    }

    let matchers = Arc::new(args.matchers);
    let multi = endpoints.len() > 1;

    // All connection tasks funnel formatted lines through one channel so
    // stdout writes are serialised (no interleaved half-lines).
    let (tx, mut rx) = mpsc::channel::<Line>(4096);

    let mut tasks = Vec::new();
    for addr in endpoints {
        let tx = tx.clone();
        let matchers = matchers.clone();
        tasks.push(tokio::spawn(async move {
            if let Err(e) = tail_one(&addr, signal.as_u8(), &matchers, tx).await {
                eprintln!("tail: connection to {addr} ended with error: {e:#}");
            }
        }));
    }
    drop(tx); // so `rx` closes once every task's sender is gone

    // Printer: drain until all connections end or Ctrl-C.
    let printer = async {
        while let Some(line) = rx.recv().await {
            if multi {
                println!("[{}] {}", line.origin, line.text);
            } else {
                println!("{}", line.text);
            }
        }
    };

    tokio::select! {
        _ = printer => {
            eprintln!("tail: all connections closed");
        }
        r = tokio::signal::ctrl_c() => {
            r.context("installing ctrl-c handler")?;
            eprintln!("tail: interrupted; closing");
        }
    }

    // Dropping the tasks closes the sockets, which the server sees as EOF
    // and deregisters the subscription.
    for t in tasks {
        t.abort();
    }
    Ok(())
}

/// Handshake + subscribe against one server, formatting each record onto the
/// shared print channel. Wraps the reusable [`dial_subscribe`] primitive with
/// the CLI's presentation (a `<ts> <LEVEL> {labels} body` line tagged by
/// origin) and a `subscribed` stderr note the moment the subscription is live.
async fn tail_one(
    addr: &str,
    signal: u8,
    matchers: &[String],
    out_tx: mpsc::Sender<Line>,
) -> Result<()> {
    // Raw records flow dial → formatter; the oneshot signals "subscription live".
    let (rec_tx, mut rec_rx) = mpsc::channel::<scry_proto::generated::TailRecordOutput>(4096);
    let (sub_tx, sub_rx) = oneshot::channel::<()>();

    let origin = addr.to_string();
    let formatter = async move {
        while let Some(rec) = rec_rx.recv().await {
            let line = Line {
                origin: origin.clone(),
                text: format_record(&rec),
            };
            if out_tx.send(line).await.is_err() {
                break; // printer gone → shutting down
            }
        }
    };
    let notifier = async {
        if sub_rx.await.is_ok() {
            eprintln!("tail: subscribed to {addr} ({} matcher(s))", matchers.len());
        }
    };

    let (res, _, _) = tokio::join!(
        dial_subscribe(addr, signal, matchers, rec_tx, Some(sub_tx)),
        formatter,
        notifier,
    );
    res
}

/// Connect to one server that speaks the tail sub-protocol (an ingester, or the
/// queryd relay), perform the native handshake, `Subscribe`, and forward every
/// `TailRecord` it sprays back onto `tx` until the connection ends.
///
/// This is the reusable core shared by the `scry tail` CLI and the queryd
/// front-door relay (D-053): the relay dials each discovered ingester with this
/// and re-frames the raw records downstream to its own client. `on_subscribed`
/// (if provided) fires once the `Subscribe` has been sent and accepted, so a
/// caller can wait for the stream to be live.
pub async fn dial_subscribe(
    addr: &str,
    signal: u8,
    matchers: &[String],
    tx: mpsc::Sender<scry_proto::generated::TailRecordOutput>,
    on_subscribed: Option<oneshot::Sender<()>>,
) -> Result<()> {
    let stream = TcpStream::connect(addr)
        .await
        .with_context(|| format!("connecting to {addr}"))?;
    stream.set_nodelay(true)?;
    let (rd, wr) = stream.into_split();
    let mut rd = BufReader::new(rd);
    let mut wr = BufWriter::new(wr);

    // Hello: announce logs (the only tailable signal). agent_id is a
    // throwaway per-invocation UUID; a tail client never writes.
    write_frame(
        &mut wr,
        &build::hello(build::HelloArgs {
            protocol_version: PROTOCOL_VERSION_V0,
            agent_id: uuid::Uuid::now_v7().into_bytes(),
            agent_version: env!("CARGO_PKG_VERSION"),
            hostname: &hostname_string(),
            signals: SIGNAL_BIT_LOGS,
            capabilities: 0,
            resource_attrs: Vec::new(),
        }),
    )
    .await?;
    wr.flush().await?;

    match read_frame::<Frame, _>(&mut rd).await?.msg {
        FrameMsg::HelloAck(_) => {}
        FrameMsg::Error(e) => bail!(
            "server rejected handshake: code={} msg={:?}",
            e.code,
            e.message
        ),
        other => bail!("expected HelloAck, got a different frame: {other:?}"),
    }

    // Subscribe: the connection is now a one-way delivery stream.
    write_frame(&mut wr, &build::subscribe(signal, matchers)).await?;
    wr.flush().await?;
    if let Some(s) = on_subscribed {
        let _ = s.send(());
    }

    loop {
        match read_frame::<Frame, _>(&mut rd).await {
            Ok(frame) => match frame.msg {
                FrameMsg::TailRecord(t) => {
                    // If the sink is gone, we're shutting down.
                    if tx.send(t).await.is_err() {
                        break;
                    }
                }
                FrameMsg::Error(e) => {
                    bail!("server sent Error: code={} msg={:?}", e.code, e.message)
                }
                FrameMsg::Goodbye(_) => break,
                _ => {} // ignore anything else on a tail connection
            },
            Err(FrameError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e).context("reading tail frame"),
        }
    }
    Ok(())
}

/// Render one record as `<rfc3339 ts> <LEVEL> {k=v,…} body`.
fn format_record(t: &scry_proto::generated::TailRecordOutput) -> String {
    let ts = format_ts(t.ts_unix_nano);
    let level = severity_name(t.severity);
    let labels = format_labels(&t.labels);
    if labels.is_empty() {
        format!("{ts} {level} {}", t.body)
    } else {
        format!("{ts} {level} {labels} {}", t.body)
    }
}

fn format_ts(ns: u64) -> String {
    let secs = (ns / 1_000_000_000) as i64;
    let nanos = (ns % 1_000_000_000) as u32;
    match chrono::DateTime::from_timestamp(secs, nanos) {
        Some(dt) => dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        None => ns.to_string(),
    }
}

/// Map an OTel-style `severity_number` (1–24) to a short level name.
fn severity_name(sev: u8) -> &'static str {
    match sev {
        1..=4 => "TRACE",
        5..=8 => "DEBUG",
        9..=12 => "INFO",
        13..=16 => "WARN",
        17..=20 => "ERROR",
        21..=24 => "FATAL",
        _ => "-",
    }
}

fn format_labels(labels: &[LabelPair]) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let inner = labels
        .iter()
        .map(|p| format!("{}={}", p.key, p.value))
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{inner}}}")
}

fn hostname_string() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|s| s.trim().to_string())
        })
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "scry-tail".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn severity_names_cover_bands() {
        assert_eq!(severity_name(0), "-");
        assert_eq!(severity_name(9), "INFO");
        assert_eq!(severity_name(17), "ERROR");
        assert_eq!(severity_name(24), "FATAL");
    }

    #[test]
    fn labels_render_braced() {
        let pairs = vec![
            LabelPair {
                key: "namespace".into(),
                value: "prod".into(),
            },
            LabelPair {
                key: "pod".into(),
                value: "web-1".into(),
            },
        ];
        assert_eq!(format_labels(&pairs), "{namespace=prod,pod=web-1}");
        assert_eq!(format_labels(&[]), "");
    }

    #[test]
    fn timestamp_is_rfc3339() {
        // 2021-01-01T00:00:00Z = 1609459200 s.
        let s = format_ts(1_609_459_200_000_000_000);
        assert!(s.starts_with("2021-01-01T00:00:00"), "got {s}");
    }
}
