//! `scry-query-probe` — a minimal client for the `QueryFrame` wire protocol.
//!
//! The only other client of the query wire is the TypeScript desktop app, so
//! scripts (and humans) have no way to drive a running `scry query` daemon
//! from a shell. This dev/ops harness fills that gap: it connects, sends one
//! `QueryRequest`, drains the response frames, and prints the server-reported
//! `EndOfStream.total_rows` — exiting non-zero on a `StreamError`.
//!
//! The query wire has no handshake (unlike ingest): the client sends exactly
//! one `QueryRequest` and reads `SchemaMsg` + N×`BatchMsg` + `EndOfStream`.
//! We don't decode the Arrow IPC payloads — the row count on `EndOfStream` is
//! all a smoke test needs, and skipping Arrow keeps this harness tiny.
//!
//! Used by `scripts/smoke.sh` to prove the queryd result cache: run the same
//! query twice and assert the daemon logs `cache=hit` the second time.

use std::net::SocketAddr;

use anyhow::{bail, Context, Result};
use clap::Parser;
use scry_proto::constants::Signal;
use scry_proto::framing::{read_frame, write_frame};
use scry_proto::{QueryFrame, QueryFrameMsg};
use scry_query::{Query, QueryRequest};
use tokio::io::{BufReader, BufWriter};
use tokio::net::TcpStream;

#[derive(Parser, Debug)]
#[command(
    name = "scry-query-probe",
    about = "Send one QueryRequest to a `scry query` daemon and print EndOfStream.total_rows"
)]
struct Args {
    /// Daemon address, e.g. 127.0.0.1:4100.
    #[arg(long)]
    addr: SocketAddr,

    /// Signal: metrics | logs | traces | profiles.
    #[arg(long, default_value = "logs")]
    signal: String,

    /// Optional SQL against the signal's table. If omitted the server runs
    /// `SELECT * FROM <signal>` (with any matcher/time preselect applied).
    #[arg(long)]
    sql: Option<String>,

    /// Repeatable equality matcher `key=value` (AND'd), applied before scan.
    #[arg(short = 'm', long = "matcher")]
    matchers: Vec<String>,

    /// Inclusive lower time bound, unix nanoseconds.
    #[arg(long)]
    ts_min: Option<u64>,

    /// Inclusive upper time bound, unix nanoseconds.
    #[arg(long)]
    ts_max: Option<u64>,

    /// Logs full-text substring (`body_contains`).
    #[arg(long)]
    body_contains: Option<String>,

    /// `LIMIT N` (ignored when `--sql` is set — express it in the SQL).
    #[arg(long)]
    limit: Option<usize>,

    /// Caller-supplied request id echoed in the daemon's per-query logs.
    #[arg(long)]
    request_id: Option<String>,
}

fn parse_signal(s: &str) -> Result<u8> {
    Ok(match s.to_ascii_lowercase().as_str() {
        "metrics" => Signal::Metrics as u8,
        "logs" => Signal::Logs as u8,
        "traces" => Signal::Traces as u8,
        "profiles" => Signal::Profiles as u8,
        other => bail!("unknown signal '{other}' (want metrics|logs|traces|profiles)"),
    })
}

fn parse_matcher(s: &str) -> Result<(String, String)> {
    let (k, v) = s
        .split_once('=')
        .with_context(|| format!("matcher '{s}' is not key=value"))?;
    Ok((k.to_string(), v.to_string()))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let matchers = args
        .matchers
        .iter()
        .map(|m| parse_matcher(m))
        .collect::<Result<Vec<_>>>()?;

    let req = QueryRequest {
        signal: parse_signal(&args.signal)?,
        query: Query {
            matchers,
            ts_min: args.ts_min,
            ts_max: args.ts_max,
            trace_id: None,
            body_contains: args.body_contains,
        },
        sql: args.sql,
        limit: args.limit,
        request_id: args.request_id,
    };

    let sock = TcpStream::connect(args.addr)
        .await
        .with_context(|| format!("connect {}", args.addr))?;
    let (r, w) = sock.into_split();
    let mut r = BufReader::new(r);
    let mut w = BufWriter::new(w);

    let request_frame = QueryFrame {
        msg: QueryFrameMsg::QueryRequest(req.to_wire().into()),
    };
    write_frame(&mut w, &request_frame)
        .await
        .context("write QueryRequest")?;
    // write_frame flushes the framing; ensure the buffered writer drains too.
    tokio::io::AsyncWriteExt::flush(&mut w)
        .await
        .context("flush request")?;

    // Drain the response: SchemaMsg + BatchMsg… + EndOfStream (or StreamError).
    // Arrow payloads are ignored — only the terminal row count is reported.
    let total_rows: u64 = loop {
        let frame: QueryFrame = read_frame(&mut r).await.context("read response frame")?;
        match frame.msg {
            QueryFrameMsg::SchemaMsg(_) | QueryFrameMsg::BatchMsg(_) => continue,
            QueryFrameMsg::EndOfStream(end) => break end.total_rows,
            QueryFrameMsg::StreamError(err) => {
                bail!(
                    "server StreamError code={:#06x} message={}",
                    err.code,
                    err.message
                );
            }
            other => bail!("unexpected frame in data-query response: {other:?}"),
        }
    };

    println!("{total_rows}");
    Ok(())
}
