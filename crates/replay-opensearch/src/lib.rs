//! `scry replay-opensearch` — high-speed OpenSearch → scry replay bench.
//!
//! Reads documents out of an OpenSearch cluster oldest→newest (PIT +
//! `search_after`), maps each `_source` to a scry log record ([`map`]), and
//! ships them into a `scry ingest` server over the native wire ([`wire`]) at a
//! controlled, **auto-ramping** record rate that holds at the knee when scry
//! starts throttling ([`pace`]). A progress bar over the whole corpus plus a
//! rolling stats line ([`stats`]) show where scry gives up.
//!
//! Two-stage pipeline over a bounded channel: a **fetcher** task pages the PIT
//! and streams raw `_source` values to the **sender** task, which maps, groups
//! consecutive records into per-fingerprint `LogStream`s, paces, and ships.

pub mod map;
pub mod os;
pub mod pace;
pub mod stats;
pub mod wire;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use clap::Parser;
use scry_proto::generated::{LogStream, LogsBatch};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::time::{interval, MissedTickBehavior};
use uuid::Uuid;

use crate::map::{doc_to_record, MapCounters, MappingConfig};
use crate::os::{Auth, OsClient};
use crate::pace::{Pacer, RampController};
use crate::stats::Stats;
use crate::wire::WireSender;

#[derive(Parser, Debug)]
#[command(
    about = "Replay documents from an OpenSearch cluster into a scry ingest server at an auto-ramping rate"
)]
pub struct Args {
    /// OpenSearch base URL (e.g. https://os.internal:9200).
    #[arg(long)]
    pub os_url: String,

    /// Index or index pattern to read.
    #[arg(long)]
    pub os_index: String,

    /// scry ingest server address (host:port).
    #[arg(long)]
    pub target: String,

    /// Document field carrying the record timestamp.
    #[arg(long, default_value = "@timestamp")]
    pub timestamp_field: String,

    /// Candidate body fields, first present wins (comma-separated).
    #[arg(long, value_delimiter = ',', default_value = "message,body")]
    pub body_field: Vec<String>,

    /// Candidate severity fields, first present wins (comma-separated).
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "severity,log.level,level"
    )]
    pub severity_field: Vec<String>,

    /// Candidate service-name fields, first present wins (comma-separated).
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "service.name,service,app,k8s_app"
    )]
    pub service_field: Vec<String>,

    /// Extra document fields to promote to stream labels (repeatable).
    #[arg(long)]
    pub label_field: Vec<String>,

    /// Cap on per-document attributes.
    #[arg(long, default_value_t = 64)]
    pub max_attrs: usize,

    /// OpenSearch page size (hits per `_search`).
    #[arg(long, default_value_t = 5000)]
    pub page_size: usize,

    /// Records per wire batch shipped to scry.
    #[arg(long, default_value_t = 5000)]
    pub batch_records: usize,

    /// Starting target rate (records/sec).
    #[arg(long, default_value_t = 500)]
    pub rate: u32,

    /// Rate increment applied each step interval.
    #[arg(long, default_value_t = 500)]
    pub rate_step: u32,

    /// How often to step the rate up.
    #[arg(long, value_parser = parse_duration, default_value = "15s")]
    pub rate_step_interval: Duration,

    /// Rate ceiling (0 = unbounded).
    #[arg(long, default_value_t = 0)]
    pub rate_max: u32,

    /// Raw JSON filter query (default: match_all).
    #[arg(long)]
    pub query: Option<String>,

    /// Bearer token for OpenSearch (mutually exclusive with basic auth / SigV4).
    #[arg(long)]
    pub os_bearer: Option<String>,

    /// HTTP basic-auth username (with --os-password).
    #[arg(long)]
    pub os_username: Option<String>,

    /// HTTP basic-auth password.
    #[arg(long)]
    pub os_password: Option<String>,

    /// Custom CA certificate (PEM) to trust for the OpenSearch endpoint.
    #[arg(long)]
    pub ca_cert: Option<PathBuf>,

    /// Sign OpenSearch requests with AWS SigV4 (Amazon OpenSearch Service /
    /// Serverless).
    #[arg(long)]
    pub os_aws_sigv4: bool,

    /// AWS region for SigV4 (defaults to the AWS config chain).
    #[arg(long)]
    pub os_aws_region: Option<String>,

    /// SigV4 signing name: `es` (managed domains) or `aoss` (Serverless).
    #[arg(long, default_value = "es")]
    pub os_aws_service: String,

    /// PIT keep-alive window.
    #[arg(long, default_value = "5m")]
    pub pit_keep_alive: String,

    /// HTTP request timeout for OpenSearch calls.
    #[arg(long, value_parser = parse_duration, default_value = "60s")]
    pub os_timeout: Duration,
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    let (num, unit) = s
        .trim()
        .find(|c: char| c.is_alphabetic())
        .map(|i| (&s[..i], &s[i..]))
        .unwrap_or((s, "s"));
    let n: u64 = num
        .trim()
        .parse()
        .map_err(|_| format!("bad number in {s:?}"))?;
    let dur = match unit.trim() {
        "ms" => Duration::from_millis(n),
        "s" | "" => Duration::from_secs(n),
        "m" => Duration::from_secs(n * 60),
        "h" => Duration::from_secs(n * 3600),
        other => return Err(format!("unknown duration unit {other:?}")),
    };
    Ok(dur)
}

pub async fn run(args: Args) -> Result<()> {
    // ── Resolve auth ───────────────────────────────────────────────────
    let auth = resolve_auth(&args).await?;

    // ── HTTP client + OpenSearch reader ────────────────────────────────
    let http = scry_httpsig::build_http_client(args.os_timeout, args.ca_cert.as_deref())?;
    let query: Value = match &args.query {
        Some(q) => serde_json::from_str(q).context("parsing --query JSON")?,
        None => serde_json::json!({ "match_all": {} }),
    };
    let os = Arc::new(OsClient::new(
        http,
        args.os_url.clone(),
        args.os_index.clone(),
        auth,
        args.pit_keep_alive.clone(),
        query,
    ));

    // ── Total for the progress bar ─────────────────────────────────────
    let total = os.count().await.context("counting documents (_count)")?;
    tracing::info!(total, "opensearch corpus counted");

    let cfg = MappingConfig {
        timestamp_field: args.timestamp_field.clone(),
        body_fields: args.body_field.clone(),
        severity_fields: args.severity_field.clone(),
        service_fields: args.service_field.clone(),
        label_fields: args.label_field.clone(),
        max_attrs: args.max_attrs,
    };

    // ── Fetcher task: PIT + search_after → pages of _source ─────────────
    // A page contains fully owned JSON documents and can be large. One queued
    // page still overlaps fetch and send while avoiding four-page retention.
    let (tx, rx) = mpsc::channel::<Vec<Value>>(1);
    let fetch_os = os.clone();
    let ts_field = args.timestamp_field.clone();
    let page_size = args.page_size;
    let fetcher: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
        let pit = fetch_os.open_pit().await.context("opening PIT")?;
        let mut after: Option<Vec<Value>> = None;
        let result: Result<()> = async {
            loop {
                let page = fetch_os
                    .search_after(&pit, page_size, &ts_field, after.as_deref())
                    .await?;
                if page.sources.is_empty() {
                    break;
                }
                after = page.next_after;
                if tx.send(page.sources).await.is_err() {
                    // Sender gone (error downstream); stop fetching.
                    break;
                }
                if after.is_none() {
                    break;
                }
            }
            Ok(())
        }
        .await;
        // Best-effort PIT close regardless of outcome.
        let _ = fetch_os.close_pit(&pit).await;
        result
    });

    // ── Sender: map + group + pace + ship ──────────────────────────────
    let sender_result = run_sender(args, cfg, total, rx).await;

    // Surface a fetcher error if the sender didn't already fail.
    let fetch_join = fetcher.await;
    sender_result?;
    match fetch_join {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(e).context("opensearch fetcher"),
        Err(e) => return Err(anyhow::anyhow!("fetcher task panicked: {e}")),
    }
    Ok(())
}

async fn resolve_auth(args: &Args) -> Result<Auth> {
    if args.os_aws_sigv4 {
        let signer = scry_httpsig::build_sigv4_signer(
            args.os_aws_region.clone(),
            args.os_aws_service.clone(),
        )
        .await?;
        return Ok(Auth::SigV4(Arc::new(signer)));
    }
    if let Some(token) = &args.os_bearer {
        return Ok(Auth::Bearer(token.clone()));
    }
    match (&args.os_username, &args.os_password) {
        (Some(u), Some(p)) => Ok(Auth::Basic {
            username: u.clone(),
            password: p.clone(),
        }),
        (Some(_), None) | (None, Some(_)) => {
            bail!("--os-username and --os-password must be given together")
        }
        (None, None) => Ok(Auth::None),
    }
}

/// The sender half: consume pages, map + group records into batches, pace, and
/// ship them to the ingest server, updating stats and the auto-ramp controller.
async fn run_sender(
    args: Args,
    cfg: MappingConfig,
    total: u64,
    mut rx: mpsc::Receiver<Vec<Value>>,
) -> Result<()> {
    let agent_id = *Uuid::now_v7().as_bytes();
    let hostname = hostname_string();
    let mut sender = WireSender::connect(&args.target, agent_id, &hostname)
        .await
        .context("connecting to scry ingest server")?;

    let mut stats = Stats::new(total);
    let mut controller = RampController::new(args.rate, args.rate_step, args.rate_max);
    stats.set_target_rate(controller.rate());
    let mut pacer = Pacer::new();
    let mut counters = MapCounters::default();

    // The carry-forward timestamp cursor, seeded with ingest-now for any doc
    // that precedes the first valid timestamp.
    let mut carry_ts = unix_nanos_now();

    // Per-batch accumulation state.
    let mut builder = BatchBuilder::new();

    let mut ramp_tick = interval(args.rate_step_interval);
    ramp_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ramp_tick.reset();
    let mut ui_tick = interval(Duration::from_secs(1));
    ui_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ui_tick.reset();

    loop {
        tokio::select! {
            maybe_page = rx.recv() => {
                match maybe_page {
                    Some(sources) => {
                        for src in &sources {
                            let rec = doc_to_record(src, &cfg, &mut carry_ts, &mut counters);
                            builder.push(rec);
                            if builder.record_count as usize >= args.batch_records {
                                flush(&mut builder, &mut sender, &mut pacer, &mut stats, &controller).await?;
                            }
                        }
                    }
                    None => break, // fetcher done
                }
            }
            _ = ramp_tick.tick() => {
                let pressure = stats.interval_pressure(sender.inflight(), sender.max_inflight());
                controller.tick(pressure);
                stats.set_target_rate(controller.rate());
                if controller.frozen() {
                    stats.println(format!("↳ holding at knee: {} rec/s", controller.rate()));
                }
            }
            _ = ui_tick.tick() => {
                stats.refresh_line(sender.inflight(), sender.max_inflight(), &counters);
            }
        }
    }

    // Flush the tail and drain outstanding acks.
    if builder.record_count > 0 {
        flush(
            &mut builder,
            &mut sender,
            &mut pacer,
            &mut stats,
            &controller,
        )
        .await?;
    }
    for ack in sender.drain().await {
        stats.on_ack(&ack);
    }
    stats.refresh_line(sender.inflight(), sender.max_inflight(), &counters);

    let summary = format!(
        "replayed {} records | throttled={} rejected={} | ts_inherited={} body_missing={}",
        total, // corpus size; sent == total on a clean drain
        stats.throttled(),
        stats.rejected(),
        counters.ts_inherited,
        counters.body_missing,
    );
    stats.finish(summary.clone());
    sender.shutdown("replay done").await?;
    tracing::info!("{summary}");
    Ok(())
}

/// Encode + pace + ship the current batch, resetting the builder.
async fn flush(
    builder: &mut BatchBuilder,
    sender: &mut WireSender,
    pacer: &mut Pacer,
    stats: &mut Stats,
    controller: &RampController,
) -> Result<()> {
    let (batch, count, ts_min, ts_max) = builder.take();
    if count == 0 {
        return Ok(());
    }
    let payload = batch.encode().context("encoding LogsBatch for the wire")?;

    pacer.pace(count as usize, controller.rate()).await;

    // Ship with a single reconnect-and-retry on link failure.
    let acks = match sender
        .send_logs_batch(count, ts_min, ts_max, &payload)
        .await
    {
        Ok(acks) => acks,
        Err(e) => {
            tracing::warn!(error = %e, "wire send failed; reconnecting");
            sender
                .reconnect()
                .await
                .context("reconnecting to ingest server after send failure")?;
            sender
                .send_logs_batch(count, ts_min, ts_max, &payload)
                .await
                .context("resending batch after reconnect")?
        }
    };
    for ack in &acks {
        stats.on_ack(ack);
    }
    stats.on_records_sent(count as u64);
    Ok(())
}

/// Accumulates mapped records into per-fingerprint streams for one wire batch.
struct BatchBuilder {
    streams: Vec<LogStream>,
    index: HashMap<u64, usize>,
    record_count: u32,
    ts_min: u64,
    ts_max: u64,
}

impl BatchBuilder {
    fn new() -> Self {
        Self {
            streams: Vec::new(),
            index: HashMap::new(),
            record_count: 0,
            ts_min: u64::MAX,
            ts_max: 0,
        }
    }

    fn push(&mut self, rec: map::MappedRecord) {
        let ts = rec.entry.ts_unix_nano;
        self.ts_min = self.ts_min.min(ts);
        self.ts_max = self.ts_max.max(ts);
        let idx = *self.index.entry(rec.fingerprint).or_insert_with(|| {
            self.streams.push(LogStream {
                fingerprint: rec.fingerprint,
                labels: rec.labels.clone(),
                entries: Vec::new(),
            });
            self.streams.len() - 1
        });
        self.streams[idx].entries.push(rec.entry);
        self.record_count += 1;
    }

    /// Take the accumulated batch, leaving the builder empty.
    fn take(&mut self) -> (LogsBatch, u32, u64, u64) {
        let streams = std::mem::take(&mut self.streams);
        self.index.clear();
        let count = self.record_count;
        let ts_min = if self.ts_min == u64::MAX {
            0
        } else {
            self.ts_min
        };
        let ts_max = self.ts_max;
        self.record_count = 0;
        self.ts_min = u64::MAX;
        self.ts_max = 0;
        (LogsBatch { streams }, count, ts_min, ts_max)
    }
}

fn unix_nanos_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn hostname_string() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|s| s.trim().to_string())
        })
        .unwrap_or_else(|| "unknown".into())
}
