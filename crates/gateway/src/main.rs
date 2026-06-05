//! `scry-gateway` daemon: a fan-out hub. It accepts records over the native
//! binschema wire **and** the foreign HTTP push protocols (OTLP traces,
//! Pyroscope profiles, Prometheus remote-write) and forwards every record,
//! best-effort, to every configured downstream sink. Each sink is opt-in (at
//! least one required): the scry ingest server (`--upstream`), Grafana Loki, and
//! OpenSearch — the latter two logs-only, with the OpenSearch sink self-managing
//! its per-service rolling data streams + lifecycle assets.

use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use clap::Parser;
use scry_gateway::{
    aws_sign::SigV4Signer,
    loki::LokiSink,
    mimir::MimirSink,
    opensearch::{OpenSearchConfig, OpenSearchSink},
    router, serve_wire,
    sink::{spawn_sink, AppState, SinkHandle, ACCEPT_ALL},
    sink_scry::{ScryConnect, ScrySink},
    tls::build_http_client,
};
use scry_proto::{
    constants::{SIGNAL_BIT_LOGS, SIGNAL_BIT_METRICS, SIGNAL_BIT_PROFILES, SIGNAL_BIT_TRACES},
    LabelPair,
};
use tokio::sync::watch;
use uuid::Uuid;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[command(
    name = "scry-gateway",
    about = "fan-out push gateway for scry (native + OTLP + Pyroscope + remote-write in; scry + Loki + OpenSearch out)"
)]
struct Args {
    /// HTTP listen address (foreign protocols: /v1/traces, /ingest, /api/v1/write).
    #[arg(long, default_value = "0.0.0.0:4318")]
    listen: String,

    /// Native binschema ingest listen address (scry-agent and other native
    /// producers point here). Opt-in: when unset, no native listener is bound
    /// and the gateway serves only the foreign HTTP protocols. Set it (e.g.
    /// `0.0.0.0:4000`) to accept the native wire.
    #[arg(long)]
    listen_wire: Option<String>,

    /// Upstream scry ingest server address (the scry sink). Opt-in: when unset,
    /// no scry sink is built and the gateway forwards only to Loki/OpenSearch.
    /// At least one sink (this, `--loki-url`, or `--opensearch-url`) is required.
    #[arg(long)]
    upstream: Option<String>,

    /// Grafana Loki base URL (e.g. http://loki:3100). When set, logs are also
    /// pushed to `{url}/loki/api/v1/push`. Logs only.
    #[arg(long)]
    loki_url: Option<String>,

    /// OpenSearch base URL (e.g. http://opensearch:9200). When set, logs are
    /// also bulk-indexed to `{url}/_bulk`. Logs only.
    #[arg(long)]
    opensearch_url: Option<String>,

    /// OpenSearch index *prefix*. The write target is `<prefix>-<service>`, where
    /// `<service>` is the log stream's service name (`service.name`/`service`/
    /// `app`/`k8s_app`) or `general` when absent — each a rolling data stream.
    #[arg(long, default_value = "scry-logs")]
    opensearch_index: String,

    /// Disable OpenSearch self-management. By default the sink creates and keeps
    /// asserting the ISM rollover policy, the index template (with `flat_object`
    /// label mappings), and the per-service data streams — so cluster-side drift
    /// can't silently break ingest. Set this to assume the cluster owns them.
    #[arg(long)]
    opensearch_unmanaged: bool,

    /// OpenSearch rollover trigger size (per backing index). ISM rolls a data
    /// stream's write index over when it reaches this size. No auto-delete.
    #[arg(long, default_value = "30gb")]
    opensearch_rollover_size: String,

    /// OpenSearch rollover trigger age (per backing index).
    #[arg(long, default_value = "1d")]
    opensearch_rollover_age: String,

    /// How often to re-assert the OpenSearch managed assets (ISM policy + index
    /// template), correcting drift. Also re-asserted at startup and on a write error.
    #[arg(long, value_parser = parse_duration, default_value = "5m")]
    opensearch_reconcile_interval: Duration,

    /// Sign OpenSearch requests with **AWS SigV4** — required for Amazon
    /// OpenSearch Service (managed domains) and OpenSearch Serverless, which
    /// reject unsigned requests. Credentials come from the standard AWS chain
    /// (env vars, shared profile, EKS IRSA, EC2/ECS IMDS). Leave off for a
    /// self-hosted cluster.
    #[arg(long)]
    opensearch_aws_sigv4: bool,

    /// AWS region for SigV4 signing. Falls back to the resolved AWS config
    /// (`AWS_REGION` / profile) when unset. Only used with `--opensearch-aws-sigv4`.
    #[arg(long)]
    opensearch_aws_region: Option<String>,

    /// SigV4 signing name: `es` for Amazon OpenSearch Service (managed domains),
    /// `aoss` for OpenSearch Serverless. Only used with `--opensearch-aws-sigv4`.
    #[arg(long, default_value = "es")]
    opensearch_aws_service: String,

    /// Mimir base URL (e.g. http://mimir:9009). When set, metrics are also
    /// re-emitted as Prometheus remote-write to `{url}/api/v1/push`. Metrics only.
    #[arg(long)]
    mimir_url: Option<String>,

    /// Mimir tenant ID, sent as the `X-Scope-OrgID` header on every push.
    /// Required by multi-tenant Mimir; leave unset for a single-tenant cluster.
    #[arg(long)]
    mimir_tenant: Option<String>,

    /// Custom CA certificate (PEM file, may contain a bundle) added to the trust
    /// store for the HTTPS sinks (Loki / OpenSearch / Mimir). Augments the
    /// built-in roots — use it for endpoints fronted by a private/internal CA.
    #[arg(long)]
    ca_cert: Option<PathBuf>,

    /// Per-sink queue depth. Each sink drains its own bounded queue; on overflow
    /// it drops + counts (best-effort), so this bounds buffering during a
    /// downstream outage.
    #[arg(long, default_value_t = 1024)]
    sink_queue_cap: usize,

    /// HTTP client timeout for the Loki/OpenSearch/Mimir sinks.
    #[arg(long, value_parser = parse_duration, default_value = "30s")]
    sink_http_timeout: Duration,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();

    // ── Build the sinks ────────────────────────────────────────────────
    // Every sink is opt-in; at least one must be configured. The scry sink is
    // not special — a gateway that only tees logs to Loki/OpenSearch needs no
    // scry server at all.
    let mut sinks: Vec<SinkHandle> = Vec::new();

    // scry sink (opt-in via --upstream): the worker connects lazily, so a
    // down/absent upstream at startup is not fatal.
    if let Some(upstream) = args.upstream.clone() {
        let conn = ScryConnect {
            addr: upstream,
            agent_id: *Uuid::now_v7().as_bytes(),
            hostname: hostname_string(),
            signals: SIGNAL_BIT_METRICS | SIGNAL_BIT_LOGS | SIGNAL_BIT_TRACES | SIGNAL_BIT_PROFILES,
            resource_attrs: vec![LabelPair {
                key: "service".into(),
                value: "scry-gateway".into(),
            }],
        };
        sinks.push(spawn_sink(
            "scry",
            ACCEPT_ALL,
            args.sink_queue_cap,
            move |rx| ScrySink::new(conn).run(rx),
        ));
    }

    // Optional HTTP sinks (Loki/OpenSearch logs, Mimir metrics) share one
    // reqwest client, which carries any custom CA certificate.
    if args.loki_url.is_some() || args.opensearch_url.is_some() || args.mimir_url.is_some() {
        let http = build_http_client(args.sink_http_timeout, args.ca_cert.as_deref())?;

        if let Some(url) = args.loki_url.clone() {
            let sink = LokiSink::new(http.clone(), &url);
            sinks.push(spawn_sink(
                "loki",
                SIGNAL_BIT_LOGS,
                args.sink_queue_cap,
                move |rx| sink.run(rx),
            ));
            tracing::info!(url = %url, "loki sink enabled (logs)");
        }
        if let Some(url) = args.opensearch_url.clone() {
            let managed = !args.opensearch_unmanaged;
            let signer = if args.opensearch_aws_sigv4 {
                Some(Arc::new(
                    build_sigv4_signer(
                        args.opensearch_aws_region.clone(),
                        args.opensearch_aws_service.clone(),
                    )
                    .await?,
                ))
            } else {
                None
            };
            let sink = OpenSearchSink::new(
                http.clone(),
                OpenSearchConfig {
                    base: url.clone(),
                    prefix: args.opensearch_index.clone(),
                    manage: managed,
                    rollover_size: args.opensearch_rollover_size.clone(),
                    rollover_age: args.opensearch_rollover_age.clone(),
                    reconcile_interval: args.opensearch_reconcile_interval,
                    signer,
                },
            );
            sinks.push(spawn_sink(
                "opensearch",
                SIGNAL_BIT_LOGS,
                args.sink_queue_cap,
                move |rx| sink.run(rx),
            ));
            tracing::info!(
                url = %url,
                prefix = %args.opensearch_index,
                managed,
                sigv4 = args.opensearch_aws_sigv4,
                "opensearch sink enabled (logs)"
            );
        }
        if let Some(url) = args.mimir_url.clone() {
            let sink = MimirSink::new(http.clone(), &url, args.mimir_tenant.clone());
            sinks.push(spawn_sink(
                "mimir",
                SIGNAL_BIT_METRICS,
                args.sink_queue_cap,
                move |rx| sink.run(rx),
            ));
            tracing::info!(
                url = %url,
                tenant = args.mimir_tenant.as_deref().unwrap_or("(none)"),
                "mimir sink enabled (metrics)"
            );
        }
    }

    if sinks.is_empty() {
        bail!(
            "no sinks configured: set at least one of --upstream (scry), \
             --loki-url, --opensearch-url, or --mimir-url"
        );
    }

    let sink_names: Vec<&str> = sinks.iter().map(|s| s.name()).collect();
    tracing::info!(
        listen = %args.listen,
        listen_wire = args.listen_wire.as_deref().unwrap_or("(disabled)"),
        upstream = args.upstream.as_deref().unwrap_or("(disabled)"),
        sinks = ?sink_names,
        "scry-gateway ready"
    );

    let state = AppState::new(sinks);

    // ── Shutdown plumbing: one signal (SIGINT or SIGTERM) fans out to
    //    every server. SIGTERM matters in k8s, where the agent→gateway
    //    deployment lives. ─────────────────────────────────────────────
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        tracing::info!("shutdown signal received");
        let _ = shutdown_tx.send(true);
    });

    // ── HTTP (foreign) server ──────────────────────────────────────────
    let http_fut = {
        let app = router(state.clone());
        let listen = args.listen.clone();
        let mut rx = shutdown_rx.clone();
        async move {
            let listener = tokio::net::TcpListener::bind(&listen)
                .await
                .with_context(|| format!("binding HTTP listener on {listen}"))?;
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = rx.changed().await;
                })
                .await
                .context("HTTP server error")?;
            Ok::<(), anyhow::Error>(())
        }
    };

    // ── Native wire server (opt-in via --listen-wire) ──────────────────
    match args.listen_wire.clone() {
        Some(addr) => {
            let mut rx = shutdown_rx.clone();
            let wire_fut = serve_wire(addr, state.clone(), async move {
                let _ = rx.changed().await;
            });
            tokio::try_join!(http_fut, wire_fut)?;
        }
        None => {
            http_fut.await?;
        }
    }

    tracing::info!("scry-gateway shutting down");
    Ok(())
}

/// Resolve when the process receives SIGINT (ctrl_c) or SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

/// Resolve AWS credentials + region from the standard chain and build the SigV4
/// signer for the OpenSearch sink. Region precedence: `--opensearch-aws-region`,
/// then whatever the AWS config resolves (`AWS_REGION` / profile).
async fn build_sigv4_signer(region: Option<String>, service: String) -> Result<SigV4Signer> {
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
    if let Some(r) = region.clone() {
        loader = loader.region(aws_config::Region::new(r));
    }
    let cfg = loader.load().await;
    let resolved_region = cfg
        .region()
        .map(|r| r.as_ref().to_string())
        .or(region)
        .context("no AWS region for OpenSearch SigV4: set --opensearch-aws-region or AWS_REGION")?;
    let provider = cfg
        .credentials_provider()
        .context("no AWS credentials resolved for OpenSearch SigV4 (env, profile, IRSA, IMDS)")?;
    tracing::info!(region = %resolved_region, service = %service, "opensearch SigV4 signing enabled");
    Ok(SigV4Signer::new(provider, resolved_region, service))
}

fn hostname_string() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| "scry-gateway".to_string())
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let (num, mult) = if let Some(v) = s.strip_suffix("ms") {
        (v, 1u64)
    } else if let Some(v) = s.strip_suffix('s') {
        (v, 1000)
    } else if let Some(v) = s.strip_suffix('m') {
        (v, 60_000)
    } else {
        (s, 1000)
    };
    let base: u64 = num.parse().map_err(|_| format!("invalid duration: {s}"))?;
    Ok(Duration::from_millis(base * mult))
}
