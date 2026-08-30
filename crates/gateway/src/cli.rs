//! `scry gateway` — the fan-out hub entry point (was the `scry-gateway` bin).
//!
//! Accepts records over the native binschema wire, OTLP/gRPC, **and** the foreign
//! HTTP push protocols (OTLP traces, Pyroscope profiles, Prometheus remote-write) and
//! forwards every record, best-effort, to every configured downstream sink.
//! Each sink is opt-in (at least one required): the scry ingest server
//! (`--upstream`), Grafana Loki, OpenSearch, and Mimir.

use scry_duration::parse_duration;
use scry_status::LocalStatus;
use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use clap::Parser;
use uuid::Uuid;

use crate::{
    loki::LokiSink,
    metrics::{GatewayMetrics, SinkKind, SinkReporter},
    mimir::MimirSink,
    opensearch::{OpenSearchConfig, OpenSearchSink},
    otlp_grpc::serve as serve_otlp_grpc,
    router, serve_wire,
    sink::{spawn_sink_instrumented, AppState, SinkHandle, ACCEPT_ALL},
    sink_scry::{ScryConnect, ScrySink},
    status::{GatewayStatus, ValkeyFleetSource},
};
use scry_httpsig::{build_http_client, build_sigv4_signer};
use scry_proto::{
    constants::{SIGNAL_BIT_LOGS, SIGNAL_BIT_METRICS, SIGNAL_BIT_PROFILES, SIGNAL_BIT_TRACES},
    LabelPair,
};
use tokio::sync::watch;

/// CLI arguments for the `scry gateway` subcommand.
#[derive(Parser, Debug)]
#[command(
    about = "fan-out push gateway for scry (native + OTLP/HTTP + OTLP/gRPC + Pyroscope + remote-write in; scry + Loki + OpenSearch + Mimir out)"
)]
pub struct Args {
    /// HTTP listen address (foreign protocols: /v1/traces, /ingest, /api/v1/write).
    #[arg(long, default_value = "0.0.0.0:4318")]
    listen: String,

    /// OTLP/gRPC trace listen address. Opt-in: when unset, the gateway accepts
    /// OTLP/HTTP protobuf only. Use 0.0.0.0:4317 for standard OTel exporters.
    #[arg(long)]
    listen_otlp_grpc: Option<String>,

    /// Native binschema ingest listen address (scry-agent and other native
    /// producers point here). Opt-in: when unset, no native listener is bound
    /// and the gateway serves only the foreign HTTP protocols. Set it (e.g.
    /// `0.0.0.0:4000`) to accept the native wire.
    #[arg(long)]
    listen_wire: Option<String>,

    /// Maximum simultaneous native-wire ingest sessions. Excess sessions are
    /// rejected immediately and should reconnect with backoff.
    #[arg(long, default_value_t = 256)]
    wire_max_connections: usize,

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

    /// Per-sink queue depth. Each item can contain a complete decoded batch, so
    /// keep this deliberately small; on overflow the sink drops + counts rather
    /// than retaining an outage-sized memory backlog.
    #[arg(long, default_value_t = 16)]
    sink_queue_cap: usize,

    /// HTTP client timeout for the Loki/OpenSearch/Mimir sinks.
    #[arg(long, value_parser = parse_duration, default_value = "30s")]
    sink_http_timeout: Duration,

    /// Valkey URL used to publish this gateway into Fleet. Falls back to
    /// SCRY_VALKEY_URL; absent means no fleet publication.
    #[arg(long)]
    valkey_url: Option<String>,

    /// Deployment namespace for Valkey keys. Falls back to
    /// SCRY_VALKEY_NAMESPACE, then `scry`.
    #[arg(long)]
    valkey_namespace: Option<String>,

    /// Local status HTTP endpoint. A bare flag binds 127.0.0.1:4098.
    #[arg(long, num_args = 0..=1, default_missing_value = "127.0.0.1:4098")]
    stats_listen: Option<String>,
}

/// Run the fan-out gateway: build the configured sinks, serve the foreign HTTP
/// protocols (+ the native wire if `--listen-wire`), and tee every record.
pub async fn run(args: Args) -> Result<()> {
    let valkey_url = args
        .valkey_url
        .clone()
        .or_else(|| std::env::var(scry_valkey::VALKEY_URL_ENV).ok());
    let status_enabled = valkey_url.is_some() || args.stats_listen.is_some();
    let metrics = status_enabled.then(|| Arc::new(GatewayMetrics::default()));

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
        let reporter = SinkReporter::new(metrics.clone(), SinkKind::Scry);
        sinks.push(spawn_sink_instrumented(
            "scry",
            ACCEPT_ALL,
            args.sink_queue_cap,
            metrics.clone(),
            move |rx| ScrySink::new(conn, reporter).run(rx),
        ));
    }

    // Optional HTTP sinks (Loki/OpenSearch logs, Mimir metrics) share one
    // reqwest client, which carries any custom CA certificate.
    if args.loki_url.is_some() || args.opensearch_url.is_some() || args.mimir_url.is_some() {
        let http = build_http_client(args.sink_http_timeout, args.ca_cert.as_deref())?;

        if let Some(url) = args.loki_url.clone() {
            let reporter = SinkReporter::new(metrics.clone(), SinkKind::Loki);
            let sink = LokiSink::new(http.clone(), &url, reporter);
            sinks.push(spawn_sink_instrumented(
                "loki",
                SIGNAL_BIT_LOGS,
                args.sink_queue_cap,
                metrics.clone(),
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
            let reporter = SinkReporter::new(metrics.clone(), SinkKind::OpenSearch);
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
                reporter,
            );
            sinks.push(spawn_sink_instrumented(
                "opensearch",
                SIGNAL_BIT_LOGS,
                args.sink_queue_cap,
                metrics.clone(),
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
            let reporter = SinkReporter::new(metrics.clone(), SinkKind::Mimir);
            let sink = MimirSink::new(http.clone(), &url, args.mimir_tenant.clone(), reporter);
            sinks.push(spawn_sink_instrumented(
                "mimir",
                SIGNAL_BIT_METRICS,
                args.sink_queue_cap,
                metrics.clone(),
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
        listen_otlp_grpc = args.listen_otlp_grpc.as_deref().unwrap_or("(disabled)"),
        listen_wire = args.listen_wire.as_deref().unwrap_or("(disabled)"),
        upstream = args.upstream.as_deref().unwrap_or("(disabled)"),
        sinks = ?sink_names,
        "scry-gateway ready"
    );

    let state = match &metrics {
        Some(metrics) => AppState::with_metrics(sinks, metrics.clone()),
        None => AppState::new(sinks),
    };
    let instance_uuid = Uuid::now_v7();
    let status: Option<Arc<GatewayStatus>> = metrics.clone().map(|metrics| {
        Arc::new(GatewayStatus::new(
            instance_uuid.to_string(),
            args.listen.clone(),
            args.listen_otlp_grpc.clone(),
            args.listen_wire.clone(),
            state.clone(),
            metrics,
        ))
    });
    let keys = scry_valkey::Keyspace::resolve(args.valkey_namespace.as_deref())?;
    let valkey = match valkey_url {
        Some(url) => Some(
            scry_valkey::ValkeyClient::connect(&url, instance_uuid, keys)
                .await
                .with_context(|| format!("connecting gateway to Valkey at {url}"))?,
        ),
        None => None,
    };
    let registration = match (&valkey, &status) {
        (Some(client), Some(status)) => {
            let source = status.clone();
            let producer: scry_valkey::StatusProducer = Arc::new(move || {
                serde_json::to_string(&source.snapshot()).expect("gateway status serializes")
            });
            Some(
                scry_valkey::StatusRegistration::spawn(
                    client,
                    instance_uuid,
                    scry_valkey::STATUS_TTL,
                    producer,
                )
                .await?,
            )
        }
        _ => None,
    };

    // ── Shutdown plumbing: one signal (SIGINT or SIGTERM) fans out to
    //    every server. SIGTERM matters in k8s, where the agent→gateway
    //    deployment lives. ─────────────────────────────────────────────
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        tracing::info!("shutdown signal received");
        let _ = shutdown_tx.send(true);
    });

    let status_fut = {
        let status = status.clone();
        let fleet = valkey
            .clone()
            .map(|client| Arc::new(ValkeyFleetSource(client)) as Arc<dyn scry_status::FleetSource>);
        let listen = args.stats_listen.clone();
        let mut rx = shutdown_rx.clone();
        async move {
            if let (Some(local), Some(listen)) = (status, listen) {
                scry_status::serve_status(
                    listen,
                    local,
                    fleet,
                    instance_uuid.to_string(),
                    async move {
                        let _ = rx.changed().await;
                    },
                )
                .await?;
            } else {
                let _ = rx.changed().await;
            }
            Ok::<(), anyhow::Error>(())
        }
    };

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

    // ── Optional OTLP/gRPC + native wire servers ──────────────────────
    let serve_result: Result<()> = async {
        match (args.listen_otlp_grpc.clone(), args.listen_wire.clone()) {
            (Some(grpc_addr), Some(wire_addr)) => {
                let mut grpc_rx = shutdown_rx.clone();
                let grpc_fut = serve_otlp_grpc(grpc_addr, state.clone(), async move {
                    let _ = grpc_rx.changed().await;
                });
                let mut wire_rx = shutdown_rx.clone();
                let wire_fut = serve_wire(
                    wire_addr,
                    state.clone(),
                    args.wire_max_connections,
                    async move {
                        let _ = wire_rx.changed().await;
                    },
                );
                tokio::try_join!(http_fut, grpc_fut, wire_fut, status_fut)?;
            }
            (Some(grpc_addr), None) => {
                let mut rx = shutdown_rx.clone();
                let grpc_fut = serve_otlp_grpc(grpc_addr, state.clone(), async move {
                    let _ = rx.changed().await;
                });
                tokio::try_join!(http_fut, grpc_fut, status_fut)?;
            }
            (None, Some(wire_addr)) => {
                let mut rx = shutdown_rx.clone();
                let wire_fut = serve_wire(
                    wire_addr,
                    state.clone(),
                    args.wire_max_connections,
                    async move {
                        let _ = rx.changed().await;
                    },
                );
                tokio::try_join!(http_fut, wire_fut, status_fut)?;
            }
            (None, None) => {
                tokio::try_join!(http_fut, status_fut)?;
            }
        }
        Ok(())
    }
    .await;

    if let Some(registration) = registration {
        let _ = tokio::time::timeout(Duration::from_secs(2), registration.deregister()).await;
    }
    if let Some(client) = valkey {
        let _ = tokio::time::timeout(Duration::from_secs(2), client.quit()).await;
    }
    tracing::info!("scry-gateway shutting down");
    serve_result
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
