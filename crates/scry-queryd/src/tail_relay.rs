//! The `scry query` live-tail front-door (D-053).
//!
//! A **transparent relay**: the `scry tail` client connects here and speaks the
//! exact same tail sub-protocol it would to an ingester (Hello → HelloAck →
//! Subscribe → a stream of `TailRecord`s). The relay, in turn, acts as a tail
//! *client* to every ingester it discovers in the Valkey registry
//! ([`scry_valkey::discover_tail_endpoints`]), fanning their live records back
//! down to the one client. The wire frames are reused verbatim — no
//! query-schema involvement.
//!
//! Why a *separate* listener from the query port: queryd's query wire
//! (`QueryFrame`) and this tail sub-protocol (`Frame`) are different binschema
//! unions whose first bytes collide, so they cannot share a socket.
//!
//! **No Valkey ⇒ refuse.** Without a registry there is nothing to discover, so
//! the relay replies `Error(ERR_TAIL_UNAVAILABLE)` and closes rather than
//! silently streaming nothing.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use scry_proto::{
    build,
    constants::{
        Signal, ERR_BAD_MATCHER, ERR_HELLO_REQUIRED, ERR_PROTOCOL_VERSION, ERR_TAIL_UNAVAILABLE,
        PROTOCOL_VERSION_V0,
    },
    framing::{read_frame, write_frame},
    Frame, FrameMsg,
};
use scry_tail::dial_subscribe;
use scry_valkey::{discover_tail_endpoints, ValkeyClient};
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Bound on records buffered from all upstreams toward one client. Full ⇒ the
/// upstream `dial_subscribe` sender blocks briefly; a slow client can't grow
/// memory unbounded (best-effort — the ingester tap already drops on its side).
const RELAY_CHANNEL_CAP: usize = 8192;

/// Accept tail-client connections on `listen` and serve each until it hangs up
/// or `shutdown` fires. `valkey` is cloned per connection for discovery; `None`
/// makes every subscription refuse with `ERR_TAIL_UNAVAILABLE`.
pub async fn serve_tail_relay(
    listen: SocketAddr,
    valkey: Option<ValkeyClient>,
    rediscover: Duration,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<()> {
    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("binding tail-relay listener on {listen}"))?;
    info!(%listen, "live-tail front-door ready");

    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("tail-relay shutting down");
                return Ok(());
            }
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(x) => x,
                    Err(e) => { warn!(error = %e, "tail-relay accept failed"); continue; }
                };
                let valkey = valkey.clone();
                tokio::spawn(async move {
                    if let Err(e) = serve_conn(stream, peer, valkey, rediscover).await {
                        warn!(%peer, error = %format!("{e:#}"), "tail-relay connection ended with error");
                    }
                });
            }
        }
    }
}

/// One tail-client connection: handshake, read the `Subscribe`, then either
/// refuse (no Valkey) or fan-in from the discovered ingesters.
async fn serve_conn(
    stream: TcpStream,
    peer: SocketAddr,
    valkey: Option<ValkeyClient>,
    rediscover: Duration,
) -> Result<()> {
    stream.set_nodelay(true).ok();
    let (rd, wr) = stream.into_split();
    let mut rd = BufReader::new(rd);
    let mut wr = BufWriter::new(wr);

    // ── Handshake: expect Hello, reply HelloAck (mirrors scry-server). ──
    let first = read_frame::<Frame, _>(&mut rd)
        .await
        .context("reading Hello")?;
    let hello = match first.msg {
        FrameMsg::Hello(h) => h,
        _ => {
            let _ = write_frame(
                &mut wr,
                &build::error(ERR_HELLO_REQUIRED, "Hello required first"),
            )
            .await;
            let _ = wr.flush().await;
            return Ok(());
        }
    };
    if hello.protocol_version != PROTOCOL_VERSION_V0 {
        let _ = write_frame(
            &mut wr,
            &build::error(
                ERR_PROTOCOL_VERSION,
                &format!(
                    "server supports v{PROTOCOL_VERSION_V0:#06x}; client asked for v{:#06x}",
                    hello.protocol_version
                ),
            ),
        )
        .await;
        let _ = wr.flush().await;
        return Ok(());
    }
    write_frame(
        &mut wr,
        &build::hello_ack(build::HelloAckArgs {
            protocol_version: PROTOCOL_VERSION_V0,
            writer_id: "scry-queryd-tail",
            session_id: 1,
            capabilities: 0,
            suggested_batch_bytes: 0,
            max_batch_bytes: 0,
            max_inflight_batches: 0,
        }),
    )
    .await?;
    wr.flush().await?;

    // ── Expect a Subscribe. ──
    let sub = match read_frame::<Frame, _>(&mut rd)
        .await
        .context("reading Subscribe")?
        .msg
    {
        FrameMsg::Subscribe(s) => s,
        FrameMsg::Goodbye(_) => return Ok(()),
        other => {
            warn!(%peer, kind = ?std::mem::discriminant(&other), "expected Subscribe on tail relay");
            let _ = write_frame(
                &mut wr,
                &build::error(ERR_BAD_MATCHER, "expected a Subscribe frame"),
            )
            .await;
            let _ = wr.flush().await;
            return Ok(());
        }
    };

    // v1: logs only.
    if sub.signal != Signal::Logs as u8 {
        let _ = write_frame(
            &mut wr,
            &build::error(ERR_BAD_MATCHER, "only the logs signal is tailable"),
        )
        .await;
        let _ = wr.flush().await;
        return Ok(());
    }

    let specs: Vec<String> = sub.matchers.into_iter().map(|m| m.spec).collect();
    // Validate locally for a clean error (each upstream re-validates too).
    if let Err(e) = scry_match::LabelFilter::parse(&specs) {
        let _ = write_frame(
            &mut wr,
            &build::error(ERR_BAD_MATCHER, &format!("bad matcher: {e}")),
        )
        .await;
        let _ = wr.flush().await;
        return Ok(());
    }

    // ── Refuse without Valkey (no way to discover ingesters). ──
    let valkey = match valkey {
        Some(v) => v,
        None => {
            info!(%peer, "refusing tail: no Valkey configured");
            let _ = write_frame(
                &mut wr,
                &build::error(
                    ERR_TAIL_UNAVAILABLE,
                    "live tail requires Valkey for ingester discovery (or tail an ingester directly with `scry tail --ingest`)",
                ),
            )
            .await;
            let _ = wr.flush().await;
            return Ok(());
        }
    };

    info!(%peer, matchers = specs.len(), "tail relay: fanning in from discovered ingesters");
    relay(peer, &mut rd, &mut wr, valkey, Arc::new(specs), rediscover).await
}

/// The fan-in loop: rediscover ingesters on a tick, keep one upstream
/// `dial_subscribe` per live address, and forward every record downstream until
/// the client hangs up.
async fn relay<R, W>(
    peer: SocketAddr,
    rd: &mut R,
    wr: &mut W,
    valkey: ValkeyClient,
    matchers: Arc<Vec<String>>,
    rediscover: Duration,
) -> Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let (tx, mut rx) = mpsc::channel::<scry_proto::generated::TailRecordOutput>(RELAY_CHANNEL_CAP);
    // addr → upstream task. We keep a `tx` clone alive here, so `rx` never
    // closes while the client is connected even with zero upstreams.
    let mut upstreams: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();

    let mut tick = tokio::time::interval(rediscover.max(Duration::from_millis(250)));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let result = loop {
        tokio::select! {
            // Downstream: record → frame → client.
            rec = rx.recv() => {
                match rec {
                    Some(r) => {
                        let frame = build::tail_record(build::TailRecordArgs {
                            signal: r.signal,
                            ts_unix_nano: r.ts_unix_nano,
                            severity: r.severity,
                            labels: r.labels,
                            body: r.body,
                            attributes: r.attributes,
                        });
                        if write_frame(wr, &frame).await.is_err() || wr.flush().await.is_err() {
                            break Ok(()); // client gone
                        }
                    }
                    None => break Ok(()), // unreachable while `tx` held, but safe
                }
            }
            // Periodic rediscovery: reconcile the upstream set.
            _ = tick.tick() => {
                match discover_tail_endpoints(valkey.inner()).await {
                    Ok(addrs) => reconcile(&addrs, &mut upstreams, &tx, &matchers),
                    Err(e) => warn!(%peer, error = %e, "tail rediscovery failed; keeping current upstreams"),
                }
            }
            // Watch the client read half so a hangup/Goodbye tears us down.
            r = read_frame::<Frame, _>(rd) => {
                match r {
                    Ok(f) if matches!(f.msg, FrameMsg::Goodbye(_)) => break Ok(()),
                    Ok(_) => {} // ignore any other frame on a tail conn
                    Err(_) => break Ok(()), // EOF / framing error → hangup
                }
            }
        }
    };

    // Tear down every upstream (their socket close deregisters at each ingester).
    for (_, h) in upstreams {
        h.abort();
    }
    result
}

/// Bring the live upstream set in line with the discovered addresses: prune
/// finished tasks (so a restarted ingester is re-dialed), drop tasks whose
/// address vanished, and dial newly-appeared addresses.
fn reconcile(
    addrs: &[String],
    upstreams: &mut HashMap<String, tokio::task::JoinHandle<()>>,
    tx: &mpsc::Sender<scry_proto::generated::TailRecordOutput>,
    matchers: &Arc<Vec<String>>,
) {
    use std::collections::HashSet;
    let desired: HashSet<&str> = addrs.iter().map(String::as_str).collect();

    // Drop upstreams that vanished or whose task already ended.
    upstreams.retain(|addr, handle| {
        if !desired.contains(addr.as_str()) {
            handle.abort();
            return false;
        }
        if handle.is_finished() {
            return false; // re-dialed below if still desired
        }
        true
    });

    // Dial newly-desired (or re-appeared) addresses.
    for addr in addrs {
        if upstreams.contains_key(addr) {
            continue;
        }
        let addr_owned = addr.clone();
        let tx = tx.clone();
        let matchers = matchers.clone();
        debug!(upstream = %addr_owned, "tail relay dialing ingester");
        let handle = tokio::spawn(async move {
            if let Err(e) =
                dial_subscribe(&addr_owned, Signal::Logs as u8, &matchers, tx, None).await
            {
                debug!(upstream = %addr_owned, error = %format!("{e:#}"), "tail upstream ended");
            }
        });
        upstreams.insert(addr.clone(), handle);
    }
}
