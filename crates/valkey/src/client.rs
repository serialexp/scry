//! The Valkey connection handle + a health watch.
//!
//! [`ValkeyClient`] wraps a `fred` [`Client`] (auto-reconnecting under the
//! hood) and a [`watch`] channel tracking whether the connection is currently
//! healthy. The maintenance/convergence drivers read the watch to pick an
//! adaptive cadence (poll faster while degraded). Construction is fallible and
//! **optional**: [`ValkeyClient::from_env`] returns `Ok(None)` when
//! `SCRY_VALKEY_URL` is unset, so the daemons degrade to a correct
//! single-instance path (no pub/sub, no lease ⇒ maintenance pauses) rather
//! than refusing to start.

use anyhow::{Context, Result};
use fred::prelude::*;
use tokio::sync::watch;
use uuid::Uuid;

/// Environment variable naming the Valkey endpoint (e.g.
/// `redis://127.0.0.1:6379`). Unset ⇒ Valkey-less degraded operation.
pub const VALKEY_URL_ENV: &str = "SCRY_VALKEY_URL";

/// A connected Valkey handle shared across the lease provider, the pub/sub
/// sink, and the subscriber. Cheap to clone (the inner `fred::Client` is an
/// `Arc` handle).
#[derive(Clone)]
pub struct ValkeyClient {
    client: Client,
    /// This instance's identity — the lease holder token prefix and the
    /// pub/sub event origin.
    holder: Uuid,
    /// `true` while the connection is healthy. Lags reality by one event but
    /// is only ever used to choose a polling cadence, never for correctness.
    health: watch::Receiver<bool>,
}

impl ValkeyClient {
    /// Connect to `url` and start the health watcher. `holder` is this
    /// instance's stable id (reused across reconnects).
    pub async fn connect(url: &str, holder: Uuid) -> Result<Self> {
        let config = Config::from_url(url).with_context(|| format!("parsing Valkey url {url}"))?;
        let client = Builder::from_config(config)
            .build()
            .context("building Valkey client")?;
        client.init().await.context("connecting to Valkey")?;

        let (tx, rx) = watch::channel(client.is_connected());
        spawn_health_watcher(&client, tx);

        tracing::info!(%url, %holder, "connected to Valkey");
        Ok(Self {
            client,
            holder,
            health: rx,
        })
    }

    /// Connect from `SCRY_VALKEY_URL`, or `Ok(None)` if it is unset. `holder`
    /// is this instance's stable id.
    pub async fn from_env(holder: Uuid) -> Result<Option<Self>> {
        match std::env::var(VALKEY_URL_ENV) {
            Ok(url) if !url.trim().is_empty() => Ok(Some(Self::connect(url.trim(), holder).await?)),
            _ => {
                tracing::info!(
                    "{VALKEY_URL_ENV} unset; running without Valkey (single-instance: no pub/sub, maintenance paused)"
                );
                Ok(None)
            }
        }
    }

    /// The underlying `fred` client, for commands / publish.
    pub fn inner(&self) -> &Client {
        &self.client
    }

    /// This instance's id (lease holder, event origin).
    pub fn holder(&self) -> Uuid {
        self.holder
    }

    /// A receiver for the connection-health flag (`true` = healthy).
    pub fn health(&self) -> watch::Receiver<bool> {
        self.health.clone()
    }

    /// Whether the connection is currently up.
    pub fn is_connected(&self) -> bool {
        self.client.is_connected()
    }

    /// Close the connection (best-effort).
    pub async fn quit(&self) {
        let _ = self.client.quit().await;
    }
}

/// Spawn a task that flips `tx` to `false` on errors / unresponsive
/// connections and back to `true` on (re)connect.
fn spawn_health_watcher(client: &Client, tx: watch::Sender<bool>) {
    let mut reconnect = client.reconnect_rx();
    let mut errors = client.error_rx();
    let mut unresponsive = client.unresponsive_rx();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                r = reconnect.recv() => match r {
                    Ok(_) => { let _ = tx.send(true); }
                    Err(_) => break, // sender dropped (client gone)
                },
                e = errors.recv() => match e {
                    Ok(_) => { let _ = tx.send(false); }
                    Err(_) => break,
                },
                u = unresponsive.recv() => match u {
                    Ok(_) => { let _ = tx.send(false); }
                    Err(_) => break,
                },
            }
        }
    });
}
