//! Block-event pub/sub over Valkey.
//!
//! Publishers send [`Envelope`]-framed [`BlockEvent`]s to per-signal channels
//! (`scry/blocks/<signal>`); each instance subscribes to the signals it
//! serves and applies received events to its catalog (via
//! `scry_cluster::apply_event`). This is the low-latency convergence tier — a
//! best-effort hint, with cursor polling and the full walk as the
//! source-of-truth backstops, so a dropped publish is never a correctness
//! problem.
//!
//! Subscriptions use a dedicated `fred` [`SubscriberClient`] (a subscribed
//! Redis/Valkey connection can't issue normal commands) with
//! [`manage_subscriptions`](SubscriberClient::manage_subscriptions) so
//! channels are automatically re-subscribed after a reconnect.

use anyhow::{Context, Result};
use fred::clients::SubscriberClient;
use fred::prelude::*;
use fred::types::Message;
use scry_block::Envelope;

/// The pub/sub channel an event for `signal` is published on.
pub fn channel_for(signal: &str) -> String {
    format!("scry/blocks/{signal}")
}

/// Publish one envelope to its signal's channel. Returns the number of
/// subscribers that received it (informational).
pub async fn publish_envelope(client: &Client, env: &Envelope) -> Result<u64> {
    let channel = channel_for(env.event.signal());
    let bytes = env.to_bytes().context("serialise envelope")?;
    let receivers: i64 = client
        .publish(channel, bytes)
        .await
        .context("PUBLISH block event")?;
    Ok(receivers.max(0) as u64)
}

/// Build and connect a subscriber, subscribed to `scry/blocks/<signal>` for
/// each signal. Returns the client (kept alive by the caller) and a broadcast
/// receiver of raw [`Message`]s — parse each with [`parse_envelope`].
pub async fn subscribe_blocks(
    url: &str,
    signals: &[&str],
) -> Result<(SubscriberClient, tokio::sync::broadcast::Receiver<Message>)> {
    let config = Config::from_url(url).with_context(|| format!("parsing Valkey url {url}"))?;
    let sub = Builder::from_config(config)
        .build_subscriber_client()
        .context("building Valkey subscriber")?;
    sub.init().await.context("connecting Valkey subscriber")?;
    // Re-subscribe automatically across reconnects.
    sub.manage_subscriptions();
    for s in signals {
        sub.subscribe(channel_for(s))
            .await
            .with_context(|| format!("subscribe {}", channel_for(s)))?;
    }
    let rx = sub.message_rx();
    Ok((sub, rx))
}

/// Parse a received pub/sub [`Message`] into an [`Envelope`]. Returns `None`
/// (logging) for an unparseable payload — a malformed message must never abort
/// the consumer loop.
pub fn parse_envelope(msg: &Message) -> Option<Envelope> {
    let bytes = match msg.value.as_bytes() {
        Some(b) => b,
        None => {
            tracing::warn!(channel = %msg.channel, "pub/sub message had non-bytes payload; skipping");
            return None;
        }
    };
    match Envelope::from_bytes(bytes) {
        Ok(env) => Some(env),
        Err(e) => {
            tracing::warn!(channel = %msg.channel, error = %e, "unparseable block event; skipping");
            None
        }
    }
}
