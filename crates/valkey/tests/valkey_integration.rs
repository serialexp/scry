//! Integration tests against a *real* Valkey.
//!
//! These are `#[ignore]`d so `cargo test --workspace` stays node-/docker-free
//! (mirroring the Garage smoke split). Run them explicitly after
//! `scripts/dev-valkey-up.sh`:
//!
//! ```bash
//! scripts/dev-valkey-up.sh
//! cargo test -p scry-valkey -- --ignored
//! # or against a non-default endpoint:
//! SCRY_VALKEY_URL=redis://host:6379 cargo test -p scry-valkey -- --ignored
//! ```
//!
//! Each test namespaces its keys/channels with a unique UUID so concurrent or
//! repeated runs never collide and no cleanup is required (leases carry a TTL;
//! stray keys self-expire).

use std::time::Duration;

use fred::prelude::ClientLike;
use scry_block::{BlockEvent, BlockEventSink, Envelope};
use scry_cluster::{LeaseGuard, LeaseProvider};
use scry_valkey::{
    channel_for, parse_envelope, publish_envelope, subscribe_blocks, ValkeyClient,
    ValkeyLeaseProvider, ValkeySink,
};
use uuid::Uuid;

fn url() -> String {
    std::env::var("SCRY_VALKEY_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string())
}

async fn client() -> ValkeyClient {
    ValkeyClient::connect(&url(), Uuid::now_v7())
        .await
        .expect("connect to Valkey (is scripts/dev-valkey-up.sh running?)")
}

fn unique_key(kind: &str) -> String {
    format!("scry/test/{kind}/{}", Uuid::now_v7())
}

fn deleted_event(signal: &str) -> BlockEvent {
    BlockEvent::Deleted {
        signal: signal.to_string(),
        uuids: vec![Uuid::now_v7()],
    }
}

#[tokio::test]
#[ignore = "requires a real Valkey (scripts/dev-valkey-up.sh)"]
async fn lease_is_mutually_exclusive() {
    let c = client().await;
    let provider = ValkeyLeaseProvider::new(c.inner().clone());
    let key = unique_key("lease");

    let first = provider
        .try_acquire(&key, Duration::from_secs(10))
        .await
        .expect("acquire")
        .expect("first acquisition wins");
    // The fence is valid while held.
    assert!(first.fence().check().is_ok());

    // A second contender cannot take a key that is still held.
    let second = provider
        .try_acquire(&key, Duration::from_secs(10))
        .await
        .expect("acquire");
    assert!(second.is_none(), "second acquisition must fail while held");

    // Release invalidates the fence and frees the key…
    first.release().await;

    // …so a fresh contender can now win.
    let third = provider
        .try_acquire(&key, Duration::from_secs(10))
        .await
        .expect("acquire")
        .expect("acquisition after release must win");
    third.release().await;
}

#[tokio::test]
#[ignore = "requires a real Valkey (scripts/dev-valkey-up.sh)"]
async fn lease_renews_past_its_initial_ttl() {
    let c = client().await;
    let provider = ValkeyLeaseProvider::new(c.inner().clone());
    let key = unique_key("renew");

    // Short TTL: the auto-renew (every ttl/3 ≈ 200ms) must keep it alive.
    let held = provider
        .try_acquire(&key, Duration::from_millis(600))
        .await
        .expect("acquire")
        .expect("acquire wins");

    // Wait well past the initial TTL; renewal should have extended it.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(held.fence().check().is_ok(), "fence still valid after renews");

    // A contender still cannot take it — proof the key didn't expire.
    let contender = provider
        .try_acquire(&key, Duration::from_millis(600))
        .await
        .expect("acquire");
    assert!(contender.is_none(), "renewed lease must still be held");

    held.release().await;
}

#[tokio::test]
#[ignore = "requires a real Valkey (scripts/dev-valkey-up.sh)"]
async fn dropping_a_lease_invalidates_its_fence() {
    let c = client().await;
    let provider = ValkeyLeaseProvider::new(c.inner().clone());
    let key = unique_key("drop");

    let held = provider
        .try_acquire(&key, Duration::from_secs(10))
        .await
        .expect("acquire")
        .expect("acquire wins");
    let fence = held.fence();
    assert!(fence.check().is_ok());

    drop(held);
    // Drop latches the fence invalid synchronously (no await needed).
    assert!(fence.check().is_err(), "dropped lease must fence off");
}

#[tokio::test]
#[ignore = "requires a real Valkey (scripts/dev-valkey-up.sh)"]
async fn pubsub_round_trips_an_envelope() {
    let signal = format!("metrics-{}", Uuid::now_v7());

    // Subscriber first, so it is listening before we publish.
    let (sub, mut rx) = subscribe_blocks(&url(), &[signal.as_str()])
        .await
        .expect("subscribe");

    // Give the subscription a beat to register on the server.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let pubc = client().await;
    let event = deleted_event(&signal);
    let env = Envelope::new(Uuid::now_v7(), 1, event.clone());
    publish_envelope(pubc.inner(), &env)
        .await
        .expect("publish");

    let msg = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("did not receive published message in time")
        .expect("broadcast channel closed");

    assert_eq!(msg.channel.to_string(), channel_for(&signal));
    let got = parse_envelope(&msg).expect("parse envelope");
    assert_eq!(got.event.signal(), signal);

    let _ = sub.quit().await;
}

#[tokio::test]
#[ignore = "requires a real Valkey (scripts/dev-valkey-up.sh)"]
async fn sink_publishes_emitted_events() {
    let signal = format!("logs-{}", Uuid::now_v7());

    let (sub, mut rx) = subscribe_blocks(&url(), &[signal.as_str()])
        .await
        .expect("subscribe");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let pubc = client().await;
    let origin = Uuid::now_v7();
    let (sink, task) = ValkeySink::spawn(pubc.inner().clone(), origin);

    sink.emit(deleted_event(&signal));

    let msg = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("sink did not publish in time")
        .expect("broadcast channel closed");
    let got = parse_envelope(&msg).expect("parse envelope");
    assert_eq!(got.origin, origin, "sink stamps this instance's origin");
    assert_eq!(got.event.signal(), signal);

    drop(sink);
    let _ = task.await;
    let _ = sub.quit().await;
}
