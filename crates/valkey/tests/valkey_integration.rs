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
use std::sync::Arc;

use scry_valkey::{
    channel_for, discover_status_blobs, discover_tail_endpoints, parse_envelope, publish_envelope,
    subscribe_blocks, StatusRegistration, TailRegistration, ValkeyClient, ValkeyLeaseProvider,
    ValkeySink,
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
    assert!(
        held.fence().check().is_ok(),
        "fence still valid after renews"
    );

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
    publish_envelope(pubc.inner(), &env).await.expect("publish");

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

#[tokio::test]
#[ignore = "requires a real Valkey (scripts/dev-valkey-up.sh)"]
async fn tail_registration_is_discoverable_and_deregisters() {
    let c = client().await;

    // Two instances register distinct advertised addresses (the addr embeds a
    // UUID so a shared registry prefix can't collide across concurrent runs).
    let uuid_a = Uuid::now_v7();
    let uuid_b = Uuid::now_v7();
    let addr_a = format!("10.0.0.1:{}", &uuid_a.simple().to_string()[..8]);
    let addr_b = format!("10.0.0.2:{}", &uuid_b.simple().to_string()[..8]);

    let reg_a = TailRegistration::spawn(
        c.inner().clone(),
        uuid_a,
        addr_a.clone(),
        Duration::from_secs(10),
    )
    .await
    .expect("register A");
    let reg_b = TailRegistration::spawn(
        c.inner().clone(),
        uuid_b,
        addr_b.clone(),
        Duration::from_secs(10),
    )
    .await
    .expect("register B");

    // Discovery sees both (subset — a shared Valkey may host other entries).
    let found = discover_tail_endpoints(c.inner()).await.expect("discover");
    assert!(found.contains(&addr_a), "A discoverable: {found:?}");
    assert!(found.contains(&addr_b), "B discoverable: {found:?}");

    // Deregister A promptly; B remains.
    reg_a.deregister().await;
    let found = discover_tail_endpoints(c.inner()).await.expect("discover");
    assert!(
        !found.contains(&addr_a),
        "A gone after deregister: {found:?}"
    );
    assert!(found.contains(&addr_b), "B still present: {found:?}");

    reg_b.deregister().await;
}

#[tokio::test]
#[ignore = "requires a real Valkey (scripts/dev-valkey-up.sh)"]
async fn tail_registration_renews_past_its_ttl() {
    let c = client().await;
    let uuid = Uuid::now_v7();
    let addr = format!("10.0.0.9:{}", &uuid.simple().to_string()[..8]);

    // Short TTL: the ttl/3 heartbeat must keep the key alive past it.
    let reg = TailRegistration::spawn(
        c.inner().clone(),
        uuid,
        addr.clone(),
        Duration::from_millis(600),
    )
    .await
    .expect("register");

    tokio::time::sleep(Duration::from_millis(1500)).await;
    let found = discover_tail_endpoints(c.inner()).await.expect("discover");
    assert!(
        found.contains(&addr),
        "renewed registration still present: {found:?}"
    );

    reg.deregister().await;
}

#[tokio::test]
#[ignore = "requires a real Valkey (scripts/dev-valkey-up.sh)"]
async fn status_registration_publishes_fresh_snapshots_and_deregisters() {
    let c = client().await;

    // The producer stamps a unique marker (so a shared registry can't collide)
    // and a monotonic counter, so we can prove the value CHURNS each heartbeat
    // (the property that distinguishes the status registry from the tail one).
    let uuid = Uuid::now_v7();
    let marker = format!("marker-{}", uuid.simple());
    let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let m = marker.clone();
    let cnt = counter.clone();
    let producer: scry_valkey::StatusProducer = Arc::new(move || {
        let n = cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!(r#"{{"marker":"{m}","tick":{n}}}"#)
    });

    // Short TTL so the ttl/3 heartbeat must both keep the key alive AND refresh
    // its value past the TTL.
    let reg = StatusRegistration::spawn(
        c.inner().clone(),
        uuid,
        Duration::from_millis(600),
        producer,
    )
    .await
    .expect("register status");

    // Discoverable immediately (initial SET is synchronous).
    let blobs = discover_status_blobs(c.inner()).await.expect("discover");
    let mine: Vec<&String> = blobs.iter().filter(|b| b.contains(&marker)).collect();
    assert_eq!(mine.len(), 1, "exactly one of our blobs present: {blobs:?}");
    let first_tick: u64 = serde_json::from_str::<serde_json::Value>(mine[0]).unwrap()["tick"]
        .as_u64()
        .unwrap();

    // After several heartbeats the key is still alive (past TTL) and its value
    // has advanced (a re-published fresh snapshot, not just a TTL bump).
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let blobs = discover_status_blobs(c.inner()).await.expect("discover");
    let mine: Vec<&String> = blobs.iter().filter(|b| b.contains(&marker)).collect();
    assert_eq!(mine.len(), 1, "still present past TTL: {blobs:?}");
    let later_tick: u64 = serde_json::from_str::<serde_json::Value>(mine[0]).unwrap()["tick"]
        .as_u64()
        .unwrap();
    assert!(
        later_tick > first_tick,
        "status value churns each heartbeat ({first_tick} → {later_tick})"
    );

    // Deregister removes it promptly.
    reg.deregister().await;
    let blobs = discover_status_blobs(c.inner()).await.expect("discover");
    assert!(
        !blobs.iter().any(|b| b.contains(&marker)),
        "gone after deregister: {blobs:?}"
    );
}

// ── staged-deletions registry ────────────────────────────────────────
//
// The registry that lets an instance which was not listening — because it
// booted after the staging, or had never seen the block — find out which
// blocks its peers have already hidden. A staged deletion is deliberately
// invisible in the bucket (the objects stay put for the grace window), so
// this is the only place that truth lives outside the owner's catalog.

/// A fabricated meta: no bucket objects, which is fine because every assertion
/// here is about catalog rows.
fn staged_test_meta(uuid: Uuid) -> scry_block::BlockMeta {
    scry_block::BlockMeta {
        uuid,
        signal: "logs".to_string(),
        writer_id: Uuid::now_v7(),
        ts_min_unix_nano: 1_700_000_000_000_000_000,
        ts_max_unix_nano: 1_700_000_000_000_000_001,
        row_count: 10,
        byte_size: 100,
        schema_version: 1,
        level: 0,
        producer_version: String::new(),
        label_fingerprint_bloom: None,
        has_postings: false,
        postings_size_bytes: None,
        series_types: None,
        all_fingerprints: None,
        has_body_bloom: false,
        body_bloom_size_bytes: None,
        wal_seg_max: None,
        wal_shard: None,
        compacted_from: Vec::new(),
    }
}

#[tokio::test]
#[ignore = "requires a real Valkey (scripts/dev-valkey-up.sh)"]
async fn staged_deletions_round_trip() {
    let c = client().await;
    let now = 1_700_000_000_000_000_000u64;
    let eligible = now + 600_000_000_000;
    let mine = [Uuid::now_v7(), Uuid::now_v7()];

    scry_valkey::stage_deletions(c.inner(), &mine, eligible, now)
        .await
        .expect("stage");

    let listed = scry_valkey::list_staged_deletions(c.inner())
        .await
        .expect("list");
    for uuid in &mine {
        assert_eq!(
            listed.iter().find(|(u, _)| u == uuid).map(|(_, e)| *e),
            Some(eligible),
            "staged block {uuid} must come back with its peer's own deadline"
        );
    }

    scry_valkey::unstage_deletions(c.inner(), &mine)
        .await
        .expect("unstage");
    let listed = scry_valkey::list_staged_deletions(c.inner())
        .await
        .expect("list after unstage");
    assert!(
        !listed.iter().any(|(u, _)| mine.contains(u)),
        "reaped blocks must leave the registry: {listed:?}"
    );
}

/// The seam: retention emits `SoftDeleted` into a `BlockEventSink` and knows
/// nothing about Valkey. The sink is what mirrors it into the registry.
#[tokio::test]
#[ignore = "requires a real Valkey (scripts/dev-valkey-up.sh)"]
async fn sink_mirrors_soft_deleted_into_the_registry() {
    let c = client().await;
    let (sink, _task) = ValkeySink::spawn(c.inner().clone(), Uuid::now_v7());
    let now = 1_700_000_000_000_000_000u64;
    let eligible = now + 600_000_000_000;
    let uuid = Uuid::now_v7();

    sink.emit(BlockEvent::SoftDeleted {
        signal: "logs".to_string(),
        uuids: vec![uuid],
        deleted_at_unix_nano: now,
        delete_eligible_at_unix_nano: eligible,
    });

    // The publisher is a background task; give it a moment to drain.
    let staged = await_registry_contains(&c, uuid, true).await;
    assert!(
        staged,
        "a SoftDeleted emitted through the sink must appear in the registry"
    );

    sink.emit(BlockEvent::Deleted {
        signal: "logs".to_string(),
        uuids: vec![uuid],
    });
    let still_there = await_registry_contains(&c, uuid, false).await;
    assert!(
        !still_there,
        "once the objects are really gone the entry must go too"
    );
}

/// Poll the registry until `uuid`'s presence matches `want`, or we give up.
/// Returns the final observed presence.
async fn await_registry_contains(c: &ValkeyClient, uuid: Uuid, want: bool) -> bool {
    for _ in 0..50 {
        let listed = scry_valkey::list_staged_deletions(c.inner())
            .await
            .expect("list");
        let present = listed.iter().any(|(u, _)| *u == uuid);
        if present == want {
            return present;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    scry_valkey::list_staged_deletions(c.inner())
        .await
        .expect("list")
        .iter()
        .any(|(u, _)| *u == uuid)
}

/// End to end, and the whole reason the registry exists: an instance seeds its
/// catalog from the bucket — which still holds the objects of a block a peer
/// staged, because that is what the grace window is for — and then converges,
/// and the block stops being listed for queries.
#[tokio::test]
#[ignore = "requires a real Valkey (scripts/dev-valkey-up.sh)"]
async fn converge_hides_a_block_a_peer_already_staged() {
    let c = client().await;
    let tmp = tempfile::tempdir().unwrap();
    let catalog =
        scry_catalog::Catalog::open(&tmp.path().join("cat.sqlite"), "test-bucket").unwrap();

    // What the bucket walk would have produced: the block, live.
    let uuid = Uuid::now_v7();
    catalog.insert_block(&staged_test_meta(uuid)).unwrap();
    assert_eq!(
        catalog.list_blocks().unwrap().len(),
        1,
        "live after the walk"
    );

    // What a peer did before we existed, and never re-announces.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    scry_valkey::stage_deletions(c.inner(), &[uuid], now + 600_000_000_000, now)
        .await
        .expect("peer stages the deletion");

    let applied = scry_valkey::converge_staged_deletions(c.inner(), &catalog)
        .await
        .expect("converge");

    assert!(applied >= 1, "at least our own entry was applied");
    assert!(
        catalog.list_blocks().unwrap().is_empty(),
        "a block the peer staged must not be served after convergence"
    );

    scry_valkey::unstage_deletions(c.inner(), &[uuid])
        .await
        .ok();
}
