//! Integration tests against a conditional-write-capable S3 endpoint.
//!
//! Skipped unless `SCRY_OBJSTORE_*` env vars are set. The dev harness is:
//!
//! ```
//! ./scripts/dev-seaweedfs-up.sh
//! set -a; source docker/seaweedfs/.env; set +a
//! cargo test -p scry-objstore --test s3_compat -- --nocapture
//! ```
//!
//! This is deliberately not a `#[ignore]` test: absent configuration skips at
//! runtime, while the dedicated CI service job supplies SeaweedFS.

use bytes::Bytes;
use object_store::{path::Path, ObjectStore, ObjectStoreExt};
use scry_objstore::{open, probe_conditional_writes, ObjStoreConfig};

fn cfg_or_skip() -> Option<ObjStoreConfig> {
    const REQUIRED_ENV: &str = "SCRY_S3_COMPAT_REQUIRED";
    let configured =
        std::env::vars_os().any(|(key, _)| key.to_string_lossy().starts_with("SCRY_OBJSTORE_"));
    if !configured && std::env::var_os(REQUIRED_ENV).is_none() {
        eprintln!("skipping: SCRY_OBJSTORE_* is not configured");
        return None;
    }
    Some(
        ObjStoreConfig::from_env()
            .unwrap_or_else(|error| panic!("invalid S3 compatibility-test configuration: {error}")),
    )
}

#[tokio::test]
async fn roundtrip_put_get_list_delete() {
    let cfg = match cfg_or_skip() {
        Some(c) => c,
        None => return,
    };
    let store = open(&cfg).await.expect("open objstore");

    // Use a per-test prefix so concurrent runs don't trample each other.
    let prefix = format!("test/roundtrip/{}", uuid_like(std::time::SystemTime::now()));
    let key_a = Path::from(format!("{prefix}/a.bin"));
    let key_b = Path::from(format!("{prefix}/b.bin"));

    // PUT
    store
        .put(&key_a, Bytes::from_static(b"hello scry").into())
        .await
        .expect("put a");
    store
        .put(&key_b, Bytes::from_static(b"hello again").into())
        .await
        .expect("put b");

    // GET
    let got = store.get(&key_a).await.expect("get a");
    let bytes = got.bytes().await.expect("bytes a");
    assert_eq!(&bytes[..], b"hello scry");

    // LIST under prefix
    use futures::StreamExt;
    let list_prefix = Path::from(prefix.clone());
    let mut s = store.list(Some(&list_prefix));
    let mut listed: Vec<String> = Vec::new();
    while let Some(m) = s.next().await {
        listed.push(m.expect("list entry").location.to_string());
    }
    listed.sort();
    assert_eq!(
        listed,
        vec![format!("{prefix}/a.bin"), format!("{prefix}/b.bin")]
    );

    // DELETE
    store.delete(&key_a).await.expect("delete a");
    store.delete(&key_b).await.expect("delete b");
}

#[tokio::test]
async fn conditional_create_and_update_are_atomic() {
    let cfg = match cfg_or_skip() {
        Some(c) => c,
        None => return,
    };
    let store = open(&cfg).await.expect("open objstore");
    let prefix = Path::from(format!(
        "test/conditional/{}",
        uuid_like(std::time::SystemTime::now())
    ));
    probe_conditional_writes(store.as_ref(), &prefix)
        .await
        .expect("S3 backend must enforce conditional create and ETag update");
}

// Cheap monotonic-ish suffix without pulling in uuid as a dev-dep.
fn uuid_like(t: std::time::SystemTime) -> String {
    let nanos = t
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{nanos:x}")
}
