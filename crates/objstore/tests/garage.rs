//! Integration test against a running Garage instance.
//!
//! Skipped unless `SCRY_OBJSTORE_*` env vars are set. The dev harness
//! is:
//!
//! ```
//! ./scripts/dev-garage-up.sh
//! set -a; source docker/garage/.env; set +a
//! cargo test -p scry-objstore --test garage -- --nocapture
//! ```
//!
//! This is deliberately not a `#[ignore]` test — Garage being up is
//! detected at runtime. CI will pick this up once we wire a service
//! container into the pipeline.

use bytes::Bytes;
use object_store::{path::Path, ObjectStore, ObjectStoreExt};
use scry_objstore::{open, ObjStoreConfig};

fn cfg_or_skip() -> Option<ObjStoreConfig> {
    match ObjStoreConfig::from_env() {
        Ok(c) => Some(c),
        Err(e) => {
            eprintln!("skipping: {e}");
            None
        }
    }
}

#[tokio::test]
async fn roundtrip_put_get_list_delete() {
    let cfg = match cfg_or_skip() {
        Some(c) => c,
        None => return,
    };
    let store = open(&cfg).expect("open objstore");

    // Use a per-test prefix so concurrent runs don't trample each other.
    let prefix = format!(
        "test/roundtrip/{}",
        uuid_like(std::time::SystemTime::now())
    );
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


// Cheap monotonic-ish suffix without pulling in uuid as a dev-dep.
fn uuid_like(t: std::time::SystemTime) -> String {
    let nanos = t
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{nanos:x}")
}
