//! Conditional, metadata-last publication for occurrence projections.
//!
//! Both deterministic objects are immutable. The data object is created first and
//! the commit marker is created only after the data object was either created or
//! verified byte-for-byte. A commit marker is therefore the visibility boundary.

use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{
    path::Path, Error as ObjectStoreError, GetOptions, ObjectStore, ObjectStoreExt, PutPayload,
};
use sha2::{Digest, Sha256};

use crate::projection::ProjectionKeys;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationOutcome {
    Created,
    ExistingIdentical,
    Collision,
}

#[derive(Debug, thiserror::Error)]
pub enum PublicationError {
    #[error("conditional create of occurrence projection {kind} object `{path}` failed: {source}")]
    Put {
        kind: &'static str,
        path: String,
        #[source]
        source: ObjectStoreError,
    },
    #[error("reading existing occurrence projection {kind} object `{path}` failed: {source}")]
    ReadBack {
        kind: &'static str,
        path: String,
        #[source]
        source: ObjectStoreError,
    },
}

/// Publishes deterministic occurrence-projection bytes without ever overwriting.
///
/// `data_bytes` and `commit_bytes` must be the final deterministic representations
/// for `keys`. Existing objects are accepted only after their size, SHA-256 digest,
/// and exact bytes have all been verified. Reads are capped at the expected byte
/// length even if a backend returns metadata or a stream that violates its contract.
///
/// A failed create can be ambiguous (for example, a timeout after the server stored
/// the object), so every create error is followed by a safe read-back attempt. An
/// identical read-back completes idempotently; a different object is a collision.
/// If read-back cannot establish either result, the original create error is
/// returned, except that `AlreadyExists` reports the read-back failure because the
/// object is known to exist.
pub async fn publish_occurrence_projection(
    store: Arc<dyn ObjectStore>,
    keys: &ProjectionKeys,
    data_bytes: Bytes,
    commit_bytes: Bytes,
) -> Result<PublicationOutcome, PublicationError> {
    let data = create_or_verify(
        store.as_ref(),
        "data",
        &Path::from(keys.data.as_str()),
        &data_bytes,
    )
    .await?;
    if data == ObjectOutcome::Collision {
        return Ok(PublicationOutcome::Collision);
    }

    let commit = create_or_verify(
        store.as_ref(),
        "commit",
        &Path::from(keys.commit.as_str()),
        &commit_bytes,
    )
    .await?;
    if commit == ObjectOutcome::Collision {
        return Ok(PublicationOutcome::Collision);
    }

    Ok(
        if data == ObjectOutcome::Created || commit == ObjectOutcome::Created {
            // This includes recovery from either incomplete-object state. Under normal
            // metadata-last publication only data-written/commit-missing can occur.
            PublicationOutcome::Created
        } else {
            PublicationOutcome::ExistingIdentical
        },
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObjectOutcome {
    Created,
    ExistingIdentical,
    Collision,
}

async fn create_or_verify(
    store: &dyn ObjectStore,
    kind: &'static str,
    path: &Path,
    expected: &Bytes,
) -> Result<ObjectOutcome, PublicationError> {
    match scry_objstore::put_create(store, path, PutPayload::from(expected.clone())).await {
        Ok(_) => Ok(ObjectOutcome::Created),
        Err(put_error) => match verify_existing(store, kind, path, expected).await {
            Ok(outcome) => Ok(outcome),
            Err(read_error) if matches!(put_error, ObjectStoreError::AlreadyExists { .. }) => {
                Err(read_error)
            }
            Err(_) => Err(PublicationError::Put {
                kind,
                path: path.to_string(),
                source: put_error,
            }),
        },
    }
}

async fn verify_existing(
    store: &dyn ObjectStore,
    kind: &'static str,
    path: &Path,
    expected: &[u8],
) -> Result<ObjectOutcome, PublicationError> {
    let meta = store
        .head(path)
        .await
        .map_err(|source| PublicationError::ReadBack {
            kind,
            path: path.to_string(),
            source,
        })?;
    if meta.size != expected.len() as u64 {
        return Ok(ObjectOutcome::Collision);
    }
    if expected.is_empty() {
        return Ok(ObjectOutcome::ExistingIdentical);
    }

    // The exact range makes the body bound independent of backend stream behavior.
    let result = store
        .get_opts(
            path,
            GetOptions {
                range: Some((0..expected.len() as u64).into()),
                ..Default::default()
            },
        )
        .await
        .map_err(|source| PublicationError::ReadBack {
            kind,
            path: path.to_string(),
            source,
        })?;

    let expected_digest = Sha256::digest(expected);
    let mut actual_digest = Sha256::new();
    let mut offset = 0usize;
    let mut exact = true;
    let mut stream = result.into_stream();
    while let Some(chunk) =
        stream
            .try_next()
            .await
            .map_err(|source| PublicationError::ReadBack {
                kind,
                path: path.to_string(),
                source,
            })?
    {
        let Some(end) = offset.checked_add(chunk.len()) else {
            return Ok(ObjectOutcome::Collision);
        };
        // Do not consume an unbounded or contract-violating response.
        if end > expected.len() {
            return Ok(ObjectOutcome::Collision);
        }
        actual_digest.update(&chunk);
        exact &= chunk.as_ref() == &expected[offset..end];
        offset = end;
    }

    if offset == expected.len()
        && actual_digest.finalize().as_slice() == expected_digest.as_slice()
        && exact
    {
        Ok(ObjectOutcome::ExistingIdentical)
    } else {
        Ok(ObjectOutcome::Collision)
    }
}

#[cfg(test)]
mod tests {
    use object_store::{memory::InMemory, ObjectStoreExt};

    use super::*;

    fn keys() -> ProjectionKeys {
        ProjectionKeys {
            data: "_scry/errors/v1/occurrences/2026-03-20/019d0000-0000-7000-8000-000000000001/gen.parquet".to_owned(),
            commit: "_scry/errors/v1/occurrences/2026-03-20/019d0000-0000-7000-8000-000000000001/gen.commit.json".to_owned(),
        }
    }

    #[tokio::test]
    async fn identical_republication_is_idempotent() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let keys = keys();
        let data = Bytes::from_static(b"deterministic parquet");
        let commit = Bytes::from_static(b"{\"digest\":\"deterministic\"}");

        assert_eq!(
            publish_occurrence_projection(store.clone(), &keys, data.clone(), commit.clone())
                .await
                .unwrap(),
            PublicationOutcome::Created
        );
        assert_eq!(
            publish_occurrence_projection(store, &keys, data, commit)
                .await
                .unwrap(),
            PublicationOutcome::ExistingIdentical
        );
    }

    #[tokio::test]
    async fn differing_data_or_commit_is_a_collision_and_never_overwrites() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let keys = keys();
        let data = Bytes::from_static(b"winner data");
        let commit = Bytes::from_static(b"winner commit");
        publish_occurrence_projection(store.clone(), &keys, data.clone(), commit.clone())
            .await
            .unwrap();

        assert_eq!(
            publish_occurrence_projection(
                store.clone(),
                &keys,
                Bytes::from_static(b"contender data"),
                commit.clone(),
            )
            .await
            .unwrap(),
            PublicationOutcome::Collision
        );
        assert_eq!(
            publish_occurrence_projection(
                store.clone(),
                &keys,
                data.clone(),
                Bytes::from_static(b"contender commit"),
            )
            .await
            .unwrap(),
            PublicationOutcome::Collision
        );

        assert_eq!(
            store
                .get(&Path::from(keys.data.as_str()))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            data
        );
        assert_eq!(
            store
                .get(&Path::from(keys.commit.as_str()))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            commit
        );
    }

    #[tokio::test]
    async fn same_length_differences_are_collisions() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let keys = keys();
        publish_occurrence_projection(
            store.clone(),
            &keys,
            Bytes::from_static(b"abc"),
            Bytes::from_static(b"commit-a"),
        )
        .await
        .unwrap();

        assert_eq!(
            publish_occurrence_projection(
                store,
                &keys,
                Bytes::from_static(b"abd"),
                Bytes::from_static(b"commit-a"),
            )
            .await
            .unwrap(),
            PublicationOutcome::Collision
        );
    }

    #[tokio::test]
    async fn recovers_data_without_commit_by_publishing_commit_last() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let keys = keys();
        let data = Bytes::from_static(b"already uploaded data");
        let commit = Bytes::from_static(b"commit after recovery");
        scry_objstore::put_create(
            store.as_ref(),
            &Path::from(keys.data.as_str()),
            PutPayload::from(data.clone()),
        )
        .await
        .unwrap();

        assert_eq!(
            publish_occurrence_projection(store.clone(), &keys, data, commit.clone())
                .await
                .unwrap(),
            PublicationOutcome::Created
        );
        assert_eq!(
            store
                .get(&Path::from(keys.commit.as_str()))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            commit
        );
    }
}
