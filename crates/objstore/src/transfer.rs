//! Streaming transfers between local files and single objects.
//!
//! Whole-database snapshots (the catalog, the error-monitoring store) are the
//! largest single objects scry moves, and they grow with the deployment.
//! Reading one into a `Vec` before a PUT, or collecting a GET body before
//! writing it out, costs the snapshot's full size in RAM at exactly the moment
//! a daemon is starting or already busy. These helpers keep memory bounded by
//! a few upload parts regardless of object size:
//!
//! - [`upload_file`] sends a file with a single PUT when it fits one part and
//!   as a multipart upload otherwise. A multipart object becomes visible only
//!   when the upload completes, so readers never observe a partial object;
//!   a failed upload is aborted.
//! - [`download_to_file`] streams a GET body to disk, refuses objects over a
//!   caller-supplied size limit before writing anything, verifies the length,
//!   and fsyncs before returning, so a caller can rename the file into place.

use std::path::Path as FsPath;

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore, ObjectStoreExt, WriteMultipart};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Bytes per multipart part, and the largest file sent with one PUT.
///
/// Above S3's 5 MiB minimum part size; 8 MiB × S3's 10,000-part limit allows
/// objects up to ~78 GiB, far beyond any snapshot.
pub const UPLOAD_PART_BYTES: usize = 8 * 1024 * 1024;

/// Parts uploaded concurrently. With one part being filled, peak buffered
/// data is `(UPLOAD_CONCURRENCY + 1) * UPLOAD_PART_BYTES`.
const UPLOAD_CONCURRENCY: usize = 2;

/// Upload the local file at `source` to `location` without holding more than
/// a few [`UPLOAD_PART_BYTES`] parts in memory. Returns the bytes uploaded.
///
/// The file must not change during the upload; a length that differs from
/// the size observed at open is an error (and the multipart upload is
/// aborted).
pub async fn upload_file(store: &dyn ObjectStore, source: &FsPath, location: &Path) -> Result<u64> {
    let mut file = tokio::fs::File::open(source)
        .await
        .with_context(|| format!("opening {} for upload", source.display()))?;
    let size = file
        .metadata()
        .await
        .with_context(|| format!("stat {}", source.display()))?
        .len();

    if size <= UPLOAD_PART_BYTES as u64 {
        let chunk = read_chunk(&mut file, source, UPLOAD_PART_BYTES).await?;
        if chunk.len() as u64 != size {
            bail!(
                "{} changed during upload: expected {size} bytes, read {}",
                source.display(),
                chunk.len()
            );
        }
        store
            .put(location, chunk.into())
            .await
            .with_context(|| format!("PUT {location}"))?;
        return Ok(size);
    }

    let upload = store
        .put_multipart(location)
        .await
        .with_context(|| format!("starting multipart upload of {location}"))?;
    let mut writer = WriteMultipart::new_with_chunk_size(upload, UPLOAD_PART_BYTES);
    let streamed: Result<u64> = async {
        let mut sent = 0u64;
        loop {
            writer
                .wait_for_capacity(UPLOAD_CONCURRENCY)
                .await
                .with_context(|| format!("uploading a part of {location}"))?;
            let chunk = read_chunk(&mut file, source, UPLOAD_PART_BYTES).await?;
            if chunk.is_empty() {
                break;
            }
            sent += chunk.len() as u64;
            writer.put(chunk);
        }
        if sent != size {
            bail!(
                "{} changed during upload: expected {size} bytes, read {sent}",
                source.display()
            );
        }
        Ok(sent)
    }
    .await;
    match streamed {
        Ok(sent) => {
            writer
                .finish()
                .await
                .with_context(|| format!("completing multipart upload of {location}"))?;
            Ok(sent)
        }
        Err(error) => {
            if let Err(abort) = writer.abort().await {
                tracing::warn!(%location, error = %abort, "aborting failed multipart upload failed");
            }
            Err(error)
        }
    }
}

/// Download `location` into `dest` (created or truncated), streaming the body.
///
/// Returns `Ok(None)` when the object does not exist, leaving `dest`
/// untouched. An object larger than `max_bytes` is refused before anything is
/// written. On success the file is fsynced and holds exactly the object's
/// bytes; on failure a partially written `dest` is removed.
pub async fn download_to_file(
    store: &dyn ObjectStore,
    location: &Path,
    dest: &FsPath,
    max_bytes: u64,
) -> Result<Option<u64>> {
    let result = match store.get(location).await {
        Ok(result) => result,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("GET {location}")),
    };
    let size = result.meta.size;
    if size > max_bytes {
        bail!("{location} is {size} bytes, over the {max_bytes}-byte download limit");
    }
    let mut file = tokio::fs::File::create(dest)
        .await
        .with_context(|| format!("creating {}", dest.display()))?;
    let copied: Result<u64> = async {
        let mut stream = result.into_stream();
        let mut written = 0u64;
        while let Some(chunk) = stream
            .try_next()
            .await
            .with_context(|| format!("reading {location} body"))?
        {
            written += chunk.len() as u64;
            if written > size {
                bail!("{location} body exceeds its advertised {size} bytes");
            }
            file.write_all(&chunk)
                .await
                .with_context(|| format!("writing {}", dest.display()))?;
        }
        if written != size {
            bail!("{location} body ended after {written} of {size} bytes");
        }
        file.sync_all()
            .await
            .with_context(|| format!("fsync {}", dest.display()))?;
        Ok(written)
    }
    .await;
    drop(file);
    match copied {
        Ok(written) => Ok(Some(written)),
        Err(error) => {
            let _ = tokio::fs::remove_file(dest).await;
            Err(error)
        }
    }
}

/// Read up to `limit` bytes (fewer only at end of file) into a fresh buffer.
async fn read_chunk(file: &mut tokio::fs::File, source: &FsPath, limit: usize) -> Result<Bytes> {
    let mut buf = Vec::with_capacity(limit);
    file.take(limit as u64)
        .read_to_end(&mut buf)
        .await
        .with_context(|| format!("reading {}", source.display()))?;
    Ok(Bytes::from(buf))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use tempfile::TempDir;

    use super::*;

    fn pattern(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i * 31 % 251) as u8).collect()
    }

    async fn round_trip(len: usize) {
        let dir = TempDir::new().unwrap();
        let source = dir.path().join("source");
        let data = pattern(len);
        std::fs::write(&source, &data).unwrap();
        let store = Arc::new(InMemory::new());
        let key = Path::from("snap/object");

        assert_eq!(
            upload_file(store.as_ref(), &source, &key).await.unwrap(),
            len as u64
        );
        let dest = dir.path().join("dest");
        assert_eq!(
            download_to_file(store.as_ref(), &key, &dest, len as u64)
                .await
                .unwrap(),
            Some(len as u64)
        );
        assert_eq!(std::fs::read(&dest).unwrap(), data);
    }

    #[tokio::test]
    async fn small_file_round_trips_with_a_single_put() {
        round_trip(0).await;
        round_trip(1234).await;
        round_trip(UPLOAD_PART_BYTES).await;
    }

    #[tokio::test]
    async fn large_file_round_trips_through_multipart() {
        // Several full parts plus a short tail.
        round_trip(3 * UPLOAD_PART_BYTES + 17).await;
    }

    #[tokio::test]
    async fn missing_object_is_none_and_leaves_no_file() {
        let dir = TempDir::new().unwrap();
        let dest = dir.path().join("dest");
        let store = InMemory::new();
        assert_eq!(
            download_to_file(&store, &Path::from("absent"), &dest, u64::MAX)
                .await
                .unwrap(),
            None
        );
        assert!(!dest.exists());
    }

    #[tokio::test]
    async fn oversize_object_is_refused_before_writing() {
        let dir = TempDir::new().unwrap();
        let dest = dir.path().join("dest");
        let store = InMemory::new();
        let key = Path::from("big");
        store.put(&key, pattern(100).into()).await.unwrap();
        let error = download_to_file(&store, &key, &dest, 99)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("download limit"), "{error}");
        assert!(!dest.exists(), "nothing is written for a refused object");
    }
}
