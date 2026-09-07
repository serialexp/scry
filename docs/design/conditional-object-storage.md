# Conditional object-storage contract — Design

Status: complete
Owner: Bart
Last updated: 2026-09-07

## Implementation status

### Done

- [x] **Conditional primitives.** `scry-objstore` exposes atomic create and ETag/version compare-and-swap helpers through `object_store::PutMode`.
- [x] **Semantic capability probe.** The probe verifies rejected creates/updates preserve bytes, current-version update works, and concurrent create and ETag-CAS races each have exactly one winner.
- [x] **Conforming development backend.** Pinned SeaweedFS 4.45 replaces Garage as the canonical local S3 harness.
- [x] **Live integration coverage.** The backend-neutral `s3_compat` test exercises ordinary CRUD and conditional semantics through `PooledStore`.
- [x] **CI coverage.** A dedicated SeaweedFS job runs the live S3 contract.
- [x] **Development migration.** Storage smokes and profile tools share the SeaweedFS environment loader; deprecated Garage lifecycle commands forward for one release.

### Outstanding

_(nothing in this design)_

## Why this exists

Scry's immutable UUID block paths do not require conditional writes, so the original local Garage harness deliberately omitted them. The error-monitoring control plane adds immutable logical IDs and revision heads whose correctness requires S3 conditional create and ETag compare-and-swap. Testing only against a backend that accepts but ignores those headers would make the most important race behavior untested.

This contract makes conditional behavior an explicit opt-in storage capability. Existing block ingest/query remains compatible with ordinary S3-compatible stores; a future role that uses control-plane records must run the capability probe and fail closed when it does not pass.

## Contract

- `put_create` maps to `PutMode::Create` / S3 `If-None-Match: *`. Existing keys return `AlreadyExists` and retain their bytes.
- `put_update` maps to `PutMode::Update(UpdateVersion)` / S3 ETag `If-Match`. A stale version returns `Precondition` and retains current bytes.
- Callers preserve both ETag and version when returned, because object-store implementations may use either.
- Ambiguous transport failure is resolved by reading the key/version and comparing the expected content digest before retrying.
- A backend is conforming only if the semantic probe passes, including concurrent create and ETag-CAS races. Header acceptance alone is insufficient.

AWS S3 provides the required operations. SeaweedFS 4.45 routes `If-None-Match: *` and strong ETag `If-Match` to filer write conditions evaluated atomically under the owning path lock, and is the pinned local/CI implementation. The conforming SeaweedFS bucket keeps object versioning/object lock disabled. Upstream issue #8073 recorded a versioning-plus-locking bug fixed in SeaweedFS 4.09; the semantic probe remains authoritative for any future configuration.

## Development harness

`docker/seaweedfs/docker-compose.yml` runs one `weed mini`, binds S3 only to `127.0.0.1:8333`, persists `/data`, and uses fixed local-only credentials and bucket `scry-dev`. `scripts/dev-seaweedfs-up.sh` performs signed S3 readiness and bucket creation before writing `docker/seaweedfs/.env` atomically. It also runs a CLI conditional probe when the installed AWS CLI exposes the required flags; the Rust `s3_compat` test is the authoritative capability check.

Storage-bearing scripts source `scripts/lib/dev-objstore.sh`. `DEV_OBJSTORE_ENV` can point at another disposable S3 environment. Destructive reset refuses a bucket not named `scry-dev` unless `ALLOW_NON_DEV_BUCKET_RESET=1` is explicit.

Garage remains relevant historical context in old decisions and measurements, but is no longer the runnable development backend. The transitional `dev-garage-{up,down}.sh` wrappers invoke SeaweedFS and should be removed after one release.

## Verification

Hermetic tests use `InMemory`, `PooledStore`, and a deliberately non-conforming store that strips preconditions. The live `s3_compat` test runs against SeaweedFS locally and is required—not skippable—by the dedicated CI job. Newer AWS CLI versions additionally let the harness check rejected create/stale update behavior before producing its environment file.

## References

- `crates/objstore/src/conditional.rs`
- `crates/objstore/tests/s3_compat.rs`
- `docker/seaweedfs/`
- `scripts/lib/dev-objstore.sh`
- AWS S3 conditional writes documentation
- SeaweedFS routed conditional-write implementation
- [Error monitoring architecture](error-monitoring.md)
