# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`scry` is an opinionated single-binary replacement for the Grafana observability stack (Loki + Tempo + Mimir + Pyroscope). It stores metrics, logs, traces, and profiles as immutable parquet blocks in S3-compatible object storage and serves queries from the same bucket. One deployment, one tenant, no hash ring, no service split.

Status: **v0.7 complete — all four signals flow end to end and query back, and logs have full-text body search.** A record goes in via the wire protocol (or a foreign protocol terminated by `scry-gateway`: OTLP traces, Pyroscope profiles, Prometheus remote-write), lands in a per-writer WAL, gets uploaded as a parquet block (with a per-signal postings sidecar for metrics/logs and a per-block `body.bloom` full-text skip sidecar for logs; traces/profiles carry neither) to Garage, shows up in a SQLite catalog (online or reconciled from the bucket), and is queryable through DataFusion either locally (`scry-query`) or over the `scry-queryd` daemon's binschema-framed wire. v0.1 (storage), v0.2 (metrics ingest+query), v0.3 (query daemon), v0.4 (logs), the unnumbered push-gateway milestone, v0.5 (traces query: `--trace-id` by-id lookup + promoted-column matchers, predicate pushdown), v0.6 (profiles retrieval query: raw pprof blob out), and v0.7 (full-text log search: `--grep`/`body_contains` accelerated by an inline byte-trigram bloom skip sidecar) are all sealed by `scripts/smoke.sh`, which asserts the full ingest → store → **query** round-trip per signal (plus a `--trace-id` lookup for traces and a `--grep` ≡ `body LIKE` equivalence + bloom-sidecar check for logs). Metrics/logs preselect via postings; logs additionally skip whole blocks via the body bloom on `--grep`; traces/profiles push matcher/time/trace-id filters as parquet row predicates and prune on row-group stats. See `docs/decisions.md § D-029…D-035` for the rationale and the README milestone table for scope. The full-text bloom is one-sided (false positives cost a scan, never a missed match; exact `contains` is the backstop), case-sensitive, byte-trigram (n=3), and sized optimally per block at seal; see D-035. Still not implemented: profiles flamegraph aggregation (pprof parse + stack-merge — deferred, Grafana renders pre-aggregated data), PromQL (demoted — own UI removes the Grafana-compat driver), regex/case-insensitive full-text, compaction/retention (v0.8), and the multi-bucket / Valkey convergence from `docs/ARCHITECTURE.md`.

## Commands

Workspace build / test:

```bash
cargo build --release --workspace
cargo test --workspace
cargo test -p scry-proto fingerprint::tests::order_independence   # single test
```

Smoke-test the wire protocol end-to-end (two terminals):

```bash
./target/release/scry-ingestd --listen 127.0.0.1:4000
./target/release/noise-spewer --addr 127.0.0.1:4000 --rate 50 --duration 3s
```

The sink prints per-session counters on disconnect (`batches=… samples=… log_entries=… spans=… profiles=… rejected=0`).

Smoke-test the storage + query path end-to-end (single terminal — wraps the sink, spewer, scry-list reconcile, a row-count assertion, and a `scry-query` round-trip assertion):

```bash
scripts/dev-garage-up.sh         # one-time per machine
scripts/smoke.sh                  # default SIGNAL=dummy (v0.1 path)
SIGNAL=metrics scripts/smoke.sh   # v0.2: also asserts ≥1 block w/ postings + query round-trip
SIGNAL=logs    scripts/smoke.sh   # v0.4: same, for logs
SIGNAL=both    scripts/smoke.sh   # v0.4 exit criterion: metrics + logs through one sink
```

For `metrics`/`logs`/`both` the script additionally reconciles a fresh catalog and runs `scry-query --signal <sig>` against it, asserting the queried row count equals the sink-accepted count — i.e. ingest → store → query is loss-free. Tweak via env vars: `BATCHES=20000 RATE=4000 scripts/smoke.sh` for stress runs. The script empties the dev Garage bucket on every run; don't point it at a bucket whose contents you want to keep.

The smoke script also wraps `scry-ingestd` in `/usr/bin/time -v` and prints a service-performance block at the end — peak RSS, user/sys CPU, records/sec, and **CPU-µs / record** (the headline regression sentinel; rate-independent, unlike `%CPU` which slides with the inter-batch idle gap). Full `time -v` output is kept in `$SMOKE_DIR/sink.time` for context (ctx switches, page faults, etc.). The script sends SIGINT directly to the scry-ingestd PID, not to `time`, because GNU time does not forward signals to its child.

Regenerate Rust bindings from the wire schema (only path that should touch `crates/proto/src/generated.rs` or `crates/binschema-runtime/src/*.rs`):

```bash
scripts/gen-proto.sh
# or, if binschema lives somewhere else:
BINSCHEMA_DIR=/path/to/binschema scripts/gen-proto.sh
```

The script shells out to a local checkout of binschema (`$HOME/Projects/binschema` by default). Both the generated bindings and the vendored runtime are committed so a normal `cargo build` never needs node or binschema installed.

## Workspace layout

Crates in `crates/`:

**Protocol**
- **`binschema-runtime`** — vendored binschema Rust runtime (bitstream, encoder/decoder context). Regenerated by `scripts/gen-proto.sh`; do not hand-edit.
- **`scry-proto`** — the wire protocol crate. Re-exports the generated types from `generated.rs` and adds four hand-written modules: `framing` (length-prefixed framing over async streams, 32 MiB cap), `constants` (Signal enum incl. `Dummy = 0xFE` for v0.1, ACK/REJECT/ERR/GOODBYE numeric codes, batch-size defaults), `fingerprint` (xxh3-64 over canonically-sorted labels, via `twox-hash`), and `build` (ergonomic `Frame`-returning constructors).

**Storage (v0.1)**
- **`scry-objstore`** — thin wrapper over apache `object_store` (`AmazonS3Builder`). `ObjStoreConfig::from_env()` reads `SCRY_OBJSTORE_*`. Path-style for Garage; HTTP allowed for localhost. No `put_if_absent` — Garage 1.0.x silently ignores `If-None-Match: *`, and v0.1's UUID v7 paths make conditional PUT unnecessary.
- **`scry-wal`** — per-writer, per-signal append-only WAL. `<dir>/<signal>/wal-<u64-seq>.log`, framed `[len:u32 BE][crc32:u32 BE][payload]`, 256 MiB segments, fsync on rotation, replay tolerates torn tails. `Wal::{open, append, rotate, mark_uploaded, replay}`. **Not internally synchronized** — wrap in `Arc<tokio::sync::Mutex<_>>` when sharing.
- **`scry-block`** — block builder + reader. `DummyBlockBuilder` buffers records column-shaped (three Vecs, sorted on close), serialises a single-row-group parquet (zstd-3), uploads parquet + JSON sidecar via `object_store::ObjectStore::put`. `BlockMeta` is the sidecar struct; `block_path` builds the canonical key (`<signal>/<yyyy>/<mm>/<dd>/<writer_id>/<block_uuid>.{parquet,meta.json}`).
- **`scry-catalog`** — rusqlite-bundled catalog. Schema verbatim from `ARCHITECTURE.md § The catalog § Schema` minus the `buckets` table (single-bucket v0.1). `Catalog::{open, insert_block, list_blocks, get_block, block_count, reconcile_from_bucket}`. WAL journal mode, partial indices on `(signal,date,ts_min,ts_max)` and `(bucket,signal,date,level)` where `deleted_at IS NULL`.

**Server**
- **`scry-server`** — the ingest server as a library. Owns the TCP listener, handshake, Batch/Ping/Goodbye dispatch, the per-session counters, and the process-scoped `DummyPipeline` (WAL + active block builder + optional catalog, shared across sessions via `Arc<Mutex<_>>`, per `ARCHITECTURE.md § The WAL` — WAL is per-writer, not per-session). Pipeline is constructed by the caller and passed to `Server::new(...)`, so the eventual single `scry` binary can share it with future background uploaders or catalog-lookup code. `Server::serve_with_shutdown(shutdown_fut)` binds, accepts until the future completes, then flushes the pipeline once and returns.

**Binaries**
- **`noise-spewer`** — TCP client. Does the Hello handshake, then emits random metrics/logs/traces/profiles/dummy batches at a target rate, respecting `max_inflight_batches` from the HelloAck. Payload generators live in `gen.rs`. `--max-batches N` for an exact count (rate × duration is off-by-one).
- **`scry-ingestd`** — the ingest server daemon; a thin CLI around `scry-server`. Parses flags, optionally constructs a `DummyPipeline` from `SCRY_OBJSTORE_*` env + `--wal-dir [--catalog]`, then hands it to `Server::serve_with_shutdown(ctrl_c)`. All wire-protocol behaviour lives in `scry-server`. (Formerly `noise-sink`.)
- **`scry-list`** — catalog inspector. Opens a SQLite catalog (creates the schema if missing), runs `reconcile_from_bucket` unless `--no-reconcile`, prints one line per block + a `# total rows=N bytes=N` trailer.

**Tooling**
- `docker/garage/` — single-node Garage (`dxflrs/garage:v1.0.1`) for local development. `scripts/dev-garage-up.sh` brings it up and runs `init.sh` (idempotent layout + bucket + key creation; writes credentials to `docker/garage/.env`).
- `scripts/smoke.sh` — the v0.1 scripted exit criterion; see Commands.

## Architecture you have to read multiple files to grasp

The wire protocol is a **flat tagged union, not header+payload.** Each `Frame` IS a discriminated union whose first byte is the tag matching one of nine `FrameMsg` variants (Hello/HelloAck/Batch/BatchAck/FlowControl/Ping/Pong/Goodbye/Error). Framing is `[len: u32 BE][Frame]` with a 32 MiB cap (`framing::MAX_FRAME_BYTES`). There is intentionally no separate `protocol.header` section in the schema — binschema's header+payload model didn't fit, and we dropped it. If you find yourself wanting one, read the schema's comments first.

The schema (`proto/ingest.schema.json`) is the source of truth for both formats and constants. `crates/proto/src/constants.rs` mirrors the numeric values by hand so call sites can `match` on them; if you change a code in the schema, mirror it there. The schema currently has **zero `computed` fields** (`length_of`, `crc32_of`, …). `crates/proto/src/build.rs` relies on this: it constructs `Frame`s via `XxxInput { ... }.into()`, which populates `const` fields (tag bytes) from the generated `From` impls. If a `computed` field is ever added, `.into()` will silently fill it with the codegen default — the on-the-wire bytes are still correct (the encode path recomputes them), but the in-memory `Output` struct will be misleading. The caveat is documented at the top of `build.rs`; audit every constructor there before merging such a change.

Series-dictionary compression: every signal that carries labels (`MetricsBatch.series`, `LogsBatch.streams`, etc.) references a per-batch dictionary entry by `fingerprint: u64` rather than re-shipping labels per sample. The fingerprint is xxh3-64 over `key\0value\0`-joined, lexicographically sorted `(key, value)` pairs (`fingerprint::fingerprint`). The same function must be used on the encode and decode side or fingerprints won't match.

Batches are zstd-compressed at level 3 by the spewer; the sink decompresses based on `Batch.compression` (`COMPRESSION_NONE = 0`, `COMPRESSION_ZSTD = 1`). `uncompressed_size` is on the wire so the sink can pre-size its buffer; `record_count` is on the wire so the sink can sanity-check before decoding.

The big-picture architecture for the eventual storage/query system — block layout, bucket pool, catalog, WAL, ingest/query split, per-signal specialisations, resource isolation — is in `docs/ARCHITECTURE.md` (~2000 lines). Architectural decisions with rationale (D-001 … D-030) are in `docs/decisions.md`. Read both before changing anything beyond the noise harnesses; in particular D-027 (resource isolation between signals and workloads) and D-028 (profiling as a development principle) shape any future hot-path code.

## Conventions

- **Big-endian everywhere on the wire.** `xxd` on a captured stream should be human-readable.
- **Reasons are numeric.** Free-text `message` fields in BatchAck/Goodbye/Error are operator-log only. Both sides decide based on the numeric code.
- **Versioning lives in the handshake.** `Hello.protocol_version` is the only negotiation point; no per-message version bytes. Bump it and let the server take `min(agent, server)`.
- **`unsafe_code = "forbid"`** workspace-wide (`Cargo.toml`).
- **Release profile uses thin LTO and `codegen-units = 1`.** Don't override casually — the harnesses are sized to give realistic numbers.
