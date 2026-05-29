# CURRENT_TASK — handoff

This document hands off two things to a fresh agent working **from the
`scry` repo root** (`/home/bart/Projects/scry`): (1) a just-completed,
**uncommitted** chunk of work — traces + profiles storage — and (2) the
**next task** Bart picked: a new `scry-gateway` crate that terminates
foreign push protocols (Pyroscope, OTLP) and forwards as binschema.

Addressee is Bart. Keep all of `~/.claude/CLAUDE.md`'s rules in mind
(notably: #2 quality over speed, #3 never proclaim success — Bart
decides, #5 this doc, #7 do it right, #13/#15 git discipline, never
commit unless explicitly asked).

---

## Part 1 — DONE but UNCOMMITTED: traces + profiles storage

### What it is
scry stores four signals as parquet blocks on S3. Metrics (v0.2) and
logs (v0.4) flowed the full spine; **traces and profiles did not** — the
server only *counted* them. This work makes the daemon actually **store**
traces and profiles, mirroring the metrics/logs pattern end-to-end:
wire → streaming decode → per-signal block builder → per-writer WAL →
parquet + meta sidecar → SQLite catalog.

Locked design decisions (from Bart, earlier):
- **Storage only.** No query (trace-by-id is v0.5 query, flamegraph
  aggregation v0.6 query — both deferred). Verified via `scry-list`
  reconcile + row-count equality, no query round-trip.
- **Profiles = opaque blob.** One parquet row per `ProfileBlob`, pprof
  bytes stored verbatim in a `Binary` column. No pprof parsing.
- **Traces = native nested Arrow.** Scalar span fields as typed columns,
  attributes as `Map<Utf8,Utf8>`, `events[]`/`links[]` as native
  `List<Struct<…>>` (with nested `Map`). Full fidelity so the query
  phase never re-ingests.
- **No postings** for either signal. Trace-by-id rides parquet row-group
  `trace_id` min/max stats (block sorted by `(trace_id, start)`);
  profiles ride block `ts_min/ts_max`. Postings are an inverted index for
  *label-matcher* queries (metrics/logs); traces/profiles don't need that
  access pattern at the storage milestone.

### Late addition (this session): promoted trace columns
After a discussion about "select all root spans for service X" (a
discovery query with no trace id to reach in by), we **promoted three
OTel discovery axes** out of the `resource_labels` Map into dedicated
**nullable `Utf8`** columns on the traces block:
`service_name`, `service_namespace`, `deployment_environment`
(`deployment.environment` *and* the newer `deployment.environment.name`
both accepted). They are denormalised **copies** — the originals stay in
the Map (full fidelity). They do **not** enable row-group pruning (block
is trace_id-sorted, so service values scatter across every row group),
but they make service-scoped discovery a plain column predicate and are
the clean key a future service index/clustered variant would build on,
avoiding a block rewrite later. Verified on a real landed block:
`service_name='api' AND parent_span_id IS NULL` → root spans for a
service, as a column predicate.

### Files
**New:**
- `crates/block/src/traces.rs` — `TracesBlockBuilder`, `SIGNAL="traces"`,
  one row/span, sorted `(trace_id, start)`. Nested events/links assembled
  from parts (flatten children across sorted spans, `OffsetBuffer`,
  `MapArray`→`StructArray`→`ListArray`). Promoted columns added.
- `crates/block/src/profiles.rs` — `ProfilesBlockBuilder`,
  `SIGNAL="profiles"`, sorted by `ts`, opaque pprof in `Binary` column.
- `crates/block/tests/traces_roundtrip.rs`,
  `crates/block/tests/profiles_roundtrip.rs` — encode→upload(in-mem)→
  read-back→assert schema/sort/nested fidelity/nullability/no-postings
  (+ promoted-column values incl. a null case).

**Modified:**
- `crates/proto/src/streaming.rs` — `TracesAppender`/`ProfilesAppender`
  traits + `DecodedSpan`/`DecodedEvent`/`DecodedLink` + `decode_traces_batch_into` /
  `decode_profiles_batch_into`, with `*_matches_generated` round-trip
  tests. (Also fixed two pre-existing deny-level clippy `approx_constant`
  literals in an unrelated metrics test — surfaced to Bart at the time.)
- `crates/block/src/lib.rs` — `pub mod` + `pub use` for both builders.
- `crates/server/src/decode.rs` — `pub fn traces(...)`/`pub fn profiles(...)`.
- `crates/server/src/server.rs` — type aliases, `Server` pipeline fields,
  `Server::new` +2 params, `Signal::Traces`/`Signal::Profiles` dispatch
  arms (two-phase decode-out-of-lock, like logs), `Count*Appender`s;
  **deleted the old `decode_payload` counting fn**.
- `crates/server/src/lib.rs` — doc example + exports.
- `crates/server/src/stats.rs` — `traces_upload`/`profiles_upload`
  `UploadStats` + accessors + bottleneck/snapshot wiring.
- `crates/scry-ingestd/src/main.rs` — 5-tuple `Pipelines`, open
  traces/profiles `ShardedPipeline`s, pass to `Server::new`.
- `crates/noise-spewer/src/gen.rs` — traces resources now carry
  `service.name`/`service.namespace`/`deployment.environment` so the
  promoted columns are exercised by smoke.
- `scripts/smoke.sh` — `traces`/`profiles`/`all` SIGNAL modes;
  storage-only signals assert blocks-landed + reconciled-rows ==
  sink-accepted (no query leg); query leg gated to metrics/logs.

### Verification done (all green)
- `cargo test --workspace` — pass.
- `cargo clippy -p scry-block -p noise-spewer --all-targets` — clean
  except one pre-existing `metrics_roundtrip.rs` warning (not ours).
- `SIGNAL=traces scripts/smoke.sh` — 40000 == sink-accepted == catalog
  rows, loss-free. (`SIGNAL=profiles` and `SIGNAL=all` also passed
  earlier in the session.)
- Pulled the landed traces parquet from Garage and confirmed promoted
  columns carry real values, originals still in the Map.

### State
- Branch `main`, **nothing committed.** `git status` shows the 9 M + 4 ??
  files above. Bart has **not** asked to commit — do not commit unless he
  does. A clean conventional-commit split is possible (whole-file per
  Rule #13): e.g. one `feat` commit for the traces+profiles storage
  vertical across all the files. Confirm with Bart first.

---

## Part 2 — NEXT TASK: `scry-gateway` (foreign-protocol ingest)

### Why
The daemon can now *store* all four signals, but **real-telemetry
ingestion only covers kube logs**. There is **no HTTP/gRPC listener
anywhere** (confirmed: no axum/hyper/tonic/warp/actix in any Cargo.toml).
The only ingest transport is scry's own binschema TCP push protocol, and
the only real producer is `scry-agent` (Kubernetes CRI logs, **logs
only** — the other collectors in `docs/ARCHITECTURE.md` §Ingest, incl.
prom-scraper / pprof-puller / OTLP-receiver, are unbuilt). The
noise-spewer can send all four signals but it's synthetic.

By design scry does **not** speak OTLP/Pyroscope/Loki/Prom-remote-write
on its wire ("we own this end so we don't inherit protocol quirks",
ARCHITECTURE.md:609-611). Bart's apps already **push** Pyroscope, and he
wants OTLP trace push too — both are *push* protocols (HTTP), the
opposite of the doc's *pull*-based pprof-puller idea.

### Decision (Bart picked this)
Build a **separate `scry-gateway` binary/crate** that terminates
Pyroscope/OTLP **HTTP push** and **forwards as binschema** to the ingest
daemon. Rationale: keeps both the binschema server *and* the per-node
agent clean; one place owns all foreign-protocol translation. The gateway
is "just another binschema client" — same handshake the agent/spewer use.

Rejected alternatives: HTTP receiver inside `scry-ingestd` (pollutes the
binschema-only server); receiver collectors inside `scry-agent` (agent is
per-node, awkward target for already-pushing central clients).

### Open questions to resolve with Bart BEFORE finalising a plan
These gate the design — ask via AskUserQuestion early:
1. **Pyroscope protocol flavor his clients emit:** legacy `POST /ingest?
   name=<app>{labels}&from=&until=&format=&sampleRate=...` (pyroscope-io
   SDKs / grafana agent) vs. the newer Grafana Pyroscope push
   (`push.v1.PusherService/Push` over Connect/gRPC). And body **format**
   (`pprof`/`pprof_gz` is the common, clean case — maps straight to our
   opaque blob; `folded`/`jfr` would need tagging/conversion).
2. **OTLP traces transport:** OTLP/HTTP protobuf at `POST /v1/traces`
   (`application/x-protobuf`, the standard) and/or OTLP/HTTP JSON. (gRPC
   OTLP is a bigger lift — confirm if needed.)
3. **Build order:** Pyroscope→profiles first (clients already push), then
   OTLP→traces. Confirm.
4. Gateway endpoint config: listen addr, upstream daemon addr, per-tenant
   labels? Keep v0 single-tenant.

### Reusable internals (read these first)
- **Binschema client to copy:** `crates/agent/src/client.rs` — `Client`
  (`connect` handshake, background ack-reader, `send_batch` with inflight
  flow control, `shutdown`). **Caveat:** `Client::connect` **hardcodes
  `signals: SIGNAL_BIT_LOGS`** (client.rs:66). The gateway must announce
  `SIGNAL_BIT_TRACES | SIGNAL_BIT_PROFILES` — small refactor to make the
  signals bitmask a `connect` param (and optionally lift `Client` into a
  shared crate, or just re-implement; it's ~140 lines).
- **Batch construction:** `crates/proto/src/build.rs` — `build::hello`,
  `build::batch(BatchArgs{ session_id, batch_id, signal, ts_min/max,
  record_count, compression, uncompressed_size, payload })`,
  `build::goodbye`. Constants in `scry_proto::constants`
  (`Signal::{Traces,Profiles}`, `SIGNAL_BIT_*`, `COMPRESSION_ZSTD`,
  `PROTOCOL_VERSION_V0`).
- **How to build+compress a payload:** `crates/noise-spewer/src/gen.rs` —
  `make_batch` (encode the `TracesBatch`/`ProfilesBatch` via the generated
  `encode_into`, `zstd::encode_all(level 3)`, wrap in `build::batch`).
  This is the exact shape the gateway's translators emit.
- **Wire types + wire layout:** `crates/proto/src/generated.rs`
  (`TracesBatch{resources[],scopes[],spans[]}`, `Span`, `SpanEvent`,
  `SpanLink`, `ResourceEntry`, `ScopeEntry`, `ProfilesBatch`,
  `ProfileBlob{ts,duration,labels,format,data}`). Wire byte layouts +
  the `format` byte semantics (1 = pprof_gz) documented in
  `crates/proto/src/streaming.rs` (search `ProfileBlob`/`Span`).
- **Storage side (already done):** `crates/block/src/{traces,profiles}.rs`
  — confirms exactly what fields land, so the translator knows what to
  populate (e.g. resource labels incl. service.* feed the promoted
  columns; ProfileBlob.format/data is opaque).
- **Server dispatch (sanity):** `crates/server/src/server.rs`
  `Signal::Traces`/`Signal::Profiles` arms — what the daemon expects.

### Likely shape (NOT a committed plan — confirm Q1-3 first)
- New crate `crates/gateway` (`scry-gateway` bin). Add **axum** (+ tower)
  to the workspace for the HTTP server; **prost** + an OTLP proto source
  (e.g. `opentelemetry-proto`) for OTLP traces decode. Pyroscope `/ingest`
  needs only raw-body + query-param parsing (no new proto).
- Handlers:
  - `POST /ingest` (Pyroscope) → parse name/labels/from-until/format,
    take the (gz) pprof body verbatim → one `ProfileBlob{ts, duration,
    labels, format, data}` → `ProfilesBatch` → binschema batch upstream.
  - `POST /v1/traces` (OTLP/HTTP protobuf) → decode
    `ExportTraceServiceRequest` → map ResourceSpans/ScopeSpans/Spans into
    `TracesBatch` (build `resources[]`/`scopes[]` dicts + `resource_idx`/
    `scope_idx`; map OTel span kind/status enums to our `u8`s; events→
    SpanEvent, links→SpanLink). Mind id widths (trace_id 16, span_id 8).
- One persistent `Client` per upstream daemon (or a small pool); batch
  accumulation + flush by size/age; reuse inflight flow control. v0 can
  skip the agent's local spool (at-least-once) — note it as a follow-up.
- Tests: protocol-decode unit tests (pyroscope query parse; OTLP→
  TracesBatch mapping with a fixture request) + an e2e smoke leg
  (gateway → scry-ingestd → reconcile rows == pushed records). Extend
  `scripts/smoke.sh` or add `scripts/smoke-gateway.sh`.
- Verify with real clients: point Bart's pyroscope-emitting apps at the
  gateway; confirm a profiles block lands and reconciles.

---

## Working context / dev infra
- **Repo:** `/home/bart/Projects/scry` (this is the right CWD; prior work
  was driven awkwardly from `/home/bart/Projects/db`).
- **Object store:** Garage in docker (`scry-garage`, currently Up).
  Config/creds in `docker/garage/.env`
  (`SCRY_OBJSTORE_ENDPOINT=http://127.0.0.1:3900`, bucket `scry-dev`).
  `scripts/dev-garage-up.sh` to (re)start. `aws` + `sqlite3` + `python3`
  (pyarrow 24) CLIs available; no `duckdb`.
- **Verify loop:**
  `cargo test --workspace` ·
  `cargo clippy -p <crate> --all-targets` ·
  `SIGNAL=traces|profiles|all scripts/smoke.sh` (storage-only signals
  assert reconciled-rows == sink-accepted; metrics/logs/both also do a
  query round-trip).
- **Inspect a landed block:** `aws --endpoint-url $SCRY_OBJSTORE_ENDPOINT
  s3 ls/cp s3://scry-dev/...` then read parquet with pyarrow.
- **git:** allowlist per Rule #15 (commit / branch-checkout / push / read-
  only / add only). Never commit unless Bart asks. Co-author trailer:
  `Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>`.
