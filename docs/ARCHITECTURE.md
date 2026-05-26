# scry — Architecture

This document describes the storage, ingest, query, compaction, and
retention design of `scry`. It is the load-bearing reference; the README
is marketing.

## Guiding principles

1. **One thing per problem.** One binary, one block format, one
   compactor, one retention loop, one wire protocol. If two parts of the
   system can share a mechanism, they must.
2. **Object storage is the source of truth.** Everything else (WAL,
   in-memory state, catalog cache) is recoverable from the bucket. If a
   process dies, you re-derive its state by listing the bucket.
3. **Writers don't coordinate.** Multi-writer must work without a
   distributed lock manager, a ring, or a consensus protocol.
   Coordination happens at *compaction* time, not at write time, and
   uses object-storage-native primitives (conditional PUT, ETags).
4. **No knob without a defended reason.** Every config option must
   justify its existence in a code review. The bias is to delete.
5. **All four signals share the storage layer.** Per-signal code lives
   only in (a) the agent's collector, (b) the parquet schema for that
   signal's payload, and (c) the query frontend that knows what
   questions to ask. Everything else is shared.

## System overview

```
┌─────────────────────────────────────────────────────────────────┐
│  agents (one per host)                                          │
│    ├─ prom scraper      ─┐                                      │
│    ├─ file/journald tail ├─► binschema wire protocol ──────┐    │
│    ├─ pprof puller      ─┘                                  │    │
│    └─ (optional) OTLP receiver                              │    │
└──────────────────────────────────────────────────────────────┼───┘
                                                               │
                                                               ▼
┌─────────────────────────────────────────────────────────────────┐
│  scry server (one binary, N replicas)                           │
│                                                                 │
│  ingest ──► WAL (local SSD) ──► block builder ──► object store  │
│                                                       │         │
│  catalog (S3 list + sqlite cache) ◄───────────────────┘         │
│                                                                 │
│  query (DataFusion) ◄── catalog ◄── object store                │
│                                                                 │
│  compactor (background) ─► reads small blocks ─► writes merged  │
│  retention (background) ─► deletes blocks older than retention  │
└─────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
                ┌──────────────────────────────────────┐
                │  S3-compatible object storage        │
                │  s3://bucket/<signal>/<date>/<wid>/  │
                │       <block_uuid>.parquet           │
                │       <block_uuid>.meta              │
                └──────────────────────────────────────┘
```

## The record model

Every signal reduces to:

```rust
struct Record<Payload> {
    ts: i64,                  // unix nanos
    labels: LabelSet,         // interned {k: v} pairs
    payload: Payload,         // signal-specific
}
```

Per-signal payloads:

| Signal    | Payload                                                          |
|-----------|------------------------------------------------------------------|
| metrics   | `{ value: f64 }` for gauges/counters; `{ buckets: Vec<(f64, u64)>, sum: f64, count: u64 }` for histograms |
| logs      | `{ line: String, level: Option<Level> }`                         |
| traces    | `{ trace_id: [u8;16], span_id: [u8;8], parent_id: Option<[u8;8]>, name: String, kind: SpanKind, start_ns: i64, end_ns: i64, attrs: AttrMap, events: Vec<Event>, status: Status }` |
| profiles  | `{ profile_type: ProfileType, samples: Vec<Sample>, locations: Vec<Location>, functions: Vec<Function> }` (pprof-shaped) |

All four become a parquet file with a small fixed schema header (`ts`,
exploded label columns) and a payload column group. Per-signal payload
schemas are versioned independently; the catalog records the schema
version of each block.

## Storage layer

### Block layout in the bucket

```
s3://<bucket>/<signal>/<yyyy>/<mm>/<dd>/<writer_id>/<block_uuid>.parquet
s3://<bucket>/<signal>/<yyyy>/<mm>/<dd>/<writer_id>/<block_uuid>.meta.json
```

- `signal` ∈ `{metrics, logs, traces, profiles}`. Each signal is a
  totally independent prefix; nothing crosses.
- Date is the *block's min timestamp*, day-aligned UTC.
- `writer_id` is a stable UUID per writing process, persisted under the
  WAL directory. Writers never share a prefix, so they never collide.
- `block_uuid` is a v7 UUID (time-ordered), making bucket listings
  return blocks in roughly write order.
- The `.meta.json` sidecar is small (KB) and holds:
  - min/max timestamp
  - row count
  - label fingerprint set (xxh3 hashes of every distinct label name and
    every distinct `(name, value)` pair seen in the block) — used to
    prune blocks before reading
  - per-column min/max for the payload columns we predicate-push on
  - parquet schema version
  - producer writer_id and software version

The sidecar is what the catalog reads to prune; we never open the
parquet just to find out whether it might contain matches.

### The catalog

The catalog is **derived state**: a SQLite database mirroring "what
blocks exist and what's in their `.meta.json`". It's rebuilt from a
bucket `ListObjects` walk at startup, and kept in sync incrementally
during runtime.

- Writers append rows when they upload a block.
- Readers query the catalog to plan a query (which blocks intersect the
  time range, which can be pruned by label fingerprint).
- Other readers learn about new blocks via either (a) periodic
  incremental `ListObjects` polling with a `start_after` marker, or (b)
  an optional pub/sub notification (S3 event notifications, NATS,
  whatever) — *optional* because polling is fine at our cadence.

The catalog **is not the source of truth.** If it drifts from the
bucket, we re-derive it. This is the property that lets multi-writer
work without coordination: writers don't have to agree on the catalog,
because the catalog is just a cache of `ListObjects`.

### The WAL

A local SSD WAL sits between ingest and the block builder. It serves
*two* purposes:

1. **Backpressure / RAM cap.** Incoming records hit the WAL
   immediately. RAM only holds the *currently building* block, which
   has bounded size. Spikes in ingest rate become disk writes, not
   OOMs.
2. **Crash safety.** A block isn't acknowledged-as-durable to upstream
   readers until it's been uploaded to object storage. If the process
   dies, the WAL replays into a new block on startup.

WAL design:

- Append-only segments of fixed max size (e.g. 256 MiB), named
  `wal-<u64-seq>.log`.
- Each record framed by `[len: u32][crc32: u32][binschema payload]`.
- `fsync` on segment rotation, not per record. We accept "last few ms of
  records on a crash" as the durability boundary; if you need
  per-record fsync, a different system is the right answer.
- A segment is **deleted** once every record in it has been included in
  a parquet block that has been successfully uploaded *and*
  acknowledged by the object store.
- On startup: scan WAL dir, replay any segment not marked-uploaded into
  a fresh block, then resume normal operation.

The WAL is *per writer*. No sharing.

### The block builder

In-memory builder per active block:

- Holds row buffers per column (arrow `RecordBatch` builders).
- Closes the block when *any* of these is true:
  - row count ≥ `max_rows_per_block` (default ~1M),
  - byte estimate ≥ `target_block_bytes` (default ~128 MiB before
    compression),
  - wall-clock age ≥ `max_block_age` (default 5 min),
  - explicit flush requested (e.g. graceful shutdown).
- On close: serialize to parquet (zstd, level 3, row group size tuned
  to ~1 MiB compressed), upload to object storage with
  `If-None-Match: *` (so a retry never overwrites), write the sidecar,
  insert a catalog row, then mark the consumed WAL segments as
  uploaded.

Block builder lifecycle is **per `(signal, day)` pair**, so a block
never straddles a day boundary. This keeps the partition pruning trivial
and makes retention a pure prefix-delete.

## Ingest

The agent and server speak a single binschema-defined wire protocol.
Sketch (not final):

```
Hello       { agent_id, hostname, agent_version }
Hello.Ack   { server_time_ns, session_id }
Batch       { session_id, seq, signal, records: Vec<Record> }
Batch.Ack   { seq, durable_seq }   // durable_seq = highest seq written to WAL+fsynced
Bye         { session_id }
```

- Long-lived TCP connection (TLS in prod), multiplexed by `seq`.
- Backpressure: server stops `Ack`ing when its WAL is behind its target
  buffer. Agent stops reading from collectors when too many unacked
  batches are in flight.
- Re-delivery: agent persists unacked batches to *its own* local
  spool, replays after reconnect. Server dedupes by `(session_id, seq)`
  for the lifetime of a session; cross-session dedup is the agent's
  responsibility (idempotent batch IDs).

This protocol is **dumb on purpose.** No service discovery. No mesh. No
"smart agent." If you want fan-out to multiple servers, run multiple
agent→server links.

## Query

DataFusion is the query engine. The flow:

1. Query frontend (per-signal) parses the user query into a logical
   plan over a virtual `(signal, ts, labels..., payload...)` table.
2. Planner consults the catalog: enumerate candidate blocks by time
   range, then prune by label-fingerprint bloom and per-column min/max
   from sidecars.
3. DataFusion executes the plan against the surviving parquet files
   via `object_store`. Predicate pushdown into parquet row groups
   handles intra-block pruning.
4. Frontend post-processes the result into signal-shaped output (a
   PromQL-style matrix, a list of log lines, a trace tree, a
   flamegraph).

For query *languages*, we will prefer existing Rust parser crates over
writing our own:

- `promql-parser` for metrics.
- `logql-parser` / similar for logs (evaluate what's actually
  maintained).
- TraceQL grammar can be hand-lifted (it's small).
- For profiles, the query surface is small enough that a fixed REST
  endpoint is probably enough.

Building our own query language is **explicitly deferred**. We keep the
internal DataFusion plan as the stable interface and add language
frontends on top.

## Caching

Object storage is roughly six orders of magnitude slower than RAM
(~30 ms cross-network vs ~50 ns pointer-chase). Aggressive caching is
not a "nice to have"; it's the difference between "works" and "fast."

The load-bearing property that makes caching cheap here:

> **Blocks are immutable.** A parquet file, once uploaded, is byte-for-
> byte identical for the rest of its existence. The only state change
> a block ever undergoes is deletion (by compaction or retention),
> which is performed by us, so we always know it's coming.

This means caches need no TTL, no stale-while-revalidate, no
distributed invalidation protocol. The only invalidation event is
"this block was deleted" and it's a local event in the process that
performed the deletion.

### Cache layers

From cheapest/hottest to most expensive:

| Layer                       | Size per entry | What it gives                                 | Backing |
|-----------------------------|----------------|-----------------------------------------------|---------|
| **1. Catalog**              | KB             | "which blocks exist, what's roughly in them"  | SQLite + RAM |
| **2. Parquet footer**       | KB–~MB         | schema, row-group offsets, per-column stats   | RAM (LRU) |
| **3. Page index**           | KB             | per-page min/max within a column chunk        | RAM (LRU, alongside footer) |
| **4. Decompressed pages**   | MB–tens of MB  | actual data ready to feed the executor        | RAM bounded + optional local-SSD spill |
| **5. OS page cache (WAL)**  | —              | hot WAL reads served from RAM                 | Linux, free |

A footer cache hit is ~10⁶× faster than fetching the footer from S3,
and *every* query that touches a block needs the footer. Layers 1–3
are mandatory from v0.1. Layer 4 is a v0.5/v0.6 optimisation (likely
via `liquid-cache` or a similar DataFusion extension).

### Sizing

Parquet footers run ~0.1–1% of file size. At 128 MiB target block size
that's ~128 KB–1.3 MiB of footer per block. A 1 GiB RAM cache holds
metadata for ~1,000–10,000 blocks — at Bart's projected ~5 TiB/yr
that's "all of them, easily."

### Implementation

We provide a `ParquetMetadataCache` to `parquet-rs` keyed on
`(bucket, path, etag)`. The ETag pin is belt-and-braces — since we
upload with `If-None-Match: *` and blocks are immutable, a stale entry
should be impossible by construction, but matching on ETag means a
hypothetical overwrite would miss naturally rather than serve wrong
metadata.

Eviction: LRU bounded by total bytes (not entry count), with explicit
eviction when retention or compaction deletes a block in this process.
Other processes' deletions are observed via catalog updates (see
[Synchronisation](#synchronisation)) and trigger eviction the same way.

## Compaction

Background task in the same process. Per-signal-per-day:

1. List blocks for `(signal, day)` in the catalog.
2. If the count of "small" blocks (< 32 MiB compressed, say) exceeds a
   threshold, plan a merge:
   - Pick the K smallest blocks (bounded total input size).
   - Read all of them in a streaming merge by `ts`.
   - Write one new block. Upload with `If-None-Match: *`.
   - Insert the new catalog row.
   - Delete the inputs *only after* the new row is durable in the
     catalog and the new parquet object is confirmed present.
3. Repeat until "small block count" is below the threshold.

Multi-writer correctness: compaction work is partitioned by
`(signal, day)`. A lightweight lease (a small object at
`s3://.../_compact_lease/<signal>/<yyyy-mm-dd>` with a TTL and an ETag
check on takeover) ensures only one writer compacts a given partition
at a time. Worst case: a stale lease causes wasted work; correctness is
preserved because compaction output is content-addressed and inputs are
only deleted after success.

Compaction never touches the WAL. The WAL is purely the
"recent-and-not-yet-uploaded" path.

## Synchronisation

`scry` is designed for 1–N identical instances sharing one object-store
bucket. Each instance plays four roles simultaneously: **writer**
(owns a WAL, uploads blocks under its own `writer_id` prefix),
**reader** (serves queries from all blocks regardless of authorship),
**compactor** (background work, contests for partition-scoped leases),
and **retention-runner** (background work, no coordination needed).

The two foundational properties that make multi-instance coordination
tractable:

1. **Writers never share a key prefix.** Block paths include
   `writer_id`, so concurrent writes can never collide. There is no
   such thing as a "write conflict" in this system.
2. **Blocks are immutable.** A block, once uploaded, never changes.
   The only state transition is deletion, and deletion is performed by
   one of the instances themselves.

Coordination is therefore only needed for three things: discovering
peers' new blocks, agreeing who compacts a given partition, and
avoiding deletion-during-read races.

### Block discovery: Valkey pub/sub with polling backstop

When an instance uploads a new block, peers need to know about it so
their catalogs are fresh and their queries don't miss recent data.

The design uses **Valkey pub/sub** as the low-latency notification path
and **periodic `ListObjects` polling** as the source-of-truth backstop:

- On block upload, the writer `PUBLISH`es to
  `scry/blocks/<signal>` a message containing the block path and the
  sidecar contents.
- Every instance `SUBSCRIBE`s to those channels and updates its
  catalog on receipt. Propagation latency is sub-millisecond.
- Independently, every instance polls `ListObjects` as a backstop
  (see [Cursor-driven polling](#cursor-driven-polling) below).
- Every 30 minutes, a full bucket walk reconciles drift end-to-end.

#### Cursor-driven polling

Polling does *not* re-list the bucket from scratch; that would scale
with bucket lifetime and waste both list-API calls and CPU. Instead,
each instance keeps a small cursor table in its SQLite catalog:

```sql
CREATE TABLE poll_cursors (
  signal       TEXT NOT NULL,
  writer_id    TEXT NOT NULL,
  date         TEXT NOT NULL,  -- yyyy-mm-dd
  highest_uuid TEXT NOT NULL,  -- last UUID v7 seen in this prefix
  PRIMARY KEY (signal, writer_id, date)
);
```

A poll for `(signal, writer_id, date)` is:

```
LIST prefix=<signal>/<date>/<writer_id>/  start-after=<highest_uuid>
```

Because block UUIDs are v7 (time-prefixed and lexically sortable by
creation time), `start-after` returns only blocks newer than what
we've already ingested. Each cursor is updated whenever we observe a
block via *either* pub/sub or polling — both paths converge on the
same state.

**Crucially, poll cost does not grow with bucket size.** A bucket
five years old polls at the same speed as one started yesterday,
because we only scan today's and yesterday's per-writer prefixes
(yesterday is included to catch late uploads near day boundaries).

#### Polling cadence

Polling cadence adapts to Valkey health:

- **Healthy (Valkey reachable, recent message received):** poll
  every 60 seconds. Pure backstop; pub/sub is doing the real work.
- **Degraded (Valkey unreachable or silent past threshold):** poll
  every 5 seconds. Pub/sub is no longer trusted, polling is the
  primary mechanism.
- **On Valkey reconnect:** immediately trigger one full cursor
  sweep across all `(signal, writer_id, date)` rows before
  returning to the healthy cadence. Reconnect is the moment of
  maximum unknown; that's when the sweep earns its cost.

These cadences are baked-in behavior, not config knobs. If they
prove wrong in practice we'll revisit.

This is a deliberate three-tier defense: pub/sub for normal-case
latency, short polling for "Valkey was briefly down," full walks for
"something we don't understand happened." All three converge on the
bucket as the source of truth — Valkey is a cache-invalidation hint,
not a system of record.

A single Valkey instance handles enormous fan-out before becoming a
bottleneck; at our scale (1–N small N) it's a non-issue. Failure
modes:

- **Valkey down:** instances fall back to polling. Query staleness
  rises from ~0 ms to ≤5 s. No correctness impact.
- **Peer disconnected from Valkey:** same as above for that peer.
- **Network partition:** each partitioned side still serves queries
  from blocks it knows about; new writes from the *other* side become
  visible after partition heals (via polling reconciliation).

### Compaction: per-partition object-storage leases

Compaction work is scoped per `(signal, day)` partition. Multiple
instances run the compactor loop; for each candidate partition, the
instance attempts to acquire a short-lived lease before starting:

```
PUT s3://<bucket>/_compact_lease/<signal>/<yyyy-mm-dd>
    If-None-Match: *
    Body: { writer_id, expires_at: now() + 5min }
```

- **Acquire:** `PUT If-None-Match: *`. 412 means someone else has it.
- **Renew:** `PUT If-Match: <etag>` periodically while working.
- **Takeover after expiry:** `GET` to check `expires_at`, then
  `PUT If-Match: <etag>` to atomically replace.
- **Release:** `DELETE If-Match: <etag>` on clean exit.

S3 (since 2020), R2, MinIO, and Garage all support conditional writes.
Object stores that don't are explicitly unsupported.

**Correctness if the lease is buggy or contested:** two instances do
redundant work and produce two valid merged blocks (different UUIDs,
same input data). The next compaction round merges those two into one.
**Correctness is preserved by immutability + content addressing;** the
lease is purely an efficiency optimisation. We will not write
elaborate recovery logic for double-compaction because there's
nothing to recover.

### Compaction deletion: 10-minute grace period

The compactor's output sequence:

1. Upload the merged block (with `If-None-Match: *`).
2. Insert the new catalog row in this instance and `PUBLISH` it.
3. Mark the input blocks `superseded_by = <new_uuid>` in the catalog
   (locally and via pub/sub). **New queries skip superseded blocks.**
4. Wait 10 minutes.
5. Delete the input blocks from object storage.
6. Drop their catalog rows.

The 10-minute grace period exists so that in-flight queries which
already planned against the input blocks can complete their reads
before the bytes disappear. This is fixed and not configurable. (If
operational reality ever produces queries that take >10 min, we'll
revisit; the architectural decision is "don't add a knob until forced
to.")

During the grace period, both the old inputs and the new merged block
exist in the bucket. The `superseded_by` flag prevents double-reads:
queries planned *after* the supersede event see only the merged block,
queries planned *before* keep reading from the inputs they were
already plumbed to.

### Retention: no coordination

Retention's only operation is "delete blocks older than cutoff" — an
idempotent prefix-delete. Multiple instances racing to retire the same
day produce no incorrect outcome; whichever DELETE lands first wins
and the rest get 204 No Content. Each instance manages its own
catalog rows for the deleted prefixes (drop them on observing the
deletion via pub/sub or polling). No leases, no leader, no
coordination.

### writer_id

Each instance has a stable `writer_id` that prefixes all its block
paths. Default behavior: on first startup, generate a v4 UUID and
persist it to `<wal_dir>/writer_id`. Operators who want
human-readable prefixes (e.g. `ingest-eu-1`, `ingest-eu-2`) can set
`writer_id` in the config; they're responsible for uniqueness.

No coordination needed in either mode: UUIDs don't collide, and
explicitly named writers are the operator's problem.

### Catalog reconciliation and crash recovery

Each instance has its own SQLite catalog mirroring "what blocks
exist." Drift sources:

- **Missed pub/sub messages** while the instance was offline or
  partitioned from Valkey.
- **Crashed mid-upload:** the parquet may exist without its sidecar,
  or vice versa. We treat any block missing its sidecar as
  not-uploaded; the WAL still has the data and the next start re-
  uploads under a new UUID.
- **Out-of-band bucket operations** by an operator.

Defense:

- **Short polling (5 s)** catches near-real-time misses.
- **Full bucket walk (30 min)** catches everything else: add catalog
  rows for blocks present in the bucket but not the catalog, drop
  rows for blocks the catalog claims exist but `HEAD` says don't.
- **On startup:** full walk before serving queries or accepting
  writes.

### Cache invalidation across instances

Combined with the [Caching](#caching) layer: when an instance deletes
a block (its own retention, its own compaction output), it evicts the
block's catalog row, footer, page-index, and any cached pages
locally. Peer instances learn of the deletion via pub/sub
(`scry/blocks/deleted/<signal>` channel) and do the same.

If a peer misses the deletion notice, its next attempt to read the
block returns 404 from object storage and that triggers local
eviction reactively. The combination of (a) proactive notification and
(b) reactive cleanup on 404 means no instance ever serves stale
metadata for long, and no instance ever crashes because metadata
outlived its data.

## Retention

Background task. Per signal, on a schedule:

1. Compute the cutoff date: `today - retention[signal]`.
2. Delete every prefix `<bucket>/<signal>/<yyyy>/<mm>/<dd>/` where
   `<yyyy>/<mm>/<dd>` < cutoff.
3. Drop the corresponding catalog rows.

Because blocks never straddle a day boundary, this is a pure
prefix-delete. No partial-block resurrection logic. No
"open the block and find old records" scan.

## Configuration

The entire config file:

```toml
[storage]
backend  = "s3"           # s3 | fs
bucket   = "scry-prod"
endpoint = "https://s3.eu-west-1.amazonaws.com"
region   = "eu-west-1"
# credentials via standard AWS env vars

[wal]
dir         = "/var/lib/scry/wal"
segment_mib = 256
# writer_id auto-generated and persisted under wal.dir on first start;
# set explicitly here to override (must be unique across instances).
# writer_id = "ingest-eu-1"

[listen]
ingest = "0.0.0.0:4000"
query  = "0.0.0.0:4001"

[valkey]
url = "redis://valkey.internal:6379"
# Used for low-latency block-discovery pub/sub between instances.
# Optional: omit for single-instance deployments. When set but
# unreachable, scry falls back to ListObjects polling.

[retention]
metrics  = "90d"
logs     = "30d"
traces   = "7d"
profiles = "14d"
```

That's the whole thing. Anything we are tempted to add gets argued for
on the basis of "what specific outcome can the user not get without
it."

## Open questions

These are deliberately left unresolved at v0; they need answers before
the milestones that depend on them:

- **Profiles payload format.** Native pprof is the obvious answer, but
  pprof-in-parquet has nontrivial schema questions (deeply nested,
  shared symbol tables). We may want to denormalise on ingest and store
  one row per sample-with-resolved-stack.
- **High-cardinality metrics.** At what point do we have to do
  something smarter than per-block label-fingerprint blooms? Real-world
  measurement will decide; we won't speculate now.
- **TLS / auth between agent and server.** Probably mTLS with a CA file
  shipped to agents, but the operational shape (cert rotation, joining)
  is worth a dedicated mini-design.
- **Read replicas.** Single-writer / multi-reader is implicit in the
  design (any process pointed at the bucket can serve queries), but the
  catalog-cache-coherence story for *just* readers needs sketching.
- **PromQL on parquet, performantly.** Mimir's blocks-storage path
  caches a lot in store-gateways. We get away with less because our
  scale is smaller, but this is the milestone with the most unknowns.
