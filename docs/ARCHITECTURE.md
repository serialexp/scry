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

[listen]
ingest = "0.0.0.0:4000"
query  = "0.0.0.0:4001"

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
