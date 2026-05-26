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

The diagram shows the data plane. The **control plane** is a single
Valkey instance, used for:

- Agent → server discovery (live server registry + consistent
  hashing on the agent side; see [Discovery](#discovery)).
- Block-event pub/sub between instances for fast catalog
  convergence (see [Block discovery](#block-discovery-valkey-pubsub-with-polling-backstop)).
- Bucket-event pub/sub for pool changes (auto-provisioning,
  sealing).

Valkey is a **cache-invalidation hint**, not a system of record;
object storage is always the source of truth. Valkey unavailability
gracefully degrades to polling-based discovery; no correctness is
lost.

### Deployment topologies

A scry binary can run in one of three modes, selected by config:

| Mode             | Ingest | Query | Compaction | Retention | Notes |
|------------------|--------|-------|------------|-----------|-------|
| **full** (default) | ✓ | ✓ | ✓ | ✓ | The "one binary does it all" mode. Correct choice up to ~30 instances. |
| **ingest-only**  | ✓ | ✗ | ✓ | ✓ | Specialised writer nodes — receive from agents, manage WAL, run background tasks. No query endpoint exposed. |
| **query-only**   | ✗ | ✓ | ✗ | ✗ | Specialised reader nodes — read from object storage, serve queries, no WAL, no agent traffic. Stateless beyond its catalog cache. |

Single-mode deployments are configured via `[role]`:

```toml
[role]
ingest     = true
query      = true
background = true   # compaction + retention
```

All three default to `true`; setting one to `false` excludes that
subsystem. The discovery layer only registers servers that have
`ingest = true`; query-only nodes register on a separate channel
(`scry/queriers/<region>`) for query load-balancing if a query
router is in front.

This split is **optional** — at small scale, `full` everywhere is
fine. At large scale, dedicating ingest nodes (high CPU + WAL disk
I/O) separately from query nodes (high RAM for caches + network
bandwidth for object-store reads) lets you size them to their
actual workload.

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

### Block layout

A block is addressed by `(bucket, path)`, where `bucket` is a logical
name in scry's config (see [Bucket pool and sealing](#bucket-pool-and-sealing))
that maps to a concrete `(backend, endpoint, region, bucket_name)`.
Within any bucket, the path layout is:

```
<signal>/<yyyy>/<mm>/<dd>/<writer_id>/<block_uuid>.parquet
<signal>/<yyyy>/<mm>/<dd>/<writer_id>/<block_uuid>.meta.json
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

### Bucket pool and sealing

A scry deployment is configured with an **ordered list of buckets**.
The first single-bucket deployment uses a list of length one;
multi-bucket deployments append entries as old buckets fill up.

Two real-world constraints motivate this:

1. **Hard provider limits.** Hetzner Object Storage caps a bucket at
   100 TiB. S3 and R2 have no documented limit, but other providers
   vary. The design must survive a hard ceiling somewhere.
2. **Full-walk performance.** The 30-min reconciliation scan walks the
   bucket. At 100 TiB / 128 MiB blocks ≈ 800k objects, that's ~800
   list pages. Acceptable but degrading. Multiple smaller buckets
   parallelise the walk for free.

#### Bucket states

Each bucket in the catalog is either:

- **Open** — accepts new writes; first open bucket in config order is
  the *active* bucket.
- **Sealed** — no new writes, but blocks are still read and
  compacted. Sealing is *advisory*, not enforced; in-flight uploads
  to a freshly-sealed bucket complete normally.
- **Drained** — sealed and contains zero blocks (all retention'd
  out). Operator may remove from config and delete the underlying
  bucket out-of-band to reclaim provider quota.

The catalog's `buckets` and `blocks` tables track all of this; see
[Schema](#schema) for full definitions.

#### Write path

Writers always upload to the **earliest open bucket** in the config
list. This is deterministic without coordination — every writer
picks the same one. On successful upload, the writer increments
`total_bytes` for that bucket in its local catalog and publishes the
block-created event (which carries the bucket name and the block's
byte size, so peers update *their* `total_bytes` too).

`total_bytes` will diverge briefly across instances because pub/sub
takes time to propagate, but converges on the same value once
events drain.

#### Automatic sealing

When `total_bytes >= max_bytes` on the active bucket, the writer that
notices first triggers a seal:

1. Acquire a single global seal lease at
   `<next_bucket>/_seal_lease` (conditional PUT, short TTL).
2. Write a `_sealed` marker object into the *outgoing* bucket. This
   is advisory — the bucket still accepts writes from peers who
   haven't seen the seal yet, but new peers will route around it.
3. Set `sealed_at = now()` in the local catalog and publish a
   `bucket-sealed` event on Valkey.
4. Release the seal lease.

Peers receive the event and switch their "earliest open" calculation
to the next bucket. Sealing is idempotent — multiple writers racing
to seal converge on the same outcome.

**Slack between `max_bytes` and the provider's hard limit matters.**
Because pub/sub propagation takes a moment and multiple writers may
have blocks in flight, the bucket can overshoot `max_bytes` by tens
to hundreds of MiB. Configure `max_bytes` well below the hard limit
(e.g. 90 TiB on a 100 TiB Hetzner bucket) to absorb this.

#### Query path

The query planner consults the catalog for matching blocks. Each
block's `bucket` column tells it where to fetch from. The planner
groups blocks by bucket and issues parallel reads against each
bucket's `object_store` instance. Multi-bucket adds zero hot-path
cost — we were already issuing parallel ranged GETs per block.

#### Multi-bucket and compaction

Compaction is scoped per `(bucket, signal, day)`, so the lease key
becomes `<bucket>/_compact_lease/<signal>/<yyyy-mm-dd>`. Within a
partition, the merged output goes to the *current* active bucket
even if the inputs came from a sealed one (because the active bucket
is where new writes go by definition). Inputs are deleted from their
original buckets after the grace period. No cross-bucket consistency
problem because object-store APIs are independent per bucket.

#### Multi-bucket and retention

Retention deletes blocks from whatever bucket they live in. When a
sealed bucket reaches zero remaining blocks, scry logs a notice and
marks it `drained` in the catalog. The operator removes it from
config and deletes the underlying bucket out-of-band.

#### Auto-provisioning (optional)

A scry deployment can opt into having scry create new buckets itself
when it needs them, instead of requiring operator config edits.
Configured via a `[storage.template]` block:

```toml
[storage.template]
enabled      = true
name_pattern = "scry-{installation}-{date:%Y%m}"  # e.g. scry-prod-202605
installation = "prod"
backend      = "s3"
endpoint     = "https://fsn1.your-objectstorage.com"
region       = "eu-central"
max_bytes    = "90 TiB"
# credentials need s3:CreateBucket and s3:ListAllMyBuckets in addition
# to data-plane permissions
```

When the template is enabled:

- **Bootstrap.** First startup with no buckets in the catalog: scry
  resolves the pattern, calls `CreateBucket`, and starts writing. No
  `[[storage.buckets]]` seed needed.
- **Existing deployments.** `[[storage.buckets]]` still works as a
  seed (buckets that existed before scry was managing them). Anything
  scry creates at runtime is recorded in the catalog, not in the
  config file — scry never modifies user files.

##### Naming pattern

`name_pattern` accepts placeholders:

- `{installation}` — verbatim from `installation = "..."`.
- `{date:<strftime>}` — strftime-formatted current date at creation
  time. `{date:%Y%m}` for monthly, `{date:%Y}` for yearly, etc.

If the resolved name collides with an existing bucket (either an
older scry-created one whose date overlaps, or someone else's), scry
appends `-2`, `-3`, ... until it finds a free name. Concretely: if
the active bucket fills twice in May, the second creation resolves to
`scry-prod-202605` (taken), then `scry-prod-202605-2`.

Note that the date in the name reflects *when scry decided to
provision the bucket*, not necessarily the date range of data inside
it (pre-provisioning means a bucket created in May may not start
receiving data until June). The catalog records the precise creation
timestamp; the name is a human-friendly hint.

##### Pre-provisioning at a watermark

The next bucket is created **proactively** when the active bucket
reaches **70% of `max_bytes`**, not at the moment of sealing. The
pre-provisioned bucket sits open-but-unused (writers still prefer the
earlier-in-pool active bucket) until the active one seals, at which
point the switch is a local flag flip with no API calls in the hot
path.

Rationale: bucket creation can fail (provider quota, IAM eventual
consistency, transient network issues). Decoupling creation from
sealing means failures are retried calmly under no time pressure;
the sealing path itself is purely local state changes plus a Valkey
publish.

Watermark and pre-provision are baked-in behavior, not config knobs.

##### Coordination

`CreateBucket` is idempotent at the protocol level: a second writer
attempting the same name gets "already owned by you" and treats it
as success. No lease is needed for creation — racing writers
converge on the same bucket, and at worst one extra empty bucket
ends up provisioned (cheap, harmless).

The seal step still uses the seal lease (see [Automatic
sealing](#automatic-sealing)). The new bucket simply already exists
when seal fires.

##### Bucket-pool discovery

A new writer (cold start, or recovering from a long partition) needs
to discover all buckets in the pool, including ones created by peers
while it was absent. The mechanism:

1. **Config seed** — any `[[storage.buckets]]` entries.
2. **Valkey** — query `scry/buckets/list` for the live pool snapshot
   from peers.
3. **Provider `ListBuckets` filtered by `name_pattern`** — if the
   template is enabled, scry can list the provider account's buckets
   and pick up anything matching the configured pattern. This is the
   recovery mechanism for "Valkey was unreachable when peers created
   new buckets." Requires `s3:ListAllMyBuckets`.

All three sources are merged into the catalog; runtime updates flow
in via `bucket-created` events on Valkey.

##### Failure modes

- **`CreateBucket` fails (quota, permissions, network):** scry does
  *not* seal the active bucket. Writes continue past `max_bytes` —
  this is why `max_bytes` must be well below the provider's hard
  limit. Logged loudly, retried on next pre-provision cycle.
  Operator alerted via metrics/logs.
- **Template misconfigured at startup** (bad credentials, pattern
  doesn't resolve, region unreachable): startup fails fast with a
  clear error rather than running degraded.
- **Drained buckets** are *never* auto-deleted. Even empty buckets
  have audit/compliance value. scry surfaces them in status output;
  destruction is operator-driven.

### The catalog

The catalog is **derived state**: a SQLite database mirroring "what
blocks exist, what's roughly in them, and which buckets they live in."
Each instance maintains its own catalog; instances converge via Valkey
events (see [Synchronisation](#synchronisation)).

- Writers append rows when they upload a block.
- Readers query the catalog to plan a query (which blocks intersect
  the time range, which can be pruned by label fingerprint).
- Peers learn about new blocks via Valkey pub/sub, with
  `ListObjects`-based polling and periodic full walks as backstops.

The catalog **is not the source of truth.** Object storage is. If a
catalog drifts from the bucket, we re-derive by walking the bucket
and reading sidecars. This is the property that lets multi-writer
work without coordination: writers don't have to agree on the
catalog, because the catalog is just a cache of what's in the bucket.

#### Schema

The complete catalog schema, consolidated. All tables are per
instance; cross-instance convergence happens via Valkey events on top.

```sql
-- All buckets known to this instance.
CREATE TABLE buckets (
  name        TEXT PRIMARY KEY,         -- logical name from config or template
  endpoint    TEXT NOT NULL,
  region      TEXT,
  max_bytes   INTEGER,                  -- soft cap; triggers seal when crossed
  state       TEXT NOT NULL,            -- open | sealed | drained
  sealed_at   INTEGER,                  -- unix ts, NULL while open
  total_bytes INTEGER NOT NULL DEFAULT 0,
  created_at  INTEGER NOT NULL
);

-- All blocks (mirrors object-store sidecars).
CREATE TABLE blocks (
  uuid           TEXT PRIMARY KEY,      -- UUID v7
  bucket         TEXT NOT NULL REFERENCES buckets(name),
  signal         TEXT NOT NULL,         -- metrics | logs | traces | profiles
  date           TEXT NOT NULL,         -- yyyy-mm-dd of ts_min
  writer_id      TEXT NOT NULL,         -- producer
  level          INTEGER NOT NULL DEFAULT 0,  -- compaction level; 0 = freshly written
  ts_min         INTEGER NOT NULL,      -- unix nanos
  ts_max         INTEGER NOT NULL,
  row_count      INTEGER NOT NULL,
  byte_size      INTEGER NOT NULL,      -- parquet on-disk size
  schema_version INTEGER NOT NULL,
  fingerprint    BLOB,                  -- xxh3 label-fingerprint bloom
  superseded_by  TEXT REFERENCES blocks(uuid),   -- set during compaction grace
  deleted_at     INTEGER                -- soft-delete during grace period
);

CREATE INDEX idx_blocks_query   ON blocks(signal, date, ts_min, ts_max)
  WHERE deleted_at IS NULL;
CREATE INDEX idx_blocks_compact ON blocks(bucket, signal, date, level)
  WHERE deleted_at IS NULL;

-- Per-(signal, writer, date) cursor for incremental ListObjects polling.
CREATE TABLE poll_cursors (
  signal       TEXT NOT NULL,
  writer_id    TEXT NOT NULL,
  date         TEXT NOT NULL,
  highest_uuid TEXT NOT NULL,
  PRIMARY KEY (signal, writer_id, date)
);
```

The `level` column is populated from day one even though tiered
compaction policy (see [Compaction](#compaction)) is a later
milestone — adding the column up front means no schema migration
when the policy lands. Freshly-written blocks are always L0; merged
outputs increment.

`superseded_by` and `deleted_at` together implement compaction's
grace-period semantics: a block being phased out is marked
`superseded_by = <merged_uuid>`, removed from query planning via the
partial index `WHERE deleted_at IS NULL`, and physically deleted from
object storage 10 minutes later when `deleted_at` is set.

#### Bootstrap

A new instance bootstraps its catalog from, in priority order:

1. **Catalog snapshot in object storage** (if present). A designated
   bucket holds a periodically-updated parquet of catalog rows,
   keyed by `(bucket, uuid)`. Snapshots are written by the instance
   currently holding the snapshot lease, once per hour. Snapshot
   bootstrap is O(GB read) regardless of bucket population.
2. **Tail Valkey** from a sequence number recorded in the snapshot.
3. **Full bucket walk** as the ultimate fallback when no snapshot
   exists (first deployment, or all snapshots lost).

Snapshots are an optimisation that becomes load-bearing past a few
hundred thousand blocks; small deployments can skip the snapshot
mechanism entirely and bootstrap by full walk.

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

Ingest covers everything from a record being produced on a host to
being durable in the WAL of a scry server. Three pieces: the **agent**
(what runs on the host), the **discovery layer** (how agents find
servers), and the **wire protocol** (how data moves between them).

### The agent

A single Rust binary running per host. It is the only thing producing
data for scry; we explicitly own this end of the pipeline so we don't
inherit OTLP/Prom-remote-write/Loki-push protocol quirks.

#### Collectors

The agent runs a configurable set of collectors:

- **Prometheus scraper.** Scrapes HTTP `/metrics` endpoints on a
  schedule. Targets discovered via static config, file SD, or a
  pluggable discovery (k8s, EC2, etc. — same shape Prometheus uses).
- **File tail.** Watches log files with rotation handling. Parses
  according to a configurable format (JSON, plain text with a
  timestamp regex, logfmt).
- **journald reader.** Streams systemd journal entries.
- **pprof puller.** Periodically fetches profiling endpoints
  (`/debug/pprof/...`) and ships them as profile records.
- **OTLP receiver** (optional). For applications that already emit
  OTLP and where re-instrumenting isn't worth it. Speaks OTLP/HTTP
  on a configurable port; translates to the internal record format
  before shipping.

Collectors are independent goroutines/tasks; each runs at its own
cadence and pushes into a shared outbound buffer.

#### Local spool

The agent has its own local disk spool (a smaller version of the
server's WAL):

- Records arriving from collectors are appended to a spool segment
  before being shipped.
- Spool is fsynced on segment rotation, not per record.
- A spool segment is deleted only after the server has acknowledged
  every record in it as WAL-durable.
- On agent restart, unshipped spool segments are replayed.

This means agent → server is *at-least-once* delivery. Cross-restart
dedup is the agent's job: each batch carries a stable
`(agent_id, batch_id)` and the server filters duplicates against a
recent-batch-id cache (a few minutes' worth, the time it takes for a
batch to clear the WAL on the receiver).

#### Backpressure

If the agent's outbound buffer fills (server unreachable or
applying backpressure), collectors stop reading from their sources.
Prom scrapes get skipped (recorded as a hole in the time series),
log tail pauses (kernel buffers, then file accumulates), journald
pauses, pprof pulls skip. **The agent never drops records silently
and never grows memory unboundedly.** Either the source's own
buffer absorbs the pause, or the system surfaces backpressure to
the source where it can be reasoned about.

### Discovery

Agents must locate scry servers. DNS round-robin works up to maybe
20–30 servers; beyond that we need a real discovery layer. The design
uses **Valkey as a service registry**:

#### Server registration

Each scry server, on startup and every 10 seconds thereafter, runs:

```
ZADD scry/servers/<region> <now_ms> <addr:port>
EXPIRE scry/servers/<region> 30
```

A sorted set keyed by last-heartbeat timestamp. A background reaper
on any instance prunes entries with `<now_ms - 30000>` (servers that
missed three heartbeats).

#### Agent-side selection

Agents pull the live server set from Valkey, cached locally and
refreshed every 30 s. They use **consistent hashing with virtual
nodes** to pick a server: `hash(agent_id) → ring position → owning
vnode → server`.

Properties:

- **Stable affinity.** A given agent always picks the same server
  while the server set is unchanged. That agent's WAL data
  concentrates on one server rather than fragmenting across many.
- **Smooth rebalancing.** Adding or removing one server reassigns
  only `1/N` of agents.
- **Top-K fallback.** Each agent computes its top-3 servers from
  the ring. If the primary is unreachable, fall back to #2 then #3
  without waiting for Valkey's TTL.

#### Capacity-aware weighting

Agents and servers don't have uniform capacity in practice. One
high-volume service can produce 100× the data of another; one
server may have more bandwidth than the next. We support this with
**weighted virtual nodes**:

- Each server publishes a capacity weight along with its
  registration (`ZADD scry/servers/<region> <ts> <addr:port:weight>`).
  Weight is operator-configured or auto-derived from
  CPU/network/WAL-throughput headroom.
- The hash ring gives a server `weight × base_vnodes` positions.
  Higher-capacity servers get more vnodes and receive a
  proportionally larger share of agent assignments.
- Reassignment is rare — only on join/leave events, not on transient
  load fluctuation. We deliberately avoid "load-based steering" of
  individual agents (it's chatty and prone to thrashing).

#### Alternative backends

For deployments with existing service-mesh infrastructure
(Kubernetes Service + Envoy/EDS, Consul, Nomad), a pluggable
`[discovery]` backend in agent config can use those instead. Valkey
is the default because we already require it.

### Wire protocol

Agent ↔ server speak a single binschema-defined binary protocol.
Sketch (not final):

```
Hello       { agent_id, hostname, agent_version, capabilities }
Hello.Ack   { server_time_ns, session_id, server_caps }
Batch       { session_id, batch_id, signal, records: Vec<Record> }
Batch.Ack   { batch_id, durable_seq }   // durable_seq = highest WAL-fsynced batch
Bye         { session_id, reason }
```

- Long-lived TCP connection with TLS (mTLS in prod), multiplexed.
- Backpressure: server stops `Ack`ing when its WAL is behind its
  target buffer. Agent stops reading from collectors when too many
  unacked batches are in flight.
- Re-delivery: agent persists unacked batches to its local spool,
  replays on reconnect.
- Cross-session dedup: stable `(agent_id, batch_id)` identifies a
  batch across sessions; server keeps a recent-batch-id cache.

The protocol is **dumb on purpose.** No routing decisions in the
protocol itself; routing is in the discovery layer (above).

### Server-side flow

When a server receives a `Batch`:

1. **Validate** schema, deduplicate against the recent-batch-id
   cache.
2. **Append** records to the appropriate WAL segment (per signal).
3. **Update** the block builder's in-memory state for the affected
   `(signal, day)` blocks.
4. **Ack** when the WAL segment containing this batch has been
   fsynced.

The block builder's lifecycle is described under
[Storage layer → The block builder](#the-block-builder).

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

Compaction runs as a background task in every server process,
competing for partition-scoped leases. Its job is to reduce the
number of small blocks (and thus the number of objects to open and
metadata to load on every query) while bounding the write
amplification cost.

### Tiered levels

Blocks live at one of several **levels**, recorded in
`blocks.level`. Each level has a target size; compaction merges
within a level to produce one block at the next level up.

| Level | Target size | Source                                          |
|-------|-------------|-------------------------------------------------|
| L0    | ~128 MiB    | Freshly written from the block builder          |
| L1    | ~1 GiB      | Merge of ~8 L0 blocks                           |
| L2    | ~10 GiB     | Merge of ~8 L1 blocks                           |
| L3    | ~100 GiB    | Merge of ~8 L2 blocks                           |

L3 is the practical ceiling — past that, individual parquet files
get large enough that random-access reads suffer. For most
deployments L2 is the highest level reached.

The level structure caps total write amplification at roughly
`log_k(total_size_per_day)` where `k` is the level fan-out (8 in the
table above). At 50 TB/month and a 90-day retention, each byte
written gets re-written ~3 times across compaction passes, total. A
naïve "merge whenever there are small blocks" policy would re-write
each byte 5–10 times, which at scale dominates real ingest cost.

### Compaction policy

For each `(bucket, signal, day, level)` partition, the policy:

1. Count blocks at this level for this partition.
2. If count `>= K` (e.g. 8), select the K smallest by byte size and
   plan a merge into level `level + 1`.
3. If count `< K`, do nothing — wait for more blocks to arrive.

This is **size-tiered** rather than **levelled** compaction
(LevelDB-style). Size-tiered is simpler, has better write
amplification at the cost of slightly worse read amplification, and
fits append-mostly observability workloads better than the
write-mostly KV workloads LevelDB targets.

### Per-merge sequence

1. Acquire the compaction lease for the partition (see
   [Multi-writer coordination](#multi-writer-coordination) below).
2. Read the K input blocks via streaming merge sorted by `ts`.
3. Write one new block to the *current active bucket* (regardless of
   which buckets the inputs lived in), at level `input_level + 1`.
   Upload with `If-None-Match: *`.
4. Insert the new catalog row. Publish `block-created` on Valkey.
5. Mark inputs `superseded_by = <new_uuid>` in the catalog.
   Publish `blocks-superseded`. **At this moment new queries skip
   the inputs.**
6. Wait the [10-minute grace period](#compaction-deletion-10-minute-grace-period).
7. Set `deleted_at = now()` on inputs, then `DELETE` the input
   objects from their respective buckets.
8. Drop the input catalog rows. Publish `blocks-deleted`. Release
   the lease.

### Multi-writer coordination

Compaction work is partitioned by `(bucket, signal, day)`. A
lightweight lease (a small object at
`<bucket>/_compact_lease/<signal>/<yyyy-mm-dd>` with a TTL and an
ETag check on takeover) ensures only one writer compacts a given
partition at a time. The lease is acquired by conditional PUT and
renewed by `If-Match: <etag>`.

**Worst case:** a stale lease causes wasted work — two writers both
produce a merged block. Both blocks are valid; the next compaction
round picks them up as small-at-the-next-level and merges them.
**Correctness is preserved by immutability + content addressing.**

Compaction never touches the WAL. The WAL is the
"recent-and-not-yet-uploaded" path; compaction operates strictly on
already-uploaded blocks.

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
each instance keeps a `poll_cursors` table in its SQLite catalog
(schema in [Schema](#schema)) recording the highest UUID v7 seen per
`(signal, writer_id, date)`.

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

## Scaling

scry is designed to scale from a single host running a homelab's
worth of observability data to a multi-node deployment ingesting tens
of terabytes per month. This section sketches the envelope and the
dimensions that need attention as scale grows.

### Sizing envelope

| Compressed/month | Sustained ingest | Servers (with redundancy) | Bucket pool size |
|------------------|------------------|---------------------------|-------------------|
| 500 GB           | ~0.2 MB/s        | 1                         | 1                 |
| 5 TB             | ~2 MB/s          | 2                         | 1–2               |
| 50 TB            | ~19 MB/s sust, ~200 MB/s peak | 6–10           | 5–10 (rotating)   |

A single server with modern NVMe and a 10 Gbps NIC handles
50–100 MB/s of compressed ingest comfortably (parquet encode + WAL
fsync + object-store PUT bandwidth). Capacity is scaled by adding
servers, not by tuning individual ones harder.

### What scales as-is

The following dimensions scale linearly or sublinearly with
deployment size with no design changes:

- **Object-store PUT/GET rate.** S3 partition rate limits apply per
  prefix; `writer_id` in the path naturally distributes writes
  across partitions, giving us `N_writers × 3500 PUT/sec` headroom.
- **WAL throughput.** Per server, no cross-server interaction.
- **Compaction parallelism.** Partition-scoped leases let N servers
  compact N different partitions concurrently with no coordination
  overhead.
- **Retention.** Idempotent DELETE; no coordination at any scale.
- **Catalog row count.** SQLite handles millions of rows trivially.
  90-day retention at 50 TB/month is ~1.2M block rows; query
  planning on indexed columns is sub-millisecond.
- **Cache hit rates.** Block immutability means cache entries are
  valid for their full lifetime; hit rates stay high as catalog
  grows.
- **Cursor-driven polling cost.** Bounded by recent write rate, not
  by bucket lifetime or population.
- **Bucket pool size.** Auto-provisioning + retention together cap
  the live pool size at `ceil(retention_days / bucket_fill_days) +
  small_constant`, regardless of deployment age.

### What needs attention at the upper end

These are dimensions where the design degrades or hits practical
limits past ~6–10 instances or 50+ TB/month, and where specific
extensions are planned:

#### Write amplification — tiered compaction

Already addressed: see [Compaction → Tiered levels](#tiered-levels).
The catalog carries a `level` column from v0.1 so the policy can be
added without schema migration. Naïve "merge whenever small"
compaction multiplies real ingest cost by 5–10× at scale; tiered
keeps it at ~3×.

#### Hot-shard imbalance — capacity-aware assignment

Already addressed: see [Discovery → Capacity-aware weighting](#capacity-aware-weighting).
Without it, naïve consistent hashing assumes uniform agent load,
and one heavy producer can saturate a single server while peers
sit idle.

#### Catalog cold-start cost — snapshot bootstrap

Already addressed: see [Schema → Bootstrap](#bootstrap). A fresh
server bootstrapping its catalog from a full bucket walk is O(N
objects). At ~1.2M blocks (50 TB/month scale) that's ~2 minutes of
ListObjects on cold paths. Snapshot bootstrap drops this to seconds.

#### Pub/sub fan-out — channel sharding

When per-block message rate exceeds a few thousand per second
(deep into the upper envelope), a single `scry/blocks/<signal>`
channel becomes hot. The fix is to shard:
`scry/blocks/<signal>/<hash(writer_id) % N_shards>`. Subscribers
listen to all shards for signals they care about; publishers
hash-route. Not implemented in v0.1, baked into the channel-name
schema so the shift is non-breaking when added.

#### Full-walk reconciliation work — distributed walks

The 30-minute full bucket walk is currently run by every instance
independently. Past a few instances this is redundant. The fix is
hash-partitioned walks: each `(bucket, signal, date)` partition is
assigned to one instance based on `hash(partition) % N_servers`;
that instance walks it and publishes results. Not v0.1.

#### Metrics cardinality — separate index layer

The single biggest scaling unknown in the design. Per-block label-
fingerprint blooms prune well at modest cardinality but lose
effectiveness as the number of distinct label combinations grows.
Above some threshold, a separate **postings-list index**
(Prometheus-TSDB style, or simpler "labels-to-block-set"
materialised view) is required. This is the topic of a dedicated
metrics design conversation, deliberately deferred from the
generic-storage-layer design captured here. **Until this is
resolved, scry's metrics ceiling is unproven**, even though the
other three pillars scale predictably.

### What's deliberately out of scope

- **Distributed query execution.** Splitting one query across N
  reader instances (Ballista-style) is real work and adds a
  coordination protocol. Single-instance DataFusion reading parquet
  from object storage scales very far — Trino and Athena run this
  same model at petabyte scale. If we ever measure a real ceiling,
  we add Ballista or roll our own. Not in the foreseeable plan.
- **Cross-region replication.** Geographic distribution is a
  separate problem from scaling within a region. The intended
  pattern is "run regional scry deployments, query each
  independently." A future cross-region story would build on top
  of this rather than embedding into the storage layer.
- **Geo-aware agent routing.** Agents pick servers from their
  local region's registry; cross-region agent traffic is not a
  scry concern.

### Operational notes

- **Network bandwidth.** At 50 TB/month sustained ingest +
  compaction read/write traffic + query reads, expect 10 Gbps NICs
  to be the minimum on ingest servers and 25 Gbps preferred at
  peak. Query servers want similar for fast object-store reads.
- **Object-store egress costs.** Free on Hetzner; $0.09/GB on AWS
  S3. Plan provider choice accordingly — at 50 TB/month, an AWS-
  hosted scry deployment is ~$4500/month in egress alone if
  queries pull all the data through scry rather than letting
  clients query directly. This is one reason we keep queries
  *server-side* (clients receive results, not raw parquet).

## Configuration

The entire config file:

```toml
# Recommended shape: let scry manage the bucket pool itself. On
# bootstrap and when buckets fill, scry creates new ones from the
# template. Catalog tracks them; no config edits required at runtime.
[storage.template]
enabled      = true
name_pattern = "scry-{installation}-{date:%Y%m}"
installation = "prod"
backend      = "s3"
endpoint     = "https://fsn1.your-objectstorage.com"
region       = "eu-central"
max_bytes    = "90 TiB"  # seal at ~90% of the provider's hard limit
# credentials need s3:CreateBucket and s3:ListAllMyBuckets in
# addition to data-plane perms; via standard AWS env vars

# Alternative / supplemental: hand-listed buckets. Use this when you
# want operator-managed buckets (no template), or when migrating
# existing buckets into a scry-managed deployment.
# [[storage.buckets]]
# name      = "legacy-imported-2025"
# backend   = "s3"
# endpoint  = "https://fsn1.your-objectstorage.com"
# bucket    = "scry-prod-2025"
# region    = "eu-central"
# max_bytes = "90 TiB"

[wal]
dir         = "/var/lib/scry/wal"
segment_mib = 256
# writer_id auto-generated and persisted under wal.dir on first start;
# set explicitly here to override (must be unique across instances).
# writer_id = "ingest-eu-1"

[listen]
ingest = "0.0.0.0:4000"
query  = "0.0.0.0:4001"

[role]
# All true by default. Set false to specialise this server. See
# Deployment topologies. Discovery only advertises ingest=true nodes
# on the ingest channel.
ingest     = true
query      = true
background = true   # compaction + retention

[valkey]
url = "redis://valkey.internal:6379"
region = "eu-central"
# Used for the control plane: agent->server discovery, block-event
# pub/sub, bucket-event pub/sub. Optional for single-instance
# deployments. When set but unreachable, scry degrades to polling-
# based block discovery and refuses new agent registrations until
# Valkey is back.

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

These genuinely need answers before the milestones that depend on
them. Items that are "deferred to a known plan" (tiered compaction
policy, channel sharding, distributed walks) live in
[Scaling](#scaling) rather than here.

- **Metrics cardinality and indexing.** The biggest scaling unknown.
  Per-block label-fingerprint blooms prune well at modest cardinality
  but degrade as the number of distinct label combinations grows. A
  postings-list index (Prometheus-TSDB-style) or simpler
  labels-to-block-set materialised view is likely needed past some
  threshold. Deserves a dedicated design conversation before any
  serious metrics workload is loaded.
- **Profiles payload schema.** Native pprof is the obvious answer,
  but pprof-in-parquet has nontrivial schema questions (deeply
  nested, shared symbol tables). We may want to denormalise on
  ingest and store one row per sample-with-resolved-stack. Resolves
  during v0.4 design.
- **PromQL semantics on DataFusion.** Range vectors, instant
  vectors, recording rules, and alerting do not map cleanly to SQL.
  How much we can lower into DataFusion's logical plan vs how much
  needs custom plan nodes is an open performance/complexity
  tradeoff. Resolves during v0.5 design.
- **TLS / auth between agent and server.** Probably mTLS with a CA
  shipped to agents, but the operational shape (cert rotation,
  joining flow, agent identity binding) is worth a dedicated mini-
  design before v0.2 ships outside the homelab.
- **Backup and disaster recovery.** Object storage gives us
  durability, but does scry itself need to back anything up? The
  catalog is derived; the WAL is per-server and recoverable from
  re-ingest in principle. The honest answer is "depends what
  guarantees we offer," which we haven't pinned down.
- **License.** Probably MIT or Apache-2.0. Pick before external
  contributors show up.
