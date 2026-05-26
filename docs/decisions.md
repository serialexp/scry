# Decisions

A log of architectural decisions we have made, with the reasoning at the
time. New decisions append; existing entries are not edited (if a
decision is reversed, add a new entry that supersedes it).

---

## D-001: Single Rust binary, not microservices

**Date:** 2026-05-26
**Status:** accepted

The Grafana stack's distributor/ingester/querier/store-gateway/compactor
split exists to let each component scale independently. At our target
scale (single-digit to low-tens of TB/yr) that benefit is zero, and the
cost (config explosion, deployment complexity, inter-service
serialization) is high. One binary with subsystems is the right shape.

When we someday want to run separate read replicas (queriers pointed at
the same bucket), that's a deployment-time choice, not an architectural
split — the same binary can run in "ingest-only" or "query-only" mode.

## D-002: Parquet on S3-compatible object storage as the only store

**Date:** 2026-05-26
**Status:** accepted

We considered separate stores per signal (Loki/Tempo/Mimir each use
different on-disk formats). Parquet handles all four payload shapes,
DataFusion already speaks it, and `object_store` already abstracts every
S3-compatible backend plus local FS. One format, one backend
abstraction, no boltdb-shipper / tsdb-index / vParquet zoo.

No Azure Blob or GCS in v1, but they cost nothing to add later because
`object_store` already supports them.

## D-003: Single-writer preferred, multi-writer required to work

**Date:** 2026-05-26
**Status:** accepted

Bart is running multiple writers today and needs that to keep working,
even though the *preferred* deployment is single-writer. The design
accommodates this by making writers never share a key prefix
(`<signal>/<date>/<writer_id>/...`), so writes can never collide. The
only place coordination is needed is compaction, which uses
object-storage leases rather than a consensus protocol.

This explicitly rejects the "single-writer now, multi-writer later via
rewrite" option.

## D-004: WAL on local SSD, not pure in-memory buffering

**Date:** 2026-05-26
**Status:** accepted

A pure in-memory ingest buffer cannot bound RAM under ingestion spikes;
that's how Loki ingesters OOM. A WAL on local SSD turns spikes into
disk pressure (cheap, predictable) instead of memory pressure
(catastrophic). It also provides crash safety as a free side effect.

`fsync` on segment rotation, not per record. Per-record durability is
not a goal for observability data.

## D-005: S3-compatible only for v1

**Date:** 2026-05-26
**Status:** accepted

Bart's deployment is S3-compatible (R2/MinIO/Garage all qualify). Azure
Blob and GCS are not in v1 scope but cost ~nothing to add later via
`object_store`'s existing implementations.

## D-006: binschema for the agent↔server wire protocol

**Date:** 2026-05-26
**Status:** accepted

Bart's own [`binschema`](../../binschema) project gives us a declarative
wire format with Rust codegen and bit-level precision. Compared to
gRPC/protobuf: no runtime weight, no schema-evolution baggage we don't
need, and (importantly) Bart already owns it, so the agent and server
share one source of truth for the protocol.

We will define the protocol as a binschema JSON5 file, vendored into
this repo, and generate the Rust types into both the agent crate and
the server crate at build time.

## D-007: Single tenant per deployment

**Date:** 2026-05-26
**Status:** accepted

Per-tenant limits and overrides are responsible for a huge fraction of
the Grafana stack's config complexity. We delete the concept entirely:
one deployment, one tenant, one bucket. If you need isolation, run
another deployment. This is fine because deployments are cheap (one
binary, one bucket prefix).

## D-008: Defer query language design

**Date:** 2026-05-26
**Status:** accepted

The internal stable interface is a DataFusion logical plan over the
`Record<Payload>` virtual table. Language frontends (PromQL, LogQL-ish,
TraceQL, or our own) come *later*, and we prefer to lift existing Rust
parsers (e.g. `promql-parser`) rather than write our own. Building a
query language before we know what queries hurt is the kind of yak we
won't shave.

## D-009: Defer UI

**Date:** 2026-05-26
**Status:** accepted

No UI in early milestones. Once the query layer stabilises, we'll
decide between (a) a Grafana datasource adapter, (b) a small purpose-
built web UI, or (c) both. None of those choices affect the storage or
query design, so deferring costs nothing.

## D-011: Aggressive caching because blocks are immutable

**Date:** 2026-05-26
**Status:** accepted

Object storage round-trip (~30 ms) vs RAM access (~50 ns) is six
orders of magnitude. A query that hits S3 for metadata once per block
is unworkable; one that hits a local RAM cache for metadata and only
S3 for actual data bytes is competitive.

The architectural enabler is block immutability. Because a parquet
file never changes after upload, every cache layer (catalog, footer,
page index, decompressed pages) is invalidation-free except for
deletion events, which we generate ourselves and can therefore
propagate locally.

This decision links to D-003 (multi-writer): writers never share a
key prefix, so no writer ever invalidates another writer's cached
metadata. Cross-instance cache invalidation reduces to "notify peers
when I delete a block."

Layers 1–3 (catalog, footer, page index) are mandatory from v0.1.
Layer 4 (decompressed pages) is a later optimisation.

## D-010: Start with naming + repo + this doc

**Date:** 2026-05-26
**Status:** accepted

Before any code: name the project, write the README, write the
architecture, write this decisions log. This document exists so a year
from now we can answer "why did we do it this way" without re-deriving
it from first principles.

The first *code* milestone (v0.1) is the storage layer with a dummy
record type. No signals, no ingest agent, no query — just "can we write
and read a parquet block through the WAL+catalog+object-store path."

---

## D-012: Valkey pub/sub for block discovery, polling as backstop

**Date:** 2026-05-26
**Status:** accepted

Multi-instance deployments need a way for peers to learn of new
blocks. We chose **Valkey pub/sub** for low-latency notification with
**`ListObjects` polling (5 s)** and **full bucket walks (30 min)** as
defense-in-depth backstops.

Why Valkey: it's an operational primitive we already understand, a
single instance handles vastly more fan-out than we will ever need,
and the failure mode is benign — if Valkey is unreachable, query
staleness rises from ~0 ms to ≤5 s but correctness is unaffected
because polling is always running. Bucket state is the source of
truth; Valkey is a cache-invalidation hint.

Alternatives considered and rejected: polling-only (5 s of staleness
in the normal case is more than we'd like), peer-to-peer push (more
code, no real benefit over Valkey at any scale), S3 event
notifications / SQS (more moving parts, vendor-specific, only worth
it at scale we won't reach).

Single-instance deployments leave `[valkey]` unset and rely on
polling alone.

## D-013: Per-partition compaction leases via object-storage conditional writes

**Date:** 2026-05-26
**Status:** accepted

Multiple instances run the compactor loop. For each `(signal, day)`
partition, an instance acquires a short-lived lease by `PUT`ing a
small lease object under `_compact_lease/...` with
`If-None-Match: *`. Renewed periodically with `If-Match: <etag>`.
Takeover after expiry uses `GET` + `PUT If-Match: <etag>`.

This requires the object store to support conditional writes. S3 (as
of 2020), R2, MinIO, and Garage all do. Backends that don't are
unsupported.

Correctness if the lease mechanism misbehaves: two instances do
redundant work and produce two valid merged blocks. The next
compaction round merges them. **We will not write defensive recovery
logic for double-compaction**, because immutability + content
addressing already preserves correctness.

Alternative considered: single elected leader does all compaction.
Rejected because per-partition leases distribute load evenly with no
SPOF, and the mechanism is no more complex.

## D-014: 10-minute deletion grace period, fixed

**Date:** 2026-05-26
**Status:** accepted

After compaction uploads the merged block and supersedes the inputs
in the catalog, the input blocks remain in the bucket for 10 minutes
before deletion. This protects in-flight queries that planned against
the inputs before they were superseded.

Fixed value, no config knob. If the value ever proves wrong in
practice we'll revisit, but the default behavior is "this is not
something operators should be thinking about." Mimir exposes a
`deletion-delay` knob; we deliberately don't.

## D-015: writer_id auto-generated, optionally overridden

**Date:** 2026-05-26
**Status:** accepted

Default: random v4 UUID generated on first startup, persisted at
`<wal_dir>/writer_id`, used forever after. Optional config override
(`writer_id = "ingest-eu-1"`) for operators who want human-readable
block prefixes; uniqueness is then the operator's responsibility.

This gives "no-config works fine" by default and "tidy named writers"
when an operator cares. No coordination protocol either way — UUIDs
don't collide, and explicit names are an operator concern.

## D-016: Cursor-driven polling, not prefix walks

**Date:** 2026-05-26
**Status:** accepted

Polling for new blocks uses `ListObjects` with `start-after` keyed on
the highest UUID v7 we've already ingested per `(signal, writer_id,
day)` prefix. Because v7 UUIDs are time-prefixed and lexically
sortable, this returns only blocks created after our cursor with no
client-side filtering and no scan-from-scratch.

The cursor table lives in the SQLite catalog and is updated by both
the pub/sub path and the polling path — they converge on the same
state. Poll cost is bounded by recent write rate, not by bucket
lifetime; a 5-year-old bucket polls as fast as a fresh one.

Polling cadence adapts to Valkey health: 60 s when pub/sub is
healthy (pure backstop), 5 s during a Valkey outage (primary
mechanism), and an immediate full-cursor sweep on reconnect.
Cadences are baked in, not exposed as knobs.

This explicitly avoids adding an hour-level prefix to the block path.
The benefit such a prefix would give (bounded recovery scan after a
Valkey outage) is already provided by UUID v7 + `start-after`, while
the costs (smaller compaction unit, more leases, more cursor rows)
are real. Prefer the cheaper mechanism we already have.

## D-017: Bucket pool with automatic sealing, from v0.1

**Date:** 2026-05-26
**Status:** accepted

A scry deployment is configured with an *ordered list of buckets*,
not a single bucket. Writers always upload to the earliest open
bucket. When a bucket's tracked `total_bytes` exceeds `max_bytes`,
the first writer to notice seals it (via a single global seal lease
and a `_sealed` marker object) and peers route to the next bucket.
Sealed buckets are still read and compacted; drained buckets (sealed
+ zero blocks remaining) are surfaced to the operator for removal.

This addresses two real constraints: hard provider limits (Hetzner's
100 TiB/bucket ceiling) and degrading full-walk performance at large
single-bucket populations. Multi-bucket also gives free parallelism
on the 30-min reconciliation scan.

Single-bucket deployments use a list of length one. Default behavior
is identical to a hypothetical single-bucket-only design — operators
who never multi-bucket pay zero added complexity.

We do this in v0.1 (not v0.6+) because the schema seam — `bucket`
column on `blocks`, `buckets` table in the catalog — is cheap now and
expensive after the catalog has been in the field for a year. We
explicitly reject the "ship single-bucket now, migrate later" path
as paying for a benefit we don't get.

Sealing is automatic on the `max_bytes` soft cap, not manual,
because operators shouldn't need to monitor and intervene at the
moment a bucket fills. `max_bytes` should be configured well below
the provider's hard limit (e.g. 90 TiB on Hetzner's 100 TiB) to
absorb in-flight writes that cross the threshold before pub/sub
propagates the seal event.

Multi-bucket-per-signal (one pool per signal, so logs and traces
don't share quota) is a future extension; not in v0.1.

## D-018: Auto-provisioning of buckets from a template

**Date:** 2026-05-26
**Status:** accepted

When the operator opts in via `[storage.template]`, scry creates new
buckets itself rather than requiring config edits. Bootstrap and
pool-extension both flow through the template. Runtime-created
buckets are recorded in the catalog; scry never modifies the
operator's config file.

Name pattern is date-based with collision suffix:
`scry-{installation}-{date:%Y%m}`, falling back to `-2`, `-3`, ... on
collision. Matches the operator's mental model of "this bucket holds
data from around month X."

The next bucket is **pre-provisioned at a 70% watermark**, not at
the moment of sealing. This keeps the seal path free of API calls
that can fail or be slow, and turns `CreateBucket` failures into
calmly-retried background events rather than write-stalling crises.

`CreateBucket` is idempotent — concurrent writers attempting the
same name converge on the same bucket. No lease needed for creation;
only sealing uses the lease.

Bucket-pool discovery for a writer joining late uses, in order:
config seed, peer state from Valkey, and a provider `ListBuckets`
filtered by the template's name pattern. The last is permission-
cheap (one extra IAM action) and removes the need for any custom
pool-coherence protocol.

We **never auto-delete** even drained buckets — destruction of data
is operator-driven. We **never modify** the user's config file —
runtime state lives in the catalog. Both are non-negotiable
guardrails on a system that takes a lot of automated actions.

Required permissions when template is enabled: `s3:CreateBucket`
and `s3:ListAllMyBuckets` in addition to the data-plane ones. If the
operator doesn't want to grant these, they leave `template.enabled =
false` and manage `[[storage.buckets]]` manually.

## D-019: Tiered compaction (L0 → L1 → L2 → L3)

**Date:** 2026-05-26
**Status:** accepted (schema seam from v0.1, policy lands v0.6)

Compaction merges blocks within a level into one block at the next
level up. Targets: L0 ≈ 128 MiB (freshly written), L1 ≈ 1 GiB, L2 ≈
10 GiB, L3 ≈ 100 GiB. Level fan-out is ~8, capping write
amplification at roughly `log_8(total_size_per_day)` — about 3× at
50 TB/month rather than the 5–10× of naïve "merge whenever small."

Size-tiered (not levelled / LevelDB-style) because observability is
append-mostly and size-tiered's slightly worse read amplification
is recovered by our parquet pruning anyway.

The `level` column on `blocks` is added in v0.1 so the policy can
land later without a schema migration. Until then, all blocks are
L0 and "compaction" just doesn't do anything except mark blocks
for promotion when the threshold is crossed.

## D-020: Valkey-as-service-registry + consistent hashing for agent routing

**Date:** 2026-05-26
**Status:** accepted

Agents discover scry servers via a Valkey sorted set
(`scry/servers/<region>`) with TTL-based heartbeats and reaping.
Agent-side selection uses consistent hashing on `agent_id` with
top-3 fallback for live server outages.

Properties: stable affinity (an agent's data concentrates on one
server, not fragmented across many), smooth rebalancing (1/N
churn on server set changes), no central LB required, no DNS
truncation issues.

Alternative discovery backends (k8s/EDS, Consul) are pluggable
via the agent's `[discovery]` config block. Valkey is the default
because we already depend on it for control-plane pub/sub.

DNS round-robin was rejected as the default because it breaks at
~20–30 servers (UDP truncation), has no health awareness, and gives
no stable affinity.

## D-021: Capacity-aware agent assignment via weighted virtual nodes

**Date:** 2026-05-26
**Status:** accepted (policy is v0.7+; mechanism in v0.1 wire and
discovery)

Servers publish a capacity weight along with their registration.
The consistent-hash ring grants a server `weight × base_vnodes`
positions, so higher-capacity servers receive a proportionally
larger share of agents.

We deliberately do *not* implement load-based per-agent steering
("which agent is hot, move it elsewhere"). That pattern is chatty,
prone to thrashing, and rarely worth its complexity. Static
weighting plus large vnode counts give us 90% of the benefit.

Weight is operator-configured initially; auto-derivation from
CPU/network/WAL-throughput headroom is a v0.7+ extension.

## D-022: Deployment topology modes (full / ingest-only / query-only)

**Date:** 2026-05-26
**Status:** accepted

A scry binary can run in one of three modes selected by `[role]`
flags. `full` is the default and is correct up to ~30 instances.
`ingest-only` and `query-only` exist for large deployments where
sizing ingest (CPU + WAL disk I/O) separately from query (RAM +
network) yields meaningful operational wins.

Query-only nodes are stateless beyond their catalog cache; they
register on a separate discovery channel (`scry/queriers/<region>`)
so a query router can pick from them independently of ingest
routing.

This split was added because the design is otherwise symmetric and
adding the role flag costs nothing now but would require careful
threading later. The wire protocol and catalog don't change.

## D-023: Catalog snapshot bootstrap for fast cold starts

**Date:** 2026-05-26
**Status:** accepted (mechanism v0.6+; snapshot writer optional
before that)

A new instance bootstraps its catalog from (1) catalog snapshot in
object storage if present, (2) Valkey tail from the snapshot's
sequence number, (3) full bucket walk as fallback. Snapshots are
written hourly by the instance holding a designated lease and
stored as parquet objects in a known location.

This caps cold-start time at "one snapshot read + recent tail"
regardless of bucket size. Without it, a fresh instance walking
1.2M blocks takes minutes; with it, seconds.

Small deployments can skip the snapshot writer entirely and rely
on the full-walk fallback.

## D-024: Scatter-gather query execution across the worker pool

**Date:** 2026-05-26
**Status:** accepted

Queries fan out across the live query worker pool rather than being
served end-to-end by a single instance. A coordinator (the instance
the client routed to) plans the query, partitions block scans across
workers, dispatches `Execute(partial_plan, blocks)` RPCs over Arrow
Flight, and merges streaming Arrow `RecordBatch`es into the final
result. Workers run the partial plan (filter, project, partial
aggregate); coordinator runs the final plan (merge, final aggregate,
sort, limit).

This is **scatter-gather (MPP-lite), not full distributed query
execution.** Ballista / Trino-style multi-stage execution with
shuffles and distributed joins is rejected as out of scope.
Observability queries are overwhelmingly scan + filter + aggregate,
which scatter-gather handles directly. We get ~95% of the benefit
at ~5% of the complexity.

The key property that makes the design simple: **workers don't
plan, only execute against the explicit block list the coordinator
hands them.** Worker catalog freshness is irrelevant for query
correctness; cold workers serve queries immediately; failed workers
are replaced by reassigning their blocks. This is only possible
because blocks are immutable and content-addressed by full path.

Pre-aggregation pushdown via DataFusion's existing partial/final
`Aggregate` modes is where most of the win lives. For typical
aggregating queries (which is most of them), partial results sent
over the network are 6–7 orders of magnitude smaller than the raw
matched rows.

Threshold for fan-out (~20 blocks) is internal behavior, not a
config knob. Below threshold the coordinator executes locally;
above, it scatters. Point lookups (trace-by-id) forward the whole
query to one worker by hash with no merge.

Worker pool discovery uses the same Valkey-sorted-set mechanism as
agent → ingest server discovery, on a separate channel
(`scry/queriers/<region>`). A `full` node appears in both
registries; `ingest-only` only in ingest; `query-only` only in
queriers.

Arrow Flight is the chosen transport: zero-copy receive, designed
for this exact use case, already integrated with DataFusion.

Implementation is a v0.6+ milestone (after v0.5 metrics ships), but
the design lands now so coordinator/worker split, partial/final
plan threading, and the queriers registry channel are baked in
from the start.

## Deferred / open

These are not decisions yet; they're flagged for "we'll decide when the
constraint shows up":

- **Profiles payload schema.** Native pprof vs. denormalised
  one-row-per-sample. Decide during v0.4 design.
- **High-cardinality metrics index.** Per-block label-fingerprint blooms
  may suffice; if not, we add a sketch (HLL? cuckoo filter?) — decide
  based on measurement during v0.5.
- **TLS / auth model.** Probably mTLS with a CA file. Mini-design
  before v0.2 ships outside Bart's homelab.
- **Read-replica catalog coherence.** Polling `ListObjects` is fine
  initially; revisit if query staleness becomes a complaint.
- **License.** TBD. Probably MIT or Apache-2.0; pick before any
  external contributor shows up.
