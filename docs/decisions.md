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
**Status:** SUPERSEDED by D-038 (v0.9). The `If-None-Match`/`If-Match`
object-store lease below is **not buildable on Garage** — Garage has no
consensus and its own docs state conditional writes cannot implement mutual
exclusion between concurrent writers (it silently ignores `If-None-Match: *`,
which is also why D-029 dropped `put_if_absent`). v0.9 replaces it with a
**Valkey lease** (`SET NX PX` + Lua compare-and-set renew/release); see D-038.
The "double-compaction is harmless" claim below is also weaker than stated:
blocks are addressed by **random UUID**, not content hash, so two merged blocks
of the same inputs are *distinct* and both live — a later merge unions rather
than dedupes them. Single-winner coordination is therefore a **correctness**
requirement, which D-038's lease + commit-point fence + grace=0 delete provide.
The original (now-historical) design follows.

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

## D-025: Per-block postings index + intra-block sort for metrics

**Date:** 2026-05-26
**Status:** accepted (schema seam in v0.1, full implementation in v0.5)

Metrics blocks get treatment beyond the generic record/storage
layer: a per-block postings index (`<block>.postings.parquet`)
mapping `(label_name, label_value) → series_fingerprints`, plus
intra-block sort by `(series_fingerprint, ts)` so that
fingerprint-based row-group pruning is effective.

The combined pruning hierarchy for metric queries:

1. Catalog → blocks in time range (ms).
2. Bloom → coarse "might this block contain this label?" (μs/block).
3. Postings → exact series fingerprints in this block matching the
   label predicate (ms/block).
4. Parquet row-group min/max on `series_fingerprint` → skip ~99%
   of row groups within matched blocks.
5. Parquet page index → skip pages within matched row groups.

Without postings + intra-block sort, the bloom-only approach
catastrophically degrades past ~500k active series because (a)
common labels defeat the bloom and (b) time-ordered rows defeat
intra-block pruning. With them, scry's metrics layer comfortably
targets the 30–60M-active-series envelope.

Sized concretely: at 60M active series with ~50k distinct (label,
value) pairs globally, per-block postings files are 100 KB to a few
MB — a small fraction of the main parquet's size.

Series fingerprint is xxh3-64 of the canonicalised label set. At
60M series, birthday-collision probability is ~10⁻⁵, acceptable; the
parquet rows carry the full label set so collisions are
post-hoc detectable.

Other signals (logs, traces, profiles) do not use postings; their
query patterns don't benefit. Logs are time + label + substring
search; traces are by trace-id; profiles are by `(profile_type,
time_range)`. Bloom-only is sufficient for all three.

The `has_postings` and `postings_size_bytes` columns are added to
the `blocks` table in v0.1 so the index can land in v0.5 metrics
work without a schema migration. The `.postings.parquet` sidecar
path is reserved in the block layout from v0.1.

Optional cardinality safeguards (`max_series_per_metric_name`,
`max_total_active_series`, per-label cardinality-explosion
exclusion from the index) are *opt-in* — disabled by default,
configurable for deployments worried about runaway exporter
cardinality.

This decision resolves the last remaining "asterisk" on the metrics
pillar from earlier scaling analysis. The metrics ceiling is now
designed-for, not unknown.

## D-026: Memory and CPU are not free — resource discipline as a first-class principle

**Date:** 2026-05-26
**Status:** accepted

Resource discipline is added as Guiding Principle #6, raising it
from an implicit property of various design choices to a stated
rule that every future addition must justify itself against.

The principle has five concrete implications, all already
visible in the existing design but not previously named together:

- **Bounded by construction, not by tuning.** WAL caps RAM at the
  current building block. LRU caches are byte-bounded. Scatter-
  gather's coordinator memory is bounded by group keys.
- **Backpressure over buffering.** Upstream stops when downstream
  is slow; pressure surfaces where reasonable, not in RAM growth.
- **Sketches over sets.** HyperLogLog for distinct counts, bloom
  filters for membership, count-min for frequency. Bounded
  approximation replaces unbounded materialization.
- **Per-query memory budgets** at workers, enforced via DataFusion's
  `MemoryPool`. Queries that would exceed budget spill to disk or
  fail with a clear error. The worker keeps serving other queries.
- **Streaming over materialization.** Partial aggregation,
  Arrow Flight streaming, parquet row-group iteration — wherever
  incremental beats batch.

This is the principle that most concretely distinguishes scry
from the Grafana stack, which assumes Grafana-Cloud-style
autoscaling infra where "throw memory at it" is the response to
most failure modes. **scry is designed for known, bounded
resource envelopes on hardware you own**, not for elastic clouds
where memory is a slider.

The principle was articulated after the design was largely
complete, but auditing what we have shows we'd already been
following it everywhere except one place: per-query memory
budgets at workers. That gap is closed by the new "Per-query
memory budgets" subsection in Query, with a configurable
`[query]` block (`memory_per_worker`, `memory_per_query_max`,
`spill_dir`, `spill_disk_max`).

The cost of stating this principle now is zero — nothing in the
design changes. The benefit is that future contributions
(features, optimisations, refactors) get evaluated against the
explicit bound, so we don't drift back into "we'll tune it later"
thinking that makes operators' lives hard.

## D-027: Resource isolation between workloads and signals

**Date:** 2026-05-26
**Status:** accepted

The single-binary architecture (D-001) means ingest, query,
compaction, retention, and four signals all share one process.
That's an operational win — one config, one set of metrics, one
deploy — but contention between those workloads is now our
problem to solve rather than the kernel's. The Grafana stack
side-steps this by splitting workloads into separate services
that can be scaled and resource-bounded independently; we keep
the unified process and enforce isolation inside it.

Five mechanisms, all reflected in the new "Resource isolation"
section of ARCHITECTURE.md and the corresponding config blocks:

- **Named Tokio runtimes per workload class** (`query`, `ingest`,
  `background`, `control`). Cross-runtime calls go through
  channels. A pegged compaction loop cannot stall query workers
  because they're on different schedulers.
- **Named memory pools with hard caps** (`query`, `caches`,
  `wal_builders`, `ingest_buffers`). A pool that hits its
  ceiling does not steal from another — it spills, evicts, or
  applies backpressure. The process cannot OOM from one
  subsystem saturating, because every allocator has a named
  home with a known cap.
- **Per-signal WAL subdirectories.** Each signal gets its own
  segment sequence so fsync on a fat trace segment doesn't
  delay a metric append, and a stuck logs builder cannot pin
  trace segments.
- **Token-bucket fair scheduler across signals at ingest.**
  Default is equal-weight fair share (no effect under normal
  load, prevents monopolisation under contention); explicit
  byte-rate caps and `unlimited` exemptions are configurable.
- **Compaction throttled by self-observed query P99.** Instead
  of operators sizing compaction parallelism in advance, each
  server tracks its own rolling P99 query latency and pauses
  new compactions when latency exceeds a threshold. Reactive,
  not predictive, but it has the "no knob" property: compaction
  uses whatever headroom queries leave it.

The compaction throttle deserves a note on the decision itself.
Two reasonable options exist: a *static* concurrency cap, or a
*dynamic* feedback loop on observed latency. A combined "static
ceiling + dynamic backoff under the ceiling" is technically
strictly more conservative, but it forces operators to size the
ceiling — which is exactly the kind of "you must understand and
tune this" knob the project exists to avoid. The dynamic-only
path has one knob (`pause_if_query_p99_above`) whose meaning
("how much query latency are you willing to trade for faster
compaction catch-up") is a product decision, not a hardware
sizing exercise. We accept the worst case where idle servers
might run a lot of parallel compactions; bound that with a small
`max_concurrent` floor (default 2) sized to avoid saturating
object-store connection pools rather than to throttle CPU.

The cost of this whole section is real: more config surface
(`[runtime]`, `[memory]`, `[ingest.rate_limits]`, `[compaction]`),
more code to keep workloads on the right scheduler, and a P99
tracking loop with its own correctness properties. It's still
within the "one screen of config" target, and every block has a
direct user-visible failure mode it prevents.

The alternative we rejected: rely on cgroups and the OOM killer
as the backstop. That works in the abstract — Linux is happy to
kill a process that misbehaves — but a server that dies during a
compaction is one that has to recover its WAL, re-warm its
caches, and re-establish its place in the worker pool, all of
which add latency to the queries the kill was supposed to
protect. Better to fail at the subsystem level (a single query
spills or fails; a single ingest batch gets backpressure) than
at the process level.

## D-028: Profiling and performance as a development principle

**Date:** 2026-05-26
**Status:** accepted

Added as Guiding Principle #7. Paired with D-026 (resource
discipline): #6 says "stay within the budget"; #7 says "and be
fast enough within it." Neither holds up without the other —
bounded memory doesn't help if a query takes ten minutes, and a
fast query that OOMs the worker isn't fast.

Concretely:

- Hot paths ship with Criterion benchmarks that run in CI.
  Regressions are bugs, not metrics drift.
- Flamegraphs are checked into `bench/baselines/` so regressions
  are visible as a diff. Without checked-in baselines, "the
  flamegraph looks weird" is unfalsifiable.
- Per-item allocation in hot loops is treated as a defect class,
  not a style preference. The pattern shows up repeatedly across
  hot-loop code (and is called out at length in CLAUDE.md);
  baking it into the principles means every PR review can cite it.
- The boundary between "ours to profile" and "trust the engine":
  DataFusion's execution layer is treated as a black box that
  does the right thing; the glue we write around it (projection
  construction, postings application, scatter-gather merge) is
  not. That's where the profiling effort goes.
- Stale benchmarks get deleted, not preserved. A benchmark that
  no longer reflects the real workload creates false confidence.

We do not adopt a hard "no performance regressions ever" policy
— sometimes the right design is slower in microbenchmarks and
better in production (e.g. fewer allocations at the cost of one
extra index). The policy is "regressions get *noticed* and
*argued*," not "regressions get rejected."

The cost is real: maintaining benchmarks is work, and CI time
goes up. The alternative is the classic "ship, profile when
users complain" loop, which for an observability system means
the users complaining are the ones whose own observability is
broken. We'd rather pay the development tax.

## D-029: v0.1 storage layer — minimum viable scope

**Date:** 2026-05-26
**Status:** accepted

The full storage layer in `ARCHITECTURE.md` (bucket pool, sealing,
Valkey convergence, signal-specific paths, postings index, catalog
snapshots) is too much to land in one milestone. v0.1 ships the
**smallest pipeline that exercises the architecture end-to-end**:
one dummy record type, one bucket, one writer, no signals, no query.
Details in [`v0.1-storage.md`](./v0.1-storage.md).

Three sub-choices pinned here because they're the language-level ones
future readers will want the rationale for:

- **Object-store client: apache `object_store` (arrow-rs).** Speaks
  S3-compatible out of the box (works with Garage), includes a local
  filesystem backend for unit tests, and means we don't write S3
  retry / multipart / signing logic ourselves. The cost is one more
  large dependency and an opinion about async-trait-shape we don't
  fully control. Acceptable; the alternative (rolling our own `trait
  ObjectStore` over `aws-sdk-s3`) is more code we'd have to keep
  correct against every provider's quirks.

- **SQLite client: `rusqlite` (bundled feature).** Synchronous, no
  compile-time SQL checks, no `.sqlx/` cache to commit, no system
  sqlite required. The catalog is four tables and a handful of
  queries; `sqlx`'s compile-time guarantees aren't worth the build
  complexity at this size. The catalog API hides the choice — we
  can swap to `sqlx` later if a real reason appears.

- **Crate split: four crates per concern (`scry-objstore`,
  `scry-wal`, `scry-block`, `scry-catalog`).** Each owns one piece
  of the architecture, none depends on another except through narrow
  type seams. Composition lives in the existing `noise-sink` for
  v0.1; an eventual `scry-server` crate gathers them in v0.2 when
  the first real signal arrives. The alternative — a single
  `scry-storage` crate with submodules — is fewer Cargo.tomls but
  worse for isolated testing and harder to reason about ownership
  in. Four small crates is the right grain.

The dummy record itself (`{ ts_unix_nano, key, value }`) is a v0.1-only
artefact. It does not appear in any wire-protocol decision; the
spewer reuses the existing `Batch` frame with a `Signal::Dummy = 0xFE`
sentinel that is removed when real signals come online (0xFF was
already taken by `SIGNAL_ALL` in `FlowControl`).

## D-030: v0.1 storage layer — complete

**Date:** 2026-05-26
**Status:** accepted

The pipeline described in D-029 is in. `scripts/smoke.sh` is the
scripted exit criterion: it empties the dev Garage bucket, runs
noise-sink (`--storage --wal-dir --catalog`), sends a known number
of dummy batches through noise-spewer (using `--max-batches` for an
exact count — `--rate × --duration` is off-by-one in practice), then
runs `scry-list` against a fresh empty catalog and asserts that the
reconciled row count equals exactly the number of records sent and
that at least one block landed. The smoke run at `--max-batches 2000`
(× 256 records/batch = 512,000 records) passes with `total_rows=512000
blocks=1`.

What that proves, in architecture terms:

- The wire path decodes a `DummyBatch` correctly under zstd.
- The WAL durability boundary works in both directions: a SIGKILL
  mid-stream leaves the most recent few records lost (consistent
  with the "fsync on rotation, not per record" doc), but everything
  earlier replays into a fresh block on next start.
- The block builder produces a parquet that round-trips through
  `object_store` to Garage, sorted by `ts_unix_nano`, with a sidecar
  that's parseable as `BlockMeta`.
- The catalog can be **rebuilt from the bucket alone**
  (`reconcile_from_bucket`). That's the property that lets multi-
  writer work later without coordination, and v0.1 demonstrates it
  with one writer and three blocks across separate process runs.

What's deferred to v0.2 (still applies as written in
`v0.1-storage.md § Open questions parked for v0.2`):

- Label-fingerprint bloom in the catalog `fingerprint BLOB` column —
  decided when the first signal with labels lands.
- WAL replay across `(signal, day)` boundary — no day boundary in
  v0.1 because every record is "now".
- Signal-specific block builders — likely a trait per signal that
  produces a `RecordBatch`; details when signal #2 arrives.
- Whether `scry-server` is its own crate or a binary inside
  `scry-storage` — defer until composition pressure shows up.

`Signal::Dummy = 0xFE` is the only piece of v0.1 that goes away when
real signals land. Everything else (WAL framing, sidecar schema,
catalog schema, object-storage path layout) is forward-compatible.

## D-031: Query daemon speaks binschema, not Arrow Flight

**Date:** 2026-05-27
**Status:** accepted (reverses the client↔daemon portion of D-024)

The v0.3 query daemon (`scry-queryd`) speaks the same length-prefixed
binschema framing pattern as ingest, not Arrow Flight. `QueryFrame`
is defined in `proto/query.schema.json` alongside the existing
ingest schema; the wire shape is `client → server: QueryRequest`
followed by `server → client: SchemaMsg, BatchMsg*, EndOfStream |
StreamError`. The Arrow IPC payload itself (schema + record batches)
is unchanged from D-024 — binschema is purely the envelope. We
keep zero-copy decode (the client feeds `arrow_ipc::reader::Stream
Decoder` directly), drop `arrow-flight`/`tonic`, and run one
framing layer across the product instead of two.

What we keep:
- Same TCP transport pattern as ingest. One framing layer in the
  codebase, one set of frame helpers (`scry-proto::framing::{
  read_frame, write_frame}` generalised over a `Framed` trait).
- The Arrow IPC payload itself, byte-for-byte. `IpcDataGenerator`
  on the server, `StreamDecoder` on the client.
- Per-batch streaming, mid-stream error mapping (DataFusion
  `ResourcesExhausted` → `StreamError(QUERY_ERR_RESOURCES)`),
  `scan_complete` observability surface unchanged.

What we lose (acknowledged, deferred):
- Plug-and-play with Arrow-native tools (`pyarrow.flight`,
  `datafusion-cli --flight`, `arrow-js`). No current caller needs
  them; if one shows up we can re-introduce a Flight gateway in
  front of the binschema daemon later.

**Scope.** This decision covers the client↔daemon transport (CLI
talking to a single `scry-queryd`). The worker↔coordinator transport
inside scatter-gather (D-024) is still hypothetical — v0.6+ work —
and will be re-decided at that time. The binschema migration here is
evidence in favour of also doing worker↔coordinator on binschema for
the same one-vocabulary reason, but that's not a commitment.

The migration also surfaced a real gap in binschema: the Rust
generator emits `NotImplemented` for `optional` fields inside
discriminated_union variants. We worked around it with explicit
`*_present: uint8` companion fields in `QueryRequest`; the gap is
filed for the binschema project to fix upstream. Doing the migration
exposed this, which is one of the reasons we did it now rather than
waiting.

## D-032: Logs as the second real signal (v0.4 step 1)

**Date:** 2026-05-28
**Status:** accepted

Until v0.4 there was exactly one fully-implemented signal end-to-end
(metrics: ingest → block → catalog → query daemon → CLI, plus the
postings cache and the binschema query transport). Until a second
signal exists, every "signal-agnostic" boundary in the codebase is
only ever exercised in one shape — which is to say, untested. This
step lights up logs as the second real signal specifically to force
out the abstractions that had been hardcoded "metrics" everywhere.

The intent here is architectural validation, not feature parity with
the logs ecosystem (Loki, ClickHouse, Elastic). v0 logs answer
"what entries match these stream labels in this time window, plus
optionally arbitrary SQL on the resulting table" — that's enough to
exercise every per-signal seam.

Four design choices, recorded so the v0.5/v0.6 work doesn't
re-litigate them:

1. **Log indexing mirrors metrics.** Postings sidecar on
   `LogStream.labels` only — service, host, env — playing the same
   role as a metrics series-label inverted index. Per-entry attributes
   (`LogEntry.attributes`: trace_id, status, …) become a flat
   `Map<Utf8,Utf8>` column on the main parquet, queryable through SQL
   but not pushdown-eligible at the postings layer. This is the same
   "per-series indexed, per-sample not" split metrics has, intentionally
   — per-entry indexing is a different problem (per-entry attribute
   cardinality is unbounded) and not blocking v0.4.

2. **Body-substring search is deferred.** `body LIKE '%pat%'` works
   today, but as a full column scan after time-range + label pruning.
   Real substring / phrase / RE2 search will be a tantivy-backed phase
   later (tantivy is already used elsewhere in the codebase and is
   strong at this). Deliberately staged: v0.4 step 1 proves the
   two-signal shape; the indexing answer can be its own decision when
   we have a real query mix to size against. **(Superseded by D-035: v0.7
   shipped full-text as a scan path + inline bloom skip sidecar, not a
   tantivy inverted index.)**

3. **Duplicate-first for signal-divergent code; share only the
   genuinely-shared envelope.** `LogsBlockBuilder`, `LogsTable`, the
   per-signal `decode::logs` adapter, and the postings-resolve dance
   are all parallel-and-similar to their metrics counterparts rather
   than abstracted behind a "signal-shaped trait." This is the right
   v0 shape — once logs picks up body-search resolution, the
   builder and table provider will diverge enough that an early
   abstraction would have to be rewritten anyway. *But* the
   `(matchers, ts_min, ts_max)` query envelope is genuinely the same
   shape for both signals today, and the duplication would look like
   a bug — so that one struct is shared (renamed from `MetricsQuery`
   to `Query` in `scry-query::lib`). When a signal diverges, `Query`
   either grows new optional fields (everyone ignores what they don't
   need) or splits into a signal-tagged enum; today the share is
   honest.

4. **Wire dispatch via an explicit signal byte.** `QueryRequest`
   gained a `signal: uint8` field (required, not present-gated; 0
   is not a valid signal and the server rejects it with
   `QUERY_ERR_BAD_REQUEST`). The CLI grew `--signal logs|metrics`
   defaulting to `metrics`. The `Signal` enum in
   `scry-proto::constants` already had the byte values from the
   ingest path (`Metrics=1, Logs=2, …`) so the same constants serve
   both directions.

The single non-trivial architectural change this step required was a
new `all_fingerprints: Option<Vec<u64>>` field on `BlockMeta`. The
empty-matcher postings fallback used to read `meta.series_types`
(metrics-specific, carries counter-vs-gauge metadata). That doesn't
generalise — logs has no per-stream type metadata. `all_fingerprints`
is the signal-agnostic shape both block builders populate, and
`scry-query::postings::resolve_fingerprints` now drives off it. The
old `series_types` field stays populated for metrics blocks (type-
aware queries don't exist yet but will) but is no longer the
empty-matcher read path.

The catalog (`signal` is just a column; index already covers
`(signal, date, ts_min, ts_max)`), the WAL (subdirectory is keyed off
`B::SIGNAL` of the pipeline's block builder type), `scry-list`, the
postings cache (keyed by block UUID, globally unique), and the
ingest server's `Pipeline<B>` generic all needed zero changes — every
abstraction that was claimed to be signal-agnostic actually was.
That's the exit criterion this step was set up to test, and it
passed.

## D-033: v0.4 logs vertical — complete

**Date:** 2026-05-28
**Status:** accepted

The logs signal described in D-032 is in, end to end, and sealed by the
same `scripts/smoke.sh` that sealed v0.1/v0.2 — extended with a **query
round-trip leg**. The script now, for `SIGNAL=logs`, `metrics`, and
`both`, reconciles a fresh catalog from the bucket and then drives it
through `scry-query --signal <sig>` (implicit `SELECT * FROM <table>`,
stream-drained), asserting the scanned row count equals exactly the
sink-accepted count for that signal. v0.1 only proved *bytes landed in
the bucket*; v0.4's headline is *querying logs back*, so the seal proves
the full `ingest → WAL → block → bucket → catalog → DataFusion query`
loop is loss-free for both signals through one shared plumbing.

What that proves, in architecture terms, on top of D-030:

- The per-signal postings sidecar (`build_postings` / `encode_postings`)
  works identically for logs' stream labels and metrics' series labels —
  `SIGNAL=both` lands ≥1 block of each with a non-empty postings file.
- The shared `(matchers, ts_min, ts_max)` `Query` envelope (D-032) and
  the `signal: uint8` wire byte route to the right table provider
  (`LogsTable` vs `MetricsTable`) and read back the right rows.
- The `all_fingerprints` empty-matcher fallback on `BlockMeta` (the one
  non-trivial new field D-032 needed) drives correctly for a signal with
  no per-series type metadata.

Two things were *not* required by v0.4 but landed during it, triggered by
running the live saturation harness against the new logs path: the live
stats endpoint (`/stats.json` + bottleneck classifier) and a chain of
ingest-throughput work — lock-free WAL segment release, an 8-way sharded
ingest pipeline, a contiguous-sort metrics encode, decode-out-of-lock +
column merge, and tunable/adaptive block compression (`--compression
dense|fast|auto`). These are performance, not signal scope; they're
captured in their own commits, not gated on this seal.

Deferred from v0.4 to later, unchanged:

- **Body-substring search** for logs — deferred to v0.7. v0.4 logs query is
  label-predicate + time-range only. *(Shipped in v0.7 as a bloom skip path,
  not a tantivy index — see D-035.)*
- **Profiles payload schema** (native pprof vs. denormalised
  one-row-per-sample) — see Deferred / open; decide as v0.5/v0.6 design
  starts.

## D-034: traces + profiles query verticals (v0.5 / v0.6) — complete

**Date:** 2026-05-29
**Status:** accepted

scry stored all four signals before it could query all four. Traces and
profiles ingest + block storage landed ahead of their query paths (the
traces/profiles `BlockBuilder`s + the `scry-gateway` foreign-protocol
push front-end shipped as unnumbered milestones between D-033 and here),
so for a window the catalog held traces/profiles blocks that no query
table could read — `query_service.rs` accepted only `Signal::Metrics |
Signal::Logs`, and `scry-query --signal` rejected the rest. This decision
lights up the **query** side for both and renumbers the roadmap to a
storage-then-query split: **v0.5 = traces query, v0.6 = profiles
retrieval query.**

**Scope, traces (v0.5):**

- `SELECT *` round-trip + the shared `(matchers, ts_min, ts_max)` `Query`
  preselect, plus a dedicated **`--trace-id <hex>`** by-id lookup (a new
  `trace_id: Option<[u8;16]>` field on `Query` and a `trace_id` bytes
  field on the wire `QueryRequest`; empty = absent, same sentinel as
  `sql`). The block is sorted by `trace_id`, so the by-id equality on the
  `FixedSizeBinary(16)` column prunes via row-group min/max stats.
- Promoted resource columns (`service.name`, `service.namespace`,
  `deployment.environment[.name]`) are first-class `--matcher` targets;
  any other matcher key is **rejected up front** (pointing the user at
  `--sql`) rather than silently ignored, which would over-return rows.

**Scope, profiles (v0.6):** retrieval only — select profile rows by time
(and, via `--sql`, by label against the `labels` Map) and stream them
back including the raw pprof `data` blob, loss-free like logs. Label
matchers are rejected up front for the same reason as traces' unknown
keys.

**No postings for either.** Unlike metrics/logs, traces/profiles blocks
carry no postings sidecar (`has_postings = false`). Their query modules
(`crates/query/src/{traces,profiles}.rs`, mirroring `logs.rs`) skip the
postings resolve entirely; matcher / trace-id / time filters become
DataFusion **row-filter predicates** pushed into `ParquetSource`
(`with_predicate` + `with_pushdown_filters(true)`), and row-group
statistics do the pruning. The query-side schemas reuse the block
writers' `main_schema()` verbatim, so the registered table type can never
drift from the on-disk parquet type.

**Seal.** `scripts/smoke.sh` now runs its query round-trip leg for
`SIGNAL=traces` and `SIGNAL=profiles` (and all four under `SIGNAL=all`):
reconcile a fresh catalog, drive `scry-query --signal <sig>`, assert the
scanned row count equals the sink-accepted count. For traces it
additionally picks the densest `trace_id` from the landed block (hex via
DataFusion's `encode()`) and asserts a `--trace-id` lookup returns
exactly that trace's spans — proving the predicate prunes, not just that
the count happens to match. The remote (`scry-queryd`) path is covered by
`crates/server/tests/query_e2e.rs::traces_round_trip`, which proves the
`Signal::Traces` daemon dispatch and that `trace_id` survives the
binschema wire round-trip into a server-side prune.

**Profiles payload schema — decided.** The "native pprof vs. denormalised
one-row-per-sample" open question (below, from v0.4) is resolved in favour
of **one row per profile blob with the pprof carried as an opaque
`Binary` column** (`ts_unix_nano`, `duration_nano`, `labels` Map,
`format` u8, `data` Binary). Rationale: nothing in scry parses pprof yet,
and retrieval round-trips the blob untouched.

**Flamegraph aggregation — deferred.** Parsing pprof and merging stacks
over a time range into a flame-tree is explicitly *not* in v0.6. Grafana's
flamegraph panel renders *pre-aggregated* data — the Pyroscope/Phlare
backend parses pprof and merges stacks server-side; the UI never parses
raw pprof. So aggregation is backend/query work, but with no UI and no
query language consuming it yet, retrieval is the useful step. Aggregation
becomes its own stage (needs a pprof-parser dep + the nested-set output a
UI consumes) when something consumes it.

Deferred elsewhere: full-text/body-substring logs search (v0.7 — shipped, see
D-035), PromQL (demoted; own-UI removes the Grafana-compat driver),
compaction/retention (v0.8).

## D-035: full-text log search — scan path + inline bloom skip sidecar (v0.7)

**Date:** 2026-05-29
**Status:** accepted

**Supersedes the "tantivy-backed phase" framing in D-032 point 2 and
D-033's deferred-items list.** Those decisions deferred body-substring
search to "a tantivy-backed phase later." When v0.7 came up for design we
revisited that and chose a different shape — driven by a roadmap change:
PromQL was the original v0.7, justified by Grafana compatibility, but scry
now has its own query UI (the `desktop/` Tauri app), which removes the
Grafana-compat driver. Full-text log search ("grep the bodies") is the more
valuable logs operation, so it takes v0.7 and PromQL is demoted.

The mechanism is **not** a stored inverted index (tantivy / Elasticsearch
style). It is a **scan path accelerated by a per-block bloom skip
sidecar**:

- **Storage.** Bodies stay where they already are — the `body` Utf8 column
  in the logs main parquet (zstd, Loki-class ~0.1× raw). Each block gains
  one extra sidecar object, `<uuid>.body.bloom`, alongside the existing
  postings sidecar. The bloom runs ~1–3% of body size at the default 1%
  FPR, so total storage is roughly an order of magnitude below a full
  inverted index.
- **Query.** `body LIKE '%pat%'` was reachable in v0.4 only via hand-written
  `--sql`, as a full column scan. v0.7 adds a first-class surface
  (`Query::body_contains`, the `--grep` CLI flag, a `body_contains` field on
  the wire `QueryRequest` — empty = absent, the same sentinel convention as
  `sql`/`trace_id`). The bloom lets a query *skip whole blocks* that cannot
  contain the term before any parquet (or even postings) I/O, so selective
  searches beat a Loki-classic full-window scan.

**Why a hand-rolled bloom, not a crate or tantivy.**

1. **One-sided error is exactly the correctness property we need.** A bloom
   yields false positives (a wasted scan) but **never** false negatives
   (never drops a block that matches). The exact `contains(body, pat)`
   predicate stays in the scan as the backstop on survivors, so the bloom
   is a pure accelerator — a stale, missing, or unparseable bloom can only
   cost a scan, never a result. Every failure path (`body_bloom`,
   `bloom_cache`) resolves to "keep the block."
2. **Built offline from the complete body set at seal time.** Because the
   block builder sees every body, it sizes each filter optimally for its
   exact distinct-gram count (`m = -n·ln p / (ln2)²`, `k = (m/n)·ln2`,
   `p = 1%`) and the bloom exists the instant the block does — no
   recent-data gap like Loki's out-of-band bloom-compactor, and no extra
   service (fits the single-binary thesis).
3. **`unsafe_code = "forbid"` workspace-wide** rules out most bloom crates;
   we already depend on `twox-hash` for fingerprinting. The filter is ~60
   lines (`crates/block/src/bloom.rs`): byte-trigram tokenization,
   Kirsch–Mitzenmacher double-hashing (`g_i = h1 + i·h2` from two seeded
   xxh3 hashes), a `magic|version|ngram|k|m_bits|bitset` serialised form.

**Tokenization: byte-level trigrams (n=3), case-sensitive** — chosen to
match the `contains` predicate's semantics exactly, which is what makes the
superset guarantee hold: if a pattern P (len ≥ 3) occurs in a body, every
trigram window of P was inserted, so all of P's grams test present and the
block is kept. Patterns shorter than 3 bytes can't be trigrammed →
`contains_pattern` returns `true` (the bloom can't rule them out) → those
blocks are scanned. Regex / case-insensitive / phrase search are future
work.

**Granularity: per-block**, mirroring the postings sidecar and slotting into
the same `build_logs_table_from_candidates` prune loop. Per-row-group
granularity is a documented future refinement.

**Catalog.** `BlockMeta` gained `has_body_bloom: bool` and
`body_bloom_size_bytes: Option<u64>` (mirroring `has_postings` /
`postings_size_bytes`); the catalog table gained the matching columns. Fresh
schema, no migration (Rule #8 — smoke wipes the catalog).

**Caching.** A `BloomCache` (`crates/query/src/bloom_cache.rs`) mirrors the
postings cache — byte-budgeted, LRU, single-flight, keyed by block UUID —
but with its own budget (`SCRY_BLOOM_CACHE_BYTES`, default 64 MiB) so cheap
blooms aren't evicted by larger postings. The daemon constructs one at
startup beside the postings cache and logs per-query hit/miss deltas in
`scan_complete`; the one-shot CLI passes `None` and takes the direct fetch
path. A cached `None` records "this block has no usable bloom" so a
known-bad block isn't re-fetched every query.

**Seal.** `scripts/smoke.sh` (logs / both / all) now asserts (a) every logs
block carries a `body.bloom` sidecar (`has_body_bloom = 1`,
`body_bloom_size_bytes > 0`); (b) a `--grep <token>` query returns *exactly*
the same row count as an un-accelerated `body LIKE '%token%'` scan — the
"skip never loses a match" equivalence on a real bucket; (c) a `--grep` of
an absent token prunes to zero rows. The no-false-negative property is
additionally proven exhaustively over random bodies in
`crates/block/src/bloom.rs` unit tests and the skip≡scan equivalence in
`crates/query/tests/logs_end_to_end.rs::logs_body_contains_bloom_skip_equals_scan`.

**Future storage optimization, noted:** binary-fuse filters would be ~30%
smaller for immutable data; deferred, the classic bloom is simpler and the
sidecar bytes are already a small fraction of the block.

## D-036: size-tiered compaction — single-instance, DataFusion sort-merge (v0.8)

**Date:** 2026-05-30
**Status:** accepted

Every milestone through v0.7 only ever *writes* immutable blocks; nothing
reorganises them. A busy writer fans out into many small L0 blocks (one per
WAL rotation per shard), and every query opens all of them — object count
and per-block metadata load grow without bound. v0.8 closes that loop with
**compaction**: merge many small same-level blocks into fewer larger ones.
The full design (tiered levels L0→L3, size-tiered policy, per-merge
sequence, per-partition object-store leases) already lives in
`ARCHITECTURE.md § Compaction`; this decision records the **single-instance
subset** that shipped, and what was deliberately deferred.

**Scope shipped.** Size-tiered merge + the supersede→grace→delete lifecycle,
as a standalone `scry-compact` tool (engine = `scry-compact` lib, CLI =
`--once` one pass / `--watch` loop). **Not** shipped: the distributed
object-store lease (v0.8 is one compactor), retention/TTL reaping, and the
in-`scry-ingestd` background loop — all deferred, listed below.

**Decisions, with rationale:**

- **Standalone tool first.** The engine is a library so the eventual
  in-daemon background loop reuses it verbatim; the CLI mirrors `scry-list`
  (construct store + catalog from `SCRY_OBJSTORE_*`, run to completion).
  Shipping the tool first keeps compaction operable and testable in
  isolation before it's wired into the long-running daemon.

- **Size-tiered, fanout 8, L3 ceiling.** A `(signal, date, level)`
  partition with ≥ `fanout` blocks merges its `fanout` *smallest* into one
  block at `level + 1`. Size-tiered (vs levelled) bounds write
  amplification to ~`log_fanout(total)` rewrites per byte, which suits
  append-mostly observability data. `max_level` (default 3) stops merging
  past L3, where individual parquet files get large enough that
  random-access reads suffer. One merge per partition per pass keeps a pass
  bounded and predictable; repeated passes (or `--watch`) converge a
  backlog.

- **DataFusion sort-merge, not hand-rolled k-way.** The merged main parquet
  is the K inputs read back via `read_parquet` and re-sorted by the
  signal's sort key with `ORDER BY`, streamed (and spilled to disk under
  memory pressure) into a new parquet. This reuses the query crate's
  object-store registration pattern and never holds the whole partition in
  RAM. A hand-rolled streaming k-way merge over the already-sorted inputs
  would cut the re-sort cost, but DataFusion's spilling sort is correct and
  bounded today; the k-way merge is a noted later optimisation. **One
  subtlety:** the merge `SessionContext` sets
  `parquet.schema_force_view_types = false`, because DataFusion otherwise
  reads string columns back as `Utf8View` — which both breaks the
  body-column downcast (for the bloom rebuild) and would make the merged
  block's schema differ from a freshly-written L0 block. The merged block
  must be schema-identical to an L0 block so every reader treats it the
  same.

- **`superseded_by IS NULL` is the safety mechanism.** The single change
  that makes compaction safe for *all four* query signals is `list_blocks`
  filtering `WHERE deleted_at IS NULL AND superseded_by IS NULL`. The
  per-merge lifecycle is: merge → `insert_block(merged)` →
  `mark_superseded(inputs, merged)` → (grace) → delete input objects →
  `delete_blocks(inputs)`. The instant step 3 commits, queries read the
  merged block and skip the inputs — *atomically, before any object is
  deleted*. That's what lets the grace period default to **0** for the
  single-instance tool: there's no window where both the merged block and
  its inputs are live. A non-zero `--grace` only matters once a *concurrent
  reader* might be mid-scan against an input it listed before the
  supersede; that's the multi-instance follow-up's concern.

- **`level` promoted into the `meta.json` sidecar.** The catalog is derived
  state (reconcilable from the bucket), so the block's level must live in
  the sidecar or a reconcile would demote every compacted block back to
  L0. `BlockMeta` gains `#[serde(default)] level: u32` (old sidecars
  deserialise to 0); `insert_block` writes it; `row_to_entry` reads it
  back. `series_types` and `all_fingerprints` remain sidecar-only (not
  promoted to catalog columns) — the merge rebuilds them and the reconcile
  path already ignores them.

- **Sidecars rebuilt, not copied.** postings (metrics/logs) are the union
  of the inputs' postings, re-sorted/deduped via the shared
  `scry_block::postings` encode/decode (lifted out of the logs/metrics
  builders so all three call one implementation); the logs body bloom is
  re-accumulated from the merged body column during the same streaming pass
  via a new streaming `BodyBloomBuilder` (memory stays bounded to the
  distinct-gram set); metrics `series_types` is unioned from the inputs'
  `meta.json`. `all_fingerprints` is the distinct fingerprint set
  accumulated during the stream. traces/profiles carry no sidecars.

**Why a stale-lease double-merge is harmless (forward-compatibility).**
Blocks are immutable and content-addressed under a compactor `writer_id`,
and meta.json is uploaded *last* (the "block exists" signal for reconcile).
If a merge dies partway, the worst case is an orphaned merged block the next
pass treats as just another input at its level — correctness is never at
risk. This is exactly the property the multi-instance lease relies on, so
v0.8's single-instance engine is already forward-compatible with it.

**Seal.** `scripts/smoke.sh` (logs / both / all) gained a compaction leg:
after ingest seals several small L0 logs blocks (forced via
`--block-max-rows`), `scry-compact --once --fanout 2 --grace 0` runs, then a
*fresh reconcile from the bucket* asserts (a) the live logs block count
dropped, (b) ≥1 logs block now sits at level ≥1, (c) the reconciled catalog
still queries back exactly the pre-compaction logs row count — which, because
reconcile rebuilds purely from bucket sidecars, proves *both* losslessness
and that the superseded inputs' objects were deleted, and (d) `--grep` still
matches every body through the rebuilt bloom. The merge correctness is
additionally proven in `crates/compact/tests/compaction_e2e.rs` (lossless +
sorted + postings union + `series_types` union + catalog transitions +
input-object reaping, for both logs and metrics), with `postings`
encode↔decode roundtrip and streaming-bloom≡one-shot equivalence in
`crates/block` unit tests.

**Deferred (tracked below):** the per-partition object-store compaction
lease (multi-instance), retention/TTL, the in-`scry-ingestd` background
loop, the hand-rolled k-way streaming merge, and per-row-group sidecar
granularity.

## D-037: per-signal TTL retention — single-instance, dry-run by default (v0.8)

**Date:** 2026-05-30
**Status:** accepted

The second half of v0.8. Compaction (D-036) reorganises blocks; retention
reclaims storage by *deleting* them. Without it a scry deployment grows
without bound — every block ever written stays in the bucket and the catalog
forever. Retention drops blocks whose data is entirely past a per-signal age
limit, removing their objects and catalog rows.

Retention is the **delete tail of compaction's lifecycle with no merge**, so
it reuses compaction's machinery rather than reinventing it: the
`delete_block_objects` helper (lifted into `scry-block` so both tools share
it — see below), `Catalog::delete_blocks`, the standalone-tool skeleton
(`scry-retention`, lib + bin, `--once` / `--watch`), and the same
object-before-row deletion ordering (the catalog is derived state). Like
compaction it is **single-instance** — no distributed lease; that's the
shared multi-instance follow-up.

**Decisions, with rationale:**

- **Opt-in, no implicit deletion.** A signal is reaped only if a TTL is
  configured for it — per-signal (`--ttl-logs 30d`) or via a blanket
  `--ttl 30d` default applying to all signals. A signal with no TTL is
  **never** touched (`RetentionConfig::ttl_for` returns `None` → skipped).
  The CLI refuses a run with *no* TTL configured at all. Retention deletes
  data irreversibly; the default posture is "touch nothing unless explicitly
  told which signal and how old."

- **Dry-run by default; `--apply` to delete.** A normal run only *previews*:
  it lists the candidate blocks and bytes and touches nothing. Deletion
  requires `--apply`. This makes the dangerous path opt-in and the safe path
  the default — the opposite of compaction (which has no destructive-preview
  concern because its deletes are always of just-superseded inputs).

- **Whole-block `ts_max` criterion.** A block is reaped only when its
  *newest* record (`ts_max_unix_nano`) is strictly older than `now - ttl`.
  Using `ts_max` (not `ts_min` or the block date) guarantees a block still
  holding any in-window data is never dropped — retention only ever removes
  blocks that are entirely expired. We do **not** rewrite a block that
  straddles the TTL to drop its old prefix (partial-block rewriting is
  deferred); whole-block granularity is simpler and, with compaction keeping
  blocks time-bounded, wastes little.

- **`now` is injected, not read internally.** `plan_reaping` and
  `retain_once` take `now_unix_nano` as a parameter; only `main.rs` reads
  `SystemTime::now()`. This makes the age policy a pure, deterministic
  function — the e2e test plants a 90-day-old and a 1-hour-old block against
  a fixed `now` and asserts the exact cutoff, no clock games.

- **`deleted_at` gives a correct grace window.** Because `list_blocks` already
  filters `deleted_at IS NULL` (added for compaction in D-036), a new
  `Catalog::mark_deleted` lets the reaper soft-delete expired blocks — queries
  stop listing them *immediately* — then wait the configured `--grace` before
  removing objects, so a concurrent reader that already listed a block keeps
  its objects for the grace window. At the single-instance default
  (`--grace 0`) this step is skipped and objects+rows go straight away.

**Shared helper move.** `delete_block_objects` (delete a block's parquet +
meta.json + flagged sidecars, NotFound-tolerant) was lifted from
`scry-compact`'s `merge.rs` into `scry-block` (next to `block_path`, the
block-layout knowledge it depends on). Both compaction and retention now call
`scry_block::delete_block_objects`; retention does not depend on the
compaction crate.

**Seal.** `scripts/smoke.sh` (logs / both / all) gained a retention leg after
the compaction leg: against the real bucket, (a) a **dry-run** with
`--ttl-logs 0s` reports every logs block as a candidate yet a fresh reconcile
shows the live count unchanged (dry-run is inert), and (b) `--apply` with
`--ttl-logs 0s` reaps every logs block while the **metrics blocks are
untouched** (signal-scoping, end-to-end). The precise age cutoff, dry-run
inertness, signal-scoping, lossless survival of the in-window block, and
idempotent re-runs are all proven in `crates/retention/tests/retention_e2e.rs`
with controlled timestamps; the policy edges (boundary `<`, opt-in,
override-beats-default, saturating huge TTL) in `policy.rs` unit tests, and
`mark_deleted` query-skip in `crates/catalog`.

**Deferred (shared with compaction / tracked below):** the multi-instance
coordination lease, the in-`scry-ingestd` background reaper loop, partial
(row-prefix) block rewriting at the TTL boundary, and size/quota-based
eviction (retention here is purely age-based).

## D-038: Valkey lease for multi-instance maintenance (supersedes D-013) (v0.9)

**Date:** 2026-05-31
**Status:** accepted

v0.9 makes scry multi-instance: 1–N identical `full`-mode instances share one
bucket; every instance ingests, queries, converges its catalog, and contends
for the leases that gate destructive maintenance. This decision is the
**exclusion** half (coordination); D-039 is the **convergence** half.

**Why a lease at all (correctness, not efficiency).** Blocks are addressed by
random **UUID v7, not by content hash** (D-029). So if two instances compact
the same `(signal, date, level)` partition, they each produce a *distinct*
merged block containing the same rows — both live, both queried, rows
double-counted; a later merge of the two **unions** them (no dedupe). D-013's
"benign double-merge" only holds if exactly one committer wins. So single-winner
maintenance is a hard correctness requirement.

**Why Valkey, not the D-013 object-store lease.** D-013's `If-None-Match: *`
lease is **not implementable on Garage**: Garage has no consensus and silently
ignores conditional-write preconditions between concurrent writers (the same
reason D-029 dropped `put_if_absent`). Rather than restrict scry to S3-class
backends, v0.9 coordinates through **Valkey**, which the architecture already
assumes for the (future) service registry.

**Mechanism (`scry-valkey`):**
- **Acquire** = `SET key holder NX PX ttl` (server-side expiry → client clock
  skew is irrelevant). Key granularity: one lease per `(signal, date,
  input_level)` for compaction (independent partitions proceed in parallel
  across instances) and **one global lease** `scry/lease/retention` for
  retention (a pass spans all signals and is cheap).
- **Renew** every `ttl/3` via a Lua compare-and-`PEXPIRE` (extends only if the
  value is still our holder id). The guard **latches its fence invalid on the
  first renewal failure**, strictly before server-side expiry, so the old
  holder stops acting before any peer can acquire.
- **Release** = Lua compare-and-`DEL` (delete only if still ours), on drop.

**Engines stay Valkey-agnostic.** `scry-compact`/`scry-retention` take only a
`&dyn Fence` (`scry-block`) and a `&dyn BlockEventSink`; `scry-cluster`'s
maintenance loop is generic over a `LeaseProvider` trait (static dispatch →
native `async fn` in trait, no `async-trait`). Production injects
`ValkeyLeaseProvider`; tests inject an in-process `LocalLeaseProvider`.

**The load-bearing invariants (why single-winner actually holds):**
- **Commit-point fence.** `merge_blocks` uploads `main → [postings] → [bloom]`
  then **`meta.json` last**; reconcile keys on `meta.json`, so a block with no
  `meta.json` is invisible. The fence is checked immediately before the
  `meta.json` PUT — a lease lost during the minutes-long merge ⇒ no `meta.json`,
  no catalog row, inputs untouched; only uncommitted data objects leak
  (reclaimable by a future full-walk/orphan-GC). This is what makes a
  *concurrent* double-merge benign.
- **grace=0 closes the *sequential* re-merge window.** The lease serialises
  concurrent merges, but `compact_partition` plans from a catalog snapshot and
  does **not** re-validate inputs after acquiring the lease. With the
  lease-default grace (600 s) a stale peer that planned the same inputs could,
  in the window after the winner committed but before it deleted the inputs,
  re-merge them into a second live block. The smoke runs **`--compact-grace 0`**:
  the winner deletes inputs immediately, so a stale peer's re-merge **404s on
  the GET and aborts** before its own `meta.json` commit — no duplicate. (With
  grace > 0 the brief duplicate is the documented D-039 soft edge, bounded by
  convergence latency.)
- **No-lease ⇒ pause, never race.** `try_acquire` returning `Err` (backend
  unreachable) or `Ok(None)` (peer holds it) skips that unit; with Valkey
  absent maintenance pauses entirely (unless `--allow-unfenced-maintenance`,
  which asserts sole ownership via `LocalLeaseProvider`). The standalone
  `scry-compact`/`scry-retention` CLIs are unchanged (sole-instance, unfenced).

**Seal.** `MULTI=1 scripts/smoke.sh` (→ `scripts/smoke-multi.sh`) runs two
`scry-ingestd` on one bucket + one Valkey: it asserts each instance's catalog
converges to the union of both instances' rows, that the live row total is
**exactly** the ingested total after both run compaction (a duplicate merge
would inflate it) with a block reaching level ≥ 1 (compaction actually ran), and
that coordinated retention reaps every block to zero with no panic / pass
failure — only the lease winner logs actual deletes. Lease mutual exclusion,
renew-past-TTL, and fence-on-release are also covered by gated
`#[ignore]` integration tests against a real Valkey (`crates/valkey/tests`).

## D-039: Three-tier catalog convergence (pub/sub + cursor poll + full walk) (v0.9)

**Date:** 2026-05-31
**Status:** accepted

The convergence half of multi-instance (D-038 is exclusion). Each instance has
its own SQLite catalog (derived state); a block one instance writes, compacts,
or reaps must become visible to peers, and a peer-deleted block must not be a
404 landmine. **The bucket is the single source of truth; Valkey is only ever a
hint.** Three tiers, all converging on the bucket:

1. **pub/sub apply (low-latency hint).** On every successful upload / supersede
   / delete, the instance publishes a `BlockEvent` (`Created{meta}` |
   `Superseded{inputs,by,by_meta}` | `Deleted{signal,uuids}`) on
   `scry/blocks/<signal>`. Peers apply each idempotently: `Created`→`insert_block`
   (`INSERT OR IGNORE`); `Superseded`→insert `by_meta` (satisfy the FK) then
   `mark_superseded`; `Deleted`→`delete_blocks`. `Created` is byte-identical to
   a `meta.json`, so the event reuses `BlockMeta`'s serde.
2. **incremental cursor poll (dropped-event backstop).** Per `(signal, writer,
   date)` the catalog keeps a high-water `poll_cursors` row; the poller lists
   only objects after that UUID (`list_with_offset`) and upserts them. Cursors
   are a high-water mark (monotonic UPSERT) — they never regress on delete.
3. **full walk (ultimate backstop).** Periodic `reconcile_from_bucket`
   discovers brand-new prefixes no event or cursor has seen.

**Idempotency is the whole game.** Events may duplicate, reorder, or
self-deliver; every apply is a no-op when already applied. The one **soft
edge**: a `Superseded` arriving before its inputs' `Created` causes a brief
double-count (both inputs and the merged block momentarily live) — bounded by
the poll interval / full walk, and harmless given queries union live blocks.
(D-038's grace=0 turns the analogous *compaction* re-merge hazard from a
permanent duplicate into a clean 404-abort.)

**404-tolerant reads close the loop.** A peer can delete a block this instance
still lists. The `EvictOnNotFound` object-store decorator (`scry-query`) catches
a `NotFound` during a scan read, parses the block UUID from the path, and
records it; the driver (`scry-query` CLI and `scry-queryd`'s `QueryService`)
`delete_blocks` the stale row and does **one** transparent re-plan. For
metrics/logs the 404 surfaces at postings-sidecar fetch (before any wire
output) → fully transparent re-plan; for traces/profiles it can surface
mid-scan (schema already sent) → the row is evicted to heal the next query and a
clean `StreamError` is returned.

**Topology.** `scry-ingestd` runs all three tiers + the D-038 maintenance loop
(`--mode full`), or convergence only (`ingest-only` / `query-only` / when no
lease is available). `scry-queryd` is query-only: it runs the three convergence
tiers but never leases (no destructive work). The `writer_uuid` is persisted to
`<wal_dir>/writer_id` so a restart reuses its prefix rather than bloating the
per-`(signal, writer, date)` poll fan-out with a fresh UUID each restart.

## Deferred / open

These are not decisions yet; they're flagged for "we'll decide when the
constraint shows up":

- **Profiles flamegraph aggregation.** pprof parse + stack-merge over a
  time range → the flame-tree shape a UI consumes. Deferred from v0.6 by
  D-034 (Grafana renders pre-aggregated data; nothing consumes it yet).
  Becomes its own stage when a UI / query language lands.
- **Multi-instance compaction lease.** ✅ **Done in v0.9 (D-038).** A Valkey
  per-`(signal, date, level)` lease (not the unbuildable-on-Garage D-013
  object-store lease) gives single-winner compaction; the commit-point fence +
  grace=0 delete keep it correct under UUID (not content) addressing.
- **Retention multi-instance.** ✅ **Done in v0.9 (D-038):** a single global
  Valkey retention lease coordinates reaping across instances. Still deferred:
  rewriting a block that *straddles* the TTL to drop only its expired row prefix
  (today retention only drops wholly-expired blocks).
- **In-`scry-ingestd` background compaction/retention loop.** ✅ **Done in v0.9
  (D-038/D-039):** `scry-ingestd --mode full` runs the lease-guarded
  compaction + retention loops as background tasks sharing the pipeline's store
  + catalog. The standalone `scry-compact`/`scry-retention` CLIs remain for
  single-instance / ad-hoc use.
- **Orphan-object GC.** A lost lease mid-merge (or a crash after data-object
  upload but before the `meta.json` commit) leaks uncommitted parquet/sidecar
  objects with no `meta.json` and no catalog row. They are invisible to queries
  and reconcile; a full-walk-based reaper that deletes data objects with no
  sibling `meta.json` past some age would reclaim them. Deferred (the leak is
  bounded and harmless).
- **Hand-rolled k-way streaming merge.** D-036 merges via a DataFusion
  `ORDER BY` re-sort, which is correct and memory-bounded but re-sorts
  already-sorted inputs. A streaming k-way merge over the sorted inputs
  would cut that cost; do it if merge CPU shows up in profiles.
- **High-cardinality metrics index.** Per-block label-fingerprint blooms
  may suffice; if not, we add a sketch (HLL? cuckoo filter?) — decide
  based on measurement during v0.5.
- **TLS / auth model.** Probably mTLS with a CA file. Mini-design
  before v0.2 ships outside Bart's homelab.
- **Read-replica catalog coherence.** Polling `ListObjects` is fine
  initially; revisit if query staleness becomes a complaint.
- **License.** TBD. Probably MIT or Apache-2.0; pick before any
  external contributor shows up.

---

## D-040: Browser query UI as a password-gated byte-pipe (`scry-webui`) (v1.0-wip)

**Context.** The query UI already exists as a Tauri + SolidJS desktop app
(`desktop/`). Crucially, its architecture puts the *entire* query wire protocol
(binschema framing, the `QueryFrame` union, Arrow IPC decode) in TypeScript; the
Tauri Rust shell is a **dumb byte-pipe** — its one `run_query` command opens a
TCP socket to `scry-queryd`, writes the framed request, reads to EOF, and returns
the bytes. The transport is already abstracted behind a `Transport` interface.
Bart wanted to reach the UI from a phone / other machines without running the
desktop binary there: serve it as a web service on his home machine that
connects to `scry-queryd` exactly the way the desktop app does, gated by a simple
password.

**Decision.** Add a standalone **`scry-webui`** crate (axum 0.8) that (a) serves
the SolidJS bundle (embedded via `rust-embed`, so the binary is self-contained)
and (b) exposes `POST /api/query` as the same dumb byte-pipe the Tauri shell
implements — dial the server's *own configured* `--queryd`, write the framed
request, read to EOF, return the bytes. Auth is a single shared password
(`SCRY_WEBUI_PASSWORD`, never argv) → a signed session cookie (`axum-extra`
`SignedCookieJar`, key HKDF-derived from the password so sessions survive a
restart and a password change invalidates them). The frontend stays
**dual-mode**: the `Transport` interface gets two impls in separate modules —
`TauriTransport` (native socket, desktop) and `HttpTransport` (`fetch` to
`/api/query`, browser) — selected at runtime by `isTauri()` via a dynamic
`import()`, so the browser bundle never pulls in `@tauri-apps`.

**Rationale.**
- *Byte-pipe symmetry.* The server having zero protocol knowledge means the web
  path can never drift from the desktop path — both ship the identical framed
  bytes the TypeScript client produces. New signals / protocol changes need no
  server change.
- *SSRF-safe.* The browser cannot choose the upstream: the server ignores any
  client-supplied address and always dials its configured `--queryd`. The
  desktop `addr` field is hidden in the browser shell.
- *Single binary.* `rust-embed` over `ServeDir` keeps deployment to one file
  (run `bun run build` in `desktop/` before `cargo build -p scry-webui`; debug
  builds read `desktop/dist` from disk for fast frontend iteration).
- *Cookie hardening level matches the threat.* `HttpOnly` + `SameSite=Strict`,
  but **not `Secure`** — the server speaks plain HTTP on a home LAN and a
  `Secure` cookie is dropped over `http://`. Put it behind a TLS reverse proxy
  to harden, then flip `Secure`.
- *502 on upstream failure* so a down `scry-queryd` is a clear gateway error,
  not a hung request or a 500.

**Alternatives weighed.** Folding the server into `scry-queryd` (rejected: keeps
the query daemon free of HTML/auth/static-asset concerns, and the UI host is a
home-machine deployment, not part of the clustered query tier — it is
deliberately **not** in the Docker image / k8s manifests). A WebSocket bridge
(unnecessary — the one-request-per-connection lifecycle is a clean
request/response, so plain `POST` suffices). Browser-only (rejected: Bart wanted
to keep the desktop app too).

**Status.** Implemented. Sealed by `scripts/smoke-webui.sh` (builds the bundle +
release binary, stands up a stub upstream, asserts the SPA serves and the
login → `/api/me` → `/api/query` relay → logout surface) plus the `scry-webui`
Rust integration tests (`tests/auth.rs`, `tests/query.rs`). The real protocol
round-trip is covered by `scripts/smoke.sh`'s per-signal query legs.

---

## D-041: `scry-gateway` becomes a fan-out hub (native + foreign in; scry / Loki / OpenSearch out, all opt-in)

**Context.** `scry-gateway` was a one-way, single-destination foreign-protocol
*terminator*: it accepted OTLP/HTTP traces, Pyroscope profiles, and Prometheus
remote-write over HTTP, mapped each request to a typed `*Batch`, and forwarded it
to **one** upstream scry ingest server over a single shared wire client.
Separately, `scry-agent` tails k8s logs and ships native binschema **straight to a
scry ingest server** — never touching the gateway. Bart wanted to run the agent
against a cluster, point it at the gateway, and have the gateway deliver every
record to the scry backend **and** to Loki and/or OpenSearch in the formats those
systems consume — so an existing Loki/OpenSearch-based stack keeps working while
scry runs alongside.

**Decision.** Generalize the gateway from "terminate → forward to one" into a
**fan-out hub**: decode each inbound into a typed `*Batch`, then tee it
best-effort to *every* configured sink it is compatible with.

- **Native inbound, opt-in.** Add `wire.rs`, a trimmed native binschema listener
  (Hello/HelloAck + Batch/Ping/Goodbye), so the agent (and anything that speaks
  the wire) can point at the gateway. It is enabled only when `--listen-wire` is
  set — no default bind — so existing HTTP-only deployments and the smoke are
  unchanged, and there is no surprise grab of a well-known port. `scry-server`'s
  `Server` is **not** reused: it is hardwired to the WAL+parquet
  `ShardedPipeline` and its dispatch is private, so the ~150-line handshake is
  duplicated deliberately (the gateway is a fan-out, not a store).
- **All in → all out, zero routing config.** Every accepted record goes to every
  compatible sink. No per-source/per-destination rules; "more complicated stuff =
  run two gateways." This keeps the gateway a dumb, predictable tee.
- **Every sink is opt-in; scry is not special.** `--upstream`, `--loki-url`, and
  `--opensearch-url` are all `Option`s with no default; a sink exists only when
  configured, and at least one must be (else a startup `bail!`). A logs-only
  gateway that tees to Loki/OpenSearch needs **no scry server at all** — scry is
  one optional best-effort sink among several, not a mandatory backend. (Earlier
  the scry sink was always built against a default `127.0.0.1:4000` and connected
  eagerly at boot, which wrongly made a scry server a hard dependency of running
  the gateway.)
- **The scry sink connects lazily, like the HTTP sinks.** Its worker connects on
  first use (and reconnects after a drop), so a down/absent scry server at startup
  is a per-item connect error, not a fatal boot error — symmetric with
  Loki/OpenSearch, which connect per request. All three sinks behave uniformly:
  configured ⇒ best-effort delivery that tolerates a down target; unconfigured ⇒
  absent.
- **Logs-only foreign sinks.** Loki and OpenSearch receive **logs only**; metrics,
  traces, and profiles go to the scry sink alone. Loki has no metrics model that
  fits scry's sample shape, and OpenSearch is scoped to logs for now. Sinks carry
  a `SIGNAL_BIT_*` accept-mask; a non-matching signal is skipped at offer time.
- **Best-effort, independent per-sink delivery.** Each sink owns its own bounded
  `mpsc` queue + worker task; `offer` is a non-blocking `try_send` that drops +
  counts on a full/closed queue. A slow or down Loki never blocks scry, the other
  sinks, or the inbound.

**Consequence (accepted).** With best-effort fan-out the inbound **ACKs on
enqueue**, not on downstream confirmation. The scry sink is just another
best-effort sink, so the agent's reconnect/retry no longer provides end-to-end
backpressure *through* the gateway — durability across a downstream outage is
bounded by each sink's in-memory queue depth (`--sink-queue-cap`). This is
inherent in the chosen topology (one inbound → N independent sinks) + semantics
(no head-of-line blocking). A synchronous-scry variant (ACK only after scry
accepts, Loki/OpenSearch still best-effort) is a small change if the trade-off
ever bites; not built now. There is no on-disk spool (v0 was already best-effort
with no spool).

**Rationale.**
- *Typed `*Batch` is the natural seam.* Every inbound already decodes to a typed
  batch; fan-out is "offer that batch to N sinks" — the mappers (`map_traces`,
  `map_remote_write`, `to_push_request`, `to_bulk_ndjson`) stay pure and
  unit-tested, the workers are thin shells.
- *Independent queues over a shared one.* A shared queue would let the slowest
  sink stall the others; per-sink queues isolate failure and make "drop + count"
  a local decision.
- *Arc the payload once.* `Fanout` wraps the batch in an `Arc` so fanning to N
  sinks is N refcount bumps, not N deep copies.
- *Opt-in native listener.* Defaulting it on would unconditionally bind a port,
  risking a startup failure / smoke flake for zero benefit to HTTP-only users.
  Opt-in is the least-surprise, no-regression choice.
- *Lazy scry connect over eager.* An eager connect made the gateway unable to
  boot without a reachable scry server even when scry wasn't a configured target.
  Lazy connect keeps the gateway's start independent of any one sink's
  availability — the whole point of a best-effort fan-out.

**Loki/OpenSearch mapping.** Loki: one stream per scry `LogStream`; label keys
sanitized to Loki's `[a-zA-Z_][a-zA-Z0-9_]*` grammar (illegal chars → `_`, leading
digit gains a `_` prefix); each entry → `[ts_unix_nano_string, body]`, with entry
`attributes` + severity carried as **structured metadata** (the optional 3rd tuple
element, dropped when empty so the push is valid against a Loki without structured
metadata). OpenSearch: `_bulk` NDJSON, one `{"create":{"_index":<index>}}` action
+ doc per entry; `@timestamp` as ISO-8601 millis (auto-detected as a date), plus
`body`, `severity`, stream labels, and entry attributes as fields.

**Status.** Implemented in `crates/gateway` (`sink.rs`, `sink_scry.rs`, `loki.rs`,
`opensearch.rs`, `wire.rs`; `upstream.rs` removed). The pure mappers and a wire
`LogsBatch` encode/decode round-trip are unit-tested; `scripts/smoke-gateway.sh`
(unchanged) still asserts the HTTP inbounds → scry sink path end to end with exact
row counts. Live Loki + OpenSearch delivery is manual out-of-CI verification (the
plan documents the steps). Out of scope: routing/filtering rules, metrics/traces
to Loki/OpenSearch, remote-write-out, on-disk spool, auth to Loki/OpenSearch
beyond a static URL/header cred, and any `scry-server`/wire-protocol change.

## D-042: the OpenSearch sink self-manages per-service rolling data streams

**Context.** The OpenSearch sink (D-041) initially wrote every log to a single
static index named by `--opensearch-index`, and assumed an operator had set up the
index, mappings, and any lifecycle policy out-of-band. Bart's lived experience is
that this is exactly what breaks: *"these things always break for me if I don't
have the service control them — people randomly change them on the cluster."* An
operator widens a rollover threshold, deletes a template, or a dynamic mapping
locks a label field to the wrong type and docs start silently dropping at `_bulk`.
A single shared index also conflates every service's logs into one mapping space,
which is where the type conflicts come from in the first place.

**Decision.** The sink **owns** its lifecycle assets and **routes per service**.

- **`--opensearch-index` is a prefix, not a name.** Each scry `LogStream` is
  written to the data stream `<prefix>-<service>`, where `<service>` is the first
  present, non-empty, sanitized value of `service.name` → `service` → `app` →
  `k8s_app`, else `general`. This covers the OTLP/Pyroscope convention
  (`service.name`) and the scry-agent k8s convention (a pod's `app` label surfaces
  as `k8s_app`); logs with no service identity land in `<prefix>-general` rather
  than being force-fit to a label that isn't there. Segments are lowercased and
  coerced to OpenSearch's index-name grammar (`[a-z0-9_-]`, illegal → `-`,
  collapsed, trimmed).

- **Data streams, not plain indices.** Append-only time-series is exactly the
  data-stream model; the bulk action was already `create` (required by data
  streams). Rollover/date handling is therefore the cluster's job, driven by ISM,
  not a date suffix we compute.

- **Self-management is the default** (`--opensearch-unmanaged` opts out). The sink
  asserts three things and keeps re-asserting them — **at startup, on a schedule
  (`--opensearch-reconcile-interval`, default 5m), and after any write error** —
  *not* boot-only, because the whole point is to correct drift that happens while
  running:
  1. an **ISM rollover policy** `<prefix>-rollover`: a single `hot` state with a
     `rollover` action keyed on `--opensearch-rollover-size` (30gb) +
     `--opensearch-rollover-age` (1d) and **no delete state** (Bart chose
     "size+age rollover, no auto-delete" — deletion stays a human/retention
     decision). It auto-attaches via `ism_template.index_patterns ["<prefix>-*"]`.
  2. an **index template** `<prefix>` (`index_patterns ["<prefix>-*"]`,
     `data_stream: {}`, priority 200) with **explicit mappings**: `@timestamp`
     date, `body` text, `severity` short, and crucially `labels`/`attributes` as
     **`flat_object`** — arbitrary label keys can never explode the mapping or
     conflict on type, the root cause of the silent-drop failure mode.
  3. the per-service **data streams**, created lazily on first sight (tracked in a
     worker-local set, cleared on each reconcile so a wiped cluster re-bootstraps).

**Why these specifics.**
- *`ism_template` auto-attach, never manual attach.* For a data stream the policy
  must match the *data-stream name pattern* and auto-attach; manually attaching to
  the current write index does **not** survive rollover (the new backing index
  comes up unmanaged). Validated live: after rollover the backing indices
  `.ds-<prefix>-<svc>-000001` all report `policy_id = <prefix>-rollover`.
- *ISM policy is read-then-write, the template is idempotent.* OpenSearch refuses
  an unconditional overwrite of an existing ISM policy; correcting drift requires
  GET `_seq_no`/`_primary_term` → conditional PUT. A concurrent-edit `409` is
  non-fatal (next reconcile retries). The index-template PUT just overwrites.
- *Reconcile on error, not just on a timer.* A `_bulk` failure is the strongest
  signal that an asset is wrong *right now*; waiting up to the interval would keep
  dropping logs. The worker sets a `needs_reconcile` flag the loop drains
  immediately.
- *`flat_object` over dynamic/object/nested.* Dynamic mapping is precisely the
  drift vector; `nested` is overkill and query-hostile for free-form labels;
  `flat_object` keeps subfields keyword-searchable by dot-path
  (`labels.service`, `attributes.status`) with one stable mapping entry.

**AWS SigV4 (managed OpenSearch / Serverless).** Amazon OpenSearch Service domains
and OpenSearch Serverless collections reject unsigned requests, so a self-managing
sink that fires management calls (ISM policy, index template, data streams) *and*
bulk writes must sign **all** of them. `--opensearch-aws-sigv4` turns on AWS SigV4
signing via `crates/gateway/src/aws_sign.rs` (`SigV4Signer`, built on `aws-sigv4` +
`aws-config`):
- **Credentials + region from the standard chain.** `aws-config`'s default provider
  resolves env vars, the shared profile, EKS IRSA (web-identity), or EC2/ECS IMDS —
  whichever the deployment offers — so nothing secret ever reaches argv. Region is
  taken from the resolved config or `--opensearch-aws-region`; absence is a startup
  error rather than a silent unsigned request.
- **Signing name is explicit.** `--opensearch-aws-service` selects `es` (managed
  domains) vs `aoss` (Serverless) — they sign under different service names.
- **Sign just before send.** Each request is built as a `reqwest::Request`, a
  `SignableRequest` view (method/url/headers/body) is signed, and the resulting
  `Authorization` + `X-Amz-*` headers are copied back. `host` is derived by aws-sigv4
  from the URL (matching what hyper sends), and `x-amz-content-sha256` is emitted
  (`PayloadChecksumKind::XAmzSha256`) so the body is covered — required by Serverless,
  accepted by managed domains. Credentials are cached in the single-threaded worker
  and refreshed only within 5 min of expiry, so steady-state signing never round-trips
  to IMDS/STS. When the flag is off the sink sends plain requests (self-hosted
  OpenSearch with basic/no auth), so the signing path is zero-cost for non-AWS users.

**Status.** Implemented in `crates/gateway/src/opensearch.rs` (pure
`service_segment`/`data_stream_name`/`to_bulk_ndjson`/`ism_policy_doc`/
`index_template_doc` + the `OpenSearchSink` worker) and wired in `main.rs`
(`--opensearch-unmanaged`/`--opensearch-rollover-size`/`--opensearch-rollover-age`/
`--opensearch-reconcile-interval`). Unit tests cover service-key priority, the
`general` fallback, segment sanitization, per-stream bulk routing, and the
policy/template JSON shapes, plus `aws_sign.rs` (SigV4 header structure for both `es`
and `aoss` signing names). Validated live against `opensearchproject/opensearch:
2.17.1`: per-service data streams auto-created, policy auto-attached to backing
indices, `flat_object` subfields + full-text `body` queryable, and an operator's
hand-edit to the ISM policy (min_size `999gb`, edited description) reverted on the
next reconcile. SigV4 against Amazon OpenSearch is not exercised by the smoke (no AWS
endpoint in CI); the signer is covered by a deterministic header-shape unit test.
Still out of scope: auto-delete/retention in ISM (deliberate), non-log signals to
OpenSearch, and auth beyond a static URL/header cred or AWS SigV4.

## D-043: scry-agent keep-only label allow-list (node-side admission control)

**Context.** `scry-agent` tails every container log file under `/var/log/pods` on
its node and ships all of it over the wire. On a busy node that means forwarding
the world — including chatty system/sidecar containers nobody queries — which
wastes node→upstream bandwidth and upstream ingest/storage. The forwarding work
needed a way to say "only forward these streams." This is the agent counterpart to
the gateway's all-in/all-out stance (D-041): the gateway deliberately has *no*
routing config ("run two gateways"), but the agent is the volume source, so a valve
belongs here.

**Decision.** Add an opt-in, node-side, **keep-only (allow-list)** label filter to
the agent.

- **Where: the agent, at admission.** Confirmed with Bart against the alternatives
  (gateway-side, per-sink). Filtering at the per-node tailer cuts volume at the
  source — the dropped bytes never enter a batch, never go on the wire, never reach
  the upstream. A gateway-side filter would still pay the node→gateway hop; a
  per-sink filter would reintroduce exactly the routing config D-041 rejects.
- **Semantics: keep-only, global, AND-of-matchers.** A stream is forwarded only if
  its labels satisfy **every** configured matcher (logical AND). "Global" because
  the agent has a single upstream — there is nothing to route *between*. Cross-label
  OR is expressed with a regex alternation (`namespace=~"a|b"`); the rarer
  cross-label OR is out of scope (run intent through labels, or a second concern).
- **Opt-in, keep-all default.** No `--keep` flags ⇒ the filter is empty ⇒ everything
  ships, byte-for-byte today's behavior. The feature can never silently start
  dropping logs.
- **Matcher syntax: Prometheus-style + regex.** `key=value` | `key!=value` |
  `key=~regex` | `key!~regex`. scry-query's existing `--matcher` is equality-only and
  returns a bare `(String,String)` for SQL, so it can't carry an op/regex and isn't
  worth generalizing across crates for one consumer — the agent gets its own small
  `filter.rs`. Regex is genuinely needed for an allow-list that "cuts the world"
  (`namespace=~"prod-.*"`), justifying a new `regex` workspace dependency. Regexes
  are **whole-string-anchored** (`^(?:…)$`) exactly like Prometheus, so `=~"prod"`
  does not match `production`. Values may be optionally double-quoted. A label the
  stream doesn't carry is treated as the empty string, so `key=~".+"` means
  "present and non-empty".

**Why these specifics.**
- *Drop before any batch state changes.* The check lives in `ingest()` right after
  `stream::stream_labels` computes `(fingerprint, labels)` and before the ts/byte/
  count updates and the per-fingerprint `LogStream` insert — so a dropped line
  leaves zero trace and cannot be shipped.
- *Cache the verdict per fingerprint.* A stream's label set (and thus its
  fingerprint) is stable, so the keep/drop decision is computed once per distinct
  stream and cached in a `HashMap<u64, bool>` that lives across flushes (not in
  `Pending`, which is drained each flush). This keeps the possibly-regex matchers
  off the per-line hot path — steady state is one map lookup, zero allocations per
  line. (`stream_labels` already allocates per line today; that pre-existing cost is
  not addressed here.)
- *Observability.* A dropped-line counter is emitted on a sparse `info!` (every
  100k) and in the final `agent done` log, so operators can confirm the filter is
  cutting volume without flooding the log.

**Consequence.** A dropped log is gone — there is no node-side spool and the agent
is already at-most-once (a batch in flight during a crash is lost). The allow-list
is a deliberate, operator-chosen data-loss valve, not a buffering or sampling
mechanism.

**Status.** Implemented in `crates/agent/src/filter.rs` (`MatchOp` / `Matcher` /
`LabelFilter` with `parse`/`keeps`, all pure + unit-tested) and wired into
`crates/agent/src/main.rs` (`--keep`, the keep-cache, and the drop in `ingest`).
Unit tests cover the four operators, quoted values, malformed/invalid-regex specs,
AND semantics, absent-label-as-empty, whole-string anchoring, and the empty-filter
keep-all case. Out of scope: per-sink/gateway-side filtering, deny-list semantics,
dynamic/hot-reloaded matchers, and dropping non-log signals.

## D-044: gateway Mimir metrics sink + custom CA for the HTTP sinks

**Context.** The fan-out hub (D-041) grew a gap as producers started pushing
metrics through it: the gateway *accepts* metrics (the Prometheus remote-write
inbound and the native wire) but the only sink that consumes them is the scry sink.
A deployment that sends metrics through the gateway could land them in scry but not
in Mimir — the metrics counterpart to the Loki/OpenSearch logs sinks was simply
missing. Separately, all three HTTP sinks reach their endpoints with a stock
`reqwest::Client` trusting only the built-in webpki roots, so any endpoint fronted
by a private/internal CA fails TLS.

**Decision.** Add a **Mimir remote-write output sink** (metrics only) and a
**custom CA certificate** option shared by every HTTP sink.

- *Mimir sink is the inverse of the remote-write inbound.* `mimir::to_write_request`
  reverses `promwrite::map_remote_write`: build a `fingerprint → labels` map from the
  batch's series dictionary, group `samples` by fingerprint into one `TimeSeries`
  each (first-seen order), ns → ms timestamps, drop a sample whose fingerprint has no
  dict entry. It **reuses** `promwrite`'s prost wire types + `encode_snappy` rather
  than redeclaring the protobuf, so encode is guaranteed symmetric with decode (a
  round-trip unit test asserts it). POST to `{url}/api/v1/push` (the Mimir
  distributor remote-write path) with `Content-Type: application/x-protobuf`,
  `Content-Encoding: snappy`, `X-Prometheus-Remote-Write-Version: 0.1.0`, and
  `X-Scope-OrgID: <tenant>` when `--mimir-tenant` is set (multi-tenant Mimir). Mask
  `SIGNAL_BIT_METRICS`; best-effort drop-on-failure like the other sinks (D-041).
- *Custom CA augments, not replaces.* `tls::build_http_client(timeout, ca_cert)`
  parses a PEM bundle (`reqwest::Certificate::from_pem_bundle`) and *adds* each cert
  as a trusted root — public-CA endpoints keep working, the private CA is trusted on
  top. One global `--ca-cert` applies to all HTTP sinks; the scry sink uses the
  native binschema TCP wire (no TLS) so it's unaffected. A bundle file covers the
  "different CA per endpoint" case without per-sink flags.

**Why these specifics.** Reusing the remote-write codec keeps a single source of
truth for the wire format; same v1 limits as the inbound (no remote-write v2, native
histograms, exemplars, per-series metadata). Augmenting the trust store (rather than
a `--tls-insecure` skip-verify) is the correct mechanism for private CAs and keeps
verification on. Per-endpoint CA files and mTLS client certs are out of scope.

**Status.** Implemented in `crates/gateway/src/mimir.rs` (`to_write_request` +
`MimirSink`, pure mapper unit-tested incl. a remote-write round-trip) and
`crates/gateway/src/tls.rs` (`build_http_client`), wired into `main.rs` (`--mimir-url`
/ `--mimir-tenant` / `--ca-cert`, shared HTTP client across Loki/OpenSearch/Mimir,
sink registration + `bail!` guard). Verified end-to-end against a stub Mimir server:
metrics pushed to the remote-write inbound arrive at `/api/v1/push` snappy-encoded
with the tenant + version headers and a non-empty body.

## D-045: scry-agent scrapes Prometheus endpoints (Alloy-replacement for metrics)

**Context.** `scry-agent` was logs-only: it tails CRI container logs and ships
`LogsBatch`. Metrics reaching scry therefore had to come from *some other* collector
(Alloy, the Prometheus agent, an OTel collector) pushing through `scry-gateway`'s
remote-write inbound. To "go full scry" — one node agent, no second collector — the
agent needs to *pull* Prometheus `/metrics` endpoints itself and ship `MetricsBatch`.
The pod watch it already runs for log-label enrichment makes Kubernetes
annotation-based target discovery a natural extension.

**Decision.** Teach the agent to scrape, over the **same wire connection** as logs
(Hello declares `SIGNAL_BIT_LOGS | SIGNAL_BIT_METRICS` when scraping is active).

- *Discovery = annotations + static, unioned.* `prometheus.io/{scrape,port,path,scheme}`
  pod annotations (extend `apply_event` to also capture `metadata.annotations` +
  `status.podIP` and build a `ScrapeTarget`) **plus** repeatable `--scrape-target <url>`
  for non-k8s targets. `spawn_scrape_scheduler` mirrors `spawn_log_scanner`: reconcile
  the desired set (static ∪ discovered) against running per-target scrape tasks (spawn
  new, abort vanished), each task feeding a `metrics_tx` mpsc the batcher folds into a
  `MetricsBatch`. Target labels follow the `stream_labels` convention
  (`namespace`/`pod`/`node` + `k8s_<label>`) plus the Prometheus-conventional
  `job`/`instance`.
- *Hand-rolled text-exposition parser.* `promparse.rs` parses the format ourselves —
  no parser dependency. Bart's call: this is fundamental to ingest and we want to be
  able to modify it. Lenient (malformed sample lines skipped + counted, not fatal);
  handles HELP/TYPE, all four metric families + untyped, Go floats (NaN/±Inf/exponents),
  escaped label values, optional ms timestamps; type resolution strips
  `_bucket`/`_sum`/`_count` to the family. The pure mapping `scrape_to_series` is
  unit-tested in isolation from the HTTP fetch.
- *Label semantics match the remote-write inbound.* Every series carries
  `__name__=<metric>` as a label, fingerprinted with the same `fingerprint()`, so a
  metric ingested via scrape is byte-identical in identity to the same metric ingested
  via the gateway. Target labels are authoritative: a colliding exposed label is
  renamed `exported_<key>` (Prometheus `honor_labels: false`).
- *Synthesize `up` + `scrape_duration_seconds`.* Like Prometheus, so a down/erroring
  target is recorded as `up=0` data rather than silent absence.
- *Auth = plain HTTP + optional bearer.* `--scrape-bearer` accepts `@/path` to read the
  token from a file (keep secrets off argv). HTTPS/mTLS to targets is deferred.

**Why these specifics.** Sharing the existing connection + batcher (rather than a
second client) keeps the agent one process, one socket; the `--keep` allow-list and
the per-fingerprint decision cache extend to metric series unchanged. Reusing the
fingerprint convention is what makes scrape-ingested and push-ingested metrics
interchangeable at query time.

**Known gaps vs Alloy (deferred, not built).** Relabeling / `metric_relabel`;
Service/Endpoints/EC2/Consul/DNS SD; HTTPS/mTLS to targets; native (sparse)
histograms; exemplars/OpenMetrics extras; per-target `honor_labels`/`honor_timestamps`;
scrape WAL/durability — the agent stays at-most-once, same as the log path. A
deployment needing any of these still runs a real Alloy/Prometheus and pushes through
the gateway.

**Status.** Implemented in `crates/agent/src/{promparse.rs,scrape.rs}` (new, fully
unit-tested) + `discovery.rs` (annotation SD + `spawn_scrape_scheduler`) + `main.rs`
(metrics batcher arm, `flush_metrics`, shared `ship_frame` reconnect helper,
dual-signal Hello, `--scrape-*` flags). Sealed by `scripts/smoke-agent-metrics.sh`:
a stub `/metrics` (3 exposed samples) → agent (`--no-discovery --scrape-target`) →
scry-ingestd → bucket → `scry-list`/`scry-query` asserts exactly 5 rows land (3 exposed
+ synthesized `up` + `scrape_duration_seconds`) and query back loss-free.

## D-046: scry-webui multi-target — pick one of N configured queryds by id

**Context.** D-040's `scry-webui` dialed a single `--queryd`. Bart runs more than
one `scry-queryd` he wants to query from the same browser tab — a local
dev daemon and the remote gothab one (reached via an SSH `-L` forward) — and
switching between them meant restarting the server with a different `--queryd`.
The constraint from D-040 stands: the browser must **never** be able to name a
raw upstream address (SSRF).

**Decision.** Make `--queryd` **repeatable** as `id=host:port`
(`--queryd local=127.0.0.1:4101 --queryd gothab=127.0.0.1:4100`); the first
listed is the default. A single bare `host:port` is still accepted (id
`default`) for the common one-target case; once there's more than one, every
entry must be named. The server holds a `Vec<Target { id, label, addr }>`
allowlist where `addr` is `#[serde(skip)]` — it never crosses the wire. A new
auth-gated `GET /api/targets` returns the `{id, label}` list + the default id.
`POST /api/query` reads the selected target from the **`X-Scry-Target` header**
(a target *id*, resolved server-side against the allowlist): absent/empty → the
default target; unknown id → **400** (distinct from the 502 an unreachable
upstream gives). The frontend gains a `target` form field + a `targets()` signal
seeded from `/api/targets` after login; in browser mode `HttpTransport` sends the
selected id as `X-Scry-Target`, and `QueryForm` always shows a target `<select>`
(even with one target, so it's clear which daemon answers). The desktop shell is
unchanged — `TauriTransport` still dials a raw `addr`.

**Rationale.**
- *SSRF invariant preserved.* The browser only ever sends an opaque id; the
  address mapping lives server-side. An attacker-controlled header can at worst
  pick another already-allowed upstream or get a 400 — never reach an arbitrary
  host. The raw address is `serde(skip)` so it can't leak through `/api/targets`.
- *Symmetry with the byte-pipe.* Routing is a one-line id→addr lookup before the
  existing relay; the server still has zero protocol knowledge.
- *Always-shown selector.* Even a single target renders the dropdown, so the
  answering daemon is never ambiguous and adding a second target needs no UI
  change.

**Status.** Implemented in `crates/scry-webui/src/{lib.rs,query.rs,main.rs}`
(`Target`/`parse_targets`/`resolve_target`, `/api/targets`, `X-Scry-Target`
routing) + frontend (`store.ts` `target`/`targets`/`fetchTargets`,
`transport-http.ts` header, `QueryForm.tsx` selector). Sealed by the extended
`scripts/smoke-webui.sh` (two stub queryds with distinct markers; asserts
`/api/targets` lists both + default and never leaks the addr, header routing hits
the right upstream, default when header absent, unknown id → 400) + new
`tests/query.rs` integration tests.

## D-047: Config-driven agent pipeline (TOML)

**Date:** 2026-06-08
**Status:** accepted

The scry-agent's processing pipeline — how labels are reshaped and which
fields are extracted before records go on the wire — is owned by a **TOML
config file** (`--config`, `SCRY_AGENT_CONFIG` env), usually mounted from a
Kubernetes ConfigMap. The file is read once at startup; a ConfigMap change is
applied by restarting the DaemonSet (no live reload).

**Decision.** Split the agent's configuration into two domains:

- **Flags own runtime.** Where to connect (`--server-addr`), intervals,
  batch sizes, `--scrape-target` URLs stay on CLI flags (or their env
  equivalents). These are deployment-specific but don't need team-level
  review.
- **Config owns the pipeline.** Label manipulation, field extraction, and
  per-signal keep filters go in the TOML file. These are the "how" of
  processing and benefit from review/version-control.

Six features in the first cut:

1. **Per-signal `keep`** — the global `--keep` flag is rejected when
   `--config` is set; move matchers into `[logs] keep` and `[metrics] keep`.
2. **`label_map`** — surface a pod label (`k8s_<key>`) under a chosen
   stream-label name, suppressing the `k8s_<key>` twin so the value isn't
   double-indexed.
3. **`static_labels`** — inject fixed labels on every log stream / metric
   series (e.g. `cluster = "gothab-prod"`).
4. **JSON body fields → stream labels** (`[logs.json] labels`) —
   low-cardinality fields promoted to stream identity (fingerprinted →
   postings). Each unique value spawns a distinct stream fingerprint, so a
   high-cardinality field here is expensive (sparse streams, many postings).
   Deliberately no hard cap — operators are expected to choose low-card
   fields.
5. **JSON body fields → per-entry attributes** (`[logs.json] metadata`) —
   high-cardinality data stored in the `Map<Utf8,Utf8>` attributes column,
   SQL-queryable without stream proliferation.
6. **Metric `label_map`** — rename exposed metric label keys (e.g.
   `container_name` → `container`), applied before the `exported_<key>`
   collision check.

**Label precedence** (low→high, later wins):

1. Non-core base labels (`k8s_<key>` from pod labels)
2. `label_map` surfaced names (moved from `k8s_<key>` slots)
3. `json.labels` extracted fields
4. `static_labels`
5. Core keys (`namespace` / `pod` / `container` / `node`) — always
   authoritative

**`deny_unknown_fields`** is set on every section so a typo'd key
(`static_label` vs `static_labels`) fails loudly at startup rather than
silently no-op.

**Rationale.**

- *ConfigMap-native.* Kubernetes operators are used to editing YAML/TOML in
  ConfigMaps; adding yet another `--flag` for every pipeline concern would
  bloat the `Args` struct and make per-cluster overrides painful.
- *Single source of truth for the pipeline.* A team can PR the config file
  the same way they PR anything else; the `--config` + `--keep` bail ensures
  there's never ambiguity about which keep filters are active.
- *No wire/storage changes needed.* The scry backend already stores
  arbitrary stream labels (→ postings) and per-entry structured metadata
  (→ `Map<Utf8,Utf8>` column). All six features are purely agent-side
  transforms.
- *No live reload.* A restart is cheap and avoids the complexity of
  watching for file changes, hot-swapping `LabelFilter`s mid-stream, and
  reasoning about partial application. The DaemonSet is already a restart
  boundary.

**Status.** Implemented in `crates/agent/src/config.rs` (`FileConfig`,
`LogsSection`, `JsonSection`, `MetricsSection`, `LogPipeline`,
`JsonPipeline`, `MetricPipeline`, `resolve()`, `load()`, `compile()`,
`from_global_keep()`). Integrated in `crates/agent/src/main.rs`
(`--config`/`SCRY_AGENT_CONFIG` flag, `config::resolve` at startup,
`enrich_cache` in the batcher loop), `crates/agent/src/stream.rs`
(`enrich_labels`, `enrich_entry`, `json_scalar`, `json_attr`),
`crates/agent/src/scrape.rs` (`relabel_scrape`), and
`crates/agent/src/discovery.rs` (static_labels/label_map parameters on
`spawn_scrape_scheduler`, `stamp_target`). 20+ unit tests. Sealed by
`scripts/smoke-agent-config.sh` (CRI log fixture + stub `/metrics` + TOML
config → agent → scry-ingestd → bucket → scry-query, 10 assertions covering
features 1/3/4/5/6; feature 2 proven by the `enrich_labels` unit test).

## D-048: scry-agent kubelet/cadvisor scraping + label-selector pod SD

**Date:** 2026-06-08
**Status:** accepted

D-045 made the agent a Prometheus scraper (static `--scrape-target` +
`prometheus.io/scrape` annotation SD). Two pieces needed for real k8s parity
with Prometheus/Alloy were deferred because they need TLS, a ServiceAccount
bearer token, and new RBAC: **kubelet/cadvisor scraping** (per-container and
kubelet-self metrics, served over HTTPS on :10250) and **node-exporter-style
discovery** (scrape pods by a label selector, no annotation required). D-048
adds both, agent-side, reusing the existing scrape→wire pipeline.

**Decisions (locked with Bart).**

- **node-exporter SD = pod-label selector, node-local.** Reuses the existing
  node-field-selected pod watch — **no new pod RBAC**. A `[[metrics.scrape_pods]]`
  job names a `selector` (`matchLabels` AND semantics), `port`, and optional
  `path`/`scheme`/`job`; a matching pod on this node becomes a scrape target.
  Annotation SD still takes precedence when both apply. Cluster-wide
  Service/Endpoints SD stays out of scope (it would need cluster RBAC the
  per-node DaemonSet shouldn't hold).
- **Kubelet reached via `https://${NODE_IP}:10250`,** with `NODE_IP` from the
  **downward API** (`fieldRef: status.hostIP`) wired to the new `--node-ip` /
  `NODE_IP` flag. The address is a template — `${NODE_IP}` / `${NODE_NAME}`
  are interpolated at startup, erroring if a referenced token is empty.
- **Kubelet TLS = configurable, default `insecure_skip_verify = true`.** The
  kubelet serving cert rarely has a SAN matching the IP we dial, so skip-verify
  matches Prometheus' stock cadvisor job. A `ca_file` (PEM bundle) opts into
  verification instead. We dropped a TLS `server_name` override — reqwest+rustls
  can't cleanly override the verified hostname, and `insecure_skip_verify` /
  `ca_file` cover the practical cases.
- **Kubelet endpoints = `/metrics/cadvisor` (job=cadvisor) + `/metrics`
  (job=kubelet),** both on by default, individually togglable.
- Consistent with D-047: **config owns the pipeline** (the `[metrics.kubelet]`
  block + `[[metrics.scrape_pods]]` jobs live in the TOML file), **flags own
  runtime** (`NODE_IP` is a flag/env).

**Per-target TLS + rotating bearer.** reqwest bakes TLS config at client-build
time, so a single shared client can't vary verification per request. The agent
keeps a `ClientPool` — `Mutex<HashMap<TlsProfile, reqwest::Client>>` —
lazily building (at most) one client per distinct `TlsProfile`
(`{insecure_skip_verify, ca_file}`). Standard targets all share the default
verify-on profile (one client); kubelet adds one skip-verify client. The
`ScrapeTarget` bearer becomes a `BearerSource` (`Literal | File(PathBuf)`),
and `BearerSource::File` is **re-read inside `fetch` per scrape** — so a
projected ServiceAccount token (rotated hourly) is followed for free, and the
existing `--scrape-bearer @file` form folds into the same path. A file read
every scrape interval is negligible.

**RBAC.** Kubelet scraping needs a new ClusterRole rule —
`apiGroups:[""] resources:["nodes/metrics","nodes/proxy"] verbs:["get"]` —
because the kubelet delegates authz to a SubjectAccessReview on those
resources. Pod-label SD needs **no** new RBAC (the existing pod watch already
covers it).

**Frozen.** `scrape_to_series` / `relabel_scrape` (the pure scrape→wire
mapping) and `stamp_target` (static-labels + relabel application) are unchanged
— their existing tests are untouched. New functionality is layered around them
(`BearerSource`, `TlsProfile`, `ClientPool`, `pod_matches`,
`assemble_pod_target`, `build_kubelet_targets`, `resolve_kubelet_address`) to
minimize regression risk.

**Status.** Implemented in `crates/agent/src/config.rs` (`KubeletSection`,
`TlsSection`, `PodScrapeJobSection` file structs; `KubeletConfig`,
`PodScrapeJob` runtime forms on `MetricPipeline`), `crates/agent/src/scrape.rs`
(`BearerSource`, `TlsProfile`, `ClientPool`, per-scrape bearer re-read in
`fetch`), `crates/agent/src/discovery.rs` (`pod_matches`, selector path in
`build_scrape_target`, `assemble_pod_target`; `spawn_scrape_scheduler` takes a
`ClientPool`), and `crates/agent/src/main.rs` (`--node-ip`,
`build_kubelet_targets`, `resolve_kubelet_address`, `resolve_bearer` →
`BearerSource`). Manifests: `deploy/k8s/agent-rbac.yaml` (nodes/metrics+proxy
rule), `deploy/k8s/agent-daemonset.yaml` (`NODE_IP` env + `--config` arg +
ConfigMap mount), new `deploy/k8s/agent-config.example.yaml`. Sealed by
`scripts/smoke-agent-kubelet.sh` (self-signed HTTPS + bearer-gated stub serving
`/metrics/cadvisor` + `/metrics` → agent `[metrics.kubelet]` config → scry-ingestd
→ bucket → scry-query; asserts 6 rows land, both endpoints scraped, both
scrapes auth'd over TLS-skip-verify, and the `cluster` static label rides all
rows). Pod-label SD needs the k8s pod watch the smoke omits → proven by the
`pod_matches` / `build_scrape_target` unit tests (same approach as D-047
feature 2). Known gaps vs Prometheus/Alloy (deferred): Service/Endpoints SD,
per-job TLS for pod-SD targets, mTLS client certs, hot config reload.
