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
