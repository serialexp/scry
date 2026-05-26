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
