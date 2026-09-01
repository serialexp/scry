# Query label and value suggestions — Design

Status: complete
Owner: Bart
Last updated: 2026-09-01

## Implementation status

Tracking the gap between this design and the implementation on `main`.
The decisions below are settled; the Outstanding list is the implementation
contract. Move an item to Done only when it is implemented and covered by
focused tests.

### Done

- [x] **Ownership boundary.** Suggestions are owned by each queryd process,
  rather than by clients, gateways, or a shared external cache.
- [x] **Refresh policy.** Queryd proactively warms the suggestion state for a
  one-hour lookback before serving suggestion requests, then runs a refresh
  pass every 30 seconds.
- [x] **Read strategy.** Refreshes use projected postings reads, bounded
  parallelism, and per-block single-flight so one block is not read more than
  once concurrently.
- [x] **Autocomplete contract.** Results are global and inexact: they are
  process-wide best-effort suggestions, not an authoritative enumeration.
- [x] **Bounds.** A label value response is capped at 1,000 values; the
  `__name__` value space is capped at 10,000.
- [x] **Observability direction.** Queryd status reports estimated resident
  memory for the suggestion state.
- [x] **Client responsibility.** No client-side suggestion cache is required
  or part of this feature.

- [x] **Phase 1 — process-wide state.** Queryd owns concurrency-safe bounded label names and values shared by every request handler.
- [x] **Phase 2 — projected postings reader.** Warming reads only postings `label_name`/`label_value`, never fingerprint lists or data columns.
- [x] **Phase 3 — bounded refresh executor.** Reads use configurable bounded parallelism and process-wide per-block single-flight; errors remain retryable.
- [x] **Phase 4 — warm and periodic scheduling.** Queryd makes a one-hour startup attempt before listening and schedules 30-second completion-relative refreshes.
- [x] **Phase 5 — global inexact responses.** Metrics/logs handlers expand and answer the process-wide deterministic suggestion view with 1,000/10,000 value caps.
- [x] **Phase 6 — status and memory accounting.** Query status and Fleet expose retained counts, saturation/failures, and `resident_bytes_estimate` without claiming RSS precision.
- [x] **Phase 7 — verification and rollout.** Catalog/query/server/frontend focused tests cover selection, lifecycle, bounds, telemetry, and existing metadata round trips.

### Outstanding

_(nothing)_

## Why this exists

Label-name and label-value autocomplete is a planning aid for query authors.
Today, making every request discover its suggestions independently would repeat
postings work, amplify object-store reads, and make latency depend on the
number of blocks visible to that queryd. It also encourages clients to grow
unbounded or stale caches in an attempt to hide that cost.

This design makes the cache a queryd concern. A process maintains one bounded,
shared suggestion view, warms it proactively over the recent one-hour window,
and refreshes it frequently enough to incorporate new blocks. The view is
explicitly approximate: absence is not proof that a name/value does not exist,
and the returned set is not a catalog authority.

## Goals

- Serve label-name and label-value suggestions from queryd-owned process-wide
  state shared by all clients and query requests on that process.
- Build and refresh that state from projected postings reads rather than full
  metric-block reads.
- Keep refresh work bounded: both the number of simultaneous reads and the
  amount of retained suggestion state must have explicit limits.
- Prevent duplicate concurrent reads of the same block with per-block
  single-flight coordination.
- Make the initial one-hour warm a readiness prerequisite for suggestion
  serving, while allowing subsequent refreshes to run periodically.
- Return globally deduplicated, deterministic, inexact autocomplete results.
- Cap ordinary label values at 1,000 and metric-name (`__name__`) values at
  10,000.
- Report an estimate of resident suggestion memory through queryd status.
- Keep clients simple: clients may request suggestions on demand and need no
  cache or cache-invalidation protocol.

## Non-goals (v1)

- An authoritative label catalog, completeness guarantee, or exact negative
  lookup.
- A cross-queryd distributed cache or Valkey-owned suggestion index.
- Persisting suggestions independently of the underlying block postings.
- Reading arbitrary data columns, computing query results, or changing query
  execution admission.
- Client-side caching, cache invalidation messages, or a required client cache
  TTL.
- Raising the result caps dynamically based on a request or allowing an
  unbounded response.
- Making a failed or incomplete refresh take queryd or ordinary data queries
  offline after the initial warm has completed.

## Semantics and API behavior

### Scope

The suggestion snapshot is process-wide. Every label-name request and every
label-value request handled by a queryd reads the same published snapshot; it
is not keyed by client, tenant, query, or individual query attempt. The
underlying query's time range and matchers are not used to imply exactness.
The snapshot is assembled from the blocks selected by the refresh policy and
catalog state visible to that queryd.

A label-name request returns globally deduplicated label names. A label-value
request returns globally deduplicated values for the requested label name. For
`__name__`, values are metric names and use the larger 10,000-item cap. All
other label names use the 1,000-item cap. Ordering must be deterministic (a
stable lexical order is preferred) so repeated responses are usable without a
client cache.

The contract is inexact by design. A response may omit a currently present
name/value because a block was not yet refreshed, a read failed, or its bound
was reached. Implementations should expose truncation/partial-refresh metadata
when the wire shape permits it; that metadata describes confidence in the
response, not a promise that the next pass will contain every value. A caller
must treat suggestions as hints and use normal query execution for truth.

### Warm readiness and refresh

At startup, queryd discovers the eligible block set and proactively performs a
warm pass covering the preceding one hour. Suggestion endpoints are not served
until this initial pass reaches its readiness condition. The readiness result
is published even when individual blocks are unavailable, provided the pass
has made a bounded attempt for the eligible set; the response remains marked
inexact and the failed blocks are eligible for later retry. This prevents one
slow or missing object from creating an unbounded startup wait while preserving
the agreed warm-before-serving behavior.

After readiness, queryd starts a refresh pass every 30 seconds. Each pass
reads newly eligible blocks and merges their suggestions into the shared view.
Requests continue using the existing view while a pass is in flight. Retired
suggestions may remain conservatively until queryd restarts; false-positive
autocomplete is part of the inexact contract and avoids expensive negative
maintenance. A failed pass does not erase a usable view and is visible in
status/logging.

## Storage and refresh design

Postings readers request only the projected columns needed for autocomplete
(label name and label value, plus the block metadata needed to associate the
rows). They must not deserialize series fingerprints or metric data merely to
construct suggestions. The exact projection follows the existing postings
schema and remains compatible with blocks produced by rolling upgrades.

The refresh executor has a configured, finite concurrency limit for projected
postings reads. Work is scheduled by block, with per-block single-flight:
concurrent refresh triggers or requests needing the same block join the
existing read rather than starting another object-store operation. A block
read produces a bounded contribution; aggregation deduplicates into the
snapshot and enforces the response/state bounds before publication.

Refreshes merge complete per-block contributions under one short-lived lock;
requests therefore observe a valid prefix of completed block contributions,
never a partially decoded block. Catalog removals need no eager negative update:
retaining an unreachable suggestion is explicitly safe under this autocomplete
contract. Errors are retryable per block; metrics/status record reads, hits,
in-flight fills, and failures.

The retained-memory estimate accounts for string capacities and conservative
container/index overhead. In-flight projected buffers are bounded separately by
read concurrency and are exposed through the in-flight-fill gauge rather than
folded into a misleadingly precise retained-byte number. Bounds are applied
before high-cardinality values can expand retained state without limit.

## Status and operations

Queryd status includes:

- whether the initial one-hour warm is pending, ready, or degraded;
- last successful and last attempted refresh times and refresh age;
- eligible, attempted, successful, and failed block counts;
- configured/in-use projected-read parallelism and single-flight activity; and
- **estimated resident memory** for suggestion snapshots, indexes, and
  in-flight refresh buffers.

The memory number is an estimate suitable for capacity and alerting, not a
replacement for process RSS and not a per-request allocation measurement.
Status must remain useful during object-store outages and must not require a
successful suggestion request to update.

## Phasing and verification

1. Establish the bounded process-wide data model and status fields.
2. Reuse the postings catalog/object path with a narrow projection.
3. Add the bounded executor and per-block single-flight registry.
4. Implement startup warm/readiness and the 30-second scheduler.
5. Integrate the label-name/value handlers and global inexact response
   semantics.
6. Exercise status, memory estimates, rolling upgrades, missing/retired
   blocks, partial reads, caps, deterministic ordering, and concurrent callers
   in focused tests and a queryd smoke path.

## Open questions for review

There are no product decisions outstanding. Implementation review should still
confirm the concrete configuration names/defaults for read parallelism,
precisely define the wire representation of partial/truncated metadata, and
choose the status field names without changing the decisions above.

## References

- `docs/ARCHITECTURE.md` — metric block catalog and per-block postings layout.
- `docs/decisions.md` — D-025, per-block postings index and intra-block sort.
- `docs/design/query-attempt-supersession.md` — queryd label-name/value access
  and block-read retry context.
