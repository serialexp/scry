# Cache-aware distributed query execution

Status: Phase 0 and Phase 1 control plane implemented; distributed execution disabled
Owner: Bart
Last updated: 2026-09-01

## Why this exists

Today the query frontend sends each request to exactly one queryd. That daemon
lists the candidate blocks, resolves sidecars, reads every surviving block from
object storage, executes the complete DataFusion plan, and returns the result.
Adding queryd replicas increases throughput across independent requests, but it
does not make one cold, expensive query faster.

This is especially wasteful because observability traffic has strong temporal
locality. Several dashboards may query the same signal and historical range with
different filters or aggregations. A queryd that recently scanned those immutable
blocks may already hold useful sidecars or compressed block ranges, while another
queryd would retrieve them again. At the same time, locality must not cause an
already-overloaded queryd to receive more work while idle peers sit unused.

This design implements the intent of D-024 as **cache-aware scatter/gather**. The
queryd receiving the client request becomes the coordinator. It uses the queryd
status snapshots already published in Valkey to shortlist workers, asks those
workers for exact locality and admission bids, assigns each immutable input block
to one worker, and merges their partial results. Locality is a preference, not
ownership: any healthy queryd can read any block from shared object storage.

## Existing foundations

The implementation can build on several properties already present on `main`:

- Every queryd converges its own catalog and can read the shared object store.
- Query table providers already construct one scan branch per explicit block.
- Blocks are immutable and have stable UUID-derived object paths.
- Queryd already publishes a canonical status snapshot to Valkey approximately
  every two seconds. It includes its role and query address, in-flight queries,
  admission waiters/rejections, DataFusion memory use, RSS, and aggregate cache
  telemetry.
- Postings and body-bloom caches are process-wide and keyed by block UUID.
- The result cache is keyed by normalized request plus the sorted candidate block
  UUID set, so exact repeated requests remain a coordinator-local fast path.
- Query attempts can be superseded and restarted if selected blocks disappear
  during compaction or retention.

Queryd does **not** currently retain main Parquet contents. Its object-store pool
retains reusable buffer capacity, not bytes associated with a block. Therefore
exact main-data locality requires a new bounded immutable-object range cache;
until that exists, locality consists only of postings, blooms, metadata, and
stable affinity learned from previous assignments.

## Review record

The initial draft was independently reviewed against the repository by Claude and
Antigravity. Review findings are incorporated before this proposal is considered
implementation-ready. In particular, the design now pins SQL lowering, postings
ownership, attempt-safe partial output, authentication, transport, reservation
semantics, locality-summary bounds, and coordinator final-merge admission rather
than leaving those as implementation details.

## Goals

- Use idle capacity across queryd replicas to accelerate one expensive query.
- Prefer workers that already hold the required immutable block data or sidecars.
- Avoid retrieving one block independently on several workers for the same query.
- Avoid assigning work to busy, queued, memory-pressured, stale, or incompatible
  workers even when they report locality.
- Keep the receiving queryd as the sole client-facing coordinator and preserve
  the public query protocol.
- Push partial aggregation to workers where it substantially reduces network
  transfer; support bounded filtered-row streams where partial aggregation is not
  possible.
- Make fragment failure, cancellation, retry, and query-attempt supersession
  explicit and bounded.
- Keep all discovery and locality information advisory. Correctness comes from
  explicit immutable block lists and coordinator validation, not Valkey state.
- Preserve local execution for small queries and whenever distribution would be
  slower or less safe.

## Non-goals

- General distributed SQL, arbitrary shuffles, distributed joins, or a Ballista/
  Trino-style multi-stage scheduler.
- Exclusive block ownership or mandatory partition placement.
- Storing block data in Valkey.
- Making a worker's independently converged catalog authoritative for a fragment.
- Sharing complete result-cache values through Valkey or consulting a worker's
  whole-response cache for fragment execution; fragment results are attempt-scoped.
- Guaranteeing that an operating-system page-cache hit is measurable. Scheduling
  uses application-managed cache residency and stable affinity only.
- Distributing label-suggestion requests or other tiny metadata operations in the
  first version.
- Allowing peer assistance to weaken query memory, admission, result-size, or
  deadline limits.

## Terminology

- **Coordinator:** the queryd that accepted the client connection.
- **Worker:** a queryd executing one fragment on behalf of a coordinator. A queryd
  may coordinate some queries while working on others.
- **Candidate block:** an immutable catalog block in the coordinator's stable
  request snapshot. Catalog/time selection is mandatory; any coordinator-side
  sidecar pruning performed before cache-key construction is part of that snapshot.
- **Fragment:** an explicit set of blocks plus a restricted executable operation,
  deadline, attempt identity, and output contract.
- **Locality:** application-observable cached data for a block: complete compressed
  object, required byte ranges, footer, postings, or bloom.
- **Bid:** an authoritative, short-lived worker response describing exact locality
  and whether it can admit a proposed fragment now.

## High-level flow

1. The coordinator applies request defaults, lists candidate blocks, and checks its
   exact whole-response cache as it does today.
2. It classifies the query. Small scans, unsupported plans, and cache hits remain
   local.
3. It reads the latest query-role snapshots from a background-refreshed in-memory
   worker-pool view; request handlers never `SCAN` Valkey. It discards stale,
   incompatible, overloaded, or memory-pressured peers.
4. From bounded locality summaries and load state, it shortlists a small worker
   set. The coordinator itself is always a candidate.
5. It sends exact bid requests containing block UUIDs and required data classes to
   shortlisted peers. A worker checks current cache residency and attempts a
   non-blocking admission reservation before replying.
6. The coordinator assigns every block to exactly one admitted worker, preferring
   locality and then low expected completion time. Uncached work uses stable
   rendezvous affinity so future queries tend to return to the same worker.
7. Workers execute their explicit fragments and stream partial aggregates or
   filtered rows to the coordinator.
8. The coordinator combines fragment outputs, applies global operations, writes
   the public response, and populates its ordinary whole-response cache.
9. Cancellation, deadline expiry, worker loss, or attempt supersession cancels all
   outstanding fragments. Failed block assignments may be retried locally or on
   another worker within the original query budget.

```text
                           Valkey status snapshots
                        load + bounded locality summary
                                     |
                                     v
client ---> queryd A (coordinator / final plan / response cache)
                 |          |                         |
                 | bid +    | bid +                   | local fragment
                 | blocks   | blocks                  |
                 v          v                         v
              queryd B   queryd C                  queryd A
              warm data  idle/cold                 local data
                 |          |                         |
                 +------ partial result streams ------+
```

## Work is partitioned by immutable block

The scheduling unit is a block, not an arbitrary clock interval. Blocks can
straddle interval boundaries, and splitting only by time could make two workers
open the same block. Each fragment carries the original timestamp predicate but
contains a disjoint explicit block list. Across one accepted query attempt, every
block is assigned exactly once.

Block weights initially use catalog `byte_size`, adjusted by signal and known
selectivity where available. Scheduling balances estimated bytes rather than
block counts. Later measurements may add row count, compressed-to-decoded ratios,
column projection, and historical scan throughput.

A very large block remains one unit in v1. Intra-block row-group partitioning is a
possible later extension, but requires stable row-group identities and more
careful duplicate/retry handling.

## Query classification and plan contract

The coordinator, not workers, parses and validates the public request. Workers do
not accept arbitrary client SQL or consult their catalog to choose inputs.

### Classification and SQL lowering

There is no structured aggregate request model today: `QueryRequest.sql` may
contain arbitrary SQL over the narrowed signal table. The coordinator therefore
uses DataFusion to parse and construct the logical plan, then pattern-matches an
allowlisted shape:

```text
Limit? -> Sort? -> Aggregate? -> Projection? -> Filter? -> signal TableScan
```

Supported expressions are lowered into a versioned Scry-owned expression IR
(column refs, typed literals, comparisons, boolean combinations, supported
string predicates, and allowlisted aggregates). Unknown nodes, expressions,
UDFs, aliases that cannot be resolved, joins, windows, subqueries, and other
non-decomposable shapes force local execution before any bid. Workers never
receive or parse client SQL and never receive arbitrary DataFusion plan bytes.

A matcher-only request (`sql` absent, currently equivalent to `SELECT *`) is the
first filtered-row shape. The synthesized `labels` column requires per-block
fingerprint-to-label state; v1 either ships the worker-resolved label output or
runs locally. Distribution is disabled until that schema path has an explicit
conformance test.

### Initial supported classes

1. **Decomposable aggregates.** Filters, projection, and DataFusion partial
   aggregate execute on workers. The coordinator runs merge/final aggregate,
   global sort, and limit. `count`, `sum`, `min`, `max`, average represented as
   sum+count, and mergeable grouped aggregates are the first target.
2. **Filtered row streams.** Workers scan/filter/project explicit blocks and
   return rows. The coordinator performs the global merge ordering and limit.
   Distribution requires an estimated bounded transfer and is disabled for large
   unselective dumps.
3. **Point lookup.** A trace/profile lookup can be sent whole to the preferred
   worker for its candidate block set, with the coordinator relaying the result.
4. **Historical part of a live query.** Stored blocks may be distributed; the
   coordinator alone performs existing ingester live-window fan-out and combines
   it with historical output.

Unsupported/non-decomposable plans run locally. Distribution is an optimization,
never a requirement for query correctness.

### Plan representation

Do not begin by serializing arbitrary DataFusion physical-plan internals, which
are version-sensitive and unsafe across mixed binaries. Define a versioned,
restricted internal `FragmentPlan` from Scry's normalized query model:

```text
FragmentPlan {
    protocol_version,
    signal,
    query_attempt,
    fragment_id,
    fragment_attempt,
    blocks: [BlockRef],
    ts_min,
    ts_max,
    required_columns,
    predicates,
    operation: PartialAggregate | FilteredRows | PointLookup,
    aggregate_spec?,
    ordering?,
    row_limit?,
    byte_limit,
    deadline_unix_ms,
}
```

`BlockRef` contains exactly the worker-facing catalog snapshot: deployment/bucket
identifier, signal, UTC date, UUID, writer UUID, minimum/maximum timestamp, main
object byte size, schema version, and postings/bloom presence and sizes. It never
contains a raw object URL or arbitrary path. The worker maps the identifier to its
locally configured object-store client and reconstructs canonical paths with
`scry_block::block_path`. A worker rejects unknown deployments, protocol versions,
or schema versions before admission.

### Sidecar resolution ownership

Workers own postings and body-bloom resolution for their assigned blocks. This is
what makes sidecar locality useful and avoids transferring potentially large
fingerprint lists from the coordinator. The coordinator initially selects blocks
by its catalog snapshot; each worker may prune assigned blocks to an empty
fingerprint/bloom intersection and returns a valid empty fragment result. The
worker reports the exact subset it pruned and scanned for validation and timing.
The result-cache candidate key remains based on the coordinator's authoritative
candidate snapshot before **worker-side** sidecar pruning, matching current cache
correctness. If the coordinator itself prunes before key construction, that
already-stable pruned set is the snapshot carried into scheduling.

## Discovery and the role of Valkey status

The existing status heartbeat is the worker membership and coarse scheduling
source. Queryd maintains a background-refreshed, atomically replaceable in-memory
worker-pool view; request handling never runs the registry's Lua `SCAN`/`GET`
operation. V1 may initially filter query roles from the shared status prefix, but
production scaling should partition keys as `<namespace>/status/query/<id>` so a
coordinator does not fetch unrelated ingest/gateway/compact snapshots.

The query status payload must add:

```text
worker {
    fragment_protocol_version,
    execute_addr,
    fragment_slots_limit,
    fragment_slots_in_use,
    draining,
}
locality {
    generation,
    digest,
    main_cache_bytes,
    main_cache_budget_bytes,
    sidecar_cache_bytes,
}
```

Existing fields supply `queries_in_flight`, admission waiting/rejections,
DataFusion reserved memory, RSS, cache pressure, address, version, and heartbeat
time.

`execute_addr` is an explicitly advertised routable address, separate from the
worker bind address. Startup rejects wildcard advertise addresses; deployments
may derive it from `NODE_IP` or set `SCRY_QUERY_WORKER_ADVERTISE_ADDR`, following
the existing tail-address pattern.

Status is intentionally approximate and may be roughly two seconds old. It is
used only to shortlist and rank workers. It does not reserve capacity, prove
residency, or authorize execution.

### Bounded locality summaries

Publishing every resident block UUID on every heartbeat would make Valkey status
large and expensive. Each queryd instead publishes:

- a Bloom filter of at most 16 KiB over block UUIDs with main-data residency;
- optionally separate filters, within the same aggregate cap, for postings and blooms;
- at most 64 exact MRU block UUIDs;
- at most 32 coarse `(signal, UTC day) -> resident bytes/block count` entries;
- a monotonic generation incremented on material cache membership changes.

Locality is published only when its generation changes under
`<namespace>/locality/<instance_uuid>`, with a longer bounded TTL. The two-second
heartbeat remains lean and carries generation/digest only; coordinators load
locality blobs in their background worker-pool refresh rather than on query paths.
Plain Bloom
filters are periodically rebuilt so eviction does not create permanent stale
positives. False positives only cause an unnecessary exact bid. Publication lag
may temporarily omit new entries, but stable assignment still provides affinity.

## Exact bid and admission protocol

After shortlisting at most a configured number of peers, the coordinator sends:

```text
BidRequest {
    coordinator_id,
    query_attempt,
    offer_id,
    deadline_unix_ms,
    signal,
    required_columns,
    requires_postings,
    requires_bloom,
    blocks: [{ uuid, estimated_scan_bytes }],
    output_kind,
    estimated_output_bytes,
}
```

The worker responds:

```text
BidResponse {
    offer_id,
    worker_id,
    locality_generation,
    exact_locality_per_block,
    admitted_blocks,
    reservation_token,
    reservation_expires_unix_ms,
    estimated_start_delay_ms,
    available_fragment_slots,
    memory_pressure,
}
```

or declines with `Busy`, `Queued`, `MemoryPressure`, `Draining`, `Deadline`,
`Unsupported`, or `StaleOffer`.

A successful bid includes a short-lived reservation token. The worker obtains it
with non-blocking fragment admission; bids must never create an unbounded remote
queue. The reservation holds both one fragment slot and a conservative weighted
memory allowance derived from offered input/output shape. It is reflected
separately from ordinary client admission in status while still competing for the
same process memory and cgroup safety limits. If the coordinator does not confirm
execution before expiry, both reservations are released. Once execution starts,
the absolute fragment deadline—not the reservation TTL—is the crash backstop.
Tokens are bound to coordinator identity, offer ID, exact block-set digest,
operation, and expiry. Bid requests are idempotent by `(coordinator_id, offer_id)`.
The coordinator immediately sends idempotent `ReleaseReservation(token)` to every
successful bidder that receives no assignment; TTL is only the crash/partition
backstop, not normal release behavior.

Before seeking worker bids, the coordinator reserves its final-merge memory and
remote-output window. If that admission fails, it executes locally or rejects
under existing query resource semantics; it never dispatches output it cannot
consume.

## Scheduling policy

The first scheduler should be deterministic and inspectable rather than
statistically clever.

### Eligibility

A peer is excluded when:

- its heartbeat exceeds the freshness threshold;
- role, protocol, schema, or deployment namespace is incompatible;
- it is draining;
- fragment slots are exhausted;
- query admission is queued or recently rejecting;
- DataFusion/cgroup memory pressure exceeds the configured threshold;
- the remaining query deadline cannot cover dispatch and estimated work.

The coordinator applies these rules to itself as well, except it may keep a small
local fragment needed to drive final-plan progress.

### Preference order

For each block:

1. worker has the complete main object locally and an admission token;
2. worker has the footer and all required column-chunk ranges locally;
3. worker has the footer and some required ranges locally;
4. worker has footer plus required postings/bloom locally;
5. worker has relevant sidecars locally;
6. cold worker selected by rendezvous hash of `(deployment, signal, block UUID)`;
7. least-loaded remaining worker;
8. coordinator local fallback.

Within a locality class, choose the lowest estimated completion cost:

```text
cost = queue_delay
     + uncached_bytes / observed_object_store_throughput
     + estimated_compute / available_compute_share
     + estimated_result_bytes / peer_bandwidth
     + pressure_penalty
```

The first implementation may use normalized integer weights for these terms.
Expose the component scores in trace-level diagnostics so production tuning is
possible.

### Assignment construction

A greedy bin-packing pass is sufficient initially:

- assign strong-locality blocks first;
- cap blocks/bytes per worker and workers per query;
- balance estimated total cost, not number of blocks;
- apply rendezvous assignment per cold block, then coalesce adjacent/same-day
  blocks only within the same worker's assignment;
- do not dispatch fragments whose estimated benefit is below fixed overhead;
- keep every assignment disjoint.

Stable rendezvous affinity for cold blocks lets the cache improve itself over
time without fixed ownership. If the preferred worker is busy or absent, another
worker handles the block immediately.

## Main-data range cache

Locality is most valuable only after queryd gains a bounded content cache for
immutable Parquet reads.

### Cache key and value

Keys are `(object path, byte range, object version/size)`. Since UUID-derived block
objects are immutable, successful bytes never need coherence invalidation.
Deletion from object storage does not invalidate already cached bytes for an
accepted attempt; query-attempt/catalog rules still determine whether that block
may be used.

A practical implementation is an `ObjectStore` decorator above `PooledStore`,
backed by one sparse local file per block and an interval map of validated extents:

- cache the Parquet footer and fetched compressed ranges at their true file offsets;
- satisfy subranges from interval coverage and fetch only uncovered gaps, so
  dynamically coalesced requests still hit;
- avoid one-file-per-range inode explosion;
- optionally promote to complete-object residency after enough coverage;
- byte-budget LRU or segmented-LRU eviction by block;
- per-block/range single-flight to prevent duplicate local fetches;
- checksum/length validation before publishing an extent;
- atomic index publication and unlink-based block eviction;
- separate limits for disk bytes, in-flight fills, and memory buffers.

The initial index may be rebuilt from a dedicated cache directory; persistence
format remains an implementation decision, but startup work must be bounded and
corrupt/partial sparse files must be discarded.

Memory-only caching is simpler but competes directly with DataFusion and existing
query caches. Persistent local SSD is the default production target. The cache is
rebuildable and never a source of truth.

### Residency accounting

A block is `complete` only when every byte is validated and indexed. Otherwise the
bid reports exact covered ranges relevant to the offered projection. Merely
having reusable buffer capacity or hoping bytes remain in the kernel page cache
does not count as residency.

## Worker execution protocol

Use a private query-worker listener, separate from the public client and live-tail
protocols. D-031 already standardized client/queryd traffic on length-prefixed
binschema carrying Arrow IPC batches and identified the same framing as the likely
worker transport. V1 therefore uses generated, versioned binschema control frames
and Arrow IPC payload frames over TCP, one bounded connection per fragment.
Arrow Flight remains a possible later transport optimization, not a v1 dependency.

The worker listener requires a deployment-scoped shared secret supplied through a
file/secret environment setting. Each connection performs a nonce + timestamp
HMAC-SHA-256 handshake binding protocol version, coordinator instance ID, worker
instance ID, and deployment namespace. Execution/reservation messages are then
bound to that authenticated connection and token claims. Rotation supports
current and previous keys for one bounded overlap window; startup refuses remote
execution when authentication is configured inconsistently. The public query port
cannot issue worker operations.

Required operations:

- `Bid` (including required columns/ranges for exact locality)
- `ReleaseReservation(reservation_token)`
- `Execute(reservation_token, FragmentPlan)`
- `Cancel(query_attempt, fragment_id, fragment_attempt)`
- streamed `Schema`, `Batch`, and `FragmentStats`
- terminal `FragmentComplete` or `FragmentError`

Internal authentication should use deployment-scoped credentials and authenticate
both coordinator and worker. A worker validates that every requested object path
matches the declared deployment bucket and canonical block-path form.

## Combining results

### Partial aggregates

Workers explicitly construct DataFusion `AggregateMode::Partial` execution and
never apply final aggregate, global order, or limit locally. In particular,
`avg` is transmitted as mergeable sum/count accumulator state, not a worker-local
average. Workers emit accumulator state, not presentation rows. A fragment
attempt's complete output is retained in an attempt-local bounded buffer/spool and
is **not incorporated into final aggregate state until `FragmentComplete`**.
Failure therefore discards the entire fragment attempt; no irreversible partial
state needs subtraction. Cardinality/output limits are enforced while building
that buffer, and overflow cancels distributed execution before public output.

After all required fragment attempts complete, the coordinator unions their
states and runs DataFusion final aggregate, global ordering, and limit. Aggregate
state schema and semantic version are part of the fragment protocol. Aggregate
queries defer public output until fragment completion and finalization, so worker
failure does not consume a client-visible supersession. Floating-point aggregation
retains DataFusion's existing order-dependent behavior; distributed combination
may change low-order rounding and must be covered by the same tolerance contract
as parallel local execution.

### Filtered rows

Workers return schema-identical projected rows tagged internally with fragment
attempt. When global ordering is required, each worker must first produce a
monotonic fragment stream across all of its potentially overlapping blocks (for
example via a bounded/spilling sort-preserving merge); only then can the
coordinator perform a correct bounded k-way merge and stop workers once a global
limit is satisfied. Each worker Arrow IPC stream is decoded with an independent
schema/dictionary decoder before its materialized `RecordBatch` values enter the
coordinator merge, preventing dictionary-ID collisions across independently
encoded streams. Backpressure flows from the client through the coordinator to
worker streams.

The coordinator must not expose a fragment's rows in a way that prevents retry.
For ordered streams it can accept rows only from the currently active attempt and
resume/restart before those rows cross the public query-attempt boundary. If a
failure occurs after provisional public output, use the existing query-attempt
supersession mechanism to retract the complete attempt and restart; never append a
replacement fragment and risk duplicates.

## Correctness invariants

1. The coordinator chooses the candidate snapshot and sends explicit block refs;
   worker catalog freshness is irrelevant.
2. Each block appears in at most one accepted fragment assignment per query
   attempt.
3. The final result is accepted only after every required fragment has completed
   or been safely replaced.
4. Fragment identity is `(query_attempt, fragment_id, fragment_attempt)`. Results
   from stale attempts are discarded.
5. A retry processes the same explicit block set unless the whole query attempt is
   superseded and replanned.
6. Worker output schema and aggregate-state version are validated before merge.
7. Valkey status and locality summaries never establish correctness; stale data
   can only produce a poor scheduling choice.
8. Whole-response cache entries are created only from a successfully completed
   final query attempt and retain the existing request-plus-candidate-set key.
9. A live query's ingester snapshot belongs to the coordinator attempt and is not
   independently refetched by workers.

## Failure, timeout, and cancellation behavior

- **Bid timeout/miss:** ignore the peer and schedule elsewhere.
- **Reservation expiry:** worker releases capacity; coordinator obtains a new bid.
- **Failure before public output:** retry the fragment on another worker or
  locally, bounded by maximum attempts and the original deadline.
- **Failure after provisional public output:** cancel all fragments, emit query
  attempt supersession with new reason `3 = distributed fragment failed`, and
  restart the complete attempt if its retry budget permits. Distributed and block-
  disappearance resets share the existing bounded query-attempt budget; they do
  not create a second retry loop. Streaming distributed filtered rows therefore
  require the already-shipped `QUERY_CAP_ATTEMPT_SUPERSESSION` capability; a
  non-capable client is eligible only when the coordinator buffers the bounded
  complete response before exposing it.
- **Coordinator disconnect:** cancellation tokens close every worker stream and
  release reservations promptly; TTL is the crash backstop.
- **Worker crash:** status expires naturally; in-flight coordinator detects EOF
  and retries/restarts.
- **Valkey outage:** no new remote scheduling. The query executes locally using
  existing behavior; already-established worker RPCs do not depend on Valkey.
- **Object deleted during scan:** worker returns structured block-not-found with
  UUID. Coordinator applies the existing targeted repair/supersession policy.
- **Overload:** workers decline rather than queueing unbounded fragment requests.
- **Mixed versions:** incompatible peers are filtered by status and must also
  reject at RPC negotiation.

Retries and optional hedging are block-set attempts, not independent additive
streams. Hedging is disabled initially; if added, only the first complete attempt
wins and all others are cancelled.

## Resource bounds

Distributed execution must not turn each client query into unbounded cluster
work.

Coordinator limits:

- maximum peers shortlisted and bid concurrently;
- maximum workers and fragments per query;
- maximum aggregate remote output bytes buffered/in flight;
- maximum final-merge memory reservation;
- maximum fragment retries and total remote-attempt bytes;
- one inherited absolute deadline.

Worker limits:

- separate bounded remote-fragment slots and weighted memory reservations;
- shared process-wide DataFusion pool with local client queries, but a lower
  remote pressure threshold (initially 70% reserved/cgroup pressure versus the
  ordinary local-query threshold) so peer work cannot consume front-door reserve;
- maximum accepted blocks, scan bytes, output bytes, and duration per fragment;
- bounded range-cache fill concurrency and disk budget;
- non-blocking bid admission and short reservation TTL.

Remote fragments should receive lower or equal priority to locally coordinated
client queries by default, preventing peer traffic from making a node unavailable
to its own clients. Fairness must also cap one coordinator's share of each worker.

## Observability

### Status additions

Publish worker capability, fragment slot use, draining state, cache totals, and
locality generation/digest through the query status record. Publish bounded
membership/MRU/coverage summaries in the generation-addressed locality record.

### Per-query coordinator telemetry

- local vs distributed decision and reason;
- candidates, assigned blocks/bytes, worker count;
- bid peers, responses, declines, and latency;
- estimated resident bytes selected vs cold bytes;
- local and per-worker queue/execute/transfer/finalize timings;
- retries, cancellations, supersessions, and local fallbacks;
- partial-result and final-result bytes;
- object-store bytes avoided by application-cache hits.

### Per-worker telemetry

- bid requests/accepts/declines by reason;
- reservations active/expired;
- fragments active/completed/failed/cancelled;
- scan and output bytes;
- range-cache hit/miss/fill/eviction bytes;
- coordinator fairness and admission pressure.

The public `QueryStats` frame adds one coordinator wall-clock phase,
`remote_wait_us`. Summed worker compute and transfer counters are detail fields,
not timeline phases, so D-066's `server_total_us >= sum(coordinator phases)`
invariant remains true. It may include at most a small bounded worker summary.
This is protocol work: update
`proto/query.schema.json`, regenerate Rust and TypeScript bindings, and update the
CLI probe, desktop/browser decoders, and compatibility tests before rollout.

## Security

The worker API can cause expensive object reads and compute, so it is not safe to
expose as an unauthenticated cluster port.

- authenticate deployment membership with the pinned nonce/timestamp HMAC
  handshake and bounded current/previous shared-key rotation;
- bind to the private network by default;
- reject arbitrary object-store URLs and noncanonical block paths;
- reject arbitrary serialized physical plans;
- cap all message lengths before allocation;
- include coordinator identity in reservations and audit logs;
- use nonce/expiry protection against replayed execution tokens.

## Rollout plan

Implementation note (2026-09-01): Phase 0 is active, including bucket-stable
implicit query windows and local/shadow decision counters. The implicit lower
bound rounds inward to the next 30-second boundary (or a shorter configured
window), so the safety window remains a hard maximum and may omit only the oldest
partial bucket. The Phase 1 private
worker listener, mutual HMAC authentication, bounded bid/reservation/release/
cancel protocol, exact sidecar-residency bids, and deterministic scheduler are
implemented but do not dispatch query execution. Peer-view refresh and shadow bid
issuance remain before Phase 2.

### Phase 0 — metrics and decision correction

- Fix the default-window result-cache key issue so implicit recent ranges do not
  generate a unique key on every request.
- Measure actual object-store bytes versus catalog compressed bytes by signal,
  projection, and matcher class, plus observed object-store throughput. These
  measurements calibrate distribution thresholds and avoid treating selective
  postings-pruned scans as full-block reads.
- Add distributed-decision telemetry while all queries still execute locally.
- Amend D-024, D-031, and D-066 documentation to state the cache-aware design,
  binschema worker transport, and implementation gate explicitly.

### Phase 1 — worker control plane and shadow bidding

- Add authenticated binschema worker listener, routable advertise-address
  validation, protocol negotiation, `Bid`, explicit release, reservation TTLs,
  `Cancel`, and structured declines.
- Maintain a background in-memory peer view from existing Valkey status.
- Run shadow scheduling against sidecar locality/stable affinity and compare
  predictions with actual local scans without dispatching work.

### Parallel cache track — bounded main range cache

- Add the sparse-file interval-map object-store decorator with single-flight
  fills, explicit disk/memory budgets, and exact residency queries.
- Publish bounded generation-referenced locality summaries.
- Prove cache correctness across eviction, cancellation, partial fill, process
  restart, compaction deletion, and corrupt/truncated local files.
- Do not block basic distributed aggregate execution on completion of this track;
  cold stable assignment and sidecar locality are already useful.

### Phase 2 — point lookup and partial aggregate pushdown

- Execute explicit block-list point lookups on peers.
- Define versioned mergeable aggregate states and the allowlisted SQL-to-IR lowering.
- Split supported plans into explicit worker partial and coordinator final operations.
- Add grouped aggregate, histogram, retry-buffering, and numerical-equivalence tests.

### Phase 3 — filtered-row fragments

- Add bounded selective row streams only after aggregate execution is proven.
- Add independent IPC decoders, worker-local monotonic sorting, coordinator k-way
  merge, cancellation propagation, attempt supersession, and global limit.
- Require supersession-capable clients or buffer the complete response.

### Phase 4 — cache-aware production scheduling

- Enable locality/load scoring behind a feature flag.
- Start with a low worker/query cap and conservative distribution threshold.
- Validate object-store bytes avoided, p50/p95/p99 latency, coordinator overhead,
  worker fairness, cache churn, and failure recovery before raising limits.

## Test and qualification plan

### Deterministic tests

- status parsing excludes stale/busy/pressured/incompatible workers;
- locality Bloom false positives cause only exact-bid misses;
- exact bids reserve slot and weighted memory capacity atomically and expire/release it;
- multiple coordinator/catalog snapshots cannot authorize arbitrary paths or buckets;
- scheduler assigns each block once and balances bytes, preferring locality unless
  load cost dominates;
- rendezvous affinity is stable and minimally remaps on worker membership change;
- fragments reject malformed paths, versions, schemas, oversized requests, and
  expired/foreign tokens;
- partial aggregate merge equals local execution across signals, nulls, groups,
  empty fragments, and floating-point tolerances;
- ordered row merge preserves global order and limit;
- worker failure before output retries without omissions or duplicates;
- failure after provisional output supersedes/restarts the complete attempt;
- client cancellation releases coordinator and worker resources;
- Valkey outage and no eligible peers fall back locally;
- main range cache covers concurrent single-flight, sparse ranges, promotion,
  eviction, failed fill, corruption, and deletion races;
- result cache records only a completed final attempt.

### Integration and chaos tests

- multiple queryds with asymmetric warmed block sets select the warm worker;
- a warm but saturated worker loses to an idle cold worker when predicted faster;
- cold blocks distribute once across idle workers and become stable future
  affinity;
- worker kill, coordinator kill, network partition, delayed status, Valkey loss,
  and object-store throttling do not corrupt results or leak reservations;
- a worker cache hit may finish an attempt-snapshot block already reaped from the
  store; a sibling worker's 404 still restarts the whole query attempt once without
  omission or duplication;
- compaction/retention during distributed scans triggers bounded repair or query
  attempt restart;
- aggregate-heavy queries reduce object-store and network bytes as expected;
- raw-query transfer limits prevent coordinator memory growth;
- mixed-version rollout safely stays local or uses compatible peers only;
- each worker IPC stream uses isolated dictionary state, including colliding
  dictionary IDs across simultaneous peers.

Compare every distributed result byte-for-byte where deterministic, and by an
explicit numerical tolerance where parallel floating-point aggregation is not
order-stable.

## Open questions

1. Should the range-cache interval index persist in SQLite or rebuild from
   bounded per-block extent metadata in a dedicated cache directory?
2. Which aggregate functions are sufficiently stable and common to enter the
   first partial-pushdown allowlist?
3. Should remote fragments always have lower priority than local client work, or
   should a coordinator carry an end-to-end priority through the cluster?
4. For open-ended recent queries, what time bucketing preserves useful result and
   locality affinity without serving stale results?

## Decision summary

Scry will distribute expensive queries by explicit immutable block sets. The
receiving queryd coordinates. Existing Valkey status supplies membership, coarse
load, and bounded locality summaries; direct bids provide exact residency and
atomic capacity admission. Assignment prefers application-managed cached data but
lets current load override locality. Cold blocks use stable affinity so the cache
warms predictably. Workers execute restricted partial operations, and the
coordinator alone produces the client-visible final result.
