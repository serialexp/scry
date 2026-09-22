# Query memory pressure — Design

Status: implemented; production qualification pending
Owner: Bart
Last updated: 2026-09-20

## Implementation status

This checklist tracks the gap between the design and `main`. Move an item to
Done only after implementation and focused verification.

### Done

- [x] **Incident diagnosis.** The Gothab trace-id rejection was matched to raw
  cgroup file-cache accounting before query planning, not to the query's size.
- [x] **Cross-project survey.** Gothab's committed-memory accounting and
  trim/reprobe admission were compared with Scry's mount-aware cgroup discovery,
  query caches, object buffers, DataFusion pool, status, and client surfaces.

- [x] **Phase 1 — allocator alignment.** Replaced process-wide mimalloc with a
  shared jemalloc allocator and exposed a tested safe pressure-purge operation.
  Native glibc and x86_64-musl builds pass. Apple and aarch64-musl checks require
  the corresponding SDK/cross-C toolchains, which are not installed on this host.
- [x] **Phase 2 — coherent cgroup snapshots.** Reads effective limit, current
  charge, and conservative clean-file reclaimability from one mount-aware cgroup;
  required discovery and snapshot failures fail closed.
- [x] **Phase 3 — application reclamation.** Added safe, fallible release
  operations and owned-capacity estimates for result/sidecar caches, idle object
  buffers, and generation-fenced derived label metadata.
- [x] **Phase 4 — reclaim and reprobe.** Serializes/rate-limits reclamation,
  performs the O(cache-size) work on Tokio's blocking pool, purges jemalloc,
  reprobes the cgroup, and rejects only if committed pressure remains.
- [x] **Phase 5 — truthful diagnostics.** Separates queue, shared DataFusion,
  process pressure, admission/runtime probe failures, status, and Fleet fields.
- [x] **Phase 6 — deterministic verification.** Covers accounting, discovery
  failures, concurrency, reclamation, runtime cancellation, recovery, control
  bypass, and client-visible messages. Workspace tests and native release builds
  pass; focused allocator/resources/query/object-store Clippy is warning-free.
  Workspace/server `-D warnings` remains blocked by unrelated existing lints.

### Outstanding

- [ ] **Phase 7 — real-cgroup qualification.** Run
  `scripts/profile-query-memory.sh` against a disposable finite-cgroup queryd
  with representative cached data. The harness captures selective, broad, and
  recovery queries, RSS/high-water, raw/committed/peak cgroup charge, optional
  status counters, latency, and OOM events; no suitable disposable dataset and
  queryd are running on this host in this session.

## Why this exists

A production trace lookup carrying one exact trace ID was rejected with “Query
too large” before Scry considered the trace ID or selected a block. Queryd's
cgroup guard compared raw `memory.current` with `memory.max - reserve`. At the
rejection, the process had roughly 101 MiB anonymous memory while its cgroup
held roughly 1.08 GiB of filesystem cache, most of it clean and reclaimable.
The guard therefore mistook kernel-managed cache for committed process pressure
and blamed a request it had not planned.

The existing guard is still necessary: DataFusion's shared memory pool does not
account for every Arrow/Parquet buffer, cache, SQLite page, allocator page, or
other process allocation. The correction is not to remove the guard. It is to
measure committed pressure conservatively, release safe retained state before
shedding useful work, ask the allocator to return unused pages, and trust a
fresh kernel reading afterward.

## Goals

- Never reject a selective query solely because the cgroup contains clean,
  reclaimable ordinary filesystem cache.
- Preserve a fixed emergency reserve for allocations and terminal responses.
- On genuine new-query pressure, release every safe idle or recomputable
  application-owned store, purge allocator-unused pages, and reprobe.
- Keep active query data and DataFusion reservations safe; cancellation/drop,
  not cache mutation, releases active execution state.
- Serialize and rate-limit global reclamation so concurrent arrivals cannot
  create a purge storm.
- Keep control-plane metadata and fleet diagnosis available during data-query
  pressure.
- Tell operators whether failure came from the count queue, DataFusion's shared
  pool, process committed memory, or an unavailable safety probe.
- Preserve mount-aware cgroup-v1/v2 behavior, namespace mount roots, ancestor
  constraints, and `memory.high`.

## Non-goals

- Claiming a calibrated per-query memory estimate. Candidate compressed bytes
  are not an execution working-set estimate for streaming Parquet scans.
- Adding a fixed per-query reservation without measurement.
- Forcibly releasing active DataFusion plans, streams, Arrow batches, or object
  buffers checked out by readers.
- Treating every byte of page cache, slab, tmpfs, or dirty data as reclaimable.
- Changing the query wire solely to carry failure diagnostics; precise stable
  text under `QUERY_ERR_RESOURCES` is sufficient for this phase.

## Memory model

The cgroup probe discovers one effective limiting directory and version. The
same descriptor supplies its effective finite limit, usage file, and
`memory.stat`. It is refreshed at every safety decision so limit shrink is
observed.

`reclaimable_clean_file_bytes` is deliberately narrower than Linux working-set
heuristics:

```text
ordinary_file = file - shmem
file_lru = active_file + inactive_file - shmem
reclaimable_clean_file = min(ordinary_file, file_lru) - dirty - writeback
committed = current - min(current, reclaimable_clean_file)
threshold = limit - configured_reserve
```

All arithmetic saturates. Anonymous memory, shmem/tmpfs, dirty/writeback pages,
and kernel/slab memory remain committed. Missing or malformed `memory.stat`
removes the discount rather than disabling the safety gate. A missing or
malformed required current/limit reading makes new data-query admission fail
closed and cancels in-flight work through the existing runtime watcher.

For cgroup v1, a complete hierarchical `total_*` field set is preferred when
present. Scry retains its existing exact v1 unlimited sentinel handling rather
than Gothab's broad numeric cutoff.

## Reclamation sequence

A fast-path admission reads the committed snapshot. If it is safe, no cache or
allocator work occurs. If it would reject, one process-wide reclamation mutex
runs this sequence:

1. Recheck; another caller may already have relieved pressure.
2. Drop whole-response result-cache entries.
3. Drop loaded postings and bloom entries while preserving single-flight loading
   slots. Existing readers keep their `Arc`; reclamation is eventual for them.
4. Drop idle object-store buffers. Checked-out `PooledBuf`s are untouched.
5. Clear the derived label-suggestion view with a generation fence so a warm
   begun before the clear cannot republish into the new generation.
6. Trigger jemalloc decay/purge for all arenas.
7. Read a fresh cgroup snapshot and decide from it alone.

Global reclamation is limited to once per 30 seconds. This path trades subsequent
cache latency for availability and must remain exceptional. Runtime planning and
stream polling use committed-memory checks but do not repeatedly destroy global
cache state; unsafe in-flight work is cancelled and dropped.

Every release API returns entries and estimated owned capacity removed. These are
operational estimates, not promises of immediate RSS reduction: active `Arc`s,
allocator behavior, and kernel accounting determine what the reprobe observes.

## Allocator

The multicall `scry` binary uses one process-wide allocator for every role.
Align it with Gothab by moving allocator ownership into a small `scry-alloc`
crate backed by `tikv-jemallocator`.

The preferred purge implementation contains no project-authored unsafe code.
Jemalloc's documented all-arena index is `4096`. Through
`tikv-jemalloc-ctl`'s safe typed `Access<isize>` interface, write each arena's
current `dirty_decay_ms` and `muzzy_decay_ms` value back to itself. Jemalloc
documents that setting these controls considers currently-unused pages fully
decayed and purges them unless decay is disabled. If a value is `-1`, temporarily
write `0`, then restore `-1`. Failures are diagnostic; only the post-operation
cgroup snapshot determines admission.

This must be tested on glibc and built for the supported musl and Apple targets.
No behavior may depend on jemalloc background threads because they are not
supported on the static musl release targets. If the safe control cannot perform
the documented operation on a supported target, the fallback is one tiny audited
unsafe `mallctl` wrapper isolated in `scry-alloc`; unsafe remains forbidden in
every other crate.

The separate development probes/load generators keep the platform allocator
unless they explicitly opt into `scry-alloc`. Production roles dispatched by the
multicall binary all inherit jemalloc.

## Existing resource controls and terminology

Four controls remain independent:

1. **Concurrent query admission:** active and bounded waiting socket counts plus
   queue timeout.
2. **DataFusion shared pool:** one process-wide `GreedyMemoryPool` shared by all
   request contexts.
3. **Process committed-memory guard:** cgroup-aware admission and runtime safety
   for memory outside DataFusion's accounting.
4. **Query-local bounds:** live-response row/byte limits and result-size/cache
   limits.

This implementation does not call any of these a per-query memory budget. A
future weighted reservation needs a measured estimator, an explicit envelope,
and representative-scale qualification.

## Errors and observability

The protocol continues using `QUERY_ERR_RESOURCES`, with cause-specific messages:

- query service saturated;
- query queue wait timed out;
- shared DataFusion pool exhausted;
- process memory safety pressure remains after reclamation;
- process memory safety probe unavailable.

“Query too large” is reserved for a future request-specific bound and is not used
for process pressure discovered before planning.

Queryd logs and status report bounded, fixed-name fields for guard enabled state,
limit, reserve/threshold, raw current, clean-file reclaimable, committed charge,
admission outcomes, reclamation attempts/rate limiting, owner-estimated release,
post-reprobe observed reduction, and runtime cancellation phase. Queue outcomes
remain separate. Fleet labels DataFusion reservation, cgroup committed pressure,
and queue saturation distinctly. Failure-only data is not inserted into
success-only `QueryStats`.

## Concurrency and failure rules

- Only one global reclaim sequence may run at a time.
- Loading sidecar slots and checked-out buffers are never removed.
- Cache entries already cloned by readers remain valid.
- A label warm from an old generation cannot republish after a clear boundary.
- Poisoned safety-state locks fail closed or recover only where invariants remain
  explicit; they do not panic the daemon casually.
- A failed allocator purge does not earn capacity.
- A failed post-purge probe rejects the query.
- Limit shrink is observed on the next check.
- Runtime cancellation, disconnect, and errors release request-local state by
  ordinary RAII/drop paths.

## Performance contract

Fast-path admission performs bounded procfs reads and no cache traversal. Global
reclamation is O(number of retained cache entries plus idle buffers), occurs only
on a would-be rejection, is serialized, rate-limited, and dispatched to Tokio's
blocking pool rather than occupying an async runtime worker. No per-row or
per-record allocations are added to query execution.

Representative qualification must populate realistic caches and page cache under
a finite cgroup, then verify both a selective trace lookup and a broad query. It
records query latency, RSS, raw/committed cgroup charge, peak, `memory.events`,
cache release estimates, and reclaim counts. The run passes only with zero OOM or
OOM-kill events and a usable post-pressure daemon.

## Verification matrix

- v2 leaf and parent limits, `memory.high`, namespace roots, escaped paths, and
  deterministic selection among mounts/ancestors.
- v1 combined controllers, hierarchical totals, and unlimited sentinel.
- clean-cache formula including shmem, dirty, writeback, malformed, duplicate,
  clamp, zero, and overflow boundaries.
- false raw-current pressure admits; equivalent anonymous/dirty/shmem pressure
  reclaims once and rejects if still unsafe.
- recheck-before-reclaim, mandatory post-reprobe, and process-wide rate limit
  under concurrent arrivals.
- result/sidecar/label/buffer releases preserve active readers, loading slots,
  checked-out buffers, and generation boundaries.
- data-query rejection does not prevent metadata/fleet/error diagnosis.
- planning and streaming runtime pressure still cancel safely.
- queue, DataFusion, process pressure, and probe failures have distinct messages
  and status counters.
- real-cgroup query qualification produces no OOM event and the next safe query
  remains serviceable.

## Deferred work

- Calibrated weighted future-memory estimates and reservations.
- Per-query DataFusion pools or stage-specific caps.
- Query spill configuration and bounded spill admission.
- A structured wire failure-details frame if clients later need machine-readable
  remediation rather than stable server text.
