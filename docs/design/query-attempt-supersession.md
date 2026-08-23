# Query-attempt supersession and zero-grace compaction

Status: partial — lineage/catalog, non-blocking reaping, protocol/clients, and local-lineage query restart landed; targeted authoritative repair and rollout remain
Owner: Bart
Last updated: 2026-08-23

## Why this exists

Compaction currently protects queries that selected an input block immediately before it was superseded by sleeping for a ten-minute grace period before deleting the input objects. The sleep occurs inline in the serial compaction pass, so production removes only seven net sidecars roughly every ten minutes and cannot drain the current backlog.

The target behavior is:

1. Compaction commits replacement B9 plus durable proof that B9 represents B1…B8.
2. Peers immediately stop planning B1…B8, and a maintenance reaper deletes their objects without delaying further merges.
3. If an already-running query discovers that B8 disappeared, queryd emits a non-terminal `ResponseSuperseded` frame.
4. The client retracts every provisional result from that attempt while queryd repairs its catalog, resolves B8 to the current terminal replacement, and starts a new attempt on the same response.
5. Only an attempt ending in `EndOfStream` is committed by a client or result cache.

This avoids result buffering/spooling, preserves immediate streaming on the normal path, and charges the uncommon compaction race for discarded work and a retry.

## Review record

The draft was independently reviewed against the repository by Claude and Codex. Their blocking findings have been incorporated:

- ancestry must include intermediate blocks, not just L0 leaves;
- lineage replay must be monotonic and fork-detecting rather than a last-write-wins pointer;
- input ancestry must be loaded from durable sidecars;
- catalog non-live state must not depend on the current `superseded_by` foreign key;
- post-`meta.json` commit recovery and physical reaping must survive lease loss, delete failure, and process restart;
- targeted repair needs single-flight, deadlines, bounded concurrency, stable/fork outcomes, and multi-partition support;
- live-query snapshots, metadata queries, cache completion, retention deletions, rollout gating, and canonical live predicates all need explicit treatment;
- compaction and retention grace defaults must be separated.

Optional reviewer suggestions accepted here include explicit attempt IDs/reasons, capability negotiation, bounded retry/time budgets, named telemetry, and D-061 as the eventual decision record.

## Goals

- Remove the compaction pass's inline grace wait and allow the backlog to converge.
- Preserve exact query results across a block disappearing before or during a scan: no omission and no duplicate accepted rows.
- Keep normal query responses streaming and byte-efficient; do not spool or buffer full results.
- Make replacement discovery independent of best-effort Valkey pub/sub, cursor ordering, and intermediate replacement survival.
- Make compaction logical commit and physical cleanup idempotent and crash-recoverable.
- Make all catalog and convergence paths order-independent and fail closed on contradictory lineage.
- Keep retention grace behavior unchanged.

## Non-goals

- Progressive browser/Tauri rendering. Their transports currently collect the complete response before TypeScript decoding; changing them to async chunk iteration is separate work.
- Result buffering, temporary response files, or per-query read leases.
- Compatibility with old query clients. Capability negotiation is used to fail explicitly and support safe rollout, not to preserve old semantics.
- Changing retention policy or its reader-drain grace.
- Orphan GC for output objects that never reached the `meta.json` commit point.

## Implementation status

### Done

- [x] Root-cause analysis of production sidecar accumulation.
- [x] Repository exploration across protocol, query service, clients, compaction, catalog, and convergence.
- [x] Independent Claude and Codex reviews incorporated into this plan.
- [x] **Phase 0 — executable design contract.** Repository design, schema/state-machine contracts, bounds, failure taxonomy, and rollout gates are pinned here.
- [x] **Phase 1 — lineage-aware catalog foundation.** Durable full ancestor closure, pointer-independent live state, schema migration, monotonic claims, intermediate resolution, and fork detection landed.
- [x] **Phase 2 — crash-recoverable compaction foundation.** Metadata commit is authoritative; grace is a non-blocking eligibility timestamp; pending reaps survive and retry; metadata deletes last.
- [x] **Phase 3 — attempt protocol and clients.** Capability and `ResponseSuperseded(0x12)` landed in generated Rust/TS bindings and strict first-party decoders.
- [x] **Phase 4a — local-lineage query restart.** Queryd emits bounded resets, rebuilds attempt-local Arrow/cache state, fails closed on unresolved live blocks/forks, and caches only flushed final attempts.

### Outstanding

- [x] **Phase 4b — targeted repair baseline.** Data queries perform deadline-bounded signal-date reconciliation across writers before repairing/restarting and fail closed on unresolved live rows/forks.
- [x] **Phase 4c — targeted repair hardening.** Repair now has a whole-operation deadline, process-wide bounded GET concurrency, per-partition single-flight/short-TTL sharing, stable-listing retries, atomic catalog application, metadata-query repair, and attempt-local live refetch/dedup.
- [ ] **Phase 5 — staged rollout.** Deploy lineage-aware writers/readers/clients with grace still enabled and bootstrap lineage-bearing catalogs/snapshots.
- [ ] **Phase 6 — zero-grace activation and cleanup.** Enable immediate reaping, verify behavior/backlog decline, then remove the compaction grace path without changing retention grace.
- [ ] **Documentation closeout.** Record D-061 and update architecture, operator docs, CLI help, and this checklist as implementation lands.

## Protocol contract

### Capability negotiation

Add a query capability bitset to every request shape that can touch block objects (data, label names, and label values). Define `QUERY_CAP_ATTEMPT_SUPERSESSION`.

- A queryd configured for zero-grace-capable operation rejects a data client lacking this capability before writing a response.
- Label metadata requests do not emit resets, but the capability/version field prevents ambiguous mixed protocol deployments.
- All first-party Rust and TypeScript clients send it.
- The JSON schema version is bumped, but runtime safety depends on the transmitted capability, not schema metadata.
- The rollout feature gate keeps zero-grace compaction disabled until every maintenance-capable ingest/standalone compactor, queryd, embedded web asset, CLI/probe, and relevant open client session is upgraded.

### Response frame

Add `ResponseSuperseded` on discriminator `0x12` with:

- `superseded_attempt: u32`
- `next_attempt: u32`
- `reason: u8`

Reasons initially include:

- `0`: unknown (forward extension point)
- `1`: superseded block disappeared
- `2`: retired/deleted block disappeared

Attempt 0 is implicit. Normal and cached responses remain:

```text
Schema Batch* EndOfStream
```

A restarted response is:

```text
Schema Batch* ResponseSuperseded Schema Batch* EndOfStream
```

Rules:

- active attempt starts at 0;
- reset requires `superseded_attempt == active` and `next_attempt == active + 1`;
- reset invalidates the attempt's schema, Arrow dictionaries, batches, rows, and counts;
- the next frame must be a fresh `Schema`;
- `EndOfStream` is valid only after a schema and its count must equal final decoded rows;
- `StreamError` terminates the whole response and invalidates every provisional attempt;
- frames after either terminal frame are invalid;
- EOF awaiting a schema or terminal frame is invalid;
- “bytes escaped” means a schema frame write completed successfully, not that it was merely constructed.

Use separate limits for repair and emitted attempts: at most three replacement-resolution rounds, at most two emitted resets, and one wall-clock recovery deadline. Pin the exact constants and rationale in Phase 0. Exhaustion is terminal rather than silently omitting rows.

## Durable lineage model

### Full transitive ancestor closure

Extend `BlockMeta` with a serde-defaulted, sorted, deduplicated field:

```rust
compacted_from: Vec<Uuid>
```

For output O:

```text
ancestors(O) = union({I.uuid} ∪ ancestors(I) for every direct input I)
```

This includes both direct/intermediate blocks and L0 leaves. Thus a live B17 can resolve a stale B9 as well as B1. At fanout 8 and max level 3 the maximum is `8 + 64 + 512 = 584` UUIDs, not 512.

Before an output `meta.json` PUT, `merge_blocks` must GET every input's durable `meta.json` (reusing/expanding the existing metadata fetch path), verify all N input sidecars were loaded, build the complete closure, and reject cycles, malformed ancestry, duplicate direct inputs, or configured size/count overflow. Catalog-synthesized `BlockMeta` is not authoritative because `row_to_entry` currently omits sidecar-only fields.

Add startup validation for all compaction entry points:

- maximum fanout;
- maximum level;
- maximum ancestry UUID count;
- maximum serialized ancestry bytes.

Generation and decoding both enforce the limits. Never truncate lineage. Keep metadata forward-compatible (no `deny_unknown_fields`) so rollback binaries can parse lineage-bearing sidecars even though they cannot safely compact them.

### Catalog representation

Replace pointer-dependent liveness with two independent concepts:

1. `blocks.superseded` (or equivalent non-null timestamp/boolean), filtered by the canonical live predicate;
2. durable claims `lineage(ancestor_uuid, descendant_uuid, observed_at, partition identity)` with no foreign key to `blocks`.

Do not use last-write-wins `ancestor → latest output`. Claims are monotonic and replayable in any order. Resolution computes maximal currently present descendants:

- exactly one maximal live descendant: terminal replacement;
- none: no live replacement is known;
- more than one incomparable maximal descendant: lineage fork; fail closed and surface telemetry.

Because B17 records B9 as an ancestor, replaying B17 before a late B9 cannot regress B1/B9 to the intermediate. Two outputs claiming the same ancestry without a descendant relation form a detectable fork rather than an arbitrary winner.

Add a real SQLite migration that rebuilds `blocks` to remove or neutralize the existing `superseded_by REFERENCES blocks(uuid)` constraint. `init_schema` must not merely stamp a new `user_version` over an old local schema. Snapshot schema version is bumped and old snapshots are rejected.

Add transactional APIs:

- `apply_compaction(output_meta, direct_inputs)`: insert/update output and closure claims, mark known represented blocks non-live, and stage their physical reaping in one transaction;
- `apply_block_meta(meta)`: insert a reconciled block and its claims; if its UUID is already represented by a known live descendant, insert it non-live;
- `resolve_terminal(uuid)` with explicit unique/none/fork outcomes;
- `list_pending_reaps()` / completion updates;
- lineage pruning described below.

A query/catalog reader must never observe a newly committed output and its represented inputs as simultaneously live through local application.

### Canonical live predicate

Define one catalog predicate and reuse it in:

- all signal candidate lists;
- compaction planning;
- retention planning;
- label-cache warming/metadata selection;
- `live_row_count` and status metrics;
- catalog snapshot/reporting semantics.

Clarify and rename/report `block_count` as physical rows versus logical live blocks. Tests pin every consumer.

### Bounded lineage retention

Durability resides in current replacement `meta.json` closure, so SQLite lineage is a rebuildable index, not an eternal tombstone store. Claims remain while their descendant is present or pending reap. Prune claims whose descendant chain has no present/pending terminal and whose partition is beyond the maximum configured retention horizon plus a safety margin. If any signal has unlimited retention, retain claims for its extant outputs and rebuild from live sidecars; stale claims with no extant/pending descendant can still be removed after an authoritative stable partition reconciliation.

Pruning must never remove claims needed by a pending physical reap or active repair. Targeted repair now prunes only after a stable authoritative partition listing, retaining claims whose descendant still has a committed `meta.json`; because every extant terminal sidecar carries the full transitive closure, edges to disappeared intermediates are redundant. `catalog_lineage_rows` exposes retained claim growth. Longer repeated compaction/retention and snapshot-cycle soak coverage remains desirable.

## Logical commit, lease handoff, and physical reaping

### Commit sequence

For a partition lease holder:

1. reconcile committed outputs for that partition under the lease before planning, so a previous holder's post-commit crash is observed;
2. merge inputs and upload output main/index objects;
3. load input metadata and construct/validate full closure;
4. fence check;
5. PUT output `meta.json` last: this is the logical commit point;
6. idempotently apply output, lineage, supersession, and pending-reap rows to the local catalog;
7. emit best-effort `Created`/`Superseded` hints;
8. if the fence is still valid, begin immediate physical reaping; if not, leave pending reaps for a later holder.

After step 5 succeeds, logical publication is mandatory idempotent completion work, not something undone by later lease loss. A new holder reconciles the partition before planning. If reconciliation finds competing committed outputs with incomparable claims, it records a fork and stops destructive work for that partition.

Inputs are never deleted before output metadata with complete lineage commits. A failed pre-commit operation leaves inputs live. A crash or fence loss after commit leaves output authoritative and inputs non-live/pending cleanup.

### Retryable physical reaping

Do not sleep in a merge and do not make cleanup one-shot. Existing catalog rows already contain paths/metadata needed for deletion; use explicit pending-reap state in the catalog rather than a response spool or new object type.

Every maintenance pass:

- acquires the appropriate partition lease;
- processes pending superseded inputs idempotently;
- deletes each input's data/index objects first and `meta.json` last;
- treats NotFound as success;
- continues with other inputs after one failure and aggregates/report errors;
- removes the input block row only after every object is confirmed absent, while retaining/pruning lineage under the rules above;
- emits `Deleted` per completed input/batch.

Pending reaps survive process restart and catalog snapshot. A partition reconciliation can reconstruct pending work when committed output closure and still-extant ancestor sidecars coexist.

Keep compaction grace enabled during the first implementation/deployment phases while proving these semantics. In the activation phase set it to zero/non-blocking, then remove `CompactConfig::grace`, `tokio::time::sleep`, standalone `--grace`, and ingest `--compact-grace` in a cleanup release. If an old grace flag is supplied after removal, fail explicitly rather than silently ignore it.

Split the current shared default: retention grace remains 600 seconds with Valkey (0 without) and retains its existing behavior. A mechanical source/test check ensures no sleep remains in compaction lifecycle code.

## Reconciliation and targeted repair

### General convergence

`Created`, `Superseded`, polling, full walks, snapshot restore, and late insertion all call the same lineage-aware catalog APIs. Reconciliation is no longer additive-only: current output metadata can mark represented ancestors non-live even when events were missed or replayed in reverse order.

Incremental cursor polling remains a freshness optimization, not a correctness mechanism. Unknown compactor prefixes and outputs behind an advanced cursor are recoverable from targeted/full partition reconciliation.

### Query-triggered repair

A failed attempt may record several missing UUIDs across several signal/date partitions. Preserve candidate context before catalog mutation and classify the actual stream failure precisely. Restart only when the error root is object-store `NotFound` and the attempt-local missing set is non-empty. Construct a fresh `EvictOnNotFound` per attempt so state cannot leak between attempts.

For each affected partition:

1. try local `resolve_terminal`;
2. if unresolved, mapped terminal is gone, or stability is uncertain, run authoritative targeted reconciliation over `signal/yyyy/mm/dd/` across writers;
3. list/fetch with bounded concurrency, a hard deadline, and short-TTL partition result cache;
4. single-flight repairs by `(signal,date)` so concurrent racing queries share one operation;
5. apply metadata/claims transactionally and resolve again;
6. verify the unique terminal replacement's committed metadata/main object before restarting;
7. if the partition changes during repair, repeat within the repair/time budget until a stable unique terminal is observed;
8. on a fork or deadline, fail closed.

If no replacement exists after stable authoritative reconciliation and the original object is confirmed absent, classify it as retired/deleted (retention or an observed deletion), mark it non-live, and restart without replacement. This preserves current retention-race behavior. An unstable/unexplained absence before the deadline is terminal; never immediately evict and silently omit.

Expose repair hit/miss/fork/timeout latency and single-flight metrics.

## Query service behavior

Refactor `QueryService::run_query` so an attempt includes candidate listing, result-cache lookup, planning, schema emission, stream execution, and terminal emission.

Attempt-local state includes:

- candidates and candidate-derived cache key;
- physical plan and execution stream;
- `EvictOnNotFound` and missing context;
- live snapshot plus watermark dedup state;
- Arrow schema/IPC generator/dictionary tracker;
- row count;
- `ResponseTee`;
- attempt number and discarded-work telemetry.

On a repairable mid-scan disappearance:

1. stop and discard the attempt-local stream/tee;
2. repair all affected partitions;
3. emit and flush `ResponseSuperseded` (never tee it);
4. discard all attempt-local state;
5. increment attempt and restart from candidate listing.

Planning-time 404s repair/retry without a reset only when no schema write succeeded. Label-name/value handlers use the same bounded planning-time repair loop because they access postings but emit one terminal response and therefore need no reset frame.

For `--live`, reacquire the live snapshot and recompute watermark dedup attempt-locally after the candidate/catalog snapshot. This prevents a newly durable L0 block from being returned both from refreshed blocks and a stale live snapshot. Live attempts never use result cache.

Only the final successful attempt contributes accepted rows and result bytes. Add:

- `query_attempt_restarts_total`;
- `query_discarded_rows_total`;
- `lineage_repair_hit/miss/fork/timeout_total` and latency;
- `pending_reaps` / `compaction_inputs_reaped_total`;
- attempt/recovery fields in `scan_complete` and status output.

### Result cache

- Abandoned attempt bytes and reset frames are never inserted.
- A replacement attempt may hit an existing cache entry; after the already-emitted reset, write that plain cached `Schema/Batch*/EOS` sequence.
- A final miss inserts exactly one plain `Schema/Batch*/EOS` sequence under the repaired candidate key.
- Insert only after EOS write **and flush** succeed; current behavior that can insert after EOS write failure must be corrected.
- Failed, disconnected, oversized, and live attempts leave no cache state.
- Old candidate-set entries remain correct because immutable merges are lossless; they become unreachable as candidate keys change and age out by bounded LRU.
- Postings/bloom cache entries for retired UUIDs are bounded unreachable LRU residue unless explicit invalidation is cheap.

## Client behavior

Update every semantic decoder:

- Rust remote `scry get`;
- `scry-query-probe`;
- server E2E helpers;
- TypeScript `runQuery` and handwritten `TaggedFrame` union.

Use explicit `awaiting_schema`, `streaming(attempt)`, and `complete` states. On reset, validate IDs, destroy Arrow decoder/dictionaries, batches, schema, and row counts, then require a new schema. Reject malformed ordering, duplicate schemas, non-monotonic IDs, request/metadata frames in a data response, reset after terminal, frames after terminal, and premature EOF.

Only the final attempt becomes CLI output or `QueryResult`. The Solid store currently publishes only after `runQuery` resolves, so no store/component reset is required; preserve its existing stale-while-running display of the previous committed result. WebUI and Tauri remain ordered byte relays, with relay round-trip tests for multi-attempt bytes.

Update every exhaustive `QueryFrameMsg` match, generated Rust/TS bindings, handwritten TS union, protocol type exports, framing tests, probe, CLI, and server test helpers through the documented generators only.

## Phased implementation

### Phase 0 — design document and pinned contracts

Create `docs/design/query-attempt-supersession.md` with this design, the required status/checklist, sequence diagrams, catalog schema/migration, exact constants, closure/size limits, canonical live predicate, fork semantics, retry/time budgets, 404 taxonomy, pending-reap state, and rollout gate. Add tests that initially describe the pure closure/resolution and client state machines where practical.

### Phase 1 — lineage-aware catalog and convergence

- Add `BlockMeta.compacted_from`, closure validation, and input-sidecar ancestry loading.
- Implement the blocks-table rebuild migration, independent non-live state, lineage claims, pending reaps, atomic APIs, terminal resolution, fork detection, and pruning.
- Route event consumption, poll/full-walk, snapshot, late insert, status counts, label metadata, compaction planning, and retention planning through canonical semantics.
- Keep physical deletion timing unchanged during this phase.

### Phase 2 — crash-recoverable compaction with grace still enabled

- Reconcile partition before planning under lease.
- Enforce output metadata as commit point and mandatory idempotent logical apply after it.
- Add retryable pending reaping, meta-last deletion, aggregate failure handling, restart recovery, and lease-handoff behavior.
- Preserve the current nonzero production grace as a temporary rollout gate, but implement it as a reap eligibility timestamp rather than sleeping the compaction pass. Further merges continue immediately.

This phase alone fixes the ten-minute compactor serialization while retaining reader protection during rollout.

### Phase 3 — protocol and strict clients

- Add request capability and `ResponseSuperseded(0x12)` to `proto/query.schema.json`.
- Regenerate Rust and TS bindings with `scripts/gen-proto-all.sh`/documented generators.
- Update all exhaustive matches and implement strict attempt state machines in Rust CLI, probe, TS client, and test helpers.
- Test web/Tauri byte relay behavior and explicit rejection of clients lacking capability.

### Phase 4 — query/metadata repair and restart

- Add attempt-scoped 404 capture/context and bounded single-flight partition repair.
- Refactor data-query execution around attempts, including live snapshot/dedup and cache reset/hit/final-flush behavior.
- Add planning-time repair to label-name/value paths.
- Add telemetry and deterministic fault-injection coverage for every signal.

### Phase 5 — staged rollout with grace retained

1. Deploy all maintenance-capable ingest instances/standalone compactors with lineage metadata, atomic catalog, and non-blocking deferred reap support.
2. Publish and verify a lineage-aware catalog snapshot before restarting queryd. Do not let `init_schema` falsely stamp old local catalogs; migrate them or rebuild deliberately.
3. Stagger queryd restarts to avoid simultaneous 300k-sidecar reconciles. A version-mismatched snapshot fallback must be operationally bounded.
4. Deploy all first-party clients and embedded web assets; expire/restart old browser sessions as needed.
5. Verify capability reporting, no lineage forks, bounded pending reaps, correct live counts, and successful attempt-restart fault tests while old grace still protects readers.

### Phase 6 — zero-grace activation and cleanup

- Explicitly enable immediate reap eligibility only after the fleet gate is green.
- Observe reset/repair/error counters, pending-reap drain, object-store errors, exact query counts, and sidecar-count decline.
- Run backlog compaction with bounded partition concurrency and object-store pressure limits; compaction throughput work must not bypass leases or lineage invariants.
- Roll back by pausing immediate reaping/compaction, not by undoing catalog lineage.
- In a cleanup release remove compaction grace config and all compaction lifecycle sleeps; retain retention grace and its 600-second Valkey default.
- Record D-061 and update `docs/ARCHITECTURE.md`, `docs/decisions.md`, README, CLAUDE.md, CLI help, and the design checklist.

## Required verification

### Metadata and catalog

- Old metadata without ancestry parses; new metadata is accepted by an old-shape tolerant parser.
- L1/L2/L3 closure includes direct intermediates and all leaves, sorted/deduped; fanout 8/L3 bound is 584.
- Resolution succeeds from every leaf and intermediate UUID.
- Config/metadata over bounds is rejected, never truncated.
- Replay permutations: B17→B9→leaves, B9→leaves→B17, and Deleted events before/after each.
- Two incomparable terminal descendants claiming one ancestor fail closed.
- Output and represented inputs are never both logically live after atomic apply.
- Late ancestor insertion is born non-live.
- Delete an output while old inputs still referenced it; no FK failure.
- Canonical live count/list/status/label/retention/compaction consumers agree.
- Snapshot migration/version behavior is honest; lineage growth remains bounded after repeated compaction+retention.

### Compaction and reaping

- Input ancestry is loaded from all durable sidecars before output commit.
- Failure/fence loss at every pre-commit point leaves inputs live.
- Lease loss/crash during and immediately after meta PUT converges to one logical output.
- New holder reconciles committed output before planning; competing outputs form a fork and stop.
- Partial delete at every object, process restart, and retry eventually reaps all inputs; NotFound is success; meta deletes last.
- One failed input does not block other pending reaps.
- Deferred grace does not block subsequent merges; zero grace creates no inline sleep.
- Retention grace remains unchanged.

### Protocol and clients

- Exact `0x12` frame round trips in Rust and TS; capability is transmitted/validated.
- Normal response remains one schema/attempt/EOS.
- Reset after schema, dictionaries, and rows discards all provisional state.
- Reset→cache hit and reset→fresh success yield only final rows/count.
- Reset→StreamError/EOF, malformed IDs/order, duplicate schema, frame after terminal, and request-frame response all reject.
- Old/no-capability client is rejected before streaming; old TS decoder rejects unknown discriminator rather than concatenating attempts.
- Multi-attempt response bytes traverse WebUI and Tauri relays unchanged.

### Query correctness and caching

- Planning and mid-scan deletion races for metrics/logs/traces/profiles return exact original rows with no omission/duplication.
- A query selected B9 while B9→B17; repair resolves B9 specifically.
- Missing blocks across multiple dates repair within bounds or fail closed.
- Concurrent queries on one partition perform one targeted reconciliation.
- Attempt-local eviction state does not leak into a later attempt.
- Stable no-replacement retirement (including retention race) restarts without terminal error.
- Targeted listing racing another compaction reaches one stable terminal or errors on budget/fork.
- Live WAL records becoming durable across a restart boundary are not duplicated.
- Label-name/value postings deletion races repair before response.
- Second race exhausts configured budget; non-404/resource errors never reset.
- Disconnect during reset/replacement/EOS inserts no cache entry.
- Final cache entry is exactly one plain Schema/Batch*/EOS stream under final candidates; live/oversized/abandoned attempts leave none.

### Commands and smoke coverage

- Focused crate/unit/integration tests for each phase.
- Protocol generation drift checks for Rust and TS.
- Frontend tests and build.
- `cargo fmt --check`, repository clippy command, then `cargo test --workspace`.
- Existing compaction, multi-instance, catalog snapshot, live-query, WebUI, and query smoke legs using local Garage/Valkey.
- New deterministic smoke/fault harness that forces a mid-scan compacted-input deletion and verifies reset plus exact final result.
- A bounded local backlog experiment demonstrating compaction continues during deferred grace and sidecars decline at the expected fanout rate.

## Affected areas

Primary files/modules include:

- `proto/query.schema.json`, generated query bindings, protocol exports/constants/framing tests, generation scripts;
- `crates/block/src/meta.rs`, events/path/delete helpers;
- `crates/catalog/src/lib.rs`, snapshot/version/migration and roundtrip tests;
- `crates/compact/src/{policy,merge,engine,lib}.rs` and failure/E2E tests;
- `crates/cluster/src/{consume,maintain,poll}.rs` and convergence tests;
- `crates/query/src/{evict,cli}.rs` and caches as needed;
- `crates/server/src/{query_service,stats}.rs` and query E2E/fault-store tests;
- `crates/scry-ingestd/src/lib.rs`, `crates/scry-queryd/src/lib.rs`, query probe;
- `desktop/src/proto/generated.ts`, `desktop/src/protocol/client.ts` and new protocol tests;
- WebUI/Tauri relay tests;
- deployment config and operator/design/architecture/decision documentation.

## Review resolution summary

- **Ancestry:** use full ancestor closure including intermediates (Claude/Codex blocker accepted).
- **Catalog:** use monotonic ancestor/descendant claims plus fork detection, separate non-live state, no block-row FK (accepted).
- **Commit/fence:** meta PUT commits logical output; later lease loss defers reaping, and next holder reconciles before planning (accepted).
- **Reaping:** explicit retryable catalog-backed pending work, meta-last deletes, no serial sleep (accepted).
- **Retention:** stable no-replacement disappearance is a retired outcome, not automatically an error (accepted).
- **Targeted repair:** single-flight, bounded concurrency/deadline/cache, multi-partition and stability loop (accepted).
- **Compatibility:** transmitted capability plus staged fleet gate; no promise of old-client support (accepted).
- **Live/metadata/cache:** attempt-local live dedup, metadata repair, final-flush cache admission, and canonical live counts included (accepted).
- **Rollout:** lineage-bearing snapshot/readers/compactors first, grace retained until fleet verification, zero grace activated separately (accepted).
