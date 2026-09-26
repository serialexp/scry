# Error issue indexing and lifecycle — Design

Status: partial — issue index, cursor-driven grouping, lease-guarded clustered processing, snapshots, list, and basic detail reads implemented; workflow outstanding
Owner: Bart
Last updated: 2026-09-26

## Implementation status

This document consumes the dedicated occurrence projection defined by
[Error intake](error-intake.md), then materializes the outputs of
[Error grouping](error-grouping.md). Alert consumers are defined in
[Alert evaluation](alert-evaluation.md). D-074 places the occurrence projection,
`errors.sqlite`, and `scry errors` in an earlier full slice than grouping/issues.
The `issues` and `occurrence_issues` SQLite tables, cursor-driven grouping
(`ErrorsDb::group_page`), read-only issue listing, basic issue/occurrence detail
reads, query-wire serving, lease-guarded clustered processing in ingestd, and
snapshot publication/adoption/refresh are now implemented. Human workflow state,
facets, regressions, durable transitions, cursor pagination, immutable grouping
projections, and rich occurrence inspection remain outstanding.

### Done

- [x] **Repository fit survey.** Block convergence, catalog snapshots, immutable
  object truth, Valkey leases, and local projections have been assessed.
- [x] **Prerequisite slice — occurrence storage/index.** D-074 now publishes
  deterministic immutable occurrence Parquet plus metadata-last conditional commits
  and folds them into a deployment-bound, independently rebuildable `errors.sqlite`.
  Discovery and raw-source processing are bounded and periodic in exclusive
  single-writer mode; extraction accepts only logs schema 3/raw-record v2.
- [x] **Phase 0a — issue index tables.** `errors.sqlite` schema v4 contains `issues`
  (keyed by `deployment_id, issue_id`, indexed on `last_seen_unix_nano`) and
  `occurrence_issues` (keyed by `deployment_id, app_id, event_id`, with the current
  issue ID and grouping generation stored on each membership).
  `fold_grouped()` transactionally upserts issues (accumulating `occurrence_count`,
  `last_seen`, `max_severity`) and inserts occurrence-issue links with
  INSERT-OR-IGNORE idempotency. `ErrorsDb::open_read_only()` opens with
  `SQLITE_OPEN_READ_ONLY` for queryd. `list_issues(limit)` returns `IssueSummary`
  ordered by `last_seen DESC` with UUID-formatted IDs. Tests cover ordering,
  limit, aggregates, idempotent duplicates, and read-only access.
- [x] **Phase 0b — grouping reconciliation.** `group_occurrence_page()` in errorsd
  reads ungrouped occurrences from SQLite, decodes OCC1, computes fingerprint v1,
  derives issue IDs, and folds into issue tables. Called from `reconcile_once`
  after occurrence processing. `GroupingReport` added to `EngineReport` and
  `ErrorsStatus` (issues_created/updated, occurrences_grouped). Integration test
  asserts end-to-end grouping from log block through issue creation.
  *Superseded in detail by the schema v5 rework below.*
- [x] **Schema v5 grouping rework (2026-09-26).** Every newly folded occurrence
  gets a strictly increasing `fold_seq`; grouping walks `fold_seq` from a persisted
  per-generation cursor (`grouping_cursors`), so late-arriving occurrences (old
  `occurred_at`, new `fold_seq`) are never skipped and each pass reads only unseen
  rows. A page's mapping rows, issue aggregates, failure records, cursor advance,
  and (on drain) generation activation commit in one fenced transaction.
  `occurrence_issues` is keyed by the full occurrence identity (`deployment_id,
  app_id, app_identity_sha256, event_id`) plus `grouping_generation`, so several
  generations coexist and distinct application identities sharing a compact
  `app_id` never share a membership row. Undecodable or identity-mismatched OCC1
  rows land in `grouping_failures` and the cursor moves past them, so one poison
  row cannot wedge grouping. Within a page, occurrences of one issue fold into a
  single issue upsert; `first_seen` is lowered by late occurrences; `max_severity`
  comes from the decoded occurrence; the latest occurrence is max(clamped time,
  event ID). Readers list the *active* generation recorded in `metadata`: the first
  generation is active immediately, a later one once its backlog drains. The v5
  migration discards pre-v5 grouping rows (fp-v1, with the defects above) and
  regroups every stored occurrence. The engine drains the backlog page by page
  (`grouping_page_rows` 1024) within a per-pass row budget (262,144) and time
  budget (5 s).
- [x] **Indexes and bounded reads (2026-09-26).** `issues_by_generation_last_seen`
  `(deployment_id, grouping_generation, last_seen DESC, issue_id)` serves the
  list; `occurrence_issues_by_issue` `(deployment_id, issue_id, sort_at, event_id)`
  serves newest-first occurrence history with an event-ID tie-break;
  `occurrences_by_fold_seq` serves the grouping cursor. `EXPLAIN QUERY PLAN` tests
  assert index use and no temp B-tree sort for every hot query. Reads are clamped
  to `MAX_ISSUE_PAGE_ROWS` (1000); the query-wire default is 100. Timestamps are
  JSON numbers; browser doubles keep ~256 ns precision, adequate for display.
- [x] **Clustered processing and snapshots (2026-09-26).** See "Multi-instance and
  no-Valkey behavior" below for the implemented lease, adoption, and refresh model.
- [x] **Scale verification (2026-09-26).** `crates/errors/tests/scale.rs`
  (ignored; run in release) folds and groups one million real OCC1 occurrences
  across 10,000 issues (one hot issue with 100,000) and times every hot read.
  Measured on the home development machine: fold ~145k rows/s; grouping 15k-30k
  rows/s at 10k uniformly hit issues (bound by random leaf writes to
  `occurrence_issues_by_issue`) and 95k-118k rows/s at 50-100 issues; slowest
  1024-row page 0.3-1.2 s (a WAL checkpoint); list/detail read medians 1 µs-0.9 ms.
  Before the page-level issue aggregation and 64 MiB writer page cache the same
  run measured fold 67k rows/s, grouping 26k rows/s, slowest page 1.04 s.

### Outstanding

- [ ] **Decision — workflow conflicts.** Specify operation-level expected-revision
  and commutative-fold behavior.
- [ ] **Decision — retention statistics.** Choose lifetime versus retained-window
  issue aggregates.
- [ ] **Phase 1 — issue lifecycle.** Add durable immutable workflow commands
  (resolve/ignore/assign), revision-checked mutations, regression detection,
  facets, and audit history. The rebuildable issue index and aggregates exist but
  carry no human workflow state.
- [ ] **Phase 2 — remaining clustered reconciliation.** Add hints, partitioned
  leases, snapshot GC/history, immutable grouping projections, compaction locator
  repair, and grouping/issue cold rebuild from objects. The single
  `lease/errors/project` lease, snapshot adoption, and queryd refresh are
  implemented; snapshots are whole-file uploads (see TODO.md).
- [ ] **Phase 3 — complete issue API.** Add cursor-paginated/filterable reads,
  full occurrence detail, and revision-checked mutations through the control
  plane. Query wire currently serves a server-capped, limit-only issue list and
  basic issue summary/recent-occurrence metadata.
- [ ] **Phase 4 — verification.** Add multi-process (real Valkey) takeover,
  retention, fingerprint-collision, workflow-conflict, and grouping cold-rebuild
  coverage. Focused `get_issue`/occurrence-list, query-wire handler
  (`crates/server/tests/issues_e2e.rs`), in-process lease failover, and migration
  tests exist.

## Why this exists

An error occurrence is immutable telemetry; an issue is a changing product view.
Counts and grouping can be rebuilt, while “Bart resolved this,” assignment, ignore
rules, and audit history cannot be inferred from logs. Putting all of that in the
per-instance block catalog would make catalog loss destructive and couple error
schema upgrades to query bootstrap.

This design separates authoritative immutable records, rebuildable issue
projections, and human workflow commands. It also defines how multiple Scry
instances converge without treating Valkey as a database.

## Goals

- Idempotently project every accepted occurrence into exactly one active issue
  membership for a grouping version.
- Preserve stable occurrence identity across block compaction and reprocessing.
- Track first/last seen, counts, severity, releases, environments, and affected
  users with explicit exact/approximate semantics.
- Persist resolve/reopen/ignore/assign/comment/merge decisions durably with audit.
- Detect release/time regressions without rewriting raw telemetry.
- Rebuild from object storage and expose freshness/lag.
- Support leased multi-instance writers and a correct declared single-instance
  mode without Valkey.

## Non-goals (v1)

- Using local SQLite, a catalog snapshot, Valkey, or fleet status as durable truth.
- Mutable issue rows in object storage updated by last-writer-wins.
- Storing full arbitrary event payloads again in the issue database.
- Exactly up-to-the-last-record issue freshness before block seal. A real-time
  observer is an acceleration only.
- Organization/team RBAC, comments with rich attachments, or external ticket sync.

## Authorities

- Raw event: an ordinary immutable logs block; trace blocks are correlation data
  only and never authoritative occurrence input.
- Occurrence: dedicated immutable projection row derived only from an eligible
  exception log with a valid canonical producer `scry.event.id`.
- Grouping result: immutable derived projection generation tied to exact occurrence
  and processor versions.
- Workflow: immutable operator command/event objects.
- Current issue view: replaceable `errors.sqlite` projection folded from both.
- Snapshot: optional bootstrap optimization, version-checked and replaceable.
- Valkey: leases, hints, and status only.

This database is separate from `Catalog`, whose schema and snapshot readiness own
block queryability. Losing `errors.sqlite` delays issue views but never forces the
block catalog to rebuild or loses operator decisions.

## Projection objects

D-074 supersedes the earlier implication that the first error projection combines
extraction and grouping. The foundation publishes a dedicated, immutable,
metadata-last occurrence generation before grouping exists:

```text
_scry/errors/v1/occurrences/<date>/<source-log-block-uuid>/
  <extractor-generation>.parquet
  <extractor-generation>.commit.json   # conditional create, PUT last
```

It contains only eligible exception logs with valid canonical `scry.event.id`,
the deterministic `app_id`, occurrence fields, source-log locator/provenance, and
best-effort trace/span link IDs. It contains no span-event occurrences, generated
or synthetic IDs, heuristic dedup state, fingerprint, or issue membership. A local
`errors.sqlite` occurrence index folds these objects and is independently
rebuildable before grouping/issues ship.

A later grouping generation consumes committed occurrence objects:

```text
_scry/errors/v1/projections/<date>/<occurrence-generation-id>/
  <processor-generation>.parquet
  <processor-generation>.commit.json   # conditional create, PUT last
```

A grouping row contains at least:

```text
deployment_id, app_id, event_id, occurred_at, received_at
source_log_block_uuid, occurrence_projection_key, optional locator_hint
issue_id, fingerprint_digest, fingerprint_version, grouping_policy_revision
parser_version, symbolicator_version, artifact_digest_set
exception_type, title, severity, handled, grouping_quality
service, environment, release, user_hash, trace_id, span_id
canonical_result_digest
```

The raw payload is not duplicated. `event_id` is stable identity; block UUID/row
ordinal is only an acceleration hint because compaction rewrites physical rows.
Occurrence detail lookup resolves by event ID through the index/query layer and
repairs stale hints.

Projection keys are deterministic. Data and the commit object use conditional
create. Identical replay verifies the existing digest and folds once; different
bytes at one identity receive a precondition conflict and are quarantined without
overwrite. Data precedes the small commit object, which embeds exact keys/digests.
Valkey leases avoid duplicate expensive work, while the conditional commit is the
storage fence even if a former holder's request arrives late.

## Local projection schema

Indicative tables, not final SQL:

```text
processed_occurrence_generations(source_log_block_uuid, extractor_generation,
                 occurrence_projection_key, committed_at, PRIMARY KEY(...))
processed_grouping_generations(occurrence_projection_key, processor_generation,
                 grouping_projection_key, committed_at, PRIMARY KEY(...))
occurrences(deployment_id, app_id, event_id, active_issue_id, occurred_at,
            occurrence_projection_key, locator_hint, grouping_quality, ...,
            PRIMARY KEY(deployment_id, app_id, event_id))
issues(issue_id PRIMARY KEY, app_id, grouping_version, canonical_digest,
       title, first_seen, last_seen, occurrence_count, latest_event_id,
       max_severity, workflow_revision, projection_revision, ...)
issue_facets(issue_id, facet_name, facet_value, count, first_seen, last_seen, ...)
workflow_events(command_id PRIMARY KEY, issue_id, durable_key, applied_revision, ...)
issue_aliases(old_issue_id, target_issue_id, reason, created_at, ...)
reconcile_cursors(prefix_partition, highest_key, ...)
```

Indexes support deterministic cursor pagination by `(last_seen DESC, issue_id)`,
issue occurrence history `(issue_id, occurred_at DESC, event_id)`, and bounded
facet filters. The schema declares which facet counts are exact, sampled, or
approximate. User counts should begin as bounded sketches if exact sets cannot be
strictly capped; UI labels them accordingly.

## Discovery and processing

`scry errors` starts a pub/sub consumer before seed reconciliation, then uses
block-created hints, cursor polling, and periodic full walks. Walkers list keys to
learn UUIDs and GET only unknown committed projection/source metadata, following
D-066. Polls/walks are completion-relative and bounded-concurrency. A transient
object error records lag and skips work rather than discarding the whole view.

In the implemented foundation slice, for each logs schema-3 source block, `scry
errors` applies bounded admission, accepts only server-stamped raw-record v2, selects
eligible exception logs with valid canonical producer IDs, folds exact duplicate
IDs/collisions, and conditionally commits the dedicated occurrence generation
metadata-last. It never scans trace span events for occurrences.

A subsequent implemented local phase reads ungrouped occurrences from SQLite,
computes the basic type/message fingerprint, derives deterministic issue IDs, and
folds issue aggregates/membership directly into `errors.sqlite`. It does not yet
write the immutable grouping-generation objects described above, support concurrent
algorithm generations, or cold-rebuild grouping state from those objects.

In the later complete grouping phase, for each committed occurrence generation:

1. acquire the partitioned projection lease when clustered;
2. read canonical occurrences with bounded admission;
3. symbolize and group using exact versioned inputs;
4. stage deterministic grouping projection data;
5. recheck lease fence and source liveness/version policy;
6. PUT commit metadata last;
7. emit a hint; every instance folds the committed object idempotently.

Compaction descendants are a recovery source, not a projection gap. If an input is
superseded before processing, errorsd follows durable ancestry to the live descendant.
A descendant is skipped when all ancestors/events are already processed; otherwise
folds are insert-if-absent by scoped event ID because one occurrence can legitimately
appear at several compaction levels. The freshness bound is compaction physical-reap
grace (which can be zero), not retention TTL; deployments needing asynchronous
projection must configure grace or a durable accepted-record path accordingly.
Retention expiry before any surviving copy is processed is the irrecoverable case.

A direct durably accepted-log observer may provisionally update a low-latency view,
but cannot notify until the equivalent durable occurrence/issue-transition identity
exists. Block reconciliation confirms or replaces provisional state. Trace events
cannot drive this observer.

## Durable issue transitions

Issue lifecycle transitions consumed by alerts are authoritative append-only records,
not side effects re-emitted whenever `errors.sqlite` rebuilds:

```text
_scry/errors/v1/transitions/<partition>/<logical-id>.json
```

`IssueTransition` includes schema/logical ID, causal occurrence or workflow command
IDs, issue/grouping/lifecycle policy versions, kind (`new`, `regressed`, `resolved`,
`severity_changed`, `release_first_seen`, etc.), old/new state digests, event and
server times, and migration provenance. The logical ID and key are deterministic
from deployment, issue, causal IDs, old/new state digests, transition kind, and
policy versions. The object is created with `If-None-Match: *`; a differing existing
digest is a conflict/quarantine.
A rebuild folds existing transitions and never emits replacements merely because it
rediscovers historical first-seen state. Group migration emits explicit migration
transitions that cannot masquerade as new/regressed.

Alert consumers persist per-monitor/partition consumption checkpoints plus a bounded
processed-transition identity index in their authoritative state. Cursor advance and
resulting alert transition are one causal committed record, so crash/replay is
idempotent. Poison records remain visible, block checkpoint advance only according
to a documented quarantine policy, and have explicit retention. Transition records
and consumer dedup/checkpoints outlive every notification derived from them.

## Workflow event log

Illustrative immutable command path:

```text
_scry/errors/v1/workflow/<issue-id>/<uuidv7-command-id>.json
```

Each event has schema version, command ID/idempotency key, issue ID, action,
actor, server-observed timestamp, optional expected revision, operation-specific
payload, and previous-state evidence. Actions include resolve, reopen, ignore with
optional expiry, assign/unassign, comment, merge/alias, and later delete/redact.

Mutation acquires `lease/errors/workflow/<issue-id>` in clustered mode, reads the
latest durable fold, validates expected revision and action, checks the fence, and
writes one immutable object. It acknowledges after its conditional create/CAS is durable, not merely SQLite
update. Duplicate command ID with identical content folds once; differing content
conflicts. The issue's revision head is advanced with ETag `If-Match`, so two commands
based on one revision cannot both become the next accepted revision: the loser gets
409/current metadata. Ambiguous timeout is reconciled by reading the command and
revision head before deciding whether to retry.

Whole-record last-writer-wins is rejected. Review selects per-operation semantics:
status changes likely require expected revision/409; independent fields such as
assignment may be revisioned registers; comments are append-only; idempotent tag
adds/removes can commute. Fold order and ties are deterministic and audited.

## Issue lifecycle

Base status: `unresolved | resolved | ignored`. Ignore may expire. Disable/delete is
not inferred from raw retention. New occurrence updates aggregates and latest
example but does not erase workflow.

A regression occurs when an occurrence arrives after resolution and meets the
configured policy, for example first occurrence in a newer `service.version` or
a post-resolution quiet interval. The state transition records the prior resolve,
triggering event, release/environment, and policy version. It reopens the issue
only if policy says so. Late old telemetry uses event/received times and a bounded
lateness policy so it cannot fabricate a regression silently.

Manual merge creates an alias/redirect and an audited membership policy; it does
not rewrite raw events. Unmerge requires retained component history and is deferred
unless explicitly designed. Grouping-version migrations use the audited movement
policy from `error-grouping.md` and update counts transactionally within a local
fold before publishing a new snapshot/view.

## Retention

Raw occurrence detail expires with its source logs; independently retained linked
traces may also expire. The issue view marks retained sample/link availability and
never promises a deleted example. Review chooses whether
aggregate counts are lifetime facts retained independently or describe the current
raw-retention window. The API always names the semantics.

Workflow/audit, projection generations, aliases, user facets, and notification
references each have explicit retention. Removing user data or source context is
a cross-authority tombstone/redaction operation, not ordinary block retention.
Projection GC retains generations referenced by active migrations/audit, then
deletes commit metadata last. Orphaned staged data is age-collected. Workflow/
transition/tombstone history is retained forever initially; any future compaction
must publish an immutable checkpoint covering exact input IDs and hashes before
inputs are eligible for deletion. A tombstone cannot expire before all older
revisions and dependent alerts are gone, or cold rebuild could resurrect state.

## Multi-instance and no-Valkey behavior

Partition leases use the existing namespaced `LeaseProvider`/Fence semantics,
for example `lease/errors/project/<partition>`. Idempotent deterministic output is
still required. Lease loss before commit abandons staged data for GC.

When Valkey is absent, exactly one declared `scry errors` process may use
`LocalLeaseProvider`; this is the correct single-instance fallback. Configuration
must not silently infer exclusivity from absence. If multiple processors are
possible without a fence, projection/workflow mutations pause unless an explicitly
named unsafe override is selected. Raw telemetry ingest and existing reads continue.

Workflow writes during a transient clustered Valkey outage fail closed rather than
risk conflicting revision decisions. Previously durable workflow state remains
readable. Projection age and lease state are visible.

**Implemented (2026-09-26).** Clustered processing runs in `scry ingestd` (`full`
mode with `--errors-db`) under one deployment-wide lease, `lease/errors/project`,
rather than per-partition leases; the standalone `scry errors` role keeps its
exclusive local single-writer lock. Without Valkey the ingester uses
`LocalLeaseProvider` only with `--allow-unfenced-maintenance`; otherwise error
processing pauses while ingest continues. Before its first pass the worker probes
conditional writes and resolves the deployment manifest, failing closed and
retrying each interval. Every durable write is fenced: publication and each SQLite
commit re-check the lease, and a lost lease stops the pass before its next write.
On acquiring the lease, an instance adopts the bucket snapshot
(`_scry/errors/v1/snapshot.sqlite`) when it has folded more projection commits than
the local database or the local database is missing; non-holders never download
it. Only the holder uploads snapshots, at most every `--catalog-snapshot-interval`
and only after a pass changed occurrence or issue state; the upload is a
`VACUUM INTO` copy switched to rollback-journal mode, fence-checked before the PUT.
A superseded holder's in-flight upload can briefly regress the bucket copy; that
only costs catch-up time because a snapshot is a cache of committed projections.
`scry queryd --errors-db` HEADs the snapshot every `--errors-refresh-interval`
(default 60 s); on a new version it streams the object to a sibling file, validates
schema version, deployment, and journal mode, removes stale `-wal`/`-shm`/`-journal`
files, renames it into place, and swaps its read-only handle without interrupting
in-flight reads. A database in WAL mode is owned by a colocated writer and is only
reopened when its inode changes, never overwritten.

## API contract

Reads are cursor-paginated and bounded:

- issue list/filter/sort; issue summary/facets/grouping explanation;
- occurrence list and one occurrence detail/raw-query reference;
- workflow/audit history and issue aliases;
- projection health/age.

Mutations use an authenticated control API, `Idempotency-Key`, and `If-Match`/
expected revision; conflicts return 409 with current metadata. A durable-accepted
response distinguishes “object committed, local projection pending” from a fully
folded representation. Arbitrary browser object keys, SQL, and unbounded facet
values are not accepted as workflow input. HTTP/API ownership is detailed in
[error-monitoring-ui.md](error-monitoring-ui.md).

## Failure semantics

- Hint loss: polling/full walk discovers committed work.
- Duplicate projection/command: idempotent by deterministic identity.
- Same logical identity/different bytes: conditional create/CAS rejects overwrite;
  quarantine the conflict and retain the accepted digest.
- Object write failure or lost lease: reconcile the conditional commit by key/digest;
  the storage precondition, not lease timing, determines whether it committed.
- SQLite transaction failure: durable object remains and replays.
- Snapshot mismatch/corruption: discard only the error snapshot and rebuild.
- Source logs block compacted: follow ancestry to a descendant, insert occurrences
  by stable scoped event ID, skip already-covered ancestry, and repair locator hints.
- Source logs retained before any source/descendant is processed: report an irrecoverable
  gap and never invent an occurrence. Projection SLO/grace must prevent this.
- Late event: apply explicit lateness/regression policy and preserve provenance.
- Valkey loss: pause fenced writes, report stale, never mark issues healthy.

## Observability and bounds

Status reports source cursor/oldest lag, processed/pending/failed blocks,
occurrences/issues/fingerprint quality, provisional backlog, workflow fold lag,
SQLite/snapshot bytes and age, reconcile GET/failure counts, lease validity,
migration progress, retention gaps, and API latency/conflicts. Values are bounded
aggregates, never exception text or user IDs.

Bound per-block distinct events/issues, projection output bytes, processor memory,
read concurrency, pending queue, reprocessing share, facet values per issue, page
size, workflow payload/comment length, local cache size, and rebuild concurrency.

## Verification

- Replay each object twice and in varied listing order; projections/folds match.
- Crash at every data/metadata/SQLite boundary and cold-rebuild from objects.
- Two-instance lease loss/takeover produces one committed generation/transition.
- Compaction/retention fixtures prove event identity and locator repair; eligibility
  fixtures prove span events and missing/malformed/non-canonical IDs never project.
- Revision conflicts, duplicate commands, ignore expiry, late events, resolve/
  regression, aliases, and grouping migrations preserve counts/audit.
- Valkey-free declared single-instance and fail-closed accidental multi-instance
  configurations are explicit.

## Open questions for review

- Lifetime or retention-window aggregates?
- Which workflow operations commute and which require strict expected revision?
- Exact versus approximate affected-user counts and privacy defaults?
- Does v1 support merge/alias, or only versioned automatic groups and resolve?
- Required freshness SLO relative to minimum raw retention?
- How is single-instance exclusivity declared/proved operationally?

## References

- [Error monitoring](error-monitoring.md)
- [Error grouping](error-grouping.md)
- [Alert evaluation](alert-evaluation.md)
- [Errors and alerts UI/API](error-monitoring-ui.md)
- `crates/catalog/src/{lib,snapshot}.rs`
- `crates/cluster/src/poll.rs`; `crates/valkey/src/lease.rs`
