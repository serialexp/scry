# Alert and monitor evaluation — Design

Status: partial — first scalar alerting vertical slice implemented; delivery, issue monitors, snapshots, and full qualification outstanding
Owner: Bart
Last updated: 2026-09-26

## Implementation status

This document consumes generic queries and, in a later slice, issue transitions. It
produces durable alert state; once delivery is implemented, delivery-bearing
transitions also produce durable intents governed by
[Notification delivery](notification-delivery.md). The first ungrouped scalar slice
now has a separate alert domain/role, immutable rule revisions and state heads,
`alerts.sqlite`, bounded query-wire evaluation, local/Valkey lease modes, a private
control API, secure webui proxy, and browser CRUD/state views. Delivery,
issue-transition monitors, snapshots, richer history/status, and full clustered
qualification remain outstanding.

### Done

- [x] **Execution survey.** Query admission/memory guards, maintenance scheduling,
  Valkey leases, status, and current Alerts deferral have been reviewed.
- [x] **First vertical slice — scalar role and control plane.** Added the separate
  `scry-alert`/`scry alert` role, immutable revision/state records with conditional
  heads, deployment-bound `alerts.sqlite`, direct bounded query-wire scalar client,
  aligned scheduling, local lock or Valkey per-monitor leases, private bearer-token
  CRUD/validate/test API, secure webui proxy, and browser CRUD/state views. Delivery
  is intentionally outside this slice.
- [x] **Phase 0a — initial durable model.** Versioned monitor, transition, empty
  intent array, rule/state head, tombstone, mutation-receipt, and SQLite projection
  contracts exist. Storage tests cover immutable replay, tombstone anti-resurrection,
  and competing head CAS; reads pin object versions and validate the winning state
  head's transition identity and digest before folding.
- [x] **Phase 1a — initial evaluator.** Scalar evaluation uses ordinary queryd
  admission with deadlines and cumulative frame/byte/row limits, newest completed
  aligned slots, deterministic jitter, Pending/Recovering persistence, explicit
  no-data/error policies, Valkey leases, and an explicit locally locked mode.
- [x] **Phase 1b — evaluator hardening (D-076 follow-up, 2026-09-26).**
  - Staleness is ordered by `(monitor_revision, slot_id)`, so an interval edit
    cannot strand a monitor behind an older revision's larger slot IDs.
  - A `--evaluation-delay` lateness allowance defaults to 90 s, above ingest's
    60 s block flush age.
  - The rule and state head are re-read durably under the lease before the query
    and again before the commit. Covered slots are never queried twice.
  - Recovering plus a breaching value returns to Firing. A `KeepLast` error never
    restarts a `for` hold.
  - A NoData/Error status entered from Firing or Recovering remembers what it
    interrupted. A true condition resumes Firing with its original `since`, and a
    false one goes through Recovering. The transition is marked `resumed`, so
    delivery treats it as a continuation rather than a second Firing.
  - State head v2 embeds the full state. A transition object is written only when
    the status changes.
  - The scheduler runs bounded concurrent tasks and reconciles in its own task. It
    shuts down cleanly.
  - Rule and target revision objects are keyed per command, so a crashed command
    cannot block later saves.
  - The projection load quarantines individual bad records instead of failing the
    pass, and replaces `alerts.sqlite` schema v2 with v3.

### Outstanding

- [ ] **Phase 2 — issue monitors.** Consume new/regressed/resolved/severity/release
  transitions with exact deduplication.
- [ ] **Phase 3b — complete scalar monitors.** Add expression/function/physical-plan
  allowlisting beyond the current single-statement/single-signal-relation boundary,
  configurable evaluator budgets, durable skip/error audit history, richer late/clock
  diagnostics, incremental convergence, and representative-scale scheduler
  qualification. Basic ungrouped threshold, `for`, recovery, no-data, and
  execution-error behavior is implemented.
  - The scheduler tick itself has a 100k-monitor benchmark (`#[ignore]`d
    `tick_scales_to_the_projection_bound`, 250 ms budget).
  - The 30 s reconcile still issues one head GET per monitor and per target. That
    is O(N) object-store reads per pass until incremental convergence lands.
  - Hold timers reset on *every* rule revision, including cosmetic ones. Resetting
    only on semantic edits (query, condition, `for`, recovery) is outstanding.
  - Two-pass GC of unreachable transition candidates and superseded state is
    outstanding.
  - A chained transition's `previous_transition_key` is taken from the prior head
    without re-reading that transition.
- [ ] **Phase 4b — complete control API/status.** Add cursor-paginated transition
  history, silences, manual reconciliation/status, fleet registration, projection
  lag/staleness, and snapshot health. Basic revisioned CRUD, validate/test, current
  state, service auth, CSRF proxy, and browser views are implemented.
- [ ] **Phase 5 — verification.** Crash, clock, late-data, overload, edit/delete,
  failover, and notification-intent atomicity tests.

## Why this exists

Running `SELECT count(*)` every minute is not an alert engine. Useful alerting must
remember pending/firing state, distinguish no data from execution failure, avoid
replaying every missed tick after restart, suppress stale rule results, and create
one durable notification intent per transition. It must also use Scry's existing
query budgets instead of opening an unbounded background execution path.

This design supports both Sentry-like issue events and broader bounded telemetry
thresholds while keeping detection separate from delivery.

## Goals

- Persist versioned monitor definitions and alert-instance state durably.
- Detect issue transitions and scalar query thresholds with explicit semantics.
- Run through queryd's ordinary admission, DataFusion pool, cgroup guard, timeout,
  and typed resource-error path.
- Preserve pending time and transition identity across restart/failover.
- Make no-data, stale, disabled, and execution-error states visible and distinct.
- When delivery is enabled in a later slice, atomically commit transitions with
  notification intents before any external delivery.
- Bound schedules, concurrency, queueing, result shape/bytes, and history.

## Non-goals (v1)

- PromQL compatibility, arbitrary DataFusion SQL returning unbounded rows, live
  queries, arbitrary code, anomaly/ML detection, or distributed joins.
- Exactly-once remote notifications.
- Using queryd fleet status as rule truth.
- Per-user authorization beyond the webui's initially all-or-nothing admin session.
- Inhibition trees or full Alertmanager configuration compatibility in phase one.

## Ownership and authority

D-076 selects a reusable `scry-alert` engine and separate thin `scry-alertd` role
(`scry alert`) for the first vertical slice; alert evaluation is not folded into
errorsd, queryd, or webui. `alerts.sqlite` is a local projection. Authoritative
immutable objects live under the suite's reserved namespace; Valkey supplies
per-rule leases and hints only. The first slice is clustered from the outset and
also supports an explicitly configured local single-writer mode. Alertd connects
directly to configured, allowlisted queryd targets via a small query-wire client
rather than routing through webui or calling `QueryService` internals.

This preserves queryd admission (`active`, `waiting`, queue timeout), DataFusion
memory, cgroup guard, default-window validation, and resource errors. Alert queries
receive a separate low-priority/alert admission class if queryd later supports
classed reservations; until then alertd's concurrency must be conservative and
normal socket admission is the common ceiling.

## Monitor types

### Issue-transition monitor

Consumes durable issue transition IDs from `scry errors`:

- new issue;
- regression/reopen;
- first seen in release/environment;
- severity escalation;
- resolved/ignored/assigned where enabled.

Filters include app/service, environment, release, severity, handled state,
exception type, grouping quality, tags/facets, and issue ownership. One transition
is consumed idempotently by identity; it is not rediscovered by repeatedly querying
current issue state.

### Scalar query monitor

Stores a domain query independent of generated wire structs:

```text
AlertQuery {
  target_id, signal, equality_matchers,
  explicit lookback, bounded SQL/expression, row_limit, live=false
}
Condition {
  reducer: Count|Min|Max|Sum|Avg|Last,
  comparator: Lt|Lte|Gt|Gte|Eq|Ne,
  threshold: finite f64
}
```

The first vertical slice accepts only **ungrouped** restricted SQL that produces
exactly one numeric/boolean scalar. Grouped/labeled result sets are deferred. An
empty result is no-data, not zero, unless the reducer contract says
`count(empty)=0`; multiple rows or columns are invalid. NaN/Inf handling is
explicit. Current AST validation requires exactly one read-only query whose only
relation, including nested queries, is the selected telemetry table; it rejects
DDL/DML, joins, CTEs, table-valued/derived sources, and additional relations. The
query-wire request supplies the explicit bounded time window and disables live mode.
A function/expression and physical-plan allowlist is still outstanding, so the
current implementation must not be described as proving that every filesystem,
object, or network-capable scalar function is excluded. A validation/test run uses
normal admission but creates no state transition or notification intent.

## Durable rule model

Indicative versioned definition:

```text
Monitor {
  schema_version, id, revision, name, enabled, kind,
  schedule: FixedInterval { every, deterministic_jitter },
  query_or_issue_filter,
  condition,
  for_duration,
  recover_for,
  no_data_policy: Inactive|Pending|Firing|NoData,
  execution_error_policy: KeepLast|Error|Firing,
  error_for_duration,
  repeat_interval,
  notify_on_resolve,
  labels, annotations, notifier_ids,
  created_at, updated_at
}
```

Arbitrary executable notifier/query blobs are not persisted. Durations, maps,
annotations, targets, group cardinality, and SQL size are bounded. Notifier
references are validated but secrets remain in destination configuration.

Immutable rule revisions and tombstones:

```text
_scry/alerts/v1/rules/<rule-id>/head.json
_scry/alerts/v1/rules/<rule-id>/revisions/<revision:020>-<sha256(command-id)>.json
_scry/alerts/v1/rules/<rule-id>/tombstones/<sha256(command-id)>.json
```

The command digest in the revision key keeps two commands that both try revision
`n` apart. A command that crashed or lost its head CAS leaves only an unreachable
revision object, and a later save of revision `n` writes its own key. The head's
exact `revision_key` is the only visibility authority, and a head advances only
from revision `n-1` to `n`. Notification targets use the same layout under
`_scry/alerts/v1/targets/<target-id>/`.

Mutation holds `lease/alert/rule/<id>` in clustered mode and uses expected revision.
Changing query/condition/`for` resets Pending; cosmetic labels/annotations need not.
*Implemented:* the status carries across revisions, but Pending and Recovering hold
time resets on any revision change, cosmetic ones included. Semantic-only resets are
outstanding.
Disable moves to Disabled and sends no resolve unless explicitly configured.

## Alert-instance state machine

Per `(monitor, group)` state:

```text
Inactive -> Pending -> Firing -> Recovering -> Inactive
                 \-> NoData
                 \-> Error
                 \-> Disabled
```

Persist rule revision, status, `since`, last evaluation/value, next due time,
consecutive failures, error class, transition sequence, and last notification as
immutable evaluation/state records or a causal transition chain under
`_scry/alerts/v1/state/<monitor>/<group>/<unique-record>.json`; `alerts.sqlite` is
only its fold. Each successful grouped evaluation records the complete bounded group
set. A disappeared group follows explicit no-data/recovery policy and inactive group
state has an audited TTL. Group overflow rejects the whole evaluation as a resource
error; it never evaluates a truncated set.

`for_duration` and recovery duration use persisted `since`; restart does not reset
them. `Recovering` is a durable state, not an in-memory/display-only derivation, so
its start time and progress survive restart and clustered takeover. Every rule must
store explicit no-data and execution-error policies; neither inherits a hidden
deployment default. `KeepLast` retains prior health but marks it stale/error—it never
reports OK. Under the `NoData`/`Error` policies an outage that interrupts Firing or
Recovering records that active status; the alert was never observed to resolve, so
the outage ending resumes it (true condition → Firing, keeping the original `since`,
marked `resumed` so no second Firing notification is sent; false condition →
Recovering) instead of restarting at Pending or jumping to Inactive. Group identity is a wide cryptographic digest of canonical typed labels
plus collision disambiguator (needed only after grouped rules are introduced).

Transition sequence is monotonic per `(monitor, group)` across cosmetic and semantic
rule revisions. Every state change has a deterministic key from deployment, monitor/
group, sequence, effective semantic revision, evaluation slot, and kind and is
published with `PutMode::Create`. Different bytes at that key are a conflict, never
an overwrite. Firing, Resolved, and optional Reminder intents are embedded in the
bounded transition commit object (preferred) or referenced with exact key/length/
digest so visibility has one commit point. The state head advances by ETag CAS and
is the visibility authority: readers fold only transition records reachable from the
winning head's causal chain. A losing or crashed writer may leave an unreachable
transition candidate; two-pass GC reaps it only after the head-retention horizon.
A reminder can be coalesced; firing/resolved cannot.

*Implemented (ungrouped, state schema v2):*

```text
_scry/alerts/v1/state/v2/<monitor>/head.json
_scry/alerts/v1/transitions/v2/<monitor>/<sequence:020>-<revision:020>-<slot:020>-<status>.json
```

**State head.** The head embeds the complete `AlertState`: status, `since`, last
slot, value, stale flag and sequence. It also carries a `TransitionRef` naming the
latest transition, with that transition's sequence, revision, slot, status and
SHA-256. Every evaluated slot advances the head by CAS.

**Transitions.** A transition object is published with `PutMode::Create`, before
the head, and only when the status changes. It records `previous_status` and chains
to its predecessor through `previous_transition_key`. The sequence therefore counts
status changes, not evaluations.

**Staleness order.** Staleness is ordered by `(monitor_revision, slot_id)`. Slot IDs
are only comparable within one revision because `every` may change, so an older
revision can never overwrite a newer one.

**Schema v1.** Version-1 heads and transitions are not read. Upgrading restarts
each monitor's state and sequence.

**Local projection.** `alerts.sqlite` (schema v3) projects only rules, current state
and targets. It has no transition table, and the v2 → v3 migration rebuilds the
projection from the bucket. The projection keeps a deleted rule or target as a
tombstone row, which stops a reconcile or late evaluation from resurrecting it. A
reconcile prunes only rows that were missing from its listing and folded before the
pass began.

## Scheduling

Intervals define wall-clock-aligned logical slots: `slot_id = floor(time/every)`.
Deterministic per-rule jitter changes execution time, never slot identity or query
window. On startup reconcile durable rule/state objects and evaluate at most the newest
eligible overdue slot. Never replay every missed interval. Future scheduler
qualification must prove that slow completion cannot create uncovered gaps or shift
future windows.

*Implemented scheduler:*

- **Tick.** The scheduler ticks once per second over a keyset-paginated projection.
  Each due, uncovered slot is dispatched to a spawned task.
- **Bounds.** At most `MAX_CONCURRENT_EVALUATIONS` (8) tasks run at once, behind a
  global semaphore. An in-flight set stops a monitor from being dispatched twice. A
  slow evaluation therefore never blocks the tick or other monitors.
- **Fairness.** When capacity runs out, the next tick resumes after the last
  dispatched monitor (round-robin), so an early monitor ID cannot starve later ones.
- **Lease backoff.** When the lease backend is unavailable, dispatch backs off for
  5 s.
- **Reconciliation.** Full bounded object-store reconciliation runs in its own task
  every 30 s.
- **Shutdown.** SIGINT/SIGTERM stops dispatch and drains in-flight evaluations for
  up to 10 s. After that it aborts them, and an aborted evaluation has not committed.
- **Not yet done.** The scheduler does not persist an audit record for skipped older
  slots. It does not use convergence hints or incremental cursors, and it does not
  surface clock-jump diagnostics.

No overlapping evaluation for the same `(rule, group, slot)`. Reevaluating a slot
uses the same identity and cannot allocate a new transition sequence. Global controls
include max concurrent/queued evaluations, queue timeout, per-query deadline, max
result rows/bytes/groups, maximum due lag, and backoff after resource errors.
Long-running rules cannot consume all slots; weighted/fair admission across monitors
is required before grouped rules can fan out.

For each due unit:

1. acquire `lease/alert/eval/<rule-id-or-shard>`;
2. re-read latest durable rule/state and suppress stale/deleted revision;
3. execute the admitted query or consume transition;
4. classify value/no-data/error and compute state using injected time;
5. recheck rule revision and lease fence;
6. commit state transition metadata-last, including complete notification intents
   only after delivery is enabled in a later slice;
7. publish a hint and release; eventual delivery occurs separately.

The current evaluator (`scry_alertd::evaluate_monitor_slot`) implements steps 1–6 as
follows. Step 7 has no hint yet.

1. Acquire the lease and check the fence.
2. Re-read the rule head:
   - deleted: tombstone the local row;
   - newer revision: fold it and return `Superseded`;
   - behind: error.
3. Re-read the state head:
   - newer revision: `Superseded`;
   - already covers the slot: fold it and return `AlreadyCovered` without querying.
4. Query, or observe NoData when the monitor is disabled.
5. Compute the state.
6. Check the fence and the rule head again.
7. Create the transition, only when the status changed.
8. CAS the state head. A lost CAS folds the winner and returns `Superseded`.
9. Fold into `alerts.sqlite`.

Do not hold a lease while waiting on external notification I/O.

## Late data and evaluation windows

Queries use explicit half-open `[slot_end-lookback, slot_end)` event-time windows,
plus a configured lateness allowance before the slot's one evaluation. Received time
is retained for diagnosis. Window overlap is intentional when lookback exceeds the
interval; there are no gaps from execution duration. Once evaluated, a past slot is
never revised for late data and is not re-evaluated to change prior value or state.
Late arrivals can affect only a later slot whose window includes them. This keeps one
final result and transition identity per slot and avoids retrospective alert churn.

The lateness allowance is `scry alert --evaluation-delay <seconds>` (default 90,
range 0–3600). A slot ending at `t` is not evaluated before `t + delay +
jitter`. The default exceeds ingest's 60 s `block_max_age_secs`: an ingester buffers
records for up to that long before a block becomes queryable, so with a shorter delay
the slot's one evaluation would miss data that is merely unflushed. Raise it when
ingest flushes more slowly or clocks are skewed.

Issue transitions carry their own durable occurrence/transition time and are
consumed once. A late old occurrence cannot become a regression unless the issue
lifecycle policy classified it as one.

## Silences, cooldowns, and reminders

A silence is a revisioned immutable control record under
`_scry/alerts/v1/silences/<silence-id>/<revision-or-command>.json`, matching monitor/
group labels and a bounded time interval. It suppresses delivery, not evaluation/
history. The
state and “would have notified” intent remain explainable. Expired silences need no
mutable deletion. Cooldown/repeat intervals suppress/coalesce Reminder intents only;
they never suppress Firing or Resolved transitions.

V1 may defer inhibition between alerts, but must not emulate it by dropping
transitions. Maintenance windows can be modeled as silences with explicit actor and
expiry.

## Degraded and failure semantics

- Valkey unavailable/lease lost: pause commit-requiring evaluation, keep prior
  durable state visible as stale, and never infer recovery.
- Query resource rejected/timeout/network error: classify execution error, apply
  policy after configured duration, back off, and avoid fleet-wide error storms.
- No data: apply independent no-data policy.
- Rule edited/deleted during query: latest-revision check plus conditional state-
  head CAS rejects the stale result even if its request arrives late.
- Crash after query/before commit: next holder reevaluates; no intent exists.
- Crash after transition/intent commit: projection and dispatcher resume.
- Object-store write/list failure: no transition or delivery without confirmed
  durable intent.
- Process restart: one bounded catch-up, persisted Pending/Firing duration retained.
- queryd target down: fail over only among configured equivalent targets; target
  identity remains in evaluation history.

## Multi-instance and no-Valkey behavior

The first slice supports clustered operation, rather than postponing fencing until a
later phase. Per-rule leases distribute work and avoid a global leader; deterministic
identities and folds remain necessary. An explicit local single-writer mode may use
`LocalLeaseProvider` without Valkey and must hold an exclusive local process lock.
It is a distinct operator-selected mode, not a fallback inferred from Valkey being
absent. Clustered mode fails closed on unavailable fencing or lease loss. Raw
telemetry and existing state reads remain available.

Clustered startup does not wait for Valkey. The control API and projection come up
immediately. Valkey is connected in the background, with a 10 s connect timeout and
retry backoff capped at 30 s. Until it connects, evaluation and target test sends
fail closed because they need a lease. CRUD stays available because it is guarded by
head CAS, not a lease. The Valkey lease acquire (`SET NX PX`) is
bounded by `min(ttl/3, 5 s)`. A timed-out acquire releases its possibly-applied
token best-effort, so an unresponsive Valkey cannot hang a caller or silently hold a
lease.

## API and observability

The implemented control endpoints provide monitor list/get/create/update/delete,
validation, and side-effect-free test evaluation. Mutations use UUID
`Idempotency-Key` values; create/update persist request-digest receipts for exact
replay, delete persists a command tombstone, and updates/deletes also require
`If-Match`. The webui proxies these routes only to an allowlisted alertd and adds
CSRF/origin defense; see [error-monitoring-ui.md](error-monitoring-ui.md). Transition
history, groups, silences, manual reconciliation/status, and destination/delivery
routes remain outstanding.

The target fleet role `alert` reports rules enabled, due/running/queued, oldest due age,
evaluation outcomes/resource rejections, state counts/staleness, transitions,
silences, query latency/targets, outbox pending/age, delivery summary, lease health,
and local projection reconcile/snapshot health. No query text, secret, error body,
or user value enters status.

## Verification

- Pure state-table tests across thresholds, `for`, recovery, no-data, errors,
  reminders, disable/edit, late values, NaN, and injected clocks.
- Scheduler tests for deterministic jitter, completion-relative cadence, restart
  catch-up, no overlap, fairness, queue/deadline/result bounds, and cancellation.
- SQL validation parses exactly one statement, allowlists tables/functions/operators,
  rejects external table/filesystem/network features, verifies explicit time bounds,
  and inspects the plan; queryd integration proves normal admission and resource
  errors cannot bypass memory guards.
- Grouped evaluation tests reconcile the complete bounded group set, reject rather
  than truncate overflow, define absent-group no-data/recovery, and GC inactive state.
- Crash-point/object-failure tests prove no delivery without durable transition.
- Two alertd instances prove one transition; takeover retains Pending/Firing time.
- Valkey loss marks stale and resumes without duplicate Firing/Resolved intents.

## Selected first-slice decisions and remaining review

D-076 resolves the first-slice choices: a separate `scry alert` role; clustered
operation plus explicit local single-writer mode; ungrouped restricted scalar SQL;
explicit per-rule no-data and execution-error policies; persisted `Recovering`; no
late revision of evaluated slots; browser CRUD under shared-admin authorization;
and no notification delivery. These are design decisions, not implementation claims.

Still open for later slices:

- Which concrete no-data/error policy choices should the rule editor suggest while
  still persisting the operator's explicit selection?
- Are silences required before the first notifier rollout?
- What grouped-result identity and cardinality limits apply when grouped monitors
  are introduced?

## References

- [Error monitoring](error-monitoring.md)
- [Issue lifecycle](error-issues.md)
- [Notification delivery](notification-delivery.md)
- [Errors and alerts UI/API](error-monitoring-ui.md)
- `crates/server/src/query_service.rs`; `crates/server/src/memory_guard.rs`
- `crates/compact/src/resource.rs`; `crates/valkey/src/lease.rs`
- `proto/query.schema.json`
