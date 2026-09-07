# Alert and monitor evaluation — Design

Status: draft, not yet implemented
Owner: Bart
Last updated: 2026-09-07

## Implementation status

This document consumes generic queries and issue transitions. It produces durable
intents governed by [Notification delivery](notification-delivery.md).

### Done

- [x] **Execution survey.** Query admission/memory guards, maintenance scheduling,
  Valkey leases, status, and current Alerts deferral have been reviewed.

### Outstanding

- [ ] **Decision — role topology.** Confirm separate alert role versus a combined
  control role with independent engines and budgets.
- [ ] **Phase 0 — durable model.** Define versioned monitor/revision/state/
  transition/evaluation records and rebuildable projection.
- [ ] **Phase 1 — evaluator.** Add completion-relative scheduling, jitter,
  admission, query-wire client, leases, and state-machine semantics.
- [ ] **Phase 2 — issue monitors.** Consume new/regressed/resolved/severity/release
  transitions with exact deduplication.
- [ ] **Phase 3 — scalar monitors.** Evaluate bounded queries, thresholds, `for`,
  recovery, no-data, and execution-error policies.
- [ ] **Phase 4 — control API/status.** Add revisioned CRUD, validation/test,
  silences, history, staleness, and fleet observability.
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
- Atomically commit transitions with notification intents before delivery.
- Bound schedules, concurrency, queueing, result shape/bytes, and history.

## Non-goals (v1)

- PromQL compatibility, arbitrary DataFusion SQL returning unbounded rows, live
  queries, arbitrary code, anomaly/ML detection, or distributed joins.
- Exactly-once remote notifications.
- Using queryd fleet status as rule truth.
- Per-user authorization beyond the webui's initially all-or-nothing admin session.
- Inhibition trees or full Alertmanager configuration compatibility in phase one.

## Ownership and authority

A reusable `scry-alert` engine and thin `scry-alertd` role (`scry alert`) are the
initial shape. `alerts.sqlite` is a local projection. Authoritative immutable
objects live under the suite's reserved namespace; Valkey supplies per-rule leases
and hints only. Alertd evaluates through configured, allowlisted queryd targets via
a small query-wire client rather than calling `QueryService` internals.

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

V1 validation requires either one numeric/boolean scalar row or a bounded set of
labeled scalar groups defined by a typed schema. An empty set is no-data, not zero,
unless the reducer contract says `count(empty)=0`. NaN/Inf handling is explicit.
Unbounded time, live mode, DDL/DML, filesystem/object functions, or ambiguous
schemas are rejected. A validation/test run uses normal admission but creates no
state transition or outbox event.

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
  no_data: Inactive|Pending|Firing|NoData,
  execution_error: KeepLast|Error|Firing,
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

Immutable rule revisions and tombstones are illustrative:

```text
_scry/alerts/v1/rules/<rule-id>/<revision>.json
_scry/alerts/v1/rules/<rule-id>/tombstones/<command-id>.json
```

Mutation holds `lease/alert/rule/<id>` in clustered mode and uses expected revision.
Changing query/condition/`for` resets Pending; cosmetic labels/annotations need not.
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
them. `KeepLast` retains prior health but marks it stale/error—it never reports OK.
No-data policy is separate from execution-error policy. Group identity is a wide
cryptographic digest of canonical typed labels plus collision disambiguator.

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

## Scheduling

Intervals define wall-clock-aligned logical slots: `slot_id = floor(time/every)`.
Deterministic per-rule jitter changes execution time, never slot identity or query
window. On startup reconcile durable rule/state objects, evaluate at most the newest
eligible overdue slot, and record older skipped slots. Never replay every missed
interval. While running use monotonic wakeups mapped to slots; persist wall timestamps
for recovery/audit. Clock jumps are bounded and surfaced. Slow completion cannot
create uncovered gaps or shift future windows.

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
6. commit state transition plus complete notification intents metadata-last;
7. publish a hint and release; delivery occurs separately.

Do not hold a lease while waiting on external notification I/O.

## Late data and evaluation windows

Queries use explicit half-open `[slot_end-lookback, slot_end)` event-time windows,
plus a configured lateness allowance/watermark policy. Received time is retained
for diagnosis. Window overlap is intentional when lookback exceeds interval; there
are no gaps from execution duration. Re-evaluating a past slot must not emit another
transition with a new identity. Review must define whether late data can revise a
prior scalar value, trigger only the current state, or cause an audited late transition.

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

Per-rule leases distribute work and avoid a global leader. Deterministic identities
and folds remain necessary. In a declared single alertd deployment,
`LocalLeaseProvider` is correct without Valkey. If exclusivity is not declared or
multiple instances are possible, mutations/evaluation/delivery fail closed unless
an explicitly unsafe override is selected. Raw telemetry and existing state reads
remain available.

## API and observability

Control endpoints provide monitor CRUD with `Idempotency-Key`/`If-Match`, validation,
test evaluation, state/groups, cursor-paginated history, silences, and enable/
disable. Webui proxies to an allowlisted alertd and adds CSRF/origin defense; see
[error-monitoring-ui.md](error-monitoring-ui.md).

Fleet role `alert` reports rules enabled, due/running/queued, oldest due age,
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

## Open questions for review

- Separate `scry alert` and `scry errors`, or one control role with hard resource
  isolation?
- V1 scalar-only rules or bounded labeled multi-dimensional alert instances?
- Late-data revision policy for already evaluated windows?
- Which no-data/error defaults minimize surprise without hiding monitoring failure?
- Are silences required before first notifier rollout?
- How does a no-Valkey deployment declare/prove one alertd instance?

## References

- [Error monitoring](error-monitoring.md)
- [Issue lifecycle](error-issues.md)
- [Notification delivery](notification-delivery.md)
- [Errors and alerts UI/API](error-monitoring-ui.md)
- `crates/server/src/query_service.rs`; `crates/server/src/memory_guard.rs`
- `crates/compact/src/resource.rs`; `crates/valkey/src/lease.rs`
- `proto/query.schema.json`
