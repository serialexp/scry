# Notification delivery — Design

Status: draft, not yet implemented
Owner: Bart
Last updated: 2026-09-07

## Implementation status

This document delivers intents created by
[Alert evaluation](alert-evaluation.md). Delivery never decides alert state.

### Done

- [x] **Failure-model survey.** Existing HTTP client/TLS/rotating-secret patterns
  and lease semantics have been reviewed.

### Outstanding

- [ ] **Decision — first integrations.** Confirm generic HTTPS webhook first and
  whether Slack webhook ships in the same phase.
- [ ] **Phase 0 — destination model.** Implement revisioned metadata, secret
  references, validation/test, TLS profiles, and redacted API views.
- [ ] **Phase 1 — durable outbox.** Project committed intents and attempts with
  at-least-once identity, retry schedules, dead letters, and manual retry.
- [ ] **Phase 2 — webhook worker.** Add bounded pooled HTTP delivery, response
  classification, idempotency, templates, and observability.
- [ ] **Phase 3 — integrations.** Add Slack formatting, then SMTP/on-call adapters
  only through the common delivery contract.
- [ ] **Phase 4 — verification.** Crash-after-send, Retry-After, rotated secrets,
  malformed templates, rate limits, SSRF, redaction, and multi-instance tests.

## Why this exists

A monitor transition and an HTTP request are not one transaction. If Scry sends
before recording intent, a crash loses the notification; if it records success
before sending, it lies; if the remote accepts and the response is lost, retry may
duplicate. A Valkey lease prevents concurrent workers but cannot remove this
fundamental uncertainty.

This design therefore promises durable, observable, bounded **at-least-once**
delivery. Alert evaluation commits a complete notification intent first. Independent
workers deliver it, persist attempts, and isolate destinations from one another.

## Goals

- Never perform external I/O without a durable notification intent.
- Resume pending/retrying delivery after crash or local projection loss.
- Provide stable event/idempotency identity to cooperative receivers.
- Bound concurrency, queue age, retries, response bodies, templates, and clients.
- Keep secrets out of durable objects, SQLite, status, APIs, logs, and payload
  previews.
- Allow one broken destination to fail independently.
- Expose test, history, dead-letter, and manual retry workflows.

## Non-goals (v1)

- Exactly-once external effects.
- Arbitrary code/templates, browser-supplied destinations, HTTP redirects to
  unvalidated hosts, or credentials embedded in rule JSON.
- Receiving Slack/PagerDuty callbacks or mutating issues from chat.
- Full Alertmanager routing/inhibition/template compatibility.
- Treating successful socket write as acceptance.

## Intent contract

Alert evaluation commits a complete rendered snapshot sufficient to deliver later
without rereading mutable rule/issue state:

```text
NotificationIntent {
  schema_version,
  event_id, monitor_id, monitor_revision, alert_group, transition_seq,
  kind: Firing|Resolved|Reminder,
  notifier_id, pinned_non_secret_notifier_revision,
  created_at, starts_at, optional ends_at,
  rule_name, labels, annotations,
  condition_summary, value_summary,
  issue_summary/link or query summary,
  template_version,
  idempotency_key
}
```

Fields and rendered sizes are bounded and scrubbed. No secret value is included.
`event_id` is deterministic over deployment, monitor revision/group, transition
sequence, kind, and destination. One intent per destination prevents a failed
Slack target from blocking a webhook. Firing and Resolved are never coalesced;
Reminder creation may be coalesced by evaluator policy.

Intent lives inside the committed alert transition or in a deterministic object
that transition commits atomically by reference. Visibility must have one commit
point; readers never see state as Firing without its required intents.

## Destination model

```text
Notifier {
  schema_version, id, revision, name, enabled, kind,
  endpoint metadata,
  secret_ref,
  timeout,
  tls_profile { ca_file, insecure_skip_verify=false },
  bounded headers/format options,
  created_at, updated_at
}
SecretRef = OpaqueConfiguredSecretId(id)
```

Definitions are immutable revisions in object storage and projected locally.
Mutation uses expected revision and a lease in clustered mode. Intents pin all
non-secret delivery metadata to one immutable notifier revision; only its secret
value is resolved live so rotation remains possible and historical routing cannot
change silently. Referenced revisions/tombstones outlive every dependent intent.
API responses show only metadata and an opaque secret ID, never paths, environment
names, or values. Operators map IDs at startup to allowlisted files under a
canonical secret root (rejecting symlink/escape) or allowlisted environment names.
API users cannot select arbitrary process-readable files. Resolve file values on
each attempt so Kubernetes-projected rotation is followed. Terminal newlines are
trimmed where the secret format calls for it.

Generic webhook requires HTTPS by default, validates a configured absolute URL,
rejects userinfo/fragments and non-allowlisted schemes, caps headers, and does not
permit arbitrary `Host`, `Content-Length`, hop-by-hop, idempotency, or signature
header overrides. Exact external host/CIDR policy is startup configuration, not API
input. Resolve all A/AAAA answers, reject loopback/private/link-local/multicast and
cloud-metadata ranges by default, pin a validated address for the connection, and
revalidate DNS/IP on every redirect; redirects are disabled initially. Explicit
internal-destination exceptions are operator configuration. Disable environment/
system proxy inheritance. No ambient cookies, proxy/cloud credentials, or browser
headers are attached.

TLS verification defaults on. Custom CA adds to built-in roots. Skip verification
is explicit, prominent, and status-visible without revealing destination details.
Pooled `reqwest::Client`s are keyed by TLS profile, following the agent scrape
pattern, with bounded idle pools.

## Durable outbox projection

Illustrative authoritative records:

```text
_scry/alerts/v1/transitions/...        # contains/references intents
_scry/alerts/v1/outbox-results/<event-id>/<attempt-id>.json
```

There is no mutable “pending object” that must be atomically overwritten. Local
`alerts.sqlite` folds committed intents and immutable attempt results into:

```text
outbox(event_id PRIMARY KEY, notifier_id, status, next_attempt_at,
       attempt_count, first/last_attempt_at, last_error_class, ...)
attempts(attempt_id PRIMARY KEY, event_id, started_at, finished_at,
         outcome, response_class, retry_after, worker_id, ...)
```

Status is `pending | delivering | retrying | succeeded | dead`. `delivering` is a
leased local observation; durable absence of a result makes it eligible again
after lease expiry. A success result is written only after the remote reports an
accepted status. Failed result records carry redacted classifications, not response
bodies containing possible secrets.

Dispatcher acquires `lease/alert/deliver/<event-id>` (the event already identifies
destination), re-folds durable attempts, claims the outbox head with ETag `If-Match`,
resolves the live secret named by the intent's pinned non-secret notifier revision,
sends, then conditionally persists the result/head. Attempt IDs are UUIDv7 records
carrying deterministic event identity and use create-if-absent. Success is absorbing;
late failure CAS cannot reopen it. Durable retry scheduling and maximum-attempt
accounting fold attempt IDs, and manual retry is an immutable authorized command.
Do not hold evaluator leases. Conditional writes provide storage exclusion; the
lease distributes work and bounds duplicate remote sends but exactly-once remains
impossible across remote acceptance and local success CAS.

## Delivery and retry semantics

Send `Idempotency-Key: <event-id>` where protocol permits and include the same ID in
versioned payloads. Cooperative webhook receivers can deduplicate. Slack incoming
webhooks and SMTP may not, so possible duplicates are part of the contract.

Classification:

- accepted 2xx: durable success;
- DNS/connect/TLS/timeout, 408, 425, 429, and 5xx: retry;
- 409 retries only when destination contract says idempotent conflict is transient;
- other 4xx: permanent/dead immediately;
- oversized/invalid response is classified without buffering unbounded content;
- honor bounded `Retry-After`, otherwise exponential backoff with full jitter.

Configuration sets maximum attempts and maximum age. Exhaustion writes durable
`dead`, remains visible, and does not block later events. Manual retry creates a
new attempt for the same event ID after authorization; it does not invent a new
Firing transition. Destination disable pauses pending delivery with visible reason
unless policy explicitly dead-letters; deletion is a tombstone and cannot erase
history.

Crash cases:

- before send: lease expires and another worker sends;
- after remote acceptance/before success record: retry can duplicate;
- after failure/before result: retry is conservative;
- after result: fold prevents another send;
- projection loss: object replay reconstructs eligibility.

## Templates and payloads

Templates are versioned, bounded, non-Turing-complete field selection/formatting.
No network/file/env access, loops over unbounded event data, or secret interpolation.
Render and scrub at intent creation so later rule/template edits cannot rewrite
history; destination adapters may add protocol framing only.

Generic webhook emits a stable JSON schema with transition, labels, annotations,
value/issue summary, timestamps, links, and event ID. It can optionally sign exact
body bytes with an HMAC secret reference and timestamp; signature version and
replay window are documented. Slack adapter maps the same intent into bounded
blocks/text and includes Scry link and transition, without owning alert logic.
SMTP later uses the same snapshot and Message-ID derived from event ID.

Links are built from configured public Scry base URL and typed IDs, never from
untrusted Host headers. Payload previews in UI are scrubbed and clearly exclude
secret-derived headers/signatures.

## Resource isolation

Independent limits: pending projection scan, max concurrent deliveries globally
and per destination/host, bounded waiters, request timeout, connect timeout,
response header/body bytes, retry computations, client pool, template output, and
dead/history retention. Fair scheduling prevents one failing endpoint from
occupying every slot. Destination rate limits and circuit-breaker/backoff do not
stop unrelated destinations.

Delivery work is never executed on ingest or interactive query task pools. If
outbox age exceeds SLO, status/alerts report it; workers do not bypass bounds.
Notification-on-notification recursion is prevented by a separate local/operator
health channel rather than writing delivery failures into the same monitored rule
without inhibition.

## Integrations and phasing

1. generic HTTPS JSON webhook with optional HMAC;
2. Slack incoming webhook formatting;
3. SMTP with TLS and stable Message-ID;
4. PagerDuty/Opsgenie/Teams/Discord adapters after their acknowledgement,
   deduplication, and secret models are individually designed.

A generic webhook can call an existing on-call router in phase one. OAuth Slack app,
Jira tickets, chat actions, and bidirectional workflows are separate features.

## Control API and observability

Authenticated endpoints provide destination list/create/update/delete with expected
revision, test-send, redacted payload preview, delivery history, dead-letter list,
and manual retry. Test sends use the same admission/TLS/secret/response handling but
an explicit test event namespace and no monitor transition.

Status exposes intents created, pending/retrying/dead/succeeded, oldest age,
delivery latency, outcomes by notifier kind/status class, rate-limit/backoff,
secret-resolution/TLS/config errors, active leases, and projection lag. It never
includes URL path/query, headers, secret refs/values, message body, issue content,
or response body. Logs use event/notifier IDs and error classes only.

## Multi-instance and no-Valkey behavior

Cluster dispatch requires the normal namespaced lease and fails closed on lease
loss. Durable intents remain pending. A declared single alertd process may use the
local lease provider. Absence of Valkey alone does not prove exclusivity; accidental
multi-process unfenced dispatch is refused unless an explicitly unsafe override is
selected. At-least-once still permits crash-window duplicates in every mode.

## Verification

- Deterministic intent/event/body snapshots and template output bounds.
- Stub server covers every status, Retry-After/date bounds, slow headers/body,
  disconnect-before/after acceptance, redirects, DNS changes, TLS/CA, HMAC, and
  idempotency.
- Crash injection around send/result proves conservative at-least-once behavior.
- File secret rotation is observed; secrets never appear in API/status/log/output
  captures or durable object fixtures.
- Two workers race/take over one event; one ordinary attempt occurs while
  acceptance-before-record remains a documented duplicate case.
- Fairness, per-host/global concurrency, queue age, dead-letter, manual retry,
  disabled/tombstoned destination, and cold rebuild tests.

## Retention dependency graph

Control retention is ordered, not independent TTLs. A terminal success/dead receipt
and the destination revision it references remain at least as long as the intent and
its idempotency promise. Intent/receipt compaction, if added, publishes an immutable
checkpoint covering exact input IDs and hashes before deleting those records. Rule
and destination tombstones remain until every prior revision, alert state, intent,
and receipt that can reference them is gone. Deleting receipts while retaining an
intent would resurrect pending delivery after cold rebuild and is forbidden. Manual
retry after the documented receiver idempotency horizon is labeled as possibly
duplicating even with the same event ID.

## Open questions for review

- Generic webhook only in first implementation, or Slack formatting alongside it?
- Initial success/dead attempt-history and receiver idempotency retention?
- Is HMAC signing required for v1 generic webhooks?

## References

- [Error monitoring](error-monitoring.md)
- [Alert evaluation](alert-evaluation.md)
- [Errors and alerts UI/API](error-monitoring-ui.md)
- `crates/agent/src/scrape.rs` (file secrets and TLS client pool)
- `crates/httpsig/`; `crates/valkey/src/lease.rs`
