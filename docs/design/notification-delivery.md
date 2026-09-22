# Notification delivery — Design

Status: partial — notification-target CRUD, encrypted secrets, preview/test-send, and browser UI implemented; monitor intents and delivery worker outstanding
Owner: Bart
Last updated: 2026-09-22

## Implementation status

This document delivers intents created by
[Alert evaluation](alert-evaluation.md). Delivery never decides alert state. D-076
excluded notification intents, targets, outbox processing, and external delivery
from the completed first alerting slice; D-077 and D-078 select the following target
and delivery contracts. The notification-target foundation now implements immutable
CRUD, XChaCha-encrypted logical secrets and CLI rotation, redacted projection/API
views, JSON templates and built-ins, mandatory HMAC, public-HTTPS admission, durable
preview/test-send, and the separate browser UI section. Monitor-linked intents, the
alert outbox and projection, retry worker, external transition delivery, and delivery
status remain outstanding.

### Done

- [x] **Failure-model survey.** Existing HTTP client/TLS/rotating-secret patterns
  and lease semantics have been reviewed.
- [x] **Decision — JSON-only custom templates.** D-078 selects bounded JSON values
  with JSON-aware placeholder substitution; arbitrary text bodies are excluded.
- [x] **Decision — secret encryption and rotation.** D-078 selects versioned
  XChaCha20-Poly1305 envelopes, current/previous environment keys, explicit CLI
  re-encryption, and a stable logical `latest` secret.
- [x] **Decision — retry exhaustion.** D-078 selects an absolute six-hour horizon,
  eight nominal attempts, and the user-facing terminal state **Delivery failed**.
- [x] **Decision — webhook signing.** D-078 requires one fixed, versioned
  HMAC-SHA256 contract for every generic webhook.
- [x] **Decision — webhook network policy.** D-078 permits only public,
  WebPKI-valid HTTPS without redirects, proxies, private addresses, or exceptions.
- [x] **Phase 0 — notification-target model and UI.** Implemented immutable revisioned
  targets, secret-free SQLite projection, XChaCha-encrypted object-store secret
  generations, current/previous environment-key rotation CLI, bounded JSON templates
  and built-ins, redacted CRUD/preview/test-send APIs, public-HTTPS DNS validation and
  address pinning, mandatory HMAC, and the separate browser Notification targets UI.

### Outstanding
- [ ] **Phase 1 — durable outbox.** Atomically commit one Firing or Resolved intent per
  selected target and project at-least-once delivery state with bounded retries.
- [ ] **Phase 2 — webhook worker.** Add bounded pooled HTTP delivery, response
  classification, idempotency, built-in/custom per-target templates, and observability.
- [ ] **Phase 3 — built-in formats.** Ship useful built-in target formats such as
  generic JSON and Slack-compatible payloads through the same target contract; do not
  introduce independently reusable formatting resources.
- [ ] **Defer — failed-delivery workflow and manual retry.** Retain visible
  **Delivery failed** history, but defer dedicated management and manual retry controls.
- [ ] **Phase 4 — verification.** Crash-after-send, Retry-After, encryption-key
  rotation, malformed templates, rate limits, SSRF, redaction, and multi-instance
  tests.

## Why this exists

A monitor transition and an HTTP request are not one transaction. If Scry sends
before recording intent, a crash loses the notification; if it records success
before sending, it lies; if the remote accepts and the response is lost, retry may
duplicate. A Valkey lease prevents concurrent workers but cannot remove this
fundamental uncertainty.

This design therefore promises durable, observable, bounded **at-least-once**
delivery. Alert evaluation commits a complete notification intent first. Independent
workers deliver it, persist attempts, and isolate notification targets from one
another.

## Goals

- Never perform external I/O without a durable notification intent.
- Resume pending/retrying delivery after crash or local projection loss.
- Provide stable event/idempotency identity to cooperative receivers.
- Bound concurrency, queue age, retries, response bodies, templates, and clients.
- Keep the dedicated signing secret out of durable target objects, SQLite, status,
  APIs, logs, and payload previews; store only authenticated ciphertext in object
  storage. Custom header values are explicitly non-secret plaintext metadata.
- Allow one broken notification target to fail independently.
- Expose test and delivery history; dedicated failed-delivery management and manual
  retry workflows are deferred.

## Non-goals (v1)

- Exactly-once external effects.
- Arbitrary code/templates, browser-supplied notification targets, redirects,
  proxies, private-network exceptions, or credentials embedded in rule JSON.
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
  notification_target_id, pinned_non_secret_target_revision,
  logical_secret: latest,
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
sequence, kind, and notification target. One intent per target prevents a failed
Slack target from blocking a webhook target. Every configured target receives both
Firing and Resolved transitions; sending only Firing would leave operators with a noisy,
open-ended alarm. Firing and Resolved are never coalesced. Reminder creation remains
deferred and may later be coalesced by evaluator policy.

Intent lives inside the committed alert transition or in a deterministic object
that transition commits atomically by reference. Visibility must have one commit
point; readers never see state as Firing without its required intents.

## Notification-target model

```text
NotificationTarget {
  schema_version, id, revision, name, enabled, kind,
  endpoint metadata,
  logical_secret_ref: latest,
  timeout,
  network_policy: PublicWebPkiHttps,
  format: BuiltIn(format_id) | CustomTemplate(template),
  bounded headers/options,
  created_at, updated_at
}
```

“Notification target” is the product and API term; the implementation should not
retain a separate `Notifier`/destination vocabulary. Formatting belongs to the target
revision itself. A target selects a built-in format or stores one complete custom
template with bounded placeholders; there is no separately managed/reusable format
resource.

Definitions are immutable revisions in object storage and projected locally.
Mutation uses expected revision and per-target object-store CAS; the current 10,000-
target admission limit is projection-local and therefore cluster-soft until a durable
quota reservation is added. Intents pin the target revision so later endpoint/template
edits cannot silently rewrite historical routing.
The dedicated target signing secret is encrypted before it is written to S3-compatible
object storage. The versioned envelope uses XChaCha20-Poly1305. Every alertd instance
receives a required current key and, only during rotation, one optional previous key
through environment variables, and decrypts only while validating or delivering. API
responses never return that secret's ciphertext, key identifiers that disclose
deployment configuration, or plaintext; mutation accepts a write-only secret field and
an unchanged-secret sentinel. Custom header values are ordinary visible target metadata:
they are stored and returned in plaintext and must not be used for sensitive credentials.

Rotation is an explicit operator workflow: deploy the new current key with the old key
as previous, run an idempotent CLI re-encryption command over every referenced target
secret, verify that no envelope requires the previous key, then remove the previous
key. Intents pin non-secret target revisions while naming the stable logical `latest`
secret. Re-encryption replaces ciphertext behind that logical identity, so queued
intents need not be rewritten and do not remain pinned to retired key material.
Referenced revisions, logical secrets, and tombstones outlive every dependent intent.

A generic webhook accepts only an absolute `https` URL with no userinfo or fragment,
uses public WebPKI roots, caps headers, and does not permit arbitrary `Host`,
`Content-Length`, hop-by-hop, idempotency, or signature header overrides. Custom CAs,
certificate/hostname verification bypasses, redirects, and environment/system or
explicit proxies are unsupported. Resolve all A/AAAA answers and reject every
loopback, private, link-local, multicast, cloud-metadata, or otherwise non-public
address before pinning one validated public address for the connection. There are no
hostname or CIDR exceptions in startup configuration or API input. No ambient cookies,
proxy/cloud credentials, or browser headers are attached. Phase 0 test-send constructs
a policy-pinned client per request under a small concurrency bound; the delivery worker
must add bounded reusable client pools before it handles sustained delivery volume.

## Durable outbox projection

Illustrative authoritative records:

```text
_scry/alerts/v1/transitions/...        # contains/references intents
_scry/alerts/v1/outbox-results/<event-id>/<attempt-id>.json
```

There is no mutable “pending object” that must be atomically overwritten. Local
`alerts.sqlite` folds committed intents and immutable attempt results into:

```text
outbox(event_id PRIMARY KEY, notification_target_id, status, next_attempt_at,
       attempt_count, first/last_attempt_at, last_error_class, ...)
attempts(attempt_id PRIMARY KEY, event_id, started_at, finished_at,
         outcome, response_class, retry_after, worker_id, ...)
```

Status is `pending | delivering | retrying | succeeded | delivery_failed`;
**Delivery failed** is its user-facing rendering. `delivering` is a leased local
observation; durable absence of a result makes it eligible again after lease expiry.
A success result is written only after the remote reports an accepted status. Failed
result records carry redacted classifications, not response bodies containing possible
secrets.

Dispatcher acquires `lease/alert/deliver/<event-id>` (the event already identifies the
notification target), re-folds durable attempts, claims the outbox head with ETag
`If-Match`, resolves the target's stable logical `latest` secret while using the
intent's pinned non-secret target revision, sends, then conditionally persists the
result/head. Attempt IDs are UUIDv7 records
carrying deterministic event identity and use create-if-absent. Success is absorbing;
late failure CAS cannot reopen it. Durable retry scheduling and maximum-attempt
accounting fold attempt IDs. Do not hold evaluator leases. Conditional writes provide
storage exclusion; the lease distributes work and bounds duplicate remote sends but
exactly-once remains
impossible across remote acceptance and local success CAS.

## Delivery and retry semantics

Send `Idempotency-Key: <event-id>` where protocol permits and include the same ID in
versioned payloads. Cooperative webhook receivers can deduplicate. Slack incoming
webhooks and SMTP may not, so possible duplicates are part of the contract.

Classification:

- accepted 2xx: durable success;
- DNS/connect/TLS/timeout, 408, 425, 429, and 5xx: retry;
- 409 retries only when the notification-target contract says an idempotent conflict
  is transient;
- other 4xx: permanent **Delivery failed** immediately;
- oversized/invalid response is classified without buffering unbounded content;
- honor `Retry-After` only when it remains within the absolute retry horizon;
  otherwise schedule from the nominal offsets with bounded jitter.

Retry eligibility ends absolutely six hours after intent creation. At most eight
attempts use nominal offsets of 0, 1, 5, 15, 30, 60, 120, and 240 minutes. Bounded
jitter or `Retry-After` may move an attempt while it remains within the horizon, but
cannot create a ninth attempt or extend the horizon. A permanent response, eight
exhausted attempts, or horizon expiry writes durable `delivery_failed`, displayed as
**Delivery failed**; it remains visible and does not block later events. This is a
terminal delivery outcome, not movement into a separate queue. A disabled
notification target pauses pending delivery with a visible reason; deletion is a
tombstone and cannot erase history. Dedicated failed-delivery management and manual
retry are deferred.

Crash cases:

- before send: lease expires and another worker sends;
- after remote acceptance/before success record: retry can duplicate;
- after failure/before result: retry is conservative;
- after result: fold prevents another send;
- projection loss: object replay reconstructs eligibility.

## Templates and payloads

Formatting is configured directly on each notification target. The target chooses a
built-in template or supplies one complete custom template. Custom templates are JSON
only: the template is a bounded JSON value, and string leaves may contain a bounded,
non-Turing-complete set of placeholders. Substitution is JSON-aware and cannot inject
raw JSON syntax. Validation rejects malformed JSON, unsupported placeholders,
excessive nesting or output, and every non-JSON custom body. Formats are not
independently named, versioned, or shared between targets. The target's own immutable
revision versions its template. Templates have no network/file/env access, loops over
unbounded event data, or secret interpolation. Render and scrub at intent creation so
later target edits cannot rewrite history; protocol adapters may add framing only.

The generic JSON built-in emits a stable schema with transition, labels, annotations,
value/issue summary, timestamps, links, and event ID. A Slack-compatible built-in maps
the same intent into bounded JSON blocks/text and includes the Scry link and transition,
without owning alert logic. Every generic webhook signs the exact transmitted body
bytes with HMAC-SHA256 using its encrypted target secret. Signing is mandatory, with
no target-level disable switch or algorithm negotiation. The v1 wire sends
`Idempotency-Key: <event-id>`, `X-Scry-Signature-Timestamp: <unix-seconds>`, and
`X-Scry-Signature: v1=<lowercase-hex>`. The MAC input is the ASCII prefix
`v1\n<unix-seconds>\n<event-id>\n` followed immediately by the exact transmitted body
bytes. Receivers should require a five-minute timestamp window and deduplicate by
event ID. SMTP later uses the same snapshot and Message-ID derived from event ID.

Links are built from configured public Scry base URL and typed IDs, never from
untrusted Host headers. Payload previews in UI are scrubbed and clearly exclude
secret-derived headers/signatures.

## Resource isolation

Independent limits: pending projection scan, max concurrent deliveries globally
and per notification target/host, bounded waiters, request timeout, connect timeout,
response header/body bytes, retry computations, client pool, template output, and
delivery-history retention. Fair scheduling prevents one failing endpoint from
occupying every slot. Notification-target rate limits and circuit-breaker/backoff do
not stop unrelated targets.

Delivery work is never executed on ingest or interactive query task pools. If
outbox age exceeds SLO, status/alerts report it; workers do not bypass bounds.
Notification-on-notification recursion is prevented by a separate local/operator
health channel rather than writing delivery failures into the same monitored rule
without inhibition.

## Integrations and phasing

Delivery begins only after the no-delivery first alerting slice is complete; none of
these phases is implied by browser rule CRUD or alert-state evaluation:

1. generic public HTTPS JSON webhook with mandatory HMAC-SHA256;
2. Slack incoming webhook formatting;
3. SMTP with TLS and stable Message-ID;
4. PagerDuty/Opsgenie/Teams/Discord adapters after their acknowledgement,
   deduplication, and secret models are individually designed.

A generic webhook can call an existing on-call router in phase one. OAuth Slack app,
Jira tickets, chat actions, and bidirectional workflows are separate features.

## Control API and observability

Authenticated endpoints provide notification-target list/create/update/delete with
expected revision, test-send, redacted payload preview, and recent delivery outcomes.
The Alerts UI exposes targets in a separate section rather than mixing them into the
monitor list. Test-send is part of the first target UI and uses the actual
admission/template/TLS/secret/response path with an explicit test event namespace and
no monitor transition. Dedicated failed-delivery management and manual-retry APIs
are deferred.

Status exposes intents created, pending/retrying/delivery-failed/succeeded, oldest
age, delivery latency, outcomes by notification-target kind/status class, rate-limit/
backoff, secret-resolution/TLS/config errors, active leases, and projection lag. It
never includes URL path/query, headers, secret refs/values, message body, issue content,
or response body. Logs use event/notification-target IDs and error classes only.

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
- Current/previous environment-key overlap, repeated CLI re-encryption, restart after
  each crash point, and removal of the previous key are tested; dedicated signing
  secrets never appear in API/status/log/output captures or durable object fixtures.
- Two workers race/take over one event; one ordinary attempt occurs while
  acceptance-before-record remains a documented duplicate case.
- Fairness, per-host/global concurrency, queue age, terminal **Delivery failed**
  history, disabled/tombstoned notification target, and cold rebuild tests.

## Retention dependency graph

Control retention is ordered, not independent TTLs. A terminal success or
`delivery_failed` receipt and the notification-target revision it references remain at
least as long as the intent and its idempotency promise. Intent/receipt compaction, if
added, publishes an immutable checkpoint covering exact input IDs and hashes before
deleting those records. Rule and notification-target tombstones remain until every
prior revision, alert state, intent, and receipt that can reference them is gone.
Deleting receipts while retaining an intent would resurrect pending delivery after
cold rebuild and is forbidden.

## Selected decisions and remaining review

D-076 deferred delivery from the completed first alerting slice. D-077 selects the
next slice: notification targets are managed in their own Alerts UI section; each
target owns either a built-in or complete custom template; monitors emit both Firing
and Resolved intents; target secrets are encrypted in object storage; test-send ships
with target CRUD; and dedicated failed-delivery management/manual retry is deferred.

D-078 resolves the blocking policy choices: custom templates are JSON-only; secret
envelopes use XChaCha20-Poly1305 with current/previous environment keys, explicit CLI
re-encryption, and stable logical `latest` identity; retry has an absolute six-hour,
eight-attempt schedule ending in **Delivery failed**; every generic webhook uses the
fixed HMAC-SHA256 contract; and endpoints are public WebPKI HTTPS only, without
redirects, proxies, private addresses, or exceptions. Exact wire/header names, envelope
encoding, CLI command syntax, and validation UX are implementation details that must
preserve those decisions.

## References

- [Error monitoring](error-monitoring.md)
- [Alert evaluation](alert-evaluation.md)
- [Errors and alerts UI/API](error-monitoring-ui.md)
- `crates/agent/src/scrape.rs` (file secrets and TLS client pool)
- `crates/httpsig/`; `crates/valkey/src/lease.rs`
