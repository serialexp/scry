# Error monitoring — Architecture

Status: partial — occurrence foundation, fingerprint v1 grouping, and issue list UI implemented; not yet deployed
Owner: Bart
Last updated: 2026-09-15

## Implementation status

Tracking the gap between this design suite and the implementation tree. D-073
accepts logs v2 and the `_scry/<subsystem>/` namespace. D-074's identity,
pre-WAL scrubbing, server receipt stamping, occurrence projection, rebuildable
`errors.sqlite`, and periodic single-writer `scry errors` foundation are implemented.
Fingerprint v1 grouping (OCC1 decoder, message normalization, digest, quality
levels), deterministic issue identity, issue/occurrence_issues SQLite tables with
grouping reconciliation, and issue list visibility through the query wire protocol
and a browser `/errors` route have landed. None of this has been deployed.
Artifacts, symbolication, stack parsing, issue workflow, alert evaluation, and
browser SDK remain outstanding.

### Done

- [x] **Existing-system survey.** OTLP mappings, immutable storage, query,
  browser UI, Valkey coordination, and the inert Alerts surface have been mapped.
- [x] **Upstream survey.** Current OpenTelemetry exception/event conventions,
  browser capture, source-map debug IDs, and representative issue/alert behavior
  have been reviewed.
- [x] **Architecture decisions.** D-073 selects lossless logs v2, immutable
  object-store authority with local projections, and `_scry/<subsystem>/`.
- [x] **Phase 0a — reader-only namespace support.** A shared classifier excludes
  `_catalog/` and `_scry/` before block suffix checks in catalog, convergence, and
  targeted query repair paths; no `_scry/` product writer is enabled.
- [x] **Phase 0b — logs v2 reader contract.** The bounded canonical raw codec,
  generated envelope, historical Parquet v2 schema, mixed-version query
  normalization, and schema-safe compaction landed without enabling a writer.
- [x] **Phase 0c — coordinated occurrence foundation.** Uniform key redaction spans
  proto/server/gateway/agent/replay. Producers emit raw-record v1 and ingest stamps
  receipt time before WAL into raw-record v2/logs schema 3. Strict log-only IDs,
  manifest/app identity, immutable conditional occurrence commits, deployment-bound
  rebuildable `errors.sqlite`, bounded commit/raw paging, and periodic exclusive
  single-writer `scry errors` reconciliation are implemented. This has not been deployed.
- [x] **Phase 1a — fingerprint v1 grouping.** OCC1 binary decoder, regex-based
  message normalization (UUIDs, hex, timestamps, IPs, floats, long numbers),
  SHA-256 fingerprint digest, `GroupingQuality` levels (TypeAndMessage / TypeOnly /
  Fallback), deterministic `IssueId` from domain-tagged deployment/app/fingerprint,
  `issues` and `occurrence_issues` SQLite tables, `fold_grouped()` transactional
  upsert, and `group_occurrence_page()` reconciliation phase in errorsd. Bounded
  catalog query (`list_source_blocks`) replaced unbounded `list_blocks()`.
- [x] **Phase 3a — issue list over query wire.** `IssueListRequest`/
  `IssueListResponse` added to the query protocol schema, queryd `--errors-db`
  opens `errors.sqlite` read-only, `QueryService` dispatches before admission
  (same pattern as `FleetStatusRequest`), `QUERY_ERR_ISSUES_UNAVAILABLE` error
  code, TypeScript `fetchIssueList()` client, SolidJS `/errors` route with issue
  inbox table (title, count, severity, quality, first/last seen, polling refresh),
  store signals, and nav link. The webui relay passes the new frames unchanged.

### Outstanding

- [ ] **Clustered occurrence orchestration.** Add Valkey leases/fencing, hints, and
  multi-instance convergence; clustered `scry errors` currently fails closed.
- [ ] **Occurrence freshness and lifecycle.** Add accepted-record low-latency hints,
  snapshot bootstrap, and projection/orphan garbage collection.
- [ ] **Phase 1b — artifacts and symbolication.** Upload, index, and symbolize
  stacks; stack parsing for supported runtimes; explicit fingerprint overrides;
  reprocessing after late artifacts. Fingerprint v1 is implemented but operates
  without symbolication or parsed frames.
- [ ] **Phase 2 — issue lifecycle and workflow.** Add durable immutable workflow
  commands (resolve/ignore/assign), revision-checked mutations, issue facets,
  regression detection, and issue-transition event stream. The rebuildable issue
  index exists but carries no human workflow state.
- [ ] **Phase 3b — issue detail and mutations UI.** Add issue detail view,
  occurrence inspector, grouping explanation, stack/source display, trace links,
  workflow actions, and CSRF-protected mutation proxies. The issue list is
  implemented; detail, mutation, and deep-link views remain.
- [ ] **Phase 4 — alert evaluation.** Persist and evaluate typed issue-transition
  and scalar threshold monitors under independent resource admission.
- [ ] **Phase 5 — notification delivery.** Deliver durable intents through bounded,
  observable, at-least-once notifier workers.
- [ ] **Phase 6 — browser SDK.** Ship standards-based browser capture, public
  intake controls, build integration, and source-map upload tooling.
- [ ] **Phase 7 — qualification.** Complete security, failure, rolling-upgrade,
  rebuild, multi-instance, capacity, and end-to-end smoke qualification.

## Why this exists

Scry already accepts OpenTelemetry logs and traces containing exceptions, but it
stores them only as generic telemetry. It has no stable occurrence identity,
source-map symbolication, issue grouping, issue lifecycle, rule evaluator, or
notification delivery. The existing Alerts route is deliberately inert
(`v1.0-web-ui.md`). A Sentry-like product is therefore not one adapter: it is a
set of related data-plane and control-plane systems.

This document is the suite's contract. The focused documents define mechanics:

- [Error intake](error-intake.md)
- [Stack processing and issue grouping](error-grouping.md)
- [Issue indexing and lifecycle](error-issues.md)
- [Alert evaluation](alert-evaluation.md)
- [Notification delivery](notification-delivery.md)
- [Errors and alerts UI/API](error-monitoring-ui.md)
- [Browser SDK and build integration](browser-error-sdk.md)

A focused document may refine a mechanism but must not contradict the ownership,
durability, identity, or failure semantics here.

## Goals

- Receive server and browser exceptions through OpenTelemetry-compatible paths.
- Preserve sufficient raw information to export, reprocess, and regroup later.
- Correlate errors with logs and traces without requiring a sampled trace.
- Turn occurrences into stable, explainable, versioned issues.
- Provide durable resolve/ignore/regression and audit workflows.
- Notify on issue transitions and bounded query thresholds without duplicate
  storms or resource starvation.
- Remain correct from one instance without Valkey through multiple identical
  instances coordinated by Valkey.
- Treat browser input, stack traces, source maps, and notifier secrets as
  security-sensitive data.
- Keep all retained state bounded, observable, and testable.

## Non-goals (first complete release)

- Session replay, screenshots, attachments, local-variable capture, or profiling
  an individual crash.
- AI diagnosis, suspected-commit analysis, GitHub/Jira/Linear automation, or
  interactive Slack issue mutation.
- Native/mobile minidumps and symbol servers. The artifact model must leave room
  for them, but v1 symbolication is JavaScript source maps.
- Arbitrary user code in rule expressions or notification templates.
- Exactly-once delivery to external notification systems.
- Multi-tenancy inside one deployment. `app_id` partitions product identity and
  limits; it does not reverse D-007's one-tenant deployment contract.
- Perfect counts when source SDKs sample, browsers terminate, privacy controls
  suppress events, or clients are offline.

## Terminology

- **Telemetry event:** an OTLP LogRecord Event or span event received and stored in
  the ordinary telemetry plane.
- **Occurrence:** one eligible exception LogRecord Event with a valid canonical
  `scry.event.id`. Span events never create occurrences.
- **Artifact:** immutable generated code, source map, and committed manifest
  addressed by application and debug ID.
- **Fingerprint:** a versioned digest of canonical grouping components. It is not
  an occurrence ID.
- **Issue:** occurrences assigned to one grouping identity plus a mutable human
  workflow overlay.
- **Monitor:** a durable definition that detects a condition or consumes issue
  transitions.
- **Alert instance:** persisted evaluation state for one monitor/group.
- **Notification intent:** immutable, durable request to deliver one transition
  to one destination.
- **Projection:** replaceable local or object-store-derived state rebuildable from
  authoritative immutable records.

## System invariants

1. Object storage is durable truth. Local SQLite databases are rebuildable
   projections; Valkey is coordination, discovery, and freshness only.
2. Accepted raw telemetry is never made dependent on symbolication, grouping,
   issue indexing, queryd, Valkey, or a notifier being available.
3. Occurrence identity, issue identity, and delivery identity are distinct.
4. All transformations retain schema and algorithm versions plus enough inputs
   to explain and repeat their output.
5. A hint or lease may improve freshness/exclusion but never substitutes for an
   idempotent reconciliation path.
6. A state transition and its notification intents become durable before any
   external request is attempted.
7. Loss of evaluation never means `OK`; stale/error/no-data are distinct states.
8. Error processing has independent byte, CPU, concurrency, queue, and deadline
   budgets and cannot bypass normal query admission.
9. No browser-visible credential is treated as a secret.
10. One minimal sensitive-key scrubber replaces matched values with the fixed
    string marker `[REDACTED]` before WAL on every logs and traces path, including ordinary
    telemetry; metrics and profiles are excluded. This is not a promise to scrub
    secrets embedded in free text. Stronger product-specific privacy policies are
    additional controls.
11. Only eligible exception logs with valid canonical `scry.event.id` values are
    authoritative occurrences. Span events, generated IDs, synthetic IDs, and
    heuristic cross-signal deduplication are excluded.

## End-to-end flow

```text
browser/server SDK
  -> eligible OTLP exception log with canonical event ID
  -> scry gateway -> scrub every log/trace -> native ingest -> WAL -> log blocks
                                                              |
                                                    block poll/walk
                                                              v
scry errors -> immutable occurrence projection -> local errors.sqlite
  -> later symbolication/grouping -> issue projections
  -> Issues API/UI and durable workflow commands
                                     |
                       issue transition event stream
                                     v
scry alert: scheduled/transition evaluation -> durable state + outbox intents
  -> notifier workers -> webhook/Slack/email/etc.
```

Bounded catalog paging plus immutable occurrence-commit discovery are the
implemented correctness path. A direct accepted-record observer remains outstanding;
when added, it may reduce detection latency only if it applies the same eligibility,
scrubbed-input, identity, and extraction contract. Loss of that hint must heal after
sealing. Trace IDs on the log provide best-effort links to ordinary retained traces,
not another occurrence source.

## Component ownership

The occurrence foundation implements two reusable domain crates with a thin role
wrapper; later phases extend them and add the alert pair:

- `scry-errors`: implemented occurrence identity/extraction, immutable projection,
  conditional publication, collision quarantine, SQLite fold, OCC1 binary decoder,
  fingerprint v1 (normalization/digest/quality), deterministic issue identity,
  issue/occurrence_issues tables, `fold_grouped`, and read-only `list_issues`;
  later artifact, symbolication, stack parsing, issue workflow, and facet domain
  behavior remains outstanding.
- `scry-errorsd`: implemented `scry errors` manifest initialization, bounded periodic
  single-writer reconciliation, grouping reconciliation phase (`group_occurrence_page`),
  and status; clustered leases and a private product control endpoint remain
  outstanding.
- `scry-alert`: rule/state/outbox domain and evaluator/notifier engines.
- `scry-alertd`: `scry alert`, schedules, queryd clients, leases, APIs, and status.

This follows the existing engine/thin-role convention. It does not require four
processes: the multicall binary can run roles separately, and a later supervised
`full` composition can host them together without merging ownership boundaries.
Queryd remains the generic admitted query executor. Gateway remains protocol
translation/fan-out, not durable issue authority. Webui remains the authenticated
browser front door and allowlisted proxy.

## Durable namespace and projections

D-073 reserves `_scry/<subsystem>/` for control products while retaining
`_catalog/` as a permanent legacy sibling. One shared reserved-control-prefix
classifier is used by catalog reconcile, cluster poll/full walk, targeted query
repair, inherited list/maintenance paths, and tests. Reader-only support must be
deployed fleet-wide before any `_scry/` product object is written.

The deployment manifest is absent until a feature that needs deployment identity is
enabled. The first coordinated-version participant creates it conditionally; races
read and validate the winner, and the stable value is never inferred from a bucket
name or process configuration. `app_id` is deterministically derived by a versioned
algorithm from normalized `service.namespace` and `service.name`, not assigned by
the gateway or accepted as a producer claim.

Authoritative records are immutable and committed metadata-last:

```text
_scry/deployment/v1/manifest.json
_scry/artifacts/v1/...
_scry/errors/v1/occurrences/...
_scry/errors/v1/projections/...
_scry/errors/v1/workflow/...
_scry/errors/v1/transitions/...
_scry/alerts/v1/rules/...
_scry/alerts/v1/silences/...
_scry/alerts/v1/transitions/...
_scry/alerts/v1/outbox-results/...
```

The first complete product slice stops after the dedicated immutable occurrence
projection, its rebuildable `errors.sqlite` index, and the `scry errors` role.
Grouping, artifacts, issue membership, and workflow remain later phases; the
occurrence projection is not merely an internal grouping output.

`errors.sqlite` and `alerts.sqlite` are separate from the block catalog. This
avoids coupling their schema migrations/snapshots to block query readiness.
Optional snapshots follow the existing version-checked, atomic-restore pattern;
a mismatch rebuilds only its owning projection.

The error-control plane requires an S3-compatible backend with atomic conditional
writes: `If-None-Match: *`/`PutMode::Create` and ETag-guarded `If-Match` updates.
AWS S3 provides both, and the supported development backend must exercise them
(SeaweedFS is the intended local test target). Startup capability probes fail the
error/control roles closed when these semantics are absent; a backend that merely
accepts but ignores preconditions is unsupported for this feature.

Immutable records use deterministic keys plus create-if-absent: identical retry is
idempotent after digest verification, and different bytes at one identity return a
conflict without overwrite. Revisioned mutable heads use ETag compare-and-swap.
Valkey leases still distribute expensive projection/evaluation work and reduce
contention, but correctness at the object-store commit point comes from conditional
writes rather than a pre-PUT lease check. In explicitly single-process operation a
local lease provider can schedule work, while conditional commits preserve storage
integrity in every mode.

## Consistency and degraded operation

- Raw telemetry follows existing ingest durability and eventual catalog
  convergence.
- Issue freshness is eventual and reports source-block and workflow lag.
- A workflow mutation acknowledges only after its immutable command is durable.
  The response may include a locally folded result, but exposes projection age.
- Valkey outage pauses work requiring distributed exclusion. Existing raw events,
  workflow commands, rule state, and outbox remain durable and readable.
- Query overload is an evaluation error, not a false recovery.
- Artifact absence yields an unsymbolicated occurrence and bounded retry; it does
  not reject raw ingest.
- Notification endpoint failure affects only that destination. Other destinations
  and future transitions proceed.

## Retention and deletion

Raw occurrence retention remains the owning signal's retention policy. Issue
projections may preserve lifetime aggregate facts after raw detail expires, but
must mark unavailable detail rather than dangling silently. Workflow audit and
notification history have explicit independent retention.

Artifacts default to at least the maximum raw-error retention or are reference-
pinned. Shorter time-to-idle retention is allowed only with a visible loss of
future rebuild/symbolication guarantees. Privacy deletion needs a tombstone or
redaction workflow spanning raw telemetry where feasible, projections, source
context, users, and notification payload history; this requires its own accepted
policy before user identifiers are enabled by default.

## Resource isolation and observability

Each role publishes bounded fleet status: projection lag, pending blocks,
reprocessing queue, symbolication memory/CPU/timeouts, issue cardinality, due and
running evaluations, query resource rejections, state staleness, outbox age,
delivery retries/dead letters, and object-store/Valkey health. Status is never
correctness state.

Capacity presets eventually include error intake rate/body limits, artifact
bytes/cache, symbolication concurrency, projection queue/rebuild throughput,
rule evaluation concurrency, query deadlines/result bounds, and delivery pools.
Completion-relative scheduling with deterministic jitter prevents catch-up and
fleet synchronization storms.

## Security model

Trusted server ingestion continues to support deployment ingress controls. Every
logs and traces path applies the uniform sensitive-key scrubber before WAL,
regardless of whether a record is error-eligible; metrics and profiles do not.
The `[REDACTED]` marker and fixed key set are intentionally minimal, and the system
makes no free-text scrubbing claim.

Public browser intake is a distinct hardened surface: TLS, exact origin policy
where meaningful, public app keys, revocation, per-app byte/event/cardinality
quotas, small decompressed bodies, schema limits, replay resistance where
possible, and server-side scrubbing. Origin and a public key authenticate neither
a person nor payload truth.

Artifact upload and all control-plane mutations require authenticated operator
access. Browser/Tauri read and mutation APIs initially inherit the single shared
webui session, so authorization is all-or-nothing; CSRF/origin protection is a
prerequisite for mutation. Secret values are resolved from opaque, operator-
configured secret IDs and never appear in object records, projections, responses,
status, logs, or rendered intents.

## Cross-document decisions requiring review

1. **Raw representation (resolved by D-073 and refined by D-074):** extend generic
   logs with a logs v2 typed canonical raw record plus stable flat query projection.
   Error-specific structured context remains versioned inside the typed record;
   only eligible exception logs feed the separate immutable occurrence projection.
2. **Namespace (resolved by D-073):** use `_scry/<subsystem>/`. All existing
   binaries must understand the root at least one rolling release before any object
   is written beneath it.
3. **Object-store capability contract (resolved by D-072):** conditional create and ETag CAS are
   required; define the startup probe, errors, and supported SeaweedFS/AWS matrix.
4. **Issue transitions:** freeze the durable errors-to-alerts transition log,
   deterministic logical IDs, rebuild-without-re-emission rule, and consumer cursor.
5. **Workflow conflicts:** reject stale expected revisions, deterministically fold
   operation-specific commutative events, or combine both by operation.
6. **Late symbolication:** keep original issue stable with a suggested merge, or
   move occurrences through an audited alias transaction.
7. **Lifetime counts:** preserve lifetime aggregates after raw retention or make
   issue statistics retention-window scoped.
8. **Role topology:** separate `scry errors` and `scry alert` roles initially, or a
   single control role with internally independent engines and budgets.
9. **Single-instance declaration:** how an operator proves that local unfenced
   mode cannot accidentally be started twice.
10. **Artifact upload authentication:** define CI/operator credentials and the
    authenticated service hop before build tooling ships.
11. **Notifier revisions:** whether queued intents use pinned or latest destination
    metadata, and how disable/rotation affects them.
12. **Privacy beyond the D-074 baseline:** defaults for user identifiers, URL
   query/fragment retention, free-text treatment, `sourcesContent`, and deletion
   guarantees.

## Verification strategy

- Pure fixtures for OTLP normalization, typed value round trips, occurrence
  deduplication, stack parsing, symbolication, canonical fingerprint bytes,
  lifecycle folds, rule transitions, and retry classification.
- Property tests for deterministic map ordering, idempotent replay, collision
  handling, monotonic revisions, and restart scheduling.
- Object-store contract tests against SeaweedFS and AWS-compatible semantics prove
  atomic `PutMode::Create`, ETag update conflicts, capability-probe failure, and
  ambiguous timeout reconciliation before any control-plane test relies on them.
- Integration tests with object-store errors, late artifacts, catalog rebuilds,
  compaction/retention, Valkey lease loss, stale rule edits, query exhaustion,
  notifier timeouts, and crash points around commits/sends.
- Browser tests for hostile rejection values, duplicate initialization, unload,
  CSP/CORS/origin behavior, scrubbing, and framework boundaries.
- Multi-instance smoke tests proving one committed projection/evaluation while
  duplicate observations remain harmless, plus single-instance no-Valkey tests.
- End-to-end smoke: browser and server exceptions -> raw query -> issue -> linked
  trace/source -> transition -> retried notification, followed by cold rebuild.

## Review protocol

The design-suite documents are reviewed as one unit. Reviewers must look for conflicting
identity, authority, retention, no-Valkey, and failure claims—not merely local
completeness. Substantive findings are incorporated and summarized here. Open
product choices remain explicit rather than being silently selected during prose
editing.

### Review record

- **2026-09-07 — D-073 resolves the foundational representation and namespace.**
  Bart selected logs v2 and `_scry/<subsystem>/` after a code-backed comparison of
  both feasible raw models and namespace rollout costs. Reader-only namespace
  support lands before the first control writer; the remaining product decisions
  below stay open.
- **2026-09-07 — Claude and Codex whole-suite reviews.** Both reviewers read all
  eight documents against the repository. Incorporated findings include: a durable
  issue-transition stream and consumer checkpoints; projection-time occurrence
  collision handling; a WAL-acknowledged browser intake distinct
  from D-041 fan-out ACKs; compaction-ancestry recovery and reap-grace constraints;
  a two-release reserved-prefix deployment gate and commit-object separation; scoped
  deployment/app identity; deterministic issue IDs; aligned alert slots; persisted
  grouped-state semantics; control-service/CI authentication and real CSRF session
  nonces; pinned non-secret notifier revisions; secret-ID/SSRF hardening; retention
  dependency ordering; mixed log-schema migration requirements; hostile timestamp/
  stream-fingerprint controls; and output escaping. Their Garage-driven recommendation
  to avoid conditional writes was subsequently rejected: the control plane requires
  real S3 conditional create/update semantics, and the local integration backend must
  test them. Antigravity review could not be obtained because its CLI invocation
  failed before reading the prompt; it supplied no findings.
- **2026-09-10 — D-074 approves the occurrence-foundation slice.** Authoritative
  occurrences now come only from eligible exception logs with canonical producer
  IDs; traces are best-effort links only. It also fixes uniform pre-WAL key
  scrubbing, conditional deployment-manifest creation, deterministic app identity,
  the dedicated occurrence projection/SQLite/role slice, and coordinated-version
  rollout. These decisions explicitly supersede the earlier dual-signal,
  synthetic-ID, heuristic-dedup, optional-scrub, and independently rolling text.
- **Still blocking review decisions after D-074:** workflow fold conflicts;
  late-symbolication membership; lifetime count semantics; source-map/parser
  implementation; single-instance exclusivity; privacy defaults beyond the minimal
  scrub baseline; and initial notification policy. These remain explicit rather
  than silently selected by reviewers.

## References

- `docs/ARCHITECTURE.md` — durable truth, resource isolation, block lifecycle.
- `docs/decisions.md` — D-007, D-027, D-038/D-039, D-041, D-049, D-055,
  D-059, D-064, D-066, D-070, D-071, D-073, D-074.
- `docs/design/gateway-ingestion-protocols.md`
- `docs/design/conditional-object-storage.md`
- `docs/design/v1.0-web-ui.md`
- OpenTelemetry exception logs, Events, Logs data model/SDK, resource/user/session
  semantic conventions.
- TC39 source-map debug-ID proposal; Sentry source-map and grouping references
  cited in [error-grouping.md](error-grouping.md).
