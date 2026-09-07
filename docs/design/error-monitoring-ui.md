# Errors, issues, and alerts UI/API — Design

Status: draft, not yet implemented
Owner: Bart
Last updated: 2026-09-07

## Implementation status

This document presents the backend contracts in the rest of the
[Error monitoring suite](error-monitoring.md). It does not redefine their state.

### Done

- [x] **Frontend survey.** Solid routing/stores, Arrow query transport, inspector,
  browser/Tauri split, webui authentication, and inert Alerts route are mapped.

### Outstanding

- [ ] **Decision — control topology.** Confirm separate configured errors/alertd
  upstreams and proxy routes versus one combined control endpoint.
- [ ] **Decision — authorization.** Accept all-or-nothing shared-session admin for
  v1 or require identities/roles before mutation.
- [ ] **Phase 0 — control API.** Add authenticated, CSRF-protected, bounded,
  revisioned proxies and typed clients for issues, artifacts, rules, and delivery.
- [ ] **Phase 1 — Errors views.** Add issue list/detail, occurrence inspector,
  grouping explanation, stack/source, trace jump, workflow, and deep links.
- [ ] **Phase 2 — Alerts views.** Replace the inert page with monitor/state/history,
  editor/test, silences, destinations, outbox, and delivery diagnostics.
- [ ] **Phase 3 — parity and accessibility.** Implement browser/Tauri transport,
  per-view stores, keyboard/screen-reader behavior, and responsive layouts.
- [ ] **Phase 4 — verification.** Contract, auth/CSRF, conflict, stale/partial,
  pagination, accessibility, browser, and Tauri tests.

## Why this exists

The current SolidJS application has Explore, Dashboards, Alerts, and Fleet routes,
but Alerts explicitly has no backend. Generic telemetry reads travel as framed
queries returning Arrow through a deliberately protocol-blind webui relay. Error
monitoring adds mutable issue workflow, artifacts, monitor rules, destinations,
and retry operations that are not queries and should not be forced into that wire.

This design defines one product surface over separate, explicit control-plane APIs,
while preserving existing query transport for raw telemetry and trace detail.

## Goals

- Make issues discoverable and debuggable from occurrence through linked trace and
  source context.
- Make grouping quality and decisions explainable rather than opaque.
- Support revision-safe resolve/ignore/assignment and monitor/destination editing.
- Expose stale, partial, sampled, retained-away, processing, and delivery states
  honestly.
- Keep Solid state in per-view stores rather than prop drilling or one mega-store.
- Work through browser/webui and Tauri with equivalent typed behavior.
- Keep all endpoints paginated, bounded, authenticated as appropriate, and SSRF-safe.

## Non-goals (v1)

- Replacing Explore or returning generic query results as JSON.
- Browser access to Valkey/object storage/private daemon addresses.
- Fine-grained organization/team RBAC unless selected as a review prerequisite.
- Session replay, AI diagnosis, ticket integrations, or chat-based mutation.
- Exposing source-map objects, notifier secrets, or arbitrary server errors.

## Information architecture

Add `/errors` as a first-class route; retain `/alerts` for monitors and delivery:

```text
/errors                          issue inbox/list
/errors/:issueId                 issue detail
/errors/:issueId/events/:eventId occurrence detail/deep link
/alerts                          monitors and alert instances
/alerts/:ruleId                  rule detail/history/editor
/alerts/destinations             notifier administration
/alerts/delivery                 outbox/dead-letter diagnostics
```

The shell shows health badges only from bounded summary APIs: error-processing lag,
stale alerts, and dead notifications. It does not poll full issue lists globally.
A source-context capability can be hidden independently when authorization/policy
for `sourcesContent` is unavailable.

## Errors inbox

Rows show title/type, service/app, severity, status, first/last seen, observed count,
affected-user estimate with exactness marker, environment/release facets, regression,
grouping quality, and processing lag. Cursor pagination uses stable
`(last_seen, issue_id)` ordering plus an `as_of_projection_revision` snapshot token;
without one the contract is explicitly at-least-once and clients deduplicate IDs as
moving rows can cross pages. Filters are typed, cardinality-bounded, shareable
in route/query state, and never become arbitrary SQL.

A row clearly distinguishes:

- unresolved/resolved/ignored and ignore expiry;
- new/regressed/ongoing;
- symbolicated/partial/raw grouping quality;
- exact/sampled/approximate counts;
- raw examples available versus expired by retention;
- projection fresh versus stale/rebuilding.

Bulk mutations are deferred until single-issue revision conflicts and audit behavior
are proven. Auto-refresh merges by issue ID without moving the currently selected
row unexpectedly.

## Issue detail

Header contains status workflow, title/type, service/environment/release, first/
last seen, counts, and revision. Tabs/panels:

- latest and selectable occurrences;
- exception chain and stack with raw/symbolicated toggle;
- optional authorized source context;
- structured attributes/resource/browser/session/user context after redaction;
- linked trace/spans and nearby logs by canonical binary trace ID;
- releases/environments/users/facets over the documented count scope;
- grouping explanation: algorithm/rules/artifacts, contributing and excluded frames,
  explicit override, quality, aliases/migrations;
- activity/audit: resolve/reopen/ignore/assign/comments/regressions/moves;
- monitors that match this issue and recent notification outcomes.

Generalize the current log `InspectorRail`/row-selection concepts where useful, but
do not couple issue detail to `LogRow`. Error occurrence, stack, and workflow are
separate domain types. Fix existing OTLP trace linking to recognize canonical
correlation rather than only `trace_id`/`traceId`/`trace.id`; `otel.trace_id` remains
a compatibility alias.

Mutations include `If-Match` revision and `Idempotency-Key`, backed by the control
plane's object ETag CAS. A 409 presents current state and asks the user to reapply;
it never silently overwrites. Ambiguous timeout is resolved by command ID/status.
A durable accepted but locally pending fold is shown as pending synchronization.
Optimistic UI is limited to reversible presentation and reconciles from server truth.

## Alerts experience

Replace the inert Alerts page only when backend phases exist. List shows monitor
type, enabled, current state/groups, last/next evaluation, stale/error/no-data,
firing since, matched issue/query, destinations, and delivery failures.

Editor uses typed builders:

- issue event source/filter/triggers; or
- telemetry signal, equality matchers, explicit lookback, constrained SQL/expression,
  scalar reducer/comparator/threshold;
- schedule/jitter, `for`, recovery, no-data and execution-error policies;
- labels/annotations, destinations, resolve/reminder behavior.

A validation/test action reports query schema/value/timing and resource rejection
without transitioning state or delivering. Raw SQL remains an Advanced field with
server validation and a prominent bounded-execution explanation.

Detail shows the state timeline, values, evaluations, errors, silences, transitions,
and notification intents/attempts. `NoData`, `Error`, `Stale`, and `Disabled` are
not styled as healthy green. Silence creation requires scope, start/end, actor, and
preview of affected groups. Dead letters link to redacted attempt diagnostics and
manual retry.

Destination UI lists kind/name/enabled/revision/health only. Secret fields are
write-only reference configuration; reading never returns a resolved value. Test
send uses the actual worker path. Payload preview is scrubbed and excludes secret-
derived headers/signatures.

## API topology

Keep `/api/query` and `/api/tail` as framed byte pipes. Add versioned HTTP control
routes in webui, authenticated by the existing signed session and proxied only to
operator-configured allowlisted private daemons:

```text
/api/v1/errors/issues...
/api/v1/errors/artifacts...       # upload may use streaming/body-specific limits
/api/v1/alerts/rules...
/api/v1/alerts/silences...
/api/v1/alerts/destinations...
/api/v1/alerts/delivery...
```

The browser selects an opaque configured target ID, never an address. Webui strips
hop-by-hop/credential headers, applies operation-specific body/time/admission limits,
and translates neither domain payload nor private errors beyond a stable envelope.
Unknown target is 400; unconfigured capability is 409; unavailable upstream is 502;
conflict remains 409; overload is 429/503 with bounded retry guidance.

Read schemas are typed versioned JSON and cursor-paginated. Large artifact upload is
a dedicated authenticated streaming route with compressed/expanded bounds and
digest verification, not the generic 8 MiB relay. Occurrence raw data can continue
through admitted queryd where appropriate, but issue membership/workflow comes from
errorsd so clients do not reconstruct authority with SQL.

Tauri implements the same `ControlTransport` interface by dialing configured daemon
addresses from trusted native configuration. It does not reuse query framing for
HTTP CRUD or expose raw addresses to frontend state.

## Authentication, authorization, and CSRF

The existing shared password creates one HttpOnly, SameSite=Strict signed cookie.
V1 can provide all authenticated users full issue/alert administration only if
review explicitly accepts that limitation. UI and API state it. With no distinct
principals, source-context access is a deployment-level on/off policy—not per-user
authorization—or stronger identity/RBAC becomes a release prerequisite.

Before any mutation route ships, add a random per-session nonce to the signed cookie
(the current expiry-only cookie cannot bind CSRF state), require secure cookies/TLS,
verify same-origin Origin/Referer where present, and require a nonce-bound CSRF token/
custom header for state-changing requests. SameSite is defense-in-depth, not the
sole contract. Login-screen public
error intake is a different narrowly scoped endpoint and never grants control access.
Audit actor is the available authenticated identity (`shared-admin` initially), not
untrusted browser text.

Private errorsd/alertd control listeners are not the unauthenticated status listener,
bind loopback by default, and require scoped high-entropy bearer credentials or mTLS
from Phase 0 for webui, Tauri, and CI callers. The browser session cookie is never
forwarded as service authentication. Capabilities separate issue mutation, source-
context read, artifact upload, rule mutation, and destination administration.

## Frontend state architecture

Add cohesive stores such as:

```text
desktop/src/store/errors.ts
desktop/src/store/issueDetail.ts
desktop/src/store/alerts.ts
desktop/src/store/destinations.ts
```

Stores own typed transport calls, pagination, selected IDs, request cancellation,
status unions, conflict reconciliation, and polling. Arrow Tables remain signals
where existing query code requires it. Components consume stores directly; route
IDs are inputs, not props threaded through the tree. Shared domain types/formatters
live outside components. Do not expand the current Explore `store.ts` into a
cross-product mega-store.

Each operation has `idle | loading | ready | refreshing | error` plus explicit
`stale/partial` metadata. Poll from completion with visibility-aware backoff and
cancel on route/target change. Background refresh never erases a usable view on
transient failure. Expected validation/auth/conflict errors remain UI feedback;
unexpected client faults may go to the browser SDK without recursively reporting
the reporting UI.

## Accessibility and responsive behavior

All exception text, source context, comments, annotations, URLs, and previews are
attacker-controlled and render as text only: no untrusted HTML/Markdown, URL schemes
are allowlisted, downloads use safe content-disposition/type headers, CSP is
restrictive, and diagnostics escape control/log-injection characters.

Status/color always has text/icon; tables support keyboard row selection and
stable focus after refresh; mutations and conflicts use `role=alert`/live regions;
stack frames are navigable lists with copy controls and raw text alternative;
charts/timelines have tabular summaries. Virtualized long stacks preserve screen-
reader access. Mobile collapses detail into routes/drawers rather than hiding
workflow. Destructive actions require explicit confirmation and name scope.

## Partial and degraded states

- errorsd unavailable: Explore still works; Errors shows upstream unavailable.
- rebuilding projection: serve last snapshot with age/progress or explicit not-ready.
- artifact missing: raw stack and retry/status remain visible.
- raw occurrence retained away: aggregates/audit remain with clear unavailable detail.
- queryd unavailable: issue summary/workflow remain; linked telemetry says unavailable.
- alert evaluation paused: prior state shown stale, never OK.
- notifier dead: issue remains firing/resolved independently; delivery diagnostic
  links to dead result.
- browser target lacks errors/alerts capability: route explains configuration rather
  than rendering an empty healthy list.

## Verification

- Contract tests share versioned JSON fixtures between Rust and TypeScript.
- Webui tests cover auth, CSRF, target allowlisting, header stripping, route limits,
  unavailable/conflict/overload propagation, and no address/secret leakage.
- Store tests cover pagination merge, cancellation, stale refresh, conflicts,
  pending durable folds, target changes, and exactness labels.
- Component/accessibility tests cover keyboard/focus/live regions/raw stack and
  responsive route behavior.
- Browser and Tauri integration tests exercise issue-to-occurrence-to-trace,
  resolve/regression, monitor validation, test send, dead letter, and manual retry.

## Open questions for review

- One combined control daemon endpoint or separate errorsd/alertd targets?
- Is shared-admin authorization acceptable for first release, especially source
  context and destination mutation?
- Which issue list facets are guaranteed efficiently indexed in v1?
- Are comments/assignment/merge included in initial UI or later lifecycle phases?
- Does artifact upload belong in this UI, CLI/CI only, or both with different auth?

## References

- [Error monitoring](error-monitoring.md)
- [Issue lifecycle](error-issues.md)
- [Alert evaluation](alert-evaluation.md)
- [Notification delivery](notification-delivery.md)
- [Browser SDK](browser-error-sdk.md)
- `docs/design/v1.0-web-ui.md`
- `desktop/src/{App,store}.tsx`; `desktop/src/components/explore/InspectorRail.tsx`
- `crates/scry-webui/src/{lib,auth,query}.rs`
