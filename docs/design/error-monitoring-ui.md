# Errors, issues, and alerts UI/API — Design

Status: partial — issue list/basic detail and first scalar Alerts CRUD/state UI implemented; rich issue inspection and later alerting views outstanding
Owner: Bart
Last updated: 2026-09-22

## Implementation status

This document presents the backend contracts in the rest of the
[Error monitoring suite](error-monitoring.md). It does not redefine their state.
The issue list is now served over the query wire protocol and displayed in a
browser `/errors` route. A basic `/errors/:issueId` route shows issue metadata and
recent occurrence identifiers, timestamps, and trace IDs; the read API also returns
span IDs. Rich occurrence inspection, trace navigation, grouping explanation,
workflow mutations, and the later delivery/silence/history alert surfaces remain
outstanding. The first scalar alert slice now exposes secure shared-admin rule CRUD,
validation, current state, and status polling in `/alerts`.

### Done

- [x] **Frontend survey.** Solid routing/stores, Arrow query transport, inspector,
  browser/Tauri split, webui authentication, and inert Alerts route are mapped.
- [x] **Phase 1a — issue list view.** `/errors` route added to the SolidJS app with
  a table showing title, occurrence count, max severity badge, grouping quality,
  and first/last seen timestamps. Store signals (`issueListStatus`, `issues`,
  `issueListError`, `issueListUpdatedAt`) and `refreshIssues()` action poll via
  `fetchIssueList()` over the query wire (`IssueListRequest` → `IssueListResponse`).
  The webui relay passes frames unchanged. Queryd `--errors-db` opens `errors.sqlite`
  read-only and serves from a `Mutex<ErrorsDb>`. Nav link placed between Alerts and
  Fleet. Error state (ISSUES_UNAVAILABLE) handled gracefully.
- [x] **Phase 1b — basic issue detail read.** `/errors/:issueId` is linked from the
  inbox and polls `IssueOccurrencesRequest` over the query wire. SQLite/queryd,
  typed client/store state, and the Solid page expose the issue summary plus recent
  event IDs, occurrence timestamps, and trace IDs. The API additionally returns
  span IDs, which the page does not display. This is metadata-only, not the designed
  occurrence inspector.
- [x] **First alerting vertical slice — browser rule CRUD/state.** Added direct
  same-target webui-to-alertd proxying, environment bearer service auth, secure
  cookies by default, nonce CSRF/same-origin mutation checks, bounded proxying,
  dedicated Solid alert store, scalar rule list/create/edit/delete/validate, conflict
  handling, polling, and explicit alert state/staleness presentation. Delivery UI
  and issue/artifact mutations are not included.

### Outstanding

- [ ] **Phase 0 — remaining control APIs.** Add bounded revisioned proxies and typed
  clients for issues, artifacts, silences, and later delivery operations.
- [ ] **Phase 1c — complete issue inspection and workflow.** Add event deep links,
  raw occurrence inspection, exception chain, grouping explanation, stack/source
  display, actual trace navigation and nearby logs, facets/audit/monitor panels,
  and workflow actions (resolve/ignore/assign). Basic issue metadata and recent
  occurrence rows are implemented.
- [ ] **Phase 2b — later Alerts views.** Add full transition history, silences, destinations,
  outbox, and delivery diagnostics after their backend phases exist.
- [ ] **Phase 3 — parity and accessibility.** Extract the error views from the
  monolithic store, add completion-relative visibility-aware polling, and implement
  keyboard/screen-reader behavior and responsive layouts. Basic list/detail reads
  already use the shared browser/Tauri query transport.
- [ ] **Phase 4 — verification.** Add list/detail query-handler, typed-client,
  store, and component tests, followed by control contract, auth/CSRF, conflict,
  stale/partial, pagination, accessibility, browser, and Tauri coverage.

## Why this exists

The SolidJS application has Explore, Dashboards, Alerts, Errors, and Fleet routes.
The first scalar Alerts backend/UI now exists; this section records the original
motivation for keeping its control path separate. Generic telemetry reads travel as
framed queries returning Arrow through a deliberately protocol-blind webui relay. Error
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
- Fine-grained organization/team RBAC; v1 intentionally uses one shared-admin
  authorization level for every authenticated browser session.
- Session replay, AI diagnosis, ticket integrations, or chat-based mutation.
- Exposing source-map objects, notifier secrets, or arbitrary server errors.

## Information architecture

Add `/errors` as a first-class route; retain `/alerts` for monitors, with delivery
subroutes appearing only in the later delivery slice:

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

Implemented alert mutations include `If-Match` revision and a UUID
`Idempotency-Key`, backed by object ETag CAS. Create/update persist a command receipt
containing the request digest and result identity; delete persists a command
Tombstone, so exact ambiguous retries replay while key reuse with different input
conflicts. A 409 asks the user to reload and reapply; it never silently overwrites.
A general command-status endpoint and explicit locally-pending-fold presentation
remain future control-plane work.
Optimistic UI is limited to reversible presentation and reconciles from server truth.

## Alerts experience

The first vertical slice replaces the inert Alerts page with browser CRUD for
ungrouped restricted scalar-SQL rules and their explicit no-data/error policies,
backed by the separate alertd service. It does not expose destinations, delivery,
or grouped-rule authoring. The browser backend and UI are implemented; native Tauri
control transport and the later alert surfaces remain outstanding.

The eventual list shows monitor
type, enabled, current state/groups, last/next evaluation, stale/error/no-data,
firing since, matched issue/query, destinations, and delivery failures.

Editor uses typed builders:

- issue event source/filter/triggers; or
- telemetry signal, equality matchers, explicit lookback, constrained SQL/expression,
  scalar reducer/comparator/threshold;
- schedule/jitter, `for`, recovery, no-data and execution-error policies;
- labels/annotations, destinations, resolve/reminder behavior.

The browser currently exposes structural validation only. Alertd also has a
side-effect-free test-evaluation endpoint that reports value/no-data/error, but typed
client/UI output for that endpoint, schema/timing details, and resource diagnostics
remain outstanding. Raw SQL remains an Advanced field with server validation and a
prominent bounded-execution explanation.

Detail shows the state timeline, values, evaluations, errors, silences, transitions,
and notification intents/attempts. `NoData`, `Error`, `Stale`, and `Disabled` are
not styled as healthy green. Silence creation requires scope, start/end, actor, and
preview of affected groups. Dead letters link to redacted attempt diagnostics and
manual retry.

Alerts has separate **Monitors** and **Notification targets** sections. A target shows
kind/name/enabled/revision/health and owns its selected built-in format or complete
custom placeholder template; formats are not independently managed or shared.
Secret fields are write-only: the API accepts plaintext only on mutation, encrypts it
for object storage, and never returns plaintext or ciphertext. Test-send ships with
target CRUD and uses the actual worker path. Payload preview is scrubbed and excludes
secret-derived headers/signatures.

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

Browser alert operations use the same opaque configured target IDs already used for
query selection; the client never learns or supplies an address. For the first
slice, webui resolves that target ID to its configured alertd endpoint and proxies
rule CRUD directly to alertd. Alertd independently resolves the same target ID to a
configured queryd endpoint and connects directly to queryd for evaluation; alert
queries never hairpin through webui. Webui strips hop-by-hop/credential headers,
applies operation-specific body/time/admission limits, and translates neither domain
payload nor private errors beyond a stable envelope.
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
V1 deliberately grants every authenticated session the `shared-admin` identity and
full alert-rule administration; there are no per-user roles in this slice. UI and
API state that limitation. Cookies are `Secure` by default. Plain-HTTP development
requires an explicit insecure-cookie opt-out; startup must not infer it from the bind
address or request scheme. With no distinct principals, source-context access is a
deployment-level on/off policy—not per-user authorization.

Before any mutation route ships, add a random per-session nonce to the signed cookie
(the current expiry-only cookie cannot bind CSRF state), require secure cookies/TLS,
verify same-origin Origin/Referer where present, and require a nonce-bound CSRF token/
custom header for state-changing requests. SameSite is defense-in-depth, not the
sole contract. Login-screen public
error intake is a different narrowly scoped endpoint and never grants control access.
Audit actor is the available authenticated identity (`shared-admin` initially), not
untrusted browser text.

Private errorsd/alertd control listeners are not the unauthenticated status listener
and bind loopback by default. The first alerting slice requires a scoped high-entropy
service bearer token supplied to webui and alertd through environment variables.
Webui authenticates directly to alertd with that token; the browser session cookie
is never forwarded as service authentication. Token files and live reload are not
supported: rotation updates the environment and restarts the affected services.
Later mTLS remains possible but is not the v1 contract. Capabilities separate issue
mutation, source-context read, artifact upload, rule mutation, and destination
administration.

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

## Selected first-slice decisions and remaining review

D-076 selects separate alertd topology, direct webui-to-alertd and alertd-to-queryd
hops, reuse of opaque target IDs, environment-supplied service bearer auth with
restart-based rotation and no token files, shared-admin browser CRUD, secure cookies
by default with explicit insecure opt-out, and no delivery UI. These are decisions;
the corresponding alerting implementation remains outstanding.

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
