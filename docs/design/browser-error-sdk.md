# Browser error SDK and build integration — Design

Status: draft, not yet implemented
Owner: Bart
Last updated: 2026-09-07

## Implementation status

This SDK emits the contract in [Error intake](error-intake.md) and uploads artifacts
defined by [Error grouping](error-grouping.md).

### Done

- [x] **Browser ecosystem survey.** OTel JS/web exception behavior, browser error
  events/privacy, current Scry Solid boot, transport, and Vite build are mapped.

### Outstanding

- [ ] **Decision — package shape.** Confirm `@scry/browser` as a policy layer over
  upstream OTel packages versus documentation/helpers only.
- [ ] **Phase 0 — core capture.** Implement singleton init, global/rejection/manual
  capture, safe normalization, occurrence IDs, context, filtering, batching, and
  self-diagnostics.
- [ ] **Phase 1 — framework integrations.** Add Solid ErrorBoundary helper first,
  then separately tested React/Vue adapters as demand requires.
- [ ] **Phase 2 — transport resilience.** Add browser-safe OTLP export, bounded
  memory/offline policy, retry/backoff, unload flush, and public intake config.
- [ ] **Phase 3 — build tooling.** Add Vite debug-ID injection, source-map manifest
  generation/upload CLI, release/build association, and CI verification.
- [ ] **Phase 4 — first-party adoption.** Instrument Scry's own browser bundle and
  meaningful catch boundaries with recursion/expected-error filters.
- [ ] **Phase 5 — verification.** Browser matrix, privacy, hostile values, lifecycle,
  duplication, transport failure, sourcemap, and end-to-end tests.

## Why this exists

Raw OpenTelemetry can export browser telemetry, and upstream experimental web
exception instrumentation captures ordinary uncaught errors and rejected promises.
A Sentry-like experience also needs stable occurrence IDs, robust normalization of
arbitrary rejection reasons, browser lifecycle handling, privacy defaults, build/
debug identity, and ergonomic framework/manual capture. Those are policy and
product conventions, not a reason to invent a proprietary wire.

`@scry/browser` is therefore proposed as a small, standards-based layer that emits
OTLP exception LogRecord Events. Applications already owning an OTel SDK can use
lower-level helpers instead of installing a second provider.

## Goals

- Capture uncaught exceptions, unhandled rejections, framework failures, and manual
  exceptions into the canonical OTel event contract.
- Coexist with existing OTel providers/instrumentations and install exactly once.
- Preserve active trace context when available without treating absence as failure.
- Attach service/version/environment, browser, session, pseudonymous user, URL,
  breadcrumbs, and frame debug IDs under explicit privacy policy.
- Deliver best effort through a bounded resilient queue without blocking the app.
- Integrate deterministic debug IDs and authenticated artifact upload into builds.
- Never recursively report SDK/exporter failure as application error.

## Non-goals (v1)

- Session replay, DOM snapshots, screenshots, console monkey-patching by default,
  network-body capture, local variables, or resource-load errors as exceptions.
- Hiding the experimental status of browser OTel instrumentation/logs.
- Treating a public app key as a secret or proof that an event is genuine.
- Unlimited persistent offline storage or guaranteed page-exit delivery.
- Shipping all framework adapters before there is demand and test coverage.

## Package modes

Proposed API:

```ts
initScry({
  endpoint: "https://telemetry.example.com",
  appKey: "public-app-id",
  serviceName: "shop-web",
  serviceVersion: "4f8b2d1",
  environment: "production",
  transport: "otlp-http-protobuf",
  privacy: { ... },
})

captureException(error, context?)
setUser({ id?, hash?, ... })
setSession({ id, previousId? })
addBreadcrumb(...)
flush({ deadlineMs })
shutdown()
```

Two integration modes avoid duplicate providers:

1. managed: SDK constructs/configures the required browser LoggerProvider/exporter;
2. attached: application supplies an OTel Logger or callback and Scry only captures,
   normalizes, and emits.

Initialization uses a well-known global/symbol registry, returns the existing
compatible instance on duplicate init, and loudly rejects conflicting configuration.
Shutdown removes exactly its listeners and flushes within bounds. Multiple bundles
cannot install duplicate global handlers.

## Capture surfaces

Register additive `window.addEventListener("error", ...)` and
`unhandledrejection` listeners. Never replace `window.onerror`, suppress default
console reporting, or call `preventDefault`. `ErrorEvent.error === undefined`
(resource errors and muted cross-origin errors) is not automatically an exception;
a separate resource-error event can be designed later.

Normalize without invoking unsafe getters repeatedly:

- Error/DOMException: name, message, stack;
- AggregateError: bounded ordered child exception chain with dropped count;
- string/number/boolean/bigint/symbol/null/undefined: safe type and display value;
- object/function: defensive tag/stringification with cycle/depth/size protection;
- Promise rejection may be temporarily unhandled; policy must decide a short export
  delay versus immediate immutable capture plus later annotation.

Manual `captureException` accepts context while active OTel context still exists
and is preferred inside catch blocks/framework boundaries. It creates
`scry.event.id` before queueing. Global callbacks often have no active span; omit
correlation rather than fabricate it.

A top-level Solid helper wraps `ErrorBoundary`, records exactly once, and preserves
application fallback/reset semantics. For Scry's own app it initializes before
`render()` in `desktop/src/index.tsx`; expected authorization, validation,
cancellation, and typed server errors at existing catch boundaries are filtered or
captured with explicit classification rather than all reported as bugs.

## Context and breadcrumbs

Resource: `service.name`, namespace, `service.version`, and
`deployment.environment.name`. Dynamic event attributes: browser conventions,
`user_agent.original`, current document URL after scrub, `session.id`, user fields,
route, and custom context. Use `user.hash` by default where possible; never cookies,
auth headers, tokens, or local/session storage values.

Breadcrumbs are a bounded in-memory ring of structured preceding events (navigation,
user action names, console categories if opted in, and request metadata without
bodies/secrets). On capture, snapshot at most configured count/bytes into typed
occurrence context. Breadcrumbs do not independently create error issues. Values
pass the same scrubber and cardinality/length limits.

Fetch/XHR tracing may carry context and failures through existing OTel
instrumentation, but SDK does not report every non-2xx as an exception. Application/
HTTP semantic conventions determine operation error status. Avoid instrumenting the
telemetry endpoint itself and redact URL queries/headers.

## Queue, retry, and lifecycle

Use a bounded in-memory batch queue measured by events and encoded bytes. Admission
prioritizes the first unseen local group and FATAL/ERROR, but cannot know global
issue state. On overflow apply documented drop policy and local counters; never grow
without bound. An optional IndexedDB queue is separately bounded by bytes/age and
encrypted only if a credible browser key model exists—otherwise sensitive persistence
is opt-in, not default.

OTLP/HTTP protobuf or JSON is sent to the hardened browser intake's required durable
Scry path. Only its WAL-backed acknowledgement is `durably accepted`; the generic
fan-out gateway's enqueue 2xx is not sufficient and is not used as this endpoint.
gRPC is not a browser option. Exponential backoff with full jitter handles transient network,
429/5xx, and bounded Retry-After. Permanent 4xx drops with diagnostics. Occurrence
ID remains stable over retries; batches can split without changing IDs.

Use normal batching while visible. On `visibilitychange` to hidden/pagehide, attempt
a short deadline flush using keepalive/sendBeacon only when encoding/headers/size
are supported by the intake. Beacon acceptance means queued by the browser, not
server durability. Do not block unload. Online/offline events adjust scheduling but
are hints. SDK self-diagnostics go to a callback/console debug mode and never through
its own captured global error path.

## Privacy and sampling

Client scrub improves ergonomics; server scrub is authoritative because clients can
be old or malicious. Default-drop cookies/auth headers, URL credentials, configured
query keys, and obvious secrets. Bound/custom-allow attribute keys. Messages/stacks
may contain sensitive values and require documented patterns; overbroad destructive
scrub rules must be testable with preview fixtures.

Sampling is independent from trace head sampling. Retain unhandled ERROR/FATAL by
default, with bounded flood protection. Sampling/rate limiting occurs after enough
normalization to apply group-aware policies and records decision/probability when
possible. Never promise exact counts. First/new global issue cannot be known client-
side; server processing protects it if received.

User/session capture requires explicit application calls, consent/retention policy,
and opaque non-secret IDs. Cross-origin browser restrictions and ad blockers can
remove stack/context or all delivery; quality/status reflects this.

## Debug IDs and build workflow

A Vite plugin (then other bundlers separately) generates one deterministic UUID
per output JS+map content, injects `//# debugId=<uuid>` near generated EOF, writes
map `debugId`, and emits a build manifest mapping generated URL/module to debug ID.
Injection order is specified so ID derivation excludes its own fields and remains
reproducible. Production validation fails if generated and map IDs disagree.

Runtime captures per-frame/module debug metadata when available. `service.version`
is the release display value; build ID groups artifacts but exact debug ID selects
the map. Mixed-build pages are valid.

CI uses a new `scry artifacts upload` (or an artifacts subcommand under the selected
control role) with a scoped artifact-upload token or mTLS identity—not the public
app key or web session cookie—to upload generated
code/map, verify digests and embedded IDs, then PUT manifest last. It can strip
`sourcesContent` per policy. Idempotent identical upload succeeds; differing bytes
for an existing ID fail the build. A verify/dry-run command checks bundle coverage
without mutation.

Artifacts should upload before deployment. Late upload still queues bounded
reprocessing; unlike a purely ingest-time system, historical events can improve
according to the reviewed membership policy.

## Public configuration and CSP/CORS

Only endpoint, public app key, service metadata, sampling/privacy policy, and safe
feature flags enter the bundle. Vite's `VITE_*` variables and existing
`__APP_VERSION__` support Scry's own adoption. No artifact/notifier/operator secret
is embedded.

Deployment docs require CSP `connect-src` for the endpoint and exact server CORS
origins/preflight. Same-origin webui proxy avoids CORS but cannot report login/pre-
auth errors if it requires session; public error intake remains separately scoped.
App key/origin can be copied/spoofed, so server quotas, schema validation, and scrub
remain mandatory.

## Compatibility and versioning

Version SDK API/config, occurrence extension, breadcrumb schema, debug manifest,
and build plugin independently. Pin and test compatible OTel package ranges; attached
mode avoids taking ownership of a user's global provider. Unknown config keys can
fail in development and be reported via diagnostics in production without crashing
the host app.

When upstream OTel browser exception behavior changes, canonical output fixtures
protect Scry's contract. Prefer standard fields; Scry extensions are namespaced and
listed centrally. SDK upgrades do not silently change fingerprint inputs—server
processor versions own grouping.

## Verification

- Real-browser tests for synchronous handler throws, promise reasons of every type,
  AggregateError/cycles/hostile getters, muted cross-origin errors, resource errors,
  rejectionhandled, duplicate init, shutdown/re-init, and framework reset.
- Context tests for active/missing spans, service/release/environment, privacy,
  session/user changes, breadcrumbs, and no credential capture.
- Transport tests for batching/bytes, 429/Retry-After, offline/online, pagehide,
  keepalive/beacon limits, abort, overflow/drop priorities, and stable retry IDs.
- CSP/CORS/origin/app-key integration against the actual hardened endpoint.
- Reproducible build/debug-ID tests, mixed chunks/builds, upload retry/collision,
  stripped sourcesContent, and late-upload end-to-end symbolication.
- Instrument Scry's own Solid app in a smoke test: thrown boundary/global/rejection
  reaches raw telemetry and one issue without recursive reporter events.

## Open questions for review

- Full `@scry/browser` package or lower-level helpers/documented upstream OTel setup?
- Managed and attached mode both in v1?
- Immediate unhandled rejection export or short `rejectionhandled` grace?
- IndexedDB queue opt-in, deferred, or excluded?
- Which breadcrumbs are enabled by default?
- Is Vite the only first build integration, and where does artifact upload auth live?

## References

- [Error monitoring](error-monitoring.md)
- [Error intake](error-intake.md)
- [Error grouping](error-grouping.md)
- [Errors and alerts UI/API](error-monitoring-ui.md)
- `@opentelemetry/instrumentation-web-exception` source/tests.
- WHATWG/MDN Window error and unhandled rejection behavior.
- OpenTelemetry JS browser SDK/exporter guidance.
- `desktop/src/index.tsx`; `desktop/vite.config.ts`; `desktop/src/env.ts`
