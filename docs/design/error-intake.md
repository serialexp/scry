# Error event contract and intake — Design

Status: partial — lossless logs v2 intake landed; canonical extraction outstanding
Owner: Bart
Last updated: 2026-09-10

## Implementation status

This document refines [Error monitoring](error-monitoring.md). D-073 accepts a
lossless logs v2 representation; reader, native writer, and gateway OTLP producer
mechanics have landed, while operator enablement and canonical extraction remain.

### Done

- [x] **Standards survey.** Current OTLP LogRecord/Event, exception-log,
  AnyValue, trace-context, browser, user, session, service, and deployment
  conventions have been checked.
- [x] **Current mapping survey.** Gateway HTTP/gRPC mappings and their fidelity
  gaps have been identified.
- [x] **Decision — logs representation.** D-073 selects logs v2: bounded canonical
  typed raw records alongside stable flat query projections.
- [x] **Phase 0a — reader/storage/query contract.** Added the bounded canonical
  envelope/validator, exact Parquet v2 schema, v1/v2 query normalization, nullable
  legacy/live compatibility, and schema-safe opaque compaction.
- [x] **Phase 0b — native writer mechanics.** The ingest server can negotiate logs
  v2 behind explicit `--enable-logs-v2`, write exact raw records plus the stable
  projection, and rotate live/recovery blocks before schema changes.
- [x] **Phase 0c — producer fidelity.** Gateway OTLP maps into bounded canonical
  logs v2 with event/observed time, typed values, correlation, schema and dropped
  counts, and reselects the format from each connection's negotiated capabilities.

### Outstanding

- [ ] **Phase 1 — canonical extraction.** Normalize exception logs and deprecated
  span events into one bounded occurrence contract with exact/heuristic dedup.
- [ ] **Phase 2 — hardened browser intake.** Add public app keys, exact origin
  policy, quotas, small bounds, scrubbing, and dedicated status.
- [ ] **Phase 3 — trusted server guidance.** Publish per-language setup and
  independent exception-log sampling guidance.
- [ ] **Phase 4 — verification.** Add lossless format, rejection, abuse, replay,
  dual-signal, correlation, and end-to-end tests.

## Why this exists

Scry Gateway accepts OTLP logs and traces over HTTP protobuf/JSON/gzip and gRPC,
but its current log mapping drops top-level `event_name` and observed time,
duplicates trace IDs into string attributes, and stringifies every `AnyValue`.
Those losses are tolerable for basic log search but not for an error product that
must preserve typed context, distinguish event schema from occurrence identity,
and reprocess historical events.

This design defines what producers send, what Scry accepts and retains, and how
untrusted browser traffic differs from trusted server telemetry. Grouping begins
only after this contract; see [error-grouping.md](error-grouping.md).

## Goals

- Make the stable OpenTelemetry exception LogRecord Event the primary format.
- Continue accepting deprecated exception span events during ecosystem migration.
- Preserve raw typed values and canonical top-level fields without moving standard
  data into ad-hoc attributes.
- Assign a retry-stable occurrence identity independent of issue grouping.
- Keep exception retention independent of head trace sampling.
- Bound allocation, nesting, strings, cardinality, and decompression before data
  enters durable storage.
- Provide a browser-safe intake surface without weakening the normal gateway.

## Non-goals (v1)

- Defining issue grouping, source-map lookup, or notification state here.
- Treating any client claim—including severity, user, origin, or fingerprint—as
  trusted truth.
- Guaranteed delivery from a terminating/offline browser.
- Capturing resource load failures as fabricated exceptions. They may become a
  separate event type later.
- Supporting Sentry Envelope/DSN compatibility unless separately designed.

## Canonical producer event

The primary occurrence is an OTLP `LogRecord` with a non-empty top-level
`event_name`:

```text
event_name = "exception"                         # global/unhandled
# or "http.server.request.exception"             # operation-specific
time_unix_nano = when the exception occurred
observed_time_unix_nano = when observed/exported
severity_number = ERROR(17) for ordinary unhandled exceptions
trace_id/span_id/flags = active context when available
body = optional display text
attributes:
  exception.type
  exception.message
  exception.stacktrace
  scry.event.id
  ...bounded context...
```

`event_name` identifies a low-cardinality event schema and contains no dynamic
values. It is not `event.name` (deprecated), an occurrence ID, or an issue
fingerprint. `exception.type` is distinct from low-cardinality `error.type`,
which describes operation outcomes. `error.message` is not introduced.
`exception.escaped` is deprecated and not required. Handled/mechanism semantics,
when explicitly captured, use versioned `scry.error.*` attributes until an OTel
standard exists.

Severity follows OTel's expected impact: FATAL 21 for likely process termination,
ERROR 17 for unhandled non-terminating exceptions, WARN 13 for expected handled
failures, and DEBUG 5 for cancellation/non-problems. Detection uses event schema
and exception content, not severity alone.

### Resource and context attributes

Stable application identity belongs in Resource attributes:

- `service.name` (required by Scry browser/server setup)
- `service.namespace`
- `service.version` as release/build display identity
- `service.instance.id` where meaningful
- `deployment.environment.name`

Do not create a competing generic `release` truth. A legacy alias may map into
`service.version` while preserving its raw source.

Users and sessions change within a process and are occurrence/context attributes:
`user.id`, preferably privacy-preserving `user.hash`, optional user display fields,
`session.id`, and `session.previous_id`. Authentication cookies/tokens are never
identifiers. Browser context uses applicable OTel browser attributes,
`user_agent.original`, and `browser.document.url.full`, with privacy policy applied
before durability.

## Occurrence identity

OTel defines no generic event ID. Scry adds `scry.event.id`: a canonical lowercase
128-bit random UUID generated before client buffering and retained unchanged over
SDK, exporter, collector, gateway, WAL replay, and network retries. Scope is
`(deployment, app_id, event_id)`. A repeated ID with canonically identical content
is idempotent; the same ID with different canonical content is rejected and
counted as an identity collision.

Gateway-generated IDs are allowed only when a producer cannot supply one. They
provide internal identity after that boundary but cannot make pre-gateway retry
delivery exactly deduplicable. For legacy records, a synthetic ID hashes canonical
semantic content plus event timestamp and canonical stream identity; identical real
occurrences can still collide and compaction/replay can still double-count, so
provenance and quality are explicit. Physical block/row identity is never used.

Raw ingest has no deployment-wide ID index. It may append duplicate/conflicting
representations; exact deduplication and collision quarantine happen in the durable
error projection. Intake may reject a conflicting ID only when the hardened route
consults authoritative durable identity state. Canonical occurrence v1 excludes
source signal, gateway receipt time, and physical location; it includes scrubbed
semantic content, normalizes Unicode and float `-0`/NaN/Inf representations, rejects
duplicate map keys, and records canonicalization and scrub-policy versions. Scrub
precedes content hashing. First-folded canonical content wins by deterministic
object order; conflicting content is quarantined rather than silently counted.

Issue fingerprint is deliberately separate. One issue contains many event IDs.

## Typed value preservation

OTLP AnyValue supports strings, booleans, doubles, signed int64, bytes, null,
arrays, and nested key/value lists. Empty and zero values are meaningful; map
order is not. The native representation must retain type tags, int64 precision,
bytes, null, array order, and nested maps. Body remains typed. Duplicate keys are
rejected or resolved according to one documented boundary rule before hashing.

Canonical serialization for identity/deduplication sorts map keys, preserves array
order and all type tags, and length-prefixes components. JSON decoding must not
round int64 through a JavaScript-style number. D-073 selects logs v2: each accepted
record retains the bounded canonical typed payload used by detail/export/
reprocessing alongside a selected stable flat query projection.

At minimum retain top-level `event_name`, event/observed timestamps, severity
number/text, body, attributes, trace/span IDs and flags, Resource and scope
metadata, schema URLs where useful, and dropped attribute counts. Standard fields
remain standard fields; compatibility aliases such as `otel.trace_id` may be
synthesized only at query/UI boundaries.

## Deprecated span exception events

A trace span event named `exception` with standard exception attributes is
accepted and normalized. During OTel's migration, one logical exception may arrive
as a log, span event, or both (`logs/dup`). Exact cross-signal dedup uses the same
producer `scry.event.id` embedded in both representations.

Without that ID, Scry may identify a probable duplicate within a narrow bounded
window using trace ID, span ID, event time, exception type, and a canonical digest
of exception content. This is explicitly heuristic, records its confidence, and
must never collapse independent exceptions merely because messages match. UI
counts distinguish exact from heuristic dedup policy.

## Trace correlation and sampling

Canonical correlation is the LogRecord's binary top-level trace ID (16 nonzero
bytes), span ID (8 nonzero bytes), and low trace-flag bits. A span ID without a
trace ID is invalid. Browser global callbacks often run after active context has
unwound, so no trace context is normal.

Exception logs are not dropped merely because their linked trace was head-sampled
out. Recommended SDK policy retains ERROR/FATAL exception events independently,
then uses error-aware tail sampling to preserve linked traces where deployed.
First-seen/new-regression protection happens after grouping and cannot repair an
event discarded at the SDK.

Product counters are observed telemetry, not claims about all real exceptions.
Where sampling occurs after grouping, retain the sampling decision/probability and
avoid presenting sampled count as exact raw count.

## Intake surfaces

### Trusted server intake

Existing gateway OTLP HTTP and gRPC routes remain the standard server path behind
operator TLS/auth ingress. HTTP protobuf, JSON, gzip, and gRPC must produce the
same canonical representation and rejection accounting. Partial success names
invalid records; no format silently drops an exception.

### Public browser intake

Do not expose the existing general unauthenticated gateway listener directly.
Add a dedicated endpoint—same-origin through webui or a hardened gateway listener—
that accepts only the required OTLP Logs subset initially. Its public `app_key`
identifies policy, quotas, and revocation; it is not secret authentication.

Required controls:

- The public key resolves server-side to authoritative `app_id`; client service
  identity is retained only as an untrusted facet and cannot select another app.
- A stable `deployment_id` is created in a bucket control manifest and scopes IDs,
  records, quotas, and restore; roles fail startup on bucket/namespace mismatch.
- TLS; explicit exact allowed origins and preflight policy (origin absence remains
  possible for non-browser clients and is separately governed).
- Per-app event/byte/concurrency/cardinality token buckets and global overload
  admission; rate-limit responses include bounded retry guidance.
- Route-specific compressed and expanded limits substantially below the generic
  32 MiB receiver, applied before allocation and after decompression.
- Attribute count, nesting, array, string, stack, breadcrumb, debug-image, and
  batch-record limits; identity-bearing fields fail loudly instead of truncating.
- Server-side allow/drop/hash/redact rules before durable storage, including URL
  queries/fragments, headers, cookies, user fields, messages, stacks, and custom
  attributes.
- No ambient forwarding credentials, no browser-selected upstream address, and
  no notifier/artifact secrets in client configuration.
- Sparse failure logs and dedicated accepted/rejected/rate-limited/scrubbed status
  without reflecting sensitive payloads.

Illustrative starting bounds, to qualify rather than accept blindly: 1 MiB expanded
request/event, 256 KiB raw stack, 32 exception-chain elements, 256 frames per
exception, 64 debug images, 8–16 KiB ordinary strings, and explicit batch limits.

## Browser edge cases

The SDK observes `window.error` and `unhandledrejection` without replacing existing
handlers or calling `preventDefault`. It must normalize strings, primitives, null,
DOMException, AggregateError, objects with hostile getters, and ordinary Error.
Resource errors lacking `ErrorEvent.error` are not exception events. A later
`rejectionhandled` annotates history or suppresses a not-yet-exported event under
a documented delay; it never deletes accepted history.

Cross-origin browser privacy may reduce an error to “Script error.” or suppress a
rejection entirely. The intake accepts missing stack/context and marks capture
quality. Initialization is singleton/idempotent across multiple bundles and guards
against recursively reporting exporter failures.

## Failure semantics

- Unsupported or over-limit record: reject that record with partial-success count
  and stable numeric reason; accept independent valid records.
- Scrubbing failure: fail closed for the affected record, never store unsanitized
  content as fallback.
- The generic fan-out gateway retains D-041 semantics: its 2xx means received/
  enqueued, not durable acceptance, and a full sink queue can lose data after ACK.
  It must never be described as the durable browser-error endpoint.
- The hardened error route requires a durable Scry sink and answers success only
  after the native ingest WAL ACK; queue/downstream pressure returns partial failure
  or 429/503 so the browser can retry. `received`, `gateway-enqueued`, and `durably
  accepted` are distinct metrics and API terms.
- Duplicate identical raw occurrence can be appended; projection folding is
  idempotent success by scoped event ID.
- Duplicate ID with differing content is quarantined during projection, counted,
  and never silently replaces the deterministic winner.
- Missing artifact/grouping service/Valkey/queryd: raw ingest continues.
- Clock skew: retain producer event time and server observed/received time. A
  server-controlled index/retention timestamp and allowed-skew policy prevent one
  future event pinning a mixed block or one old event disappearing before projection;
  out-of-range producer time is quarantined/faceted, never silently rewritten.
- Public stream identity compares canonical label bytes as well as xxh3. The same
  64-bit fingerprint with different labels is rejected; xxh3 is not a security or
  application-identity boundary.

## Schema evolution and rollout

Version independently: raw typed log encoding, Scry occurrence extension,
canonical identity serialization, scrub policy, and extracted flat projection.
Readers reject unsupported identity-critical versions rather than guessing.
D-073 reserves capability bit `0x0000_0008`, payload magic `0x534c3200`
(`SL2\0`), and canonical raw-record version 1; readers land before advertisement
or writer enablement.

The stable logs v2 Parquet/query projection has these columns in contractual order:

| Index | Column | Arrow type | Nullable | Logs v1 normalization |
| ---: | --- | --- | :---: | --- |
| 0 | `stream_fingerprint` | `UInt64` | no | existing value |
| 1 | `ts_unix_nano` | `UInt64` | no | existing value |
| 2 | `severity` | `UInt8` | no | existing value |
| 3 | `body` | `Utf8` | no | existing value |
| 4 | `attributes` | `Map<entries: Struct<keys: Utf8 non-null, values: Utf8 nullable> non-null>` | no | existing value |
| 5 | `observed_ts_unix_nano` | `UInt64` | yes | typed NULL |
| 6 | `severity_text` | `Utf8` | yes | typed NULL |
| 7 | `event_name` | `Utf8` | yes | typed NULL |
| 8 | `trace_id` | `FixedSizeBinary(16)` | yes | typed NULL |
| 9 | `span_id` | `FixedSizeBinary(8)` | yes | typed NULL |
| 10 | `trace_flags` | `UInt8` | yes | typed NULL |
| 11 | `raw_record_version` | `UInt16` | yes | typed NULL |
| 12 | `raw_record` | `Binary` | yes | typed NULL |

The normalized SQL table appends synthesized `labels` at index 13 using the same
Map layout as `attributes`. The first five v2 fields retain the exact v1 names,
types, nullability, and order. V1 normalization clones them and appends eight typed
NULL arrays; it never fabricates missing fidelity. V2 keeps a root string body as
familiar unquoted text, while non-string bodies and every identity-bearing Resource/
attribute value use deterministic typed canonical text; exact types remain in
`raw_record`. Stream identity hashes length-delimited canonical label bytes and
rejects a repeated hash with different exact labels.

Gateway canonicalization accepts at most the logs-v2 decoder's 16 MiB envelope even
though generic OTLP transport permits 32 MiB requests. Total record-encoding work is
also capped to a small multiple of the decoded OTLP size, preventing shared Resource/
scope metadata from amplifying into unbounded repeated work. It rejects affected records
with deterministic partial-success reasons for malformed IDs, unsupported Resource
entities/profile dictionary references, duplicate or empty keys, and encoding bounds.
Absent and explicitly empty OTLP bodies both normalize to canonical null; `-0.0`
normalizes to `+0.0` and NaN payloads to the canonical NaN by design.

The representation decision must specify the exact binschema/WAL/Parquet v2 shape,
version-specific DataFusion adapters into one stable query schema, and compaction
compatibility. Mixed-schema blocks compact only within `(signal, schema_version)`
until a lossless upgrade compactor exists. Rolling tests run old/new writers and
readers concurrently; adding fields without those adapters is not “additive.”

Rollout order is: ship all readers/adapters first; preserve new OTLP fields; add
typed round trips; teach query/UI aliases; and extraction shadow mode; then enable
new writers, `logs/dup`, and SDK guidance. The native writer is present behind
mutual negotiation and visible default-off `--enable-logs-v2`. The gateway requests
logs v2 automatically for canonical OTLP batches and refuses to downgrade them when
the current upstream session does not negotiate it; native-v1 and Loki inputs remain
v1. V1 and v2 share the logs WAL by explicit product decision, so once v2 has been
accepted an operator must not roll that WAL back to an older
binary that cannot replay SL2 frames. Disabling new v2 acceptance does not disable
recovery in a capable binary. Existing string-only blocks remain queryable and
produce explicitly lower-quality occurrences without fabricated fidelity or
guaranteed exact deduplication.

## Verification

- Golden protobuf/JSON/gzip/gRPC parity for every AnyValue variant, null/empty,
  int64 extremes, duplicate keys, observed timestamps, event names, scope/schema,
  dropped counts, and binary trace context.
- Exact duplicate/collision and heuristic dual-signal fixtures.
- Browser-origin/preflight, key revocation, decompression bomb, nesting, oversized
  stack, cardinality, scrub failure, hostile object, duplicate initialization, and
  unload/offline tests.
- Sampling tests proving unsampled traces do not suppress exception logs.
- End-to-end accepted event survives WAL, block, compaction, reconcile, query, and
  extraction byte-for-byte in its canonical typed form.

## Open questions for review

- Can a lossless recursive log representation and structured exception extension
  remain a coherent general log schema, or is a fifth signal warranted?
- Same-origin webui endpoint, dedicated gateway listener, or both?
- Which browser fields are default-allow, default-hash, and default-drop?
- Is temporary-unhandled rejection export delayed to observe `rejectionhandled`?
- Which SDKs can carry `scry.event.id` on both log and span event automatically?
- What are qualified production bounds and per-app default quotas?

## References

- [Error monitoring](error-monitoring.md)
- [Browser SDK](browser-error-sdk.md)
- OpenTelemetry semantic conventions: exception logs, Events, service,
  deployment environment, browser, user, and session.
- OpenTelemetry Logs data model, SDK, AnyValue, and OTLP logs protobuf.
- `crates/gateway/src/{otlp,otlp_logs,otlp_common,otlp_grpc}.rs`
- `docs/design/gateway-ingestion-protocols.md`
