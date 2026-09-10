# Error event contract and intake — Design

Status: partial — D-074 occurrence foundation implemented; browser intake and producer guidance outstanding
Owner: Bart
Last updated: 2026-09-10

## Implementation status

This document refines [Error monitoring](error-monitoring.md). D-073 accepts a
lossless logs v2 representation. D-074's coordinated occurrence foundation is now
implemented: producers emit canonical raw-record v1, ingest stamps trusted receipt
time into canonical raw-record v2 before WAL, and logs Parquet/query schema v3
exposes `received_ts_unix_nano`. Browser intake and producer guidance remain
outstanding; this implementation has not been deployed.

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
- [x] **Phase 1 — occurrence foundation.** Producers emit canonical raw-record v1;
  ingest applies uniform log/trace key redaction and stamps trusted receipt time
  before WAL into raw-record v2 and logs schema 3. Strict log-only extraction,
  deployment/app identity, immutable conditional occurrence commits, rebuildable
  deployment-bound `errors.sqlite`, bounded paging, and periodic single-writer
  `scry errors` reconciliation are implemented.

### Outstanding

- [ ] **Clustered occurrence orchestration.** Add Valkey lease/fencing, convergence
  hints, and safe multi-writer takeover; clustered mode currently fails closed.
- [ ] **Accepted-record low-latency hints.** Add the optional acceleration path;
  sealed-block/occurrence-commit reconciliation remains the correctness path.
- [ ] **Occurrence snapshots and GC.** Add bounded snapshot bootstrap, generation
  and orphan cleanup, and retention-aware garbage collection.
- [ ] **Phase 2 — hardened browser intake.** Add public app keys, exact origin
  policy, quotas, small bounds, route-specific policy, and dedicated status.
- [ ] **Phase 3 — trusted server guidance.** Publish per-language setup and
  independent exception-log sampling guidance.
- [ ] **Phase 4 — verification.** Add lossless format, rejection, abuse, replay,
  occurrence-projection, scrubbing, correlation, and end-to-end tests.

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

- Make eligible OpenTelemetry exception LogRecord Events the sole authoritative
  source of error occurrences.
- Retain trace spans as ordinary telemetry that can be linked best-effort from IDs
  carried by an exception log, never as occurrence input.
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
- Claiming that bounded key-based scrubbing removes secrets from free-text bodies,
  messages, stack traces, or other string values.

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

Stable application identity is derived by Scry rather than accepted as a separate
producer-selected identifier. Normalize `service.namespace` and required
`service.name` with one versioned algorithm, encode the pair unambiguously, and
derive `app_id` deterministically from those bytes. Missing or invalid
`service.name` makes an exception log ineligible; an absent namespace normalizes to
the documented empty namespace. The source Resource attributes remain retained:

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
`(deployment_id, app_id, event_id)`. A syntactically valid canonical ID is required
for an exception log to be eligible for occurrence extraction. A repeated ID with
canonically identical content is idempotent; the same ID with different canonical
content is rejected and counted as an identity collision.

The gateway and extractor never generate an occurrence ID. Missing, malformed, or
non-canonical IDs leave a record queryable as an ordinary log but ineligible for the
error occurrence projection. No content hash, receipt coordinates, physical
block/row identity, or other synthetic identity substitutes for `scry.event.id`.

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

The original proposal accepted and normalized deprecated trace span events named
`exception`, then used exact or heuristic cross-signal deduplication when the same
exception arrived as both a log and span event. **D-074 supersedes that proposal.**
Span events remain ordinary trace telemetry and are never authoritative occurrence
inputs. Scry performs no cross-signal occurrence deduplication, heuristic or
otherwise. Producers that want an error occurrence emit one eligible exception log
with `scry.event.id`; its top-level trace/span IDs provide best-effort linkage to a
retained trace when one exists.

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

## Uniform pre-WAL sensitive-key scrubbing

D-074 requires one minimal, versioned sensitive-key scrubber on **all** log and
trace intake paths, including ordinary telemetry that is not eligible as an error
occurrence. It runs before any WAL append or other durable write. Metrics and
profiles are outside this policy. The first implementation matches a fixed,
documented case-insensitive set of sensitive attribute/map keys at every supported
nested level and replaces each matched value with the fixed string marker
`[REDACTED]` while preserving the key. Every protocol adapter and native producer
path must reach the
same scrubber; no route may bypass it.

This is deliberately key-based damage reduction, not content inspection. It makes
no promise to detect or scrub secrets embedded in free-text bodies, exception
messages, stack traces, URLs, or values under unrecognized keys. Later configurable
browser/privacy policy may be stricter but cannot weaken this uniform baseline.
Scrubbing is deterministic and precedes WAL durability, canonical content hashing,
and occurrence projection. A scrubber failure rejects the affected record rather
than storing the original value.

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

- The public key resolves server-side to policy and the expected normalized service
  namespace/name. `app_id` is derived from that pair by the same canonical algorithm;
  conflicting client service identity is rejected or retained only as an untrusted
  facet and cannot select another app.
- A stable `deployment_id` is read from a conditionally created bucket manifest and
  scopes IDs, records, quotas, and restore; roles fail startup on bucket/namespace
  mismatch. The first coordinated-version role that needs it creates the manifest
  with `PutMode::Create`; losers read and validate the winner. It is not created for
  buckets that never enable this product slice.
- TLS; explicit exact allowed origins and preflight policy (origin absence remains
  possible for non-browser clients and is separately governed).
- Per-app event/byte/concurrency/cardinality token buckets and global overload
  admission; rate-limit responses include bounded retry guidance.
- Route-specific compressed and expanded limits substantially below the generic
  32 MiB receiver, applied before allocation and after decompression.
- Attribute count, nesting, array, string, stack, breadcrumb, debug-image, and
  batch-record limits; identity-bearing fields fail loudly instead of truncating.
- The uniform all-log/all-trace sensitive-key baseline plus route-specific
  allow/drop/hash/redact rules before durable storage. Any stronger treatment of URL
  queries/fragments, headers, cookies, user fields, messages, stacks, and custom
  attributes is an explicit browser policy, not a claim made by the baseline.
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

Schema fields remain independently versioned: raw typed log encoding, canonical
identity serialization, scrub policy, application identity derivation, extracted
occurrence projection, and physical logs schema. Producers emit canonical raw-record
v1 and may not claim receipt time. On acceptance, the server validates and redacts
the complete batch, samples one trusted receipt timestamp, rewrites every record as
canonical raw-record v2, and only then passes those bytes to decode/live/WAL. Replay
preserves existing v2 receipt stamps and re-applies deterministic redaction.

Operational rollout is not independent: gateway/native producers, ingest, the
deployment-manifest contract, and `scry errors` roll as one coordinated version for
this phase. The implementation does not support stale producers feeding the newer
occurrence system. Readers reject unsupported identity-critical versions rather
than guessing. D-073 reserves capability bit `0x0000_0008` and payload magic
`0x534c3200` (`SL2\0`). Its producer grammar is canonical raw-record v1; D-074 adds
server-stamped canonical raw-record v2 and logs Parquet/query schema v3. This code
has not been deployed.

The stable logs v3 Parquet/query projection has these columns in contractual order:

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
| 13 | `received_ts_unix_nano` | `UInt64` | yes | typed NULL |

The normalized SQL table appends synthesized `labels` at index 14 using the same
Map layout as `attributes`. Logs schema 2 remains the historical producer-v1 shape
through `raw_record` at index 12; schema 3 appends trusted receipt time without
moving those columns. V1/v2 normalization inserts typed NULL for unavailable later
fields and never fabricates fidelity. In schema 3, `raw_record_version = 2`, the
raw record's nonzero `received_time_unix_nano`, and `received_ts_unix_nano` agree.
A root string body remains familiar unquoted text, while non-string bodies and every
identity-bearing Resource/attribute value use deterministic typed canonical text;
exact types remain in `raw_record`. Stream identity hashes length-delimited canonical
label bytes and rejects a repeated hash with different exact labels.

Gateway canonicalization accepts at most the logs-v2 decoder's 16 MiB envelope even
though generic OTLP transport permits 32 MiB requests. Total record-encoding work is
also capped to a small multiple of the decoded OTLP size, preventing shared Resource/
scope metadata from amplifying into unbounded repeated work. It rejects affected records
with deterministic partial-success reasons for malformed IDs, unsupported Resource
entities/profile dictionary references, duplicate or empty keys, and encoding bounds.
Absent and explicitly empty OTLP bodies both normalize to canonical null; `-0.0`
normalizes to `+0.0` and NaN payloads to the canonical NaN by design.

The representation decision specifies the exact binschema/WAL and versioned Parquet
shapes, DataFusion adapters into the stable schema-3 query contract, and compaction
compatibility. Compaction can normalize logs schema 2 into schema 3 with
`received_ts_unix_nano = NULL`; it preserves schema-3 receipt values and rejects
unknown/mislabeled schemas. Historical logs-v2 reader-first rollout
continues to govern those blocks; D-074 supersedes any implication that the new
occurrence producer/extractor slice supports mixed stale and current components.

The original D-073 rollout was reader-first: ship readers/adapters, preserve new
OTLP fields, add typed round trips, then enable writers. The native writer remains
behind mutual negotiation and visible default-off `--enable-logs-v2`; the gateway
requests logs v2 for canonical OTLP batches and refuses to downgrade them when the
upstream session does not negotiate it. Native-v1 and Loki inputs remain v1. V1 and
v2 share the logs WAL, so once v2 has been accepted an operator must not roll that
WAL back to a binary that cannot replay SL2 frames; disabling new v2 acceptance does
not disable recovery in a capable binary.

**D-074 supersedes the planned extraction-shadow/`logs/dup` rollout and the claim
that lower-fidelity legacy records could become occurrences.** Existing string-only
blocks remain queryable only as ordinary logs. The occurrence foundation is
implemented but not deployed. When enabled, its producer/ingest/errors components
must roll as one coordinated version; errorsd accepts only schema-3/raw-record-v2
input and never extracts span events.

## Verification

- Golden protobuf/JSON/gzip/gRPC parity for every AnyValue variant, null/empty,
  int64 extremes, duplicate keys, observed timestamps, event names, scope/schema,
  dropped counts, and binary trace context.
- Exact duplicate/collision fixtures plus proof that span events and logs without a
  valid canonical `scry.event.id` never enter the occurrence projection.
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
- Which SDKs can generate and preserve canonical `scry.event.id` on exception logs
  automatically?
- What are qualified production bounds and per-app default quotas?

## References

- [Error monitoring](error-monitoring.md)
- [Browser SDK](browser-error-sdk.md)
- OpenTelemetry semantic conventions: exception logs, Events, service,
  deployment environment, browser, user, and session.
- OpenTelemetry Logs data model, SDK, AnyValue, and OTLP logs protobuf.
- `crates/gateway/src/{otlp,otlp_logs,otlp_common,otlp_grpc}.rs`
- `docs/design/gateway-ingestion-protocols.md`
