# Gateway fleet status and forwarding telemetry — Design

Status: complete
Owner: Bart
Last updated: 2026-09-03

## Implementation status

Tracking the gap between this design and the implementation on `main`.

### Done

- [x] **Telemetry semantics selected.** Fleet and local status show protocol+transport inbound activity and the complete enqueue/delivery loss path.
- [x] **Status topology selected.** Gateway supports both Valkey fleet publication and an opt-in local status page.
- [x] **Phase 1 — shared status surface.** Generic status envelope and HTTP fleet page live in the reusable `scry-status` leaf crate.
- [x] **Phase 2 — gateway metrics core.** Fixed-index relaxed atomics cover inbound, queue, and sink outcomes without allocating in fan-out hot loops.
- [x] **Phase 3 — inbound instrumentation.** HTTP, OTLP/gRPC, and native-wire accepted/rejected work and mapped records are counted at protocol seams.
- [x] **Phase 4 — downstream instrumentation.** Per-sink enqueue, delivery, retry, skip, failure, and OpenSearch partial-bulk outcomes are distinct.
- [x] **Phase 5 — publication and local page.** Optional Valkey registration, namespace selection, local status endpoint, and bounded graceful cleanup are wired.
- [x] **Phase 6 — Fleet UI.** A dedicated gateway card renders protocol and per-sink forwarding counters.
- [x] **Phase 7 — documentation.** D-067, architecture, Fleet design, README, and project guidance describe the shipped contract.
- [x] **Verification closeout.** Workspace/frontend tests and the gateway-aware status smoke pass.

### Outstanding

_(nothing)_

## Why this exists

The gateway acknowledges inbound pushes after decoding and best-effort fan-out offer. It does not currently expose how many HTTP, gRPC, or native-wire requests it accepted or rejected, nor whether each configured sink queued, dropped, retried, delivered, or finally lost those batches. Access logs cannot reconstruct those stages after the fact.

This document defines gateway status as a first-class Fleet role and pins counter meanings so operators do not mistake inbound acceptance for downstream delivery.

## Goals

- Show gateway instances beside agents, ingesters, and queriers in Fleet.
- Provide the same status locally when Valkey is absent or Fleet is unavailable.
- Separate inbound acceptance, per-sink queue admission, and final downstream disposition.
- Break inbound work down by protocol and transport: OTLP/HTTP, OTLP/gRPC, Loki/HTTP, Prometheus remote-write/HTTP, legacy Pyroscope/HTTP, Pyroscope Push/HTTP, and native wire.
- Keep all labels bounded and all common-path increments allocation-free.
- Preserve D-041's independent, best-effort, ACK-on-enqueue fan-out semantics.

## Non-goals

- Durable metric history, Prometheus exposition, or self-ingestion of gateway telemetry.
- Routing, synchronous downstream acknowledgements, an on-disk spool, or changed retry policy.
- Making Valkey mandatory to run a gateway.
- Inferring delivery by subtracting unrelated counters.

## Status topology

`scry gateway` gains the same optional `--valkey-url` and `--valkey-namespace` conventions as ingest/query. With Valkey configured it mints one process-lifetime UUID, publishes a canonical `StatusSnapshot` under `<namespace>/status/<uuid>`, and deregisters on graceful shutdown. Without a URL, gateway forwarding is unchanged.

An independent opt-in `--stats-listen [addr]` serves `/stats.json` and the existing self-contained fleet dashboard. With Valkey it renders the complete fleet; without Valkey it renders the local gateway snapshot. A configured but unreachable Valkey is a startup error, matching the daemon roles.

The generic envelope/server moves from the ingest-oriented `scry-server` dependency tree into a small `scry-status` leaf crate. `scry-server` re-exports it for source compatibility; role-specific ingest/query metrics remain where they are.

## Counter contract

All counters are monotonic for one gateway process lifetime. The implementation uses fixed enums and arrays of relaxed atomics; status serialization turns those arrays into named JSON only on a heartbeat or local status request.

### Inbound

For each protocol+transport, expose accepted and rejected requests (native wire uses batches), fixed rejection-reason counters, and mapped records by signal. A record means a log entry, metric sample, span, or profile sample. Native wire additionally exposes accepted/rejected connections and an active-connection gauge.

HTTP route middleware observes requests that fail before handler entry; protocol handlers own detailed decode/mapping reasons. OTLP/gRPC counts decoded Export calls and service failures visible at the Tonic service seam. Native wire counts centralized handshake, rejection, and ACK outcomes.

### Fan-out and delivery

For every configured sink and accepted signal:

1. `enqueued` means `try_send` accepted the batch.
2. `dropped_full` and `dropped_closed` are queue-admission losses.
3. `attempts` counts actual downstream sends; Scry's reconnect/resend increments `retries`.
4. Final disposition is exactly one of `delivered`, `failed`, `partial_failure`, or `skipped_empty` after a worker consumes an enqueued batch.
5. Queue depth/capacity is sampled from the bounded channel, not maintained with another hot-path gauge.

An inbound 2xx/accepted ACK still means decode and non-blocking fan-out offer completed. It does not assert that every compatible sink enqueued or delivered the batch.

OpenSearch `_bulk` responses with `errors:true` are parsed and classified as partial/complete item failure; they are never reported as full delivery success. Management reconciliation is control-plane work, not a delivery retry.

## Fleet presentation

The SolidJS Fleet view adds a gateway role badge and dedicated fields. It displays each inbound protocol's accepted/rejected totals, mapped records per signal, and each sink's enqueued, queue-dropped, delivered, failed/partial, retry, and queue-depth values. Missing fields from rolling upgrades render as unavailable rather than fabricated zeroes.

## Verification

Focused tests cover status compatibility, allocation-safe counter indexing, queue full/closed behavior, HTTP/gRPC/wire acceptance and representative rejects, downstream 2xx/non-2xx/retry/empty behavior, and OpenSearch partial bulk responses. `scripts/smoke-status.sh` adds a gateway to the Valkey fleet, drives HTTP and gRPC requests, verifies local and cross-fleet visibility/counters, and checks prompt deregistration. Workspace Rust tests, frontend Vitest/typecheck/build, and gateway/status smoke scripts seal the change.
