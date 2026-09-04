# Gateway ingestion protocols — Design

Status: complete
Owner: Bart
Last updated: 2026-09-03

## Implementation status

### Done

- [x] **Shared OTLP HTTP transport.** Traces, logs, and metrics accept protobuf or JSON with optional bounded gzip decompression.
- [x] **Loki receiver.** JSON and raw-Snappy protobuf pushes map into native log streams.
- [x] **OTLP logs.** HTTP and gRPC preserve resource/scope identity, entry metadata, severity, and trace correlation.
- [x] **OTLP structured metrics.** Gauge, delta/cumulative Sum, Histogram, ExponentialHistogram, and Summary points map into the native v2 model with explicit partial-success rejection accounting.
- [x] **OTLP trace parity.** Existing trace mapping now accepts JSON and gzip as well as protobuf.
- [x] **Modern Pyroscope receiver.** Push v1 Connect JSON/protobuf/gzip validates pprof metadata and normalizes storage to gzipped pprof.
- [x] **Observability.** Loki and modern Pyroscope requests have distinct bounded status counters.
- [x] **End-to-end proof.** The gateway smoke exercises every receiver/transport and verifies exact storage and query row counts for all four signals.

### Outstanding

_(nothing in this design; aggregate OTLP metric conversion and alpha OTLP Profiles are tracked separately)_

## Why this exists

The gateway originally accepted OTLP traces, Prometheus remote-write, and the legacy Pyroscope multipart endpoint. Loki pushes, OTLP logs and metrics, and the current Pyroscope Push API could not be pointed at scry directly. This left the nominal Grafana-stack replacement dependent on a collector for common ingress protocols.

This design completes the practical push surface without changing scry's native wire or storage schemas. Every receiver projects into an existing typed batch and therefore retains D-041's all-compatible-sinks fan-out and ACK-on-enqueue semantics.

## Receiver matrix

| Receiver | Path/listener | Encodings | Native signal |
|---|---|---|---|
| OTLP traces | `POST /v1/traces` | protobuf/JSON, identity/gzip | traces |
| OTLP logs | `POST /v1/logs` | protobuf/JSON, identity/gzip | logs |
| OTLP structured metrics | `POST /v1/metrics` | protobuf/JSON, identity/gzip | metrics |
| OTLP gRPC | `--listen-otlp-grpc` | traces/logs/metrics, gRPC gzip accepted | corresponding signal |
| Loki | `POST /loki/api/v1/push` | JSON or raw-Snappy protobuf | logs |
| Prometheus | `POST /api/v1/write`, `/api/v1/push` | remote-write v1/v2 raw Snappy | metrics |
| Pyroscope legacy | `POST /ingest` | multipart gzipped pprof | profiles |
| Pyroscope Push v1 | `POST /push.v1.PusherService/Push` | Connect unary protobuf/JSON, identity/gzip | profiles |
| Scry native | `--listen-wire` | binschema | all four |

The HTTP listener is protocol-compatible, not an authentication layer: as before,
operators put TLS and tenant/auth enforcement in their ingress proxy. Scry remains
single-tenant and ignores `X-Scope-OrgID`.

All HTTP request and expanded gzip/Snappy bodies are bounded to 32 MiB. Unsupported media/content encodings are refused rather than guessed.

## Mapping semantics

### Logs

Loki stream labels become scry stream labels and structured metadata becomes entry attributes. Protobuf label sets are parsed with quoted escape handling, and scry computes its own canonical fingerprint rather than trusting Loki's hash.

OTLP resource attributes form stream identity, augmented by non-colliding `otel.scope.name` and `otel.scope.version`. Event time wins, with observed time as fallback. Severity maps numerically; severity text, valid trace/span IDs, and flags remain namespaced entry attributes. AnyValue data is stringified because the native log metadata model is string-to-string.

### Metrics

Resource attributes, scope identity, and point attributes form the series labels; point attributes override equal resource keys and the metric name authoritatively supplies `__name__`. Gauge maps to gauge, cumulative monotonic Sum to counter, and cumulative non-monotonic Sum to unknown.

Gauge, delta/cumulative Sum, Histogram, ExponentialHistogram, and Summary points retain exact integer/double values, temporality and start time, optional statistics, sparse or explicit buckets, quantiles, flags, descriptors, and exemplars. Structurally invalid points and zero timestamps are not silently dropped: OTLP reports their rejected count through `partial_success`.

### Profiles

Push v1 contains raw pprof bytes and labels but no envelope timestamp. The receiver parses pprof fields 9/10 (`time_nanos`/`duration_nanos`) for correct block indexing, accepts raw or gzipped pprof, and stores normalized gzip under native profile format 1. Malformed profiles and negative metadata are rejected.

## Non-goals

- Alpha `v1development` OTLP Profiles ingestion and structured-OTLP-to-pprof conversion.
- Native or exploded representation of OTLP aggregate metrics in this round.
- OTLP exemplars, start timestamps, metric descriptions/units, or exact integer storage.
- Changing best-effort fan-out acknowledgements, routing, or sink behavior.

## Verification

Focused tests cover format dispatch, gzip bounds, mappings, partial success, Loki label parsing and wire tags, pprof validation, and counters. `scripts/smoke-gateway.sh` sends all receiver forms through gateway → native ingest → Garage, reconciles a fresh catalog, and requires exact catalog and `scry get` row counts for metrics, logs, traces, and profiles, including postings invariants.
