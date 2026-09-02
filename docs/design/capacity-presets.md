# Capacity presets and synthetic qualification

**Status:** deferred design; the values below are hypotheses, not capacity guarantees.

## Goal

Provide low-, medium-, and high-volume deployment presets for each Scry role, with explicit hardware guidance and a repeatable synthetic qualification suite. Presets should turn the existing memory, concurrency, cache, and batching controls into coherent starting configurations without hiding their resolved values.

A preset becomes a supported capacity envelope only after it passes the synthetic workload on documented hardware. Until then it is a recommendation.

## Workload dimensions

Do not define capacity by records/second alone. A profile must state:

- uncompressed ingest MiB/s;
- records/s and average record size for metrics, logs, traces, and profiles;
- agent and ingest-connection count;
- concurrent and queued query count;
- query ranges and query corpus;
- live-query ingester/fleet size;
- retention and expected object-storage volume;
- compaction throughput/backlog;
- object-store and Valkey latency assumptions.

## Initial profile targets

| Profile | Intended workload | Ingest node | Query node | Agent per node |
|---|---|---:|---:|---:|
| Low | Homelab/small cluster; initially target up to ~10 MiB/s, 4 concurrent queries, 10–25 agents | 4 vCPU, 8 GiB | 4 vCPU, 4 GiB | 100–500m CPU, 256 MiB |
| Medium | Production cluster; initially target ~25–75 MiB/s, 16 concurrent queries, 25–150 agents | 8–16 vCPU, 16–32 GiB | 8–16 vCPU, 16 GiB | 250m–1 CPU, 512 MiB |
| High | Large installation; initially target 100+ MiB/s per ingest replica, 32 concurrent queries per queryd, hundreds of agents | 16–32 vCPU, 32–64 GiB | 16–32 vCPU, 32–64 GiB | 500m–2 CPU, 512 MiB–1 GiB |

These ranges must be recalibrated from measurements. Signal mixture, query shape, compression, object storage, and Valkey can change them substantially.

## Configuration form

Prefer inspectable deployment presets:

```text
deploy/presets/
  low/
    ingest.args
    query.args
    gateway.args
    resources.yaml
  medium/
  high/
```

A future `--profile low|medium|high` shortcut may load equivalent defaults. Explicit CLI flags always override preset values. Every daemon logs the fully resolved configuration and estimated memory envelope at startup.

### Query controls

Presets should set:

- DataFusion memory budget and cgroup reserve;
- postings, bloom, result-cache, and object-buffer retained-byte budgets;
- sidecar fill concurrency;
- active/waiting query admission and queue timeout;
- live-fetch concurrency, per-peer bytes, aggregate bytes, and rows.

### Ingest controls

Do not publish an ingest capacity promise until the ingest memory-envelope work exposes and validates:

- ingest shard count;
- block target size;
- encode/upload concurrency with memory-weighted admission;
- live-ring bytes;
- connection admission;
- compaction budget;
- WAL/PVC sizing.

### Compaction controls

Compaction is qualified against a **shared, process-wide bounded DataFusion pool** and a bounded spill path, not against an independent budget per partition. The pool is reserved separately from query and ingest headroom. Each candidate merge makes a weighted reservation based on its input/partition shape; the scheduler waits or rejects according to queue policy when the reservation is unavailable. A partition-count concurrency limit is only a secondary guard and must not replace byte-weighted admission.

The merge is staged and streamed: bounded RecordBatches are written to a temporary/staged output as they arrive, while postings, bloom, and other sidecars are built incrementally. The complete output is published at the commit point (metadata last); a failed stage is cleaned up and its inputs remain live. Thus a full DataFusion pool or spill area is an ordinary per-partition resource failure, not permission to exceed the envelope and not data loss. The maintenance loop records the failure, releases the reservation, and retries with bounded backoff or defers that partition without crashing or declaring success.

Profiles should set and display:

- shared compaction DataFusion pool limit and reserved safety headroom;
- spill directory/filesystem and byte capacity;
- weighted admission limit, queue policy/timeout, fanout, and secondary concurrency;
- pressure-backoff settings and staged-output cleanup policy.

The conservative default derives from the process cgroup memory limit (`memory.max` on cgroup v2, cgroup v1 fallback), minus explicit query/ingest reservations and safety headroom. Missing, unlimited, or invalid cgroup limits use a conservative fixed default rather than host RAM. Explicit overrides are allowed, and startup telemetry must identify the source and resolved envelope. Runtime telemetry should include pool limit/reserved/in-use bytes, weighted queued/running work, admission wait time, spill usage/capacity and cleanup failures, and per-partition duration, bytes/rows, and outcome.

## Synthetic qualification

Provide a repeatable command such as:

```bash
scripts/bench-profile.sh low
scripts/bench-profile.sh medium
scripts/bench-profile.sh high
```

Each run should:

1. launch daemons under the profile's CPU/memory limits;
2. generate a documented signal mixture and record-size distribution;
3. ramp to the target steady ingest rate;
4. run a fixed concurrent query corpus;
5. exercise live queries across multiple fake ingesters;
6. trigger block rotation and compaction;
7. inject slow object storage, unavailable sinks, and reconnects;
8. record steady/peak RSS, CPU, ingest/rejections, WAL growth, upload latency, compaction backlog, query p50/p95/p99, overloads/timeouts, and data/protocol errors.

A profile passes only if it sustains its target for a fixed duration, remains below its cgroup limit with explicit safety headroom, meets query-latency objectives, and loses no accepted data.

## Delivery sequence

1. Keep this schema and target document current.
2. Finish ingest memory-envelope controls.
3. Add compaction budgeting/spill.
4. Build and version the synthetic workload/query corpus.
5. Qualify low, then medium, then high.
6. Publish measured results with hardware, storage, signal mix, query corpus, duration, and software version.
