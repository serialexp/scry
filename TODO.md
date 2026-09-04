
## D-070 bounded memory follow-ups (review of ebffc11, blocking release)

### Compaction implementation

Implemented in the post-`ebffc11` hardening pass: main parquet is no longer
charged twice at admission; waiters are cancellation-safe; standalone passes
survive partition failures; the dedicated daemon resolves finite/nested cgroups
and `memory.high`; postings use a sorted k-way stream; sidecar growth is
permit-relative and fails controllably; pre-commit objects have cleanup guards;
DataFusion resource exhaustion is classified; live telemetry is sampled; and
DataFusion/weighted peaks are captured at allocation events. Remaining release
work is tracked below.

- [x] Finish admission policy: charge sidecars and fixed writer state rather
      than compressed main parquet already governed by DataFusion; distinguish
      permanent oversize from queue pressure and cover real default-envelope
      merges plus transient deferral/recovery.
- [x] Make standalone `scry compact --watch` absorb per-partition resource
      failures like the leased path. One rejected partition currently aborts the
      complete pass and exits the daemon instead of leaving inputs live and
      continuing.
- [x] Make weighted-admission waiter accounting cancellation-safe. `admit()`
      increments before awaiting and only decrements on normal completion, so a
      dropped future leaks a waiter and can eventually make every merge report
      `QueueFull`; use an RAII waiter guard and test cancellation.
- [x] Consolidate cgroup detection across compactd, embedded ingest compaction,
      query, and the legacy standalone compact entry point in `scry-resources`.
      Detection maps cgroup namespace mount roots, walks ancestors, handles v1/v2
      and `memory.high`, and supplies the query pressure guard's usage path.
- [x] Never resolve a finite cgroup to an envelope larger than its safe budget.
      Finite small limits and unsafe explicit overrides now fail rather than
      selecting the 512 MiB fallback; embedded maintenance uses the same policy.
- [x] Account sidecar/cardinality peaks within each acquired permit: cap input
      metadata before collection, preflight postings row groups and validate rows
      before cloning, bound bloom finalisation/serialization, and retain the
      existing incremental limits for fingerprints, postings, series types, and
      output metadata. Parquet 58 exposes row-group rather than exact repeated-row
      allocation bounds, so the preflight is deliberately conservative.
- [x] Finish streaming postings output. `encode_postings_to_writer` exists, but
      compaction still materializes the merged postings Arrow arrays and complete
      encoded sidecar in memory. Implement a sorted k-way/external merge with
      bounded batches and spill-backed or multipart output.
- [x] Cover staged cleanup with injected pre-commit upload failure, cancellation
      after completed objects, permit release/recovery, fence abort, and ambiguous
      `meta.json` failure. Bucket guidance requires aborting incomplete multipart
      uploads while avoiding lifecycle expiry of complete block objects.
- [x] Classify DataFusion pool exhaustion and spill-area exhaustion as
      `resource_failed`, not generic `partition_failed`.
- [x] Make resource telemetry live during active passes. It is currently copied
      only immediately before and after awaiting a pass, so reserved/running/spill
      gauges usually appear as zero. Use direct dependency-neutral atomics or a
      periodic sampler, and retain sampled peak counters.
- [x] Expose status/resource telemetry in standalone compact mode. Weighted and
      DataFusion peaks are event-driven; spill peak remains explicitly sampled
      because DataFusion's `DiskManager` has no public lifecycle observer.
- [x] Refuse tmpfs/ramfs spill unless an explicit unsafe override is supplied,
      and reserve a bounded dirty-page/writeback share inside the advertised
      memory envelope.
- [x] Validate that DataFusion + non-DataFusion sub-budgets and writer buffers
      fit the advertised envelope.
- [ ] Complete production resource qualification. Deterministic tests now cover
      metadata/postings/bloom cardinality rejection, admission pressure,
      cancellation, staged cleanup, permit release, and next-pass survival.
      Still run and retain artifacts from concurrent forced sort spill and
      spill-cap exhaustion under a real cgroup using
      `scripts/profile-compact-memory.sh`; unit/E2E fixtures cannot establish RSS.

### Ingest, WAL, and block building
- [ ] Rotate/upload or spill block-sized chunks during WAL recovery. Replay
      currently appends all surviving WAL frames into one builder per shard and
      can materialize an arbitrarily large acknowledged backlog across up to 40
      signal/shard builders before serving.
- [ ] Add one global ingest memory envelope covering active builders and
      encode/upload tasks. Today 5 signals × 8 shards × 128 MiB targets can retain
      ~5 GiB before encode scratch, while upload concurrency scales with physical
      CPU count rather than available memory.
- [ ] Stop idle connections retaining peak scratch-builder capacity for all
      signals. Shrink/replace oversized scratch after merge or use a bounded
      shared pool; enforce an aggregate connection-scratch budget.
- [ ] Reject corrupt/implausible WAL frame lengths before allocation. Replay
      trusts a `u32` header and can attempt a nearly 4 GiB `Vec` before checking
      truncation or CRC.

### Query and queryd
- [ ] Budget per-query fingerprint→label materialization outside DataFusion.
      Intern while constructing, build only when projected, and cap/reserve
      fingerprints, label pairs, candidates, and bytes.
- [ ] Stream queryd metadata pair discovery into bounded sets. Projected postings
      reads currently materialize a complete sidecar before applying the
      1,000/10,000 suggestion limits, multiplied by fill concurrency.
- [ ] Add in-flight byte admission to object-store range reads. The buffer pool
      caps only idle retained buffers; checked-out concurrent Parquet ranges are
      unbounded outside the DataFusion pool.
- [ ] Treat postings/bloom cache budgets as peak-fill budgets as well as retained
      budgets: HEAD/reserve before download, cap decoded structures, and prevent
      concurrent oversized fills from exceeding the process envelope.
- [ ] Do not spawn one rejection task per excess query socket. Reject/drop inline
      under a short timeout or use a separately bounded rejection pool.
- [ ] Tighten live-query memory: enforce a smaller frame bound before allocation,
      reserve before decode/retention, avoid cloning the retained live rows, and
      account capacities/container overhead.
- [ ] Bound Fleet and live-registry discovery by instance count and total bytes;
      avoid Lua/full-response accumulation and parse→reserialize duplication.
- [ ] Put global byte/name/block/TTL limits on the process metadata suggestion
      view, and remove stale UUID single-flight `Weak` entries so long-lived
      queryd memory does not grow monotonically.
- [ ] Reduce query response copies (Arrow payload, framed payload, wire buffer,
      result-cache tee) and budget serialization across active queries.
- [ ] Reconcile queryd's independent DataFusion/cache/live-fetch budgets against
      the cgroup limit at startup, and configure/measure a bounded disk-backed
      query spill area. One-shot/public query helpers must stop constructing
      unbounded default DataFusion runtimes.

### Gateway, replay, and tail
- [x] Represent OTLP Histogram, ExponentialHistogram, and Summary data without
      loss. Ingest protocol and metrics-block schema v2 preserve exact numbers,
      temporality/start time, buckets, quantiles, descriptors, flags, reset hints,
      and exemplars; Prometheus remote-write v1/v2 native histograms map to the
      same canonical representation.
- [ ] Add reset-aware structured metric operators after the raw SQL/UI boundary:
      ordered cumulative-to-delta/rate with predecessor lookback, strict explicit
      histogram merge, exponential scale alignment, histogram quantile/fraction,
      and PromQL range-vector/staleness semantics. Summary quantiles must remain
      non-mergeable and exact-match only.
- [ ] Add the bundled Linux process-sampling backend for `scry agent`: resolve the
      opted-in pod's current container ID to stable cgroup/process handles, ship an
      isolated opt-in `hostPID` deployment overlay with minimal perf/eBPF capabilities,
      generate and symbolize pprof, probe degraded capabilities, and qualify amd64 +
      arm64 across supported kernels. The bounded pod-selected pprof puller is the
      safe first backend; do not silently fall back to privileged sampling.
- [ ] Add alpha OTLP Profiles (`v1development`) only with a complete structured
      OTLP-to-pprof conversion; Pyroscope Push v1 is the stable profile ingress.
- [ ] Add global/per-route concurrency admission to gateway HTTP/gRPC. Request
      bodies and gzip/Snappy expansion now have a 32 MiB ceiling; concurrency
      still needs an aggregate retained-byte/admission budget.
- [ ] Make gateway sink queues byte-weighted, not only item-count bounded, and
      stream/chunk Loki/OpenSearch/native destination encoders instead of building
      complete payloads per slow sink worker.
- [ ] Bound OpenSearch replay by bytes as well as page/record count: cap CLI
      sizing knobs, response/document sizes, and flush mapped batches before the
      negotiated wire ceiling.
- [ ] Add dedicated tail subscriber/endpoint limits and global/per-subscriber
      byte budgets; current bounded record-count queues still multiply retained
      payloads by subscriber count.

## D-056 replay-opensearch (follow-ups, non-blocking)
- [ ] `--follow`: after draining to PIT open-time, re-open the PIT and continue
      past the last-seen timestamp for a live tail (core is drain-once-to-now).
- [ ] PIT `slice` parallelism / multi-connection fan-out — split the corpus into
      N `slice` shards each on its own reader+connection, for when a single
      reader caps below scry's ingest ceiling (would surface `--slices`).
- [ ] Legacy `scroll` API fallback for pre-2.4 OpenSearch/Elasticsearch clusters
      (PIT-only today).
- [ ] Replay metrics/traces/profiles, not just logs (logs-only in v1).
- [ ] Resumable checkpointing of the `search_after` cursor so an interrupted
      multi-TB run can resume instead of restarting from the oldest doc.

## D-055 catalog snapshot bootstrap (follow-ups, non-blocking)
- [ ] Real `ALTER TABLE`-based catalog migration framework. Today
      `Catalog::init_schema` is `CREATE TABLE IF NOT EXISTS` only and cross-version
      persistence is guarded by `PRAGMA user_version` — a `CATALOG_SCHEMA_VERSION`
      bump forces one cold reconcile (the snapshot is refused) until the next
      snapshot is written at the new version. Additive migrations would let a newer
      binary accept an older snapshot instead of rebuilding.
- [ ] Lease-gate snapshot production so only the maintenance-lease holder uploads
      under multi-instance (today every `--catalog-snapshot-interval` instance
      uploads — correct but redundant bandwidth). Also: snapshot history/GC +
      compression.
- [ ] Fold snapshot restore into `scry get` / one-shot query paths (daemon-only
      today).

## D-054 merged history+live query (follow-ups, non-blocking)
- [ ] Unit test for schema parity between the live `RecordBatch`
      (`build_live_logs_batch`) and the block-backed `LogsTable` schema. Currently
      proven end-to-end (the `UNION ALL` in `smoke-live.sh` fails loudly on a
      mismatch), but a direct `assert_eq!(schema, logs_table_schema())` would catch
      a drift without needing Garage+Valkey. The dedup *selector* is already
      unit-tested (`live_record_is_durable`).
- [ ] Extend the merged view to metrics/traces/profiles (logs-only in v1).
      **Still open after D-065** — that widened the best-effort *tail* to
      metrics, not this exact, watermark-deduplicated `--live` query. The
      metrics chart's live half is a client-side approximation of `date_bin`
      with a strictly-newer-bucket seam; an exact merged metrics query would
      replace it.
- [ ] `scry get` one-shot live-merge (daemon-only today, mirroring D-053's tail
      front-door).

## D-065 metrics live tail (follow-ups, non-blocking)
- [ ] Labels repeat on every `TailSample`, as they do on every `TailRecord`. For
      a wide metrics tail that is real bandwidth; a per-connection series
      dictionary (send labels once per fingerprint, then samples reference it)
      would fix it for both frames. Deferred: the drop-on-full channel already
      bounds the server, and consistency with `TailRecord` won for now.
- [ ] Surface the server-side drop count to the client. A wide-open metrics tail
      with no matchers drops heavily and currently just looks sparse; the count
      exists on the server (logged) but never reaches the CLI or the UI, so
      backpressure reads as data loss.
- [ ] Traces/profiles stay untailable (both the server and the relay refuse
      them). A span stream is plausible; a pprof blob stream is not.

## scry tail: SIGPIPE (pre-existing, found 2026-08-29)
- [ ] `scry tail ... | head -5` panics instead of exiting quietly:
      `failed printing to stdout: Broken pipe (os error 32)`. Rust ignores
      SIGPIPE and `println!` panics once the reader closes, so piping a tail
      into `head`/`grep -m`/`less` and quitting always ends in a panic message.
      Affects the logs tail identically — not introduced by D-065, just noticed
      while exercising it. Fix: treat a `BrokenPipe` write error on stdout as a
      clean exit in the tail print loop (write via `Stdout::write_all` and match
      the error, rather than `println!`).

## Web UI / desktop (mobile)
- [ ] Collapsible sidebar: let the query form collapse entirely (toggle) so the
      results pane can use the full width on mobile. Current `@media (max-width:720px)`
      only stacks form-above-results; a manual collapse would be better.

## Query result cache (pre-existing, found 2026-08-28)
- [x] The D-059 default-query-window clamp defeated the result cache for queries
      without explicit bounds by putting the exact current nanosecond into the
      key. The effective lower bound is now snapped to a 30-second bucket
      (capped by unusually short configured windows) before both candidate
      selection and cache-key construction, so execution and identity remain
      identical while repeated dashboard requests can hit.
