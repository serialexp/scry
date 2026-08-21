
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
- [ ] `scry get` one-shot live-merge (daemon-only today, mirroring D-053's tail
      front-door).

## Web UI / desktop (mobile)
- [ ] Collapsible sidebar: let the query form collapse entirely (toggle) so the
      results pane can use the full width on mobile. Current `@media (max-width:720px)`
      only stacks form-above-results; a manual collapse would be better.

## Test flakes
- [ ] `crates/compact/tests/compaction_e2e.rs:285` `logs_compaction_is_lossless_and_reaps_inputs`
      intermittently fails the "merged rows must be sorted by (fp, ts)" assertion under full
      `cargo test --workspace` parallel load; passes reliably in isolation. The assertion
      assumes the merged block scans as a single ordered partition, but DataFusion may split
      the scan across partitions under load, interleaving batch order in the collected result.
      Pre-existing (not from the metrics with_labels work). Fix: either sort the collected rows
      before the monotonicity check, or force target_partitions=1 for that query's SessionContext.
