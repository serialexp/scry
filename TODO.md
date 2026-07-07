
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
