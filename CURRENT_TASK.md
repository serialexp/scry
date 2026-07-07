# CURRENT TASK: D-054 — merged history + live query with per-writer WAL-segment dedup

## Status: COMPLETE (pending Bart's review / regression sign-off)
All plan tasks #39–#48 done. Full workspace builds (debug + release), `cargo test
--workspace` green (65 suites), and `scripts/smoke-live.sh` PASSES all four assertions.

## The milestone
"Request the last minute": one query that unions stored parquet blocks (history) with the
still-in-flight records at the ingesters (live), deduplicated across the block-commit seam.
Plan: `~/.claude/plans/snazzy-zooming-riddle.md`. Logs-only first cut, Valkey-required.

## Locked decisions (Bart)
1. Live source = dedicated retained recent-window **ring** at each ingester.
2. Dedup grain = per-record **WAL-segment tag** `(writer, shard, seg)`.
3. Discovery = **Valkey-required, refuse otherwise** (mirror tail).
4. Scope = **logs only**.
5. Watermark = **persistent monotonic high-water table in the catalog** (advanced atomically
   with insert_block; never recomputed from live blocks — sharding + compaction make that wrong).

## The dedup invariant + the segment-0 fix
Unit of dedup = WAL instance `(writer_id, signal, shard)`. `H` = durable segment high-water
(catalog `wal_watermarks`) = highest segment fully committed to a block. A live record tagged
`(writer, shard, seg)` is durable (drop it) iff a watermark exists AND `seg ≤ H`.
**Absent watermark = `None` (nothing durable) ⇒ keep ALL live records** — NOT `unwrap_or(0)`.
WAL segments are 0-based, so `H` absent ≠ `Some(0)`: collapsing them dropped a fresh ingester's
first-segment records before its first flush (a gap). Selector is the pure, unit-tested
`live_record_is_durable(seg, Option<H>)` in `query_service.rs`. **This was a real bug in the
task-#46 dedup code — fixed this session (Rule #0.5); the smoke's assertion (a) proves it.**

## DONE — full implementation
- **Watermark plumbing** (#39–#41): `BlockMeta.{wal_seg_max,wal_shard}` (serde default);
  builder setters in all 5 builders; `Pipeline` stamps them in `spawn_upload` (sealed seg +
  `shard_index`); `ingest_decoded` returns the appended `SegmentId`. Catalog columns +
  `wal_watermarks` table + `advance_watermark`/`get_watermark`, advanced atomically with
  `insert_block` (+ reconcile/apply_event/poll piggyback). Unit-tested.
- **LiveRing** (#42): `crates/server/src/live_ring.rs` — logs-only bounded ring (age + byte
  eviction), `RetainingLogsAppender` decorator. Fed off the logs phase-2 seam in `server.rs`;
  phase-2 stamps shard+seg. 5 unit tests.
- **Ingest wire** (#43): `LiveQuery`=0x52 / `LiveBatch`=0x53 in `proto/ingest.schema.json`,
  regenerated bindings, `build::{live_query,live_batch}` constructors.
- **Serve LiveQuery** (#44): `server.rs` dispatch arm snapshots the ring under the predicate
  (scry_match filter + body_contains + ts range), replies one `LiveBatch`.
- **Query wire** (#45): `live: bool` on `QueryRequest` (`proto/query.schema.json` + regen +
  `crates/query/src/wire.rs`); `QUERY_ERR_LIVE_UNAVAILABLE = 0x0005`.
- **Merge + dedup** (#46): `QueryService.fetch_live_logs` fans `LiveQuery` out to ingesters
  discovered via an injected `&dyn LiveDiscovery` (`live_merge.rs` — Valkey-agnostic), dedups
  with `live_record_is_durable`, registers a `logs_live` MemTable behind `CREATE VIEW logs …
  UNION ALL …`. No Valkey ⇒ refuse with `QUERY_ERR_LIVE_UNAVAILABLE`. Live queries bypass the
  result cache. queryd injects `ValkeyLiveDiscovery` (over the D-053 tail registry).
- **Probe** (#47): `--live` on `scry-query-probe`.
- **Ingestd** (#48): `--live-window-secs` (90) / `--live-window-max-bytes` (128 MiB) flags →
  `LiveRing::new` → `Server::with_live_ring`.
- **Docs/smoke** (#48): `scripts/smoke-live.sh` (Garage + dev Valkey), D-054 in
  `docs/decisions.md`, CLAUDE.md updated.

## Verify (run for regression)
`cargo build --release --workspace` ✅ + `cargo test --workspace` ✅ (65 ok).
`scripts/smoke-live.sh` ✅ (a live-half / b history / c dedup-exact / d refuse).
Still worth Bart running: `SIGNAL=logs scripts/smoke.sh`, `MULTI=1 scripts/smoke.sh`,
`scripts/smoke-tail.sh`, `scripts/smoke-tail-queryd.sh` (tail untouched, but confirm).
Dev Valkey: container `scry-valkey-smoke` on `127.0.0.1:6380`.
