# Current task — query-attempt supersession and zero-grace compaction

## Problem

Production accumulated ~319k block metadata sidecars because coordinated compaction's 600-second grace was implemented as an inline sleep inside every serial merge. The accepted design is in `docs/design/query-attempt-supersession.md`: durable compaction lineage, non-blocking/retryable physical reaping, and a `ResponseSuperseded` query frame that tells clients to discard a provisional attempt while queryd replans.

## Implemented in this run

- Added `BlockMeta.compacted_from`: validated full transitive ancestor closure including intermediate outputs, bounded to 584 UUIDs / 24 KiB; all L0 builders initialize it empty.
- Compaction fetches every input `meta.json` exactly once before merge commit and uses durable ancestry in the output sidecar.
- Catalog schema v3 adds pointer-independent `superseded`, durable `block_lineage`, and persistent pending-reap fields; rebuilds old `blocks` tables to remove the self-FK; lineage replay is order-independent and detects forks.
- `list_blocks`, `block_count`, and `live_row_count` use the canonical logical-live predicate.
- Compaction treats output `meta.json` as logical commit, atomically applies output/supersession, records grace as reap eligibility, never sleeps inline, retries pending object deletion, and deletes `meta.json` last. Retention grace default was split from compaction grace.
- Query schema 0.2 adds request capabilities and `ResponseSuperseded` tag `0x12`; bindings regenerated for Rust and TypeScript.
- Rust CLI/probe and TypeScript client use strict attempt state machines and discard Arrow state/rows on reset.
- Queryd validates capability, performs bounded same-connection mid-scan restart from locally known lineage, fails closed on unresolved still-live blocks/forks, and only caches a final attempt after EOS write+flush.

## Verified so far

- `cargo check --workspace`
- `cargo test -p scry-catalog -p scry-block`
- `cargo test -p scry-compact --lib --tests`
- `cargo test -p scry-cluster`
- `cargo test -p scry-server --lib`
- `cargo test -p scry-proto framing::tests`
- `cd desktop && bun test`
- TypeScript typecheck was run by the delegated protocol implementation.

## Remaining work

1. Add/finish deterministic Rust coverage for targeted-repair single-flight, concurrency/stability bounds, and mid-scan fault injection. TypeScript multi-attempt fixtures now cover successful discard/restart and malformed transitions.
2. Implement conservative lineage pruning only after an authoritative stable partition reconcile can prove no extant or pending descendant needs each claim. Lineage row-count telemetry is now exposed; claims are deliberately retained meanwhile.
3. Add richer repair/reset counters and latency telemetry beyond logs and catalog lineage size.
4. Run deployment smokes and stage the fleet with grace retained before enabling zero grace. D-061 and architecture documentation are now recorded, but rollout/operator evidence remains outstanding.

Do not deploy or mutate production without Bart's explicit confirmation. The design intentionally retains production grace as a non-blocking eligibility timestamp until the fleet rollout gate is complete.
