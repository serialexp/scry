# CURRENT_TASK — handoff

## v0.9 — scry is now multi-instance (Valkey lease + pub/sub convergence)

All 7 phases of the v0.9 plan are implemented and green. Phases 1–6 are
committed (Phase 6 = `32aaece`); **Phase 7 (this session) is in the working
tree, not yet committed**. Rationale: `docs/decisions.md § D-038` (Valkey lease,
supersedes D-013) + `§ D-039` (three-tier convergence).

The single-instance path is preserved byte-for-byte: with no `SCRY_VALKEY_URL`,
the convergence loops spawn but idle, maintenance pauses (no lease ⇒ no
destructive work), and the standalone `scry-compact`/`scry-retention` CLIs run
unfenced exactly as in v0.8. `SIGNAL=both scripts/smoke.sh` + `cargo test
--workspace` stay green.

---

## What shipped (the shape of it)

**Two new crates:**
- **`scry-valkey`** — the only crate that talks to Valkey (`fred` 10.1). The
  `ValkeyClient` handle, the `SET NX PX` + Lua compare-and-set lease
  (`LeaseProvider`/`ValkeyLeaseProvider`, auto-renew every `ttl/3`, latches
  invalid before server-side expiry), pub/sub (`subscribe_blocks`,
  `parse_envelope`), and `ValkeySink: BlockEventSink`.
- **`scry-cluster`** — Valkey-agnostic orchestration: `apply_event` (idempotent
  catalog mutations), `poll_once` + `full_walk` (cursor-poll + full-reconcile
  tiers), `run_maintenance_loop<L: LeaseProvider>` (drives
  `run_compaction_pass`/`run_retention_pass`), `LocalLeaseProvider`.

**Edited crates:** `scry-block` seams (`Fence`/`AlwaysValid`, `BlockEvent`/
`Envelope`, `BlockEventSink`/`NoopSink`); `scry-catalog` `poll_cursors` table;
`scry-compact` `compact_partition` + commit-point fence in `merge_blocks`;
`scry-retention` `retain_planned`; `scry-query` `EvictOnNotFound` + one-shot
re-plan; `scry-server::pipeline` catalog → `std::sync::Mutex<Catalog>` + an
`event_sink` emitted in `run_upload`; `scry-ingestd`/`scry-queryd` wiring.

**Correctness invariants (load-bearing):**
- Blocks are addressed by random **UUID v7, not content hash** → single-winner
  compaction is a *correctness* requirement, not just efficiency.
- **Commit-point fence:** `merge_blocks` uploads `main → [postings] → [bloom]`
  then `meta.json` LAST; reconcile keys on `meta.json`. The fence is checked
  immediately before the `meta.json` PUT, so a lost lease leaves only harmless
  uncommitted bytes (no row, no events, inputs untouched).
- **grace=0 closes the sequential re-merge window:** the winner deletes inputs
  immediately, so a stale peer's re-merge 404s at the input GET and aborts
  before its own `meta.json` commit. This is what makes the multi smoke
  deterministic.

---

## Phase 7 (this session) — what's in the tree

- **`scripts/smoke-multi.sh`** (NEW) — two `scry-ingestd --mode full` instances
  sharing one bucket + Valkey. Phase 1: spew logs to both, wait for both
  catalogs to converge to the same total live row count (pub/sub + cursor
  poll, no double-count), assert `level ≥ 1` after a compaction round with rows
  unchanged. Phase 2: restart with `--ttl-logs 1s --retention-apply
  --retention-grace 0`, assert both catalogs reap to zero, bucket reconcile = 0,
  no panic / no "pass failed" in either daemon log.
- **`scripts/smoke.sh`** — `if [[ "${MULTI:-0}" == "1" ]]; then exec
  scripts/smoke-multi.sh "$@"; fi` after `cd "$ROOT"`.
- **Docs:** `docs/decisions.md` (D-013 → SUPERSEDED, full D-038 + D-039, three
  deferred entries flipped to ✅, orphan-GC deferred entry added); `README.md`
  (v0.9 row → ✅); `CLAUDE.md` (status paragraph + Workspace layout + Commands +
  decisions range); this file.

### Smoke gotchas (don't re-trip)

- The pass/fail grep must be `grep -iEq "panicked|pass failed"`. A looser
  `ERROR`/`error` grep false-positives on the benign lowercase `error=io:
  unexpected end of file` field inside a WARN — that WARN is caused by the
  smoke's own `wait_bind` TCP probe disconnecting without a handshake.
- Assert via the daemons' **own** catalogs (which track `superseded_by`
  correctly), not a fresh bucket reconcile mid-merge. Total live rows is the
  stable convergence / no-duplication signal.
- A native host redis on `127.0.0.1:6379` collides with the dev
  `scry-valkey` container (host networking). Run a throwaway
  `docker run -d -p 6380:6379 valkey/valkey:8.0-alpine` and pass
  `SCRY_VALKEY_URL=redis://127.0.0.1:6380`. The committed script defaults to
  6379 but honors `SCRY_VALKEY_URL`.

The multi smoke passed deterministically 3× against Valkey on 6380.

---

## Remaining

- **Commit Phase 7** as a single whole-file feature commit — ONLY my Phase 7
  files (`scripts/smoke-multi.sh`, `scripts/smoke.sh`, `docs/decisions.md`,
  `README.md`, `CLAUDE.md`, `CURRENT_TASK.md`). **Exclude** parallel/unrelated
  working-tree changes: `Dockerfile`, `crates/scry-list/src/main.rs`,
  `deploy/k8s/queryd-*.yaml`. (Per Rule #13: whole files only — verify the
  split is clean before staging.)
- **Cleanup:** remove the throwaway `scry-valkey-smoke` container (port 6380)
  once Bart's clean dev Valkey is the target.

## Deferred to future rounds (out of v0.9 scope)
Catalog snapshot bootstrap (the O(GB) cold-start optimisation); multi-bucket
pool + sealing/auto-provisioning; orphan-object GC for uncommitted merge
leftovers; partial-block rewriting at the TTL boundary; agent↔server discovery
via a Valkey service registry. Next milestone is v1.0 (Grafana adapters / own
UI).
