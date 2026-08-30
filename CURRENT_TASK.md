# Current task — runaway catalog full-walk (D-066), then per-phase query timing

## Problem

The Explore UI reported `1,000 rows · 9868.3 ms` with no way to attribute the
time. Reading the live gothab cluster answered it: **the query was not slow, it
was queued.**

Both scry pods were permanently stuck in a full-bucket catalog walk.
`scry-queryd` was walking 346,386 sidecar objects, `scry-server-0` 347,484,
both on node `app-1` against the same Garage. `crates/cluster/src/poll.rs`
fetched them in a sequential `for` loop at ~5 GETs/sec, so one pass took 15-20
hours — against a fixed-rate 30-minute timer, which meant the next tick was
always already due and the walk restarted the instant it finished. It had run
continuously for 5 days, reporting `inserted=0` every pass. Any non-404 error
aborted the whole pass and discarded ~15 hours of progress (happened 3× in 2
days). The actual query was 27 candidate blocks / 680 KB / 13 rows in 4,199 ms,
postings cache 27 hits 0 misses.

## Part A — the walk fix: DONE AND COMMITTED

Files, all exclusively mine (no overlap with the parallel agent):
`crates/catalog/src/lib.rs`, `crates/cluster/{Cargo.toml,src/poll.rs,tests/convergence.rs}`,
`crates/scry-ingestd/src/lib.rs`, `crates/scry-queryd/src/lib.rs`.

- **A1** `Catalog::known_block_uuids()` — every row in `blocks`, **no liveness
  filter**. `parse_block_key` reads `(signal, date, writer_id, uuid)` straight
  off the object key (the inverse of `block_path`), so a listed sidecar the
  catalog already has is skipped without a GET. A converged walk now costs a
  LIST and ~zero GETs. Using the live-only `list_blocks` predicate here would
  re-fetch every superseded input forever *and* resurrect soft-deleted blocks —
  there is a test pinning that.
- **A2** Walk first, then sleep `interval` — the gap is measured from
  *completion*, not a fixed rate. The first pass stays immediate on purpose
  (see the bug below).
- **A3** A non-404 GET failure, and an unparseable sidecar, are counted
  (`fetch_failed` / `failed`) and skipped instead of aborting the pass.
- **A3b (found while writing tests)** A skipped block must also **hold its
  prefix's cursor**. UUIDv7 is monotonic, so a later success in the same prefix
  would otherwise advance the cursor *past the gap* and the incremental poll
  could never see the skipped block again. New `poisoned` set does this; it
  also fixes the same pre-existing hole on the parse-failure path.
- **A4** `buffer_unordered(SIDECAR_FETCH_CONCURRENCY = 16)` for the residual
  GETs — matters on a cold boot, deliberately landed after A1 so it can't just
  make a pointless walk hit the store harder.

**Bug I introduced and fixed:** I first made the loop *sleep before* the first
walk, reasoning the boot seed walk had already run. It hasn't — on a
snapshot-restored boot (D-055) the boot walk is **skipped**, and a restored
catalog carries no poll cursors, so that first periodic walk is the only thing
that seeds them. Without it `poll_once` is blind forever.
`scripts/smoke-catalog-snapshot.sh` phase 3 caught it (converged to 9000 ≠
18000). Do not "simplify" that back.

### Verified
- `cargo test -p scry-cluster -p scry-catalog` — 48 green, incl. 3 new tests.
- A/B'd: all 3 new tests **fail without the fix** with the right symptoms, and
  the cursor-hold test fails specifically when `poisoned` is bypassed.
- `cargo build --workspace` clean, `cargo test --workspace` — **74 test
  binaries, no failures**, with the parallel agent's work merged in.
- `scripts/smoke-catalog-snapshot.sh` — all 4 phases green, including phase 3
  (`N2=18000`), the one that caught the snapshot-boot bug above.
- `MULTI=1 scripts/smoke.sh` — EXIT=0, `[multi] PASS`.
- `SIGNAL=both scripts/smoke.sh` — EXIT=0,
  `cache verdicts: miss hit (expected: miss hit)`.

### How it got committed
A parallel agent was mid-flight on a `scry-status` crate + gateway status page,
and **`Cargo.lock` was shared** — 2 lines mine (test-only `async-trait`/`bytes`
dev-deps on scry-cluster), 16 theirs. CI runs `--locked`, so neither half could
commit the lock alone. Bart's decision was **two commits, theirs first**: their
gateway-status work went in as `b8e179e` carrying the whole lock *and* my
`crates/cluster/Cargo.toml`, so that commit is `--locked`-green and still
compiles (my `QueryStats` match arm isn't needed until the generated enum
variant arrives in commit 2). Mine goes on top. No lock was hand-edited and
nothing was stashed.

## Part B — per-phase query timing: DONE (uncommitted)

Plan in `~/.claude/plans/snappy-leaping-meteor.md`. Key decisions already made:
a separate **`QueryStats` frame (tag `0x1E`) before `EndOfStream`**, *not*
fields on `EndOfStream` — the terminator is written through `write_and_tee` and
is baked into the result-cache entry, so timings inside it would replay the
original query's 9-second breakdown on every 2 ms cache hit. Consequence: the
cache stops caching the EOS frame and `result_cache::Entry` gains `rows: u64`.

Bart's decision at the time was **do only the non-colliding parts** —
`proto/query.schema.json`, the regenerated bindings
(`crates/proto/src/generated_query.rs`, `desktop/src/proto/generated.ts`), and
`crates/query/src/cli.rs` — leaving `crates/server/src/query_service.rs` and
`desktop/src/protocol/client.ts` until the other agent landed. **They have now
landed, so that block is lifted** and the rest of Part B is unblocked.

### Done so far (uncommitted, alongside Part A)
- `QueryStats` (tag `0x1E`) + `LiveNodeTiming` in `proto/query.schema.json`,
  schema version 0.2.0 → 0.3.0. Durations are **microseconds** — ms would round
  a whole cache-hit path to 0. Fields: the nine phases, the two sidecar-fetch
  totals, three DataFusion detail metrics, `server_total_us` measured
  *independently* of the phases, `cache_hit`, `attempts`, block/byte counts,
  `node_id`, and the `live_nodes` fan-out list.
- Bindings regenerated with `scripts/gen-proto-all.sh`; re-exported from
  `crates/proto/src/lib.rs`.
- `crates/query/src/cli.rs`: the remote path captures the frame (dropping it on
  a `ResponseSuperseded`, since a discarded attempt's timings describe work
  we threw away) and prints four `# timing:` lines beside `# scan:` / `# pool:`.
  A client-side `wall_start` before the connect makes `wall − server` — the
  transport hop — visible, which is the half the daemon can't measure.
- `crates/server/src/query_service.rs`: **one line**, a `QueryStats(_) =>`
  arm in the "client sent a response frame" name-lookup at :562. Required for
  the workspace to compile at all (exhaustive match on the enum); nowhere near
  the `run_query` timing work that's still deferred, and the parallel agent has
  not touched this file.
- Tests: 2 Rust round-trips in `crates/proto/src/framing.rs` (incl. one
  asserting `EndOfStream` is still the last frame) + 4 TS in the new
  `desktop/src/protocol/queryStats.test.ts`. Green: `scry-proto`, `scry-query`,
  `scry-server --lib` (38) and `--test query_e2e` (5), `bun run test` (100),
  `tsc --noEmit`.
- **Trap, hit once:** `desktop/src/proto/` is generator-owned and
  `scripts/gen-proto-ts.sh:78` runs `rm -f "$OUT"/*.ts`, so a hand-written file
  there is silently deleted by the next regen — it ate the first copy of that
  test. Hand-written protocol tests go in `desktop/src/protocol/`.
- **Vendored-runtime drift: fixed at the source.** Regenerating reverted the
  clippy fixes commit `1637e0a` had hand-applied to
  `crates/binschema-runtime/src/{bitstream,context}.rs` — because those fixes
  only ever existed in scry's *vendored copy*, never in binschema. The
  generator copies `~/Projects/binschema/rust/src/` verbatim and live (no dist
  snapshot: `findRustRuntimeDir()` prefers a published `rust-runtime/`, which
  doesn't exist here, then falls back to `../../rust/src`), so anything not
  upstream is erased on every regen. Fixed upstream instead —
  binschema `222b135` — then regenerated. `cargo clippy --lib` clean there,
  15/15 runtime tests, and binschema's pre-commit hook ran 390 suites / 1216
  tests green.
  The regenerated file now differs from scry HEAD by **7 lines**, both
  improvements: the `CompressionDictionary` alias moved *above*
  `EncodeContext`'s doc comment (in `1637e0a` it sat between the comment and
  the struct, so the comment documented the alias and `EncodeContext` had no
  docs), plus a restored `// UTF-8 byte length` comment. Do not hand-patch
  these files again — patch binschema and re-run `scripts/gen-proto-all.sh`.
- **And the file now says so.** binschema `ff13733` makes `generate` stamp a
  `@generated by binschema — DO NOT EDIT` banner naming the upstream directory
  onto every runtime file it copies, in all five languages, via one
  `copyRuntimeFile` helper (+ unit test). That's the `+7 lines` on each
  vendored runtime file in this diff. **binschema's `dist/` is gitignored**, so
  the rebuilt CLI is local-only — anyone else regenerating must
  `bun run build` in `packages/binschema` first.
- **Watch out:** the disk is at **91%** (`target/` alone is 251G) and two
  concurrent builds hit a transient `No space left on device` mid-link twice.
  It cleared on retry, but it will bite again.

- **A stale smoke assertion, not a cache bug.** `SIGNAL=both scripts/smoke.sh`
  failed its cache leg (`miss miss`, expected `miss hit`). I initially wrote
  this up as a result-cache bug; Bart corrected it and he is right — the two
  probes weren't submitting the same query, so there was no reason to expect a
  hit. D-059 (`apply_default_window`, `f83bdbb`, 2026-08-21) changed a
  bounds-less query from "all time" to "the last hour *from now*", so two
  probes seconds apart get different windows and different cache keys. The
  assertion was written `bdec69f` (2026-07-03) and had been red for nine days.
  Fixed by passing `--ts-min 1` in both probes — an explicit bound disables the
  default window entirely — with a comment saying why. The cache is correct.
  **The real target is still the *first* query being slow, not the second.**

### The rest of Part B, now also done
- **B1 — the result cache stopped caching `EndOfStream`.** A cached blob is
  `SchemaMsg | BatchMsg…`; `Entry` gained `rows: u64` and `get` returns a
  `CachedResponse { bytes, rows }`, so the server synthesizes a fresh
  `QueryStats | EndOfStream` after a replay. Also fixes the pre-existing wart
  where a hit logged `rows=0`.
- **B2 — `fetch_nanos: AtomicU64`** on the postings and bloom cache stats,
  timed **around `get_or_try_init`** (not the raw GET) so it measures what the
  *caller* waited for — own fill, joined single-flight, or permit wait — and
  recorded on the `Err` arm too, so a slow failing store can't read as fast. It
  rides the `CacheStarts` delta that already existed, so no timer is threaded
  through the four `register_*_table_from_candidates` functions. The assertion
  lives in `query_e2e.rs`, not a unit test: `postings_cache.rs`'s unit tests
  replicate the cache internals rather than driving `get_or_fetch`, so a test
  there would not have touched this code.
- **B3 — `run_query` phase timers + the emit.** `PhaseTimers` (9 phases) +
  `PlanTimings`; `handle_connection`/`run_query` now take `admission_wait` as a
  parameter because queueing happens *before* the per-query stopwatch and is
  the phase most likely to be the whole answer on a loaded daemon.
  `fetch_live_logs` returns per-peer `LiveNodeTiming` (failures included,
  sorted by addr for a stable UI). `QueryStats` is written **untee'd** on both
  the cache-hit and the normal path. `emit_scan_complete` logs the phases too.
- **B4 — `client.ts`** decodes the frame and times `tableFromIPC` separately,
  so `elapsedMs − server_total − decode` is the transport hop; `buildQueryTiming`
  is pure and unit-tested (11 cases).
- **B5 — the disclosure.** `resultTiming` signal in `store.ts` (a signal, not
  the store: the store's run outcome carries *scalars*, matching `resultTable`),
  and a new `TimingPopover.tsx` behind the `{rows} · {ms}ms` chip. Bars are a
  timeline that sums to the whole, with `other` as its own bar; DataFusion's
  counters are figures in a labelled aside, never slices, because they are
  summed across partitions and can exceed the phase containing them.

**Trap for whoever touches the e2e tests:** the moment the server started
emitting `QueryStats`, five `query_e2e` tests failed with "unexpected frame".
That is the correct symptom, not a regression — any exhaustive frame drain
needs a `QueryStats` arm. `probe.rs` was fixed pre-emptively for the same
reason; it would have `bail!`ed and broken `smoke-live.sh`.

**Not instrumented: local `scry get`.** It does not go through `QueryService`,
so it would need its own separate instrumentation. Flagged, not started.

Docs are current: D-066 in `docs/decisions.md` covers both halves and the first
measurements, and the `scry-cluster` / `scry-ingestd` / `scry-queryd` /
`scry-query` bullets in `CLAUDE.md` are updated.

## Also found, not addressed

- **Compaction is losing ground badly** — ~7 blocks reclaimed per 17 minutes
  against continuous ingest, so 346k blocks keeps growing. The merge itself is
  only 1.6-45 s; ~16 of every 17 minutes is spent *outside* it, suspects being
  `reconcile_partition`'s per-partition bucket LIST and `catalog.list_blocks()`
  re-run inside the loop at `maintain.rs:184` over a 346k-row table. Likely a
  *victim* of the same contention — re-measure after Part A ships. Bart asked
  for investigation only, no changes.
- `scry-gateway` was in CrashLoopBackOff on `--listen-otlp-grpc`; that is just
  the manifest running ahead of the deployed v0.17.0 image (commit `1637e0a`
  added the flag). Not a bug.
- gothab still runs **v0.17.0** and was only ever read, never modified.

---

# Previous task (complete) — query-attempt supersession and zero-grace compaction

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
4. ~~Run deployment smokes and stage the fleet with grace retained before
   enabling zero grace.~~ **Done — the rollout gate is closed.** Verified
   against the live `gothab` cluster on 2026-08-29: `scry-server-0`'s
   StatefulSet args carry `--compact-grace=0` (with `--ttl=90d
   --retention-apply`), on image `serialexp/scry:v0.17.0`; all five scry pods
   Running with 0 restarts at 4d10h; 24h of `scry-server` logs show active
   compaction and no panic / "pass failed", and `scry-queryd` logs show no
   supersession-related error (the only ERROR-ish lines on either side are
   `object_store` 503/timeout retries against Garage, logged at INFO and
   retried). So zero grace is live and has been stable for days.

Do not deploy or mutate production without Bart's explicit confirmation.
