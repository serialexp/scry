# CURRENT_TASK — handoff

## v0.8 — compaction + retention, both landed (single-instance), uncommitted

Both halves of the v0.8 line are implemented and green. Everything below is in
the working tree, **not yet committed**. Rationale: `docs/decisions.md § D-036`
(compaction) + `§ D-037` (retention); status notes in
`docs/ARCHITECTURE.md § Compaction` and `§ Retention`.

---

## Part A — compaction (landed earlier)

### What shipped

- **`BlockMeta.level`** (`crates/block/src/meta.rs`, `#[serde(default)]`) +
  catalog plumbing: `insert_block` writes `meta.level`, `row_to_entry` reads
  it back. Every encoder sets `level: 0`. Old sidecars deserialise to 0.
- **Catalog** (`crates/catalog/src/lib.rs`): `list_blocks` filters
  `WHERE deleted_at IS NULL AND superseded_by IS NULL`. New
  `mark_superseded(inputs, merged)` and `delete_blocks(uuids)` transactions.
- **Shared sidecar helpers** (`crates/block`): `postings.rs`
  (`encode_postings` / `decode_postings` / `merge_postings`); `bloom.rs`
  streaming `BodyBloomBuilder` (`new`/`add_body`/`finish`).
- **`crates/compact`** (`scry-compact`, lib + bin): `policy.rs`, `merge.rs`
  (DataFusion sort-merge + sidecar rebuild), `engine.rs` (`compact_once`),
  `main.rs` (`--once` / `--watch`).

### Key gotcha (don't regress)

The merge `SessionContext` **must** set
`parquet.schema_force_view_types = false`. DataFusion otherwise reads parquet
string columns back as `Utf8View`, which (a) breaks the body-column downcast
in the bloom rebuild and (b) would make the merged block's schema differ from
a freshly-written L0 block. Merged blocks must be schema-identical to L0.

---

## Part B — retention (this session)

### What shipped

- **`delete_block_objects` lifted into `scry-block`**
  (`crates/block/src/lib.rs`, after `block_path`): deletes parquet +
  meta.json + flagged sidecars, NotFound-tolerant, takes `&dyn ObjectStore`.
  `scry-compact` dropped its local copy and imports it
  (`engine.rs`: `delete_block_objects(store.as_ref(), &input.meta)`).
- **Catalog `mark_deleted(uuids, deleted_at_unix_nano)`**
  (`crates/catalog/src/lib.rs`): soft-delete via `deleted_at`, mirroring
  `mark_superseded`. Since `list_blocks` filters `deleted_at IS NULL`, a
  marked block drops out of queries instantly — that's what makes a non-zero
  `--grace` a correct window. Catalog test
  `marked_deleted_blocks_drop_out_of_list_blocks` (9 catalog tests pass).
- **`crates/retention`** (`scry-retention`, lib + bin), added to workspace
  `Cargo.toml` members:
  - `policy.rs` — `RetentionConfig { default_ttl, overrides, grace, apply }`
    with `ttl_for(signal) = overrides.or(default_ttl)` (opt-in: `None` →
    never reaped) and `any_ttl_configured()`. `plan_reaping(blocks, cfg,
    now_unix_nano)` selects blocks where the signal has a TTL **and**
    `ts_max_unix_nano < now - ttl` (whole-block, strict `<`); sorts
    deterministically `(signal, date, uuid)`. 6 unit tests.
  - `engine.rs` — `RetentionReport { scanned, reaped, bytes_reaped, dry_run,
    by_signal }`. `retain_once(store, catalog, cfg, now_unix_nano)`:
    `list_blocks` → `plan_reaping` → dry-run returns the report untouched;
    apply optionally `mark_deleted` + sleep when `grace > 0`, then
    `delete_block_objects` per block (objects first), then `delete_blocks`
    (rows last) — same ordering as compaction.
  - `main.rs` — clap: `--catalog`, `--ttl` (global), `--ttl-{metrics,logs,
    traces,profiles}` overrides, `--grace` (default 0), `--apply` (default
    false → dry-run), `--no-reconcile`, `--watch`, `--interval` (default
    3600). Errors if no TTL configured at all. `parse_duration` extends the
    spewer's with `d` (days). `now` from `SystemTime::now()`.

### Tests / smoke

- `crates/retention/tests/retention_e2e.rs`
  (`logs_retention_dry_run_then_apply`): old logs (NOW−90d) + recent logs
  (NOW−1h) + ancient metrics (NOW−200d, no TTL); `ttl_logs=7d`. Dry-run inert
  (3 blocks, parquet present); apply reaps only the old logs block (NotFound +
  `get_block` None), recent logs + metrics survive, surviving query returns
  exactly the 50 recent rows; second apply reaps 0 (idempotent).
- `scripts/smoke.sh`: `-p scry-retention` in the build; a retention leg
  (gated `RETAIN=1`, logs/both/all) after compaction — baseline reconcile;
  dry-run `--ttl-logs 0s` asserts "would reap" present + "reaped" absent +
  count unchanged; apply `--ttl-logs 0s --apply` asserts logs=0 and
  metrics==baseline (signal-scoping end to end).

### Decisions (D-037)

Single-instance; **opt-in per signal** (no implicit deletion); **dry-run by
default**, `--apply` to delete; **whole-block `ts_max`** criterion (in-window
data never dropped); reuses compaction's delete plumbing + the `deleted_at`
query-skip for a correct grace.

---

## Verified (run before final commit)

- `cargo test --workspace` green (compaction e2e + retention e2e + catalog +
  block units).
- `cargo clippy -p scry-retention -p scry-compact -p scry-block --tests`
  clean (workspace warnings are all pre-existing in dependency crates).
- `SIGNAL=logs scripts/smoke.sh` + `SIGNAL=both scripts/smoke.sh` PASS with
  compaction **and** retention legs (both: pre-retention logs=2 metrics=20 →
  dry-run logs=2 → apply logs=0 metrics=20).

## Not done / deferred (shared follow-ups)

- **Multi-instance lease** (compaction + retention share the deferred
  per-`(signal, day)` object-store lease). Single-instance engines are
  forward-compatible.
- **In-`scry-ingestd` background loop** for both compaction and retention.
- **Hand-rolled k-way streaming merge** (compaction optimisation).
- **Partial-block rewriting** (retention drops whole blocks only) and
  **size/quota eviction** (retention is purely age-based).

## Next step

Commit the v0.8 work (conventional commits; whole-file commits only per
Rule #13). Suggested split if desired:
1. `feat(block): level + shared postings/streaming-bloom helpers + lift delete_block_objects`
2. `feat(catalog): supersede/delete/mark_deleted + superseded/deleted query filter`
3. `feat(compact): scry-compact size-tiered compaction crate`
4. `feat(retention): scry-retention per-signal TTL reaper`
5. `test(smoke): compaction + retention legs` + docs

Check each commit stages whole files cleanly first (e.g. `scry-block`'s
`lib.rs` changes span both compaction helpers and the lifted
`delete_block_objects` — bundle them in commit 1).
