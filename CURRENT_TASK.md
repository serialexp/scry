# CURRENT_TASK — handoff

This document hands off to a fresh agent working **from the `scry` repo
root** (`/home/bart/Projects/scry`). It describes two just-completed,
**uncommitted** chunks of work: (1) the **desktop query app** under
`desktop/`, and (2) the **traces + profiles query verticals**
(v0.5 / v0.6), which lit up the query side for the two signals that
already stored but couldn't be read back.

Addressee is Bart. Keep all of `~/.claude/CLAUDE.md`'s rules in mind
(notably: #2 quality over speed, #3 never proclaim success — Bart
decides, #5 this doc, #7 do it right, #13/#15 git discipline, never
commit unless explicitly asked).

---

## DONE but UNCOMMITTED: desktop query app (`desktop/`)

A GUI alternative to the `scry-query` CLI: a **TypeScript implementation
of the query wire protocol** in a Tauri v2 + SolidJS desktop window that
opens a **native TCP socket** to `scry-queryd` (a browser can't, hence
desktop). Bart chose Tauri + SolidJS + native socket.

### Shape
- The query protocol lives **entirely in TypeScript**; Rust is a dumb
  byte pipe (`src-tauri` `run_query`: connect → write framed request →
  read to EOF → return raw bytes as `tauri::ipc::Response`/ArrayBuffer).
  Rust holds zero protocol knowledge, so it never changes when the wire
  schema does.
- `desktop/src/proto/` — TS bindings generated from
  `proto/query.schema.json` by `scripts/gen-proto-ts.sh` (the TS
  counterpart to `gen-proto.sh`; vendored + committed).
- `desktop/src/protocol/` — `constants.ts` (Signal/QUERY_ERR_* mirrored
  from Rust), `framing.ts` (`[len:u32 BE][body]`), `client.ts` (build
  request → drive transport → decode QueryFrames → concat Arrow IPC →
  `apache-arrow` `tableFromIPC`), `transport.ts` (`Transport` iface +
  `TauriTransport`).
- `desktop/src/` — SolidJS UI: `store.ts` (createStore form/run state +
  a `createSignal` holding the Arrow `Table` — kept OUT of the deep-
  proxied store), `components/QueryForm.tsx`, `components/ResultsTable.tsx`,
  `App.tsx`.
- Root `Cargo.toml` gains `exclude = ["desktop/src-tauri"]` so
  `cargo build --workspace` is unaffected.

### binschema TS generator bug (flagged for Bart — he owns binschema)
The 0.6.x **TS** generator emits non-strict-clean code: discriminated-
union members typed as a bare union but used as a tagged `{type,value}`
at runtime; cross-class private `byteOffset` access; unused locals.
Runtime behaviour is correct (proven live). Worked around by (1)
`gen-proto-ts.sh` stamping `// @ts-nocheck` on vendored files, (2) a
single `as unknown as` cast at each boundary in `client.ts`. The Rust
generator is fine. Worth fixing upstream so the casts/`@ts-nocheck` go.

### Verification (all green)
- `cd desktop && bun install && bun run typecheck` → exit 0.
- `bun run build` (vite) → built (one benign `fs` externalization warning
  from the unused binschema file-handle reader).
- `cargo build` in `desktop/src-tauri` → compiles (tauri/webkit pulled).
- `cargo build --workspace` at root → still green, desktop crate excluded.
- **Live protocol smoke**: `desktop/scripts/smoke-protocol.ts` (drives
  the real `runQuery` over a `node:net` transport, no GUI) against a
  `scry-queryd` fed fresh metrics → `SELECT *` + SQL decode correctly
  (cols `series_fingerprint:Uint64, ts_unix_nano:Uint64, value:Float64`,
  bigints/floats round-trip), client rows == server rows, bad SQL
  surfaces as `QUERY_ERR_SQL_PARSE`. PASS.
- GUI launch (`bun run app:dev`) not run here — needs a display; that's
  Bart's manual step on the desktop.

### Not done by design
- Streaming/incremental result rendering (collect-then-decode is fine for
  limited results; the daemon is one-shot per connection anyway).
- Result virtualization beyond a 2000-row display cap.
- Matcher autosuggest / schema-aware forms, query history.

---

## DONE but UNCOMMITTED: traces + profiles query (v0.5 / v0.6)

### What it is

scry stored all four signals but only queried metrics + logs back.
Traces/profiles blocks were catalogued yet unreadable —
`query_service.rs` accepted only `Signal::Metrics | Signal::Logs` and
`scry-query --signal` rejected the rest. This work lights up the query
side for both, mirroring the v0.4 logs vertical, and renumbers the
roadmap (storage-then-query split; gateway shipped unnumbered,
traces/profiles storage landed ahead of query).

### What changed

**New query modules** (mirror `crates/query/src/logs.rs`):
- `crates/query/src/traces.rs` — `TracesTable` `TableProvider`,
  `traces_schema()` (reuses `TracesBlockBuilder::main_schema()`),
  `list_/build_/register_traces_table[_from_candidates]`, `traces_query`.
  **No postings** — matcher/time/trace-id filters push as parquet row
  predicates. Promoted matchers (`service.name`, `service.namespace`,
  `deployment.environment[.name]`) map to the promoted Utf8 columns; any
  other matcher key is rejected (→ use `--sql`). `--trace-id` builds a
  `col("trace_id").eq(FixedSizeBinary(16, …))` equality; block is sorted
  by `trace_id` so row-group stats prune.
- `crates/query/src/profiles.rs` — `ProfilesTable`, retrieval only
  (time bounds + `--sql`; raw pprof `data` Binary out). Label matchers
  rejected (→ `--sql`). Flamegraph aggregation deferred (D-034).

**Trace-by-id plumbing:**
- `Query.trace_id: Option<[u8;16]>` (`crates/query/src/lib.rs`).
- Wire: `trace_id` bytes field on `QueryRequest` in
  `proto/query.schema.json` (empty = absent). Regenerated via
  `scripts/gen-proto.sh` → `crates/proto/src/generated_query.rs`.
  Mapped in `crates/query/src/wire.rs` `to_wire`/`from_wire`.
- CLI: `--trace-id <hex>` (`crates/query/src/bin/scry-query.rs`), implies
  `--signal traces`; `parse_signal`/`table_name`/dispatch extended for
  traces + profiles.
- Daemon: the four signal matches in
  `crates/server/src/query_service.rs` (accepted signal / candidates /
  registration / default table) extended to Traces + Profiles.

**binschema runtime fix (incidental, required):** the local binschema
*dist* was stale — its generated `lib.rs` referenced a `codecs` module
its copy step never emitted. Rebuilt `~/Projects/binschema` dist
(`bun run build` in `packages/binschema`), taught `scripts/gen-proto.sh`
to vendor **all** runtime `.rs` files (not a hardcoded three), and added
`flate2 = "1.0"` to `crates/binschema-runtime/Cargo.toml` (codecs needs
it; dormant — scry's schemas use no `compressed` regions). The vendored
runtime is now a faithful copy again.

**Tests / seal:**
- Unit: `wire.rs` trace_id roundtrip; `traces.rs`/`profiles.rs`
  schema-shape + matcher-validation tests.
- Daemon e2e: `crates/server/tests/query_e2e.rs::traces_round_trip`
  (proves `Signal::Traces` dispatch + `trace_id` over the wire prunes
  server-side).
- `scripts/smoke.sh`: query round-trip leg now runs for `traces` and
  `profiles` (and all four under `SIGNAL=all`), plus a `--trace-id`
  assertion that the by-id lookup returns exactly that trace's spans.

**Docs:** `docs/decisions.md` D-034 (traces+profiles query verticals,
profiles payload schema decided = opaque pprof Binary, flamegraph
deferred with the Grafana-renders-pre-aggregated rationale); README
Status + scope table (v0.5/v0.6 ✅); CLAUDE.md status paragraph.

### Verification status (re-run before committing)

- `cargo build --workspace` — green.
- `cargo test --workspace` — green (incl. new unit + e2e tests).
- `scripts/smoke.sh` for `metrics`, `logs`, `traces`, `profiles`, `all`
  — all PASS against the dev Garage (`scripts/dev-garage-up.sh` first).
- `cargo clippy` — confirm clean before commit.

### Not done by design (deferred)

- **Profiles flamegraph aggregation** — pprof parse + stack-merge. Needs
  a pprof-parser dep + nested-set output a UI consumes. Out of scope per
  D-034; nothing consumes it yet.
- Arbitrary span-attribute / profile-label *matcher* filtering (Map
  element access) — via `--sql` for now; a typed predicate surface can
  come with the query-language work (v0.7).
