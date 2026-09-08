# Repository guidance

## Project in brief

`scry` is an opinionated Rust observability system for metrics, logs, traces, and
profiles. One multicall `scry` binary provides the ingest, query, maintenance,
gateway, agent, tail, replay, and web roles. Records flow through a per-writer WAL
into immutable Parquet blocks in S3-compatible object storage; local SQLite
catalogs index those blocks and DataFusion executes queries. Multiple instances
can share a bucket, using Valkey for lease-guarded maintenance and fast catalog
convergence while retaining object storage as the data source of truth.

The project is pre-1.0 but all four signal paths are operational. Do not maintain
a milestone history, feature inventory, or backlog in this file; those belong in
the documents below.

## Sources of truth

Read the relevant section before changing a subsystem; do not preload every large
document indiscriminately.

- [`README.md`](README.md): current capabilities, workspace map, local operation,
  deployment, smoke-test index, and release process.
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md): load-bearing system principles,
  invariants, and subsystem design. Some sections describe future architecture;
  check their implementation-status notes and the code.
- [`docs/decisions.md`](docs/decisions.md): accepted decisions and supersession
  history. A later decision overrides an earlier one.
- [`docs/design/`](docs/design/): focused feature designs and implementation
  checklists.
- [`TODO.md`](TODO.md): known follow-ups. [`CURRENT_TASK.md`](CURRENT_TASK.md):
  transient handoff context when active work is recorded there.
- [`proto/README.md`](proto/README.md) and `proto/*.schema.json`: protocol guide
  and authoritative wire schemas.

When documentation and implementation disagree, investigate rather than silently
choosing whichever supports the intended change. Correct stale documentation as
part of the work when its intended contract is clear; ask when it is not.

## Architecture and implementation constraints

- Preserve the structural resource bounds in
  [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md#guiding-principles): stream rather
  than materialize, apply backpressure rather than grow queues, bound caches and
  per-query state by bytes, and fail cleanly instead of risking OOM.
- Treat per-item allocation in hot loops as a defect class. Reuse scratch storage,
  measure performance-sensitive changes, and inspect the relevant benchmark or
  flamegraph before asserting an optimization.
- Object storage contains committed immutable blocks and remains the recoverable
  data source of truth. Valkey carries coordination, ephemeral registries, and
  convergence hints. New multi-instance compaction commits and retention staging
  require a valid single-winner lease and pause if fencing is unavailable;
  physical cleanup already durably staged as non-live is idempotent and lease-free.
- Keep shared engines independent of deployment services where the existing seams
  provide that separation (for example `Fence`, `BlockEventSink`, discovery, and
  fleet-status traits).
- `unsafe_code = "forbid"` is workspace-wide.
- The Tauri/SolidJS app under `desktop/` is intentionally not a Cargo workspace
  member. Rust workspace commands do not validate it.

## Build and validation

Use focused checks while iterating, then broaden validation according to the
change. Common commands:

```bash
cargo fmt --all --check
cargo test -p <package> [test-filter]
cargo test --workspace
cargo clippy --workspace --all-targets
cargo build --release --workspace

cd desktop
bun run typecheck
bun run test
bun run build
```

Run the smoke test that owns the changed path; the indexed list is in
[`README.md`](README.md#build-and-run-locally) and scripts are named
`scripts/smoke-*.sh`. `scripts/smoke.sh` covers per-signal ingest → storage →
query, and `MULTI=1 scripts/smoke.sh` covers shared-bucket coordination.

**Safety:** storage smoke tests empty their configured development bucket. Never
point them at a bucket whose contents must be retained. Live object-store and
Valkey tests may require local services and explicit environment configuration;
use the repository's `scripts/dev-*-up.sh` helpers and do not weaken a required
integration test into a silent skip.

For frontend or version changes on the configured home development machine,
`just recompile-webui` is the install path: it embeds the freshly built bundle,
installs `~/.cargo/bin/scry`, restarts `scry-webui.service`, and verifies it. A
plain Cargo install can embed stale or empty assets.

## Protocol and generated code

The three schemas under `proto/` are authoritative. Run:

```bash
scripts/gen-proto-all.sh
# or with another binschema checkout:
BINSCHEMA_DIR=/path/to/binschema scripts/gen-proto-all.sh
```

This regenerates applicable Rust and TypeScript bindings plus the vendored
binschema runtimes. Do not hand-edit:

- `crates/proto/src/generated*.rs`;
- `crates/binschema-runtime/src/*.rs`;
- `desktop/src/proto/*.ts`.

`scripts/gen-proto.sh` and `scripts/gen-proto-ts.sh` are language-specific entry
points; prefer the all-language wrapper when changing a schema so clients cannot
drift. `scripts/gen-proto-check.sh` is the drift gate.

Protocol hazards worth keeping in immediate context:

- Framing is `[length: u32 big-endian][message]`. A message is a flat tagged
  union whose first byte is its discriminator, not a separate header and payload.
- Numeric protocol constants are mirrored by hand in
  `crates/proto/src/constants.rs`; synchronize them whenever schemas change.
- Reason and status codes drive behavior. Free-text messages are for operators.
- Ingest version negotiation occurs only in `Hello`: the requested version must
  be explicitly supported and `HelloAck` echoes it. The private worker protocol
  requires an exact version; the public query wire has no version handshake.
- Series fingerprints use the shared canonical implementation in
  `scry-proto`; never reproduce its hashing or label-ordering logic locally.
- The schemas currently contain no `computed` fields. If one is introduced,
  audit every constructor in `crates/proto/src/build.rs`: generated `Input ->
  Output` conversion may leave the in-memory computed slot at its default even
  though serialization computes correct wire bytes.

## Repository conventions

- Keep changes cohesive and add focused tests for success, failure, and boundary
  behavior. Use the relevant end-to-end smoke when a wire, storage, convergence,
  or browser path changes.
- Do not let operator-facing roles absorb reusable behavior; keep binaries and
  subcommand crates thin over testable libraries.
- Avoid hand-maintained inventories of methods, flags, and implementation status
  here. Update the owning README, architecture section, decision, focused design,
  CLI help, or code comments instead.
- Workspace versions, changelogs, release commits, and tags are owned by the
  pinned just-release workflow. Use conventional commits, but do not hand-edit
  versions or create/push release tags. See [`README.md`](README.md#releases).
- **Never assume an upward version change is accidental or stale.** If Cargo,
  generated lockfile entries, desktop metadata, or another version-bearing file
  moves forward, first check the current workspace manifest, recent release
  history, and related metadata. A committed lockfile can lag the released
  workspace version. Preserve a coherent forward change; if intent remains
  unclear, stop and ask before changing or reverting any version.
- Do not casually override the release profile (`thin` LTO,
  `codegen-units = 1`); performance harnesses assume it.
