# CURRENT_TASK — CLI install/release + D-056 (both COMPLETE, uncommitted)

## Follow-up: prebuilt `scry` CLI binary — release artifacts + install.sh (DONE)
Bart wanted to download+install the `scry` CLI/server binary (where
`replay-opensearch` now lives), like `~/Projects/cool-rust-terminal` does.
Added (modeled on that repo, headless — no desktop entry):
- `install.sh` (repo root): detect os-arch → `GET /releases/latest` → download
  `scry-<ver>-<os>-<arch>.tar.gz` + verify `.sha256` → install to
  `/usr/local/bin` or `~/.local/bin`. Distinct from `desktop/install.sh` (GUI).
- `.github/workflows/release.yml` new **`cli`** job: matrix — linux x86_64/aarch64
  static **musl** via `cross`, macOS x86_64/aarch64 native — guards
  tag≡workspace version, tarballs `scry`+`README.md`+`.sha256`, `softprops`
  attaches to the SAME draft Release the `desktop` job creates (draft, publish
  manually). No Windows.
- Docs: README `## Install` section; CLAUDE.md Tooling bullet.
- Verified: native static-musl build of `scry` links `statically linked`
  (proves mimalloc-under-musl) and `scry replay-opensearch --help` runs;
  release.yml YAML validates (jobs image/desktop/cli); `install.sh` `bash -n` +
  `--help`/bad-arg/detect_platform all correct. (shellcheck unavailable on host —
  used `bash -n` + review.) Choices confirmed by Bart: musl, linux+macos, no win.
- NOT committed (git allowlist — waiting on Bart).

## What
New `scry replay-opensearch` subcommand: replays an existing OpenSearch corpus
into a `scry ingest` server at an auto-ramping, hold-at-knee rate to find scry's
ingest throughput ceiling. Reads oldest→newest via PIT + `search_after`, maps
each `_source` → scry log record (convention + overridable flags, faithful copy
of the original `@timestamp`), ships over the native wire. Progress bar + stats
line. Decision record = D-056.

## Status: DONE — all tasks #55–#64 complete. NOT committed (waiting on Bart).

### Code
- New leaf crate `crates/httpsig` (`scry-httpsig`): `build_http_client` + `SigV4Signer`
  extracted verbatim from `gateway/src/{tls,aws_sign}.rs` (both deleted); gateway
  repointed to `scry_httpsig::…`, aws deps moved out of gateway into httpsig.
- New crate `crates/replay-opensearch` (`scry-replay-opensearch`): `os.rs` (PIT read
  client), `map.rs` (pure doc→record, 14 unit tests), `wire.rs` (hand-rolled ack-aware
  ingest loop), `pace.rs` (token bucket + ramp controller, 3 tests), `stats.rs`
  (indicatif bar), `lib.rs` (2-stage fetch→map+send pipeline + Args).
- `crates/scry` main.rs + Cargo.toml: `Cmd::ReplayOpensearch` wired.
- Workspace Cargo.toml: members + `indicatif` dep + both crate paths.

### Docs
- `docs/decisions.md` D-056 appended.
- `CLAUDE.md`: eleven-roles multicall paragraph + two Binaries bullets
  (replay-opensearch, httpsig) + smoke-osreplay entry under Tooling.
- `TODO.md`: D-056 deferred follow-ups section.

### Verification (all PASS)
- `cargo test --workspace` — 69 ok, 0 failed.
- `cargo build --release --workspace` — clean.
- `scripts/smoke-osreplay.sh` — 7/7 assertions (50 rows, service=api 17,
  grep 48, ts_inherited=2, body_missing=2).
- `scripts/smoke-gateway.sh` — PASS (httpsig extraction regression).
- `SIGNAL=logs scripts/smoke.sh` — PASS (wire path regression).

## Next
Nothing pending. Awaiting Bart's decision to commit (conventional-commit; git is
allowlist — do not commit until asked). No wire-schema change (no gen-proto).
