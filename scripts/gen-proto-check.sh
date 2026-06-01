#!/usr/bin/env bash
# CI drift gate: regenerate ALL wire-protocol bindings and fail if the
# committed output differs from what the schemas produce. This is the
# guard against the "added a schema field, regenerated Rust but forgot
# TS (or vice-versa)" bug class — a desync there silently corrupts the
# wire frame (e.g. a TS QueryRequest missing `body_contains` decodes as
# INVALID_VARIANT on the daemon and the query dies mid-stream).
#
# Mechanism-agnostic: it just runs scripts/gen-proto-all.sh (which honours
# BINSCHEMA_DIR) and then `git diff --exit-code`s the generated paths.
# Point BINSCHEMA_DIR at:
#   - a local checkout (dev machines: $HOME/Projects/binschema, the default), or
#   - a `pnpm dlx binschema@<ver>`-materialized package in CI
#     (see .github/workflows — note this needs a binschema npm build that
#      ships the TS runtime; see scripts/gen-proto-ts.sh header).
#
# Exit 0 = in sync. Exit non-zero = drift (and prints the diff).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Paths the generators own. If any of these change after a regen, the
# committed bindings are stale.
GEN_PATHS=(
  crates/binschema-runtime/src
  crates/proto/src/generated.rs
  crates/proto/src/generated_query.rs
  desktop/src/proto
)

echo "── regenerating all bindings to check for drift ──"
"$ROOT/scripts/gen-proto-all.sh"

echo "── diffing generated paths against the working tree ──"
if git diff --exit-code -- "${GEN_PATHS[@]}"; then
  echo "✓ generated bindings are in sync with the schemas"
  exit 0
else
  echo
  echo "✗ DRIFT: generated bindings differ from the schemas." >&2
  echo "  Run 'scripts/gen-proto-all.sh' and commit the result." >&2
  exit 1
fi
