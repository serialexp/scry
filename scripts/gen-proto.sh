#!/usr/bin/env bash
# Regenerate Rust bindings from the two scry wire-protocol schemas:
#   * proto/ingest.schema.json -> crates/proto/src/generated.rs
#   * proto/query.schema.json  -> crates/proto/src/generated_query.rs
#
# We commit the generated source (both `generated*.rs`) and the
# vendored binschema runtime (crates/binschema-runtime/src/*.rs) so the
# normal build does not depend on node / binschema being installed. This
# script is the only path that should touch those files.
#
# The binschema generator emits a fresh copy of the runtime alongside
# each schema's generated.rs. The runtime is schema-independent, so the
# two runs MUST produce byte-identical runtime files; the script
# asserts that before copying so we don't accidentally diverge.
#
# Usage:
#   scripts/gen-proto.sh                # uses default binschema location
#   BINSCHEMA_DIR=/path scripts/gen-proto.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BINSCHEMA_DIR="${BINSCHEMA_DIR:-$HOME/Projects/binschema}"
CLI="$BINSCHEMA_DIR/packages/binschema/dist/cli/index.js"

if [[ ! -f "$CLI" ]]; then
  echo "error: binschema CLI not found at $CLI" >&2
  echo "       set BINSCHEMA_DIR to override (currently: $BINSCHEMA_DIR)" >&2
  exit 1
fi

INGEST_SCHEMA="$ROOT/proto/ingest.schema.json"
QUERY_SCHEMA="$ROOT/proto/query.schema.json"

TMP_INGEST="$(mktemp -d)"
TMP_QUERY="$(mktemp -d)"
trap 'rm -rf "$TMP_INGEST" "$TMP_QUERY"' EXIT

echo "validating $INGEST_SCHEMA"
node "$CLI" validate --schema "$INGEST_SCHEMA"
echo "validating $QUERY_SCHEMA"
node "$CLI" validate --schema "$QUERY_SCHEMA"

echo "generating Rust (ingest) into $TMP_INGEST"
node "$CLI" generate --language rust --schema "$INGEST_SCHEMA" --out "$TMP_INGEST"

echo "generating Rust (query) into $TMP_QUERY"
node "$CLI" generate --language rust --schema "$QUERY_SCHEMA" --out "$TMP_QUERY"

# The two runs must produce byte-identical runtime files. If they ever
# diverge it's a bug in the generator (or a sign that the runtime has
# acquired schema-specific knowledge); fail loudly rather than silently
# overwriting one set with the other.
#
# We enumerate whatever `.rs` files the generator emits rather than
# hardcoding the set — the runtime has grown modules over time (e.g.
# `codecs.rs`), and a hardcoded list silently drops new files, leaving
# `lib.rs` referencing a `mod` that was never vendored.
INGEST_RT="$TMP_INGEST/binschema_runtime/src"
QUERY_RT="$TMP_QUERY/binschema_runtime/src"
runtime_files=()
for path in "$INGEST_RT"/*.rs; do
  runtime_files+=("$(basename "$path")")
done

for f in "${runtime_files[@]}"; do
  if [[ ! -f "$QUERY_RT/$f" ]]; then
    echo "error: binschema runtime ($f) present for ingest but not query — generator bug?" >&2
    exit 1
  fi
  if ! cmp -s "$INGEST_RT/$f" "$QUERY_RT/$f"; then
    echo "error: binschema runtime ($f) differs between schemas — generator bug?" >&2
    diff -u "$INGEST_RT/$f" "$QUERY_RT/$f" >&2 || true
    exit 1
  fi
done
# Guard the other direction too: a file only the query run emits would
# otherwise be silently skipped.
for path in "$QUERY_RT"/*.rs; do
  f="$(basename "$path")"
  if [[ ! -f "$INGEST_RT/$f" ]]; then
    echo "error: binschema runtime ($f) present for query but not ingest — generator bug?" >&2
    exit 1
  fi
done

echo "copying runtime  -> crates/binschema-runtime/src/ (${runtime_files[*]})"
# Drop any stale vendored runtime files the generator no longer emits,
# so a removed module can't linger and shadow the fresh set.
rm -f "$ROOT/crates/binschema-runtime/src"/*.rs
for f in "${runtime_files[@]}"; do
  cp "$INGEST_RT/$f" "$ROOT/crates/binschema-runtime/src/$f"
done

echo "copying generated -> crates/proto/src/generated.rs"
cp "$TMP_INGEST/src/generated.rs" "$ROOT/crates/proto/src/generated.rs"

echo "copying generated -> crates/proto/src/generated_query.rs"
cp "$TMP_QUERY/src/generated.rs" "$ROOT/crates/proto/src/generated_query.rs"

echo "done. Review with: git diff crates/binschema-runtime crates/proto/src/generated.rs crates/proto/src/generated_query.rs"
