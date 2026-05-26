#!/usr/bin/env bash
# Regenerate Rust bindings from proto/ingest.schema.json.
#
# We commit the generated source (crates/proto/src/generated.rs) and the
# vendored binschema runtime (crates/binschema-runtime/src/*.rs) so the
# normal build does not depend on node / binschema being installed. This
# script is the only path that should touch those files.
#
# Usage:
#   scripts/gen-proto.sh                # uses default binschema location
#   BINSCHEMA_DIR=/path scripts/gen-proto.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BINSCHEMA_DIR="${BINSCHEMA_DIR:-$HOME/Projects/binschema}"
CLI="$BINSCHEMA_DIR/packages/binschema/dist/cli/index.js"
SCHEMA="$ROOT/proto/ingest.schema.json"

if [[ ! -f "$CLI" ]]; then
  echo "error: binschema CLI not found at $CLI" >&2
  echo "       set BINSCHEMA_DIR to override (currently: $BINSCHEMA_DIR)" >&2
  exit 1
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "validating $SCHEMA"
node "$CLI" validate --schema "$SCHEMA"

echo "generating Rust into $TMP"
node "$CLI" generate --language rust --schema "$SCHEMA" --out "$TMP"

echo "copying runtime  -> crates/binschema-runtime/src/"
cp "$TMP/binschema_runtime/src/lib.rs"       "$ROOT/crates/binschema-runtime/src/lib.rs"
cp "$TMP/binschema_runtime/src/bitstream.rs" "$ROOT/crates/binschema-runtime/src/bitstream.rs"
cp "$TMP/binschema_runtime/src/context.rs"   "$ROOT/crates/binschema-runtime/src/context.rs"

echo "copying generated -> crates/proto/src/generated.rs"
cp "$TMP/src/generated.rs" "$ROOT/crates/proto/src/generated.rs"

echo "done. Review with: git diff crates/binschema-runtime crates/proto/src/generated.rs"
