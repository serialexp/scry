#!/usr/bin/env bash
# Regenerate the TypeScript query-protocol bindings for the desktop app
# from proto/query.schema.json. The TS counterpart to scripts/gen-proto.sh
# (which emits the Rust bindings the daemon uses).
#
#   proto/query.schema.json -> desktop/src/proto/generated.ts
#                            +  desktop/src/proto/<binschema TS runtime>
#
# We commit the generated source AND the vendored binschema TS runtime so
# a normal `bun install && bun run build` never needs binschema installed.
# This script is the only path that should touch desktop/src/proto/*.ts.
#
# IMPORTANT: the binschema TS generator copies its runtime files from
# `<cwd>/src/runtime`, so the CLI MUST be invoked with the working
# directory set to the binschema package root. We do that explicitly.
#
# Usage:
#   scripts/gen-proto-ts.sh
#   BINSCHEMA_DIR=/path scripts/gen-proto-ts.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BINSCHEMA_DIR="${BINSCHEMA_DIR:-$HOME/Projects/binschema}"
PKG="$BINSCHEMA_DIR/packages/binschema"
CLI="$PKG/dist/cli/index.js"

if [[ ! -f "$CLI" ]]; then
  echo "error: binschema CLI not found at $CLI" >&2
  echo "       set BINSCHEMA_DIR to override (currently: $BINSCHEMA_DIR)" >&2
  exit 1
fi

QUERY_SCHEMA="$ROOT/proto/query.schema.json"
OUT="$ROOT/desktop/src/proto"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "validating $QUERY_SCHEMA"
node "$CLI" validate --schema "$QUERY_SCHEMA"

echo "generating TypeScript (query) into $TMP"
# cwd MUST be the binschema package so the generator finds src/runtime.
( cd "$PKG" && node "$CLI" generate --language ts --schema "$QUERY_SCHEMA" --out "$TMP" )

mkdir -p "$OUT"
# Drop any stale vendored TS the generator no longer emits so a removed
# runtime module can't linger and shadow the fresh set.
rm -f "$OUT"/*.ts

echo "copying generated + runtime -> $OUT/"
# The binschema 0.6.x TS generator emits code that does not pass the
# desktop app's strict tsconfig: it declares discriminated-union members
# as a bare union yet accesses them as a tagged `{ type, value }`
# envelope, reaches into the runtime's private `byteOffset`, and leaves
# unused locals. The *runtime behaviour* is correct — these are purely
# static-typing defects in the generator. We treat src/proto/* as
# vendored generated output (like the Rust `generated*.rs`) and stamp a
# `@ts-nocheck` banner so our own source still typechecks strictly.
# Re-stamping here means it survives every regen.
BANNER='// @ts-nocheck — VENDORED binschema-generated output. Do not hand-edit;
// regenerate with scripts/gen-proto-ts.sh. The binschema 0.6.x TS
// generator emits code that does not satisfy our strict tsconfig
// (bare-union variants used as { type, value }, cross-class private
// access, unused locals). Runtime behaviour is correct; only the
// emitted static types are at fault. Tracked upstream in binschema.
'
for path in "$TMP"/*.ts; do
  dest="$OUT/$(basename "$path")"
  { printf '%s\n' "$BANNER"; cat "$path"; } > "$dest"
done

echo "done. Review with: git diff desktop/src/proto"
