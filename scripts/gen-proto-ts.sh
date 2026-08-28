#!/usr/bin/env bash
# Regenerate the TypeScript protocol bindings for the desktop app. The TS
# counterpart to scripts/gen-proto.sh (which emits the Rust bindings the
# daemon uses).
#
#   proto/query.schema.json  -> desktop/src/proto/generated.ts
#   proto/ingest.schema.json -> desktop/src/proto/generated-ingest.ts
#                             + desktop/src/proto/<binschema TS runtime>
#
# Two schemas, one vendored runtime. The query schema is the UI's data path
# (QueryFrame: request → Arrow batches). The ingest schema is needed only for
# the live-tail sub-protocol (Hello/HelloAck/Subscribe/TailRecord) the Live
# toggle speaks — the tail wire reuses the *ingest* Frame union, not the query
# one, because queryd's tail front-door is a transparent relay.
#
# Both generator runs emit the same runtime modules. We copy the runtime once
# (from the query run) and assert the ingest run produced byte-identical files,
# so a binschema version skew between the two can't silently ship two
# incompatible bitstream implementations.
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
INGEST_SCHEMA="$ROOT/proto/ingest.schema.json"
OUT="$ROOT/desktop/src/proto"
TMP="$(mktemp -d)"
TMP_INGEST="$(mktemp -d)"
trap 'rm -rf "$TMP" "$TMP_INGEST"' EXIT

echo "validating $QUERY_SCHEMA"
node "$CLI" validate --schema "$QUERY_SCHEMA"
echo "validating $INGEST_SCHEMA"
node "$CLI" validate --schema "$INGEST_SCHEMA"

echo "generating TypeScript (query) into $TMP"
# cwd MUST be the binschema package so the generator finds src/runtime.
( cd "$PKG" && node "$CLI" generate --language ts --schema "$QUERY_SCHEMA" --out "$TMP" )
echo "generating TypeScript (ingest) into $TMP_INGEST"
( cd "$PKG" && node "$CLI" generate --language ts --schema "$INGEST_SCHEMA" --out "$TMP_INGEST" )

# Both runs must agree on the runtime, or the two generated modules would be
# decoding against different bitstream semantics.
for path in "$TMP"/*.ts; do
  base="$(basename "$path")"
  [[ "$base" == "generated.ts" ]] && continue
  if ! cmp -s "$path" "$TMP_INGEST/$base"; then
    echo "error: runtime module $base differs between the query and ingest generator runs" >&2
    echo "       (binschema version skew?) — refusing to vendor a mixed runtime" >&2
    exit 1
  fi
done

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

# The ingest bindings ride alongside under their own name; their runtime
# imports (`./bit-stream.js`, …) resolve to the shared copy above.
{ printf '%s\n' "$BANNER"; cat "$TMP_INGEST/generated.ts"; } > "$OUT/generated-ingest.ts"

echo "done. Review with: git diff desktop/src/proto"
