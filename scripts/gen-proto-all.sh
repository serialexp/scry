#!/usr/bin/env bash
# One command to regenerate ALL scry wire-protocol bindings from the
# schemas in proto/ — Rust (ingest + query) AND the TypeScript query
# bindings — so the two never drift out of sync.
#
# The root cause of a class of bugs we hit: a schema field is added, the
# Rust bindings get regenerated (scripts/gen-proto.sh), but the TS
# bindings (scripts/gen-proto-ts.sh) are forgotten — leaving the desktop
# / browser query client encoding a frame the daemon can't decode. This
# wrapper runs both, so regenerating one without the other is no longer
# a thing you can do by accident.
#
# Both delegated scripts honour BINSCHEMA_DIR (default
# $HOME/Projects/binschema); see their headers.
#
# Usage:
#   scripts/gen-proto-all.sh
#   BINSCHEMA_DIR=/path scripts/gen-proto-all.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "── regenerating Rust bindings (ingest + query) ──"
"$ROOT/scripts/gen-proto.sh"

echo "── regenerating TypeScript bindings (query) ──"
"$ROOT/scripts/gen-proto-ts.sh"

echo "── all bindings regenerated. Review with: git diff proto crates/proto crates/binschema-runtime desktop/src/proto ──"
