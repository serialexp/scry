#!/usr/bin/env bash
# Bring up the local Valkey instance used for scry's multi-instance
# coordination (lease + block-event pub/sub) during development and the
# gated integration tests. Mirrors scripts/dev-garage-up.sh.
#
# After this, point the daemons / tests at it with:
#   export SCRY_VALKEY_URL=redis://127.0.0.1:6379
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/docker/valkey"

docker compose up -d

# Wait until it answers PING so callers can use it immediately.
for _ in $(seq 1 50); do
  if docker exec scry-valkey valkey-cli ping 2>/dev/null | grep -q PONG; then
    echo "valkey ready at redis://127.0.0.1:6379"
    exit 0
  fi
  sleep 0.2
done

echo "valkey did not become ready in time" >&2
exit 1
