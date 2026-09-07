#!/usr/bin/env bash
# Stop SeaweedFS while preserving data. Pass --wipe to remove its named volume
# and generated Scry environment file.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/docker/seaweedfs"

case "${1:-}" in
  "")
    docker compose down
    ;;
  --wipe)
    docker compose down -v
    rm -f "$ROOT/docker/seaweedfs/.env"
    echo "wiped SeaweedFS volume and .env"
    ;;
  *)
    echo "usage: $0 [--wipe]" >&2
    exit 2
    ;;
esac
