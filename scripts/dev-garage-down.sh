#!/usr/bin/env bash
# Tear down the local Garage container. By default keeps the volumes so
# data and credentials persist across restarts. Pass --wipe to delete
# the volumes and the .env file (next 'up' starts from scratch).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/docker/garage"

if [[ "${1:-}" == "--wipe" ]]; then
  docker compose down -v
  rm -f "$ROOT/docker/garage/.env"
  echo "wiped volumes and .env"
else
  docker compose down
fi
