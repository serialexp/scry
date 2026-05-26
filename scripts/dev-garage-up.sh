#!/usr/bin/env bash
# Bring up the local Garage S3-compatible store for development and
# initialise its layout / bucket / credentials.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/docker/garage"

docker compose up -d
"$ROOT/docker/garage/init.sh"
