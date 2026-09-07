#!/usr/bin/env bash
# Bring up the local SeaweedFS S3-compatible store and initialise its bucket.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/docker/seaweedfs"

docker compose up -d
"$ROOT/docker/seaweedfs/init.sh"
