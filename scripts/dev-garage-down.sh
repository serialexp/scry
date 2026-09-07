#!/usr/bin/env bash
# Transitional compatibility wrapper. SeaweedFS is now Scry's local S3 backend.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
echo "warning: dev-garage-down.sh is deprecated; use scripts/dev-seaweedfs-down.sh" >&2
exec "$ROOT/scripts/dev-seaweedfs-down.sh" "$@"
