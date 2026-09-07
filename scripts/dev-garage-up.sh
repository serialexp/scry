#!/usr/bin/env bash
# Transitional compatibility wrapper. SeaweedFS is now Scry's local S3 backend.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
echo "warning: dev-garage-up.sh is deprecated; use scripts/dev-seaweedfs-up.sh" >&2
exec "$ROOT/scripts/dev-seaweedfs-up.sh" "$@"
