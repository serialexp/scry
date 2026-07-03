#!/usr/bin/env bash
#
# release.sh — cut a scry release, keeping crate versions and git tags in sync.
#
# Single source of truth for the version is `[workspace.package].version` in the
# root Cargo.toml; every first-party crate inherits it via
# `version.workspace = true` (the vendored binschema-runtime is the lone
# exception — it tracks upstream binschema, not scry's cadence). This script
# bumps that one field, commits it, and creates the matching annotated `vX.Y.Z`
# tag, so the crates a build reports (CARGO_PKG_VERSION lands in block
# `producer_version` and the wire `agent_version`) always match the tag.
#
# Usage:
#   scripts/release.sh <version> [-m "<tag message>"]
#   scripts/release.sh 0.13.0
#   scripts/release.sh 0.13.0 -m "v0.13.0 — own UI, Grafana adapters"
#
# It does NOT push. Review, then:
#   git push origin main && git push origin v<version>
#
set -euo pipefail

die() { echo "release: $*" >&2; exit 1; }

VERSION="${1:-}"
[ -n "$VERSION" ] || die "usage: scripts/release.sh <version> [-m message]"
shift

# Accept a -m message; default to a conventional release line.
MSG=""
while [ $# -gt 0 ]; do
  case "$1" in
    -m) MSG="${2:-}"; shift 2 ;;
    *) die "unknown argument: $1" ;;
  esac
done
[ -n "$MSG" ] || MSG="v${VERSION}"

# Validate semver-ish X.Y.Z (optional pre-release / build suffix).
echo "$VERSION" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z.-]+)?$' \
  || die "version '$VERSION' is not X.Y.Z"

cd "$(dirname "$0")/.."
ROOT_TOML="Cargo.toml"
TAG="v${VERSION}"

# Refuse to clobber an existing tag.
git rev-parse -q --verify "refs/tags/${TAG}" >/dev/null 2>&1 \
  && die "tag ${TAG} already exists"

# Working tree must be clean so the release commit is exactly the version bump.
[ -z "$(git status --porcelain)" ] || die "working tree not clean; commit or stash first"

# Rewrite the single [workspace.package].version line, in-place, idempotently.
python3 - "$ROOT_TOML" "$VERSION" <<'PY'
import re, sys
path, version = sys.argv[1], sys.argv[2]
lines = open(path).read().splitlines(keepends=True)
in_wp = False
done = False
for i, l in enumerate(lines):
    s = l.strip()
    if s == "[workspace.package]":
        in_wp = True
        continue
    if in_wp and s.startswith("[") and s != "[workspace.package]":
        break
    if in_wp and re.match(r'version\s*=\s*"', s):
        lines[i] = re.sub(r'(version\s*=\s*")[^"]*(")', r'\g<1>' + version + r'\g<2>', l)
        done = True
        break
if not done:
    sys.exit("release: could not find [workspace.package].version in " + path)
open(path, "w").write("".join(lines))
PY

# Refresh Cargo.lock for the new version (cheap, no network).
cargo update --workspace --offline >/dev/null 2>&1 || cargo update --workspace >/dev/null 2>&1 || true

# Align the frontend version (tauri.conf.json + package.json) with the workspace
# version, so the release commit carries the UI bump too and the committed tree
# stays in lockstep (CI's build-time stamp is then always a no-op). Idempotent.
FRONTEND_JSON=(desktop/src-tauri/tauri.conf.json desktop/package.json)
bun scripts/stamp-version.mjs

echo "release: set workspace version -> ${VERSION}"
git --no-pager diff -- "$ROOT_TOML" Cargo.lock "${FRONTEND_JSON[@]}" | sed -n '1,60p'

git add "$ROOT_TOML" Cargo.lock "${FRONTEND_JSON[@]}"
git commit -m "chore(release): ${TAG}"
git tag -a "$TAG" -m "$MSG"

cat <<EOF

release: committed and tagged ${TAG}.
  push with:  git push origin main && git push origin ${TAG}
EOF
