#!/usr/bin/env bash
# Reusable profiling harness for scry-query.
#
# Builds scry-query under the `profiling` cargo profile (release-mode
# optimizations + full debug info, defined in workspace Cargo.toml),
# then records a flamegraph via cargo-flamegraph + perf. The SVG lands
# in `flamegraphs/<timestamp>-<label>.svg` so successive runs are
# directly comparable side-by-side.
#
# Usage:
#
#     scripts/profile-query.sh <label> -- <scry-query args>
#
# Examples:
#
#     # Selective single-metric scan against the smoke bucket.
#     scripts/profile-query.sh selective -- \
#         --catalog /tmp/scry-smoke/online.sqlite \
#         --matcher __name__=scry_http_requests_total
#
#     # AND'd predicate, lower-cardinality result.
#     scripts/profile-query.sh and-predicate -- \
#         --catalog /tmp/scry-smoke/online.sqlite \
#         --matcher __name__=scry_http_requests_total \
#         --matcher env=prod
#
# Prerequisites:
#   * cargo-flamegraph installed (`cargo install flamegraph`)
#   * /usr/bin/perf (linux-tools-<kver>)
#   * `perf_event_paranoid <= 2` is enough on this box (userspace
#     samples via task-clock work unprivileged at paranoid=2). If perf
#     refuses sampling here on a different host, lower it once:
#
#         sudo sysctl -w kernel.perf_event_paranoid=1
#
#     or pass `--root` to cargo-flamegraph (requires sudo per run).
#
# The bucket env (SCRY_OBJSTORE_*) is loaded from docker/garage/.env
# the same way smoke.sh does it, so the binary can reach Garage.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ $# -lt 2 ]]; then
    sed -n '/^# Usage:/,/^# Prerequisites:/p' "$0" | sed 's/^# \{0,1\}//'
    exit 64
fi

LABEL="$1"
shift
if [[ "${1:-}" != "--" ]]; then
    echo "error: arguments after <label> must be preceded by '--'" >&2
    exit 64
fi
shift

# Validate label early — it ends up in a filename.
if ! [[ "$LABEL" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "error: label '$LABEL' must match [A-Za-z0-9._-]+" >&2
    exit 64
fi

OUT_DIR="${OUT_DIR:-$ROOT/flamegraphs}"
mkdir -p "$OUT_DIR"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$OUT_DIR/${STAMP}-${LABEL}.svg"

# ── Bucket env ──────────────────────────────────────────────────────
ENV_FILE="${ENV_FILE:-$ROOT/docker/garage/.env}"
if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
else
    echo "warning: $ENV_FILE not found; scry-query will fail unless SCRY_OBJSTORE_* is exported by the caller" >&2
fi

# ── Build (separate step so build time isn't profiled) ──────────────
echo "==> building scry-query [profile=profiling]"
cargo build --profile profiling -p scry-query

BIN="$ROOT/target/profiling/scry-query"
if [[ ! -x "$BIN" ]]; then
    echo "error: $BIN not built; cargo output above" >&2
    exit 1
fi

# ── Record flamegraph ───────────────────────────────────────────────
#
# Sampling frequency — prime number to avoid beating with common
# periodic events on the host. 99971 Hz (~100k samples/sec) is the
# default; pushing it to ~1MHz on sub-second queries loses ~20% of
# samples to perf's buffer overflowing. Tweak via FREQ=N for longer
# or coarser runs.
#
# Drop `--root` here: paranoid=2 on this box lets userspace samples
# through unprivileged. If you hit "permission denied" on another
# host, either lower the sysctl or add --root back.
FREQ="${FREQ:-99971}"

echo "==> profiling: $BIN $*"
echo "==> output:   $OUT"
cargo flamegraph \
    --profile profiling \
    --bin scry-query \
    --output "$OUT" \
    --freq "$FREQ" \
    -- "$@"

echo
echo "flamegraph written: $OUT"
echo "open with: xdg-open $OUT   (or just drop the SVG into a browser)"
