#!/usr/bin/env bash
# Deep-symbol profiling harness for scry-query.
#
# Differs from profile-query.sh in two ways:
#
#   1. Uses DWARF call-graph unwinding (`--call-graph dwarf,16384`).
#      Frame-pointer unwinds get truncated as soon as the optimizer
#      drops a frame pointer, which is exactly what produced our
#      ~38% [unknown] / [scry-query] mystery slice in the default
#      flamegraph. DWARF walks the debuginfo so we get real symbols
#      all the way through inlined hot paths.
#
#   2. Runs `perf record` as root. perf_event_paranoid=2 lets
#      userspace task-clock samples through unprivileged, but DWARF
#      unwind needs the kernel to copy a large stack snapshot per
#      sample — that requires CAP_PERFMON (root in practice).
#
# Output (all chowned back to the invoking user before exit):
#
#   flamegraphs/<stamp>-<label>.deep.perf.data
#   flamegraphs/<stamp>-<label>.deep.folded     (collapsed stacks, parseable)
#   flamegraphs/<stamp>-<label>.deep.svg        (flamegraph)
#   flamegraphs/<stamp>-<label>.deep.script.txt (perf script raw, for grepping)
#   flamegraphs/<stamp>-<label>.deep.top.txt    (perf report --stdio top, sanity)
#
# Usage (run via sudo, args go AFTER `--`):
#
#   sudo -E ./scripts/profile-query-deep.sh selective -- \
#       --catalog /tmp/scry-smoke/online.sqlite \
#       --matcher __name__=scry_http_requests_total
#
# Prerequisites:
#   * cargo + the `profiling` profile (already in workspace Cargo.toml)
#   * inferno-collapse-perf + inferno-flamegraph on $PATH for the
#     invoking user (you have both at ~/.cargo/bin)
#   * /usr/bin/perf
#
# The build step runs BEFORE we become root, so cargo writes to your
# CARGO_HOME and reuses the warm artifact cache. Only `perf record`
# and post-processing run as root.

set -euo pipefail

# ── Figure out who's invoking us (so we can chown back) ─────────────
if [[ -z "${SUDO_USER:-}" ]]; then
    echo "error: run via sudo (need root for DWARF call-graph perf record)" >&2
    echo "       sudo -E $0 <label> -- <scry-query args>" >&2
    exit 64
fi
REAL_USER="$SUDO_USER"
REAL_HOME="$(getent passwd "$REAL_USER" | cut -d: -f6)"

# ── Args ────────────────────────────────────────────────────────────
if [[ $# -lt 2 ]]; then
    sed -n '/^# Usage/,/^# Prerequisites/p' "$0" | sed 's/^# \{0,1\}//'
    exit 64
fi
LABEL="$1"; shift
if [[ "${1:-}" != "--" ]]; then
    echo "error: arguments after <label> must be preceded by '--'" >&2
    exit 64
fi
shift
if ! [[ "$LABEL" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "error: label '$LABEL' must match [A-Za-z0-9._-]+" >&2
    exit 64
fi

# ── Paths ───────────────────────────────────────────────────────────
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

OUT_DIR="${OUT_DIR:-$ROOT/flamegraphs}"
mkdir -p "$OUT_DIR"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BASE="$OUT_DIR/${STAMP}-${LABEL}.deep"
PERF_DATA="$BASE.perf.data"
FOLDED="$BASE.folded"
SVG="$BASE.svg"
SCRIPT_TXT="$BASE.script.txt"
TOP_TXT="$BASE.top.txt"

# ── Bucket env (load as REAL_USER would have) ───────────────────────
ENV_FILE="${ENV_FILE:-$ROOT/docker/garage/.env}"
if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
fi

# ── Build as the invoking user (NOT root) ───────────────────────────
#
# Use absolute paths to cargo/inferno binaries — `sudo -u … bash -c`
# runs non-login non-interactive, so $REAL_USER's .profile/.bashrc
# never sources, and ~/.cargo/bin isn't on PATH. Resolve cargo from
# the standard rustup install location.
CARGO_BIN="$REAL_HOME/.cargo/bin/cargo"
if [[ ! -x "$CARGO_BIN" ]]; then
    echo "error: $CARGO_BIN not executable; adjust CARGO_BIN env if rustup lives elsewhere" >&2
    exit 1
fi

echo "==> building scry (get) [profile=profiling] as $REAL_USER"
sudo -u "$REAL_USER" -H bash -c "cd '$ROOT' && '$CARGO_BIN' build --profile profiling -p scry"

BIN="$ROOT/target/profiling/scry"
if [[ ! -x "$BIN" ]]; then
    echo "error: $BIN not built" >&2
    exit 1
fi

# ── perf record with DWARF unwind ───────────────────────────────────
#
# --call-graph dwarf,16384 : copy 16 KB of stack per sample for the
#   user-space DWARF unwinder. Default 8 KB truncates the deep async
#   futures stacks DataFusion produces; 16 KB is usually enough.
# -F 49999 : ~50k samples/sec. Higher rates lose chunks in our prior
#   default-path runs even at default 99971. Drop to 50k for DWARF
#   record overhead.
# --aio : use async I/O writing perf.data (less sample loss).
# -g : enable call-graphs (redundant with --call-graph but explicit).
FREQ="${FREQ:-49999}"
STACK_SIZE="${STACK_SIZE:-16384}"

echo "==> profiling: $BIN get $*"
echo "==> output:    $BASE.*"
perf record \
    -F "$FREQ" \
    --call-graph "dwarf,$STACK_SIZE" \
    --aio \
    -g \
    -o "$PERF_DATA" \
    -- "$BIN" get "$@" \
    >/dev/null

# ── Symbolize + collapse + flamegraph ───────────────────────────────
#
# `perf script` resolves addresses → symbols using the binary's
# debuginfo. With --call-graph dwarf, this is where the inline-frame
# expansion happens. The folded format is what inferno-flamegraph
# wants and is also what I (Claude) can grep over for hot paths.
echo "==> perf script → folded stacks"
perf script -i "$PERF_DATA" --no-inline > "$SCRIPT_TXT"

# inferno lives in REAL_USER's cargo bin. Call it as them so we
# don't accidentally rely on root's PATH.
sudo -u "$REAL_USER" -H bash -c \
    "'$REAL_HOME/.cargo/bin/inferno-collapse-perf' < '$SCRIPT_TXT' > '$FOLDED'"

echo "==> flamegraph SVG"
sudo -u "$REAL_USER" -H bash -c \
    "'$REAL_HOME/.cargo/bin/inferno-flamegraph' < '$FOLDED' > '$SVG'"

echo "==> perf report top frames (text)"
perf report -i "$PERF_DATA" --stdio --no-children --sort=overhead,symbol \
    | head -200 > "$TOP_TXT" || true

# ── Hand outputs back to the user ───────────────────────────────────
chown "$REAL_USER:$REAL_USER" \
    "$PERF_DATA" "$FOLDED" "$SVG" "$SCRIPT_TXT" "$TOP_TXT"

echo
echo "wrote:"
echo "  $PERF_DATA    ($(du -h "$PERF_DATA" | cut -f1))"
echo "  $FOLDED       ($(wc -l < "$FOLDED") collapsed stacks)"
echo "  $SVG          (open in browser)"
echo "  $SCRIPT_TXT   ($(du -h "$SCRIPT_TXT" | cut -f1))"
echo "  $TOP_TXT      (perf report top, $(wc -l < "$TOP_TXT") lines)"
echo
echo "tell Claude to read: $FOLDED  and  $TOP_TXT"
