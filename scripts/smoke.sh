#!/usr/bin/env bash
# v0.1 storage exit criterion — end-to-end smoke test.
#
# Sends a known number of DummyRecords through the wire path, runs the
# storage pipeline (WAL → block → object store → online catalog),
# then bootstraps a fresh catalog from the bucket via scry-list and
# asserts:
#
#   * the new catalog's total row count equals exactly the number of
#     records the spewer generated, and
#   * at least one block landed in the bucket.
#
# The dev Garage bucket (`scry-dev`) is emptied at the start of the
# run so the post-condition is unambiguous. Don't point this at any
# bucket whose contents you want to keep.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# ── Parameters ──────────────────────────────────────────────────────
# Deterministic count via --max-batches (rate × duration is off-by-one
# in practice because the spewer trims its last interval tick on
# graceful exit). DURATION_SECS is a safety upper bound only — the
# spewer hits --max-batches first at any sane rate.
BATCHES="${BATCHES:-2000}"
RATE="${RATE:-400}"
DURATION_SECS="${DURATION_SECS:-30}"
RECORDS_PER_BATCH=256              # see crates/noise-spewer/src/gen.rs::render_dummy
EXPECTED_RECORDS=$(( BATCHES * RECORDS_PER_BATCH ))
LISTEN="${LISTEN:-127.0.0.1:4099}"
SMOKE_DIR="${SMOKE_DIR:-/tmp/scry-smoke}"

# ── Pre-flight ──────────────────────────────────────────────────────
if [[ ! -f docker/garage/.env ]]; then
    echo "[smoke] docker/garage/.env missing; run scripts/dev-garage-up.sh first" >&2
    exit 2
fi
# shellcheck disable=SC1091
set -a; source docker/garage/.env; set +a

if ! command -v aws >/dev/null; then
    echo "[smoke] aws CLI not on PATH — needed for bucket reset" >&2
    exit 2
fi

# ── Build ───────────────────────────────────────────────────────────
echo "[smoke] building release binaries..."
cargo build --release -p noise-sink -p noise-spewer -p scry-list >&2

# ── Clean slate ─────────────────────────────────────────────────────
rm -rf "$SMOKE_DIR"
mkdir -p "$SMOKE_DIR"

echo "[smoke] emptying bucket s3://$SCRY_OBJSTORE_BUCKET/ ..."
AWS_ACCESS_KEY_ID="$SCRY_OBJSTORE_ACCESS_KEY_ID" \
AWS_SECRET_ACCESS_KEY="$SCRY_OBJSTORE_SECRET_ACCESS_KEY" \
AWS_REGION="$SCRY_OBJSTORE_REGION" \
    aws --endpoint-url "$SCRY_OBJSTORE_ENDPOINT" \
        s3 rm "s3://$SCRY_OBJSTORE_BUCKET/" --recursive >/dev/null || true

# ── Run the pipeline ────────────────────────────────────────────────
echo "[smoke] starting noise-sink on $LISTEN..."
RUST_LOG="${RUST_LOG:-info}" ./target/release/noise-sink \
    --listen "$LISTEN" \
    --storage \
    --wal-dir "$SMOKE_DIR/wal" \
    --catalog "$SMOKE_DIR/online.sqlite" \
    > "$SMOKE_DIR/sink.log" 2>&1 &
SINK_PID=$!
# Make sure we don't leave a sink running if the script aborts.
trap 'kill -9 $SINK_PID 2>/dev/null || true' EXIT

# Wait for the listener to actually bind. A small poll loop keeps us
# robust against slow startup without leaning on an arbitrary sleep.
for _ in $(seq 1 50); do
    if (echo > "/dev/tcp/${LISTEN%:*}/${LISTEN#*:}") 2>/dev/null; then
        break
    fi
    sleep 0.1
done

echo "[smoke] spewer: $BATCHES batches × $RECORDS_PER_BATCH records = $EXPECTED_RECORDS records expected (rate=$RATE b/s)"
./target/release/noise-spewer \
    --addr "$LISTEN" \
    --signals dummy \
    --rate "$RATE" \
    --duration "${DURATION_SECS}s" \
    --max-batches "$BATCHES" \
    > "$SMOKE_DIR/spewer.log" 2>&1

echo "[smoke] SIGINT noise-sink → graceful flush..."
kill -INT "$SINK_PID"
wait "$SINK_PID" 2>/dev/null || true
trap - EXIT

# ── Verify ──────────────────────────────────────────────────────────
echo "[smoke] reconciling a fresh catalog from the bucket..."
./target/release/scry-list \
    --catalog "$SMOKE_DIR/recon.sqlite" \
    > "$SMOKE_DIR/scry-list.txt" 2>&1
cat "$SMOKE_DIR/scry-list.txt"

# Parse the trailer line produced by scry-list:
#   # 1 block(s) in catalog (bucket=scry-dev)
#   <uuid>  <date>  rows=... bytes=... ts=...  signal=dummy  writer=...
#   # total rows=204800 bytes=9952617
block_count=$(awk '/^# [0-9]+ block\(s\) in catalog/ { print $2; exit }' "$SMOKE_DIR/scry-list.txt")
total_rows=$(awk -F'[= ]' '/^# total rows=/ { print $4; exit }' "$SMOKE_DIR/scry-list.txt")

echo "[smoke] ──── assertions ────"
echo "[smoke] expected records : $EXPECTED_RECORDS"
echo "[smoke] catalog rows     : $total_rows"
echo "[smoke] catalog blocks   : $block_count"

failed=0
if [[ "${total_rows:-}" != "$EXPECTED_RECORDS" ]]; then
    echo "[smoke] FAIL: catalog row count != expected"
    failed=1
fi
if [[ -z "${block_count:-}" || "$block_count" -lt 1 ]]; then
    echo "[smoke] FAIL: catalog reports zero blocks"
    failed=1
fi

if [[ $failed -eq 0 ]]; then
    echo "[smoke] PASS"
    exit 0
else
    echo "[smoke] sink log tail:"
    tail -20 "$SMOKE_DIR/sink.log" || true
    exit 1
fi
