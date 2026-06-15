#!/usr/bin/env bash
# v0.9 multi-instance exit criterion — two scry ingest instances sharing one
# bucket + one Valkey, end to end.
#
# Also reachable as `MULTI=1 scripts/smoke.sh` (that script execs this one).
#
# Proves the three things that make N instances safe on one bucket:
#
#   1. Convergence — ingest logs to BOTH instances under distinct writer
#      prefixes, then assert each instance's *own* online catalog converges to
#      the union of both instances' rows (Valkey pub/sub + cursor poll). The
#      bucket is the truth; each catalog is derived and must agree.
#
#   2. Single-winner compaction — both instances run compaction over the same
#      `(signal, date, level)` partition and contend for its Valkey lease.
#      Exactly one may commit a merge, so the live row count must stay EQUAL to
#      the ingested total (a double-merge would leave two distinct merged
#      blocks — UUID-addressed, not content-addressed — and double-count). We
#      assert no duplication AND that compaction actually happened (≥1 block at
#      level ≥ 1). Grace is 0 so a stale peer's re-merge attempt 404s on the
#      already-deleted inputs and aborts cleanly instead of committing a dup.
#
#   3. Coordinated retention — both instances run retention with a tiny TTL and
#      contend for the single global retention lease. Reaping is idempotent and
#      NotFound-tolerant, so the end state is deterministic: every block gone,
#      no errors in either log.
#
# Single-instance correctness is unaffected: this is a separate harness; the
# existing `scripts/smoke.sh` SIGNAL matrix still runs one daemon, no Valkey.
#
# Prereqs: `scripts/dev-garage-up.sh` and `scripts/dev-valkey-up.sh` (or point
# SCRY_VALKEY_URL at any reachable Valkey/Redis). The dev Garage bucket is
# emptied on every run — don't point this at a bucket you care about.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# ── Parameters ──────────────────────────────────────────────────────
LISTEN_A="${LISTEN_A:-127.0.0.1:4000}"
LISTEN_B="${LISTEN_B:-127.0.0.1:4001}"
SMOKE_DIR="${SMOKE_DIR:-/tmp/scry-smoke-multi}"
VALKEY_URL="${SCRY_VALKEY_URL:-redis://127.0.0.1:6379}"
# Logs is the richest signal (postings + body bloom + compaction + retention),
# so we exercise the multi-instance path against it. 60 records/batch
# (3 streams × 20 entries) per crates/noise-spewer/src/gen.rs::render_logs.
RECORDS_PER_BATCH=60
BATCHES_A="${BATCHES_A:-600}"
BATCHES_B="${BATCHES_B:-600}"
RATE="${RATE:-3000}"
# Small blocks so each instance seals several L0 blocks → the shared
# (logs, date, 0) partition has ≥ fanout blocks and compaction has work to do.
BLOCK_MAX_ROWS="${BLOCK_MAX_ROWS:-12000}"
COMPACT_FANOUT="${COMPACT_FANOUT:-2}"

EXPECTED_ROWS=$(( (BATCHES_A + BATCHES_B) * RECORDS_PER_BATCH ))

# ── Pre-flight ──────────────────────────────────────────────────────
if [[ ! -f docker/garage/.env ]]; then
    echo "[multi] docker/garage/.env missing; run scripts/dev-garage-up.sh first" >&2
    exit 2
fi
# shellcheck disable=SC1091
set -a; source docker/garage/.env; set +a

for c in aws sqlite3; do
    command -v "$c" >/dev/null || { echo "[multi] $c CLI not on PATH" >&2; exit 2; }
done

# Valkey must be reachable — this harness has nothing to coordinate without it.
if command -v valkey-cli >/dev/null; then VK=valkey-cli
elif command -v redis-cli >/dev/null; then VK=redis-cli
else VK=""; fi
if [[ -n "$VK" ]]; then
    if ! "$VK" -u "$VALKEY_URL" ping 2>/dev/null | grep -q PONG; then
        echo "[multi] Valkey at $VALKEY_URL not answering PING; run scripts/dev-valkey-up.sh (or set SCRY_VALKEY_URL)" >&2
        exit 2
    fi
    # Clear any leftover lease/cursor keys from a previous run so a stale lease
    # can't stall this run's compaction. (Bucket is truth; Valkey is ephemeral.)
    "$VK" -u "$VALKEY_URL" flushall >/dev/null 2>&1 || true
else
    echo "[multi] no valkey-cli/redis-cli on PATH to verify $VALKEY_URL; proceeding (connect failures will surface in daemon logs)" >&2
fi

# ── Build ───────────────────────────────────────────────────────────
echo "[multi] building release binaries..."
cargo build --release -p scry -p noise-spewer >&2

# ── Clean slate ─────────────────────────────────────────────────────
rm -rf "$SMOKE_DIR"
mkdir -p "$SMOKE_DIR"

echo "[multi] emptying bucket s3://$SCRY_OBJSTORE_BUCKET/ ..."
AWS_ACCESS_KEY_ID="$SCRY_OBJSTORE_ACCESS_KEY_ID" \
AWS_SECRET_ACCESS_KEY="$SCRY_OBJSTORE_SECRET_ACCESS_KEY" \
AWS_REGION="$SCRY_OBJSTORE_REGION" \
    aws --endpoint-url "$SCRY_OBJSTORE_ENDPOINT" \
        s3 rm "s3://$SCRY_OBJSTORE_BUCKET/" --recursive >/dev/null || true

# ── Helpers ─────────────────────────────────────────────────────────
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill -9 "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

# Start a scry ingest; echoes its PID. Extra flags after the 4 positional.
start_ingestd() {
    local name=$1 listen=$2 waldir=$3 catalog=$4; shift 4
    RUST_LOG="${RUST_LOG:-info,scry_compact=debug}" \
        ./target/release/scry ingest \
            --listen "$listen" \
            --storage \
            --wal-dir "$waldir" \
            --catalog "$catalog" \
            --valkey-url "$VALKEY_URL" \
            --block-max-rows "$BLOCK_MAX_ROWS" \
            "$@" \
        > "$SMOKE_DIR/$name.log" 2>&1 &
    echo $!
}

wait_bind() {
    local addr=$1
    for _ in $(seq 1 100); do
        (echo > "/dev/tcp/${addr%:*}/${addr#*:}") 2>/dev/null && return 0
        sleep 0.1
    done
    echo "[multi] $addr never bound" >&2; return 1
}

stop_daemon() { # graceful flush then reap
    local pid=$1
    kill -INT "$pid" 2>/dev/null || true
    for _ in $(seq 1 100); do [[ -d /proc/$pid ]] || return 0; sleep 0.1; done
    kill -9 "$pid" 2>/dev/null || true
}

# Live (non-superseded, non-deleted) block count + summed rows for a signal,
# read straight from a daemon's own SQLite catalog.
live_count() { sqlite3 "$1" "SELECT count(*) FROM blocks WHERE deleted_at IS NULL AND superseded_by IS NULL AND signal='$2';"; }
live_rows()  { sqlite3 "$1" "SELECT COALESCE(sum(row_count),0) FROM blocks WHERE deleted_at IS NULL AND superseded_by IS NULL AND signal='$2';"; }
max_level()  { sqlite3 "$1" "SELECT COALESCE(max(level),0) FROM blocks WHERE deleted_at IS NULL AND superseded_by IS NULL AND signal='$2';"; }
distinct_writers() { sqlite3 "$1" "SELECT count(DISTINCT writer_id) FROM blocks WHERE deleted_at IS NULL AND superseded_by IS NULL AND signal='$2';"; }

fail() { echo "[multi] FAIL: $*" >&2; echo "---- A.log (tail) ----" >&2; tail -30 "$SMOKE_DIR/A.log" >&2; echo "---- B.log (tail) ----" >&2; tail -30 "$SMOKE_DIR/B.log" >&2; exit 1; }

# ════════════════════════════════════════════════════════════════════
# Phase 1 — convergence + single-winner compaction
# ════════════════════════════════════════════════════════════════════
echo "[multi] ── phase 1: convergence + single-winner compaction ──"
PA=$(start_ingestd A "$LISTEN_A" "$SMOKE_DIR/walA" "$SMOKE_DIR/catA.sqlite" \
        --mode full --poll-interval 1 --full-walk-interval 5 \
        --compact-interval 2 --compact-grace 0 --compact-fanout "$COMPACT_FANOUT")
PIDS+=("$PA")
PB=$(start_ingestd B "$LISTEN_B" "$SMOKE_DIR/walB" "$SMOKE_DIR/catB.sqlite" \
        --mode full --poll-interval 1 --full-walk-interval 5 \
        --compact-interval 2 --compact-grace 0 --compact-fanout "$COMPACT_FANOUT")
PIDS+=("$PB")
wait_bind "$LISTEN_A"; wait_bind "$LISTEN_B"

echo "[multi] spewing $BATCHES_A batches → A, $BATCHES_B batches → B (logs)..."
./target/release/noise-spewer --addr "$LISTEN_A" --signals logs \
    --rate "$RATE" --max-batches "$BATCHES_A" > "$SMOKE_DIR/spewA.log" 2>&1 &
SPA=$!
./target/release/noise-spewer --addr "$LISTEN_B" --signals logs \
    --rate "$RATE" --max-batches "$BATCHES_B" > "$SMOKE_DIR/spewB.log" 2>&1 &
SPB=$!
wait "$SPA"; wait "$SPB"
echo "[multi] spew done; expected union rows = $EXPECTED_ROWS"

# Wait for BOTH catalogs to converge to the full union (and compaction to stop
# changing the row total). Total rows is invariant under compaction, so it is
# the stable convergence/no-duplication signal; block count is not.
echo "[multi] waiting for catalog convergence (pub/sub + poll)..."
converged=0
for _ in $(seq 1 80); do  # up to ~40s
    ra=$(live_rows "$SMOKE_DIR/catA.sqlite" logs)
    rb=$(live_rows "$SMOKE_DIR/catB.sqlite" logs)
    if [[ "$ra" == "$EXPECTED_ROWS" && "$rb" == "$EXPECTED_ROWS" ]]; then
        converged=1; break
    fi
    sleep 0.5
done
ra=$(live_rows "$SMOKE_DIR/catA.sqlite" logs)
rb=$(live_rows "$SMOKE_DIR/catB.sqlite" logs)
echo "[multi] catA rows=$ra  catB rows=$rb  (expected $EXPECTED_ROWS)"
[[ "$converged" == 1 ]] || fail "catalogs did not converge to $EXPECTED_ROWS rows (A=$ra B=$rb) — convergence or duplicate-merge bug"

# Convergence proof: each catalog sees BOTH writer prefixes (its own + peer's).
wa=$(distinct_writers "$SMOKE_DIR/catA.sqlite" logs)
wb=$(distinct_writers "$SMOKE_DIR/catB.sqlite" logs)
echo "[multi] distinct writers seen: A=$wa B=$wb (expect ≥2 each before full compaction)"

# Give compaction a beat to run, then assert it happened and still no dup.
sleep 4
la=$(max_level "$SMOKE_DIR/catA.sqlite" logs)
lb=$(max_level "$SMOKE_DIR/catB.sqlite" logs)
ca=$(live_count "$SMOKE_DIR/catA.sqlite" logs)
ra=$(live_rows "$SMOKE_DIR/catA.sqlite" logs)
rb=$(live_rows "$SMOKE_DIR/catB.sqlite" logs)
echo "[multi] post-compaction: A blocks=$ca rows=$ra maxlevel=$la | B rows=$rb maxlevel=$lb"
[[ "$ra" == "$EXPECTED_ROWS" && "$rb" == "$EXPECTED_ROWS" ]] \
    || fail "row total changed under compaction (A=$ra B=$rb, expected $EXPECTED_ROWS) — duplicate merge (lease did not enforce single-winner)"
if [[ "$la" -lt 1 && "$lb" -lt 1 ]]; then
    fail "no block reached level ≥ 1 — compaction never ran, single-winner assertion would be vacuous"
fi
echo "[multi] ✓ convergence + single-winner compaction"

stop_daemon "$PA"; stop_daemon "$PB"

# ════════════════════════════════════════════════════════════════════
# Phase 2 — coordinated retention
# ════════════════════════════════════════════════════════════════════
# Fresh catalogs so both instances must (re)discover the bucket's blocks via a
# fast full-walk, then reap them under the single global retention lease. TTL
# 1s with wall-clock-stamped data (minutes old by now) ⇒ everything expires.
echo "[multi] ── phase 2: coordinated retention ──"
PA=$(start_ingestd Aret "$LISTEN_A" "$SMOKE_DIR/walA" "$SMOKE_DIR/catAret.sqlite" \
        --mode full --poll-interval 1 --full-walk-interval 2 \
        --compact-interval 3600 \
        --ttl-logs 1s --retention-apply --retention-grace 0 --retention-interval 2)
PIDS+=("$PA")
PB=$(start_ingestd Bret "$LISTEN_B" "$SMOKE_DIR/walB" "$SMOKE_DIR/catBret.sqlite" \
        --mode full --poll-interval 1 --full-walk-interval 2 \
        --compact-interval 3600 \
        --ttl-logs 1s --retention-apply --retention-grace 0 --retention-interval 2)
PIDS+=("$PB")
wait_bind "$LISTEN_A"; wait_bind "$LISTEN_B"

echo "[multi] waiting for coordinated reaping (full-walk discover → lease → reap)..."
reaped=0
for _ in $(seq 1 80); do  # up to ~40s
    ra=$(live_count "$SMOKE_DIR/catAret.sqlite" logs)
    rb=$(live_count "$SMOKE_DIR/catBret.sqlite" logs)
    # Both must have discovered (>0 at some point) then dropped to 0.
    if [[ "$ra" == 0 && "$rb" == 0 ]]; then reaped=1; break; fi
    sleep 0.5
done
ra=$(live_count "$SMOKE_DIR/catAret.sqlite" logs)
rb=$(live_count "$SMOKE_DIR/catBret.sqlite" logs)
echo "[multi] after retention: A live blocks=$ra  B live blocks=$rb (expected 0/0)"
[[ "$reaped" == 1 ]] || fail "retention did not converge to 0 live blocks (A=$ra B=$rb)"

# Bucket truth: a fresh reconcile finds no logs blocks left.
./target/release/scry list --catalog "$SMOKE_DIR/recon.sqlite" > "$SMOKE_DIR/recon.txt" 2>&1
left=$(sqlite3 "$SMOKE_DIR/recon.sqlite" "SELECT count(*) FROM blocks WHERE signal='logs' AND deleted_at IS NULL;")
echo "[multi] bucket reconcile: logs blocks remaining = $left (expected 0)"
[[ "$left" == 0 ]] || fail "bucket still has $left logs blocks after coordinated retention"

# No panics and no maintenance-pass failures in either daemon log. (We match
# the real failure signals — a panic, or the maintenance loop's "compaction/
# retention pass failed" — not the lowercase `error=` field, which appears in
# benign WARN lines like the wait_bind TCP probe's "no frame before handshake".)
if grep -iEq "panicked|pass failed" "$SMOKE_DIR/Aret.log" "$SMOKE_DIR/Bret.log"; then
    fail "panic or maintenance-pass failure in a retention-phase daemon log (see $SMOKE_DIR/{Aret,Bret}.log)"
fi
echo "[multi] ✓ coordinated retention"

stop_daemon "$PA"; stop_daemon "$PB"
trap - EXIT
cleanup

echo "[multi] PASS"
