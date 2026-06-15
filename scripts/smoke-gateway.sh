#!/usr/bin/env bash
# scry gateway end-to-end smoke test.
#
# Exercises the foreign-protocol push path:
#
#   curl → scry gateway (OTLP/HTTP + Pyroscope HTTP) → binschema wire
#        → scry ingest (WAL → block → object store → online catalog)
#        → scry list reconcile from bucket → row-count assertions
#
# Sends a known number of OTLP trace exports, Pyroscope profile pushes, and
# Prometheus remote-write requests, then reconciles a fresh catalog from the
# bucket and asserts, per signal:
#
#   * metrics:  catalog rows == METRIC_REQUESTS × SERIES × SAMPLES, ≥1 block
#     *with* a postings sidecar, and a scry get round-trip returns the same
#     row count (ingest → store → query loss-free, the v0.2 exit criterion).
#   * traces:   catalog rows == TRACE_REQUESTS × SPANS_PER_REQ
#   * profiles: catalog rows == PROFILE_REQUESTS
#   * ≥1 block landed for each; traces/profiles carry no postings sidecar
#     (storage-only; same invariant as scripts/smoke.sh).
#
# This proves the translate → forward → store path is loss-free: every span,
# profile, and metric sample the gateway accepted over HTTP lands in the bucket.
#
# Tunables (env): TRACE_REQUESTS (50), SPANS_PER_REQ (4), PROFILE_REQUESTS (30),
# PROFILE_BYTES (4096), METRIC_REQUESTS (20), SERIES (5), SAMPLES (4),
# INGEST_LISTEN (127.0.0.1:4097), GW_LISTEN (127.0.0.1:4319),
# SMOKE_DIR (/tmp/scry-gateway-smoke).
#
# The dev Garage bucket (`scry-dev`) is emptied at the start of the run, so
# don't point this at a bucket whose contents you want to keep.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# ── Parameters ──────────────────────────────────────────────────────
TRACE_REQUESTS="${TRACE_REQUESTS:-50}"
SPANS_PER_REQ="${SPANS_PER_REQ:-4}"
PROFILE_REQUESTS="${PROFILE_REQUESTS:-30}"
PROFILE_BYTES="${PROFILE_BYTES:-4096}"
METRIC_REQUESTS="${METRIC_REQUESTS:-20}"
SERIES="${SERIES:-5}"
SAMPLES="${SAMPLES:-4}"
INGEST_LISTEN="${INGEST_LISTEN:-127.0.0.1:4097}"
GW_LISTEN="${GW_LISTEN:-127.0.0.1:4319}"
SMOKE_DIR="${SMOKE_DIR:-/tmp/scry-gateway-smoke}"

EXPECTED_TRACES=$(( TRACE_REQUESTS * SPANS_PER_REQ ))
EXPECTED_PROFILES=$(( PROFILE_REQUESTS ))
# Each request re-sends the same fixture series with the same timestamps, so
# the catalog accumulates one row per (series × sample) per request.
EXPECTED_METRICS=$(( METRIC_REQUESTS * SERIES * SAMPLES ))

# ── Pre-flight ──────────────────────────────────────────────────────
if [[ ! -f docker/garage/.env ]]; then
    echo "[gw-smoke] docker/garage/.env missing; run scripts/dev-garage-up.sh first" >&2
    exit 2
fi
# shellcheck disable=SC1091
set -a; source docker/garage/.env; set +a

for tool in aws sqlite3 curl; do
    if ! command -v "$tool" >/dev/null; then
        echo "[gw-smoke] $tool CLI not on PATH" >&2
        exit 2
    fi
done

# ── Build ───────────────────────────────────────────────────────────
echo "[gw-smoke] building release binaries..."
cargo build --release -p scry -p scry-gateway >&2

# ── Clean slate ─────────────────────────────────────────────────────
rm -rf "$SMOKE_DIR"
mkdir -p "$SMOKE_DIR"

echo "[gw-smoke] emptying bucket s3://$SCRY_OBJSTORE_BUCKET/ ..."
AWS_ACCESS_KEY_ID="$SCRY_OBJSTORE_ACCESS_KEY_ID" \
AWS_SECRET_ACCESS_KEY="$SCRY_OBJSTORE_SECRET_ACCESS_KEY" \
AWS_REGION="$SCRY_OBJSTORE_REGION" \
    aws --endpoint-url "$SCRY_OBJSTORE_ENDPOINT" \
        s3 rm "s3://$SCRY_OBJSTORE_BUCKET/" --recursive >/dev/null || true

# ── Start scry ingest ──────────────────────────────────────────────
echo "[gw-smoke] starting scry ingest on $INGEST_LISTEN ..."
RUST_LOG="${RUST_LOG:-info}" ./target/release/scry ingest \
    --listen "$INGEST_LISTEN" \
    --storage \
    --wal-dir "$SMOKE_DIR/wal" \
    --catalog "$SMOKE_DIR/online.sqlite" \
    > "$SMOKE_DIR/ingestd.log" 2>&1 &
INGEST_PID=$!

GW_PID=""
cleanup() {
    [[ -n "$GW_PID" ]] && kill -9 "$GW_PID" 2>/dev/null || true
    kill -9 "$INGEST_PID" 2>/dev/null || true
}
trap cleanup EXIT

wait_for_port() {
    local host="${1%:*}" port="${1#*:}"
    for _ in $(seq 1 100); do
        if (echo > "/dev/tcp/$host/$port") 2>/dev/null; then return 0; fi
        sleep 0.1
    done
    return 1
}

if ! wait_for_port "$INGEST_LISTEN"; then
    echo "[gw-smoke] scry ingest did not bind $INGEST_LISTEN" >&2
    tail -20 "$SMOKE_DIR/ingestd.log" >&2 || true
    exit 1
fi

# ── Start scry gateway ──────────────────────────────────────────────
echo "[gw-smoke] starting scry gateway on $GW_LISTEN (upstream $INGEST_LISTEN) ..."
RUST_LOG="${RUST_LOG:-info}" ./target/release/scry gateway \
    --listen "$GW_LISTEN" \
    --upstream "$INGEST_LISTEN" \
    > "$SMOKE_DIR/gateway.log" 2>&1 &
GW_PID=$!

if ! wait_for_port "$GW_LISTEN"; then
    echo "[gw-smoke] scry gateway did not bind $GW_LISTEN" >&2
    tail -20 "$SMOKE_DIR/gateway.log" >&2 || true
    exit 1
fi

# ── Emit fixtures ───────────────────────────────────────────────────
echo "[gw-smoke] emitting fixtures (otlp: $SPANS_PER_REQ spans, pprof: $PROFILE_BYTES bytes) ..."
./target/release/scry-gateway-probe otlp      "$SMOKE_DIR/otlp.bin"  "$SPANS_PER_REQ"      > "$SMOKE_DIR/probe.log"
./target/release/scry-gateway-probe pprof     "$SMOKE_DIR/pprof.bin" "$PROFILE_BYTES"     >> "$SMOKE_DIR/probe.log"
./target/release/scry-gateway-probe promwrite "$SMOKE_DIR/promwrite.bin" "$SERIES" "$SAMPLES" >> "$SMOKE_DIR/probe.log"

# ── Drive traffic ───────────────────────────────────────────────────
GW_URL="http://$GW_LISTEN"

echo "[gw-smoke] POSTing $TRACE_REQUESTS OTLP trace exports → $GW_URL/v1/traces ..."
for _ in $(seq 1 "$TRACE_REQUESTS"); do
    curl -sf -o /dev/null \
        -H 'Content-Type: application/x-protobuf' \
        --data-binary "@$SMOKE_DIR/otlp.bin" \
        "$GW_URL/v1/traces" \
        || { echo "[gw-smoke] FAIL: OTLP POST returned non-2xx" >&2; exit 1; }
done

echo "[gw-smoke] POSTing $PROFILE_REQUESTS Pyroscope profiles → $GW_URL/ingest ..."
for i in $(seq 1 "$PROFILE_REQUESTS"); do
    from=$(( 1700000000 + i ))
    until=$(( from + 10 ))
    curl -sf -o /dev/null \
        -F "profile=@$SMOKE_DIR/pprof.bin" \
        "$GW_URL/ingest?from=$from&until=$until&name=smoke.app%7Benv%3Dprod%7D&spyName=bunspy&sampleRate=100" \
        || { echo "[gw-smoke] FAIL: Pyroscope POST returned non-2xx" >&2; exit 1; }
done

echo "[gw-smoke] POSTing $METRIC_REQUESTS remote-write requests → $GW_URL/api/v1/write ..."
for _ in $(seq 1 "$METRIC_REQUESTS"); do
    curl -sf -o /dev/null \
        -H 'Content-Type: application/x-protobuf' \
        -H 'Content-Encoding: snappy' \
        -H 'X-Prometheus-Remote-Write-Version: 0.1.0' \
        --data-binary "@$SMOKE_DIR/promwrite.bin" \
        "$GW_URL/api/v1/write" \
        || { echo "[gw-smoke] FAIL: remote-write POST returned non-2xx" >&2; exit 1; }
done

# ── Drain + flush ───────────────────────────────────────────────────
# Give the upstream a moment to drain the socket into the WAL/builder
# before we trigger the flush. The gateway returns 200 once the frame is
# written; the server appends asynchronously.
sleep 2
echo "[gw-smoke] stopping gateway, then SIGINT scry ingest → graceful flush ..."
kill "$GW_PID" 2>/dev/null || true
GW_PID=""
sleep 1
kill -INT "$INGEST_PID"
wait "$INGEST_PID" 2>/dev/null || true
trap - EXIT

# ── Verify ──────────────────────────────────────────────────────────
echo "[gw-smoke] reconciling a fresh catalog from the bucket..."
./target/release/scry list \
    --catalog "$SMOKE_DIR/recon.sqlite" \
    > "$SMOKE_DIR/scry-list.txt" 2>&1
cat "$SMOKE_DIR/scry-list.txt"

rows_for() {
    sqlite3 "$SMOKE_DIR/recon.sqlite" \
        "SELECT COALESCE(SUM(row_count),0) FROM blocks WHERE signal='$1';"
}
blocks_for() {
    sqlite3 "$SMOKE_DIR/recon.sqlite" \
        "SELECT COUNT(*) FROM blocks WHERE signal='$1';"
}
postings_for() {
    sqlite3 "$SMOKE_DIR/recon.sqlite" \
        "SELECT COUNT(*) FROM blocks WHERE signal='$1' AND has_postings=1;"
}

metrics_rows=$(rows_for metrics)
metrics_blocks=$(blocks_for metrics)
metrics_postings=$(postings_for metrics)
traces_rows=$(rows_for traces)
profiles_rows=$(rows_for profiles)
traces_blocks=$(blocks_for traces)
profiles_blocks=$(blocks_for profiles)
traces_postings=$(postings_for traces)
profiles_postings=$(postings_for profiles)

echo "[gw-smoke] ──── assertions ────"
echo "[gw-smoke] metrics  : rows=$metrics_rows (expected $EXPECTED_METRICS) blocks=$metrics_blocks postings=$metrics_postings"
echo "[gw-smoke] traces   : rows=$traces_rows (expected $EXPECTED_TRACES) blocks=$traces_blocks postings=$traces_postings"
echo "[gw-smoke] profiles : rows=$profiles_rows (expected $EXPECTED_PROFILES) blocks=$profiles_blocks postings=$profiles_postings"

failed=0
assert_eq() { # label actual expected
    if [[ "$2" != "$3" ]]; then echo "[gw-smoke] FAIL: $1: got $2, expected $3"; failed=1; fi
}
assert_ge1() { # label count
    if [[ "${2:-0}" -lt 1 ]]; then echo "[gw-smoke] FAIL: $1: no blocks landed"; failed=1; fi
}
assert_zero() { # label count
    if [[ "${2:-0}" -ne 0 ]]; then echo "[gw-smoke] FAIL: $1: expected no postings, got $2"; failed=1; fi
}

assert_eq  "metrics rows == accepted samples"   "$metrics_rows"   "$EXPECTED_METRICS"
assert_eq  "traces rows == accepted spans"      "$traces_rows"    "$EXPECTED_TRACES"
assert_eq  "profiles rows == accepted profiles" "$profiles_rows"  "$EXPECTED_PROFILES"
assert_ge1 "metrics blocks landed"              "$metrics_blocks"
assert_ge1 "traces blocks landed"               "$traces_blocks"
assert_ge1 "profiles blocks landed"             "$profiles_blocks"
assert_ge1 "metrics postings present"           "$metrics_postings"
assert_zero "traces postings absent"            "$traces_postings"
assert_zero "profiles postings absent"          "$profiles_postings"

# Query round-trip for metrics (v0.2 exit criterion): the reconciled catalog
# must query back exactly the rows the bucket holds — proving ingest → store →
# query is loss-free, not just that bytes landed.
./target/release/scry get \
    --catalog "$SMOKE_DIR/recon.sqlite" \
    --signal metrics \
    > "$SMOKE_DIR/query.metrics.txt" 2>&1 || true
metrics_queried=$(awk '/^# scan:/ { print $3; exit }' "$SMOKE_DIR/query.metrics.txt")
echo "[gw-smoke] metrics queried rows : ${metrics_queried:-<none>} (expected $EXPECTED_METRICS)"
if [[ "${metrics_queried:-}" != "$EXPECTED_METRICS" ]]; then
    echo "[gw-smoke] FAIL: metrics query returned ${metrics_queried:-<none>}, expected $EXPECTED_METRICS"
    cat "$SMOKE_DIR/query.metrics.txt"
    failed=1
fi

if [[ $failed -eq 0 ]]; then
    echo "[gw-smoke] PASS"
    exit 0
else
    echo "[gw-smoke] ingestd log tail:"; tail -20 "$SMOKE_DIR/ingestd.log" || true
    echo "[gw-smoke] gateway log tail:"; tail -20 "$SMOKE_DIR/gateway.log" || true
    exit 1
fi
