#!/usr/bin/env bash
# scry-agent metrics-scraping exit criterion — end-to-end.
#
# Proves the agent's Prometheus pull path lands queryable metrics in the
# bucket, against a real scry-ingestd + Garage:
#
#   stub /metrics  →  scry-agent (--scrape-target)  →  scry-ingestd (--storage)
#                  →  bucket  →  scry-list reconcile  →  scry-query
#
# A python stub serves a fixed exposition body with 3 explicit samples
# (2 counter series + 1 gauge). The agent scrapes it exactly once (the
# scrape interval is set far longer than the run, and the first tick
# fires immediately), synthesizing `up` + `scrape_duration_seconds`, so a
# single scrape yields a deterministic 5 series / 5 samples. We then assert:
#
#   * the reconciled catalog holds exactly 5 metric rows (3 exposed + the
#     2 synthesized — so a non-5 count means synthesis or mapping broke),
#   * ≥1 metrics block landed with a postings sidecar, and
#   * scry-query --signal metrics scans those 5 rows back (ingest→store→
#     query is loss-free for scraped metrics).
#
# Self-contained except for Garage (needs docker/garage/.env) and python3.
# The dev bucket is emptied at the start of the run.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

LISTEN="${LISTEN:-127.0.0.1:4097}"
STUB_PORT="${STUB_PORT:-19100}"
SMOKE_DIR="${SMOKE_DIR:-/tmp/scry-agent-metrics}"
EXPECTED_ROWS=5   # 2 counter + 1 gauge exposed + up + scrape_duration_seconds

# ── Pre-flight ──────────────────────────────────────────────────────
if [[ ! -f docker/garage/.env ]]; then
    echo "[agent-metrics] docker/garage/.env missing; run scripts/dev-garage-up.sh first" >&2
    exit 2
fi
# shellcheck disable=SC1091
set -a; source docker/garage/.env; set +a

for tool in aws sqlite3 python3; do
    command -v "$tool" >/dev/null || { echo "[agent-metrics] $tool not on PATH" >&2; exit 2; }
done

# ── Build ───────────────────────────────────────────────────────────
echo "[agent-metrics] building release binaries..."
cargo build --release -p scry-ingestd -p scry-list -p scry-query -p scry-agent >&2

# ── Clean slate ─────────────────────────────────────────────────────
rm -rf "$SMOKE_DIR"; mkdir -p "$SMOKE_DIR"
echo "[agent-metrics] emptying bucket s3://$SCRY_OBJSTORE_BUCKET/ ..."
AWS_ACCESS_KEY_ID="$SCRY_OBJSTORE_ACCESS_KEY_ID" \
AWS_SECRET_ACCESS_KEY="$SCRY_OBJSTORE_SECRET_ACCESS_KEY" \
AWS_REGION="$SCRY_OBJSTORE_REGION" \
    aws --endpoint-url "$SCRY_OBJSTORE_ENDPOINT" \
        s3 rm "s3://$SCRY_OBJSTORE_BUCKET/" --recursive >/dev/null || true

# ── Stub /metrics endpoint ──────────────────────────────────────────
cat > "$SMOKE_DIR/metrics.txt" <<'EOF'
# HELP test_requests_total Total requests served
# TYPE test_requests_total counter
test_requests_total{method="get"} 42
test_requests_total{method="post"} 7
# HELP test_temperature_celsius Current temperature
# TYPE test_temperature_celsius gauge
test_temperature_celsius 21.5
EOF

python3 - "$STUB_PORT" "$SMOKE_DIR/metrics.txt" > "$SMOKE_DIR/stub.log" 2>&1 <<'PY' &
import sys, http.server, socketserver
port = int(sys.argv[1]); body = open(sys.argv[2], 'rb').read()
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain; version=0.0.4')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers(); self.wfile.write(body)
    def log_message(self, *a): pass
with socketserver.TCPServer(('127.0.0.1', port), H) as s:
    s.serve_forever()
PY
STUB_PID=$!

# ── scry-ingestd (storage) ──────────────────────────────────────────
echo "[agent-metrics] starting scry-ingestd on $LISTEN..."
RUST_LOG="${RUST_LOG:-info}" ./target/release/scry-ingestd \
    --listen "$LISTEN" \
    --storage \
    --wal-dir "$SMOKE_DIR/wal" \
    --catalog "$SMOKE_DIR/online.sqlite" \
    > "$SMOKE_DIR/ingestd.log" 2>&1 &
INGEST_PID=$!

trap 'kill -9 "$STUB_PID" "$INGEST_PID" "${AGENT_PID:-}" 2>/dev/null || true' EXIT

# Wait for both listeners to bind.
for _ in $(seq 1 50); do
    (echo > "/dev/tcp/${LISTEN%:*}/${LISTEN#*:}") 2>/dev/null && break
    sleep 0.1
done
for _ in $(seq 1 50); do
    (echo > "/dev/tcp/127.0.0.1/$STUB_PORT") 2>/dev/null && break
    sleep 0.1
done

# ── scry-agent: one scrape of the stub ──────────────────────────────
# --no-discovery: pure static-target path, no Kubernetes needed.
# --scrape-interval 600s: only the immediate first tick fires in this run.
echo "[agent-metrics] running scry-agent against the stub (one scrape)..."
RUST_LOG="${RUST_LOG:-info}" ./target/release/scry-agent \
    --server-addr "$LISTEN" \
    --no-discovery \
    --scrape-target "http://127.0.0.1:$STUB_PORT/metrics" \
    --scrape-interval 600s \
    --node-name smoke-node \
    > "$SMOKE_DIR/agent.log" 2>&1 &
AGENT_PID=$!

# Give it time to connect + scrape + the first flush, then graceful stop
# (drains the pending metrics batch).
sleep 3
echo "[agent-metrics] SIGINT scry-agent → drain + flush..."
kill -INT "$AGENT_PID"
wait "$AGENT_PID" 2>/dev/null || true

# Stop the ingest server so its final block flush completes, then reconcile.
echo "[agent-metrics] SIGINT scry-ingestd → final block flush..."
kill -INT "$INGEST_PID"
wait "$INGEST_PID" 2>/dev/null || true
trap 'kill -9 "$STUB_PID" 2>/dev/null || true' EXIT

# ── Verify ──────────────────────────────────────────────────────────
echo "[agent-metrics] reconciling a fresh catalog from the bucket..."
./target/release/scry-list --catalog "$SMOKE_DIR/recon.sqlite" \
    > "$SMOKE_DIR/scry-list.txt" 2>&1
cat "$SMOKE_DIR/scry-list.txt"

total_rows=$(awk -F'[= ]' '/^# total rows=/ { print $4; exit }' "$SMOKE_DIR/scry-list.txt")
metrics_blocks=$(sqlite3 "$SMOKE_DIR/recon.sqlite" \
    "SELECT COUNT(*) FROM blocks WHERE signal='metrics';")
metrics_postings=$(sqlite3 "$SMOKE_DIR/recon.sqlite" \
    "SELECT COUNT(*) FROM blocks WHERE signal='metrics' AND has_postings=1 AND postings_size_bytes>0;")

./target/release/scry-query \
    --catalog "$SMOKE_DIR/recon.sqlite" \
    --signal metrics \
    > "$SMOKE_DIR/query.metrics.txt" 2>&1 || true
queried=$(awk '/^# scan:/ { print $3; exit }' "$SMOKE_DIR/query.metrics.txt")

echo "[agent-metrics] ──── assertions ────"
echo "[agent-metrics] catalog rows     : ${total_rows:-<none>} (expected $EXPECTED_ROWS)"
echo "[agent-metrics] metrics blocks   : ${metrics_blocks:-0} (with postings: ${metrics_postings:-0})"
echo "[agent-metrics] queried rows     : ${queried:-<none>} (expected $EXPECTED_ROWS)"

failed=0
if [[ "${total_rows:-}" != "$EXPECTED_ROWS" ]]; then
    echo "[agent-metrics] FAIL: catalog rows != $EXPECTED_ROWS (scrape mapping / synthesis broke, or rows lost)"
    failed=1
fi
if [[ "${metrics_blocks:-0}" -lt 1 ]]; then
    echo "[agent-metrics] FAIL: no metrics blocks landed in the bucket"
    failed=1
fi
if [[ "${metrics_postings:-0}" -lt 1 ]]; then
    echo "[agent-metrics] FAIL: metrics block carries no postings sidecar"
    failed=1
fi
if [[ "${queried:-}" != "$EXPECTED_ROWS" ]]; then
    echo "[agent-metrics] FAIL: query returned ${queried:-<none>} rows, expected $EXPECTED_ROWS"
    cat "$SMOKE_DIR/query.metrics.txt"
    failed=1
fi

if [[ $failed -eq 0 ]]; then
    echo "[agent-metrics] PASS"
    exit 0
else
    echo "[agent-metrics] agent log tail:"; tail -20 "$SMOKE_DIR/agent.log" || true
    echo "[agent-metrics] ingestd log tail:"; tail -20 "$SMOKE_DIR/ingestd.log" || true
    exit 1
fi
