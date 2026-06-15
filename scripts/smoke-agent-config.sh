#!/usr/bin/env bash
# scry agent config-pipeline exit criterion — end-to-end.
#
# Proves the agent's TOML config-pipeline features land queryable logs +
# metrics in the bucket, against a real scry ingest + Garage:
#
#   CRI log tree ─┐
#                  ├→ scry agent (--config agent.toml) → scry ingest (--storage)
#   stub /metrics ─┘
#                  → bucket → scry list reconcile → scry get
#
# A Python stub serves a fixed exposition body with 1 gauge explicitly
# carrying container_name (exercising metric label_map). A single CRI log
# file carries a JSON body whose fields are extracted via the TOML pipeline.
# The agent scrapes exactly once (scrape-interval >> run duration) and replays
# logs from the start, yielding a deterministic set of rows. We then assert:
#
#   * Logs: a row landed; static_label → postings; json.labels → postings;
#     message_field replaced body; json.metadata → attributes Map.
#   * Metrics: relabeled key (container_name→container) queryable; old key
#     gone; static labels ride on exposed + synthesized series.
#
# 9 assertions (6 logs, 3 metrics) + coverage note.
#
# Feature coverage (from the TOML pipeline spec):
#   Feature 1 (per-signal keep)     — exercised implicitly (no keep set, so
#                                      all streams/series flow — a keep-test
#                                      would be a no-op here; proven by unit
#                                      tests).
#   Feature 2 (label_map surfacing) — covered by unit test (enrich_labels).
#   Feature 3 (static_labels)       — ✅ cluster=gothab-smoke on logs + metrics.
#   Feature 4 (json.labels → stream) — ✅ level promoted to a stream label.
#   Feature 5 (json.metadata → attr) — ✅ request_id per-entry attribute.
#   Feature 6 (metric label_map)    — ✅ container_name→container.
#
# Self-contained except for Garage (needs docker/garage/.env) and python3.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

LISTEN="${LISTEN:-127.0.0.1:4097}"
STUB_PORT="${STUB_PORT:-19101}"
SMOKE_DIR="${SMOKE_DIR:-/tmp/scry-agent-config}"
# Metrics: 1 exposed gauge (test_info) + up + scrape_duration_seconds = 3.
EXPECTED_METRIC_ROWS=3

# ── Pre-flight ──────────────────────────────────────────────────────
if [[ ! -f docker/garage/.env ]]; then
    echo "[agent-config] docker/garage/.env missing; run scripts/dev-garage-up.sh first" >&2
    exit 2
fi
# shellcheck disable=SC1091
set -a; source docker/garage/.env; set +a

for tool in aws sqlite3 python3; do
    command -v "$tool" >/dev/null || { echo "[agent-config] $tool not on PATH" >&2; exit 2; }
done

# ── Build ───────────────────────────────────────────────────────────
echo "[agent-config] building release binaries..."
cargo build --release -p scry >&2

# ── Clean slate ─────────────────────────────────────────────────────
rm -rf "$SMOKE_DIR"; mkdir -p "$SMOKE_DIR"
echo "[agent-config] emptying bucket s3://$SCRY_OBJSTORE_BUCKET/ ..."
AWS_ACCESS_KEY_ID="$SCRY_OBJSTORE_ACCESS_KEY_ID" \
AWS_SECRET_ACCESS_KEY="$SCRY_OBJSTORE_SECRET_ACCESS_KEY" \
AWS_REGION="$SCRY_OBJSTORE_REGION" \
    aws --endpoint-url "$SCRY_OBJSTORE_ENDPOINT" \
        s3 rm "s3://$SCRY_OBJSTORE_BUCKET/" --recursive >/dev/null || true

# ── CRI log tree ────────────────────────────────────────────────────
# Single pod: namespace=default, pod=mypod, container=app.
# One log line at known timestamp with JSON body.
POD_DIR="$SMOKE_DIR/pods/default_mypod_smokeuid/app"
mkdir -p "$POD_DIR"
cat > "$POD_DIR/0.log" <<'EOF'
2026-06-08T12:00:00.000000000Z stdout F {"level":"warn","msg":"hello","request_id":"r1"}
EOF

# ── Agent TOML config ───────────────────────────────────────────────
cat > "$SMOKE_DIR/agent.toml" <<'TOML'
[logs]
static_labels = { cluster = "gothab-smoke" }

[logs.json]
labels = ["level"]
metadata = ["request_id"]
message_field = "msg"

[metrics]
static_labels = { cluster = "gothab-smoke" }
label_map = { container_name = "container" }
TOML

# ── Stub /metrics endpoint ──────────────────────────────────────────
cat > "$SMOKE_DIR/metrics.txt" <<'EOF'
# TYPE test_info gauge
test_info{container_name="c1"} 1
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

# ── scry ingest (storage) ──────────────────────────────────────────
echo "[agent-config] starting scry ingest on $LISTEN..."
RUST_LOG="${RUST_LOG:-info}" ./target/release/scry ingest \
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

# ── scry agent: one scrape + one log file ───────────────────────────
# --no-discovery: pure static-target path + CRI file scan, no Kubernetes.
# --from-start: replay the CRI log file from byte 0.
# --scrape-interval 600s: only the immediate first tick fires in this run.
echo "[agent-config] running scry agent against stub + log tree..."
RUST_LOG="${RUST_LOG:-info}" ./target/release/scry agent \
    --server-addr "$LISTEN" \
    --no-discovery \
    --from-start \
    --logs-root "$SMOKE_DIR/pods" \
    --scrape-target "http://127.0.0.1:$STUB_PORT/metrics" \
    --scrape-interval 600s \
    --node-name smoke-node \
    --config "$SMOKE_DIR/agent.toml" \
    > "$SMOKE_DIR/agent.log" 2>&1 &
AGENT_PID=$!

# Give it time to connect + scrape + read logs + flush, then graceful stop.
sleep 4
echo "[agent-config] SIGINT scry agent → drain + flush..."
kill -INT "$AGENT_PID"
wait "$AGENT_PID" 2>/dev/null || true

# Stop the ingest server so its final block flush completes, then reconcile.
echo "[agent-config] SIGINT scry ingest → final block flush..."
kill -INT "$INGEST_PID"
wait "$INGEST_PID" 2>/dev/null || true
trap 'kill -9 "$STUB_PID" 2>/dev/null || true' EXIT

# ── Verify ──────────────────────────────────────────────────────────
echo "[agent-config] reconciling a fresh catalog from the bucket..."
./target/release/scry list --catalog "$SMOKE_DIR/recon.sqlite" \
    > "$SMOKE_DIR/scry-list.txt" 2>&1
cat "$SMOKE_DIR/scry-list.txt"

total_rows=$(awk -F'[= ]' '/^# total rows=/ { print $4; exit }' "$SMOKE_DIR/scry-list.txt")
logs_blocks=$(sqlite3 "$SMOKE_DIR/recon.sqlite" \
    "SELECT COUNT(*) FROM blocks WHERE signal='logs';")
metrics_blocks=$(sqlite3 "$SMOKE_DIR/recon.sqlite" \
    "SELECT COUNT(*) FROM blocks WHERE signal='metrics';")

# ── Logs assertions ─────────────────────────────────────────────────
echo "[agent-config] ──── logs assertions ────"

# 1. Default query → at least 1 log row (our line landed).
./target/release/scry get \
    --catalog "$SMOKE_DIR/recon.sqlite" \
    --signal logs \
    > "$SMOKE_DIR/query.logs.default.txt" 2>&1 || true
logs_default=$(awk '/^# scan:/ { print $3; exit }' "$SMOKE_DIR/query.logs.default.txt")

# 2. Static label → postings.
./target/release/scry get \
    --catalog "$SMOKE_DIR/recon.sqlite" \
    --signal logs \
    --matcher cluster=gothab-smoke \
    > "$SMOKE_DIR/query.logs.static.txt" 2>&1 || true
logs_static=$(awk '/^# scan:/ { print $3; exit }' "$SMOKE_DIR/query.logs.static.txt")

# 3. JSON label (level=warn) → postings.
./target/release/scry get \
    --catalog "$SMOKE_DIR/recon.sqlite" \
    --signal logs \
    --matcher level=warn \
    > "$SMOKE_DIR/query.logs.level.txt" 2>&1 || true
logs_level=$(awk '/^# scan:/ { print $3; exit }' "$SMOKE_DIR/query.logs.level.txt")

# 4. message_field replaced body (raw JSON gone; "hello" is the body).
./target/release/scry get \
    --catalog "$SMOKE_DIR/recon.sqlite" \
    --signal logs \
    --show \
    --sql "SELECT body FROM logs" \
    > "$SMOKE_DIR/query.logs.body.txt" 2>&1 || true

# 5. json.metadata → attributes Map (request_id=r1 present).
./target/release/scry get \
    --catalog "$SMOKE_DIR/recon.sqlite" \
    --signal logs \
    --show \
    --sql "SELECT attributes FROM logs" \
    > "$SMOKE_DIR/query.logs.attrs.txt" 2>&1 || true

# ── Metrics assertions ──────────────────────────────────────────────
echo "[agent-config] ──── metrics assertions ────"

# 6. Relabel applied: container=c1 matches.
./target/release/scry get \
    --catalog "$SMOKE_DIR/recon.sqlite" \
    --signal metrics \
    --matcher container=c1 \
    > "$SMOKE_DIR/query.metrics.relabel.txt" 2>&1 || true
metrics_relabel=$(awk '/^# scan:/ { print $3; exit }' "$SMOKE_DIR/query.metrics.relabel.txt")

# 7. Old key container_name=c1 matches 0 (relabel removed it).
./target/release/scry get \
    --catalog "$SMOKE_DIR/recon.sqlite" \
    --signal metrics \
    --matcher container_name=c1 \
    > "$SMOKE_DIR/query.metrics.oldkey.txt" 2>&1 || true
metrics_oldkey=$(awk '/^# scan:/ { print $3; exit }' "$SMOKE_DIR/query.metrics.oldkey.txt")

# 8. Static label on metrics.
./target/release/scry get \
    --catalog "$SMOKE_DIR/recon.sqlite" \
    --signal metrics \
    --matcher cluster=gothab-smoke \
    > "$SMOKE_DIR/query.metrics.static.txt" 2>&1 || true
metrics_static=$(awk '/^# scan:/ { print $3; exit }' "$SMOKE_DIR/query.metrics.static.txt")

# 9. Static label rides on synthesized up series.
./target/release/scry get \
    --catalog "$SMOKE_DIR/recon.sqlite" \
    --signal metrics \
    --matcher __name__=up \
    --matcher cluster=gothab-smoke \
    > "$SMOKE_DIR/query.metrics.up.txt" 2>&1 || true
metrics_up=$(awk '/^# scan:/ { print $3; exit }' "$SMOKE_DIR/query.metrics.up.txt")

# ── Results ─────────────────────────────────────────────────────────
echo "[agent-config] ──── results ────"
echo "[agent-config] catalog total rows : ${total_rows:-<none>}"
echo "[agent-config] logs blocks         : ${logs_blocks:-0}"
echo "[agent-config] metrics blocks      : ${metrics_blocks:-0}"
echo "[agent-config]"
echo "[agent-config] logs default        : ${logs_default:-<none>} (expected ≥ 1)"
echo "[agent-config] logs cluster label  : ${logs_static:-<none>} (expected ≥ 1)"
echo "[agent-config] logs level label    : ${logs_level:-<none>} (expected ≥ 1)"
echo "[agent-config] logs body           : $(head -5 "$SMOKE_DIR/query.logs.body.txt" | grep -c 'hello') match(es) for 'hello' (expected ≥ 1)"
echo "[agent-config] logs attrs          : $(head -5 "$SMOKE_DIR/query.logs.attrs.txt" | grep -c 'request_id') match(es) for 'request_id' AND $(head -5 "$SMOKE_DIR/query.logs.attrs.txt" | grep -c 'r1') match(es) for 'r1' (expected each ≥ 1)"
echo "[agent-config]"
echo "[agent-config] metrics relabel     : ${metrics_relabel:-<none>} (expected ≥ 1, key=container)"
echo "[agent-config] metrics oldkey      : ${metrics_oldkey:-0} (expected == 0, key=container_name)"
echo "[agent-config] metrics static      : ${metrics_static:-<none>} (expected ≥ 1)"
echo "[agent-config] metrics up+static   : ${metrics_up:-<none>} (expected ≥ 1)"

failed=0

if [[ "${logs_default:-0}" -lt 1 ]]; then
    echo "[agent-config] FAIL: logs default query returned ${logs_default:-0} rows, expected ≥ 1"
    failed=1
fi
if [[ "${logs_static:-0}" -lt 1 ]]; then
    echo "[agent-config] FAIL: logs --matcher cluster=gothab-smoke returned ${logs_static:-0} rows"
    failed=1
fi
if [[ "${logs_level:-0}" -lt 1 ]]; then
    echo "[agent-config] FAIL: logs --matcher level=warn returned ${logs_level:-0} rows"
    failed=1
fi
if ! grep -q 'hello' "$SMOKE_DIR/query.logs.body.txt" 2>/dev/null; then
    echo "[agent-config] FAIL: SELECT body FROM logs does not contain 'hello' (message_field may not have replaced body)"
    failed=1
fi
if ! grep -q 'request_id' "$SMOKE_DIR/query.logs.attrs.txt" 2>/dev/null; then
    echo "[agent-config] FAIL: SELECT attributes FROM logs does not contain 'request_id'"
    failed=1
fi
if ! grep -q 'r1' "$SMOKE_DIR/query.logs.attrs.txt" 2>/dev/null; then
    echo "[agent-config] FAIL: SELECT attributes FROM logs does not contain 'r1'"
    failed=1
fi
if [[ "${metrics_relabel:-0}" -lt 1 ]]; then
    echo "[agent-config] FAIL: metrics --matcher container=c1 returned ${metrics_relabel:-0} rows (relabel may not have fired)"
    failed=1
fi
if [[ "${metrics_oldkey:-0}" -ne 0 ]]; then
    echo "[agent-config] FAIL: metrics --matcher container_name=c1 returned ${metrics_oldkey:-0} rows, expected 0 (old key not suppressed)"
    failed=1
fi
if [[ "${metrics_static:-0}" -lt 1 ]]; then
    echo "[agent-config] FAIL: metrics --matcher cluster=gothab-smoke returned ${metrics_static:-0} rows"
    failed=1
fi
if [[ "${metrics_up:-0}" -lt 1 ]]; then
    echo "[agent-config] FAIL: metrics --matcher __name__=up --matcher cluster=gothab-smoke returned ${metrics_up:-0} rows"
    failed=1
fi

if [[ $failed -eq 0 ]]; then
    echo "[agent-config] PASS"
    exit 0
else
    echo "[agent-config] agent log tail:"; tail -20 "$SMOKE_DIR/agent.log" || true
    echo "[agent-config] ingestd log tail:"; tail -20 "$SMOKE_DIR/ingestd.log" || true
    exit 1
fi
