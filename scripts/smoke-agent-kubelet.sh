#!/usr/bin/env bash
# scry-agent kubelet/cadvisor-scraping exit criterion — end-to-end (D-048).
#
# Proves the agent's kubelet scrape path — HTTPS + skip-verify TLS + a
# file-backed (rotating) ServiceAccount bearer token — lands queryable metrics
# in the bucket, against a real scry-ingestd + Garage:
#
#   self-signed HTTPS stub (/metrics/cadvisor + /metrics, Bearer-gated)
#       →  scry-agent (--config, [metrics.kubelet])  →  scry-ingestd (--storage)
#       →  bucket  →  scry-list reconcile  →  scry-query
#
# The stub serves ONE exposed sample per endpoint and **401s without the right
# Authorization: Bearer header**, so a successful scrape proves the agent read
# the bearer file and presented it over TLS-skip-verify HTTPS. The agent
# scrapes each endpoint exactly once (interval >> run), synthesizing
# `up` + `scrape_duration_seconds` per scrape, so a deterministic
# 6 series / 6 samples land: (cadvisor: 1 exposed + up + scrape_duration) +
# (kubelet: 1 exposed + up + scrape_duration). We then assert:
#
#   * the reconciled catalog holds exactly 6 metric rows,
#   * ≥1 metrics block landed with a postings sidecar,
#   * scry-query --signal metrics scans those 6 rows back,
#   * job=cadvisor and job=kubelet each select 3 rows (both endpoints scraped),
#   * __name__=up selects 2 rows (both scrapes succeeded → bearer+TLS worked),
#   * cluster=gothab-smoke (a static_label) rides all 6 rows.
#
# Pod-label SD needs the k8s pod watch this smoke omits → proven by the
# pod_matches / build_scrape_target unit tests (same approach as D-047 feat 2).
#
# Self-contained except for Garage (needs docker/garage/.env), python3,
# openssl, aws, sqlite3.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

LISTEN="${LISTEN:-127.0.0.1:4098}"
STUB_PORT="${STUB_PORT:-10250}"
SMOKE_DIR="${SMOKE_DIR:-/tmp/scry-agent-kubelet}"
EXPECTED_ROWS=6   # (cadvisor: 1 + up + scrape_duration) + (kubelet: 1 + up + scrape_duration)
TOKEN="smoke-sa-token-$$"

# ── Pre-flight ──────────────────────────────────────────────────────
if [[ ! -f docker/garage/.env ]]; then
    echo "[agent-kubelet] docker/garage/.env missing; run scripts/dev-garage-up.sh first" >&2
    exit 2
fi
# shellcheck disable=SC1091
set -a; source docker/garage/.env; set +a

for tool in aws sqlite3 python3 openssl; do
    command -v "$tool" >/dev/null || { echo "[agent-kubelet] $tool not on PATH" >&2; exit 2; }
done

# ── Build ───────────────────────────────────────────────────────────
echo "[agent-kubelet] building release binaries..."
cargo build --release -p scry-ingestd -p scry-list -p scry-query -p scry-agent >&2

# ── Clean slate ─────────────────────────────────────────────────────
rm -rf "$SMOKE_DIR"; mkdir -p "$SMOKE_DIR"
echo "[agent-kubelet] emptying bucket s3://$SCRY_OBJSTORE_BUCKET/ ..."
AWS_ACCESS_KEY_ID="$SCRY_OBJSTORE_ACCESS_KEY_ID" \
AWS_SECRET_ACCESS_KEY="$SCRY_OBJSTORE_SECRET_ACCESS_KEY" \
AWS_REGION="$SCRY_OBJSTORE_REGION" \
    aws --endpoint-url "$SCRY_OBJSTORE_ENDPOINT" \
        s3 rm "s3://$SCRY_OBJSTORE_BUCKET/" --recursive >/dev/null || true

# ── Self-signed cert + bearer-token file ────────────────────────────
openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout "$SMOKE_DIR/key.pem" -out "$SMOKE_DIR/cert.pem" \
    -days 1 -subj "/CN=kubelet-smoke" >/dev/null 2>&1
printf '%s' "$TOKEN" > "$SMOKE_DIR/token"

# ── Bodies ──────────────────────────────────────────────────────────
cat > "$SMOKE_DIR/cadvisor.txt" <<'EOF'
# HELP container_cpu_usage_seconds_total Cumulative cpu time consumed
# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{container="app"} 12.5
EOF
cat > "$SMOKE_DIR/kubelet.txt" <<'EOF'
# HELP kubelet_running_pods Number of pods currently running
# TYPE kubelet_running_pods gauge
kubelet_running_pods 7
EOF

# ── HTTPS kubelet stub (Bearer-gated) ───────────────────────────────
python3 - "$STUB_PORT" "$SMOKE_DIR" "$TOKEN" > "$SMOKE_DIR/stub.log" 2>&1 <<'PY' &
import sys, ssl, http.server, socketserver
port = int(sys.argv[1]); d = sys.argv[2]; tok = sys.argv[3]
cadvisor = open(d + '/cadvisor.txt', 'rb').read()
kubelet  = open(d + '/kubelet.txt', 'rb').read()
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.headers.get('Authorization') != 'Bearer ' + tok:
            self.send_response(401); self.end_headers(); self.wfile.write(b'unauthorized'); return
        body = cadvisor if self.path == '/metrics/cadvisor' else kubelet if self.path == '/metrics' else None
        if body is None:
            self.send_response(404); self.end_headers(); return
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain; version=0.0.4')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers(); self.wfile.write(body)
    def log_message(self, *a): pass
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
ctx.load_cert_chain(d + '/cert.pem', d + '/key.pem')
with socketserver.TCPServer(('127.0.0.1', port), H) as s:
    s.socket = ctx.wrap_socket(s.socket, server_side=True)
    s.serve_forever()
PY
STUB_PID=$!

# ── Agent config (kubelet block + static label) ─────────────────────
cat > "$SMOKE_DIR/agent.toml" <<EOF
[metrics]
static_labels = { cluster = "gothab-smoke" }

[metrics.kubelet]
enabled = true
address = "https://127.0.0.1:$STUB_PORT"
cadvisor = true
kubelet = true
bearer_file = "$SMOKE_DIR/token"

[metrics.kubelet.tls]
insecure_skip_verify = true
EOF

# ── scry-ingestd (storage) ──────────────────────────────────────────
echo "[agent-kubelet] starting scry-ingestd on $LISTEN..."
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

# ── scry-agent: one scrape per kubelet endpoint ─────────────────────
echo "[agent-kubelet] running scry-agent (kubelet config, one scrape per endpoint)..."
RUST_LOG="${RUST_LOG:-info}" ./target/release/scry-agent \
    --server-addr "$LISTEN" \
    --no-discovery \
    --config "$SMOKE_DIR/agent.toml" \
    --scrape-interval 600s \
    --node-name smoke-node \
    --node-ip 127.0.0.1 \
    > "$SMOKE_DIR/agent.log" 2>&1 &
AGENT_PID=$!

sleep 3
echo "[agent-kubelet] SIGINT scry-agent → drain + flush..."
kill -INT "$AGENT_PID"
wait "$AGENT_PID" 2>/dev/null || true

echo "[agent-kubelet] SIGINT scry-ingestd → final block flush..."
kill -INT "$INGEST_PID"
wait "$INGEST_PID" 2>/dev/null || true
trap 'kill -9 "$STUB_PID" 2>/dev/null || true' EXIT

# ── Verify ──────────────────────────────────────────────────────────
echo "[agent-kubelet] reconciling a fresh catalog from the bucket..."
./target/release/scry-list --catalog "$SMOKE_DIR/recon.sqlite" \
    > "$SMOKE_DIR/scry-list.txt" 2>&1
cat "$SMOKE_DIR/scry-list.txt"

total_rows=$(awk -F'[= ]' '/^# total rows=/ { print $4; exit }' "$SMOKE_DIR/scry-list.txt")
metrics_blocks=$(sqlite3 "$SMOKE_DIR/recon.sqlite" \
    "SELECT COUNT(*) FROM blocks WHERE signal='metrics';")
metrics_postings=$(sqlite3 "$SMOKE_DIR/recon.sqlite" \
    "SELECT COUNT(*) FROM blocks WHERE signal='metrics' AND has_postings=1 AND postings_size_bytes>0;")

q() {  # run scry-query with the given matchers, echo the scanned row count
    ./target/release/scry-query --catalog "$SMOKE_DIR/recon.sqlite" --signal metrics "$@" \
        > "$SMOKE_DIR/q.out" 2>&1 || true
    awk '/^# scan:/ { print $3; exit }' "$SMOKE_DIR/q.out"
}

queried=$(q)
cadvisor_rows=$(q --matcher job=cadvisor)
kubelet_rows=$(q --matcher job=kubelet)
up_rows=$(q --matcher __name__=up)
cluster_rows=$(q --matcher cluster=gothab-smoke)

echo "[agent-kubelet] ──── assertions ────"
echo "[agent-kubelet] catalog rows     : ${total_rows:-<none>} (expected $EXPECTED_ROWS)"
echo "[agent-kubelet] metrics blocks   : ${metrics_blocks:-0} (with postings: ${metrics_postings:-0})"
echo "[agent-kubelet] queried rows     : ${queried:-<none>} (expected $EXPECTED_ROWS)"
echo "[agent-kubelet] job=cadvisor     : ${cadvisor_rows:-<none>} (expected 3)"
echo "[agent-kubelet] job=kubelet      : ${kubelet_rows:-<none>} (expected 3)"
echo "[agent-kubelet] __name__=up      : ${up_rows:-<none>} (expected 2 → both scrapes auth'd over TLS)"
echo "[agent-kubelet] cluster=...smoke : ${cluster_rows:-<none>} (expected $EXPECTED_ROWS → static label on all)"

failed=0
check() {  # check <actual> <expected> <message>
    if [[ "${1:-}" != "$2" ]]; then
        echo "[agent-kubelet] FAIL: $3 (got ${1:-<none>}, expected $2)"
        failed=1
    fi
}
check "$total_rows" "$EXPECTED_ROWS" "catalog rows wrong"
check "$queried" "$EXPECTED_ROWS" "query scanned wrong row count"
check "$cadvisor_rows" 3 "job=cadvisor rows wrong"
check "$kubelet_rows" 3 "job=kubelet rows wrong"
check "$up_rows" 2 "__name__=up rows wrong (a scrape failed → bearer/TLS path broke)"
check "$cluster_rows" "$EXPECTED_ROWS" "static label did not ride all rows"
if [[ "${metrics_blocks:-0}" -lt 1 ]]; then
    echo "[agent-kubelet] FAIL: no metrics blocks landed in the bucket"; failed=1
fi
if [[ "${metrics_postings:-0}" -lt 1 ]]; then
    echo "[agent-kubelet] FAIL: metrics block carries no postings sidecar"; failed=1
fi

if [[ $failed -eq 0 ]]; then
    echo "[agent-kubelet] PASS"
    exit 0
else
    echo "[agent-kubelet] agent log tail:";   tail -25 "$SMOKE_DIR/agent.log"   || true
    echo "[agent-kubelet] ingestd log tail:"; tail -20 "$SMOKE_DIR/ingestd.log" || true
    echo "[agent-kubelet] stub log tail:";    tail -20 "$SMOKE_DIR/stub.log"    || true
    exit 1
fi
