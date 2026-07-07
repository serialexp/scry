#!/usr/bin/env bash
# scry replay-opensearch exit criterion (D-056) — end-to-end.
#
# Proves the OpenSearch → scry replay path lands a faithful, queryable copy of
# the corpus, against a real scry ingest + Garage:
#
#   python stub OpenSearch (PIT + search_after)  →  scry replay-opensearch
#                  →  scry ingest (--storage)  →  bucket
#                  →  scry list reconcile  →  scry get
#
# The stub serves a fixed corpus of M=50 documents with ascending @timestamp
# (epoch-ms), round-robin services (api / worker / scheduler), and a few docs
# deliberately missing @timestamp (carry-forward) or `message` (empty body). It
# implements `_count`, `_pit` (POST open / DELETE close), and `_search` with
# `search_after` paging over an internal seq cursor (page size forces multiple
# pages). We then assert:
#
#   * the reconciled catalog holds exactly M log rows (nothing lost / dup'd),
#   * ≥1 logs block landed,
#   * scry get --signal logs scans M rows back,
#   * --matcher service=api selects exactly the api subset (labels preserved),
#   * --grep "log line" selects M minus the empty-body docs (bodies preserved),
#   * the replay summary reports ts_inherited=2 and body_missing=2.
#
# Self-contained except for Garage (needs docker/garage/.env) and python3.
# The dev bucket is emptied at the start of the run.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

LISTEN="${LISTEN:-127.0.0.1:4098}"
STUB_PORT="${STUB_PORT:-19200}"
SMOKE_DIR="${SMOKE_DIR:-/tmp/scry-osreplay}"
M=50                 # total docs
EXPECTED_API=17      # seq 0,3,...,48
EXPECTED_BODY=48     # M minus 2 empty-body docs
EXPECTED_TS_INHERIT=2
EXPECTED_BODY_MISSING=2

# ── Pre-flight ──────────────────────────────────────────────────────
if [[ ! -f docker/garage/.env ]]; then
    echo "[osreplay] docker/garage/.env missing; run scripts/dev-garage-up.sh first" >&2
    exit 2
fi
# shellcheck disable=SC1091
set -a; source docker/garage/.env; set +a

for tool in aws sqlite3 python3; do
    command -v "$tool" >/dev/null || { echo "[osreplay] $tool not on PATH" >&2; exit 2; }
done

# ── Build ───────────────────────────────────────────────────────────
echo "[osreplay] building release binary..."
cargo build --release -p scry >&2

# ── Clean slate ─────────────────────────────────────────────────────
rm -rf "$SMOKE_DIR"; mkdir -p "$SMOKE_DIR"
echo "[osreplay] emptying bucket s3://$SCRY_OBJSTORE_BUCKET/ ..."
AWS_ACCESS_KEY_ID="$SCRY_OBJSTORE_ACCESS_KEY_ID" \
AWS_SECRET_ACCESS_KEY="$SCRY_OBJSTORE_SECRET_ACCESS_KEY" \
AWS_REGION="$SCRY_OBJSTORE_REGION" \
    aws --endpoint-url "$SCRY_OBJSTORE_ENDPOINT" \
        s3 rm "s3://$SCRY_OBJSTORE_BUCKET/" --recursive >/dev/null || true

# ── Stub OpenSearch ─────────────────────────────────────────────────
python3 - "$STUB_PORT" "$M" > "$SMOKE_DIR/stub.log" 2>&1 <<'PY' &
import sys, json, http.server, socketserver

port = int(sys.argv[1]); M = int(sys.argv[2])

# Build the corpus: ascending @timestamp, round-robin service, a couple of
# docs missing @timestamp (carry-forward) or message (empty body).
SERVICES = ["api", "worker", "scheduler"]
docs = []
base_ms = 1_700_000_000_000
for seq in range(M):
    src = {"service": SERVICES[seq % 3], "severity": ["info", "warn", "error"][seq % 3]}
    if seq not in (10, 11):            # 2 docs missing @timestamp
        src["@timestamp"] = base_ms + seq * 1000
    if seq not in (20, 21):            # 2 docs missing message
        src["message"] = f"log line {seq}"
    src["host"] = f"host-{seq % 4}"    # extra field → attribute
    docs.append(src)

def hits_after(after_seq, size):
    out = []
    for seq in range(M):
        if seq <= after_seq:
            continue
        out.append({"_source": docs[seq], "sort": [seq]})
        if len(out) >= size:
            break
    return out

class H(http.server.BaseHTTPRequestHandler):
    def _json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers(); self.wfile.write(body)

    def _read_body(self):
        n = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(n) if n else b""
        try:
            return json.loads(raw) if raw else {}
        except Exception:
            return {}

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        if path.endswith("/_count"):
            self._json({"count": M})
        elif path.endswith("/_pit"):
            self._json({"pit_id": "smoke-pit"})
        elif path.endswith("/_search"):
            body = self._read_body()
            size = int(body.get("size", 10))
            after = body.get("search_after")
            after_seq = int(after[0]) if after else -1
            hits = hits_after(after_seq, size)
            self._json({"hits": {"hits": hits}})
        else:
            self._json({"error": f"unknown path {path}"}, 404)

    def do_DELETE(self):
        if self.path.split("?", 1)[0].endswith("/_pit"):
            self._json({"succeeded": True})
        else:
            self._json({"error": "unknown"}, 404)

    def log_message(self, *a):
        pass

with socketserver.TCPServer(("127.0.0.1", port), H) as s:
    s.serve_forever()
PY
STUB_PID=$!

# ── scry ingest (storage) ──────────────────────────────────────────
echo "[osreplay] starting scry ingest on $LISTEN..."
RUST_LOG="${RUST_LOG:-info}" ./target/release/scry ingest \
    --listen "$LISTEN" \
    --storage \
    --wal-dir "$SMOKE_DIR/wal" \
    --catalog "$SMOKE_DIR/online.sqlite" \
    > "$SMOKE_DIR/ingestd.log" 2>&1 &
INGEST_PID=$!

trap 'kill -9 "$STUB_PID" "$INGEST_PID" 2>/dev/null || true' EXIT

# Wait for both listeners to bind.
for _ in $(seq 1 50); do
    (echo > "/dev/tcp/${LISTEN%:*}/${LISTEN#*:}") 2>/dev/null && break
    sleep 0.1
done
for _ in $(seq 1 50); do
    (echo > "/dev/tcp/127.0.0.1/$STUB_PORT") 2>/dev/null && break
    sleep 0.1
done

# ── scry replay-opensearch: drain the stub into scry ────────────────
# Small page size forces multiple search_after pages; fixed rate (no ramp
# effect at this scale) keeps the run deterministic.
echo "[osreplay] replaying $M docs from the stub into scry..."
RUST_LOG="${RUST_LOG:-info}" ./target/release/scry replay-opensearch \
    --os-url "http://127.0.0.1:$STUB_PORT" \
    --os-index "logs-*" \
    --target "$LISTEN" \
    --page-size 10 \
    --batch-records 100 \
    --rate 100000 \
    --rate-max 100000 \
    > "$SMOKE_DIR/replay.log" 2>&1 || {
        echo "[osreplay] FAIL: replay exited non-zero"; cat "$SMOKE_DIR/replay.log"; exit 1;
    }
cat "$SMOKE_DIR/replay.log"

# Stop the ingest server so its final block flush completes, then reconcile.
echo "[osreplay] SIGINT scry ingest → final block flush..."
kill -INT "$INGEST_PID"
wait "$INGEST_PID" 2>/dev/null || true
trap 'kill -9 "$STUB_PID" 2>/dev/null || true' EXIT

# ── Verify ──────────────────────────────────────────────────────────
echo "[osreplay] reconciling a fresh catalog from the bucket..."
./target/release/scry list --catalog "$SMOKE_DIR/recon.sqlite" \
    > "$SMOKE_DIR/scry-list.txt" 2>&1
cat "$SMOKE_DIR/scry-list.txt"

total_rows=$(awk -F'[= ]' '/^# total rows=/ { print $4; exit }' "$SMOKE_DIR/scry-list.txt")
logs_blocks=$(sqlite3 "$SMOKE_DIR/recon.sqlite" \
    "SELECT COUNT(*) FROM blocks WHERE signal='logs';")

query_count() { # args passed straight to scry get; echoes the scanned row count
    ./target/release/scry get --catalog "$SMOKE_DIR/recon.sqlite" --signal logs "$@" \
        > "$SMOKE_DIR/q.txt" 2>&1 || true
    awk '/^# scan:/ { print $3; exit }' "$SMOKE_DIR/q.txt"
}

queried=$(query_count)
api_rows=$(query_count --matcher service=api)
body_rows=$(query_count --grep "log line")

# ts_inherited / body_missing from the replay summary line.
ts_inherit=$(grep -oE 'ts_inherited=[0-9]+' "$SMOKE_DIR/replay.log" | tail -1 | cut -d= -f2)
body_missing=$(grep -oE 'body_missing=[0-9]+' "$SMOKE_DIR/replay.log" | tail -1 | cut -d= -f2)

echo "[osreplay] ──── assertions ────"
echo "[osreplay] catalog rows   : ${total_rows:-<none>} (expected $M)"
echo "[osreplay] logs blocks    : ${logs_blocks:-0} (expected ≥1)"
echo "[osreplay] queried rows   : ${queried:-<none>} (expected $M)"
echo "[osreplay] service=api    : ${api_rows:-<none>} (expected $EXPECTED_API)"
echo "[osreplay] grep 'log line': ${body_rows:-<none>} (expected $EXPECTED_BODY)"
echo "[osreplay] ts_inherited   : ${ts_inherit:-<none>} (expected $EXPECTED_TS_INHERIT)"
echo "[osreplay] body_missing   : ${body_missing:-<none>} (expected $EXPECTED_BODY_MISSING)"

failed=0
[[ "${total_rows:-}"   == "$M"                     ]] || { echo "[osreplay] FAIL: catalog rows != $M"; failed=1; }
[[ "${logs_blocks:-0}" -ge 1                       ]] || { echo "[osreplay] FAIL: no logs blocks landed"; failed=1; }
[[ "${queried:-}"      == "$M"                     ]] || { echo "[osreplay] FAIL: query returned ${queried:-<none>}, expected $M"; failed=1; }
[[ "${api_rows:-}"     == "$EXPECTED_API"          ]] || { echo "[osreplay] FAIL: service=api != $EXPECTED_API (labels lost)"; failed=1; }
[[ "${body_rows:-}"    == "$EXPECTED_BODY"         ]] || { echo "[osreplay] FAIL: grep count != $EXPECTED_BODY (bodies lost)"; failed=1; }
[[ "${ts_inherit:-}"   == "$EXPECTED_TS_INHERIT"   ]] || { echo "[osreplay] FAIL: ts_inherited != $EXPECTED_TS_INHERIT"; failed=1; }
[[ "${body_missing:-}" == "$EXPECTED_BODY_MISSING" ]] || { echo "[osreplay] FAIL: body_missing != $EXPECTED_BODY_MISSING"; failed=1; }

if [[ $failed -eq 0 ]]; then
    echo "[osreplay] PASS"
    exit 0
else
    echo "[osreplay] replay log tail:"; tail -20 "$SMOKE_DIR/replay.log" || true
    echo "[osreplay] ingestd log tail:"; tail -20 "$SMOKE_DIR/ingestd.log" || true
    exit 1
fi
