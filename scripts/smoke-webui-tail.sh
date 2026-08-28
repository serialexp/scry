#!/usr/bin/env bash
#
# smoke-webui-tail.sh — the browser live-tail exit criterion: a real log line
# ingested by a real ingester, discovered through Valkey, relayed by the query
# daemon's tail front-door, relayed *again* by `scry web`, and finally decoded
# by the shipped TypeScript client.
#
#   noise-spewer ─► scry ingest --valkey-url --tail-advertise-addr   (no storage)
#                        ▲ SET scry/tail/ingesters/<uuid> = its addr
#                        │
#                   scry query --tail-listen --valkey-url            (front-door)
#                        ▲ discover → dial → fan-in
#                        │
#                   scry web --queryd id=… --queryd-tail id=…        (byte pipe)
#                        ▲ POST /api/tail, chunked response
#                        │
#              desktop/scripts/tail-probe.ts  (bun, real HttpTransport + runTail)
#
# `scripts/smoke-tail-queryd.sh` already proves the Rust half. What is new below
# the web server is nothing; what is new *above* it is everything — this is the
# only place the TypeScript tail client meets a real server, so it is where a
# binschema regression, a framing bug, or a non-streaming relay gets caught.
#
# Asserts:
#   1. /api/targets reports `live` per target — true where --queryd-tail gave it
#      a tail address, false where it did not. (The UI disables its toggle on
#      this flag, and an address must never appear in the JSON.)
#   2. The `live` target streams: the probe decodes matching records while
#      service=worker / service=scheduler are excluded by the subscription.
#   3. A target with no tail address is refused with 409 → the client raises
#      LiveUnavailableError, so the UI can say why instead of hanging on an
#      empty pane.
#   4. A target whose queryd has no Valkey is refused with the wire's own
#      ERR_TAIL_UNAVAILABLE (9) → the client raises TailError with that code.
#
# Storage-free by design (like smoke-tail-queryd.sh): the ingester runs without
# --storage and the query daemons point at a dummy object store with convergence
# effectively disabled, so **only a dev Valkey is required** — no Garage. Point
# it at SCRY_VALKEY_URL (default redis://127.0.0.1:6380, this machine's
# `scry-valkey-smoke`).
#
# Env knobs: SCRY_VALKEY_URL, IA/QQ/QT/RQ/RT/WEB ports, SPEW_RATE, BATCHES.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

VALKEY_URL="${SCRY_VALKEY_URL:-redis://127.0.0.1:6380}"
IA="${IA:-127.0.0.1:14440}"          # ingester ingest + tail port
QQ="${QQ:-127.0.0.1:14441}"          # queryd query port
QT="${QT:-127.0.0.1:14442}"          # queryd tail-listen (front-door)
RQ="${RQ:-127.0.0.1:14443}"          # Valkey-less queryd query port
RT="${RT:-127.0.0.1:14444}"          # Valkey-less queryd tail-listen
WEB="${WEB:-127.0.0.1:14445}"        # scry web
S3_PORT="${S3_PORT:-127.0.0.1:14446}" # stub object store (always-empty bucket)
PASS="${PASS:-tail-smoke-pw}"
SPEW_RATE="${SPEW_RATE:-200}"
BATCHES="${BATCHES:-200}"

TMP="$(mktemp -d)"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$TMP"; }
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  for f in ingest queryd refuse web probe_live probe_plain probe_refuse; do
    [ -f "$TMP/$f.log" ] && { echo "---- $f.log (tail) ----" >&2; tail -20 "$TMP/$f.log" >&2; }
  done
  exit 1
}
ok() { echo "  ok: $*"; }

command -v bun >/dev/null     || fail "bun not found (needed to run the TS tail client)"
command -v curl >/dev/null    || fail "curl not found"
command -v python3 >/dev/null || fail "python3 not found (needed to read the targets JSON)"

# ── A stub object store that is always empty ─────────────────────────
#
# Nothing on the tail path reads or writes a block, but `scry query` refuses to
# start until it has seeded its catalog from the bucket — an *unreachable*
# endpoint is a fatal boot error, not a warning. Rather than drag Garage in for
# a test that stores nothing, we answer the two requests a cold boot makes:
# GET `_catalog/snapshot.sqlite` (404 ⇒ no snapshot) and ListObjectsV2 (an empty
# listing ⇒ a seed of zero blocks). Signatures are never checked.
STUB_S3_PY='
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

EMPTY_LIST = (
    b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
    b"<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
    b"<Name>scry-smoke</Name><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys>"
    b"<IsTruncated>false</IsTruncated></ListBucketResult>"
)

class H(BaseHTTPRequestHandler):
    def _listing(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(EMPTY_LIST)))
        self.end_headers()
        self.wfile.write(EMPTY_LIST)

    def _missing(self):
        self.send_response(404)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        if "list-type=2" in self.path:
            self._listing()
        else:
            self._missing()

    do_HEAD = do_GET

    def log_message(self, *a):
        pass

ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
'
python3 -c "$STUB_S3_PY" "${S3_PORT#*:}" >"$TMP/stub-s3.log" 2>&1 &
PIDS+=($!)
export SCRY_OBJSTORE_ENDPOINT="http://$S3_PORT"
export SCRY_OBJSTORE_REGION="garage"
export SCRY_OBJSTORE_BUCKET="scry-smoke"
export SCRY_OBJSTORE_ACCESS_KEY_ID="dummy"
export SCRY_OBJSTORE_SECRET_ACCESS_KEY="dummy"
export SCRY_OBJSTORE_PATH_STYLE="true"

# ── Pre-flight: Valkey must answer. ──────────────────────────────────
if command -v valkey-cli >/dev/null; then VK=valkey-cli
elif command -v redis-cli >/dev/null; then VK=redis-cli
else VK=""; fi
if [ -n "$VK" ]; then
  "$VK" -u "$VALKEY_URL" ping 2>/dev/null | grep -q PONG \
    || fail "Valkey at $VALKEY_URL not answering PING (start scry-valkey-smoke, or set SCRY_VALKEY_URL)"
  keys=$("$VK" -u "$VALKEY_URL" --scan --pattern 'scry/tail/ingesters/*' 2>/dev/null || true)
  [ -n "$keys" ] && echo "$keys" | xargs -r "$VK" -u "$VALKEY_URL" del >/dev/null 2>&1 || true
else
  echo "note: no valkey-cli/redis-cli to pre-check $VALKEY_URL; proceeding" >&2
fi

echo "== building frontend bundle (scry web embeds it) =="
( cd desktop && bun run build ) >"$TMP/fe-build.log" 2>&1 \
  || { cat "$TMP/fe-build.log"; fail "frontend build failed"; }

echo "== building release scry + noise-spewer =="
cargo build --release -p scry -p noise-spewer >"$TMP/cargo.log" 2>&1 \
  || { cat "$TMP/cargo.log"; fail "build failed"; }
SCRY=./target/release/scry
SPEWER=./target/release/noise-spewer

wait_bind() {
  local addr=$1
  for _ in $(seq 1 100); do
    (exec 3<>"/dev/tcp/${addr%:*}/${addr#*:}") 2>/dev/null && { exec 3>&- 3<&-; return 0; }
    sleep 0.1
  done
  return 1
}

# ── The daemons ──────────────────────────────────────────────────────
echo "== starting ingester ($IA), storage-less, registering in Valkey =="
RUST_LOG=info "$SCRY" ingest --listen "$IA" --valkey-url "$VALKEY_URL" \
  --tail-advertise-addr "$IA" --lease-ttl 10 >"$TMP/ingest.log" 2>&1 &
PIDS+=($!)
wait_bind "$IA" || fail "ingester never bound"
ok "ingester listening on $IA"

echo "== starting query daemon front-door (query $QQ, tail $QT) =="
RUST_LOG=info "$SCRY" query --listen "$QQ" --catalog "$TMP/queryd.sqlite" \
  --valkey-url "$VALKEY_URL" --tail-listen "$QT" --tail-rediscover-interval 1 \
  --poll-interval 999999 --full-walk-interval 999999 >"$TMP/queryd.log" 2>&1 &
PIDS+=($!)
wait_bind "$QT" || fail "queryd tail-listen never bound"
ok "front-door listening on $QT"

echo "== starting a second query daemon with NO Valkey (query $RQ, tail $RT) =="
RUST_LOG=info "$SCRY" query --listen "$RQ" --catalog "$TMP/refuse.sqlite" \
  --tail-listen "$RT" --poll-interval 999999 --full-walk-interval 999999 \
  >"$TMP/refuse.log" 2>&1 &
PIDS+=($!)
wait_bind "$RT" || fail "Valkey-less queryd tail-listen never bound"
ok "Valkey-less front-door listening on $RT"

# Three targets, covering all three live outcomes in one server:
#   live   — has a tail address, backed by a Valkey-connected front-door
#   plain  — no tail address at all               (409 at the web tier)
#   refuse — has a tail address, but no Valkey    (ERR_TAIL_UNAVAILABLE)
echo "== starting scry web on $WEB =="
SCRY_WEBUI_PASSWORD="$PASS" RUST_LOG=info "$SCRY" web \
  --listen "$WEB" \
  --queryd "live=$QQ"   --queryd-tail "live=$QT" \
  --queryd "plain=$QQ" \
  --queryd "refuse=$RQ" --queryd-tail "refuse=$RT" \
  >"$TMP/web.log" 2>&1 &
PIDS+=($!)
BASE="http://$WEB"
for _ in $(seq 1 100); do curl -sf -o /dev/null "$BASE/" && break; sleep 0.1; done
curl -sf -o /dev/null "$BASE/" || { cat "$TMP/web.log"; fail "scry web did not become ready"; }
ok "scry web serving on $BASE"

probe() { ( cd desktop && bun scripts/tail-probe.ts --base "$BASE" --password "$PASS" "$@" ); }

# ════════════════════════════════════════════════════════════════════
# 1. /api/targets reports the live capability (and no addresses)
# ════════════════════════════════════════════════════════════════════
echo "== 1: /api/targets live flags =="
probe --targets >"$TMP/targets.json" 2>"$TMP/probe_targets.log" \
  || { cat "$TMP/probe_targets.log" >&2; fail "targets probe failed"; }

python3 - "$TMP/targets.json" <<'PY' || fail "targets JSON did not report the expected live flags"
import json, sys
doc = json.load(open(sys.argv[1]))
by = {t["id"]: t for t in doc["targets"]}
assert set(by) == {"live", "plain", "refuse"}, by
assert by["live"]["live"] is True, "live target should advertise a tail endpoint"
assert by["refuse"]["live"] is True, "refuse target has a tail address (its queryd is what refuses)"
assert by["plain"]["live"] is False, "plain target has no tail address"
blob = json.dumps(doc)
for leak in ("127.0.0.1", ":1444"):
    assert leak not in blob, f"targets JSON leaked an address: {blob}"
print("targets ok")
PY
ok "targets report live true/true/false and leak no addresses"

# ════════════════════════════════════════════════════════════════════
# 2. The live target actually streams decoded records
# ════════════════════════════════════════════════════════════════════
echo "== 2: streaming through the web relay =="
probe --target live --matcher 'service="api"' --seconds 12 \
  >"$TMP/records.ndjson" 2>"$TMP/probe_live.log" &
PROBE_PID=$!
PIDS+=("$PROBE_PID")

for _ in $(seq 1 120); do grep -q subscribed "$TMP/probe_live.log" && break; sleep 0.1; done
grep -q subscribed "$TMP/probe_live.log" \
  || { cat "$TMP/probe_live.log" >&2; fail "TS client never subscribed through /api/tail"; }
ok "TS client subscribed (Hello+Subscribe pipelined through two relays)"

# Give the front-door a beat to discover + dial the ingester; records logged
# before the upstream subscription lands are dropped by design.
sleep 3
echo "-- spewing logs --"
"$SPEWER" --addr "$IA" --signals logs --rate "$SPEW_RATE" --max-batches "$BATCHES" \
  >"$TMP/spew.log" 2>&1 || fail "spew failed"

wait "$PROBE_PID" || fail "tail probe exited non-zero (see $TMP/probe_live.log)"

n=$(wc -l <"$TMP/records.ndjson")
[ "$n" -gt 0 ] || { cat "$TMP/probe_live.log" >&2; fail "TS client decoded no records"; }
ok "TS client decoded $n record(s) end-to-end"

python3 - "$TMP/records.ndjson" <<'PY' || fail "decoded records failed their shape/filter assertions"
import json, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
assert rows, "no rows"
for r in rows:
    svc = r["labels"].get("service")
    assert svc == "api", f"subscription filter leaked service={svc!r}"
    assert int(r["ts"]) > 0, f"record has no timestamp: {r}"
    assert isinstance(r["body"], str) and r["body"] != "", f"record has no body: {r}"
print(f"{len(rows)} rows ok")
PY
ok "every decoded record is service=api with a timestamp and a body"

# ════════════════════════════════════════════════════════════════════
# 3. A target with no tail address is refused at the web tier (409)
# ════════════════════════════════════════════════════════════════════
echo "== 3: target without a tail address =="
set +e
probe --target plain --seconds 5 >"$TMP/plain.ndjson" 2>"$TMP/probe_plain.log"
code=$?
set -e
[ "$code" = 3 ] || { cat "$TMP/probe_plain.log" >&2; fail "expected exit 3 (refused), got $code"; }
grep -q LiveUnavailable "$TMP/plain.ndjson" \
  || { cat "$TMP/plain.ndjson" >&2; fail "409 did not surface as a LiveUnavailableError"; }
ok "no tail address → 409 → LiveUnavailableError (the UI can explain itself)"

# ════════════════════════════════════════════════════════════════════
# 4. A Valkey-less queryd refuses on the wire, and the code survives both relays
# ════════════════════════════════════════════════════════════════════
echo "== 4: Valkey-less front-door =="
set +e
probe --target refuse --seconds 8 >"$TMP/refuse.ndjson" 2>"$TMP/probe_refuse.log"
code=$?
set -e
[ "$code" = 3 ] || { cat "$TMP/probe_refuse.log" >&2; fail "expected exit 3 (refused), got $code"; }
python3 - "$TMP/refuse.ndjson" <<'PY' || fail "refusal did not arrive as ERR_TAIL_UNAVAILABLE (9)"
import json, sys
doc = json.loads(open(sys.argv[1]).read().strip().splitlines()[-1])
assert doc["refused"] == "TailError", doc
assert doc["code"] == 9, doc
print("refusal ok")
PY
ok "Valkey-less front-door → ERR_TAIL_UNAVAILABLE (9) decoded by the TS client"

# No panics anywhere.
if grep -iq panicked "$TMP/ingest.log" "$TMP/queryd.log" "$TMP/refuse.log" "$TMP/web.log"; then
  fail "a daemon panicked (see $TMP/*.log)"
fi

echo
echo "ALL WEBUI-TAIL SMOKE CHECKS PASSED"
