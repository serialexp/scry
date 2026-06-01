#!/usr/bin/env bash
#
# smoke-webui.sh — end-to-end exercise of the scry-webui browser server.
#
# Builds the SolidJS bundle + the release scry-webui binary (so the rust-embed
# single-binary asset path is what's tested), stands up a tiny fake scry-queryd,
# and asserts the whole web surface:
#   - GET /                  serves the embedded SPA
#   - GET /api/me            401 when unauthenticated
#   - POST /api/login        401 on wrong password (no cookie)
#   - POST /api/query        401 when unauthenticated (auth gate before dial)
#   - POST /api/login        204 + signed cookie on the right password
#   - GET /api/me            204 with the cookie
#   - POST /api/query        relays bytes to the upstream and streams the reply
#   - POST /api/logout       clears the session (subsequent /api/me → 401)
#
# The real query *protocol* round-trip (framing/Arrow) is covered by the
# scry-webui Rust integration tests and by scripts/smoke.sh's per-signal query
# legs; here the upstream is a stub so the focus stays on the web layer.
#
# Env knobs: SCRY_WEBUI_PASSWORD (default "smoke-secret"), WEBUI_PORT (18080),
# FAKE_QUERYD_PORT (14199).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

PASS="${SCRY_WEBUI_PASSWORD:-smoke-secret}"
WEBUI_PORT="${WEBUI_PORT:-18080}"
FAKE_PORT="${FAKE_QUERYD_PORT:-14199}"
MARKER="PONG-FROM-FAKE-QUERYD"

COOKIE_JAR=$(mktemp)
TMP=$(mktemp -d)
WEBUI_PID=""
FAKE_PID=""

cleanup() {
  [ -n "$WEBUI_PID" ] && kill "$WEBUI_PID" 2>/dev/null || true
  [ -n "$FAKE_PID" ] && kill "$FAKE_PID" 2>/dev/null || true
  rm -rf "$COOKIE_JAR" "$TMP"
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }
ok()   { echo "  ok: $*"; }

command -v bun >/dev/null     || fail "bun not found (needed to build the frontend)"
command -v python3 >/dev/null || fail "python3 not found (needed for the fake queryd)"
command -v curl >/dev/null    || fail "curl not found"

echo "== building frontend bundle (desktop/dist) =="
( cd desktop && bun run build ) >"$TMP/fe-build.log" 2>&1 \
  || { cat "$TMP/fe-build.log"; fail "frontend build failed"; }

echo "== building release scry-webui (embeds the bundle) =="
cargo build --release -p scry-webui >"$TMP/cargo.log" 2>&1 \
  || { cat "$TMP/cargo.log"; fail "scry-webui build failed"; }

echo "== starting fake scry-queryd on 127.0.0.1:$FAKE_PORT =="
python3 - "$FAKE_PORT" "$MARKER" >"$TMP/fake.log" 2>&1 <<'PY' &
import socket, sys
port = int(sys.argv[1]); marker = sys.argv[2].encode()
srv = socket.socket()
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", port)); srv.listen(16)
while True:
    conn, _ = srv.accept()
    try:
        conn.recv(1 << 16)   # consume the relayed request frame
        conn.sendall(marker) # reply, then close → relay's read_to_end ends
    finally:
        conn.close()
PY
FAKE_PID=$!

echo "== starting scry-webui on 127.0.0.1:$WEBUI_PORT =="
SCRY_WEBUI_PASSWORD="$PASS" ./target/release/scry-webui \
  --listen "127.0.0.1:$WEBUI_PORT" --queryd "127.0.0.1:$FAKE_PORT" \
  >"$TMP/webui.log" 2>&1 &
WEBUI_PID=$!

BASE="http://127.0.0.1:$WEBUI_PORT"
for _ in $(seq 1 100); do
  curl -sf -o /dev/null "$BASE/" && break
  sleep 0.1
done
curl -sf -o /dev/null "$BASE/" || { cat "$TMP/webui.log"; fail "scry-webui did not become ready"; }

echo "== assertions =="

# 1. SPA served at /
code=$(curl -s -o "$TMP/idx.html" -w '%{http_code}' "$BASE/")
[ "$code" = 200 ] || fail "GET / expected 200, got $code"
grep -qi 'html' "$TMP/idx.html" || fail "GET / did not return HTML"
ok "GET / serves the embedded SPA"

# 2. unauthenticated /api/me → 401
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/me")
[ "$code" = 401 ] || fail "/api/me (unauth) expected 401, got $code"
ok "/api/me unauthenticated → 401"

# 3. wrong password → 401, no cookie
rm -f "$TMP/badjar"
code=$(curl -s -o /dev/null -w '%{http_code}' -c "$TMP/badjar" \
  -X POST -H 'content-type: application/json' \
  -d '{"password":"definitely-wrong"}' "$BASE/api/login")
[ "$code" = 401 ] || fail "login (wrong) expected 401, got $code"
! grep -q scry_session "$TMP/badjar" 2>/dev/null || fail "wrong login set a session cookie"
ok "login wrong password → 401, no cookie"

# 4. /api/query before auth → 401 (gate runs before any upstream dial)
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST --data-binary 'PING' "$BASE/api/query")
[ "$code" = 401 ] || fail "/api/query (unauth) expected 401, got $code"
ok "/api/query unauthenticated → 401"

# 5. correct password → 204 + signed cookie
code=$(curl -s -o /dev/null -w '%{http_code}' -c "$COOKIE_JAR" \
  -X POST -H 'content-type: application/json' \
  -d "{\"password\":\"$PASS\"}" "$BASE/api/login")
[ "$code" = 204 ] || fail "login (correct) expected 204, got $code"
grep -q scry_session "$COOKIE_JAR" || fail "login did not set a session cookie"
ok "login correct password → 204 + cookie"

# 6. /api/me with the cookie → 204
code=$(curl -s -o /dev/null -w '%{http_code}' -b "$COOKIE_JAR" "$BASE/api/me")
[ "$code" = 204 ] || fail "/api/me (auth) expected 204, got $code"
ok "/api/me authenticated → 204"

# 7. /api/query with the cookie → relays to the upstream, returns its bytes
code=$(curl -s -o "$TMP/qresp" -w '%{http_code}' -b "$COOKIE_JAR" \
  -X POST --data-binary 'PING' "$BASE/api/query")
[ "$code" = 200 ] || fail "/api/query (auth) expected 200, got $code"
got=$(cat "$TMP/qresp")
[ "$got" = "$MARKER" ] || fail "/api/query relay expected '$MARKER', got '$got'"
ok "/api/query authenticated → relayed to upstream ('$got')"

# 8. logout clears the session
curl -s -o /dev/null -b "$COOKIE_JAR" -c "$COOKIE_JAR" -X POST "$BASE/api/logout"
code=$(curl -s -o /dev/null -w '%{http_code}' -b "$COOKIE_JAR" "$BASE/api/me")
[ "$code" = 401 ] || fail "/api/me after logout expected 401, got $code"
ok "logout → session cleared (/api/me → 401)"

echo
echo "ALL WEBUI SMOKE CHECKS PASSED"
