#!/usr/bin/env bash
#
# smoke-webui.sh — end-to-end exercise of the scry web browser server.
#
# Builds the SolidJS bundle + the release scry web binary (so the rust-embed
# single-binary asset path is what's tested), stands up a tiny fake scry query,
# and asserts the whole web surface:
#   - GET /                  serves the embedded SPA
#   - GET /api/me            401 when unauthenticated
#   - POST /api/login        401 on wrong password (no cookie)
#   - POST /api/query        401 when unauthenticated (auth gate before dial)
#   - POST /api/login        204 + signed cookie on the right password
#   - GET /api/me            204 with the cookie
#   - GET /api/targets       401 unauth; lists both configured targets + default
#   - POST /api/query        relays bytes to the DEFAULT upstream (no header)
#   - POST /api/query        X-Scry-Target routes to the SELECTED upstream
#   - POST /api/query        unknown X-Scry-Target → 400
#   - POST /api/logout       clears the session (subsequent /api/me → 401)
#
# Two distinct stub scry-queryds are stood up (each replies a unique marker) so
# the target-routing assertions prove the header actually selects the upstream.
#
# The real query *protocol* round-trip (framing/Arrow) is covered by the
# scry web Rust integration tests and by scripts/smoke.sh's per-signal query
# legs; here the upstreams are stubs so the focus stays on the web layer.
#
# Env knobs: SCRY_WEBUI_PASSWORD (default "smoke-secret"), WEBUI_PORT (18080),
# FAKE_QUERYD_PORT (14199), FAKE_QUERYD_PORT2 (14200).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

PASS="${SCRY_WEBUI_PASSWORD:-smoke-secret}"
WEBUI_PORT="${WEBUI_PORT:-18080}"
FAKE_PORT="${FAKE_QUERYD_PORT:-14199}"
FAKE_PORT2="${FAKE_QUERYD_PORT2:-14200}"
MARKER="PONG-FROM-LOCAL-QUERYD"
MARKER2="PONG-FROM-GOTHAB-QUERYD"

COOKIE_JAR=$(mktemp)
TMP=$(mktemp -d)
WEBUI_PID=""
FAKE_PID=""
FAKE_PID2=""

cleanup() {
  [ -n "$WEBUI_PID" ] && kill "$WEBUI_PID" 2>/dev/null || true
  [ -n "$FAKE_PID" ] && kill "$FAKE_PID" 2>/dev/null || true
  [ -n "$FAKE_PID2" ] && kill "$FAKE_PID2" 2>/dev/null || true
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

# Guard against frontend-version drift: `bun run build` stamps the displayed
# version from the workspace Cargo.toml (scripts/stamp-version.mjs), so the built
# bundle must contain that exact version. Catches a broken/removed stamp step.
WS_VERSION=$(sed -n '/^\[workspace.package\]/,/^\[/{s/^version *= *"\([^"]*\)".*/\1/p}' Cargo.toml | head -1)
[ -n "$WS_VERSION" ] || fail "could not read [workspace.package].version from Cargo.toml"
grep -rqF "$WS_VERSION" desktop/dist/assets \
  || fail "built bundle does not contain workspace version $WS_VERSION (stamp step broken?)"
ok "bundle carries workspace version $WS_VERSION"

echo "== building release scry web (embeds the bundle) =="
cargo build --release -p scry >"$TMP/cargo.log" 2>&1 \
  || { cat "$TMP/cargo.log"; fail "scry-webui build failed"; }

# A tiny stub scry query: accept connections, drain the relayed request, reply
# a fixed marker, close (so the relay's read_to_end terminates).
FAKE_PY='
import socket, sys
port = int(sys.argv[1]); marker = sys.argv[2].encode()
srv = socket.socket()
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", port)); srv.listen(16)
while True:
    conn, _ = srv.accept()
    try:
        conn.recv(1 << 16)
        conn.sendall(marker)
    finally:
        conn.close()
'

echo "== starting fake scry query 'local' on 127.0.0.1:$FAKE_PORT =="
python3 -c "$FAKE_PY" "$FAKE_PORT" "$MARKER" >"$TMP/fake.log" 2>&1 &
FAKE_PID=$!

echo "== starting fake scry query 'gothab' on 127.0.0.1:$FAKE_PORT2 =="
python3 -c "$FAKE_PY" "$FAKE_PORT2" "$MARKER2" >"$TMP/fake2.log" 2>&1 &
FAKE_PID2=$!

echo "== starting scry web on 127.0.0.1:$WEBUI_PORT (two named targets) =="
SCRY_WEBUI_PASSWORD="$PASS" ./target/release/scry web \
  --listen "127.0.0.1:$WEBUI_PORT" \
  --queryd "local=127.0.0.1:$FAKE_PORT" \
  --queryd "gothab=127.0.0.1:$FAKE_PORT2" \
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

# 7. /api/targets unauthenticated → 401 (names don't leak before login)
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/targets")
[ "$code" = 401 ] || fail "/api/targets (unauth) expected 401, got $code"
ok "/api/targets unauthenticated → 401"

# 8. /api/targets with cookie → lists both ids + default, never the raw address
code=$(curl -s -o "$TMP/targets.json" -w '%{http_code}' -b "$COOKIE_JAR" "$BASE/api/targets")
[ "$code" = 200 ] || fail "/api/targets (auth) expected 200, got $code"
grep -q '"id":"local"'  "$TMP/targets.json" || fail "/api/targets missing 'local' id ($(cat "$TMP/targets.json"))"
grep -q '"id":"gothab"' "$TMP/targets.json" || fail "/api/targets missing 'gothab' id ($(cat "$TMP/targets.json"))"
grep -q '"default":"local"' "$TMP/targets.json" || fail "/api/targets default should be 'local' ($(cat "$TMP/targets.json"))"
! grep -q "$FAKE_PORT" "$TMP/targets.json" || fail "/api/targets leaked the raw upstream address"
ok "/api/targets → lists local+gothab, default local, no raw addr"

# 9. /api/query with no header → routes to the DEFAULT target (local)
code=$(curl -s -o "$TMP/qresp" -w '%{http_code}' -b "$COOKIE_JAR" \
  -X POST --data-binary 'PING' "$BASE/api/query")
[ "$code" = 200 ] || fail "/api/query (default) expected 200, got $code"
got=$(cat "$TMP/qresp")
[ "$got" = "$MARKER" ] || fail "/api/query default expected '$MARKER', got '$got'"
ok "/api/query no header → default target ('$got')"

# 10. /api/query X-Scry-Target: local → the local upstream
code=$(curl -s -o "$TMP/qresp" -w '%{http_code}' -b "$COOKIE_JAR" \
  -H "X-Scry-Target: local" -X POST --data-binary 'PING' "$BASE/api/query")
[ "$code" = 200 ] || fail "/api/query (local) expected 200, got $code"
got=$(cat "$TMP/qresp")
[ "$got" = "$MARKER" ] || fail "/api/query local expected '$MARKER', got '$got'"
ok "/api/query X-Scry-Target=local → local upstream ('$got')"

# 11. /api/query X-Scry-Target: gothab → the OTHER upstream (proves routing)
code=$(curl -s -o "$TMP/qresp" -w '%{http_code}' -b "$COOKIE_JAR" \
  -H "X-Scry-Target: gothab" -X POST --data-binary 'PING' "$BASE/api/query")
[ "$code" = 200 ] || fail "/api/query (gothab) expected 200, got $code"
got=$(cat "$TMP/qresp")
[ "$got" = "$MARKER2" ] || fail "/api/query gothab expected '$MARKER2', got '$got'"
ok "/api/query X-Scry-Target=gothab → gothab upstream ('$got')"

# 12. /api/query unknown target → 400 (id not in the allowlist)
code=$(curl -s -o /dev/null -w '%{http_code}' -b "$COOKIE_JAR" \
  -H "X-Scry-Target: nope" -X POST --data-binary 'PING' "$BASE/api/query")
[ "$code" = 400 ] || fail "/api/query (unknown target) expected 400, got $code"
ok "/api/query X-Scry-Target=nope → 400"

# 13. logout clears the session
curl -s -o /dev/null -b "$COOKIE_JAR" -c "$COOKIE_JAR" -X POST "$BASE/api/logout"
code=$(curl -s -o /dev/null -w '%{http_code}' -b "$COOKIE_JAR" "$BASE/api/me")
[ "$code" = 401 ] || fail "/api/me after logout expected 401, got $code"
ok "logout → session cleared (/api/me → 401)"

echo
echo "ALL WEBUI SMOKE CHECKS PASSED"
