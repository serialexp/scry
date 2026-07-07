#!/usr/bin/env bash
#
# smoke-tail-queryd.sh — end-to-end exercise of the queryd live-tail FRONT-DOOR
# (D-053): `scry tail --queryd` → `scry query --tail-listen` → Valkey ingester
# discovery → fan-in from N ingesters.
#
# Unlike the direct v1 path (smoke-tail.sh), the tail client here never names an
# ingester. It talks to the query daemon, which discovers the live ingesters
# from the Valkey registry (each ingester SET-heartbeats its advertised tail
# address) and dials each as a tail client, relaying their records back.
#
#   scry ingest --valkey-url … --tail-advertise-addr 127.0.0.1:IA   (A, no storage)
#   scry ingest --valkey-url … --tail-advertise-addr 127.0.0.1:IB   (B, no storage)
#         ▲ each SET scry/tail/ingesters/<uuid> = its addr, TTL-renewed
#         │
#   scry query --tail-listen 127.0.0.1:QT --valkey-url …            (front-door)
#         ▲ discover A,B from Valkey → dial each → fan-in
#         │  Hello → HelloAck → Subscribe → [TailRecord…]
#   scry tail --queryd 127.0.0.1:QT 'service="api"'
#
# Asserts:
#   1. Fan-in — with the tail subscribed and BOTH ingesters registered, spew to
#      A only (count rises), then to B only (count rises again). The second rise
#      can only come from B (A is idle), so the relay discovered and fanned in
#      from *both* ingesters.
#   2. Filter — the filtered tail ('service="api"') prints only service=api;
#      worker/scheduler are excluded, every line is well-formed.
#   3. Refuse — a `scry query --tail-listen` with NO Valkey refuses each
#      subscription with `ERR_TAIL_UNAVAILABLE` (code=9), not a silent empty
#      stream.
#
# Storage-free by design: the ingesters run WITHOUT --storage (the logs tap
# still fires), and the query daemon is pointed at a dummy object store with
# convergence effectively disabled (huge intervals), so **only a dev Valkey is
# required** — no Garage. Point it at the dev Valkey with SCRY_VALKEY_URL
# (default redis://127.0.0.1:6380 — this machine's `scry-valkey-smoke`).
#
# Env knobs: VALKEY_URL, IA/IB/QQ/QT/RQ/RT ports, SPEW_RATE (200), BATCHES (200).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

VALKEY_URL="${SCRY_VALKEY_URL:-redis://127.0.0.1:6380}"
IA="${IA:-127.0.0.1:14420}"          # ingester A ingest + tail port
IB="${IB:-127.0.0.1:14421}"          # ingester B ingest + tail port
QQ="${QQ:-127.0.0.1:14422}"          # queryd query port (unused, but required)
QT="${QT:-127.0.0.1:14423}"          # queryd tail-listen (front-door)
RQ="${RQ:-127.0.0.1:14424}"          # refuse-queryd query port
RT="${RT:-127.0.0.1:14425}"          # refuse-queryd tail-listen
SPEW_RATE="${SPEW_RATE:-200}"
BATCHES="${BATCHES:-200}"

TMP="$(mktemp -d)"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$TMP"; }
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  for f in ia ib queryd refuse tail_api; do
    [ -f "$TMP/$f.log" ] && { echo "---- $f.log (tail) ----" >&2; tail -20 "$TMP/$f.log" >&2; }
  done
  exit 1
}
ok() { echo "  ok: $*"; }

# Dummy object store — the tail relay never touches it; queryd only needs the
# env to parse. A refused endpoint means the single startup convergence poll
# fails fast with one warning and then the huge interval keeps it quiet.
export SCRY_OBJSTORE_ENDPOINT="http://127.0.0.1:1"
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
  # Clear any stray registry keys from a prior aborted run so discovery is
  # deterministic (keys embed ephemeral UUIDs + TTL-expire, but be tidy).
  keys=$("$VK" -u "$VALKEY_URL" --scan --pattern 'scry/tail/ingesters/*' 2>/dev/null || true)
  [ -n "$keys" ] && echo "$keys" | xargs -r "$VK" -u "$VALKEY_URL" del >/dev/null 2>&1 || true
else
  echo "note: no valkey-cli/redis-cli to pre-check $VALKEY_URL; proceeding" >&2
fi

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

# ════════════════════════════════════════════════════════════════════
# Phase 1 — fan-in from two discovered ingesters + filter
# ════════════════════════════════════════════════════════════════════
echo "== phase 1: fan-in + filter =="

echo "-- starting ingester A ($IA) and B ($IB), storage-less, registering in Valkey --"
RUST_LOG=info "$SCRY" ingest --listen "$IA" --valkey-url "$VALKEY_URL" \
  --tail-advertise-addr "$IA" --lease-ttl 10 >"$TMP/ia.log" 2>&1 &
PIDS+=($!)
RUST_LOG=info "$SCRY" ingest --listen "$IB" --valkey-url "$VALKEY_URL" \
  --tail-advertise-addr "$IB" --lease-ttl 10 >"$TMP/ib.log" 2>&1 &
PIDS+=($!)
wait_bind "$IA" || fail "ingester A never bound"
wait_bind "$IB" || fail "ingester B never bound"
ok "both ingesters listening"

# Confirm both registered in Valkey before the front-door tries to discover.
if [ -n "$VK" ]; then
  for _ in $(seq 1 50); do
    n=$("$VK" -u "$VALKEY_URL" --scan --pattern 'scry/tail/ingesters/*' 2>/dev/null | wc -l)
    [ "${n:-0}" -ge 2 ] && break
    sleep 0.1
  done
  n=$("$VK" -u "$VALKEY_URL" --scan --pattern 'scry/tail/ingesters/*' 2>/dev/null | wc -l)
  [ "${n:-0}" -ge 2 ] || fail "expected ≥2 tail registrations in Valkey, saw $n"
  ok "both ingesters registered in the Valkey tail registry ($n keys)"
fi

echo "-- starting query daemon front-door (tail-listen $QT) --"
RUST_LOG=info "$SCRY" query --listen "$QQ" --catalog "$TMP/queryd.sqlite" \
  --valkey-url "$VALKEY_URL" --tail-listen "$QT" --tail-rediscover-interval 1 \
  --poll-interval 999999 --full-walk-interval 999999 >"$TMP/queryd.log" 2>&1 &
PIDS+=($!)
wait_bind "$QT" || fail "queryd tail-listen never bound"
ok "front-door listening on $QT"

echo "-- starting filtered tail through the front-door (service=\"api\") --"
RUST_LOG=info "$SCRY" tail --queryd "$QT" 'service="api"' \
  >"$TMP/tail_api.out" 2>"$TMP/tail_api.log" &
PIDS+=($!)
# The client prints "subscribed" once its Subscribe reaches the relay.
for _ in $(seq 1 100); do grep -q subscribed "$TMP/tail_api.log" && break; sleep 0.1; done
grep -q subscribed "$TMP/tail_api.log" || fail "tail never subscribed to the front-door"
# Give the relay a beat to discover (≤1s tick) + dial both ingesters + let each
# upstream Subscribe land before we spew (records before a live sub are dropped).
sleep 3
ok "tail subscribed; relay had time to discover + dial both ingesters"

count_api() { grep -c 'service=api' "$TMP/tail_api.out" 2>/dev/null || echo 0; }

echo "-- spewing logs to A only --"
"$SPEWER" --addr "$IA" --signals logs --rate "$SPEW_RATE" --max-batches "$BATCHES" \
  >"$TMP/spewA.log" 2>&1 || fail "spew to A failed"
sleep 1
c_after_a=$(count_api)
[ "$c_after_a" -gt 0 ] || { cat "$TMP/tail_api.out" >&2; fail "tail received no service=api records after spewing A — fan-in from A broken"; }
ok "after spewing A: $c_after_a service=api records (A fanned in)"

echo "-- spewing logs to B only --"
"$SPEWER" --addr "$IB" --signals logs --rate "$SPEW_RATE" --max-batches "$BATCHES" \
  >"$TMP/spewB.log" 2>&1 || fail "spew to B failed"
sleep 1
c_after_b=$(count_api)
[ "$c_after_b" -gt "$c_after_a" ] \
  || fail "count did not rise after spewing B ($c_after_a → $c_after_b) — B not discovered/fanned in (A was idle, so the delta must be B's)"
ok "after spewing B: $c_after_b service=api records (delta ⇒ B fanned in)"

echo "-- filter assertions --"
! grep -q 'service=worker'    "$TMP/tail_api.out" || fail "filtered tail leaked service=worker"
! grep -q 'service=scheduler' "$TMP/tail_api.out" || fail "filtered tail leaked service=scheduler"
labelled=$(grep -c 'service=' "$TMP/tail_api.out" 2>/dev/null || echo 0)
[ "$labelled" = "$c_after_b" ] || fail "printed $labelled labelled lines but only $c_after_b were service=api"
ok "every printed line is service=api (worker/scheduler excluded)"

grep -Eq '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+([Zz]|[+-][0-9:]+) +(TRACE|DEBUG|INFO|WARN|ERROR|FATAL|-) +\{' \
  "$TMP/tail_api.out" || { head "$TMP/tail_api.out" >&2; fail "tail lines not in '<ts> <LEVEL> {labels} body' shape"; }
ok "tail lines are well-formed"

echo "== phase 1 PASSED =="

# ════════════════════════════════════════════════════════════════════
# Phase 2 — refuse without Valkey
# ════════════════════════════════════════════════════════════════════
echo "== phase 2: refuse-without-Valkey =="

echo "-- starting a query daemon with --tail-listen but NO --valkey-url --"
RUST_LOG=info "$SCRY" query --listen "$RQ" --catalog "$TMP/refuse.sqlite" \
  --tail-listen "$RT" --poll-interval 999999 --full-walk-interval 999999 \
  >"$TMP/refuse.log" 2>&1 &
PIDS+=($!)
wait_bind "$RT" || fail "refuse-queryd tail-listen never bound"
ok "refuse front-door listening on $RT (no Valkey)"

echo "-- tailing it; expect an ERR_TAIL_UNAVAILABLE (code=9) refusal --"
# The relay refuses the subscription and closes, so the client exits on its own;
# `timeout` is just a backstop against a hang regression.
timeout 15 "$SCRY" tail --queryd "$RT" 'service="api"' \
  >"$TMP/refuse_tail.out" 2>"$TMP/refuse_tail.log" || true

grep -q 'code=9' "$TMP/refuse_tail.log" \
  || { cat "$TMP/refuse_tail.log" >&2; fail "expected a code=9 (ERR_TAIL_UNAVAILABLE) refusal from the Valkey-less front-door"; }
grep -qi 'requires Valkey' "$TMP/refuse_tail.log" \
  || { cat "$TMP/refuse_tail.log" >&2; fail "refusal did not carry the 'requires Valkey' reason"; }
ok "Valkey-less front-door refused the tail with ERR_TAIL_UNAVAILABLE"

# The refusing tail must NOT have streamed any records.
[ ! -s "$TMP/refuse_tail.out" ] || { cat "$TMP/refuse_tail.out" >&2; fail "refuse path leaked records to stdout"; }
ok "no records leaked on the refuse path"

echo "== phase 2 PASSED =="

# No panics in any daemon log.
if grep -iq panicked "$TMP/ia.log" "$TMP/ib.log" "$TMP/queryd.log" "$TMP/refuse.log"; then
  fail "a daemon panicked (see $TMP/*.log)"
fi

echo
echo "ALL QUERYD-TAIL SMOKE CHECKS PASSED"
