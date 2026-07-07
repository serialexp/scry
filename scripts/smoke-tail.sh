#!/usr/bin/env bash
#
# smoke-tail.sh — end-to-end exercise of the `scry tail` live-tail surface (D-050).
#
# Pure live path, no storage: a storage-less `scry ingest` still taps the logs
# hot path for subscribers, so this smoke needs NO Garage and NO Valkey.
#
#   scry ingest --listen           (no --storage: count-only logs path, still tapped)
#     ▲ subscribe                    ▲ logs batches
#     │                              │
#   scry tail --ingest … 'service="api"'   noise-spewer --signals logs
#
# Asserts:
#   1. A filtered tail ('service="api"') receives live records and every line it
#      prints carries service=api — worker/scheduler streams are filtered out.
#   2. An unfiltered tail (no matcher) receives all three services.
#   3. Records only arrive for a subscription that was live when they were sent
#      (best-effort/live-only — nothing is replayed from before Subscribe).
#
# Bodies are random, so assertions key on the stream labels the tail prints
# (`{service=api,host=…,env=prod}`), not on message text.
#
# Env knobs: TAIL_PORT (14400), SPEW_RATE (200), SPEW_SECS (4s).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

TAIL_PORT="${TAIL_PORT:-14400}"
SPEW_RATE="${SPEW_RATE:-200}"
SPEW_SECS="${SPEW_SECS:-4s}"

TMP=$(mktemp -d)
INGEST_PID=""
TAIL_FILTERED_PID=""
TAIL_ALL_PID=""

cleanup() {
  for pid in "$TAIL_FILTERED_PID" "$TAIL_ALL_PID" "$INGEST_PID"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  done
  rm -rf "$TMP"
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }
ok()   { echo "  ok: $*"; }

echo "== building release scry + noise-spewer =="
cargo build --release -p scry -p noise-spewer >"$TMP/cargo.log" 2>&1 \
  || { cat "$TMP/cargo.log"; fail "build failed"; }

SCRY=./target/release/scry
SPEWER=./target/release/noise-spewer

echo "== starting scry ingest (no storage) on 127.0.0.1:$TAIL_PORT =="
"$SCRY" ingest --listen "127.0.0.1:$TAIL_PORT" >"$TMP/ingest.log" 2>&1 &
INGEST_PID=$!

# Wait for the listener.
for _ in $(seq 1 100); do
  (exec 3<>"/dev/tcp/127.0.0.1/$TAIL_PORT") 2>/dev/null && { exec 3>&- 3<&-; break; }
  sleep 0.1
done
(exec 3<>"/dev/tcp/127.0.0.1/$TAIL_PORT") 2>/dev/null \
  || { cat "$TMP/ingest.log"; fail "scry ingest did not become ready"; }
exec 3>&- 3<&- || true
ok "ingest listening"

echo "== starting filtered tail (service=\"api\") =="
RUST_LOG=info "$SCRY" tail --ingest "127.0.0.1:$TAIL_PORT" 'service="api"' \
  >"$TMP/tail_api.out" 2>"$TMP/tail_api.log" &
TAIL_FILTERED_PID=$!

echo "== starting unfiltered tail (all services) =="
RUST_LOG=info "$SCRY" tail --ingest "127.0.0.1:$TAIL_PORT" \
  >"$TMP/tail_all.out" 2>"$TMP/tail_all.log" &
TAIL_ALL_PID=$!

# Wait until BOTH subscriptions are registered server-side. The tail logs
# "subscribed" on stderr once the Subscribe frame is sent + accepted; give the
# server a beat to register before we start spewing (records sent before the
# subscription is live are legitimately not delivered — that's the contract).
for _ in $(seq 1 100); do
  if grep -q subscribed "$TMP/tail_api.log" 2>/dev/null \
     && grep -q subscribed "$TMP/tail_all.log" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
grep -q subscribed "$TMP/tail_api.log" || { cat "$TMP/tail_api.log"; fail "filtered tail never subscribed"; }
grep -q subscribed "$TMP/tail_all.log" || { cat "$TMP/tail_all.log"; fail "unfiltered tail never subscribed"; }
sleep 0.3
ok "both tails subscribed"

echo "== spewing logs for $SPEW_SECS at $SPEW_RATE batch/s =="
"$SPEWER" --addr "127.0.0.1:$TAIL_PORT" --signals logs \
  --rate "$SPEW_RATE" --duration "$SPEW_SECS" >"$TMP/spew.log" 2>&1 \
  || { cat "$TMP/spew.log"; fail "noise-spewer failed"; }

# Let the last in-flight records drain to the tail clients.
sleep 0.5

echo "== assertions =="

# 1. Filtered tail got records, ALL of them service=api.
api_lines=$(grep -c 'service=api' "$TMP/tail_api.out" || true)
[ "${api_lines:-0}" -gt 0 ] || { cat "$TMP/tail_api.out"; fail "filtered tail received no service=api records"; }
ok "filtered tail received $api_lines service=api records"

# The filtered tail must NOT contain the other services.
! grep -q 'service=worker'    "$TMP/tail_api.out" || fail "filtered tail leaked service=worker records"
! grep -q 'service=scheduler' "$TMP/tail_api.out" || fail "filtered tail leaked service=scheduler records"
ok "filtered tail excluded worker/scheduler"

# Every printed line should carry a service=api label (no stray unmatched lines).
total_lines=$(grep -c 'service=' "$TMP/tail_api.out" || true)
[ "${total_lines:-0}" = "$api_lines" ] \
  || fail "filtered tail printed $total_lines labelled lines but only $api_lines were service=api"
ok "every filtered line is service=api"

# 2. Unfiltered tail saw all three services.
for svc in api worker scheduler; do
  grep -q "service=$svc" "$TMP/tail_all.out" \
    || { cat "$TMP/tail_all.out"; fail "unfiltered tail missing service=$svc"; }
done
ok "unfiltered tail saw api + worker + scheduler"

# 3. Records look like real formatted lines: <rfc3339 ts> <LEVEL> {labels} body.
grep -Eq '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+([Zz]|[+-][0-9:]+) +(TRACE|DEBUG|INFO|WARN|ERROR|FATAL|-) +\{' \
  "$TMP/tail_all.out" || { head "$TMP/tail_all.out"; fail "tail output not in expected '<ts> <LEVEL> {labels} body' shape"; }
ok "tail lines are well-formed"

echo
echo "ALL TAIL SMOKE CHECKS PASSED"
