#!/usr/bin/env bash
#
# smoke-tail-metrics.sh — end-to-end exercise of the METRICS live tail (D-065):
# `scry tail --signal metrics` over both paths — direct to an ingester, and
# through the `scry query --tail-listen` front-door with Valkey discovery.
#
# Metrics are the second tailable signal. They do not reuse the logs record
# frame: a sample is a float, not a body, so it rides its own `TailSample`
# (tag 0x54) with the series' type, fingerprint and value. This script is the
# proof that the frame survives the whole chain — ingest tap → wire → relay →
# CLI — and that the logs tail is unaffected by its arrival.
#
#   scry ingest --valkey-url … --tail-advertise-addr 127.0.0.1:IA  (no storage)
#         ▲ SET <ns>/tail/ingesters/<uuid> = its addr, TTL-renewed
#         │
#   scry query --tail-listen 127.0.0.1:QT --valkey-url …           (front-door)
#         ▲ discover → dial → relay
#   scry tail --queryd 127.0.0.1:QT --signal metrics 'env="dev"'
#
# Asserts:
#   1. Direct — `--ingest --signal metrics` streams samples, each line shaped
#      `<rfc3339> <metric-name> {labels} <value>` with a parseable float.
#   2. Filter — `env="dev"` prints only env=dev series; other envs excluded.
#   3. Front-door — the same subscription through queryd's relay also streams,
#      proving the relay forwards TailSample and not just TailRecord.
#   4. Logs unaffected — a logs tail against the same ingester still yields
#      well-formed log lines and NO metric lines (the two signals do not
#      cross-deliver; the registry snapshot is per-signal).
#   5. Untailable signals are refused with a clear error rather than a silent
#      connection that streams nothing.
#
# Storage-free by design: the ingester runs WITHOUT --storage (the metrics tap
# still fires on the count-only path), and the query daemon uses the
# reachable-but-empty stub object store from `lib/stub-objstore.sh` with
# convergence effectively disabled, so **only a dev Valkey is required** — no
# Garage. Point it at one with SCRY_VALKEY_URL (default redis://127.0.0.1:6380
# — the long-lived `scry-valkey-smoke` container).
#
# Env knobs: SCRY_VALKEY_URL, IA/QQ/QT/S3_PORT ports, SPEW_RATE, BATCHES.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

VALKEY_URL="${SCRY_VALKEY_URL:-redis://127.0.0.1:6380}"
IA="${IA:-127.0.0.1:14440}"            # ingester ingest + tail port
QQ="${QQ:-127.0.0.1:14441}"            # queryd query port (bound, unused)
QT="${QT:-127.0.0.1:14442}"            # queryd tail-listen (front-door)
S3_PORT="${S3_PORT:-127.0.0.1:14443}"  # stub object store (always-empty bucket)
SPEW_RATE="${SPEW_RATE:-200}"
BATCHES="${BATCHES:-200}"

TMP="$(mktemp -d)"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$TMP"; }
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  for f in ingest queryd tail_direct tail_relay tail_logs; do
    [ -f "$TMP/$f.log" ] && { echo "---- $f.log (tail) ----" >&2; tail -20 "$TMP/$f.log" >&2; }
  done
  exit 1
}
ok() { echo "  ok: $*"; }

# A reachable-but-empty object store: queryd will not start until its catalog
# has seeded from the bucket, so the endpoint has to answer even though the
# tail relay never reads a block.
# shellcheck source=lib/stub-objstore.sh
. "$ROOT/scripts/lib/stub-objstore.sh"
start_stub_objstore "$S3_PORT" scry-smoke "$TMP/stub-s3.log" || fail "stub object store did not start"
PIDS+=("$STUB_OBJSTORE_PID")

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

wait_subscribed() {
  local logfile=$1
  for _ in $(seq 1 100); do grep -q subscribed "$logfile" && return 0; sleep 0.1; done
  return 1
}

echo "-- starting storage-less ingester ($IA), registering in Valkey --"
RUST_LOG=info "$SCRY" ingest --listen "$IA" --valkey-url "$VALKEY_URL" \
  --tail-advertise-addr "$IA" --lease-ttl 10 >"$TMP/ingest.log" 2>&1 &
PIDS+=($!)
wait_bind "$IA" || fail "ingester never bound"
ok "ingester listening on $IA (no --storage: the count-only metrics path is still tapped)"

# ════════════════════════════════════════════════════════════════════
# Phase 1 — direct metrics tail + filter
# ════════════════════════════════════════════════════════════════════
echo "== phase 1: direct metrics tail =="

RUST_LOG=info "$SCRY" tail --ingest "$IA" --signal metrics 'env="dev"' \
  >"$TMP/tail_direct.out" 2>"$TMP/tail_direct.log" &
PIDS+=($!)
wait_subscribed "$TMP/tail_direct.log" || fail "metrics tail never subscribed to the ingester"
sleep 1
ok "metrics tail subscribed directly"

echo "-- spewing metrics --"
"$SPEWER" --addr "$IA" --signals metrics --rate "$SPEW_RATE" --max-batches "$BATCHES" \
  >"$TMP/spew.log" 2>&1 || fail "metrics spew failed"
sleep 2

n_direct=$(wc -l < "$TMP/tail_direct.out")
[ "$n_direct" -gt 0 ] || { tail -20 "$TMP/tail_direct.log" >&2; fail "direct metrics tail received no samples"; }
ok "direct tail received $n_direct samples"

# Line shape: <rfc3339> <metric-name> {labels} <value>. The value must parse as
# a float — that is the whole point of TailSample over a stringified body.
head -1 "$TMP/tail_direct.out" | grep -Eq \
  '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+([Zz]|[+-][0-9:]+) +[A-Za-z_][A-Za-z0-9_]* +\{.*\} +-?[0-9]' \
  || { head -3 "$TMP/tail_direct.out" >&2; fail "sample lines not in '<ts> <metric> {labels} <value>' shape"; }
ok "sample lines are well-formed"

# Every line's last field must be a number.
bad=$(awk '{ v=$NF; if (v+0 != v && v !~ /^-?[0-9]/) print }' "$TMP/tail_direct.out" | head -3)
[ -z "$bad" ] || { echo "$bad" >&2; fail "some sample values are not numeric"; }
ok "every sample value parses as a number"

# The metric name is lifted out of the labels, so __name__ should not appear in
# the brace group.
! grep -q '__name__' "$TMP/tail_direct.out" \
  || fail "__name__ leaked into the label group instead of the name slot"
ok "__name__ rendered in the name slot, not the label group"

echo "-- filter assertions --"
n_dev=$(grep -c 'env=dev' "$TMP/tail_direct.out" 2>/dev/null || echo 0)
[ "$n_dev" = "$n_direct" ] || fail "printed $n_direct lines but only $n_dev were env=dev"
! grep -qE 'env=(prod|staging)' "$TMP/tail_direct.out" || fail "filtered metrics tail leaked a non-dev env"
ok "every printed sample is env=dev (other envs excluded)"

echo "== phase 1 PASSED =="

# ════════════════════════════════════════════════════════════════════
# Phase 2 — the same subscription through the queryd front-door
# ════════════════════════════════════════════════════════════════════
echo "== phase 2: metrics tail through the queryd front-door =="

RUST_LOG=info "$SCRY" query --listen "$QQ" --catalog "$TMP/queryd.sqlite" \
  --valkey-url "$VALKEY_URL" --tail-listen "$QT" --tail-rediscover-interval 1 \
  --poll-interval 999999 --full-walk-interval 999999 >"$TMP/queryd.log" 2>&1 &
PIDS+=($!)
wait_bind "$QT" || fail "queryd tail-listen never bound"
ok "front-door listening on $QT"

RUST_LOG=info "$SCRY" tail --queryd "$QT" --signal metrics 'env="dev"' \
  >"$TMP/tail_relay.out" 2>"$TMP/tail_relay.log" &
PIDS+=($!)
wait_subscribed "$TMP/tail_relay.log" || fail "metrics tail never subscribed to the front-door"
# Let the relay discover (≤1s tick), dial, and land its upstream Subscribe.
sleep 3
ok "metrics tail subscribed through the relay"

"$SPEWER" --addr "$IA" --signals metrics --rate "$SPEW_RATE" --max-batches "$BATCHES" \
  >"$TMP/spew2.log" 2>&1 || fail "second metrics spew failed"
sleep 2

n_relay=$(wc -l < "$TMP/tail_relay.out")
[ "$n_relay" -gt 0 ] \
  || { tail -20 "$TMP/queryd.log" >&2; fail "front-door relayed no metric samples — TailSample not forwarded"; }
ok "front-door relayed $n_relay samples"

n_relay_dev=$(grep -c 'env=dev' "$TMP/tail_relay.out" 2>/dev/null || echo 0)
[ "$n_relay_dev" = "$n_relay" ] || fail "relay leaked non-dev samples ($n_relay_dev/$n_relay were env=dev)"
ok "the relay honours the subscription filter"

echo "== phase 2 PASSED =="

# ════════════════════════════════════════════════════════════════════
# Phase 3 — logs are unaffected, and untailable signals are refused
# ════════════════════════════════════════════════════════════════════
echo "== phase 3: logs unaffected + untailable signals refused =="

RUST_LOG=info "$SCRY" tail --ingest "$IA" 'service="api"' \
  >"$TMP/tail_logs.out" 2>"$TMP/tail_logs.log" &
PIDS+=($!)
wait_subscribed "$TMP/tail_logs.log" || fail "logs tail never subscribed"
sleep 1

"$SPEWER" --addr "$IA" --signals logs --rate "$SPEW_RATE" --max-batches "$BATCHES" \
  >"$TMP/spew3.log" 2>&1 || fail "logs spew failed"
sleep 2

n_logs=$(wc -l < "$TMP/tail_logs.out")
[ "$n_logs" -gt 0 ] || fail "logs tail received nothing — the metrics work regressed the logs path"
grep -Eq '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+([Zz]|[+-][0-9:]+) +(TRACE|DEBUG|INFO|WARN|ERROR|FATAL|-) +\{' \
  "$TMP/tail_logs.out" || { head -3 "$TMP/tail_logs.out" >&2; fail "log lines lost their '<ts> <LEVEL> {labels} body' shape"; }
ok "logs tail still yields $n_logs well-formed log lines"

# A logs subscription must not receive metric samples: the registry snapshot is
# taken per signal, so the two never cross-deliver.
! grep -qE '^\S+ +scry_[a-z_]+ +\{' "$TMP/tail_logs.out" \
  || fail "logs tail received metric samples — signals are cross-delivering"
ok "no metric samples leaked into the logs tail"

echo "-- an untailable signal must be refused, not silently empty --"
timeout 10 "$SCRY" tail --ingest "$IA" --signal traces \
  >"$TMP/tail_traces.out" 2>"$TMP/tail_traces.log" || true
grep -qiE "unsupported --signal|expected 'logs' or 'metrics'" "$TMP/tail_traces.log" \
  || { cat "$TMP/tail_traces.log" >&2; fail "a traces tail should be rejected with a clear message"; }
[ ! -s "$TMP/tail_traces.out" ] || fail "the refused traces tail printed records"
ok "traces tail refused with a clear message and streamed nothing"

echo "== phase 3 PASSED =="

if grep -iq panicked "$TMP/ingest.log" "$TMP/queryd.log"; then
  fail "a daemon panicked (see $TMP/*.log)"
fi

echo
echo "ALL METRICS-TAIL SMOKE CHECKS PASSED"
