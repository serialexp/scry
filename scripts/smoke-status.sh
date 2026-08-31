#!/usr/bin/env bash
#
# smoke-status.sh — end-to-end exercise of daemon/gateway status pages + the
# Valkey-aggregated fleet view (D-057, D-067).
#
# Both `scry ingest` and `scry query` expose an opt-in status HTTP endpoint
# (`--stats-listen`, bare ⇒ 127.0.0.1:4098). When both are on the same Valkey,
# each heartbeats its FULL status snapshot into `scry/status/<uuid>`, and either
# page renders the whole fleet from Redis (self included), marking which entry
# is the local reporter.
#
#   scry ingest --stats-listen 127.0.0.1:SI --valkey-url …   (storage-less)
#         └── heartbeats snapshot → scry/status/<ingest-uuid>
#   scry query  --stats-listen 127.0.0.1:SQ --valkey-url …
#         └── heartbeats snapshot → scry/status/<query-uuid>
#
# Asserts:
#   1. Fleet from either side — GET /stats.json on the INGEST page lists BOTH
#      the ingest and the query instance (source:"mixed" for the ingester, which
#      also merges locally-reported agents; "valkey" for queryd); ditto the QUERY
#      page. Each instance is tagged with the correct role, and each page's
#      self_id matches the instance serving it.
#   2. Local fallback — a `scry query --stats-listen` with NO Valkey serves a
#      one-entry page (source:"local"), role "query", self_id = that instance.
#
# Storage-free by design: the ingester runs WITHOUT --storage (the status page
# only needs the counters), and the query daemon is pointed at a dummy object
# store with convergence effectively disabled (huge intervals), so **only a dev
# Valkey is required** — no Garage. Point it at the dev Valkey with
# SCRY_VALKEY_URL (default redis://127.0.0.1:6380 — this machine's
# `scry-valkey-smoke`).
#
# Env knobs: VALKEY_URL, II/QQ ingest+query wire ports, SI/SQ/SL stats ports,
# S3_PORT (the stub object store from lib/stub-objstore.sh).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

VALKEY_URL="${SCRY_VALKEY_URL:-redis://127.0.0.1:6380}"
II="${II:-127.0.0.1:14440}"          # ingester wire port
QQ="${QQ:-127.0.0.1:14441}"          # queryd wire port
SI="${SI:-127.0.0.1:14442}"          # ingester stats port
SQ="${SQ:-127.0.0.1:14443}"          # queryd stats port
LQ="${LQ:-127.0.0.1:14444}"          # local-fallback queryd wire port
SL="${SL:-127.0.0.1:14445}"          # local-fallback queryd stats port
S3_PORT="${S3_PORT:-127.0.0.1:14446}" # stub object store (always-empty bucket)
GW="${GW:-127.0.0.1:14447}"          # gateway HTTP push port
SG="${SG:-127.0.0.1:14448}"          # gateway local stats port

TMP="$(mktemp -d)"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$TMP"; }
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  for f in ingest queryd localq; do
    [ -f "$TMP/$f.log" ] && { echo "---- $f.log (tail) ----" >&2; tail -20 "$TMP/$f.log" >&2; }
  done
  exit 1
}
ok() { echo "  ok: $*"; }

# A reachable-but-empty object store. The ingester runs storage-less and the
# queryds never scan a block, but queryd will not start until its catalog has
# seeded from the bucket, so the endpoint has to answer.
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
  # Clear any stray status keys from a prior aborted run so the fleet is
  # deterministic (keys embed ephemeral UUIDs + TTL-expire, but be tidy).
  keys=$("$VK" -u "$VALKEY_URL" --scan --pattern 'scry/status/*' 2>/dev/null || true)
  [ -n "$keys" ] && echo "$keys" | xargs -r "$VK" -u "$VALKEY_URL" del >/dev/null 2>&1 || true
else
  echo "note: no valkey-cli/redis-cli to pre-check $VALKEY_URL; proceeding" >&2
fi

echo "== building release scry =="
cargo build --release -p scry >"$TMP/cargo.log" 2>&1 \
  || { cat "$TMP/cargo.log"; fail "build failed"; }
SCRY=./target/release/scry

wait_bind() {
  local addr=$1
  for _ in $(seq 1 100); do
    (exec 3<>"/dev/tcp/${addr%:*}/${addr#*:}") 2>/dev/null && { exec 3>&- 3<&-; return 0; }
    sleep 0.1
  done
  return 1
}

# ════════════════════════════════════════════════════════════════════
# Phase 1 — fleet aggregation from either side
# ════════════════════════════════════════════════════════════════════
echo "== phase 1: fleet aggregation (ingest + query on one Valkey) =="

echo "-- starting ingester ($II, stats $SI), storage-less, heartbeating status --"
RUST_LOG=info "$SCRY" ingest --listen "$II" --valkey-url "$VALKEY_URL" \
  --stats-listen "$SI" --lease-ttl 10 >"$TMP/ingest.log" 2>&1 &
PIDS+=($!)

echo "-- starting query daemon ($QQ, stats $SQ), heartbeating status --"
RUST_LOG=info "$SCRY" query --listen "$QQ" --catalog "$TMP/queryd.sqlite" \
  --valkey-url "$VALKEY_URL" --stats-listen "$SQ" \
  --poll-interval 999999 --full-walk-interval 999999 >"$TMP/queryd.log" 2>&1 &
PIDS+=($!)

echo "-- starting gateway ($GW, stats $SG), heartbeating status --"
RUST_LOG=info "$SCRY" gateway --listen "$GW" --upstream 127.0.0.1:1 \
  --valkey-url "$VALKEY_URL" --stats-listen "$SG" >"$TMP/gateway.log" 2>&1 &
PIDS+=($!)

wait_bind "$SI" || fail "ingester stats endpoint never bound"
wait_bind "$SQ" || fail "queryd stats endpoint never bound"
wait_bind "$SG" || fail "gateway stats endpoint never bound"
ok "all status endpoints listening"

# Drive one definite rejected OTLP/HTTP request and let status heartbeat.
curl -s -o /dev/null -w '%{http_code}' -X POST --data-binary 'not-protobuf' "http://$GW/v1/traces" | grep -q '^400$' \
  || fail "gateway did not reject malformed OTLP/HTTP"
sleep 2

curl -sf "http://$SI/stats.json" >"$TMP/ingest.json" || fail "ingest /stats.json fetch failed"
curl -sf "http://$SQ/stats.json" >"$TMP/queryd.json" || fail "queryd /stats.json fetch failed"
curl -sf "http://$SG/stats.json" >"$TMP/gateway.json" || fail "gateway /stats.json fetch failed"
ok "fetched all /stats.json documents"

python3 - "$TMP/ingest.json" "$TMP/queryd.json" "$TMP/gateway.json" <<'PY' || fail "fleet assertions failed (see above)"
import json, sys

ingest = json.load(open(sys.argv[1]))
query  = json.load(open(sys.argv[2]))
gateway = json.load(open(sys.argv[3]))

def roles_by_id(doc):
    return {i["instance_id"]: i["role"] for i in doc["instances"]}

ok = True
def check(cond, msg):
    global ok
    print(("  ok: " if cond else "  FAIL: ") + msg)
    ok = ok and cond

# Each page reports itself via source + self_id.
# The ingester merges its own locally-reported agents with the Valkey fleet, so
# its source is "mixed"; queryd has no local agents to merge and reports
# "valkey". Either way the point is the same: not "local" -- Valkey is in play.
check(ingest.get("source") == "mixed",  f"ingest page source=mixed (got {ingest.get('source')})")
check(query.get("source")  == "valkey", f"query page source=valkey (got {query.get('source')})")

ing_self = ingest.get("self_id")
qry_self = query.get("self_id")
check(bool(ing_self), "ingest page has a self_id")
check(bool(qry_self), "query page has a self_id")
check(ing_self != qry_self, "ingest and query self_ids differ")

ing_map = roles_by_id(ingest)
qry_map = roles_by_id(query)
gw_map = roles_by_id(gateway)
gw_self = gateway.get("self_id")

# The reporting instance must appear in its own fleet with the right role.
check(ing_map.get(ing_self) == "ingest", "ingest self appears as role=ingest in ingest page")
check(qry_map.get(qry_self) == "query",  "query self appears as role=query in query page")
check(gw_map.get(gw_self) == "gateway", "gateway self appears as role=gateway")

# Cross-visibility: every page lists the gateway and the two daemon roles.
check(ing_map.get(qry_self) == "query",  "ingest page lists the query instance as role=query")
check(qry_map.get(ing_self) == "ingest", "query page lists the ingest instance as role=ingest")
check(any(v == "gateway" for v in ing_map.values()), "ingest page lists gateway")
check(any(v == "gateway" for v in qry_map.values()), "query page lists gateway")
check(any(v == "ingest" for v in gw_map.values()) and any(v == "query" for v in gw_map.values()), "gateway page lists ingest and query")
gw = next(i for i in gateway["instances"] if i["instance_id"] == gw_self)
check(gw.get("version"), "gateway reports version")
check(gw["data"]["inbound"]["otlp_http"]["rejected"] >= 1, "gateway reports rejected OTLP/HTTP request")

# self_id resolves to exactly one listed instance (the page marks it client-side).
check(list(ing_map).count(ing_self) == 1, "ingest self_id resolves to exactly one entry")
check(list(qry_map).count(qry_self) == 1, "query self_id resolves to exactly one entry")

# ---- catalog gauge ---------------------------------------------------------
# queryd has an online catalog, so it samples one. It has only just started, so
# it has exactly one reading and therefore must NOT report a trend: a rate
# extrapolated from a single sample would be fiction, and the gauge is required
# to say "unknown" rather than guess.
qd = next(i for i in query["instances"] if i["instance_id"] == qry_self)["data"]
cat = qd.get("catalog")
check(isinstance(cat, dict), "query instance carries a catalog gauge")
if isinstance(cat, dict):
    check(cat.get("sampled") is True, f"query gauge has sampled the catalog (got {cat.get('sampled')})")
    check(cat.get("blocks") == 0, f"empty catalog reports 0 blocks (got {cat.get('blocks')})")
    check(cat.get("blocks_per_hour") is None, "a single reading yields no trend")
    check(cat.get("sample_failures") == 0, "the read-only sampling connection works")
    # The flat mirrors must agree with the envelope; they are what a
    # mid-rollout UI falls back to.
    check(qd.get("catalog_blocks") == cat.get("blocks"), "flat catalog_blocks mirrors the gauge")

# This ingester is storage-less, so there is no catalog for it to observe. It
# must report that as absent, not as a catalog that happens to contain nothing.
ind = next(i for i in ingest["instances"] if i["instance_id"] == ing_self)["data"]
check(ind.get("catalog") is None, "a storage-less ingester reports no catalog gauge")
check(isinstance(ind.get("retention"), dict), "ingester reports a retention section")
check(ind.get("retention", {}).get("passes") == 0, "no retention passes have run")
bal = ind.get("blocks", {})
check(bal.get("created") == 0 and bal.get("reclaimed") == 0 and bal.get("net") == 0,
      f"idle ingester's block balance is flat (got {bal})")

sys.exit(0 if ok else 1)
PY
ok "fleet aggregation works from both sides (each page lists both instances)"

# The HTML dashboard is served at /.
curl -sf "http://$SI/" >"$TMP/ingest.html" || fail "ingest / (HTML) fetch failed"
grep -qi '<html' "$TMP/ingest.html" || fail "ingest / did not serve an HTML page"
ok "ingest / serves the dashboard HTML"

echo "== phase 1 PASSED =="

# ════════════════════════════════════════════════════════════════════
# Phase 2 — local fallback without Valkey
# ════════════════════════════════════════════════════════════════════
echo "== phase 2: local fallback (query, no Valkey) =="

# NB: unset SCRY_VALKEY_URL for THIS daemon only — the query daemon falls back
# to the env var when `--valkey-url` is absent, which would defeat the point of
# the local-fallback assertion (it would connect to Valkey and render the fleet).
RUST_LOG=info env -u SCRY_VALKEY_URL "$SCRY" query --listen "$LQ" --catalog "$TMP/localq.sqlite" \
  --stats-listen "$SL" --poll-interval 999999 --full-walk-interval 999999 \
  >"$TMP/localq.log" 2>&1 &
PIDS+=($!)
wait_bind "$SL" || fail "local-fallback queryd stats endpoint never bound"
sleep 1

curl -sf "http://$SL/stats.json" >"$TMP/localq.json" || fail "local queryd /stats.json fetch failed"

python3 - "$TMP/localq.json" <<'PY' || fail "local-fallback assertions failed (see above)"
import json, sys
doc = json.load(open(sys.argv[1]))
ok = True
def check(cond, msg):
    global ok
    print(("  ok: " if cond else "  FAIL: ") + msg)
    ok = ok and cond

check(doc.get("source") == "local", f"source=local (got {doc.get('source')})")
insts = doc["instances"]
check(len(insts) == 1, f"exactly one instance (got {len(insts)})")
check(insts and insts[0]["role"] == "query", "the single instance is role=query")
check(insts and insts[0]["instance_id"] == doc.get("self_id"), "self_id matches the single instance")
sys.exit(0 if ok else 1)
PY
ok "Valkey-less query serves a single-entry local page"

echo "== phase 2 PASSED =="

# No panics in any daemon log.
if grep -iq panicked "$TMP/ingest.log" "$TMP/queryd.log" "$TMP/localq.log"; then
  fail "a daemon panicked (see $TMP/*.log)"
fi

echo
echo "ALL STATUS-PAGE SMOKE CHECKS PASSED"
