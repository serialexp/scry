#!/usr/bin/env bash
#
# smoke-live.sh — end-to-end exercise of the merged history+live query (D-054):
# `scry query --live` (logs) unions the stored parquet blocks with the
# still-in-flight records buffered at the ingester, deduped across the
# block-commit seam by the per-writer WAL-segment watermark.
#
#   scry ingest --storage --valkey-url … --tail-advertise-addr 127.0.0.1:IA
#         │  logs → WAL → (live ring, seg-tagged) → block on flush
#         │  on flush: insert_block advances H(writer,"logs",shard) = wal_seg_max
#         ▼
#   scry query --catalog <shared> --valkey-url …            (query daemon)
#         │  --live: discover the ingester via Valkey → LiveQuery → LiveBatch
#         │  dedup: keep a live record iff its wal_seg > H (absent H ⇒ nothing
#         │         durable ⇒ keep all); union with the block table
#         ▼
#   scry-query-probe --addr QQ --signal logs [--live]       (prints total_rows)
#
# The ingester and the query daemon share ONE SQLite catalog file, so the
# watermark the ingester advances on `insert_block` is immediately visible to
# the daemon's dedup (SQLite WAL cross-process visibility) — no convergence
# needed, so the daemon's poll/full-walk intervals are set huge.
#
# Assertions (N ≡ total logs ingested ≡ post-flush blocks-only count):
#   (a) LIVE HALF — before any flush, `probe --live` sees all N in-flight
#       records (blocks-only sees < N: they're genuinely un-flushed). This is
#       the segment-0 gap the `unwrap_or(0)` watermark bug used to drop.
#   (b) HISTORY HALF — after a flush, blocks-only `probe` sees exactly N.
#   (c) DEDUP EXACT — after the flush, `probe --live` STILL sees exactly N,
#       not ~2N: the same records are in both the block AND the ring, but the
#       watermark drops the ring copies. No double across the seam.
#   (d) REFUSE — `probe --live` against a query daemon with NO --valkey-url is
#       refused with QUERY_ERR_LIVE_UNAVAILABLE (code 0x0005) and streams no
#       rows (not a silent blocks-only degrade).
#
# Needs Garage (docker/garage/.env — run scripts/dev-garage-up.sh) AND a dev
# Valkey (SCRY_VALKEY_URL, default redis://127.0.0.1:6380 — this machine's
# `scry-valkey-smoke`). Needs `aws` (bucket empty) + a TCP-capable bash.
#
# Env knobs: SCRY_VALKEY_URL, IA/QQ/RQ ports, SPEW_RATE (400), BATCHES (200),
# MAX_AGE (10s block-max-age), LIVE_WINDOW (120s).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

VALKEY_URL="${SCRY_VALKEY_URL:-redis://127.0.0.1:6380}"
IA="${IA:-127.0.0.1:14440}"          # ingester ingest + live-query + tail port
QQ="${QQ:-127.0.0.1:14441}"          # query daemon (Valkey-backed) query port
RQ="${RQ:-127.0.0.1:14442}"          # refuse query daemon (no Valkey) query port
SPEW_RATE="${SPEW_RATE:-400}"
BATCHES="${BATCHES:-200}"
MAX_AGE="${MAX_AGE:-10}"             # block-max-age-secs: flush timer
LIVE_WINDOW="${LIVE_WINDOW:-120}"    # live-window-secs: keep ring through flush

TMP="$(mktemp -d)"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$TMP"; }
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  for f in ingest queryd refuse; do
    [ -f "$TMP/$f.log" ] && { echo "---- $f.log (tail) ----" >&2; tail -25 "$TMP/$f.log" >&2; }
  done
  exit 1
}
ok() { echo "  ok: $*"; }

# ── Pre-flight: Garage credentials + Valkey. ─────────────────────────
if [ ! -f docker/garage/.env ]; then
  fail "docker/garage/.env missing; run scripts/dev-garage-up.sh first"
fi
set -a; source docker/garage/.env; set +a

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

echo "== emptying bucket s3://$SCRY_OBJSTORE_BUCKET/ =="
AWS_ACCESS_KEY_ID="$SCRY_OBJSTORE_ACCESS_KEY_ID" \
AWS_SECRET_ACCESS_KEY="$SCRY_OBJSTORE_SECRET_ACCESS_KEY" \
AWS_REGION="$SCRY_OBJSTORE_REGION" \
  aws --endpoint-url "$SCRY_OBJSTORE_ENDPOINT" \
      s3 rm "s3://$SCRY_OBJSTORE_BUCKET/" --recursive >/dev/null 2>&1 || true

echo "== building release scry + noise-spewer + scry-query-probe =="
cargo build --release -p scry -p noise-spewer -p scry-queryd >"$TMP/cargo.log" 2>&1 \
  || { cat "$TMP/cargo.log"; fail "build failed"; }
SCRY=./target/release/scry
SPEWER=./target/release/noise-spewer
PROBE=./target/release/scry-query-probe

CAT="$TMP/catalog.sqlite"

wait_bind() {
  local addr=$1
  for _ in $(seq 1 100); do
    (exec 3<>"/dev/tcp/${addr%:*}/${addr#*:}") 2>/dev/null && { exec 3>&- 3<&-; return 0; }
    sleep 0.1
  done
  return 1
}

# probe_live / probe_blocks: print total_rows on success, or empty on error.
probe_live()   { "$PROBE" --addr "$QQ" --signal logs --live 2>>"$TMP/probe.err"; }
probe_blocks() { "$PROBE" --addr "$QQ" --signal logs        2>>"$TMP/probe.err"; }

# ════════════════════════════════════════════════════════════════════
# Setup — ingester (storage + Valkey advertise) + Valkey-backed queryd
# ════════════════════════════════════════════════════════════════════
echo "== starting ingester ($IA), storage on, block-max-age=${MAX_AGE}s, live-window=${LIVE_WINDOW}s =="
RUST_LOG=info "$SCRY" ingest \
  --listen "$IA" --storage \
  --wal-dir "$TMP/wal" --catalog "$CAT" \
  --valkey-url "$VALKEY_URL" --tail-advertise-addr "$IA" --lease-ttl 10 \
  --block-max-age-secs "$MAX_AGE" \
  --live-window-secs "$LIVE_WINDOW" \
  >"$TMP/ingest.log" 2>&1 &
PIDS+=($!)
wait_bind "$IA" || fail "ingester never bound $IA"
ok "ingester listening on $IA"

# Confirm it registered its tail/live address in Valkey before the daemon needs
# to discover it.
if [ -n "$VK" ]; then
  for _ in $(seq 1 50); do
    n=$("$VK" -u "$VALKEY_URL" --scan --pattern 'scry/tail/ingesters/*' 2>/dev/null | wc -l)
    [ "${n:-0}" -ge 1 ] && break
    sleep 0.1
  done
  [ "${n:-0}" -ge 1 ] || fail "ingester never registered in the Valkey tail registry"
  ok "ingester registered in the Valkey registry ($n key)"
fi

echo "== starting Valkey-backed query daemon ($QQ), sharing the catalog =="
RUST_LOG=info "$SCRY" query \
  --listen "$QQ" --catalog "$CAT" \
  --valkey-url "$VALKEY_URL" \
  --poll-interval 999999 --full-walk-interval 999999 \
  >"$TMP/queryd.log" 2>&1 &
PIDS+=($!)
wait_bind "$QQ" || fail "query daemon never bound $QQ"
ok "query daemon listening on $QQ"

# ════════════════════════════════════════════════════════════════════
# Phase 1 — live half BEFORE any flush
# ════════════════════════════════════════════════════════════════════
echo "== phase 1: live half (pre-flush) =="
echo "-- spewing $BATCHES logs batches to the ingester --"
"$SPEWER" --addr "$IA" --signals logs --rate "$SPEW_RATE" --max-batches "$BATCHES" \
  >"$TMP/spew.log" 2>&1 || fail "spew failed"
# Records now sit in the WAL + live ring; the block won't seal until MAX_AGE.

L1="$(probe_live || true)"
B1="$(probe_blocks || true)"
[ -n "$L1" ] || fail "probe --live (pre-flush) produced no count (see $TMP/probe.err)"
[ -n "$B1" ] || fail "probe (pre-flush blocks-only) produced no count (see $TMP/probe.err)"
echo "  pre-flush: live=$L1  blocks-only=$B1"
[ "$L1" -gt 0 ] || fail "live half saw 0 rows before flush — the ring/fan-in or the segment-0 watermark gate is broken"
[ "$B1" -lt "$L1" ] || fail "blocks-only ($B1) is not < live ($L1) pre-flush — records already flushed, test would be vacuous (lower BATCHES or raise MAX_AGE)"
ok "live half sees $L1 in-flight rows; only $B1 are durable yet (genuinely in-flight)"

# ════════════════════════════════════════════════════════════════════
# Phase 2 — force the flush, then history half + dedup exactness
# ════════════════════════════════════════════════════════════════════
echo "== phase 2: history half + dedup (post-flush) =="
echo "-- waiting for the block-max-age flush (${MAX_AGE}s) + upload --"
# Poll blocks-only until it stops rising (block sealed + inserted + watermark
# advanced). Bounded wait; MAX_AGE + upload is a few seconds on local Garage.
B2=0
for _ in $(seq 1 60); do
  sleep 1
  cur="$(probe_blocks || echo 0)"
  [ -n "$cur" ] || cur=0
  if [ "$cur" -ge "$L1" ]; then B2="$cur"; break; fi
  B2="$cur"
done
echo "  post-flush blocks-only=$B2  (target N=$L1)"
[ "$B2" -eq "$L1" ] || fail "history half: blocks-only=$B2 ≠ N=$L1 — a flush lost or duplicated rows"
ok "history half: every one of the $L1 records is durable in a block (blocks-only=$B2)"

# THE dedup assertion: the ring STILL holds all $L1 records (live-window is
# ${LIVE_WINDOW}s ≫ elapsed), and the block now holds them too. A correct merge
# drops the ring copies via the watermark ⇒ exactly N, not ~2N.
L2="$(probe_live || true)"
[ -n "$L2" ] || fail "probe --live (post-flush) produced no count (see $TMP/probe.err)"
echo "  post-flush live (merged)=$L2  (must equal N=$L1, NOT ~2N)"
[ "$L2" -eq "$L1" ] || fail "dedup FAILED: merged live=$L2 ≠ N=$L1 (double across the block-commit seam — watermark did not hold)"
ok "dedup exact: merged history+live = $L2 = N (ring copies of now-durable records were dropped)"

# ════════════════════════════════════════════════════════════════════
# Phase 3 — refuse without Valkey
# ════════════════════════════════════════════════════════════════════
echo "== phase 3: refuse-without-Valkey =="
echo "-- starting a query daemon with NO --valkey-url ($RQ), sharing the catalog --"
RUST_LOG=info "$SCRY" query \
  --listen "$RQ" --catalog "$CAT" \
  --poll-interval 999999 --full-walk-interval 999999 \
  >"$TMP/refuse.log" 2>&1 &
PIDS+=($!)
wait_bind "$RQ" || fail "refuse query daemon never bound $RQ"

# A plain (non-live) query must still work against it (blocks-only path intact).
rb="$("$PROBE" --addr "$RQ" --signal logs 2>"$TMP/refuse_blocks.err" || true)"
[ -n "$rb" ] && [ "$rb" -eq "$L1" ] \
  || fail "non-live query against the Valkey-less daemon returned '$rb' (want $L1) — blocks path regressed"
ok "non-live query against the Valkey-less daemon still works (blocks-only=$rb)"

echo "-- probe --live against it; expect QUERY_ERR_LIVE_UNAVAILABLE (0x0005) --"
if "$PROBE" --addr "$RQ" --signal logs --live >"$TMP/refuse_live.out" 2>"$TMP/refuse_live.err"; then
  fail "probe --live against a Valkey-less daemon SUCCEEDED — it must refuse"
fi
grep -q '0x0005' "$TMP/refuse_live.err" \
  || { cat "$TMP/refuse_live.err" >&2; fail "refusal was not QUERY_ERR_LIVE_UNAVAILABLE (code 0x0005)"; }
[ ! -s "$TMP/refuse_live.out" ] \
  || { cat "$TMP/refuse_live.out" >&2; fail "refuse path leaked a row count to stdout"; }
ok "Valkey-less daemon refused the live query with QUERY_ERR_LIVE_UNAVAILABLE and streamed nothing"

# No panics / no upload 'pass failed' anywhere.
if grep -iq panicked "$TMP/ingest.log" "$TMP/queryd.log" "$TMP/refuse.log"; then
  fail "a daemon panicked (see $TMP/*.log)"
fi

echo
echo "ALL LIVE-MERGE SMOKE CHECKS PASSED"
