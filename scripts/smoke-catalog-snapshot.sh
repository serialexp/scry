#!/usr/bin/env bash
#
# smoke-catalog-snapshot.sh — end-to-end exercise of catalog snapshot bootstrap
# (D-055): the ingester periodically uploads its online catalog as ONE object
# (`_catalog/snapshot.sqlite`); a cold `scry query` restores it in a single GET
# instead of walking every block sidecar, then its own poll loop fills the delta.
#
#   scry ingest --storage --catalog A --catalog-snapshot-interval 3s
#         │  logs → WAL → parquet blocks → bucket; insert_block into catalog A
#         │  every 3s: VACUUM INTO + PUT  _catalog/snapshot.sqlite
#         ▼
#   scry query --catalog B  (B starts EMPTY)
#         │  cold boot: restore_snapshot(_catalog/snapshot.sqlite) → catalog B
#         │  poll loop then picks up blocks written AFTER the snapshot
#         ▼
#   scry-query-probe --addr QQ --signal logs   (prints total_rows)
#
# Assertions:
#   (a) SNAPSHOT PRODUCED — `_catalog/snapshot.sqlite` appears in the bucket.
#   (b) RESTORE-ON-BOOT — the cold query daemon logs "restored catalog from
#       bucket snapshot blocks=…" (NOT a full reconcile) and a probe returns the
#       ingester's row count N immediately, with the poll loop effectively idle.
#   (c) INCREMENTAL DELTA — spew M more logs post-snapshot; the daemon's poll
#       loop lifts the count to N+M (snapshot + delta, no double-count).
#   (d) RESERVED PREFIX — a fresh reconcile over the bucket counts exactly the
#       block sidecars (the `_catalog/` snapshot object is never mis-parsed as a
#       block), and no daemon logs a sidecar parse failure.
#
# Needs Garage (docker/garage/.env — run scripts/dev-garage-up.sh) + `aws`. NO
# Valkey (single-instance; snapshot production needs no lease).
#
# Env knobs: IA/QQ ports, SPEW_RATE (400), BATCHES (150), MAX_AGE (3s block
# flush), SNAP_INT (3s snapshot interval).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

IA="${IA:-127.0.0.1:14460}"          # ingester ingest port
QQ="${QQ:-127.0.0.1:14461}"          # cold query daemon port
SPEW_RATE="${SPEW_RATE:-400}"
BATCHES="${BATCHES:-150}"
MAX_AGE="${MAX_AGE:-3}"              # block-max-age-secs: flush timer
SNAP_INT="${SNAP_INT:-3}"           # catalog-snapshot-interval (seconds)

TMP="$(mktemp -d)"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$TMP"; }
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  for f in ingest queryd; do
    [ -f "$TMP/$f.log" ] && { echo "---- $f.log (tail) ----" >&2; tail -25 "$TMP/$f.log" >&2; }
  done
  exit 1
}
ok() { echo "  ok: $*"; }

# ── Pre-flight: Garage credentials. ──────────────────────────────────
if [ ! -f docker/garage/.env ]; then
  fail "docker/garage/.env missing; run scripts/dev-garage-up.sh first"
fi
set -a; source docker/garage/.env; set +a

aws_s3() {
  AWS_ACCESS_KEY_ID="$SCRY_OBJSTORE_ACCESS_KEY_ID" \
  AWS_SECRET_ACCESS_KEY="$SCRY_OBJSTORE_SECRET_ACCESS_KEY" \
  AWS_REGION="$SCRY_OBJSTORE_REGION" \
    aws --endpoint-url "$SCRY_OBJSTORE_ENDPOINT" "$@"
}

echo "== emptying bucket s3://$SCRY_OBJSTORE_BUCKET/ =="
aws_s3 s3 rm "s3://$SCRY_OBJSTORE_BUCKET/" --recursive >/dev/null 2>&1 || true

echo "== building release scry + noise-spewer + scry-query-probe =="
cargo build --release -p scry -p noise-spewer -p scry-queryd >"$TMP/cargo.log" 2>&1 \
  || { cat "$TMP/cargo.log"; fail "build failed"; }
SCRY=./target/release/scry
SPEWER=./target/release/noise-spewer
PROBE=./target/release/scry-query-probe

CAT_A="$TMP/catalog-a.sqlite"        # ingester's authoritative online catalog
CAT_B="$TMP/catalog-b.sqlite"        # cold query daemon — starts absent
CAT_C="$TMP/catalog-c.sqlite"        # throwaway, for the reconcile prefix check

wait_bind() {
  local addr=$1
  for _ in $(seq 1 100); do
    (exec 3<>"/dev/tcp/${addr%:*}/${addr#*:}") 2>/dev/null && { exec 3>&- 3<&-; return 0; }
    sleep 0.1
  done
  return 1
}

# Row count in a catalog file (online, no reconcile) via `scry list`'s trailer.
a_rows() {
  "$SCRY" list --catalog "$CAT_A" --no-reconcile 2>/dev/null \
    | sed -n 's/^# total rows=\([0-9]*\) .*/\1/p'
}
a_blocks() {
  "$SCRY" list --catalog "$CAT_A" --no-reconcile 2>/dev/null \
    | sed -n 's/^# \([0-9]*\) block(s) .*/\1/p'
}
probe() { "$PROBE" --addr "$QQ" --signal logs 2>>"$TMP/probe.err"; }

# ════════════════════════════════════════════════════════════════════
# Setup — ingester with storage + periodic catalog snapshot
# ════════════════════════════════════════════════════════════════════
echo "== starting ingester ($IA): storage on, block-max-age=${MAX_AGE}s, snapshot-interval=${SNAP_INT}s =="
RUST_LOG=info "$SCRY" ingest \
  --listen "$IA" --storage \
  --wal-dir "$TMP/wal" --catalog "$CAT_A" \
  --block-max-age-secs "$MAX_AGE" \
  --catalog-snapshot-interval "${SNAP_INT}s" \
  >"$TMP/ingest.log" 2>&1 &
PIDS+=($!)
wait_bind "$IA" || fail "ingester never bound $IA"
ok "ingester listening on $IA"

# ════════════════════════════════════════════════════════════════════
# Phase 1 — ingest, seal blocks, produce a snapshot covering all of them
# ════════════════════════════════════════════════════════════════════
echo "== phase 1: ingest + seal + snapshot =="
"$SPEWER" --addr "$IA" --signals logs --rate "$SPEW_RATE" --max-batches "$BATCHES" \
  >"$TMP/spew.log" 2>&1 || fail "spew failed"

# Wait for blocks to seal + insert into catalog A, then for the row count to
# stabilise (spewer has exited, so it converges to a fixed N).
N=0; stable=0
for _ in $(seq 1 60); do
  sleep 1
  cur="$(a_rows || echo '')"; [ -n "$cur" ] || cur=0
  if [ "$cur" -gt 0 ] && [ "$cur" -eq "$N" ]; then
    stable=$((stable + 1)); [ "$stable" -ge 2 ] && break
  else
    stable=0
  fi
  N="$cur"
done
[ "$N" -gt 0 ] || fail "no rows ever landed in catalog A (blocks never sealed)"
BLK="$(a_blocks || echo 0)"
ok "ingester sealed $BLK block(s), N=$N rows durable in catalog A"

# (a) A snapshot object must appear, taken AFTER A stabilised so it covers all N.
echo "-- waiting for a fresh _catalog/snapshot.sqlite (covering all $N rows) --"
sleep "$((SNAP_INT + 2))"   # guarantee ≥1 snapshot after stabilisation
snap_seen=0
for _ in $(seq 1 30); do
  if aws_s3 s3 ls "s3://$SCRY_OBJSTORE_BUCKET/_catalog/snapshot.sqlite" >/dev/null 2>&1; then
    snap_seen=1; break
  fi
  sleep 1
done
[ "$snap_seen" -eq 1 ] || fail "the ingester never uploaded _catalog/snapshot.sqlite"
grep -q "catalog snapshot uploaded" "$TMP/ingest.log" || fail "ingester never logged a snapshot upload"
ok "(a) snapshot produced: _catalog/snapshot.sqlite present in the bucket"

# ════════════════════════════════════════════════════════════════════
# Phase 2 — cold query daemon restores from the snapshot (not a reconcile)
# ════════════════════════════════════════════════════════════════════
echo "== phase 2: cold restore-on-boot (catalog B starts absent) =="
[ ! -e "$CAT_B" ] || fail "catalog B unexpectedly exists before boot"
# poll enabled (for phase 3 delta), full-walk disabled (that's the O(all) walk
# we're replacing). Restore is proven by the boot log line, not by disabling poll.
RUST_LOG=info "$SCRY" query \
  --listen "$QQ" --catalog "$CAT_B" \
  --poll-interval 2 --full-walk-interval 999999 \
  >"$TMP/queryd.log" 2>&1 &
PIDS+=($!)
wait_bind "$QQ" || fail "cold query daemon never bound $QQ"

# (b) The definitive proof the snapshot path ran (vs a reconcile).
grep -q "restored catalog from bucket snapshot" "$TMP/queryd.log" \
  || fail "cold daemon did NOT restore from the snapshot (see queryd.log)"
P1="$(probe || true)"
[ -n "$P1" ] || fail "probe against the cold daemon produced no count (see $TMP/probe.err)"
[ "$P1" -eq "$N" ] \
  || fail "restore-on-boot: probe=$P1 ≠ N=$N (snapshot did not carry the full catalog)"
ok "(b) cold daemon restored the catalog from one GET and serves all $N rows"

# ════════════════════════════════════════════════════════════════════
# Phase 3 — incremental delta after the snapshot (poll picks it up)
# ════════════════════════════════════════════════════════════════════
echo "== phase 3: post-snapshot delta =="
"$SPEWER" --addr "$IA" --signals logs --rate "$SPEW_RATE" --max-batches "$BATCHES" \
  >"$TMP/spew2.log" 2>&1 || fail "second spew failed"

# Wait for A to reflect the new rows (N2 > N), then for the daemon's poll to
# converge B up to the same N2.
N2=0; stable=0
for _ in $(seq 1 60); do
  sleep 1
  cur="$(a_rows || echo '')"; [ -n "$cur" ] || cur=0
  if [ "$cur" -gt "$N" ] && [ "$cur" -eq "$N2" ]; then
    stable=$((stable + 1)); [ "$stable" -ge 2 ] && break
  else
    stable=0
  fi
  N2="$cur"
done
[ "$N2" -gt "$N" ] || fail "second spew added no durable rows to catalog A (N2=$N2, N=$N)"

P2=0
for _ in $(seq 1 60); do
  sleep 1
  P2="$(probe || echo 0)"; [ -n "$P2" ] || P2=0
  [ "$P2" -ge "$N2" ] && break
done
[ "$P2" -eq "$N2" ] \
  || fail "delta: daemon converged to $P2 ≠ N2=$N2 (poll missed or double-counted the post-snapshot blocks)"
ok "(c) incremental delta: daemon reached N2=$N2 = snapshot ($N) + delta ($((N2 - N)))"

# ════════════════════════════════════════════════════════════════════
# Phase 4 — the reserved `_catalog/` prefix is not a block
# ════════════════════════════════════════════════════════════════════
echo "== phase 4: reserved-prefix reconcile =="
RECON_OUT="$("$SCRY" list --catalog "$CAT_C" 2>"$TMP/reconcile.err")" \
  || { cat "$TMP/reconcile.err" >&2; fail "reconcile list failed"; }
RB="$(echo "$RECON_OUT" | sed -n 's/^# \([0-9]*\) block(s) .*/\1/p')"
A_BLK="$(a_blocks || echo 0)"
[ -n "$RB" ] && [ "$RB" -eq "$A_BLK" ] \
  || fail "reconcile counted $RB blocks, A has $A_BLK — the snapshot object was mis-parsed as a block"
if grep -qi "sidecar JSON parse failed\|_catalog" "$TMP/reconcile.err" "$TMP/ingest.log" "$TMP/queryd.log"; then
  fail "a _catalog/ object was fetched/parsed as a block sidecar (see logs)"
fi
ok "(d) reserved prefix: reconcile counts exactly $RB block(s); _catalog/ never parsed as a block"

# No panics anywhere.
if grep -iq panicked "$TMP/ingest.log" "$TMP/queryd.log"; then
  fail "a daemon panicked (see $TMP/*.log)"
fi

echo
echo "ALL CATALOG-SNAPSHOT SMOKE CHECKS PASSED"
