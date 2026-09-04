#!/usr/bin/env bash
# End-to-end proof for every supported gateway receiver:
# Loki JSON+protobuf, OTLP logs/metrics/traces protobuf+JSON+gzip+gRPC,
# Prometheus remote-write, legacy Pyroscope, and Pyroscope Push v1.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
SMOKE_DIR="${SMOKE_DIR:-/tmp/scry-gateway-smoke}"
INGEST_LISTEN="${INGEST_LISTEN:-127.0.0.1:4097}"
GW_LISTEN="${GW_LISTEN:-127.0.0.1:4319}"
GW_GRPC_LISTEN="${GW_GRPC_LISTEN:-127.0.0.1:4320}"
REQUESTS="${REQUESTS:-2}"; RECORDS="${RECORDS:-4}"
PROFILE_BYTES="${PROFILE_BYTES:-4096}"; SERIES="${SERIES:-3}"; SAMPLES="${SAMPLES:-2}"

# Four OTLP/HTTP encodings plus gRPC, two Loki encodings, two Push encodings.
EXPECTED_LOGS=$(( (4 * REQUESTS * RECORDS) + (2 * REQUESTS * RECORDS) + (REQUESTS * RECORDS) ))
EXPECTED_TRACES=$(( (4 * REQUESTS * RECORDS) + (REQUESTS * RECORDS) ))
EXPECTED_METRICS=$(( (4 * REQUESTS * RECORDS) + (REQUESTS * RECORDS) + (REQUESTS * SERIES * SAMPLES) ))
EXPECTED_PROFILES=$(( REQUESTS + (2 * REQUESTS * RECORDS) ))

[[ -f docker/garage/.env ]] || { echo "[gw-smoke] docker/garage/.env missing; run scripts/dev-garage-up.sh" >&2; exit 2; }
set -a; source docker/garage/.env; set +a
for tool in aws sqlite3 curl; do command -v "$tool" >/dev/null || { echo "missing $tool" >&2; exit 2; }; done

echo "[gw-smoke] building release binaries..."
cargo build --release -p scry -p scry-gateway >&2
rm -rf "$SMOKE_DIR"; mkdir -p "$SMOKE_DIR"
echo "[gw-smoke] emptying dev bucket..."
AWS_ACCESS_KEY_ID="$SCRY_OBJSTORE_ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$SCRY_OBJSTORE_SECRET_ACCESS_KEY" AWS_REGION="$SCRY_OBJSTORE_REGION" \
  aws --endpoint-url "$SCRY_OBJSTORE_ENDPOINT" s3 rm "s3://$SCRY_OBJSTORE_BUCKET/" --recursive >/dev/null || true

./target/release/scry ingest --listen "$INGEST_LISTEN" --storage --wal-dir "$SMOKE_DIR/wal" --catalog "$SMOKE_DIR/online.sqlite" >"$SMOKE_DIR/ingestd.log" 2>&1 & INGEST_PID=$!
GW_PID=""
cleanup() { [[ -n "$GW_PID" ]] && kill -9 "$GW_PID" 2>/dev/null || true; kill -9 "$INGEST_PID" 2>/dev/null || true; }
trap cleanup EXIT
wait_port() { local h="${1%:*}" p="${1#*:}"; for _ in $(seq 1 200); do (echo >"/dev/tcp/$h/$p") 2>/dev/null && return; sleep .1; done; return 1; }
wait_port "$INGEST_LISTEN" || { tail -40 "$SMOKE_DIR/ingestd.log"; exit 1; }
./target/release/scry gateway --listen "$GW_LISTEN" --listen-otlp-grpc "$GW_GRPC_LISTEN" --upstream "$INGEST_LISTEN" >"$SMOKE_DIR/gateway.log" 2>&1 & GW_PID=$!
wait_port "$GW_LISTEN"; wait_port "$GW_GRPC_LISTEN"
GW_URL="http://$GW_LISTEN"; GRPC_URL="http://$GW_GRPC_LISTEN"

for signal in traces logs metrics; do
  for representation in proto json proto-gzip json-gzip; do
    file="$SMOKE_DIR/otlp-$signal-$representation.bin"
    ./target/release/scry-gateway-probe "otlp-$signal" "$file" "$representation" "$RECORDS" >/dev/null
    case "$representation" in proto|proto-gzip) type=application/x-protobuf;; *) type=application/json;; esac
    encoding=(); [[ "$representation" == *-gzip ]] && encoding=(-H 'Content-Encoding: gzip')
    for _ in $(seq 1 "$REQUESTS"); do curl -sf -o /dev/null -H "Content-Type: $type" "${encoding[@]}" --data-binary "@$file" "$GW_URL/v1/$signal"; done
  done
  ./target/release/scry-gateway-probe grpc "$GRPC_URL" "$signal" "$REQUESTS" "$RECORDS" >/dev/null
done

./target/release/scry-gateway-probe loki-json "$SMOKE_DIR/loki.json" "$RECORDS" >/dev/null
./target/release/scry-gateway-probe loki-proto "$SMOKE_DIR/loki.bin" "$RECORDS" >/dev/null
for _ in $(seq 1 "$REQUESTS"); do
  curl -sf -o /dev/null -H 'Content-Type: application/json' --data-binary "@$SMOKE_DIR/loki.json" "$GW_URL/loki/api/v1/push"
  curl -sf -o /dev/null -H 'Content-Type: application/x-protobuf' -H 'Content-Encoding: snappy' --data-binary "@$SMOKE_DIR/loki.bin" "$GW_URL/loki/api/v1/push"
done

./target/release/scry-gateway-probe promwrite "$SMOKE_DIR/promwrite.bin" "$SERIES" "$SAMPLES" >/dev/null
for _ in $(seq 1 "$REQUESTS"); do curl -sf -o /dev/null -H 'Content-Type: application/x-protobuf' -H 'Content-Encoding: snappy' --data-binary "@$SMOKE_DIR/promwrite.bin" "$GW_URL/api/v1/write"; done

./target/release/scry-gateway-probe pprof "$SMOKE_DIR/legacy.pprof" "$PROFILE_BYTES" >/dev/null
for i in $(seq 1 "$REQUESTS"); do from=$((1700000000+i)); curl -sf -o /dev/null -F "profile=@$SMOKE_DIR/legacy.pprof" "$GW_URL/ingest?from=$from&until=$((from+10))&name=legacy.smoke"; done
./target/release/scry-gateway-probe pyroscope-push "$SMOKE_DIR/push.bin" "$RECORDS" proto >/dev/null
./target/release/scry-gateway-probe pyroscope-push "$SMOKE_DIR/push.json.gz" "$RECORDS" json-gzip >/dev/null
for _ in $(seq 1 "$REQUESTS"); do
  curl -sf -o /dev/null -H 'Content-Type: application/proto' --data-binary "@$SMOKE_DIR/push.bin" "$GW_URL/push.v1.PusherService/Push"
  curl -sf -o /dev/null -H 'Content-Type: application/json' -H 'Content-Encoding: gzip' --data-binary "@$SMOKE_DIR/push.json.gz" "$GW_URL/push.v1.PusherService/Push"
done

sleep 2; kill "$GW_PID" 2>/dev/null || true; GW_PID=""; sleep 1; kill -INT "$INGEST_PID"; wait "$INGEST_PID" 2>/dev/null || true; trap - EXIT
./target/release/scry list --catalog "$SMOKE_DIR/recon.sqlite" >"$SMOKE_DIR/list.txt" 2>&1

rows() { sqlite3 "$SMOKE_DIR/recon.sqlite" "SELECT COALESCE(SUM(row_count),0) FROM blocks WHERE signal='$1';"; }
blocks() { sqlite3 "$SMOKE_DIR/recon.sqlite" "SELECT COUNT(*) FROM blocks WHERE signal='$1';"; }
postings() { sqlite3 "$SMOKE_DIR/recon.sqlite" "SELECT COUNT(*) FROM blocks WHERE signal='$1' AND has_postings=1;"; }
failed=0
for spec in "logs:$EXPECTED_LOGS:yes" "metrics:$EXPECTED_METRICS:yes" "traces:$EXPECTED_TRACES:no" "profiles:$EXPECTED_PROFILES:no"; do
  IFS=: read -r signal expected indexed <<<"$spec"; actual=$(rows "$signal"); count=$(blocks "$signal"); sidecars=$(postings "$signal")
  echo "[gw-smoke] $signal rows=$actual expected=$expected blocks=$count postings=$sidecars"
  [[ "$actual" == "$expected" && "$count" -ge 1 ]] || failed=1
  if [[ "$indexed" == yes ]]; then [[ "$sidecars" -ge 1 ]] || failed=1; else [[ "$sidecars" == 0 ]] || failed=1; fi
  ./target/release/scry get --catalog "$SMOKE_DIR/recon.sqlite" --signal "$signal" --default-window-secs 0 >"$SMOKE_DIR/query-$signal.txt" 2>&1 || true
  queried=$(awk '/^# scan:/ {print $3; exit}' "$SMOKE_DIR/query-$signal.txt")
  [[ "$queried" == "$expected" ]] || { echo "[gw-smoke] $signal query rows=${queried:-none} expected=$expected"; failed=1; }
done
[[ $failed -eq 0 ]] || { tail -40 "$SMOKE_DIR/ingestd.log"; tail -40 "$SMOKE_DIR/gateway.log"; exit 1; }
echo "[gw-smoke] PASS"
