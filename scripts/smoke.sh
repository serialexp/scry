#!/usr/bin/env bash
# v0.1 storage exit criterion — end-to-end smoke test.
#
# Sends a known number of DummyRecords through the wire path, runs the
# storage pipeline (WAL → block → object store → online catalog),
# then bootstraps a fresh catalog from the bucket via scry-list and
# asserts:
#
#   * the new catalog's total row count equals exactly the number of
#     records the spewer generated, and
#   * at least one block landed in the bucket.
#
# The dev Garage bucket (`scry-dev`) is emptied at the start of the
# run so the post-condition is unambiguous. Don't point this at any
# bucket whose contents you want to keep.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# ── Parameters ──────────────────────────────────────────────────────
# DURATION_SECS is a generous upper bound; if the spewer can't reach
# --max-batches in time it logs "duration reached" and we treat what
# it actually sent as the source of truth (see assertions below).
# Don't make this too small — at high BATCHES the sink's effective
# throughput is bounded by parquet+S3 upload time, which can be well
# below the requested RATE.
BATCHES="${BATCHES:-2000}"
RATE="${RATE:-400}"
DURATION_SECS="${DURATION_SECS:-300}"
RECORDS_PER_BATCH=256              # see crates/noise-spewer/src/gen.rs::render_dummy
EXPECTED_RECORDS=$(( BATCHES * RECORDS_PER_BATCH ))
LISTEN="${LISTEN:-127.0.0.1:4099}"
SMOKE_DIR="${SMOKE_DIR:-/tmp/scry-smoke}"

# ── Pre-flight ──────────────────────────────────────────────────────
if [[ ! -f docker/garage/.env ]]; then
    echo "[smoke] docker/garage/.env missing; run scripts/dev-garage-up.sh first" >&2
    exit 2
fi
# shellcheck disable=SC1091
set -a; source docker/garage/.env; set +a

if ! command -v aws >/dev/null; then
    echo "[smoke] aws CLI not on PATH — needed for bucket reset" >&2
    exit 2
fi

# GNU time (not the bash builtin) gives us peak RSS + total CPU
# of the sink over its whole lifetime, including the final flush
# triggered by SIGINT. The shell builtin `time` cannot do this.
TIME_BIN="${TIME_BIN:-/usr/bin/time}"
if [[ ! -x "$TIME_BIN" ]]; then
    echo "[smoke] GNU time not at $TIME_BIN — install \`time\` or set TIME_BIN" >&2
    exit 2
fi

# ── Build ───────────────────────────────────────────────────────────
echo "[smoke] building release binaries..."
cargo build --release -p noise-sink -p noise-spewer -p scry-list >&2

# ── Clean slate ─────────────────────────────────────────────────────
rm -rf "$SMOKE_DIR"
mkdir -p "$SMOKE_DIR"

echo "[smoke] emptying bucket s3://$SCRY_OBJSTORE_BUCKET/ ..."
AWS_ACCESS_KEY_ID="$SCRY_OBJSTORE_ACCESS_KEY_ID" \
AWS_SECRET_ACCESS_KEY="$SCRY_OBJSTORE_SECRET_ACCESS_KEY" \
AWS_REGION="$SCRY_OBJSTORE_REGION" \
    aws --endpoint-url "$SCRY_OBJSTORE_ENDPOINT" \
        s3 rm "s3://$SCRY_OBJSTORE_BUCKET/" --recursive >/dev/null || true

# ── Run the pipeline ────────────────────────────────────────────────
# noise-sink runs under /usr/bin/time so we can capture peak RSS +
# user/sys CPU across its whole lifetime, including the final flush.
# Time does NOT forward SIGINT to its child, so we send the shutdown
# signal directly to noise-sink via its PID (found with pgrep).
echo "[smoke] starting noise-sink on $LISTEN..."
RUST_LOG="${RUST_LOG:-info}" "$TIME_BIN" -v -o "$SMOKE_DIR/sink.time" \
    ./target/release/noise-sink \
        --listen "$LISTEN" \
        --storage \
        --wal-dir "$SMOKE_DIR/wal" \
        --catalog "$SMOKE_DIR/online.sqlite" \
    > "$SMOKE_DIR/sink.log" 2>&1 &
TIME_PID=$!
# Find the actual noise-sink child of /usr/bin/time. Fork+exec is
# fast but not instantaneous; poll briefly.
SINK_PID=""
for _ in $(seq 1 50); do
    SINK_PID=$(pgrep -P "$TIME_PID" 2>/dev/null || true)
    [[ -n "$SINK_PID" ]] && break
    sleep 0.05
done
if [[ -z "$SINK_PID" ]]; then
    echo "[smoke] could not locate noise-sink under time(pid=$TIME_PID)" >&2
    kill -9 "$TIME_PID" 2>/dev/null || true
    exit 1
fi
# Cleanup on script abort. Kill the sink first (so time writes its
# stats file), then the time wrapper if it survives.
trap 'kill -9 "$SINK_PID" "$TIME_PID" 2>/dev/null || true' EXIT

# Wait for the listener to actually bind. A small poll loop keeps us
# robust against slow startup without leaning on an arbitrary sleep.
for _ in $(seq 1 50); do
    if (echo > "/dev/tcp/${LISTEN%:*}/${LISTEN#*:}") 2>/dev/null; then
        break
    fi
    sleep 0.1
done

# Background RSS sampler. Emits "epoch_ns,vmrss_kb" every 100 ms so we
# can see the shape of memory growth — steady creep vs spike at flush
# — not just the peak number that `time -v` reports at the end.
(
    while [[ -d /proc/$SINK_PID ]]; do
        ts=$(date +%s%N)
        rss=$(awk '/^VmRSS:/ { print $2; exit }' /proc/$SINK_PID/status 2>/dev/null || echo "")
        [[ -n "$rss" ]] && printf '%s,%s\n' "$ts" "$rss"
        sleep 0.1
    done
) > "$SMOKE_DIR/rss.csv" 2>/dev/null &
RSS_PID=$!

echo "[smoke] spewer: $BATCHES batches × $RECORDS_PER_BATCH records = $EXPECTED_RECORDS records expected (rate=$RATE b/s, duration cap=${DURATION_SECS}s)"
./target/release/noise-spewer \
    --addr "$LISTEN" \
    --signals dummy \
    --rate "$RATE" \
    --duration "${DURATION_SECS}s" \
    --max-batches "$BATCHES" \
    > "$SMOKE_DIR/spewer.log" 2>&1


echo "[smoke] SIGINT noise-sink → graceful flush..."
kill -INT "$SINK_PID"
# Wait on the time wrapper, not on the sink directly — time exits
# after the sink does, AND it's the process that writes sink.time.
wait "$TIME_PID" 2>/dev/null || true
# Sampler exits on its own when /proc/$SINK_PID disappears; reap.
wait "$RSS_PID" 2>/dev/null || true
trap - EXIT

# ── Verify ──────────────────────────────────────────────────────────
echo "[smoke] reconciling a fresh catalog from the bucket..."
./target/release/scry-list \
    --catalog "$SMOKE_DIR/recon.sqlite" \
    > "$SMOKE_DIR/scry-list.txt" 2>&1
cat "$SMOKE_DIR/scry-list.txt"

# Parse the trailer line produced by scry-list:
#   # 1 block(s) in catalog (bucket=scry-dev)
#   <uuid>  <date>  rows=... bytes=... ts=...  signal=dummy  writer=...
#   # total rows=204800 bytes=9952617
block_count=$(awk '/^# [0-9]+ block\(s\) in catalog/ { print $2; exit }' "$SMOKE_DIR/scry-list.txt")
total_rows=$(awk -F'[= ]' '/^# total rows=/ { print $4; exit }' "$SMOKE_DIR/scry-list.txt")

# The correctness assertion is "every record the spewer sent landed
# in the catalog." We can't compare against EXPECTED_RECORDS directly
# because the spewer may have hit the duration cap (see above). Use
# the sink's session summary as the ground-truth count of records it
# accepted — that's what we want to match against the bucket.
sink_accepted=$(awk '
    /session_id=.* dummy=/ {
        for (i=1; i<=NF; i++) if ($i ~ /^dummy=/) { sub("dummy=","",$i); total += $i }
    }
    END { print total+0 }
' "$SMOKE_DIR/sink.log")

echo "[smoke] ──── assertions ────"
echo "[smoke] requested records : $EXPECTED_RECORDS"
echo "[smoke] sink accepted     : $sink_accepted"
echo "[smoke] catalog rows      : $total_rows"
echo "[smoke] catalog blocks    : $block_count"

failed=0
# Primary correctness assertion: every record the sink acked must
# show up in the bucket. If this fails, records were lost between
# WAL and parquet upload — a real durability bug.
if [[ "${total_rows:-}" != "${sink_accepted:-X}" ]]; then
    echo "[smoke] FAIL: catalog row count != sink-accepted (records lost between WAL and bucket)"
    failed=1
fi
if [[ -z "${block_count:-}" || "$block_count" -lt 1 ]]; then
    echo "[smoke] FAIL: catalog reports zero blocks"
    failed=1
fi
# Throughput observation: the sink's accepted count below
# EXPECTED_RECORDS means the spewer was rate-capped by sink-side
# back-pressure (currently: parquet+S3 flush blocks the ingest
# lock). Not a correctness failure, but worth flagging.
if [[ "${sink_accepted:-0}" -lt "$EXPECTED_RECORDS" ]]; then
    deficit=$(( EXPECTED_RECORDS - sink_accepted ))
    echo "[smoke] NOTE: sink throughput-capped; spewer fell ${deficit} records short of requested $EXPECTED_RECORDS"
    echo "[smoke]       (parquet+S3 flush blocks the ingest lock — see DummyPipeline::flush)"
fi

# ── Service performance ────────────────────────────────────────────
# Parse /usr/bin/time -v output. The full sink.time file is kept for
# anyone who wants the long form. We surface the headline numbers
# here so regressions are obvious in the smoke output itself.
#
# Most useful single regression sentinel: CPU-µs per record. It's
# wall-clock-independent so it doesn't slide around with machine
# load, and it captures the cost of the whole ingest pipeline
# (decode + WAL append + builder append + occasional parquet
# build/upload during the final flush).
if [[ -f "$SMOKE_DIR/sink.time" ]]; then
    awk -v records="$EXPECTED_RECORDS" '
        /Maximum resident set size/ { rss_kb     = $NF }
        /User time \(seconds\)/     { user_sec   = $NF }
        /System time \(seconds\)/   { sys_sec    = $NF }
        /Percent of CPU/            { cpu_pct    = $NF }
        /Elapsed \(wall clock\)/ {
            # Format: "h:mm:ss" or "m:ss.ss"
            n = split($NF, p, ":")
            if (n == 3) { wall = p[1]*3600 + p[2]*60 + p[3] }
            else        { wall = p[1]*60 + p[2] }
        }
        /Voluntary context switches/    { vcs  = $NF }
        /Involuntary context switches/  { ivcs = $NF }
        /Minor \(reclaiming/            { minflt = $NF }
        /Major \(requiring/             { majflt = $NF }
        END {
            cpu_sec    = user_sec + sys_sec
            rss_mib    = rss_kb / 1024.0
            rec_per_s  = (wall > 0) ? records / wall : 0
            us_per_rec = (records > 0) ? cpu_sec * 1e6 / records : 0
            printf "[smoke] ──── service performance ────\n"
            printf "[smoke] wall clock        : %.2f s\n", wall
            printf "[smoke] peak RSS          : %.1f MiB\n", rss_mib
            printf "[smoke] user CPU          : %.2f s\n", user_sec
            printf "[smoke] system CPU        : %.2f s\n", sys_sec
            printf "[smoke] %%CPU              : %s\n",     cpu_pct
            printf "[smoke] records/sec       : %.0f\n",    rec_per_s
            printf "[smoke] CPU-µs / record   : %.2f\n",    us_per_rec
            printf "[smoke] ctx switches      : voluntary=%s involuntary=%s\n", vcs, ivcs
            printf "[smoke] page faults       : minor=%s major=%s\n",           minflt, majflt
        }
    ' "$SMOKE_DIR/sink.time"

    if [[ -s "$SMOKE_DIR/rss.csv" ]]; then
        awk -F, '
            NR == 1 { t0 = $1; min = $2; max = $2; max_t = $1 }
            {
                if ($2 < min) min = $2
                if ($2 > max) { max = $2; max_t = $1 }
                samples[NR] = $2
                last_t = $1
            }
            END {
                n = NR
                if (n == 0) exit
                # Median
                # (simple insertion sort — n is at most a few hundred for our runs)
                for (i = 1; i <= n; i++) {
                    for (j = i; j > 1 && samples[j-1] > samples[j]; j--) {
                        tmp = samples[j]; samples[j] = samples[j-1]; samples[j-1] = tmp
                    }
                }
                median = (n % 2) ? samples[(n+1)/2] : (samples[n/2] + samples[n/2+1]) / 2
                wall_s     = (last_t - t0) / 1e9
                peak_at_s  = (max_t - t0) / 1e9
                printf "[smoke] RSS trace         : min=%.1f MiB  median=%.1f MiB  max=%.1f MiB  (n=%d over %.1fs)\n", \
                    min/1024.0, median/1024.0, max/1024.0, n, wall_s
                printf "[smoke] RSS peak at       : %.2f s into sink lifetime (%.0f%% of run)\n", \
                    peak_at_s, (wall_s > 0 ? peak_at_s/wall_s*100 : 0)
            }
        ' "$SMOKE_DIR/rss.csv"
    fi
else
    echo "[smoke] (no sink.time — perf stats unavailable)"
fi

if [[ $failed -eq 0 ]]; then
    echo "[smoke] PASS"
    exit 0
else
    echo "[smoke] sink log tail:"
    tail -20 "$SMOKE_DIR/sink.log" || true
    exit 1
fi
