#!/usr/bin/env bash
# v0.1/v0.2/v0.4 storage + query exit criterion — end-to-end smoke test.
#
# Sends a known number of batches through the wire path, runs the
# storage pipeline (WAL → block → object store → online catalog),
# then bootstraps a fresh catalog from the bucket via scry-list and
# asserts:
#
#   * the new catalog's total row count equals exactly the number of
#     records the sink accepted,
#   * at least one block landed in the bucket, and
#   * (metrics/logs/both) the reconciled catalog *queries back* the same
#     row count through scry-query — i.e. ingest → store → query is
#     loss-free. This last leg is the v0.4 exit criterion: the headline
#     of the logs milestone is querying logs back, so the seal proves the
#     full round-trip live, not just that bytes landed in the bucket.
#
# Parameterised by `SIGNAL` (default `dummy`):
#
#   SIGNAL=dummy   ./scripts/smoke.sh    # v0.1 path
#   SIGNAL=metrics ./scripts/smoke.sh    # v0.2 path (also asserts ≥1
#                                        #  block with has_postings=1)
#   SIGNAL=logs    ./scripts/smoke.sh    # v0.4 path (also asserts ≥1
#                                        #  block with has_postings=1 —
#                                        #  logs gets the same stream-
#                                        #  level postings as metrics)
#   SIGNAL=traces  ./scripts/smoke.sh    # v0.5 path (asserts query
#                                        #  round-trip: SELECT * rows ==
#                                        #  accepted, plus a --trace-id
#                                        #  by-id lookup returns exactly
#                                        #  that trace's spans)
#   SIGNAL=profiles ./scripts/smoke.sh   # v0.6 path (asserts retrieval
#                                        #  round-trip: SELECT * rows ==
#                                        #  accepted; raw pprof blob out)
#   SIGNAL=both    ./scripts/smoke.sh    # v0.4 cross-signal: spew
#                                        #  metrics + logs through the
#                                        #  same sink, assert both
#                                        #  sink-accepted counts and
#                                        #  the per-signal block count.
#   SIGNAL=all     ./scripts/smoke.sh    # full stack: round-robin all
#                                        #  four real signals (metrics,
#                                        #  logs, traces, profiles)
#                                        #  through one sink; per-signal
#                                        #  loss-free + block-shape
#                                        #  assertions; query leg for all
#                                        #  four + the --trace-id lookup.
#
# Records-per-batch and the connection-summary counter parsed from
# scry-ingestd's log are signal-specific. The per-signal record
# definitions match the server.rs counters:
#
#   dummy    → records  = DummyRecord count        (256/batch)
#   metrics  → records  = MetricSample count       (400/batch — 8 series × 50 samples)
#   logs     → records  = LogEntry count           (60/batch  — 3 streams × 20 entries)
#   traces   → records  = Span count               (20/batch  — 5 traces × 4 spans)
#   profiles → records  = ProfileBlob count        (1/batch)
#
# traces/profiles carry no postings sidecar (trace-by-id rides row-group
# trace_id stats; profiles query by (type, time) block stats), so the
# postings assertion block skips them — same as dummy.
#
# The dev Garage bucket (`scry-dev`) is emptied at the start of the
# run so the post-condition is unambiguous. Don't point this at any
# bucket whose contents you want to keep.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# ── Parameters ──────────────────────────────────────────────────────
SIGNAL="${SIGNAL:-dummy}"
case "$SIGNAL" in
    dummy)
        RECORDS_PER_BATCH=256        # crates/noise-spewer/src/gen.rs::render_dummy
        SUMMARY_TAG="dummy"          # counter key in server.rs connection summary
        ;;
    metrics)
        RECORDS_PER_BATCH=400        # crates/noise-spewer/src/gen.rs::render_metrics (8×50)
        SUMMARY_TAG="samples"
        ;;
    logs)
        RECORDS_PER_BATCH=60         # crates/noise-spewer/src/gen.rs::render_logs (3×20)
        SUMMARY_TAG="log_entries"
        ;;
    traces)
        RECORDS_PER_BATCH=20         # crates/noise-spewer/src/gen.rs::render_traces (5 traces × 4 spans)
        SUMMARY_TAG="spans"          # storage-only: no query round-trip leg
        ;;
    profiles)
        RECORDS_PER_BATCH=1          # crates/noise-spewer/src/gen.rs::render_profiles (1 blob/batch)
        SUMMARY_TAG="profiles"       # storage-only: no query round-trip leg
        ;;
    both)
        # Cross-signal mode: the spewer round-robins between metrics
        # and logs at the configured RATE. Approximations:
        #
        # - The spewer's --signals flag picks one signal per batch in
        #   round-robin order, so over a long run the per-signal batch
        #   counts converge to half each (± 1).
        # - EXPECTED_RECORDS isn't a single scalar in this mode; the
        #   metrics + logs assertions each compare their own
        #   sink-accepted count to (BATCHES/2) × that signal's records
        #   per batch.
        #
        # The legacy EXPECTED_RECORDS variable below is computed for
        # diagnostic reporting only (sum of both signals' expected
        # totals).
        RECORDS_PER_BATCH=0          # not meaningful in cross-signal mode
        SUMMARY_TAG=""               # parsed per-signal below
        ;;
    all)
        # Full-stack mode: the spewer round-robins all four real signals
        # (metrics, logs, traces, profiles) through one connection, so
        # over a long run each gets ≈ BATCHES/4 batches. Like `both`,
        # per-signal assertions live under "── Verify ──"; the query leg
        # now runs for all four signals, plus the --trace-id by-id lookup.
        RECORDS_PER_BATCH=0          # not meaningful in cross-signal mode
        SUMMARY_TAG=""               # parsed per-signal below
        ;;
    *)
        echo "[smoke] unsupported SIGNAL='$SIGNAL'; expected dummy|metrics|logs|traces|profiles|both|all" >&2
        exit 2
        ;;
esac

# DURATION_SECS is a generous upper bound; if the spewer can't reach
# --max-batches in time it logs "duration reached" and we treat what
# it actually sent as the source of truth (see assertions below).
# Don't make this too small — at high BATCHES the sink's effective
# throughput is bounded by parquet+S3 upload time, which can be well
# below the requested RATE.
BATCHES="${BATCHES:-2000}"
RATE="${RATE:-400}"
DURATION_SECS="${DURATION_SECS:-300}"
if [[ "$SIGNAL" == "both" ]]; then
    # Round-robin distribution: half the batches each. Used only for
    # diagnostic reporting in this mode; the real assertions live
    # under the "── Verify ──" section below.
    EXPECTED_METRICS=$(( (BATCHES / 2) * 400 ))
    EXPECTED_LOGS=$(( (BATCHES / 2) * 60 ))
    EXPECTED_RECORDS=$(( EXPECTED_METRICS + EXPECTED_LOGS ))
elif [[ "$SIGNAL" == "all" ]]; then
    # Round-robin across four signals: ≈ BATCHES/4 batches each.
    # Diagnostic only; assertions compare per-signal sink-accepted vs
    # catalog rows below.
    EXPECTED_METRICS=$(( (BATCHES / 4) * 400 ))
    EXPECTED_LOGS=$(( (BATCHES / 4) * 60 ))
    EXPECTED_TRACES=$(( (BATCHES / 4) * 20 ))
    EXPECTED_PROFILES=$(( (BATCHES / 4) * 1 ))
    EXPECTED_RECORDS=$(( EXPECTED_METRICS + EXPECTED_LOGS + EXPECTED_TRACES + EXPECTED_PROFILES ))
else
    EXPECTED_RECORDS=$(( BATCHES * RECORDS_PER_BATCH ))
fi

# For logs-bearing runs, force a small per-block row cap so the ingest
# volume seals *several* logs blocks instead of one. This is what makes
# the v0.7 full-text leg meaningful: the per-block body-bloom skip loop
# has to run across multiple candidate blocks (prune the misses, keep the
# hits) rather than trivially over a single block. A single spewer is one
# session → one shard, so `max_rows` governs block count directly. At the
# default BATCHES the smallest logs volume is `all` mode (~30k rows), so
# 20k caps to ≥2 logs blocks there and more in logs/both modes.
INGEST_EXTRA_ARGS=()
case "$SIGNAL" in
    logs|both|all) INGEST_EXTRA_ARGS+=(--block-max-rows "${BLOCK_MAX_ROWS:-20000}") ;;
esac
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
if [[ "$SIGNAL" == "metrics" || "$SIGNAL" == "logs" || "$SIGNAL" == "both" || "$SIGNAL" == "all" ]] \
   && ! command -v sqlite3 >/dev/null; then
    echo "[smoke] sqlite3 CLI not on PATH — needed for has_postings assertion" >&2
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
cargo build --release -p scry-ingestd -p noise-spewer -p scry-list -p scry-query >&2

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
# scry-ingestd runs under /usr/bin/time so we can capture peak RSS +
# user/sys CPU across its whole lifetime, including the final flush.
# Time does NOT forward SIGINT to its child, so we send the shutdown
# signal directly to scry-ingestd via its PID (found with pgrep).
echo "[smoke] starting scry-ingestd on $LISTEN (signal=$SIGNAL)..."
RUST_LOG="${RUST_LOG:-info}" "$TIME_BIN" -v -o "$SMOKE_DIR/sink.time" \
    ./target/release/scry-ingestd \
        --listen "$LISTEN" \
        --storage \
        --wal-dir "$SMOKE_DIR/wal" \
        --catalog "$SMOKE_DIR/online.sqlite" \
        "${INGEST_EXTRA_ARGS[@]+"${INGEST_EXTRA_ARGS[@]}"}" \
    > "$SMOKE_DIR/sink.log" 2>&1 &
TIME_PID=$!
# Find the actual scry-ingestd child of /usr/bin/time. Fork+exec is
# fast but not instantaneous; poll briefly.
SINK_PID=""
for _ in $(seq 1 50); do
    SINK_PID=$(pgrep -P "$TIME_PID" 2>/dev/null || true)
    [[ -n "$SINK_PID" ]] && break
    sleep 0.05
done
if [[ -z "$SINK_PID" ]]; then
    echo "[smoke] could not locate scry-ingestd under time(pid=$TIME_PID)" >&2
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

# Map the smoke-level SIGNAL to the spewer's `--signals` CSV. "both"
# round-robins between metrics and logs in the same connection.
case "$SIGNAL" in
    both) SPEWER_SIGNALS="metrics,logs" ;;
    all)  SPEWER_SIGNALS="metrics,logs,traces,profiles" ;;
    *)    SPEWER_SIGNALS="$SIGNAL" ;;
esac

if [[ "$SIGNAL" == "both" ]]; then
    echo "[smoke] spewer: $BATCHES batches (round-robin metrics+logs) ≈ $EXPECTED_METRICS metric samples + $EXPECTED_LOGS log entries (rate=$RATE b/s, duration cap=${DURATION_SECS}s)"
elif [[ "$SIGNAL" == "all" ]]; then
    echo "[smoke] spewer: $BATCHES batches (round-robin all 4 signals) ≈ $EXPECTED_METRICS samples + $EXPECTED_LOGS log entries + $EXPECTED_TRACES spans + $EXPECTED_PROFILES profiles (rate=$RATE b/s, duration cap=${DURATION_SECS}s)"
else
    echo "[smoke] spewer: $BATCHES batches × $RECORDS_PER_BATCH records = $EXPECTED_RECORDS records expected (rate=$RATE b/s, duration cap=${DURATION_SECS}s)"
fi
./target/release/noise-spewer \
    --addr "$LISTEN" \
    --signals "$SPEWER_SIGNALS" \
    --rate "$RATE" \
    --duration "${DURATION_SECS}s" \
    --max-batches "$BATCHES" \
    > "$SMOKE_DIR/spewer.log" 2>&1


echo "[smoke] SIGINT scry-ingestd → graceful flush..."
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
# accepted — that's what we want to match against the bucket. The
# summary tag (`dummy=` / `samples=` / `log_entries=`) is signal-
# specific because different signals count different things as a
# "record."
parse_sink_count() {
    local tag="$1"
    awk -v tag="${tag}=" '
        $0 ~ "session_id=.* " tag {
            for (i=1; i<=NF; i++) if (index($i, tag) == 1) {
                sub(tag, "", $i); total += $i
            }
        }
        END { print total+0 }
    ' "$SMOKE_DIR/sink.log"
}

if [[ "$SIGNAL" == "both" ]]; then
    sink_metrics=$(parse_sink_count samples)
    sink_logs=$(parse_sink_count log_entries)
    sink_accepted=$(( sink_metrics + sink_logs ))
elif [[ "$SIGNAL" == "all" ]]; then
    sink_metrics=$(parse_sink_count samples)
    sink_logs=$(parse_sink_count log_entries)
    sink_traces=$(parse_sink_count spans)
    sink_profiles=$(parse_sink_count profiles)
    sink_accepted=$(( sink_metrics + sink_logs + sink_traces + sink_profiles ))
else
    sink_accepted=$(parse_sink_count "$SUMMARY_TAG")
fi

echo "[smoke] ──── assertions ────"
echo "[smoke] signal            : $SIGNAL"
if [[ "$SIGNAL" == "both" ]]; then
    echo "[smoke] requested metrics : $EXPECTED_METRICS"
    echo "[smoke] requested logs    : $EXPECTED_LOGS"
    echo "[smoke] sink metrics      : $sink_metrics"
    echo "[smoke] sink logs         : $sink_logs"
elif [[ "$SIGNAL" == "all" ]]; then
    echo "[smoke] requested metrics : $EXPECTED_METRICS"
    echo "[smoke] requested logs    : $EXPECTED_LOGS"
    echo "[smoke] requested traces  : $EXPECTED_TRACES"
    echo "[smoke] requested profiles: $EXPECTED_PROFILES"
    echo "[smoke] sink metrics      : $sink_metrics"
    echo "[smoke] sink logs         : $sink_logs"
    echo "[smoke] sink traces       : $sink_traces"
    echo "[smoke] sink profiles     : $sink_profiles"
else
    echo "[smoke] requested records : $EXPECTED_RECORDS"
fi
echo "[smoke] sink accepted     : $sink_accepted"
echo "[smoke] catalog rows      : $total_rows"
echo "[smoke] catalog blocks    : $block_count"

failed=0
# Primary correctness assertion: every record the sink acked must
# show up in the bucket. If this fails, records were lost between
# WAL and parquet upload — a real durability bug. In `both` mode the
# catalog totals across signals, so the sum is what we compare.
if [[ "${total_rows:-}" != "${sink_accepted:-X}" ]]; then
    echo "[smoke] FAIL: catalog row count != sink-accepted (records lost between WAL and bucket)"
    failed=1
fi
if [[ -z "${block_count:-}" || "$block_count" -lt 1 ]]; then
    echo "[smoke] FAIL: catalog reports zero blocks"
    failed=1
fi

# Per-signal block-count + postings assertions. Metrics and logs both
# write a postings sidecar; dummy/traces/profiles don't. In `both`/`all`
# modes we want at least one block of each signal *and* the right
# postings shape for each — postings present for metrics/logs (the
# whole point of v0.4 step 1: the postings path works through the same
# plumbing), and postings *absent* for traces/profiles (trace-by-id rides
# row-group trace_id stats; profiles query by (type, time) block stats).
case "$SIGNAL" in
    metrics) expect_postings_for=("metrics") ;;
    logs)    expect_postings_for=("logs") ;;
    both)    expect_postings_for=("metrics" "logs") ;;
    all)     expect_postings_for=("metrics" "logs") ;;
    *)       expect_postings_for=() ;;
esac
if [[ ${#expect_postings_for[@]} -gt 0 ]]; then
    for sig in "${expect_postings_for[@]}"; do
        sig_blocks=$(sqlite3 "$SMOKE_DIR/recon.sqlite" \
            "SELECT COUNT(*) FROM blocks WHERE signal = '$sig';")
        sig_postings=$(sqlite3 "$SMOKE_DIR/recon.sqlite" \
            "SELECT COUNT(*) FROM blocks WHERE signal = '$sig' AND has_postings = 1 AND postings_size_bytes IS NOT NULL AND postings_size_bytes > 0;")
        echo "[smoke] $sig blocks         : $sig_blocks (with postings: $sig_postings)"
        if [[ "${sig_blocks:-0}" -lt 1 ]]; then
            echo "[smoke] FAIL: no $sig blocks landed in the bucket"
            failed=1
        fi
        if [[ "${sig_postings:-0}" -lt 1 ]]; then
            echo "[smoke] FAIL: no $sig blocks carry a postings sidecar"
            failed=1
        fi
    done
fi

# Body-bloom sidecar (v0.7 full-text): every logs block must carry a
# `body.bloom` sidecar — built inline at seal from the complete body set.
# Metrics/traces/profiles never do. Asserting `has_body_bloom=1` here is the
# storage-side half of the full-text milestone; the query-side half is the
# `--grep` ≡ `LIKE` leg below.
if [[ "$SIGNAL" == "logs" || "$SIGNAL" == "both" || "$SIGNAL" == "all" ]]; then
    logs_with_bloom=$(sqlite3 "$SMOKE_DIR/recon.sqlite" \
        "SELECT COUNT(*) FROM blocks WHERE signal = 'logs' AND has_body_bloom = 1 AND body_bloom_size_bytes IS NOT NULL AND body_bloom_size_bytes > 0;")
    logs_total_blocks=$(sqlite3 "$SMOKE_DIR/recon.sqlite" \
        "SELECT COUNT(*) FROM blocks WHERE signal = 'logs';")
    echo "[smoke] logs blocks w/ bloom: $logs_with_bloom / $logs_total_blocks"
    if [[ "${logs_with_bloom:-0}" -lt 1 || "${logs_with_bloom:-0}" -ne "${logs_total_blocks:-0}" ]]; then
        echo "[smoke] FAIL: not every logs block carries a body.bloom sidecar ($logs_with_bloom/$logs_total_blocks)"
        failed=1
    fi
    # The full-text leg below is only a real test of the per-block skip
    # loop if there's more than one logs block to prune over. `--block-max-rows`
    # is set above to force this; assert it actually happened.
    if [[ "${logs_total_blocks:-0}" -lt 2 ]]; then
        echo "[smoke] FAIL: expected ≥2 logs blocks (so the body-bloom skip runs across blocks); got ${logs_total_blocks:-0}"
        echo "[smoke]       (tune BLOCK_MAX_ROWS / BATCHES if ingest volume changed)"
        failed=1
    fi
fi

# No-postings signals (traces/profiles) in `all` mode: assert ≥1 block
# landed and that it carries NO postings sidecar (has_postings=0) — these
# signals push matcher/time/trace-id filters as row predicates instead of
# resolving postings. Single traces/profiles runs assert blocks-landed
# via the query round-trip above; here we make the no-postings invariant
# explicit since sqlite3 is already required.
if [[ "$SIGNAL" == "all" ]]; then
    for sig in traces profiles; do
        sig_blocks=$(sqlite3 "$SMOKE_DIR/recon.sqlite" \
            "SELECT COUNT(*) FROM blocks WHERE signal = '$sig';")
        sig_postings=$(sqlite3 "$SMOKE_DIR/recon.sqlite" \
            "SELECT COUNT(*) FROM blocks WHERE signal = '$sig' AND has_postings = 1;")
        echo "[smoke] $sig blocks        : $sig_blocks (with postings: $sig_postings)"
        if [[ "${sig_blocks:-0}" -lt 1 ]]; then
            echo "[smoke] FAIL: no $sig blocks landed in the bucket"
            failed=1
        fi
        if [[ "${sig_postings:-0}" -ne 0 ]]; then
            echo "[smoke] FAIL: $sig blocks must not carry postings (got $sig_postings)"
            failed=1
        fi
    done
fi

# ── Query round-trip (v0.4 exit criterion) ──────────────────────────
# Ingest + storage is only half the milestone; the headline of v0.4 is
# *querying logs back*. Drive the reconciled catalog through scry-query
# (implicit `SELECT * FROM <table>`, stream-drained) and assert the
# scanned row count equals what the bucket holds for that signal —
# proving the ingest → store → query round-trip is loss-free.
#
# This leg runs for every signal that has a query table — all four real
# signals now do (metrics, logs, traces, profiles). dummy has no table
# and is excluded. traces/profiles came online as query verticals in
# v0.5/v0.6: traces support `SELECT *` + matcher/time preselect +
# `--trace-id` by-id lookup; profiles support retrieval by time (raw
# pprof blob out). Flamegraph aggregation is still deferred (see
# docs/decisions.md D-034).
if [[ "$SIGNAL" != "dummy" ]]; then
    case "$SIGNAL" in
        metrics)  query_pairs=("metrics:$sink_accepted") ;;
        logs)     query_pairs=("logs:$sink_accepted") ;;
        traces)   query_pairs=("traces:$sink_accepted") ;;
        profiles) query_pairs=("profiles:$sink_accepted") ;;
        both)     query_pairs=("metrics:$sink_metrics" "logs:$sink_logs") ;;
        all)      query_pairs=("metrics:$sink_metrics" "logs:$sink_logs" "traces:$sink_traces" "profiles:$sink_profiles") ;;
    esac
    for pair in "${query_pairs[@]}"; do
        sig="${pair%%:*}"
        exp="${pair##*:}"
        # scry-query prints its trailer ("# scan: <N> rows total | ...")
        # on stderr; `|| true` keeps set -e from aborting before we get
        # to assert on the parsed count.
        ./target/release/scry-query \
            --catalog "$SMOKE_DIR/recon.sqlite" \
            --signal "$sig" \
            > "$SMOKE_DIR/query.$sig.txt" 2>&1 || true
        queried=$(awk '/^# scan:/ { print $3; exit }' "$SMOKE_DIR/query.$sig.txt")
        echo "[smoke] $sig queried rows  : ${queried:-<none>} (expected $exp)"
        if [[ "${queried:-}" != "$exp" ]]; then
            echo "[smoke] FAIL: $sig query returned ${queried:-<none>} rows, expected $exp (ingest→store→query lost rows)"
            echo "[smoke] scry-query output:"
            cat "$SMOKE_DIR/query.$sig.txt"
            failed=1
        fi
    done
fi

# ── Trace-by-id lookup (v0.5) ────────────────────────────────────────
# The headline traces operation is "give me one trace." Prove the
# `--trace-id` flag prunes to exactly that trace's spans and nothing
# else. Pick the densest trace_id from the landed blocks (hex via
# DataFusion's `encode()`), look it up by id, and assert the scanned
# row count equals that trace's own span count — so the lookup returns
# that trace and *only* that trace, not the whole table. `--trace-id`
# implies `--signal traces`, so we deliberately omit `--signal` here to
# exercise that inference too.
if [[ "$SIGNAL" == "traces" || "$SIGNAL" == "all" ]]; then
    ./target/release/scry-query \
        --catalog "$SMOKE_DIR/recon.sqlite" \
        --signal traces \
        --show \
        --sql "SELECT encode(trace_id, 'hex') AS tid, count(*) AS n FROM traces GROUP BY trace_id ORDER BY n DESC, tid LIMIT 1" \
        > "$SMOKE_DIR/query.traceid-pick.txt" 2>&1 || true
    # comfy_table output: border lines start with '+', header + data
    # rows start with '|'. The 2nd '|'-row is the first data row.
    read -r pick_tid pick_n < <(awk -F'|' '
        /^\|/ {
            cnt++
            if (cnt == 2) {
                gsub(/^[ \t]+|[ \t]+$/, "", $2)
                gsub(/^[ \t]+|[ \t]+$/, "", $3)
                print $2, $3
                exit
            }
        }' "$SMOKE_DIR/query.traceid-pick.txt")
    if [[ -z "${pick_tid:-}" || -z "${pick_n:-}" ]]; then
        echo "[smoke] FAIL: could not extract a trace_id to look up"
        echo "[smoke] pick query output:"
        cat "$SMOKE_DIR/query.traceid-pick.txt"
        failed=1
    else
        ./target/release/scry-query \
            --catalog "$SMOKE_DIR/recon.sqlite" \
            --trace-id "$pick_tid" \
            > "$SMOKE_DIR/query.traceid.txt" 2>&1 || true
        tid_rows=$(awk '/^# scan:/ { print $3; exit }' "$SMOKE_DIR/query.traceid.txt")
        echo "[smoke] trace-id lookup    : ${pick_tid:0:16}… → ${tid_rows:-<none>} spans (expected $pick_n)"
        if [[ "${tid_rows:-}" != "$pick_n" ]]; then
            echo "[smoke] FAIL: --trace-id returned ${tid_rows:-<none>} spans, expected $pick_n (pruning/predicate bug)"
            echo "[smoke] scry-query output:"
            cat "$SMOKE_DIR/query.traceid.txt"
            failed=1
        fi
    fi
fi

# ── Full-text search (v0.7) ──────────────────────────────────────────
# The headline logs operation is "grep the bodies." Prove two things:
#   1. The bloom-accelerated `--grep` path returns *exactly* the same rows
#      as an un-accelerated `body LIKE '%token%'` scan — the load-bearing
#      "skip never loses a match" equivalence, on a real bucket.
#   2. An absent token prunes to zero rows.
# The spewer renders every log body as "request <rand> processed in <n>ms"
# (see crates/noise-spewer/src/gen.rs::render_logs), so "processed" occurs
# in every body — a deterministic needle present in all logs blocks. We
# omit `--signal` on the grep call to exercise the grep→logs inference.
if [[ "$SIGNAL" == "logs" || "$SIGNAL" == "both" || "$SIGNAL" == "all" ]]; then
    case "$SIGNAL" in
        logs) logs_exp="$sink_accepted" ;;
        *)    logs_exp="$sink_logs" ;;
    esac

    # (1a) bloom-accelerated grep path.
    ./target/release/scry-query \
        --catalog "$SMOKE_DIR/recon.sqlite" \
        --grep processed \
        > "$SMOKE_DIR/query.grep.txt" 2>&1 || true
    grep_rows=$(awk '/^# scan:/ { print $3; exit }' "$SMOKE_DIR/query.grep.txt")

    # (1b) un-accelerated LIKE scan over the same column (no --grep → no
    # body_contains → no bloom skip; pure DataFusion substring scan).
    ./target/release/scry-query \
        --catalog "$SMOKE_DIR/recon.sqlite" \
        --signal logs \
        --sql "SELECT * FROM logs WHERE body LIKE '%processed%'" \
        > "$SMOKE_DIR/query.like.txt" 2>&1 || true
    like_rows=$(awk '/^# scan:/ { print $3; exit }' "$SMOKE_DIR/query.like.txt")

    echo "[smoke] grep 'processed'   : ${grep_rows:-<none>} rows (LIKE scan: ${like_rows:-<none>}, expected $logs_exp)"
    if [[ "${grep_rows:-}" != "${like_rows:-X}" ]]; then
        echo "[smoke] FAIL: --grep count (${grep_rows:-<none>}) != body LIKE count (${like_rows:-<none>}) — bloom skip lost rows"
        echo "[smoke] grep output:"; cat "$SMOKE_DIR/query.grep.txt"
        echo "[smoke] LIKE output:"; cat "$SMOKE_DIR/query.like.txt"
        failed=1
    fi
    # "processed" is in every body, so the match set is the full logs table.
    if [[ "${grep_rows:-}" != "$logs_exp" ]]; then
        echo "[smoke] FAIL: --grep 'processed' returned ${grep_rows:-<none>} rows, expected all $logs_exp logs rows"
        failed=1
    fi

    # (2) absent token → bloom prunes every block → zero rows.
    ./target/release/scry-query \
        --catalog "$SMOKE_DIR/recon.sqlite" \
        --grep "zzqq-no-such-token-xyz" \
        > "$SMOKE_DIR/query.grep-miss.txt" 2>&1 || true
    miss_rows=$(awk '/^# scan:/ { print $3; exit }' "$SMOKE_DIR/query.grep-miss.txt")
    echo "[smoke] grep absent token  : ${miss_rows:-<none>} rows (expected 0)"
    if [[ "${miss_rows:-}" != "0" ]]; then
        echo "[smoke] FAIL: --grep of an absent token returned ${miss_rows:-<none>} rows, expected 0"
        echo "[smoke] grep-miss output:"; cat "$SMOKE_DIR/query.grep-miss.txt"
        failed=1
    fi

    # (3) Selective needle. Pull a real per-line token from one body
    # (bodies are "request <rand8> processed in <n>ms"; the rand8 is
    # unique per entry) and grep for it. Because the token lives in only
    # one block (or few), the body-bloom *skips the other blocks* before
    # scan — and the result must still exactly equal an un-accelerated
    # LIKE scan. This is the equivalence that proves selective skipping is
    # loss-free, not just the all-match / no-match extremes. (rand8 is
    # alphanumeric, so it's safe to interpolate into LIKE and --grep.)
    ./target/release/scry-query \
        --catalog "$SMOKE_DIR/recon.sqlite" \
        --signal logs --show \
        --sql "SELECT body FROM logs LIMIT 1" \
        > "$SMOKE_DIR/query.body-pick.txt" 2>&1 || true
    needle_tok=$(awk -F'|' '
        /^\|/ {
            cnt++
            if (cnt == 2) {
                gsub(/^[ \t]+|[ \t]+$/, "", $2)
                split($2, a, " ")
                print a[2]   # "request <TOKEN> processed in …"
                exit
            }
        }' "$SMOKE_DIR/query.body-pick.txt")
    if [[ -z "${needle_tok:-}" || ! "$needle_tok" =~ ^[A-Za-z0-9]+$ ]]; then
        echo "[smoke] FAIL: could not extract a body token to grep (got '${needle_tok:-}')"
        echo "[smoke] body-pick output:"; cat "$SMOKE_DIR/query.body-pick.txt"
        failed=1
    else
        ./target/release/scry-query \
            --catalog "$SMOKE_DIR/recon.sqlite" \
            --grep "$needle_tok" \
            > "$SMOKE_DIR/query.grep-sel.txt" 2>&1 || true
        sel_grep=$(awk '/^# scan:/ { print $3; exit }' "$SMOKE_DIR/query.grep-sel.txt")
        ./target/release/scry-query \
            --catalog "$SMOKE_DIR/recon.sqlite" \
            --signal logs \
            --sql "SELECT * FROM logs WHERE body LIKE '%${needle_tok}%'" \
            > "$SMOKE_DIR/query.like-sel.txt" 2>&1 || true
        sel_like=$(awk '/^# scan:/ { print $3; exit }' "$SMOKE_DIR/query.like-sel.txt")
        echo "[smoke] grep selective tok : ${sel_grep:-<none>} rows (LIKE scan: ${sel_like:-<none>}, token=$needle_tok)"
        if [[ "${sel_grep:-}" != "${sel_like:-X}" ]]; then
            echo "[smoke] FAIL: selective --grep count (${sel_grep:-<none>}) != LIKE count (${sel_like:-<none>}) — bloom skip lost a match"
            echo "[smoke] grep output:"; cat "$SMOKE_DIR/query.grep-sel.txt"
            echo "[smoke] LIKE output:"; cat "$SMOKE_DIR/query.like-sel.txt"
            failed=1
        fi
        if [[ "${sel_grep:-0}" -lt 1 ]]; then
            echo "[smoke] FAIL: selective --grep '$needle_tok' returned ${sel_grep:-<none>} rows, expected ≥1 (the token came from a real body)"
            failed=1
        fi
    fi
fi

# Throughput observation: the sink's accepted count below
# EXPECTED_RECORDS means the spewer was rate-capped by sink-side
# back-pressure. Not a correctness failure, but worth flagging.
if [[ "${sink_accepted:-0}" -lt "$EXPECTED_RECORDS" ]]; then
    deficit=$(( EXPECTED_RECORDS - sink_accepted ))
    echo "[smoke] NOTE: sink throughput-capped; spewer fell ${deficit} records short of requested $EXPECTED_RECORDS"
fi

# In `both`/`all` modes the per-signal sink counts each get an additional
# observation versus their own per-signal expectation. Differences
# from the round-robin estimate are typically just the duration cap
# clipping mid-RR cycle.
if [[ "$SIGNAL" == "both" || "$SIGNAL" == "all" ]]; then
    if [[ "${sink_metrics:-0}" -lt "$EXPECTED_METRICS" ]]; then
        deficit=$(( EXPECTED_METRICS - sink_metrics ))
        echo "[smoke] NOTE: metrics fell $deficit short of expected $EXPECTED_METRICS"
    fi
    if [[ "${sink_logs:-0}" -lt "$EXPECTED_LOGS" ]]; then
        deficit=$(( EXPECTED_LOGS - sink_logs ))
        echo "[smoke] NOTE: logs fell $deficit short of expected $EXPECTED_LOGS"
    fi
fi
if [[ "$SIGNAL" == "all" ]]; then
    if [[ "${sink_traces:-0}" -lt "$EXPECTED_TRACES" ]]; then
        deficit=$(( EXPECTED_TRACES - sink_traces ))
        echo "[smoke] NOTE: traces fell $deficit short of expected $EXPECTED_TRACES"
    fi
    if [[ "${sink_profiles:-0}" -lt "$EXPECTED_PROFILES" ]]; then
        deficit=$(( EXPECTED_PROFILES - sink_profiles ))
        echo "[smoke] NOTE: profiles fell $deficit short of expected $EXPECTED_PROFILES"
    fi
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
