#!/usr/bin/env bash
# Opt-in real-cgroup qualification for D-075 query memory pressure handling.
#
# This harness is deliberately non-destructive: it does not seed or empty a
# bucket. Point it at an already-running disposable queryd and provide commands
# for a selective query, a broad representative query, and a final recovery
# query. The queryd must be the sole process in QUERYD_CGROUP.
#
# Required:
#   QUERYD_CGROUP=/sys/fs/cgroup/.../queryd.scope
#   SELECTIVE_QUERY_COMMAND='target/release/scry-query-probe --addr ... --signal traces ...'
#   BROAD_QUERY_COMMAND='target/release/scry-query-probe --addr ... --signal logs ...'
#   RECOVERY_QUERY_COMMAND='target/release/scry-query-probe --addr ... --signal traces ...'
#
# Optional:
#   OUT_DIR=target/profile-query-memory
#
# The script captures kernel/process evidence, query latency, and status output
# when STATUS_URL is supplied. It fails on a query error, a new cgroup OOM event,
# or an unusable post-pressure daemon. It never writes cgroup controls or mutates
# production services.
set -euo pipefail

: "${QUERYD_CGROUP:?set QUERYD_CGROUP to the disposable queryd cgroup-v2 directory}"
: "${SELECTIVE_QUERY_COMMAND:?set SELECTIVE_QUERY_COMMAND to a selective non-destructive query}"
: "${BROAD_QUERY_COMMAND:?set BROAD_QUERY_COMMAND to a broad representative non-destructive query}"
: "${RECOVERY_QUERY_COMMAND:?set RECOVERY_QUERY_COMMAND to a post-pressure recovery query}"

for file in memory.current memory.max memory.peak memory.stat memory.events cgroup.procs; do
  if [[ ! -r "$QUERYD_CGROUP/$file" ]]; then
    echo "missing readable cgroup-v2 file: $QUERYD_CGROUP/$file" >&2
    exit 2
  fi
done

mapfile -t cgroup_pids <"$QUERYD_CGROUP/cgroup.procs"
if (( ${#cgroup_pids[@]} != 1 )); then
  echo "QUERYD_CGROUP must contain exactly one process; found ${#cgroup_pids[@]}" >&2
  exit 2
fi
QUERYD_PID=${cgroup_pids[0]}
if [[ ! -r "/proc/$QUERYD_PID/status" ]]; then
  echo "queryd process disappeared before qualification: $QUERYD_PID" >&2
  exit 2
fi

OUT_DIR=${OUT_DIR:-target/profile-query-memory}
mkdir -p "$OUT_DIR"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
RUN_DIR="$OUT_DIR/$STAMP"
mkdir -p "$RUN_DIR"

snapshot() {
  local name=$1
  cat "$QUERYD_CGROUP/memory.current" >"$RUN_DIR/$name.memory.current"
  cat "$QUERYD_CGROUP/memory.max" >"$RUN_DIR/$name.memory.max"
  cat "$QUERYD_CGROUP/memory.peak" >"$RUN_DIR/$name.memory.peak"
  cat "$QUERYD_CGROUP/memory.stat" >"$RUN_DIR/$name.memory.stat"
  cat "$QUERYD_CGROUP/memory.events" >"$RUN_DIR/$name.memory.events"
  awk '/^(VmRSS|VmHWM):/ { print }' "/proc/$QUERYD_PID/status" >"$RUN_DIR/$name.process-memory"
  if [[ -n ${STATUS_URL:-} ]]; then
    curl --fail --silent --show-error "$STATUS_URL" >"$RUN_DIR/$name.status.json"
  fi
}

event_value() {
  local snapshot_name=$1 key=$2
  awk -v key="$key" '$1 == key { print $2 }' "$RUN_DIR/$snapshot_name.memory.events"
}

run_query() {
  local name=$1 command=$2
  local start_ns end_ns status
  start_ns=$(date +%s%N)
  set +e
  bash -lc "$command" >"$RUN_DIR/$name.stdout" 2>"$RUN_DIR/$name.stderr"
  status=$?
  set -e
  end_ns=$(date +%s%N)
  printf '%s %s\n' "$status" "$(( (end_ns - start_ns) / 1000000 ))" >"$RUN_DIR/$name.result"
  if (( status != 0 )); then
    echo "FAIL: $name query exited $status; artifacts: $RUN_DIR" >&2
    exit "$status"
  fi
}

snapshot before
run_query selective "$SELECTIVE_QUERY_COMMAND"
snapshot after-selective
run_query broad "$BROAD_QUERY_COMMAND"
snapshot after-broad
run_query recovery "$RECOVERY_QUERY_COMMAND"
snapshot after-recovery

before_oom=$(event_value before oom)
after_oom=$(event_value after-recovery oom)
before_kill=$(event_value before oom_kill)
after_kill=$(event_value after-recovery oom_kill)

python3 - "$RUN_DIR" <<'PY'
import json
import pathlib
import sys

run = pathlib.Path(sys.argv[1])

def scalar(name):
    value = (run / name).read_text().strip()
    return None if value == "max" else int(value)

def stat(name):
    values = {}
    for line in (run / name).read_text().splitlines():
        key, value = line.split()
        values[key] = int(value)
    return values

def process_memory(name):
    values = {}
    for line in (run / name).read_text().splitlines():
        key, value, unit = line.split()
        assert unit == "kB"
        values[key.removesuffix(":") + "_bytes"] = int(value) * 1024
    return values

def query_result(name):
    status, elapsed_ms = (run / f"{name}.result").read_text().split()
    return {"exit_status": int(status), "elapsed_ms": int(elapsed_ms)}

def snapshot(name):
    values = stat(f"{name}.memory.stat")
    file_bytes = values.get("file", 0)
    shmem = values.get("shmem", 0)
    file_lru = max(0, values.get("inactive_file", 0) + values.get("active_file", 0) - shmem)
    ordinary_file = max(0, file_bytes - shmem)
    clean_file = max(0, min(ordinary_file, file_lru) - values.get("file_dirty", 0) - values.get("file_writeback", 0))
    current = scalar(f"{name}.memory.current")
    return {
        "current_bytes": current,
        "peak_bytes": scalar(f"{name}.memory.peak"),
        "committed_bytes": max(0, current - min(current, clean_file)),
        "reclaimable_clean_file_bytes": clean_file,
        "anon_bytes": values.get("anon", 0),
        "file_bytes": file_bytes,
        "shmem_bytes": shmem,
        "dirty_bytes": values.get("file_dirty", 0),
        "writeback_bytes": values.get("file_writeback", 0),
        **process_memory(f"{name}.process-memory"),
    }

summary = {
    "limit_bytes": scalar("before.memory.max"),
    "queries": {name: query_result(name) for name in ("selective", "broad", "recovery")},
    "snapshots": {name: snapshot(name) for name in ("before", "after-selective", "after-broad", "after-recovery")},
}
(run / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))
PY

if (( after_oom > before_oom || after_kill > before_kill )); then
  echo "FAIL: qualification incremented cgroup OOM counters; artifacts: $RUN_DIR" >&2
  exit 1
fi

echo "PASS: selective, broad, and recovery queries completed without a cgroup OOM event; artifacts: $RUN_DIR"
