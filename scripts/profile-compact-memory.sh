#!/usr/bin/env bash
# Opt-in Linux cgroup-v2 qualification harness for D-070.
#
# This script observes an already-running compactor PID. Run the compactor with
# an explicit persistent --spill-dir and constrained memory budget, ideally via
# systemd-run/container cgroup delegation, then trigger representative concurrent
# compaction separately. The script fails if the cgroup records an OOM event.
set -euo pipefail

usage() {
  echo "usage: $0 PID OUTPUT_DIR [SAMPLE_SECONDS]" >&2
  echo "example: $0 \$(pgrep -n scry) artifacts/compact-memory 120" >&2
  exit 2
}

[[ $# -ge 2 && $# -le 3 ]] || usage
pid=$1
out=$2
seconds=${3:-60}
[[ -r "/proc/$pid/status" ]] || { echo "cannot read /proc/$pid/status" >&2; exit 1; }
mkdir -p "$out"

cgroup_rel=$(awk -F: '$1 == "0" { print $3; exit }' "/proc/$pid/cgroup")
[[ -n "$cgroup_rel" ]] || { echo "PID is not in a visible cgroup-v2 hierarchy" >&2; exit 1; }
cgroup_dir="/sys/fs/cgroup${cgroup_rel}"
for file in memory.current memory.events memory.max; do
  [[ -r "$cgroup_dir/$file" ]] || { echo "missing $cgroup_dir/$file" >&2; exit 1; }
done

cp "$cgroup_dir/memory.events" "$out/memory.events.before"
printf 'epoch_ns,vmrss_kib,cgroup_current_bytes\n' > "$out/samples.csv"
end=$((SECONDS + seconds))
while (( SECONDS < end )) && [[ -d "/proc/$pid" ]]; do
  ts=$(date +%s%N)
  rss=$(awk '/^VmRSS:/ { print $2; exit }' "/proc/$pid/status")
  current=$(cat "$cgroup_dir/memory.current")
  printf '%s,%s,%s\n' "$ts" "$rss" "$current" >> "$out/samples.csv"
  sleep 0.1
done

cp "$cgroup_dir/memory.events" "$out/memory.events.after"
cat "$cgroup_dir/memory.max" > "$out/memory.max"
if [[ -r "$cgroup_dir/memory.peak" ]]; then
  cat "$cgroup_dir/memory.peak" > "$out/memory.peak"
fi

before_oom=$(awk '$1 == "oom" { print $2 }' "$out/memory.events.before")
after_oom=$(awk '$1 == "oom" { print $2 }' "$out/memory.events.after")
before_kill=$(awk '$1 == "oom_kill" { print $2 }' "$out/memory.events.before")
after_kill=$(awk '$1 == "oom_kill" { print $2 }' "$out/memory.events.after")
if (( after_oom > before_oom || after_kill > before_kill )); then
  echo "FAIL: cgroup recorded OOM activity; see $out" >&2
  exit 1
fi
[[ -d "/proc/$pid" ]] || { echo "FAIL: compactor exited during qualification" >&2; exit 1; }
echo "PASS: no OOM events and compactor survived; measurements in $out"
