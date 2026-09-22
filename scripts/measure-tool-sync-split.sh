#!/usr/bin/env bash
# Measure the fetch/process split during mediapm tool synchronization.
#
# Runs the mediapm online demo with a COLD workspace (fresh artifact root +
# fresh download cache) and a JSONL tick sink, then parses the JSONL to
# compute:
#   - R_tool: tool-sync share of total runtime
#   - R_split: fetch vs process split within tool sync
#   - Per-tool wall-clock
#
# Requires: cargo, python3, network access.
# Timeout: 1800s for the cargo run (background watchdog).
#
# --ticks <path>  parse a captured JSONL fixture without running cargo (offline mode).
# --keep-ticks <path>  copy the ticks JSONL to <path> before cleanup.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
    cat <<'EOF'
usage: measure-tool-sync-split.sh [--help] [--ticks <path>] [--keep-ticks <path>]

Runs the mediapm online demo with a cold workspace and a JSONL tick sink,
then parses the JSONL to compute:
  - R_tool: tool-sync share of total runtime
  - R_split: fetch vs process split within tool sync
  - Per-tool wall-clock

Options:
  --ticks <path>     offline mode: parse a captured JSONL file (no network)
  --keep-ticks <path>  copy the ticks JSONL to <path> before cleanup

Requires: cargo, python3, network access (not needed for --ticks).
Timeout: 1800s for the cargo run.
EOF
}

WATCHDOG_SECS=1800
ticks_path=""
keep_ticks_path=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h | --help) usage; exit 0 ;;
        --ticks)
            if [[ $# -lt 2 ]]; then
                echo "error: --ticks requires a path" >&2; exit 1
            fi
            ticks_path="$2"; shift 2
            ;;
        --ticks=*) ticks_path="${1#--ticks=}"; shift ;;
        --keep-ticks)
            if [[ $# -lt 2 ]]; then
                echo "error: --keep-ticks requires a path" >&2; exit 1
            fi
            keep_ticks_path="$2"; shift 2
            ;;
        --keep-ticks=*) keep_ticks_path="${1#--keep-ticks=}"; shift ;;
        *) echo "unknown argument: $1" >&2; exit 1 ;;
    esac
done

# --- Offline mode: parse a pre-captured JSONL and exit ---
if [[ -n "$ticks_path" ]]; then
    if [[ ! -s "$ticks_path" ]]; then
        echo "error: --ticks file does not exist or is empty: $ticks_path" >&2; exit 1
    fi
    python3 - "$ticks_path" <<'PYEOF'
import json, re, sys

ticks_path = sys.argv[1]
ticks = []
with open(ticks_path) as f:
    for line in f:
        line = line.strip()
        if not line: continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if obj.get("type") == "tick":
            ticks.append(obj)

if not ticks:
    print("error: 0 ticks in JSONL", file=sys.stderr); sys.exit(1)

TOOL_SYNC_RE = re.compile(r'\[(res|fch|pro)\]')

def extract_phase(bar):
    for text in (bar.get("label", ""), bar.get("prefix", "")):
        m = TOOL_SYNC_RE.search(text)
        if m: return m.group(1)
    return None

def is_active(bar):
    status = bar.get("status", "")
    if status == "Active": return True
    pos = bar.get("position", 0)
    total = bar.get("total", 0)
    return total > 0 and pos < total

def tool_id(bar):
    return bar.get("label", "").split(" ", 1)[0] or ""

ts_ticks = []
for t in ticks:
    for b in t.get("bars", []):
        if extract_phase(b) and is_active(b):
            ts_ticks.append(t); break

if not ts_ticks:
    print("error: no tool-sync ticks found (provisioning never ran or harness is still warm)", file=sys.stderr); sys.exit(1)

ts_ticks.sort(key=lambda t: t["elapsed_secs"])
first_ts = ts_ticks[0]["elapsed_secs"]

fetch_s = 0.0; proc_s = 0.0
for i in range(len(ts_ticks) - 1):
    dt = ts_ticks[i+1]["elapsed_secs"] - ts_ticks[i]["elapsed_secs"]
    for b in ts_ticks[i]["bars"]:
        ph = extract_phase(b)
        if is_active(b) and ph == "fch": fetch_s += dt; break
    for b in ts_ticks[i]["bars"]:
        ph = extract_phase(b)
        if is_active(b) and ph == "pro": proc_s += dt; break
# Tail: if any bar is still active at the last tick, count a small tail
# (use the previous interval's dt as the estimate, or 0 if first tick).
if len(ts_ticks) >= 2:
    tail_dt = ts_ticks[-1]["elapsed_secs"] - ts_ticks[-2]["elapsed_secs"]
else:
    tail_dt = 0.0
for b in ts_ticks[-1]["bars"]:
    ph = extract_phase(b)
    if is_active(b) and ph == "fch": fetch_s += tail_dt; break
for b in ts_ticks[-1]["bars"]:
    ph = extract_phase(b)
    if is_active(b) and ph == "pro": proc_s += tail_dt; break

total_tool_sync = ts_ticks[-1]["elapsed_secs"] - first_ts
split_sum = fetch_s + proc_s
r_split_f = fetch_s / split_sum if split_sum > 0 else 0.0
r_split_p = proc_s / split_sum if split_sum > 0 else 0.0

tool_t = {}
for t in ts_ticks:
    for b in t["bars"]:
        if extract_phase(b) and is_active(b):
            tid = tool_id(b)
            e = t["elapsed_secs"]
            if tid not in tool_t: tool_t[tid] = [e, e]
            else: tool_t[tid][1] = e

per_tool = sorted(((tid, e[1]-e[0]) for tid, e in tool_t.items()), key=lambda x: -x[1])
slowest = per_tool[0] if per_tool else None

print("=== mediapm tool-sync measurement ===")
print(f"total_tool_sync: {total_tool_sync:.1f}s")
print(f"R_split: fetch={fetch_s:.1f}s process={proc_s:.1f}s")
print(f"R_split_ratio: fetch={r_split_f:.2f} process={r_split_p:.2f}")
print(f"per_tool (provisioned tools, sorted by duration):")
for tid, dur in per_tool:
    print(f"  {tid}: {dur:.1f}s")
if not per_tool:
    print("  (none)")
PYEOF
    exit 0
fi

# --- Live demo mode ---
tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

echo "=== running demo (cold workspace, JSONL sink) ===" >&2
echo "=== watchdog timeout: ${WATCHDOG_SECS}s ===" >&2

CACHE_DIR="$tmpdir/cache"
ARTIFACT_DIR="$tmpdir/artifact"
mkdir -p "$CACHE_DIR" "$ARTIFACT_DIR"

# Record wall-clock start (epoch seconds) for the shell-measured total.
WALL_START=$(date +%s)

MEDIAPM_EXAMPLE_CACHE_ROOT="$CACHE_DIR" \
MEDIAPM_EXAMPLE_ARTIFACT_ROOT="$ARTIFACT_DIR" \
MEDIAPM_PROGRESS_DEBUG="$tmpdir/ticks.jsonl" \
RUSTC_WRAPPER="" \
    cargo run --package mediapm --example mediapm_demo_online \
    > "$tmpdir/run.txt" 2>&1 &
CARGO_PID=$!

(
    sleep "$WATCHDOG_SECS"
    kill "$CARGO_PID" 2>/dev/null || true
) &
WATCHDOG_PID=$!

KILLED=0
wait "$CARGO_PID" || { RC=$?; if kill -0 "$CARGO_PID" 2>/dev/null; then KILLED=1; fi || true; }
kill "$WATCHDOG_PID" 2>/dev/null || true
wait "$WATCHDOG_PID" 2>/dev/null || true

WALL_END=$(date +%s)
WALL_SECS=$(( WALL_END - WALL_START ))

if [[ "$KILLED" -eq 1 ]]; then
    echo "error: cargo run killed after ${WALL_SECS}s (watchdog timeout ${WATCHDOG_SECS}s)" >&2
    echo "partial output retained at: $tmpdir" >&2
    # Disable trap so tmpdir is retained for diagnosis.
    trap - EXIT
    exit 1
fi

echo "=== demo completed in ${WALL_SECS}s ===" >&2

if [[ -n "$keep_ticks_path" ]]; then
    mkdir -p "$(dirname "$keep_ticks_path")"
    cp "$tmpdir/ticks.jsonl" "$keep_ticks_path"
    echo "=== ticks retained at ${keep_ticks_path} ===" >&2
fi

ticks_path="$tmpdir/ticks.jsonl"
if [[ ! -s "$ticks_path" ]]; then
    echo "error: no JSONL ticks produced (demo may have failed)" >&2
    echo "cargo output (last 20 lines):" >&2
    tail -20 "$tmpdir/run.txt" >&2 || true
    echo "partial output retained at: $tmpdir" >&2
    trap - EXIT
    exit 1
fi

# Parse JSONL and compute metrics.
python3 - "$ticks_path" "$WALL_SECS" <<'PYEOF'
import json, re, sys

ticks_path = sys.argv[1]
wall_clock = int(sys.argv[2])

ticks = []
with open(ticks_path) as f:
    for line in f:
        line = line.strip()
        if not line: continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if obj.get("type") == "tick":
            ticks.append(obj)

if not ticks:
    print("error: 0 ticks in JSONL", file=sys.stderr); sys.exit(1)

TOOL_SYNC_RE = re.compile(r'\[(res|fch|pro)\]')

def extract_phase(bar):
    for text in (bar.get("label", ""), bar.get("prefix", "")):
        m = TOOL_SYNC_RE.search(text)
        if m: return m.group(1)
    return None

def is_active(bar):
    status = bar.get("status", "")
    if status == "Active": return True
    pos = bar.get("position", 0)
    total = bar.get("total", 0)
    return total > 0 and pos < total

def tool_id(bar):
    return bar.get("label", "").split(" ", 1)[0] or ""

# Group isolation: restrict to ticks containing >=1 active res/fch/pro bar.
# Phase vocabulary is disjoint per screen (tool sync: res/fch/pro; workflow: wf;
# materialization: stg/vrf/cmt/wrt/mat), so this subset is exactly one group.
# [prn] excluded (separate owned_group, separate sink/clock).
ts_ticks = []
for t in ticks:
    for b in t.get("bars", []):
        if extract_phase(b) and is_active(b):
            ts_ticks.append(t); break

if not ts_ticks:
    print("error: no tool-sync ticks found", file=sys.stderr)
    print("diagnosis: provisioning never ran (harness is still warm) or demo failed", file=sys.stderr)
    sys.exit(1)

ts_ticks.sort(key=lambda t: t["elapsed_secs"])
first_ts = ts_ticks[0]["elapsed_secs"]

# Interval union within the isolated group's own clock.
# For each pair of consecutive ticks, check the PRECEDING tick's bars
# (they cover the interval to the next tick). Also check the LAST tick
# for any active bars still running at the end.
fetch_s = 0.0; proc_s = 0.0
for i in range(len(ts_ticks) - 1):
    dt = ts_ticks[i+1]["elapsed_secs"] - ts_ticks[i]["elapsed_secs"]
    for b in ts_ticks[i]["bars"]:
        ph = extract_phase(b)
        if is_active(b) and ph == "fch": fetch_s += dt; break
    for b in ts_ticks[i]["bars"]:
        ph = extract_phase(b)
        if is_active(b) and ph == "pro": proc_s += dt; break
# Tail: if any bar is still active at the last tick, count a small tail
# (use the previous interval's dt as the estimate, or 0 if first tick).
if len(ts_ticks) >= 2:
    tail_dt = ts_ticks[-1]["elapsed_secs"] - ts_ticks[-2]["elapsed_secs"]
else:
    tail_dt = 0.0
for b in ts_ticks[-1]["bars"]:
    ph = extract_phase(b)
    if is_active(b) and ph == "fch": fetch_s += tail_dt; break
for b in ts_ticks[-1]["bars"]:
    ph = extract_phase(b)
    if is_active(b) and ph == "pro": proc_s += tail_dt; break

total_tool_sync = ts_ticks[-1]["elapsed_secs"] - first_ts
r_tool = total_tool_sync / wall_clock if wall_clock > 0 else 0.0
split_sum = fetch_s + proc_s
r_split_f = fetch_s / split_sum if split_sum > 0 else 0.0
r_split_p = proc_s / split_sum if split_sum > 0 else 0.0

# Per-tool wall-clock: first/last active tick per tool.
tool_t = {}
for t in ts_ticks:
    for b in t["bars"]:
        if extract_phase(b) and is_active(b):
            tid = tool_id(b)
            e = t["elapsed_secs"]
            if tid not in tool_t: tool_t[tid] = [e, e]
            else: tool_t[tid][1] = e

per_tool = sorted(((tid, e[1]-e[0]) for tid, e in tool_t.items()), key=lambda x: -x[1])
slowest = per_tool[0] if per_tool else None

# --- Plausibility gate ---
if r_tool < 0.01 and not per_tool:
    print("error: R_tool < 0.01 and no provisioned tools detected", file=sys.stderr)
    print("harness failure — provisioning did not run or data is invalid", file=sys.stderr)
    sys.exit(1)

# --- Output ---
print("=== mediapm tool-sync measurement ===")
print(f"total_wall_clock: {wall_clock}s")
print(f"total_tool_sync: {total_tool_sync:.1f}s")
print(f"R_tool: {r_tool:.2f} ({total_tool_sync:.1f}s / {wall_clock}s)")
print(f"R_split: fetch={fetch_s:.1f}s process={proc_s:.1f}s")
print(f"R_split_ratio: fetch={r_split_f:.2f} process={r_split_p:.2f}")
if slowest:
    print(f"slowest_tool: {slowest[0]} ({slowest[1]:.1f}s)")
print(f"per_tool (provisioned tools, sorted by duration):")
for tid, dur in per_tool:
    print(f"  {tid}: {dur:.1f}s")
if not per_tool:
    print("  (none)")
PYEOF

# Disable trap so tmpdir is retained for diagnosis.
trap - EXIT
echo ""
echo "=== data retained at: $tmpdir ===" >&2
