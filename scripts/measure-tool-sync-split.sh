#!/usr/bin/env bash
# Measure the fetch/process split during mediapm tool synchronization.
#
# Runs the mediapm online demo with an empty cache and a JSONL tick sink,
# then parses the JSONL to compute:
#   - R_tool: tool-sync share of total runtime
#   - R_split: fetch vs process split within tool sync
#   - Per-tool wall-clock
#
# Requires: cargo, python3, network access.
# Timeout: 600s for the cargo run (background watchdog).
set -euo pipefail

usage() {
    cat <<'EOF'
usage: measure-tool-sync-split.sh [--help]

Runs the mediapm online demo with an empty cache and a JSONL tick sink,
then parses the JSONL to compute:
  - R_tool: tool-sync share of total runtime
  - R_split: fetch vs process split within tool sync
  - Per-tool wall-clock

Requires: cargo, python3, network access.
Timeout: 600s for the cargo run.
EOF
}

for arg in "$@"; do
    case "$arg" in
        -h | --help) usage; exit 0 ;;
        *) echo "unknown argument: $arg" >&2; exit 1 ;;
    esac
done

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

echo "=== running demo (empty cache, JSONL sink) ===" >&2

# Run cargo in background; watchdog kills after 600s.
MEDIAPM_EXAMPLE_CACHE_ROOT="$tmpdir/cache" \
MEDIAPM_PROGRESS_DEBUG="$tmpdir/ticks.jsonl" \
RUSTC_WRAPPER="" \
    cargo run --package mediapm --example mediapm_demo_online \
    > "$tmpdir/run.txt" 2>&1 &
CARGO_PID=$!

(
    sleep 600
    kill "$CARGO_PID" 2>/dev/null || true
) &
WATCHDOG_PID=$!

wait "$CARGO_PID" || true
kill "$WATCHDOG_PID" 2>/dev/null || true
wait "$WATCHDOG_PID" 2>/dev/null || true

ticks_path="$tmpdir/ticks.jsonl"
if [[ ! -s "$ticks_path" ]]; then
    echo "error: no JSONL ticks produced (demo may have failed)" >&2
    echo "cargo output (last 20 lines):" >&2
    tail -20 "$tmpdir/run.txt" >&2 || true
    exit 1
fi

python3 - "$ticks_path" <<'PYEOF'
import json
import sys

ticks_path = sys.argv[1]
ticks = []
with open(ticks_path) as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if obj.get("type") == "tick":
            ticks.append(obj)

if not ticks:
    print("error: 0 ticks in JSONL", file=sys.stderr)
    sys.exit(1)

ticks.sort(key=lambda t: t["tick"])

TOOL_SYNC_PHASES = {"res", "fch", "pro", "prn"}


def extract_phase(prefix):
    """Extract phase tag from prefix like 'yt-dlp 2025.07.15 [fch]'."""
    if "[" in prefix and prefix.endswith("]"):
        return prefix.rsplit("[", 1)[1].rstrip("]")
    return None


def extract_tool_id(label):
    """Extract tool_id from label like 'yt-dlp 2025.07.15 [fch]'."""
    parts = label.split(" ", 1)
    return parts[0] if parts else label


def is_bar_active(bar):
    """Check if a bar is still in progress."""
    pos = bar.get("position", 0)
    total = bar.get("total", 0)
    return total > 0 and pos < total


def is_tool_sync_bar(bar):
    """Check if a bar belongs to tool sync."""
    phase = extract_phase(bar.get("prefix", ""))
    return phase is not None and phase in TOOL_SYNC_PHASES


def has_phase(bars, phase):
    """Check if any bar has the given phase and is active."""
    for bar in bars:
        if extract_phase(bar.get("prefix", "")) == phase and is_bar_active(bar):
            return True
    return False


def has_any_tool_sync(bars):
    """Check if any tool-sync bar is active."""
    for bar in bars:
        if is_tool_sync_bar(bar) and is_bar_active(bar):
            return True
    return False


# Compute interval unions using tick-to-tick deltas.
total_time = ticks[-1]["elapsed_secs"]
tool_sync_seconds = 0.0
fetch_seconds = 0.0
process_seconds = 0.0

for i in range(len(ticks) - 1):
    dt = ticks[i + 1]["elapsed_secs"] - ticks[i]["elapsed_secs"]
    bars = ticks[i]["bars"]  # state at tick i covers interval to tick i+1

    if has_any_tool_sync(bars):
        tool_sync_seconds += dt
    if has_phase(bars, "fch"):
        fetch_seconds += dt
    if has_phase(bars, "pro"):
        process_seconds += dt

r_tool = tool_sync_seconds / total_time if total_time > 0 else 0.0
split_sum = fetch_seconds + process_seconds
r_split_fetch = fetch_seconds / split_sum if split_sum > 0 else 0.0
r_split_process = process_seconds / split_sum if split_sum > 0 else 0.0

# Per-tool wall-clock: first/last active tick per tool.
tool_first = {}
tool_last = {}
for tick in ticks:
    for bar in tick["bars"]:
        if is_tool_sync_bar(bar) and is_bar_active(bar):
            tool_id = extract_tool_id(bar.get("label", ""))
            t = tick["elapsed_secs"]
            if tool_id not in tool_first:
                tool_first[tool_id] = t
            tool_last[tool_id] = t

slowest_tool = ""
slowest_time = 0.0
for tool_id in tool_first:
    duration = tool_last[tool_id] - tool_first[tool_id]
    if duration > slowest_time:
        slowest_time = duration
        slowest_tool = tool_id

# Output.
print("=== mediapm tool-sync measurement ===")
print(f"total_wall_clock: {total_time:.1f}s")
print(f"R_tool: {r_tool:.2f} ({tool_sync_seconds:.1f}s / {total_time:.1f}s)")
print(f"R_split: fetch={fetch_seconds:.1f}s process={process_seconds:.1f}s")
print(f"R_split_ratio: fetch={r_split_fetch:.2f} process={r_split_process:.2f}")
if slowest_tool:
    print(f"slowest_tool: {slowest_tool} ({slowest_time:.1f}s)")
else:
    print("slowest_tool: (none)")
PYEOF
