#!/usr/bin/env pwsh
# Measure the fetch/process split during mediapm tool synchronization.
#
# Runs the mediapm online demo with an empty cache and a JSONL tick sink,
# then parses the JSONL to compute:
#   - R_tool: tool-sync share of total runtime
#   - R_split: fetch vs process split within tool sync
#   - Per-tool wall-clock
#
# Requires: cargo, python3, network access.
# Timeout: 600s for the cargo run (background job).
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Show-Usage {
    $usage = @"
usage: measure-tool-sync-split.ps1 [--help]

Runs the mediapm online demo with an empty cache and a JSONL tick sink,
then parses the JSONL to compute:
  - R_tool: tool-sync share of total runtime
  - R_split: fetch vs process split within tool sync
  - Per-tool wall-clock

Requires: cargo, python3, network access.
Timeout: 600s for the cargo run.
"@
    Write-Output $usage
}

foreach ($arg in $args) {
    switch ($arg) {
        { $_ -eq '-h' -or $_ -eq '--help' } { Show-Usage; exit 0 }
        default {
            [Console]::Error.WriteLine("unknown argument: $arg")
            exit 1
        }
    }
}

$tmpdir = Join-Path ([System.IO.Path]::GetTempPath()) "mediapm-measure-$(Get-Random)"
New-Item -ItemType Directory -Path $tmpdir | Out-Null

try {
    Write-Output "=== running demo (empty cache, JSONL sink) ==="

    $env:MEDIAPM_EXAMPLE_CACHE_ROOT = Join-Path $tmpdir "cache"
    $env:MEDIAPM_PROGRESS_DEBUG = Join-Path $tmpdir "ticks.jsonl"
    $env:RUSTC_WRAPPER = ""

    $runLog = Join-Path $tmpdir "run.txt"

    # Run cargo in a background job with a 600s timeout.
    $job = Start-Job -ScriptBlock {
        param($logPath)
        & cargo run --package mediapm --example mediapm_demo_online > $logPath 2>&1
    } -ArgumentList $runLog

    $completed = Wait-Job $job -Timeout 600
    if ($null -eq $completed) {
        Stop-Job $job -ErrorAction SilentlyContinue
        Remove-Job $job -Force
        [Console]::Error.WriteLine("error: cargo run timed out after 600s")
        exit 1
    }
    Remove-Job $job

    $ticksPath = Join-Path $tmpdir "ticks.jsonl"
    if (-not (Test-Path $ticksPath) -or (Get-Item $ticksPath).Length -eq 0) {
        [Console]::Error.WriteLine("error: no JSONL ticks produced (demo may have failed)")
        Write-Output "cargo output (last 20 lines):"
        if (Test-Path $runLog) {
            Get-Content $runLog -Tail 20
        }
        exit 1
    }

    # Write the Python parser to a temp file and run it.
    $pyScript = Join-Path $tmpdir "parse_ticks.py"
    @'
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
    if "[" in prefix and prefix.endswith("]"):
        return prefix.rsplit("[", 1)[1].rstrip("]")
    return None


def extract_tool_id(label):
    parts = label.split(" ", 1)
    return parts[0] if parts else label


def is_bar_active(bar):
    pos = bar.get("position", 0)
    total = bar.get("total", 0)
    return total > 0 and pos < total


def is_tool_sync_bar(bar):
    phase = extract_phase(bar.get("prefix", ""))
    return phase is not None and phase in TOOL_SYNC_PHASES


def has_phase(bars, phase):
    for bar in bars:
        if extract_phase(bar.get("prefix", "")) == phase and is_bar_active(bar):
            return True
    return False


def has_any_tool_sync(bars):
    for bar in bars:
        if is_tool_sync_bar(bar) and is_bar_active(bar):
            return True
    return False


total_time = ticks[-1]["elapsed_secs"]
tool_sync_seconds = 0.0
fetch_seconds = 0.0
process_seconds = 0.0

for i in range(len(ticks) - 1):
    dt = ticks[i + 1]["elapsed_secs"] - ticks[i]["elapsed_secs"]
    bars = ticks[i]["bars"]

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

print("=== mediapm tool-sync measurement ===")
print(f"total_wall_clock: {total_time:.1f}s")
print(f"R_tool: {r_tool:.2f} ({tool_sync_seconds:.1f}s / {total_time:.1f}s)")
print(f"R_split: fetch={fetch_seconds:.1f}s process={process_seconds:.1f}s")
print(f"R_split_ratio: fetch={r_split_fetch:.2f} process={r_split_process:.2f}")
if slowest_tool:
    print(f"slowest_tool: {slowest_tool} ({slowest_time:.1f}s)")
else:
    print("slowest_tool: (none)")
'@ | Set-Content -Path $pyScript -Encoding UTF8

    & python3 $pyScript $ticksPath
}
finally {
    Remove-Item Env:MEDIAPM_EXAMPLE_CACHE_ROOT -ErrorAction SilentlyContinue
    Remove-Item Env:MEDIAPM_PROGRESS_DEBUG -ErrorAction SilentlyContinue
    Remove-Item Env:RUSTC_WRAPPER -ErrorAction SilentlyContinue

    if (Test-Path $tmpdir) {
        Remove-Item -Recurse -Force $tmpdir -ErrorAction SilentlyContinue
    }
}
