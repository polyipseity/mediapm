#!/usr/bin/env pwsh
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
# Timeout: 1800s for the cargo run (background job).
#
# --ticks <path>  parse a captured JSONL fixture without running cargo (offline mode).
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$WATCHDOG_SECS = 1800

function Show-Usage {
    $usage = @"
usage: measure-tool-sync-split.ps1 [--help] [--ticks <path>]

Runs the mediapm online demo with a cold workspace and a JSONL tick sink,
then parses the JSONL to compute:
  - R_tool: tool-sync share of total runtime
  - R_split: fetch vs process split within tool sync
  - Per-tool wall-clock

Options:
  --ticks <path>  offline mode: parse a captured JSONL file (no network)

Requires: cargo, python3, network access (not needed for --ticks).
Timeout: 1800s for the cargo run.
"@
    Write-Output $usage
}

$ticksPath = $null

$i = 0
while ($i -lt $args.Count) {
    $arg = $args[$i]
    switch ($arg) {
        { $_ -eq '-h' -or $_ -eq '--help' } { Show-Usage; exit 0 }
        '--ticks' {
            if ($i + 1 -ge $args.Count) {
                [Console]::Error.WriteLine("error: --ticks requires a path")
                exit 1
            }
            $ticksPath = $args[$i + 1]
            $i += 2
        }
        default {
            if ($arg.StartsWith('--ticks=')) {
                $ticksPath = $arg.Substring('--ticks='.Length)
                $i += 1
            } else {
                [Console]::Error.WriteLine("unknown argument: $arg")
                exit 1
            }
        }
    }
}

# --- Python parser (identical logic for both offline and live modes) ---
$parserScript = @'
import json, re, sys

ticks_path = sys.argv[1]
wall_clock = int(sys.argv[2]) if len(sys.argv) > 2 else 0

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
    if wall_clock > 0:
        print("error: no tool-sync ticks found", file=sys.stderr)
        print("diagnosis: provisioning never ran (harness is still warm) or demo failed", file=sys.stderr)
    else:
        print("error: no tool-sync ticks found (provisioning never ran or harness is still warm)", file=sys.stderr)
    sys.exit(1)

ts_ticks.sort(key=lambda t: t["elapsed_secs"])
first_ts = ts_ticks[0]["elapsed_secs"]

# Interval union within the isolated group's own clock.
fetch_s = 0.0; proc_s = 0.0
for i in range(len(ts_ticks) - 1):
    dt = ts_ticks[i+1]["elapsed_secs"] - ts_ticks[i]["elapsed_secs"]
    for b in ts_ticks[i]["bars"]:
        ph = extract_phase(b)
        if is_active(b) and ph == "fch": fetch_s += dt; break
    for b in ts_ticks[i]["bars"]:
        ph = extract_phase(b)
        if is_active(b) and ph == "pro": proc_s += dt; break
# Tail: if any bar is still active at the last tick, count a small tail.
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

if wall_clock > 0:
    r_tool = total_tool_sync / wall_clock
    if r_tool < 0.01 and not per_tool:
        print("error: R_tool < 0.01 and no provisioned tools detected", file=sys.stderr)
        print("harness failure - provisioning did not run or data is invalid", file=sys.stderr)
        sys.exit(1)

print("=== mediapm tool-sync measurement ===")
if wall_clock > 0:
    print(f"total_wall_clock: {wall_clock}s")
print(f"total_tool_sync: {total_tool_sync:.1f}s")
if wall_clock > 0:
    print(f"R_tool: {r_tool:.2f} ({total_tool_sync:.1f}s / {wall_clock}s)")
print(f"R_split: fetch={fetch_s:.1f}s process={proc_s:.1f}s")
print(f"R_split_ratio: fetch={r_split_f:.2f} process={r_split_p:.2f}")
if per_tool:
    print(f"slowest_tool: {per_tool[0][0]} ({per_tool[0][1]:.1f}s)")
print(f"per_tool (provisioned tools, sorted by duration):")
for tid, dur in per_tool:
    print(f"  {tid}: {dur:.1f}s")
if not per_tool:
    print("  (none)")
'@

# --- Offline mode: parse a pre-captured JSONL and exit ---
if ($null -ne $ticksPath) {
    if (-not (Test-Path $ticksPath) -or (Get-Item $ticksPath).Length -eq 0) {
        [Console]::Error.WriteLine("error: --ticks file does not exist or is empty: $ticksPath")
        exit 1
    }
    $pyFile = Join-Path ([System.IO.Path]::GetTempPath()) "mediapm-parse-$(Get-Random).py"
    try {
        $parserScript | Set-Content -Path $pyFile -Encoding UTF8
        $output = & python3 $pyFile $ticksPath 2>&1 | Out-String
        [Console]::Write($output)
        exit $LASTEXITCODE
    } finally {
        if (Test-Path $pyFile) { Remove-Item $pyFile -ErrorAction SilentlyContinue }
    }
}

# --- Live demo mode ---
$tmpdir = Join-Path ([System.IO.Path]::GetTempPath()) "mediapm-measure-$(Get-Random)"
New-Item -ItemType Directory -Path $tmpdir | Out-Null

$cleanupTmpdir = $true
try {
    Write-Output "=== running demo (cold workspace, JSONL sink) ==="
    Write-Output "=== watchdog timeout: ${WATCHDOG_SECS}s ==="

    $env:MEDIAPM_EXAMPLE_CACHE_ROOT = Join-Path $tmpdir "cache"
    $env:MEDIAPM_EXAMPLE_ARTIFACT_ROOT = Join-Path $tmpdir "artifact"
    $env:MEDIAPM_PROGRESS_DEBUG = Join-Path $tmpdir "ticks.jsonl"
    $env:RUSTC_WRAPPER = ""

    $runLog = Join-Path $tmpdir "run.txt"

    $wallStart = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()

    $job = Start-Job -ScriptBlock {
        param($logPath)
        & cargo run --package mediapm --example mediapm_demo_online > $logPath 2>&1
    } -ArgumentList $runLog

    $completed = Wait-Job $job -Timeout $WATCHDOG_SECS
    if ($null -eq $completed) {
        Stop-Job $job -ErrorAction SilentlyContinue
        Remove-Job $job -Force
        $wallEnd = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
        $wallSecs = $wallEnd - $wallStart
        [Console]::Error.WriteLine("error: cargo run killed after ${wallSecs}s (watchdog timeout ${WATCHDOG_SECS}s)")
        [Console]::Error.WriteLine("partial output retained at: $tmpdir")
        $cleanupTmpdir = $false
        exit 1
    }
    Remove-Job $job

    $wallEnd = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    $wallSecs = $wallEnd - $wallStart
    Write-Output "=== demo completed in ${wallSecs}s ==="

    $ticksFile = Join-Path $tmpdir "ticks.jsonl"
    if (-not (Test-Path $ticksFile) -or (Get-Item $ticksFile).Length -eq 0) {
        [Console]::Error.WriteLine("error: no JSONL ticks produced (demo may have failed)")
        Write-Output "cargo output (last 20 lines):"
        if (Test-Path $runLog) {
            Get-Content $runLog -Tail 20
        }
        [Console]::Error.WriteLine("partial output retained at: $tmpdir")
        $cleanupTmpdir = $false
        exit 1
    }

    $pyScript = Join-Path $tmpdir "parse_ticks.py"
    $parserScript | Set-Content -Path $pyScript -Encoding UTF8

    $output = & python3 $pyScript $ticksFile $wallSecs 2>&1 | Out-String
    [Console]::Write($output)
    $cleanupTmpdir = $false
} finally {
    Remove-Item Env:MEDIAPM_EXAMPLE_CACHE_ROOT -ErrorAction SilentlyContinue
    Remove-Item Env:MEDIAPM_EXAMPLE_ARTIFACT_ROOT -ErrorAction SilentlyContinue
    Remove-Item Env:MEDIAPM_PROGRESS_DEBUG -ErrorAction SilentlyContinue
    Remove-Item Env:RUSTC_WRAPPER -ErrorAction SilentlyContinue

    if ($cleanupTmpdir -and (Test-Path $tmpdir)) {
        Remove-Item -Recurse -Force $tmpdir -ErrorAction SilentlyContinue
    } elseif (-not $cleanupTmpdir -and (Test-Path $tmpdir)) {
        Write-Output ""
        Write-Output "=== data retained at: $tmpdir ==="
    }
}
