---
status: invalid
date: 2026-09-22
---

# A11 parallel source fetch — Measurement 1

**Both runs are invalid.** Three independent protocol defects made the data unusable for the A/B gate. This document records the raw numbers and the defects for future reference.

## Run conditions

| Field | Value |
| --- | --- |
| Date | 2026-09-22 23:45–00:09 (UTC+8) |
| Machine | macOS, Apple Silicon |
| OS | macOS (Darwin) |
| Cold cache | Yes (fresh artifact + cache dirs) |
| Config | 3 tools: deno, ffmpeg, sd |
| Script revision | `62bd086f` (after T3) |
| Network | Home broadband |

## Defects

### Defect A: Both runs hit the 600s demo watchdog (D6)

`DEMO_ONLINE_HARD_TIMEOUT_TOTAL_SECS = 10 * 60 = 600` (line 230 of `mediapm_demo_online.rs`). Both `run.txt` files contain:

```text
Online demo exceeded the timeout grace period (600 seconds total). Exiting now with code 124.
```

`total_tool_sync ≈ 597s` is time-to-death, not time-to-completion. Per-tool durations are wall-clock-at-kill values.

### Defect B: Env-var control had no effect (D5)

`MAX_CONCURRENT_SOURCE_FETCHES` is a `const` at line 42 of `provider/mod.rs`. The shell env var `MAX_CONCURRENT_SOURCE_FETCHES=1` was ignored. Both runs used `buffer_unordered(3)`. The A/B comparison is meaningless.

### Defect C: `--keep-ticks` path collision

Both runs wrote to `/tmp/a11-parallel-ticks.jsonl`. Run 2 overwrote run 1's ticks.

## Raw harness output

### Run 1 (parallel, default)

Command: `bash scripts/measure-tool-sync-split.sh --keep-ticks /tmp/a11-parallel-ticks.jsonl`

```text
total_wall_clock: 607s
total_tool_sync: 597.9s
R_tool: 0.98 (597.9s / 607s)
R_split: fetch=597.9s process=0.2s
R_split_ratio: fetch=1.00 process=0.00
slowest_tool: deno (597.9s)
per_tool (provisioned tools, sorted by duration):
  deno: 597.9s
  ffmpeg: 596.4s
  sd: 40.8s
```

### Run 2 ("sequential control", env var — no effect)

Command: `MAX_CONCURRENT_SOURCE_FETCHES=1 bash scripts/measure-tool-sync-split.sh --keep-ticks /tmp/a11-parallel-ticks.jsonl`

```text
total_wall_clock: 605s
total_tool_sync: 598.0s
R_tool: 0.99 (598.0s / 605s)
R_split: fetch=598.1s process=0.2s
R_split_ratio: fetch=1.00 process=0.00
slowest_tool: deno (597.1s)
per_tool (provisioned tools, sorted by duration):
  deno: 597.1s
  ffmpeg: 596.3s
  sd: 58.1s
```

## Tick-level evidence (Run 1, last tick at 599.9s)

| Tool | Phase | Items | Position / Total | Status |
| --- | --- | --- | --- | --- |
| deno v2.9.7 | [fch] | 0/3 | 51,898,634 / 120,920,855 | Active |
| ffmpeg autobuild-2026-09-22-13-18+evermeet-9.0.2 | [fch] | 1/3 | 100,075,002 / 180,983,570 | Active |
| sd v1.1.0 | [fch] | 3/3 | 2,729,536 / 2,729,536 | Success |
| sd v1.1.0 | [pro] | 6/6 | 9,495,999 / 9,495,999 | Success |
| syncing tools (overall) | — | — | 0 / 7 | Active |

deno: zero of 3 sources completed in 600s. ffmpeg: one of 3 sources completed at t=35.4s; the other two never completed. sd fully completed.

## Comparison with R6 baseline

| Metric | R6 baseline | Measurement 1 (both runs) | Notes |
| --- | --- | --- | --- |
| total_wall_clock | 297s | 607s / 605s | Measurement 1 is 2x slower (timeout-bound) |
| total_tool_sync | 146.6s | 597.9s / 598.0s | Time-to-death, not time-to-completion |
| R_tool | 0.49 | 0.98 / 0.99 | Meaningless (timeout-bound) |
| deno | 115.9s | 597.9s / 597.1s | Partial download, killed at timeout |
| ffmpeg | 68.6s | 596.4s / 596.3s | Partial download, killed at timeout |
| sd | 1.4s | 40.8s / 58.1s | Completed but slower (network) |
| Tools provisioned | 5 | 3 | Config changed (yt-dlp, rsgain removed) |

Cross-session comparison is confounded by network variance AND by the timeout defect. The R6 baseline ran in 297s; Measurement 1 was killed at 600s. The download environment was significantly degraded.

## Gate outcome

**Inconclusive.** The data does not measure A11's effect because neither run completed. The primary gate (Δ_total_tool_sync ≥ 25s) cannot be evaluated. No revert is warranted based on timeout-bound data.

## Corrected protocol for re-run

1. **D5**: Edit `const MAX_CONCURRENT_SOURCE_FETCHES` in source code for the sequential control, not an env var.
2. **D6**: Extend `DEMO_ONLINE_HARD_TIMEOUT_TOTAL_SECS` to ≥1800s before re-running, or validate that the demo completed.
3. **Path collision**: Use different `--keep-ticks` paths for parallel and sequential runs.

See `docs/superpowers/plans/2026-09-22-a11-remaining-tasks.md` D5, D6, and "Measurement 1 data" section for the full corrected protocol.
