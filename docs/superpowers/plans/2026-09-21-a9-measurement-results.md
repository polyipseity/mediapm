# A9-2 Measurement Results

## Run conditions

| Field | Value |
| --- | --- |
| Date | 2026-09-22 |
| Machine | macOS arm64 (Apple Silicon) |
| OS | macOS |
| Cold cache | Yes (MEDIAPM_EXAMPLE_CACHE_ROOT + MEDIAPM_EXAMPLE_ARTIFACT_ROOT set to fresh tmpdir) |
| Script version | post-R5 (`793d3f10`) |
| Network | home broadband |

## Raw output

```text
=== running demo (cold workspace, JSONL sink) ===
=== watchdog timeout: 1800s ===
=== demo completed in 297s ===
=== mediapm tool-sync measurement ===
total_wall_clock: 297s
total_tool_sync: 146.6s
R_tool: 0.49 (146.6s / 297s)
R_split: fetch=133.7s process=43.0s
R_split_ratio: fetch=0.76 process=0.24
slowest_tool: deno (115.9s)
per_tool (provisioned tools, sorted by duration):
  deno: 115.9s
  ffmpeg: 68.6s
  yt-dlp: 29.1s
  rsgain: 3.2s
  sd: 1.4s
```

## Key numbers

| Metric | Value |
| --- | --- |
| total_wall_clock | 297s |
| total_tool_sync | 146.6s |
| R_tool | 0.49 |
| R_split fetch | 133.7s |
| R_split process | 43.0s |
| R_split_ratio fetch | 0.76 |
| R_split_ratio process | 0.24 |
| slowest_tool | deno (115.9s) |
| per_tool | deno: 115.9s, ffmpeg: 68.6s, yt-dlp: 29.1s, rsgain: 3.2s, sd: 1.4s |

## Plausibility check

- [x] `R_tool > 0.05` — 0.49 ✓
- [x] Per-tool count: 5 of 6 (media-tagger missing — launcher, no download, benign)
- [x] `total_tool_sync` materially > 0 — 146.6s ✓

## Per-source analysis (from `analyze-per-source.py`)

| Tool | Source 0 | Source 1 | Source 2 | Sequential | Parallel (max) | Savings |
| --- | --- | --- | --- | --- | --- | --- |
| deno | 71.5s (42.7MB) | 20.8s (38.5MB) | 13.9s (39.7MB) | 106.2s | 71.5s | 34.7s |
| ffmpeg | 8.1s (86.3MB) | 27.1s (27.7MB) | 4.3s (67.0MB) | 39.5s | 27.1s | 12.4s |
| yt-dlp | 1.8s (18.0MB) | 5.9s (37.0MB) | 19.6s (40.4MB) | 27.3s | 19.6s | 7.7s |

## A11 gate decision

| Condition | Threshold | Observed | Outcome |
| --- | --- | --- | --- |
| R_tool | ≥ 0.25 | 0.49 | passes |
| fetch share | ≥ 0.4 | 0.91 | passes → A11 candidate |
| process share | ≥ 0.4 | 0.29 | fails → A10 closed |
| deno dominant source | < 80% | 67% | passes (balanced) |
| bandwidth-bound | avg/min rate < 2× | 2.42× | partially, but deno source 0 at 0.57 MB/s vs source 1 at 1.77 MB/s on same connection → serialization-bound |

**A11 proceeds. A10 closed.**

## Notes

- media-tagger absent from per_tool: 1520 ticks, 0 ever Active. Launcher generation is instantaneous. Benign.
- per_tool sum (218.2s) > window (146.6s): expected — interval unions over concurrent tools.
- Cross-tool parallelism (MAX_CONCURRENT=4) compressed 218→147 (−33%).
- Critical path is deno (106.2s of 146.6s tool-sync window = 72%).
