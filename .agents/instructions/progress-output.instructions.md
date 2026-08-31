---
description: "Use when editing progress-bar rendering code in mediapm-utils or any consumer (conductor workflow screen, mediapm tool-sync, materialization). Records the ACTUAL rendered output format so agents never guess or invent ASCII mocks."
name: "Progress Bar Rendered Output Format"
applyTo: "src/mediapm-utils/src/progress.rs, src/mediapm-utils/src/progress/inner/**/*.rs, src/mediapm-conductor/src/orchestration/coordinator.rs, src/mediapm/src/output/progress.rs, src/mediapm/src/conductor_bridge/sync/**/*.rs"
---

# Progress bar rendered output format

This file is the authoritative reference for what progress bars actually look like on a terminal. Any agent editing progress code MUST read this before changing templates, glyphs, colors, prefix/suffix shapes, or ordering. Do NOT invent ASCII mocks.

## Source of truth

- `src/mediapm-utils/src/progress/inner/components.rs` — templates, styles, truncation functions (`semantic_truncate_prefix`, `semantic_truncate_suffix`, `render_prefix_components`, `render_suffix_components`)
- `src/mediapm-utils/src/progress/inner/renderer.rs` — layout (`recompute_layout`), single push point (`sync_snapshot_to_bar`), pre-roll, resize handling, debug sink
- Verified by `src/mediapm-utils/tests/progress_output/*.rs` using exact `assert_eq!(term.contents(), concat!(...))`

## Width constants

```text
MIN_PREFIX_WIDTH = 12      (decreaseable floor — bars never shrink below this)
MAX_PREFIX_WIDTH = 40      (hard ceiling — prefixes truncate beyond this)
MIN_SUFFIX_WIDTH = 12
MAX_SUFFIX_WIDTH = 50
```

`max_prefix_width(cols)` and `max_suffix_width(cols)` are `const fn` that return the constant ceilings; the `cols` parameter is intentionally unused. Clamping in `recompute_layout`: `prefix_w = max_prefix.clamp(MIN_PREFIX_WIDTH, max_prefix_width(cols))`, `suffix_w = max_suffix.clamp(MIN_SUFFIX_WIDTH, max_suffix_width(cols))`.

## Production templates

Templates are **dynamic `format!` strings** built inside `apply_*_bar_style` functions using live `prefix_w`/`suffix_w` values. There are NO static `CHILD_BAR_TEMPLATE`/`OVERALL_BAR_TEMPLATE` constants.

| Function | Template pattern | Wide bar style |
|---|---|---|
| `apply_overall_bar_style` | `{spinner:.green} {prefix:>{pw}.{pw}} {wide_bar:0.magenta/dim} {msg:<{sw}.{sw}}` | magenta/dim |
| `apply_bar_style` | `{spinner:.green} {prefix:>{pw}.{pw}} {wide_bar:0.yellow/dim} {msg:<{sw}.{sw}}` | yellow/dim |
| `apply_done_bar_style` | `{spinner:.white/.dim} {prefix:>{pw}.{pw}} {wide_bar:0.green/dim} {msg:<{sw}.{sw}}` | green/dim |
| `apply_failed_bar_style` | `{spinner:.red} {prefix:>{pw}.{pw}} {wide_bar:0.red/dim} {msg:<{sw}.{sw}}` | red/dim |

Where `{pw}` = `prefix_w`, `{sw}` = `suffix_w` (numeric, from the cell values). `prefix_w` is always ≥ 12 (clamped by `MIN_PREFIX_WIDTH`), so the dynamic template path is the only reachable path.

All styles: `tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")`, `progress_chars("█░")`.

## bar_color_code

| Status | is_overall | Color code |
|---|---|---|
| `Failed` | any | `"31"` (red) |
| `Active` | `true` | `"35"` (magenta) |
| `Active` | `false` | `"33"` (yellow) |
| `Warning` | any | `"33"` (yellow) |
| `Success` | any | `"32"` (green) |

Used by `render_suffix_components` to color the `count/total` segment.

## Truncation dispatch

`sync_snapshot_to_bar` (in `renderer.rs`) is the single push point from `SharedState` → indicatif. ANSI overhead for prefix truncation: **13 bytes** for `Failed`/`Warning` (reset + color escape), **4 bytes** for other states. It calls `semantic_truncate_prefix(&components, prefix_w - ansi_overhead)` and `semantic_truncate_suffix(&components, suffix_w)`. Client-defined truncation (via `ProgressBarApi::truncate_prefix` trait) takes precedence when installed; the renderer only calls the trait.

## Prefix truncation order

Removal order (least-important first), verified verbatim by `semantic_truncate_prefix_*` unit suites:

| Step | Component | Mode |
|---|---|---|
| 1 | `version` | Progressive (shrink chars one at a time) |
| 2 | `count`/`total` | Atomic (removed together, never partial) |
| 3 | `phase` | Atomic |
| 4 | `marker` | Atomic (`[F]`/`[W]` removed before tool_name) |
| 5 | `tool_name` | Progressive |
| 6 | fallback | Hard truncate whatever remains |

`count` and `total` are stored separately but rendered and trimmed as one unit — a bare count or bare total is never shown.

## Suffix truncation order

| Step | Component | Mode |
|---|---|---|
| 1 | `custom` | Progressive |
| 2 | `eta` | Atomic |
| 3 | `rate` | Atomic |
| 4 | `elapsed` | Atomic |
| 5 | `count`/`total` | Atomic (removed together) |
| 6 | fallback | Hard truncate |

`eta` renders only when `rate` is present (eta-only-when-rate guard).

## Test-only vs production output

- **Raw `ProgressBar` tests** use the test-only `{elapsed_precise}` template, producing `[00:00:00]` format (bracketed, HH:MM:SS).
- **`ProgressGroup` tests** use the production renderer, producing `0s` / `42s` / `1m35s` format via `format_elapsed` (compact, no brackets).
- Worked examples below are labeled accordingly.

## Per-screen specs

### Screen A: Tool-sync (`src/mediapm/src/conductor_bridge/sync/`)

Phases: `[res]` resolve, `[fch]` fetch, `[pro]` process, `[prn]` prune. Phases are shortened from longer names (`resolve` → `res`, `fetch` → `fch`, `process` → `pro`, `prune` → `prn`).

- **Resolve bar** shows `cached (N)` message via `SuffixComponents::custom`.
- **Skip bar** shows `skipped cached (N)` (when cached) vs `skipped` (when not cached).
- **Prune bar** (`[prn]`) uses the `tools.len()` call before the `retain` block to count prune candidates and sets the total to that count. Zero-bar guard: `[prn]` bar is not created when there are no candidates. The bar advances once per document-rewrite removal, then once more for filesystem prune.
- Overall bar uses `apply_overall_bar_style` (magenta); child bars use `apply_bar_style` (yellow).

### Screen B: Workflow (`src/mediapm-conductor/src/orchestration/`)

Phase: `[wf]`. Per-step child bars with tool name. Worker-slot bars show states: idle (`Active`, tool_name `"idle"`), active (`Active`, tool_name `"wf/step (tool)"`), pending-retry (`Warning`, marker `[W]`), failed (`Failed`, marker `[F]`), finalize (`Success`, tool_name `"idle"`). Worker labels are mediapm-agnostic — `tool` is the conductor step's own `ToolSpec.name`, never a managed-tool name.

### Screen C: Materialization (`src/mediapm/src/materializer/`)

Phases: `[mat]` overall, `[stg]` staging, `[vrf]` verify, `[cmt]` commit, `[wrt]` write (per-file sub-bar inside `media_folder` extraction). Per-entry child bars with `[stg]` → `[vrf]` → `[cmt]` phase transitions.

## Worked examples (Production, W=80)

Children render above the overall bar: child bars first, overall bar last.

### Screen A — tool-sync, mid-resolve (3 tools, ffmpeg resolving)

```text
⠋                   ffmpeg@7.1 [res] 0/100 0s 42.5/d
⠙                  yt-dlp@2025.1 [fch] 45/82 3s 14.2 MiB/s 4s
⠹                         [prn] 0/1
⠹                 syncing tools 3/12 0s 211/s 0s
```

- 4th line (overall, last): magenta spinner + magenta/dim bar.
- Lines 1-3: child bars, yellow spinner + yellow/dim bar.
- `[prn]` prune bar shows `N/M` count (M = prune candidates from `tools.len()` before `retain`; N = items removed so far + filesystem prune step).

### Screen B — workflow, mid-run (3 workers, pool_size=3)

```text
⠙                  default/s5 (echo@v1) 1/2 running
⠙                  default/s6 (import) 0/1 running
⠙                           idle 1/1 idle
⠹                       workflow 4/12 0s 211/s 0s
```

- Worker label format: `workflow_id/step_id (tool_name)`.
- Idle worker shows full empty bar (pinned total=1, pos=0).

### Screen C — materialization (media entry mid-commit)

```text
⠋                      media [stg] 2/5
⠙                   commit [cmt] 128/256
⠹                materialize 3/8 0s 12/s 0s
```

- Per-entry bar transitions `[stg]` → `[vrf]` → `[cmt]`.
- `[wrt]` per-file sub-bars appear inside `media_folder` entries.

## Debug JSONL

`MEDIAPM_PROGRESS_DEBUG=1` env var enables JSONL tick output to stderr. Each line is a JSON object with fields: `type` (`"tick"`), `tick` (monotonic counter), `elapsed_secs` (f64), `bars` (array of per-slot state: `slot`, `bound`, `label`, `prefix`, `position`, `total`, `status`, `elapsed_secs`, `rate_bytes_per_sec`, `eta_secs`, `suffix`, `dirty`).

## Pre-roll, gap conversion, finalize

- **Pre-roll**: On first draw, writes blank lines to scroll existing terminal content into scrollback, then repositions cursor. Only fires when `pre_roll_term` is `Some` (not in test mode).
- **Gap conversion**: Child bars that are inactive or finished show a fully-dimmed empty bar (`total=1, pos=0`). The `WorkerSpinner` style pins idle workers to this state.
- **Finalize**: `group.join()` returns after all bars finish; the renderer removes blank reserved slots and triggers a final draw so only finished bars persist in scrollback.

## Authoritative tests

These test files use `assert_eq!(term.contents(), concat!(...))` and ARE the real format:

- `src/mediapm-utils/tests/progress_output/*.rs` (terminal.rs, consumer.rs, transition.rs, progress_group.rs, spinner.rs, regression.rs, single_bar.rs, resolve_label.rs)
- `src/mediapm/src/output/progress.rs`
