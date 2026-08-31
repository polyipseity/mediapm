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

## Coloring reference

All color is applied via ANSI SGR escapes (`\x1b[{code}m`). The `console` crate provides the escape generation; `bar_color_code` returns the raw numeric code as a `&str` for `format!` interpolation.

### Bar fill and spinner colors

Applied by the `apply_*_bar_style` functions in `components.rs`. Each function sets the spinner color, the bar fill color, and the dim suffix color.

| State | Spinner | Bar fill | Applied by |
|---|---|---|---|
| Active overall | green | magenta/dim | `apply_overall_bar_style` |
| Active child | green | yellow/dim | `apply_bar_style` |
| Success/Warning finished | white/dim | green/dim | `apply_done_bar_style` |
| Failed | red | red/dim | `apply_failed_bar_style` |

### Prefix marker colors

Rendered by `render_prefix_components`. The marker (`[F]` or `[W]`) is wrapped in ANSI color before the rest of the prefix.

| Marker | Color | ANSI code |
|---|---|---|
| `[F]` (Failed) | red bold | `\x1b[31m` |
| `[W]` (Warning) | yellow bold | `\x1b[33m` |
| No marker | uncolored | — |

### Suffix count/total color

`render_suffix_components` wraps the `count/total` segment in `\x1b[{code}m` using `bar_color_code(status, is_overall)`. The color matches the bar's semantic status, not the bar's visual style.

### `bar_color_code` lookup

| Status | is_overall | Color code | Effective color |
|---|---|---|---|
| `Failed` | any | `31` | red |
| `Active` | `true` | `35` | magenta |
| `Active` | `false` | `33` | yellow |
| `Warning` | any | `33` | yellow |
| `Success` | any | `32` | green |

### ANSI overhead in truncation

`sync_snapshot_to_bar` subtracts an ANSI overhead from `prefix_w` before calling `semantic_truncate_prefix`. This accounts for the escape sequences that will be inserted around the prefix content.

- **13 bytes** for `Failed`/`Warning` (reset + color escape around marker)
- **4 bytes** for other states (reset only)

See the [truncation ordering](#prefix-truncation-order) section below for how this interacts with component removal.

## Truncation dispatch

`sync_snapshot_to_bar` (in `renderer.rs`) is the single push point from `SharedState` → indicatif. It subtracts the ANSI overhead (see [coloring reference](#ansi-overhead-in-truncation)) from `prefix_w`, then calls `semantic_truncate_prefix(&components, prefix_w - ansi_overhead)` and `semantic_truncate_suffix(&components, suffix_w)`. Client-defined truncation (via `ProgressBarApi::truncate_prefix` trait) takes precedence when installed; the renderer only calls the trait.

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

## Post-finish result messages

After progress bars finish, the CLI prints structured result lines via primitives in `src/mediapm/src/output/report.rs`. These appear below the finalized progress bar output.

### Output primitives

| Primitive | Stream | Format | Styling |
|---|---|---|---|
| `print_result(icon, op, fields, duration)` | stdout | `{icon} {bold_op}    {k}={v}  {k}={v}  in {duration}` | icon: styled per StatusIcon; op: bold |
| `print_warning(msg)` | stderr | `Δ {msg}` | Δ yellow |
| `print_hint(msg)` | stderr | `→ {msg}` | → cyan bold |
| `print_heading(heading)` | stderr | `{heading}` + dim underline | heading bold, underline dim |
| `print_error(msg)` | stderr | `✗ {msg}` | ✗ red bold |

### StatusIcon glyphs

| Variant | Glyph | Style |
|---|---|---|
| `Success` | `✓` | green bold |
| `NoChange` | `–` | dim |
| `Warning` | `Δ` | yellow bold |
| `Error` | `✗` | red bold |

### Per-screen summary formats

**Screen A — `mediapm sync`** (via `print_sync_summary` in `output/mod.rs`):

```text
✓ sync complete    executed=3  cached=2  materialized=1  removed=1
  Δ warning message
```

- Icon: `✓` (green bold) when `executed > 0 || materialized > 0`; `–` (dim) otherwise.
- Fields: `executed` always shown; `cached`, `materialized`, `removed`, `removed_empty`, `added_tools`, `updated_tools` shown only when >0.
- Warnings: one `print_warning` line per warning.

**Screen A — `mediapm tool sync`** (via `print_result` + `ToolsSyncSummary`):

```text
✓ tools synced    added=2  updated=1  pruned=0  removed=0
  Δ warning message
```

- Fields: `added`, `updated`, `pruned`, `removed` (all always shown).
- Warnings: one `print_warning` line per warning.

**Screen B — workflow** (via `RunSummary`):

`RunSummary` is `#[derive(Debug)]` and printed via `{summary:?}`. The coordinator's overall bar suffix already shows `cached=N  failed=N  retried=N` before finish. There is no `print_result` call for workflow runs — the summary is conveyed through the progress bar's final state (success/warning) and the `RunSummary` struct returned to the caller.

**Screen C — materialization** (folded into sync summary):

Materialization results are folded into `SyncSummary.materialized_paths` and printed as part of the Screen A sync summary. No separate materialization summary line exists.

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

### Screen A — tool-sync post-finish

After all bars finish, the CLI prints:

```text
✓ sync complete    executed=3  cached=2  materialized=1  removed=1
  Δ some warning message
```

- `✓` is green bold; `sync complete` is bold.
- Fields separated by two spaces; `executed` always shown, others only when >0.
- Each warning on its own indented line with yellow `Δ`.

### Screen A — tool sync post-finish

```text
✓ tools synced    added=2  updated=1  pruned=0  removed=0
```

- All four fields always shown.
- No duration tracked for tool sync.

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

## Related files

- `src/mediapm/src/output/report.rs` — post-finish result primitives (`print_result`, `print_warning`, `print_hint`, `print_heading`, `print_error`, `StatusIcon`)
- `src/mediapm/src/output/mod.rs` — `print_sync_summary` (Screen A sync summary)
- `src/mediapm-conductor/src/api.rs` — `RunSummary` struct (Screen B)
- `src/mediapm/src/lib.rs` — `SyncSummary`, `ToolsSyncSummary` struct definitions
