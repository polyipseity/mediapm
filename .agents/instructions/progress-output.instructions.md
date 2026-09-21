---
description: "Use when editing progress-bar rendering code in mediapm-utils or any consumer (conductor workflow screen, mediapm tool-sync, materialization). Records the ACTUAL rendered output format so agents never guess or invent ASCII mocks."
name: "Progress Bar Rendered Output Format"
applyTo: "src/mediapm-utils/src/progress/mod.rs, src/mediapm-utils/src/progress/traits.rs, src/mediapm-utils/src/progress/recording.rs, src/mediapm-utils/src/progress/inner/**/*.rs, src/mediapm-utils/src/progress/truncation.rs, src/mediapm-conductor/src/orchestration/progress_labels.rs, src/mediapm-conductor/src/orchestration/coordinator.rs, src/mediapm/src/materializer/progress_labels.rs, src/mediapm/src/materializer/mod.rs, src/mediapm/src/output/progress.rs, src/mediapm/src/conductor_bridge/sync/**/*.rs"
---

# Progress bar rendered output format

This file is the authoritative reference for what progress bars actually look like on a terminal. Any agent editing progress code MUST read this before changing templates, glyphs, colors, prefix/suffix shapes, or ordering. Do NOT invent ASCII mocks.

## Source of truth

- `src/mediapm-utils/src/progress/inner/components.rs`: templates, styles, truncation functions (`semantic_truncate_prefix`, `semantic_truncate_suffix`, `render_prefix_components`, `render_suffix_components`)
- `src/mediapm-utils/src/progress/inner/renderer.rs`: layout (`recompute_layout`), single push point (`sync_snapshot_to_bar`), pre-roll, resize handling, debug sink
- Verified by `src/mediapm-utils/tests/progress_output/*.rs` using exact `assert_eq!(term.contents(), concat!(...))`

## Width constants

```text
MIN_PREFIX_WIDTH = 0       (decreasable floor, effective floor ~4 from ANSI reset in sync_snapshot_to_bar)
MAX_PREFIX_WIDTH = 40      (hard ceiling, prefixes truncate beyond this)
MIN_SUFFIX_WIDTH = 0       (decreasable floor, no ANSI overhead on suffix side)
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

### Status-list format rule

All status-list suffixes use the **number-first** format: `{n} {word}`, comma-joined, no parentheses. Each status is a single word (compound words banned). `None` → bare word, `Some(0)` → dropped, `Some(n)` → `n word`. The format is defined once here; other files link to this section. See `StatusCount` and `format_status_list` in `src/mediapm-utils/src/progress/mod.rs`.

### `bar_color_code` lookup

| Status | is_overall | Color code | Effective color |
|---|---|---|---|
| `Failed` | any | `31` | red |
| `Active` | `true` | `35` | magenta |
| `Active` | `false` | `33` | yellow |
| `Warning` | any | `33` | yellow |
| `Success` | any | `32` | green |

### ANSI overhead in truncation

`sync_snapshot_to_bar` subtracts an ANSI overhead from `prefix_w` before calling either the client-truncation trait or the built-in `semantic_truncate_prefix`/`render_prefix_components` path.

- **Client-truncated bars** (when `BarLabelTruncation` is installed via `set_truncation`): always **4 bytes**, since client strings carry no colored markers and only the ANSI reset is needed. The renderer prepends `\x1b[0m` to the client-truncated prefix string.
- **Built-in bars** (`PrefixComponents` path): **13 bytes** for `Failed`/`Warning` (reset + color escape around marker); **4 bytes** for all other states.

`recompute_layout` uses the same overhead logic to compute the available prefix width before layout.

## Truncation dispatch

`sync_snapshot_to_bar` (in `renderer.rs`) is the single push point from `SharedState` → indicatif. It determines whether client-defined truncation (`BarLabelTruncation` via `set_truncation`) or built-in truncation (`PrefixComponents` via `set_prefix_components`) applies:

- **Client-truncated bars**: `ansi_overhead = 4`. Calls `truncation.truncate_prefix(prefix_w - 4)` then prepends `\x1b[0m` to the result. Suffix path: calls `truncation.truncate_suffix(suffix_w, &fresh_suffix)` where `fresh_suffix` is the merged `SuffixComponents` (auto-derived + user-set fields).
- **Built-in bars**: `ansi_overhead` is 13 for `Failed`/`Warning`, 4 otherwise. Calls `semantic_truncate_prefix(&components, prefix_w - ansi_overhead)` then `render_prefix_components` to wrap the prefix in colored markers.

`recompute_layout` checks for installed client truncation to apply the same overhead rule for width budgeting. It also passes `&full_suffix` (the same merged components) to client truncation for width budgeting.

## Buffer ordering and style dedup

The tick loop and attach operation are designed to minimize visible flicker:

- **Buffer-first tick**: `tick()` enables the `WriteGate` (suppresses writes) BEFORE calling `recompute_layout()`. This ensures all `set_style` calls from `recompute_layout` → `sync_slot` → `apply_*_bar_style` are suppressed until `WriteGate::open()` releases them atomically.
- **Buffered attach**: `attach()` wraps its entire body in a `WriteGate::suppress()`/`open()` pair so slot shifts, sync_slot, and recompute_layout during bar attachment are buffered and appear atomically.
- **Style dedup**: `sync_slot` caches `(prefix_w, suffix_w, is_overall, status_code)` in `SlotCache` and only calls `apply_*_bar_style` when any component of this tuple changes. This eliminates redundant style writes when bar dimensions and status are stable across ticks.

## Three bar-label structs

All three structs implement [`BarLabelTruncation`] (defined in `src/mediapm-utils/src/progress/truncation.rs`) and are rendered via the client-truncation path in `sync_snapshot_to_bar`. The renderer prepends `\x1b[0m` (4-byte ANSI reset) to all client-truncated prefixes; no colored markers are embedded in the truncated string.

Truncation uses `truncate_ordered` (shared from `mediapm_utils::progress`). This function drops **trailing** parts first: the **first** element in the parts list is kept **longest**; the **last** element is dropped **first** under width pressure.

### Struct 1: `StepBarLabel` (conductor per-step bars)

**File**: `src/mediapm-conductor/src/orchestration/progress_labels.rs`

Carries real-progress fields: version, completed/total, phase, workflow/step identity.

| Field | Meaning | Example |
|-------|---------|---------|
| `version` | Tool version | `"7.1"` |
| `completed` / `total` | Progress tally | `"2"` / `"5"` |
| `phase` | Workflow phase tag | `"wf"` |
| `status_marker` | Terminal state marker | `""` / `"F"` / `"W"` |
| `workflow_id` | Workflow name | `"default"` |
| `step_id` | Step identifier | `"s3"` |
| `tool` | Conductor tool name | `"ffmpeg"` |

**Prefix parts order** (first = kept longest under truncation):

| Pos | Part | Rendered | Mode | Reasoning |
|-----|------|----------|------|-----------|
| 1 | `version` | `[7.1]` | Progressive | Version is least-important context; shrunk chars first |
| 2 | `count/total` | `2/5` | Atomic | Removed together; bare count never shown |
| 3 | `phase` | `[wf]` | Atomic | Removed entirely |
| 4 | `status_marker` | `[F]`/`[W]` | Atomic | Removed before tool identity |
| 5 | `workflow_id` | `default` | Progressive | Workflow name shrunk char-by-char |
| 6 | `step_id` | `s3` | Progressive | Step id shrunk char-by-char |
| 7 | `tool` | `(ffmpeg)` | Progressive | Tool name in parens; most important identity |
| 8 | fallback | — | Hard truncate | Characters beyond budget are cut |

**Prefix render shape:** `[version] count/total [phase] [marker] workflow_id step_id (tool)`

**Suffix parts order** (first = kept longest under truncation):

| Pos | Part | Mode |
|-----|------|------|
| 1 | `custom` | Progressive |
| 2 | `eta` | Atomic |
| 3 | `rate` | Atomic |
| 4 | `elapsed` | Atomic |
| 5 | `count/total` | Atomic |
| 6 | fallback | Hard truncate |

`eta` renders only when `rate` is present (eta-only-when-rate guard). `count` and `total` are stored separately but rendered and trimmed as one unit; a bare count or bare total is never shown.

### Struct 2: `WorkerBarLabel` (conductor worker-slot bars)

**File**: `src/mediapm-conductor/src/orchestration/progress_labels.rs`

Carries activity flag only, with no workflow phase and no progress tally.

| Field | Meaning | Example |
|-------|---------|---------|
| `status_marker` | Terminal state marker | `""` / `"F"` / `"W"` |
| `workflow_id` | Workflow name | `"default"` |
| `step_id` | Step identifier | `"s5"` |
| `tool` | Conductor tool name | `"echo"` |
| `activity` | Current activity | `"active"` / `"idle"` |

**Prefix parts order** (first = kept longest under truncation):

| Pos | Part | Rendered | Mode | Reasoning |
|-----|------|----------|------|-----------|
| 1 | `workflow_id` | `default` | Progressive | Workflow name is least-important; shrunk first |
| 2 | `step_id` | `s5` | Progressive | Step id shrunk char-by-char |
| 3 | `tool` | `(echo)` | Progressive | Tool name in parens |
| 4 | `activity` | `[active]`/`[idle]` | Atomic | Removed as unit |
| 5 | `status_marker` | `[F]`/`[W]` | Atomic | Removed |
| 6 | fallback | — | Hard truncate | Characters beyond budget are cut |

**Prefix render shape:** `workflow_id step_id (tool) [activity] [marker]`

**Suffix parts order:** Same as `StepBarLabel` (custom → eta → rate → elapsed → fallback).

### Struct 3: `MaterializationBarLabel` (mediapm materialization bars)

**File**: `src/mediapm/src/materializer/progress_labels.rs`

Carries file-path identity and phase. No version, no count/total, no workflow/step.

| Field | Meaning | Example |
|-------|---------|---------|
| `status_marker` | Terminal state marker | `""` / `"F"` |
| `entry_path` | Directory portion of hierarchy path | `"Music/Artist/Album"` |
| `entry_name` | Basename of hierarchy entry | `"song.mkv"` |
| `file_name` | Extracted file basename (sub-bars only) | `"cover.jpg"` |
| `phase` | Materialization phase tag | `"stg"` / `"vrf"` / `"cmt"` / `"wrt"` / `"mat"` |

**Prefix parts order** (first = kept longest under truncation):

| Pos | Part | Rendered | Mode | Reasoning |
|-----|------|----------|------|-----------|
| 1 | `entry_name` | `song.mkv` | Progressive | Basename kept longest — most important identity |
| 2 | `file_name` | `cover.jpg` | Progressive | Sub-bar extracted filename; only on per-file write sub-bars |
| 3 | `status_marker` | `[F]`/`[W]` | Atomic | Removed |
| 4 | `phase` | `[stg]`/`[vrf]`/`[cmt]`/`[wrt]`/`[mat]` | Atomic | Removed entirely |
| 5 | `entry_path` | `Music/Artist/Album` | Progressive | Directory path dropped first — losing leading segments is least harmful |
| 6 | fallback | — | Hard truncate | Characters beyond budget are cut |

**Prefix render shape:** `entry_name [phase] [marker] entry_path` (with optional `file_name` between `entry_name` and `marker`).

**Suffix:** None (materialization bars do not set suffix components).

### Why three structs instead of one

Each screen carries different semantic fields:

- **Step bars** need progress tallies (`completed`/`total`) and version, whereas materialization and worker bars have neither.
- **Worker bars** need activity flags but no progress tally or phase.
- **Materialization bars** need path/name decomposition but no workflow identity, no version, no count/total.

A single generic struct would either carry unused fields (polluting the API) or require `Option` wrappers everywhere. Three focused structs give each screen its own field names and its own truncation order with zero overhead.

### Legacy built-in `PrefixComponents` truncation order

The built-in `semantic_truncate_prefix` path (used when no `BarLabelTruncation` is installed) still follows this removal order:

| Step | Component | Mode |
|---|---|---|
| 1 | `version` | Progressive (shrink chars one at a time) |
| 2 | `count`/`total` | Atomic (removed together, never partial) |
| 3 | `phase` | Atomic |
| 4 | `marker` | Atomic (`[F]`/`[W]` removed before tool_name) |
| 5 | `tool_name` | Progressive |
| 6 | fallback | Hard truncate whatever remains |

`count` and `total` are stored separately but rendered and trimmed as one unit — a bare count or bare total is never shown. The built-in path is still used by Screen A (tool-sync) bars which do not install `BarLabelTruncation`.

## Test-only vs production output

- **Raw `ProgressBar` tests** use the test-only `{elapsed_precise}` template, producing `[00:00:00]` format (bracketed, HH:MM:SS).
- **`ProgressGroup` tests** use the production renderer, producing `0s` / `42s` / `1m35s` format via `format_elapsed` (compact, no brackets).
- Worked examples below are labeled accordingly.

## Per-screen specs

### Screen A: Tool-sync (`src/mediapm/src/conductor_bridge/sync/`)

Phases: `[res]` resolve, `[fch]` fetch, `[pro]` process, `[prn]` prune. Phases are shortened from longer names (`resolve` → `res`, `fetch` → `fch`, `process` → `pro`, `prune` → `prn`).

- **Resolve bar** shows `N cached` status list via `SuffixComponents::status_list`.
- **Skip bar** shows `skipped, N cached` status list (when cached) vs `skipped` (when not cached).
- **Prune bar** (`[prn]`) uses the `tools.len()` call before the `retain` block to count prune candidates and sets the total to that count. Zero-bar guard: `[prn]` bar is not created when there are no candidates. The bar advances once per document-rewrite removal, then once more for filesystem prune.
- Overall bar uses `apply_overall_bar_style` (magenta); child bars use `apply_bar_style` (yellow).

### Screen B: Workflow (`src/mediapm-conductor/src/orchestration/`)

Phase: `[wf]`. Per-step child bars use `StepBarLabel` for client-defined truncation. Worker-slot bars use `WorkerBarLabel` with separate `workflow_id`/`step_id`/`tool` fields.

Worker-slot states (`worker_slot_label` in `coordinator.rs`):

| State | `workflow_id` | `step_id` | `tool` | `activity` | `status_marker` |
|-------|---------------|-----------|--------|------------|------------------|
| Active | workflow name | step id | conductor tool name | `active` | (empty) |
| PendingRetry | (empty) | (empty) | `idle` | `idle` | `W` |
| Failed | (empty) | (empty) | `idle` | `idle` | `F` |
| Idle / Succeeded | (empty) | (empty) | `idle` | `idle` | (empty) |

Worker labels are mediapm-agnostic; `tool` is the conductor step's own `ToolSpec.name`, never a managed-tool name.

### Screen C: Materialization (`src/mediapm/src/materializer/`)

Uses `MaterializationBarLabel` for client-defined truncation. Paths are split into `entry_path` (directory) and `entry_name` (basename) via `split_entry_path`. Under width pressure, the directory path is truncated first while the basename is preserved.

Phases: `[mat]` overall, `[stg]` staging, `[vrf]` verify, `[cmt]` commit, `[wrt]` write (per-file sub-bar inside `media_folder` extraction). Per-entry child bars with `[stg]` → `[vrf]` → `[cmt]` phase transitions. No suffix components are set.

## Post-finish result messages

After progress bars finish, the CLI prints structured result lines via primitives centralized in `mediapm_utils::report` (feature `report`). These appear below the finalized progress bar output. Both `mediapm` and `mediapm-conductor` render through the same code, so the log format is identical across binaries.

### Output primitives

| Primitive | Stream | Format | Styling |
|---|---|---|---|
| `format_result_line(icon, op, fields, duration) -> String` | — | `{icon} {bold_op}    {k}={v}  {k}={v}  in {duration}` | Pure, testable |
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
✓ sync complete    executed=3  cached=2  materialized=5  pruned_tools=0  removed_tools=0
  Δ warning message
```

- Icon: `Warning` when `workflow_failed_steps > 0`; `Success` when `executed > 0 || materialized > 0`; `NoChange` otherwise.
- Fields: `executed` always shown; `cached`, `materialized`, `skipped`, `removed`, `removed_empty`, `added_tools`, `updated_tools`, `pruned_tools`, `removed_tools`, `skipped_tools`, `failed` shown only when >0.
- Warnings: one `print_warning` line per warning.

**Screen A — `mediapm tool sync`** (via `print_result` + `ToolsSyncSummary`):

```text
✓ tools synced    added=2  updated=1  pruned=0  removed=0
  Δ warning message
```

- Fields: `added`, `updated`, `pruned`, `removed` (all always shown).
- Warnings: one `print_warning` line per warning.

**Screen B — workflow** (via `CliSyncObserver` in `output/observer.rs`):

```text
✓ workflow    executed=3  cached=2  failed=0
```

- Icon: `Warning` when `failed_steps > 0`; `NoChange` when nothing ran; `Success` otherwise.
- Fields: `executed`, `cached`, `failed`.
- The conductor CLI (`conductor run`) renders identically via `format_result_line`.

**Screen C — materialization** (via `CliSyncObserver` in `output/observer.rs`):

```text
✓ materialized    paths=5  skipped=3  removed=1
```

- Icon: `Success` when `materialized + removed > 0`; `NoChange` otherwise.
- Fields: `paths`, `skipped`, `removed`.

### Per-phase observer contract

The library (`MediaPmService::sync_library_with_tag_update_checks_and_observer`) calls `observer.on_phase(report)` immediately after each screen's `group.join()` returns, in order: `Tools` → `Workflow` → `Materialization`. The `CliSyncObserver` renders structured `print_result` lines. Tests use a recording observer to assert the exact phase sequence and values.

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
⠙                  default s5 (echo@v1) [active] 1/2 running
⠙                  default s6 (import) [active] 0/1 running
⠙                          (idle) [idle] 1/1 idle
⠹                       workflow 4/12 0s 211/s 0s
```

- Worker label format: `workflow_id step_id (tool) [activity]` — each field is a separate truncatable part.
- Active workers show `workflow_id`, `step_id`, `(tool)`, `[active]`.
- Idle worker shows `(idle) [idle]` with full empty bar (pinned total=1, pos=0).

### Screen C — materialization (media entry mid-commit)

```text
⠋                      song.mkv [stg] 2/5
⠙                   song.mkv [cmt] 128/256
⠹             materializing [mat] 3/8 0s 12/s 0s
```

- Per-entry bar prefix: `entry_name [phase]` — basename is kept longest under truncation.
- Per-entry bar transitions `[stg]` → `[vrf]` → `[cmt]`.
- Overall bar: `materializing [mat]`.
- `[wrt]` per-file sub-bars appear inside `media_folder` entries with `file_name` set.

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
- **Gap conversion**: Child bars that are inactive or finished show a fully-dimmed empty bar (`total=1, pos=0`). Idle workers are explicitly finished (via `finish_success`), so they appear as dimmed empty bars. The `WorkerSpinner` style detects this state.
- **Finalize**: `group.join()` returns after all bars finish; the renderer removes blank reserved slots and triggers a final draw so only finished bars persist in scrollback.

## Authoritative tests

These test files use `assert_eq!(term.contents(), concat!(...))` and ARE the real format:

- `src/mediapm-utils/tests/progress_output/*.rs` (terminal.rs, consumer.rs, transition.rs, progress_group.rs, spinner.rs, regression.rs, single_bar.rs, resolve_label.rs)
- `src/mediapm/src/output/progress.rs`

## Global toggle and auto-detection

Progress is suppressed by passing `no_progress: true` or by constructing a `ProgressGroup::disabled()`. Progress bars are also automatically hidden when stderr is not a TTY (indicatif self-detects via `console::Term::stderr()`). The `--quiet` / `MEDIAPM_QUIET` flags suppress hints and progress.

## Spinner animation

Every progress bar uses a daemon ticker at 50 ms intervals, keeping the spinner animating even during long periods without position updates. The spinner uses braille dots: `⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏`. For deterministic tests, disable the ticker with `.with_ticker_enabled(false)`.

## Library stack

The styling stack uses `indicatif` 0.17 (`ProgressBar`, `MultiProgress`, `ProgressStyle`, `HumanBytes`, `HumanCount`) and `console` 0.15 (`Term::stderr().size()` for terminal width detection, `style()` for ANSI coloring). Do not add `owo-colors`, `colored`, `termion`, or other styling crates; `console::style()` is the single styling entry point.

## Formatting helpers

`format_bytes(u64) -> String` wraps `indicatif::HumanBytes` and produces output like `"650.23 MiB"` or `"1.24 GiB"`. `format_count(u64) -> String` wraps `indicatif::HumanCount` and produces output like `"1.2M"` or `"42"`. There is no `format_throughput` function; use `format_bytes(value) + "/s"` inline if throughput formatting is needed.

## Duration formatting

The `format_duration(Duration) -> String` function formats durations as follows: values under 1 second show two decimal places (e.g., `0.01s`, `0.05s`); values from 1 to 9 seconds show two decimal places (e.g., `1.00s`, `9.00s`); values from 10 to 59 seconds show whole seconds without decimals (e.g., `10s`, `42s`); values from 1 to 59 minutes show minutes and seconds (e.g., `1m 0s`, `30m 42s`); values of 1 hour or more show hours, minutes, and seconds (e.g., `1h 0m 0s`, `2h 15m 30s`).

## Dependency boundary rule

The conductor library (`mediapm-conductor`) must not depend on indicatif directly. It receives progress updates via `Fn` callbacks typed as `ProgressCallback`. Only the conductor CLI binary and the `mediapm` crate may use indicatif, accessed through `mediapm-utils/progress` with the `progress` feature enabled.

## Common usage pattern in handlers

Every CLI command handler follows a consistent shape: perform the operation, print the result line via `print_result` with the appropriate `StatusIcon`, then print any warnings or hints on stderr. The sync command passes a `CliSyncObserver` that prints per-phase result lines after each progress screen finishes, and `print_sync_summary` at the end for the combined summary.

## Related files

- `src/mediapm/src/output/report.rs` — post-finish result primitives (`print_result`, `print_warning`, `print_hint`, `print_heading`, `print_error`, `StatusIcon`)
- `src/mediapm/src/output/mod.rs` — `print_sync_summary` (Screen A sync summary)
- `src/mediapm-conductor/src/api.rs` — `RunSummary` struct (Screen B)
- `src/mediapm/src/lib.rs` — `SyncSummary`, `ToolsSyncSummary` struct definitions

## Module reference

| Module | Crate | Feature | Purpose |
|---|---|---|---|
| `mediapm_utils::report` | `mediapm-utils` | `report` | `StatusIcon`, `print_result`, `format_result_line`, `print_warning`, `print_hint`, `print_error`, `print_heading`, `print_status_report`, `format_duration` |
| `mediapm_utils::progress` | `mediapm-utils` | `progress` | `ProgressGroup`, `TrackedHandle`, `format_bytes`, `format_count` |
| `mediapm_utils::progress` (always) | `mediapm-utils` | — | `DownloadProgressSnapshot`, `ProgressCallback` |
| `mediapm::output::progress` | `mediapm` | — | `ProgressGroup`, `TrackedHandle`, `ProgressBarApi`, `ProgressGroupApi` re-exports |
| `mediapm::output::report` | `mediapm` | — | Re-exports from `mediapm_utils::report` |
