---
description: "Use when editing progress-bar rendering code in mediapm-utils or any consumer (conductor workflow screen, mediapm tool-sync, materialization). Records the ACTUAL rendered output format so agents never guess or invent ASCII mocks."
name: "Progress Bar Rendered Output Format"
applyTo: "src/mediapm-utils/src/progress/mod.rs, src/mediapm-utils/src/progress/traits.rs, src/mediapm-utils/src/progress/recording.rs, src/mediapm-utils/src/progress/inner/**/*.rs, src/mediapm-utils/src/progress/truncation.rs, src/mediapm-conductor/src/orchestration/progress_labels.rs, src/mediapm-conductor/src/orchestration/coordinator.rs, src/mediapm/src/materializer/progress_labels.rs, src/mediapm/src/materializer/mod.rs, src/mediapm/src/output/progress.rs, src/mediapm/src/conductor_bridge/sync/**/*.rs"
---

# Progress bar rendered output format

This file records the rules the renderer follows: what goes in a prefix, how a label is ranked for truncation, and which screen installs which label. For what a frame actually looks like, the authority is the three runnable examples listed under "Worked examples" and the transcripts they generate. Any agent editing progress code MUST read this before changing templates, glyphs, colors, prefix/suffix shapes, or ordering, and MUST NOT hand-write a terminal frame into documentation.

## Source of truth

- `src/mediapm-utils/src/progress/inner/components.rs`: templates, styles, truncation functions (`semantic_truncate_prefix`, `semantic_truncate_suffix`, `render_prefix_components`, `render_suffix_components`), plus `StatusCount` (`:19`) and `format_status_list` (`:31`)
- `src/mediapm-utils/src/progress/inner/renderer.rs`: layout (`recompute_layout`), single push point (`sync_snapshot_to_bar`), pre-roll, resize handling, debug sink
- Verified by `src/mediapm-utils/tests/progress_output/*.rs` using exact `assert_eq!(term.contents(), concat!(...))`, and by the three runnable screen examples listed under "Worked examples"

## Width constants

```text
MIN_PREFIX_WIDTH = 0       (decreasable floor, effective floor ~4 from ANSI reset in sync_snapshot_to_bar)
MAX_PREFIX_WIDTH = 40      (hard ceiling, prefixes truncate beyond this)
MIN_SUFFIX_WIDTH = 0       (decreasable floor, no ANSI overhead on suffix side)
MAX_SUFFIX_WIDTH = 65
```

`max_prefix_width()` (`src/mediapm-utils/src/progress/inner/components.rs:569`) and `max_suffix_width()` (`:574`) are `const fn` returning those two ceilings. Neither takes an argument, so no terminal width enters the budget. `recompute_layout` clamps the widths it measured against the constants: `prefix_w = max_prefix.clamp(MIN_PREFIX_WIDTH, MAX_PREFIX_WIDTH)`, `suffix_w = max_suffix.clamp(MIN_SUFFIX_WIDTH, MAX_SUFFIX_WIDTH)`.

## Two layout facts to know before changing a label

**The seed label sets the prefix ceiling.** The renderer measures the label a bar was created with, not the text that bar draws. `SharedState::with_time_source_and_style` splits the seed into prefix components at `src/mediapm-utils/src/progress/inner/renderer.rs:180`, and `recompute_layout` takes the screen-wide maximum of those measured prefixes at `:920`. Client truncation can only cut width, never add it, so the seed is the ceiling and a longer rendered label is cut down to it. The conductor seeds every worker slot with `idle [wf]` (`src/mediapm-conductor/src/orchestration/coordinator.rs:343`), so on the workflow screen an active worker row renders `[active]` and nothing else at any terminal width. This is the behaviour behind the bug report that prompted these examples.

**The budget never subtracts the terminal width.** Because the two helpers take no argument, narrowing the terminal shrinks the fill first and the labels not at all. Each screen runs out of fill at a different width, and no template switches at any of them: the workflow screen still has a fill column at 41 columns and has none at 40, materialization still gives every row one at 39 and has none at 37, and tool sync is squeezed to a six-column bar at 60 and breaks the same way below 54 (`src/mediapm/examples/mediapm_progress_workflow.rs:23` and `:134`, `mediapm_progress_materialize.rs:29`, `mediapm_progress_tool_sync.rs:79`). That is why the examples default to width 80 and the unit tests pin 40 (`common::mk()` in `src/mediapm-utils/tests/progress_output/common.rs`): 40 is a width that keeps assertions short, not a width chosen to be realistic.

## Production templates

Templates are **dynamic `format!` strings** built inside `apply_*_bar_style` functions using live `prefix_w`/`suffix_w` values. There are NO static `CHILD_BAR_TEMPLATE`/`OVERALL_BAR_TEMPLATE` constants.

| Function | Template pattern | Wide bar style |
|---|---|---|
| `apply_overall_bar_style` | `{spinner:.green} {prefix:>{pw}.{pw}} {wide_bar:0.magenta/dim} {msg:<{sw}.{sw}}` | magenta/dim |
| `apply_bar_style` | `{spinner:.green} {prefix:>{pw}.{pw}} {wide_bar:0.yellow/dim} {msg:<{sw}.{sw}}` | yellow/dim |
| `apply_done_bar_style` | `{spinner:.white/.dim} {prefix:>{pw}.{pw}} {wide_bar:0.green/dim} {msg:<{sw}.{sw}}` | green/dim |
| `apply_failed_bar_style` | `{spinner:.red} {prefix:>{pw}.{pw}} {wide_bar:0.red/dim} {msg:<{sw}.{sw}}` | red/dim |

Where `{pw}` = `prefix_w` and `{sw}` = `suffix_w`, both read from the cells `recompute_layout` writes. There is no static template to fall back to.

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

Only two of the three are installed by production code: the conductor coordinator installs `WorkerBarLabel`, the mediapm materializer installs `MaterializationBarLabel`, and `StepBarLabel` has no production caller. It is exercised by `mediapm_progress_workflow` and by `src/mediapm-conductor/tests/int/progress_labels.rs`, so the type is supported and tested while a live workflow draws no such bar.

Fitting uses `fit_segments` (shared from `mediapm_utils::progress`). Segments are supplied most important first and the list is walked from the tail, so the **last** segment is the first to yield. Under width pressure, elastic segments are shortened from the front and keep their informative tail, so a path retains its filename and immediate parent; once no segment can shrink further, whole segments drop from the tail until the remainder fits. No segment is ever cut at a character boundary: a segment is shown whole, shortened from the front, or absent.

**Segment order alone determines the outcome.** `fit_segments` drops from the tail unconditionally, so a field survives by its position in the list. When designing a new label, rank the fields and mark the shrink behaviour (`Shrink::Keep` for whole, `Shrink::FrontEllipsis` for elastic).

The per-screen orderings summarised under each struct below are a convenience, not the spec. The normative ranking, and the reasoning behind each segment's classification, live in the `prefix_segments` and `suffix_segments` bodies of `src/mediapm-conductor/src/orchestration/progress_labels.rs` (`StepBarLabel`, `WorkerBarLabel`) and `src/mediapm/src/materializer/progress_labels.rs` (`MaterializationBarLabel`). Read the code when a field's classification is in question.

### Struct 1: `StepBarLabel` (conductor per-step bars, no production caller)

**File**: `src/mediapm-conductor/src/orchestration/progress_labels.rs`

Carries real-progress fields: version, completed/total, phase, workflow/step identity. A live `run_workflow` never draws one, so nothing on the workflow screen is pinned by these fields.

| Field | Meaning | Example |
|-------|---------|---------|
| `version` | Tool version | `"7.1"` |
| `completed` / `total` | Progress tally | `"2"` / `"5"` |
| `phase` | Workflow phase tag | `"wf"` |
| `status_marker` | Terminal state marker | `""` / `"F"` / `"W"` |
| `workflow_id` | Workflow name | `"default"` |
| `step_id` | Step identifier | `"s3"` |
| `tool` | Conductor tool name | `"ffmpeg"` |

### Struct 2: `WorkerBarLabel` (conductor worker-slot bars)

**File**: `src/mediapm-conductor/src/orchestration/progress_labels.rs`

Carries activity flag only, with no workflow phase and no progress tally. Prefix segments are ordered `status_marker`, `activity`, `workflow_id`, `step_id`, `tool` (`src/mediapm-conductor/src/orchestration/progress_labels.rs:171-189`), so the activity marker leads and the tool name is the last and only elastic segment. This is the label a live workflow installs: the coordinator creates one bar per pool member at `src/mediapm-conductor/src/orchestration/coordinator.rs:340-353` and re-labels each one through `set_truncation` on every dispatch and every step outcome.

| Field | Meaning | Example |
|-------|---------|---------|
| `status_marker` | Terminal state marker | `""` / `"F"` / `"W"` |
| `workflow_id` | Workflow name | `"default"` |
| `step_id` | Step identifier | `"s5"` |
| `tool` | Conductor tool name | `"echo"` |
| `activity` | Current activity | `"active"` / `"idle"` |

### Struct 3: `MaterializationBarLabel` (mediapm materialization bars)

**File**: `src/mediapm/src/materializer/progress_labels.rs`

Carries file-path identity and phase. No version, no count/total, no workflow/step.

| Field | Meaning | Example |
|-------|---------|---------|
| `status_marker` | Terminal state marker, which the materializer never writes | `""` |
| `entry_path` | Directory portion of hierarchy path | `"Music/Artist/Album"` |
| `entry_name` | Basename of hierarchy entry | `"song.mkv"` |
| `file_name` | Extracted file basename (sub-bars only) | `"cover.jpg"` |
| `phase` | Materialization phase tag | `"stg"` / `"vrf"` / `"cmt"` / `"wrt"` / `"mat"` |

**Suffix:** always empty. `truncate_suffix` returns `String::new()` unconditionally (`src/mediapm/src/materializer/progress_labels.rs:68-70`), and the client-truncation path replaces a bar's suffix with that return value rather than adding to it, so elapsed, rate, eta, and count never render on a materialization row.

### Why the structs are separate

They carry different fields, and the truncation order is a per-screen decision rather than a property of a label type. A worker bar wants its activity marker first and has no tally; a materialization bar wants its phase first and no identifiers at all. Merging them would force every screen's ranking through one shared struct, and the ranking is the part that has to be right.

### Built-in `PrefixComponents` truncation order

The built-in `semantic_truncate_prefix` path (used when no `BarLabelTruncation` is installed) does not fit through `fit_segments`: it shrinks some components progressively and drops others atomically. Its removal order is the normative spec on the function itself, in `src/mediapm-utils/src/progress/inner/components.rs`, which also records why `count` and `total` are stored separately but rendered and trimmed as one unit. Read it there rather than here.

The built-in path is used by Screen A (tool-sync) bars, which do not install `BarLabelTruncation`.

## Test-only vs production output

- **Raw `ProgressBar` tests** use the test-only `{elapsed_precise}` template, producing `[00:00:00]` format (bracketed, HH:MM:SS).
- **`ProgressScreen` tests** use the production renderer, producing `0s` / `42s` / `1m35s` format via `format_elapsed` (compact, no brackets).
- The runnable screen examples render through the production renderer, so they are neither of the two test-only shapes above.

## Per-screen specs

### Screen A: Tool-sync (`src/mediapm/src/conductor_bridge/sync/`)

Phases: `[res]` resolve, `[fch]` fetch, `[pro]` process, `[prn]` prune. Phases are shortened from longer names (`resolve` → `res`, `fetch` → `fch`, `process` → `pro`, `prune` → `prn`).

- **Bar label** is `{tool_id} {version} [{phase}]`, space-separated (`src/mediapm/src/conductor_bridge/sync/provision.rs:144` and `:213`). A tool reporting no human-readable version drops the version and the space, so the label is `media-tagger [fch]`.
- **Resolve bar** totals `metadata_fetch_count` (`provision.rs:212`), which is 2 for ffmpeg (BtbN autobuild tag plus evermeet version) and 1 for every other tool. It is a lookup count, never a byte total or a percentage.
- **Resolve bar** shows `N cached` status list via `SuffixComponents::status_list`.
- **Skip bar** shows `skipped, N cached` status list (when cached) vs `skipped` (when not cached).
- **Byte columns** render through `format_count` (`src/mediapm-utils/src/progress/inner/components.rs:205`), which is decimal: 31 457 280 bytes reads `31M`, not `29.9MiB`.
- **Prune bar** (`[prn]`) uses the `tools.len()` call before the `retain` block to count prune candidates and sets the total to that count. Zero-bar guard: `[prn]` bar is not created when there are no candidates. The bar advances once per document-rewrite removal, then once more for filesystem prune.
- Overall bar uses `apply_overall_bar_style` (magenta); child bars use `apply_bar_style` (yellow).

### Screen B: Workflow (`src/mediapm-conductor/src/orchestration/`)

Phase `[wf]` appears in the seeds rather than in a client label: the coordinator seeds every worker slot `idle [wf]` (`coordinator.rs:343`) and the overall bar `workflow [wf]`.

A live run registers worker-slot bars only. `StepBarLabel` has no production caller, so nothing on this screen shows a per-step version or tally. The bars that do exist use `WorkerBarLabel`, whose prefix order puts the activity marker ahead of the identifiers; combined with the seed ceiling above, an active row renders `[active]` on its own and a warning or failed row renders `[W]` or `[F]` with its identifiers already dropped.

Worker-slot states (`worker_slot_label` in `coordinator.rs:195`):

| State | `workflow_id` | `step_id` | `tool` | `activity` | `status_marker` |
|-------|---------------|-----------|--------|------------|------------------|
| Active | workflow name | step id | conductor tool name | `active` | (empty) |
| PendingRetry | (empty) | (empty) | `idle` | `idle` | `W` |
| Failed | (empty) | (empty) | `idle` | `idle` | `F` |
| Idle / Succeeded | (empty) | (empty) | `idle` | `idle` | (empty) |

Worker labels are mediapm-agnostic; `tool` is the conductor step's own `ToolSpec.name`, never a managed-tool name.

### Screen C: Materialization (`src/mediapm/src/materializer/`)

Uses `MaterializationBarLabel` for client-defined truncation. Paths are split into `entry_path` (directory) and `entry_name` (basename) via `split_entry_path`. Prefix segments are ordered `phase`, `status_marker`, `entry_name`, `file_name`, `entry_path`, so the phase tag leads, the basename is kept whole, and the elastic directory path is shortened from the front and yields first.

Phases: `[mat]` overall, `[stg]` staging, `[vrf]` verify, `[cmt]` commit, `[wrt]` write (per-extracted-file sub-bar inside a ZIP folder variant).

Three things on this screen do not behave the way the phase list suggests:

- **The `[stg]` → `[vrf]` → `[cmt]` transition runs only for `HierarchyEntryKind::Media`.** The per-entry bar is created with phase `stg` for every entry kind (`src/mediapm/src/materializer/mod.rs:396-403`), and only the `Media` arm calls `set_truncation` again, at `mod.rs:429-448`. A `MediaFolder` or `Playlist` entry shows `[stg]` for its whole run.
- **No row carries a status marker.** Nothing outside the label's own unit test writes `status_marker`, which that test does at `src/mediapm/src/materializer/progress_labels.rs:122`. `finish_warning` (`mod.rs:525`) and `finish_error` (`mod.rs:557` and `:573`) set a bar's terminal state and stop there, so a warning and a failure are indistinguishable on screen apart from the bar colour.
- **No row carries a suffix**, for the reason given under `MaterializationBarLabel` above.

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

**Screen A: `mediapm sync`** (via `print_sync_summary` in `output/mod.rs`):

```text
✓ sync complete    executed=3  cached=2  materialized=5
  Δ some warning message
```

- Icon: `Warning` when `workflow_failed_steps > 0`; `Success` when `executed > 0 || materialized > 0`; `NoChange` otherwise.
- Fields: `executed` always shown; `cached`, `materialized`, `skipped`, `removed`, `removed_empty`, `added_tools`, `updated_tools`, `pruned_tools`, `removed_tools`, `skipped_tools`, `failed` shown only when >0.
- Warnings: one `print_warning` line per warning.

**Screen A: `mediapm tool sync`** (via `CliSyncObserver::on_phase` in `output/observer.rs`, which calls `print_result` with the `ToolsSyncSummary` fields):

```text
✓ tools synced    added=2  updated=1  pruned=0  removed=0
  Δ warning message
```

- Fields: `added`, `updated`, `pruned`, `removed` (all always shown).
- Warnings: one `print_warning` line per warning.

**Screen B: workflow** (via `CliSyncObserver` in `output/observer.rs`):

```text
✓ workflow    executed=3  cached=2  failed=0
```

- Icon: `Warning` when `failed_steps > 0`; `NoChange` when nothing ran; `Success` otherwise.
- Fields: `executed`, `cached`, `failed`.
- The conductor CLI (`conductor run`) renders identically via `format_result_line`.

**Screen C: materialization** (via `CliSyncObserver` in `output/observer.rs`):

```text
✓ materialized    paths=5  skipped=3  removed=1
```

- Icon: `Success` when `materialized + removed > 0`; `NoChange` otherwise.
- Fields: `paths`, `skipped`, `removed`.

### Per-phase observer contract

The library (`MediaPmService::sync_library_with_tag_update_checks_and_observer`) calls `observer.on_phase(report)` immediately after each screen's `group.join()` returns, in order: `Tools` → `Workflow` → `Materialization`. The `CliSyncObserver` renders structured `print_result` lines. Tests use a recording observer to assert the exact phase sequence and values.

## Worked examples: run them, do not transcribe them

Do not paste a terminal frame into this file. Every screen has a runnable example that renders it through the shared capture harness in `src/mediapm/examples/support/`, and every run has a committed transcript under `src/mediapm/examples/fixtures/`. Run the example to see the frame; read the fixture to compare against it. The transcripts are generated output and must not be edited by hand, because a hand-edited fixture no longer proves that the renderer draws what the code says it draws. Children render above the overall bar: child bars first, overall bar last.

| Screen | Example | Fixtures |
| --- | --- | --- |
| A tool sync | `cargo run -p mediapm --example mediapm_progress_tool_sync` | `src/mediapm/examples/fixtures/mediapm_progress_tool_sync/` |
| B workflow | `cargo run -p mediapm --example mediapm_progress_workflow` | `src/mediapm/examples/fixtures/mediapm_progress_workflow/` |
| C materialization | `cargo run -p mediapm --example mediapm_progress_materialize` | `src/mediapm/examples/fixtures/mediapm_progress_materialize/` |

Every example takes `--width` and `--height`. The defaults are 80 and 24 (`src/mediapm/examples/support/mod.rs:41`). Each fixture directory holds a width-80 transcript and one narrow transcript: 60 columns for tool sync, 41 for workflow, 39 for materialization. Regenerate one by running the example at that width and redirecting stdout.

Each example also asserts its own grid inline, with `assert_eq!` against a `concat!` literal in `src/mediapm/examples/mediapm_progress_*.rs`, so a layout change fails the suite before anyone reaches for a transcript.

## Debug JSONL

`MEDIAPM_PROGRESS_DEBUG=1` env var enables JSONL tick output to stderr. Each line is a JSON object with fields: `type` (`"tick"`), `tick` (monotonic counter), `elapsed_secs` (f64), `bars` (array of per-slot state: `slot`, `bound`, `label`, `prefix`, `position`, `total`, `status`, `elapsed_secs`, `rate_bytes_per_sec`, `eta_secs`, `suffix`, `dirty`).

## Pre-roll, gap conversion, finalize

- **Pre-roll**: On first draw, writes blank lines to scroll existing terminal content into scrollback, then repositions cursor. Only fires when `pre_roll_term` is `Some` (not in test mode).
- **Gap conversion**: Child bars that are inactive or finished show a fully-dimmed empty bar (`total=1, pos=0`). Idle workers are explicitly finished (via `finish_success`), so they appear as dimmed empty bars. The `WorkerSpinner` style detects this state.
- **Finalize**: `group.join()` returns after all bars finish; the renderer removes blank reserved slots and triggers a final draw so only finished bars persist in scrollback.

## Authoritative tests

These test files use `assert_eq!(term.contents(), concat!(...))` against an inline literal and ARE the real format:

- `src/mediapm-utils/tests/progress_output/` — `consumer.rs`, `elapsed.rs`, `layout.rs`, `lifecycle.rs`, `render.rs`, `resize.rs`, `spinner.rs`; `common.rs` and `debug.rs` are helpers and the debug sink
- `src/mediapm/examples/mediapm_progress_tool_sync.rs`, `mediapm_progress_workflow.rs`, `mediapm_progress_materialize.rs` — one exact grid per screen, at the default width and at the narrow one

`src/mediapm/src/output/progress.rs` also asserts against `term.contents()`, but with substring checks rather than exact literals, so it pins behaviour such as "elapsed reads `0s` after finish" rather than a full frame.

## Global toggle and auto-detection

Progress is suppressed by passing `no_progress: true` or by constructing a `ProgressScreen::disabled()`. Progress bars are also automatically hidden when stderr is not a TTY (indicatif self-detects via `console::Term::stderr()`). The `--quiet` / `MEDIAPM_QUIET` flags suppress hints and progress.

## Spinner animation

Every progress bar uses a daemon ticker at 50 ms intervals, keeping the spinner animating even during long periods without position updates. The spinner uses braille dots: `⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏`. For deterministic tests, disable the ticker with `.with_ticker_enabled(false)`.

## Library stack

The styling stack uses `indicatif` 0.17 (`ProgressBar`, `MultiProgress`, `ProgressStyle`, `HumanBytes`, `HumanCount`) and `console` 0.15 (`Term::stderr().size()` for terminal width detection, `style()` for ANSI coloring). Do not add `owo-colors`, `colored`, `termion`, or other styling crates; `console::style()` is the single styling entry point.

## Duration formatting

The `format_duration(Duration) -> String` function formats durations as follows: values under 1 second show two decimal places (e.g., `0.01s`, `0.05s`); values from 1 to 9 seconds show two decimal places (e.g., `1.00s`, `9.00s`); values from 10 to 59 seconds show whole seconds without decimals (e.g., `10s`, `42s`); values from 1 to 59 minutes show minutes and seconds (e.g., `1m 0s`, `30m 42s`); values of 1 hour or more show hours, minutes, and seconds (e.g., `1h 0m 0s`, `2h 15m 30s`).

## Dependency boundary rule

The conductor library (`mediapm-conductor`) must not depend on indicatif directly. It receives progress updates via `Fn` callbacks typed as `ProgressCallback`. Only the conductor CLI binary and the `mediapm` crate may use indicatif, accessed through `mediapm-utils/progress` with the `progress` feature enabled.

## Common usage pattern in handlers

Every CLI command handler follows a consistent shape: perform the operation, print the result line via `print_result` with the appropriate `StatusIcon`, then print any warnings or hints on stderr. The sync command passes a `CliSyncObserver` that prints per-phase result lines after each progress screen finishes, and `print_sync_summary` at the end for the combined summary.

## Related files

- `src/mediapm/src/output/mod.rs` — re-exports the post-finish result primitives from `mediapm_utils::report` (`:15`) and defines `print_sync_summary` (Screen A sync summary, `:29`)
- `src/mediapm-conductor/src/api.rs` — `RunSummary` struct (Screen B)
- `src/mediapm/src/lib.rs` — `SyncSummary`, `ToolsSyncSummary` struct definitions

## Module reference

| Module | Crate | Feature | Purpose |
|---|---|---|---|
| `mediapm_utils::report` | `mediapm-utils` | `report` | `StatusIcon`, `print_result`, `format_result_line`, `print_warning`, `print_hint`, `print_error`, `print_heading`, `print_status_report`, `format_duration` |
| `mediapm_utils::progress` | `mediapm-utils` | `progress` | `ProgressScreen`, `ProgressBarHandle`, `ProgressTerminal`, `ProgressScreenApi`, `ProgressBarApi`, `BarLabelTruncation`, `fit_segments`, `Segment`, `Shrink`, `front_ellipsis` |
| `mediapm_utils::progress` (always) | `mediapm-utils` | — | `DownloadProgressSnapshot`, `ProgressCallback` |
| `mediapm::output::progress` | `mediapm` | — | `ProgressScreen`, `ProgressBarHandle`, `ProgressBarApi`, `ProgressScreenApi`, `ProgressTerminal` re-exports |
| `mediapm::output::report` | `mediapm` | — | Re-exports from `mediapm_utils::report` |
