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
FRAME_OVERHEAD_COLUMNS = 4 (spinner glyph plus the three spaces between the four template fields)
MIN_BAR_FILL = 4           (floor held back under the fill, and the width at which a frame stops drawing one)
```

`max_prefix_width(cols)` (`src/mediapm-utils/src/progress/inner/components.rs:595`) and `max_suffix_width(cols)` (`:601`) are `const fn` capping those two ceilings at the terminal width, so neither reserves more than the line holds. `recompute_layout` (`renderer.rs:1131`) then spends what is left of `cols`, in two steps:

```text
reserved = cols - (FRAME_OVERHEAD_COLUMNS + MIN_BAR_FILL)
suffix_w = min(measured suffix, suffix_ceiling, reserved)
prefix_w = min(measured prefix, prefix_ceiling, reserved - suffix_w)
fill     = cols - (FRAME_OVERHEAD_COLUMNS + prefix_w + suffix_w)
```

`draw_fill` is `fill > MIN_BAR_FILL`. A frame that answers no draws itself from the same four fields with `{wide_bar}` and the space in front of it left out, and re-settles the two label slots over the columns the floor was holding, with the suffix measured again without its timing:

```text
reserved   = cols - FRAME_OVERHEAD_COLUMNS
suffix_w   = min(measured suffix without timing, suffix_ceiling, reserved)
prefix_w   = min(measured prefix, prefix_ceiling, reserved - suffix_w)
```

The suffix is settled first because it is the field that must not wrap: a suffix past the end of the line spills onto the row below, where it reads as a second bar. `cols` arrives from [`DimensionSource::dimensions`](`src/mediapm-utils/src/progress/inner/debug.rs:12`), which is `RealTerminalSource` on a terminal and `TestDimensionSource` under a test.

On a frame with no bar, the timing is the first thing the suffix gives up, because the columns it wants are the ones the label and the tally want. A row that would have nothing left keeps it: a worker slot has no tally of its own, so a stripped worker row would read `⠙` and nothing else, and a lone spinner is a worse frame than the four cells of bar that were dropped. `keeps_timing` (`renderer.rs:112`) is that condition, `compose_suffix` (`renderer.rs:64`) applies it, and `without_timing` (`renderer.rs:89`) is the one strip both the draw path and the measurement pass go through, so the two cannot answer differently about the same row.

## Two layout facts to know before changing a label

**The slot is measured from what a bar draws, not from the seed it was built with.** `SharedState::with_time_source_and_style` turns the seed into built-in prefix components at `src/mediapm-utils/src/progress/inner/renderer.rs:238`, and `snap.prefix` is their render, which is the right width to measure for a bar that draws them. A bar with a `BarLabelTruncation` installed draws something else, so `recompute_layout` asks the client for that instead: it calls `truncate_prefix` at the ceiling and measures the result (`renderer.rs:936`). `snap.prefix` is the wrong width for such a bar, because the seeds are short by design and `idle` is what the conductor installs on every worker slot (`src/mediapm-conductor/src/orchestration/coordinator.rs:447`). A client label may be longer or shorter than the string its bar was created with, so the measurement has to follow the label.

**The budget is the terminal's, and the ceiling only trims it.** The slot is what is left after the spinner, the separators and the suffix, capped at `MAX_PREFIX_WIDTH`, and the fill takes what neither label wants, down to a floor of `MIN_BAR_FILL` columns. Narrowing the terminal shrinks the fill first and takes columns from the label one ranked field at a time, so what changes across widths is which field survives, not how much of the line the labels occupy.

## No row wraps, and a frame below the crossing has no bar

Every width the progress examples accept draws one line per row on all three screens, which the three `*_screen_never_wraps_a_row` sweeps pin.

A frame drops its fill when the fill the budget affords is at or below `MIN_BAR_FILL`. Four cells of `░░░░` or `████` carry a quarter-resolution fraction that the count beside them already states, and they are paid for out of a label being clipped down to its tail. So at that point the frame stops drawing a bar and gives the columns to the label and the count instead, which is what `{spinner} {prefix} {msg}` is for.

Above a screen's own crossing nothing changes: the fill is back, the four template fields are back, and a frame looks the way it always did. The crossings differ per screen because each screen's labels differ, and the width at which the labels stop overflowing the line is a property of those labels. Measured over the three example screens at every width from 8 to 120, the fill is at the floor at every width up to 58 on tool sync and materialization and up to 65 on the workflow screen, whose labels are seven columns wider. Read `recompute_layout` for the question it asks; there is no width constant to look up.

A row still says something at the bottom of the range, and that is the property the `*_screen_rows_never_go_empty` sweeps pin. Two things carry a row once the bar is gone: the label, and the count and timing in the suffix. A row with neither renders its timing rather than nothing, as `keeps_timing` decides above. What is left is the bottom of the range on a screen whose suffixes are timing-only, where the suffix slot measures nothing and there are no columns for the timing either. That is measured per screen in the doc comment of `assert_narrow_rows_carry_a_label_or_a_count` in `src/mediapm/examples/support/mod.rs`.

## Production templates

Templates are **dynamic `format!` strings** built inside `apply_*_bar_style` functions using live `prefix_w`/`suffix_w` values and the frame's `draw_fill`. There are NO static `CHILD_BAR_TEMPLATE`/`OVERALL_BAR_TEMPLATE` constants.

| Function | Template pattern | Wide bar style |
|---|---|---|
| `apply_overall_bar_style` | `{spinner:.green} {prefix:>{pw}.{pw}} {wide_bar:0.magenta/dim} {msg:<{sw}.{sw}}` | magenta/dim |
| `apply_bar_style` | `{spinner:.green} {prefix:>{pw}.{pw}} {wide_bar:0.yellow/dim} {msg:<{sw}.{sw}}` | yellow/dim |
| `apply_done_bar_style` | `{spinner:.white/.dim} {prefix:>{pw}.{pw}} {wide_bar:0.green/dim} {msg:<{sw}.{sw}}` | green/dim |
| `apply_failed_bar_style` | `{spinner:.red} {prefix:>{pw}.{pw}} {wide_bar:0.red/dim} {msg:<{sw}.{sw}}` | red/dim |

Where `{pw}` = `prefix_w` and `{sw}` = `suffix_w`, both read from the cells `recompute_layout` writes. There is no static template to fall back to.

`draw_fill == false` builds the same four fields with `{wide_bar}` and the space in front of it left out: `{spinner:.green} {prefix:>{pw}.{pw}} {msg:<{sw}.{sw}}`, so a frame with no bar is the same layout minus the third field. The space in front of `{msg}` stays, which is why `render_suffix_components` gives the count/total segment its own leading space. `frame_template` in `components.rs` holds both shapes and takes each caller's colours as arguments, so a style is one call rather than two.

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

## Truncation dispatch

`sync_snapshot_to_bar` (in `renderer.rs`) is the single push point from `SharedState` → indicatif. It determines whether client-defined truncation (`BarLabelTruncation` via `set_truncation`) or built-in truncation (`PrefixComponents` via `set_prefix_components`) applies:

- **Client-truncated bars**: `ansi_overhead = 4`. Calls `truncation.truncate_prefix(prefix_w - 4)` then prepends `\x1b[0m` to the result. Suffix path: calls `truncation.truncate_suffix(suffix_w, &fresh_suffix)` where `fresh_suffix` is the merged `SuffixComponents` (auto-derived + user-set fields), with the timing stripped on a frame that draws no bar unless the row would otherwise have nothing to say.
- **Built-in bars**: `ansi_overhead` is 13 for `Failed`/`Warning`, 4 otherwise. Calls `semantic_truncate_prefix(&components, prefix_w - ansi_overhead)` then `render_prefix_components` to wrap the prefix in colored markers.

`recompute_layout` checks for installed client truncation to apply the same overhead rule for width budgeting. It measures the suffix through the same `render_slot_suffix` the draw path renders through, so a client label is asked for its own output in both passes.

## Buffer ordering and style dedup

The tick loop and attach operation are designed to minimize visible flicker:

- **Buffer-first tick**: `tick()` enables the `WriteGate` (suppresses writes) BEFORE calling `recompute_layout()`. This ensures all `set_style` calls from `recompute_layout` → `sync_slot` → `apply_*_bar_style` are suppressed until `WriteGate::open()` releases them atomically.
- **Buffered attach**: `attach()` wraps its entire body in a `WriteGate::suppress()`/`open()` pair so slot shifts, sync_slot, and recompute_layout during bar attachment are buffered and appear atomically.
- **Style dedup**: `sync_slot` caches `(prefix_w, suffix_w, is_overall, draw_fill, status_code)` in `SlotCache` and only calls `apply_*_bar_style` when any component of this tuple changes. This eliminates redundant style writes when bar dimensions and status are stable across ticks.

## Three bar-label structs

All three structs implement [`BarLabelTruncation`] (defined in `src/mediapm-utils/src/progress/truncation.rs`) and are rendered via the client-truncation path in `sync_snapshot_to_bar`. The renderer prepends `\x1b[0m` (4-byte ANSI reset) to all client-truncated prefixes; no colored markers are embedded in the truncated string.

All three are installed by production code: the conductor coordinator puts `WorkerBarLabel` on each worker slot and `StepBarLabel` on the pinned overall row, and the mediapm materializer installs `MaterializationBarLabel`.

Fitting uses `fit_segments` (shared from `mediapm_utils::progress`). Segments are supplied most important first and the list is walked from the tail, so the **last** segment is the first to yield. Under width pressure, elastic segments are shortened from the front, keeping the tail of the text, and once no segment can shrink further whole segments drop from the tail until the remainder fits. The clip is blind, so a tail may begin mid-word and a truncated path reads `he Wall`. Snapping to the next word or slash used to prevent that and cost monotonicity instead: two widths inside one word could render the same length, or go backwards. The clip came back, and `rendered_length_is_monotone_in_width` pins it.

Brackets are decoration on the segment (`brackets: Option<Brackets>`) rather than part of its text, and they are applied after fitting, to a segment that came through whole and to no other. A clipped segment gives them up, which is why a shortened row never shows a `)` whose `(` was cut away. Nothing marks a cut, so a shortened segment is indistinguishable from one that was always that short.

**Segment order alone determines the outcome.** `fit_segments` drops from the tail unconditionally, so a field survives by its position in the list. When designing a new label, rank the fields and mark the shrink behaviour (`Shrink::Keep` for whole, `Shrink::Front` where the tail is the informative end, `Shrink::Head` for a version, which numbers itself from the left).

The per-screen orderings summarised under each struct below are a convenience, not the spec. The normative ranking, and the reasoning behind each segment's classification, live in the `prefix_segments` and `suffix_segments` bodies of `src/mediapm-conductor/src/orchestration/progress_labels.rs` (`StepBarLabel`, `WorkerBarLabel`) and `src/mediapm/src/materializer/progress_labels.rs` (`MaterializationBarLabel`). Read the code when a field's classification is in question.

### Struct 1: `StepBarLabel` (conductor overall row)

**File**: `src/mediapm-conductor/src/orchestration/progress_labels.rs`

Carries workflow, step and tool identity, plus a version and a tally. The coordinator builds it for the pinned overall row and fills in neither the version nor the tally: `UnifiedToolSpec` holds a builtin id such as `echo@v1` and no version, and the overall bar counts steps through its fill rather than through a tally. The workflow example is the only caller that fills a version, so it is the only screen where `Shrink::Head` has anything to cut.

| Field | Meaning | Example |
|-------|---------|---------|
| `version` | Tool version, head-keeping. Empty in production, so no segment is rendered. | `"7.1"` |
| `completed` / `total` | Progress tally, rendered in the suffix only when `completed` is set | `"2"` / `"5"` |
| `status_marker` | Terminal state marker | `""` / `"F"` / `"W"` |
| `workflow_id` | Workflow name | `"default"` |
| `step_id` | Step identifier | `"s3"` |
| `tool` | Conductor tool name | `"ffmpeg"` |

### Struct 2: `WorkerBarLabel` (conductor worker-slot bars)

**File**: `src/mediapm-conductor/src/orchestration/progress_labels.rs`

Carries activity flag only, with no workflow phase and no progress tally. Prefix segments are ordered `status_marker`, `workflow_id`, `step_id`, `tool`, `activity`, so the activity marker trails and the tool name is the only elastic segment. This is the label the coordinator creates a bar for, one per pool member (`src/mediapm-conductor/src/orchestration/coordinator.rs:447`), and re-labels through `set_truncation` on every dispatch and every step outcome.

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
| `status_marker` | Terminal state marker | `""` / `"W"` / `"F"` |
| `entry_path` | Directory portion of hierarchy path | `"Music/Artist/Album"` |
| `entry_name` | Basename of hierarchy entry | `"song.mkv"` |
| `file_name` | Extracted file basename (sub-bars only) | `"cover.jpg"` |
| `phase` | Phase this row is on, as `Option<MaterializationPhase>`; absent renders no tag | `Some(Staging)` |

**Suffix:** the auto-derived timing, in the same order `WorkerBarLabel` uses (`src/mediapm/src/materializer/progress_labels.rs:109`). `truncate_suffix` fits `elapsed`, `rate` and `eta` through `fit_segments` and has to read them out of the merged `SuffixComponents` to do it: the client path replaces a bar's suffix with what this method returns rather than adding to it, so a label that returns an empty string takes elapsed, rate and eta off the screen rather than narrowing them.

### Why the structs are separate

They carry different fields, and the truncation order is a per-screen decision rather than a property of a label type. A worker bar wants its activity marker first and has no tally; a materialization bar wants its phase first and a path, where the path shortens from the head because its tail is the informative end. Merging them would force every screen's ranking through one shared struct, and the ranking is the part that has to be right.

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

No row on this screen carries a phase tag. The seeds are `workflow` for the overall bar and `idle` for every worker slot (`coordinator.rs:447`), and both are replaced by a client label as soon as the bar has one.

The overall row uses `StepBarLabel`, re-installed at each dispatch and at each step outcome, so it names what is running: `default s3 (ffmpeg)`. The conductor carries no tool version, so the `version` field is empty there and its segment is dropped. Worker slots use `WorkerBarLabel`, which ranks `status_marker`, `workflow_id`, `step_id`, `tool`, `activity` and leaves `tool` the only elastic segment, so a running slot reads `default s5 (echo) [active]` wherever the terminal has room for it. `fit_segments` walks from the tail, so a narrow row gives up the `[active]` tag and then the tool name and keeps the identifiers. The seed does not cap the slot: the label is measured from what the row draws, so a row keeps its names at widths where the four-column seed would have clipped them.

Worker-slot states (`worker_slot_label` in `coordinator.rs:200`):

| State | `workflow_id` | `step_id` | `tool` | `activity` | `status_marker` |
|-------|---------------|-----------|--------|------------|------------------|
| Active | workflow name | step id | conductor tool name | `active` | (empty) |
| PendingRetry | (empty) | (empty) | (empty) | `idle` | `W` |
| Failed | (empty) | (empty) | (empty) | `idle` | `F` |
| Idle / Succeeded | (empty) | (empty) | (empty) | `idle` | (empty) |

`tool` is empty in every state but `Active`, because a slot with no step on it has no tool to name. The row stops at its activity marker: `[idle]`, or `[W] [idle]` and `[F] [idle]` for a retry and a final failure. Earlier revisions filled `tool` with the string `idle`, which rendered as `(idle)` beside the `[idle]` that `activity` had already said.

Worker labels are mediapm-agnostic; `tool` is the conductor step's own `ToolSpec.name`, never a managed-tool name.

### Screen C: Materialization (`src/mediapm/src/materializer/`)

Uses `MaterializationBarLabel` for client-defined truncation. Paths are split into `entry_path` (directory) and `entry_name` (basename) via `split_entry_path`. Prefix segments are ordered `status_marker`, `entry_name`, `entry_path`, `file_name`, `phase`, so the phase tag trails the way it does on tool-sync and is the first whole segment a narrow row gives up, both halves of the path are elastic and shorten from the head, and columns come back in the order `entry_path` shrinks, then `entry_name` shrinks, then whole segments drop from the tail: `phase`, `file_name`, and the path halves after them. `entry_name` is elastic rather than kept whole because a name kept whole is a name that vanishes once nothing else can shrink, and its tail still carries the extension and the bracketed media id that separates one entry from another.

The phase is a `MaterializationPhase` enum, not a string. `HierarchyEntryKind::phases()` declares what each kind walks, and the match on the enum is exhaustive, so an arm that forgot to declare a phase it runs is a compile error: `Media` walks `[stg]`, `[vrf]` and `[cmt]`, while `MediaFolder` and `Playlist` declare `[stg]` and nothing more, which is all their arms do. A folder row therefore reads `[stg]` for its whole life, and its tag says nothing about how far it got. See "Materialization phase tags" below for what each phase does.

A warning or a failure carries its marker in the text as well as in the bar colour. `EntryPhaseBar::finish` (`src/mediapm/src/materializer/mod.rs`) re-installs the row's current phase with `status_marker` set to `W` or `F` immediately before `finish_warning` or `finish_error`, so `[W]` and `[F]` are readable on the row itself and a row that stopped part-way through a multi-phase entry reports where. A row also carries the timing the renderer derives for it, for the reason given under `MaterializationBarLabel` above, except on a frame that has dropped its bar where the timing gives way to the label unless the row would then have nothing at all.

### Materialization phase tags

The tags cost three columns each in the prefix slot, which is why the full word is not spelled out there. They render bracketed, so `[cmt]` on a row is the commit phase.

| Tag | Phase | What the row is doing |
|-----|-------|------------------------|
| `stg` | staging | copying CAS content into the staging area |
| `vrf` | verify | checking staged bytes before committing |
| `cmt` | commit | writing into the library |
| `wrt` | write | one file inside a folder variant |

`stg`, `vrf` and `cmt` are the three phases a media entry walks, and `wrt` labels a sub-bar for a single extracted file inside a folder variant. No overall bar on any screen carries a tag: the three seeds are `workflow`, `idle` and `materializing`.

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

Do not paste a terminal frame into this file. Screen A is `mediapm_progress_tool_sync`, screen B is `mediapm_progress_workflow`, and screen C is `mediapm_progress_materialize`. Run one with `--width N` to see a frame at that width, or read the transcripts under `src/mediapm/examples/fixtures/`. The transcripts are generated output and must not be edited by hand, because a hand-edited fixture no longer proves that the renderer draws what the code says it draws. Children render above the overall bar: child bars first, overall bar last.

Every example takes `--scenario`, `--width` and `--height`. A transcript is named `<stem>-<scenario>-width-<N>.txt`, and the three scenarios are `baseline`, `dense` and `states`. A name with no scenario segment is the width-only shape these files carried before the axis existed and reads as `baseline`; a segment naming no scenario is an error, because a typo there would quietly drop a scenario's coverage. `--height` is only for drawing a screen somewhere other than the height its own bar count asks for.

Height follows from the scenario: `Scenario::height()` is `bars + 1`. The renderer draws one line per bar, and the newline that commits the frame scrolls the top row away once the frame fills the terminal, so the frame is given a row it does not draw. That one row is exactly enough at every band size from 4 to 255 bars.

Regenerate a transcript by running the example with its scenario and width and redirecting stdout. The directory listing is the set of scenarios and widths, since each file is named for both and each screen's test reads both back out of those names. A file in one of those directories that is not a transcript is a test failure, not something to leave lying around.

Each example's test walks its own fixture directory and compares what the screen renders against each transcript, so a layout change fails the suite before anyone reaches for a transcript by hand.

## Debug JSONL

`MEDIAPM_PROGRESS_DEBUG=1` env var enables JSONL tick output to stderr. Each line is a JSON object with fields: `type` (`"tick"`), `tick` (monotonic counter), `elapsed_secs` (f64), `bars` (array of per-slot state: `slot`, `bound`, `label`, `prefix`, `position`, `total`, `status`, `elapsed_secs`, `rate_bytes_per_sec`, `eta_secs`, `suffix`, `dirty`).

## Pre-roll, gap conversion, finalize

- **Pre-roll**: On first draw, writes blank lines to scroll existing terminal content into scrollback, then repositions cursor. Only fires when `pre_roll_term` is `Some` (not in test mode).
- **Gap conversion**: Child bars that are inactive or finished show a fully-dimmed empty bar (`total=1, pos=0`) where the frame has room for a fill. Idle workers are explicitly finished (via `finish_success`), so that is the bar they show. `WorkerSpinner` detects the state. Below the fill crossing the frame draws no bar, so an idle worker reads `[idle]` and its timing on its own.
- **Finalize**: `group.join()` returns after all bars finish; the renderer removes blank reserved slots and triggers a final draw so only finished bars persist in scrollback.

## Authoritative tests

The `mediapm-utils` suite uses `assert_eq!(term.contents(), concat!(...))` against an inline literal and IS the real format:

- `src/mediapm-utils/tests/progress_output/` — `consumer.rs`, `elapsed.rs`, `layout.rs`, `lifecycle.rs`, `render.rs`, `resize.rs`, `spinner.rs`; `common.rs` and `debug.rs` are helpers and the debug sink
- `src/mediapm/examples/mediapm_progress_tool_sync.rs`, `mediapm_progress_workflow.rs`, `mediapm_progress_materialize.rs` — one test that walks the screen's transcript directory and compares every frame it finds there, plus a `*_screen_never_wraps_a_row` sweep and a `*_screen_rows_never_go_empty` sweep over the widths the harness accepts

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
| `mediapm_utils::progress` | `mediapm-utils` | `progress` | `ProgressScreen`, `ProgressBarHandle`, `ProgressTerminal`, `ProgressScreenApi`, `ProgressBarApi`, `BarLabelTruncation`, `fit_segments`, `Segment`, `Shrink`, `Brackets`, `front_tail` |
| `mediapm_utils::progress` (always) | `mediapm-utils` | — | `DownloadProgressSnapshot`, `ProgressCallback` |
| `mediapm::output::progress` | `mediapm` | — | `ProgressScreen`, `ProgressBarHandle`, `ProgressBarApi`, `ProgressScreenApi`, `ProgressTerminal` re-exports |
| `mediapm::output::report` | `mediapm` | — | Re-exports from `mediapm_utils::report` |
