---
description: "Use when editing progress-bar rendering code in mediapm-utils or any consumer (conductor workflow screen, mediapm tool-sync, materialization). Records the ACTUAL rendered output format so agents never guess or invent ASCII mocks."
name: "Progress Bar Rendered Output Format"
applyTo: "src/mediapm-utils/src/progress/mod.rs, src/mediapm-utils/src/progress/traits.rs, src/mediapm-utils/src/progress/recording.rs, src/mediapm-utils/src/progress/inner/**/*.rs, src/mediapm-utils/src/progress/truncation.rs, src/mediapm-conductor/src/orchestration/progress_labels.rs, src/mediapm-conductor/src/orchestration/coordinator.rs, src/mediapm/src/materializer/progress_labels.rs, src/mediapm/src/materializer/mod.rs, src/mediapm/src/output/progress.rs, src/mediapm/src/conductor_bridge/sync/**/*.rs"
---

# Progress bar rendered output format

This file records the rules the renderer follows: what goes in a prefix, how a label is ranked for truncation, and which screen installs which label. For what a frame actually looks like, the authority is the three runnable examples listed under "Worked examples" and the transcripts they generate. Any agent editing progress code MUST read this before changing row rendering, glyphs, colors, prefix/suffix shapes, or ordering, and MUST NOT hand-write a terminal frame into documentation.

## Source of truth

- `src/mediapm-utils/src/progress/inner/plan.rs`: the frame as a value — `FramePlan`, `PlanRow`, `RenderStyle`, `RowAnimation`, and every row render (`render`, `render_row`, `render_spinner`, `render_fill`)
- `src/mediapm-utils/src/progress/inner/frame.rs`: the frame sequence — `tick()` → `run_frame()`: resize absorb, `build_plan`, debug snapshot, one `write_frame`
- `src/mediapm-utils/src/progress/inner/writer.rs`: `write_frame(term, previous, next, force)` — the only terminal writer (per-row diff repaint)
- `src/mediapm-utils/src/progress/inner/gate.rs`: `WriteGate` — the term handle plus `commit_frame` (the CR+LF that advances past a committed frame); no write suppression exists
- `src/mediapm-utils/src/progress/inner/renderer.rs`: `build_plan`, `sync_slot` (cache-only), `retained_row`, `blank_row`, `finalize`, `SlotCache`/`StyleKey`, resize handling
- `src/mediapm-utils/src/progress/inner/layout.rs`: `settle_layout` (the width budget), `slot_row_strings`, `sync_snapshot_to_slot`, `keeps_timing`/`without_timing`
- `src/mediapm-utils/src/progress/inner/components.rs`: truncation (`semantic_truncate_prefix`, `semantic_truncate_suffix`, `render_prefix_components`, `render_suffix_components`), colors (`bar_color_code`), format helpers, `StatusCount` (`:19`), `format_status_list` (`:31`), `PARKED_GLYPH` (`:633`)
- Verified by `src/mediapm-utils/tests/progress_output/*.rs` using exact `assert_eq!(term.contents(), concat!(...))`, and by the three runnable screen examples listed under "Worked examples"

## Width constants

```text
MIN_PREFIX_WIDTH = 0       (decreasable floor, effective floor ~4 from the ANSI reset the client-truncation path prepends)
MAX_PREFIX_WIDTH = 40      (hard ceiling, prefixes truncate beyond this)
MIN_SUFFIX_WIDTH = 0       (decreasable floor, no ANSI overhead on suffix side)
MAX_SUFFIX_WIDTH = 65
FRAME_OVERHEAD_COLUMNS = 4 (spinner glyph plus the three spaces between the four template fields)
MIN_BAR_FILL = 4           (floor held back under the fill, and the width at which a frame stops drawing one)
```

`max_prefix_width(cols)` (`src/mediapm-utils/src/progress/inner/components.rs::max_prefix_width`) and `max_suffix_width(cols)` (`components.rs::max_suffix_width`) are `const fn` capping those two ceilings at the terminal width, so neither reserves more than the line holds. `settle_layout` (`layout.rs::settle_layout`) then spends what is left of `cols`, in two steps:

```text
reserved = cols - (FRAME_OVERHEAD_COLUMNS + MIN_BAR_FILL)
suffix_w = min(measured suffix, suffix_ceiling, reserved)
prefix_w = min(measured prefix, prefix_ceiling, reserved - suffix_w)
fill     = cols - (FRAME_OVERHEAD_COLUMNS + prefix_w + suffix_w)
```

`draw_fill` is `fill > MIN_BAR_FILL`. A frame that draws no fill renders the same four fields minus the fill column and the space in front of it, and re-settles the two label slots over the columns the floor was holding, with the suffix measured again without its timing:

```text
reserved   = cols - FRAME_OVERHEAD_COLUMNS
suffix_w   = min(measured suffix without timing, suffix_ceiling, reserved)
prefix_w   = min(measured prefix, prefix_ceiling, reserved - suffix_w)
```

The suffix is settled first because it is the field that must not wrap: a suffix past the end of the line spills onto the row below, where it reads as a second bar. `cols` arrives from [`DimensionSource::dimensions`](`src/mediapm-utils/src/progress/inner/debug.rs::DimensionSource::dimensions`), which is `RealTerminalSource` on a terminal and `TestDimensionSource` under a test.

On a frame with no bar, the timing is the first thing the suffix gives up, because the columns it wants are the ones the label and the tally want. A row that would have nothing left keeps it: a worker slot has no tally of its own, so a stripped worker row would read `⠙` and nothing else, and a lone spinner is a worse frame than the four cells of bar that were dropped. `keeps_timing` (`layout.rs::keeps_timing`) is that condition, `compose_suffix` (`layout.rs::compose_suffix`) applies it, and `without_timing` (`layout.rs::without_timing`) is the one strip both the draw path and the measurement pass go through, so the two cannot answer differently about the same row.

## Two layout facts to know before changing a label

**The slot is measured from what a row draws, not from the seed it was built with.** `SharedState::with_time_source_and_style` (`src/mediapm-utils/src/progress/inner/tracked.rs::with_time_source_and_style`) turns the seed into built-in prefix components, and `snap.prefix` is their render, which is the right width to measure for a row that draws them. A row with a `BarLabelTruncation` installed draws something else, so `settle_layout` asks the client for that instead: it calls `truncate_prefix` at the ceiling and measures the result (`layout.rs::settle_layout`). `snap.prefix` is the wrong width for such a row, because the seeds are short by design and `idle` is what the conductor installs on every worker slot (`src/mediapm-conductor/src/orchestration/coordinator.rs::run_workflow`). A client label may be longer or shorter than the string its row was created with, so the measurement has to follow the label.

**The budget is the terminal's, and the ceiling only trims it.** The slot is what is left after the spinner, the separators and the suffix, capped at `MAX_PREFIX_WIDTH`, and the fill takes what neither label wants, down to a floor of `MIN_BAR_FILL` columns. Narrowing the terminal shrinks the fill first and takes columns from the label one ranked field at a time, so what changes across widths is which field survives, not how much of the line the labels occupy.

## No row wraps, and a frame below the crossing has no bar

Every width the progress examples accept draws one line per row on all three screens, which the three `*_screen_never_wraps_a_row` sweeps pin.

A frame drops its fill when the fill the budget affords is at or below `MIN_BAR_FILL`. Four cells of `░░░░` or `████` carry a quarter-resolution fraction that the count beside them already states, and they are paid for out of a label being clipped down to its tail. So at that point the frame stops drawing a bar and gives the columns to the label and the count instead, which is what `{spinner} {prefix} {msg}` is for.

Above a screen's own crossing nothing changes: the fill is back, the four row fields are back, and a frame looks the way it always did. The crossings differ per screen because each screen's labels differ, and the width at which the labels stop overflowing the line is a property of those labels. Measured over the three example screens at every width from 8 to 120, the fill is at the floor at every width up to 58 on tool sync and materialization and up to 65 on the workflow screen, whose labels are seven columns wider. Read `settle_layout` for the question it asks; there is no width constant to look up.

A row still says something at the bottom of the range, and that is the property the `*_screen_rows_never_go_empty` sweeps pin. Two things carry a row once the bar is gone: the label, and the count and timing in the suffix. A row with neither renders its timing rather than nothing, as `keeps_timing` decides above. What is left is the bottom of the range on a screen whose suffixes are timing-only, where the suffix slot measures nothing and there are no columns for the timing either. That is measured per screen in the doc comment of `assert_narrow_rows_carry_a_label_or_a_count` in `src/mediapm/examples/support/mod.rs`.

## Row rendering

A frame is an `FramePlan` (one per tick) that `plan.rs` renders to strings. Each `PlanRow` carries the four fields the old templates held — spinner, prefix, fill, message — plus its `RenderStyle` and `RowAnimation`; there are no `format!` templates and no style-builder functions anywhere.

| RenderStyle | Spinner | Fill run color | RowAnimation |
|---|---|---|---|
| `Overall` | green | magenta/dim | `Animate` only when the source is Active and opted in, else `Static("idle")`/`Static("terminal")` |
| `Active` | green | yellow/dim | `Animate` only when the source opted in via `set_animating(true)`, else `Static("idle")` |
| `Done` | white/dim | green/dim | `Static("terminal")` — the fill draws a full run regardless of position |
| `Failed` | red | red/dim | `Static("terminal")` |
| `Blank` | (no glyph) | (no fill) | `Static` — `render_row` returns full-width spaces before any fill is built |

`draw_fill == false` drops the fill field and the space in front of it: the row is spinner, prefix, message. The space in front of the message stays, which is why `render_suffix_components` gives the count/total segment its own leading space.

All styles: `TICK_CHARS = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"`, `progress chars "█░"`. The fill matrix is in `render_fill`: a `Done` row and a `total == 0` row draw a full `█` run; every other row splits `fill_w` by `position / total`.

## Coloring reference

All color is applied via ANSI SGR escapes (`\x1b[{code}m`). The `console` crate provides the escape generation; `bar_color_code` returns the raw numeric code as a `&str` for `format!` interpolation.

### Bar fill and spinner colors

`RenderStyle` selects the colors at render time in `plan.rs`; `bar_color_code` supplies the numeric code per status. A parked row's `·` carries no color.

| State | Spinner | Bar fill | Rendered by |
|---|---|---|---|
| Active overall | green | magenta/dim | `RenderStyle::Overall` |
| Active child | green | yellow/dim | `RenderStyle::Active` |
| Success/Warning finished | white/dim | green/dim | `RenderStyle::Done` |
| Failed | red | red/dim | `RenderStyle::Failed` |

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

All status-list suffixes use the **number-first** format: `{n} {word}`, comma-joined, no parentheses. Each status is a single word (compound words banned). `None` → bare word, `Some(0)` → dropped, `Some(n)` → `n word`. The format is defined once here; other files link to this section. See `StatusCount` and `format_status_list` in `src/mediapm-utils/src/progress/inner/components.rs` (`:19` and `:31`), the same file the source-of-truth list above names.

### `bar_color_code` lookup

| Status | is_overall | Color code | Effective color |
|---|---|---|---|
| `Failed` | any | `31` | red |
| `Active` | `true` | `35` | magenta |
| `Active` | `false` | `33` | yellow |
| `Warning` | any | `33` | yellow |
| `Success` | any | `32` | green |

## Truncation dispatch

`sync_snapshot_to_slot` (in `layout.rs`) records each slot's strings in the `SlotCache`; it never writes a terminal. Whether client-defined truncation (`BarLabelTruncation` via `set_truncation`) or built-in truncation (`PrefixComponents` via `set_prefix_components`) applies is decided per row:

- **Client-truncated rows**: `settle_layout` measures through `truncate_prefix(prefix_ceiling)` / `truncate_suffix(...)`, and the slot row applies the same calls, prepending the 4-byte ANSI reset `\x1b[0m` to the result (`layout.rs::client_truncated_prefix`). Suffix: `truncate_suffix(suffix_w, &fresh_suffix)` where `fresh_suffix` is the merged `SuffixComponents` (auto-derived + user-set fields), with the timing stripped on a frame that draws no fill unless the row would otherwise have nothing to say.
- **Built-in rows**: the status-marker overhead is 13 for `Failed`/`Warning`, 4 otherwise. `semantic_truncate_prefix(&components, prefix_w - ansi_overhead)` then `render_prefix_components` wraps the prefix in colored markers.

`settle_layout` applies the same overhead rule for width budgeting. It measures the suffix through the same `render_slot_suffix` the row renders through, so a client label is asked for its own output in both passes.

## Single writer and per-frame sync

Every frame has exactly one writer. `run_frame()` (`frame.rs::run_frame`) does, in order: absorb a resize bookkeeping-only (`maybe_adjust_for_resize` — no restyle, no write), `build_plan` (which calls `settle_layout` for the width budget and builds one `PlanRow` per slot), emit the debug snapshot, render the plan to strings, then one `write_frame` through the gate's term. The facts that follow:

- **Every bound slot syncs every frame**: there is no change-detection layer at the snapshot level. `sync_snapshot_to_slot` re-records position, timing strings, status and style key every frame — it is what retained rows and blank decisions read. An event that only moves the clock (an elapsed string growing, a rate or ETA changing length) is re-derived on the same frame it happens; nothing needs a "declare your change" call.
- **The writer diffs frames per row**: `write_frame` repaints only rows that differ from the previous frame (skipping a write entirely when the frames are equal), so per-frame re-recording does not translate into per-row terminal writes. A resize forces a full repaint (`force = true` bypasses only the equality short-circuit).
- **`StyleKey` feeds retention and restart, not writes**: `sync_slot` caches a `StyleKey` of `(prefix_w, suffix_w, is_overall, draw_fill, status_code)` in `SlotCache`; a retained row restyles itself from it, and a source that goes Active again after a terminal state gets `invalidate_position()` so its position restarts from the new baseline. `mark_blanked` resets the key to `StyleKey::unknown()` so a re-bound slot re-styles whatever the derived inputs say.
- **Terminal transitions apply once**: a clear's blank is a one-shot announced by `finish_and_clear` (`SharedState::take_cleared_blank`) held until a rebind clears it. An unbound slot keeps whatever it last drew — the retention contract (`retained_row`, re-settled at current widths). A slot that never held content draws a full-width blank line (`blank_row` / `RenderStyle::Blank` — deliberately plain spaces: a fixed-width blank could wrap after a terminal shrink, and its pixels are an empty line either way).
- **Between screens**: `join()` finalizes the screen and `commit_frame` advances the cursor past the committed frame (CR+LF), so the next screen's frame starts on the row below the retained one.

## Three bar-label structs

All three structs implement [`BarLabelTruncation`] (defined in `src/mediapm-utils/src/progress/truncation.rs`) and flow through the client-truncation path: `settle_layout` measures them via `truncate_prefix`/`truncate_suffix`, and the slot row renders them the same way. The renderer prepends `\x1b[0m` (4-byte ANSI reset) to all client-truncated prefixes; no colored markers are embedded in the truncated string.

All three are installed by production code: the conductor coordinator puts `WorkerBarLabel` on each worker slot, the mediapm materializer installs `MaterializationBarLabel`, and the workflow example puts `StepBarLabel` on its per-step rows. The coordinator installs no label on the pinned overall row at all — its prefix is the static seed `running workflows` — so production renders no `StepBarLabel` anywhere.

Fitting uses `fit_segments` (shared from `mediapm_utils::progress`). Segments are supplied most important first and the list is walked from the tail, so the **last** segment is the first to yield. Under width pressure, elastic segments are shortened from the front, keeping the tail of the text, and once no segment can shrink further whole segments drop from the tail until the remainder fits. The clip is blind, so a tail may begin mid-word and a truncated path reads `he Wall`. Snapping to the next word or slash used to prevent that and cost monotonicity instead: two widths inside one word could render the same length, or go backwards. The clip came back, and `rendered_length_is_monotone_in_width` pins it.

Brackets belong to the segment (`brackets: Option<Brackets>`) rather than to its text, and a bracket carries how many consecutive segments it wraps, counting itself. `Segment::spanning_brackets(Brackets::Round, 2)` opens the group on a tool name and closes it on the version beside it; `Segment::brackets` is the one-member group, which is what every fixed marker uses. The pair is applied after fitting and draws only when every member of the group came through whole, so a group whose members were shortened or dropped renders as bare content rather than as half of a pair: a shortened row never shows a `)` whose `(` was cut away, and never shows `(ffmpeg v7.` on its own. Groups are consecutive and do not overlap. Nothing marks a cut, so a shortened segment is indistinguishable from one that was always that short.

**Segment order alone determines the outcome.** `fit_segments` drops from the tail unconditionally, so a field survives by its position in the list. When designing a new label, rank the fields and mark the shrink behaviour (`Shrink::Keep` for whole, `Shrink::Front` where the tail is the informative end, `Shrink::Head` for a version, which numbers itself from the left).

The per-screen orderings summarised under each struct below are a convenience, not the spec. The normative ranking, and the reasoning behind each segment's classification, live in the `prefix_segments` and `suffix_segments` bodies of `src/mediapm-conductor/src/orchestration/progress_labels.rs` (`StepBarLabel`, `WorkerBarLabel`) and `src/mediapm/src/materializer/progress_labels.rs` (`MaterializationBarLabel`). Read the code when a field's classification is in question.

### Struct 1: `StepBarLabel` (workflow example's step rows)

**File**: `src/mediapm-conductor/src/orchestration/progress_labels.rs`

Carries workflow, step and tool identity, plus a version and a tally. Production no longer installs it: the coordinator's pinned overall row is a static seed, and the overall bar counts steps through its fill rather than through a tally. The workflow example's step rows are its only caller and the only place a version (`v7.1`) is filled, so they are also the only screen where `Shrink::Head` has anything to cut.

| Field | Meaning | Example |
|-------|---------|---------|
| `version` | Tool version, head-keeping, inside the tool name's brackets. Filled only by the workflow example's step rows; production installs no `StepBarLabel`, so it never renders there. | `"v7.1"` |
| `completed` / `total` | Progress tally, rendered in the suffix only when `completed` is set | `"2"` / `"5"` |
| `status_marker` | Terminal state marker | `""` / `"F"` / `"W"` |
| `workflow_id` | Workflow name | `"default"` |
| `step_id` | Step identifier | `"s3"` |
| `tool` | Conductor tool name, parenthesized; the pair wraps the version when there is one | `(ffmpeg v7.1)` |

### Struct 2: `WorkerBarLabel` (conductor worker-slot bars)

**File**: `src/mediapm-conductor/src/orchestration/progress_labels.rs`

Carries the slot's activity flag and live tally, with no workflow phase. Prefix segments are ordered `status_marker`, `workflow_id`, `step_id`, `tool`, `activity`, so the activity marker trails and the tool name is the only elastic segment. The tally is a suffix segment: `suffix_segments` leads with a `Segment::keep` `{completed}/{total}`, rendered only when both fields are non-empty, with the auto-derived `elapsed`, `rate`, `eta` behind it and elastic `custom` last. On Screen B the coordinator sets `suppress_rate_eta` on every worker bar, so the suffix reaching the label carries the tally and the elapsed only — the rate and eta a step-count position would derive are dropped at `timing_strings`, the same single point the width budget measures through. The renderer's built-in `count`/`total` are not read here, so the bar's own position never doubles as a second tally. This is the label the coordinator creates a bar for, one per pool member (`src/mediapm-conductor/src/orchestration/coordinator.rs::run_workflow`), and re-labels through `set_truncation` on every dispatch and every step outcome.

| Field | Meaning | Example |
|-------|---------|---------|
| `status_marker` | Terminal state marker | `""` / `"F"` / `"W"` |
| `workflow_id` | Workflow name | `"default"` |
| `step_id` | Step identifier | `"s5"` |
| `tool` | Conductor tool name | `"echo"` |
| `activity` | Current activity | `"active"` / `"idle"` |
| `completed` | Terminal outcomes on this slot (successes + final failures); numerator of the suffix-leading tally | `"2"` |
| `total` | Dispatches to this slot, growing as it pulls work and counting each attempt again on re-dispatch; denominator, rendered only with `completed` | `"5"` |

### Struct 3: `MaterializationBarLabel` (mediapm materialization bars)

**File**: `src/mediapm/src/materializer/progress_labels.rs`

Carries entry identity. No version, no count/total, no workflow/step, no phase, no path directory.

| Field | Meaning | Example |
|-------|---------|---------|
| `status_marker` | Terminal-state marker, rendered bracketed; empty omits the segment | `""` / `"W"` / `"F"` |
| `media_id` | Identity the row materializes for, bracketed and elastic; a playlist row carries no media id, so it passes the hierarchy id here (empty omits) | `"youtube.dQw4w9WgXcQ"` |
| `variant` | Variant the row writes, bracketed and elastic; empty while unresolved, on a folder row covering every variant, and on the overall bar | `"remaster"` |
| `entry_name` | Basename of the hierarchy entry, elastic and cut from the left so the extension-bearing tail survives | `"song.mkv"` |

**Suffix:** the auto-derived timing, in the same order `WorkerBarLabel` uses (`src/mediapm/src/materializer/progress_labels.rs::MaterializationBarLabel::suffix_segments`): `elapsed`, `rate`, `eta`, then an elastic `custom`. `truncate_suffix` fits them through `fit_segments` and has to read `elapsed`, `rate` and `eta` out of the merged `SuffixComponents` to do it: the client path replaces a bar's suffix with what this method returns rather than adding to it, so a label that returns an empty string takes the timing off the screen rather than narrowing it. The label renders no `count`/`total` — it has no tally of its own to name the numbers.

### Why the structs are separate

They carry different fields, and the truncation order is a per-screen decision rather than a property of a label type. A worker bar wants its activity marker first and keeps its tally in the suffix; a materialization bar wants the media id and variant that identify the entry, and its basename shortens from the left because its tail — the extension — is the informative end. Merging them would force every screen's ranking through one shared struct, and the ranking is the part that has to be right.

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

- **Bar label** is `{tool_id} {version} [{phase}]`, space-separated (`src/mediapm/src/conductor_bridge/sync/provision.rs::fetch_and_import_tool_payload`). A tool reporting no human-readable version drops the version and the space, so the label is `media-tagger [fch]`.
- **Resolve bar** totals `metadata_fetch_count` (`provision.rs::fetch_and_import_tool_payload`), which is 5 for ffmpeg (the BtbN autobuild tag, the martin-riedl macOS-leg probe, and three jellyfin fallback reads: the repo index plus one pointer file per macOS arch) and 1 for every other tool. It is a lookup count, never a byte total or a percentage. `resolve_tool_fetch_ffmpeg_leaves_version_and_vcs_hash_none` asserts the number against a fully seeded cache, so a new lookup fails the suite rather than silently making this line wrong.
- **Resolve bar** shows `N cached` status list via `SuffixComponents::status_list`.
- **Skip bar** shows `skipped, N cached` status list (when cached) vs `skipped` (when not cached).
- **Byte columns** render through `format_count` (`src/mediapm-utils/src/progress/inner/components.rs::format_count`), which is decimal: 31 457 280 bytes reads `31M`, not `29.9MiB`.
- **Prune bar** (`[prn]`) uses the `tools.len()` call before the `retain` block to count prune candidates and sets the total to that count. Zero-bar guard: `[prn]` bar is not created when there are no candidates. The bar advances once per document-rewrite removal, then once more for filesystem prune.
- Overall bar renders in `RenderStyle::Overall` (magenta); child bars in `RenderStyle::Active` (yellow).

### Screen B: Workflow (`src/mediapm-conductor/src/orchestration/`)

No row on this screen carries a phase tag. The seeds are `running workflows` for the overall bar — set by each screen owner, `src/mediapm/src/service.rs` for the library sync and `src/mediapm-conductor/src/cli.rs` for the conductor CLI — and `idle` for every worker slot (`coordinator.rs::run_workflow`). The overall bar's seed is never replaced — the coordinator installs no label on that row — while each worker slot is replaced by a client label as soon as the bar has one.

The overall row installs no label at all: its prefix is the static seed `running workflows`, and its progress comes from the built-in position/total, the run's status-list suffix, and the bar's terminal status, which still renders the built-in `[W]`/`[F]` marker. The status list is set once at run end from four counters — `executed`, `cached`, `failed`, `retried`, in that order — and is always populated; `format_status_list` drops zero counts, so a clean run's suffix reads exactly `N executed` (e.g. `3 executed`) and a run with drops reads them comma-joined in the same order (`2 executed, 1 cached, 1 failed`). Worker slots use `WorkerBarLabel`, which ranks `status_marker`, `workflow_id`, `step_id`, `tool`, `activity` and leaves `tool` the only elastic segment, so a running slot reads `default s5 (echo) [active]` wherever the terminal has room for it. Each slot's suffix leads with its live tally `A/B` — terminal outcomes on that slot over dispatches to it; a retryable attempt counts in the total when dispatched and moves no completed count until it settles terminally, possibly on another slot. The suffix then carries the elapsed time and nothing else pace-related: the coordinator sets `suppress_rate_eta` on every worker bar, so `timing_strings` drops the step-count-derived rate and eta before either the measurement pass or the draw path sees them. Dispatch is a dependency-aware ready queue over a free-worker FIFO pool, so steps of independent branches appear concurrently across slots and which slot runs which step is schedule-dependent. `fit_segments` walks from the tail, so a narrow row gives up the `[active]` tag and then the tool name and keeps the identifiers. The seed does not cap the slot: the label is measured from what the row draws, so a row keeps its names at widths where the four-column seed would have clipped them.

Worker-slot states (`worker_slot_label` in `coordinator.rs::worker_slot_label`):

| State | `workflow_id` | `step_id` | `tool` | `activity` | `status_marker` |
|-------|---------------|-----------|--------|------------|------------------|
| Active | workflow name | step id | conductor tool name | `active` | (empty) |
| PendingRetry | (empty) | (empty) | (empty) | `idle` | `W` |
| Failed | (empty) | (empty) | (empty) | `idle` | `F` |
| Idle / Succeeded | (empty) | (empty) | (empty) | `idle` | (empty) |

`tool` is empty in every state but `Active`, because a slot with no step on it has no tool to name. The prefix stops at its activity marker: `[idle]`, or `[W] [idle]` and `[F] [idle]` for a retry and a final failure; the tally and the elapsed follow as suffix segments. Earlier revisions filled `tool` with the string `idle`, which rendered as `(idle)` beside the `[idle]` that `activity` had already said.

Worker labels are mediapm-agnostic; `tool` is the conductor step's own `ToolSpec.name`, never a managed-tool name.

### Screen C: Materialization (`src/mediapm/src/materializer/`)

Uses `MaterializationBarLabel` for client-defined truncation. The label carries identity only — `status_marker`, `media_id`, `variant`, `entry_name`, in that truncation order. The materializer splits each entry's relative path via `split_entry_path` and puts the basename in `entry_name`; the directory portion is dropped, so the label carries no path. Under width pressure the fields yield in reverse rank: the basename gives columns first, then the variant, then the media id, and an elastic segment that is shortened drops its bracket pair rather than leaving half of one, so a squeezed id renders bare (`media-001`) instead of `a-001]`. `entry_name` is elastic rather than kept whole because a name kept whole is a name `fit_segments` drops once nothing else can shrink; cutting it from the left keeps its informative tail — the extension. There is no phase tag, no extracted-file name, and no count/total on the label: the `A/B` fraction, the outcome colour and the finish marker already say what a row is doing.

The screen has no phase model: a row's total is the files it writes and it advances once per file written, so the `A/B` fraction, the outcome colour and the finish marker carry the row's state, and a row's label is identity only — no tag repeats what the fraction already says.

A warning or a failure carries its marker in the text as well as in the bar colour. `EntryRowBar::finish` (`src/mediapm/src/materializer/mod.rs`) re-installs the row's identity with `status_marker` set to `W` or `F` immediately before `finish_warning` or `finish_error`, so `[W]` and `[F]` are readable on the row itself. A row also carries the timing the renderer derives for it, for the reason given under `MaterializationBarLabel` above, except on a frame that has dropped its bar where the timing gives way to the label unless the row would then have nothing at all.

### Overall bar seeds

No overall bar on any screen carries a tag: the seeds are `syncing tools` on tool-sync, `workflow` on the conductor screen, `materializing` on the materialization example, and `idle` on every worker slot.

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

A missing hierarchy path is one the run could not produce, so the library does not hold what the config asked for. The Screen A summary and the Screen C phase line both report it as `Error`, and `mediapm sync` exits non-zero. A failed workflow step is the same case: the media that step was to produce is missing, so it is `Error` too. It used to be a `Warning` on the grounds that the paths the step owns may still have materialized, which does not hold up: those paths are materialized from what the step was supposed to produce. A skipped path is the opposite case and is neither. It is a path whose recorded hash is the one the run resolved and whose length matches the length of the CAS object that hash names, so the run wrote nothing. The check reads no content from the target, so a same-length in-place edit still counts as a skip.

### Per-screen summary formats

**Screen A: `mediapm sync`** (via `print_sync_summary` in `output/mod.rs`):

```text
✓ sync complete    executed=3  cached=2  materialized=5
  Δ some warning message
```

```text
✗ sync complete    executed=3  materialized=5  missing=3
  Δ some warning message
✗ sync did not produce everything the config asked for: missing=3 failed_steps=0
```

- Icon: `Error` when `missing_paths > 0 || workflow_failed_steps > 0`, ahead of every count; `Warning` when the run carried warnings; `Success` when `executed > 0 || materialized > 0`; `NoChange` otherwise. `skipped_paths` is read by none of these, so an entry that was already correct leaves the icon alone.
- Exit status: `0` clean, `3` a finished run that warned, `4` a finished run that left the library short, so the summary and its `print_error` counts line are printed (the warning path adds nothing, since the `Δ` lines already said it); `1` is the failure band and never passes through `sync_outcome` — a usage or feature problem, an entry that raised, and a lost CAS object, which is fatal and prints no summary at all because the entry raised before the run reached its outcome. `sync_exit_status` in `src/mediapm/src/main.rs` holds the `0`/`3`/`4` mapping and reads [`sync_outcome`](../../src/mediapm/src/output/mod.rs), which is the same function `sync_summary_icon` reads, so the line a user sees and the status a script sees come from one rule; `2` is unused because POSIX reserves it for shell misuse. The band-by-band contract, including a test that spawns the binary for each band reachable offline, is the module doc of `src/mediapm/tests/e2e/sync_exit_status.rs`.
- Fields: `executed` always shown; `cached`, `materialized`, `skipped`, `missing`, `removed`, `removed_empty`, `added_tools`, `updated_tools`, `pruned_tools`, `removed_tools`, `skipped_tools`, `failed` shown only when >0.
- Warnings: one `print_warning` line per warning.

**Screen A: `mediapm tool sync`** (via `CliSyncObserver::on_phase` in `output/observer.rs`, which calls `print_result` with the `ToolsSyncSummary` fields):

```text
✓ tools synced    added=2  updated=1  pruned=0  removed=0
  Δ warning message
```

- Fields: `added`, `updated`, `pruned`, `removed` (all always shown).
- `added` counts a tool the recorded registry has no entry for; `updated` counts a tool whose identity changed against the newest recorded entry, and identity is the canonical version plus the content-map hash. A pass that reproduces an entry already recorded — re-registering the no-payload `import` builtin, for instance — counts it in `skipped_tools`, which this line does not print, so `updated` answers "did the backing conductor tool change" rather than "did the pass touch it".
- Warnings: one `print_warning` line per warning.

**Screen B: workflow** (via `CliSyncObserver` in `output/observer.rs`):

```text
✓ workflow    executed=3  cached=2  failed=0
```

- Icon: `Error` when `failed_steps > 0`; `NoChange` when nothing ran; `Success` otherwise. The rule is `workflow_icon` in `output/observer.rs`.
- Fields: `executed`, `cached`, `failed`.
- The conductor CLI (`conductor run`) renders identically via `format_result_line`.

**Screen C: materialization** (via `CliSyncObserver` in `output/observer.rs`):

```text
✗ materialized    paths=5  skipped=3  removed=1  missing=2
```

- Icon: `Error` when `missing_paths > 0`, ahead of the counts; `Success` when `materialized + removed > 0`; `NoChange` otherwise. The screen above this line already ends red on a missing path, and the line agrees with it. The rule is `materialization_icon` in `output/observer.rs`.
- Fields: `paths`, `skipped`, `removed`, plus `missing` when it is above zero.

### Per-phase observer contract

The library (`MediaPmService::sync_library_with_tag_update_checks_and_observer`) calls `observer.on_phase(report)` immediately after each screen's `group.join()` returns, in order: `Tools` → `Workflow` → `Materialization`. The `CliSyncObserver` renders structured `print_result` lines. Tests use a recording observer to assert the exact phase sequence and values.

## Worked examples: run them, do not transcribe them

Do not paste a terminal frame into this file. Screen A is `mediapm_progress_tool_sync`, screen B is `mediapm_progress_workflow`, and screen C is `mediapm_progress_materialize`. Run one with `--width N` to see a frame at that width, or read the transcripts under `src/mediapm/examples/fixtures/`. The transcripts are generated output and must not be edited by hand, because a hand-edited fixture no longer proves that the renderer draws what the code says it draws. Children render above the overall bar: child bars first, overall bar last.

Every example takes `--scenario`, `--width` and `--height`. A transcript is named `<stem>-<scenario>-width-<N>.txt`, and the three scenarios are `baseline`, `dense` and `states`. A name with no scenario segment is the width-only shape these files carried before the axis existed and reads as `baseline`; a segment naming no scenario is an error, because a typo there would quietly drop a scenario's coverage. `--height` is only for drawing a screen somewhere other than the height its own bar count asks for.

Height follows from the scenario: `Scenario::height()` is `bars + 1`. The renderer draws one line per bar, and the newline that commits the frame scrolls the top row away once the frame fills the terminal, so the frame is given a row it does not draw. That one row is exactly enough at every band size from 4 to 255 bars.

`Scenario::render_at` (`src/mediapm/examples/support/scenarios.rs`) is the entry point a transcript is captured through, and it checks two things before handing the frame back. It asserts the drawn grid has one line per declared bar, and it draws the same scenario a second time at twice the derived height and asserts the two grids are identical. The second check is the load-bearing one and nothing else in the suite replaces it: a renderer whose output depends on the terminal height produces the right row count at the height its own bar count asks for and a different frame at any other height, so every transcript comparison would pass while the screen was wrong on a terminal nobody captured it on. A scenario that declares more bars than it draws fails the first check, one that declares fewer fails the second, which is why it is checked from both sides. `render_at_refuses_a_frame_that_moves_with_the_terminal_height` and its two neighbours in the same test module pin both refusals, so dropping either assertion fails the suite instead of quietly losing the check.

Regenerate a transcript by running the example with its scenario and width and redirecting stdout. The directory listing is the set of scenarios and widths, since each file is named for both and each screen's test reads both back out of those names. A file in one of those directories that is not a transcript is a test failure, not something to leave lying around.

Each example's test walks its own fixture directory and compares what the screen renders against each transcript, so a layout change fails the suite before anyone reaches for a transcript by hand.

## Debug JSONL

`MEDIAPM_PROGRESS_DEBUG=1` env var enables JSONL tick output to stderr. Each line is a JSON object with fields: `type` (`"tick"`), `tick` (monotonic counter), `elapsed_secs` (f64), `bars` (array of per-slot state: `slot`, `bound`, `label`, `prefix`, `position`, `total`, `status`, `elapsed_secs`, `rate_bytes_per_sec`, `eta_secs`, `suffix`).

## Pre-roll, gap conversion, finalize

- **Pre-roll**: owned by `ProgressTerminal` (`pre_roll_if_needed`, `terminal.rs::pre_roll_if_needed`): once per terminal, before the first screen's first frame, it writes blank lines to scroll existing terminal content into scrollback, then repositions the cursor. Only fires when `pre_roll_term` is `Some` (not in test mode).
- **Gap conversion**: inactive or finished rows render where the frame has room for a fill. Idle workers are explicitly finished (via `finish_success`), which is what a `Done`-styled row shows. Below the fill crossing the frame draws no bar, so an idle worker reads `[idle]` and its timing on its own.
- **Finalize**: `group.join()` runs `finalize` (`renderer.rs::finalize`): blank reserved slots are removed from the plan, and one last full-fill frame is written so only finished bars persist in scrollback. A screen whose frames were all blank writes nothing at all (`blank_first_frame` inside `frame.rs::run_frame` — a never-drawn screen owes no frame).

## Authoritative tests

The `mediapm-utils` suite uses `assert_eq!(term.contents(), concat!(...))` against an inline literal and IS the real format:

- `src/mediapm-utils/tests/progress_output/` — `consumer.rs`, `elapsed.rs`, `layout.rs`, `lifecycle.rs`, `render.rs`, `resize.rs`, `spinner.rs`; `common.rs` and `debug.rs` are helpers and the debug sink. These drive the renderer with the ticker off and a test time source, so their exact `assert_eq!(term.contents(), concat!(...))` comparisons hold every column including the spinner
- `src/mediapm/examples/mediapm_progress_tool_sync.rs`, `mediapm_progress_workflow.rs`, `mediapm_progress_materialize.rs` — one test that walks the screen's transcript directory and compares every frame it finds there through `assert_frames_match`, plus a `*_screen_never_wraps_a_row` sweep and a `*_screen_rows_never_go_empty` sweep over the widths the harness accepts

`src/mediapm/src/output/progress.rs` also asserts against `term.contents()`, but with substring checks rather than exact literals, so it pins behaviour such as "elapsed reads `0s` after finish" rather than a full frame.

## Global toggle and auto-detection

Progress is suppressed by passing `no_progress: true` or by constructing a `ProgressScreen::disabled()`. Progress bars are also automatically hidden when stderr is not a TTY (`console::Term::stderr()` detection). The `--quiet` / `MEDIAPM_QUIET` flags suppress hints and progress.

## Spinner animation

The terminal runs a daemon ticker at 50 ms intervals, so a frame is computed even during long periods without position updates. Animation is **park-by-default**: a row cycles the braille spinner `⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏` only when its status is Active AND its source opted in via `set_animating(true)` (`ProgressBarApi`); every other row parks on the fixed `·` (`PARKED_GLYPH`) — idle slots, finished rows, terminal rows. Production call sites opt in where the caller is genuinely working (the tool-sync phases, the materializer); an idle worker slot parks. For deterministic tests, disable the ticker with `.with_ticker_enabled(false)`.

### Which glyph a row ends on is not the renderer's to decide

An animated row's glyph is `TICK_CHARS[frame_counter % 10]`, and the frame counter advances once per computed frame — not per row. So the glyph on a row is a function of how many frames were drawn while it was animating, and the frame count is a function of everything else on screen: handing another row more ticks moves this row's glyph too. Two runs that drive the same rows in the same order can end on different glyphs, and nothing in this repository controls which.

That is what the transcript comparison is for. `assert_frames_match` (`src/mediapm-utils/src/progress/frame_match.rs`) compares every column of two frames exactly and exempts the spinner column alone, so a frame is checked for layout without being checked on animation phase. Its own doc records what it cannot see, and the swap it cannot name is stated there as a fact about the output rather than as a defect. Reach for it instead of a byte comparison anywhere a fixture records a frame, and do not re-tighten the spinner column: a fixture that pins the glyph records the machine that captured it.

## Library stack

The stack is `console` 0.15 (`Term::stderr().size()` for terminal width detection, `style()` for ANSI coloring) plus `indicatif` 0.17 for its terminal types (`TermLike`, `InMemoryTerm`, and the `MultiProgress` type the migration-seam builder methods still accept) and `HumanBytes`/`HumanCount` formatting. The renderer draws frames itself; it owns no `ProgressBar`. Do not add `owo-colors`, `colored`, `termion`, or other styling crates; `console::style()` is the single styling entry point.

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
