---
description: "Use when editing progress rendering, truncation, or provider byte-budget tracking. Covers progress frame layout, label truncation, and MultiItemBudget invariants."
name: "Workflow runtime progress"
applyTo: "src/mediapm-utils/src/progress/mod.rs, src/mediapm-utils/src/progress/traits.rs, src/mediapm-utils/src/progress/recording.rs, src/mediapm-utils/src/progress/inner/**/*.rs, src/mediapm-utils/src/progress/truncation.rs, src/mediapm-utils/src/progress/multi_item_budget.rs, src/mediapm-utils/src/progress/byte_budget.rs, src/mediapm-utils/src/progress/download.rs, src/mediapm-conductor/src/orchestration/progress_labels.rs, src/mediapm-conductor/src/orchestration/coordinator.rs, src/mediapm-conductor/src/tools/provider/mod.rs, src/mediapm/src/materializer/progress_labels.rs, src/mediapm/src/materializer/mod.rs, src/mediapm/src/output/progress.rs, src/mediapm/src/conductor_bridge/sync/**/*.rs"
---

# Workflow runtime progress

Rules and invariants for progress rendering and provider byte-budget tracking. Do not hand-write terminal frames in docs; use runnable examples and tests as the rendered-output authority.

## Source of truth

- `src/mediapm-utils/src/progress/inner/plan.rs`: `FramePlan`, `PlanRow`, `RenderStyle`, `RowAnimation`, row rendering.
- `src/mediapm-utils/src/progress/inner/frame.rs`: `tick()` -> `run_frame()` (resize absorb, `build_plan`, debug snapshot, one `write_frame`).
- `src/mediapm-utils/src/progress/inner/writer.rs`: `write_frame(term, previous, next, force)` is the only terminal writer (per-row diff repaint).
- `src/mediapm-utils/src/progress/inner/gate.rs`: `WriteGate`, `commit_frame`.
- `src/mediapm-utils/src/progress/inner/renderer.rs`: `build_plan`, `sync_slot`, retention, finalize, `SlotCache`/`StyleKey`.
- `src/mediapm-utils/src/progress/inner/layout.rs`: `settle_layout`, width budgeting, `keeps_timing`/`without_timing`.
- `src/mediapm-utils/src/progress/inner/components.rs`: truncation, colors, `StatusCount`, `format_status_list`, `PARKED_GLYPH`.
- `src/mediapm-utils/src/progress/truncation.rs`: `BarLabelTruncation`, `fit_segments`, segment/bracket types.
- Tests: `src/mediapm-utils/tests/progress_output/*.rs` use exact `assert_eq!(term.contents(), concat!(...))`; screen examples under `src/mediapm/examples/mediapm_progress_*.rs` and transcript fixtures are the rendered-frame authority.

## Layout invariants

- Width constants: `MIN_PREFIX_WIDTH=0`, `MAX_PREFIX_WIDTH=40`, `MIN_SUFFIX_WIDTH=0`, `MAX_SUFFIX_WIDTH=65`, `FRAME_OVERHEAD_COLUMNS=4`, `MIN_BAR_FILL=4`.
- `settle_layout` computes `reserved = cols - (FRAME_OVERHEAD_COLUMNS + MIN_BAR_FILL)`, allocates suffix first (it must not wrap), then prefix, then fill remainder. `draw_fill` is `fill > MIN_BAR_FILL`.
- When no fill is drawn, re-settle over `cols - FRAME_OVERHEAD_COLUMNS` with suffix measured without timing where possible. `keeps_timing`/`without_timing` decide whether timing stays when a row would otherwise become empty.
- No row wraps. A row must never go empty: label, count/timing, or timing-only fallback carries it.
- The slot is measured from rendered output, not the seed, when client truncation is installed. `settle_layout` measures client labels at the ceiling; built-in rows account for status-marker overhead.

## Truncation and labels

- Client-truncated rows implement `BarLabelTruncation` and go through `truncate_prefix`/`truncate_suffix`. The renderer prepends `\x1b[0m` to client-truncated prefixes. Suffix truncation uses merged `SuffixComponents`; timing may be stripped on no-fill frames unless the row would be empty.
- Built-in rows use `PrefixComponents`/`semantic_truncate_prefix` and `render_prefix_components`; marker overhead is 13 for `Failed`/`Warning`, 4 otherwise.
- `fit_segments` takes most-important-first segments, walks from tail, shortens elastic segments from the front, drops whole tail segments when needed, and applies bracket groups only when all group members remain whole. No cut marker. Segment order is the truncation contract.
- Label structs:
  - `StepBarLabel` (`src/mediapm-conductor/src/orchestration/progress_labels.rs`): workflow example step rows; workflow/step/tool/version/tally. Not installed by production coordinator.
  - `WorkerBarLabel` (`progress_labels.rs`): conductor worker slots; status/workflow/step/tool/activity prefix and live tally suffix. Coordinator suppresses rate/eta on Screen B worker bars.
  - `MaterializationBarLabel` (`src/mediapm/src/materializer/progress_labels.rs`): materialization rows; status_marker/media_id/variant/entry_name with timing suffix, no count/total.
- For normative ranking and segment classification, read `prefix_segments`/`suffix_segments` in the label implementations.

## Rendering facts

- One writer per frame. `run_frame` order: resize absorb, `build_plan`, debug snapshot, render, `write_frame`. `write_frame` diffs rows and skips equal frames; resize forces repaint.
- Every bound slot syncs every frame; `StyleKey` is for retention/restart, not write suppression. Terminal transitions apply once; cleared blank is one-shot.
- `RenderStyle`/`RowAnimation` drive spinner, fill color, and animation. A row animates only when Active and opted in via `set_animating(true)`; otherwise it parks on `PARKED_GLYPH`. `StatusCount`/`format_status_list` use number-first `{n} word`.
- Debug JSONL: `MEDIAPM_PROGRESS_DEBUG=1` emits tick JSONL to stderr. Pre-roll is once per terminal before first frame; finalize writes a last full-fill frame and drops blank reserved slots. Progress is suppressed by `no_progress`, `ProgressScreen::disabled()`, non-TTY stderr, or quiet flags as applicable.

## MultiItemBudget and provider progress

- `MultiItemBudget` (`src/mediapm-utils/src/progress/multi_item_budget.rs`) is a thread-safe `Vec<ItemBudget>` of `(pos: AtomicU64, total: AtomicU64)`. `pos <= total` is a hard `assert!` always compiled. Atomics use `AcqRel`/`Acquire`/`Release`.
- `advance` uses a `compare_exchange_weak` loop; `set_pos` is monotonic absolute set. `total == 0` is indeterminate and contributes 0 bytes to aggregate. `aggregate()` sums all items.
- Extraction helpers take only `local_cb: Option<&dyn Fn(u64)>`; they report local compressed/read position. The outer phase loop owns the budget and maps local progress with `set_pos`/`advance`.
- Fetch fires progress after every chunk yielded by `reqwest::Response::bytes_stream()`; daemon ticker consolidates draw frequency. Resolve/Fetch/Process use `ProviderPhase` and `ProviderProgressSnapshot { phase, items: (completed,total), bytes: (completed,total) }` (`src/mediapm-conductor/src/tools/provider/mod.rs`).
- Process uses 2 budget items for archive sources (decompress + compress) and 1 for binary/CAS import. `CountingReader` is Cell-based, single-threaded, and fires at `SUB_ENTRY_CHUNK=65536`. Unified 64 KB sub-entry chunk applies across ZIP/tar.gz/tar.xz/pack.
- Accuracy invariants: monotonic non-decreasing, `pos <= total`, eventual completion. ZIP uses proportional estimate; tar.gz/xz track compressed bytes via `CountingReader`. ByteBudget remains legacy/single-position outside provider pipeline.

## Pointers

- Progress implementation: `src/mediapm-utils/src/progress/inner/{plan,frame,writer,gate,renderer,layout,components}.rs`, `src/mediapm-utils/src/progress/{mod,truncation,multi_item_budget,byte_budget,download}.rs`.
- Labels and consumers: `src/mediapm-conductor/src/orchestration/{coordinator,progress_labels}.rs`, `src/mediapm/src/materializer/{mod,progress_labels}.rs`, `src/mediapm/src/output/progress.rs`.
- Provider progress: `src/mediapm-conductor/src/tools/provider/mod.rs`, `src/mediapm/src/conductor_bridge/sync/**/*.rs`.
- Rendered-output checks: `src/mediapm-utils/tests/progress_output/*.rs`, `src/mediapm/examples/mediapm_progress_{tool_sync,workflow,materialize}.rs`.
