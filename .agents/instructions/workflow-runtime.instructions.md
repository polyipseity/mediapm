---
description: "Use for progress rendering and provider byte budgets."
name: "Workflow runtime progress"
applyTo: "src/mediapm-utils/src/progress/mod.rs, src/mediapm-utils/src/progress/traits.rs, src/mediapm-utils/src/progress/recording.rs, src/mediapm-utils/src/progress/inner/**/*.rs, src/mediapm-utils/src/progress/truncation.rs, src/mediapm-utils/src/progress/multi_item_budget.rs, src/mediapm-utils/src/progress/byte_budget.rs, src/mediapm-utils/src/progress/download.rs, src/mediapm-conductor/src/orchestration/progress_labels.rs, src/mediapm-conductor/src/orchestration/coordinator.rs, src/mediapm-conductor/src/tools/provider/mod.rs, src/mediapm/src/materializer/progress_labels.rs, src/mediapm/src/materializer/mod.rs, src/mediapm/src/output/progress.rs, src/mediapm/src/conductor_bridge/sync/**/*.rs"
---

# Workflow runtime progress

Never hand-write frames in docs; examples/tests are the rendered-output authority.

## Source of truth

- `src/mediapm-utils/src/progress/inner/plan.rs`: `FramePlan`, `PlanRow`, `RenderStyle`, `RowAnimation`. `src/mediapm-utils/src/progress/inner/frame.rs`: `tick()` -> `run_frame()` (resize absorb, `build_plan`, debug snapshot, one `write_frame`).
- `src/mediapm-utils/src/progress/inner/writer.rs`: `write_frame(term, previous, next, force)` sole writer (per-row diff repaint). `src/mediapm-utils/src/progress/inner/gate.rs`: `WriteGate`, `commit_frame`. `src/mediapm-utils/src/progress/inner/renderer.rs`: `build_plan`, `sync_slot`, retention, finalize, `SlotCache`/`StyleKey`.
- `src/mediapm-utils/src/progress/inner/layout.rs`: `settle_layout`, budgeting, `keeps_timing`/`without_timing`. `src/mediapm-utils/src/progress/inner/components.rs`: truncation, colors, `StatusCount`, `format_status_list`, `PARKED_GLYPH`; `src/mediapm-utils/src/progress/truncation.rs`: `BarLabelTruncation`, `fit_segments`, segment/bracket types.
- Tests `src/mediapm-utils/tests/progress_output/*.rs` use exact `assert_eq!(term.contents(), concat!(...))`; examples `src/mediapm/examples/mediapm_progress_*.rs` + transcripts are frame authority.

## Layout

- Constants `MIN_PREFIX_WIDTH=0`, `MAX_PREFIX_WIDTH=40`, `MIN_SUFFIX_WIDTH=0`, `MAX_SUFFIX_WIDTH=65`, `FRAME_OVERHEAD_COLUMNS=4`, `MIN_BAR_FILL=4`.
- `settle_layout`: `reserved = cols - (FRAME_OVERHEAD_COLUMNS + MIN_BAR_FILL)`; suffix first (no wrap), prefix, remainder; `draw_fill` = `fill > MIN_BAR_FILL`. No fill: re-settle over `cols - FRAME_OVERHEAD_COLUMNS`, suffix without timing where possible; `keeps_timing`/`without_timing` decide timing when a row would empty.
- No row wraps or empties: label, count/timing, or timing-only fallback. With client truncation, measure slot from rendered output (`settle_layout` at ceiling for client labels, markers for built-ins).

## Truncation and labels

- Client rows implement `BarLabelTruncation`, `truncate_prefix`/`truncate_suffix`; renderer prepends `\x1b[0m` to client prefixes. Suffix uses merged `SuffixComponents`; timing may drop on no-fill unless row empties. Built-ins use `PrefixComponents`/`semantic_truncate_prefix`/`render_prefix_components`; marker overhead 13 for `Failed`/`Warning`, else 4.
- `fit_segments`: most-important-first, walk tail, shorten elastic from front, drop whole tail segments, brackets only when all members whole; no cut marker; order is contract.
- `StepBarLabel` (`src/mediapm-conductor/src/orchestration/progress_labels.rs`): example step rows (workflow/step/tool/version/tally), not installed by production coordinator. `WorkerBarLabel` (`progress_labels.rs`): worker slots, status/workflow/step/tool/activity + tally; rate/eta suppressed on Screen B. `MaterializationBarLabel` (`src/mediapm/src/materializer/progress_labels.rs`): status_marker/media_id/variant/entry_name + timing, no count/total. Ranking: `prefix_segments`/`suffix_segments` in implementations.

## Rendering

One writer/frame; `run_frame`: resize absorb, `build_plan`, debug snapshot, render, `write_frame` (row diff, skip equal, resize repaint). Slots sync each frame; `StyleKey` = retention/restart; transitions once, blank one-shot. `RenderStyle`/`RowAnimation` drive spinner/fill; animate only Active + `set_animating(true)`, else `PARKED_GLYPH`; `StatusCount`/`format_status_list` use `{n} word`. `MEDIAPM_PROGRESS_DEBUG=1` -> JSONL stderr, pre-roll once, finalize full-fill and drop blank slots; suppressed by `no_progress`, `ProgressScreen::disabled()`, non-TTY stderr, quiet flags.

## MultiItemBudget and provider progress

- `MultiItemBudget` (`src/mediapm-utils/src/progress/multi_item_budget.rs`): thread-safe `Vec<ItemBudget>` of `(pos: AtomicU64, total: AtomicU64)`; `pos <= total` hard `assert!`; atomics `AcqRel`/`Acquire`/`Release`. `advance` = `compare_exchange_weak` loop; `set_pos` monotonic; `total == 0` indeterminate/0 bytes; `aggregate()` sums.
- Extraction helpers take only `local_cb: Option<&dyn Fn(u64)>` (local compressed/read position); outer loop owns budget, maps via `set_pos`/`advance`.
- Fetch fires after every `reqwest::Response::bytes_stream()` chunk, daemon ticker consolidates. Resolve/Fetch/Process use `ProviderPhase`, `ProviderProgressSnapshot { phase, items: (completed,total), bytes: (completed,total) }` (`src/mediapm-conductor/src/tools/provider/mod.rs`).
- Process: 2 items for archives (decompress + compress), 1 binary/CAS import; `CountingReader` Cell-based, single-threaded, `SUB_ENTRY_CHUNK=65536`; 64 KB sub-entry chunk for ZIP/tar.gz/tar.xz/pack. Monotone, `pos <= total`, eventual completion; ZIP proportional, tar.gz/xz compressed bytes; `ByteBudget` legacy outside provider pipeline.

## Pointers

`src/mediapm-utils/src/progress/inner/{plan,frame,writer,gate,renderer,layout,components}.rs`, `src/mediapm-utils/src/progress/{mod,truncation,multi_item_budget,byte_budget,download}.rs`, `src/mediapm-conductor/src/orchestration/{coordinator,progress_labels}.rs`, `src/mediapm/src/materializer/{mod,progress_labels}.rs`, `src/mediapm/src/output/progress.rs`, `src/mediapm/src/conductor_bridge/sync/**/*.rs`, `src/mediapm-utils/tests/progress_output/*.rs`, `src/mediapm/examples/mediapm_progress_{tool_sync,workflow,materialize}.rs`.
