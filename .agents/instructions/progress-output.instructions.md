---
description: "Use when editing progress-bar rendering code in mediapm-utils or any consumer (conductor workflow screen, mediapm tool-sync, materialization). Records the ACTUAL rendered output format so agents never guess or invent ASCII mocks."
name: "Progress Bar Rendered Output Format"
applyTo: "src/mediapm-utils/src/progress.rs, src/mediapm-conductor/src/orchestration/coordinator.rs, src/mediapm/src/output/progress.rs, src/mediapm/src/conductor_bridge/sync/**/*.rs"
---

# Progress bar rendered output format

This file is the **authoritative reference** for what the progress bars actually
look like on a terminal. Any agent editing progress code MUST read this before
changing templates, glyphs, colors, prefix/suffix shapes, or ordering. Do NOT
invent ASCII mocks — the real format is captured from the `progress_output`
test suite below.

Source of truth: `src/mediapm-utils/src/progress.rs` templates
(`CHILD_BAR_TEMPLATE` ~line 803, `OVERALL_BAR_TEMPLATE` ~line 806) +
`render_prefix_components` (~line 1035) + `render_suffix_components` (~line 870)

- `bar_color_code` (~line 830), verified by
`src/mediapm-utils/tests/progress_output/*.rs` and
`src/mediapm/src/output/progress.rs` using
`assert_eq!(term.contents(), concat!(...))`.

## Production templates (verbatim)

```text
CHILD_BAR_TEMPLATE       = "{spinner:.green} {prefix:>30.30} {wide_bar:.yellow/dim} {msg:<25.55}"
OVERALL_BAR_TEMPLATE      = "{spinner:.green} {prefix:>30.30} {wide_bar:.magenta/dim} {msg:<25.55}"
COMPACT_BAR_TEMPLATE      = "{spinner:.green} {prefix:>25.25} {msg:<12.40}"          // width < 60
COMPACT_OVERALL_BAR_TEMPLATE = "{spinner:.green} {prefix:>25.25} {msg:<12.40}"      // width < 60
DONE_BAR_TEMPLATE         = "{spinner:.white/.dim} {prefix:>30.30} {wide_bar:.green/dim} {msg:<25.55}"
FAILED_BAR_TEMPLATE       = "{spinner:.red} {prefix:>30.30} {wide_bar:.red/dim} {msg:<25.55}"
```

The compact variants drop the `{wide_bar}` field entirely (used when terminal
width is below 60 columns). `DONE_*` and `FAILED_*` are the finished-state
templates applied via `apply_done_bar_style` / `apply_failed_bar_style`.

## Glyphs (NOT `▶`/`●`/`████`)

- **Spinner:** braille cycle `⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏` (10 frames). Frame 0 = `⠋`, frame 1 =
  `⠙`, … wraps. Driven by the ~50ms daemon ticker; each `bar.tick()` advances
  one frame. (Note: the first *tick* shows frame 1 `⠙`, not frame 0 `⠋`.)
- **Bar fill:** `progress_chars("█░")` → filled `█`, empty `░`. NOT `=` or `#`.
- **ANSI colors (real stderr only):** spinner green (`\x1b[32m`); child
  `wide_bar` yellow/dim (`\x1b[33m` + dim); overall `wide_bar` magenta/dim
  (`\x1b[35m` + dim); `count/total` in `bar_color_code` color (overall=35,
  child=33, failed=31, success=32); marker `[F]` red, `[W]` yellow. The
  `InMemoryTerm` tests compare against strings that may omit the SGR bytes, but
  REAL stderr output includes them.

## Prefix shape (`render_prefix_components`, starts with `\x1b[0m`)

```text
[{marker}] <tool_name>[ <version>][ [<phase>]][ <count>/<total>]
```

- `marker`: `F` (failed, red `[F]`), `W` (warning, yellow `[W]`), else empty.
- `tool_name`: always present (right-aligned to 30 in the template).
- `version`: e.g. `@7.1`. `phase`: e.g. `res`/`fch`/`pro`/`wf`. `count/total`:
  e.g. `3/12`.

## Suffix / `{msg}` shape (`render_suffix_components`)

```text
[ \x1b[{color}m{count}/{total}\x1b[0m][ {elapsed}][ {rate}[ {eta}]][ {custom}]
```

- `count/total` colored (omitted if both empty).
- `elapsed`: e.g. `0s`, `0:00:05`.
- `rate`: e.g. `0/d`, `12.3 MiB/s` (eta shown only when rate present).
- `custom`: e.g. `cached (2)`, `skipped`, `idle`.

## Ordering in `term.contents()`

Children render **above** the overall bar: child bars first, overall bar last.

## Spacing law (W ≥ 60)

`spaces_before_label = 1 (template) + (30 − label_len)`. Bar width at W=80 is
exactly **21** chars of `█`/`░` (filled = round(pos·21/total)).

## Worked example — sync screen (`syncing tools N/M`)

```text
⠹                        overall ░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d
⠹                          tool1 ░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d
⠹     ffmpeg autobuil [res]  0/100 0s 0/d          (W=40 compact: version truncated, [res] kept)
⠹             syncing tools  0/100 0s 0/d          (W=40 compact: multi-word label whole)
```

The overall bar is magenta; child bars are yellow. Note the right-aligned
30-char prefix and the `count/total elapsed rate` suffix — the canonical shape.

## Worked example — conductor workflow screen (`[wf]`)

Worker-slot child bars use the **same wide_bar child template** as other child
bars (`{spinner:.green} {prefix:>30.30} {wide_bar:.yellow/dim} {msg:<25.55}`),
driven by per-worker state (`total = assigned`, `pos = succeeded`). The overall
bar uses the sync template (`{spinner:.green} {prefix:>30.30}
{wide_bar:.magenta/dim} {msg:<25.55}`). Child bars render above the overall bar.

Live screen, mid-run (pool_size = 3, 12 global steps, 4 done overall), W=80:

```text
⠙             wf-1/step-5 (echo) ██████████████░░░░░░░░  2/3 running
⠙             wf-1/step-6 (import) ██████████░░░░░░░░░░░░░  1/2 running
⠙                           idle █████████████████████  1/1 idle
⠹                 workflow steps ███████░░░░░░░░░░░░░░  4/12 0s 211/s 0s
```

- Worker labels are mediapm-agnostic: `workflow_id/step_id (tool)` where
  `tool` is the conductor step's own `tool` field (e.g. `echo`, `fs`,
  `import`) — never a mediapm managed-tool name.
- Each worker bar fills the line (21-char `wide_bar` + `succeeded/assigned`
  suffix), matching the overall bar's density.
- Overall bar: magenta spinner + 21-char `wide_bar` partially `█`-filled to
  4/12, suffix `4/12 0s 211/s 0s`. Template identical to the sync
  `syncing tools` overall bar.

## Authoritative tests

These test files use `assert_eq!(term.contents(), concat!(...))` and ARE the
real format — change them only when the format intentionally changes:

- `src/mediapm-utils/tests/progress_output/*.rs` (terminal.rs, consumer.rs,
  transition.rs, progress_group.rs, spinner.rs, regression.rs, single_bar.rs,
  resolve_label.rs)
- `src/mediapm/src/output/progress.rs`
