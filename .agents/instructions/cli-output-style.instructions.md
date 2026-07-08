---
description: "Use when editing CLI output, progress bars, or result formatting. Covers StatusIcon usage, print_result field conventions, progress bar construction, styling rules, and crate architecture."
name: "CLI Output Style"
applyTo: "src/**/*.rs"
---

# CLI Output Style Policy

Every user-facing line in mediapm CLI tools follows precise conventions for consistency.

## Architecture — Where types live

| Type / module | Crate | Feature gate | Available to |
|---|---|---|---|
| `mediapm_utils::progress::DownloadProgressSnapshot` | `mediapm-utils` | always | All crates |
| `mediapm_utils::progress::ProgressCallback` | `mediapm-utils` | always | All crates |
| `mediapm_utils::progress::{ProgressGroup, ProgressHandle}` | `mediapm-utils` | `progress` | Crates with indicatif |
| `mediapm_utils::progress::{set_progress_enabled, format_bytes, ...}` | `mediapm-utils` | `progress` | Crates with indicatif |
| `crate::output::report::{StatusIcon, print_result, ...}` | `mediapm` | `cli` | `mediapm` crate only |

**Rule**: The conductor *library* (`mediapm-conductor`) must not depend on indicatif. It receives progress via `Fn` callbacks (`ProgressCallback`). The conductor *CLI binary* can use indicatif via `mediapm-utils/progress`. The `mediapm` crate uses indicatif via `mediapm-utils/progress`.

## Library Stack

| Crate | Role | API surface used |
|---|---|---|
| `indicatif` 0.17 | Progress bar rendering | `ProgressBar`, `MultiProgress`, `ProgressStyle`, `HumanBytes`, `HumanCount` |
| `console` 0.15 | Terminal detection & styling | `Term::stderr().size()` for width detection, `style()` for ANSI coloring |
| `pulsebar` | **Do not use** — removed, replaced by indicatif | — |

Do not add `owo-colors`, `colored`, `termion`, or other styling crates. `console::style()` is the single styling entry point.

## Shared types (`mediapm-utils::progress`)

### Download-progress types (always available, no indicatif)

```rust
pub struct DownloadProgressSnapshot {
    pub downloaded_bytes: u64,
    pub total_bytes: Option<u64>,
}
pub type ProgressCallback = Arc<dyn Fn(DownloadProgressSnapshot) + Send + Sync>;
```

Used at the conductor library boundary. The conductor library's `run_workflow` and related APIs take callbacks; they never import `indicatif`.

## Progress Bars (`mediapm::output::progress` / `mediapm-utils::progress`)

### Core types

- `ProgressGroup` — wraps `MultiProgress`. Created via `ProgressGroup::new()` (no overall bar) or `ProgressGroup::with_overall(label, total)` (pins an aggregate bar at the bottom).
- `ProgressHandle` — wraps `ProgressBar`. Clones share state. Methods: `advance(delta)`, `set_position(pos)`, `set_message(msg)`, `set_prefix(prefix)`, `set_total(total)`, `total()`, `finish()`, `finish_success(msg)`, `finish_error(msg)`, `abandon()`.

### Construction

```rust
// Simple group, one bar:
let group = ProgressGroup::new();
let pb = group.add_bar(total, "materializing");

// Group with overall bar at bottom:
let (group, overall) = ProgressGroup::with_overall("sync", phase_count);
let pb1 = group.add_bar(sub_total, "phase 1");
let pb2 = group.add_bar(sub_total, "phase 2");
```

### Visual templates

**Wide terminal (≥ 60 cols) — child bar:**

```text
{spinner:.green} {prefix:>12.12} [{elapsed_precise}] {wide_bar:.cyan/blue} {pos}/{len} {msg} ({eta})
```

**Wide terminal — overall bar:**

```text
{prefix:>12.12} [{elapsed_precise}] {wide_bar:.green/dim} {pos}/{len} {msg}
```

**Narrow terminal (< 60 cols) — compact fallback:**

```text
{spinner:.green} {prefix} [{elapsed_precise}] {pos}/{len} {msg}
```

The active template is chosen automatically via `apply_bar_style()` which checks `terminal_width()`.

### Styling rules

| Element | Style |
|---|---|
| Spinner | green, braille dots (`⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏`) |
| Prefix | bold, right-aligned to 12 chars |
| Elapsed | cyan |
| Bar fill (child) | cyan on blue |
| Bar fill (overall) | green on dim |
| ETA | dim |
| Progress chars | `█` (fill), `░` (empty) |

### Global toggle

```rust
output::progress::set_progress_enabled(false);  // suppress all progress output
output::progress::progress_enabled();            // query current state
```

Suppressed automatically when stderr is not a TTY or when `--quiet` / `MEDIAPM_QUIET` is active.

### Formatting helpers

```rust
format_bytes(u64) -> String   // "650.23 MiB", "1.24 GiB"  — wraps indicatif::HumanBytes
format_count(u64) -> String   // "1.2M", "42"              — wraps indicatif::HumanCount
```

There is no `format_throughput` function. Use `format_bytes(value) + "/s"` inline if needed.

## Operation Results (`output::report`)

### Universal shape

Every result line follows this exact structure:

```text
{icon} {bold_op}    {k}={v}  {k}={v}    in {duration}
```

- Printed to **stdout**.
- `icon` is a styled glyph from `StatusIcon`.
- `op` is a short past-tense phrase in bold (e.g. `"tool added"`, `"media removed"`).
- `fields` are `key=value` pairs separated by **two spaces**, preceded by **four spaces** from the op.
- `duration` is appended as `in 1.23s` when provided (`Option<Duration>`). Omitted when `None`.

### StatusIcon

```rust
pub enum StatusIcon {
    Success,  // ✓  bold green — changes applied
    NoChange, // –  dim        — already up to date
    Warning,  // Δ  bold yellow — completed with degradation
    Error,    // ✗  bold red   — handled failure
}
```

Every icon has a Unicode glyph. No ASCII fallback is implemented currently.

### Print functions

All to **stdout**:

```rust
print_result(icon, op, &fields, duration);          // main result line
print_status_report(&[("key", &value), ...]);        // aligned key-value pairs, no icon
```

All to **stderr**:

```rust
print_warning(msg);    // "  Δ {msg}" — Δ in yellow
print_hint(msg);       // "  → {msg}" — → in bold cyan
print_error(msg);      // "  ✗ {msg}" — ✗ in bold red
print_heading(heading); // bold heading, dimmed ── underline
```

### Field conventions

- Keys are `snake_case`, alphabetic only.
- Well-known abbreviations are OK: `id`, `dir`, `ref`.
- Separator is `=` with no spaces.
- Numbers are bare digits (no comma separators).
- Strings are unquoted unless they contain spaces.
- Order: quantity fields first, then identifiers/names, then boolean flags.
- Pass `&value as &dyn std::fmt::Display` for the field value.
- The caller decides which fields to include. Zero-valued fields may be omitted when they aren't meaningful (e.g. `pruned=0`), but primary metrics are always shown.

### Operation name catalog

Use these exact operation name strings. They are always lowercase, past tense, and under 30 chars.

| CLI command | `op` string |
|---|---|
| `tool add` | `"tool added"` |
| `tool sync` | `"tools synced"` |
| `tool remove` | `"tool removed"` |
| `refresh-runtime` | `"runtime refreshed"` |
| `media add` | `"media added"` |
| `media remove` | `"media removed"` |
| `media invalidate` | `"media invalidated"` |
| `hierarchy add` | `"hierarchy added"` |
| `hierarchy remove` | `"hierarchy removed"` |
| `global init` | `"global dir initialized"` |
| `global tool-cache prune` | `"tool cache pruned"` |
| `global tool-cache clear` | `"tool cache cleared"` |
| `sync` (full library) | `"sync complete"` (via `print_sync_summary` legacy wrapper) |

### Warnings and hints

- Warnings always go to stderr, indented by two spaces, prefixed with `Δ` in yellow.
- Hints go to stderr, prefixed with `→` in bold cyan. Suppressed by `--quiet`.
- Warnings and hints are **never** on the same line as the result.

### Duration formatting

```rust
fn format_duration(d: Duration) -> String;
```

| Duration | Output |
|---|---|
| < 1 s | `0.01s`, `0.05s` (sub-second, 2 decimal places) |
| 1 – 9 s | `1.00s`, `9.00s` |
| 10 – 59 s | `10s`, `42s` |
| 1 – 59 m | `1m 0s`, `30m 42s` |
| ≥ 1 h | `1h 0m 0s`, `2h 15m 30s` |

### Output stream policy

| Content | Stream | Suppressible |
|---|---|---|
| Progress bars | stderr | Yes (`--quiet`, non-TTY) |
| Result line (`print_result`) | stdout | No |
| Status report (`print_status_report`) | stdout | No |
| Warnings | stderr | No |
| Hints | stderr | Yes (`--quiet`) |
| Errors | stderr | No |
| Headings | stderr | No |

## Usage pattern in `main.rs`

Every CLI command handler follows this shape:

```rust
// 1. Perform the operation.
let result = service.do_something()?;

// 2. Print the result line.
print_result(StatusIcon::Success, "op name", &[
    ("key1", &value1 as &dyn std::fmt::Display),
    ("key2", &value2),
], None);

// 3. Print warnings if any.
for w in &result.warnings {
    print_warning(w);
}

// 4. Print a hint when a follow-up action is needed.
print_hint("run 'mediapm sync' to apply changes");
```

The sync command uses the legacy `print_sync_summary(&summary)` wrapper which internally calls `print_result`.

## Materializer progress usage

The materializer (`src/mediapm/src/materializer/mod.rs`) is the primary progress bar consumer:

```rust
let group = ProgressGroup::new();
let pb = group.add_bar(flattened.len() as u64, "materializing");

// ... spawn concurrent tasks, each calling pb.advance(1) ...

pb.finish_success("materialization complete");
group.join_and_clear();
```

No overall bar is used in the materializer. The group is discarded after `join_and_clear()`.

## Adding new result output

When adding a new CLI command or operation:

1. Use `print_result` with the appropriate `StatusIcon` (usually `Success`).
2. Use the same past-tense naming convention for the op string.
3. Pass fields as a slice of `(&str, &dyn Display)` tuples.
4. Add `print_warning` loops for any non-fatal diagnostics.
5. Add `print_hint` for post-action steps the user should take.
6. Never inline warnings or hints on the result line.
