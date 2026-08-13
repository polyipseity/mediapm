# Mediapm Utils Crate

> **mediapm-utils** provides shared type aliases (`StringMap`, `BinaryInputMap`), path-resolution utilities (`PathMode`), and builtin descriptor/CLI helpers (`describe`, `BuiltinCliArgs`). Used by all `src/mediapm-conductor-builtins/*/` crates.

Crate: `mediapm-utils`

## Modules

| Module | Contents | Feature gate |
| --- | --- | --- |
| `types` | `StringMap`, `BinaryInputMap` type aliases | always |
| `path` | `PathMode` enum, `parse_path_mode`, `resolve_path_for_root` | always |
| `temp` | `artifact_dir`, `cache_dir`, `runtime_dir_for_workspace`, `is_managed_path`, `remove_dir_all_with_retry` | always |
| `builtin` | `describe()`, `describe_json_compact()`, `BuiltinMeta`, `describe_meta()`, `describe_json_compact_meta()`, `validate_only_known_keys()`, `BuiltinCliArgs`, `parse_string_pairs` | `cli` feature for `BuiltinCliArgs`/`parse_string_pairs` |
| `nickel` | `render_document_as_nickel`, `is_bare_nickel_identifier`, `render_field_name`, `render_nickel_value` | `nickel` feature |

## Conventions

- Keep this crate dependency-free beyond `clap` (optional) and `indicatif`/`console` (optional, behind `progress` feature). It is the lowest-common-denominator shared utility for all builtins.
- `StringMap` (`BTreeMap<String, String>`) is the canonical argument-payload type across all builtin API/CLI contracts.
- `BinaryInputMap` (`BTreeMap<String, Vec<u8>>`) is the canonical binary-payload type for content-oriented operations.
- New shared utilities should go here only if used by multiple builtins. Builtin-specific code stays in the respective builtin crate.
- Path utilities in `path` must remain cross-platform and avoid host-specific assumptions beyond POSIX/macOS/Windows norms.

### MEDIAPM_PROGRESS_DEBUG

When set, progress bar renderers emit one JSONL line per tick (every ~50ms)
with the full state of every bar slot. Values:

- `auto` (or empty) — writes to `progress-debug-<pid>.jsonl` in the current
  working directory.
- Any other value is treated as a file path to write to.

Stderr output is intentionally not supported — debug output must not compete
with terminal rendering. Monitor live with `tail -f <file>`.

The JSONL format is documented in [`ProgressDebugSink`]. Each record includes
`"type": "tick"` for self-describing, extensible event streams.

All field names use `snake_case`.

## Optional `progress` feature

- When enabled, pulls in `indicatif` + `console` and provides `ProgressGroup`, `ProgressHandle`, `format_bytes`, `format_count`.
- `DownloadProgressSnapshot` and `ProgressCallback` are **always** available (no feature gate).
- The conductor *library* must NOT depend on this feature — it uses `Fn` callbacks instead.
- The conductor *CLI binary* and the `mediapm` crate enable this feature.
- **SI prefixes are 1000-based**: `format_count` and `format_rate` use decimal SI prefixes (`k` = 1,000, `M` = 1,000,000, `G` = 1,000,000,000), not binary (`Ki` = 1,024). Byte counts and transfer rates follow the same convention.

### Pre-roll contract

`ProgressRenderer::pre_roll_if_needed()` reserves full terminal height before
the first `MultiProgress` draw, preventing intervening stderr output from
being overwritten during bar draws.

**Invariants:**

- Writes exactly `rows` newlines (terminal height from `DimensionSource`),
  then cursor-up `rows`.
- One-shot: only the first `tick()` call writes; subsequent calls are no-ops.
- In test mode (user-provided `MultiProgress` via `with_multi_progress`),
  `pre_roll_term` is `None` and pre-roll is a hard no-op.
- Production path testable via `with_pre_roll_capture()` builder method +
  same `InMemoryTerm` used for `MultiProgress`.
- `pre_roll_term` field stores `Option<Box<dyn TermLike>>`: `Some(...)` in
  production (writes to `console::Term::stderr()`), `None` or user-provided
  term in test mode.

### Test convention

> **Hard rule:** Exact-output matching is mandatory for all progress bar rendering tests.

When writing or modifying unit/integration tests for progress bar rendering
(in `src/progress.rs` `mod tests` or `tests/progress_output/`), **MUST** use
exact `assert_eq!(term.contents(), concat!(...))` matching over substring
assertions. See `.agents/instructions/rust-conventions.instructions.md`
("Terminal output matching") for the full rule and capture strategy.
