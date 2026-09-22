# Mediapm Utils Crate

Crate: `mediapm-utils`

## Modules

| Module | Contents | Feature gate |
| --- | --- | --- |
| `types` | `StringMap`, `BinaryInputMap` type aliases | always |
| `path` | `PathMode` enum, `parse_path_mode`, `resolve_path_for_root` | always |
| `temp` | `artifact_dir`, `cache_dir`, `runtime_dir_for_workspace`, `is_managed_path`, `remove_dir_all_with_retry` | always |
| `builtin` | `describe()`, `BuiltinMeta`, `validate_only_known_keys()`, `BuiltinCliArgs`, `parse_string_pairs` | `cli` feature for `BuiltinCliArgs`/`parse_string_pairs` |
| `nickel` | `render_document_as_nickel`, `is_bare_nickel_identifier`, `render_field_name`, `render_nickel_value` | `nickel` feature |

See `.agents/instructions/temp-directory-spec.instructions.md` for the canonical temp-directory spec.

## Conventions

- Keep this crate dependency-free beyond `clap` (optional) and `indicatif`/`console` (optional, behind `progress` feature). It is the shared utility for all builtins.
- `StringMap` (`BTreeMap<String, String>`) is the canonical argument-payload type across all builtin API/CLI contracts.
- `BinaryInputMap` (`BTreeMap<String, Vec<u8>>`) is the canonical binary-payload type for content-oriented operations.
- New shared utilities should go here only if used by multiple builtins. Builtin-specific code stays in the respective builtin crate.
- Path utilities in `path` must remain cross-platform and avoid host-specific assumptions beyond POSIX/macOS/Windows norms.

### MEDIAPM_PROGRESS_DEBUG

See `progress-output.instructions.md` ("Debug JSONL") for the full JSONL debug format, field names, and monitoring instructions.

## Optional `progress` feature

- When enabled, pulls in `indicatif` + `console` and provides `ProgressGroup`, `TrackedHandle`, `format_bytes`, `format_count`.
- `DownloadProgressSnapshot` and `ProgressCallback` are always available (no feature gate).
- The conductor *library* must not depend on this feature -- it uses `Fn` callbacks instead.
- The conductor *CLI binary* and the `mediapm` crate enable this feature.
- **SI prefixes are 1000-based**: `format_count` and `format_rate` use decimal SI prefixes (`k` = 1,000, `M` = 1,000,000, `G` = 1,000,000,000), not binary (`Ki` = 1,024). Byte counts and transfer rates follow the same convention.

All progress bar rendering, templates, glyphs, colors, prefix/suffix shapes, truncation dispatch, per-screen specs, post-finish result messages, and pre-roll contract live in `progress-output.instructions.md` and `progress-budget.instructions.md`. Read those files before editing progress code. The actual rendered format is verified by `src/mediapm-utils/tests/progress_output/*.rs`.
