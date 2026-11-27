# Mediapm Utils Crate

Crate: `mediapm-utils`

## Modules

| Module | Contents | Feature gate |
| --- | --- | --- |
| `types` | `StringMap`, `BinaryInputMap` aliases | always |
| `path` | `PathMode`, `parse_path_mode`, `resolve_path_for_root` | always |
| `temp` | `output_dir`, `cache_dir`, `runtime_dir_for_workspace`, `is_managed_path`, `remove_dir_all_with_retry` | always |
| `builtin` | `describe()`, `BuiltinMeta`, `validate_only_known_keys()`, `BuiltinCliArgs`, `parse_string_pairs` | `cli` for latter two |
| `nickel` | `render_document_as_nickel`, `is_bare_nickel_identifier`, `render_field_name`, `render_nickel_value` | `nickel` |

Canonical temp spec: `.agents/instructions/temp-and-paths.instructions.md`.

## Conventions

Dependencies only optional `clap`, `indicatif`/`console` (`progress`). `StringMap` (`BTreeMap<String, String>`) canonical args; `BinaryInputMap` (`BTreeMap<String, Vec<u8>>`) canonical binary payload across builtin CLI/API. New shared utilities only when used by multiple builtins; builtin-specific code stays in its crate. `path` utilities cross-platform (POSIX/macOS/Windows norms only).

## Optional `progress` feature

- Enables `indicatif` + `console`, provides `ProgressGroup`, `TrackedHandle`, `format_bytes`, `format_count`; `DownloadProgressSnapshot`/`ProgressCallback` always available.
- Conductor library must not enable it (uses `Fn` callbacks); conductor CLI binary and `mediapm` enable it.
- SI prefixes 1000-based: `format_count`/`format_rate` use `k` = 1,000, `M` = 1,000,000, `G` = 1,000,000,000, not `Ki` = 1,024; byte counts/rates follow suit.
- Rendering, truncation, provider budgets: `workflow-runtime.instructions.md` ("Rendering facts"). Debug JSONL format there (`MEDIAPM_PROGRESS_DEBUG`); rendered format verified by `src/mediapm-utils/tests/progress_output/*.rs`.
