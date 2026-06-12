# Mediapm Utils Crate

> **mediapm-utils** provides shared type aliases (`StringMap`, `BinaryInputMap`), path-resolution utilities (`PathMode`), and builtin descriptor/CLI helpers (`describe`, `BuiltinCliArgs`). Used by all `src/conductor-builtins/*/` crates.

Crate: `mediapm-utils`

## Modules

| Module | Contents | Feature gate |
|--------|----------|-------------|
| `types` | `StringMap`, `BinaryInputMap` type aliases | always |
| `path` | `PathMode` enum, `parse_path_mode`, `resolve_path_for_root` | always |
| `builtin` | `describe()`, `describe_json_compact()`, `BuiltinMeta`, `describe_meta()`, `describe_json_compact_meta()`, `validate_only_known_keys()`, `BuiltinCliArgs`, `parse_string_pairs` | `cli` feature for `BuiltinCliArgs`/`parse_string_pairs` |

## Conventions

- Keep this crate dependency-free beyond `clap` (optional). It is the lowest-common-denominator shared utility for all builtins.
- `StringMap` (`BTreeMap<String, String>`) is the canonical argument-payload type across all builtin API/CLI contracts.
- `BinaryInputMap` (`BTreeMap<String, Vec<u8>>`) is the canonical binary-payload type for content-oriented operations.
- New shared utilities should go here only if used by multiple builtins. Builtin-specific code stays in the respective builtin crate.
- Path utilities in `path` must remain cross-platform and avoid host-specific assumptions beyond POSIX/macOS/Windows norms.
