# Conductor Builtins

Crate-local guidance for `src/mediapm-conductor-builtins/`. Workspace policy: `AGENTS.md`; focused rules: `.agents/instructions/*.instructions.md`.

Five builtins share one contract: `BTreeMap<String, String>` args + optional payload bytes, fail-fast validation before side effects, CLI/API parity. Each sub-crate has its own `AGENTS.md`.

## Contract

- CLI: `--arg KEY VALUE` (strings). API: `BTreeMap<String, String>` + optional `&[u8]`.
- Reject undeclared keys, missing required keys, invalid combinations before any side effect.
- Pure builtins return deterministic output; impure builtins return side effects and must clean up on failure.
- Conductor discovers builtins at compile time via `registered_builtin_ids()`; it owns lifecycle/binding/capture, builtins own implementation/validation/error semantics.

## Builtins

| Builtin | Kind | Key args/behavior |
| --- | --- | --- |
| `echo` | pure | `message`, `output` pass-through |
| `fs` | impure | `operation` read/write/remove/copy/move/list, `path/source/destination`; sandbox path checks |
| `archive` | pure | `action` pack/unpack/repack/transform; validate contiguous `find_N/replace_N`, `filter` glob, `mode` text/binary |
| `import` | impure | `source`, `dest`; URL/file/glob/dir into CAS |
| `export` | impure | `cas_hash`, `dest`; CAS to file/dir/glob/URL |

## Invariants and edge cases

- Path safety: reject traversal and sandbox escapes; handle symlink loops with rejection/depth limit; reject Windows reserved names on all platforms for consistent fail-fast.
- Import URL: enforce configurable timeout (default 300s) at HTTP client level.
- Archive extraction: enforce decompressed-size limit (default 1 GB) and abort on zip bombs; transform runs sequentially, validates keys/filter/mode, text mode requires UTF-8.
- Export: preflight free space, atomic temp+move write, cleanup partial file on failure/full disk.
- Parity: same args produce identical CLI/API behavior; document parsing differences explicitly.
- Determinism: pure output is byte-for-byte identical for identical input; metadata must be deterministic or omitted.
- New builtin checklist: crate/lib/main, validate undeclared args, register in `registered_builtin_ids()`, add CLI+API parity tests, document args/purity/errors/examples.

## Testing

- Parametrized CLI/API parity across builtins. Edge cases per builtin. Impure tests verify side effects and cleanup. Pure tests assert deterministic output. Path-safety/security cases cover special chars and archive symlink escape.
