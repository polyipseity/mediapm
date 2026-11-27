# Conductor Builtins

`src/mediapm-conductor-builtins/`: workspace policy in `AGENTS.md`, focused rules in `.agents/instructions/*.instructions.md`. Five builtins share `BTreeMap<String, String>` args + optional payload bytes, fail-fast validation before side effects, CLI/API parity; each sub-crate has `AGENTS.md`.

## Contract

- CLI `--arg KEY VALUE` (strings); API `BTreeMap<String, String>` + optional `&[u8]`.
- Reject undeclared keys, missing required keys, invalid combinations before side effects.
- Pure builtins deterministic; impure builtins clean up on failure.
- Compile-time discovery via `registered_builtin_ids()`; conductor owns lifecycle/binding/capture, builtins own implementation/validation/error semantics.

| Builtin | Kind | Key args/behavior |
| --- | --- | --- |
| `echo` | pure | `message`, `output` pass-through |
| `fs` | impure | `operation` read/write/remove/copy/move/list, `path/source/destination`; sandbox checks |
| `archive` | pure | `action` pack/unpack/repack/transform; contiguous `find_N/replace_N`, `filter` glob, `mode` text/binary |
| `import` | impure | `source`, `dest`; URL/file/glob/dir into CAS |
| `export` | impure | `cas_hash`, `dest`; CAS to file/dir/glob/URL |

## Invariants

Reject traversal/escapes, symlink loops (reject/depth limit), Windows reserved names on all platforms. Import URL timeout 300s at HTTP client; archive decompressed limit 1 GB; transform sequential, validates keys/filter/mode, text mode UTF-8. Export preflights free space, atomic temp+move, cleans partial on failure/full disk. Parity: same args -> identical CLI/API, parsing differences documented; pure output byte-identical, metadata deterministic or omitted. New builtin: crate/lib/main, undeclared-arg validation, `registered_builtin_ids()`, parity tests, args/purity/errors/examples docs. Tests: parametrized parity, edge cases, impure cleanup, pure determinism, special chars, archive symlink escape.
