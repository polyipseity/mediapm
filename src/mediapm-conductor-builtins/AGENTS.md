# Conductor Builtins Crate Instructions

Crate-local guidance for `src/mediapm-conductor-builtins/`. Follow workspace-wide policy in `AGENTS.md` and focused instruction files in `.agents/instructions/`.

The crate provides five standard tools (`echo`, `fs`, `archive`, `import`, `export`) sharing one CLI/API contract: `BTreeMap<String, String>` args, fail-fast validation, deterministic payloads for pure tools, side effects for impure ones. Each sub-crate ships its own `AGENTS.md` for builtin-specific details.

## Conventions

- Each builtin provides a library API and an optional CLI binary.
- All builtins share the same input conventions: `BTreeMap<String, String>` args plus optional raw payload bytes for content-oriented operations.
- Fail fast on undeclared or missing input keys.

---

## Cross-Crate Context

Cross-crate data flow, shared invariants, and integration boundaries are documented in `mediapm-architecture.instructions.md`. Builtins-specific integration points:

- Conductor discovers builtins at compile time via `registered_builtin_ids()`.
- CLI: builtin binary receives `--arg KEY VALUE` pairs.
- API: builtin library receives `BTreeMap<String, String>` params plus optional binary payload.
- Pure builtins return deterministic payloads; impure signal via side effects.
- Ownership: Conductor owns tool lifecycle, input binding, output capture. Builtins own implementation, error semantics, validation.
- Contract: CLI and API inputs/outputs are identical (parity); undeclared keys are rejected immediately.

## D. Conductor-Builtins Specification

## D. Conductor-Builtins Specification

### D1. Shared Framework, CLI, API, and Validation

Each builtin follows: parameter validation → execution → output. Pure builtins produce deterministic output; impure builtins perform side effects.

- CLI: `--arg KEY VALUE` keyed pairs (all strings). Optional one default key for positional convenience, but explicit keyed form always supported.
- API: `BTreeMap<String, String>` args + optional raw payload bytes (`&[u8]`).
- Pure builtin success: deterministic bytes or `BTreeMap<String, String>`. Impure builtin success: side effects primary, no forced string-only payload.
- Validate all inputs before any side effects. Reject undeclared keys, missing required keys, and invalid combinations immediately.

### D2. Builtin Specifications (5 Builtins)

| Builtin | Kind | Purity | Key Parameters |
| --- | --- | --- | --- |
| echo | String pass-through | Pure | `message` (required), `output` |
| fs | File/directory operations | Impure | `operation` (read/write/remove/copy/move/list), `path`, `source`, `destination` |
| archive | ZIP/tar+zstd pack/unpack/repack/transform | Pure | `action` (pack/unpack/repack/transform), `path`, `filter`, `mode`, `find_N`/`replace_N` |
| import | File/directory/glob/URL → CAS | Impure | `source`, `dest` |
| export | CAS → file/directory/glob/URL | Impure | `cas_hash`, `dest` |

### D3. Testing Patterns

- Parametrized tests across all builtins for CLI/API parity.
- Edge-case tests per builtin (empty input, invalid args, boundary conditions).
- Impure builtin tests: verify side effects + cleanup on failure.
- Pure builtin tests: deterministic output assertion.

### D4. Documentation Requirements

Each builtin must document: CLI args, API signature, purity, error conditions, and at least one usage example.

---

## G. Builtins edge cases

### 3.1 Path Traversal & Symlink Loops (fs builtin)

`fs` sandbox enforcement rejects path traversal (`..`) and enforces sandbox-relative keys, but symlink loops within the sandbox are unaddressed — a recursive walk that follows a symlink to its parent loops forever.

- Reject symlinks in sandbox mode during traversal (or follow with a depth limit).
- Add test: "symlink loop → error, not infinite loop".

### 3.2 Windows Reserved Names (fs builtin)

Windows reserved filenames (CON, PRN, AUX, NUL, COM1–COM9, LPT1–LPT9) are valid on Unix but fail on Windows. With no Windows-specific path validation, a config that works on Linux fails on Windows with a cryptic error.

- Detect and reject Windows reserved names on all platforms (consistent fail-fast).
- Add test: "reserved Windows filenames rejected on all platforms".

### 3.3 Import from URL Timeout (import builtin)

`import` from URL has no configurable timeout, so a slow server hangs the workflow indefinitely.

- Add `timeout_secs` param (default 300).
- Enforce timeout at the HTTP client level.
- Add test: "slow URL → timeout error".

### 3.4 Archive Extraction Zip Bomb (archive builtin)

Archive extraction has no decompression size limit, so a tiny zip can exhaust memory or disk.

- Add `max_decompressed_size` param (default 1 GB).
- Track running decompressed bytes; abort on exceed.
- Add test: "zip bomb → aborted, not OOM".

### 3.4.1 Archive Transform Action Edge Cases (archive builtin)

The `transform` action performs regex-based replacement on zip entries, but its parameters and edge-case behavior are undocumented.

- Transforms run sequentially in numbered order — each operates on the output of the previous.
- Validate numbered key contiguity (`find_0`, `find_1`, ...) at parse time; fail fast.
- Validate `filter` uses glob syntax; `mode` is one of `text` or `binary`.
- `text` mode decodes entry bytes as UTF-8; non-UTF-8 entries in text mode error.
- `binary` mode applies regex directly on raw bytes.
- Add test: "empty zip, no-match filter, non-contiguous keys, invalid regex, binary mode, max transforms".

### 3.5 Export to Full Disk (export builtin)

`export` (materialize payload to disk) does not handle disk-full failure, so a partial file is orphaned and disk space wasted.

- Pre-flight check: verify destination has enough free space (payload size + buffer).
- Atomic write: stage to temp file, then move (not incremental write).
- Cleanup on failure: remove partial file.
- Add test: "disk full → cleanup, no orphaned files".

### 3.6 CLI vs API Parity: Argument Parsing Differences

The spec states "CLI and API inputs/outputs must be identical (parity)" but does not detail parsing differences, so CLI may work while API fails on the same input.

- Explicit parsing rules: CLI parser unquotes; API passes strings as-is.
- Add test: "same args → CLI and API produce identical output" (parametrized over all builtins).

## H. Additional Builtins Specifications

### H.2 Performance: Builtin Invocation Overhead

Builtins provide both CLI (spawned process) and library API (in-process). Conductor uses the library API for performance; CLI is for external tools or manual invocation. API invocation overhead is ~µs; CLI spawn is ~ms.

### H.3 Testing Requirements

**Path Safety and Security** — Add `tests/e2e/path_safety_and_security.rs`:

- [ ] Special characters (`:`, `*`, `?`) → rejected or escaped
- [ ] Archive symlink escape → symlinks rejected in extracted files

### H.4 Troubleshooting

#### Builtin Reports "Unknown Argument"

| Symptom | Cause | Resolution |
| --- | --- | --- |
| `Error: Unknown argument: typo_in_key` | Typo in `--arg` name | Check `--help` for valid names |
| Same error | Wrong builtin for operation | Use correct builtin (fs for files, archive for ZIP, etc.) |
| Same error | Outdated builtin version | Upgrade to version with the argument |

#### Deterministic Builtin Produces Different Output

| Symptom | Cause | Resolution |
| --- | --- | --- |
| Same input, different output | Hidden environment dependency (system time, random seed) | Verify builtin source for env dependencies |
| Same input, different output | Non-deterministic compression (ZIP timestamps) | Use `--reproducible` flag if available; verify byte-for-byte determinism |
| Same input, different output | Floating-point precision variation by platform | Pin platform or use fixed-point arithmetic |

### H.5 Implementation Checklist: New Builtin Tool

- [ ] Create `src/mediapm-conductor-builtins/<name>/` with `Cargo.toml` (package + binary target)
- [ ] Implement `lib.rs`: `async fn(BTreeMap<String, String>, Option<&[u8]>) -> Result<String, String>`
- [ ] Implement `main.rs` with CLI `--arg KEY VALUE` parsing
- [ ] Reject undeclared `--arg` keys immediately
- [ ] If pure: verify byte-for-byte determinism across multiple runs
- [ ] If impure: verify idempotent side effects (safe to retry)
- [ ] Register in `registered_builtin_ids()` in conductor
- [ ] Write integration tests (CLI + API parity)
- [ ] Document argument names, types, and examples

### H.7 Ambiguities Resolved

#### Fail-Fast Validation Scope (§7.1)

Validation errors are raised before any processing or side effects. Validation is a separate pass before execution. A validation error means nothing was started — retry is always safe. Test: "validation error → zero output, zero side effects."

#### Deterministic Payload: System State (§7.2)

Deterministic payload means byte-for-byte identical output for identical input, including file metadata (timestamps, permissions, ownership) — all metadata must be deterministic or omitted. Archive timestamps should be set to a fixed value (epoch or input mtime).
