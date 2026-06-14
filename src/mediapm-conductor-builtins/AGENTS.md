# Conductor Builtins Crate Instructions

> **Conductor-Builtins** provides five standard tools (`echo`, `fs`, `archive`, `import`, `export`)
> sharing a common CLI/API contract: `BTreeMap<String, String>` args, fail-fast validation,
> deterministic payloads for pure tools, clean side effects for impure ones.

This file defines crate-local guidance for `src/mediapm-conductor-builtins/`.
Follow this together with workspace-wide policy in `AGENTS.md` and focused
instruction files in `.agents/instructions/`.

## Scope

- Applies to all files under `src/mediapm-conductor-builtins/`.
- Sub-crates under this directory each ship their own `AGENTS.md` for
  builtin-specific details.

## Conventions

- Each builtin provides a library API and an optional CLI binary.
- All builtins share the same input conventions: `BTreeMap<String, String>`
  args plus optional raw payload bytes for content-oriented operations.
- Fail fast on undeclared or missing input keys.

---

## A. Cross-Crate Data Flow (Builtins Role)

```text
User Input (mediapm.ncl)
    ↓
MediaPm Configuration Parsing
    ├─→ CAS: Content-address media
    ├─→ Conductor: Synthesize workflows
    └─→ Builtins: Tool registration
    ↓
Conductor Workflow Execution
    ├─ Step 1: import (builtin) → CAS store
    ├─ Step 2: ffmpeg (managed tool) → CAS store
    ├─ Step 3: media-tagger (managed tool) → CAS store
    └─ Step N: export (builtin) → Materialized files
    ↓
CAS-Backed Materialization
    └─ Direct materialization to final output paths

Temp extraction directory (`mediapm_tmp_dir`, for zip processing only)
    └─ Extract → materialize → cleanup
    ↓
State Persistence (state.ncl)
    └─ Lock records: path → media_id, variant, hash
```

---

## B. Shared Invariants Across Crates (Builtins Rows)

| Invariant | Builtins Behavior |
|-----------|-------------------|
| Content Identity | Pure builtins (echo, archive) produce deterministic payloads |
| Constraint Correctness | N/A (read-only, no constraints) |
| Reconstructability | Output bytes persist; pure outputs are deterministic |
| Atomicity | File operations succeed or rollback (no orphaned state) |
| Determinism | Pure (echo, archive) deterministic; impure (fs, import, export) side-effect-driven |
| NCL↔Rust Schema Sync | N/A — CLI/API contracts enforced by builtin validation |

---

## C. Integration Boundaries — Conductor↔Builtins

**Entry Point**: Conductor discovers builtins at compile time via `registered_builtin_ids()` → `["import@1.0.0", "fs@1.0.0", ...]`.

**Operations**:

1. CLI invocation: Builtin binary receives `--arg KEY VALUE` pairs.
2. API invocation: Builtin library receives `BTreeMap<String, String>` params + optional binary payload.
3. Result handling: Pure builtins return deterministic payloads; impure signal via side effects.

**Ownership**:

- Conductor owns: Tool lifecycle, input binding resolution, output capture.
- Builtins own: Implementation logic, error semantics, validation rules.

**Contract**:

- CLI and API inputs/outputs must be identical (parity).
- Fail-fast validation: undeclared keys rejected immediately.
- No encoding of failures in success payloads (exit codes or `Result` errors only).

---

## D. Conductor-Builtins Specification (9 Sections)

### D1. Shared Builtin Framework

Each builtin follows a common pattern: parameter validation → execution → output. Pure builtins produce deterministic output; impure builtins perform side effects.

### D2. CLI Convention Contract

- `--arg KEY VALUE` keyed pairs (all arguments are strings).
- Optional one default key for positional convenience, but explicit keyed form always supported.
- Fail fast on undeclared keys, missing required keys, and invalid combinations.

### D3. API Input/Output Contract

- Input: `BTreeMap<String, String>` args + optional raw payload bytes (`&[u8]`).
- Output: deterministic bytes or `BTreeMap<String, String>` for pure builtins; impure builtins communicate success via side effects.
- Failures use ordinary Rust error types — never encode failures as fake success payloads.

### D4. Validation & Error Semantics

- Validate all inputs before any side effects.
- Reject undeclared keys, missing required keys, and invalid combinations immediately.
- CLI failures use Rust error types; no fake success payloads.

### D5. Success Payload Format Rules

- Pure builtin success: deterministic bytes or `BTreeMap<String, String>`.
- Impure builtin success: side-effect primary; no forced string-only payload.

### D6. Builtin Specifications (5 Builtins)

| Builtin | Kind | Purity | Key Parameters |
|---------|------|--------|----------------|
| echo | String pass-through | Pure | `message` (required), `output` |
| fs | File/directory operations | Impure | `operation` (read/write/remove/copy/move/list), `path`, `source`, `destination` |
| archive | ZIP/tar+zstd pack/unpack/repack/transform | Pure | `action` (pack/unpack/repack/transform), `path`, `filter`, `mode`, `find_N`/`replace_N` |
| import | File/directory/glob/URL → CAS | Impure | `source`, `dest` |
| export | CAS → file/directory/glob/URL | Impure | `cas_hash`, `dest` |

### D7. Testing Patterns

- Parametrized tests across all builtins for CLI/API parity.
- Edge-case tests per builtin (empty input, invalid args, boundary conditions).
- Impure builtin tests: verify side effects + cleanup on failure.
- Pure builtin tests: deterministic output assertion.

### D8. Integration Boundaries

Builtins communicate with Conductor via stdio (CLI) or direct library calls (API). No shared state. No circular dependencies.

### D9. Documentation Requirements

Each builtin must document: CLI args, API signature, purity, error conditions, and at least one usage example.

---

## E. Performance Characteristics (Builtins)

Builtins are lightweight wrappers: parameter parsing → operation → output. No async runtime overhead for synchronous builtins. Archive and import/export performance is bounded by I/O throughput and compression ratio.

---

## F. Key References

| Reference | Details |
|-----------|---------|
| Framework | CLI contract (`--arg`), API contract (`BTreeMap`) |
| 5 Tools | echo, fs, archive, import, export |
| Purity | Pure (echo, archive) vs. impure (fs, import, export) |
| Validation | Fail-fast; undeclared keys rejected immediately |

---

## G. Part 3: Builtins Edge Cases

Content below is sourced from the deleted `elaboration-pass-edge-cases.md` (Part 3: Builtins Edge Cases), now inlined here.

### 3.1 Path Traversal & Symlink Loops (fs builtin)

**Issue**: `fs` builtin sandbox enforcement must reject path traversal (`..`) and symlink loops.

**Current Spec**: Path validation rejects `..`; sandbox-relative key sandbox enforced.

**Gap**: Symlink loops within sandbox not addressed.

**Risk**: Infinite loop on recursive directory operations (walk follows symlink pointing to parent).

**Recommendations**:

- Reject symlinks in sandbox mode during traversal (or follow with depth limit).
- Add test: "symlink loop → error, not infinite loop".

### 3.2 Windows Reserved Names (fs builtin)

**Issue**: Windows reserved filenames (CON, PRN, AUX, NUL, COM1–COM9, LPT1–LPT9) are valid on Unix but cause errors on Windows.

**Current Spec**: No Windows-specific path validation.

**Gap**: Cross-platform behavior differs silently.

**Risk**: Config works on Linux, fails on Windows with cryptic error.

**Recommendations**:

- Detect and reject Windows reserved names on all platforms (consistent fail-fast).
- Add test: "reserved Windows filenames rejected on all platforms".

### 3.3 Import from URL Timeout (import builtin)

**Issue**: `import` from URL has no configurable timeout.

**Scenario**: Import from slow server hangs indefinitely.

**Current Spec**: "Import: file/folder/URL/CAS ingestion; impure".

**Gap**: No timeout parameter.

**Risk**: Workflow hangs forever; user must kill process.

**Recommendations**:

- Add `timeout_secs` param (default 300).
- Enforce timeout at HTTP client level.
- Add test: "slow URL → timeout error".

### 3.4 Archive Extraction Zip Bomb (archive builtin)

**Issue**: Archive extraction can produce decompression bombs (tiny zip → huge output).

**Current Spec**: "archive: ZIP pack/unpack/repack/transform; pure" — no limits.

**Gap**: No decompression size limit.

**Risk**: OOM or disk fill from malicious/crafted archive.

**Recommendations**:

- Add `max_decompressed_size` param (default 1 GB).
- Track running decompressed bytes; abort on exceed.
- Add test: "zip bomb → aborted, not OOM".

### 3.4.1 Archive Transform Action Edge Cases (archive builtin)

**Issue**: `archive` builtin `transform` action performs regex-based replacement on zip entries.

**Current Spec**: No parameter details for transform.

**Gap**: Transform parameters and edge-case behavior not documented.

**Recommendations**:

- Transforms run sequentially in numbered order — each operates on the output of the previous.
- Validate numbered key contiguity (`find_0`, `find_1`, ...) at parse time; fail fast.
- Validate `filter` uses glob syntax; `mode` is one of `text` or `binary`.
- `text` mode decodes entry bytes as UTF-8; non-UTF-8 entries in text mode error.
- `binary` mode applies regex directly on raw bytes.
- Add test: "empty zip, no-match filter, non-contiguous keys, invalid regex, binary mode, max transforms".

### 3.5 Export to Full Disk (export builtin)

**Issue**: `export` builtin (materialize payload to disk) does not handle disk-full failure.

**Current Spec**: "Impure: payload materialization".

**Gap**: No size check or cleanup on failure.

**Risk**: Partial file orphaned; disk space wasted.

**Recommendations**:

- Pre-flight check: verify destination has enough free space (payload size + buffer).
- Atomic write: stage to temp file, then move (not incremental write).
- Cleanup on failure: remove partial file.
- Add test: "disk full → cleanup, no orphaned files".

### 3.6 CLI vs API Parity: Argument Parsing Differences

**Issue**: Specification states "CLI and API inputs/outputs must be identical (parity)" but does not detail parsing differences.

**Current Spec**: "Fail-fast validation: undeclared keys rejected immediately".

**Gap**: No parity testing strategy.

**Risk**: CLI works, API fails (or vice versa) on same input.

**Recommendations**:

- Explicit parsing rules: CLI parser unquotes; API passes strings as-is.
- Add test: "same args → CLI and API produce identical output" (parametrized over all builtins).

## H. Additional Builtins Specifications

### H.1 Decision Rationale

#### Why Fail-Fast Validation?

Builtins run inside Conductor workflows — invalid input must be rejected before any side effects occur. Undeclared `--arg` keys, missing required keys, and invalid combinations are all rejected before I/O. This ensures predictable, auditable behavior: a validation error means nothing was started, so retry is safe.

### H.2 Performance: Builtin Invocation Overhead (§8.6)

Builtins provide both CLI (spawned process) and library API (in-process). Conductor uses library API for performance. CLI is available for external tools or manual invocation. Benchmark: API invocation overhead is ~µs; CLI spawn is ~ms.

### H.3 Testing Requirements

**Path Safety and Security** — Add `tests/e2e/path_safety_and_security.rs`:

- [ ] Symlink escape (`../../etc`) → rejected
- [ ] Symlink loop → depth limit prevents hang
- [ ] Windows reserved names (CON, PRN) → rejected
- [ ] Special characters (`:`, `*`, `?`) → rejected or escaped
- [ ] ZIP bomb (10GB from 1MB) → size limit prevents extraction
- [ ] Archive symlink escape → symlinks rejected in extracted files
- [ ] CLI vs API with same args → identical output (parametrized over all builtins)

### H.4 Troubleshooting

#### Builtin Reports "Unknown Argument"

| Symptom | Cause | Resolution |
|---|---|---|
| `Error: Unknown argument: typo_in_key` | Typo in `--arg` name | Check `--help` for valid names |
| Same error | Wrong builtin for operation | Use correct builtin (fs for files, archive for ZIP, etc.) |
| Same error | Outdated builtin version | Upgrade to version with the argument |

#### Deterministic Builtin Produces Different Output

| Symptom | Cause | Resolution |
|---|---|---|
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

### H.6 Extension Points

- **New builtin tools**: Follow the checklist above. Create crate, implement API, register in `registered_builtin_ids()`.

### H.7 Ambiguities Resolved

#### Fail-Fast Validation Scope (§7.1)

Validation errors are raised **before any processing or side effects** (interpretation (a)). Validation is a separate pass before execution. A validation error means nothing was started — retry is always safe. Test: "validation error → zero output, zero side effects."

#### Deterministic Payload: System State (§7.2)

Deterministic payload means byte-for-byte identical output for identical input. This includes file metadata (timestamps, permissions, ownership) — all metadata must be deterministic or omitted. Archive timestamps should be set to a fixed value (epoch or input mtime).
