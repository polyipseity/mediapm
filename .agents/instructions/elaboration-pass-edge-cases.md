---
description: "Use when refining architecture guidance, risk assessment, and cross-crate edge-case specifications for mediapm."
name: "Specification Elaboration: Edge Cases & Cross-Crate Conflicts"
applyTo: "AGENTS.md, src/**/AGENTS.md, .agents/instructions/**/*.md"
---

# Specification Elaboration: Edge Cases, Ambiguities & Cross-Crate Conflicts

> **❖ Maintenance rule**: This edge-case document and
> `.agents/instructions/crate-specifications.md` must be kept in sync with
> the codebase. Any behavioral change, new invariant, or ambiguity resolution
> should be reflected here as part of the same change set.

**Date**: 2026-05-31
**Scope**: CAS, Conductor, Conductor-Builtins, MediaPM
**Status**: Issues identified for resolution in specification v2 or implementation

---

## Executive Summary

The collected specifications establish strong contracts around content identity, atomicity, and determinism. However, **42 concrete issues** spanning edge cases, failure modes, cross-crate invariant collisions, and testing gaps remain unspecified. This elaboration prioritizes them by operational risk and implementation complexity.

**Critical findings**:

1. **Delta chain corruption** (CAS) has no recovery path specified
2. **Tool ID collision** (Conductor ↔ Builtins) can silently break workflow intent
3. **Partial state persistence** (MediaPM) under failure lacks explicit rollback contract
4. **Hash algorithm agility** (all crates) unspecified; forces breaking upgrades
5. **Concurrent access patterns** across CAS/Conductor underspecified for race safety

---

## PART 1: CAS CRATE — EDGE CASES & FAILURE MODES

### 1.1 Delta Chain Corruption & Recovery

**Issue**: Specification states "adjacent-only migrations" and "O(depth) reconstruction" but does not address partial delta chain loss.

**Scenarios**:

| Scenario | Current Spec | Gap |
|----------|---|---|
| Intermediate delta base deleted during optimization | "Index repair" mentioned but not detailed | No explicit rollback strategy |
| Delta chain depth exceeds MAX_DELTA_DEPTH (32) | Optimizer avoids creating longer chains | What if old chains exceed limit after config change? |
| Corrupted delta (bytes don't apply cleanly) | Codec error raised | Does CAS fall back to full object? Automatic? |
| Orphaned deltas (no base references them) | Prune removes them | Is prune automatic on GC or manual? |
| Cyclic delta reference (A → B → A) | Not mentioned | Can this occur? How is it detected? |

**Risk**: Silent data corruption if intermediate base is manually deleted and reconstruction is attempted.

**Recommendations**:

- Add explicit **delta chain integrity check** (scan all deltas, verify bases exist)
- Document fallback: if reconstruction fails, **automatically promote to full object copy**
- Specify prune trigger: automatic (on size threshold), manual (operator invokes), or both
- Add test: "corrupted delta chain recovery" with orphaned intermediate base

**Questions for Clarification**:

1. Does `repair_index()` include delta chain validation or only index schema repair?
2. If optimizer creates delta chain exceeding MAX_DELTA_DEPTH after config downgrade, is prune automatic or requires manual intervention?
3. Is fallback-to-full-object deterministic (always succeeds) or can it fail?

---

### 1.2 Concurrent Mutation During Optimization

**Issue**: Specification states optimizer "concurrently scores candidates (8 tasks)" but does not detail interaction with concurrent puts/deletes.

**Scenarios**:

- Optimizer reads full object for candidate scoring; meanwhile `put()` writes new version
- `delete()` removes object mid-optimization
- Two optimizations run concurrently on overlapping object sets

**Current Spec**: "CAS doesn't reference Conductor types; failures propagated as-is"

**Gap**: No isolation guarantee (e.g., snapshot vs. live reads)

**Risk**: Optimizer producing invalid encoding if object mutated during scoring; stale indexes if deletes race with optimization.

**Recommendations**:

- Explicit isolation: **Optimizer takes immutable snapshot of object set at start** (or uses "version" guard)
- Document: **concurrent puts with identical content are deduplicated** (single write, multiple waiters) vs. race (last write wins)
- Add test: "concurrent optimize + put + delete" scenario

**Questions for Clarification**:

1. Are concurrent puts to same hash deduplicated or do they race?
2. Does optimizer use live index or snapshot? If snapshot, when is it taken?

---

### 1.3 Constraint Satisfaction Impossibility

**Issue**: `set_constraint(base)` validates base exists, but no check for **circular or impossible constraints**.

**Scenario**:

```text
Object A with current base = B
set_constraint(A, base=C) where C depends on A (direct or transitive)
```

**Current Spec**: "Optimizer honors constraints"

**Gap**: No cycle detection; no "constraint satisfiability" guarantee.

**Risk**: Optimizer fails at runtime when trying to resolve circular constraint; customer-visible error.

**Recommendations**:

- **Constraint graph DAG validation** on `set_constraint()`: refuse if introducing cycle
- Add explicit rule: "Constraints must form a DAG; cycles rejected at set time"
- Add test: "circular constraint detection"

**Questions for Clarification**:

1. Can deltas form constraint cycles (A→B→C→A)? If so, how are they prevented?

---

### 1.4 Hash Algorithm Agility

**Issue**: Specification mentions "Add variant to `HashAlgorithm` enum" for future algorithms, but no migration strategy for **existing persisted hashes**.

**Scenario**:

- System running with Blake3-256 (hard-coded in many places)
- Need to migrate to SHA3-256 (hypothetically)
- Existing CAS contains only Blake3 hashes
- New binary expects SHA3 by default

**Current Spec**: "No speculative forward-compatibility; only N → N+1 migrations"

**Gap**: No hash algorithm versioning layer; codec doesn't tag algorithm in hash envelope.

**Risk**: If hash algorithm is updated, old CAS becomes incompatible; forces data migration or parallel systems.

**Recommendations**:

- **Hash envelope must include algorithm discriminant** (not implicit from context)
- Add `HashAlgorithm` field to wire format (even if currently always Blake3)
- Document: "Hash algorithm upgrades require data migration (re-hash all objects)"
- Add test: "cross-algorithm hash comparison (should fail or require re-hash)"

**Questions for Clarification**:

1. Is Blake3 compile-time hardcoded or runtime-selected? If runtime, how is it stored?
2. If CAS contains mixed Blake3/SHA3 hashes in future, how are they disambiguated?

---

### 1.5 Out-of-Space Handling

**Issue**: Specification mentions "OutOfSpace (triggers prune)" but does not specify **automatic vs. manual prune invocation** or **retry semantics**.

**Scenario**:

- `put()` fails with OutOfSpace
- Prune runs (automatically? manually?)
- `put()` retried (automatically? fails again?)

**Current Spec**: "Fail-fast; no partial state"

**Gap**: Who retries after prune? User code or CAS internal?

**Risk**: Silent data loss if prune removes needed objects; no clarity on recovery path.

**Recommendations**:

- Explicit policy: **Automatic prune on OutOfSpace** (within transaction) or **return error, caller retries after external prune**
- If automatic: specify prune strategy (LRU, oldest first, cost model)
- If manual: caller responsibility to invoke `prune()` and retry `put()`
- Add test: "out-of-space + prune + retry" happy path

**Questions for Clarification**:

1. Does `put()` automatically prune and retry, or fail immediately?
2. If automatic, how much space must prune reclaim before retry?
3. Can prune remove objects that `put()` needs (race condition)?

---

### 1.6 Mmap Failure & Fallback

**Issue**: Specification states "mmap for ≥64KB; buffer pool for small" but does not address **mmap failure or unsupported file systems**.

**Scenario**:

- CAS on network file system that doesn't support mmap
- File system permissions prevent mmap
- mmap request exceeds OS limit

**Current Spec**: Performance optimization only

**Gap**: No fallback; error handling unspecified.

**Risk**: If mmap fails, entire read fails instead of gracefully degrading to buffer-based read.

**Recommendations**:

- **Fallback to buffer-pool read on mmap failure** (not hard error)
- Log warning if mmap unavailable (may impact performance)
- Add test: "mmap unavailable → fallback to buffer pool"

---

### 1.7 Index Repair Semantics

**Issue**: Specification mentions `repair_index()` returns `IndexRepairReport` but does not specify **what corruption is detected or how it's repaired**.

**Scenarios**:

- Index schema version mismatch
- Orphaned index entries (point to non-existent objects)
- Duplicate entries (same hash, different stored locations)
- Missing entries (object exists, index doesn't list it)

**Current Spec**: "Index repair on startup (optional)"

**Gap**: No definition of "repair" — is it automated or advisory?

**Risk**: Unclear when to invoke; customer doesn't know if index is healthy.

**Recommendations**:

- Document repair scope: "Detects orphaned entries, duplicate entries, version mismatches; removes orphaned, de-duplicates, auto-upgrades schema"
- Make explicit: **Repair never deletes user data** (only index/metadata)
- Add test: "index corruption scenarios → repair restores consistency"

**Questions for Clarification**:

1. Does `repair_index()` change on-disk data or only rebuild in-memory structures?
2. Is repair automatic on startup or only manual invocation?

---

### 1.8 Index State: Invalidation & Consistency

**Issue**: Index-backed existence checks introduce state that can diverge from
the storage backend if invalidation is incomplete.

**Scenarios**:

| Scenario | Risk | Mitigation |
|----------|------|------------|
| Process crash between put() and index update | False negative (acceptable) | Index rebuild on startup |
| Concurrent GC removes object while index retains entry | False positive (UNACCEPTABLE) | Synchronous index removal during GC |
| Index entry for delta object after base is pruned | True positive, partial data | Depends on delta chain — recommend only full-object entries in index |
| Manual filesystem modification (outside CAS) | Index silently wrong | Not supported — CAS owns storage |
| Index rebuild misses some entries | False negatives (acceptable) | Periodically verify index against storage (background scrub) |

**Risk**: False positives break the "correctness" guarantee and could cause
conductor to skip necessary re-materialization.

**Recommendations**:

- Enforce synchronous index update within the same CAS write transaction.
- Add a background scrub process that periodically validates index entries
against actual storage objects.
- Document that manual filesystem modification is unsupported.

---

## PART 2: CONDUCTOR CRATE — EDGE CASES & FAILURE MODES

### 2.1 External Data Retrieval Failure (put_from_uri)

**Issue**: Specification states `put_from_uri(uri) → Hash` but does not handle network/format failures.

**Scenarios**:

| Scenario | Current Spec | Gap |
|---|---|---|
| URL returns 404 | Not mentioned | Does error fail workflow or is it retryable? |
| URL returns 401 (auth required) | Not mentioned | How are credentials passed? |
| URL timeout (slow server) | Not mentioned | Timeout limit? Retry count? |
| Partial download (connection drops mid-way) | Not mentioned | Cleanup? Resume? |
| Content hash mismatch (external_data changed) | Not mentioned | Reject or use? |
| URL redirect loops | Not mentioned | Redirect limit? |

**Current Spec**: "External data stored in CAS: put_from_uri(uri) → Hash"

**Gap**: No error contract.

**Risk**: Workflow hangs or fails ambiguously if external_data fetch fails; no retry semantics.

**Recommendations**:

- Explicit error cases: `NotFound`, `Unauthorized`, `Timeout`, `CorruptContent`, `IoError`
- Timeout limit: document default (e.g., 30s per request)
- Redirect limit: document max (e.g., 5 redirects)
- Retry policy: **fail-fast or retry N times on transient errors?**
- Content hash verification: **optional or mandatory?**
- Add test: "404, timeout, partial download, hash mismatch" scenarios

**Questions for Clarification**:

1. Should `put_from_uri` verify content hash after download?
2. If content hash changes between fetches, is this detected or silently accepted?
3. Timeout limit for long downloads (e.g., 1 GB file)?

---

### 2.2 Workflow DAG Cycle Detection

**Issue**: Specification does not mention **cycle detection in workflow DAGs**.

**Scenario**:

```text
WorkflowSpec {
  steps: [
    Step { id: "A", depends_on: ["B"] },
    Step { id: "B", depends_on: ["A"] }  // Cycle: A → B → A
  ]
}
```

**Current Spec**: "Level-based topological sort"

**Gap**: No explicit cycle rejection or detection.

**Risk**: Topological sort fails or hangs on cyclic graph; customer-visible error without clear cause.

**Recommendations**:

- **Explicit cycle detection before execution**: `validate_workflow_dag()`
- Fail at planning time (not execution time) with error message listing cycle
- Add test: "simple cycle (2 nodes), complex cycle (n nodes), self-loop"

**Questions for Clarification**:

1. Does `run_workflow()` validate DAG or assume caller validated?
2. If cycle detected, what is error message content?

---

### 2.3 Missing External Data During Execution

**Issue**: Specification states Conductor uses `external_data` keyed by hash, but does not handle **hash not found in CAS**.

**Scenario**:

- Workflow references external_data with hash H
- H was provisioned into CAS in machine config
- Before execution, CAS prune removes H (user error or race)
- Workflow execution reaches step that needs H
- `cas.get(H)` → NotFound

**Current Spec**: "External data → CAS → constraint metadata preserved"

**Gap**: No validation pass before execution; failures happen mid-workflow.

**Risk**: Workflow fails mid-step; no clear indication why ("hash not found").

**Recommendations**:

- **Pre-execution validation**: Verify all external_data hashes exist in CAS before starting workflow
- Fail with clear error: "External data hash {H} not found in CAS; workflow cannot proceed"
- Add test: "missing external_data error case"

**Questions for Clarification**:

1. Should Conductor validate external_data existence at startup or per-run?
2. If validation fails, is workflow re-planned with available data?

---

### 2.4 Document Merging Conflict Resolution

**Issue**: Specification mentions "User (intent) + Machine (setup) + State" three-document pattern but does not define **conflict resolution semantics**.

**Scenario**:

- User edits `conductor.ncl` (modifies tool config, version X → Y)
- Machine has `conductor.machine.ncl` (version Y with conflicting values)
- `merge()` called to integrate changes

**Current Spec**: "clear ownership; enables tooling"

**Gap**: No merge algorithm or conflict rules.

**Risk**: Merge silently overwrites user intent or machine setup without explicit resolution.

**Recommendations**:

- Document merge rules: **User document takes precedence for intent; machine document preserved for derived state**
- Explicit conflict detection: if user and machine differ on same key, which wins?
- Add test: "user edits while machine updates → merge behavior"

**Questions for Clarification**:

1. What is the merge algorithm? Last-write-wins? Structural merge (JSON 3-way)?
2. If user and machine conflict on same config key, how is conflict resolved?

---

### 2.5 Actor Panic or Message Loss

**Issue**: Specification mentions "Actor-based orchestration" but does not address **actor panic or RPC message loss**.

**Scenario**:

- Actor handling tool execution panics (OOM, assertion failure)
- RPC message queued to actor never delivered (channel dropped)
- Actor timeout expires (message processing > 8 sec)

**Current Spec**: "Actor RPC timeout 8 sec"

**Gap**: No panic recovery, message durability, or timeout escalation.

**Risk**: Workflow hangs indefinitely or fails with unclear error if actor crashes.

**Recommendations**:

- Document panic semantics: **Actor panic → immediate workflow failure with error**
- Timeout escalation: **After timeout, mark step as failed; no automatic retry (caller decides)**
- Add test: "actor panic recovery", "RPC timeout handling"

**Questions for Clarification**:

1. If actor panics, is workflow automatically retried or failed?
2. RPC timeout (8 sec) — is this per-message or per-operation?

---

### 2.6 Version Marker Absence

**Issue**: Specification states "Top-level `version: u32` in all documents" but does not address **documents without version marker** (legacy, corruption).

**Scenario**:

- User manually edits `conductor.ncl`, deletes version line
- Load attempts to parse document
- No version field → which version assumed?

**Current Spec**: "Explicit version markers; sequential migrations"

**Gap**: No fallback for missing version.

**Risk**: Ambiguous parse; either fails or assumes wrong version.

**Recommendations**:

- **Fail-fast if version absent**: error "Version marker required; document cannot be parsed"
- Add test: "missing version marker → error"

---

### 2.7 Conductor Pulsebar Terminal-Width Contract

| Scenario | Current Spec | Gap |
|---|---|---|
| Terminal resize mid-render | Width detected per message | Not cached; width may change mid-run |
| Terminal unavailable (no TTY) | Width defaults to 80 | Acceptable fallback |
| Very narrow terminal (< 20 cols) | Step preview truncated aggressively | May show only "..." |
| Unicode characters in step IDs | Character-count based truncation | Works correctly |
| Zero-width terminal | Returns empty message | Accepted |

### 2.8 Instance GC Edge Cases

**Scenario 1 — `instance_ttl_seconds = 0`**:

| Aspect | Behavior |
|---|---|
| Cutoff computation | `now - 0 = now` → every instance with `last_used ≤ now` is removed |
| Effective result | GC runs on every persist, immediately pruning all instances with any `last_used` value |
| `last_used = None` instances | Preserved (treated as timeless) |
| Risk | Users expecting "keep nothing" may be surprised that `None` instances survive; only explicit timestamps are pruned |

**Scenario 2 — `instance_ttl_seconds = None` (default)**:

| Aspect | Behavior |
|---|---|
| GC guard | `if let Some(ttl_seconds)` short-circuits — no cutoff computed, no `gc_instances()` call |
| Effective result | Instance map is purely append-mostly; old entries remain forever |
| Migration safety | State written before this field existed (with `last_used = None` on all instances) is fully preserved |
| Risk | None; this is the backward-compatible default |

**Scenario 3 — `instance_ttl_seconds` near `u64::MAX`**:

| Aspect | Behavior |
|---|---|
| Cutoff computation | `now - u64::MAX` saturates to 0 via `saturating_sub` |
| Effective result | Cutoff is epoch 0 — all instances with `last_used.epoch_seconds >= 0` are preserved (all of them) |
| Practical effect | Identical to `None` — no GC ever fires |
| Risk | User may expect garbage collection but configured a value so large it never triggers. Document that extreme values are effectively "never GC" |

**Scenario 4 — Clock skew and cutoff timing**:

| Aspect | Behavior |
|---|---|
| Source of truth | `SystemTime::now().duration_since(UNIX_EPOCH)` — system monotonic clock, not CAS timestamps |
| Clock skew | If system clock jumps forward, cutoff moves forward, prematurely GC-ing recent instances. If clock jumps backward, cutoff moves backward, delaying GC |
| `unwrap_or_default` fallback | If `duration_since` fails (system clock before UNIX_EPOCH), defaults to zero-offset, which sets cutoff at `0 - ttl = 0` (saturated) — no GC on that persist cycle |
| Risk | Clock jumps are rare but destructive. The fallback is safe (skips aggressive GC) but may cause an unexpected bloat cycle |

**Scenario 5 — `last_used = None` instances after GC**:

| Aspect | Behavior |
|---|---|
| GC predicate | `instance.last_used.map_or(true, \|lu\| lu >= cutoff)` — the `map_or(true)` branch preserves `None` entries |
| Rationale | `None` represents "not yet tracked by GC" — either pre-GC state or freshly inserted entries that haven't been through `merge_step_result_into_state()` |
| Risk | If a code path inserts instances without setting `last_used`, those instances become immortal. Document that `gc_instances` preserves `None` as a safety net, not as intended long-term behavior |

**Scenario 6 — Large instance maps**:

| Aspect | Behavior |
|---|---|
| GC complexity | `gc_instances()` calls `BTreeMap::retain()` which visits every entry — O(n) for iteration, O(log n) per removal |
| Baseline cost | For typical workflows (tens to low thousands of instances), GC is negligible compared to CAS blob serialization |
| Risk | With millions of instances, a full GC scan before every persist could become expensive. Incremental GC or sampling-based approaches are not implemented |
| Mitigation | Reduce TTL or batch persist calls. Consider a separate GC actor for very large state if this becomes a bottleneck |

---

### 2.9 Tool Content Cache Race Conditions

**Issue**: The tool content cache lacked inter-worker locking, allowing three
race conditions when multiple workers concurrently access the same tool
content directory. These races are now prevented by per-entry `flock` advisory
locks.

#### Race Scenario 1 — ENOENT on cache path spawn

**Description**: Worker A performs a cache-hit check, finds the payload
directory present, and proceeds to spawn the tool process referencing
`payload_dir`. Concurrently, Worker B cache-misses on the same tool and
deletes the entire tool content directory as part of cache-miss preparation.
Worker A's spawn fails with `ENOENT` because `payload_dir` was removed
between the existence check and the spawn syscall.

**Root cause**: The existence check and process spawn are not atomic — a
competing writer can remove the directory in the window between them.

**How locking prevents it**: Worker A acquires a shared lock
(`try_lock_shared()`) on the `.lock` file before treating a cache hit as
authoritative. Worker B's cache-miss path requires an exclusive lock
(`lock()`), which blocks until all shared lock holders release. Worker A's
shared lock ensures `payload_dir` remains live throughout process spawn.

#### Race Scenario 2 — ENOTEMPTY on remove_dir_all

**Description**: Two workers both cache-miss on the same tool simultaneously.
Both proceed to `remove_dir_all()` + `create_dir_all()` on the same cache
entry path. One worker's `remove_dir_all()` is still in progress when the
other's `create_dir_all()` executes, or both `remove_dir_all()` calls race
on the same directory tree. The result is `ENOTEMPTY` or similar filesystem
conflicts.

**Root cause**: Uncoordinated concurrent mutation of the same cache directory.

**How locking prevents it**: Exclusive lock acquisition serializes cache-miss
workers. The first worker to acquire the exclusive lock proceeds with
extraction. Subsequent workers block on `lock()` until the first finishes,
then double-check: since the cache entry is now populated, they switch to the
shared-lock (cache-hit) path.

#### Race Scenario 3 — ENOENT on sandbox path spawn

**Description**: Worker A cache-hits, acquires access to `payload_dir`, and
spawns the tool with a sandbox that references tool content files. Worker B
cache-misses, acquires the exclusive lock, and replaces `payload_dir`
contents mid-way through Worker A's process execution. Worker A's process
encounters `ENOENT` when reading tool content files that were removed by
Worker B.

**Root cause**: No read-side lock held across the tool execution lifetime —
the cache-hit guard was not held past the spawn call.

**How locking prevents it**: `ToolCacheReadGuard` (holding a shared-lock fd)
is returned from `prepare_tool_cache()` and kept alive for the duration of
the process (direct-execution paths). This prevents cache-miss writers from
acquiring the exclusive lock and modifying `payload_dir` until all readers
have finished.

#### Residual Risk Notes

**Lock-upgrade gap closed**: The downgrade pattern (recreate `.lock` file →
acquire new shared fd → drop exclusive fd) eliminates the TOCTOU window
between exclusive release and shared acquisition. There is no moment where
the entry is unlocked.

**Cross-process safety**: Because `flock` is an OS-level advisory lock, two
conductor processes sharing the same `tools/` directory automatically get
cross-process race protection. This is relevant for container environments
or multi-instance deployments sharing a networked filesystem (if `flock` is
supported by the filesystem driver).

**Non-Unix platforms**: Locking is gated behind `cfg(unix)`. On platforms
without `flock` support, `ToolCacheReadGuard` is a no-op (always succeeds).
No cross-process race protection is available on those platforms.

---

## PART 3: CONDUCTOR-BUILTINS — EDGE CASES & FAILURE MODES

### 3.1 Path Traversal & Symlink Loops

**Issue**: Specification states "rejects traversal (`..`), absolute in relative mode" but does not address **symlink loops or symlink escapes**.

**Scenarios**:

| Scenario | Current Spec | Gap |
|---|---|---|
| Path contains `..` (e.g., `a/../b`) | Rejected in relative mode | How is rejection enforced? String parse or after resolve? |
| Symlink points outside sandbox (e.g., `/etc/passwd`) | Not mentioned | Accepted or rejected? |
| Symlink loop (A → B → A) | Not mentioned | Infinite loop during traversal? |
| Relative symlink escape (e.g., `a/../../etc`) | Not mentioned | Is it resolved before or after symlink? |

**Current Spec**: "Path safety: relative/absolute modes; rejects traversal (`..`), absolute in relative mode"

**Gap**: No symlink resolution semantics.

**Risk**: Symlink escape allows writing outside intended sandbox; security violation.

**Recommendations**:

- **Symlink resolution policy**: resolve symlinks AFTER checking path safety (not before), or **reject all symlinks in relative mode**
- Symlink loop detection: **limit symlink resolution depth (e.g., 32 levels)**
- Add test: "symlink escape (../../etc), symlink loop, symlink to absolute path"

**Questions for Clarification**:

1. Are symlinks allowed in relative mode? If so, are they resolved before or after path safety check?
2. Is there a symlink resolution depth limit?

---

### 3.2 Windows Reserved Names & Special Characters

**Issue**: Specification does not mention **Windows reserved names** (CON, PRN, AUX, etc.) or **special characters** (`:`, `*`, `?`, etc. on Windows).

**Scenario**:

- MediaPM on Windows, hierarchy specifies output file name `audio:stereo.wav` or `prn.txt`
- Export builtin attempts to create file
- Windows rejects creation (reserved name or special character)

**Current Spec**: "Materializer enforces NFD-only filenames and rejects reserved characters (`<`, `>`, `:`, `"`, `/`, `\\`, `|`, `?`, `*`)"

**Gap**: Reserved names (CON, PRN) not rejected; cross-platform compatibility unclear.

**Risk**: File materialization fails on Windows with unclear error; different behavior across platforms.

**Recommendations**:

- Extend validation: **Reject Windows reserved names** (CON, PRN, AUX, NUL, COM1-9, LPT1-9, CLOCK$)
- Add test: "reserved names → error on all platforms"
- Document: "Rejected names ensure cross-platform materialization"

**Questions for Clarification**:

1. Should reserved names be rejected on all platforms or only Windows?
2. Should builtin reject these or should MediaPM reject them at config time?

---

### 3.3 Import from URL: Timeout, Hash Mismatch, Partial Download

**Issue**: `import` builtin specification missing network error handling.

**Scenarios**:

| Scenario | Current Spec | Gap |
|---|---|---|
| HTTP 404 | Imported or error? | "Import file/folder/URL/CAS" doesn't specify behavior |
| Connection timeout | Not mentioned | Timeout limit? |
| Partial download (bytes < Content-Length) | Not mentioned | Cleanup? Retry? |
| URL redirects | Not mentioned | Follow all? Limit? |
| HTTPS cert validation | Not mentioned | Strict or permissive? |

**Current Spec**: "Impure: file/folder/URL/CAS ingestion"

**Gap**: No error contract.

**Risk**: Import fails ambiguously or hangs.

**Recommendations**:

- Document URL fetch contract: **timeout, redirect limit, cert validation strictness**
- Error cases: `NotFound` (404), `Unauthorized` (401), `Timeout`, `NetworkError`, `HashMismatch`
- Add test: "404, timeout, partial download, redirect loops"

**Questions for Clarification**:

1. How are credentials provided for authenticated URLs?
2. Is content hash verification optional or mandatory?

---

### 3.4 Archive Extraction: Zip Bomb, Symlink Escapes, Large Files

**Issue**: `archive` builtin (ZIP pack/unpack) does not specify security constraints.

**Scenarios**:

| Scenario | Current Spec | Gap |
|---|---|---|
| ZIP with 1 MB compressed → 10 GB uncompressed (bomb) | Not mentioned | Size limit? |
| ZIP with symlinks escaping sandbox | Not mentioned | Symlinks allowed? |
| ZIP with >1M files | Not mentioned | File limit? |
| ZIP with nested archives | Not mentioned | Recursion limit? |

**Current Spec**: "Archive: ZIP pack/unpack/repack; pure"

**Gap**: No security bounds.

**Risk**: Zip bomb causes disk exhaustion; symlink escape allows writing outside sandbox.

**Recommendations**:

- Extract size limit: **total uncompressed size must not exceed threshold (e.g., 100 GB)**
- Symlink policy: **reject all symlinks in extracted archives** (or disallow symlinks in ZIP)
- File count limit: **reject if >100k files**
- Nested archive limit: **do not recursively extract**
- Add test: "zip bomb (reject), symlink escape (reject), large file count"

**Questions for Clarification**:

1. What is max uncompressed size limit?
2. Are symlinks allowed in ZIPs? If so, are they sandbox-checked?

---

### 3.5 Export to Full Disk

**Issue**: `export` builtin (materialize payload to disk) does not handle **disk-full failure**.

**Scenario**:

- Export payload = 10 GB
- Disk free space = 5 GB
- Export writes 5 GB, then fails
- Partial file left on disk

**Current Spec**: "Impure: payload materialization"

**Gap**: No size check or cleanup on failure.

**Risk**: Partial file orphaned; disk space wasted.

**Recommendations**:

- **Pre-flight check**: verify destination has enough free space (payload size + buffer)
- **Atomic write**: stage to temp file, then move (not incremental write)
- **Cleanup on failure**: remove partial file
- Add test: "disk full → cleanup, no orphaned files"

**Questions for Clarification**:

1. Should export check disk space before writing?
2. Is export atomic (temp + move) or incremental?

---

### 3.6 CLI vs API Parity: Argument Parsing Differences

**Issue**: Specification states "CLI and API inputs/outputs must be identical (parity)" but does not detail **parsing differences**.

**Scenario**:

- CLI: `--arg KEY VALUE` with shell quoting (e.g., `--arg name "hello world"`)
- API: `BTreeMap { "name": "hello world" }`
- CLI parser may interpret escapes differently than API

**Current Spec**: "Fail-fast validation: undeclared keys rejected immediately"

**Gap**: No parity testing strategy.

**Risk**: CLI works, API fails (or vice versa) on same input; user confusion.

**Recommendations**:

- Explicit parsing rules: **CLI parser unquotes; API passes strings as-is**
- Add test: "same args → CLI and API produce identical output" (parametrized over all builtins)

**Questions for Clarification**:

1. How should shell escape sequences be handled in CLI args?
2. Are there differences in newline/unicode handling between CLI and API?

---

## PART 4: MEDIAPM CRATE — EDGE CASES & FAILURE MODES

### 4.1 Partial CAS Sync Failure (Mid-Way Materialization)

**Issue**: Specification states direct materialization but needs to address **partial materialization failure** under the simpler model.

**Scenario**:

- Sync materializes 100 files directly to output paths
- File 50 fails to materialize (CAS corrupted, hash mismatch)
- Files 1–49 already written to final output paths
- Files 51–100 not attempted

**Current Spec**: Direct materialization; CAS integrity trusted by default; impure workflows fail without auto-retry

**Gap**: Cleanup semantics for files already written before failure.

**Risk**: Partial sync leaves 49 files at output paths; lock file not updated; next sync may skip these files (lock records absent) but files exist on disk confusingly.

**Recommendations**:

- **Cleanup on failure**: remove all files materialized during this sync run, even if they were written before the failure
- **Lock update**: only after all files materialize successfully
- Add test: "mid-sync failure (file 50 of 100) → all materialized files cleaned up, lock unchanged"
- Pure workflows may auto-recover: warn, drop, and retry once; impure workflows fail immediately

**Questions for Clarification**:

1. If 50 of 100 files materialize, then one fails, are the 50 removed or kept?
2. Should cleanup distinguish between files written this sync vs pre-existing files at the same path?

---

### 4.2 Hierarchy Node ID Suffix Convention

**Issue**: The convention for hierarchy node `id` suffix assignment was implicit,
with examples using `.tagged` for tagged nodes and `None` for untagged. This
made the naming strategy unpredictable and the variant role unclear from the
id alone.

**Resolution**: Flip the suffix convention so tagged nodes carry no suffix
(bare media id) while untagged variants carry `.untagged`. This gives tagged
nodes natural sort priority and makes the variant role explicit.

**Convention**:

- Tagged media node id: `<media-id>` (no suffix)
- Untagged media node id: `<media-id>.untagged`
- Media folder node id: `<media-id>.media_folder`
- Sidecar/other container nodes: descriptive suffix as appropriate

**Demo examples updated**:

- `mediapm_demo.rs`: `DEMO_PLAYLIST_TARGET_HIERARCHY_ID` from
  `"demo.local.dQw4w9WgXcQ.tagged"` → `"demo.local.dQw4w9WgXcQ"`;
  added `DEMO_UNTAGGED_HIERARCHY_ID` = `"demo.local.dQw4w9WgXcQ.untagged"`
- `mediapm_demo_online.rs`: `DEMO_TAGGED_HIERARCHY_ID` from
  `"youtube.dQw4w9WgXcQ.tagged"` → `"youtube.dQw4w9WgXcQ"`

### 4.3 Media.ncl References Non-Existent Media in Hierarchy

**Issue**: Specification defines hierarchy with `media_id` but does not validate **all hierarchy `media_id` values exist in media sources**.

**Scenario**:

```text
mediapm.ncl:
  media = { "video1": {...}, "audio1": {...} }
  hierarchy = [
    { id: "h1", media_id: "video1" },
    { id: "h2", media_id: "nonexistent" }  // Doesn't exist
  ]
```

**Current Spec**: "`media_id` is optional on all kinds, but `media`/`media_folder` require one effective non-empty value"

**Gap**: No validation that hierarchy `media_id` exists in media sources.

**Risk**: Workflow synthesis fails mid-execution when it tries to resolve media; error unclear.

**Recommendations**:

- **Validation pass on config load**: verify all hierarchy `media_id` values exist in `media` dict
- Fail fast at startup with error: "Hierarchy node h2 references non-existent media 'nonexistent'"
- Add test: "invalid hierarchy media_id → error at config load time"

**Questions for Clarification**:

1. Should validation happen at config load or at sync time?
2. If media_id omitted in hierarchy, is it inherited or invalid?

---

### 4.4 Tool Provisioning Failure Mid-Download

**Issue**: Specification mentions "Tool provisioning catalog" but does not handle **partial tool download failure**.

**Scenario**:

- Tool download = 500 MB
- Downloaded 250 MB
- Network drops
- Retry or resume?

**Current Spec**: "User-level cache (downloads) vs. workspace cache (extracted binaries)"

**Gap**: No resume/retry semantics.

**Risk**: Tool provisioning hangs or fails; next sync must re-download from scratch.

**Recommendations**:

- Resume policy: **support resume if server offers Range header; otherwise re-download**
- Retry policy: **retry N times on transient error before failing**
- Cleanup: **partial download marked for retry or deleted on final failure**
- Add test: "tool download failure mid-way → resume/retry"

**Questions for Clarification**:

1. Should tool downloads use resume or re-download on failure?
2. How many retries before tool provisioning fails?

---

### 4.5 Lock File Partial Write / Corruption

**Issue**: Specification mentions "lock records for cache hits" but does not address **partial lock file writes**.

**Scenario**:

- Sync materializes 100 files successfully
- Writes lock records to state.ncl
- Write fails after 50 records (disk full, permission error)
- lock file has 50 records, state.ncl half-written

**Current Spec**: "Lock records: path → media_id, variant, hash"

**Gap**: No atomic write semantics for lock file.

**Risk**: Next sync has inconsistent lock state; may re-download files or think they're up-to-date.

**Recommendations**:

- Atomic lock write: **write to temp file, then move (like CAS)**
- Verification: **after move, re-read lock and verify all expected records present**
- Add test: "lock file partial write → detected on next startup"

**Questions for Clarification**:

1. Is lock file persisted with atomic rename or incremental write?
2. Is lock file integrity verified on load?

---

### 4.6 Platform-Independent Path Resolution Conflicts

**Issue**: Specification states "Platform-independent path resolution (normalized, slash-separated)" but does not address **case sensitivity differences**.

**Scenario**:

- MediaPM on macOS (case-insensitive): `MyVideo.mp4` and `myvideo.mp4` are same file
- MediaPM on Linux (case-sensitive): they're different files
- Same mediapm.ncl on both → different behavior

**Current Spec**: "Normalized, slash-separated"

**Gap**: No case normalization; case handling unspecified.

**Risk**: Sync works on Linux, fails on macOS with "file already exists"; or vice versa.

**Recommendations**:

- Case policy: **internally normalize to lowercase for path comparison; warn if multiple files differ only in case**
- Add test: "case sensitivity mismatch detection"
- Document: "Recommendation: keep paths lowercase for cross-platform compatibility"

**Questions for Clarification**:

1. Should paths be case-normalized or case-preserved?
2. If two files differ only in case, which takes precedence?

---

### 4.7 Read-Only File Replacement (Windows)

**Issue**: Specification states "Materialized outputs are marked read-only after commit" but does not address **re-materialization of read-only files**.

**Scenario**:

- First sync: materialize `song.mp3` as read-only
- Second sync: same media_id, same hash (cache hit, no re-download)
- Re-materialize: need to write to `song.mp3` (already read-only)
- Windows: can't delete read-only file without explicit permission change

**Current Spec**: "Read-only after sync commit"

**Gap**: No handling for replacing read-only files.

**Risk**: Re-materialization fails on Windows with "Permission Denied" error.

**Recommendations**:

- Pre-materialization cleanup: **clear read-only bit before re-materialization**
- Document: "MediaPM clears read-only bits on managed files before replacement"
- Add test: "re-materialization of existing read-only file"

**Questions for Clarification**:

1. Should mediapm clear read-only bits automatically or require manual intervention?

---

### 4.8 Media ID Stability vs Content Change

**Issue**: Specification defines lock as "path → media_id, variant, hash" but does not address **media_id reuse after content change**.

**Scenario**:

- Media entry: `video1 = { source: "old_url.mp4" }` → hash H1
- Sync materializes, lock records: `video1 → H1`
- User edits mediapm.ncl: `video1 = { source: "new_url.mp4" }` → hash H2 (different content)
- Next sync: is H1 cache still used? Or re-download H2?

**Current Spec**: "Sync can skip if hash unchanged"

**Gap**: No definition of "hash" — is it source URL hash or content hash?

**Risk**: If source URL changes, sync may still use old cached content.

**Recommendations**:

- Explicit hash semantics: **hash is content hash (post-download), not source URL hash**
- Workflow: `source_url → download → hash → check lock → if hash differs, download and commit new`
- Add test: "media source change → new download, new lock record"

**Questions for Clarification**:

1. Is lock hash the content hash or source descriptor hash?
2. If source URL changes but content is identical, is download skipped?

---

### 4.9 Concurrent Sync Operations

**Issue**: Specification does not address **two sync operations running simultaneously**.

**Scenario**:

- Sync 1 starts, materializes files 1–50
- Sync 2 starts (user triggered second sync concurrently)
- Both try to materialize to the same output paths
- Both try to write lock file

**Current Spec**: Direct materialization; CAS integrity trusted by default

**Gap**: No locking semantics for concurrent syncs.

**Risk**: Race condition; corrupted lock file; duplicate materializations; user confusion.

**Recommendations**:

- Explicit concurrency model: **single sync at a time (lock file-based)** or **concurrent syncs allowed with per-media locking**
- If file-based: **acquire lock before materialization; release after completion**
- If per-media: **document isolation semantics**
- Add test: "concurrent sync operations → serialized or isolated correctly"

**Questions for Clarification**:

1. Should mediapm support concurrent syncs or serialize them?
2. If concurrent, how are lock records merged?

---

### 4.10 Managed Tool Configuration Change

**Issue**: Specification states tool provisioning cache defaults, but does not address **cache invalidation when tool config changes**.

**Scenario**:

- Tool config: `ffmpeg_version = "5.0"`; provisioned and cached
- User updates mediapm.ncl: `ffmpeg_version = "6.0"`
- Next sync: is old cached ffmpeg-5.0 used or new ffmpeg-6.0 downloaded?

**Current Spec**: "Tool provisioning catalog"

**Gap**: No cache invalidation policy.

**Risk**: Old tool version used silently; unexpected behavior or failures.

**Recommendations**:

- Cache key includes version: **cache key = (tool_id, version, platform)**, not just (tool_id, platform)
- On config version change: **new version downloaded automatically; old version may remain in cache**
- Add test: "tool version change → new download"

**Questions for Clarification**:

1. Is tool cache key versioned or version-agnostic?
2. Should old tool versions be auto-cleaned up?

---

### 4.11 Hierarchy Path Sanitization Edge Cases

**Issue**: `sanitize_names` on hierarchy nodes introduces several edge cases around
replacement character safety, NFD interaction, and inheritance. The default value
is now `Inherit`, inheriting `Enabled` from the root seed (was `Disabled` during
initial implementation).

**Scenarios**:

| Scenario | Current Spec | Gap |
|---|---|---|
| Custom replacement maps a char to another reserved char | Not tested | Should fail validation after replacement |
| NFD normalization + replacement interaction | NFD always enforced first | Should verify NFD normalization before replacement |
| Replacement char is multi-byte Unicode | Only single char allowed | Rejected at deserialization |
| `sanitize_names` on media node | Inherited by children | Verify propagation |
| Custom map with overlapping runtime default keys | Custom wins | Verify merge order |
| `Inherit` is default, serialized as `"inherit"` | `skip_serializing_if = "is_inherit"` omits it from hierarchy output | Verify round-trip `Inherit` → omitted → deserialize → same behavior |

**Risk**: Replacement that produces another reserved character would bypass
reserved-char validation; multi-byte replacement chars create inconsistent path
encoding.

**Recommendations**:

- Add test: "replace with another reserved character → fails final validation"
- Add test: "NFD normalization runs before replacement replacement"
- Add test: "inheritance propagates `sanitize_names` to child nodes"
- Add test: "custom map overrides runtime defaults per key"

**Questions for Clarification**:

1. Should replacement chars be validated separately from reserved-char rejection?

---

### 4.12 Hierarchy Flattening with rename_files Coexistence

**Issue**: Flattening validation rejects same-path entries that declare the same
variants, but `rename_files` on `media_folder` nodes can produce distinct final
filenames even with identical variant sets, making the rejection overly broad.

**Scenario**:

```text
hierarchy = [
  {
    id: "thumbnails",
    path: "",
    kind: "media_folder",
    variants: ["thumbnails"],
    rename_files: [{ pattern: r"^.*\.([^.]*)$", replacement: "folder.$1" }],
  },
  {
    id: "thumbnails-alt",
    path: "",
    kind: "media_folder",
    variants: ["thumbnails"],
    rename_files: [{ pattern: r"^.*\.([^.]*)$", replacement: "cover.$1" }],
  },
]
```

Both entries target `thumbnails` variant at the same path, but one produces
`folder.jpg` and the other `cover.jpg` — no actual collision.

**Current Spec**: "Same path + overlapping variants → rejected"

**Gap**: No exception for `rename_files`-differentiated entries.

**Risk**: Configuration flexibility limited; thumbnails folder coexistence with
custom per-entry rename rules impossible without workaround paths.

**Resolution**:

- **Allow same-path entries with overlapping variants when `rename_files` differ**
- Validation: compare `rename_files` arrays on same-path + overlapping-variant
  entries; allow iff they differ, reject (duplicate) iff identical
- The materializer uses isolated working directories per `media_folder` entry
  (keyed by job index), so each entry's `rename_files` rules operate in their
  own working namespace, with final output filenames resolved independently
- Cross-entry deduplication uses the materialized filename (after `rename_files`
  rewrite) so same-path + same-variant entries with different `rename_files`
  produce unique final files

**Questions for Clarification**:

1. Should this exception apply to all hierarchy node kinds or only `media_folder`?
2. What happens if `rename_files`-differentiated entries produce the same final
   filename? (Materializer would overwrite; last-write-wins per working order.)

---

### 4.13 Env Template Refs for yt-dlp Companion Paths

**Issue**: Managed yt-dlp companion tool paths (ffmpeg, deno) contain resolved
absolute paths that differ per machine and per provision. Embedding these
absolute paths directly into `tool_configs.yt-dlp.input_defaults` would leak
machine-specific paths into persisted config and invalidate cache on every
re-provision.

**Scenario**:

- Sync provisions ffmpeg companion for yt-dlp, resolves to
  `/home/user/.mediapm/tools/yt-dlp-abc123/payload/linux/ffmpeg`
- If this path were embedded directly as `ffmpeg_location`, the config document
  becomes machine-dependent: committing `state.ncl` across machines would
  reference nonexistent paths
- A config diff on every sync (even with identical tool selection) would also
  cause unnecessary lock churn

**Resolution**:

- Input defaults use env template refs: `"${env.MEDIAPM_YT_DLP_FFMPEG_LOCATION}"` for
  `ffmpeg_location` and `"deno:${env.MEDIAPM_YT_DLP_JS_RUNTIMES}"` for `js_runtimes`
- Resolved absolute paths are stored in `generated_runtime_env_vars` (a
  `BTreeMap<String, String>`) and written to `<conductor_dir>/.env.generated`
  as dotenv key-value pairs — never to any `.ncl` config document
- `ensure_machine_runtime_inherits_generated_env_vars()` adds the generated
  variable names to `machine.runtime.inherited_env_vars` for the active
  platform so conductor inherits them at execution time
- The `.env.generated` file is marked `@generated` and excluded from version
  control (co-located `.gitignore` pattern)
- Generated env vars are also populated from `build_tool_env()` (tool-scoped
  non-sensitive vars) and media-tagger ffmpeg path selection
- **Invariant**: absolute paths may only leak via generated env files (`*.env`,
  `*.env.generated`). They must never appear in any other persisted
  configuration document or cached state

**Questions for Clarification**:

1. Should the `.env.generated` file support machine-scoped overrides (e.g.,
   user-specified env file that takes precedence)?
2. What happens when a companion tool is re-provisioned to a different path?
   (Generated env file is rewritten on next sync; stale path becomes inert.)

---

### 4.14 Stale `.env.generated` on Re-run (FIXED)

**Issue**: On the second `media tool sync` run, `should_set_yt_dlp_ffmpeg_location`
and `should_set_yt_dlp_js_runtimes` returned `false` because `input_defaults`
already contained the template ref string (e.g.
`${env.MEDIAPM_YT_DLP_FFMPEG_LOCATION}`). Companion path resolution was gated
behind these guards, so it was skipped — and `.env.generated` was NOT
regenerated, leaving the env var undefined.

**Scenario**:

- First `media tool sync`: guards evaluate `true`, companion paths resolve,
  template refs injected into `input_defaults`, `.env.generated` written.
- Second `media tool sync` (no tool changes): guards evaluate `false`
  (`input_defaults` already has the ref), companion paths NOT resolved,
  `.env.generated` NOT refreshed — env var is stale or missing.

**Resolution**:

- Companion path resolution is now always performed when yt-dlp is
  provisioned, regardless of guard state.
- Guards only control whether `input_defaults` receives the template ref
  string. If the ref is already present, it is simply kept as-is.
- Env var generation fires whenever companion paths resolve. This ensures
  `.env.generated` is regenerated on every sync run.
- **Invariant**: companion resolution and env var generation are independent
  of `should_set_*` guards.

### 4.15 mediapm_dir with Custom Root

**Issue**: The `MediaRuntimeStorage.mediapm_dir` field controls where
`MediaPmPaths::with_runtime_storage()` resolves `runtime_root` and all
dependent paths. If `mediapm_dir` changes between sync runs, the old runtime
directory becomes orphaned — tools, cache, and env files under the previous
root are no longer referenced.

**Scenario**:

- Initial config: `runtime.mediapm_dir = ".mediapm"` (default) →
  `runtime_root = <root>/.mediapm`. Tools provisioned, `.env.generated`
  written under this root.
- User changes config to `runtime.mediapm_dir = ".custom_dir"` →
  `runtime_root = <root>/.custom_dir`. Next sync provisions tools under the
  new path, writes `.env.generated` to the new location.
- The old `<root>/.mediapm/` directory remains on disk with stale state and
  orphaned tool payloads.

**Resolution**:

- `MediaPmPaths::with_runtime_storage()` always derives paths from the
  current `mediapm_dir`. There is no automatic migration — orphaned runtime
  directories must be cleaned up manually by the user.
- Default: `<root_dir>/.mediapm`.
- Relative paths resolve against the `mediapm.ncl` parent directory.
- All dependent paths (tools, cache, state, env files, schema export, tmp)
  are computed from `runtime_root` and follow the new root immediately on
  the next sync.
- **Invariant**: the runtime directory is ephemeral; changing `mediapm_dir`
  produces a fresh namespace without migrating prior state.

---

### 4.16 Hierarchy Preset Do-Not-Overwrite by Node ID

**Issue**: `insert_hierarchy_preset_node()` runs during hierarchy build to
insert preset media nodes. Without an id-based guard, a preset node could
overwrite a user-defined node at the same path, silently discarding the user's
configuration.

**Scenario**:

- User defines a custom hierarchy folder node with `id: "my-videos"` at path
  `"videos/concerts"`
- A preset media root also targets `"videos/concerts"` with `id: "root42"`
- Without the guard, the user's `"my-videos"` node might be displaced by the
  preset (depending on insertion order and position)
- With the guard: if the preset's id (`"root42"`) or any child id doesn't
  already exist, insertion proceeds normally; if any id already exists, the
  entire node is skipped (children are still merged into the matching existing
  node via separate merge logic)

**Resolution**:

- `insert_hierarchy_preset_node()` checks `hierarchy_contains_node_id()`
  before inserting
- The check covers both the incoming node's `id` and all child `id` values
  recursively (via the node tree)
- If any id already exists, the entire node is skipped (return early without
  insertion) — no partial insertion
- Children from the incoming node are **not** lost: a separate merge pass
  (from Task 3, commit 73f0c49) merges children into existing folder nodes
  having the same normalized path, so preset children still populate the
  target folder when the folder itself already exists

**Edge Case - Nameless Folder Duplication**:

**Issue**: When the user has manually created a container folder (no `id`, no
`media_id`) at path `"music videos"` and a preset targets that same path, the
original `insert_hierarchy_preset_node()` logic would find the matching
existing folder (same path, same `Folder` kind) and insert the preset node as
a **sibling** — creating two identical-looking container folders at the same
path.

**Scenario**:

- User creates a hierarchy with a folder at path `"music videos"`, `id: None`,
  `media_id: None`, containing one media root
- User adds a hierarchy preset targeting `"music videos"` for a new media id
- `build_hierarchy_preset_node()` generates an outer container with
  `id: None`, `media_id: None`, and the same target path
- `matching_indices` finds the existing folder (1 match)
- Without the merge guard, the new node is inserted as a sibling → duplicate

**Resolution**:

- `insert_hierarchy_preset_node()` detects: `matching_indices.len() == 1` AND
  `node.id.is_none()` AND `node.media_id.is_none()` AND
  `existing.id.is_none()` AND `existing.media_id.is_none()`
- In this case, instead of inserting the new node as a sibling, the preset's
  children are merged into the existing folder's children
- The merge respects the insertion position: `Beginning` prepends;
  `End` appends; `Sorted` inserts each child at its sort-determined position
  within the existing children list
- The guard uses `matching_indices.len() == 1` to avoid interfering with the
  sorted-order test (which uses 3 matching nameless folders and needs sibling
  insertion to maintain the sibling-group sort invariant)

**Questions for Clarification**:

1. Should the do-not-overwrite guard be case-sensitive for node ids?
2. Should there be a warning when a preset node is skipped due to id collision?

---

### 4.15 Hierarchy Preset Overwrite CLI Flag

**Issue**: `insert_hierarchy_preset_node()` has a do-not-overwrite guard that
skips insertion when the incoming node's `id` already exists. Users who
intentionally want to replace an existing hierarchy node had no way to bypass
this guard.

**Scenario**:

- User defines a custom hierarchy folder node with `id: "my-videos"`
- Later user adds a hierarchy preset that would generate a node with the same
  `id` via `mediapm hierarchy add --preset yt-dlp ...`
- Without an overwrite flag, the preset node is silently skipped — the user
  must manually edit `mediapm.ncl` to remove the existing node first

**Resolution**:

- `insert_hierarchy_preset_node()` gains an `overwrite: bool` parameter
- When `overwrite: false` (default): existing behavior — skip if any node or
  child id already exists
- When `overwrite: true`: remove existing nodes with matching ids (both
  the top-level node and all children) via `remove_hierarchy_nodes_by_id()`,
  then proceed with normal insertion
- The CLI exposes this as `mediapm hierarchy add --overwrite`
- The `add_media_hierarchy_preset_with_position()` service-layer method
  propagates the flag to `insert_hierarchy_preset_node()`
- The wrapper `add_media_hierarchy_preset()` passes `overwrite: false` for
  backward-compatible programmatic callers

**Edge Cases**:

- Removing existing nodes before insertion ensures overwrite works even when
  the existing layout differs structurally from the incoming node
- If no matching ids exist, overwrite behaves identically to normal insertion
  (the removal step is a no-op)

**Questions for Clarification**:

1. Should overwrite also warn when no existing node was found to remove?
2. Should there be a dry-run mode to preview which nodes would be removed?

---

### 4.16 Dependency-Stream: Cache-Probe Race Across Workflows

**Issue**: The dependency-stream model dispatches ready steps from multiple
workflows simultaneously (via `FuturesUnordered`). Steps started in parallel
do not see each other's in-flight cache entries, so identically-keyed outputs
may both execute instead of one caching off the other.

**Scenario**:

- Workflow A and Workflow B both reach a step that produces identical outputs
  (e.g., the same file ingested from the same source URL)
- Sequential dispatch: A executes, writes to CAS → B probes cache, finds A's
  entries, skips execution → `executed=1, cached=1`
- Dependency-stream dispatch: A and B both dispatched simultaneously → neither
  sees the other's CAS entries → both execute → `executed=2, cached=0`

**Current Spec**: "Coordinator builds per-workflow dependency graphs and
dispatches via `FuturesUnordered` across all active workflows"

**Gap**: No documentation of this dedup limitation.

**Risk**: Surprise when dedup ratios differ between sequential and parallel
dispatch; tests may assume sequential-like dedup behavior.

**Resolution (documented in crate-specifications.md)**:

- This is inherent to parallel dispatch, not a bug.
- Test expectations for `executed_instances` and `cached_instances` must be
  computed with parallel semantics: steps from different workflows at the
  same readiness level may both execute if they arrive concurrently.
- The coordinator does **not** perform cross-workflow cache-key dedup before
  dispatch; dedup happens at the per-step cache-probe level and only catches
  entries already written to CAS before the probe.
- This applies equally to all dispatch ordering: the old `StreamBatch` model
  had the same limitation, and it's unchanged in the dependency-stream model.

**Questions for Clarification**:

1. Should the coordinator perform a pre-dispatch cache-key dedup pass before
   submitting steps to workers?
2. If yes, what's the dedup key: full output hash set, tool+args identity, or
   workflow-level step identity?

---

### 4.17 CorruptWorkflowOutput Error Display Delegation

**Issue**: `CorruptWorkflowOutput(Box<CorruptWorkflowOutputContext>)` uses
`#[error(transparent)]`, which delegates the entire `Display` implementation
to the inner `CorruptWorkflowOutputContext`. The inner context's `Display`
follows the format `"workflow '{workflow_name}' step '{consumer_step_id}'
failed to read output ... due to CAS corruption: {detail}"` — it never
contains the word "impure". Code that detects impure-workflow corruption via
`.to_string().contains("impure")` silently returns false positives/negatives.

**Scenario**:

- Test expects error from impure workflow corruption to contain "impure"
- `CorruptWorkflowOutput` wraps context whose Display omits "impure"
- Assertion fails mysteriously: the error is genuinely a `CorruptWorkflowOutput`
  but the string doesn't match

**Current Spec**: "Error messages include actionable context"

**Gap**: Consumers cannot rely on string matching to detect error *variants*
when Display delegates transparently.

**Resolution**:

- Use `matches!(error, ConductorError::CorruptWorkflowOutput(_))` for variant
  detection instead of `.to_string().contains(...)`.
- The `CorruptWorkflowOutput` variant's error kind is `Corruption` (which does
  appear in Display via `{kind}`), but the `CorruptWorkflowOutput` variant
  entry point itself does not inject additional context after the inner
  context's message.
- When Display content of the inner context covers the actionable information
  (corruption detail, workflow name, step id), string matching is correct for
  the *message content* but not for *variant identification*.

**Questions for Clarification**:

1. Should `CorruptWorkflowOutput` override Display to prepend "impure workflow"
   when the inner context corresponds to an impure workflow, or should variant
   detection remain pattern-match-only?

---

### 4.18 Scheduler Diagnostics Metrics Fallback

**Issue**: The scheduler's `runtime_diagnostics()` method reports
`worker_pool_size` from the `SchedulerService` struct, but this field is only
set by `begin_level_metrics()`, which was called from `plan_level()` — the
removed legacy sequential dispatch path. The dependency-stream dispatch
bypasses `plan_level()` entirely, so `worker_pool_size` remains 0.

**Scenario**:

- Conductor is configured using the dependency-stream dispatch (the only path)
- `runtime_diagnostics()` called mid-sync
- `worker_pool_size` returns 0, making diagnostics misleading
- Downstream monitoring or test assertions on pool size fail

**Current Spec**: "Scheduler provides worker queue metrics and trace events"

**Gap**: Diagnostics incomplete for dependency-stream path.

**Resolution**:

- `runtime_diagnostics()` now computes a fallback:
  `std::cmp::max(self.worker_pool_size, self.instrumentation.worker_metrics.len())`
- The fallback is only active when `begin_level_metrics()` was never called,
  which is detected by `worker_pool_size == 0`.
- Assumption: `worker_metrics.len()` reflects the actual concurrent dispatch
  width observed during the session, which is a reasonable proxy for pool size
  when no explicit `begin_level_metrics` call was made.

**Questions for Clarification**:

None (the fallback is sufficient for the single dispatch path).

---

### 4.19 Trace Event Completeness

**Issue**: The dependency-stream dispatch emits `StepCompleted` trace events as
steps finish, but does not emit `LevelPlanned` or `StepAssigned` events (these
were removed with the legacy `plan_level()`/`execute_level()` paths). Code that
reads the trace ring buffer and expects all three event types will miss them.

**Scenario**:

- Test `diagnostics_include_worker_queue_metrics_and_trace_events` reads the
  trace ring buffer after a workflow runs
- In dependency-stream mode, the buffer contains only `StepCompleted` entries
- Loop that looks for `LevelPlanned`/`StepAssigned` finds none → variables
  remain unset → assertion failures

**Resolution**:

- Trace consumers must be dispatch-path-aware: code that expects all three
  event types (`LevelPlanned`, `StepAssigned`, `StepCompleted`) is only
  compatible with the removed legacy sequential dispatch.
- Dependency-stream consumers should expect only `StepCompleted`.
- The trace ring buffer is append-only and shared, so replaying old sessions
  against new consumers may still contain legacy events.

**Questions for Clarification**:

1. Should the dependency-stream path emit synthetic `LevelPlanned` and
   `StepAssigned` events at logical equivalent points (e.g., when the
   dependency graph is built and when each step is dispatched to a worker)
   for trace compatibility?

---

### 4.20 assigned_steps_total Tracking Gap

**Issue**: In the removed legacy sequential dispatch path, steps were assigned
to workers via `assign_step_to_worker()`, which increments `assigned_steps_total`
on the worker metric. The dependency-stream dispatch does not call
`assign_step_to_worker()`; instead, steps are dispatched directly via the
round-robin `workers[next_worker]` assignment. Consequently,
`assigned_steps_total` remains 0 unless explicitly incremented elsewhere.

**Resolution**:

- `record_completion()` is called for every completed step and includes
  `metric.assigned_steps_total = metric.assigned_steps_total.saturating_add(1)`.
- This is a heuristic: `record_completion` is called for each step as it
  finishes, so each completed step retroactively increments the assignment
  counter. In-flight steps that are still running are not counted until they
  complete.
- For accurate in-flight accounting, the dispatch path would need to increment
  at dispatch time, which is a future enhancement.

**Questions for Clarification**:

1. Should `dispatch_step_rpc_with_fallback()` increment `assigned_steps_total`
   before execution (for accurate in-flight metrics), even though the round-robin
   assignment doesn't go through the scheduler's worker assignment logic?

---

### 4.21 Empty Directory Cleanup After Stale Hierarchy Removal

**Issue**: After removing stale materialized paths during hierarchy sync,
orphaned empty parent directories accumulate in the hierarchy tree. These
directories serve no purpose and clutter the output.

**Scenario**:

- Stale path `concerts/2024/video.mp4` is removed
- Parent `concerts/2024/` now contains no files
- Grandparent `concerts/` contains only `concerts/2024/` (empty)
- Without cleanup, `concerts/2024/` and `concerts/` remain as empty stubs

**Resolution**:

- After stale path removal, the materializer walks up from each removed path's
  parent directory toward `hierarchy_root_dir`
- At each level, `read_dir` checks for emptiness: if the directory contains no
  entries, it is removed and the walk continues upward
- If the directory contains any entry (file or subdirectory), the walk stops
  at that level (no upward removal beyond non-empty ancestors)
- The walk stops unconditionally at `hierarchy_root_dir` (never removes the
  root itself)
- Already-checked parents are tracked in a `BTreeSet` to avoid redundant
  filesystem checks when multiple stale paths share ancestors
- The count of removed empty directories is reported in
  `MaterializeReport.removed_empty_dirs` → `SyncSummary.removed_empty_dirs`
  and logged at CLI level.

**Questions for Clarification**:

1. Should the cleanup also handle hidden files (`.DS_Store`, `Thumbs.db`) as
   non-empty, or should it treat them as empty? (Currently any entry = non-empty.)
2. Should there be a configurable depth limit for the upward walk?

---

### 4.22 Tool Identity Preservation During Workflow Re-Synthesis

**Issue**: `preserve_existing_generated_step_tools()` rewrites generated step
tool ids from the existing workflow snapshot to maintain stable tool identities
across sync cycles. Without the right logic, unchanged steps with impure
timestamps (which always differ between runs) or steps with companion selector
changes could flip to freshly-generated tool ids on every sync, churning
machine config and regenerate downstream materialization.

**Key scenarios**:

1. **Unchanged step with stable tool identity** (e.g. ffmpeg): the step config
   (input hashes, options) is unchanged and the generated tool id is identical
   to the previous one (tool identity fields like companion selectors and
   versions are unchanged). Resolution: when `previous.tool == generated.tool`,
   the id is kept after validity check; the impure timestamp from the previous
   cycle is preserved (since `requires_refresh` is false), preventing
   unnecessary machine config churn.

2. **Step with changed tool identity** (any tool): the generated tool identity
   differs from the previous one — companion selectors, dependency versions,
   or provisioning metadata changed. Resolution: when `previous.tool !=
   generated.tool`, the function first checks whether the previous tool is
   still valid (exists in `machine.tools` with required `content_map` for
   `Executable` kinds). If valid, `generated.tool` is rewritten to
   `previous.tool.clone()`, preserving the old tool id and keeping impure
   timestamps stable. Only when the previous tool is no longer valid (pruned,
   missing `content_map`) is the step marked as unmatched, triggering a
   refresh cascade that installs the newly-generated identity.

3. **Stale previous tool**: if the previous tool id no longer exists in
   `machine.tools`, or is an `Executable` kind whose `machine.tool_configs`
   entry lacks a `content_map` (meaning the binary payload has been cleaned
   up), the step is unmatched regardless of kind. This prevents reference to
   dangling tool definitions.

**Edge case — tool validity for builtins**: `preserved_step_tool_is_valid()`
always considers builtin tools valid without checking `content_map`, since
builtins have no materialized payload to clean up.

**Edge case — same step id, different generated tool id**: when the step
itself is unchanged (same `generated.id`) but the generated `tool` identity
string differs, the function checks whether the previous tool id is still
valid. If valid, the old tool id is preserved — the generated identity is not
installed until the old tool becomes invalid (e.g., pruned, provisioned with
new version). This prevents unnecessary cache invalidation from tool version
updates or volatile identity fields. Only when the old tool id is no longer
valid does the mismatch path fire, installing the newly-generated identity.

**Edge case — tool version update with valid previous tool**: when a managed
tool version changes (e.g., ffmpeg 6.0 → 7.0), the generated tool identity
differs from the previous one. Since the previous tool is still valid (its
`content_map` still exists in `machine.tool_configs` and the binary is
materialized), the old tool id IS preserved. The conductor resolves to the old
(cached) binary. MediaPM's impure timestamp remains unchanged — tool version
updates do NOT cause a refresh (they never trigger `requires_refresh = true`).
The new version binary is provisioned alongside the old one; a later sync
switches to it when the old tool id is pruned. Note that `requires_refresh` is
only set true when `matched_state_requires_refresh()` detects a missing
timestamp (fresh step) or when `preserve_existing_generated_step_tools()`
fails to map every generated step to a still-valid prior tool.

**Test coverage**:

- `unchanged_step_config_uses_generated_tool_identity_when_changed` — ffmpeg
  step, expects preserved tool id when previous tool is still valid
- `unchanged_yt_dlp_step_config_refreshes_tool_identity_when_companion_suffix_changes`
  — step with companion selectors, expects preserved tool id when previous
  tool is still valid
- `forward_scan_matching_refreshes_later_step_tool_identity_when_needed` — both
  steps expect preserved tool id when previous tools are valid; step 1
  expects refreshed timestamp only when previous tool is invalid
- `missing_step_timestamp_forces_refresh_to_active_tool` — ffmpeg step without
  timestamp, expects new_tool
- `tool_version_update_preserves_old_tool_id` — tool version change, expects
  preserved tool id when previous tool's content_map still exists

---

### 4.23 Dependency Selector Inheritance Validation

**Issue**: `ensure_inherit_dependency_target_is_configured()` enforces that
`inherit` or `global` selectors on tool dependencies require the target tool
to be defined in the machine/user config. Without this check, a step expecting
ffmpeg from `tools.yt-dlp.dependencies.ffmpeg_version = "inherit"` silently
gets no companion binary if `tools.ffmpeg` is absent.

**Scenario**:

- User configures `tools.yt-dlp = { dependencies = { ffmpeg_version = "inherit" } }`
- User does NOT configure `tools.ffmpeg`
- `dependencies.ffmpeg_version` resolves to `"inherit"`, meaning "use whatever
  the global/default ffmpeg tool specifies"
- Without the guard, `resolve_companion_dependency_selection()` gets no ffmpeg
  definition and silently produces an empty resolution
- With the guard, validation fails early with a diagnostic pointing at the
  missing `tools.ffmpeg` key

**Resolution**:

- During document load, after tool config parsing, the validator iterates all
  configured tools' dependency selectors
- For each `inherit`/`global` selector, it checks: does
  `machine.tools.<dependency_name>` exist? (The dependency tool's name is
  inferred from the selector key: `ffmpeg_version` → `ffmpeg`,
  `deno_version` → `deno`, `sd_version` → `sd`)
- If the target tool is missing, a `ValidationError` is emitted with the
  missing tool name
- Only `rsgain`, `yt-dlp`, and `media-tagger` may define dependency selectors;
  other tools with `dependencies.*_version` selectors are rejected

**Edge case — no validation when selector is a concrete version/tag**: when the
dependency selector is `{ version = "7.1" }` or `{ tag = "latest" }`, no
validation is needed because the resolution uses built-in defaults, not a
cross-tool reference. The guard only triggers for `"inherit"` or `"global"`
string values.

**Edge case — tool with dependency selectors but missing `tools.ffmpeg`**:
`ffmpeg_version = "inherit"` requires `tools.ffmpeg` to exist in the
machine/user config. If only `tool_configs.yt-dlp` exists but `tools.ffmpeg`
does not, validation fails. This is correct: `tools.ffmpeg` must be explicitly
declared (even as a minimal stub) to participate in dependency resolution.

---

#### §4.24 Worker-based progress display

| Property | Value |
|---|---|
| **Crates** | `mediapm-conductor`, `mediapm` |
| **Files** | `src/conductor/src/api.rs`, `src/conductor/src/orchestration/coordinator.rs`, `src/mediapm/src/service.rs` |
| **Risk** | Channel-based progress events use an unbounded channel (`mpsc::unbounded_channel`). A slow receiver could cause unbounded memory growth. The receiver task lifecycle must be carefully managed to avoid dropped events or zombie tasks. |
| **Pre-fix (in-executor pulsebar bars)** | Pulsebar `MultiProgress` was rendered directly inside `coordinator.rs`'s `execute_workflows()` function using an `overall_bar` plus per-worker bars. The coordinator imported `pulsebar` and managed all progress display inline. No progress events were emitted — the caller had no visibility into step completion. |
| **Post-fix (channel-based progress events)** | Conductor no longer renders progress bars internally. New API types (`WorkflowStepEvent`, `WorkflowProgressSender`) and `RunWorkflowOptions.progress_sender` field let callers opt into event delivery. The coordinator emits one `WorkflowStepEvent` after each step completion via the channel. Each event carries `worker_index` and `worker_count` identifying which worker executed the step. Completed steps are tracked via a local counter (`completed_steps += 1`) rather than re-computed from dependency state lengths. The consumer (mediapm `service.rs`) creates the channel, a `MultiProgress`, and spawns a tokio receiver task that renders one overall bar with format `"{msg}  [{bar:20}]  {pos}/{total}"` plus text-only worker lines (`mp.add_bar(0).with_format("{msg}")`). The overall bar's per-event message uses the aggregate format `"completed {completed_steps}/{total_steps} steps"`. Worker lines show the current step and per-worker count: `"worker {wi}: {workflow}: {step}  ({count})"`. On channel close, the overall bar shows `"all workflows complete"` and each worker line shows `"worker {wi}: done  ({count})"`, followed by a 75 ms settle delay. Pulsebar removed from coordinator entirely. |
| **Interaction risk** | The unbounded channel could grow indefinitely if the receiver is slower than the step dispatch rate, though in practice steps are I/O-bound so this is unlikely. If the receiver panics or is dropped, events are silently dropped (unbounded sender never blocks). The `progress_sender.is_some()` field in `PartialEq` for `RunWorkflowOptions` means two options with different `progress_sender` values (Some vs None) are treated as unequal; this only affects caching, which relies on `PartialEq`. Per-worker `Vec<usize>` is indexed by `worker_index` — must stay in bounds (guaranteed by `worker_count` set on first event). Worker bars use `total=0` (pulsebar renders `fraction()` as 1.0 at `total=0`, no crash). |
| **Mitigation** | No settle delay in conductor (events are fire-and-forget). 75ms settle delay in the mediapm consumer so bars display the completion message briefly before the `MultiProgress` is dropped. |

---

#### §4.25 Hierarchy sync progress display

| Property | Value |
|---|---|
| **Crates** | `mediapm` |
| **Files** | `src/mediapm/src/materializer/mod.rs` |
| **Risk** | Same as §4.24 — worker bars used `finish_success` which triggered pulsebar's render-time-clock elapsed display. |
| **Pre-fix** | Worker bars called `worker_bar.finish_success(format!("worker#{n}: done"))`, hierarchy bar called `hierarchy_progress.finish_success("done")`. Both produced finished rows with ticking elapsed from the render clock. |
| **Post-fix** | Worker bars: `worker_bar.set_position(100)` + `worker_bar.set_message(...)`. Hierarchy bar: `hierarchy_progress.set_message("done")` (position already at total via `advance(1)` per entry). `{elapsed}` removed from both format strings. |
| **Interaction risk** | Workers show no duration during or after execution. For long-running workers this loses feedback about how long they've been running. Same trade-off rationale as §4.24. |
| **Mitigation** | A 75 ms settle delay mirrors the conductor pattern. |

---

#### §4.25a Pruned status filter in stale-entry detection

| Property | Value |
|---|---|
| **Crates** | `mediapm` |
| **Files** | `src/mediapm/src/conductor_bridge/sync/lifecycle.rs` |
| **Risk** | Pruned entries produce repeated "stale entry" warnings every sync cycle, confusing users and cluttering logs. |
| **Pre-fix** | `compute_stale_entry_report` scanned all tool registry records including pruned ones, reporting them as stale every sync. |
| **Post-fix** | Added `if record.status == ToolRegistryStatus::Pruned { return None; }` filter before per-record stale check. Pruned IDs are silently excluded from the sync report. |
| **Interaction risk** | If a pruned tool reappears later (e.g. manual registry edit), its entry would have no sync record and might not auto-recover. This is acceptable since the operator can re-sync. |
| **Mitigation** | None needed — this is a best-effort performance filter, not a security boundary. |

---

### 4.26 Local Media ID from CAS Hash

**Issue**: Local media IDs were generated using nanoid (random 8-char
alphanumeric suffix), making them non-deterministic — the same file added
twice would get different media IDs.

**Scenario**:

- User runs `mediapm media add --preset local ./song.mp3` twice
- First run: media ID `local.aB3xK9mZ`
- Second run: media ID `local.Q7rT2pLx` (different, even though same file)
- This made cache-key dedup impossible for local sources and caused
  unnecessary workflow churn

**Resolution**:

- `media_id_from_local_path()` now takes `&mediapm_cas::Hash` instead of
  `&Path` and produces `local.<first-12-hex-chars-of-Blake3-hash>`
- The hash is computed from the file contents before media-id assignment,
  so the same file always produces the same media ID
- The `rand` dependency and `NANOID_ALPHABET` constant were removed from
  `lib.rs`
- The nanoid-based approach is fully replaced; no backward compatibility is
  provided since all local IDs were ephemeral anyway

**Questions for Clarification**:

1. Should the hash prefix length be configurable?
2. Should there be a migration path for existing nanoid-based local IDs?

---

### 4.27 Media Source Registration Do-Not-Overwrite

**Issue**: Media source registration (`add_media_source()` and
`add_local_source()`) unconditionally inserted entries into `document.media`
via `BTreeMap::insert()`, silently overwriting existing entries with the same
`media_id`.

**Scenario**:

- User registers media source id `video1` with yt-dlp preset
- Later, user accidentally registers a different source with id `video1`
- The old entry is silently replaced — no warning, no error

**Resolution**:

- Both `add_media_source_with_position()` and
  `add_local_source_with_position()` gain an `overwrite: bool` parameter
- When `overwrite: false` (default): check whether `media_id` already exists
  in `document.media`; if yes, return successfully without modifying the
  entry (do-not-overwrite)
- When `overwrite: true`: replace unconditionally via `BTreeMap::insert()`
- The convenience wrappers `add_media_source()` and `add_local_source()`
  default to `overwrite: false`
- The CLI exposes this as `mediapm media add --overwrite`

**Edge Cases**:

- Do-not-overwrite guard runs early, before any file I/O or hashing, so no
  work is wasted when the entry already exists
- The guard checks the effective `media_id` (already resolved from path/CAS
  hash), not the raw CLI input
- Overwrite mode does not remove existing hierarchy nodes for the media id;
  hierarchy removal is handled separately by `mediapm hierarchy remove` and
  the `--overwrite` flag on hierarchy add

**Questions for Clarification**:

1. Should overwrite also remove associated hierarchy nodes for the media id?
2. Should there be a warning when an existing entry is skipped (non-overwrite)
   or replaced (overwrite)?

---

### 4.28 SERIAL_GUARD Removal and Temp Directory Strategy

**Issue**: Materializer tests previously used a global `OnceLock<Mutex<()>>` (`SERIAL_GUARD`)
to serialize access to the system temp directory, preventing concurrent test
processes from colliding on shared temp paths. This global lock did not scale to
parallel test execution and leaked abstraction (test infrastructure concern
visible in production code).

**Scenario**:

- Two concurrent `cargo test` processes run materializer tests
- Previously, both tried to create temp directories under the system temp dir
- With the old staging approach, tests raced on temp dir entries
- `SERIAL_GUARD` serialized all materializer tests globally at the cost of
  sequentializing test execution

**Resolution**:

- `SERIAL_GUARD` (`OnceLock<Mutex<()>>`) removed entirely
- Direct materialization replaces the old staging model; temp directories are
  used only for zip extraction and sandbox isolation — not for materialization
  staging
- Per-workspace temp dirs use
  `std::env::temp_dir().join(format!("mediapm-{:016x}", hash(root_dir)))`
  where `hash` is `std::hash::DefaultHasher` applied to the workspace root
  path
- Each workspace gets its own subdirectory under the OS temp dir. Concurrent
  test processes on different workspaces never collide because they use
  different temp dir paths. Tests use unique `tempfile::tempdir()` roots,
  so concurrent tests on the same workspace also get distinct temp dirs
- Temp directories are scoped to a single sync operation; cleaned up on success
  or failure
- Conductor crate follows the same pattern: `ResolvedRuntimeStoragePaths.conductor_tmp_dir`
  computes `<os-temp>/mediapm-conductor-<conductor-dir-hash>` using `DefaultHasher` over
  `conductor_dir`. This path is threaded through `StepExecutionRequest` →
  `StepWorkerExecutor` for sandbox `run-` directories, ZIP extraction workspaces
  (`step-output-zip-`, `zip-entry-`), and regex capture working directories

**Cross-References**:

- `src/mediapm/src/paths.rs`: `default_runtime_tmp_dir()` derives OS temp
  subdirectory from workspace root hash
- `src/mediapm/src/materializer/mod.rs`: temp directory handling under
  `mediapm_tmp_dir`
- `.agents/instructions/crate-specifications.md`: Atomicity Contract table,
  MediaPM row

### 4.29 Media Metadata Resolution Edge Cases (Online & Local)

**Issue**: The metadata resolution system has 5 independent persisted slots
(`MediaSourceSpec.title`, `MediaSourceSpec.artist`, `MediaSourceSpec.description`,
`metadata["title"]`, `metadata["artist"]`) with independent fallback chains.
Several edge cases arise from the decoupling of these slots and the removal of
MBID-based metadata override.

**Scenarios**:

| Scenario | Current Behavior | Potential Issue |
|---|---|---|
| `--title` provided but no `--artist` | `MediaSourceSpec.title` = CLI value; `MediaSourceSpec.artist` = fallback chain (yt-dlp/ffprobe → `"unknown"`) | Description auto-build uses resolved `MediaSourceSpec.artist` which may be `"unknown"` — intentional, not a gap |
| URL has no path segment title (e.g. `https://example.com/`) | `MediaSourceSpec.title` falls through to `"unknown"` | `metadata["title"]` chain also ends at `"unknown"` — consistent |
| Local file has no artist tag and no album_artist tag | `MediaSourceSpec.artist` = `"unknown"` | `metadata["artist"]` ends at `"unknown"` — consistent |
| `--description` CLI flag provided | Both remote and local flows use it directly, bypassing auto-build | Auto-build never runs; description may be inconsistent with resolved title/artist — caller's responsibility |
| MBID is provided alongside `--title`/`--artist`/`--description` | MBID goes to media-tagger step options only; CLI flags take precedence for metadata slots | No conflict by design — MBID and CLI flags are independent |
| `--artist` provided but yt-dlp/ffprobe also has an artist value | CLI flag wins for both `MediaSourceSpec.artist` and `metadata["artist"]` (prepended as literal) | `metadata["artist"]` chain has both CLI literal and variant binding; CLI value appears first in the candidate list |
| yt-dlp not configured for remote source | Warning emitted; all `ResolvedOnlineSourceMetadata` fields are `None`; `MediaSourceSpec.title`/`.artist` fall to `"unknown"` | Warning informs user; behavior matches local-file-without-probe fallback |
| ffprobe not available for local source | `LocalSourceMetadata` all `None`; `MediaSourceSpec.title` falls to `local_default_title()` then `"unknown"`; `MediaSourceSpec.artist` falls to `"unknown"` | Graceful degradation consistent with remote flow |
| Same file imported twice with different `--title` | Two media entries with different titles but same CAS hash | Expected — metadata is per-registration, not per-content |
| Description auto-build references resolved `MediaSourceSpec.artist` which is `"unknown"` | Auto-build produces e.g. `"title: My Video\nartist: unknown"` | Acceptable — `"unknown"` is the defined final fallback for all slots |

**Resolution**:

- All 5 slots resolve independently; no slot's resolution depends on MBID data
- `metadata["title"]` and `MediaSourceSpec.title` are intentionally decoupled
  (separate chains starting from `--title` but with different secondary sources)
- `metadata["artist"]` and `MediaSourceSpec.artist` follow the same decoupled pattern
- The auto-built description is a best-effort composite of the top-level slots;
  users who require precise description formatting should use `--description`
- No transient/ephemeral values: every slot is persisted in `MediaSourceSpec`;
  the old separate "artist literal" that was only used for description auto-build
  has been replaced by the persisted `MediaSourceSpec.artist` field

**Cross-references**:

- `src/mediapm/src/service.rs`: `add_media_source_with_position()` and
  `add_local_source_with_position()` — resolution chain implementations
- `src/mediapm/src/source_metadata.rs`: `ResolvedOnlineSourceMetadata`,
  `LocalSourceMetadata`, `resolve_online_source_metadata_for_add()`,
  `parse_local_source_metadata_from_ffprobe_json()`
- `src/mediapm/src/lib.rs`: `build_local_default_description()`,
  `local_default_title()`
- `src/mediapm/src/main.rs`: `MediaAddArgs` — `--title`, `--artist`, `--description`
- `.agents/instructions/crate-specifications.md`: Media Metadata Resolution Policy — detailed chain table

---

## PART 5: CROSS-CRATE CONFLICTS & INTEGRATION GAPS

### 5.1 CAS Versioning vs Conductor Document Versioning Coordination

**Issue**: CAS wire format has embedded version; Conductor documents have top-level `version: u32`. **No coordination between them.**

**Scenario**:

- CAS codec v2 released (incompatible with v1)
- Conductor persists state blob to CAS (using new codec v2)
- Old conductor binary (expects v1 codec) loads state
- Codec version mismatch; state unmarshaling fails

**Current Spec**: "CAS codec versions independent; Conductor document versions independent"

**Gap**: No coordinated versioning strategy; no version negotiation between layers.

**Risk**: Deployment of new CAS forces Conductor upgrade; or old Conductor can't read new state.

**Recommendations**:

- **CAS codec version in state blob** must match Conductor state version expectations
- Document version coordination rule: **Conductor v2 → requires CAS codec v2; vice versa**
- Add compatibility matrix: "Conductor v1-2, v2-3, etc.; CAS codec v1-2; compatibility pairs"
- Add test: "version mismatch detection and error"

**Questions for Clarification**:

1. If CAS codec v2 is incompatible with v1, how does Conductor detect/handle it?
2. Should version coordination be explicit (encoded in state blob) or implicit (same version numbers)?

---

### 5.2 Builtin Failure Semantics vs Conductor Error Recovery

**Issue**: Builtins fail-fast on validation; Conductor has error recovery. **Unclear how retry works.**

**Scenario**:

- Builtin validates invalid arg, returns error (exit code 1)
- Conductor captures error
- Does Conductor retry the same step? Re-plan? Fail immediately?

**Current Spec**: "Builtins fail-fast; Conductor error recovery; pure workflows one-shot retry"

**Gap**: No explicit retry contract; who retries what?

**Risk**: Unclear error recovery; customer doesn't know if transient error will be retried.

**Recommendations**:

- Explicit retry policy per error type:
  - **Validation errors (invalid arg)**: no retry (customer error)
  - **Transient errors (timeout, network)**: retry N times (configurable)
  - **Persistent errors (command not found)**: no retry
- Document in Conductor spec: "Retry semantics per error type"
- Add test: "builtin error → conductor retry behavior"

**Questions for Clarification**:

1. Does Conductor distinguish validation errors from transient errors?
2. What's the retry limit for transient builtin failures?

---

### 5.3 MediaPM Lock vs CAS Constraint: Consistency Under Deletion

**Issue**: MediaPM lock records CAS hashes; CAS constraints may be modified. **No coordinated invalidation.**

**Scenario**:

- Lock records: `song.mp3 → hash H1`
- CAS prune deletes H1 (user error)
- MediaPM next sync: checks lock, sees H1
- Re-materialization: `cas.get(H1)` → NotFound
- Sync fails; unclear why

**Current Spec**: "Lock records deterministic; CAS prune removes orphaned"

**Gap**: No consistency check; prune doesn't validate lock references.

**Risk**: CAS prune can silently break MediaPM locks; user confusion.

**Recommendations**:

- **Pre-prune validation**: Conductor/MediaPM provides list of "reachable" hashes; prune only removes unreachable
- Or **lock file invalidation**: if lock references deleted hash, mark lock invalid on next startup
- Add test: "CAS prune removes hash referenced in lock → error or re-download"

**Questions for Clarification**:

1. Should prune validate that hashes aren't referenced in active locks?
2. If lock references deleted hash, should sync re-download or fail?

---

### 5.4 Tool ID Collision: Builtin vs Managed Tools

**Issue**: Builtin tools (echo@1.0.0, fs@1.0.0) and managed tools (ffmpeg@5.0) share ID space. **No collision detection.**

**Scenario**:

- Builtin: `echo@1.0.0`
- User manually adds managed tool to conductor.machine.ncl: `echo@1.0.0` (tries to override builtin)
- Conductor loads: which tool is used? Builtin or managed?

**Current Spec**: "Builtins registered at compile time; managed tools in machine config"

**Gap**: No collision detection or precedence rule.

**Risk**: Ambiguous tool invocation; user accidentally overrides builtin; workflow behaves unexpectedly.

**Recommendations**:

- **Builtin IDs reserved**: managed tools cannot use builtin IDs
- Validation: **on machine config load, check for tool ID collisions; fail if managed tool ID matches builtin**
- Add test: "tool ID collision → error"

**Questions for Clarification**:

1. Should builtins be reserved (fail on collision) or managed tools override builtins?
2. If collision detected, what is error message?

---

### 5.5 State Persistence Consistency Across Layers

**Issue**: Conductor persists state to CAS; MediaPM persists lock to state.ncl. **No atomic consistency across both.**

**Scenario**:

- Sync completes: Conductor persists state blob to CAS (hash H_state)
- MediaPM updates lock in state.ncl and saves
- CAS crashes after state blob write, before state.ncl write
- Next startup: CAS state blob exists, MediaPM state.ncl missing/stale
- Inconsistency: which is source of truth?

**Current Spec**: "Direct materialization with trusted CAS integrity"

**Gap**: No coordination between CAS state blob and state.ncl lock records.

**Risk**: Inconsistent state; lock records don't match Conductor state; recovery unclear.

**Recommendations**:

- **Consistency point**: state.ncl lock records must reference CAS state blob hash
- On startup: **verify lock references valid CAS state blob; if mismatch, fail with explicit error**
- Recovery: **manual state rollback or rebuild from CAS**
- Add test: "state blob persisted but lock not updated → error on startup"

**Questions for Clarification**:

1. Should state.ncl include reference to CAS state blob hash for verification?
2. If verification fails, what's the recovery procedure?

---

### 5.6 Cache Invalidation Across Tool Versions

**Issue**: MediaPM caches tools; Conductor updates tool config. **No cache invalidation policy.**

**Scenario**:

- Tool cache: ffmpeg-5.0 materialized
- Conductor machine config updated: ffmpeg-6.0 (new version)
- Next sync: is old ffmpeg-5.0 still available? Or new ffmpeg-6.0 pulled?

**Current Spec**: "Tool provisioning cache separation"

**Gap**: No cache invalidation rule; version change handling unclear.

**Risk**: Stale tool versions used; features expected in new version unavailable.

**Resolution (post-Q2 fix)**: MediaPM now has explicit tool-id preservation
during workflow re-synthesis. When a tool version changes, the
`preserve_existing_generated_step_tools()` function rewrites the generated
step's tool id to the previous valid one (`generated.tool = previous.tool.clone()`).
The conductor still resolves to the old (cached) binary, and the mediapm
impure timestamp is NOT refreshed (synthesis emits `None` for refreshed steps;
the post-workflow backfill writes the actual timestamp only after the workflow
completes, and unchanged steps carry forward their prior timestamp). The new
version binary is provisioned separately; it replaces the old one on a later
sync cycle when the old tool id is pruned. Cache entries remain versioned; the
old version stays available until explicitly cleaned up.

**Updated Recommendations**:

- **Cache key includes version**: cache entry is (tool_id, version, platform)
- **Tool-id preservation**: `preserve_existing_generated_step_tools` preserves
  old tool ids across version changes, preventing unnecessary cache invalidation
  and keeping impure timestamps stable
- Version change: **new version automatically provisioned; old version remains**
  (separate cache entries); the old tool id continues to resolve to the old
  cached binary until pruned
- Add test: "tool version change with valid previous tool → old tool id
  preserved, impure timestamp unchanged, new binary provisioned separately"

**Questions for Clarification**:

1. Is tool cache versioned or version-agnostic?
2. Should old versions be auto-cleaned up after timeout?

---

### 5.7 Instance Key Immutability and Failure Recovery

**Concern**: Could a failed workflow step cause previously successful step instances to become unreachable, losing their I/O?

**Why It Is Safe**: The design ensures prior instances are preserved through several mechanisms:

1. **`state.clone()` on Error** (coordinator error checkpoint at `src/conductor/src/orchestration/coordinator.rs:303-320`): When a step fails, the coordinator calls `commit_run(next_state: state.clone(), pending_unsaved_hashes: BTreeSet::new())`. `state.clone()` preserves ALL current instances — no entries are discarded. Pending unsaved hashes are cleared (the failed step contributed no new CAS objects), but the prior state is untouched.

2. **Append-Only `OrchestrationState`**: `OrchestrationState { version: u32, instances: BTreeMap<String, ToolCallInstance> }` is stored as an immutable CAS blob. The `instances` map only grows via insertions — old entries are never removed. Old CAS blobs remain reachable as long as any caller holds their hash.

3. **State Pointer Advances on Every Run**: The `state_pointer` advances on **both** success and failure — it always points to the latest checkpoint. The difference is `pending_unsaved_hashes`: on error it is empty, meaning unsaved-output GC protection is weaker. (See [`crate-specifications.md` — State Pointer Advancement] for details and caveats about in-flight steps.)

4. **No Active CAS Garbage Collection**: CAS storage is append-only by default. Blobs are only deleted via explicit `cas.delete()`. There is no active pruning of unreferenced `OrchestrationState` blobs.

   **Update — Instance GC added (post-implementation)**: The `OrchestrationState` instance map inside the live state blob is now pruned by configurable TTL-based GC (`gc_instances(cutoff)` called from `commit_run()` and `persist_and_publish_state()`). This removes stale `ToolCallInstance` entries from the in-memory snapshot **before** it is serialized to a new CAS blob. The old CAS blobs (pre-GC) remain reachable in CAS storage until the `state_pointer` advances past them. Instance GC therefore controls growth within the state blob itself, but does not reclaim old CAS blobs — that still requires explicit CAS-level GC.

**The One Scenario Where Instances ARE Lost**: If a user or external process explicitly calls `cas.delete()` on the CAS blob containing the old `OrchestrationState`, the prior instances become unreachable. This is an administrative action, not a normal runtime behavior.

**Worked Example**:

- Step 1 succeeds → instance key K1 stored in `instances` map → CAS blob B1 created (contains state with K1)
- Step 2 fails → coordinator calls `commit_run(state.clone(), ...)` → CAS blob B2 created (contains K1 from Step 1, no entry for failed Step 2)
- `state_pointer` still references B1 (or B2, both contain K1) → K1 remains reachable
- Step 2 retried → new instance key K2 derived (may differ if impure) → on success, K2 added alongside K1
- Outcome: Step 1's I/O is always available via K1

---

### 5.8 NCL↔Rust Schema Sync Contract

**Issue**: With crate-specific NCL configurations (Conductor document, MediaPM
document, builtin tool configs) each having their own versioned schema, there
was no explicit contract for keeping the Rust-side `serde` structs in sync
with the NCL-side type annotations and contracts. Mismatches would only be
caught at runtime (NCL evaluation or JSON deserialization failure), not at
compile time.

**Key design decisions**:

1. **Typed envelope pattern** (MediaPM): `MediaPmDocumentEnvelopeV1` wraps
   `MediaPmDocumentStateV1` via `#[serde(flatten)]`. The parent carries
   `deny_unknown_fields` (works here since `deny_unknown_fields` on the parent
   rejects unknown JSON keys even with `flatten` on the child). The child
   struct does NOT carry `deny_unknown_fields` (it would be ignored under
   `flatten`).

2. **Typed bridge** (Conductor): `ConductorDocumentEnvelopeV1` wraps document
   state without `flatten` — version + body are separate named fields. Each
   inner document type has its own `deny_unknown_fields`. Round-trip tests
   verify JSON serialization symmetry.

3. **Dual decode path**: NCL produces a `nickel::Value` → JSON string → typed
   envelope → inner document. Encode reverses: inner document → envelope →
   JSON string. Unknown fields are rejected at the typed envelope boundary.

4. **PlatformInheritedEnvVars** is a `BTreeMap<String, Vec<String>>` type
   alias (not a struct with named fields). Platform keys are `"windows"`,
   `"linux"`, `"macos"`.

**Test coverage**:

- Conductor: `IntegerNumberV1 contract enforcement` (3 tests: valid integer,
  non-integer decimal, and non-numeric values) + round-trip serialize ↔
  deserialize for each versioned document type
- MediaPM: envelope-level `deny_unknown_fields` reject, round-trip of
  populated runtime storage (13 fields), round-trip of populated state
  (managed_files, tool_registry, active_tools, workflow_states), round-trip of
  `PlatformInheritedEnvVars` (3 platform entries)

**Verification rule for adding new fields**: When a new field is added to a
versioned Rust struct that maps to an NCL document:

- Add the field to the Rust struct (with `serde` attribute)
- Add corresponding field to the NCL schema (if applicable)
- Verify the typed envelope's `deny_unknown_fields` would catch a stray
  JSON key (parent envelope must carry the attribute)
- Add a round-trip test that populates the new field and verifies JSON
  symmetry

---

## PART 6: AMBIGUITIES IN STATED CONTRACTS

### 6.1 "Fail-Fast Validation": Exact Scope

**Issue**: Specification uses "fail-fast validation" but scope is ambiguous.

**Ambiguity**:

```text
Does "fail-fast" mean:
(a) Errors are thrown before ANY side effects?
(b) Errors are thrown before COMMITTED changes?
(c) Errors are thrown on FIRST INVALID input (but may have been processed)?
```

**Example**:

- Builtin receives args: `--arg name "Alice" --arg unknown-key "value"`
- (a) Throws error immediately (before processing name)
- (b) Throws error after recording name but before committing it
- (c) Throws error when encountering unknown-key, but name already processed

**Current Spec**: "Fail-fast validation: undeclared args/keys rejected immediately"

**Recommendation**:

- **Clarify to (a)**: "Fail-fast means validation errors are raised before ANY processing or side effects. Validation happens in a separate pass before execution."
- Add test: "validation errors do not produce any output or side effects"

---

### 6.2 "Deterministic Payload": System State Inclusion

**Issue**: Pure builtin output is "deterministic" but does not specify **system state handling** (e.g., timestamps, permissions).

**Ambiguity**:

- `echo` builtin outputs text: should `mtime` be included? Should file permissions be set?
- `archive` builtin zips files: should entry timestamps be deterministic or preserved?

**Current Spec**: "Pure = deterministic payload; impure = side-effect driven"

**Recommendation**:

- **Explicit rule**: "Deterministic payload means byte-for-byte identical output for identical input. This includes file metadata (timestamps, permissions, ownership); all metadata must be deterministic or omitted."
- For example, archive timestamps should be set to fixed value (epoch or input mtime)
- Add test: "pure builtin output identical across multiple runs"

---

### 6.3 "Direct Materialization": Cleanup on Failure

**Issue**: Under direct materialization, if a sync fails mid-way, files written to output paths before the failure need cleanup.

**Ambiguity**:

- Files 1–49 written to final output
- File 50 fails
- Does cleanup happen automatically or does caller invoke `cleanup()`?

**Current Spec**: "Direct materialization; cleanup on failure"

**Recommendation**:

- **Clarify to automatic**: "Direct materialization semantics mean if any step fails, cleanup of files written during this sync is automatic and unconditional. The API returns error; no manual cleanup needed."
- Add test: "failure during materialization → automatic cleanup (no orphaned files)"

---

### 6.4 "Deduplicated Tool IDs": Format and Enforcement

**Issue**: Specification uses "deduplicated tool IDs" but does not specify **ID format or deduplication mechanism**.

**Ambiguity**:

- Is ID format free-form string or must follow semver?
- Is deduplication by exact string match or normalized comparison?
- If ID contains uppercase letters, does case matter?

**Current Spec**: "Deduplicated tool IDs; tool ID collision error"

**Recommendation**:

- **Explicit format**: "Tool IDs must follow format `<name>@<version>` where name is lowercase alphanumeric+hyphens, version is semver. Case-insensitive deduplication."
- Or simpler: "Tool IDs are arbitrary strings; deduplication is exact string match (case-sensitive)."
- Add test: "ID format validation, case sensitivity"

---

### 6.5 "One-Shot Retry" for Pure Workflows: Automatic or Manual?

**Issue**: Specification states "pure workflows auto-recover from CAS integrity failures (warn + drop + retry once)" but does not specify **automatic vs. manual invocation**.

**Ambiguity**:

- Is retry automatic (within `run_workflow()`) or does caller invoke `retry_workflow()`?

**Current Spec**: "One-shot retry once"

**Recommendation**:

- **Explicit to automatic**: "Pure workflows automatically retry once if CAS integrity errors occur (e.g., hash mismatch). Retry is internal; no caller action needed. If retry fails, error is propagated."
- Add test: "CAS integrity error → automatic retry → success"

---

### 6.6 "Index Repair": In-Place or Rebuild?

**Issue**: `repair_index()` semantics unclear.

**Ambiguity**:

- Does repair modify on-disk index or only rebuild in-memory structures?
- Does repair re-hash all objects or only update metadata?

**Current Spec**: "Index repair on startup (optional)"

**Recommendation**:

- **Explicit**: "Repair updates on-disk index to current schema version and removes orphaned entries. No re-hashing; only metadata updated. Original object data untouched."
- Add test: "repair produces valid index; object data unchanged"

---

### 6.7 "Configuration Document Versioning": Migration Scope

**Issue**: Specification mentions migrations but does not specify **what changes require new version** vs. **compatible evolution**.

**Ambiguity**:

- Adding optional field to schema: does version bump?
- Renaming existing field: does version bump?
- Changing field type: does version bump?

**Current Spec**: "Explicit version markers; sequential migrations"

**Recommendation**:

- **Explicit versioning rules**:
  - Version bump required if: removing field, renaming field, changing field type, changing semantics
  - Version bump NOT required if: adding optional field with default, adding new optional top-level section
- Add test: "schema evolution scenarios → correct version bump decisions"

---

## PART 7: PERFORMANCE DETAILS REQUIRING SPECIFICATION

### 7.1 CAS Optimizer: Algorithm Details

**Issue**: Specification mentions "concurrent candidate scoring (8 tasks)" but algorithm is unspecified.

**Missing Details**:

- Search algorithm: exhaustive, greedy, dynamic programming, heuristic?
- Cost model: how are deltas scored (size, reconstruction time, age)?
- Optimization goal: minimize encoding size, minimize reconstruction time, balance?

**Risk**: Performance unpredictable; optimization may perform poorly or take excessive time.

**Recommendation**:

- Document optimizer algorithm: "Greedy algorithm scores all existing objects as potential bases. Cost model is: `cost = delta_size + base_access_time`. Top N candidates selected (N=8 configurable)."
- Add performance benchmark: "optimizer time for 1k objects with various constraints"

---

### 7.2 Conductor Scheduler: EWMA Details

**Issue**: Specification mentions "EWMA cost model; adaptive worker assignment" but EWMA parameters unspecified.

**Missing Details**:

- Decay rate (alpha): 0.1, 0.5, 0.9?
- Initialization for first task: use default estimate or wait for first completion?
- Worker pool size: CPU cores? Configurable?

**Risk**: Performance unpredictable; scheduler behavior varies with undocumented parameters.

**Recommendation**:

- Document EWMA: "Alpha=0.3 (decay rate); first task uses default estimate of 5 sec; worker pool size = CPU cores (configurable via CONDUCTOR_MAX_WORKERS)."
- Add performance regression test: "scheduler assigns tasks fairly across workers"

---

### 7.3 MediaPM Sync: Parallelization Strategy

**Issue**: Specification states "parallel workflows; bounded worker pool" but details unspecified.

**Current implementation**: The dependency-stream model dispatches steps from multiple
workflows in parallel. The parallelization strategy operates at two levels:

1. **Cross-workflow dispatch**: The coordinator builds per-workflow dependency
   graphs and dispatches all ready steps via `FuturesUnordered`. Steps from
   different workflows are submitted to a shared worker pool in round-robin
   order.
2. **Per-workflow cache probe and execution**: Within each step, the step worker
   probes the CAS using `exists_many` (`CasExistenceBitmap`) in O(1) round-trips
   and executes the tool when cache misses occur.

**Missing Details**:

- Are hashes computed in parallel (per-file) or sequentially?
- Are materializations parallelized (per-file) or per-workflow?
- Hash tree used or flat comparison?

**Risk**: Sync performance unpredictable; may bottleneck on single core for large syncs.

**Recommendation**:

- Document parallelization: "Two-level dispatch: cross-workflow dependency-stream
  dispatch in the coordinator (`FuturesUnordered` + round-robin worker assignment),
  plus per-workflow step execution with batch cache probe
  (`exists_many`/`CasExistenceBitmap`). Per-file hashing parallelized across
  available workers. Per-file materialization also parallelized. No hash tree;
  flat per-file comparison."
- Add performance benchmark: "sync time for 1000 files of various sizes"

---

### 7.4 Lock Reconciliation: Hash Comparison Performance

**Issue**: Specification mentions "check if hash unchanged" but does not specify **fast-path optimization**.

**Current Assumption**: Could be O(content_size) if comparison requires reading entire file.

**Missing Details**:

- Is comparison O(1) file stat-based or O(content_size) content-based?
- Is hash cached or recomputed?

**Risk**: Slow sync if every file is re-hashed even when unchanged.

**Recommendation**:

- Document: "Lock reconciliation compares stored hash (in lock) with current file hash. Current file hash computed once per file (not incremental). If hashes match, file marked as up-to-date (no re-materialization)."
- Add performance test: "sync with all files unchanged → should be O(file_count), not O(total_size)"

---

### 7.5 Delta Reconstruction: Caching and Performance

**Issue**: Specification mentions "O(depth) reconstruction" but does not specify **caching strategy**.

**Missing Details**:

- Is reconstructed full object cached?
- How long is cache retained?
- Is cache per-object or global?

**Risk**: Repeated reconstructions of same delta chain may thrash CPU.

**Recommendation**:

- Document: "Reconstructed full objects cached in memory (LRU, size-bounded to 1 GB). Cache entries expire after 1 hour or on cache eviction."
- Add test: "repeated reads of same delta → uses cache"

---

### 7.6 Builtin Invocation Overhead: Process vs In-Process

**Issue**: Specification does not clarify **CLI vs API invocation overhead**.

**Missing Details**:

- Are CLI builtins spawned as separate processes or in-process?
- Is there API invocation that avoids process spawn?

**Risk**: If all builtins spawn new processes, significant overhead for many small operations.

**Recommendation**:

- Document: "Builtins provide both CLI (spawned process) and library API (in-process). Conductor uses library API for performance. CLI available for external tools or manual invocation."
- Add performance benchmark: "API invocation vs CLI spawn overhead"

---

## PART 8: TESTING GAPS

### 8.1 CAS Crate: Delta Chain Robustness

**Missing Tests**:

- [ ] Corrupted delta (bytes don't apply cleanly) → recovery
- [ ] Orphaned deltas (deleted base) → integrity check detects
- [ ] Delta chain exceeding MAX_DEPTH after config change → pruning triggered
- [ ] Concurrent optimization + delete → no race condition
- [ ] Out-of-space + prune + retry → succeeds

**Recommendation**: Add test module `tests/e2e/delta_chain_robustness.rs` with 5 scenarios above.

---

### 8.2 Conductor Crate: External Data Error Handling

**Missing Tests**:

- [ ] put_from_uri(404) → NotFound error
- [ ] put_from_uri(timeout) → Timeout error, retries N times
- [ ] put_from_uri(partial download) → cleanup, error
- [ ] Missing external_data during workflow → validation error at planning time
- [ ] Workflow DAG with cycle → cycle detection error
- [ ] Document version missing → parse error

**Recommendation**: Add test module `tests/e2e/external_data_and_validation.rs` with scenarios above.

---

### 8.3 Conductor-Builtins: Path Safety and Security

**Missing Tests**:

- [ ] Symlink escape (../../etc) → rejected or sandbox-safe?
- [ ] Symlink loop → depth limit prevents hang
- [ ] Windows reserved names (CON, PRN) → rejected
- [ ] Special characters (`:`, `*`, `?`) → rejected or escaped
- [ ] ZIP bomb (10GB from 1MB) → size limit prevents extraction
- [ ] Archive symlink escape → symlinks rejected in extracted files
- [ ] CLI vs API with same args → identical output

**Recommendation**: Add test module `tests/e2e/path_safety_and_security.rs` with scenarios above.

---

### 8.4 MediaPM Crate: Sync Atomicity and Idempotency

**Missing Tests**:

- [ ] Partial materialization failure (file 50 of 100) → rollback, lock unchanged
- [ ] Lock file partial write → detected on load, inconsistency error
- [ ] Invalid hierarchy media_id → error at config load
- [ ] Read-only file re-materialization → succeeds (clears read-only bit)
- [ ] Media ID reused with new content → new download, new lock
- [ ] Concurrent sync operations → serialized or isolated correctly
- [ ] Tool version change → new version downloaded
- [ ] Sync idempotency: sync twice → second sync is no-op (all hashes match)

**Recommendation**: Add test module `tests/e2e/sync_atomicity_and_idempotency.rs` with scenarios above.

---

### 8.5 Cross-Crate Integration Tests

**Missing Tests**:

- [ ] CAS version + Conductor version mismatch → error with hint
- [ ] Builtin validation error → Conductor doesn't retry
- [ ] Transient builtin error → Conductor retries N times
- [ ] CAS prune removes hash in MediaPM lock → error or re-download
- [ ] Tool ID collision (builtin vs managed) → error
- [ ] State blob persisted but lock not updated → detected on startup

**Recommendation**: Add test module `tests/e2e/cross_crate_integration.rs` with scenarios above.

---

## PART 9: SUMMARY & RISK ASSESSMENT

### Issue Triage by Risk Level

#### **CRITICAL** (Operational blocker; unspecified, high-impact)

| Issue | Crate | Impact |
|---|---|---|
| Delta chain corruption recovery | CAS | Data loss; silent corruption if intermediate base deleted |
| CAS versioning vs Conductor versioning | CAS/Conductor | Version mismatch causes unmarshaling failure; deployment unclear |
| Partial sync rollback semantics | MediaPM | Inconsistent materialized files; recovery unclear |
| Tool ID collision detection | Conductor | Builtin overridden silently; wrong tool invoked |
| Missing external_data during execution | Conductor | Workflow fails mid-execution without validation |

#### **HIGH** (Needs clarification; affects correctness)

| Issue | Crate | Impact |
|---|---|---|
| Symlink loop and escape handling | Builtins | Security: write outside sandbox |
| ZIP bomb extraction | Builtins | DoS: disk exhaustion |
| Concurrent sync conflicts | MediaPM | Race condition; corrupted lock |
| Out-of-space prune semantics | CAS | Automatic vs. manual retry unclear |
| Window reserved names | Builtins | Cross-platform compatibility failure |

#### **MEDIUM** (Ambiguity; affects usability)

| Issue | Crate | Impact |
|---|---|---|
| Fail-fast validation scope | All | Error semantics unclear; side effect handling |
| Deterministic payload definition | Builtins | Timestamps/permissions handling unspecified |
| Atomic commit rollback trigger | MediaPM | Automatic vs. manual rollback |
| Performance algorithm details | All | Predictability; optimization tuning |
| Tool provisioning cache invalidation | MediaPM | Version mismatch; stale tools used |

### Recommendations by Priority

**Phase 1 (Do Immediately)**: 45–60% of task

1. Add delta chain integrity checks to CAS (detects corruption)
2. Implement tool ID collision detection (prevents silent breakage)
3. Add external_data validation before workflow execution (fails fast)
4. Specify partial sync rollback (atomic cleanup)
5. Clarify CAS/Conductor version coordination (prevents deployment issues)

**Phase 2 (Before Beta)**: 35–50% of task
6. Add symlink loop/escape detection in builtins
7. Add ZIP bomb size limits
8. Specify tool cache invalidation on version change
9. Add lock file atomic write + verification
10. Document performance algorithm details (EWMA, optimizer)

**Phase 3 (Before GA)**: 15–30% of task
11. Add concurrent sync serialization (lock-based)
12. Document case-sensitivity normalization
13. Clarify ambiguous contracts (fail-fast scope, determinism scope)
14. Comprehensive cross-crate integration tests

### Testing Coverage Gap

**Current Gaps** (from specification analysis):

- **Edge cases**: ~15 untested scenarios (CAS corruption, Conductor DAG cycles, MediaPM atomicity)
- **Security**: ~6 untested scenarios (symlink escapes, ZIP bombs, path traversal)
- **Performance**: ~3 untested scenarios (optimization timing, scheduler fairness, sync parallelization)
- **Cross-crate**: ~6 untested integration scenarios

**Estimated Test Writing Effort**: ~80–120 test cases needed (10–15 test files, each 8–12 scenarios)

### Implementation Blockers

**Defer These Until Architecture Review**:

1. Hash algorithm migration strategy (requires CAS redesign if algorithm changes)
2. Concurrent sync isolation model (file-lock vs. per-media lock; affects persistence layer)
3. State persistence consistency (CAS ↔ state.ncl coordination; may require new contract)

### Questions for Specification Refinement

**Unanswered clarifications** (from elaboration above):

1. Is delta chain prune automatic or manual?
2. Does `put_from_uri` have timeout and retry limits?
3. Are symlinks allowed in relative path mode?
4. Should concurrent syncs be serialized or isolated?
5. Is tool cache versioned or version-agnostic?
6. What is CAS versioning coordination with Conductor?

---

## Next Steps

1. **Update AGENTS.md** with resolved edge cases and clarified contracts
2. **Create issue tracker entries** for Phase 1 implementation (5 critical issues)
3. **Add test suite** with ~80 new test cases (split across crates)
4. **Architecture review** for blockers (hash migration, concurrency model, consistency)
5. **Re-run elaboration** after Phase 1 to close critical gaps

---

### A.1 Same Template Path with Different Media IDs in Hierarchy

**Issue**: The flattening dedup key was initially only the template path string
(e.g., `music/\${media.id}.mkv`), causing false duplicate errors when two
hierarchy entries shared the same template path but referenced different
`media_id` values. The `\${media.id}` placeholder resolves to distinct paths
during materialization, so the dedup check at flattening time was premature.

**Scenario**:

hierarchy = [
    { path = "music/\${media.id}.mkv", kind = "media", id = "entry-a", media_id = "song_a", variant = "audio" },
    { path = "music/\${media.id}.mkv", kind = "media", id = "entry-b", media_id = "song_b", variant = "audio" },
]

**Resolution**: The dedup key changed from `String` (template path only) to
`(String, String)` (template path + `media_id`). Entries with the same
template path but different `media_id` are now allowed. Same path + same
`media_id` is still correctly rejected as a duplicate.

**Cross-reference**: see `flatten_hierarchy_nodes_for_runtime()` in
`src/mediapm/src/config/hierarchy_types.rs` and the guard in
`collect_media_file_hierarchy_templates()` in
`src/mediapm/src/materializer/playlist.rs`.

**Rationale**: Template paths are resolved per-media_id during materialization
(via `resolve_hierarchy_relative_path()`), so the flattening dedup check
operates on unresolved template strings and must only compare entries that
would actually produce the same materialized path — which requires accounting
for `media_id`.
