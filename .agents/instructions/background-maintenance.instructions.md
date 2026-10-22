---
description: "Use when implementing or modifying background maintenance tasks in the mediapm workspace. Covers RAII lifecycle, run-then-sleep loop semantics, interruptibility, testing requirements, and instance-specific per-task entries."
name: "Background Maintenance Policy"
applyTo: "src/**/*.rs"
---

# Background maintenance policy

Unified policy for all background maintenance tasks across the mediapm workspace.

## Lifecycle

- **Trigger**: At construction time of the owning instance.
- **Loop**: Run immediately at spawn, then wait a fixed interval after each complete run, then run again. Timer starts only after a run finishes.
- **Interruptibility**: Always freely interruptible. Loop checks cancellation flag before and after each work cycle. RAII guard's `abort()` provides immediate termination.
- **RAII lifecycle**: Starts when the owning instance is constructed; ends when that instance is destroyed (guard's Drop cancels the task).
- **Shutdown is two calls, not one**: `BackgroundMaintenanceGuard::cancel()` only *requests* the abort; `BackgroundMaintenanceGuard::shutdown().await` aborts **and awaits** the handle, so the task is observably stopped when it returns. Neither waits for file-system work the task already handed to `spawn_blocking` — an abort cannot cancel a dispatched blocking closure. A task that can write into a directory its owner is about to remove needs that second guarantee too: `FileSystemCas` pairs `shutdown` with a mutation gate (see `temp-directory-spec.instructions.md`, "Why a CAS temp root is safe to remove").
- **Testing**: Every background task must be tested explicitly as a background loop (not just via foreground API calls).
- **A task that mutates state must publish it**: a loop that evicts records and leaves the file promising them is worse than no loop, so the conductor's pass writes the state file it computed before the next interval.
- **A timer may reclaim only what it can account for**: the conductor's background pass deletes payload bytes, and its account is the document plus the recorded instances — every hash a document names is declared in `external_data` (enforced at document load) and every byte a run produced is a recorded instance output. A task whose account is incomplete must not delete.
- **Mechanism**: Use `mediapm_cas::BackgroundMaintenanceGuard` for all background tasks.

## Instance entries

1. **WAL consumer** — `src/mediapm-cas/src/storage/file_system.rs`. Interval 300s hardcoded. Stored as `Arc<BackgroundMaintenanceGuard>`. Field name `_bg_guard`. Constructed with `BackgroundMaintenanceGuard::new(cancelled, handle)`.
2. **Conductor maintenance** — `src/mediapm-conductor/src/conductor_impl.rs`. The full GC cycle (`gc::run_conductor_gc`): instance eviction from the recorded seeds, orphan reclamation, then CAS metadata maintenance, then a state write. Interval `DEFAULT_CONDUCTOR_GC_INTERVAL_SECONDS` (86400s), started by the first successful `ensure_actor_client`. Stored as `Option<BackgroundMaintenanceGuard>` in the `Conductor` facade. Field name `background_maintenance`. The coordinator runs no sweep of its own: two sweeps of one store race, and only the facade holds the state a pass must write back. `Conductor::shutdown` awaits the guard.

   The tick reclaims payload bytes, and the external-data invariant is what makes that safe: the root set is the surviving instances' outputs plus the merged document's `external_data` keys and `content_map` hashes, and `NickelDocument::validate_external_data_invariant` rejects at load any document that names a hash — in a `content_map`, in a `${external_data.<hash>}` reference from a step input or tool default input, or by putting the hash itself in an input — without declaring it. So a step that needs bytes kept alive declares them, and an application that forgets fails when its document is read instead of losing the bytes later. The explicit `conductor gc` command (`gc::run_conductor_gc` → `Conductor::run_gc`) runs the same cycle.
3. **Cache prune** — `src/mediapm-conductor/src/cache.rs`. Interval 86400s fixed. Stored as `Option<Arc<BackgroundMaintenanceGuard>>` in `Cache`. Started automatically inside `Cache::open()`. Field name `bg_guard`.
