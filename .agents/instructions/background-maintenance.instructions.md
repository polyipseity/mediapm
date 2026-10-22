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
- **A timer may not delete bytes it cannot account for**: background maintenance may reclaim what its own records describe and nothing else. Announcing that a blob is unreferenced requires knowing every producer, and the producers include processes the conductor does not track.
- **Mechanism**: Use `mediapm_cas::BackgroundMaintenanceGuard` for all background tasks.

## Instance entries

1. **WAL consumer** — `src/mediapm-cas/src/storage/file_system.rs`. Interval 300s hardcoded. Stored as `Arc<BackgroundMaintenanceGuard>`. Field name `_bg_guard`. Constructed with `BackgroundMaintenanceGuard::new(cancelled, handle)`.
2. **Conductor maintenance** — `src/mediapm-conductor/src/conductor_impl.rs`. Instance eviction from the recorded seeds plus CAS metadata maintenance (`gc::run_record_maintenance`), then a state write. Interval `DEFAULT_CONDUCTOR_GC_INTERVAL_SECONDS` (86400s), started by the first successful `ensure_actor_client`. Stored as `Option<BackgroundMaintenanceGuard>` in the `Conductor` facade. Field name `background_maintenance`. The coordinator runs no sweep of its own: two sweeps of one store race, and only the facade holds the state a pass must write back. `Conductor::shutdown` awaits the guard.

   The tick deletes no payload, and that is a rule rather than an omission: the store is shared with the application, which stages bytes it has not recorded anywhere yet, and no root set the conductor can compute from its own state and document names them. Only the explicit `conductor gc` command (`gc::run_conductor_gc` → `Conductor::run_gc`) reclaims bytes, because an operator chose the moment and nothing is in flight. The cost is stated where a reader will meet it: an installation that only ever syncs grows until someone runs `conductor gc`. Running byte reclamation unattended needs either protected-hash declarations from the application before it imports, or a reclaim point outside any run.
3. **Cache prune** — `src/mediapm-conductor/src/cache.rs`. Interval 86400s fixed. Stored as `Option<Arc<BackgroundMaintenanceGuard>>` in `Cache`. Started automatically inside `Cache::open()`. Field name `bg_guard`.
