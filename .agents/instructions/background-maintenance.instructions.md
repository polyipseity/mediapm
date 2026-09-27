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
- **Mechanism**: Use `mediapm_cas::BackgroundMaintenanceGuard` for all background tasks.

## Instance entries

1. **WAL consumer** — `src/mediapm-cas/src/storage/file_system.rs`. Interval 300s hardcoded. Stored as `Arc<BackgroundMaintenanceGuard>`. Field name `_bg_guard`. Constructed with `BackgroundMaintenanceGuard::new(cancelled, handle)`.
2. **Conductor CAS GC** — `src/mediapm-conductor/src/orchestration/coordinator.rs`. Interval 86400s default, configurable via `start_background_gc(interval_secs)`. Stored as `BackgroundMaintenanceGuard` in `WorkflowCoordinator`. Field name `background_gc_guard`.
3. **Cache prune** — `src/mediapm-conductor/src/cache.rs`. Interval 86400s fixed. Stored as `Option<Arc<BackgroundMaintenanceGuard>>` in `Cache`. Started automatically inside `Cache::open()`. Field name `bg_guard`.
