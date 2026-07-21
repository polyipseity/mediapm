---
description: "Use when implementing or modifying background maintenance tasks in the mediapm workspace. Covers RAII lifecycle, run-then-sleep loop semantics, interruptibility, testing requirements, and instance-specific per-task entries."
name: "Background Maintenance Policy"
applyTo: "src/**/*.rs"
---

# Background maintenance policy

Unified policy for all background maintenance tasks across the mediapm workspace.

## Lifecycle

- **Trigger**: At construction time of the owning instance.
- **Lifecycle**: Run immediately at spawn, then wait a _fixed interval after each complete run_, then run again. Timer starts only after a run finishes.
- **Interruptibility**: Always freely interruptible. Loop checks cancellation flag before and after each work cycle. RAII guard's `abort()` provides immediate termination.
- **RAII lifecycle**: Starts when the owning instance is constructed; ends when that instance is destroyed (guard's Drop cancels the task).
- **Testing**: Every background task must be tested explicitly as a background loop (not just via foreground API calls).
- **Mechanism**: Use `mediapm_cas::BackgroundMaintenanceGuard` for all background tasks.

## Instance entries

1. **WAL consumer** — `src/mediapm-cas/src/storage/file_system.rs`. Interval 300s hardcoded. Stored as `Arc<BackgroundMaintenanceGuard>`. Field name `_bg_guard`.
2. **Conductor CAS GC** — `src/mediapm-conductor/src/orchestration/coordinator.rs`. Interval 86400s default, configurable via `start_background_gc(interval_secs)`. Stored as `BackgroundMaintenanceGuard` in `WorkflowCoordinator`. Field name `background_gc_guard`.
3. **Cache prune** — `src/mediapm-conductor/src/cache.rs`. Interval 86400s fixed. Stored as `Option<Arc<BackgroundMaintenanceGuard>>` in `Cache`. Started automatically inside `open_with_index_file_name_and_ttl`. Field name `bg_guard`.
