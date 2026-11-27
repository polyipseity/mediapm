---
description: "Use for background maintenance tasks and lifecycle."
name: "Background Maintenance Policy"
applyTo: "src/**/*.rs"
---

# Background maintenance policy

## Lifecycle

- Trigger: instance construction; run at spawn, then fixed wait after each run (timer starts after finish). Interruptible: check cancellation before/after each cycle, `abort()` immediate; guard Drop cancels at destruction.
- Two shutdown calls: `BackgroundMaintenanceGuard::cancel()` only requests; `BackgroundMaintenanceGuard::shutdown().await` aborts **and awaits**, so task is stopped on return. The await settles the task, not its blocking closures. A task writing into a directory its owner will remove drains them itself: `Cache::shutdown` awaits guard, then domain in-flight count (only record of accepted-but-unstarted closure), then domain index-mutex probe (synchronous writer on another thread); probe panics outside Tokio. `FileSystemCas` pairs `shutdown` with a mutation gate (`temp-and-paths.instructions.md`, "CAS temp root removal").
- Loop-starting `Cache` (`bg_guard.is_some()`, from `Cache::open()`) must complete `Cache::shutdown().await` before last handle drops. Final `Drop` detects misuse: after best-effort cancellation it panics with `"loop-starting cache dropped without completed shutdown; call Cache::shutdown().await before drop"` unless unwinding (then `tracing::error!`/stderr, no panic). Completion shared across clones. Exempt: `open_without_background`, `OutputCache`, `MetadataCache`. `Cache::with_open`/`UserLevelCache::with_open` (`ToolDownloadCache` inherits) open, run body, await shutdown before return.
- Test every task as a loop, not only via foreground API. Mutating tasks publish computed state before next interval; timer reclaims only what it accounts for (document + recorded instances: declared `external_data` hashes at load, recorded outputs); incomplete account = no deletion.
- Mechanism: `mediapm_cas::BackgroundMaintenanceGuard` for all background tasks.

## Instance entries

1. **WAL consumer** — `src/mediapm-cas/src/storage/file_system.rs`; interval 300s hardcoded; `Arc<BackgroundMaintenanceGuard>`, field `_bg_guard`, `BackgroundMaintenanceGuard::new(cancelled, handle)`.
2. **Conductor maintenance** — `src/mediapm-conductor/src/conductor_impl.rs`; `gc::run_conductor_gc`: evict recorded seeds, reclaim orphans, CAS metadata maintenance, state write. Interval `lifecycle.background_gc_interval_seconds` (default `DEFAULT_CONDUCTOR_GC_INTERVAL_SECONDS`, 86400s), started by first successful `ensure_actor_client`; `Option<BackgroundMaintenanceGuard>` in `Conductor`, field `background_maintenance`. Coordinator runs no sweep (two sweeps race; facade holds state to write). `Conductor::shutdown` awaits guard; test owning runtime root must await `Conductor::shutdown().await` before dropping it (`Drop` only requests abort). Exception: test deliberately dropping facade to pin drop-cancel may skip await after verifying quiescence (no new GC epoch) before root drops.

   Root set = surviving outputs + merged `external_data` keys + `content_map` hashes. `NickelDocument::validate_external_data_invariant` rejects at load undeclared hash in `content_map`, `${external_data.<hash>}` from step inputs/tool defaults/command parts, or hash-valued input; resolver parse covers selectors/conditionals. Omission fails at load. `conductor gc` (`gc::run_conductor_gc` -> `Conductor::run_gc`) runs same cycle; bytes staged before declaring document exists can be reclaimed, then re-imported next run.
3. **Cache prune** — `src/mediapm-conductor/src/cache.rs`; interval 86400s fixed; `Option<Arc<BackgroundMaintenanceGuard>>` in `Cache`, field `bg_guard`, started by `Cache::open()`. `Cache::shutdown()` aborts + joins + index-write drain. Dropping last loop-starting handle without shutdown is misuse: `Drop` panics unless unwinding; reopening same root in-process requires await first. Prefer `Cache::with_open`/`UserLevelCache::with_open` (`ToolDownloadCache` inherits).
