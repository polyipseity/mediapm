---
description: "Use when working with ToolContentCache in conductor or mediapm crate. Documents the refactoring that extracted tool content caching from StepWorkerExecutor into standalone ToolContentCache<C>."
name: "ToolContentCache Architecture"
applyTo: "src/mediapm-conductor/src/tool_cache/**/*.rs, src/mediapm-conductor/src/orchestration/**/*.rs, src/mediapm/src/conductor_bridge/**/*.rs"
---

# ToolContentCache Architecture

## Design

- `ToolContentCache<C>` is the sole authority over `tools_dir/`. Uses DashMap single-flight per `tool_id` to prevent redundant extraction, `Arc<Semaphore>` to gate concurrent extractions (default 8), and `spawn_blocking` to move sync I/O off tokio worker threads.
- `materialize(tool_id, content_map)` returns `ToolCacheEntry` (wraps `PathBuf`, `Deref<Target=Path>`, `AsRef<Path>`).
- `link_to_sandbox()` is a static function (no self).
- `retain_only(active_tool_ids)` and the free function `retain_only_tool_dirs(tools_dir, active_tool_ids)` both delegate to `do_retain_only`.
- Published constants: `PAYLOAD_DIR_NAME` (`"payload"`). Published utility: `sanitize_tool_id`.

## History

Refactoring completed in 5 atomic commits on `main`:

1. `b1828e1` — docs: update spec references
2. `a7a8abf` — add tool_cache/mod.rs (standalone module, compiled, tested)
3. `87b11d4` — wire into coordinator, api, cli, protocol; modify step_worker
4. `a80adf5` — mediapm adaptation (PAYLOAD_DIR_NAME, retain_only_tool_dirs)
5. `6d85a01` — delete legacy tool_content_cache.rs (1081 lines removed)
