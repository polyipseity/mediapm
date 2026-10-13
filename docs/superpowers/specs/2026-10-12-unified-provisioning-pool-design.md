# Unified provisioning pool and fail-fast tool sync — design

## Problem 1: tool provisioning does not maximize concurrency

`src/mediapm/src/conductor_bridge/sync/mod.rs:1271-1360` splits provisioning entries into two levels with a hard barrier between them: level 0 (deduped `Dep`-kind entries) runs to full completion — fetch *and* process — before level 1 (explicit entries) even starts resolving. Dedup (`:895-921`) keeps the dep-kind entry when a tool is both a dependency and an explicit tool, so in the online demo the split is level 0 = {ffmpeg, deno, sd} and level 1 = {yt-dlp, rsgain, media-tagger}: at most 3 tools active at once, and every level-1 tool waits for the slowest level-0 tool's entire pipeline. Each tool's own res→fch→pro phases are sequential inside `provision_entry` (`:166`), which is inherent (process needs fetched bytes), but nothing requires *cross-tool* serialization beyond two real per-edge data needs: a requester's composite canonical version needs its same-step deps' resolved versions, and its inlining needs their own content maps.

## Problem 2: a failed required-tool provisioning surfaces as a misleading error

`sync/mod.rs:212` routes resolve failures into `EntryOutcome::ResolveError`, which `apply_entry_outcome` turns into a warning and continues (`:427`, `:605`). When the tool is required by a media workflow, the run later hard-fails at `src/mediapm/src/tools/workflows/mod.rs:95` with *"logical tool 'ffmpeg' is required but not active; add it under mediapm.ncl tools and run tool sync"* — advice that is wrong (the tool *is* configured; sync *did* run) and never mentions the real cause. Observed live: the online demo's ffmpeg resolve failed (no bars, `[W]` overall marker), and the only visible error was the misleading one. `ToolSyncReport.warnings` are printed on the success path only.

Both defects predate the progress-labels branch (`git diff --stat 6086b285..HEAD -- src/mediapm/src/conductor_bridge/ src/mediapm/src/tools/` is empty).

## Decisions (user rulings)

1. **Fail fast with the real cause.** Any entry's provisioning failure aborts the whole sync immediately: in-flight work is cancelled, nothing is applied, nothing is written, and the error names tool + phase + full cause chain.
2. **Unified pool with per-edge waits.** One bounded parallel pool for all tools; every tool resolves and fetches immediately; a tool waits only at its skip-check/composite (for same-step deps' resolved versions) and before its process/inlining (for same-step deps' own content maps). Cross-step deps create no edges.

## Design

### Pool contract (new module `src/mediapm/src/conductor_bridge/sync/pool.rs`)

The scheduler is a pure core: a generic driver over an injected provision closure, so all scheduling behavior is unit-testable without network.

- `PoolJob<I> { index, same_step_deps: Vec<usize>, input }` — one unit; `same_step_deps` is a DAG (cycles are rejected at the config boundary, MPM-E003).
- `DepGates` (receiver side, handed to each job) — `async fn dep_version(index) -> Option<String>` and `async fn dep_own_map(index) -> Option<BTreeMap<String, String>>`. Backed by `tokio::sync::watch` channels (multi-subscriber: one dep feeds several dependents).
- `DepPublisher` (sender side, handed to each job) — a job publishes its **bare** canonical version right after resolve (skips publish theirs from the reconstructed entry) and its **pre-inline own map** right after process (or skip reconstruction). Bare version, never the composite, preserves the non-transitive rule.
- `run_pool(jobs, concurrency, run) -> Result<Vec<(usize, O)>, PoolFailure<E>>` — drives jobs via `FuturesUnordered` + `buffer_unordered`-equivalent bounding. Success: outcomes sorted by `index`. Failure: the first `Err` returns immediately and drops every in-flight future (cancelling downloads); nothing has been applied because apply stays post-collection.

Wait points live inside the job (production `provision_entry` awaits the gates), which keeps the driver trivial and lets tests express any wait topology.

### Production wiring (`sync/mod.rs`)

- The level-0/level-1 loops are replaced by one `run_pool` call; jobs are built from the deduped entries with `same_step_deps` derived from `known_dependency_type()` (`tools/dependency.rs`) — a dep edge exists only for `SAME_STEP` roles, and the dep's index is the first entries-order match on the dep tool id.
- `provision_entry` gains `&DepGates` + `&mut DepPublisher` + the dep-index map. It awaits dep versions before the skip/composite check, publishes its bare version on every resolved path, awaits dep own maps before inlining, and feeds `inline_same_step_deps` (`:990`, signature unchanged) a map built from the gates instead of the apply-populated `provisioned_own_maps` (which is deleted).
- `EntryOutcome::ResolveError`/`FetchError` are deleted; `provision_entry` returns `Result<EntryOutcome, ProvisionFailure>` where `ProvisionFailure { tool_id, phase: Resolve|Fetch|Process, source }`. The coordinator maps a pool failure to `MediaPmError::Workflow("tool sync failed: {tool_id} ({phase}): {source}")`, finishes the overall bar red, and returns before any apply/save/state write (all-or-nothing).
- Determinism: outcomes apply sequentially in original entry order. Dedup places dep-kind entries first (`all_entries = dep_entries ++ explicit`, `:896`), so index order equals today's dep-then-explicit apply order; `state.json`/`conductor.generated.ncl` remain byte-stable for identical inputs.

### Invariants preserved

Composite canonical version format, non-transitive inlining, skip-if-up-to-date, `content_map ⊆ external_data`, the dual-write policy, and the `managed_tools` registration rules are untouched; only where the data flows *from* changes (apply-time map → per-edge channels). A failed run now writes nothing, which strengthens the dual-write story rather than altering it.

## Testing strategy (TDD)

Pool unit tests drive `run_pool` with fake closures and `tokio::time` (paused clock): index-order determinism; overlap (a dependent resolves before its dep finishes); the process-stage edge wait; cross-step deps causing no wait; first-failure abort with cancellation (drop-guard proves in-flight futures are dropped); concurrency bounding (max in-flight ≤ N). Production stays covered by the existing hermetic `tests/int/tool_sync.rs` suites; any test that pinned warning-continue for a required tool is updated in the same commit as the behavior change (repo rule), with the intended-behavior note in the task report.

## Non-goals

- No provider/network fixes (a still-down evermeet will now fail fast *with its real error* — that is the intended visible change, not a regression).
- No change to `default_tool_provision_concurrency` semantics (env override + `available_parallelism`), CrossStep scheduling, progress UI shape, or the config schema.

## Verification

`cargo test-pkg mediapm` (+ `mediapm-conductor` for safety) green; fmt/clippy via prek hooks. Runtime proof is `cargo run --example mediapm_demo_online` (level-3, network): a healthy network yields the full sync with visibly overlapping per-tool rows; an unhealthy source yields the new fail-fast error naming tool, phase, and cause — both outcomes are correct per this design.
