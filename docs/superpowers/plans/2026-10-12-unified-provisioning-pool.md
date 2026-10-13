# Unified provisioning pool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the two-level provisioning barrier with one dependency-aware bounded pool so every tool resolves and fetches concurrently, and make a required tool's provisioning failure abort the sync immediately with tool + phase + real cause.

**Architecture:** Task 1 adds a generic pure-core scheduler module (`sync/pool.rs`) whose entire wait/abort/ordering behavior is unit-tested through an injected provision closure — production code unchanged. Task 2 wires `reconcile_desired_tools` onto that pool: `provision_entry` gains per-edge gates/publisher (dep resolved versions, dep pre-inline own maps), `provisioned_own_maps` and the `ResolveError`/`FetchError` warning path are deleted, and a pool failure propagates as `tool sync failed: {tool} ({phase}): {cause}` with nothing applied or written.

**Tech Stack:** Rust (`mediapm` crate), tokio (`sync` watch channels, `FuturesUnordered`), existing `known_dependency_type()` edge registry and `inline_same_step_deps()`.

**Spec:** `docs/superpowers/specs/2026-10-12-unified-provisioning-pool-design.md` (the authority; this plan argues from it).

## Global Constraints

- Every commit message passes commitlint via the temp-dir install flow (repo has no JS manifest; validate before committing; rejections leave HEAD unmoved — fix and retry, never `--amend`).
- Never run `git reset` (any form). Never `git commit --amend`. Commits only, no push.
- Rust: immutable by default; no `.unwrap()`/`.expect()` on fallible domain paths (test code may assert); no `Box<dyn Any>`; no `#[allow]`.
- Every touched Rust item (including private items and test helpers) carries doc comments; module docs stay current.
- No network in tests. Time-based scheduling tests use `#[tokio::test(start_paused = true)]`.
- Build environment: prefix every cargo invocation with `RUSTC_WRAPPER= CARGO_BUILD_RUSTC_WRAPPER=` (sccache is blocked).
- Selective validation during development: `cargo test-pkg <crate>`, `cargo test -p mediapm <filter>`.
- Behavior change and its test/doc updates land in the same commit.

---

### Task 1: Dependency-aware pool scheduler (pure core, additive)

**Files:**

- Create: `src/mediapm/src/conductor_bridge/sync/pool.rs` — the scheduler module.
- Modify: `src/mediapm/src/conductor_bridge/sync/mod.rs` — add `mod pool;` (module declaration only; no call sites yet).

**Interfaces:**

- Consumes: nothing from other tasks (additive module).
- Produces (Task 2 relies on these exact names):

```rust
pub(crate) struct PoolJob<I> { pub index: usize, pub same_step_deps: Vec<usize>, pub input: I }
pub(crate) struct DepGates { /* watch receivers keyed by dep index */ }
impl DepGates {
    pub(crate) async fn dep_version(&self, index: usize) -> Option<String>;
    pub(crate) async fn dep_own_map(&self, index: usize) -> Option<BTreeMap<String, String>>;
}
pub(crate) struct DepPublisher { /* this job's own watch senders */ }
impl DepPublisher {
    pub(crate) fn publish_version(&mut self, version: String);
    pub(crate) fn publish_own_map(&mut self, map: BTreeMap<String, String>);
}
pub(crate) struct PoolFailure<E> { pub index: usize, pub source: E }
pub(crate) async fn run_pool<I, O, E, F, Fut>(
    jobs: Vec<PoolJob<I>>,
    concurrency: usize,
    run: F,
) -> Result<Vec<(usize, O)>, PoolFailure<E>>
where
    I: Send, O: Send, E: Send,
    F: Fn(PoolJob<I>, DepGates, DepPublisher) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<O, E>> + Send,
```

- [ ] **Step 1: Write the failing scheduler tests**

Append to `pool.rs` an inline `#[cfg(test)] mod tests` with these six tests (write them first; `run_pool` will not exist yet):

```rust
#[tokio::test(start_paused = true)]
async fn pool_returns_outcomes_in_index_order_regardless_of_completion_order() {
    let jobs = vec![
        PoolJob { index: 0, same_step_deps: vec![], input: Duration::from_millis(30) },
        PoolJob { index: 1, same_step_deps: vec![], input: Duration::from_millis(10) },
        PoolJob { index: 2, same_step_deps: vec![], input: Duration::from_millis(20) },
    ];
    let outcomes = run_pool(jobs, 4, |job, _gates, _pub| async move {
        tokio::time::sleep(job.input).await;
        Ok::<_, ()>(job.index)
    })
    .await
    .expect("all jobs succeed");
    assert_eq!(outcomes, vec![(0, 0), (1, 1), (2, 2)]);
}
```

```rust
#[tokio::test(start_paused = true)]
async fn pool_lets_a_dependent_start_before_its_dep_finishes() {
    // Event log: the dependent's independent work must be recorded before
    // the dep's completion, and the dependent's gated stage must be recorded
    // only after the dep published its own map.
    let log = Arc::new(Mutex::new(Vec::<&'static str>::new()));
    let jobs = vec![
        PoolJob { index: 0, same_step_deps: vec![], input: () },              // dep
        PoolJob { index: 1, same_step_deps: vec![0], input: () },             // dependent
    ];
    let log_dep = log.clone();
    let log_ind = log.clone();
    let log_gate = log.clone();
    let outcomes = run_pool(jobs, 4, move |job, mut gates, mut publisher| {
        let log = if job.index == 0 { log_dep.clone() } else { log_ind.clone() };
        let log_gate = log_gate.clone();
        async move {
            if job.index == 0 {
                log.lock().unwrap().push("dep-resolve");
                tokio::time::sleep(Duration::from_millis(100)).await;
                publisher.publish_version("v0".to_string());
                tokio::time::sleep(Duration::from_millis(100)).await;
                publisher.publish_own_map(BTreeMap::new());
                log.lock().unwrap().push("dep-done");
            } else {
                log.lock().unwrap().push("dependent-start");   // before dep-done
                gates.dep_version(0).await.expect("dep version");
                gates.dep_own_map(0).await.expect("dep map");
                log_gate.lock().unwrap().push("dependent-gated"); // after dep-done
            }
            Ok::<_, ()>(())
        }
    })
    .await
    .expect("both succeed");
    assert_eq!(outcomes.len(), 2);
    let log = log.lock().unwrap();
    let dep_done = log.iter().position(|e| *e == "dep-done").expect("dep finished");
    let start = log.iter().position(|e| *e == "dependent-start").expect("dependent started");
    let gated = log.iter().position(|e| *e == "dependent-gated").expect("gated stage ran");
    assert!(start < dep_done, "dependent must start before its dep finishes: {log:?}");
    assert!(gated > dep_done, "gated stage must run after the dep published: {log:?}");
}
```

```rust
#[tokio::test(start_paused = true)]
async fn pool_does_not_gate_a_job_with_no_declared_edges() {
    // A job with empty same_step_deps must complete while another job is
    // still mid-flight; with a paused clock a gating bug would hang the test.
    let jobs = vec![
        PoolJob { index: 0, same_step_deps: vec![], input: Duration::from_secs(60) },
        PoolJob { index: 1, same_step_deps: vec![], input: Duration::from_millis(1) },
    ];
    let outcomes = tokio::time::timeout(
        Duration::from_secs(5),
        run_pool(jobs, 2, |job, _gates, _pub| async move {
            tokio::time::sleep(job.input).await;
            Ok::<_, ()>(job.index)
        }),
    )
    .await
    .expect("ungated job must not wait for an unrelated job")
    .expect("both succeed");
    assert_eq!(outcomes, vec![(0, 0), (1, 1)]);
}
```

```rust
#[tokio::test(start_paused = true)]
async fn pool_aborts_and_cancels_in_flight_jobs_on_the_first_failure() {
    // Job 1 fails after a short delay; job 0 sleeps forever but holds a drop
    // guard. The pool must return the failure and drop job 0 (cancellation).
    struct CancelProbe(Arc<AtomicBool>);
    impl Drop for CancelProbe {
        fn drop(&mut self) { self.0.store(true, Ordering::SeqCst); }
    }
    let dropped = Arc::new(AtomicBool::new(false));
    let jobs = vec![
        PoolJob { index: 0, same_step_deps: vec![], input: () },
        PoolJob { index: 1, same_step_deps: vec![], input: () },
    ];
    let probe = CancelProbe(dropped.clone());
    let failure = run_pool(jobs, 2, move |job, _gates, _pub| {
        let probe = if job.index == 0 { Some(CancelProbe(probe_flag.clone())) } else { None };
        async move {
            if job.index == 0 {
                let _keep = probe;
                tokio::time::sleep(Duration::from_secs(3600)).await;
                Ok::<_, &'static str>(0)
            } else {
                tokio::time::sleep(Duration::from_millis(10)).await;
                Err("boom")
            }
        }
    })
    .await
    .expect_err("failure must propagate");
    assert_eq!(failure.index, 1);
    assert_eq!(failure.source, "boom");
    // Cancellation: after run_pool returns, every in-flight future is dropped.
    // (Await one yield so the drop runs.)
    tokio::task::yield_now().await;
    assert!(probe_flag.load(Ordering::SeqCst), "in-flight job must be cancelled");
}
```

(Adjust the guard plumbing to compile cleanly — one `Arc<AtomicBool>` cloned into the closure; the sketch shows the intent: the sleeping job's guard drops when the pool aborts.)

```rust
#[tokio::test(start_paused = true)]
async fn pool_bounds_concurrency_to_the_requested_width() {
    let active = Arc::new(AtomicUsize::new(0));
    let max_seen = Arc::new(AtomicUsize::new(0));
    let jobs: Vec<PoolJob<()>> = (0..6).map(|index| PoolJob { index, same_step_deps: vec![], input: () }).collect();
    let outcomes = run_pool(jobs, 2, {
        let active = active.clone();
        let max_seen = max_seen.clone();
        move |_job, _gates, _pub| {
            let active = active.clone();
            let max_seen = max_seen.clone();
            async move {
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(10)).await;
                active.fetch_sub(1, Ordering::SeqCst);
                Ok::<_, ()>(())
            }
        }
    })
    .await
    .expect("all succeed");
    assert_eq!(outcomes.len(), 6);
    assert_eq!(max_seen.load(Ordering::SeqCst), 2, "never more than `concurrency` in flight");
    assert!(max_seen.load(Ordering::SeqCst) >= 1);
}
```

```rust
#[tokio::test(start_paused = true)]
async fn pool_with_no_jobs_returns_empty_success() {
    let outcomes: Vec<(usize, ())> = run_pool(vec![], 4, |_job, _g, _p| async { Ok(()) }).await.expect("empty pool succeeds");
    assert!(outcomes.is_empty());
}
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `RUSTC_WRAPPER= cargo test -p mediapm --lib -- conductor_bridge::sync::pool`
Expected: FAIL to compile — `run_pool`, `PoolJob`, `DepGates`, `DepPublisher` not defined.

- [ ] **Step 3: Implement `run_pool`**

In `pool.rs`: the driver builds, per job index, a `tokio::sync::watch::channel::<Option<String>>(None)` and a `watch::channel::<Option<BTreeMap<String, String>>>(None)`; `DepGates` holds cloned receivers for the job's `same_step_deps`; `DepPublisher` holds the job's own senders. Jobs run through `futures::stream::iter(jobs).map(|job| { let gates = ...; let mut publisher = ...; let mut run = run.clone(); async move { (job.index, run(job, gates, publisher).await) } }).buffer_unordered(concurrency.max(1))`. The driver loop `while let Some(result) = stream.next().await` pushes `(index, Ok(outcome))` into a `Vec` and, on the first `Err`, immediately returns `PoolFailure { index, source }` — the dropped stream cancels every in-flight job. Success sorts the collected vec by `index` (it may already be in completion order). `dep_version`/`dep_own_map` use `watch::Receiver::wait_for(|v| v.is_some())`, mapping channel-close to `None`. Document the module (`//!`) with the DAG/abort/cancellation contract and the send-after-resolve / send-after-process publication points.

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `RUSTC_WRAPPER= cargo test -p mediapm --lib -- conductor_bridge::sync::pool`
Expected: 6 passed.

- [ ] **Step 5: Run the crate suite for regressions**

Run: `RUSTC_WRAPPER= cargo test-pkg mediapm`
Expected: green (the module is additive; `mod pool;` is the only production touch).

- [ ] **Step 6: Commit**

Validate the message, then commit `feat(mediapm): add a dependency-aware provisioning pool` and verify with `git rev-parse HEAD` / `git log -1 --format=%s`.

---

### Task 2: Wire the pool into tool sync with fail-fast errors

**Files:**

- Modify: `src/mediapm/src/conductor_bridge/sync/mod.rs` — replace the two-level loops (`:1271-1360`), thread gates/publisher into `provision_entry` (`:166`), delete `provisioned_own_maps` (`:1268`, `:410`, `:422`, `:441`, `:449`, `:455`, `:542`, `:993`) and the `ResolveError`/`FetchError` outcomes, add `ProvisionFailure`.
- Modify: `src/mediapm/src/conductor_bridge/sync/provision.rs` — only if phase labels for `ProvisionFailure` live beside `fetch_and_import_tool_payload` (prefer keeping `ProvisionFailure` in `mod.rs`).
- Modify: `.agents/instructions/tool-sync-coordinator-and-identity.instructions.md` — rewrite "Two-level parallel provisioning" and the concurrency doc sentence.
- Modify: `.agents/coverage-matrix.md` — add the pool-test rows.
- Test: existing `src/mediapm/tests/int/tool_sync.rs` suites (adjust any test that pinned warning-continue for a required tool; note the intended change in the report).

**Interfaces:**

- Consumes: Task 1's `pool::{PoolJob, DepGates, DepPublisher, PoolFailure, run_pool}`.
- Produces: `ProvisionFailure { tool_id: String, phase: ProvisionPhase, source: MediaPmError }` with `enum ProvisionPhase { Resolve, Fetch, Process }`; error text `tool sync failed: {tool_id} ({phase}): {source}` (`phase` renders lowercase: `resolve`/`fetch`/`process`).
- [ ] **Step 1: Write the failing wiring test first**

In `src/mediapm/tests/int/tool_sync.rs`, add a hermetic fail-fast test: build a service whose desired tools include one whose provider resolve fails deterministically without network (use the existing test harness's cache overrides; a tool id resolvable only via a metadata-cache miss against an unreachable endpoint — follow the file's established hermetic patterns; if no deterministic resolve-failure seam exists in the harness, drive `reconcile_desired_tools` directly with a pre-seeded state that forces the resolve arm). Assert: the sync returns `Err` whose message contains `tool sync failed:` and the failing tool id, and that neither `conductor.generated.ncl` nor `state.json` changed on disk. Run it before wiring: it must fail (current code continues with a warning).

Run: `RUSTC_WRAPPER= cargo test -p mediapm --test int tool_sync_fail_fast`
Expected: FAIL (today's code warns and continues).

- [ ] **Step 2: Add `ProvisionFailure` and convert `provision_entry` to `Result`**

In `sync/mod.rs`: add the `ProvisionPhase` enum + `ProvisionFailure` struct (docs on both). Change `provision_entry` to return `Result<EntryOutcome, ProvisionFailure>`; delete the `ResolveError`/`FetchError` variants of `EntryOutcome` and their `apply_entry_outcome` warning arms (`:427`, `:605`). Resolve-arm failure becomes `Err(ProvisionFailure { tool_id, phase: Resolve, source })`; the fetch/process arms of `fetch_and_import_tool_payload`'s error path map to `Fetch`/`Process` at the call site.

- [ ] **Step 3: Thread gates and publisher through `provision_entry`**

Extend the signature with `gates: &pool::DepGates`, `publisher: &mut pool::DepPublisher`, and `dep_indices: &BTreeMap<String, usize>` (tool id → pool index for its same-step deps). Behavior:

- After `PreResolveOutcome` resolution and **before** the skip/composite check, `let mut dep_segments = BTreeMap::new(); for dep_id in collect_same_step_dep_ids(tool_id, tool_req, known_dependency_type) { if let Some(idx) = dep_indices.get(&dep_id) { if let Some(version) = gates.dep_version(*idx).await { dep_segments.insert(dep_id, version); } } }` — feed these into `compute_composite_canonical_version` in place of the live-state lookups.
- On every Ok path that resolved a canonical version (Resolved, Skip, SkipAfterResolve, FetchedNone), call `publisher.publish_version(bare_version.clone())` where `bare_version` is the pre-composite segment.
- In the `Fetched` arm, before `inline_same_step_deps` (`:452`): build `let mut dep_maps = BTreeMap::new(); for dep_id in <same-step ids> { if let (Some(idx), Some(map)) = (dep_indices.get(&dep_id), gates.dep_own_map(*idx).await) { dep_maps.insert(dep_id, map); } }` and pass `&dep_maps` — `inline_same_step_deps`'s signature is unchanged.
- After success (and in skip paths from the reconstructed spec runtime), `publisher.publish_own_map(own_map.clone())` with the **pre-inline** map for fetched tools and the reconstructed map for skips, mirroring today's `provisioned_own_maps` values exactly.
- In the inline loop, a same-step dep that published no map must still contribute nothing (keep `inline_same_step_deps`'s absent-dep `continue`) — the pool makes that unreachable on Ok paths, and the abort covers failures.
- [ ] **Step 4: Replace the two-level loops with one `run_pool` call**

Build jobs from the deduped `entries`: `same_step_deps` = indices of first entries-order matches for each same-step dep tool id (document the first-match rule). Delete `provisioned_own_maps` and the `own_map`/`content_map` inserts that fed it. Then:

```rust
let concurrency = default_tool_provision_concurrency().min(jobs.len()).max(1);
match pool::run_pool(jobs, concurrency, |job, gates, mut publisher| {
    provision_entry(&job.input, state, generated_doc, live_state, workspace_cas.as_ref(), cache, progress_group, recheck_policy, &gates, &mut publisher, &dep_indices).await
}).await {
    Ok(outcomes) => {
        for (idx, outcome) in outcomes {
            apply_entry_outcome(&entries[idx], outcome, &mut generated_doc, &mut tool_runtimes, &mut report, &mut live_state, &mut pruned_tools, inherited_env_vars, pb.as_ref());
        }
    }
    Err(pool_failure) => {
        if let Some(bar) = pb.as_ref() { bar.finish_error(); }
        return Err(MediaPmError::Workflow(format!(
            "tool sync failed: {} ({}): {}",
            entries[pool_failure.index].tool_id,
            pool_failure.source.phase(),
            pool_failure.source,
        )));
    }
}
```

(The exact borrows follow the current code; the point is: apply happens only on full success, and the failure arm writes nothing.)

- [ ] **Step 5: Run the wiring test and the suites**

Run: `RUSTC_WRAPPER= cargo test -p mediapm --test int tool_sync_fail_fast` → PASS.
Run: `RUSTC_WRAPPER= cargo test-pkg mediapm` and `RUSTC_WRAPPER= cargo test-pkg mediapm-conductor` → green. Any test pinned to warning-continue for a required tool is updated in the same commit with an inline note; report lists each.

- [ ] **Step 6: Update docs and coverage matrix**

`tool-sync-coordinator-and-identity.instructions.md`: replace "Two-level parallel provisioning" with the unified-pool description (one pool bounded by `default_tool_provision_concurrency`; waits only at composite/skip for dep versions and before inlining for dep own maps; first failure aborts the run, cancels in-flight work, writes nothing; apply order = entry index order, which equals the old dep-then-explicit order because dedup keeps dep entries first). Update the `MEDIAPM_TOOL_PROVISION_CONCURRENCY` sentence to "max tools resolved/fetched/processed at once". `coverage-matrix.md`: add the six pool rows and one row for the fail-fast integration test.

- [ ] **Step 7: Commit**

Validate the message, then commit `refactor(mediapm): drive tool sync from one pool and fail fast` and verify with `git rev-parse HEAD` / `git log -1 --format=%s`.

- [ ] **Step 8: Runtime verification (level-3, network)**

Run `RUSTC_WRAPPER= cargo run --package mediapm --example mediapm_demo_online`. Expected: with healthy upstreams, a full sync whose per-tool rows overlap across all six tools (no level stagger); with a failing upstream, an immediate `tool sync failed: <tool> (resolve): <cause>` error instead of the old misleading one. Record which outcome occurred in the task report — both are correct per the spec.
