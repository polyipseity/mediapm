# Progress desync, overall label, and media-tagger timeout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the stacked change-detection in the progress renderer so every redraw re-derives row alignment from the latest drawing; make the workflow screen's overall row a static `running workflows` label; and bound the MusicBrainz client the media-tagger builtin uses so one step fails fast instead of grinding into the 300s RPC timeout.

**Architecture:** Three independent, separately-committed changes. Task 1 deletes the per-slot dirty flag and the SlotCache style-dedup as gating in `mediapm-utils` and routes every draw path (including blank rows, attach shifts, and resize) through the one measure→settle→apply path that already runs unconditionally each tick; string/position dedup is kept purely as write suppression. Task 2 replaces the conductor's dynamic overall-row label with the static seed word `running workflows` and deletes the label machinery. Task 3 builds a bounded `musicbrainz_rs` client (timeout + capped retries) in the media-tagger builtin.

**Tech Stack:** Rust workspace (`mediapm-utils`, `mediapm-conductor`, `mediapm` crates), indicatif/console rendering, `musicbrainz_rs` 0.13.0 + `api_bindium` 0.4.0 (ureq), tokio tests plus wiremock (already a dev-dependency of `mediapm`).

**Spec:** `docs/superpowers/specs/2026-10-11-progress-desync-and-tagger-timeout-design.md` (read it before executing; it is the authority, this plan its argument).

## Global Constraints

- Every commit message passes `bun x commitlint` (Conventional Commits, mandatory scope: `type(scope): subject`, e.g. `refactor(progress): ...`). Validate before committing using the temp-dir install flow; rejections leave HEAD unmoved — fix and retry, never `--amend`.
- Never run `git reset` (any form). Never `git commit --amend`. Commits only, no push.
- Rust: immutable by default; no `.unwrap()`/`.expect()` on fallible domain paths; no `Box<dyn Any>`; no `#[allow]` (use `#[expect(..., reason = "...")]` only when unavoidable).
- Every touched Rust item (including private items and test helpers) carries doc comments; module docs stay current.
- Terminal-output tests must use exact `assert_eq!(term.contents(), concat!(...))` matching over the default `H=24, W=40` harness; capture actual output from a deliberately-wrong expectation rather than hand-computing.
- No network in tests. Wiremock serves any HTTP the tagged builtin performs.
- Build environment: prefix every cargo invocation with `RUSTC_WRAPPER= CARGO_BUILD_RUSTC_WRAPPER=` (sccache is blocked in this environment and fails the build otherwise).
- Selective validation only during development: `cargo test-pkg <crate>` (nextest, all-targets, all-features). Full-gate commands (`test-all`, `clippy-all`, `fmt-check`) are pre-push, not per-commit.
- Do not edit files outside the three tasks' scopes. Docs touched: `.agents/instructions/progress-output.instructions.md` only.

---

### Task 1: Size every redraw from the latest drawing (progress resync)

**Files:**

- Modify: `src/mediapm-utils/src/progress/inner/tracked.rs` — delete `dirty: AtomicBool` field (`:74`), its init (`:159`), and every `store(true, ...)` site (`:215-252`, `:346-428`, `:497`).
- Modify: `src/mediapm-utils/src/progress/inner/frame.rs` — `run_frame` (`:135-232`): drop the dirty read/clear (`:167`), the resize force-dirty loop (`:152-160`); make the slot loop unconditional; keep the `resized` handling for the owed-draw `pushed` accounting only; drop the `dirty` field from `DebugSlotState` population (`:96-101`).
- Modify: `src/mediapm-utils/src/progress/inner/renderer.rs` — delete `SlotCache` `style_*` fields (`:179-212`) and the `style_changed` gate (`:379-398`) so `sync_slot` always applies the settled style; keep prefix/suffix/total/position caches as write suppression; keep `blank_bar` skip via `is_blanked()` (`:416-438`) but render blank rows through the frame budget instead of `{wide_msg}`.
- Modify: `src/mediapm-utils/src/progress/inner/components.rs` — `blank_bar_style()` (`:946`) replaced by the frame template at the settled widths with empty prefix/suffix; remove `DebugSlotState.dirty` (`:160-161`).
- Modify: `src/mediapm-utils/src/progress/inner/layout.rs` — `recompute_layout` (`:351-482`) keeps its unconditional per-slot re-sync; drop now-dead comments about the dirty-slot sync.
- Modify: `src/mediapm-utils/src/progress/inner/debug.rs` — drop the `dirty` field from the JSONL event shape.
- Test: `src/mediapm-utils/src/progress/tests/renderer.rs` — replace `dirty_tracking_*` tests (`:7`, `:29`, `:111`, `:182`) with characterization tests that pin the new contract.
- Test: `src/mediapm-utils/tests/progress_output/render.rs`, `layout.rs`, `debug.rs` — add the characterization tests named below.
- Docs: `.agents/instructions/progress-output.instructions.md` — update the "Buffer ordering and style dedup" and "Debug JSONL" sections.

**Interfaces:**

- Consumes: the existing `run_frame`/`recompute_layout`/`sync_slot` frame pipeline (unchanged signatures except dropped fields).
- Produces: no public API change; `ProgressBarApi`/`ProgressScreenApi` signatures are identical, so consumers (`mediapm-conductor/src/orchestration/coordinator.rs`, `src/mediapm/src/materializer/`) need no edit.
- [ ] **Step 1: Write the characterization test — elapsed growth resizes every row in the same frame**

Add to `src/mediapm-utils/tests/progress_output/render.rs`, following that file's existing harness (test time source + `InMemoryTerm`, `ticker` off). Build a screen with an overall bar plus two child bars of different suffix lengths; advance the test clock so the shorter suffix grows past a length boundary (`0s` → `4m18s`) without mutating any bar state except one bar's position; tick once; assert with exact `concat!(...)` matching that every row's bar ends at the same column and shares one `prefix_w`/`suffix_w`. Write it with a deliberately-wrong expected string first.

- [ ] **Step 2: Write the characterization test — a blank row draws inside the budget**

Add to `src/mediapm-utils/tests/progress_output/render.rs` (or `layout.rs`): create a screen with capacity larger than the bound bars so one slot is unbound, tick, and assert the unbound row occupies a budgeted-width empty line (the old `{wide_msg}` output is full-width and must no longer be pinned). Again write the expectation wrong first, capture the actual, re-run to pass.

- [ ] **Step 3: Run the two new tests to see current behavior**

Run: `RUSTC_WRAPPER= cargo test -p mediapm-utils --features progress --test progress_output -- render`
Expected: both tests fail or pass per current behavior — record which, and capture actual outputs for Step 4. The blank-row test is the one expected to change behavior (`{wide_msg}` → budgeted empty line).

- [ ] **Step 4: Remove the dirty flag and the style-dedup gating**

Apply the `Files:` edits above. `run_frame`'s step 4 iterates every bound slot unconditionally; `sync_slot` always applies the settled style and then calls `sync_snapshot_to_bar` (which keeps its string/position write suppression); blank rows go through the frame template at the settled widths with an empty prefix/suffix (`draw_fill = false` shape); `DebugSlotState` loses `dirty`.

- [ ] **Step 5: Replace the `dirty_tracking_*` tests**

In `src/mediapm-utils/src/progress/tests/renderer.rs`: delete `dirty_tracking_initial_state_starts_dirty`, `multiple_mutations_before_tick_single_draw`, `dirty_tracking_skips_clean_ticks`, `dirty_tracking_draws_on_mutation`; replace with (a) `clean_ticks_draw_the_same_pixels_with_fewer_pushes` — two no-mutation ticks produce byte-identical content, and the write count is asserted through the recording/gate mechanism the file already uses; (b) `mutations_still_reach_the_next_tick`.

- [ ] **Step 6: Run the affected suites**

Run:

```text
RUSTC_WRAPPER= cargo test-pkg mediapm-utils
RUSTC_WRAPPER= cargo test-pkg mediapm-conductor
```

Expected: all green. If any exact-`concat!` test pins the old blank-row or dedup behavior, read its expectation, decide whether it pins a contract that moved (update in this commit, noting it in the report) or a real regression (fix the implementation).

- [ ] **Step 7: Run the three progress screen examples' embedded tests**

Run:

```text
RUSTC_WRAPPER= cargo test -p mediapm --example mediapm_progress_tool_sync
RUSTC_WRAPPER= cargo test -p mediapm --example mediapm_progress_workflow
RUSTC_WRAPPER= cargo test -p mediapm --example mediapm_progress_materialize
```

Expected: transcripts still match frame-for-frame (this is the strongest guard that pixel output did not move).

- [ ] **Step 8: Update the progress output instructions doc**

In `.agents/instructions/progress-output.instructions.md`: rewrite "Buffer ordering and style dedup" to state that every tick re-measures, re-settles and re-applies for every slot, that string/position dedup survives only as write suppression, and that blank rows draw through the frame budget; drop the `dirty` field from the "Debug JSONL" section's field list.

- [ ] **Step 9: Commit**

Verify the message with commitlint (temp-dir install flow from the workspace instructions), then:

```bash
git add -A && git commit -m "refactor(progress): size every redraw from the latest drawing"
```

Verify with `git rev-parse HEAD` and `git log -1 --format=%s` that the commit was created with the intended message; if a hook reformatted files, re-stage and retry with a fresh commit (never `--amend`).

---

### Task 2: Static `running workflows` overall row

**Files:**

- Modify: `src/mediapm/src/service.rs:1269` — seed `with_overall("workflow", 1)` becomes `with_overall("running workflows", 1)`.
- Modify: `src/mediapm-conductor/src/orchestration/coordinator.rs` — delete `OverallBarState` (`:230-286`), `overall_bar_label` (`:286-343`), the `set_overall_label` closure (`:426-428`) and every call site (`:552-555`, `:638-668`); the overall bar keeps only `set_total` (`:410-418`). Delete any unit tests that pin the label states.
- Modify: `src/mediapm/examples/support/workflow.rs` — the workflow example's overall seed becomes `running workflows` (same literal as the service seed).
- Regenerate: `src/mediapm/examples/fixtures/mediapm_progress_workflow/*.txt` — all 30 transcripts.
- Docs: `.agents/instructions/progress-output.instructions.md` Screen B section.

**Interfaces:**

- Consumes: Task 1's untouched frame pipeline; no Task-1 symbols.
- Produces: the only overall-row text in the product is the literal `running workflows`; worker-row labels (`WorkerBarLabel`) are untouched.
- [ ] **Step 1: Regenerate the workflow transcripts against the new seed**

Change the seed in `src/mediapm/examples/support/workflow.rs` and `src/mediapm/src/service.rs:1269` to `running workflows`, then capture all 30 fixtures with the documented capture procedure (one `cargo run -q --package mediapm --example mediapm_progress_workflow -- --scenario <scenario> --width <w>` redirect per file, scenarios `baseline`, `dense`, `states`; widths 20 30 40 47 50 60 70 72 80 100 120). Prefix `RUSTC_WRAPPER=`. Verify the example's embedded test still fails-then-... no: it must pass after capture — run `RUSTC_WRAPPER= cargo test -p mediapm --example mediapm_progress_workflow`.

- [ ] **Step 2: Write the failing coordinator test first**

In `src/mediapm-conductor/src/orchestration/coordinator.rs` tests (or the coordinator's existing test module): replace any test that installs/asserts an overall label with one that asserts the overall bar never receives a truncation/label install and receives `set_total(step_count)` exactly once per dispatch. Run it before deleting the machinery:
Run: `RUSTC_WRAPPER= cargo test -p mediapm-conductor coordinator`
Expected: RED (old code sets labels on the overall bar).

- [ ] **Step 3: Delete the label machinery**

Remove `OverallBarState`, `overall_bar_label`, the `set_overall_label` closure, and its call sites per `Files:`; keep worker-slot labeling (`worker_slot_label`, `WorkerBarLabel`) untouched. Confirm `StepBarLabel` is still used by the workflow example's step rows; if nothing in `progress_labels.rs` uses it after this change, note that in the report rather than deleting it (the example's step rows consume it).

- [ ] **Step 4: Run the coordinator and service suites**

Run:

```text
RUSTC_WRAPPER= cargo test-pkg mediapm-conductor
RUSTC_WRAPPER= cargo test-pkg mediapm
```

Expected: green; the workflow example tests in the `mediapm` suite match the regenerated transcripts.

- [ ] **Step 5: Update the Screen B documentation**

In `.agents/instructions/progress-output.instructions.md` Screen B section: replace the overall-row description with the static `running workloads`-row wording — the pinned row shows only `running workflows` and the run progress count, and worker rows name the running step/tool/`[active]`. Remove any text that says the overall row re-installs `StepBarLabel` at each dispatch.

- [ ] **Step 6: Commit**

Validate the message, then commit `feat(conductor): name the overall workflow row once` and verify with `git rev-parse HEAD` / `git log -1 --format=%s`.

---

### Task 3: Bound the MusicBrainz client used by the media-tagger builtin

**Files:**

- Modify: `src/mediapm/src/builtins/media_tagger/musicbrainz.rs` — `fetch_musicbrainz_payloads` stops calling `execute_async()` (which uses the global `MUSICBRAINZ_CLIENT`: `ureq::agent()` with no timeout, `max_retries = 10`, quadratic backoff) and instead routes through a bounded client.
- Create/modify: a small seam in the same module (e.g. `pub(super) fn musicbrainz_client() -> MusicBrainzClient`) that builds the client from env-derived configuration.
- Test: `src/mediapm/src/builtins/media_tagger/musicbrainz.rs` inline `#[cfg(test)]` and/or `src/mediapm/tests/int/` using the existing wiremock dev-dependency.
- Docs: `src/mediapm/src/builtins/media_tagger/AGENTS.md` if it documents the MB path (create none). No change to `.agents/instructions/` unless a stated default moves.

**Interfaces:**

- Consumes: `MEDIAPM_HTTP_TIMEOUT_SECONDS` parsing rules from `src/mediapm-conductor/src/http/client.rs` (floor 30s, default 1800s request timeout) — reuse the same helper semantics but the MB client uses its own, tighter default: 30s per request, `max_retries = 2`.
- Produces: `fetch_musicbrainz_payloads` remains the single entry point; no signature change.

Key upstream facts (verified):

- `musicbrainz_rs = { workspace = true, features = ["async", "rustls"], default-features = false }` at `src/mediapm/Cargo.toml:47`.
- `MusicBrainzClient::new(user_agent)` builds `ureq::Agent::new_with_config(Config::builder().user_agent(..).build())` — no `timeout_global`.
- `api_bindium::ApiClient` fields: `agent` (default `ureq::agent()`), `max_retries: u32` (default `10`); `rate_limit` exists only behind its `rate_limit` feature (off).
- `MusicBrainzClient::builder().api_client(ApiClient::builder().agent(agent).max_retries(n).build()).musicbrainz_domain(..)` is available; use `FetchQuery::execute_with_client_async(&client)` (public) instead of `execute_async()`.
- `musicbrainz_rs::api_bindium` is re-exported (`musicbrainz_rs-0.13.0/src/lib.rs:70`), so no new crate dependency is needed.
- ureq 3.x config offers `timeout_global(Some(Duration))`.
- [ ] **Step 1: Write the failing test — bounded request timeout**

Add a hermetic test that mounts a wiremock server which delays its MB response, calls the fetch path with a client pointed at the mock (`musicbrainz_domain` override; if the URL scheme is https-only, instead unit-test the client seam and assert the agent's configured timeout and retry count), and asserts the request fails within a bounded wall time and after at most 3 attempts (count the mock's received requests). Write the expectation wrong first if it is an exact-match assertion; for the timing assertion, assert `elapsed < 90s` and `requests <= 3`.

- [ ] **Step 2: Write the failing test — capped retries on 503**

Add a hermetic test: wiremock always answers 503 (with and without `Retry-After`); assert the fetch fails after exactly 3 attempts total within a bounded time, and that the stale-cache fallback path still returns the cached payload when one exists (the existing `Ok/Err` match at the end of `fetch_musicbrainz_payloads` keeps that behavior).

- [ ] **Step 3: Run the two tests**

Run: `RUSTC_WRAPPER= cargo test -p mediapm --features media-tagger musicbrainz_bounded`
Expected: FAIL against the current unbounded client (the 503 test takes >10 attempts; the timeout test hangs or exceeds the bound). If the hang makes the run impractical, stop the run and record the observed timeout behavior in the report as the RED evidence.

- [ ] **Step 4: Implement the bounded client**

In `musicbrainz.rs`: add `musicbrainz_client()` building `ureq::Config::builder().user_agent(<keep the same UA string the crate used, e.g. "mediapm/<version> (+https://github.com/mediapm/mediapm)">).timeout_global(Some(<env-derived or 30s>))`, an `ApiClient::builder().agent(agent).max_retries(2).build()`, and a `MusicBrainzClient` from those; pass it via `execute_with_client_async(&client)` on both the recording and release fetch chains. The timeout reads `MEDIAPM_HTTP_TIMEOUT_SECONDS` with the documented 30s floor and a tighter default of 30s for this path (document the divergence from the 1800s conductor default in a doc comment citing this plan).

- [ ] **Step 5: Run the affected suites**

Run:

```text
RUSTC_WRAPPER= cargo test-pkg mediapm --features media-tagger
RUSTC_WRAPPER= cargo test-pkg mediapm-utils
```

Expected: green. Then run a real-path smoke check of the builtin's bounded behavior: `RUSTC_WRAPPER= ACOUSTID_API_KEY= cargo run -q --package mediapm -- builtin media-tagger --help` must not be used as evidence; instead run the offline example `cargo run -q --package mediapm --example mediapm_demo` (the offline demo invokes no managed binary but compiles the builtin) and record its output in the report. A network run of the online demo is out of scope for this task.

- [ ] **Step 6: Commit**

Validate the message, then commit `fix(mediapm): bound the MusicBrainz client and its retries` and verify with `git rev-parse HEAD` / `git log -1 --format=%s`.

---

## Definition of done

- All three commits exist on `main` (per explicit user consent to work on the main branch), each individually green under `cargo test-pkg <crate>` for the crates it touches, each message passing commitlint.
- `.agents/instructions/progress-output.instructions.md` describes the shipped behavior in both touched areas.
- The final whole-branch review (dispatched by the controller) reports no unresolved Critical or Important findings.
