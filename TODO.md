# TODO: Remove the demo full-sync test gates

## IMPORTANT: When this plan may be executed

This document is an execution plan for **removing** the temporary `#[ignore]` gates on the demo full-sync example tests.

**Do not execute this plan unless the user explicitly asks to remove the gates.** Never run it implicitly, never as a side effect of another task, never automatically. If a task (test fix, feature work, doc update, refactor) happens to touch these gates, leave them in place. If the user invokes this plan, confirm with the user first that they want the gates removed, then follow the procedure below exactly.

## Current state

Three full-sync demo example tests are temporarily gated behind `#[ignore]` because the full-sync pipeline is blocked by Stream A stubs (unimplemented provisioning/materialization paths). They are recorded as "skipped" in nextest summaries. The rest of the suite is green: 1335 passed / 0 failed / 3 skipped (ignored) as of HEAD.

Gated tests:

| Test | `#[ignore]` rationale | Failure when un-ignored |
| --- | --- | --- |
| `mediapm::example/mediapm_demo tests::main_is_exercised` | full-sync demo blocked by Stream A stubs: materializer emits no output and `managed_files` is never populated (`managed output ... missing from lockfile tracking`); see `TODO.md` — remove only on explicit user request | `managed output 'music videos/Rick Astley - Never Gonna Give You Up [demo.local.dQw4w9WgXcQ]/Rick Astley - Never Gonna Give You Up [demo.local.dQw4w9WgXcQ].m4a' missing from lockfile tracking` |
| `mediapm::example/mediapm_demo tests::generate_demo_artifacts_writes_manifest_and_import_metadata` | full-sync demo blocked by Stream A stubs: materializer emits no output and `managed_files` is never populated (`managed output ... missing from lockfile tracking`); see `TODO.md` — remove only on explicit user request | `demo artifact generation: Custom { kind: Other, error: "managed output 'music videos/Rick Astley - Never Gonna Give You Up [demo.local.dQw4w9WgXcQ]/Rick Astley - Never Gonna Give You Up [demo.local.dQw4w9WgXcQ].m4a' missing from lockfile tracking" }` |
| `mediapm::example/mediapm_demo_online tests::main_is_exercised` | full-sync online demo blocked by Stream A stubs: machine workflows never synthesized (`machine config is missing managed workflow 'mediapm.media.youtube.dQw4w9WgXcQ'`); see `TODO.md` — remove only on explicit user request | `example main should run to completion: "machine config is missing managed workflow 'mediapm.media.youtube.dQw4w9WgXcQ'"` |

The online test's in-body CI skip is **standing policy** (nondeterministic online path must not run in CI), not part of the temporary gates. The offline test has no CI skip at all — it always runs full sync once un-ignored.

## Root causes (Stream A stubs)

The full-sync path fails because provisioning/materialization is stubbed:

1. **`variant_hashes` never populated** — the provisioning pipeline that maps each media step to resolved input/output content variants is not wired. The materializer runs `sync_hierarchy` with an empty variant map, so it emits no output.
   - Marker: `// TODO: Stream A stubs — wired when provisioning pipeline is complete.` in:
     - `src/mediapm/src/config/validation/sources.rs`
     - `src/mediapm/src/config/validation/mod.rs`
     - `src/mediapm/src/config/validation/hierarchy.rs`
     - `src/mediapm/src/config/versions/mod.rs`
     - `src/mediapm/src/conductor_bridge/tool_runtime/template.rs`
     - `src/mediapm/src/tools/workflows/mod.rs`
     - `src/mediapm/src/tools/workflows/ffmpeg.rs`
     - `src/mediapm/src/tools/workflows/yt_dlp.rs`
     - `src/mediapm/src/tools/workflows/rsgain.rs`
     - `src/mediapm/src/tools/workflows/media_tagger.rs`
2. **`managed_files` never populated** — `MediaPmState.managed_files` (`BTreeMap<String, ManagedFileRecord>`, `src/mediapm/src/config/mod.rs`) is only read by the demos and state tests; no production code writes it. The demo's `assert_materialized_output_hardlinked_to_cas` (`src/mediapm/examples/mediapm_demo.rs`) requires each materialized output to be tracked in the lockfile.
3. **Machine `workflows` synthesis unwired** — `conductor.generated.ncl` ends up with `workflows: []` after `clear_machine_workflows`/reconcile. The online demo's `assert_demo_workflow_shape` (`src/mediapm/examples/mediapm_demo_online.rs`) requires a managed workflow named `mediapm.media.<media-id>` with the demo's step shape (yt-dlp ingest, ffprobe, ffmpeg transcode, rsgain, media-tagger).
4. **`executed_instances: 0` stub** — `src/mediapm/src/service.rs` line 1005: `executed_instances: 0, // stub: conductor not yet wired for full sync`. Full-sync workflow execution is not wired, so sync summaries report zero executed instances.

## Removal procedure

Follow this exact order. After each step that produces a failing/red state, leave the tree red only long enough to verify, then proceed — the final state must be fully green.

1. **Implement the root causes** (this is the substantive work; the gates are only removed once the full-sync path actually works):
   - Wire the provisioning pipeline so `variant_hashes` is populated per media step before materialization. Remove the `// TODO: Stream A stubs` markers when each file's stub is replaced with real logic.
   - Populate `MediaPmState.managed_files` for every materialized output (both primary and secondary/untagged outputs) so `assert_materialized_output_hardlinked_to_cas` finds them tracked.
   - Synthesize managed workflows (`mediapm.media.<media-id>`) into `conductor.generated.ncl` so `machine.workflows` is non-empty after sync and `assert_demo_workflow_shape` passes.
   - Wire real workflow execution so `executed_instances` reflects executed steps (remove the `service.rs` stub comment).
2. **Remove the `#[ignore]` attributes** from the three tests in `src/mediapm/examples/mediapm_demo.rs` and `src/mediapm/examples/mediapm_demo_online.rs`.
3. **Restore/verify the full-sync assertions** in `generate_demo_artifacts_writes_manifest_and_import_metadata`:
   - `tool_update_precheck_executed` is asserted `Some(true)` (full-sync run) — keep.
   - Materialization assertions now expect real output: `materialized_primary_exists` / `materialized_secondary_exists` true, `lock_managed_files_count` populated, hardlink-to-CAS checks pass.
   - If any assertion is unreachable pre-sync (e.g. config-doc assertions that were shadowed by the earlier failure), move them to run after sync or split them into a separate test — do not silently drop coverage.
4. **Run targeted verification** — un-ignored only (this matches the Phase-1 red verification):
   - `cargo run --package cargo-bin -- cargo-nextest run -p mediapm --all-targets --all-features -E 'test(main_is_exercised) or test(generate_demo_artifacts_writes_manifest_and_import_metadata)'`
   - Expect all three to PASS (the offline two fully; the online one locally, skipping in CI by standing policy).
5. **Run the full suite**:
   - `cargo run --package cargo-bin -- cargo-nextest run --workspace --all-targets --all-features`
   - Expect 1338 passed / 0 failed / 0 skipped (the online CI-skip is standing policy and only engages in CI; locally it runs full sync).
6. **Update the docs** to remove the temporary-gate language:
   - `.agents/instructions/example-execution-policy.instructions.md` — remove the "Temporarily-gated tests use `#[ignore]`" section (or mark resolved per user preference).
   - `.agents/instructions/rust-conventions.instructions.md` — remove the gate/`TODO.md` sentence in "CI auto-detection in demos".
   - `.agents/instructions/sdd-tdd-workflow.instructions.md` — remove the gate notes from the coverage rows and the full-suite row's "3 ignored" language.
   - This `TODO.md` — delete it (its purpose is served) or mark it resolved per user preference.
7. **Commit** with a Conventional Commit message, e.g. `feat(mediapm): wire provisioning pipeline for full-sync materialization` plus a follow-up `test(mediapm): un-ignore full-sync demo tests` if the substantive and test changes land separately.

## Constraints

- The plan in this file is executed ONLY when the user explicitly asks. Never implicitly, never as a side effect of another task.
- Keep the full suite green at the end of the procedure. A red intermediate state is acceptable only while verifying the gates are the only blockers.
- Preserve the offline demo's no-CI-skip policy and the online demo's CI-skip standing policy.
- Use Conventional Commits with mandatory scope; do not amend commits.
