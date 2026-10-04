---
status: not-started
committed: no
current-step: 1
inputs:
  atomicCommits: yes
  backwardsCompat: no
  maxConcurrency: 2
---

# The deferred coverage rows, grouped by owner

`.agents/coverage-matrix.md` still carries 25 rows marked `[missing]` or `[partial]`. A previous pass annotated each with the subsystem that owns it. Annotation is bookkeeping: the rows are open, and this plan is what the matrix's deferral note points at.

The counts below were read out of the matrix, not carried over:

```sh
git grep -c -E '^\|.*\[(missing|partial)\]' -- .agents/coverage-matrix.md   # 25
```

That is 17 `[partial]` and 8 `[missing]`. The 9/18 split quoted elsewhere counts the deferral sentence on line 3, which mentions both tokens in prose.

| group | rows | what closing it costs |
| --- | --- | --- |
| progress subsystem | 9 | two of them need a test seam that was declined before; the rest are label and layout assertions against existing harnesses |
| provider pipeline, extraction and repack helpers | 5 | one fixture shape reused across four rows, plus one endpoint assertion the removed test used to make |
| materializer path components, rename chain, commit path | 3 | string-level unit tests, no new harness |
| tool-sync coordinator | 2 | a seeded stale-entry fixture that no demo provides any more |
| config serde and Nickel parity | 2 | round trips that were removed rather than migrated |
| CLI layer | 2 | one end-to-end run, plus a unit row that stays open only because of it |
| suffix renderer, deleted string suffix API | 1 | a decision |
| inner renderer ANSI reset | 1 | a decision that closes as by-design |

## Two rows are decisions

Neither is a gap to fill. Writing a test for either would invent coverage.

**The deleted string suffix API** (matrix line 463) compares `set_suffix_components` with custom-only output against legacy `set_suffix(String)`. That API is gone. `git grep 'fn set_suffix' -- src/mediapm-utils` returns only `set_suffix_components`, in the trait, the renderer, the recording double and the module re-export. The comparison has no target, so the row cannot close as covered by any test.

Pick one and write down which: drop the row as no-longer-applicable, or restore the string API and keep a test that pins the two paths to identical output. What must not happen is the row sitting at `[missing]` indefinitely with no note, because a reader cannot tell whether the API is being kept.

**The client-truncation ANSI reset** (line 915) prepends `\x1b[0m` at `src/mediapm-utils/src/progress/inner/renderer.rs:1560`. Every route to the raw prefix string is closed: `InMemoryTerm` strips ANSI, the debug sink records no `prefix` field, `SlotCache::prefix` is private to `inner`, and `visible_width` cannot see a zero-width escape. A missing reset is a colour bleed, not a layout violation, so the layout harnesses cannot see it either.

Close it as by-design and mark the status so the word says so. A row marked `[covered]` would claim a test exists, which is the one thing this row is not.

## Order

The progress rows go first. They have no other owner, and they have now been deferred twice.

1. Progress subsystem, 9 rows.
2. Both decisions, 2 rows. Each is a small edit, and they retire the only two rows where writing a test would be the wrong move.
3. Provider pipeline, 5 rows. One fixture, one shape, four assertions reusing it.
4. Materializer path components, rename chain and commit path, 3 rows. String-level, no harness.
5. Config serde and Nickel parity, 2 rows.
6. Tool-sync coordinator, 2 rows. Needs a fixture that has to be written first.
7. CLI layer, 2 rows.

## Group 1: progress subsystem, 9 rows

Two rows live in the conductor-coordinator and materializer sections of the matrix, and they are grouped here because both are progress-bar rows rather than workflow or materialization behaviour. Line 873 asserts the conductor's overall bar finishes `FinishWarning` when `failed_steps > 0`; its test lands in `src/mediapm-conductor/tests/int/workflow_progress.rs`. Line 892 asserts the materializer's overall bar finishes `FinishWarning` when any entry is skipped; its test lands in `src/mediapm/src/materializer/mod.rs`. Line 859 is the pre-roll row, and the group table in the brief counted it a second time under `progress_output` preamble counts; it is one row.

Line 151 is the one that needs a new seam. The version segment of the fetch and process prefixes is built inside the download callback, which no hermetic test reaches. Closing it means reaching that callback without the network, either by extracting the label construction into something callable or by driving the bar through the callback path with a stub source. Either is a code change, not a test.

Line 431 already has its sweeps in `src/mediapm/examples/`. What is missing is the negative direction: a row that has neither label nor count, asserted to keep its timing.

Lines 904 and 906 are the shrink ladder, rules 3 and 5. `the_phase_tag_yields_first_and_the_file_name_after_it` is the only per-label order test that would catch a reordering, so line 906 wants the same treatment for the other two labels.

Lines 858, 861 and 859 assert existing behaviour against existing harnesses. Line 861 in particular must not be closed by deleting `sync_runs_every_phase_through_one_terminal`, since that integration test is currently the only thing pinning it.

## Group 2: provider pipeline, extraction and repack helpers, 5 rows

Lines 20, 210, 211, 212 and 215. Line 20 is the endpoint half of `full_pipeline_progress_monotonic`; the never-exceeds half is still asserted. Line 210 is the phase-endpoint assertion the removed process-phase budget test used to make. Lines 211 and 212 are the two halves of ZIP proportional estimation, endpoint exact and mid-entry approximate. Line 215 is the compress-overhead undercount.

Four of the five want one thing: a budget fixture that runs a source through fetch and process and can be read at each phase boundary. Built once, it closes all four, and the removed test's endpoint assertion comes back with it.

## Group 3: materializer path components, rename chain, commit path, 3 rows

Line 563 asks for a negative control proving a `Template` cannot reach a reader. The row already records why a green suite does not prove it: no reader takes `&[PathComponent]`. Line 570 is the `rename_files` replacement map, which has no production caller outside the path-component chain. Line 576 is the variant-name join, guarded by the same call as the ZIP member path that line's e2e test covers.

All three are unit tests in `src/mediapm/src/path_component.rs` and `src/mediapm/src/materializer/`. Nothing here needs a filesystem fixture.

## Group 4: config serde and Nickel parity, 2 rows

Line 361 is the `dependencies` map round trip. Two unit tests covering the flat and empty forms were removed and only the Nickel side was left, so the Rust boundary has no test. Line 948 is `sanitize_names` resolving `None` to `Inherit` at the boundary, which the row records as not yet added.

## Group 5: tool-sync coordinator, 2 rows

Line 340 is the name-match to `tools_updated` branch, which no test exercises. Line 344 is the aggregate: a seeded stale entry driving already-exists, then skip-preference, then prune-clear, then reprovision. The per-field contracts underneath stay covered, so only the aggregate is open.

The cost is the fixture. No demo seeds stale state any more, so a test has to build the stale `conductor.generated.ncl` plus a matching `state.json` itself. Line 340 falls out of the same fixture.

## Group 6: CLI layer, 2 rows

Line 862 is marked open only because of line 863, so the two close together or not at all. Line 863 wants a real `--no-progress` run asserted to draw no frame on stderr and no panic string in either captured stream. That means spawning the binary, which the unit suite in `service.rs` does not do.

## Not in this plan

The wall-clock dependence on indicatif's position token bucket and the frame comparison's spinner exemption have no code left to write; both are documented with tests pinning the wording.

The gate that reported a clean sweep without scanning anything is fixed in the preceding plan, not here.

## Gates

- `RUSTC_WRAPPER="" cargo test -p mediapm-utils --all-targets --all-features`, for the progress and suffix rows
- `RUSTC_WRAPPER="" cargo test -p mediapm-conductor --all-targets --all-features`, for the provider and coordinator rows
- `RUSTC_WRAPPER="" cargo test -p mediapm --all-targets --all-features`, for the materializer, config and tool-sync rows
- `RUSTC_WRAPPER="" cargo bin rumdl check .pi/plans/plan-2026-10-04T110000-the-deferred-coverage-rows.md`, and again over `.agents/coverage-matrix.md` when the rows are marked closed

Markdown lint runs through `cargo bin rumdl`, which needs `RUSTC_WRAPPER=""` like every other cargo invocation here and is not on PATH.

## Policy

Annotation is not coverage. A row closes when a named test asserts the spec item, or when the row is marked by-design with the reason written down. Re-annotating a row does not move it.
