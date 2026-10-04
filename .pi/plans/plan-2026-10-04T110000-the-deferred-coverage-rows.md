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

Each row below is cited by the leading cell of its matrix row, which is what a reader searches for. The matrix line number rides along as a convenience only, because a sibling commit has already moved eleven of them once. Where the matrix sets a short label in front of the description with a dash, or trails the description with one, the citation starts and stops at the description so the quoted text stays greppable.

| group | rows | what closing it costs |
| --- | --- | --- |
| progress subsystem | 9 | one is a version-segment assertion on a sync test that is already hermetic; the rest are label and layout assertions against existing harnesses |
| suffix renderer and inner renderer | 2 | a decision each, one of them a status token that does not exist yet |
| provider pipeline, extraction and repack helpers | 5 | one fixture shape reused across four rows, plus one endpoint assertion the removed test used to make |
| materializer path components, rename chain, commit path | 3 | string-level unit tests, no new harness |
| config serde and Nickel parity | 2 | round trips that were removed rather than migrated |
| tool-sync coordinator | 2 | a seeded stale-entry fixture that no demo provides any more |
| CLI layer | 2 | one end-to-end run, plus a unit row that stays open only because of it |

The table is in the order the list below runs them.

## Two rows are decisions

Neither is a gap to fill. Writing a test for either would invent coverage.

**The deleted string suffix API** (matrix row `set_suffix_components` with custom-only produces output identical to legacy `set_suffix`, line 463) compares `set_suffix_components` with custom-only output against legacy `set_suffix(String)`. That API is gone. `git grep 'fn set_suffix' -- src/mediapm-utils` returns only `set_suffix_components`, in the trait, the renderer, the recording double and the module re-export. The comparison has no target, so the row cannot close as covered by any test.

Pick one and write down which: drop the row as no-longer-applicable, or restore the string API and keep a test that pins the two paths to identical output. What must not happen is the row sitting at `[missing]` indefinitely with no note, because a reader cannot tell whether the API is being kept.

**The client-truncation ANSI reset** (matrix row The client-truncation `\x1b[0m` reset is present, line 916) prepends `\x1b[0m` at `src/mediapm-utils/src/progress/inner/renderer.rs:1560`. Every route to the raw prefix string is closed: `InMemoryTerm` strips ANSI, the debug sink records no `prefix` field, `SlotCache::prefix` is private to `inner`, and `visible_width` cannot see a zero-width escape. A missing reset is a colour bleed, not a layout violation, so the layout harnesses cannot see it either.

### The by-design token has to be added before a row can carry it

`.agents/instructions/sdd-tdd-workflow.instructions.md` defines three status values and no fourth: `[covered]`, `[partial]` and `[missing]`, listed at lines 60 to 62 and named again at line 22. There is no by-design token anywhere, so "close it as by-design" is not an action a session can take against the matrix today.

The token is `[by-design]`, it goes in that instruction file's status list beside the other three, and it is only usable once it is there. `[covered]` is not a substitute: it claims a test exists, which is the one thing the ANSI reset row is not. So the decision is two edits in this order: add `[by-design]` to the instruction file, then mark the row `[by-design]: every route to the raw prefix string is closed; a seam was declined because a missing reset is a colour bleed, not a layout violation`. A row closed this way has not been tested and does not claim to be.

## Order

The progress rows go first. They have no other owner, and they have now been deferred twice.

1. Progress subsystem, 9 rows.
2. Both decisions, 2 rows. Each is a small edit, and they retire the only two rows where writing a test would be the wrong move. Doing them second rather than last means they stop sitting open while the test work runs.
3. Provider pipeline, 5 rows. One fixture, one shape, four assertions reusing it, so nothing else can start while it is built.
4. Materializer path components, rename chain and commit path, 3 rows. String-level unit tests, no fixture and no shared state, so this group can run beside step 3 rather than after it. It sits fourth only so the plan reads in group order.
5. Config serde and Nickel parity, 2 rows. Two round trips in one file, and one of the two is at the mercy of another slice, so it is cheap to start and cheap to pause.
6. Tool-sync coordinator, 2 rows. This is the first group that needs a fixture written before any row in it can close, so it starts after the groups that need none and its fixture lands in one place.
7. CLI layer, 2 rows. Last because it is the only group that spawns a process, which makes it the slowest to iterate on and the hardest to run hermetically.

## Group 1: progress subsystem, 9 rows

Two of these rows live in the conductor-coordinator and materializer sections of the matrix, and they are grouped here because both are progress-bar rows rather than workflow or materialization behaviour. The matrix row Overall bar finishes `finish_warning` when `failed_steps > 0`, `finish_success` otherwise (line 874) asserts the conductor's overall bar warning finish; its test lands in `src/mediapm-conductor/tests/int/workflow_progress.rs`. The matrix row Overall `finish_warning` when any entry is skipped, `finish_success` otherwise (line 893) asserts the materializer's equivalent; its test lands in `src/mediapm/src/materializer/mod.rs`. Folding them in is a choice, and the conductor row carries it while the materializer row is thinner, because its test lands in `materializer/mod.rs` rather than a progress module and its only claim on this group is that the assertion is about a progress bar. A session that would rather follow the matrix annotations can split that row out, which leaves the progress group at 8 and the materializer group at 4 and still totals 25.

The matrix row `BarStyle::WorkerSpinner` 0/0 guard, the client-truncation path, and the pre-roll output shape preserved (line 860) is the pre-roll row. The group table in the brief counted it a second time under `progress_output` preamble counts; it is one row.

### Row 151 is a test, not a code change

The matrix row Fetch/process bars carry the phase tag (`[fch]`/`[pro]`) and version via `set_prefix_components` (line 151) records that the version segment of the fetch and process prefixes has no test, and gives its reason: those bars are built inside the download callback, which no hermetic test reaches. That reason is the matrix's, and the code does not support it. `set_prefix_components` sits at `src/mediapm/src/conductor_bridge/sync/provision.rs:307` and `:352`, inside the two provider progress callbacks, and `sync_multi_tool_per_tool_bars_are_order_independent` (`src/mediapm/src/conductor_bridge/sync/mod.rs:3761`) already drives both of them hermetically: `seed_two_tool_cache` (`src/mediapm/src/conductor_bridge/sync/mod.rs:3710`) seeds the yt-dlp metadata and payloads so the fetch never reaches the network. The test passes today:

```sh
RUSTC_WRAPPER="" cargo test -p mediapm --lib sync_multi_tool_per_tool_bars_are_order_independent -- --nocapture
```

`RecordingProgressTracker` records `ProgressOp::SetPrefixComponents` (`src/mediapm-utils/src/progress/recording.rs:94`), so closing this row is reading the recorded ops for the two bars and asserting their `version` field. No production seam is involved.

What the existing test does not do is assert the version, so the run above cannot settle whether that field is non-empty in this fixture. The test parses each `AddBar` label into a tool id from the first space-delimited token and a phase from the bracketed tail, which discards whatever sits between them, so its assertions pass whether or not a version was there. Before budgeting any code change, confirm the version is non-empty. Reading the two provider arms says it is: `resolve_tool_fetch` (`src/mediapm/src/tools/provider/mod.rs:372`) sets `human_readable_version` to the resolved tag for yt-dlp, and `mod.rs:462` sets it to `{CARGO_PKG_VERSION}+{MEDIAPM_GIT_HASH}` for media-tagger, so both tools in this fixture report one. A session that would rather not take the provider arms on trust can write the one-line assertion on the recorded `SetPrefixComponents` version instead, which is both the confirmation and the assertion that closes the row.

### The rest of the group

The matrix row `A row that would have nothing left without its timing keeps it rather than rendering a lone spinner` (line 431) already has its sweeps in `src/mediapm/examples/`. What is missing is the negative direction: a row that has neither label nor count, asserted to keep its timing.

The matrix rows `Rule 3 - once no segment can shrink further, whole segments drop from the tail until the remainder fits` (line 905) and `Rule 5 - per-label segment order is the policy` (line 907) are the shrink ladder. `the_phase_tag_yields_first_and_the_file_name_after_it` is the only per-label order test that would catch a reordering, so line 907 wants the same treatment for the other two labels.

The matrix row production combination: injected screen together with `overall_bar: Some` and prune candidates, with no unit pin (line 862) states its own gap: no unit test drives `reconcile_desired_tools` with an overall handle plus a prune bar, and nothing asserts the prune bar still lands on the screen when an overall handle is supplied. That row must not be closed by deleting `sync_runs_every_phase_through_one_terminal`, which is currently the only thing pinning it.

The other two rows, per-screen width layout and its clamps unchanged (line 859) and `BarStyle::WorkerSpinner` 0/0 guard, the client-truncation path, and the pre-roll output shape preserved (line 860), name the tests they have and stop there. Neither the test cell nor the status cell says what assertion is missing; both status cells read `still open, and it stays with the progress work`. Whoever picks either one up has to read the gap off the row rather than take it from here, because writing a plausible missing assertion into this plan would be a guess wearing a citation.

## Group 2: provider pipeline, extraction and repack helpers, 5 rows

The matrix rows `Regression test suite` (line 20), `Position equals total at endpoint of each phase` (line 210), `ZIP proportional estimation: endpoint exact` (line 211), `ZIP proportional estimation: mid-entry approximate` (line 212) and `Compress ZIP metadata overhead (~KB) vs payload` (line 215). Line 20 is the endpoint half of `full_pipeline_progress_monotonic`; the never-exceeds half is still asserted. Line 210 is the phase-endpoint assertion the removed process-phase budget test used to make. Lines 211 and 212 are the two halves of ZIP proportional estimation, endpoint exact and mid-entry approximate. Line 215 is the compress-overhead undercount.

Four of the five want one thing: a budget fixture that runs a source through fetch and process and can be read at each phase boundary. Built once, it closes all four, and the removed test's endpoint assertion comes back with it.

## Group 3: materializer path components, rename chain, commit path, 3 rows

The matrix rows A flattened entry's components are one of two variants (line 563), `rename_files` replacement strings are sanitized with the configured replacement map (line 570) and The extracted ZIP member path and the non-archive variant name (line 576). Line 563 asks for a negative control proving a `Template` cannot reach a reader. The row already records why a green suite does not prove it: no reader takes `&[PathComponent]`. Line 570 is the `rename_files` replacement map, which has no production caller outside the path-component chain. Line 576 is the variant-name join, guarded by the same call as the ZIP member path that the row's e2e test covers.

All three are unit tests in `src/mediapm/src/path_component.rs` and `src/mediapm/src/materializer/`. Nothing here needs a filesystem fixture.

## Group 4: config serde and Nickel parity, 2 rows

The matrix row `dependencies` flattened to `BTreeMap<String, ConfigVersionSpec>`, serde round-trip (line 361) is the `dependencies` map round trip. Two unit tests covering the flat and empty forms were removed and only the Nickel side was left, so the Rust boundary has no test.

The matrix row (b) `sanitize_names` optional at boundary (line 949) records `sanitize_names` resolving `None` to `Inherit` at the boundary, and its test cell reads `(to be added by the sanitize_names boundary slice)`. That is a scheduling risk rather than a cost: if that slice is in flight, this row closes inside it, and a session working this plan independently would duplicate the work or race it. Confirm the state of the slice before starting, and if it is live, leave line 949 to it and take line 361 alone.

## Group 5: tool-sync coordinator, 2 rows

The matrix rows `name` = bare logical tool id (line 340) and `Whole seed drives update-precheck flow (skip-preference + prune + reprovision)` (line 344). Line 340 is the name-match to `tools_updated` branch, which no test exercises. Line 344 is the aggregate: a seeded stale entry driving already-exists, then skip-preference, then prune-clear, then reprovision. The per-field contracts underneath stay covered, so only the aggregate is open.

The cost is the fixture. No demo seeds stale state any more, so a test has to build the stale `conductor.generated.ncl` plus a matching `state.json` itself. Line 340 falls out of the same fixture.

## Group 6: CLI layer, 2 rows

The matrix rows `--no-progress` selects an inert terminal (F39): unit half (line 863) and `--no-progress` end-to-end: a real `--no-progress` CLI run draws no frame on stderr (line 864). Line 863 is marked open only because of line 864, so the two close together or not at all. Line 864 wants a real `--no-progress` run asserted to draw no frame on stderr and no panic string in either captured stream, which means spawning the binary. The unit suite in `service.rs` does not do that.

## Not in this plan

The wall-clock dependence on indicatif's position token bucket, the frame comparison's spinner exemption, and `render_at`'s dependence on the harness running ticker-off are all documented with tests pinning the wording. None has code left to write.

The gate that reported a clean sweep without scanning anything is fixed in the preceding plan, not here.

## Gates

- `RUSTC_WRAPPER="" cargo test -p mediapm-utils --all-targets --all-features`, for the progress and suffix rows
- `RUSTC_WRAPPER="" cargo test -p mediapm-conductor --all-targets --all-features`, for the provider and coordinator rows
- `RUSTC_WRAPPER="" cargo test -p mediapm --all-targets --all-features`, for the materializer, config and tool-sync rows
- `RUSTC_WRAPPER="" cargo bin rumdl check .pi/plans/plan-2026-10-04T110000-the-deferred-coverage-rows.md`, and again over `.agents/coverage-matrix.md` when the rows are marked closed

Markdown lint runs through `cargo bin rumdl`, which needs `RUSTC_WRAPPER=""` like every other cargo invocation here and is not on PATH.

## Policy

Annotation is not coverage. A row closes when a named test asserts the spec item, or when the row is marked `[by-design]` with the reason written down and the token added to `.agents/instructions/sdd-tdd-workflow.instructions.md` first. Re-annotating a row does not move it.
