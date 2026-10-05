---
status: complete
maxConcurrency: 1
closing-plan-for: plan-2026-10-04T150000-the-last-16-rows.md
---

# Skip is an error, and the cleanups that decision forced

The previous plan closed every coverage row. Four decisions were left open and unanswered, and this plan settles them.

## What was asked, and what the code actually does

The question was whether a skipped media entry should turn the materialization screen yellow. Before asking that we should have established what "skipped" means, so here it is first, because it changes the answer.

A skip is `materialized: false` at `src/mediapm/src/materializer/mod.rs:256`. That flag has exactly two producers:

- `mod.rs:646`, no content hash: the upstream step ran but produced no CAS hash for the variant, so there is nothing to write
- `mod.rs:962`, path conflict: a folder variant's path already exists as a directory, so the write is declined

Neither is a benign no-op. There is no "already up to date" skip in this materializer, which means the public doc comment at `src/mediapm/src/lib.rs:103-104` is wrong.

## Decisions

1. A skipped entry is an error, not a warning. The overall row finishes `finish_error` and the CLI exits non-zero.
2. The rename replacement refusal stands. A literal from configuration is refused; an interpolated value the user cannot control gets the replacement map.
3. Delete the redundant `.min(suffix_ceiling)` in `settle`.
4. Split the three test modules past the ~300 line convention.

## Sequence

Refactoring lands before behaviour change, so each commit is bisectable and a revert does not undo two unrelated things.

### Step 1: audit and correct the docs

- Fix the `skipped_paths` doc comment at `lib.rs:103-104` to name the two real causes
- Confirm where the rename refusal is applied relative to interpolation, and record it in the matrix cell
- Establish whether the path-conflict skip can fire on a routine re-sync

No behaviour change. If the audit shows a third skip producer, step 4 changes shape and this plan is amended before anything is implemented.

### Step 2: delete the dead clamp

Remove the second `.min(suffix_ceiling)` from `settle` in `src/mediapm-utils/src/progress/inner/renderer.rs`. `measure_suffix_width` already caps at the ceiling, so this cannot fire. Drop the unpinnable note from the matrix row.

### Step 3: prove a re-sync produces no skips

Add the test this repository never had, and needed only now: a second `sync_library` over an unchanged library reports zero skipped paths and exits successfully.

This must land and pass **before** step 4. Without it, step 4 can turn every second sync into a failure and nothing in the suite says so. The path in the CI gate is `src/mediapm/tests/int/`, target `mod`.

### Step 4: make a skip an error

Four sites, in this order:

- `materializer/mod.rs:282-286`: finish `finish_error` when `skipped_paths > 0`, not only on `materialize_error`
- `src/mediapm/src/output/observer.rs:75` and `output/mod.rs:47`: summary icon becomes `Error` when skipped > 0
- `src/mediapm/src/main.rs`: a real non-zero exit. Today the binary exits 0 on success and reserves `exit(1)` for a missing CLI feature

The report survives the error, so the user sees `materialized=5 skipped=1` and still gets a non-zero exit. Returning `Err` from `sync_hierarchy` instead would discard those counts.

The per-entry row keeps its `[W]` marker: the entry was skipped, while the run as a whole failed. Red on the overall row, yellow on the entry that caused it.

### Steps 5 to 7: split the test modules

`src/mediapm/src/conductor_bridge/sync/mod.rs` (4572 lines), `src/mediapm/src/materializer/mod.rs` (3036, 29 inline tests), `src/mediapm-utils/src/progress/inner/renderer.rs` (2134). Themed sibling files, `#[cfg(test)] mod foo_<theme>;`, unit tests stay inline under ~300 lines per the convention in `.agents/instructions/rust-workflow.instructions.md`.

Pure relocation. No assertion changes, no test renames.

## Carried forward, not done here

- `mediapm tool sync` prints Success and exits 0 on a warn-only run. The rule is settled, the path is reachable, and a subprocess test shape already exists, so this is an implementation item for a later plan.
- A failed workflow step still exits 0. Status 3 now means the library is incomplete, and a step failure may or may not be the same class, so the status, the wording and the classification all need a product ruling first.
- Nothing spawns the binary against a workspace that skips, so the exit status is covered at library level only. That is why one matrix row stays partial.
- The folder arm refuses a write when a variant path is already a directory, records a notice, and falls through to `materialized: true`, so a variant that produced nothing is counted as materialized. Recorded in the matrix, out of scope here.
- Two test doubles implement `truncate_suffix` without honouring `max_width`.
- `renderer.rs` remains 1846 lines, 1831 of them production. Moving its tests out did not make the file small.

## Invariants

- Every commit passes `scripts/run-all-tests.sh`, clippy, fmt, rustdoc and rumdl before it is made
- One lane at a time, controller holds the gate
- No `git reset`, `git restore`, `git checkout`, `git stash`, `git add`, `git commit` in a lane
- Never hand-type a spinner glyph or bar fill character into an expected string
- Nothing is pushed

## Done when

- A second `sync_library` over an unchanged library succeeds with zero skips
- A skipped entry finishes the overall row `finish_error` and exits non-zero
- The `skipped_paths` doc names both real causes
- The dead clamp is gone
- All three modules are under the split threshold
- The gate is green and the tree is clean
