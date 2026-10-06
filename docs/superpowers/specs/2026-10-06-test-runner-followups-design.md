# Fixing the test-runner follow-ups: design

**Date:** 2026-10-06
**Status:** approved
**Plan:** `docs/superpowers/plans/2026-10-06-test-runner-followups.md`
**Predecessor:** `docs/superpowers/specs/2026-10-06-test-invocation-centralization-design.md`

## What this is

The centralization landed and its final review returned no Critical findings. What came out of that review and the closing gate is a set of follow-ups. They are not one change. This design covers the ones whose shape is known and the one whose shape is not yet known, and it deliberately leaves a third group for a later pass.

## Two corrections to the record

The final review claimed `cargo doc` cannot fail on a broken intra-doc link, because it read `[lints.rust] warnings = "deny"` as reaching `rustc` but not `rustdoc`. That is wrong. `[lints.rust]` applies to every rustc-family tool, rustdoc among them. Injecting a deliberately broken link into `scripts/test-runner/src/cargo.rs` produced `error: unresolved link`, a `-D rustdoc::broken-intra-doc-links` note implied by `-D warnings`, and exit 101 from both `cargo doc` and `test-runner doc`. The gate works. The probe was reverted and the tree is clean.

The controller separately attributed an intermittent gate failure to the `--all-features` alias, on the strength of one run each way, and then retracted it when a second all-features run left nothing. That retraction stands. The flake is unattributed.

## Group A: the leftover

One `cargo test-all` run in four ended with `error: test suite left mediapm temp dirs behind` and three directories on disk. Three subsequent runs were clean. The cause is unknown.

**Diagnosis first.** Sample the suite from a cleaned temp root until it reproduces, capped at ten runs. Capture per run the nextest summary, the directory names, and a recursive listing of each directory's contents. The contents carry the signal: the observed leftovers held Nickel contract files, which points at conductor-document tests, and a `store/` directory with `tools.json` and a `lock`, which points at a tool-cache test.

Ten runs is the stopping point. It is roughly seventeen minutes at a hundred seconds a run, and it bounds a diagnosis that could otherwise run all afternoon. If ten runs produce nothing, the honest report is "not reproduced in N samples", and the next step is instrumenting the janitor to name the creating process rather than continuing to sample.

**The fix follows the culprit, and there are three outcomes.**

A test that drops its `TempDir` owner before the test ends is the most likely case, since the leftover was a `mediapm-artifact-*` tree. The fix is a local binding that keeps the owner alive for the test's duration.

A cache directory that is meant to outlive a run would mean the janitor's contract is wrong rather than the suite. That weakens the gate, so it comes back to the operator as a decision instead of being fixed in passing.

Library code that creates a temp directory without an owner is the most valuable outcome, because the test only exposed it. The fix belongs in the library.

**The gate does not change.** No retry, no downgrade, no tolerance. The operator's decision was explicit: keep failing hard. Whatever the culprit turns out to be, the fix goes on the suite side.

## Group B: five known fixes

Each is small and each has a stated cause.

`janitor_command` returns whatever program it is given, and nothing asserts what that program is. A test asserting the returned value ends with `clean-mediapm-temp.sh` on unix closes a hole where the single worst defect of the whole centralization, spawning a bash-only script under dash, could return silently on any developer machine and only fail on CI.

`cli.rs:33` tells the reader the suite runs "then the janitor and tempdir gates". The code runs tempdir first, because it is a pure in-process scan and should fail before a subprocess is spent. The help text is user-visible and currently wrong.

The janitor's leftover verdict names no directory. At the moment it bails it already holds the `would remove:` lines carrying the paths, and discards them. Pre-push now runs this gate for the first time, so the first developer to trip it sees a verdict with no evidence attached.

The feature-matrix job went from 46 parallel jobs to one sequential job with no timeout. A hung probe holds it for GitHub's six-hour default.

`cargo test-all` no longer forwards nextest flags, because the runner builds the whole argv itself. `cargo test-all -- --no-fail-fast` now reaches the argument parser and exits 2. Nothing says so.

## Group E: the coverage row

One row claims the rustdoc link gate is covered and cites a compile error as its evidence. The claim is true. The evidence is weaker than the claim, and now that the gate's actual failure mode is known, the row can cite it.

## Group C: deferred, and why

`SCAN_ROOTS` cannot simply grow to include `scripts/`. The gate's own source contains both forbidden substrings, in its `FORBIDDEN` constant and again in its test fixtures, so adding the root today makes the gate fail on itself. Any fix needs an exclusion list as well as the root, and the right shape depends on what group A finds.

The gate-ordering claim is now asserted in six places with nothing enforcing agreement, and it has already produced one wrong instance, the help text in group B. Whether the fix is to have every site defer to `test.rs` or to add a test that reads the order back out is a design choice with several shapes, and it does not belong in the same conversation as a flaky-test diagnosis.

## What this design does not settle

Whether the leftover is a real leak, a test hygiene bug, or unreproducible noise. That is group A's whole purpose and the plan sequences it first for that reason.

Whether the gate ordering should be asserted in six places or one. That is group C.

Whether branch protection is updated, and whether `.vscode/settings.json` gains `test-doc-all`. Both are outside the repository and both remain open.
