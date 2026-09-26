# sync_tools progress-override seam

## Commit

The commit that carries this change is the tip of `main`, subject
`refactor(progress): add a sync_tools progress override seam` (retrieve the
hash with `git log --oneline -1`; the exact hash is reported in the session
result, since a commit cannot contain its own hash).

## New seam

`src/mediapm/src/service.rs:832`:

```rust
pub async fn sync_tools_with_progress_overrides(
    &mut self,
    check_tag_updates: bool,
    no_progress: bool,
    overrides: SyncProgressOverrides,
) -> Result<ToolsSyncSummary, MediaPmError>
```

Mirrored pattern: `sync_library_with_progress_overrides`
(`src/mediapm/src/service.rs:1204`), same override type
(`SyncProgressOverrides`, `src/mediapm/src/sync_report.rs:74`), same
construction (`sync_progress_terminal(overrides, no_progress)`,
`src/mediapm/src/service.rs:66`), same hard-wrapped Rustdoc style, same
"every other entry point delegates here" shape.

## Behaviour preservation

`sync_tools()` (`src/mediapm/src/service.rs:791`) and
`sync_tools_with_tag_update_checks()` both delegate with
`SyncProgressOverrides::default()`; the moved body is byte-identical, so
`recheck_policy`, terminal selection, `sync_tools_from_document` and all
user-visible output are unchanged. CLI behaviour untouched.

## Leak count

`RUSTC_WRAPPER="" cargo test -p mediapm --test mod 2>&1 | grep -c "█"`:
**3289 -> 0**.

## Tests

146 passed / 0 failed (harness baseline was 146/0 — unchanged; no test was
added, removed, skipped or re-enabled).

## Assertion set

Unchanged. `git diff -U0 -- src/mediapm/tests | grep -cE "assert!|assert_eq!"`
= **0** added, **0** removed. The only assertion-bearing lines touched are the
4 `.expect("first sync")` / `.expect("second sync")` lines in
`dual_write.rs`, whose text is byte-identical; rustfmt re-wrapped them onto
their own line as part of the call-chain reroute. All 42 call sites
(19 leaking sites across `int/tool_sync/{basics,composite,inline_deps,requires_sync,resolved,validation}.rs`,
`int/all_platform.rs`, `int/dual_write.rs`) were rerouted through
`common::test_sync_progress_overrides()`; no site was converted to an inert
screen.

## Mutation check

Broke `save_mediapm_state_document` in `sync_tools_from_document`:

```text
test int::tool_sync::basics::sync_creates_state_document ... FAILED
panicked at src/mediapm/tests/int/tool_sync/basics.rs:44:5
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 145 filtered out
```

Reverted by editor edit (no git checkout):

```text
test int::tool_sync::basics::sync_creates_state_document ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 145 filtered out
```

## Lint / format

`cargo fmt-check` exit 0. `cargo clippy -p mediapm --all-targets
--all-features --keep-going` exit 0, zero warnings/errors.

## Unconverted sites

None.

## Commands

18 shell commands (16 inspection/edit/verify, 1 mutation pair, 1 commit).
