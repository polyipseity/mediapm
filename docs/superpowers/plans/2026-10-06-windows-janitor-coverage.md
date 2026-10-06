# Windows janitor coverage implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the only janitor-gate argument that nothing observes on Windows, and stop CI from skipping the crate that would observe it.

**Architecture:** Two independent edits in two files. A `#[cfg(windows)]` test joins the existing `program_tests` module beside its unix twin, asserting the whole arg list of `janitor_command`'s Windows branch. A separate step joins the existing `windows` CI job so the crate's tests execute there for the first time. Neither change alters production behaviour.

**Tech Stack:** Rust 2024, clap, GitHub Actions YAML, cargo-nextest via the `test-pkg` alias.

**Spec:** `docs/superpowers/specs/2026-10-06-test-runner-followups-design.md` covers the gate this protects. The Windows-specific design was approved in conversation rather than written to a separate spec file, because it is a bounded change to an existing flow.

## Global Constraints

- Every commit message uses Conventional Commits with a mandatory scope: `type(scope): subject`. Scope must not be a bare crate or tool prefix. Use `fix(tests):` or `ci(tests):`.
- This shell exports `RUSTC_WRAPPER=sccache`, which fails here with `Operation not permitted (os error 1)`. Prefix every cargo command with `RUSTC_WRAPPER=""`.
- The workspace denies `clippy::all`, `clippy::pedantic` and rust warnings. Prefer item-scoped `#[expect(lint, reason = "...")]` over bare `#[allow(...)]`.
- The janitor gate keeps failing hard. No retry, no downgrade, no tolerance.
- The three gate error strings are frozen: `mediapm temp-dir sweep failed: <output>`, `test suite left mediapm temp dirs behind`, `unprefixed tempdir/prefix use outside src/mediapm-utils/src/temp.rs`.
- Rustdoc is a contract: doc comments explain WHY, not what the code does.
- Line endings: `.md` uses LF, `.ps1` uses CRLF.
- Do NOT touch `.vscode/`. It is sandbox write-denied and the operator has resolved it separately.
- No subagent runs `git commit` or `git push`. The controller commits.
- Never run `git reset`, `git commit --amend`, `git clean`, or `git checkout` to discard work.

## Standing constraint for both tasks

The new test is `#[cfg(windows)]`, so it does not compile on macOS or Linux. The local test count therefore stays **41** in Task 1 and is expected to stay 41. Do not "fix" that by relaxing the cfg gate. The count changing is a signal something is wrong; a count of 41 after Task 1 is correct.

---

### Task 1: Assert the Windows branch passes `--dry-run`

**Files:**

- Modify: `scripts/test-runner/src/gates/janitor.rs`
- Test: `scripts/test-runner/src/gates/janitor.rs`, module `program_tests`

**Interfaces:**

- Consumes: the existing `program_tests` module at `janitor.rs:174`, whose unix test is `the_unix_branch_spawns_the_janitor_by_path` at `:197`.
- Produces: one new `#[cfg(windows)]` test. No production code changes.

**The production code being pinned**, from `janitor_command` at `janitor.rs:96-105`:

```rust
if cfg!(windows) {
    (
        std::ffi::OsString::from("pwsh"),
        vec![
            std::ffi::OsString::from("-NoProfile"),
            std::ffi::OsString::from("-File"),
            script.with_file_name("clean-mediapm-temp.ps1").into_os_string(),
            std::ffi::OsString::from("--dry-run"),
        ],
    )
}
```

- [ ] **Step 1: Read the module before editing**

Read `scripts/test-runner/src/gates/janitor.rs` from line 174 to the end of `program_tests`, plus `janitor_command` at lines 79-110. Note how the unix test binds `let (program, args)` and asserts with `args.iter().any(...)`. The Windows test mirrors that shape.

- [ ] **Step 2: Add the test**

Add a `#[test]` immediately after `the_unix_branch_spawns_the_janitor_by_path`, inside the same `program_tests` module, gated `#[cfg(windows)]`. Name it `the_windows_branch_passes_dry_run`.

It must:

- Bind `let (program, args) = janitor_command(Path::new("/repo"));` — bind `args`, not `_args`.
- Assert `program` equals `pwsh`.
- Assert `args` contains `-NoProfile`, `-File`, an argument ending in `clean-mediapm-temp.ps1`, and `--dry-run`.
- Read the whole arg list with `args.iter().any(...)`. Do NOT assert on a fixed index: reordering the command must not silently drop the flag, and a positional assertion would break for the wrong reason.
- [ ] **Step 3: Write the doc comment**

Explain WHY, not what. The reason specific to Windows is this: the stub-based tests cannot observe the flag, because `write_stub`'s Windows variant (at `janitor.rs:365`) emits a script with no `param()` block, so `pwsh -File` hands `--dry-run` through `$args` where it lands unexamined. That is recorded in `write_stub`'s own doc comment. So nothing currently sees the flag, and without it `clean-mediapm-temp.ps1` deletes the directories the gate exists to report, destroying the evidence of the failure the gate catches.

State the consequence plainly: a missing `--dry-run` deletes the evidence, and every other test in this file keeps passing. Mention that the assertion reads the whole list so reordering cannot lose it silently, matching the unix twin.

- [ ] **Step 4: Verify**

```bash
RUSTC_WRAPPER="" cargo test -p test-runner
RUSTC_WRAPPER="" cargo clippy -p test-runner --all-targets --all-features
RUSTC_WRAPPER="" cargo fmt -p test-runner -- --check
```

Expected: `test result: ok. 41 passed; 0 failed` — the count must NOT rise, because the new test is Windows-only. Clippy silent, fmt clean.

If the count is 42, the cfg gate is wrong and you must fix it rather than adjusting the expectation.

- [ ] **Step 5: Leave changes unstaged.**

Do not run any git write command.

---

### Task 2: Run `test-runner` on Windows CI

**Files:**

- Modify: `.github/workflows/ci.yml`
- Test: YAML parse only.

**Interfaces:**

- Consumes: nothing from Task 1; the tasks are file-disjoint.
- Produces: one new step in the existing `windows` job.

**The current job**, at `ci.yml:89-105`:

```yaml
  windows:
    name: "Windows: Workspace Tests"
    runs-on: windows-latest
    steps:
      - name: Checkout repository
        uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd

      - name: Setup Rust toolchain
        uses: actions-rust-lang/setup-rust-toolchain@150fca883cd4034361b621bd4e6a9d34e5143606

      - name: Run workspace tests
        run: cargo --locked test-pkg mediapm-tests
```

The `test-pkg` alias at `.cargo/config.toml:17` expands to `cargo-nextest run --all-targets --all-features -p`, so `cargo --locked test-pkg test-runner` runs the runner's own tests.

- [ ] **Step 1: Read the workflow first**

Read `.github/workflows/ci.yml` in full enough to see the job boundaries, so the new step lands inside `windows` and not inside `feature-matrix` or `build`. Confirm which job `runs-on: windows-latest` belongs to.

- [ ] **Step 2: Add a separate step**

After the existing `Run workspace tests` step, add:

```yaml
      - name: Run test-runner gate tests
        run: cargo --locked test-pkg test-runner
```

Use a separate step rather than appending to the existing one. A distinct step means a Windows failure names the runner instead of the test crate, which is the whole point of the change.

Match the existing indentation exactly — steps sit at 6 spaces under `steps:`, and `name`/`run` at 8.

- [ ] **Step 3: Do not touch anything else**

The job `name`, `runs-on`, both action pins, and the existing `Run workspace tests` step all stay byte-identical. No other job changes. Do not add a new job; a separate job would cost another runner for no benefit.

- [ ] **Step 4: Verify**

```bash
uv run --no-project --with pyyaml python3 -c "import yaml; d=yaml.safe_load(open('.github/workflows/ci.yml')); s=[x for x in d['jobs']['windows']['steps'] if 'run' in x]; print([x['run'] for x in s])"
RUSTC_WRAPPER="" cargo bin rumdl check .github/workflows/ci.yml
git diff --stat
```

Expected: the parse prints both run commands, the second being `cargo --locked test-pkg test-runner`; rumdl reports no issues; `git diff --stat` shows one file.

If the system `python3` has no yaml module, the `uv run --no-project --with pyyaml` form is required — plain `python3 -c` will fail with `No module named yaml`.

- [ ] **Step 5: Leave changes unstaged.**

Do not run any git write command.

---

## Controller-held closing gate

After both tasks are reviewed and committed:

1. `RUSTC_WRAPPER="" cargo fmt --all -- --check`
2. `RUSTC_WRAPPER="" cargo clippy -p test-runner --all-targets --all-features`
3. `RUSTC_WRAPPER="" cargo test -p test-runner` — expect 41
4. `RUSTC_WRAPPER="" cargo bin rumdl check .github/workflows/ci.yml`
5. A YAML parse asserting both run commands sit inside the `windows` job

The full `cargo test-all` is not rerun here: neither task changes any production code path, and Task 1's change cannot execute on this host. The Windows step itself cannot be verified locally at all — its first execution is Windows CI.
