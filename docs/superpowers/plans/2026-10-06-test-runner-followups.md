# Test-runner follow-ups implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Attribute and fix the intermittent temp-directory leftover that failed one gate run in four, and land the five small correctness and documentation fixes the final review surfaced.

**Architecture:** Group A is a diagnosis that comes first, because the other work does not depend on it but the sequence is clearer when the uncertain thing runs while the certain things are still scoped. Its fix is conditional on what the diagnosis finds, and the plan gives one exact fix per outcome rather than pretending to know which will apply. Group B is five independent one-to-three-line edits split across two tasks by file ownership. Group E is one documentation edit.

**Tech Stack:** Rust 2024, cargo-nextest, clap, bash for the sampling loop, GitHub Actions YAML.

**Spec:** `docs/superpowers/specs/2026-10-06-test-runner-followups-design.md`

**Predecessor:** `docs/superpowers/specs/2026-10-06-test-invocation-centralization-design.md`, whose implementation is complete at `8a270f1b`.

## Global Constraints

- Every commit message uses Conventional Commits with a mandatory scope: `type(scope): subject`. Scope must not be a bare crate or tool prefix. Use `fix(tests):`, `docs(tests):`, `fix(ci):`, `docs(ci):`.
- This shell exports `RUSTC_WRAPPER=sccache` and sccache fails here with `Operation not permitted (os error 1)`. Prefix every cargo command with `RUSTC_WRAPPER=""`.
- The workspace denies `clippy::all`, `clippy::pedantic` and rust warnings. Prefer item-scoped `#[expect(lint, reason = "...")]` with a substantive reason over a bare `#[allow(...)]`.
- The janitor gate keeps failing hard. No retry, no downgrade, no tolerance. The fix goes on the suite side.
- The three error strings the gates emit are a contract and must not change: `mediapm temp-dir sweep failed: <output>`, `test suite left mediapm temp dirs behind`, `unprefixed tempdir/prefix use outside src/mediapm-utils/src/temp.rs`. Group B changes the *leftover* message by appending evidence to it, which is the one sanctioned exception; the task says exactly how and states what the new text is.
- Line endings: `.sh` and `.md` use LF, `.ps1` uses CRLF.
- Do not touch `.vscode/`. It is write-denied in this environment and the operator applies that edit separately.
- No subagent runs `git commit` or `git push`. The controller commits.
- Never run `git reset`, `git commit --amend`, `git clean`, or `git checkout` to discard work.
- A full suite run takes about a hundred seconds. Budget for that rather than assuming a command is quick.

---

### Task 1: Sample the leftover until it reproduces

**Files:**

- Create: `/tmp/mediapm-flake-samples/` (scratch, outside the repository, never committed)

**Interfaces:**

- Consumes: nothing.
- Produces: a samples directory holding one evidence file per run, and a verdict of either "reproduced on run N" or "not reproduced in 10 runs".

**This task changes no repository file.** It is a measurement, and its whole value is that it produces evidence rather than an opinion.

- [ ] **Step 1: Confirm the starting state**

Run:

```bash
bash scripts/clean-mediapm-temp.sh
ls -d "${TMPDIR:-/tmp}"/mediapm-* 2>/dev/null | wc -l
```

Expected: the sweep prints a count and a removal line, and the `wc -l` prints `0`. If it does not, the sweep failed and you must not start sampling, because a dirty starting state invalidates every sample.

- [ ] **Step 2: Create the samples directory**

```bash
mkdir -p /tmp/mediapm-flake-samples
```

- [ ] **Step 3: Write the sampling loop**

Write `/tmp/mediapm-flake-samples/sample.sh`:

```bash
#!/usr/bin/env bash
# Sample `cargo test-all` until it leaves a temp dir behind, capped at 10 runs.
# Usage: sample.sh <max-runs>
set -uo pipefail
cd /Users/polyipseity/dev/monorepo/self/mediapm
export RUSTC_WRAPPER=""
MAX="${1:-10}"
OUT=/tmp/mediapm-flake-samples

for i in $(seq 1 "$MAX"); do
    bash scripts/clean-mediapm-temp.sh >/dev/null 2>&1
    before=$(ls -d "${TMPDIR:-/tmp}"/mediapm-* 2>/dev/null | wc -l | tr -d ' ')
    cargo test-all >"$OUT/run-$i.log" 2>&1
    code=$?
    left=$(ls -d "${TMPDIR:-/tmp}"/mediapm-* 2>/dev/null | wc -l | tr -d ' ')
    {
        echo "run=$i exit=$code before=$before leftovers=$left"
        grep -E "Summary \[|error:" "$OUT/run-$i.log" | tail -3
    } >>"$OUT/summary.txt"
    echo "run $i: exit=$code leftovers=$left"
    if [ "$left" -gt 0 ]; then
        ls -ld "${TMPDIR:-/tmp}"/mediapm-* >"$OUT/leftover-dirs-$i.txt"
        for d in "${TMPDIR:-/tmp}"/mediapm-*; do
            echo "--- $d"
            find "$d" -maxdepth 3 | head -40
        done >"$OUT/leftover-contents-$i.txt"
        echo "REPRODUCED on run $i"
        exit 0
    fi
done
echo "NOT REPRODUCED in $MAX runs"
exit 3
```

Note the script is deliberately written so a dirty temp root cannot produce a false reproduction: it sweeps first and records the `before` count on every line, so a reader can tell whether a leftover predated the run.

- [ ] **Step 4: Run it**

```bash
chmod +x /tmp/mediapm-flake-samples/sample.sh
/tmp/mediapm-flake-samples/sample.sh 10
```

Expect one of two outcomes. A clean reproduction exits 0 and prints `REPRODUCED on run N`. Exhausting the cap exits 3 and prints `NOT REPRODUCED in 10 runs`.

Ten runs is roughly seventeen minutes. Do not raise the cap on your own initiative; if the cap is hit, that is a result to report, not a reason to keep going.

- [ ] **Step 5: Read the evidence**

```bash
cat /tmp/mediapm-flake-samples/summary.txt
ls /tmp/mediapm-flake-samples/
```

If a run reproduced, open `leftover-contents-N.txt` and note, for each leftover directory, whether it holds Nickel contract files, a `store/` tree, example artifacts, or something else. Task 2 branches on that classification.

If nothing reproduced, stop. Write the report described in step 6 and do not proceed to Task 2, because attributing a flake with no sample is guesswork.

- [ ] **Step 6: Write the report**

Write `/tmp/mediapm-flake-samples/finding.md` containing: the number of runs attempted, the per-run leftover counts, and either the directory classification with the file lists that justify it, or the sentence "not reproduced in N runs from a clean temp root". Quote the nextest summary line from any run that failed.

Do not commit anything from this task. There is nothing in the repository to commit.

---

### Task 2: Attribute the leftover to a creating site

**Files:**

- Read only. This task changes nothing.

**Interfaces:**

- Consumes: `leftover-contents-N.txt` from Task 1.
- Produces: a named creating site, or an explicit statement that the evidence does not support one.
- [ ] **Step 1: Confirm Task 1 reproduced**

If `sample.sh` printed `NOT REPRODUCED`, this task cannot run. Report that to the controller and stop; the remaining tasks in this plan are still worth doing.

- [ ] **Step 2: Classify the leftover by content**

From `leftover-contents-N.txt`, decide which of these shapes you are looking at:

Nickel contract files such as `v1.ncl`, `v2.ncl`, `mod.ncl`, `document_input.ncl` or `decode_document.ncl`. That is conductor-document test output and points at `src/mediapm/tests/` or `src/mediapm/src/conductor_bridge/`.

A `store/` directory holding `tools.json`, `lock` and `blobs`. That is a tool cache and points at the provisioning path, likely `src/mediapm/src/tools/` or an example that provisions tools.

A materialized media tree. That points at the materializer tests.

Report which shape it is and the exact filenames, because the filenames are what let the next step grep for them.

- [ ] **Step 3: Grep for the filenames**

For each distinctive filename you found, for example `document_input.ncl`:

```bash
git grep -n "document_input.ncl" -- src tests
```

Note every hit. A filename that appears in a test's setup path is the creation site.

- [ ] **Step 4: Read the creating site and check ownership**

Read the test function the grep pointed at. Answer one question: does the returned `TempDir` owner live in a local that outlives the test, or is it dropped at the end of a statement?

A `let dir = artifact_dir()?;` held in a local is correct and leaves nothing behind. A `artifact_dir()?.path().to_path_buf()` drops the owner immediately, which deletes the directory at the end of that statement, which is the opposite failure and produces a missing-directory error rather than a leftover. A site that creates the directory and stores only the path somewhere longer-lived is the leak.

Also check whether the test asserts anything about cleanup. A test that leaves a directory behind and asserts nothing about it is the most likely shape.

- [ ] **Step 5: Write the attribution into the finding**

Append to `/tmp/mediapm-flake-samples/finding.md` a section naming the file and line, the function, and which of the three fix branches applies:

Branch one, a test drops its owner too early. The fix is local to that test.

Branch two, a cache directory is meant to outlive a run. The janitor's contract is wrong and this comes back to the operator rather than being fixed here.

Branch three, library code creates a directory with no owner. The fix belongs in the library and the test only exposed it.

If the evidence does not support any of the three, say so plainly and stop. An honest "the sample shows the shape but not the site" is worth more than a confident wrong line number.

---

### Task 3: Fix the leak

**Files:**

- Modify: the file and line Task 2 named.
- Test: the same test file, or a new test beside it if the fix needs one.

**Interfaces:**

- Consumes: the branch identified in Task 2.
- Produces: a suite that leaves nothing at the temp root.
- [ ] **Step 1: Write the failing check first**

The check that proves this task worked is not a unit test; it is the gate itself. Before changing anything, record that the gate currently fails on this input:

```bash
ls -d "${TMPDIR:-/tmp}"/mediapm-* 2>/dev/null | wc -l
```

For a reproducible sample you already have the leftover from Task 1. Note it. The point is that after your fix, a rerun of `sample.sh` must reach `NOT REPRODUCED`.

- [ ] **Step 2: Apply the fix for your branch**

Branch one, a test drops its owner too early. Bind the owner in a local that lives as long as the test needs the directory:

```rust
let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
let path = dir.path().to_path_buf();
```

Keep `path` for whatever the test does with it. The owner `dir` must stay bound; naming it `_dir` drops it at the end of the statement, which is the bug you are fixing.

Branch three, library code creates a directory with no owner. The same shape applies inside the library, and the test that exposed it should keep asserting whatever it already asserts. Add nothing new unless the fix changes an observable.

Branch two does not have a code fix here. Stop and report it to the controller with the evidence, because changing the janitor's contract is an operator decision and it weakens the gate.

- [ ] **Step 3: Add a regression assertion**

If the test can assert its own cleanup cheaply, add it. The assertion that matters is that the directory does not exist after the test's work completes:

```rust
assert!(!dir.path().exists(), "the artifact dir must outlive the test body");
```

Placed at the end of the test, this fails on the pre-fix code only if the owner was already dropped, which is exactly the regression. If the shape does not admit that assertion, say why in your report rather than adding a vacuous one.

- [ ] **Step 4: Run the crate tests**

```bash
RUSTC_WRAPPER="" cargo test -p <the crate you changed> --all-features
RUSTC_WRAPPER="" cargo clippy -p <the crate you changed> --all-targets --all-features
```

Expect the crate's suite green and clippy silent.

- [ ] **Step 5: Leave the changes unstaged**

Do not run any git write command. The controller commits after review.

---

### Task 4: Prove the fix with re-sampling

**Files:**

- Read only.

**Interfaces:**

- Consumes: Task 3's change.
- Produces: an evidence statement about whether the leftover is gone.
- [ ] **Step 1: Re-run the same sampler**

```bash
/tmp/mediapm-flake-samples/sample.sh 10
```

Expect `NOT REPRODUCED in 10 runs`. That is the pass condition for this task.

- [ ] **Step 2: Record the result either way**

Append to `/tmp/mediapm-flake-samples/finding.md` the outcome of the re-sample and the total number of runs now attempted across both samplings.

If the leftover reproduces again after the fix, the fix did not address the cause. Report that plainly and do not attempt a second fix in the same round; a second guess without new evidence is how a diagnosis turns into thrashing.

---

### Task 5: Three runner-internal fixes

**Files:**

- Modify: `scripts/test-runner/src/gates/janitor.rs`
- Modify: `scripts/test-runner/src/cli.rs`
- Test: `scripts/test-runner/src/gates/janitor.rs` and `scripts/test-runner/src/cli.rs`

**Interfaces:**

- Consumes: nothing from Tasks 1 to 4.
- Produces: three independent changes, all inside the runner crate.

These three touch two files and are independent of each other. Do them as one task, one commit, one review surface.

- [ ] **Step 1: Pin the janitor program so the C1 defect cannot return silently**

In `gates/janitor.rs`, add:

```rust
#[cfg(test)]
mod program_tests {
    use super::janitor_command;
    use std::path::Path;

    /// The unix branch must spawn the janitor by path.
    ///
    /// `clean-mediapm-temp.sh` is bash: it uses `set -euo pipefail`, `[[ ]]`
    /// and `read -r -d ''`, none of which dash parses. On `ubuntu-latest` the
    /// resolved `sh` is dash, so naming an interpreter fails the gate on every
    /// run while passing on macOS, where `sh` is bash 3.2. The stub-based
    /// tests cannot catch that on a bash-as-sh host, because the stub's own
    /// `set -euo pipefail` is legal there. This assertion is what closes it
    /// everywhere, with no spawn and no interpreter dependency.
    #[test]
    #[cfg(unix)]
    fn the_unix_branch_spawns_the_janitor_by_path() {
        let (program, _args) = janitor_command(Path::new("/repo"));
        assert!(
            program.to_string_lossy().ends_with("clean-mediapm-temp.sh"),
            "the janitor must be spawned by path, not through an interpreter; got {program:?}"
        );
    }
}
```

If `janitor_command` is currently private to a nested `mod tests`, place this module beside that one so both can reach it. If the function itself is private and needs widening for the test, make it `pub(crate)` and note that in your report.

- [ ] **Step 2: Fix the help text that names the gate order backwards**

In `cli.rs`, the `Command::Test` variant's doc comment reads "Run the test suite, then the janitor and tempdir gates." The code runs tempdir then janitor. Replace it with:

```rust
    /// Run the test suite, then the tempdir and janitor gates.
    ///
    /// The tempdir gate runs first because it is a pure in-process scan, so a
    /// naming violation is reported without spending a subprocess on a tree
    /// that is already wrong.
```

The second sentence mirrors the rationale already in `test.rs` and in the documentation, so the help and the code now agree.

- [ ] **Step 3: Make the leftover verdict name the directories**

In `janitor.rs::enforce`, the loop currently sets a boolean and discards the lines. Collect the paths as well, and include them in the bail.

Change the loop so it accumulates:

```rust
    let mut leftovers: Vec<&str> = Vec::new();
    for line in combined.lines() {
        if line.starts_with("would remove") {
            leftovers.push(line);
        }
        if !is_contract_line(line) {
            eprintln!("{line}");
        }
    }
    if !leftovers.is_empty() {
        bail!(
            "test suite left mediapm temp dirs behind: {}",
            leftovers.join("; ")
        );
    }
```

That changes the message from a bare verdict to a verdict carrying the evidence, which is what a developer sees the first time pre-push trips this gate. `main.rs` already prefixes `error: `, so the rendered line becomes:

```text
error: test suite left mediapm temp dirs behind: would remove: /tmp/mediapm-artifact-abc; would remove: /tmp/mediapm-cache-xyz
```

One consequence to check rather than assume: `leftovers` also captures the janitor's own summary line, `would remove 3 mediapm temp director(ies)`, because that line also starts with `would remove`. That is acceptable and arguably useful, but it means the message may carry both per-directory lines and a count. If you judge it noisy, push only lines that start with `would remove:` into the vector and keep the broader `starts_with("would remove")` solely as the trigger. Say which you chose and why.

- [ ] **Step 4: Update the test that asserts the exact message**

`a_leftover_fails_the_gate` asserts the error string exactly. It will now fail. Update it to match the new text, and make it assert that a path from the stub's output appears in the message, because that is the property this change is for:

```rust
    let err = enforce(&root).expect_err("a leftover must fail the gate");
    let text = err.to_string();
    assert!(
        text.contains("test suite left mediapm temp dirs behind"),
        "unexpected message: {text}"
    );
    assert!(
        text.contains("/tmp/mediapm-artifact-abc"),
        "the verdict must name the leftover directory; got: {text}"
    );
```

- [ ] **Step 5: Run the gates**

```bash
RUSTC_WRAPPER="" cargo test -p test-runner
RUSTC_WRAPPER="" cargo clippy -p test-runner --all-targets --all-features
RUSTC_WRAPPER="" cargo fmt -p test-runner -- --check
RUSTC_WRAPPER="" cargo doc --locked --no-deps -p test-runner --all-features --document-private-items
```

Expect the count to rise by exactly one, from 39 to 40, because Step 1 adds one test and no test was removed. If it differs, say why rather than adjusting the expectation.

- [ ] **Step 6: Leave changes unstaged**

---

### Task 6: Two caller-side fixes

**Files:**

- Modify: `.github/workflows/ci.yml`
- Modify: `README.md`

**Interfaces:**

- Consumes: nothing from Tasks 1 to 5.
- Produces: a bounded matrix job and a documented argument-passing rule.
- [ ] **Step 1: Bound the feature-matrix job**

In `.github/workflows/ci.yml`, the `feature-matrix` job runs 48 `cargo check` probes sequentially. It has no timeout, so a hung probe holds it for GitHub's 360-minute default. Add a timeout to that job only, not to `build` or `windows`:

```yaml
  feature-matrix:
    name: "Feature Matrix"
    runs-on: ubuntu-latest
    timeout-minutes: 60
```

Add nothing else. The two action pins, the `name`, and the step body stay exactly as they are.

- [ ] **Step 2: Document the argument-passing rule**

`cargo test-all` no longer forwards nextest flags. It used to expand to `cargo-nextest run ...`, so `cargo test-all -- <nextest args>` worked. It does not now, because the runner builds the whole argv itself and rejects anything it does not recognise.

In `README.md`, inside the development commands block, add one line under `cargo test-all`:

```text
cargo test-all             # nextest + tempdir and janitor gates; takes the runner's own
                            # flags (-p, --features, --no-default-features, --no-locked),
                            # not nextest flags
```

Keep the comment column aligned with the lines above it, at column 28, since a previous pass established that alignment deliberately.

- [ ] **Step 3: Verify**

```bash
RUSTC_WRAPPER="" cargo bin rumdl check README.md
uv run --no-project --with pyyaml python3 -c "import yaml; d=yaml.safe_load(open('.github/workflows/ci.yml')); print('parses'); print('timeout:', d['jobs']['feature-matrix'].get('timeout-minutes'))"
```

Expect rumdl clean and the parse printing `timeout: 60`. The system python3 has no yaml module, which is why `uv` is used.

- [ ] **Step 4: Leave changes unstaged**

---

### Task 7: Repoint the rustdoc coverage row's evidence

**Files:**

- Modify: `.agents/coverage-matrix.md`

**Interfaces:**

- Consumes: nothing.
- Produces: one row whose evidence matches its claim.
- [ ] **Step 1: Read the row**

Find the row whose Test(s) cell cites a misspelled target in `mediapm-cas/src/verify.rs` as evidence for the rustdoc link gate. That is row 984 at the time of writing.

- [ ] **Step 2: Replace the evidence with the real failure mode**

The claim in that row is correct. The evidence is not, because a misspelled target is a compile error, not an intra-doc link. The gate was measured directly: injecting `/// See [NoSuchSymbolAnywhere] for the contract.` into `scripts/test-runner/src/cargo.rs` produced `error: unresolved link to NoSuchSymbolAnywhere` with a `-D rustdoc::broken-intra-doc-links` note implied by `-D warnings`, and exit 101 from both `cargo doc` and `test-runner doc`.

Rewrite the Test(s) cell so it cites that, and say why the link lint is denied at all: `[lints.rust] warnings = "deny"` in the root `Cargo.toml` applies to every rustc-family tool, rustdoc included, which is a fact worth recording because the final review got it backwards and a future reader will reasonably wonder.

Keep the row's Status cell as `[covered]`. The claim survives.

- [ ] **Step 3: Verify the claim is still true before writing it**

```bash
RUSTC_WRAPPER="" cargo doc --locked --no-deps -p test-runner --all-features --document-private-items
```

Expect exit 0 on the clean tree. You are confirming the clean case; the failing case was measured above and the probe has been reverted.

- [ ] **Step 4: Lint the markdown**

```bash
RUSTC_WRAPPER="" cargo bin rumdl check .agents/coverage-matrix.md
```

Expect no issues.

- [ ] **Step 5: Leave changes unstaged**

---

## Controller-held closing gate

After every task is reviewed and committed:

1. `RUSTC_WRAPPER="" cargo fmt --all -- --check`
2. `RUSTC_WRAPPER="" cargo clippy -p test-runner --all-targets --all-features`
3. `RUSTC_WRAPPER="" cargo test-all`, which is the gate and takes about a hundred seconds
4. `RUSTC_WRAPPER="" cargo test-doc-all`
5. `/tmp/mediapm-flake-samples/sample.sh 10`, which must now reach `NOT REPRODUCED in 10 runs`

Item 5 is the one that closes the original question, and it is the last thing to run rather than the first, so that a fix from Task 3 has been reviewed before it is measured seventeen times.
