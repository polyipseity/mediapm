---
description: "Use when recording or reviewing validation timing baselines, CI timing evidence, test-runner performance measurements, or R1/R4/D0-D7 diagnostics for mediapm checks and tests."
name: "Validation Timing Baselines"
applyTo: ".github/workflows/**/*.yml, .github/workflows/**/*.yaml, prek.toml, .config/nextest.toml, scripts/test-runner/**, .cargo/config.toml, .agents/instructions/ci-workflow.instructions.md, .agents/instructions/rust-workflow.instructions.md, .pi/plans/*validation-timing*.md, .pi/plans/*residual-followups-diagnostics*.md"
---

# Validation timing baselines and CI timing record

**Purpose:** durable instruction-file record for validation timing baselines and CI timing evidence, supporting SDD plan `.pi/plans/2026-11-15-validation-timing-and-speedup-final-plan.md` recommendations **R1** (baselines and CI timing), **R4** (measurement reliability metadata), **R8** timing collection, and **R11** available breakdown evidence. This record captures evidence only; it does **not** change CI, hooks, prek, or test behavior.

**Location and migration:** this file is the primary record for validation timing/baseline evidence. It was migrated from `.github/docs/validation-timing-baselines.md` on 2026-11-16 when `.github/docs/` was removed; all evidence, tables, templates, counts, dates, and references from the source record were preserved.

**Source evidence:** `.pi/plans/2026-11-15-validation-timing-and-speedup-final-plan.md`, `.pi/plans/2026-11-14-validation-timing-and-speedup-recommendations.md`, controller-provided reconnaissance and `gh run` data captured for this task. Numbers are transcribed from available evidence and are not fabricated.

## R1/R4 metadata template (reusable)

Use this template for every future baseline/timing entry. Record unknown/unavailable fields explicitly as `unknown` or `not captured` (do not guess).

| Field | Required value/content |
|---|---|
| Record ID / date | Unique label and date (UTC/local as recorded) |
| Commit SHA(s) | Full SHA(s) measured; note branch/ref if relevant |
| Command(s) | Exact command line(s), order, and scope |
| Environment | Local/CI, runner/host, OS, arch, sandbox/container details |
| Toolchain | `rustc`, `cargo`, rustup toolchain/profile/components if known |
| Build toolchain context | `cc`, `cxx`, `ar`, `ld`, SDK, target triple, relevant env (`CC/CXX/AR/RANLIB/LD/RUSTFLAGS/CARGO_TARGET_*`) when known |
| `RUSTC_WRAPPER` state | Set/unset and value if set |
| Target/cache state | Warm/cold `target/`, approximate size/state if known |
| Disk/headroom | Free space / % used before and after (or during) run |
| Timing | Wall/real, user, sys per command and totals |
| Exit/pass-fail | Exit code and pass/fail outcome |
| Counts/summaries | Nextest/doctest/matrix counts, pass/fail/skip/slow, files checked, combos completed, etc. |
| Anomalies/context | Failures, blockers, env sensitivity, retries, throttling, sandbox constraints, missing data |
| Evidence source | Log path, run ID/URL, artifact, or note |

## 2026-11-14 partial/warm local record (time-to-fail blocked)

| Field | Value |
|---|---|
| Record ID / date | `local-partial-2026-11-14-warm` (2026-11-14, 19:12:19Z-19:15:36Z) |
| Commit SHA(s) | HEAD/local `2eee9aaf` (`chore(merge): 2 remote-tracking branches`, main/origin/main at setup); clean tree at setup |
| Command(s) | Sequential validation commands (see table): `cargo fmt-check`, `cargo bin rumdl check .`, `cargo clippy-all`, `cargo check --workspace --all-targets --all-features`, `cargo test-doc-all`, `cargo test-all`, `cargo build-all`, `cargo --locked build --release -p mediapm-cas -p mediapm-conductor -p mediapm`, `cargo --locked run --package test-runner -- feature-matrix --locked` |
| Environment | Local, srt/Anthropic sandbox, macOS aarch64 |
| Toolchain | `rustc 1.99.1 (58cf8f9c3 2026-10-13)`, `cargo 1.99.1 (6f612b8e3 2026-10-09)` (cargo build hash recorded in clean-rerun context; not separately restated in partial log summary) |
| Build toolchain context | Nix-wrapped clang/ar observed in blocker context (`/Library/Application Support/nucleus/toolchains/clang/bin/clang`, warning `--target arm64-apple-macosx != arm64-apple-darwin` noted in plan evidence); detailed cc/cxx/ar/ld/SDK env not fully captured in this partial record |
| `RUSTC_WRAPPER` state | unset (`RUSTC_WRAPPER=`) |
| Target/cache state | Warm `target/` (~3.0G) |
| Disk/headroom | ~90% used before, ~96% after (`/System/Volumes/Data`, ~15.3 GiB free at end per plan evidence) |
| Timing / outcome | See command table below |
| Counts/summaries | `rumdl`: 52 files. Feature-matrix partial: 42/58 completed, 16 failed (`ring`-related combos in that env). |
| Anomalies/context | Blocker: `ring v0.17.14` build script `ar` SIGABRT under nix-wrapped clang/ar in that env; not reproduced in 2026-11-15 clean rerun. Failed heavy commands are **time-to-fail**, not healthy baselines. |
| Evidence source | `/tmp/mediapm-validation-timings.log` (19:12:19Z-19:15:36Z), plan records, controller reconnaissance |

| Command | Exit | Real (s) | User (s) | Sys (s) | Outcome / notes |
|---|---:|---:|---:|---:|---|
| `cargo fmt-check` | 0 | 0.88 | 0.61 | 0.08 | pass |
| `cargo bin rumdl check .` | 0 | 0.82 | 0.11 | 0.13 | pass (52 files) |
| `cargo clippy-all` | 0 | 12.05 | 26.14 | 3.47 | pass (warm) |
| `cargo check --workspace --all-targets --all-features` | 0 | 12.43 | 30.08 | 4.46 | pass (warm) |
| `cargo test-doc-all` | 101 | 4.66 | 6.39 | 1.20 | time-to-fail (`ring v0.17.14` `ar` SIGABRT) |
| `cargo test-all` | 101 | 7.90 | 19.26 | 3.75 | time-to-fail (`ring` `ar` SIGABRT) |
| `cargo build-all` | 101 | 6.09 | 14.32 | 4.12 | time-to-fail (`ring` `ar` SIGABRT) |
| `cargo --locked build --release -p mediapm-cas -p mediapm-conductor -p mediapm` | 101 | 29.86 | 96.04 | 9.29 | time-to-fail (`ring` `ar` SIGABRT) |
| `cargo --locked run --package test-runner -- feature-matrix --locked` | 1 | 121.89 | 181.87 | 33.26 | partial/unhealthy: 42/58 completed, 16 `ring`-related failures |

## 2026-11-15 clean sequential local baseline (all passed)

| Field | Value |
|---|---|
| Record ID / date | `local-clean-2026-11-15-sequential` (2026-11-15) |
| Commit SHA(s) | Same repository state context as recorded in plan (`2eee9aaf` context for comparison; clean rerun evidence recorded as 2026-11-15 baseline in plan) |
| Command(s) | Sequential `/usr/bin/time -p` from repo root: `cargo test-doc-all`, `cargo test-all`, `cargo build-all`, `cargo --locked build --release -p mediapm-cas -p mediapm-conductor -p mediapm`, `cargo --locked run --package test-runner -- feature-matrix --locked` |
| Environment | Local, srt/Anthropic sandbox, macOS 27.2.1, `aarch64-apple-darwin` |
| Toolchain | `rustc 1.99.1 (58cf8f9c3 2026-10-13)`, `cargo 1.99.1 (6f612b8e3 2026-10-09)`, stable toolchain (`clippy`, `rustfmt`), edition 2024, `rust-version 1.94.0` |
| Build toolchain context | `CC/CXX=/Library/Application Support/nucleus/toolchains/clang/bin/clang(++)` (clang 21.1.8, target `arm64-apple-darwin27.2.0`), `LD=.../lld/bin/ld.lld` (21.1.8), `SDKROOT/DEVELOPER_DIR=.../apple-sdk/.../MacOSX.sdk` (MacOSX.sdk, MacOSX14.4.sdk present). No `AR`/`RANLIB`/`CARGO_TARGET_*`/`RUSTFLAGS` env overrides recorded; nucleus `ar` present (nix cctools wrapper), no `.../clang/bin/llvm-ar`; PATH `llvm-ar`/`llvm-ranlib` -> LLVM 23.1.0 shims. `/usr/bin/{clang,ar}` present but no CLT/Xcode (`xcrun -f clang|ar|ranlib` aborted SIGABRT). |
| `RUSTC_WRAPPER` state | unset (`RUSTC_WRAPPER=`) |
| Target/cache state | Not fully specified beyond sequential run context; prior warm target was ~3.0G in 2026-11-14 pass (state for this rerun not separately measured in evidence) |
| Disk/headroom | 353 GiB used / 26 GiB avail / 94% before release build (post `build-all`); 354 GiB / 23 GiB / 94% during/after release + feature-matrix window (`/System/Volumes/Data`) |
| Timing / outcome | All passed, sequential total **1229.18s** real / **2022.73s** user / **353.83s** sys |
| Counts/summaries | `test-doc-all`: 9 doctests passed (mediapm 2, mediapm_cas 1, mediapm_test_support 1 compile-ok, mediapm_utils 5, others 0); observed phases ~1m50s compile + ~1m23s rustdoc/dev gate. `test-all`: nextest `[291.746s] 2946 tests run: 2946 passed (4 slow), 0 skipped`; slow: `mediapm-tests::scripts pwsh_janitor_removes_a_deep_chain` 87.524s, `pwsh_janitor_dry_run_and_real_run` 8.255s, `pwsh_janitor_self_test` ~10.6s. `build-all`: `Finished dev profile ... 1m 34s`. Release: `Finished release profile ... 6m 55s`. Feature-matrix: 58/58 clean. |
| Anomalies/context | `ring v0.17.14` compiled/built successfully in all commands; no `ar` SIGABRT observed (contrast with 2026-11-14 partial run). Disk remained 94-96% used during reruns (R4 context). |
| Evidence source | Plan and controller-provided clean rerun evidence (2026-11-15) |

| Command | Exit | Real (s) | User (s) | Sys (s) | Outcome / notes |
|---|---:|---:|---:|---:|---|
| `cargo test-doc-all` | 0 | 211.92 | 81.06 | 19.52 | pass; 9 doctests total; ~1m50s compile + ~1m23s rustdoc/dev gate observed |
| `cargo test-all` | 0 | 458.70 | 404.20 | 217.93 | pass; nextest `[291.746s] 2946 tests run: 2946 passed (4 slow), 0 skipped` |
| `cargo build-all` | 0 | 95.15 | 235.86 | 29.64 | pass; `Finished dev profile ... 1m 34s` |
| `cargo --locked build --release -p mediapm-cas -p mediapm-conductor -p mediapm` | 0 | 415.15 | 1256.44 | 78.67 | pass; `Finished release profile ... 6m 55s` |
| `cargo --locked run --package test-runner -- feature-matrix --locked` | 0 | 48.26 | 45.17 | 8.07 | pass; 58/58 combinations clean |
| **Sequential total** | 0 | **1229.18** | **2022.73** | **353.83** | all passed |

## CI timing records (successful main runs)

Evidence collected via `gh run` data (controller reconnaissance). Times below are job/step durations as reported.

### CI: Build & Test — run `43609375823` (main, success)

| Field | Value |
|---|---|
| Record ID / date | `ci-build-test-43609375823` (created 2026-11-14T15:17:30Z, updated 2026-11-14T15:34:06Z) |
| Commit SHA(s) | `2eee9aaf` — `chore(merge): 2 remote-tracking branches` |
| Environment | GitHub Actions, workflow `CI: Build & Test` (`.github/workflows/ci.yml`), runners as noted per job |
| Result | success |

| Job | Runner | Job duration (s) | Step durations (s) |
|---|---|---:|---|
| Feature Matrix | `ubuntu-latest` | 254 | setup 1, checkout 2, setup rust 1, check every feature combination 239, post setup rust 8 |
| Build & Test | `ubuntu-latest` | 957 | setup 1, checkout 2, setup rust 1, install ffprobe 14, run tests 467, run doctests and rustdoc 92, build project 366, post setup rust 13 |
| Release Build | `ubuntu-latest` | 308 | setup 1, checkout 2, setup rust 1, build release packages 295, post setup rust 7 |
| Windows: Workspace Tests | `windows-latest` | 598 | setup 1, checkout 7, setup rust 9, run workspace tests 364, run test-runner gate tests 12, check completion-generating packages 139, check CAS integration test target 16, post setup rust 46 |

### CI: Build & Test — run `43379058938` (main, success, comparison)

| Field | Value |
|---|---|
| Record ID / date | `ci-build-test-43379058938` (2026-11-13, workflow span 657s) |
| Commit SHA(s) | `225a5401` — `test(cache): cover loop-starting drop during unwind` |
| Environment | GitHub Actions, workflow `CI: Build & Test`, runners as noted per job |
| Result | success |

| Job | Runner | Job duration (s) | Step durations (s) |
|---|---|---:|---|
| Feature Matrix | `ubuntu-latest` | 85 | check every feature combination 71 (other step details not fully captured in evidence) |
| Build & Test | `ubuntu-latest` | 655 | install ffprobe 21, run tests 295, run doctests and rustdoc 30, build project 293 (other step details not fully captured in evidence) |
| Release Build | `ubuntu-latest` | 148 | build release packages 134 (other step details not fully captured in evidence) |
| Windows: Workspace Tests | `windows-latest` | 386 | setup rust 56, run workspace tests 295, run test-runner gate tests 3, check completion-generating packages 14, check CAS integration test target 4 (other step details not fully captured in evidence) |

### Commit message lint (main, success)

| Record | Run ID | Commit SHA | Message | Date/time (UTC) | Job | Job duration (s) | Step durations (s) |
|---|---:|---|---|---|---|---:|---|
| `commitlint-43609375822` | 43609375822 | `2eee9aaf` | `chore(merge): 2 remote-tracking branches` | 2026-11-14T15:17:30Z-15:17:45Z | commitlint | 12 | pull action image 4, checkout 3, lint 3 |
| `commitlint-43146911368` | 43146911368 | `dcfadc19` | (not captured in evidence) | 2026-11-12 | commitlint | 15 | pull action image 5, checkout 3, lint 3 |

## R8 — feature-matrix timing collection (collect first, no optimization yet)

- **Decision (as required):** **Collect timings first — optimization not yet justified by collected data alone; revisit after more CI/per-combo samples if needed.**
- **Collected evidence:** CI feature-matrix totals 239s check step / 254s job (run 43609375823, 2eee9aaf) and 71s check step / 85s job (run 43379058938, 225a5401). Local healthy clean run: 48.26s total, 58/58 combinations clean. Local partial/unhealthy run (2026-11-14): 121.89s, 42/58 completed, 16 `ring`-related failures in that env (not a healthy baseline).
- **Not yet available:** per-combination timings and pass/fail breakdown by combination from collected CI evidence. The 58-combination count is known, but no per-combo samples were recorded in the available data.
- **Implication:** no feature-matrix optimization is recommended from currently collected data in this task. Additional CI runs and per-combo timing samples are needed before judging bounded parallelism/sharding/subset strategies.
- **Task 7 final decision (2026-11-15, SDD implementation pass):** no feature-matrix optimization implemented. Collected totals/samples remain insufficient to justify bounded parallelism, sharding, or subset changes without additional per-combo and post-cache CI evidence; preserve current 58-combo coverage and pass/fail semantics. Revisit only with data-backed justification.

## R11 — release and per-combo breakdown evidence (available totals only)

- **Available totals/breakdowns recorded:** local clean release build 415.15s real (`Finished release profile ... 6m 55s`, 3 crates: `mediapm-cas`, `mediapm-conductor`, `mediapm`); CI release job 308s with build release packages 295s (run 43609375823) and 148s with 134s build step (run 43379058938). Feature-matrix totals recorded above (CI and local healthy/unhealthy).
- **Not yet captured:** per-combo feature-matrix durations from CI or healthy instrumented runs; per-crate aggregate release totals, `rmeta_time`, and complete per-unit phase/link attribution for the captured release `--timings` run (fields not exposed in the HTML/log). No per-crate/per-phase release aggregate table is available from collected evidence.
- **Action taken in this task:** record available totals and explicitly defer finer breakdown capture to future measurement work.
- **Follow-up (2026-11-16, Task 3 transcription):** Per-unit hotspot timings from the stable release `--timings` HTML unit timing table (including frontend/codegen splits where shown and all `mediapm*` unit rows) are transcribed in D4.1 from the captured 2026-11-15 artifact/log; no rerun or regeneration was performed.

## R4 anomalies, context, and gaps

- **`ring v0.17.14` `ar` SIGABRT (2026-11-14 partial):** occurred under nix-wrapped clang/ar in srt/Anthropic sandbox (macOS aarch64); caused time-to-fail for `test-doc-all`, `test-all`, `build-all`, release build and 16/58 feature-matrix failures. **Not reproduced in 2026-11-15 clean rerun** (all passed, 58/58 matrix). Treat as env/toolchain-sensitive anomaly, not a healthy baseline.
- **Disk pressure:** 94-96% used during reruns/partial pass (`/System/Volumes/Data`, as low as ~23 GiB available in clean rerun window; ~15.3 GiB free at end of partial pass per evidence). Timing records should continue to include disk headroom.
- **Sandbox/toolchain specificity:** srt/Anthropic sandbox, nucleus toolchains/SDK paths, clang 21.1.8/lld 21.1.8 context recorded for clean rerun; partial-run toolchain context less complete.
- **CI metadata gaps:** runner CPU/memory details, cache state, Rust cache configuration (currently none in CI per plan), queue time, and full step lists for run 43379058938 not fully captured in provided evidence.
- **Local gaps:** no clean local baseline for `cargo fmt-check`, `cargo bin rumdl check .`, `cargo clippy-all`, or `cargo check --workspace --all-targets --all-features` in the 2026-11-15 sequential rerun set (warm 2026-11-14 pass values recorded). No Windows local runs. No per-test nextest report artifact beyond summary/slow list. No per-combo feature-matrix timing breakdown. Per-unit frontend/codegen hotspot timings for the captured stable release `--timings` run are transcribed in D4.1; per-crate aggregate totals, `rmeta_time`, and complete per-unit phase/link attribution remain unavailable from the captured artifacts.
- **Comparability note:** CI and local measurements are different environments (ubuntu/windows runners vs macOS sandbox), different cache/warm states, and different dates/commits; compare only with these caveats.

## 2026-11-15 residual diagnostics execution record (D0–D7, exhaustive attempt, completed 2026-11-15T15:37Z)

**Record ID / date:** `residual-diagnostics-2026-11-15-final` (2026-11-15T14:59Z–15:37Z UTC).  
**Scope:** diagnostics-only execution of `.pi/plans/2026-11-15-residual-followups-diagnostics-plan.md` (D0–D7). No implementation, optimization, commits, hook/CI/test-runner behavior changes, and no R12.  
**Primary evidence staging (local):** `/tmp/mediapm-diag/` (saved `gh run view --json`, `gh api .../jobs --paginate`, `gh run view --log --job ...`, recent-run JSON, per-command logs, per-combo matrix logs, release timing/profiling logs and HTML artifact in `target/cargo-timings/`). Method: large `gh` API/log reads saved to files first, then parsed with `python3`/`rg`/`awk`/`tail`.  
**Headroom (start→end):** `/System/Volumes/Data` 93%→94% used (~29 GiB→~26.8 GiB avail), `target/` 2.9G→6.8G after matrix/profiling runs. No `<2%` free condition reached; no cleanup outside staging evidence, no `scripts/clean-mediapm-temp.sh`, no `cargo clean`.

### D0 — Pushed commit CI diagnostics (`7295663a`, run `43713782110`)

| Field | Value |
|---|---|
| Workflow / run | `CI: Build & Test` #405, run `43713782110`, attempt 1, `success`, event `push`, branch `main` |
| Head | `7295663a4dae65fc117ea0eb88d6d3f0ccb95c4a` — `docs(ci): record final R8 matrix decision` |
| URL | <https://github.com/polyipseity/mediapm/actions/runs/43713782110> |
| Created / started / updated (completed span) | `2026-11-15T12:25:40Z` / first job start `2026-11-15T12:25:42Z` / `2026-11-15T12:42:17Z` (run wall 997s; first-job queue/start delta 2s) |
| Commitlint companion | run `43713782049` (same SHA), `success`, `2026-11-15T12:25:40Z`–`12:25:56Z` (14s job) |

#### Jobs and key steps (all `completed/success`)

| Job (id) | Runner (group/labels) | Job window / duration | Key step durations (s) | Cache/log notes |
|---|---|---:|---:|---|
| Build & Test (`132810817159`) | `GitHub Actions 1000020106` / `GitHub Actions`, `ubuntu-latest` | 12:25:43Z–12:42:17Z / 994s | checkout 2, setup rust 2, cache 1, **cheap checks 426**, ffprobe 23, **tests 431**, **doctests+rustdoc 42**, **build 49**, post-cache 13 | Restore: `No cache found.` Save upload completed: `552482191` bytes (100%) |
| Feature Matrix (`132810817447`) | `GitHub Actions 1000020108`, `ubuntu-latest` | 12:25:43Z–12:29:36Z / 233s | checkout 2, setup rust 0, cache 1, **check every feature combination 217**, post-cache 8 | Restore: `No cache found.` Save: `375969012` bytes (100%) |
| Windows: Workspace Tests (`132810817496`) | `GitHub Actions 1000020107`, `windows-latest` | 12:25:42Z–12:32:21Z / 399s | checkout 11, setup rust 6, cache 2, **workspace tests 243**, test-runner gate tests 8, completion checks 94, CAS target check 9, post-cache 19 | Restore: `No cache found.` Save: `326577135` bytes (100%) |
| Release Build (`132810817580`) | `GitHub Actions 1000020109`, `ubuntu-latest` | 12:25:42Z–12:30:59Z / 317s | checkout 1, setup rust 4, cache 1, **release build 299**, post-cache 8 | Restore: `No cache found.` Save: `438356771` bytes (100%) |

**Cache diagnosis (D0):** first run with explicit `Swatinem/rust-cache` steps in this workflow form; all jobs cold-restored (`No cache found`) and saved caches on completion (sizes above). `cache: false` on `actions-rust-lang/setup-rust-toolchain` with separate cache steps confirmed.

#### Log-level diagnostics (extracted)

- **Build & Test / Run tests:** `cargo-nextest v0.9.146` used, `Finished test profile in 2m 09s`, `Starting 2946 tests across 46 binaries`, **`Summary [119.928s] 2946 tests run: 2946 passed (1 slow), 0 skipped`** (slow: `mediapm-tests::scripts pwsh_janitor_dry_run_and_real_run`).
- **Build & Test / Run doctests and rustdoc:** doctest compile `Finished test profile ... 20.36s`; doctests total **9 passed** (`mediapm` 2, `mediapm_cas` 1, `mediapm_test_support` 1, `mediapm_utils` 5; others 0). Rustdoc/link-gate `Finished dev profile ... 17.06s`.
- **Build & Test / Build project:** `Finished dev profile ... 48.86s`.
- **Feature Matrix:** `cargo --locked run --package test-runner -- feature-matrix --locked` ended with **`feature matrix: 58 combinations, all clean`**. 59 `Finished dev profile` lines observed in step log; **no per-combination labeled timing rows emitted** (CI per-combo extraction unavailable).
- **Release Build:** `cargo --locked build --release -p mediapm-cas -p mediapm-conductor -p mediapm` completed in 299s step (no per-crate phase breakdown in log).
- **Windows:** `cargo --locked test-pkg mediapm-tests` **`[4.528s] 9 tests run: 9 passed, 0 skipped`**; gate/check steps completed as tabulated.
- **Commitlint (`43713782049`):** image pull 4s, checkout 4s, lint 3s, `Lint free! 🎉`.

#### D0 comparison (43713782110 vs 43609375823 vs 43379058938)

| Metric | 43713782110 (7295, explicit cache steps, cold save) | 43609375823 (2eee) | 43379058938 (225a) |
|---|---:|---:|---:|
| Run wall (created→updated) | 997s | 996s | 657s |
| Build & Test job | 994s | 957s | 655s |
|  - tests | 431s | 467s | 295s |
|  - doctests+rustdoc | 42s | 92s | 30s |
|  - build project | 49s | 366s | 293s |
| Feature Matrix job / step | 233s / 217s | 254s / 239s | 85s / 71s |
| Release job / step | 317s / 299s | 308s / 295s | 148s / 134s |
| Windows job | 399s | 598s | 386s |

**43609375823 details (captured):** Build & Test nextest **`[123.603s] 2946 tests run: 2946 passed (1 slow), 0 skipped`**, doctests 9 passed total, no explicit rust-cache restore/save steps in jobs. **43379058938 details:** nextest **`[100.715s] 2946 tests run: 2946 passed (1 slow), 0 skipped`**, doctests 9 passed total, no explicit rust-cache restore/save steps. Feature matrix both **58 combinations, all clean** (239s, 71s).

**Caveats:** workflow/cache/runner/image/load differences and commit scope make deltas non-causal. Unavailable: runner hardware specs, queue internals beyond timestamps, per-combination/per-crate timings in CI logs.

### D1 — Post-cache / recent CI sampling diagnostics

**Queries:** `ci-recent-2.json` (12 CI runs), `commitlint-recent-2.json` (12 commitlint runs).

| CI Build & Test run | Commit | Conclusion | Created (UTC) | Sampling |
|---|---|---|---|---|
| 43713782110 | 7295663a | success | 2026-11-15T12:25:40Z | detailed logs/jobs (post-cache workflow, cold save) |
| 43609375823 | 2eee9aaf | success | 2026-11-14T15:17:30Z | detailed logs/jobs |
| 43379058938 | 225a5401 | success | 2026-11-13T06:29:31Z | detailed logs/jobs |
| 43146911251 | dcfadc19 | success | 2026-11-12T03:15:01Z | listed, not log-sampled |
| 43020140844 | 48acf3db | success | 2026-11-11T12:25:03Z | listed, not log-sampled |
| 43013408629+ | 1838c3f etc. | failure | 2026-11-11… | excluded from green timing comparison |

**Commitlint (captured job/step timings):** 43713782049 (7295, 14s, lint 3s), 43609375822 (2eee, 12s, lint 3s), 43379058903 (225a, 16s, lint 4s), 43146911368 (dcfa, 15s, lint 3s), 43020140853 (48ac, 13s, lint 2s) — all success; recent list shows 12/12 success entries queried.

**Sample-size limit:** only **1** post-cache-workflow successful CI run exists (437, cold restore/save). No warmed-cache before/after dataset available; comparisons remain context-sensitive.

### D2 — Local baseline backfill diagnostics (R1/R4)

**Environment snapshot (2026-11-15T15:26Z):** macOS 27.2.1 (26B131), Darwin 27.2.0 arm64, `rustc 1.99.1 (58cf8f9c3 2026-10-13)`, `cargo 1.99.1 (5f94df478 2026-08-27)`, `stable-aarch64-apple-darwin` via `rust-toolchain.toml`, `RUSTC_WRAPPER=<unset>`. Toolchain env: `CC/CXX=/Library/Application Support/nucleus/toolchains/clang/bin/clang(++)`, `LD=.../lld/bin/ld.lld`, `SDKROOT/DEVELOPER_DIR=.../apple-sdk/MacOSX.sdk`; `AR/RANLIB/RUSTFLAGS/CARGO_*` unset. Tools present: clang/clang++/ld/lld/ar/llvm-ar/llvm-ranlib/rustfmt/clippy-driver/cargo-nextest. `~/.cargo` 416M, `~/.rustup` 2.1G. Disk 93%→94%, target 2.9G→6.8G during diagnostics.

| Command | Result | real/user/sys (s) | Exit | Evidence |
|---|---|---:|---:|---|
| `RUSTC_WRAPPER= cargo fmt-check` | pass | 0.93 / 0.60 / 0.09 | 0 | earlier captured output (no per-file count emitted) |
| `RUSTC_WRAPPER= cargo bin rumdl check .` | pass (`No issues found in 53 files`) | 7.20 / 0.49 / 0.50 | 0 | earlier captured output |
| `RUSTC_WRAPPER= cargo clippy-all` | pass (`Finished dev profile` 4.00s) | 4.17 / 3.21 / 1.34 | 0 | `/tmp/mediapm-diag/r4/clippy-all.log` (start/end disk recorded) |
| `RUSTC_WRAPPER= cargo check --workspace --all-targets --all-features` | pass (`Finished dev profile` 5.55s; deps downloaded incl. proptest/rand) | 5.65 / 1.70 / 1.04 | 0 | `/tmp/mediapm-diag/r4/workspace-check.log` |
| `cargo test-all` | not rerun locally as full fresh command (heavy); CI evidence captured | CI nextest 119.928s, 2946/2946 pass, 1 slow | 0 (CI) | `43713782110-build.log`, run 436/433 logs |
| `cargo test-doc-all` | not rerun locally as full fresh command; CI evidence captured | doctests 9 pass, doctest compile 20.36s, rustdoc dev 17.06s, step 42s | 0 (CI) | CI logs |
| `cargo build-all` | not rerun locally as full fresh command; CI evidence captured | CI dev build 48.86s (437), 366s (436), 293s (433) | 0 (CI) | CI jobs/logs |
| `cargo --locked build --release -p mediapm-cas -p mediapm-conductor -p mediapm` | pass (stable `--timings`) | 191.10 / 679.48 / 29.71 | 0 | `/tmp/mediapm-diag/profiling/release-timings.log` |
| feature-matrix (58 combos) | pass, 58/58, sequential total 48.12s real | per-combo in D3 | 0 each | `/tmp/mediapm-diag/matrix/results.tsv`, logs |

**R4 notes:** commands above were run with recorded outcomes/timings where executed; fmt/rumdl had earlier timing captures without per-command disk-before/after in the same bundle (disk context recorded nearby). `test-all/test-doc-all/build-all` full local reruns were not repeated in this pass (cost/risk) and are backed by fresh CI evidence plus prior local baseline records; labeled as CI/prior rather than new local full-run timings.

### D3 — Feature-matrix per-combination diagnostics (R8/R11)

- **Enumeration:** 58 combinations from `RUSTC_WRAPPER= cargo --locked run --package test-runner -- feature-matrix --locked --print` (evidence: `/tmp/mediapm-diag/matrix/combos.txt`, derivation `scripts/test-runner/src/matrix.rs`).
- **Local full run (healthy):** all 58 executed sequentially as `RUSTC_WRAPPER= cargo --locked check -p <combo>` with per-combo start/end UTC, disk snapshots, real/user/sys, exit, pass/fail recorded. **58/58 pass, 0 failures.** Sequential total real **48.12s** (min 0.11s, max 12.55s, avg 0.830s). Evidence: `/tmp/mediapm-diag/matrix/results.tsv`, `/tmp/mediapm-diag/matrix/logs/*.log`.
- **Slowest combos (real s):** `mediapm-conductor --no-default-features` 12.55; `mediapm-conductor-builtin-import --features cli` 4.73; `mediapm-conductor --features cli` 4.03; `mediapm-test-support --no-default-features` 3.36; others ≤2.66.
- **CI extraction:** 437/436/433 logs all report `58 combinations, all clean` but emit no per-combination labeled timing rows; per-combo CI extraction unavailable (absence evidenced).

### D4 — Release/build profiling diagnostics (R11/R5/R7)

| Run | Command/mode | Result | real/user/sys (s) | Evidence |
|---|---|---|---:|---|
| Stable profiling | `RUSTC_WRAPPER= cargo --locked build --timings --release -p mediapm-cas -p mediapm-conductor -p mediapm` | pass, `Finished release profile [optimized] in 3m 11s`, **582 total units, 0 fresh**, timing report saved | 191.10 / 679.48 / 29.71 | `/tmp/mediapm-diag/profiling/release-timings.log`, `target/cargo-timings/cargo-timing-20261115T152956577Z-b883f48bd893db85.html` (+symlink) |
| Nightly attempt (diagnostic) | `cargo +nightly -Z timings build --release -p mediapm-cas -p mediapm-conductor -p mediapm` | pass, `Finished release profile [optimized] in 7m 47s`; warning `nix v0.29.0` future-incompat | 467.37 / 978.88 / 42.83 | `/tmp/mediapm-diag/profiling/nightly-z-timings.log` (heavy; not a recommended baseline) |
| Prior/CI | local clean release (prior record), CI release steps | pass | 415.15 prior local; CI 299s/295s/134s | baselines + jobs |

**Artifacts/coverage:** The stable `--timings` HTML report and build log remain the captured evidence. Per-unit hotspot data visible in the HTML unit timing table (totals, frontend/codegen splits where shown, features, and build-script-run rows) is transcribed below in D4.1. Fields not extractable from the captured HTML/log: `rmeta_time`, complete per-unit phase/link attribution beyond the table's frontend/codegen columns, and per-crate aggregate totals (some crates appear as multiple units with different targets or feature sets). `-Z timings` is nightly-only but was attempted successfully as diagnostic evidence (toolchain present for nightly invocation in this env; run completed).

#### D4.1 release timings hotspot transcription (stable `--timings`, 2026-11-15)

| Field | Value |
|---|---|
| Record ID / date | `release-timings-hotspots-2026-11-15-stable` (transcribed 2026-11-16 from diagnostics captured 2026-11-15T14:59Z–15:37Z UTC; build executed 2026-11-15T15:29:56Z–15:33:07Z) |
| Command | `RUSTC_WRAPPER= cargo --locked build --timings --release -p mediapm-cas -p mediapm-conductor -p mediapm` |
| Environment / date caveats | Local diagnostics pass, srt/Anthropic sandbox, macOS 27.2.1 (26B131), Darwin 27.2.0 arm64, `aarch64-apple-darwin` host/target. Toolchain: `rustc 1.99.1 (58cf8f9c3 2026-10-13)`, `cargo 1.99.1` (see D2 environment snapshot), `RUSTC_WRAPPER` unset. Disk ~93% used before/after this captured run per log (`/System/Volumes/Data`). No rerun or regeneration performed for this transcription. |
| Totals / build metadata (from HTML table + log) | `Finished release profile [optimized] in 3m 11s`; 191.10s real / 679.48s user / 29.71s sys; exit 0. Targets: `mediapm 0.0.0 (lib)`, `mediapm 0.0.0 (mediapm "bin")`, `mediapm-cas 0.0.0 (lib)`, `mediapm-cas 0.0.0 (mediapm-cas "bin")`, `mediapm-conductor 0.0.0 (lib)`, `mediapm-conductor 0.0.0 (mediapm-conductor "bin")`. Profile `release`, fresh units 0, dirty units 582, total units 582, max concurrency 5 (jobs=4 ncpu=10), build start `2026-11-15T15:29:56.577484Z`, total time `191.0s (3m 11.0s)`. |
| Evidence artifacts | `/tmp/mediapm-diag/profiling/release-timings.log`; `target/cargo-timings/cargo-timing-20261115T152956577Z-b883f48bd893db85.html` (+ symlink in same directory). |

**Top unit timings (HTML unit timing table, ranked by reported unit time):**

| Rank | Unit | Total | Frontend | Codegen | Features / notes |
|---:|---|---:|---:|---:|---|
| 1 | `nickel-lang-parser v0.3.0` | 86.2s | 15.6s (18%) | 70.6s (82%) | — |
| 2 | `mediapm-conductor v0.0.0` | 41.4s | 3.6s (9%) | 37.8s (91%) | `cli, default, fetch, progress, tool-presets` |
| 3 | `mediapm v0.0.0` | 39.6s | 4.3s (11%) | 35.3s (89%) | `cli, default, media-tagger` |
| 4 | `nickel-lang-core v0.18.0` | 35.9s | 4.3s (12%) | 31.6s (88%) | `default, doc, format, markdown, repl` |
| 5 | `mediapm v0.0.0 mediapm "bin"` | 35.7s | — | — | `cli, default, media-tagger`; frontend/codegen split not shown in table |
| 6 | `mediapm-conductor v0.0.0 mediapm-conductor "bin"` | 28.3s | — | — | `cli, default, fetch, progress, tool-presets`; frontend/codegen split not shown in table |
| 7 | `malachite-nz v0.9.1` | 19.4s | 4.4s (23%) | 15.0s (77%) | `enable_serde, float_helpers, serde, std` |
| 8 | `mediapm-cas v0.0.0` | 11.2s | 1.7s (16%) | 9.4s (84%) | `cli, default` |
| 9 | `musicbrainz_rs v0.13.0` | 10.6s | 2.2s (21%) | 8.4s (79%) | `async, rustls` |
| 10 | `rustls v0.23.37` | 10.3s | 1.8s (17%) | 8.5s (83%) | `log, logging, ring, std, tls12` |
| 11 | `reqwest v0.12.28` | 9.2s | 0.8s (9%) | 8.4s (91%) | `__rustls, __rustls-ring, __tls, blocking, json, rustls-tls, rustls-tls-webpki-roots, rustls-tls-webpki-roots-no-provider, stream` |
| 12 | `regex-automata v0.4.14` | 8.2s | 1.2s (15%) | 7.0s (85%) | large unicode/perf feature set |
| 13 | `chromaprint-next v0.1.0` | 7.9s | 0.3s (3%) | 7.6s (97%) | `default` |
| 14 | `mediapm-cas v0.0.0 mediapm-cas "bin"` | 6.6s | — | — | `cli, default`; frontend/codegen split not shown in table |
| 15 | `clap_builder v4.6.0` | 6.0s | 1.0s (17%) | 5.0s (83%) | `color, env, error-context, help, std, string, suggestions, usage, wrap_help` |
| 16 | `onig_sys v69.9.1 build-script (run)` | 5.3s | — | — | build-script run; frontend/codegen not applicable |
| 17 | `tokio v1.52.3` | 5.1s | 1.8s (35%) | 3.3s (65%) | runtime/io feature set |
| 18 | `comrak v0.49.0` | 5.0s | 1.0s (20%) | 4.0s (80%) | markdown/CLI feature set |
| 21 | `ring v0.17.14 build-script (run)` | 4.3s | — | — | `alloc, default, dev_urandom_fallback` |
| 25 | `tree-sitter v0.26.7 build-script (run)` | 3.9s | — | — | `default, std` |

**Workspace crate units visible in the HTML table (all `mediapm*` rows):**

| Rank | Unit | Total | Frontend | Codegen | Features / notes |
|---:|---|---:|---:|---:|---|
| 2 | `mediapm-conductor v0.0.0` | 41.4s | 3.6s (9%) | 37.8s (91%) | `cli, default, fetch, progress, tool-presets` |
| 3 | `mediapm v0.0.0` | 39.6s | 4.3s (11%) | 35.3s (89%) | `cli, default, media-tagger` |
| 5 | `mediapm v0.0.0 mediapm "bin"` | 35.7s | — | — | frontend/codegen split not shown |
| 6 | `mediapm-conductor v0.0.0 mediapm-conductor "bin"` | 28.3s | — | — | frontend/codegen split not shown |
| 8 | `mediapm-cas v0.0.0` | 11.2s | 1.7s (16%) | 9.4s (84%) | `cli, default` |
| 14 | `mediapm-cas v0.0.0 mediapm-cas "bin"` | 6.6s | — | — | frontend/codegen split not shown |
| 26 | `mediapm-cli v0.0.0` | 3.5s | 0.3s (9%) | 3.2s (91%) | `media-tagger` |
| 32 | `mediapm-utils v0.1.0` | 2.8s | 0.4s (14%) | 2.4s (86%) | `nickel, progress, report` |
| 43 | `mediapm-conductor-builtin-archive v0.1.0` | 2.0s | 0.2s (8%) | 1.9s (92%) | — |
| 58 | `mediapm-conductor-cli v0.0.0` | 1.4s | 0.2s (11%) | 1.3s (89%) | — |
| 104 | `mediapm-cas-cli v0.0.0` | 0.7s | 0.1s (12%) | 0.6s (88%) | — |
| 126 | `mediapm-conductor-builtin-import v0.1.0` | 0.5s | 0.1s (22%) | 0.4s (78%) | `fetch` |
| 145 | `mediapm-cli v0.0.0` | 0.4s | 0.2s (57%) | 0.2s (43%) | `default, media-tagger`; separate unit from rank 26 |
| 191 | `mediapm-conductor-builtin-export v0.1.0` | 0.3s | 0.1s (23%) | 0.2s (77%) | — |
| 198 | `mediapm v0.0.0 build-script` | 0.3s | — | — | `cli, default, media-tagger` |
| 203 | `mediapm v0.0.0 build-script (run)` | 0.3s | — | — | `cli, default, media-tagger` |
| 213 | `mediapm-conductor v0.0.0 build-script` | 0.3s | — | — | `cli, default, fetch, progress, tool-presets` |
| 236 | `mediapm-conductor v0.0.0 build-script (run)` | 0.3s | — | — | `cli, default, fetch, progress, tool-presets` |
| 237 | `mediapm-conductor-builtin-echo v0.1.0` | 0.3s | 0.1s (19%) | 0.2s (81%) | — |
| 251 | `mediapm-cas v0.0.0 build-script (run)` | 0.2s | — | — | `cli, default` |
| 298 | `mediapm-cas v0.0.0 build-script` | 0.2s | — | — | `cli, default` |
| 336 | `mediapm-conductor-cli v0.0.0` | 0.2s | 0.1s (61%) | 0.1s (39%) | separate unit from rank 58 |
| 371 | `mediapm-utils v0.1.0` | 0.1s | 0.1s (67%) | 0.0s (33%) | separate unit from rank 32 |
| 382 | `mediapm-conductor-builtin-fs v0.1.0` | 0.1s | 0.1s (46%) | 0.1s (54%) | — |
| 423 | `mediapm-cas-cli v0.0.0` | 0.1s | 0.1s (70%) | 0.0s (30%) | separate unit from rank 104 |

**Observed dominance (based on captured unit table only):** By reported unit time, `nickel-lang-parser v0.3.0` is the largest single unit (86.2s, 70.6s codegen / 15.6s frontend). The next largest units are `mediapm-conductor` lib (41.4s), `mediapm` lib (39.6s), `nickel-lang-core` (35.9s), `mediapm` bin (35.7s), and `mediapm-conductor` bin (28.3s). Where frontend/codegen splits are shown, codegen accounts for most of the reported time on these top units. Unit times overlap under concurrency (max 5, jobs=4, ncpu=10), so unit totals do not sum to wall time (191.0s). This transcription makes no causal optimization claim and does not aggregate per-crate totals beyond the unit rows shown.

**Not extractable from captured HTML/log:** `rmeta_time`; explicit per-unit link-phase timing (bins/build-script runs may include linking in reported total but the table does not expose a separate link column); per-crate aggregate totals across repeated units/targets; and any field not present in the unit timing table or build log. The nightly `-Z timings` run remains a separate diagnostic context (467.37s real, 582 units, 0 fresh) and is not mixed into this stable-run transcription.

### D5 — R5/R7 residual feasibility diagnostics (analysis, no changes)

Sources re-examined: `scripts/test-runner/src/{doc.rs,test.rs,nextest.rs,cargo.rs,gates/*}`, `.github/workflows/ci.yml`, `prek.toml`, `.cargo/config.toml`, `.config/nextest.toml`, prior task-5/task-7 findings.

| Opportunity | Evidence | Required outcomes/gates | Safety/equivalence | Status |
|---|---|---|---|---|
| Doctest ↔ rustdoc reuse/merge | `doc.rs`: separate `cargo test --doc` then `cargo doc --locked --no-deps --workspace --all-features --document-private-items` (unconditional scope intentional). | Preserve doctest results and full rustdoc/private-items/all-features link gate. | Not proven safe/equivalent. | not feasible now |
| Pre-push vs CI overlap reduction | `prek.toml` pre-push runs cargo-check/clippy/test/doc on push; CI runs broader jobs (cheap checks+tests+doctests+build, feature-matrix 58, release pkgs, windows checks). | Preserve local feedback contract and all CI-required outcomes. | Requires policy/contract change; not proven safe to drop. | blocked (out of scope) |
| CI/test-runner sequencing or artifact reuse | Separate CI jobs/runners; in-job reuse observed (build step faster in 437) but context-dependent. | Preserve step/job isolation and pass/fail signals. | Not proven as general optimization from data. | needs targeted proof |
| Feature-matrix parallelism/sharding/subset | 58 derived checks; local full sequential 48.12s (warm) but per-combo variance up to 12.55s; CI totals 71–239s. | Must preserve 58-combo coverage unless approved. | Safe optimization not demonstrated; sharding/parallelism changes semantics/resource use. | data collected; implementation not justified here |
| Nextest vs cargo test overlap | nextest cannot run doctests; doctests handled separately. | Doctests required. | N/A. | not feasible |

**Conclusion:** no residual duplication/reuse opportunity proven safe to implement now.

### D6 — Environment, runner, comparability, and gap diagnostics (R4)

- **Local:** macOS 27.2.1 arm64, nucleus clang/lld/SDK toolchains, `RUSTC_WRAPPER` unset, disk 93%→94%, target grew to 6.8G during diagnostics. `sysctl machdep.cpu.brand_string` previously `Operation not permitted` (recorded anomaly). Windows-local and real-online demo (`cargo run --package mediapm --example mediapm_demo_online -- --online`) not runnable here/manual (recorded).
- **CI:** runner names/groups/labels, timestamps, cache restore/save outcomes and sizes, job/step metadata captured from APIs/logs. Not exposed: CPU/RAM specs, queue-internal reasons, full runner image manifests, cache-hit-rate metric.
- **Comparability:**

| Dimension | Local | CI ubuntu | CI windows |
|---|---|---|---|
| OS/arch | macOS 27.2.1 aarch64 | ubuntu-latest (ubuntu-26.04 image seen) x64 | windows-latest x64 |
| Cache state | warm target (state evolved during pass) | 437 cold restore + saved; 436/433 no explicit rust-cache steps | 437 cold restore + saved |
| Scope | ad-hoc command diagnostics + full local matrix/profiling | workflow jobs/steps | `test-pkg` + completion/CAS checks |
| Comparability | low vs CI | comparable with caveats | separate lane |

### D7 — Exhaustive diagnostics completeness checklist (final)

| ID | Diagnostic | Status | Evidence / blocker |
|---|---|---|---|
| D0.1 | 43713782110 terminal run metadata (IDs/SHA/branch/event/attempt/status/times/URL) | captured | `run-43713782110.json`, jobs API |
| D0.2 | 43713782110 every job/step with runner/labels/timestamps/durations/conclusions | captured | `run-43713782110-jobs.json`, tables |
| D0.3 | 43713782110 log-level key-step diagnostics (checks/ffprobe/tests/doctests+rustdoc/build/matrix/release/windows/cache) | captured | `43713782110-*.log` (summaries/counts/cache sizes; CI lacks per-combo/per-crate rows) |
| D0.4 | Compare 43713782110 vs 43609375823 vs 43379058938 | captured | jobs/log comparison + extracted summaries |
| D0.5 | Unavailable CI fields/APIs/attempt evidence | captured | runner specs/queue internals/per-combo rows absent; API/log saves succeeded |
| D1.1 | Recent successful main CI sample (min 437/436/433 + discoverable) | captured | `ci-recent-2.json`; 437/436/433 detailed, others listed |
| D1.2 | Commitlint timings for corresponding/recent commits | captured | 43713782049, 43609375822, 43379058903, 43146911368, 43020140853 job/step timings |
| D1.3 | Post-cache before/after dataset and sample-size limits | captured | only 1 post-cache-workflow run (437 cold save), no warmed comparison |
| D1.4 | Queue/wait/full-step metadata where derivable | captured (derivable only) | timestamps/step deltas; internal queue reasons not exposed |
| D2.1 | Local `cargo fmt-check` (R4) | captured | pass 0.93s |
| D2.2 | Local `cargo bin rumdl check .` (R4) | captured | pass 53 files 7.20s |
| D2.3 | Local `cargo clippy-all` (R4) | captured | pass 4.17s exit 0, `r4/clippy-all.log` |
| D2.4 | Local `cargo check --workspace --all-targets --all-features` (R4) | captured | pass 5.65s exit 0, `r4/workspace-check.log` |
| D2.5 | Local `cargo test-all` diagnostics | captured-partial | not rerun locally as full fresh command; CI 437/436/433 summaries/counts captured |
| D2.6 | Local `cargo test-doc-all` diagnostics | captured-partial | not rerun locally as full fresh command; CI doctest/rustdoc counts/timings captured |
| D2.7 | Local `cargo build-all` diagnostics | captured-partial | not rerun locally as full fresh command; CI build timings captured |
| D2.8 | Local release build + D4 profiling | captured | stable `--timings` 191.10s exit 0, HTML artifact; nightly `-Z timings` attempted 467.37s exit 0 |
| D2.9 | Local feature-matrix per D3 | captured | 58/58 pass with per-combo timings |
| D3.1 | Enumerate all 58 combinations/feature sets | captured | `matrix/combos.txt` |
| D3.2 | Per-combination command/duration/exit/pass-fail/order for all 58 (healthy run) | captured | `matrix/results.tsv` + per-combo logs (all pass) |
| D3.3 | CI per-combo extraction or absence evidence | captured (absence) | CI reports totals only, no per-combo rows |
| D3.4 | Totals/pass-fail/anomalies/comparability | captured | 58/58 clean local and CI; variance/totals recorded |
| D4.1 | Release per-crate/per-phase diagnostics (`--timings`/logs) | captured (transcribed with noted limits) | Stable `--timings` HTML unit timing table transcribed in D4.1 (top units, frontend/codegen splits where shown, and all `mediapm*` unit rows) with artifact/log refs and totals; `rmeta_time`, complete per-unit phase/link attribution, and per-crate aggregate totals remain unavailable from captured artifacts |
| D4.2 | Dev/build-all/test-all/test-doc-all phase diagnostics where supported | captured | CI compile/execute/rustdoc/test splits captured; local check/clippy/build-profile markers captured |
| D4.3 | Unavailable profiling flags/modes attempts/evidence | captured | nightly `-Z timings` attempted successfully (diagnostic), stable `--timings` captured |
| D5.1 | Re-examine test-runner/CI/prek/cargo/nextest sources and prior findings | captured | source review recorded |
| D5.2 | Diagnostic matrix for every suspected duplication/reuse opportunity | captured | feasibility table |
| D5.3 | Explicit doctest↔rustdoc, pre-push vs CI, profile/feature/target, sequencing conclusions | captured | not proven safe/blocked conclusions recorded |
| D6.1 | Local diagnostics (df/du/toolchains/env/paths/sandbox) | captured | env/toolchain/disk/target/tools/anomaly recorded |
| D6.2 | CI diagnostics (runner labels/groups/cache logs/steps/queue derivations) | captured | exposed metadata/logs captured; hardware/queue internals not exposed |
| D6.3 | Windows/online diagnostics or exact non-runnable items with attempts | captured | CI windows evidence; windows-local/real-online manual not runnable here |
| D6.4 | Comparability matrix | captured | table |
| D7.1 | Update baselines with D0–D6 results, tables, evidence refs, and completeness checklist | captured | this section |
| D7.2 | Every residual follow-up has completed diagnostic entry; no unspecified/unattempted diagnostic remains | captured | all D0–D7 attempted and recorded (captured or captured-partial with explicit blocker/evidence) |

**Remaining gaps (not capturable in this pass):** warmed post-cache CI sample (none exists yet), CI per-combination labeled timing rows (not emitted), runner hardware/image/queue internals (not exposed), Windows-local and real-online manual runs (not runnable here), and full local fresh reruns of `test-all/test-doc-all/build-all` as a single isolated R4 bundle (substituted by fresh CI evidence and prior local baselines). These are recorded as environment/data constraints, not unattempted diagnostics.
