# Validation timing baselines and CI timing record

**Purpose:** repo-tracked record for validation timing baselines and CI timing evidence, supporting SDD plan `.pi/plans/2026-11-15-validation-timing-and-speedup-final-plan.md` recommendations **R1** (baselines and CI timing), **R4** (measurement reliability metadata), **R8** timing collection, and **R11** available breakdown evidence. This document records evidence only; it does **not** change CI, hooks, prek, or test behavior.

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

## R11 — release and per-combo breakdown evidence (available totals only)

- **Available totals/breakdowns recorded:** local clean release build 415.15s real (`Finished release profile ... 6m 55s`, 3 crates: `mediapm-cas`, `mediapm-conductor`, `mediapm`); CI release job 308s with build release packages 295s (run 43609375823) and 148s with 134s build step (run 43379058938). Feature-matrix totals recorded above (CI and local healthy/unhealthy).
- **Not yet captured:** per-combo feature-matrix durations from CI or healthy instrumented runs; per-crate/per-phase release build breakdowns (e.g. compile/link/codegen hotspots by crate or phase) beyond job/step totals and Cargo finish messages. No release profiling table by crate/phase is available from collected evidence.
- **Action taken in this task:** record available totals and explicitly defer finer breakdown capture to future measurement work.

## R4 anomalies, context, and gaps

- **`ring v0.17.14` `ar` SIGABRT (2026-11-14 partial):** occurred under nix-wrapped clang/ar in srt/Anthropic sandbox (macOS aarch64); caused time-to-fail for `test-doc-all`, `test-all`, `build-all`, release build and 16/58 feature-matrix failures. **Not reproduced in 2026-11-15 clean rerun** (all passed, 58/58 matrix). Treat as env/toolchain-sensitive anomaly, not a healthy baseline.
- **Disk pressure:** 94-96% used during reruns/partial pass (`/System/Volumes/Data`, as low as ~23 GiB available in clean rerun window; ~15.3 GiB free at end of partial pass per evidence). Timing records should continue to include disk headroom.
- **Sandbox/toolchain specificity:** srt/Anthropic sandbox, nucleus toolchains/SDK paths, clang 21.1.8/lld 21.1.8 context recorded for clean rerun; partial-run toolchain context less complete.
- **CI metadata gaps:** runner CPU/memory details, cache state, Rust cache configuration (currently none in CI per plan), queue time, and full step lists for run 43379058938 not fully captured in provided evidence.
- **Local gaps:** no clean local baseline for `cargo fmt-check`, `cargo bin rumdl check .`, `cargo clippy-all`, or `cargo check --workspace --all-targets --all-features` in the 2026-11-15 sequential rerun set (warm 2026-11-14 pass values recorded). No Windows local runs. No per-test nextest report artifact beyond summary/slow list. No per-combo feature-matrix timing breakdown. No per-phase/per-crate release profiling.
- **Comparability note:** CI and local measurements are different environments (ubuntu/windows runners vs macOS sandbox), different cache/warm states, and different dates/commits; compare only with these caveats.
