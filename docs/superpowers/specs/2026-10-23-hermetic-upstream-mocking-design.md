# Hermetic coverage through a mocked upstream

Design for making conditionally-skipped and conditionally-trimmed tests assert what the code must do, by mocking the upstream a test needs. Approved in session on 2026-10-23.

## Problem

Four families of tests have coverage that depends on a condition rather than on the behavior under test.

1. **Gated away entirely.** `ci_mode_detected()` skips the only test that runs the real yt-dlp to ffmpeg to tagger to rsgain chain (`tests/int/online_sync_post_sync_dump.rs:68`, `mediapm_demo_online.rs:3763`); `MEDIAPM_RUN_ONLINE_SYNC` makes the same tests opt-in; `large-tests` gates the 65 MiB streaming tests; one `#[ignore]`d fixture regenerator sits in `materializer/resolve.rs:2116`; `#[cfg(unix)]` removes 3 conductor tests and their 4 helpers, 2 temp-retry tests, 1 cover-art test and the blob-store identity helper; `skip_unless("bash"/"pwsh")` skips janitor self-tests; the janitor's chmod-000 case skips as root; `tests/e2e/service.rs:31` skips a whole test when ffprobe is absent.
2. **Trimmed assertions.** `tests/int/all_platform.rs` says of itself that it "DO NOT assert that every managed tool was synced (that depends on network availability); they assert the structural invariants of whatever entries were produced". The 7 `tests/int/tool_sync` modules are pinned to media-tagger, the one tool that "resolves without network", so ffmpeg, yt-dlp, deno, rsgain, sd and both macOS chain members have no end-to-end coverage. `mediapm_demo_online`'s test-harness path asserts only a `run_mode: config-only` manifest. Eight coverage-matrix rows are `[partial]`, two of them literally "Level-3 only".
3. **Serialized rather than fixed.** `.config/nextest.toml` puts the network tests in a `max-threads = 1` group with `terminate-after = 600`, which keeps them alive instead of making them fast.
4. **No seam reaches the payload bytes.** `github_release_asset_url` (`tools/provider/mod.rs:440`) hardcodes `https://github.com`, so the download phase cannot be redirected. `MEDIAPM_GITHUB_API_BASE`, `MEDIAPM_MARTIN_RIEDL_BASE` and `MEDIAPM_JELLYFIN_BASE` reach metadata and the macOS chain, but nothing reaches a GitHub release asset, and that is why `all_platform` cannot assert a full sync.

The documentation also claims a gate that does not exist: `example-execution-policy.instructions.md:17` and `coverage-matrix.md:612` say the three-level model covers `mediapm_cli_add_tools` and `mediapm_cli_add_hierarchy`, but neither `main_is_exercised` calls `ci_mode_detected()`, and both module docs say they download nothing.

## Principle

**A test asserts what the code must do, not what the environment allowed.** Where a condition currently selects a weaker assertion, the condition becomes a second transport: one assertion function, two ways to reach it.

**A seam is a compile-time boundary.** Code that can redirect a download or a metadata read exists only in test builds, so a shipped binary cannot be redirected by its environment.

**A fake must not pass a test the real thing would fail.** A stub may replace a remote server, because the pipeline's behavior does not depend on who serves the bytes. A stub may stand in for a local tool only where the test asserts the pipeline around that tool; where a test asserts the tool's own semantics, the real tool is the only acceptable transport, and the behavior stays on the real-network path until a faithful fake exists.

**A skipped test is a decision, not a state.** Where a real upstream is the only way to reach a behavior, the test that needs it is an explicit second path, never a runtime skip inside the shared one.

## Design

### 1. Four seams, all behind `test-seams`

Each seam is read per call, never cached in a `OnceLock`, so one test binary can point several servers at several hosts in turn.

| Seam | Status | Reaches |
| --- | --- | --- |
| `MEDIAPM_GITHUB_API_BASE` | exists, moves behind the feature | GitHub metadata, tag refs |
| `MEDIAPM_GITHUB_DOWNLOAD_BASE` | new | yt-dlp, deno, rsgain, sd, BtbN ffmpeg payloads |
| `MEDIAPM_MARTIN_RIEDL_BASE` | exists, moves behind the feature | the ffmpeg macOS snapshot and release permalinks |
| `MEDIAPM_JELLYFIN_BASE` | exists, moves behind the feature | the ffmpeg macOS fallback archive |
| `musicbrainz_endpoint`, `caa_endpoint` | exist, step options | the tagger |

The feature is declared by `mediapm` and `mediapm-conductor`, which are the two crates that build or fetch a URL. With it off, the seam functions do not exist and each URL builder uses its production constant, so absence is a compile-time fact rather than a runtime check.

`cargo test-pkg <crate>` and every workspace gate already pass `--all-features`, so the feature is on wherever the suite runs. A bare `cargo test -p mediapm` compiles the seam out; the mock helper detects that and fails with the feature name and the command to use, rather than silently reaching the real network.

### 2. A dev-only mock upstream

A new workspace member `src/mediapm-test-support/` holds what both crates' tests need and neither can host for the other, since `mediapm` depends on `mediapm-conductor` and the dependency cannot run backwards.

- `mock_upstream.rs` mounts one `wiremock` server per host, including self-directed 302s so redirect following and the macOS permalink probe are exercised rather than bypassed.
- `stub_payload.rs` builds the synthetic per-OS archives.
- `seams.rs` holds the RAII guards that set and clear the four seams under `lock_process_env()`, restoring the previous state on drop so a panicking test cannot leak a seam into a sibling.

The crate is a dev-dependency only and publishes nothing.

### 3. Stub payloads

Every OS slot gets an archive, because provisioning downloads and CAS-imports all supported platforms regardless of host. Only the host's stub is ever executed, so only it must be runnable; the others need only extract and produce a content map.

A stub is a shell script on unix and a `.cmd` on Windows, and the extraction path already restores the execute bit (`tools/provider/mod.rs:1790`), so no permission work is needed. Each stub parses only what its preset's command template passes:

| Tool | Stub behavior |
| --- | --- |
| yt-dlp | writes the video plus the `.url`, `.webloc` and `.desktop` sidecars |
| ffmpeg | copies the input to the declared output variant |
| rsgain | writes the `REPLAYGAIN_*` keys into the document it is handed |
| media-tagger | not a download; its provider is `GenerateLauncher`, and its endpoints are already injectable |

### 4. The two-path shape

Each behavior gets one assertion function. A hermetic test calls it against the mock and always runs. The real-network test calls the same function and carries `#[ignore = "real network; MEDIAPM_RUN_ONLINE_SYNC=1"]`, so a CI run reports a real pass instead of a skip, and the two assertion sets cannot drift because there is only one.

`online_sync_post_sync_dump` and `mediapm_demo_online::main_is_exercised` stop branching on `ci_mode_detected()` and `MEDIAPM_RUN_ONLINE_SYNC`. The example binary keeps its real-network behavior for an explicit `cargo run --example`, which is a different question from what the test harness runs.

### 5. What a stub cannot cover

A stub cannot apply tags. Writing tags into a Matroska file means a real muxer, so the apply step (`-map_metadata`) has no hermetic path today, and the demo's applied-file tag assertions cannot be shared verbatim across transports.

The slice-2 plan is a spike first: parse the ffmetadata document in a stub and append a Matroska `Tags` element to a committed fixture, which is valid when the Segment uses unknown size. If ffprobe reads an appended `Tags` element, the fixture test becomes mutation-proof and the apply step gains real coverage. If it does not, the fixture stays what it honestly is, a plumbing check that the right bytes reached the right path, and the coverage-matrix row stays `[partial]`.

A pre-tagged fixture must never be recorded as apply-step coverage: it passes whether or not the tagger ran.

### 6. Documentation

`example-execution-policy.instructions.md` is rewritten around the two-path model, replacing the three-level skip model. The `mediapm_cli_add_tools` and `mediapm_cli_add_hierarchy` claim is corrected, since those examples download nothing and have no gate to document. `ci-workflow.instructions.md`, `rust-conventions.instructions.md` and the coverage matrix follow the same change, and the selective-run guidance notes the `test-seams` requirement.

## Slice 1

Tool-sync end-to-end, which is where the coverage is thinnest.

1. Declare `test-seams` on both crates; move the three existing seams behind it; add `MEDIAPM_GITHUB_DOWNLOAD_BASE` to `github_release_asset_url`.
2. Create `mediapm-test-support` with the mock upstream, the stub payload builder and the seam guards, plus its own tests.
3. Convert `tests/int/all_platform.rs` to assert that every managed tool produced a payload for every OS slot, with a real-network twin.
4. Convert the 7 `tests/int/tool_sync` modules (`basics`, `composite`, `inline_deps`, `requires_sync`, `resolved`, `validation`, `dual_write`) so the assertions run against more than media-tagger: version targeting, composite versions, inlined same-step deps, skip paths, validation, and the dual-write contract.

## Deferred to slice 2

- The tag-apply spike and, if it succeeds, the mutation-proof fixture test.
- The remaining `[partial]` rows that are network-bound rather than mock-bound.
- Converting `online_sync_post_sync_dump` and `mediapm_demo_online` to the two-path shape, which needs the stub tools to produce the demo's declared artifacts.

## Testing

The new infrastructure is tested as production code is.

- A test asserts the feature-off path uses the production host, so the seam cannot leak into a release build unnoticed.
- The mock's mounts are asserted per host, including that a self-directed 302 is followed.
- Each stub payload is extracted and executed in a test, which is what proves the execute bit survives the real extraction path rather than assuming it.
- A test asserts the mock fails loudly when `test-seams` is off.
- The converted tests must be load-bearing: removing a provider's source from the mock must fail the assertion that names it.

## Success criteria

- `all_platform` asserts all six providers produced payloads, hermetically, with no network.
- The 7 `tool_sync` modules exercise providers other than media-tagger.
- No test in the suite silently skips a network path: each is either hermetic or an explicit `#[ignore]`d real-network twin.
- A release build cannot be redirected by any environment variable.
- `cargo test-all` is green, and `cargo run --package mediapm --example mediapm_demo_online` still reaches the real network.

## Out of scope

- The platform-conditional gates. They select a platform's own arm rather than hiding coverage, and each platform runs its own: blob-store identity (inode versus volume serial and file index), temp-retry errno classification, per-OS source selection, and the cover-art permission-bit mechanism. The one incidental case is `step_rpc_layering`'s fixture, which uses `sh -c "sleep 3"` only because it needs a step that outlasts a 1s budget; a portable sleeper would make those 3 tests run everywhere, and that is an optional add-on.
- `large-tests`, which `--all-features` already activates in CI, the workspace gates and the pre-push hook. It is absent only from a default local run.
- The `[ignore]`d fixture regenerator, which is a maintenance tool rather than coverage.
- The real-network tests' assertions about upstream properties, such as the current video's codec and dimensions. Those are already upstream-dependent.

## Risks

- **The mocked host is not the real host.** The checksum probe is skipped for github.com and objects.githubusercontent.com and will run against a mock host, and a mock answers a redirect the real host may not. Both are recorded rather than hidden, and the real-network path keeps its own assertions.
- **`feature-matrix` builds `test-seams` alone.** The job runs one `cargo check` per crate per feature combination, so the seam functions must compile with the feature on and the URL builders must compile with it off.
- **A stub drifts from the tool it replaces.** A stub that stops matching its preset's command template fails at the step, which is the loud failure wanted, but it is a real maintenance surface and belongs in the per-tool documentation.
