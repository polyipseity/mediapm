# Runtime Root Ownership and Payload Consumption Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a runtime root be created only by its owner, make `media add --local` stage the bytes it names, make a declared hash consumable as a step input, and pace MetaBrainz requests while retrying the failure their limiter causes.

**Architecture:** Three of the four changes are local corrections of a single wrong assumption each: that a write may create its own parent, that naming a hash stores it, and that a step input may hold something other than a reference. The fourth adds a pacer and a retry to the `MusicBrainz` fetch path. The conductor fixture change makes teardown deterministic rather than racy.

**Tech Stack:** Rust workspace (`mediapm`, `mediapm-conductor`, `mediapm-cas`, `mediapm-utils`), Tokio, `ractor`, `nickel-lang-core`, `musicbrainz_rs` over `api_bindium`/`ureq`, `wiremock` for hermetic HTTP tests, `nextest` via the `test-runner` crate.

**Spec:** `docs/superpowers/specs/2026-10-23-root-ownership-and-consumption-design.md`

## Global Constraints

- Version sequences are integers only; no compat scaffolding for unshipped revisions.
- No narration comments. Every touched item carries a doc comment stating purpose, invariants, side effects, and error behavior.
- No silent skips. A failure is surfaced or propagated, never defaulted away.
- Every behavior change lands with the test that pins it, in the same commit.
- Lint suppressions only as item-scoped `#[expect(<lint>, reason = "…")]`; never `#[allow]`.
- Run `cargo fmt --all` before `fmt-check`. `cargo clippy-pkg`/`test-pkg` already imply `--all-features`; never pass it again.
- A test that is meant to fail must fail for the stated reason; prove load-bearing tests by mutating the implementation and observing exactly that test fail, then revert.
- One commit per task, Conventional Commits with a mandatory scope (`mediapm`, `conductor`, `materializer`, `coverage`), body lines ≤100 characters.

## File Structure

| File | Responsibility |
| --- | --- |
| `src/mediapm-conductor/src/conductor_impl.rs` | Root creation at foreground entry points; a strict state write; a loop that stops when its root is gone |
| `src/mediapm-conductor/tests/int/root_ownership.rs` | New: the root-ownership contract at the public surface |
| `src/mediapm-conductor/tests/mod.rs` | `TestConductor` gains `shutdown()` |
| `src/mediapm-conductor/tests/**/*.rs` | Workflow-driving tests await that shutdown |
| `src/mediapm/src/service.rs` | `add_local_source_with_position` stages bytes and becomes async |
| `src/mediapm/src/main.rs` | The CLI caller awaits the new signature |
| `src/mediapm/tests/int/local_source_staging.rs` | New: add a local source, then sync |
| `src/mediapm-conductor/src/orchestration/step_worker/template.rs` | External-data resolution degrades to the hash without a store handle |
| `src/mediapm-conductor/tests/int/step_input_external_data.rs` | New: the resolved-input and `exists` contracts |
| `src/mediapm/src/builtins/media_tagger/musicbrainz.rs` | The pacer and the fetch retry |
| `src/mediapm/tests/int/media_tagger_hermetic.rs` | Pacer spacing and 502-then-200 wiremock tests |
| `.agents/coverage-matrix.md`, `.agents/instructions/*.md`, `src/mediapm-conductor/AGENTS.md` | The recorded contracts |

---

### Task 1: A runtime root is created by its owner

**Files:**

- Modify: `src/mediapm-conductor/src/conductor_impl.rs` (background loop at 119-150, `maintenance_pass` at 742, `save_state_file` at 829, `write_atomically` at 861, `ensure_actor_client` at 85, `run_gc` at 617)
- Create: `src/mediapm-conductor/tests/int/root_ownership.rs`
- Modify: `src/mediapm-conductor/tests/int/mod.rs`

**Interfaces:**

- Consumes: nothing.
- Produces: `fn ensure_runtime_root(paths: &RuntimeStoragePaths) -> Result<(), ConductorError>` and `fn runtime_root_is_gone(paths: &RuntimeStoragePaths) -> bool`, both private to `conductor_impl`. `write_atomically(path, write)` keeps its signature but no longer creates the parent directory.
- [ ] **Step 1: Write the failing tests**

In `conductor_impl.rs`'s `#[cfg(test)]` module, next to the existing `write_atomically` partial-failure test:

```rust
/// A save must not recreate the directory its owner removed.
///
/// The write path is the last step of a background pass, so a
/// `create_dir_all` here resurrects a removed runtime root as an orphan
/// holding one `state.json`. Failing instead is what lets the pass stop.
#[test]
fn a_state_save_into_a_removed_root_does_not_recreate_it() {
    let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let paths = RuntimeStoragePaths::new(dir.path());
    let root = paths.state_file_path.parent().expect("state parent").to_path_buf();
    drop(dir);

    let error = save_state_file(&paths, &ConductorState::new_empty())
        .expect_err("a save into a removed root must fail");

    assert!(!root.exists(), "the save recreated the removed runtime root: {error}");
}

/// A removed root reads as gone, and an existing one does not.
#[test]
fn runtime_root_is_gone_tracks_the_state_directory() {
    let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let paths = RuntimeStoragePaths::new(dir.path());
    std::fs::create_dir_all(paths.state_file_path.parent().expect("state parent"))
        .expect("create root");
    assert!(!runtime_root_is_gone(&paths), "an existing root is not gone");

    drop(dir);
    assert!(runtime_root_is_gone(&paths), "a removed root must read as gone");
}

/// The loop stops when its root is removed instead of recreating it.
///
/// Mirrors `the_background_loop_runs_a_pass_and_stops_on_shutdown`, which
/// proves the loop runs a pass on its interval. This one proves what it does
/// when the directory its state file lives in is gone: it stops, and the
/// directory stays gone.
#[tokio::test]
async fn the_background_loop_stops_when_its_root_is_removed() {
    let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let paths = RuntimeStoragePaths::new(dir.path());
    let state = ConductorState::new_empty();
    save_state_file(&paths, &state).expect("seed state file");
    let root = paths.state_file_path.parent().expect("state parent").to_path_buf();

    let mut conductor = Conductor::new(paths, mediapm_cas::new_in_memory_cas());
    conductor.background_gc_interval_secs = 1;
    conductor.replace_resolved_state(state).expect("replace state");
    conductor.start_background_maintenance();
    tokio::time::sleep(Duration::from_secs(2)).await;

    drop(dir);
    tokio::time::sleep(Duration::from_secs(2)).await;
    conductor.shutdown().await.expect("shutdown");

    assert!(!root.exists(), "a removed runtime root must not be recreated by the loop");
}

/// A foreground entry point creates the root it is about to write into.
#[test]
fn ensure_runtime_root_creates_the_state_directory() {
    let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let paths = RuntimeStoragePaths::new(dir.path().join("nested").join("runtime"));

    ensure_runtime_root(&paths).expect("create root");

    assert!(paths.state_file_path.parent().expect("state parent").is_dir());
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `RUSTC_WRAPPER= cargo test -p mediapm-conductor --all-features --lib root_ownership`
Expected: FAIL — `runtime_root_is_gone` and `ensure_runtime_root` do not exist; the first test fails on `!root.exists()` because the save recreates it.

- [ ] **Step 3: Implement**

Add beside `write_atomically`:

```rust
/// Creates the runtime root the state file lives in.
///
/// A root belongs to the actor that owns it, so only a foreground entry
/// point — an operation the caller asked for — creates one. The write path
/// deliberately does not: a save that could create its own parent would
/// resurrect a directory its owner had already removed, which is how a
/// background pass used to leave an orphan holding one state file.
fn ensure_runtime_root(paths: &RuntimeStoragePaths) -> Result<(), ConductorError> {
    let parent = state_file_parent(paths)?;
    std::fs::create_dir_all(&parent).map_err(|source| ConductorError::Io {
        operation: "creating the conductor runtime root".to_string(),
        path: parent,
        source,
    })
}

/// The directory the state file lives in.
fn state_file_parent(paths: &RuntimeStoragePaths) -> Result<PathBuf, ConductorError> {
    paths.state_file_path.parent().map(Path::to_path_buf).ok_or_else(|| {
        ConductorError::Workflow(format!(
            "the state file path '{}' has no parent directory",
            paths.state_file_path.display()
        ))
    })
}

/// Whether the runtime root the maintenance loop maintains is gone.
///
/// A gone root means the owner is gone and the store went with it, so the
/// loop stops rather than warning on every interval for the rest of the
/// process's life.
fn runtime_root_is_gone(paths: &RuntimeStoragePaths) -> bool {
    !state_file_parent(paths).is_ok_and(|parent| parent.exists())
}
```

In `start_background_maintenance`'s loop, after the cancellation check:

```rust
                if runtime_root_is_gone(&paths) {
                    break;
                }
```

In `ensure_actor_client`, before the client is initialized:

```rust
        ensure_runtime_root(&self.storage_paths)?;
```

In `run_gc`, at the top of the body:

```rust
        ensure_runtime_root(&self.storage_paths)?;
```

In `write_atomically`, delete the `create_dir_all(parent)` block and the `parent` binding it needed; keep the temp-file-plus-rename and the best-effort parent fsync.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `RUSTC_WRAPPER= cargo test -p mediapm-conductor --all-features --lib`
Expected: PASS, including the pre-existing partial-write test.

- [ ] **Step 5: Verify the leak is gone**

Run: `RUSTC_WRAPPER= cargo test-pkg mediapm-conductor` three times, checking `ls -d "$TMPDIR"/mediapm-artifact-*` after each.
Expected: no leftover directory after any run. Before this task the same loop leaked one in roughly one run in six.

- [ ] **Step 6: Commit**

```bash
git add src/mediapm-conductor/src/conductor_impl.rs src/mediapm-conductor/tests/int/root_ownership.rs src/mediapm-conductor/tests/int/mod.rs
git commit -m "fix(conductor): create a runtime root only where its owner acts"
```

---

### Task 2: A local source stages the bytes it names

**Files:**

- Modify: `src/mediapm/src/service.rs` (`add_local_source_with_position` at 503, `add_local_source` at 490)
- Modify: `src/mediapm/src/main.rs:318`
- Create: `src/mediapm/tests/int/local_source_staging.rs`
- Modify: `src/mediapm/tests/int/mod.rs`

**Interfaces:**

- Consumes: `Conductor::cas() -> &Arc<C>` and `CasApi::put(&self, data: Bytes) -> Result<Hash, CasError>`.
- Produces: `pub async fn add_local_source_with_position(...)` — the same parameters and return type as today, now `async`.
- [ ] **Step 1: Write the failing test**

```rust
/// A local source's hash names bytes that are in the store.
///
/// `media add --local` writes an import step whose `hash` option names the
/// file's content. Naming a hash is a claim that the bytes are in the store,
/// so the same call must stage them: a sync immediately afterwards resolves
/// the import step instead of failing on a missing object.
#[tokio::test]
async fn a_local_source_is_staged_before_its_hash_is_written() {
    let root = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let cache_root = mediapm_utils::temp::cache_dir().expect("cache dir");
    let storage = MediaRuntimeStorage {
        cache_root_override: Some(cache_root.path().to_path_buf()),
        ..MediaRuntimeStorage::default()
    };
    let service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(root.path(), storage)
            .await
            .expect("service");

    let payload = root.path().join("local.mkv");
    std::fs::write(&payload, b"local source bytes").expect("write payload");
    let hash = service
        .add_local_source_with_position(&payload, "media", None, None)
        .await
        .expect("add local source");

    let stored = service.conductor().cas().get(hash.clone()).await.expect("staged bytes");

    assert_eq!(stored.as_ref(), b"local source bytes");
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `RUSTC_WRAPPER= cargo test -p mediapm --all-features --test mod local_source_staging`
Expected: FAIL to compile, because `add_local_source_with_position` is not `async`; once the signature is adjusted, FAIL with a CAS object-not-found error from `get`.

- [ ] **Step 3: Implement**

In `service.rs`:

```rust
    /// Adds a local file as a media source and stages its bytes.
    ///
    /// The import step names the file's content hash, so the bytes go into
    /// the store in the same call: a name whose bytes are absent is a
    /// source that fails at its first sync. The hash comes from the store's
    /// own `put`, so the name and the bytes cannot diverge.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the file cannot be read, a CAS error when
    /// the bytes cannot be stored, and a workflow error when the source
    /// specification is invalid.
    pub async fn add_local_source_with_position(
        &self,
        path: &Path,
        media_id: &str,
        position: Option<usize>,
        recording_mbid: Option<&str>,
    ) -> Result<Hash, MediaPmError> {
        let bytes = std::fs::read(path).map_err(|source| MediaPmError::Io {
            operation: "reading a local media source".to_string(),
            path: path.to_path_buf(),
            source,
        })?;
        let hash = self.conductor.cas().put(bytes.into()).await?;
        let hash_text = hash.to_string();
        // …unchanged body, using `hash_text` where it used the computed hash…
        Ok(hash)
    }
```

Delete the `Hash::from_content` call and the now-unused import. Update `add_local_source` to `await` it. In `main.rs`, add `.await` at the call site.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `RUSTC_WRAPPER= cargo test-pkg mediapm --test mod local_source_staging`
Expected: PASS. Then `RUSTC_WRAPPER= cargo build-pkg mediapm` to confirm the CLI caller compiles.

- [ ] **Step 5: Commit**

```bash
git add src/mediapm/src/service.rs src/mediapm/src/main.rs src/mediapm/tests/int/local_source_staging.rs src/mediapm/tests/int/mod.rs
git commit -m "fix(mediapm): stage a local source's bytes when its hash is written"
```

---

### Task 3: A declared hash is consumable as a step input

**Files:**

- Modify: `src/mediapm-conductor/src/orchestration/step_worker/template.rs` (`resolve_external_data` at 974)
- Create: `src/mediapm-conductor/tests/int/step_input_external_data.rs`
- Modify: `src/mediapm-conductor/tests/int/mod.rs`

**Interfaces:**

- Consumes: `TemplateContext { cas: Option<&C>, .. }`.
- Produces: no new symbol. `resolve_external_data` returns `ResolvedValue::String(hash)` when the context carries no store handle, and the existing error when it carries one and the object is missing.
- [ ] **Step 1: Write the failing tests**

```rust
/// A step input holding an external-data reference resolves to its hash.
///
/// Step inputs resolve without a store handle, so the reference cannot be
/// read there. Returning the hash is what the sibling step-output resolver
/// already does, and the command surface — the one with a handle — turns it
/// into bytes.
#[tokio::test]
async fn an_external_data_input_resolves_to_its_hash() {
    let hash = Hash::from_content(b"declared payload");
    let step_outputs = BTreeMap::new();
    let env_vars = BTreeMap::new();
    let tokens = BTreeMap::new();
    let inputs = BTreeMap::new();
    let ctx =
        full_ctx_with_inputs(None, &step_outputs, &env_vars, &tokens, None, &inputs, "linux");

    let resolved = resolve_template(&format!("${{external_data.{hash}}}"), &ctx)
        .await
        .expect("a declared hash must resolve without a store handle");

    assert_eq!(resolved.into_string().expect("a hash is a string"), hash.to_string());
}

/// An existence test on an external-data input sees the hash.
///
/// With no store handle the reference is present, so `exists` is true. This
/// is a deliberate consequence of resolving to the hash, not an accident:
/// a conditional that gates a step on a declared payload is asking whether
/// the document names it.
#[tokio::test]
async fn an_external_data_exists_test_is_true_without_a_store_handle() {
    let hash = Hash::from_content(b"declared payload");
    let step_outputs = BTreeMap::new();
    let env_vars = BTreeMap::new();
    let tokens = BTreeMap::new();
    let inputs = BTreeMap::new();
    let ctx =
        full_ctx_with_inputs(None, &step_outputs, &env_vars, &tokens, None, &inputs, "linux");

    let resolved = resolve_template(&format!("${{external_data.{hash} ? yes | no}}"), &ctx)
        .await
        .expect("the existence test must resolve");

    assert_eq!(resolved.into_string().expect("a branch is a string"), "yes");
}
```

`full_ctx_with_inputs` is the existing test helper in `template.rs`'s test
module; both tests live beside the step-output tests it already serves.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `RUSTC_WRAPPER= cargo test -p mediapm-conductor --all-features --test mod step_input_external_data`
Expected: FAIL — both resolve to the error `external_data reference '…' requires a CAS handle but none was provided`.

- [ ] **Step 3: Implement**

In `resolve_external_data`, before the missing-handle error:

```rust
    // A step input resolves without a store handle, so the reference is
    // handed on as the hash it names — the same contract the step-output
    // resolver honours. The command surface has a handle and fetches the
    // bytes from it.
    let Some(cas) = context.cas else {
        return Ok(ResolvedValue::String(hash.to_string()));
    };
```

Keep the existing object-missing error for the `Some(cas)` path, and delete the now-dead missing-handle error.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `RUSTC_WRAPPER= cargo test-pkg mediapm-conductor`
Expected: PASS, including the existing step-worker suite.

- [ ] **Step 5: Prove the test is load-bearing**

Restore the missing-handle error temporarily; confirm exactly the two new tests fail; revert and confirm the file is byte-identical (`git diff --stat` empty).

- [ ] **Step 6: Commit**

```bash
git add src/mediapm-conductor/src/orchestration/step_worker/template.rs src/mediapm-conductor/tests/int/step_input_external_data.rs src/mediapm-conductor/tests/int/mod.rs
git commit -m "fix(conductor): resolve a declared hash as a step input without a store handle"
```

---

### Task 4: MetaBrainz requests are paced and their 502 is retried

**Files:**

- Modify: `src/mediapm/src/builtins/media_tagger/musicbrainz.rs` (constants at 38, `fetch_musicbrainz_payloads_on` at 206)
- Modify: `src/mediapm/tests/int/media_tagger_hermetic.rs`

**Interfaces:**

- Consumes: the existing `bounded_agent`, `MUSICBRAINZ_MAX_RETRIES`, and `MUSICBRAINZ_FETCH_DEADLINE`.
- Produces: `fn next_pacing_slot(last: Option<Instant>, now: Instant, interval: Duration) -> (Instant, Duration)` (pure), `async fn pace_musicbrainz_request()`, `const MUSICBRAINZ_MIN_REQUEST_INTERVAL: Duration = Duration::from_secs(1)`, and `pub const MUSICBRAINZ_FETCH_ATTEMPTS: u32 = 3` — the last is made `pub` and re-exported by the `media_tagger` module so the hermetic test can import it, alongside the constants it already imports.
- [ ] **Step 1: Write the failing tests**

Pure pacer, in `musicbrainz.rs`'s test module:

```rust
/// A request waits out the remainder of the previous request's slot.
#[test]
fn the_pacer_spaces_requests_by_the_interval() {
    let interval = Duration::from_secs(1);
    let start = Instant::now();
    let (first_slot, first_wait) = next_pacing_slot(None, start, interval);
    assert_eq!(first_wait, Duration::ZERO);
    let (second_slot, second_wait) = next_pacing_slot(Some(first_slot), start, interval);
    assert_eq!(second_wait, interval);
    let (_, third_wait) = next_pacing_slot(Some(second_slot), start + interval * 3, interval);
    assert_eq!(third_wait, Duration::ZERO);
}
```

Hermetic HTTP, in `media_tagger_hermetic.rs`:

```rust
/// A 502 is retried, not surfaced.
///
/// The proxy and the upstream both drop connections under load, and the
/// failure arrives as a 502 *response*, which `api_bindium`'s retry loop
/// never sees because it retries only 503 and transport errors. One 502
/// followed by a good response must therefore succeed. Wiremock matches
/// mocks in reverse mounting order, so the expiring 502 mock is mounted
/// after `mount_musicbrainz` and takes the first request only.
#[tokio::test]
async fn a_musicbrainz_502_is_retried() {
    let workspace = mediapm_utils::temp::artifact_dir().expect("workspace");
    let musicbrainz = wiremock::MockServer::start().await;
    mount_musicbrainz(&musicbrainz).await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path_regex("^/ws/2/"))
        .respond_with(wiremock::ResponseTemplate::new(502))
        .up_to_n_times(1)
        .mount(&musicbrainz)
        .await;

    let endpoint = format!("{}/ws/2", musicbrainz.uri());
    let options = hermetic_options(workspace.path(), &endpoint, DEFAULT_CAA_ENDPOINT);

    let result = run_internal_media_tagger(options).await;

    assert!(result.is_ok(), "one dropped connection must not fail the step: {result:?}");
}

/// A persistent 502 still fails, and is attempted the declared number of
/// times.
///
/// Retrying is bounded: the step surfaces the failure instead of looping,
/// and the attempt count is what bounds it — one request per attempt,
/// because the dependency's own loop does not retry a 502.
#[tokio::test]
async fn a_persistent_musicbrainz_502_is_surfaced() {
    let workspace = mediapm_utils::temp::artifact_dir().expect("workspace");
    let musicbrainz = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path_regex("^/ws/2/"))
        .respond_with(wiremock::ResponseTemplate::new(502))
        .mount(&musicbrainz)
        .await;

    let endpoint = format!("{}/ws/2", musicbrainz.uri());
    let options = hermetic_options(workspace.path(), &endpoint, DEFAULT_CAA_ENDPOINT);

    let error = run_internal_media_tagger(options)
        .await
        .expect_err("a persistent 502 must fail the step");

    let rendered = format!("{error:#}");
    assert!(rendered.contains("MusicBrainz"), "the error should name the fetch: {rendered}");
    let received = musicbrainz.received_requests().await.expect("request recorder is enabled");
    assert_eq!(
        received.len(),
        MUSICBRAINZ_FETCH_ATTEMPTS as usize,
        "each attempt makes one request, so the count is the bound",
    );
}
```

The 502 tests use the file's existing `mount_musicbrainz`, `hermetic_options`,
`run_internal_media_tagger`, and `DEFAULT_CAA_ENDPOINT`, exactly as
`a_musicbrainz_server_error_fails_the_step_and_names_the_endpoint` does, and add
`MUSICBRAINZ_FETCH_ATTEMPTS` to that file's `use mediapm::builtins::media_tagger::{…}`
list.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `RUSTC_WRAPPER= cargo test -p mediapm --all-features --lib the_pacer_spaces_requests_by_the_interval`
Expected: FAIL to compile — `next_pacing_slot` does not exist.
Run: `RUSTC_WRAPPER= cargo test-pkg mediapm --test mod a_musicbrainz_502_is_retried`
Expected: FAIL — the 502 surfaces as a parse error on the first attempt.

- [ ] **Step 3: Implement the pacer**

```rust
/// The minimum interval between two `MusicBrainz` requests.
///
/// The web service is rate limited rather than burst limited, and its
/// limiter answers 503. Spacing requests is cheaper than retrying them.
const MUSICBRAINZ_MIN_REQUEST_INTERVAL: Duration = Duration::from_secs(1);

/// The next free request slot and the wait until it.
///
/// Pure so the spacing rule is testable without a clock: `last` is the slot
/// the previous request took, `now` the current instant, and the returned
/// instant is the slot this request takes.
fn next_pacing_slot(last: Option<Instant>, now: Instant, interval: Duration) -> (Instant, Duration) {
    match last {
        Some(last) => {
            let slot = last + interval;
            (slot.max(now), slot.saturating_duration_since(now))
        }
        None => (now, Duration::ZERO),
    }
}

/// Waits until this process may send another `MusicBrainz` request.
///
/// The slot is reserved before the wait, so concurrent callers queue rather
/// than race: each takes the next slot and sleeps until it.
async fn pace_musicbrainz_request() {
    static LAST_SLOT: OnceLock<parking_lot::Mutex<Option<Instant>>> = OnceLock::new();
    let (slot, wait) = {
        let mut last = LAST_SLOT.get_or_init(|| parking_lot::Mutex::new(None)).lock();
        let (slot, wait) = next_pacing_slot(*last, Instant::now(), MUSICBRAINZ_MIN_REQUEST_INTERVAL);
        *last = Some(slot);
        (slot, wait)
    };
    let _ = slot;
    if !wait.is_zero() {
        tokio::time::sleep(wait).await;
    }
}
```

Call `pace_musicbrainz_request().await` once at the top of `fetch_musicbrainz_payloads_on`, before the deadline wrapper, so the wait is not charged to the fetch budget.

- [ ] **Step 4: Implement the retry**

Add the attempts constant beside `MUSICBRAINZ_MAX_RETRIES`:

```rust
/// How many times one recording+release pair is attempted.
///
/// A retry is what makes a dropped upstream connection survivable: the
/// failure arrives as a response, so the dependency's own retry loop never
/// sees it. The fetch deadline still cuts the whole sequence.
const MUSICBRAINZ_FETCH_ATTEMPTS: u32 = 3;

/// The base delay between two attempts of the pair fetch.
const MUSICBRAINZ_FETCH_RETRY_BASE_DELAY: Duration = Duration::from_millis(250);
```

Wrap the existing `tokio::time::timeout(deadline, async { … })` in an attempt loop that keeps the deadline as the total budget:

```rust
    let fetched_payload = tokio::time::timeout(deadline, async {
        let mut attempt = 0;
        loop {
            match fetch_pair_once(client, shape, recording_mbid, release_mbid).await {
                Ok(payload) => break Ok(payload),
                Err(error) => {
                    attempt += 1;
                    if attempt >= MUSICBRAINZ_FETCH_ATTEMPTS {
                        break Err(error);
                    }
                    tracing::warn!(attempt, "retrying a failed MusicBrainz fetch: {error}");
                    tokio::time::sleep(MUSICBRAINZ_FETCH_RETRY_BASE_DELAY * attempt).await;
                }
            }
        }
    })
    .await
    .map_err(|_| /* the existing deadline error, unchanged */)?;
```

Move the current body — the recording fetch, the release fetch, the payload assembly — into `async fn fetch_pair_once(...) -> anyhow::Result<MusicBrainzPayload>` unchanged.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `RUSTC_WRAPPER= cargo test-pkg mediapm --test mod media_tagger_hermetic` and `RUSTC_WRAPPER= cargo test -p mediapm --all-features --lib musicbrainz`
Expected: PASS, including the existing 503-storm and `Retry-After` tests and the retry-cap test.

- [ ] **Step 6: Commit**

```bash
git add src/mediapm/src/builtins/media_tagger/musicbrainz.rs src/mediapm/tests/int/media_tagger_hermetic.rs
git commit -m "fix(mediapm): pace MusicBrainz requests and retry a dropped connection"
```

---

### Task 5: The conductor test fixture quiesces

**Files:**

- Modify: `src/mediapm-conductor/tests/mod.rs` (`TestConductor` at 129)
- Modify: the workflow-driving test files under `src/mediapm-conductor/tests/int/` and `src/mediapm-conductor/tests/e2e/`

**Interfaces:**

- Consumes: `Conductor::shutdown()`.
- Produces: `TestConductor::shutdown(self)` (consuming, so a fixture cannot be used after it).
- [ ] **Step 1: Add the shutdown**

```rust
    /// Stops the conductor and waits for its background pass to finish.
    ///
    /// A fixture owns the directory its conductor writes into, so it must
    /// quiesce that conductor before the directory goes: a dropped guard
    /// only requests the abort, and a pass that was already writing would
    /// otherwise land after the directory is removed.
    async fn shutdown(self) {
        self.conductor.shutdown().await.expect("conductor shutdown");
    }
```

- [ ] **Step 2: Await it in every workflow-driving test**

Run: `rg -n "TestConductor::" src/mediapm-conductor/tests` to list them. In each test that calls `run_workflow`, end the body with `fixture.shutdown().await;` after the assertions, adjusting the binding so it is not borrowed at that point.

- [ ] **Step 3: Run the tests**

Run: `RUSTC_WRAPPER= cargo test-pkg mediapm-conductor`
Expected: PASS with no leftover `mediapm-artifact-*` directories and no `conductor background maintenance failed` warning in the output.

- [ ] **Step 4: Commit**

```bash
git add src/mediapm-conductor/tests
git commit -m "test(conductor): quiesce the conductor fixture before its directory goes"
```

---

### Task 6: Record the contracts

**Files:**

- Modify: `.agents/instructions/background-maintenance.instructions.md`
- Modify: `.agents/instructions/cache-and-http.instructions.md`
- Modify: `src/mediapm-conductor/AGENTS.md`
- Modify: `.agents/coverage-matrix.md`

**Interfaces:** none.

- [ ] **Step 1: Record the root-ownership rule**

In `background-maintenance.instructions.md`, add to the lifecycle section: a runtime root is created by the foreground entry point that acts for its owner (`ensure_actor_client`, `run_gc`) and never by the write path; a gone root stops the loop rather than being recreated.

- [ ] **Step 2: Record the pacing and retry contract**

In `cache-and-http.instructions.md`, extend the shared-client table's `MusicBrainz` row: the client paces requests by `MUSICBRAINZ_MIN_REQUEST_INTERVAL` (1s), retries a whole recording+release pair up to `MUSICBRAINZ_FETCH_ATTEMPTS` (3) with backoff, and treats a 502 response as retryable because the dependency retries only 503 and transport errors.

- [ ] **Step 3: Record the state-write contract**

In `src/mediapm-conductor/AGENTS.md`, state that `save_state_file` requires its directory to exist and fails otherwise, and that `ensure_runtime_root` is the only creator.

- [ ] **Step 4: Update the coverage matrix**

Add rows for: the state write not recreating a removed root; the loop stopping on a gone root; a local source's bytes being staged; an external-data step input resolving to its hash; the `exists` change; the pacer; the 502 retry; the fixture shutdown. Mark each `[covered]` against its test name.

- [ ] **Step 5: Run the docs gates**

Run: `RUSTC_WRAPPER= cargo bin rumdl check --config .rumdl.toml .agents/instructions/background-maintenance.instructions.md .agents/instructions/cache-and-http.instructions.md src/mediapm-conductor/AGENTS.md .agents/coverage-matrix.md`
Expected: no issues.

- [ ] **Step 6: Commit**

```bash
git add .agents/instructions/background-maintenance.instructions.md .agents/instructions/cache-and-http.instructions.md src/mediapm-conductor/AGENTS.md .agents/coverage-matrix.md
git commit -m "docs(coverage): record root ownership, pacing, and staging contracts"
```

---

### Task 7: Full validation

**Files:** none.

- [ ] **Step 1: Run the workspace gates**

```bash
RUSTC_WRAPPER= cargo fmt-check
RUSTC_WRAPPER= cargo clippy-all
RUSTC_WRAPPER= cargo test-all
RUSTC_WRAPPER= cargo test-doc-all
```

Expected: all rc 0, `test-all` reports no leftover temp directories.

- [ ] **Step 2: Run the offline demo**

```bash
RUSTC_WRAPPER= cargo run --package mediapm --example mediapm_demo
```

Expected: rc 0, `sync_executed: true`, 1 executed instance, 2 materialized paths, 0 warnings.

## Self-Review

- **Spec coverage:** §1 root ownership → Task 1; §2 strict write and the loop stop → Task 1; §3 local staging → Task 2; §4 consumable hash → Task 3; §5 pacing and 502 → Task 4; §6 fixture → Task 5; contracts and coverage → Task 6; gates and demo → Task 7. The spec's "Out of scope" items map to no task, deliberately.
- **Placeholders:** none. Every test body is written out; the helpers each one calls (`full_ctx_with_inputs`, `mount_musicbrainz`, `hermetic_options`, `run_internal_media_tagger`, `state_with_step_instance`, `ConductorState::new_empty`) already exist in the files named. No step says "add error handling" or "similar to Task N".
- **Type consistency:** `ensure_runtime_root`/`runtime_root_is_gone`/`state_file_parent` are introduced once and used by name; `next_pacing_slot`'s tuple shape matches its test; `MUSICBRAINZ_FETCH_ATTEMPTS` is a `u32` compared against a `u32` counter; `add_local_source_with_position` returns `Result<Hash, MediaPmError>` before and after.

---

### Task 8: A declared-hash variant is consumable end to end

Added after Task 3 landed: the spec's Testing section requires that "an ffmpeg step whose input variant is declared only in `variant_hashes` runs", and Task 3 pinned only the resolver. This task pins the whole path.

**Files:**

- Create: `src/mediapm-conductor/tests/int/step_input_external_data.rs`
- Modify: `src/mediapm-conductor/tests/int/mod.rs`

**Interfaces:**

- Consumes: the document schema's `external_data` map, a step's `inputs` map, and the input `:file(...)` materialization selector.
- Produces: no new symbol.
- [ ] **Step 1: Write the failing test**

Build a Nickel document with one workflow whose step has `inputs = { in0 = "${external_data.<hash>}" }`, a command that reads the materialized input (`sh -c 'cat <materialized path>'`, as `step_rpc_layering.rs` already runs `sh -c`), and a capture of that stdout. Stage the bytes with `cas.put` first and declare the same hash in `external_data`, as `external_data_and_validation.rs` does. Run the workflow and assert the captured stdout equals the staged bytes.

- [ ] **Step 2: Run the test to verify it fails**

Run: `RUSTC_WRAPPER= cargo test-pkg mediapm-conductor --test mod step_input_external_data`
Expected: FAIL against the pre-Task-3 resolver with the missing-CAS-handle error. If it passes without Task 3, the test is not exercising the path — say so rather than keeping it.

- [ ] **Step 3: Confirm the command surface materializes the hash**

The assertion is the whole test: the input resolves to the hash, and the command's `:file(...)` selector turns it into bytes. No implementation change is expected.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `RUSTC_WRAPPER= cargo test-pkg mediapm-conductor`
Expected: PASS, with the count one higher than 628.

- [ ] **Step 5: Commit**

```bash
git add src/mediapm-conductor/tests/int/step_input_external_data.rs src/mediapm-conductor/tests/int/mod.rs
git commit -m "test(conductor): run a step whose input is a declared hash"
```
