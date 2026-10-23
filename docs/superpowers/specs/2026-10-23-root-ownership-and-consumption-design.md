# Runtime root ownership and payload consumption

Design for four defects the retention series' reviews surfaced, plus the test fixture that shares their cause. Approved in session on 2026-10-23.

## Problem

A runtime root is a directory whose owner is the thing that created it, but three code paths disagree about who that is, and one of them resurrects a root its owner has already removed.

1. **A state write recreates a directory its owner removed.** `write_atomically` calls `create_dir_all(parent)` before writing, so a conductor background pass still running when a test removes its artifact directory writes `state.json` back into the removed tree. The conductor's own `TestConductor` fixture has the same shape: it owns an `artifact_dir()`, declares `dir` before `conductor` (so the directory goes first), and no test under `src/mediapm-conductor/tests/` awaits a shutdown.
2. **`media add --local` records a hash whose bytes are never stored.** `add_local_source_with_position` reads the file, computes `Hash::from_content`, and builds an import step naming that hash — without putting the bytes into the store. A sync immediately afterwards fails on a missing hash.
3. **A variant declared only as a hash cannot be consumed by a step.** `VariantProducer::ExternalData::to_binding` renders the variant as the template string `${external_data.<hash>}` and puts it in the step's **inputs** map; step inputs resolve with `cas: None`, and `resolve_external_data` hard-errors without a CAS handle. The bytes are declared, so retention keeps them; nothing can read them.
4. **MetaBrainz requests are unpaced, and their 502 is invisible to the retry loop.** `musicbrainz_rs` builds on `api_bindium`, whose `governor` rate limiter sits behind a `rate_limit` feature that `musicbrainz_rs` does not expose, so requests go out as fast as the workflow produces them; MusicBrainz's limiter answers `503`, and the proxy or upstream drops connections under load, which surfaces as a `502` **response** that `api_bindium`'s retry loop never sees because it retries only `503` and transport errors.

## Principle

**A root is created by its owner and by nobody else.** Foreground entry points create the runtime root because they act on behalf of the owner; the write path never does, so a write into a removed root fails loudly instead of resurrecting it.

**A payload is stored by whoever names it.** Naming a hash is a claim that the bytes are in the store, so the code that writes the name stages the bytes in the same step.

**Inputs carry references; commands resolve them.** A step input holds a hash string (its documented contract), and the command surface — the only surface with a store handle — turns it into bytes and materializes it.

**Pacing is cheaper than retrying.** A request that respects the server's published rate does not meet its limiter, so pacing is the primary remedy and retry is for the failures that pacing cannot prevent.

## Design

### 1. Who creates a runtime root

`ensure_runtime_root` creates the state file's parent directory, and runs at the foreground entry points: `ensure_actor_client` — the path `run_workflow` takes — and `run_gc`. It is the only directory creation in the conductor's state path.

### 2. The state write stops creating it

`write_atomically` drops its `create_dir_all`. A save into a gone root fails with the filesystem's own `NotFound`, mapped to the existing I/O error with an operation label naming the state file.

The background loop checks the state file's parent at the top of each iteration and **breaks** when it is gone: a removed root means the owner is gone and the store went with it, so there is nothing to maintain. This mirrors `Cache`'s root-exists guard rather than adding a public error variant or matching an error string. A failure for any other reason still warns and retries, because a disk error may heal.

### 3. A local source stages its own bytes

`add_local_source_with_position` becomes `async` and calls `self.conductor.cas().put(bytes.into()).await`, taking the hash from `put`'s return value instead of recomputing it with `Hash::from_content`. One hash derivation, one store write, no divergence between the name and the bytes.

### 4. A declared hash is consumable as a step input

With no CAS handle, `resolve_external_data` returns the hash string, exactly as `resolve_step_output` does. The step's input then holds the hash — which `ResolvedInput.value` already documents — and the command surface, which has a handle, fetches the bytes and materializes them through the input's `file(...)` selector.

The deliberate behavior change: an `exists` test on an external-data reference inside a step input now evaluates true (the hash string is present) instead of erroring. A test pins it.

### 5. MetaBrainz requests are paced, and their 502 is retried

A process-wide pacer in `musicbrainz.rs` spaces MusicBrainz requests by a named interval of one second, so a burst cannot trip the limiter. It is hand-rolled rather than enabling `api_bindium`'s `rate_limit` feature, which would require a direct dependency on a transitive crate purely to flip a flag.

A retry wraps the lookups and treats a failed fetch as retryable with backoff — the 502 arrives as a response, so nothing else retries it. It sits outside `api_bindium`'s own loop rather than replacing it: that loop keeps its cap and its `Retry-After` sleep, and the fetch deadline cuts both.

### 6. The conductor fixture quiesces

`TestConductor` declares `conductor` before `dir` and gains `async fn shutdown()`. The tests that drive a workflow await it before they end. Drop order alone cannot fix this: `BackgroundMaintenanceGuard::cancel()` aborts without awaiting, and `Drop` cannot await.

## Testing

- A state save into a removed root does not recreate it, and the background loop stops instead of warning on every interval.
- `media add --local` followed by a sync resolves the import step's hash.
- An ffmpeg step whose input variant is declared only in `variant_hashes` runs; a hermetic test asserts the resolved input holds the hash and the command materializes it.
- The `exists` change above is pinned by its own test.
- A 502-once-then-200 wiremock server yields a successful lookup; two MusicBrainz requests are spaced by the interval; the existing 503-storm and `Retry-After` tests still hold.
- The fixture fix is verified the way the examples were: repeat `cargo test-pkg mediapm-conductor` and assert no leftover temp directories.

## Out of scope

- `mediapm/tests/` has the same never-shut-down shape with a different leftover signature (`<root>/.mediapm/`, not a root `state.json`). It is recorded as an open risk rather than changed here.
- The live online demo's tag expectations against upstream's own metadata mapping; that run is the only place they are exercised, and it is not hermetic.
