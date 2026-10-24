//! Focused integration tests for mediapm contracts.

// The hermetic provisioning tests below — `all_platform`, and the `tool_sync`
// cases that answer a mock upstream — drive the provider base-URL seams, and
// those seams exist only in a `test-seams` build. Without the feature such a
// test would ignore the mock and reach the real hosts, which is the silent pass
// this refusal exists to prevent: a test that quietly downloads from the
// network is worse than one that will not build. Every gate in this repository
// passes `--all-features`.
//
// It sits here rather than inside one of those modules because `all_platform`
// and `tool_sync` are submodules of this same test binary, so one refusal
// covers both and no module needs a copy of its own.
#[cfg(not(feature = "test-seams"))]
compile_error!(
    "the hermetic provisioning tests drive the provider base-URL seams; run them with \
     `cargo test -p mediapm --all-features`"
);

mod builtins;
mod demo;
mod demo_hierarchy_golden;
mod demo_online;
/// A local media source stages the bytes its content hash names.
mod local_source_staging;
/// Hermetic media-tagger metadata-stage tests over a mocked network.
mod media_tagger_hermetic;
mod online_sync_post_sync_dump;
/// Nickel schema strictness tests (S-C1..S-C10) — validates the strict
/// closed-contract surface of `v1.ncl`/`mod.ncl` and the exported JSON schema.
mod schema_strictness;
/// Nickel schema sync-prevention tests — pins the live Nickel contract
/// sources against the `MediaPmDocument` Rust runtime model they resolve to,
/// and pins that no Rust migration dispatcher mirrors the Nickel ladder.
mod schema_sync;
// CAUTION: This is tool-sync integration (MediaPmService::sync_tools()).
// Do NOT put workflow-sync or state-sync tests here.
mod tool_sync;

/// All-platform document structure integration: verifies that managed
/// tools have per-OS content-map entries and non-empty command selectors.
mod all_platform;
/// One instance, several capture names: two steps over identical work union
/// their outputs and both variants materialize.
mod capture_union;
/// The binary and the extracted CLI definition describe the same commands.
#[cfg(feature = "cli")]
mod cli_surface;
/// The completion scripts a build script writes agree with the CLIs they describe.
#[cfg(feature = "cli")]
mod completion_contract;
/// Dual-write strategy: state.json always-writes, NCL files skip when unchanged.
mod dual_write;
/// Every CAS hash a generated step names must be declared as external data.
mod import_payload_rooting;
/// Explicit invalidation: invalidating a media step must re-execute its
/// workflow on the next sync even though the step's material is unchanged.
mod media_step_invalidation;
/// Re-sync over an unchanged library: the second run must skip no path.
mod resync;
/// Runtime root `.gitignore` creation on service construction.
mod runtime_gitignore;
/// A generated document this build cannot decode is regenerated, not fatal.
mod stale_generated_document;
/// State JSON persistence and migration tests.
mod state_persistence;
/// Double-run idempotency: an immediate second sync must execute no workflow
/// step and re-materialize no path.
mod sync_idempotency_double_run;
/// Per-phase sync observer wiring and centralized output primitives.
mod sync_observer;
/// One-terminal-per-sync progress ownership across all three sync phases.
mod sync_progress;
