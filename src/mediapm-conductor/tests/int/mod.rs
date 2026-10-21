//! Contract-focused integration scenarios for the conductor.

/// External-data invariant and decode validation.
mod external_data_and_validation;

/// Bootstrap, validation, and state-shape focused checks.
mod bootstrap;

/// Per-step RPC layering — no transport timeout on `ExecuteStep` dispatch
/// (unix-only: the test step is a real `sh` process).
#[cfg(unix)]
mod step_rpc_layering;

/// Instance GC with configurable TTL.
mod gc;

/// Cache-probe validity: a hit whose required output blob is gone from the
/// store re-executes instead of reusing the instance that names it.
mod cache_probe;

/// Name-addressed capture union: one instance serves steps that declare
/// different names, a changed capture spec replaces only its entry, and two
/// steps disagreeing about one name's capture fail the run.
mod capture_union;

/// Decode + migration pipeline (regression: record field shorthand).
mod decode_migration;

/// Nickel schema sync-prevention tests — validates v2.ncl stays in sync
/// with `NickelDocument` Rust types.
mod schema_sync;

/// Schema strictness guard — re-asserts v2.ncl strictness properties
/// (closed envelope, non-empty strings, integer guards, closed platform keys).
mod schema_strictness;

/// Platform filtering: cfg-derived FOREIGN_PLATFORM_DIRS correctness and
/// the explicit link_to_sandbox_filtered API.
mod platform_filtering;

/// Provider pipeline: resolve → fetch → process for tool provisioning.
mod provider_pipeline;

/// Provision cache and download cache domain separation.
mod provision_domain_separation;

/// Runtime tmp dir lifecycle: sandbox roots removed on normal and failed
/// workflow exits.
mod runtime_tmp_lifecycle;

/// Guard test for the `src/http/` decoupling invariant that `build.rs`
/// otherwise only reports as a build abort.
mod http_decoupling;

/// Workflow progress screen: an overall bar the caller creates, one
/// pre-created worker-slot bar per pool member that is relabelled with
/// `SetTruncation` on every dispatch, plus `Advance` and
/// `FinishSuccess`/`FinishWarning`.
mod workflow_progress;

/// Ready-queue scheduler: dependency-counter dispatch over a free worker
/// pool, with no level barrier. Ordering is observed through the test-only
/// `slow@v1` builtin, so the module shares the progress-feature gate that
/// compiles that builtin in for integration tests.
#[cfg(feature = "progress")]
mod workflow_scheduler;

/// Client-defined bar-label truncation: worker bars drop progress tally,
/// step bars yield the version before the tool name.
#[cfg(feature = "progress")]
mod progress_labels;
