//! Contract-focused integration scenarios for the conductor.

/// External-data invariant and decode validation.
mod external_data_and_validation;

/// Bootstrap, validation, and state-shape focused checks.
mod bootstrap;

/// Instance GC with configurable TTL.
mod gc;

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

/// Client-defined bar-label truncation: worker bars drop progress tally,
/// step bars yield the version before the tool name.
#[cfg(feature = "progress")]
mod progress_labels;
