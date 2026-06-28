//! Structured workflow-profiler report types and JSON artifact persistence.
//!
//! The profiler artifact is intentionally file-based so higher layers (e.g.
//! `mediapm` demos) can enable deep timing diagnostics without depending on
//! in-process tracing subscribers.

use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use super::protocol::StepPhaseTiming;

use crate::api::{RunSummary, RuntimeDiagnostics};
use crate::config::ImpureTimestamp;
use crate::error::ConductorError;

/// Wire-format version for serialized workflow profiler reports.
pub(crate) const WORKFLOW_RUN_PROFILE_VERSION: u32 = 2;

/// End-to-end workflow run profile captured from one conductor invocation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct WorkflowRunProfile {
    /// Profile schema version.
    pub version: u32,
    /// Monotonic run start timestamp in Unix nanoseconds.
    pub run_started_unix_nanos: u128,
    /// Monotonic run finish timestamp in Unix nanoseconds.
    pub run_finished_unix_nanos: u128,
    /// Artifact generation timestamp in Unix nanoseconds.
    pub generated_unix_nanos: u128,
    /// Conductor config directory path used for this run.
    pub conductor_dir_path: String,
    /// Aggregated run summary counters.
    pub summary: RunSummary,
    /// Per-step execution timings captured across all workflows and levels.
    pub step_executions: Vec<StepExecutionProfile>,
    /// Scheduler + worker diagnostics snapshot collected at run end.
    pub runtime_diagnostics: RuntimeDiagnostics,
}

impl WorkflowRunProfile {
    /// Builds one profile value from run context, step timings, and diagnostics.
    #[must_use]
    pub(crate) fn new(
        run_started_unix_nanos: u128,
        run_finished_unix_nanos: u128,
        conductor_dir_path: &Path,
        summary: RunSummary,
        step_executions: Vec<StepExecutionProfile>,
        runtime_diagnostics: RuntimeDiagnostics,
    ) -> Self {
        Self {
            version: WORKFLOW_RUN_PROFILE_VERSION,
            run_started_unix_nanos,
            run_finished_unix_nanos,
            generated_unix_nanos: ImpureTimestamp::now().as_unix_nanos(),
            conductor_dir_path: conductor_dir_path.display().to_string().replace('\\', "/"),
            summary,
            step_executions,
            runtime_diagnostics,
        }
    }
}

/// One workflow-step execution timing sample.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct StepExecutionProfile {
    /// Workflow name containing this step.
    pub workflow_name: String,
    /// Human-facing workflow label used by progress rendering.
    pub workflow_display_name: String,
    /// Retry attempt index for this workflow (`0` for first attempt).
    pub workflow_attempt: usize,
    /// Zero-based topological level index.
    pub level_index: usize,
    /// Step id.
    pub step_id: String,
    /// Immutable tool name executed for this step.
    pub tool_name: String,
    /// Worker index that completed this step.
    pub worker_index: usize,
    /// Whether this step executed instead of cache-hit reuse.
    pub executed: bool,
    /// Whether this execution was a cache rematerialization.
    pub rematerialized: bool,
    /// Observed per-step elapsed duration in milliseconds.
    pub elapsed_ms: f64,
    /// Number of output names requested from this step.
    pub requested_output_count: usize,
    /// Number of unsaved output hashes reported by this step.
    pub pending_unsaved_hashes_count: usize,
    /// Fine-grained phase timing breakdown captured by the step worker.
    pub phase_timings: StepPhaseTiming,
}

/// Persists one workflow run profile as pretty JSON.
///
/// # Errors
///
/// Returns [`ConductorError::Io`] for filesystem failures and
/// [`ConductorError::Serialization`] when JSON encoding fails.
pub(crate) fn write_profile_json(
    output_path: &Path,
    profile: &WorkflowRunProfile,
) -> Result<(), ConductorError> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent).map_err(|source| {
            ConductorError::io("creating profiler output parent directory", parent, source)
        })?;
    }

    let bytes = serde_json::to_vec_pretty(profile).map_err(|source| {
        ConductorError::Serialization(format!(
            "failed serializing workflow profiler JSON for '{}': {source}",
            output_path.display(),
        ))
    })?;

    fs::write(output_path, bytes)
        .map_err(|source| ConductorError::io("writing profiler JSON artifact", output_path, source))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies `WorkflowRunProfile::new` populates version and timestamp fields.
    #[test]
    fn workflow_run_profile_new_populates_fields() {
        let profile = WorkflowRunProfile::new(
            1000,
            2000,
            Path::new("/tmp/conductor"),
            RunSummary { total_steps: 5, executed_steps: 3, cached_steps: 2, failed_steps: 0 },
            Vec::new(),
            RuntimeDiagnostics::default(),
        );
        assert_eq!(profile.version, WORKFLOW_RUN_PROFILE_VERSION);
        assert_eq!(profile.run_started_unix_nanos, 1000);
        assert_eq!(profile.run_finished_unix_nanos, 2000);
        assert_eq!(profile.summary.total_steps, 5);
    }

    /// Verifies `write_profile_json` round-trips correctly through a temp file.
    #[test]
    fn write_profile_json_round_trips() {
        let profile = WorkflowRunProfile::new(
            100,
            200,
            Path::new("/tmp/conductor"),
            RunSummary::default(),
            Vec::new(),
            RuntimeDiagnostics::default(),
        );

        let dir = std::env::temp_dir().join("mediapm-conductor-profiler-test");
        let path = dir.join("profile.json");

        // Clean up before test.
        let _ = fs::remove_dir_all(&dir);

        write_profile_json(&path, &profile).expect("write_profile_json should succeed");

        let restored_bytes = fs::read(&path).expect("should read back profile file");
        let restored: WorkflowRunProfile =
            serde_json::from_slice(&restored_bytes).expect("should deserialize profile JSON");

        // version, timestamps, summary fields must round-trip.
        assert_eq!(restored.version, WORKFLOW_RUN_PROFILE_VERSION);
        assert_eq!(restored.summary.total_steps, 0);
        assert!(restored.generated_unix_nanos > 0);

        // Clean up after test.
        let _ = fs::remove_dir_all(&dir);
    }
}
