//! Structured workflow-profiler report types and JSON artifact persistence.
//!
//! The profiler artifact is intentionally file-based so higher layers (for
//! example `mediapm` demos) can enable deep timing diagnostics without
//! depending on in-process tracing subscribers.

use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::api::{RunSummary, RuntimeDiagnostics};
use crate::error::ConductorError;

/// Wire-format version for serialized workflow profiler reports.
pub(super) const WORKFLOW_RUN_PROFILE_VERSION: u32 = 1;

/// End-to-end workflow run profile captured from one conductor invocation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(super) struct WorkflowRunProfile {
    /// Profile schema version.
    pub version: u32,
    /// Monotonic run start timestamp in Unix nanoseconds.
    pub run_started_unix_nanos: u128,
    /// Monotonic run finish timestamp in Unix nanoseconds.
    pub run_finished_unix_nanos: u128,
    /// Artifact generation timestamp in Unix nanoseconds.
    pub generated_unix_nanos: u128,
    /// User configuration path used for this run.
    pub user_ncl_path: String,
    /// Machine configuration path used for this run.
    pub machine_ncl_path: String,
    /// Resolved runtime storage root (`conductor_dir`) used for this run.
    pub conductor_dir_path: String,
    /// Resolved volatile runtime-state path used for this run.
    pub conductor_state_config_path: String,
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
    #[expect(
        clippy::too_many_arguments,
        reason = "profile constructor collects all workflow-run context in one call; splitting into a builder would add boilerplate without improving readability at the single call site"
    )]
    pub(super) fn new(
        run_started_unix_nanos: u128,
        run_finished_unix_nanos: u128,
        user_ncl_path: &Path,
        machine_ncl_path: &Path,
        conductor_dir_path: &Path,
        conductor_state_config_path: &Path,
        summary: RunSummary,
        step_executions: Vec<StepExecutionProfile>,
        runtime_diagnostics: RuntimeDiagnostics,
    ) -> Self {
        Self {
            version: WORKFLOW_RUN_PROFILE_VERSION,
            run_started_unix_nanos,
            run_finished_unix_nanos,
            generated_unix_nanos: now_unix_nanos(),
            user_ncl_path: display_path(user_ncl_path),
            machine_ncl_path: display_path(machine_ncl_path),
            conductor_dir_path: display_path(conductor_dir_path),
            conductor_state_config_path: display_path(conductor_state_config_path),
            summary,
            step_executions,
            runtime_diagnostics,
        }
    }
}

/// One workflow-step execution timing sample.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(super) struct StepExecutionProfile {
    /// Workflow id containing this step.
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
    /// Whether fallback local execution was used after worker RPC failure.
    pub fallback_used: bool,
    /// Observed per-step elapsed duration in milliseconds.
    pub elapsed_ms: f64,
    /// Number of output names requested from this step.
    pub requested_output_count: usize,
    /// Number of unsaved output hashes reported by this step.
    pub pending_unsaved_hashes_count: usize,
}

/// Persists one workflow run profile as pretty JSON.
///
/// # Errors
///
/// Returns [`ConductorError::Io`] for filesystem failures and
/// [`ConductorError::Serialization`] when JSON encoding fails.
pub(super) fn write_profile_json(
    output_path: &Path,
    profile: &WorkflowRunProfile,
) -> Result<(), ConductorError> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent).map_err(|source| ConductorError::Io {
            operation: "creating profiler output parent directory".to_string(),
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let bytes = serde_json::to_vec_pretty(profile).map_err(|source| {
        ConductorError::Serialization(format!(
            "failed serializing workflow profiler JSON for '{}': {source}",
            output_path.display()
        ))
    })?;

    fs::write(output_path, bytes).map_err(|source| ConductorError::Io {
        operation: "writing profiler JSON artifact".to_string(),
        path: output_path.to_path_buf(),
        source,
    })
}

/// Returns current wall-clock Unix timestamp in nanoseconds.
#[must_use]
fn now_unix_nanos() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos()
}

/// Renders one path as slash-normalized display text.
#[must_use]
fn display_path(path: &Path) -> String {
    path.display().to_string().replace('\\', "/")
}

/// Entry in a phase group: `(step_prefix, substep_name, elapsed_ms)`.
///
/// `step_prefix` is the `"N-M"` portion of `step_id`; `substep_name` is the
/// remainder after the second dash; `elapsed_ms` is the raw step duration.
type PhaseSubstep = (String, String, f64);

/// Prints a human-readable timing breakdown from a conductor profile JSON file.
///
/// Reads the file at `profile_path`, deserializes it as a [`WorkflowRunProfile`],
/// groups `step_executions` by the leading numeric phase identifier in each
/// `step_id`, and prints per-phase totals with individual substep durations to
/// stdout.
///
/// The `step_id` convention used by `mediapm` workflows is
/// `"<phase_index>-<substep_index>-<step_name>"`. Steps sharing the same
/// `phase_index` are rendered under one phase group. Step ids that do not
/// match this convention are each treated as their own phase.
///
/// Silently returns when `profile_path` does not exist, cannot be read, or
/// fails to deserialize—callers do not need to guard on whether sync ran.
// The casts below are intentional display-only truncations or precision
// trade-offs. `secs as u64` is safe: the value is non-negative (ms is always
// ≥ 0) and the maximum representable workflow duration is far below u64::MAX.
// `nanos_delta as f64` loses at most a few nanoseconds of precision on very
// long runs—acceptable for a human-readable timer.
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss, clippy::cast_sign_loss)]
pub fn print_profile_timing(profile_path: &Path) {
    let Ok(content) = fs::read_to_string(profile_path) else { return };
    let Ok(profile) = serde_json::from_str::<WorkflowRunProfile>(&content) else { return };
    if profile.step_executions.is_empty() {
        return;
    }

    // Group step_executions by phase: the leading numeric segment of step_id.
    // Entry layout: (step_prefix such as "2-0", substep_name, elapsed_ms).
    let mut phases: Vec<(String, Vec<PhaseSubstep>)> = Vec::new();
    for exec in &profile.step_executions {
        let id = &exec.step_id;
        let parts: Vec<&str> = id.splitn(3, '-').collect();
        let phase_key = match parts.first().and_then(|s| s.parse::<u64>().ok()) {
            Some(n) => n.to_string(),
            None => id.clone(),
        };
        let step_prefix = match (parts.first(), parts.get(1)) {
            (Some(a), Some(b)) => format!("{a}-{b}"),
            _ => id.clone(),
        };
        let substep_name = parts.get(2).copied().unwrap_or(id.as_str()).to_string();
        if let Some(group) = phases.iter_mut().find(|(k, _)| k == &phase_key) {
            group.1.push((step_prefix, substep_name, exec.elapsed_ms));
        } else {
            phases.push((phase_key, vec![(step_prefix, substep_name, exec.elapsed_ms)]));
        }
    }

    let steps_total_ms: f64 = phases.iter().flat_map(|(_, s)| s.iter().map(|(_, _, ms)| *ms)).sum();
    let wall_total_ms =
        profile.run_finished_unix_nanos.saturating_sub(profile.run_started_unix_nanos) as f64
            / 1_000_000.0;

    let fmt = |ms: f64| -> String {
        let secs = ms / 1000.0;
        if secs >= 60.0 {
            let mins = secs as u64 / 60;
            format!("{mins}m {:.1}s", secs % 60.0)
        } else {
            format!("{secs:.1}s")
        }
    };

    println!();
    println!("=== Conductor Timing Profile ===");
    println!("Total (steps): {}  |  wall: {}", fmt(steps_total_ms), fmt(wall_total_ms));

    for (_, substeps) in &phases {
        let phase_ms: f64 = substeps.iter().map(|(_, _, ms)| *ms).sum();

        // Derive phase label as the common byte-prefix of all substep names,
        // trimmed of trailing non-alphanumeric separator characters.
        // Step names generated by mediapm are ASCII identifiers, making
        // byte-level prefix safe and sufficient.
        let phase_label = {
            let names: Vec<&str> = substeps.iter().map(|(_, n, _)| n.as_str()).collect();
            let first = names[0];
            if names.len() == 1 {
                first.to_string()
            } else {
                let prefix_len = names.iter().skip(1).fold(first.len(), |acc, s| {
                    first.bytes().zip(s.bytes()).take_while(|(a, b)| a == b).count().min(acc)
                });
                let trimmed = first[..prefix_len].trim_end_matches(|c: char| !c.is_alphanumeric());
                if trimmed.is_empty() { first.to_string() } else { trimmed.to_string() }
            }
        };

        println!();
        if substeps.len() == 1 {
            let (prefix, name, ms) = &substeps[0];
            println!("  {prefix:<5}  {name:<42}  {}", fmt(*ms));
        } else {
            println!("  {} ({} substeps) — {}", phase_label, substeps.len(), fmt(phase_ms));
            for (prefix, name, ms) in substeps {
                println!("    {prefix:<5}  {name:<40}  {}", fmt(*ms));
            }
        }
    }
    println!();
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::api::{RuntimeDiagnostics, SchedulerDiagnostics};

    use super::{StepExecutionProfile, WorkflowRunProfile, write_profile_json};

    /// Verifies profiler reports serialize to JSON with expected core fields.
    #[test]
    fn write_profile_json_persists_step_timings() {
        let temp = tempdir().expect("tempdir");
        let output_path = temp.path().join("profile").join("run.json");
        let profile = WorkflowRunProfile::new(
            10,
            20,
            temp.path().join("conductor.ncl").as_path(),
            temp.path().join("conductor.machine.ncl").as_path(),
            temp.path().join(".conductor").as_path(),
            temp.path().join(".conductor/state.ncl").as_path(),
            crate::api::RunSummary {
                executed_instances: 1,
                cached_instances: 2,
                rematerialized_instances: 0,
            },
            vec![StepExecutionProfile {
                workflow_name: "wf".to_string(),
                workflow_display_name: "wf".to_string(),
                workflow_attempt: 0,
                level_index: 1,
                step_id: "step-a".to_string(),
                tool_name: "ffmpeg@1".to_string(),
                worker_index: 0,
                executed: true,
                rematerialized: false,
                fallback_used: false,
                elapsed_ms: 123.0,
                requested_output_count: 1,
                pending_unsaved_hashes_count: 0,
            }],
            RuntimeDiagnostics {
                worker_pool_size: 1,
                scheduler: SchedulerDiagnostics {
                    ewma_alpha: 0.35,
                    unknown_cost_ms: 10.0,
                    tool_estimates: Vec::new(),
                    rpc_fallbacks_total: 0,
                },
                workers: Vec::new(),
                recent_traces: Vec::new(),
            },
        );

        write_profile_json(&output_path, &profile).expect("profile write should succeed");

        let text = std::fs::read_to_string(&output_path).expect("read profile");
        let value: serde_json::Value = serde_json::from_str(&text).expect("valid json");

        assert_eq!(value["version"].as_u64(), Some(1));
        assert_eq!(value["step_executions"][0]["step_id"].as_str(), Some("step-a"));
        assert_eq!(value["summary"]["executed_instances"].as_u64(), Some(1));
    }
}
