//! Structured workflow-profiler report types and JSON artifact persistence.
//!
//! The profiler artifact is intentionally file-based so higher layers (for
//! example `mediapm` demos) can enable deep timing diagnostics without
//! depending on in-process tracing subscribers.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::api::{RunSummary, RuntimeDiagnostics};
use crate::error::ConductorError;

/// Wire-format version for serialized workflow profiler reports.
pub(super) const WORKFLOW_RUN_PROFILE_VERSION: u32 = 2;

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

/// Fine-grained execution-phase timings for one workflow step.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[allow(clippy::struct_field_names)]
pub(super) struct StepPhaseTimingProfile {
    /// Time spent resolving step/default input bindings.
    pub resolve_inputs_ms: f64,
    /// Time spent resolving process and output specs from templates.
    pub resolve_specs_ms: f64,
    /// Time spent evaluating cache-hit/rematerialization requirements.
    pub cache_probe_ms: f64,
    /// Time spent preparing execution sandbox content before process start.
    pub materialization_ms: f64,
    /// Time spent running the tool process or builtin implementation.
    pub execution_ms: f64,
    /// Time spent capturing declared outputs into CAS.
    pub capture_outputs_ms: f64,
    /// Time spent applying persistence policies and CAS save hints.
    pub persistence_merge_ms: f64,
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
    /// Fine-grained phase timing breakdown captured by the step worker.
    pub phase_timings: StepPhaseTimingProfile,
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

/// Aggregated timing and execution counters for one tool.
#[derive(Debug, Clone, PartialEq)]
struct ToolTimingAggregate {
    /// Number of step samples recorded for this tool.
    samples: usize,
    /// Sum of observed step durations in milliseconds.
    total_ms: f64,
    /// Fastest observed step duration in milliseconds.
    min_ms: f64,
    /// Slowest observed step duration in milliseconds.
    max_ms: f64,
    /// Number of executed (non-cache-hit) steps.
    executed_samples: usize,
    /// Number of cache-hit steps.
    cached_samples: usize,
    /// Number of rematerialized executions.
    rematerialized_samples: usize,
    /// Number of fallback-path executions.
    fallback_samples: usize,
}

impl ToolTimingAggregate {
    /// Creates an empty aggregate with sentinel bounds.
    #[must_use]
    fn new() -> Self {
        Self {
            samples: 0,
            total_ms: 0.0,
            min_ms: f64::INFINITY,
            max_ms: 0.0,
            executed_samples: 0,
            cached_samples: 0,
            rematerialized_samples: 0,
            fallback_samples: 0,
        }
    }

    /// Adds one execution sample into this aggregate.
    fn update(&mut self, sample: &StepExecutionProfile) {
        self.samples = self.samples.saturating_add(1);
        self.total_ms += sample.elapsed_ms;
        self.min_ms = self.min_ms.min(sample.elapsed_ms);
        self.max_ms = self.max_ms.max(sample.elapsed_ms);
        if sample.executed {
            self.executed_samples = self.executed_samples.saturating_add(1);
        } else {
            self.cached_samples = self.cached_samples.saturating_add(1);
        }
        if sample.rematerialized {
            self.rematerialized_samples = self.rematerialized_samples.saturating_add(1);
        }
        if sample.fallback_used {
            self.fallback_samples = self.fallback_samples.saturating_add(1);
        }
    }
}

/// Formats one duration value in milliseconds into compact human text.
#[must_use]
fn format_duration_ms(ms: f64) -> String {
    let secs = ms / 1000.0;
    if secs >= 60.0 {
        let mins = (secs / 60.0).floor();
        format!("{mins:.0}m {:.1}s", secs % 60.0)
    } else {
        format!("{secs:.1}s")
    }
}

/// Renders one profile into human-readable timing lines.
#[must_use]
#[expect(
    clippy::too_many_lines,
    reason = "this function intentionally assembles one complete timing report so section ordering stays explicit and reviewable"
)]
#[expect(
    clippy::cast_precision_loss,
    reason = "profile output is display-focused and millisecond-level precision is sufficient for human diagnostics"
)]
fn render_profile_timing(profile: &WorkflowRunProfile) -> Vec<String> {
    if profile.step_executions.is_empty() {
        return Vec::new();
    }

    let wall_total_ms =
        profile.run_finished_unix_nanos.saturating_sub(profile.run_started_unix_nanos) as f64
            / 1_000_000.0;
    let step_total_ms: f64 = profile.step_executions.iter().map(|exec| exec.elapsed_ms).sum();

    let mut per_level_critical_ms = BTreeMap::<(String, usize, usize), f64>::new();
    for exec in &profile.step_executions {
        let key = (exec.workflow_display_name.clone(), exec.workflow_attempt, exec.level_index);
        per_level_critical_ms
            .entry(key)
            .and_modify(|max_elapsed| *max_elapsed = max_elapsed.max(exec.elapsed_ms))
            .or_insert(exec.elapsed_ms);
    }
    let level_critical_path_ms: f64 = per_level_critical_ms.values().copied().sum();

    let parallelism_dividend_ms = (step_total_ms - level_critical_path_ms).max(0.0);
    let orchestration_overhead_ms = (wall_total_ms - level_critical_path_ms).max(0.0);

    let mut phase_groups: Vec<(String, Vec<PhaseSubstep>)> = Vec::new();
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
        if let Some(group) = phase_groups.iter_mut().find(|(key, _)| key == &phase_key) {
            group.1.push((step_prefix, substep_name, exec.elapsed_ms));
        } else {
            phase_groups.push((phase_key, vec![(step_prefix, substep_name, exec.elapsed_ms)]));
        }
    }

    let mut tool_aggregates = BTreeMap::<String, ToolTimingAggregate>::new();
    for exec in &profile.step_executions {
        tool_aggregates
            .entry(exec.tool_name.clone())
            .or_insert_with(ToolTimingAggregate::new)
            .update(exec);
    }
    let mut tool_rows = tool_aggregates.into_iter().collect::<Vec<_>>();
    tool_rows.sort_by(|(_, left), (_, right)| {
        right.total_ms.partial_cmp(&left.total_ms).unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut slowest_steps = profile.step_executions.clone();
    slowest_steps.sort_by(|left, right| {
        right.elapsed_ms.partial_cmp(&left.elapsed_ms).unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut phase_totals = BTreeMap::<&'static str, f64>::new();
    for step in &profile.step_executions {
        phase_totals
            .entry("resolve_inputs")
            .and_modify(|ms| *ms += step.phase_timings.resolve_inputs_ms)
            .or_insert(step.phase_timings.resolve_inputs_ms);
        phase_totals
            .entry("resolve_specs")
            .and_modify(|ms| *ms += step.phase_timings.resolve_specs_ms)
            .or_insert(step.phase_timings.resolve_specs_ms);
        phase_totals
            .entry("cache_probe")
            .and_modify(|ms| *ms += step.phase_timings.cache_probe_ms)
            .or_insert(step.phase_timings.cache_probe_ms);
        phase_totals
            .entry("materialization")
            .and_modify(|ms| *ms += step.phase_timings.materialization_ms)
            .or_insert(step.phase_timings.materialization_ms);
        phase_totals
            .entry("execution")
            .and_modify(|ms| *ms += step.phase_timings.execution_ms)
            .or_insert(step.phase_timings.execution_ms);
        phase_totals
            .entry("capture_outputs")
            .and_modify(|ms| *ms += step.phase_timings.capture_outputs_ms)
            .or_insert(step.phase_timings.capture_outputs_ms);
        phase_totals
            .entry("persistence_merge")
            .and_modify(|ms| *ms += step.phase_timings.persistence_merge_ms)
            .or_insert(step.phase_timings.persistence_merge_ms);
    }

    let percent = |part: f64, whole: f64| -> f64 {
        if whole <= f64::EPSILON { 0.0 } else { (part / whole) * 100.0 }
    };

    let mut lines = vec![
        String::new(),
        "=== Conductor Timing Profile ===".to_string(),
        format!(
            "Run wall: {}  |  step sum: {}",
            format_duration_ms(wall_total_ms),
            format_duration_ms(step_total_ms)
        ),
        format!(
            "Level critical path: {} ({:.1}% of wall)",
            format_duration_ms(level_critical_path_ms),
            percent(level_critical_path_ms, wall_total_ms)
        ),
        format!(
            "Parallelism dividend: {}  |  orchestration overhead: {}",
            format_duration_ms(parallelism_dividend_ms),
            format_duration_ms(orchestration_overhead_ms)
        ),
        format!(
            "Summary: executed={}  cached={}  rematerialized={}  rpc_fallbacks={}",
            profile.summary.executed_instances,
            profile.summary.cached_instances,
            profile.summary.rematerialized_instances,
            profile.runtime_diagnostics.scheduler.rpc_fallbacks_total,
        ),
        "Scope note: this profile covers conductor workflow runtime only; mediapm post-sync checks and manifest generation are outside this timing scope.".to_string(),
        String::new(),
        "-- Internal step phase totals --".to_string(),
    ];

    for (phase_name, total_ms) in &phase_totals {
        lines.push(format!(
            "  {phase_name:<18}  {} ({:.1}% of step sum)",
            format_duration_ms(*total_ms),
            percent(*total_ms, step_total_ms)
        ));
    }

    lines.push(String::new());
    lines.push("-- Phase breakdown --".to_string());

    for (_, substeps) in &phase_groups {
        let phase_total_ms: f64 = substeps.iter().map(|(_, _, elapsed)| *elapsed).sum();
        let phase_label = {
            let names: Vec<&str> = substeps.iter().map(|(_, name, _)| name.as_str()).collect();
            let first = names[0];
            if names.len() == 1 {
                first.to_string()
            } else {
                let prefix_len = names.iter().skip(1).fold(first.len(), |acc, value| {
                    first.bytes().zip(value.bytes()).take_while(|(a, b)| a == b).count().min(acc)
                });
                let trimmed =
                    first[..prefix_len].trim_end_matches(|char_: char| !char_.is_alphanumeric());
                if trimmed.is_empty() { first.to_string() } else { trimmed.to_string() }
            }
        };

        lines.push(format!(
            "  {} ({} step{}) — {} ({:.1}% of wall)",
            phase_label,
            substeps.len(),
            if substeps.len() == 1 { "" } else { "s" },
            format_duration_ms(phase_total_ms),
            percent(phase_total_ms, wall_total_ms)
        ));
        for (prefix, name, elapsed_ms) in substeps {
            lines.push(format!("    {prefix:<5}  {name:<40}  {}", format_duration_ms(*elapsed_ms)));
        }
        lines.push(String::new());
    }

    lines.push("-- Tool breakdown --".to_string());
    for (tool_name, aggregate) in tool_rows {
        let mean_ms = if aggregate.samples == 0 {
            0.0
        } else {
            aggregate.total_ms / aggregate.samples as f64
        };
        lines.push(format!(
            "  {tool_name}\n    total={} ({:.1}% wall)  samples={}  mean={}  min={}  max={}\n    executed={}  cached={}  rematerialized={}  fallback={}",
            format_duration_ms(aggregate.total_ms),
            percent(aggregate.total_ms, wall_total_ms),
            aggregate.samples,
            format_duration_ms(mean_ms),
            format_duration_ms(if aggregate.min_ms.is_finite() { aggregate.min_ms } else { 0.0 }),
            format_duration_ms(aggregate.max_ms),
            aggregate.executed_samples,
            aggregate.cached_samples,
            aggregate.rematerialized_samples,
            aggregate.fallback_samples,
        ));
    }

    lines.push(String::new());
    lines.push("-- Slowest steps --".to_string());
    for sample in slowest_steps.iter().take(12) {
        lines.push(format!(
            "  {}  {:<24}  lvl {:>2}  worker {:>2}  {}",
            format_duration_ms(sample.elapsed_ms),
            sample.workflow_display_name,
            sample.level_index,
            sample.worker_index,
            sample.step_id,
        ));
    }
    lines.push(String::new());

    lines
}

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
    for line in render_profile_timing(&profile) {
        println!("{line}");
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::api::{RuntimeDiagnostics, SchedulerDiagnostics};

    use super::{
        StepExecutionProfile, StepPhaseTimingProfile, WorkflowRunProfile, render_profile_timing,
        write_profile_json,
    };

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
                phase_timings: StepPhaseTimingProfile {
                    resolve_inputs_ms: 1.0,
                    resolve_specs_ms: 1.0,
                    cache_probe_ms: 1.0,
                    materialization_ms: 1.0,
                    execution_ms: 100.0,
                    capture_outputs_ms: 1.0,
                    persistence_merge_ms: 1.0,
                },
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

        assert_eq!(value["version"].as_u64(), Some(2));
        assert_eq!(value["step_executions"][0]["step_id"].as_str(), Some("step-a"));
        assert_eq!(value["summary"]["executed_instances"].as_u64(), Some(1));
    }

    /// Verifies rendered profiler output includes overhead and tool sections.
    #[test]
    fn render_profile_timing_reports_overhead_and_tool_breakdown() {
        let temp = tempdir().expect("tempdir");
        let profile = WorkflowRunProfile::new(
            0,
            10_000_000_000,
            temp.path().join("conductor.ncl").as_path(),
            temp.path().join("conductor.machine.ncl").as_path(),
            temp.path().join(".conductor").as_path(),
            temp.path().join(".conductor/state.ncl").as_path(),
            crate::api::RunSummary {
                executed_instances: 2,
                cached_instances: 0,
                rematerialized_instances: 0,
            },
            vec![
                StepExecutionProfile {
                    workflow_name: "wf".to_string(),
                    workflow_display_name: "wf".to_string(),
                    workflow_attempt: 0,
                    level_index: 0,
                    step_id: "1-0-first".to_string(),
                    tool_name: "ffmpeg@1".to_string(),
                    worker_index: 0,
                    executed: true,
                    rematerialized: false,
                    fallback_used: false,
                    elapsed_ms: 4000.0,
                    requested_output_count: 1,
                    pending_unsaved_hashes_count: 0,
                    phase_timings: StepPhaseTimingProfile {
                        resolve_inputs_ms: 100.0,
                        resolve_specs_ms: 100.0,
                        cache_probe_ms: 100.0,
                        materialization_ms: 100.0,
                        execution_ms: 3500.0,
                        capture_outputs_ms: 50.0,
                        persistence_merge_ms: 50.0,
                    },
                },
                StepExecutionProfile {
                    workflow_name: "wf".to_string(),
                    workflow_display_name: "wf".to_string(),
                    workflow_attempt: 0,
                    level_index: 1,
                    step_id: "1-1-second".to_string(),
                    tool_name: "ffmpeg@1".to_string(),
                    worker_index: 0,
                    executed: true,
                    rematerialized: false,
                    fallback_used: false,
                    elapsed_ms: 3000.0,
                    requested_output_count: 1,
                    pending_unsaved_hashes_count: 0,
                    phase_timings: StepPhaseTimingProfile {
                        resolve_inputs_ms: 100.0,
                        resolve_specs_ms: 100.0,
                        cache_probe_ms: 100.0,
                        materialization_ms: 100.0,
                        execution_ms: 2500.0,
                        capture_outputs_ms: 50.0,
                        persistence_merge_ms: 50.0,
                    },
                },
            ],
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

        let rendered = render_profile_timing(&profile).join("\n");
        assert!(rendered.contains("orchestration overhead"));
        assert!(rendered.contains("-- Internal step phase totals --"));
        assert!(rendered.contains("-- Tool breakdown --"));
        assert!(rendered.contains("-- Slowest steps --"));
    }
}
