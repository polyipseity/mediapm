//! Runtime diagnostics and scheduler-trace example for the conductor crate.
//!
//! This example builds a small fan-out/fan-in workflow DAG so the actor-backed
//! runtime has multiple scheduling decisions to record. It then saves worker
//! metrics, scheduler EWMA state, and trace event counts to stdout.
//!
//! Unlike `demo.rs`, this example uses an ephemeral temporary directory and
//! avoids producing persistent artifact files.

use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use mediapm_cas::FileSystemCas;
use mediapm_conductor::{
    ConductorApi, MachineNickelDocument, NickelDocumentMetadata, NickelIdentity, OutputSaveMode,
    RuntimeDiagnostics, SchedulerTraceKind, SimpleConductor, ToolKindSpec, ToolSpec,
    UserNickelDocument, WorkflowSpec, WorkflowStepSpec,
};
use serde::Serialize;
use serde_json::{Value, json};

/// Convenient result type shared by this example.
type ExampleResult<T> = Result<T, Box<dyn Error>>;

/// Best-effort temporary directory guard for non-persistent examples.
#[derive(Debug)]
struct EphemeralRunDir {
    /// Absolute path of the temporary directory used by one example run.
    path: PathBuf,
}

impl EphemeralRunDir {
    /// Returns the temporary directory path.
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for EphemeralRunDir {
    /// Removes the temporary directory tree if it still exists.
    fn drop(&mut self) {
        if self.path.exists() {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

/// Creates a unique temporary run directory that is deleted on drop.
fn create_ephemeral_run_dir(example_name: &str) -> ExampleResult<EphemeralRunDir> {
    static SEQUENCE: AtomicU64 = AtomicU64::new(1);

    let timestamp_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let process_id = std::process::id();

    let directory_name = format!("{example_name}-{process_id}-{timestamp_ns}-{sequence}");
    let path = std::env::temp_dir().join("mediapm-conductor-examples").join(directory_name);
    fs::create_dir_all(&path)?;

    Ok(EphemeralRunDir { path })
}

/// Writes UTF-8 text to disk, creating parent directories when necessary.
fn write_text_file(path: &Path, content: &str) -> ExampleResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, content)?;
    Ok(())
}

/// Writes one public user document as latest-schema Nickel source.
fn write_user_document(path: &Path, document: &UserNickelDocument) -> ExampleResult<()> {
    write_text_file(path, &render_user_document(document)?)
}

/// Writes one public machine document as latest-schema Nickel source.
fn write_machine_document(path: &Path, document: &MachineNickelDocument) -> ExampleResult<()> {
    write_text_file(path, &render_machine_document(document)?)
}

/// Renders one public user document into latest-schema Nickel source.
fn render_user_document(document: &UserNickelDocument) -> ExampleResult<String> {
    let envelope = json!({
        "version": 1,
        "external_data": document.external_data,
        "tools": tool_specs_to_wire_json(&document.tools),
        "workflows": workflow_specs_to_wire_json(&document.workflows),
        "tool_configs": document.tool_configs,
        "impure_timestamps": document.impure_timestamps,
        "state_pointer": document.state_pointer,
    });
    Ok(format!("{}\n", render_nickel_value(&envelope, 0)))
}

/// Renders one public machine document into latest-schema Nickel source.
fn render_machine_document(document: &MachineNickelDocument) -> ExampleResult<String> {
    let envelope = json!({
        "version": 1,
        "external_data": document.external_data,
        "tools": tool_specs_to_wire_json(&document.tools),
        "workflows": workflow_specs_to_wire_json(&document.workflows),
        "tool_configs": document.tool_configs,
        "impure_timestamps": document.impure_timestamps,
        "state_pointer": document.state_pointer,
    });
    Ok(format!("{}\n", render_nickel_value(&envelope, 0)))
}

/// Converts runtime tool specs into strict persisted v1 wire-shape JSON.
fn tool_specs_to_wire_json(tools: &BTreeMap<String, ToolSpec>) -> BTreeMap<String, Value> {
    tools
        .iter()
        .map(|(tool_name, tool_spec)| {
            let wire_value = match &tool_spec.kind {
                ToolKindSpec::Builtin { name, version } => {
                    json!({ "kind": "builtin", "name": name, "version": version })
                }
                ToolKindSpec::Executable { command, env_vars, success_codes } => json!({
                    "kind": "executable",
                    "is_impure": tool_spec.is_impure,
                    "inputs": tool_spec.inputs,
                    "command": command,
                    "env_vars": env_vars,
                    "success_codes": success_codes,
                    "outputs": tool_spec.outputs,
                }),
            };
            (tool_name.clone(), wire_value)
        })
        .collect()
}

/// Converts runtime workflow specs into strict persisted v1 wire-shape JSON.
fn workflow_specs_to_wire_json(
    workflows: &BTreeMap<String, WorkflowSpec>,
) -> BTreeMap<String, Value> {
    workflows
        .iter()
        .map(|(workflow_name, workflow)| {
            let mut workflow_object = serde_json::Map::new();
            if let Some(name) = &workflow.name {
                workflow_object.insert("name".to_string(), json!(name));
            }
            if let Some(description) = &workflow.description {
                workflow_object.insert("description".to_string(), json!(description));
            }
            workflow_object.insert(
                "steps".to_string(),
                Value::Array(
                    workflow
                        .steps
                        .iter()
                        .map(|step| {
                            let mut step_object = serde_json::Map::new();
                            step_object.insert("id".to_string(), json!(step.id));
                            step_object.insert("tool".to_string(), json!(step.tool));
                            step_object.insert("inputs".to_string(), json!(step.inputs));
                            step_object.insert("depends_on".to_string(), json!(step.depends_on));
                            let outputs = step
                                .outputs
                                .iter()
                                .map(|(output_name, policy)| {
                                    let mut output_policy = serde_json::Map::new();
                                    if let Some(save) = policy.save {
                                        output_policy.insert(
                                            "save".to_string(),
                                            match save {
                                                OutputSaveMode::Unsaved => Value::Bool(false),
                                                OutputSaveMode::Saved => Value::Bool(true),
                                                OutputSaveMode::Full => {
                                                    Value::String("full".to_string())
                                                }
                                            },
                                        );
                                    }
                                    (output_name.clone(), Value::Object(output_policy))
                                })
                                .collect::<BTreeMap<_, _>>();
                            step_object.insert("outputs".to_string(), json!(outputs));
                            Value::Object(step_object)
                        })
                        .collect(),
                ),
            );
            (workflow_name.clone(), Value::Object(workflow_object))
        })
        .collect()
}

/// Returns whether one key can be emitted without quoting in Nickel record syntax.
fn is_bare_nickel_identifier(key: &str) -> bool {
    let mut chars = key.chars().peekable();

    while matches!(chars.peek(), Some('_')) {
        let _ = chars.next();
    }

    let Some(head) = chars.next() else {
        return false;
    };

    if !head.is_ascii_alphabetic() {
        return false;
    }

    chars
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '\''))
}

/// Renders one field name in deterministic Nickel record syntax.
fn render_field_name(name: &str) -> String {
    if is_bare_nickel_identifier(name) {
        name.to_string()
    } else {
        serde_json::to_string(name).unwrap_or_else(|_| format!("\"{name}\""))
    }
}

/// Renders one serde JSON value as deterministic Nickel source.
fn render_nickel_value(value: &Value, indent: usize) -> String {
    let pad = " ".repeat(indent);
    let next_pad = " ".repeat(indent + 2);

    match value {
        Value::Null => "null".to_string(),
        Value::Bool(boolean) => boolean.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(text) => serde_json::to_string(text).unwrap_or_else(|_| "\"\"".to_string()),
        Value::Array(items) => {
            if items.is_empty() {
                "[]".to_string()
            } else {
                let rendered_items = items
                    .iter()
                    .map(|item| format!("{next_pad}{},", render_nickel_value(item, indent + 2)))
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("[\n{rendered_items}\n{pad}]")
            }
        }
        Value::Object(entries) => {
            if entries.is_empty() {
                "{}".to_string()
            } else {
                let mut ordered_entries = entries.iter().collect::<Vec<_>>();
                ordered_entries.sort_by(|(left, _), (right, _)| left.cmp(right));
                let rendered_entries = ordered_entries
                    .into_iter()
                    .map(|(key, entry_value)| {
                        format!(
                            "{next_pad}{} = {},",
                            render_field_name(key),
                            render_nickel_value(entry_value, indent + 2)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("{{\n{rendered_entries}\n{pad}}}")
            }
        }
    }
}

/// Serializable copy of runtime diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize)]
struct RuntimeDiagnosticsSnapshot {
    /// Number of workers in the active execution pool.
    worker_pool_size: usize,
    /// Scheduler-level adaptive diagnostics.
    scheduler: SchedulerDiagnosticsSnapshot,
    /// Per-worker queue and execution metrics.
    workers: Vec<WorkerQueueDiagnosticsSnapshot>,
    /// Recent scheduler trace events in chronological order.
    recent_traces: Vec<SchedulerTraceEventSnapshot>,
}

impl From<RuntimeDiagnostics> for RuntimeDiagnosticsSnapshot {
    /// Mirrors public runtime diagnostics into a serializable snapshot.
    fn from(value: RuntimeDiagnostics) -> Self {
        Self {
            worker_pool_size: value.worker_pool_size,
            scheduler: value.scheduler.into(),
            workers: value.workers.into_iter().map(Into::into).collect(),
            recent_traces: value.recent_traces.into_iter().map(Into::into).collect(),
        }
    }
}

/// Serializable copy of scheduler diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize)]
struct SchedulerDiagnosticsSnapshot {
    /// EWMA alpha used to blend new observations.
    ewma_alpha: f64,
    /// Default runtime estimate for unknown tools.
    unknown_cost_ms: f64,
    /// Per-tool runtime estimates.
    tool_estimates: Vec<ToolRuntimeEstimateSnapshot>,
    /// Number of scheduler RPC fallback events observed.
    rpc_fallbacks_total: u64,
}

impl From<mediapm_conductor::SchedulerDiagnostics> for SchedulerDiagnosticsSnapshot {
    /// Mirrors public scheduler diagnostics into a serializable snapshot.
    fn from(value: mediapm_conductor::SchedulerDiagnostics) -> Self {
        Self {
            ewma_alpha: value.ewma_alpha,
            unknown_cost_ms: value.unknown_cost_ms,
            tool_estimates: value.tool_estimates.into_iter().map(Into::into).collect(),
            rpc_fallbacks_total: value.rpc_fallbacks_total,
        }
    }
}

/// Serializable copy of one tool runtime estimate.
#[derive(Debug, Clone, PartialEq, Serialize)]
struct ToolRuntimeEstimateSnapshot {
    /// Logical tool name.
    tool_name: String,
    /// Current estimated runtime in milliseconds.
    estimated_ms: f64,
}

impl From<mediapm_conductor::ToolRuntimeEstimate> for ToolRuntimeEstimateSnapshot {
    /// Mirrors one tool estimate into a serializable snapshot.
    fn from(value: mediapm_conductor::ToolRuntimeEstimate) -> Self {
        Self { tool_name: value.tool_name, estimated_ms: value.estimated_ms }
    }
}

/// Serializable copy of one worker queue metrics entry.
#[derive(Debug, Clone, PartialEq, Serialize)]
struct WorkerQueueDiagnosticsSnapshot {
    /// Worker index inside the pool.
    worker_index: usize,
    /// Total steps assigned to this worker.
    assigned_steps_total: u64,
    /// Total steps completed by this worker.
    completed_steps_total: u64,
    /// Current number of in-flight step executions.
    in_flight: u64,
    /// Peak in-flight count seen for this worker.
    peak_in_flight: u64,
    /// Steps assigned in the most recently planned level.
    last_level_assigned_steps: u64,
    /// Estimated runtime load assigned in the most recent level.
    last_level_estimated_load_ms: f64,
    /// Cumulative estimated runtime load assigned to this worker.
    cumulative_estimated_load_ms: f64,
    /// Cumulative observed runtime completed by this worker.
    cumulative_observed_load_ms: f64,
    /// Number of RPC dispatch failures seen by this worker.
    rpc_failures_total: u64,
    /// Number of fallback local executions used by this worker.
    fallback_executions_total: u64,
}

impl From<mediapm_conductor::WorkerQueueDiagnostics> for WorkerQueueDiagnosticsSnapshot {
    /// Mirrors one worker metrics entry into a serializable snapshot.
    fn from(value: mediapm_conductor::WorkerQueueDiagnostics) -> Self {
        Self {
            worker_index: value.worker_index,
            assigned_steps_total: value.assigned_steps_total,
            completed_steps_total: value.completed_steps_total,
            in_flight: value.in_flight,
            peak_in_flight: value.peak_in_flight,
            last_level_assigned_steps: value.last_level_assigned_steps,
            last_level_estimated_load_ms: value.last_level_estimated_load_ms,
            cumulative_estimated_load_ms: value.cumulative_estimated_load_ms,
            cumulative_observed_load_ms: value.cumulative_observed_load_ms,
            rpc_failures_total: value.rpc_failures_total,
            fallback_executions_total: value.fallback_executions_total,
        }
    }
}

/// Serializable copy of one scheduler trace event.
#[derive(Debug, Clone, PartialEq, Serialize)]
struct SchedulerTraceEventSnapshot {
    /// Monotonic trace sequence number.
    sequence: u64,
    /// UTC timestamp in nanoseconds since Unix epoch.
    timestamp_unix_nanos: u128,
    /// Event-specific payload.
    kind: SchedulerTraceKindSnapshot,
}

impl From<mediapm_conductor::SchedulerTraceEvent> for SchedulerTraceEventSnapshot {
    /// Mirrors one scheduler trace event into a serializable snapshot.
    fn from(value: mediapm_conductor::SchedulerTraceEvent) -> Self {
        Self {
            sequence: value.sequence,
            timestamp_unix_nanos: value.timestamp_unix_nanos,
            kind: value.kind.into(),
        }
    }
}

/// Serializable copy of scheduler trace payload variants.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum SchedulerTraceKindSnapshot {
    /// One workflow level was planned for dispatch.
    LevelPlanned {
        /// Workflow name.
        workflow_name: String,
        /// Zero-based topological level index.
        level_index: usize,
        /// Number of steps in the level.
        step_count: usize,
        /// Worker count used for planning.
        worker_count: usize,
    },
    /// One step was assigned to a worker.
    StepAssigned {
        /// Workflow name.
        workflow_name: String,
        /// Level index.
        level_index: usize,
        /// Step identifier.
        step_id: String,
        /// Tool name.
        tool_name: String,
        /// Worker index.
        worker_index: usize,
        /// Estimated runtime assigned to the worker.
        estimated_ms: f64,
    },
    /// One step completed.
    StepCompleted {
        /// Step identifier.
        step_id: String,
        /// Tool name.
        tool_name: String,
        /// Worker index.
        worker_index: usize,
        /// Whether the step executed instead of hitting cache.
        executed: bool,
        /// Whether fallback local execution was used.
        fallback_used: bool,
        /// Observed runtime in milliseconds.
        observed_ms: f64,
    },
    /// RPC fallback event.
    RpcFallback {
        /// Step identifier.
        step_id: String,
        /// Worker index.
        worker_index: usize,
        /// Human-readable fallback reason.
        reason: String,
    },
    /// EWMA estimate update event.
    EwmaUpdated {
        /// Tool name.
        tool_name: String,
        /// Previous estimate, if one existed.
        previous_estimate_ms: Option<f64>,
        /// Newly observed runtime.
        observed_ms: f64,
        /// Resulting EWMA estimate.
        new_estimate_ms: f64,
    },
}

impl From<SchedulerTraceKind> for SchedulerTraceKindSnapshot {
    /// Mirrors public scheduler trace variants into serializable variants.
    fn from(value: SchedulerTraceKind) -> Self {
        match value {
            SchedulerTraceKind::LevelPlanned {
                workflow_name,
                level_index,
                step_count,
                worker_count,
            } => Self::LevelPlanned { workflow_name, level_index, step_count, worker_count },
            SchedulerTraceKind::StepAssigned {
                workflow_name,
                level_index,
                step_id,
                tool_name,
                worker_index,
                estimated_ms,
            } => Self::StepAssigned {
                workflow_name,
                level_index,
                step_id,
                tool_name,
                worker_index,
                estimated_ms,
            },
            SchedulerTraceKind::StepCompleted {
                step_id,
                tool_name,
                worker_index,
                executed,
                fallback_used,
                observed_ms,
            } => Self::StepCompleted {
                step_id,
                tool_name,
                worker_index,
                executed,
                fallback_used,
                observed_ms,
            },
            SchedulerTraceKind::RpcFallback { step_id, worker_index, reason } => {
                Self::RpcFallback { step_id, worker_index, reason }
            }
            SchedulerTraceKind::EwmaUpdated {
                tool_name,
                previous_estimate_ms,
                observed_ms,
                new_estimate_ms,
            } => {
                Self::EwmaUpdated { tool_name, previous_estimate_ms, observed_ms, new_estimate_ms }
            }
        }
    }
}

/// Produces a concise text report summarizing scheduler trace event counts.
fn render_trace_summary(diagnostics: &RuntimeDiagnosticsSnapshot) -> String {
    let mut level_planned = 0usize;
    let mut step_assigned = 0usize;
    let mut step_completed = 0usize;
    let mut rpc_fallback = 0usize;
    let mut ewma_updated = 0usize;

    for trace in &diagnostics.recent_traces {
        match trace.kind {
            SchedulerTraceKindSnapshot::LevelPlanned { .. } => level_planned += 1,
            SchedulerTraceKindSnapshot::StepAssigned { .. } => step_assigned += 1,
            SchedulerTraceKindSnapshot::StepCompleted { .. } => step_completed += 1,
            SchedulerTraceKindSnapshot::RpcFallback { .. } => rpc_fallback += 1,
            SchedulerTraceKindSnapshot::EwmaUpdated { .. } => ewma_updated += 1,
        }
    }

    format!(
        "worker_pool_size={}\nlevel_planned={}\nstep_assigned={}\nstep_completed={}\nrpc_fallback={}\newma_updated={}\n",
        diagnostics.worker_pool_size,
        level_planned,
        step_assigned,
        step_completed,
        rpc_fallback,
        ewma_updated,
    )
}

/// Builds the user document used to exercise scheduler diagnostics.
fn build_user_document() -> UserNickelDocument {
    let builtin = |name: &str, version: &str| ToolSpec {
        kind: ToolKindSpec::Builtin { name: name.to_string(), version: version.to_string() },
        ..ToolSpec::default()
    };

    UserNickelDocument {
        metadata: NickelDocumentMetadata {
            id: "runtime-diagnostics".to_string(),
            identity: NickelIdentity {
                first: "runtime".to_string(),
                last: "diagnostics".to_string(),
            },
        },
        tools: BTreeMap::from([
            ("fanout@1.0.0".to_string(), builtin("echo", "1.0.0")),
            ("merge@1.0.0".to_string(), builtin("echo", "1.0.0")),
        ]),
        workflows: BTreeMap::from([(
            "diagnostics_dag".to_string(),
            WorkflowSpec {
                name: Some("diagnostics dag".to_string()),
                description: Some(
                    "Fan-out/fan-in workflow used to showcase scheduler diagnostics".to_string(),
                ),
                steps: vec![
                    fanout_step("alpha", "A"),
                    fanout_step("beta", "B"),
                    fanout_step("gamma", "C"),
                    WorkflowStepSpec {
                        id: "merge".to_string(),
                        tool: "merge@1.0.0".to_string(),
                        inputs: BTreeMap::from([(
                            "text".to_string(),
                            "${step_output.alpha.result}-${step_output.beta.result}-${step_output.gamma.result}"
                                .to_string()
                                .into(),
                        )]),
                        depends_on: vec![
                            "alpha".to_string(),
                            "beta".to_string(),
                            "gamma".to_string(),
                        ],
                        outputs: BTreeMap::new(),
                    },
                ],
            },
        )]),
        ..UserNickelDocument::default()
    }
}

/// Builds one fan-out step with deterministic literal input.
fn fanout_step(step_id: &str, literal: &str) -> WorkflowStepSpec {
    WorkflowStepSpec {
        id: step_id.to_string(),
        tool: "fanout@1.0.0".to_string(),
        inputs: BTreeMap::from([("text".to_string(), format!("branch={literal}").into())]),
        depends_on: Vec::new(),
        outputs: BTreeMap::new(),
    }
}

/// Builds the machine document used by the diagnostics example.
fn build_machine_document() -> MachineNickelDocument {
    MachineNickelDocument {
        metadata: NickelDocumentMetadata {
            id: "runtime-diagnostics-machine".to_string(),
            identity: NickelIdentity { first: "runtime".to_string(), last: "machine".to_string() },
        },
        ..MachineNickelDocument::default()
    }
}

/// Executes the diagnostics example and prints scheduler/worker telemetry.
async fn run_runtime_diagnostics_demo() -> ExampleResult<()> {
    let run_dir = create_ephemeral_run_dir("runtime-diagnostics")?;
    let root = run_dir.path();
    let cas_root = root.join("cas-store");
    let user_path = root.join("conductor.ncl");
    let machine_path = root.join("conductor.machine.ncl");

    write_user_document(&user_path, &build_user_document())?;
    write_machine_document(&machine_path, &build_machine_document())?;

    let conductor = SimpleConductor::new(FileSystemCas::open(&cas_root).await?);
    let run_summary = conductor.run_workflow(&user_path, &machine_path).await?;
    let diagnostics = conductor.get_runtime_diagnostics().await?;
    let diagnostics_snapshot = RuntimeDiagnosticsSnapshot::from(diagnostics);

    println!("temporary run directory (auto-cleaned): {}", root.display());
    println!(
        "run summary => executed: {}, cached: {}, rematerialized: {}",
        run_summary.executed_instances,
        run_summary.cached_instances,
        run_summary.rematerialized_instances,
    );
    println!("worker pool size: {}", diagnostics_snapshot.worker_pool_size);
    println!("trace summary:\n{}", render_trace_summary(&diagnostics_snapshot));

    Ok(())
}

#[tokio::main]
/// Executes the runtime diagnostics example.
async fn main() -> ExampleResult<()> {
    run_runtime_diagnostics_demo().await
}
