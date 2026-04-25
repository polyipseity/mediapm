//! Cache-hit and re-materialization example for the conductor crate.
//!
//! This example mirrors the conductor's persistence semantics in a small,
//! inspectable setup:
//! - two workflows resolve to the same deterministic instance key,
//! - one caller wants `save = false`, the other wants `save = "full"`,
//! - both workflows include a downstream step that reads
//!   `${step_output...}` from the shared producer,
//! - the first run deduplicates execution,
//! - the second run re-materializes the missing producer output blob while
//!   still recognizing the shared cached instances.
//!
//! Unlike `demo.rs`, this example runs in an ephemeral temporary directory and
//! prints the key persistence/rematerialization outcomes to stdout.

use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use mediapm_cas::{CasApi, FileSystemCas};
use mediapm_conductor::{
    ConductorApi, MachineNickelDocument, NickelDocumentMetadata, NickelIdentity, OutputPolicy,
    OutputSaveMode, SimpleConductor, ToolKindSpec, ToolSpec, UserNickelDocument, WorkflowSpec,
    WorkflowStepSpec,
};
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
    write_text_file(path, &render_user_document(document))
}

/// Writes one public machine document as latest-schema Nickel source.
fn write_machine_document(path: &Path, document: &MachineNickelDocument) -> ExampleResult<()> {
    write_text_file(path, &render_machine_document(document))
}

/// Renders one public user document into latest-schema Nickel source.
fn render_user_document(document: &UserNickelDocument) -> String {
    let envelope = json!({
        "version": 1,
        "external_data": document.external_data,
        "tools": tool_specs_to_wire_json(&document.tools),
        "workflows": workflow_specs_to_wire_json(&document.workflows),
        "tool_configs": document.tool_configs,
        "impure_timestamps": document.impure_timestamps,
        "state_pointer": document.state_pointer,
    });
    format!("{}\n", render_nickel_value(&envelope, 0))
}

/// Renders one public machine document into latest-schema Nickel source.
fn render_machine_document(document: &MachineNickelDocument) -> String {
    let envelope = json!({
        "version": 1,
        "external_data": document.external_data,
        "tools": tool_specs_to_wire_json(&document.tools),
        "workflows": workflow_specs_to_wire_json(&document.workflows),
        "tool_configs": document.tool_configs,
        "impure_timestamps": document.impure_timestamps,
        "state_pointer": document.state_pointer,
    });
    format!("{}\n", render_nickel_value(&envelope, 0))
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

/// Builds the user document used to demonstrate deduplication and
/// used-output-aware re-materialization.
fn build_user_document() -> UserNickelDocument {
    let builtin = |name: &str, version: &str| ToolSpec {
        kind: ToolKindSpec::Builtin { name: name.to_string(), version: version.to_string() },
        ..ToolSpec::default()
    };

    UserNickelDocument {
        metadata: NickelDocumentMetadata {
            id: "cache-rematerialization".to_string(),
            identity: NickelIdentity { first: "cache".to_string(), last: "demo".to_string() },
        },
        tools: BTreeMap::from([("echo@1.0.0".to_string(), builtin("echo", "1.0.0"))]),
        workflows: BTreeMap::from([
            (
                "workflow_a".to_string(),
                WorkflowSpec {
                    name: Some("workflow a".to_string()),
                    description: Some(
                        "Produces and consumes a shared output with save=false policy".to_string(),
                    ),
                    steps: vec![
                        WorkflowStepSpec {
                            id: "shared_a".to_string(),
                            tool: "echo@1.0.0".to_string(),
                            inputs: BTreeMap::from([(
                                "text".to_string(),
                                "hello".to_string().into(),
                            )]),
                            depends_on: Vec::new(),
                            outputs: BTreeMap::from([(
                                "result".to_string(),
                                OutputPolicy { save: Some(OutputSaveMode::Unsaved) },
                            )]),
                        },
                        WorkflowStepSpec {
                            id: "consumer_a".to_string(),
                            tool: "echo@1.0.0".to_string(),
                            inputs: BTreeMap::from([(
                                "text".to_string(),
                                "consume:${step_output.shared_a.result}".to_string().into(),
                            )]),
                            depends_on: vec!["shared_a".to_string()],
                            outputs: BTreeMap::new(),
                        },
                    ],
                },
            ),
            (
                "workflow_b".to_string(),
                WorkflowSpec {
                    name: Some("workflow b".to_string()),
                    description: Some(
                        "Produces and consumes a shared output with save=\"full\" policy"
                            .to_string(),
                    ),
                    steps: vec![
                        WorkflowStepSpec {
                            id: "shared_b".to_string(),
                            tool: "echo@1.0.0".to_string(),
                            inputs: BTreeMap::from([(
                                "text".to_string(),
                                "hello".to_string().into(),
                            )]),
                            depends_on: Vec::new(),
                            outputs: BTreeMap::from([(
                                "result".to_string(),
                                OutputPolicy { save: Some(OutputSaveMode::Full) },
                            )]),
                        },
                        WorkflowStepSpec {
                            id: "consumer_b".to_string(),
                            tool: "echo@1.0.0".to_string(),
                            inputs: BTreeMap::from([(
                                "text".to_string(),
                                "consume:${step_output.shared_b.result}".to_string().into(),
                            )]),
                            depends_on: vec!["shared_b".to_string()],
                            outputs: BTreeMap::new(),
                        },
                    ],
                },
            ),
        ]),
        ..UserNickelDocument::default()
    }
}

/// Builds the machine document used by the example.
fn build_machine_document() -> MachineNickelDocument {
    MachineNickelDocument {
        metadata: NickelDocumentMetadata {
            id: "cache-rematerialization-machine".to_string(),
            identity: NickelIdentity { first: "cache".to_string(), last: "machine".to_string() },
        },
        ..MachineNickelDocument::default()
    }
}

/// Executes the example and prints persistence/rematerialization outcomes.
async fn run_cache_and_rematerialization_demo() -> ExampleResult<()> {
    let run_dir = create_ephemeral_run_dir("cache-and-rematerialization")?;
    let root = run_dir.path();
    let cas_root = root.join("cas-store");
    let user_path = root.join("conductor.ncl");
    let machine_path = root.join("conductor.machine.ncl");

    write_user_document(&user_path, &build_user_document())?;
    write_machine_document(&machine_path, &build_machine_document())?;

    let conductor = SimpleConductor::new(FileSystemCas::open(&cas_root).await?);

    let first_run = conductor.run_workflow(&user_path, &machine_path).await?;
    let first_state = conductor.get_state().await?;

    let shared_output_hash = first_state
        .instances
        .values()
        .find(|instance| {
            instance
                .inputs
                .get("text")
                .is_some_and(|input| input.plain_content.as_slice() == b"hello")
        })
        .and_then(|instance| instance.outputs.get("result"))
        .map(|output| output.hash)
        .expect("shared result output should exist in state after first run");

    let cas_reader = FileSystemCas::open(&cas_root).await?;
    let output_existed_after_first_run = cas_reader.exists(shared_output_hash).await?;

    let second_run = conductor.run_workflow(&user_path, &machine_path).await?;

    let output_existed_after_second_run =
        FileSystemCas::open(&cas_root).await?.exists(shared_output_hash).await?;

    println!("temporary run directory (auto-cleaned): {}", root.display());
    println!(
        "first run => executed: {}, cached: {}, rematerialized: {}",
        first_run.executed_instances,
        first_run.cached_instances,
        first_run.rematerialized_instances,
    );
    println!(
        "second run => executed: {}, cached: {}, rematerialized: {}",
        second_run.executed_instances,
        second_run.cached_instances,
        second_run.rematerialized_instances,
    );
    println!("output existed after first run: {output_existed_after_first_run}");
    println!("output existed after second run: {output_existed_after_second_run}");

    Ok(())
}

#[tokio::main]
/// Executes the cache/rematerialization example.
async fn main() -> ExampleResult<()> {
    run_cache_and_rematerialization_demo().await
}
