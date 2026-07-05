//! Cache-hit and re-materialization example for the conductor crate.
//!
//! This example demonstrates:
//! - two workflows resolving to the same deterministic instance key,
//! - one caller using capture=false, the other using capture=full,
//! - cache-hit behavior on second run.

mod support;

use std::collections::BTreeMap;
use std::fs;

use mediapm_cas::FileSystemCas;
use mediapm_conductor::{
    NickelDocument, RunWorkflowOptions, RuntimeStoragePaths, SimpleConductor, ToolInputKind,
    ToolInputSpec, ToolKindSpec, ToolRuntime, ToolSpec, WorkflowSpec, WorkflowStepSpec,
    config::versions::encode_document,
};

use support::{ExampleResult, create_ephemeral_run_dir, write_text_file};

fn build_document() -> NickelDocument {
    NickelDocument {
        tools: BTreeMap::from([(
            "echo@1.0.0".to_string(),
            ToolSpec {
                kind: ToolKindSpec::Builtin { builtin_id: "echo@1.0.0".to_string() },
                name: "echo@1.0.0".to_string(),
                inputs: BTreeMap::from([(
                    "text".to_string(),
                    ToolInputSpec { kind: ToolInputKind::String, required: false },
                )]),
                default_inputs: BTreeMap::new(),
                outputs: BTreeMap::new(),
                runtime: ToolRuntime::default(),
            },
        )]),
        workflows: vec![
            WorkflowSpec {
                name: "workflow_a".to_string(),
                display_name: String::new(),
                description: "save=false demo".to_string(),
                impure: false,
                steps: vec![
                    WorkflowStepSpec {
                        id: "shared_a".to_string(),
                        tool: "echo@1.0.0".to_string(),
                        inputs: BTreeMap::from([("text".to_string(), "hello".to_string())]),
                        outputs: BTreeMap::from([(
                            "result".to_string(),
                            mediapm_conductor::OutputCaptureSpec {
                                name: "result".to_string(),
                                capture: "false".to_string(),
                                save: mediapm_conductor::SaveMode::True,
                                allow_empty: false,
                                include_topmost_folder: true,
                            },
                        )]),
                        max_retries: 0,
                        depends_on: Vec::new(),
                    },
                    WorkflowStepSpec {
                        id: "consumer_a".to_string(),
                        tool: "echo@1.0.0".to_string(),
                        inputs: BTreeMap::from([(
                            "text".to_string(),
                            "consume:${step_output.shared_a.result}".to_string(),
                        )]),
                        outputs: BTreeMap::new(),
                        max_retries: 0,
                        depends_on: vec!["shared_a".to_string()],
                    },
                ],
            },
            WorkflowSpec {
                name: "workflow_b".to_string(),
                display_name: String::new(),
                description: "save=full demo".to_string(),
                impure: false,
                steps: vec![
                    WorkflowStepSpec {
                        id: "shared_b".to_string(),
                        tool: "echo@1.0.0".to_string(),
                        inputs: BTreeMap::from([("text".to_string(), "hello".to_string())]),
                        outputs: BTreeMap::from([(
                            "result".to_string(),
                            mediapm_conductor::OutputCaptureSpec {
                                name: "result".to_string(),
                                capture: "full".to_string(),
                                save: mediapm_conductor::SaveMode::True,
                                allow_empty: false,
                                include_topmost_folder: true,
                            },
                        )]),
                        max_retries: 0,
                        depends_on: Vec::new(),
                    },
                    WorkflowStepSpec {
                        id: "consumer_b".to_string(),
                        tool: "echo@1.0.0".to_string(),
                        inputs: BTreeMap::from([(
                            "text".to_string(),
                            "consume:${step_output.shared_b.result}".to_string(),
                        )]),
                        outputs: BTreeMap::new(),
                        max_retries: 0,
                        depends_on: vec!["shared_b".to_string()],
                    },
                ],
            },
        ],
        ..NickelDocument::default()
    }
}

async fn run_cache_and_rematerialization_demo() -> ExampleResult<()> {
    let run_dir = create_ephemeral_run_dir("cache-and-rematerialization")?;
    let root = run_dir.path();
    let cas_root = root.join("cas-store");

    fs::create_dir_all(root)?;
    let config_path = root.join("conductor.ncl");
    let doc = build_document();
    let encoded = String::from_utf8(encode_document(doc)?)?;
    write_text_file(&config_path, &encoded)?;

    let conductor =
        SimpleConductor::new(RuntimeStoragePaths::new(root), FileSystemCas::open(&cas_root).await?);

    let first_run = conductor.run_workflow("workflow_a", RunWorkflowOptions::default()).await?;
    println!("First run (workflow_a): {first_run:?}");

    let second_run = conductor.run_workflow("workflow_a", RunWorkflowOptions::default()).await?;
    println!("Second run (workflow_a): {second_run:?}");

    let third_run = conductor.run_workflow("workflow_b", RunWorkflowOptions::default()).await?;
    println!("First run (workflow_b): {third_run:?}");

    Ok(())
}

#[tokio::main]
async fn main() -> ExampleResult<()> {
    run_cache_and_rematerialization_demo().await
}
