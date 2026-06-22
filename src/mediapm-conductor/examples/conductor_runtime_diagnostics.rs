//! Runtime diagnostics example for the conductor crate.
//!
//! This example builds a small fan-out/fan-in workflow DAG so the actor-backed
//! runtime has multiple scheduling decisions to record. It then prints the
//! diagnostics snapshot to stdout.

mod support;

use std::collections::BTreeMap;
use std::fs;

use mediapm_cas::FileSystemCas;
use mediapm_conductor::{
    NickelDocument, RunWorkflowOptions, RuntimeStoragePaths, SimpleConductor, ToolInputKind,
    ToolInputSpec, ToolKindSpec, ToolRuntime, ToolSpec, WorkflowSpec, WorkflowStepSpec,
    config::versions::encode_document,
};

use support::*;

fn build_document() -> NickelDocument {
    // A small fan-out/fan-in workflow with multiple steps.
    NickelDocument {
        tools: BTreeMap::from([(
            "echo@1.0.0".into(),
            ToolSpec {
                kind: ToolKindSpec::Builtin { name: "echo".into(), version: "1.0.0".into() },
                name: "echo@1.0.0".into(),
                version: "1.0.0".into(),
                inputs: BTreeMap::from([(
                    "text".into(),
                    ToolInputSpec {
                        kind: ToolInputKind::String,
                        description: String::new(),
                        required: false,
                    },
                )]),
                default_inputs: BTreeMap::new(),
                outputs: BTreeMap::new(),
                runtime: ToolRuntime::default(),
            },
        )]),
        workflows: vec![WorkflowSpec {
            name: "diagnostics_demo".into(),
            display_name: String::new(),
            description: "Fan-out/fan-in workflow for diagnostics".into(),
            impure: false,
            steps: vec![
                WorkflowStepSpec {
                    id: "step_a".into(),
                    tool: "echo@1.0.0".into(),
                    inputs: BTreeMap::from([("text".into(), "step_a".into())]),
                    outputs: BTreeMap::from([(
                        "result".into(),
                        mediapm_conductor::OutputCaptureSpec {
                            name: "result".into(),
                            capture: "false".to_string(),
                            save: true,
                        },
                    )]),
                    max_retries: 0,
                    depends_on: Vec::new(),
                },
                WorkflowStepSpec {
                    id: "step_b".into(),
                    tool: "echo@1.0.0".into(),
                    inputs: BTreeMap::from([("text".into(), "step_b".into())]),
                    outputs: BTreeMap::from([(
                        "result".into(),
                        mediapm_conductor::OutputCaptureSpec {
                            name: "result".into(),
                            capture: "false".to_string(),
                            save: true,
                        },
                    )]),
                    max_retries: 0,
                    depends_on: Vec::new(),
                },
                WorkflowStepSpec {
                    id: "step_c".into(),
                    tool: "echo@1.0.0".into(),
                    inputs: BTreeMap::from([(
                        "text".into(),
                        "join:${step_output.step_a.result}+${step_output.step_b.result}".into(),
                    )]),
                    outputs: BTreeMap::new(),
                    max_retries: 0,
                    depends_on: vec!["step_a".into(), "step_b".into()],
                },
            ],
        }],
        ..NickelDocument::default()
    }
}

async fn run_diagnostics_demo() -> ExampleResult<()> {
    let run_dir = create_ephemeral_run_dir("runtime-diagnostics")?;
    let root = run_dir.path();
    let cas_root = root.join("cas-store");
    fs::create_dir_all(root)?;

    let config_path = root.join("conductor.ncl");
    let doc = build_document();
    let encoded = String::from_utf8(encode_document(doc)?)?;
    write_text_file(&config_path, &encoded)?;

    let conductor =
        SimpleConductor::new(RuntimeStoragePaths::new(root), FileSystemCas::open(&cas_root).await?);

    let summary = conductor.run_workflow("diagnostics_demo", RunWorkflowOptions::default()).await?;
    println!("Run summary: {summary:?}");

    let diagnostics = conductor.get_runtime_diagnostics().await?;
    println!("Runtime diagnostics: {diagnostics:?}");

    Ok(())
}

#[tokio::main]
async fn main() -> ExampleResult<()> {
    run_diagnostics_demo().await
}
