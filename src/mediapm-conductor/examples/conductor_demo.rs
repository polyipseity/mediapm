//! Persistent conductor demo exercising builtin tools.
//!
//! This example demonstrates a complete conductor run loop:
//! - writes a config document via `encode_document`,
//! - runs one workflow using `SimpleConductor`,
//! - runs it a second time to demonstrate cache reuse,
//! - prints run summaries to stdout.

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
    // Demo document with one echo tool and a simple workflow.
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
            name: "demo_workflow".into(),
            display_name: "Demo Workflow".into(),
            description: "A simple demo workflow with an echo step".into(),
            impure: false,
            steps: vec![WorkflowStepSpec {
                id: "greeting".into(),
                tool: "echo@1.0.0".into(),
                inputs: BTreeMap::from([("text".into(), "Hello from conductor demo!".into())]),
                outputs: BTreeMap::from([(
                    "result".into(),
                    mediapm_conductor::OutputCaptureSpec {
                        name: "result".into(),
                        capture: "full".to_string(),
                        save: true,
                    },
                )]),
                max_retries: 0,
                depends_on: Vec::new(),
            }],
        }],
        ..NickelDocument::default()
    }
}

async fn run_demo() -> ExampleResult<()> {
    let run_dir = create_ephemeral_run_dir("conductor-demo")?;
    let root = run_dir.path();
    let cas_root = root.join("cas-store");
    fs::create_dir_all(root)?;

    let config_path = root.join("conductor.ncl");
    let doc = build_document();
    let encoded = String::from_utf8(encode_document(doc)?)?;
    write_text_file(&config_path, &encoded)?;

    let conductor =
        SimpleConductor::new(RuntimeStoragePaths::new(root), FileSystemCas::open(&cas_root).await?);

    println!("=== First run ===");
    let first_summary =
        conductor.run_workflow("demo_workflow", RunWorkflowOptions::default()).await?;
    println!("First run summary: {first_summary:?}");

    println!("=== Second run (cache reuse) ===");
    let second_summary =
        conductor.run_workflow("demo_workflow", RunWorkflowOptions::default()).await?;
    println!("Second run summary: {second_summary:?}");

    println!("Done.");
    Ok(())
}

#[tokio::main]
async fn main() -> ExampleResult<()> {
    run_demo().await
}
