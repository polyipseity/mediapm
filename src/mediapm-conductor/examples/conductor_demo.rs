//! Persistent conductor demo exercising builtin tools.
//!
//! This example demonstrates a complete conductor run loop:
//! - writes a config document via `encode_document`,
//! - runs one workflow using `SimpleConductor`,
//! - runs it a second time to demonstrate cache reuse,
//! - prints run summaries to stdout.

use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use mediapm_cas::FileSystemCas;
use mediapm_conductor::{
    NickelDocument, RunWorkflowOptions, RuntimeStoragePaths, SimpleConductor, ToolInputKind,
    ToolInputSpec, ToolKindSpec, ToolRuntime, ToolSpec, WorkflowSpec, WorkflowStepSpec,
    model::config::versions::encode_document,
};

type ExampleResult<T> = Result<T, Box<dyn Error>>;

#[derive(Debug)]
struct EphemeralRunDir {
    path: PathBuf,
}

impl EphemeralRunDir {
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for EphemeralRunDir {
    fn drop(&mut self) {
        if self.path.exists() {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

fn create_ephemeral_run_dir(example_name: &str) -> ExampleResult<EphemeralRunDir> {
    static SEQUENCE: AtomicU64 = AtomicU64::new(1);
    let timestamp_ns = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_nanos());
    let seq = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let dir = format!("{example_name}-{pid}-{timestamp_ns}-{seq}");
    let p = std::env::temp_dir().join("mediapm-conductor-examples").join(dir);
    fs::create_dir_all(&p)?;
    Ok(EphemeralRunDir { path: p })
}

fn write_text_file(path: &Path, content: &str) -> ExampleResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, content)?;
    Ok(())
}

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
