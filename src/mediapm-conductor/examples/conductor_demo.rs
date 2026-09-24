//! Persistent conductor demo exercising builtin tools.
//!
//! This example demonstrates a complete conductor run loop:
//! - writes a config document via `encode_document`,
//! - runs one workflow using `Conductor`,
//! - runs it a second time to demonstrate cache reuse,
//! - prints run summaries to stdout.

mod support;

use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;

use mediapm_cas::FileSystemCas;
use mediapm_conductor::{
    Conductor, NickelDocument, RunWorkflowOptions, RuntimeStoragePaths, WorkflowSpec,
    WorkflowStepSpec, config::versions::encode_document,
};

#[cfg(feature = "progress")]
use mediapm_utils::progress::ProgressTerminal;

use support::{ExampleResult, echo_tool, write_text_file};

fn build_document() -> NickelDocument {
    NickelDocument {
        tools: BTreeMap::from([("echo@v1".into(), echo_tool())]),
        workflows: vec![WorkflowSpec {
            name: "demo_workflow".into(),
            display_name: Some("Demo Workflow".into()),
            description: Some("A simple demo workflow with an echo step".into()),
            impure: false,
            steps: vec![WorkflowStepSpec {
                id: "greeting".into(),
                tool: "echo".into(),
                inputs: BTreeMap::from([("text".into(), "Hello from conductor demo!".into())]),
                outputs: BTreeMap::from([(
                    "result".into(),
                    mediapm_conductor::OutputCaptureSpec {
                        name: "result".into(),
                        capture: "full".to_string(),
                        save: mediapm_conductor::SaveMode::True,
                        allow_empty: false,
                        include_topmost_folder: true,
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
    let run_dir = mediapm_utils::temp::artifact_dir()
        .map_err(|source| -> Box<dyn std::error::Error> { Box::new(source) })?;
    let root = run_dir.path();
    let cas_root = root.join("cas-store");
    fs::create_dir_all(root)?;

    let config_path = root.join("conductor.ncl");
    let doc = build_document();
    let encoded = String::from_utf8(encode_document(doc)?)?;
    write_text_file(&config_path, &encoded)?;

    let conductor =
        Conductor::new(RuntimeStoragePaths::new(root), FileSystemCas::open(&cas_root).await?);

    println!("=== First run ===");
    // Hold each terminal for the rest of `run_demo`: a terminal owns the draw
    // target its screen draws into, so dropping it early would finalize the
    // workflow screen while the conductor still writes to it.
    let (first_options, _first_terminal) = run_options_with_progress();
    let first_summary = conductor.run_workflow("demo_workflow", first_options).await?;
    println!("First run summary: {first_summary:?}");

    println!("=== Second run (cache reuse) ===");
    let (second_options, _second_terminal) = run_options_with_progress();
    let second_summary = conductor.run_workflow("demo_workflow", second_options).await?;
    println!("Second run summary: {second_summary:?}");

    println!("Done.");
    Ok(())
}

/// Build run options that own a workflow progress screen: a fixed overall
/// bar (pinned at the bottom slot) plus one worker-slot spinner bar per
/// worker, created by the caller from a [`ProgressTerminal`].
///
/// The terminal is returned alongside the options because it owns the screen's
/// draw target: the caller must keep it alive for as long as the screen is in
/// use. Without the `progress` feature there is no screen to own, so the
/// second element is `()` and main() drops it immediately.
#[cfg(feature = "progress")]
fn run_options_with_progress() -> (RunWorkflowOptions, ProgressTerminal) {
    let terminal = ProgressTerminal::builder().build();
    let (group, overall) = terminal.screen().with_overall("workflow steps", 1).build();
    let options = RunWorkflowOptions {
        progress_group: Some(Arc::new(group)),
        overall_bar: Some(Arc::new(overall)),
        ..RunWorkflowOptions::default()
    };
    (options, terminal)
}

/// Without the `progress` feature there is no progress screen; use defaults.
#[cfg(not(feature = "progress"))]
fn run_options_with_progress() -> (RunWorkflowOptions, ()) {
    (RunWorkflowOptions::default(), ())
}

#[tokio::main]
async fn main() -> ExampleResult<()> {
    run_demo().await
}

#[cfg(test)]
mod tests {
    /// Ensures the documented example entry point runs end to end via `main()`.
    #[test]
    fn main_is_exercised() {
        super::main().expect("example main should run to completion");
    }
}
