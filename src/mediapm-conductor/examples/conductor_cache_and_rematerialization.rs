//! Cache-hit and re-materialization example for the conductor crate.
//!
//! This example demonstrates:
//! - one workflow whose second run is served entirely from cache,
//! - one caller using capture=false, the other using capture=full,
//! - re-materialization: a caller that needs an output the cached instance did
//!   not keep re-executes the step instead of replaying the cached entry.
//!
//! Consumers take literal inputs on purpose. A `${step_output...}` reference
//! can only resolve an output the producer's capture spec recorded, so a
//! consumer of a capture=false producer would fail on every run.

mod support;

use std::collections::BTreeMap;
use std::fs;

use mediapm_cas::FileSystemCas;
use mediapm_conductor::{
    Conductor, NickelDocument, RunWorkflowOptions, RuntimeStoragePaths, WorkflowSpec,
    WorkflowStepSpec, config::versions::encode_document,
};

use support::{ExampleResult, echo_tool, write_text_file};

fn build_document() -> NickelDocument {
    NickelDocument {
        tools: BTreeMap::from([("echo@v1".to_string(), echo_tool())]),
        workflows: vec![
            WorkflowSpec {
                name: "workflow_a".to_string(),
                display_name: None,
                description: Some("save=false demo".to_string()),
                impure: false,
                steps: vec![
                    WorkflowStepSpec {
                        id: "shared_a".to_string(),
                        tool: "echo".to_string(),
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
                        tool: "echo".to_string(),
                        // A literal input, not a `consume:` binding: `shared_a`
                        // captures with `capture: "false"`, so its result is
                        // saved to the CAS but never bound for downstream
                        // consumption — a `consume:` of it fails on every run.
                        // The consumer still depends on `shared_a`, so the
                        // workflow keeps its two-step shape.
                        inputs: BTreeMap::from([(
                            "text".to_string(),
                            "consumer after a save=true capture=false run".to_string(),
                        )]),
                        outputs: BTreeMap::new(),
                        max_retries: 0,
                        depends_on: vec!["shared_a".to_string()],
                    },
                ],
            },
            WorkflowSpec {
                name: "workflow_b".to_string(),
                display_name: None,
                description: Some("save=full demo".to_string()),
                impure: false,
                steps: vec![
                    WorkflowStepSpec {
                        id: "shared_b".to_string(),
                        tool: "echo".to_string(),
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
                        tool: "echo".to_string(),
                        inputs: BTreeMap::from([(
                            "text".to_string(),
                            "consumer after a save=full capture=full run".to_string(),
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

    let first_run = conductor.run_workflow("workflow_a", RunWorkflowOptions::default()).await?;
    println!("First run (workflow_a): {first_run:?}");

    let second_run = conductor.run_workflow("workflow_a", RunWorkflowOptions::default()).await?;
    println!("Second run (workflow_a): {second_run:?}");

    // Idempotency contract: an immediate second run against an unchanged
    // workspace is fully cached. Instance keys are content-derived, so the
    // re-run must execute nothing and serve every step from cache.
    assert_eq!(
        first_run.cached_steps, 0,
        "first run must execute everything from cold cache; summary: {first_run:?}"
    );
    assert!(
        first_run.executed_steps > 0,
        "first run must execute the workflow; summary: {first_run:?}"
    );
    assert_eq!(first_run.failed_steps, 0, "first run must fail no steps; summary: {first_run:?}");
    assert_eq!(
        second_run.executed_steps, 0,
        "second run must execute no steps; summary: {second_run:?}"
    );
    assert!(second_run.cached_steps > 0, "second run must hit the cache; summary: {second_run:?}");
    assert_eq!(
        second_run.failed_steps, 0,
        "second run must fail no steps; summary: {second_run:?}"
    );

    let third_run = conductor.run_workflow("workflow_b", RunWorkflowOptions::default()).await?;
    println!("First run (workflow_b): {third_run:?}");

    // workflow_b's `shared_b` carries the same material as `shared_a` but a
    // different capture spec, and its caller needs the `result` output the
    // capture=false run never kept, so the step re-executes instead of
    // replaying the cached entry — the re-materialization this example is
    // named for. Its consumer uses a literal input, so it must succeed.
    assert_eq!(third_run.failed_steps, 0, "workflow_b must fail no steps; summary: {third_run:?}");
    assert!(
        third_run.executed_steps > 0,
        "workflow_b's shared step must re-execute to rematerialize the output its caller needs; \
         summary: {third_run:?}"
    );

    Ok(())
}

#[tokio::main]
async fn main() -> ExampleResult<()> {
    run_cache_and_rematerialization_demo().await
}

#[cfg(test)]
mod tests {
    /// Ensures the documented example entry point runs end to end via `main()`.
    #[test]
    fn main_is_exercised() {
        super::main().expect("example main should run to completion");
    }
}
