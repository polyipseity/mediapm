//! Cache-hit and re-materialization example for the conductor crate.
//!
//! This example demonstrates:
//! - a workflow whose second run is served entirely from the instance cache,
//! - re-materialization: a later caller that requires an output the cached
//!   instance never recorded forces the step to execute again.
//!
//! The two workflows share a step whose material is identical — same tool, same
//! input, no content map — so both derive the same instance key. The declared
//! output set is deliberately *not* key material: `workflow_a`'s step declares
//! no outputs, `workflow_b`'s declares `result`. Reusing `workflow_a`'s
//! instance for `workflow_b` is therefore impossible even though the key
//! matches, because that instance recorded no `result` for a dependent to bind.

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
                description: Some("no declared outputs".to_string()),
                impure: false,
                steps: vec![
                    WorkflowStepSpec {
                        id: "shared_a".to_string(),
                        tool: "echo".to_string(),
                        inputs: BTreeMap::from([("text".to_string(), "hello".to_string())]),
                        // Declares nothing, so its instance records no output
                        // and cannot serve a caller that requires one.
                        outputs: BTreeMap::new(),
                        max_retries: 0,
                        depends_on: Vec::new(),
                    },
                    WorkflowStepSpec {
                        id: "consumer_a".to_string(),
                        tool: "echo".to_string(),
                        // A literal input: there is no recorded output to bind,
                        // and a `${step_output.shared_a...}` reference would
                        // fail to resolve on every run.
                        inputs: BTreeMap::from([(
                            "text".to_string(),
                            "consumer after a run that declared no outputs".to_string(),
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
                description: Some("declares the output its consumer binds".to_string()),
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
                                capture: "stdout".to_string(),
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
                            "${step_output.shared_b.result}".to_string(),
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

    // `shared_b` derives the same instance key as `shared_a`, yet it cannot
    // reuse that instance: the caller binds `result`, and the cached instance
    // recorded no such output. The probe rejects it, the step re-executes, and
    // the fresh instance records the output its dependent needs — the
    // re-materialization this example is named for. Both steps execute because
    // `consumer_b` also takes material `consumer_a` never used.
    assert!(
        third_run.failed_steps == 0 && third_run.executed_steps == 2 && third_run.cached_steps == 0,
        "workflow_b must re-execute both steps rather than reuse instances that cannot serve it; \
         summary: {third_run:?}"
    );

    // Re-materialization converged: the instance now records `result`, so the
    // same workflow is fully cached on its next run. Without this half, an
    // example that re-executed forever would look identical.
    let fourth_run = conductor.run_workflow("workflow_b", RunWorkflowOptions::default()).await?;
    println!("Second run (workflow_b): {fourth_run:?}");
    assert!(
        fourth_run.failed_steps == 0
            && fourth_run.executed_steps == 0
            && fourth_run.cached_steps == 2,
        "once the required output is recorded, the workflow must be served from cache; summary: \
         {fourth_run:?}"
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
