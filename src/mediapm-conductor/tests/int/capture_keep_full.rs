//! # A `full` capture records the empty object as its only permitted base
//!
//! `save = "full"` promises that the artifact is never stored as a delta
//! against another blob. The coordinator is what turns the promise into a fact:
//! the capture spec the step declares is the mode the run recorded, and the
//! step that records it constrains the object in the store.
//!
//! These tests run a real step and read the constraint back through the public
//! CAS API, so they prove the whole path - document, recorded spec, capture,
//! store - rather than the capture helper alone. The negative case matters as
//! much as the positive one: a constraint written for every capture would
//! satisfy the positive test while making the mode meaningless.

use std::collections::{BTreeMap, BTreeSet};

use crate::{TestConductor, doc_with_workflows, echo_tool, workflow_with_steps};
use mediapm_cas::{ConstraintApi, Hash};
use mediapm_conductor::api::RunWorkflowOptions;
use mediapm_conductor::{OutputCaptureSpec, SaveMode, WorkflowStepSpec};

/// Builds a capture spec for `stdout` under the given persistence mode.
fn stdout_spec(name: &str, save: SaveMode) -> OutputCaptureSpec {
    OutputCaptureSpec {
        name: name.to_string(),
        capture: mediapm_conductor::config::CaptureKind::Stdout,
        save,
        ..OutputCaptureSpec::default()
    }
}

/// Writes one workflow holding a single echo step that records `alpha`.
fn write_one_capture_step(tc: &TestConductor, save: SaveMode) {
    let step = WorkflowStepSpec {
        id: "s1".to_string(),
        tool: "echo@v1".to_string(),
        inputs: BTreeMap::from([("text".to_string(), "payload".to_string())]),
        outputs: BTreeMap::from([("alpha".to_string(), stdout_spec("alpha", save))]),
        max_retries: 0,
        depends_on: Vec::new(),
    };
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("echo@v1".to_string(), echo_tool("echo@v1"))]),
        vec![workflow_with_steps("default", vec![step])],
    ));
}

/// Runs the one-step workflow and returns the hash recorded for `alpha`.
async fn run_and_take_alpha_hash(tc: &TestConductor) -> mediapm_cas::Hash {
    let summary = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the run must complete");
    assert_eq!(
        (summary.executed_steps, summary.failed_steps),
        (1, 0),
        "the step must execute once for its capture to be recorded: {summary:?}",
    );

    let state = tc.conductor().get_state().expect("the conductor state must be readable");
    let instance = state
        .tool_call_instances
        .values()
        .next()
        .expect("the run must record exactly one instance");
    instance.outputs["alpha"].hash_nk.expect("alpha output hash")
}

/// A step whose capture says `full` leaves its output constrained to the empty
/// base.
///
/// The user-level guarantee: an artifact the config asked to keep whole is
/// protected from being re-encoded as a delta of another blob.
#[tokio::test]
async fn a_full_capture_records_the_empty_object_as_its_only_base() {
    let tc = TestConductor::new();
    write_one_capture_step(&tc, SaveMode::Full);

    let hash = run_and_take_alpha_hash(&tc).await;

    assert_eq!(
        tc.conductor().cas().get_constraint(hash).await.expect("the constraint must be readable"),
        BTreeSet::from([Hash::empty()]),
        "a full capture must record the empty object as its only permitted base"
    );
    tc.shutdown().await;
}

/// A step whose capture says `true` leaves its output unconstrained.
///
/// `true` keeps the bytes but promises nothing about their encoding, so a
/// constraint here would make every saved artifact permanently unoptimizable
/// and say nothing the mode asked for.
#[tokio::test]
async fn a_true_capture_records_no_constraint() {
    let tc = TestConductor::new();
    write_one_capture_step(&tc, SaveMode::True);

    let hash = run_and_take_alpha_hash(&tc).await;

    assert!(
        tc.conductor()
            .cas()
            .get_constraint(hash)
            .await
            .expect("the constraint must be readable")
            .is_empty(),
        "only a `full` capture records a full-capture constraint"
    );
    tc.shutdown().await;
}
