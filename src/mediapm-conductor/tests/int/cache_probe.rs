//! # A cache hit must not name a blob the store no longer holds
//!
//! `probe_cache` answers a question about recorded state: an instance exists,
//! and it names the outputs this step binds — both the names its own
//! declarations make it record and the names its dependents reference. The
//! bytes themselves live in the CAS, which state cannot speak for. A blob
//! removed behind the conductor's back — GC damage, a wiped store, an
//! operator's cleanup — used to leave the probe reporting a hit: the step was
//! skipped, and the missing object surfaced later, mid-materialization, as a
//! failure with no route back to the step that produced it.
//!
//! The probe's two halves are deliberately separate: the state half is
//! `probe_cache` and the byte half is `required_output_blobs_exist`. The
//! executor asks the store whether the outputs it is about to reuse still
//! resolve, and downgrades the hit to a miss when one does not. The
//! test walks the whole sequence — run, delete the producer's blob, run
//! again — because either half alone proves nothing: without the deleted blob
//! the re-run is an ordinary cache hit, and without the first run there is no
//! instance to probe.

use std::collections::BTreeMap;

use crate::{TestConductor, doc_with_workflows, echo_tool, workflow_with_steps};
use mediapm_cas::{CasApi, Hash, InMemoryCas};
use mediapm_conductor::api::RunWorkflowOptions;
use mediapm_conductor::{Conductor, OutputCaptureSpec, WorkflowStepSpec};

/// Producer step: the echo builtin's stdout becomes the declared `result`
/// output. Declaring it is what makes it required — a consumer binding it adds
/// the same name, so the step stands alone as a workflow too.
fn producer_step(text: &str) -> WorkflowStepSpec {
    WorkflowStepSpec {
        id: "producer".to_string(),
        tool: "echo@v1".to_string(),
        inputs: BTreeMap::from([("text".to_string(), text.to_string())]),
        outputs: BTreeMap::from([(
            "result".to_string(),
            OutputCaptureSpec {
                name: "result".to_string(),
                capture: "stdout".to_string(),
                ..OutputCaptureSpec::default()
            },
        )]),
        max_retries: 0,
        depends_on: Vec::new(),
    }
}

/// Consumer step: binds the producer's `result`, which is what makes that
/// output required rather than incidental.
fn consumer_step() -> WorkflowStepSpec {
    WorkflowStepSpec {
        id: "consumer".to_string(),
        tool: "echo@v1".to_string(),
        inputs: BTreeMap::from([(
            "text".to_string(),
            "${step_output.producer.result}".to_string(),
        )]),
        outputs: BTreeMap::new(),
        max_retries: 0,
        depends_on: vec!["producer".to_string()],
    }
}

/// Returns the hash the state records for the producer's `result` output.
///
/// Only the producer declares that output name, so finding it does not depend
/// on instance iteration order or on which run recorded it.
fn recorded_result_hash(conductor: &Conductor<InMemoryCas>) -> Hash {
    conductor
        .get_state()
        .expect("the conductor state must be readable after a run")
        .tool_call_instances
        .values()
        .find_map(|instance| instance.outputs.get("result").map(|record| record.hash))
        .expect("the producer must have recorded a `result` output")
}

/// Deleting a required output's blob re-executes the step instead of reusing
/// the instance that names it.
///
/// The user-level guarantee: a store that lost a blob behind the conductor's
/// back costs a re-execution, not a broken run. The re-execution is the only
/// repair available to either kind of step — a pure step recomputes the same
/// bytes, an impure step fetches them again.
#[tokio::test]
async fn a_missing_blob_turns_a_cache_hit_into_a_rerun() {
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("echo@v1".to_string(), echo_tool("echo@v1"))]),
        vec![workflow_with_steps("default", vec![producer_step("payload"), consumer_step()])],
    ));

    let first = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the first run must complete");
    assert_eq!(
        (first.executed_steps, first.cached_steps, first.failed_steps),
        (2, 0, 0),
        "the first run must execute both steps and cache nothing: {first:?}"
    );

    let produced = recorded_result_hash(tc.conductor());
    tc.conductor().cas().delete(produced).await.expect("the recorded blob must be deletable");
    assert!(
        tc.conductor().cas().stat(produced).await.is_err(),
        "the blob must be gone before the re-run is read, or nothing is being tested"
    );

    let second = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("a run whose blob vanished must still complete");
    assert_eq!(
        second.failed_steps, 0,
        "the re-execution must repair the missing blob, not fail on it: {second:?}"
    );
    assert_eq!(
        second.executed_steps, 1,
        "the step whose required output vanished must re-execute rather than reuse the instance \
         that names it: {second:?}"
    );
    assert_eq!(
        second.cached_steps, 1,
        "the downstream step's own material is unchanged, so it stays cached: {second:?}"
    );
    assert_eq!(
        recorded_result_hash(tc.conductor()),
        produced,
        "a deterministic re-execution must restore the same content under the same hash"
    );
}

/// A workflow's last step has no downstream reader inside the run, and its
/// declared output is still inside the blob check.
///
/// A declaration is a required name exactly as a dependent's reference is, so a
/// blob missing behind the store's back re-executes the terminal step. That is
/// the self-healing outcome: the alternative is failing during materialization,
/// with no route back to the step that produced the bytes, and the run summary
/// would hand the caller a hash for content the store no longer holds.
#[tokio::test]
async fn a_terminal_step_whose_blob_is_missing_reexecutes() {
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("echo@v1".to_string(), echo_tool("echo@v1"))]),
        vec![workflow_with_steps("default", vec![producer_step("payload")])],
    ));

    let first = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the first run must complete");
    assert_eq!(
        (first.executed_steps, first.cached_steps, first.failed_steps),
        (1, 0, 0),
        "the only step of the workflow must execute and cache nothing: {first:?}"
    );

    let produced = recorded_result_hash(tc.conductor());
    tc.conductor().cas().delete(produced).await.expect("the recorded blob must be deletable");
    assert!(
        tc.conductor().cas().stat(produced).await.is_err(),
        "the blob must be gone before the re-run is read, or nothing is being tested"
    );

    let second = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("a run whose blob vanished must still complete");
    assert_eq!(
        (second.executed_steps, second.cached_steps, second.failed_steps),
        (1, 0, 0),
        "a terminal step declaring the vanished output must re-execute it: {second:?}"
    );
    assert_eq!(
        recorded_result_hash(tc.conductor()),
        produced,
        "the re-executed step must restore the same content under the same hash"
    );
}
