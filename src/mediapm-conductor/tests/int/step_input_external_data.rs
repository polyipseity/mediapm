//! A step whose input is a declared hash runs and reads the declared bytes.
//!
//! A variant declared only as a hash reaches its step as the binding
//! `${external_data.<hash>}`. Step inputs resolve without a store handle — the
//! worker holds none at that point — so the reference cannot be read there, and
//! the command surface, which does hold one, turns the hash into bytes.
//!
//! Both halves of that division are load-bearing and both are pinned here: a
//! reference that errors at input resolution fails the step before it starts,
//! and a command that materializes the hash literal hands the tool a file
//! holding the hash text instead of the payload. The first is read off the run
//! summary, the second off the captured bytes read back out of the store, so
//! neither half can regress without failing an assertion.

use std::collections::BTreeMap;

use bytes::Bytes;
use mediapm_cas::{CasApi, Hash};
use mediapm_conductor::api::RunWorkflowOptions;
use mediapm_conductor::{
    ExternalDataEntry, OutputCaptureSpec, OutputSaveMode, ToolInputKind, ToolInputSpec,
    ToolKindSpec, ToolRuntime, ToolSpec, WorkflowStepSpec,
};

use crate::{TestConductor, doc_with_workflows, workflow_with_steps};

/// The payload the step must read, staged in the store and declared in the
/// document rather than produced by any step of the run.
const DECLARED_PAYLOAD: &[u8] = b"declared payload bytes";

/// The sandbox path the command materializes its input to.
const MATERIALIZED_INPUT_PATH: &str = "inputs/in0.bin";

/// Writes a one-step workflow whose only input is the declared hash `hash`.
///
/// The command reads that input through the `:file(...)` selector, the shape the
/// ffmpeg preset builds, so a selector that wrote the hash literal instead of the
/// bytes it names would leave the assertion in the test with the hash text.
fn write_declared_input_workflow(tc: &TestConductor, hash: Hash) {
    let reader = ToolSpec {
        version: None,
        kind: ToolKindSpec::Executable {
            command: vec![
                "sh".to_string(),
                "-c".to_string(),
                format!("cat ${{inputs.in0:file({MATERIALIZED_INPUT_PATH})}}"),
            ],
            env_vars: BTreeMap::new(),
            success_codes: vec![0],
        },
        name: "reader".into(),
        inputs: BTreeMap::from([(
            "in0".into(),
            ToolInputSpec { kind: ToolInputKind::Content, required: false },
        )]),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime::default(),
    };
    let step = WorkflowStepSpec {
        id: "s1".into(),
        tool: "reader".into(),
        inputs: BTreeMap::from([("in0".into(), format!("${{external_data.{hash}}}"))]),
        outputs: BTreeMap::from([(
            "echoed".into(),
            OutputCaptureSpec {
                name: "echoed".to_string(),
                capture: "stdout".to_string(),
                ..OutputCaptureSpec::default()
            },
        )]),
        max_retries: 0,
        depends_on: Vec::new(),
    };

    let mut doc = doc_with_workflows(
        BTreeMap::from([("reader".to_string(), reader)]),
        vec![workflow_with_steps("default", vec![step])],
    );
    doc.external_data
        .insert(hash, ExternalDataEntry { description: None, save_mode: OutputSaveMode::Saved });
    tc.write_config(doc);
}

/// A step whose input is a declared hash runs and its command reads the bytes.
///
/// The user-level guarantee: declaring a payload is enough for a step to consume
/// it. The hash is staged and declared, no step produces it, and the command
/// still hands the tool the payload — the path an ffmpeg step takes when its
/// input variant is declared only as a hash.
#[cfg(unix)]
#[tokio::test]
async fn a_step_reading_a_declared_hash_sees_the_declared_bytes() {
    let tc = TestConductor::new();
    let hash = tc
        .conductor()
        .cas()
        .put(Bytes::from_static(DECLARED_PAYLOAD))
        .await
        .expect("the declared payload must be storable");
    write_declared_input_workflow(&tc, hash);

    let summary = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("a step whose input is a declared hash must run");

    assert_eq!(
        (summary.executed_steps, summary.cached_steps, summary.failed_steps),
        (1, 0, 0),
        "the step runs once and succeeds: {summary:?}",
    );

    let state = tc.conductor().get_state().expect("the state must be readable");
    let instance = state
        .tool_call_instances
        .values()
        .next()
        .expect("the run must record one instance for the executed step");
    let echoed = instance.outputs.get("echoed").expect("the step declares `echoed`");
    let bytes = tc
        .conductor()
        .cas()
        .get(echoed.hash)
        .await
        .expect("the captured stdout must be in the store");

    assert_eq!(
        bytes.as_ref(),
        DECLARED_PAYLOAD,
        "the command must materialize the declared payload, not the hash naming it",
    );
    tc.shutdown().await;
}

/// The same step run twice reads the same payload out of one recorded instance.
///
/// The declared hash is the input, so the second run reuses the first run's
/// instance instead of executing again — the payload was never the run's to
/// produce, and nothing about the second run makes it different work.
#[cfg(unix)]
#[tokio::test]
async fn a_declared_hash_input_is_the_same_work_on_a_second_run() {
    let tc = TestConductor::new();
    let hash = tc
        .conductor()
        .cas()
        .put(Bytes::from_static(DECLARED_PAYLOAD))
        .await
        .expect("the declared payload must be storable");
    write_declared_input_workflow(&tc, hash);

    let first = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the first run must complete");
    assert_eq!(
        (first.executed_steps, first.cached_steps),
        (1, 0),
        "the first run executes the step: {first:?}",
    );

    let second = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the second run must complete");
    assert_eq!(
        (second.executed_steps, second.cached_steps),
        (0, 1),
        "the second run reuses the recorded instance: {second:?}",
    );
    tc.shutdown().await;
}
