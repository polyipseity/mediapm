//! # Distinct output declarations produce distinct instances under key v8
//!
//! Key v8 includes the output specification (output names and capture tokens)
//! in the instance key, so steps that declare different outputs do not share an
//! instance even when their command inputs are identical. Each step records its
//! own outputs and is served from cache on a steady-state re-run.
//!
//! A changed capture declaration changes the key for that step, so only that
//! step re-executes; steps whose declarations are unchanged remain cache hits.
//! File-capture byte-identity rules for stale records still apply (pinned by
//! the fs-tool tests below).

use std::collections::BTreeMap;

use crate::{TestConductor, doc_with_workflows, echo_tool, workflow_with_steps};
use mediapm_cas::{CasApi, Hash, InMemoryCas};
use mediapm_conductor::api::RunWorkflowOptions;
use mediapm_conductor::{
    Conductor, OutputCaptureSpec, ToolInputKind, ToolInputSpec, ToolKindSpec, ToolRuntime,
    ToolSpec, WorkflowStepSpec,
};

/// Builds a capture spec reading one implicit stream, every optional field at
/// its documented default.
fn stream_spec(name: &str, stream: &str) -> OutputCaptureSpec {
    OutputCaptureSpec {
        name: name.to_string(),
        capture: mediapm_conductor::config::CaptureKind::parse_token(stream)
            .expect("valid capture"),
        ..OutputCaptureSpec::default()
    }
}

/// Builds an `echo` step over the shared payload text.
///
/// Every step in these tests runs the same command with the same inputs, which
/// is what puts them under one instance key; only the declarations differ.
fn echo_step(
    id: &str,
    depends_on: &[&str],
    outputs: BTreeMap<String, OutputCaptureSpec>,
) -> WorkflowStepSpec {
    WorkflowStepSpec {
        id: id.to_string(),
        tool: "echo@v1".to_string(),
        inputs: BTreeMap::from([("text".to_string(), "payload".to_string())]),
        outputs,
        runtime: Some(mediapm_conductor::config::StepRuntime { max_retries: Some(0) }),
        depends_on: depends_on.iter().map(ToString::to_string).collect(),
    }
}

/// Writes a one-workflow document whose echo tool declares `tool_outputs` as
/// its capture defaults.
fn write_doc(
    tc: &TestConductor,
    tool_outputs: BTreeMap<String, OutputCaptureSpec>,
    steps: Vec<WorkflowStepSpec>,
) {
    let mut tool = echo_tool("echo@v1");
    tool.outputs = tool_outputs;
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("echo@v1".to_string(), tool)]),
        vec![workflow_with_steps("default", steps)],
    ));
}

/// The single instance this run recorded, asserting there is exactly one.
fn only_instance(conductor: &Conductor<InMemoryCas>) -> mediapm_conductor::ToolCallInstance {
    let state = conductor.get_state().expect("the conductor state must be readable");
    assert_eq!(state.tool_call_instances.len(), 1, "expected exactly one recorded instance");
    state.tool_call_instances.values().next().cloned().expect("the one instance")
}

/// Returns every recorded instance.
fn all_instances(conductor: &Conductor<InMemoryCas>) -> Vec<mediapm_conductor::ToolCallInstance> {
    conductor
        .get_state()
        .expect("the conductor state must be readable")
        .tool_call_instances
        .values()
        .cloned()
        .collect()
}

/// Returns the instance recorded for `workflow/step`.
fn instance_for_step(
    conductor: &Conductor<InMemoryCas>,
    workflow: &str,
    step: &str,
) -> mediapm_conductor::ToolCallInstance {
    let state = conductor.get_state().expect("the conductor state must be readable");
    let key = *state
        .step_instances
        .get(workflow)
        .and_then(|steps| steps.get(step))
        .unwrap_or_else(|| panic!("missing step_instances mapping for {workflow}/{step}"));
    state
        .tool_call_instances
        .get(&key)
        .cloned()
        .unwrap_or_else(|| panic!("missing instance for {workflow}/{step}"))
}

/// Two steps that declare different capture names run as distinct instances
/// under key v8 and are served from cache on the next run.
///
/// Different output specifications produce different instance keys, so each
/// step records its own instance and output. The steady-state guarantee remains:
/// an unchanged re-run executes nothing.
#[tokio::test]
async fn two_disjoint_declarations_union_into_one_instance_and_the_re_run_executes_nothing() {
    let tc = TestConductor::new();
    write_doc(
        &tc,
        BTreeMap::new(),
        vec![
            echo_step(
                "first",
                &[],
                BTreeMap::from([("alpha".to_string(), stream_spec("alpha", "stdout"))]),
            ),
            echo_step(
                "second",
                &["first"],
                BTreeMap::from([("beta".to_string(), stream_spec("beta", "stdout"))]),
            ),
        ],
    );

    let first = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the first run must complete");
    assert_eq!(
        (first.executed_steps, first.cached_steps, first.failed_steps),
        (2, 0, 0),
        "distinct output specs produce distinct instances, so both steps execute: {first:?}",
    );

    let instances = all_instances(tc.conductor());
    assert_eq!(instances.len(), 2, "each step records its own instance: {instances:?}");
    let first_instance = instance_for_step(tc.conductor(), "default", "first");
    let second_instance = instance_for_step(tc.conductor(), "default", "second");
    assert_ne!(first_instance.outputs["alpha"].hash_nk, None);
    assert_ne!(second_instance.outputs["beta"].hash_nk, None);
    assert_eq!(
        first_instance.outputs["alpha"].hash_nk, second_instance.outputs["beta"].hash_nk,
        "both captures read the same stdout bytes",
    );

    let second = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the second run must complete");
    assert_eq!(
        (second.executed_steps, second.cached_steps, second.failed_steps),
        (0, 2, 0),
        "unchanged steps are cache hits on the next run: {second:?}",
    );
    tc.shutdown().await;
}

/// Tool-declared captures are part of a step's recorded outputs, and a later
/// step with a different output specification runs under its own instance key.
///
/// Under key v8 the producible output specification is keyed, so a step that
/// declares `{alpha, shared}` and a step that declares `{shared}` do not share
/// an instance. Both steps still cache on an unchanged re-run.
#[tokio::test]
async fn a_tool_declared_capture_is_recorded_by_the_executing_step() {
    let tc = TestConductor::new();
    write_doc(
        &tc,
        BTreeMap::from([("shared".to_string(), stream_spec("shared", "stdout"))]),
        vec![
            echo_step(
                "first",
                &[],
                BTreeMap::from([("alpha".to_string(), stream_spec("alpha", "stdout"))]),
            ),
            echo_step(
                "second",
                &["first"],
                BTreeMap::from([("shared".to_string(), stream_spec("shared", "stdout"))]),
            ),
        ],
    );

    let first = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the first run must complete");
    assert_eq!(
        (first.executed_steps, first.cached_steps, first.failed_steps),
        (2, 0, 0),
        "different output specifications produce distinct instances: {first:?}",
    );

    let first_instance = instance_for_step(tc.conductor(), "default", "first");
    let second_instance = instance_for_step(tc.conductor(), "default", "second");
    assert!(first_instance.outputs.contains_key("alpha"));
    assert!(first_instance.outputs.contains_key("shared"));
    assert!(second_instance.outputs.contains_key("shared"));
    assert_eq!(
        first_instance.outputs["shared"].hash_nk, second_instance.outputs["shared"].hash_nk,
        "both executions capture the same stream bytes",
    );

    let second = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the second run must complete");
    assert_eq!(
        (second.executed_steps, second.cached_steps, second.failed_steps),
        (0, 2, 0),
        "unchanged steps remain cache hits: {second:?}",
    );
    tc.shutdown().await;
}

/// A changed capture identity changes the keyed output specification for that
/// step, so only that step re-executes and records the new capture.
///
/// Steps with different output declarations use distinct instances under key v8,
/// so an unchanged sibling remains a cache hit and keeps its recorded output.
#[tokio::test]
async fn a_changed_capture_spec_replaces_only_that_entry() {
    let tc = TestConductor::new();
    write_doc(
        &tc,
        BTreeMap::new(),
        vec![
            echo_step(
                "first",
                &[],
                BTreeMap::from([("alpha".to_string(), stream_spec("alpha", "stdout"))]),
            ),
            echo_step(
                "second",
                &["first"],
                BTreeMap::from([("beta".to_string(), stream_spec("beta", "stdout"))]),
            ),
        ],
    );
    tc.conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the first run must complete");
    let beta_hash = instance_for_step(tc.conductor(), "default", "second").outputs["beta"]
        .hash_nk
        .expect("beta hash");

    write_doc(
        &tc,
        BTreeMap::new(),
        vec![
            echo_step(
                "first",
                &[],
                BTreeMap::from([("alpha".to_string(), stream_spec("alpha", "stderr"))]),
            ),
            echo_step(
                "second",
                &["first"],
                BTreeMap::from([("beta".to_string(), stream_spec("beta", "stdout"))]),
            ),
        ],
    );
    let changed = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("a changed capture declaration must re-execute, not fail");
    assert_eq!(
        (changed.executed_steps, changed.cached_steps, changed.failed_steps),
        (1, 1, 0),
        "only the step whose declared identity changed may re-execute: {changed:?}",
    );

    let first_instance = instance_for_step(tc.conductor(), "default", "first");
    let second_instance = instance_for_step(tc.conductor(), "default", "second");
    assert_eq!(first_instance.outputs["alpha"].capture.as_str(), "stderr");
    assert_eq!(
        second_instance.outputs["beta"].hash_nk,
        Some(beta_hash),
        "the unchanged sibling instance and output remain cached",
    );

    let after = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the run after the change must complete");
    assert_eq!(
        (after.executed_steps, after.cached_steps, after.failed_steps),
        (0, 2, 0),
        "both steps are cache hits after the changed step re-executes: {after:?}",
    );
    tc.shutdown().await;
}

/// Two steps in one run may declare the same output name with different
/// capture identities.
///
/// Under key v8 those declarations produce different output specifications and
/// therefore different instance keys, so each step records its own instance and
/// output without a same-instance capture conflict.
#[tokio::test]
async fn two_steps_disagreeing_on_one_name_fail_the_run_before_a_write() {
    let tc = TestConductor::new();
    write_doc(
        &tc,
        BTreeMap::new(),
        vec![
            echo_step(
                "claims-stdout",
                &[],
                BTreeMap::from([("shared".to_string(), stream_spec("shared", "stdout"))]),
            ),
            echo_step(
                "claims-stderr",
                &["claims-stdout"],
                BTreeMap::from([("shared".to_string(), stream_spec("shared", "stderr"))]),
            ),
        ],
    );

    let summary = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("distinct output specifications are recorded in distinct instances");
    assert_eq!(
        (summary.executed_steps, summary.cached_steps, summary.failed_steps),
        (2, 0, 0),
        "both steps execute with distinct instance keys: {summary:?}",
    );

    let stdout_instance = instance_for_step(tc.conductor(), "default", "claims-stdout");
    let stderr_instance = instance_for_step(tc.conductor(), "default", "claims-stderr");
    assert_eq!(stdout_instance.outputs["shared"].capture.as_str(), "stdout");
    assert_eq!(stderr_instance.outputs["shared"].capture.as_str(), "stderr");
    tc.shutdown().await;
}

/// A changed declaration in one sibling does not disturb the other sibling's
/// cached instance.
///
/// Under key v8 each sibling's output specification is part of its own instance
/// key, so a cache hit for the unchanged sibling cannot claim or rewrite the
/// changed sibling's instance. The run with one changed declaration re-executes
/// only that step, and the next run is fully cached.
#[tokio::test]
async fn a_cache_hit_neither_claims_nor_rewrites_a_siblings_entry() {
    let tc = TestConductor::new();
    let write = |beta_stream: &str| {
        let mut tool = echo_tool("echo@v1");
        tool.runtime.max_concurrent_calls = Some(1);
        tc.write_config(doc_with_workflows(
            BTreeMap::from([("echo@v1".to_string(), tool)]),
            vec![workflow_with_steps(
                "default",
                vec![
                    echo_step(
                        "a-declares-beta",
                        &[],
                        BTreeMap::from([("beta".to_string(), stream_spec("beta", beta_stream))]),
                    ),
                    echo_step(
                        "z-declares-alpha",
                        &[],
                        BTreeMap::from([("alpha".to_string(), stream_spec("alpha", "stdout"))]),
                    ),
                ],
            )],
        ));
    };

    write("stdout");
    let first = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the first run must complete");
    assert_eq!(
        (first.executed_steps, first.cached_steps, first.failed_steps),
        (2, 0, 0),
        "both siblings execute under distinct instance keys: {first:?}",
    );
    let alpha_hash =
        instance_for_step(tc.conductor(), "default", "z-declares-alpha").outputs["alpha"]
            .hash_nk
            .expect("alpha hash");

    let steady = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the second run must complete");
    assert_eq!(
        (steady.executed_steps, steady.cached_steps, steady.failed_steps),
        (0, 2, 0),
        "both siblings are cache hits on a steady-state run: {steady:?}",
    );

    write("stderr");
    let changed = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("a changed declaration is a re-execution, not a conflict");
    assert_eq!(
        (changed.executed_steps, changed.cached_steps, changed.failed_steps),
        (1, 1, 0),
        "only the changed sibling re-executes: {changed:?}",
    );

    let beta_instance = instance_for_step(tc.conductor(), "default", "a-declares-beta");
    let alpha_instance = instance_for_step(tc.conductor(), "default", "z-declares-alpha");
    assert_eq!(beta_instance.outputs["beta"].capture.as_str(), "stderr");
    assert_eq!(alpha_instance.outputs["alpha"].hash_nk, Some(alpha_hash));

    let after = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the run after the replacement must complete");
    assert_eq!(
        (after.executed_steps, after.cached_steps, after.failed_steps),
        (0, 2, 0),
        "the replacement sticks and the unchanged sibling remains cached: {after:?}",
    );
    tc.shutdown().await;
}

/// Builds an `fs@v1` tool that writes one text file per call.
///
/// The artifact has to be a file rather than a process stream: `stdout` and
/// `stderr` are exempt from byte identity by design, so only a file capture can
/// express "the same work produced different bytes".
fn fs_tool(name: &str) -> ToolSpec {
    ToolSpec {
        version: None,
        kind: ToolKindSpec::Builtin { builtin_id: "fs@v1".to_string() },
        name: name.into(),
        inputs: BTreeMap::from([
            ("op".into(), ToolInputSpec { kind: ToolInputKind::String, required: false }),
            ("path".into(), ToolInputSpec { kind: ToolInputKind::String, required: false }),
            ("content".into(), ToolInputSpec { kind: ToolInputKind::String, required: false }),
        ]),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime::default(),
    }
}

/// Builds the one `fs@v1` step these tests run: write `content` to
/// `artifact.txt` and capture that file as `artifact`.
fn fs_step(content: &str) -> WorkflowStepSpec {
    WorkflowStepSpec {
        id: "s1".into(),
        tool: "fs@v1".into(),
        inputs: BTreeMap::from([
            ("op".to_string(), "write_text".to_string()),
            ("path".to_string(), "artifact.txt".to_string()),
            ("content".to_string(), content.to_string()),
        ]),
        outputs: BTreeMap::from([(
            "artifact".to_string(),
            OutputCaptureSpec {
                name: "artifact".to_string(),
                capture: mediapm_conductor::config::CaptureKind::parse_token("file:artifact.txt")
                    .expect("valid capture"),
                ..OutputCaptureSpec::default()
            },
        )]),
        runtime: Some(mediapm_conductor::config::StepRuntime { max_retries: Some(0) }),
        depends_on: Vec::new(),
    }
}

/// Writes a one-workflow document with one `fs@v1` tool, marked `impure` or
/// pure, and one file-capturing step.
///
/// The step's inputs never change between runs here, so its instance key and
/// its produced bytes are both stable: every difference these tests observe
/// comes from the recorded state, not from the step having changed.
fn write_fs_doc(tc: &TestConductor, impure: bool) {
    let mut tool = fs_tool("fs@v1");
    tool.runtime.impure = impure;
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("fs@v1".to_string(), tool)]),
        vec![workflow_with_steps("default", vec![fs_step("payload")])],
    ));
}

/// Returns the hash the single instance records for `artifact`.
fn recorded_artifact(conductor: &Conductor<InMemoryCas>) -> Hash {
    only_instance(conductor).outputs["artifact"].hash_nk.expect("artifact hash")
}

/// Rewrites the recorded `artifact` hash to a blob this run did not produce,
/// and returns the seeded hash.
///
/// This is the state a forced re-execution meets whenever the recorded artifact
/// is stale: an impure tool whose output moved (a fetch, a render, a service
/// lookup), or a store that lost the blob while the record survived. Either way
/// the stored hash disagrees with what the next execution produces, and the
/// settlement has to decide whether that is divergence or the expected outcome.
async fn seed_stale_recorded_artifact(tc: &TestConductor) -> Hash {
    let seeded = tc
        .conductor()
        .cas()
        .put(bytes::Bytes::from_static(b"an artifact this work did not produce"))
        .await
        .expect("the seed blob must be storable");
    let mut state = tc.conductor().get_state().expect("the state must be readable");
    state
        .tool_call_instances
        .values_mut()
        .next()
        .expect("the first run recorded one instance")
        .outputs
        .get_mut("artifact")
        .expect("the step declared `artifact`")
        .hash_nk = Some(seeded);
    tc.conductor().replace_resolved_state(state).expect("the state must be replaceable");
    seeded
}

/// An impure step that re-executes replaces its recorded artifact and the run
/// succeeds.
///
/// The user-level guarantee: `force_rerun` (and every other path that forces a
/// re-execution) is usable on an impure tool. Its bytes are unreproducible by
/// definition, so a stale record must be overwritten, not treated as the tool
/// contradicting itself. Before the impurity boundary existed this run failed
/// with the byte-divergence error and there was no in-band repair: the retry
/// bypassed the probe again and failed identically.
#[tokio::test]
async fn an_impure_step_replaces_a_stale_recorded_artifact_on_a_forced_rerun() {
    let tc = TestConductor::new();
    write_fs_doc(&tc, true);

    let first = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the first run must complete");
    assert_eq!(
        (first.executed_steps, first.cached_steps, first.failed_steps),
        (1, 0, 0),
        "the first run has nothing to reuse: {first:?}",
    );
    let produced = recorded_artifact(tc.conductor());

    let seeded = seed_stale_recorded_artifact(&tc).await;
    assert_ne!(seeded, produced, "the seeded record must disagree, or nothing is being tested");

    let second = tc
        .conductor()
        .run_workflow(
            "default",
            RunWorkflowOptions { force_rerun: true, ..RunWorkflowOptions::default() },
        )
        .await
        .expect("an impure step's forced re-run must not fail on its own staleness");
    assert_eq!(
        (second.executed_steps, second.cached_steps, second.failed_steps),
        (1, 0, 0),
        "a forced re-run executes the step: {second:?}",
    );
    assert_eq!(
        recorded_artifact(tc.conductor()),
        produced,
        "the newest execution's bytes must be the record, replacing the stale hash",
    );
    tc.shutdown().await;
}

/// A pure step whose re-execution produces a different artifact fails the run,
/// naming the step.
///
/// The same setup as the impure case, with impurity off: identical work must
/// produce identical artifacts, so a disagreement is either a non-deterministic
/// tool that misdeclared itself or two executions collapsed under one key.
/// Silently replacing the record would hide both. The message has to name the
/// step because a shared instance can be settled by any of several.
#[tokio::test]
async fn a_pure_step_producing_a_different_artifact_fails_the_run_naming_the_step() {
    let tc = TestConductor::new();
    write_fs_doc(&tc, false);

    tc.conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the first run must complete");
    let seeded = seed_stale_recorded_artifact(&tc).await;

    let error = tc
        .conductor()
        .run_workflow(
            "default",
            RunWorkflowOptions { force_rerun: true, ..RunWorkflowOptions::default() },
        )
        .await
        .expect_err("a pure tool cannot produce two artifacts for one input set");
    let message = error.to_string();

    assert!(message.contains("step 's1'"), "the message must name the step: {message}");
    assert!(message.contains("'artifact'"), "the message must name the output: {message}");
    assert!(
        message.contains(&seeded.to_string()),
        "the message must name the recorded hash: {message}",
    );
    tc.shutdown().await;
}

/// Two same-work siblings dispatched in one parallel cohort execute separately
/// under key v8 when they declare different output specifications.
///
/// Different output specs produce different instance keys. The steady-state
/// guarantee remains: the next run serves both steps from cache.
#[tokio::test]
async fn two_same_work_siblings_in_one_cohort_both_execute_and_then_share_the_instance() {
    let tc = TestConductor::new();
    write_doc(
        &tc,
        BTreeMap::new(),
        vec![
            echo_step(
                "first",
                &[],
                BTreeMap::from([("alpha".to_string(), stream_spec("alpha", "stdout"))]),
            ),
            echo_step(
                "second",
                &[],
                BTreeMap::from([("beta".to_string(), stream_spec("beta", "stdout"))]),
            ),
        ],
    );

    let first = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the first run must complete");
    assert_eq!(
        (first.executed_steps, first.cached_steps, first.failed_steps),
        (2, 0, 0),
        "distinct output specifications produce distinct instances: {first:?}",
    );
    assert_eq!(all_instances(tc.conductor()).len(), 2);

    let second = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the second run must complete");
    assert_eq!(
        (second.executed_steps, second.cached_steps, second.failed_steps),
        (0, 2, 0),
        "both instances are cache hits on the next run: {second:?}",
    );
    tc.shutdown().await;
}
