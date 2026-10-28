//! # Name-addressed captures union into one instance, and disagreeing steps fail
//!
//! Instance keys derive from inputs alone, so two steps that run the same work
//! share one key however differently they declare what they record. What makes
//! that sharing useful is that the recorded instance grows: each settlement
//! merges its captures into the entries the instance already holds instead of
//! replacing them. Without the union, two steps declaring different names would
//! take turns overwriting each other's record, and neither would ever be served
//! from cache.
//!
//! The union is only honest under two rules, and both are pinned here: one name
//! may not hold two different byte streams for the same work, and two steps in
//! one run may not claim one name with different capture identities. The second
//! is a configuration error reported before the run writes anything, not a
//! last-writer-wins race.

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
        capture: stream.to_string(),
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
        max_retries: 0,
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
    assert_eq!(
        state.tool_call_instances.len(),
        1,
        "identical work must be recorded as one instance, not one per step",
    );
    state.tool_call_instances.values().next().cloned().expect("the one instance")
}

/// Two steps that run the same work and declare different capture names end
/// with one instance holding both names, and the run after executes nothing.
///
/// The second step depends on the first, so the two settlements are ordered:
/// the first records `alpha`, then the second executes (it requires `beta`,
/// which the first never captured) and records `beta`. Replacing the record
/// rather than unioning into it would leave the instance holding whichever name
/// settled last, and the next run would re-execute the other one — a pair that
/// alternates one execution per sync forever.
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
        "neither step can reuse a name the other never captured, so both run once: {first:?}",
    );

    let instance = only_instance(tc.conductor());
    for name in ["alpha", "beta"] {
        assert!(
            instance.outputs.contains_key(name),
            "the shared instance must record '{name}', got {:?}",
            instance.outputs.keys().collect::<Vec<_>>(),
        );
    }
    assert_eq!(
        instance.outputs["alpha"].hash, instance.outputs["beta"].hash,
        "both captures read the same stdout, so they must name the same bytes",
    );
    assert_eq!(
        instance.step_ids.iter().map(|step| step.step_id.as_str()).collect::<Vec<_>>(),
        vec!["first", "second"],
        "provenance must name every step the instance served",
    );

    let second = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the second run must complete");
    assert_eq!(
        (second.executed_steps, second.cached_steps, second.failed_steps),
        (0, 2, 0),
        "the unioned instance must serve both steps on the next run: {second:?}",
    );
    tc.shutdown().await;
}

/// A name the tool declares is recorded by whichever step executes the work,
/// so a step declaring that name is served by that execution rather than
/// running the command again.
///
/// The two steps' declarations are disjoint (`alpha` only the first declares,
/// `shared` only the second), and the tool's default set carries `shared` into
/// the first step's capture. The user-level guarantee: an artifact declared by
/// the tool costs one execution however many steps bind it.
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
        (1, 1, 0),
        "the step declaring the tool's own capture must reuse the first execution: {first:?}",
    );

    let instance = only_instance(tc.conductor());
    for name in ["alpha", "shared"] {
        assert!(
            instance.outputs.contains_key(name),
            "the shared instance must record '{name}', got {:?}",
            instance.outputs.keys().collect::<Vec<_>>(),
        );
    }

    let second = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the second run must complete");
    assert_eq!(
        (second.executed_steps, second.cached_steps, second.failed_steps),
        (0, 2, 0),
        "an unchanged re-run must execute nothing once both names are on the instance: {second:?}",
    );
    tc.shutdown().await;
}

/// A changed capture identity under one name re-executes and replaces only that
/// entry, and the run after it is free again.
///
/// The user-level guarantee: re-declaring what a name captures invalidates that
/// name, not the instance and not its other names. The replacement is visible
/// in the recorded entry's spec, which is what a later probe compares; the
/// other step's entry must survive it.
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
    let shared_hash = only_instance(tc.conductor()).outputs["beta"].hash;

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

    let instance = only_instance(tc.conductor());
    assert_eq!(
        instance.outputs["alpha"].capture.as_ref().map(|spec| spec.capture.as_str()),
        Some("stderr"),
        "the recorded entry must carry the capture spec that produced it",
    );
    assert_ne!(
        instance.outputs["alpha"].hash, instance.outputs["beta"].hash,
        "echo writes nothing to stderr, so the replaced entry must hold different bytes",
    );
    assert_eq!(
        instance.outputs["beta"].hash, shared_hash,
        "an unrelated entry must survive the replacement untouched",
    );

    let after = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the run after the replacement must complete");
    assert_eq!(
        (after.executed_steps, after.cached_steps, after.failed_steps),
        (0, 2, 0),
        "the replaced entry must satisfy the probe from then on: {after:?}",
    );
    tc.shutdown().await;
}

/// Two steps in one run that claim one name with different capture identities
/// fail the run, naming the name, both identities and both steps, before
/// anything is written.
///
/// The user-level guarantee: a configuration whose two steps disagree about
/// what one name holds is reported as such. Resolving it by settlement order
/// would make which bytes a dependent reads depend on a race, and it would
/// leave the losing step re-executing against the winner's record forever.
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

    let error = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect_err("two capture identities for one name are a configuration error");
    let message = error.to_string();

    for needle in ["shared", "claims-stdout", "claims-stderr", "stdout", "stderr"] {
        assert!(message.contains(needle), "the error must name {needle:?}: {message}");
    }
    assert!(
        !tc.path().join("state.json").exists(),
        "a run rejected for a capture disagreement must write no state",
    );
    tc.shutdown().await;
}

/// A cache hit claims nothing and writes nothing back, so a sibling's changed
/// declaration replaces its own entry instead of colliding with a name it never
/// declared.
///
/// The two steps are siblings (no dependency edge), and the tool admits one call
/// at a time, so the ready queue's order is the settlement order: the changed
/// step executes and settles first, then the unchanged one dispatches from the
/// run-entry snapshot it holds. Nothing links them, so that snapshot is not
/// refreshed by the sibling's settlement, and the unchanged step's probe still
/// finds the pre-change record and hits.
///
/// A hit produced nothing, so it must not present the stored instance's outputs
/// as its own claims: doing so makes the unchanged step claim its sibling's name
/// under the *old* spec, and the run fails with a conflict between two steps
/// where one never declared the name at all. Nothing is written on that failure,
/// so it repeats on every sync with `--force-rerun` the only escape. Nor may a
/// hit write the stored outputs back — its snapshot carries the pre-change
/// entry, and writing it would undo the sibling's replacement and leave the
/// changed step re-executing on every later sync.
#[tokio::test]
async fn a_cache_hit_neither_claims_nor_rewrites_a_siblings_entry() {
    let tc = TestConductor::new();
    // The one-call cap fixes the settlement order, and the ready queue hands out
    // ready steps by step id: the changed step sorts first, so the unchanged
    // step's hit lands after the replacement it must not disturb.
    let write = |beta_stream: &str| {
        let mut tool = echo_tool("echo@v1");
        tool.runtime.max_concurrent_calls = 1;
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
        "a sibling's snapshot predates the other's settlement, so both run once: {first:?}",
    );
    let recorded = only_instance(tc.conductor());
    let alpha_hash = recorded.outputs["alpha"].hash;
    assert_eq!(
        alpha_hash, recorded.outputs["beta"].hash,
        "both captures read the same stream, so the shared instance holds one set of bytes",
    );

    let steady = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the second run must complete");
    assert_eq!(
        (steady.executed_steps, steady.cached_steps, steady.failed_steps),
        (0, 2, 0),
        "the unioned instance must serve both siblings: {steady:?}",
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
        "only the step whose declaration changed may re-execute: {changed:?}",
    );

    let instance = only_instance(tc.conductor());
    assert_eq!(
        instance.outputs["beta"].capture.as_ref().map(|spec| spec.capture.as_str()),
        Some("stderr"),
        "the changed entry must carry the spec this run declared",
    );
    assert_eq!(
        instance.outputs["alpha"].hash, alpha_hash,
        "the unchanged sibling's entry must survive the replacement untouched",
    );

    let after = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the run after the replacement must complete");
    assert_eq!(
        (after.executed_steps, after.cached_steps, after.failed_steps),
        (0, 2, 0),
        "the replacement must stick, so the next run executes nothing: {after:?}",
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
                capture: "file:artifact.txt".to_string(),
                ..OutputCaptureSpec::default()
            },
        )]),
        max_retries: 0,
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
    only_instance(conductor).outputs["artifact"].hash
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
        .hash = seeded;
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

/// Two same-work siblings dispatched in one parallel cohort both execute.
///
/// This is the limitation the contract records as prose rather than as a
/// defect: each step's probe reads the run-entry snapshot, and neither sees the
/// other's settlement, so the second cannot learn that the first already did the
/// work. The union is what keeps that from becoming an alternating loop — both
/// settlements land on one instance, both names survive, and the run after it
/// executes neither.
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
        "neither sibling can see the other's settlement, so both run once: {first:?}",
    );

    let instance = only_instance(tc.conductor());
    assert_eq!(
        instance.step_ids,
        std::collections::BTreeSet::from([
            mediapm_conductor::StepRef {
                workflow_id: "default".to_string(),
                step_id: "first".to_string(),
            },
            mediapm_conductor::StepRef {
                workflow_id: "default".to_string(),
                step_id: "second".to_string(),
            },
        ]),
        "one instance must name both siblings it served, each qualified by its workflow",
    );

    let second = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the second run must complete");
    assert_eq!(
        (second.executed_steps, second.cached_steps, second.failed_steps),
        (0, 2, 0),
        "the unioned instance must serve both siblings from then on: {second:?}",
    );
    tc.shutdown().await;
}
