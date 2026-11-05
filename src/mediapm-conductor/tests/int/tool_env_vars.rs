//! A tool's `env_vars` reach the process its step runs.
//!
//! `tools.<tool>.env_vars` is the surface a document states an environment
//! value on, and the value has to arrive in the child's environment rather than
//! stop at the configuration boundary. The test drives the whole path — a
//! written `conductor.ncl`, a run, a real child process — through the stub tool
//! from `mediapm-test-support`: the tool's command is a copy of the stub named
//! `yt-dlp`, whose emit mode reads the manifest path from
//! `MEDIAPM_STUB_MANIFEST`, the variable the document states. A literal that
//! never arrives leaves the stub with nothing to read, so it answers its own
//! usage failure and writes no destination.
//!
//! The document is the only source of that name here: the manifest path is the
//! copy's own path under this test's tempdir, and nothing installs the variable
//! in the test process.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::{EnvVarGuard, TestConductor, doc_with_workflows, rpc_env_lock, workflow_with_steps};
use mediapm_conductor::api::RunWorkflowOptions;
use mediapm_conductor::{ToolKindSpec, ToolRuntime, ToolSpec, WorkflowStepSpec};
use mediapm_test_support::{INVOCATION_RECORD_ENV, MANIFEST_ENV, stub_tool_path};

/// Copies the stub into `dir` under `name` and returns the copy's path.
///
/// A stub invocation with no `--mode` dispatches on the file stem of its own
/// `argv[0]`, so a copy named `yt-dlp` reads the manifest its environment names
/// and writes that manifest's entries. The executable bits travel with the copy,
/// which is what lets the step spawn it. The stub's own path would dispatch as
/// `mediapm-stub-tool` and answer the missing-mode usage failure, so the copy is
/// the whole point rather than a convenience.
///
/// # Panics
///
/// Panics when the copy cannot be written.
pub(super) fn named_stub_copy(dir: &Path, name: &str) -> PathBuf {
    let copy = dir.join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
    std::fs::copy(stub_tool_path(), &copy)
        .unwrap_or_else(|error| panic!("cannot copy the stub to '{}': {error}", copy.display()));
    copy
}

/// A step's process gets the environment the document's tool spec states.
///
/// The proof is a real child process writing a file no other surface names: the
/// stub emits the destinations of the manifest `MEDIAPM_STUB_MANIFEST` points
/// at, and the only place that variable is stated is the tool's `env_vars`. The
/// step's summary and the emitted bytes are asserted together so a run that
/// fails and one that writes the wrong bytes are distinguishable, and the
/// failure message names the variable a missing literal leaves unset.
#[tokio::test]
async fn a_tools_env_vars_reach_the_process_its_step_runs() {
    // The child inherits the conductor's environment, so an ambient value would
    // make this test pass with the fold reverted. Fail loudly instead, because
    // the proof is that the document is the only source of the name.
    assert!(
        std::env::var(MANIFEST_ENV).is_err(),
        "{MANIFEST_ENV} is set in this test process, so the child would inherit it and the \
         document's literal would prove nothing; unset it and re-run"
    );

    let tc = TestConductor::new();
    let executable = named_stub_copy(tc.path(), "yt-dlp");
    let manifest = tc.path().join("emit.manifest");
    let source = tc.path().join("manifest-source.txt");
    let destination = tc.path().join("emitted.txt");
    std::fs::write(&source, b"written by the stub").expect("write the manifest source");
    std::fs::write(&manifest, format!("{}\t{}\n", destination.display(), source.display()))
        .expect("write the emit manifest");

    let emitter = ToolSpec {
        version: None,
        kind: ToolKindSpec::Executable {
            command: vec![executable.to_string_lossy().into_owned()],
            env_vars: BTreeMap::from([(MANIFEST_ENV.to_string(), manifest.display().to_string())]),
            success_codes: vec![0],
        },
        name: "yt-dlp".into(),
        inputs: BTreeMap::new(),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime::default(),
    };
    let step = WorkflowStepSpec {
        id: "s1".into(),
        tool: "yt-dlp".into(),
        inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        max_retries: 0,
        depends_on: Vec::new(),
    };
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("yt-dlp".to_string(), emitter)]),
        vec![workflow_with_steps("default", vec![step])],
    ));

    let summary = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("a run reports a summary");

    assert_eq!(
        (summary.failed_steps, std::fs::read(&destination).ok()),
        (0, Some(b"written by the stub".to_vec())),
        "the stub writes '{}' only when {MANIFEST_ENV} names the manifest, and the document states \
         that name in the tool's env_vars; a failed step that wrote nothing is the stub's usage \
         error naming the unset variable, so the document's literal never reached the process",
        destination.display(),
    );
    tc.shutdown().await;
}

/// A command part's `${env.…}` reference resolves by the name's provenance.
///
/// The three provenances must behave differently at run time, and the child's
/// own argv is the proof: the stub appends every invocation to the record
/// `INVOCATION_RECORD_ENV` names, so what the process received is read rather
/// than inferred from the step's exit.
///
/// - A name the tool fixes with a non-empty `env_vars` literal arrives as that
///   value (`--tag=v1`).
/// - A name declared empty is inherited from the host, arrives as the host's
///   value, and never moves the instance key: the second run holds a different
///   host value and must still be the same work, so no second spawn may reach
///   the record.
/// - A name no surface defines fails resolution before spawn; its argv never
///   appears. The error text stays the template engine's own — this work does
///   not touch `resolve_env`.
#[expect(
    clippy::too_many_lines,
    reason = "the three provenance scenarios share one conductor config and one process-env lock, and the middle scenario's claim is that a changed host value still derives the key the first scenario recorded, so the phases must run in order against that single state rather than in three tests that would each rebuild it"
)]
#[tokio::test]
async fn a_command_part_env_reference_resolves_by_its_provenance() {
    // The record env var and the two test names are process-wide; the lock
    // serialises them against sibling tests that read the environment.
    let _lock = rpc_env_lock().await;

    let tc = TestConductor::new();
    let executable = named_stub_copy(tc.path(), "arg-probe");
    let record = tc.path().join("invocations.log");
    let invocation_record =
        BTreeMap::from([(INVOCATION_RECORD_ENV.to_string(), record.display().to_string())]);
    let runner = [
        executable.to_string_lossy().into_owned(),
        "--mode".to_string(),
        "sleep".to_string(),
        "--seconds".to_string(),
        "0".to_string(),
    ];

    let mut literal_env = invocation_record.clone();
    literal_env.insert("TAG".to_string(), "v1".to_string());
    let mut inherited_env = invocation_record.clone();
    inherited_env.insert("MEDIAPM_T3_TEST_HOST_TAG".to_string(), String::new());

    let tool = |name: &str, env_vars: BTreeMap<String, String>, tag_part: String| {
        let mut command = runner.to_vec();
        command.push(tag_part);
        ToolSpec {
            version: None,
            kind: ToolKindSpec::Executable { command, env_vars, success_codes: vec![0] },
            name: name.to_string(),
            inputs: BTreeMap::new(),
            default_inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            runtime: ToolRuntime::default(),
        }
    };
    let step = |id: &str, tool: &str| WorkflowStepSpec {
        id: id.to_string(),
        tool: tool.to_string(),
        inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        max_retries: 0,
        depends_on: Vec::new(),
    };

    tc.write_config(doc_with_workflows(
        BTreeMap::from([
            (
                "t-literal".to_string(),
                tool("t-literal", literal_env, "--tag=${env.TAG}".to_string()),
            ),
            (
                "t-inherited".to_string(),
                tool(
                    "t-inherited",
                    inherited_env,
                    "--tag=${env.MEDIAPM_T3_TEST_HOST_TAG}".to_string(),
                ),
            ),
            (
                "t-absent".to_string(),
                tool(
                    "t-absent",
                    invocation_record,
                    "--tag=${env.MEDIAPM_T3_ABSENT_SENTINEL}".to_string(),
                ),
            ),
        ]),
        vec![
            workflow_with_steps("w-literal", vec![step("s-literal", "t-literal")]),
            workflow_with_steps("w-inherited", vec![step("s-inherited", "t-inherited")]),
            workflow_with_steps("w-absent", vec![step("s-absent", "t-absent")]),
        ],
    ));

    let recorded = || std::fs::read_to_string(&record).unwrap_or_default();

    // Case 1: the document's literal arrives in the child's argv.
    let literal = tc
        .conductor()
        .run_workflow("w-literal", RunWorkflowOptions::default())
        .await
        .expect("the literal case runs");
    assert_eq!(
        (literal.failed_steps, recorded().lines().any(|line| line.contains("--tag=v1"))),
        (0, true),
        "the tool fixes TAG = v1, so the spawned argv must carry --tag=v1; a failed step or a \
         record without the argument means ${{env.TAG}} never resolved: {}",
        recorded(),
    );

    // Case 2: an empty declaration is inherited from the host, and its value
    // is recorded without ever becoming key material.
    let first_host_value = EnvVarGuard::set("MEDIAPM_T3_TEST_HOST_TAG", "host-one");
    let inherited = tc
        .conductor()
        .run_workflow("w-inherited", RunWorkflowOptions::default())
        .await
        .expect("the inherited case runs");
    assert_eq!(
        (inherited.failed_steps, recorded().lines().any(|line| line.contains("--tag=host-one")),),
        (0, true),
        "the host's value reaches the child exactly as the host holds it: {}",
        recorded(),
    );
    drop(first_host_value);

    let second_host_value = EnvVarGuard::set("MEDIAPM_T3_TEST_HOST_TAG", "host-two");
    let rerun = tc
        .conductor()
        .run_workflow("w-inherited", RunWorkflowOptions::default())
        .await
        .expect("the rerun reports a summary");
    assert_eq!(
        (rerun.cached_steps, recorded().lines().any(|line| line.contains("host-two")),),
        (1, false),
        "a changed host value is still the same work: the inherited reference contributes no \
         key material, so the step must be a cache hit with no second spawn (cached={}, record: \
         {})",
        rerun.cached_steps,
        recorded(),
    );
    drop(second_host_value);

    // Case 3: a name no surface defines fails before the child is spawned.
    let absent = tc
        .conductor()
        .run_workflow("w-absent", RunWorkflowOptions::default())
        .await
        .expect("the run reports a summary even for a failed step");
    assert_eq!(
        (
            absent.failed_steps,
            recorded().lines().any(|line| line.contains("MEDIAPM_T3_ABSENT_SENTINEL")),
        ),
        (1, false),
        "an undefined name must fail the step before spawn (the engine's own 'not found' \
         error), and no argv may carry the unresolved reference: {}",
        recorded(),
    );

    tc.shutdown().await;
}
