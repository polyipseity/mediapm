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

use crate::{TestConductor, doc_with_workflows, workflow_with_steps};
use mediapm_conductor::api::RunWorkflowOptions;
use mediapm_conductor::{ToolKindSpec, ToolRuntime, ToolSpec, WorkflowStepSpec};
use mediapm_test_support::{MANIFEST_ENV, stub_tool_path};

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
