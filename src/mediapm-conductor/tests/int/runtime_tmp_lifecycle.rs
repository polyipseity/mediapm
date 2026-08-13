//! Runtime tmp dir lifecycle: sandbox roots must be removed on both normal
//! workflow exit and failed workflows (unknown tool in a later level).
//!
//! The runtime tmp root is `mediapm_utils::temp::runtime_dir_for_workspace`
//! (`$TMPDIR/mediapm-runtime-{16hex}`); the conductor sandboxes step trees
//! under it (`{root}/sandbox/{instance_key}`). A leaked root would survive the
//! run and force janitor cleanup later.

use std::collections::BTreeMap;

use mediapm_cas::InMemoryCas;
use mediapm_conductor::{
    NickelDocument, RuntimeStoragePaths, SimpleConductor, ToolInputKind, ToolInputSpec,
    ToolKindSpec, ToolRuntime, ToolSpec, WorkflowSpec, WorkflowStepSpec,
    config::versions::encode_document,
};

fn echo_tool() -> ToolSpec {
    ToolSpec {
        kind: ToolKindSpec::Builtin { builtin_id: "echo@v1".to_string() },
        name: "echo@v1".into(),
        inputs: BTreeMap::from([(
            "text".into(),
            ToolInputSpec { kind: ToolInputKind::String, required: false },
        )]),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime::default(),
    }
}

/// Builds a conductor whose runtime tmp root is the stable
/// `mediapm-runtime-{16hex}` path for `workspace` (mirrors the mediapm app
/// layer wiring in `paths.rs`).
fn conductor_for(
    workspace: &std::path::Path,
) -> (SimpleConductor<InMemoryCas>, std::path::PathBuf) {
    let runtime_tmp = mediapm_utils::temp::runtime_dir_for_workspace(workspace);
    let mut paths = RuntimeStoragePaths::new(workspace);
    paths.conductor_tmp_dir = runtime_tmp.clone();
    (SimpleConductor::new(paths, InMemoryCas::new()), runtime_tmp)
}

fn write_config(workspace: &std::path::Path, doc: NickelDocument) {
    std::fs::write(workspace.join("conductor.ncl"), encode_document(doc).expect("encode"))
        .expect("write config");
}

#[tokio::test]
async fn runtime_tmp_removed_on_normal_workflow_exit() {
    let workspace = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let (conductor, runtime_tmp) = conductor_for(workspace.path());

    write_config(
        workspace.path(),
        NickelDocument {
            tools: BTreeMap::from([("echo@v1".into(), echo_tool())]),
            workflows: vec![WorkflowSpec {
                name: "default".into(),
                display_name: None,
                description: None,
                impure: false,
                steps: vec![WorkflowStepSpec {
                    id: "s1".into(),
                    tool: "echo@v1".into(),
                    inputs: BTreeMap::from([("text".into(), "hello".into())]),
                    outputs: BTreeMap::new(),
                    max_retries: 0,
                    depends_on: Vec::new(),
                }],
            }],
            ..NickelDocument::default()
        },
    );

    let summary =
        conductor.run_workflow("default", Default::default()).await.expect("workflow runs");
    assert_eq!(summary.executed_steps, 1, "echo step executes");

    assert!(
        !runtime_tmp.exists(),
        "runtime tmp root removed on normal exit: {}",
        runtime_tmp.display()
    );
}

#[tokio::test]
async fn runtime_tmp_removed_on_failed_workflow_unknown_tool() {
    let workspace = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let (conductor, runtime_tmp) = conductor_for(workspace.path());

    // Level 0 executes a valid echo step (creating the sandbox tree under the
    // runtime tmp root); level 1 references an unknown tool, which fails the
    // workflow after the level-0 sandbox already exists.
    write_config(
        workspace.path(),
        NickelDocument {
            tools: BTreeMap::from([("echo@v1".into(), echo_tool())]),
            workflows: vec![WorkflowSpec {
                name: "default".into(),
                display_name: None,
                description: None,
                impure: false,
                steps: vec![
                    WorkflowStepSpec {
                        id: "s1".into(),
                        tool: "echo@v1".into(),
                        inputs: BTreeMap::from([("text".into(), "hello".into())]),
                        outputs: BTreeMap::new(),
                        max_retries: 0,
                        depends_on: Vec::new(),
                    },
                    WorkflowStepSpec {
                        id: "s2".into(),
                        tool: "no-such-tool".into(),
                        inputs: BTreeMap::new(),
                        outputs: BTreeMap::new(),
                        max_retries: 0,
                        depends_on: vec!["s1".into()],
                    },
                ],
            }],
            ..NickelDocument::default()
        },
    );

    let error = conductor
        .run_workflow("default", Default::default())
        .await
        .expect_err("unknown tool fails the workflow");
    assert!(error.to_string().contains("no-such-tool"), "error names the unknown tool: {error}");

    assert!(
        !runtime_tmp.exists(),
        "runtime tmp root removed on failed workflow: {}",
        runtime_tmp.display()
    );
}
