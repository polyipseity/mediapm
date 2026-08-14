//! Runtime tmp dir lifecycle: sandbox roots must be removed on both normal
//! workflow exit and failed workflows (unknown tool in a later level).
//!
//! The runtime tmp root is `mediapm_utils::temp::runtime_dir_for_workspace`
//! (`$TMPDIR/mediapm-runtime-{16hex}`); the conductor sandboxes step trees
//! under it (`{root}/sandbox/{instance_key}`). A leaked root would survive the
//! run and force janitor cleanup later.

use std::collections::BTreeMap;

use mediapm_cas::InMemoryCas;
use mediapm_conductor::{RuntimeStoragePaths, SimpleConductor};

use crate::{
    bare_step, doc_with_workflows, echo_step, echo_tool, echo_workflow, workflow_with_steps,
    write_conductor_config,
};

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

#[tokio::test]
async fn runtime_tmp_removed_on_normal_workflow_exit() {
    let workspace = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let (conductor, runtime_tmp) = conductor_for(workspace.path());

    write_conductor_config(
        workspace.path(),
        doc_with_workflows(
            BTreeMap::from([("echo@v1".into(), echo_tool("echo@v1"))]),
            vec![echo_workflow("default", "echo@v1", "hello")],
        ),
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
    write_conductor_config(
        workspace.path(),
        doc_with_workflows(
            BTreeMap::from([("echo@v1".into(), echo_tool("echo@v1"))]),
            vec![workflow_with_steps(
                "default",
                vec![echo_step("echo@v1", "hello"), bare_step("s2", "no-such-tool", &["s1"])],
            )],
        ),
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
