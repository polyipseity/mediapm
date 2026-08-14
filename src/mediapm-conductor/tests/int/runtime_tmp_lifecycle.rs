//! Runtime tmp dir lifecycle: sandbox roots must be removed on both normal
//! workflow exit and failed workflows (unknown tool in a later level).
//!
//! The runtime tmp root is `mediapm_utils::temp::runtime_dir_for_workspace`
//! (`$TMPDIR/mediapm-runtime-{16hex}`); the conductor sandboxes step trees
//! under it (`{root}/sandbox/{instance_key}`). A leaked root would survive the
//! run and force janitor cleanup later.

use std::collections::BTreeMap;

use mediapm_conductor::RunWorkflowOptions;

use crate::{
    TestConductor, bare_step, doc_with_workflows, echo_step, echo_tool, echo_workflow,
    workflow_with_steps,
};

#[tokio::test]
async fn runtime_tmp_removed_on_normal_workflow_exit() {
    let (test, runtime_tmp) = TestConductor::with_runtime_tmp();

    test.write_config(doc_with_workflows(
        BTreeMap::from([("echo@v1".into(), echo_tool("echo@v1"))]),
        vec![echo_workflow("default", "echo@v1", "hello")],
    ));

    let summary = test
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("workflow runs");
    assert_eq!(summary.executed_steps, 1, "echo step executes");

    assert!(
        !runtime_tmp.exists(),
        "runtime tmp root removed on normal exit: {}",
        runtime_tmp.display()
    );
}

#[tokio::test]
async fn runtime_tmp_removed_on_failed_workflow_unknown_tool() {
    let (test, runtime_tmp) = TestConductor::with_runtime_tmp();

    // Level 0 executes a valid echo step (creating the sandbox tree under the
    // runtime tmp root); level 1 references an unknown tool, which fails the
    // workflow after the level-0 sandbox already exists.
    test.write_config(doc_with_workflows(
        BTreeMap::from([("echo@v1".into(), echo_tool("echo@v1"))]),
        vec![workflow_with_steps(
            "default",
            vec![echo_step("echo@v1", "hello"), bare_step("s2", "no-such-tool", &["s1"])],
        )],
    ));

    let error = test
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect_err("unknown tool fails the workflow");
    assert!(error.to_string().contains("no-such-tool"), "error names the unknown tool: {error}");

    assert!(
        !runtime_tmp.exists(),
        "runtime tmp root removed on failed workflow: {}",
        runtime_tmp.display()
    );
}
