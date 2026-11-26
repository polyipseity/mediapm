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
    test.shutdown().await;
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
    test.shutdown().await;
}

#[tokio::test]
async fn default_runtime_tmp_is_removed_after_a_failed_workflow() {
    use std::path::Path;
    use std::time::Duration;

    use mediapm_cas::Hash;
    use mediapm_conductor::api::RuntimeStoragePaths;
    use mediapm_conductor::{
        Conductor, ConductorState, RunWorkflowOptions, ToolCallInstance, encode_state_json,
    };
    use mediapm_utils::Timestamp;

    /// Reads the GC epoch a background pass published to the state file.
    fn gc_epoch(state_file: &Path) -> u64 {
        let bytes = std::fs::read(state_file).expect("read the conductor state file");
        let state: ConductorState =
            serde_json::from_slice(&bytes).expect("decode the conductor state file");
        state.conductor_gc_ran_at.as_unix_nanos()
    }

    /// Seeds one recorded instance so every pass classifies and advances the
    /// GC epoch: an unseeded document makes the pass abstain without one.
    fn write_seeded_state(paths: &RuntimeStoragePaths) {
        let key = Hash::from_content(b"the background pass needs one live seed");
        let instance = ToolCallInstance {
            key_version: 8,
            attempted_key_version_nk: 8,
            tool_call_id: "seed@v1".to_string(),
            command_args: Vec::new(),
            env_vars_ck: BTreeMap::new(),
            materialized_inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            impure_nk: false,
            invalidated_at_nk: None,
            executed_at_nk: Timestamp::default(),
            last_referenced_at_nk: Timestamp::default(),
            step_ids_nk: std::collections::BTreeSet::new(),
        };
        let mut state = ConductorState::new_empty();
        state
            .step_instances
            .insert("default".to_string(), BTreeMap::from([("s1".to_string(), key)]));
        state.tool_call_instances.insert(key, instance);
        let bytes = encode_state_json(&state).expect("encode the seeded state");
        std::fs::write(&paths.state_file_path, bytes).expect("write the seeded state file");
    }

    // The loop waits this long between passes, so an unstopped conductor
    // advances the published epoch during the post-shutdown wait below.
    let interval_secs = 1;
    let quiescent_wait = Duration::from_secs(3);

    let dir = mediapm_utils::temp::output_dir().expect("artifact dir");
    crate::write_conductor_config(
        dir.path(),
        crate::doc_with_workflows(
            BTreeMap::new(),
            vec![crate::workflow_with_steps(
                "default",
                vec![crate::bare_step("s1", "no-such-tool", &[])],
            )],
        ),
    );
    let paths = RuntimeStoragePaths::new(dir.path());
    let state_file = paths.state_file_path.clone();
    write_seeded_state(&paths);
    let conductor = Conductor::new(paths, mediapm_cas::InMemoryCas::new())
        .with_gc_settings(86_400, interval_secs);

    let error = conductor
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect_err("an unknown tool fails the workflow");
    assert!(error.to_string().contains("no-such-tool"), "error names the tool: {error}");

    // A pass starts with the first workflow run and publishes its epoch to
    // the state file; wait for that write before testing what stops it.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if state_file.exists() {
            let epoch = gc_epoch(&state_file);
            if epoch > 0 {
                break;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the {interval_secs}s background pass must publish a GC epoch"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    conductor.shutdown().await.expect("conductor shutdown");
    assert!(
        !dir.path().join("tmp").exists(),
        "the default runtime tmp root must be gone before its owner drops: {}",
        dir.path().display(),
    );

    let settled = gc_epoch(&state_file);
    tokio::time::sleep(quiescent_wait).await;
    assert_eq!(
        gc_epoch(&state_file),
        settled,
        "shutdown must stop the {interval_secs}s background pass; the GC epoch advanced"
    );
}
