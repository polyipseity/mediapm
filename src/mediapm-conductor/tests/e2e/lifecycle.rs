//! Comprehensive workflow-lifecycle test covering cache, GC, and tool-update behavior.

use crate::{TestConductor, dual_echo_doc, single_echo_doc};
use mediapm_conductor::api::RunWorkflowOptions;

#[tokio::test]
async fn workflow_lifecycle_cache_gc_tool_update() {
    let tc = TestConductor::new();
    tc.write_config(single_echo_doc("echo@v1", "default"));

    // ---- Phase 1: first execution ----
    let s = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("phase 1");
    assert_eq!(s.executed_steps, 1, "phase 1: first run executes");
    assert_eq!(s.cached_steps, 0, "phase 1: no cache on first run");

    // ---- Phase 2: cache hit ----
    let s = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("phase 2");
    assert_eq!(s.executed_steps, 0, "phase 2: cached on second run");
    assert_eq!(s.cached_steps, 1, "phase 2: cache hit on second run");

    // ---- Phase 3: GC preserves referenced instance ----
    tc.conductor().run_gc().await.expect("phase 3: run_gc succeeds");
    let state = tc.conductor().get_state().expect("phase 3: get_state");
    assert_eq!(state.tool_call_instances.len(), 1, "phase 3: instance survives GC");

    // ---- Phase 4: cache hit post-GC ----
    let s = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("phase 4");
    assert_eq!(s.executed_steps, 0, "phase 4: cached post-GC");
    assert_eq!(s.cached_steps, 1, "phase 4: cache hit post-GC");

    // ---- Phase 5: new tool_id + new workflow ----
    tc.write_config(dual_echo_doc());

    // "default" in dual doc targets echo-v1@v1 — cache hit from old run
    let s = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("phase 5a");
    assert_eq!(s.executed_steps, 0, "phase 5a: old tool cached");
    assert_eq!(s.cached_steps, 1, "phase 5a: old tool cached");

    let s = tc
        .conductor()
        .run_workflow("updated", RunWorkflowOptions::default())
        .await
        .expect("phase 5b");
    assert_eq!(s.executed_steps, 1, "phase 5b: new tool executes");
    assert_eq!(s.cached_steps, 0, "phase 5b: new tool fresh");

    // ---- Phase 6: both workflows cached ----
    let s = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("phase 6a");
    assert_eq!(s.executed_steps, 0, "phase 6a: default cached");
    assert_eq!(s.cached_steps, 1, "phase 6a: default cached");

    let s = tc
        .conductor()
        .run_workflow("updated", RunWorkflowOptions::default())
        .await
        .expect("phase 6b");
    assert_eq!(s.executed_steps, 0, "phase 6b: updated cached");
    assert_eq!(s.cached_steps, 1, "phase 6b: updated cached");

    // ---- Phase 7: GC preserves both instances ----
    tc.conductor().run_gc().await.expect("phase 7: run_gc succeeds");
    let state = tc.conductor().get_state().expect("phase 7: get_state");
    assert_eq!(state.tool_call_instances.len(), 2, "phase 7: both instances survive GC");

    // ---- Phase 8: both cached post-GC ----
    let s = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("phase 8a");
    assert_eq!(s.executed_steps, 0, "phase 8a: default cached post-GC");
    assert_eq!(s.cached_steps, 1, "phase 8a: default cached post-GC");

    let s = tc
        .conductor()
        .run_workflow("updated", RunWorkflowOptions::default())
        .await
        .expect("phase 8b");
    assert_eq!(s.executed_steps, 0, "phase 8b: updated cached post-GC");
    assert_eq!(s.cached_steps, 1, "phase 8b: updated cached post-GC");
}
