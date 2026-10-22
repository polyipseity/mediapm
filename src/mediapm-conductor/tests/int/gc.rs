//! Integration coverage for conductor GC.
//!
//! The first block validates the state-level transition
//! (`ConductorState::run_conductor_gc`), which applies a reachable set it is
//! given:
//! - instances outside the reachable set are evicted past TTL
//! - instances inside it survive and get their clock refreshed
//! - empty state is a no-op
//! - an empty reachable set evicts everything, which is why no production
//!   caller may hand the sweep one that is merely assumed empty
//!
//! The second block covers the seed a run records, which is what the reachable
//! set is computed from: a successful run replaces its workflow's seed with the
//! instances it executed or reused, and a run with a failed step leaves the
//! previous seed alone.

use std::collections::{BTreeMap, BTreeSet};

use mediapm_cas::{Hash, InMemoryCas};
use mediapm_conductor::api::RunWorkflowOptions;
use mediapm_conductor::{AuxData, Conductor, ConductorState, InstanceAux, ToolCallInstance};
use mediapm_utils::Timestamp;

use crate::{TestConductor, doc_with_workflows, echo_step, echo_tool, workflow_with_steps};

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

/// Creates a minimal `ToolCallInstance` with the given content-addressed key.
fn sample_instance(key: Hash) -> ToolCallInstance {
    ToolCallInstance {
        instance_key: key,
        tool_call_id: "echo@v1".to_string(),
        impure: false,
        invalidated_at: None,
        executed_at: Timestamp::default(),
        step_ids: BTreeSet::new(),
        command_args: Vec::new(),
        env_vars: BTreeMap::new(),
        materialized_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
    }
}

/// Seeds an instance with aux `last_referenced_at` far enough in the past to
/// be evicted by a zero-TTL sweep.
fn seed_state_with(keys: &[Hash]) -> ConductorState {
    let mut state = ConductorState::new_empty();
    state.aux = AuxData {
        tool_call_instance_counter: 0,
        conductor_gc_epoch: Timestamp::now(),
        instances: keys
            .iter()
            .map(|key| (*key, InstanceAux { last_referenced_at: Timestamp::from_unix_nanos(0) }))
            .collect(),
        live_instances: BTreeMap::new(),
    };
    state.tool_call_instances = keys.iter().map(|key| (*key, sample_instance(*key))).collect();
    state
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Instances outside the reachable set with last-referenced past TTL are evicted.
#[test]
fn run_conductor_gc_evicts_unreferenced_past_ttl() {
    let keep = Hash::from_content(b"keep");
    let remove = Hash::from_content(b"remove");
    let mut state = seed_state_with(&[keep, remove]);

    let referenced: BTreeSet<Hash> = [keep].into();
    state.run_conductor_gc(&referenced, 0); // TTL = 0 → anything unreferenced is evicted

    assert!(state.tool_call_instances.contains_key(&keep), "referenced instance should survive");
    assert!(
        !state.tool_call_instances.contains_key(&remove),
        "unreferenced instance past TTL should be evicted"
    );
}

/// Instances in the reachable set survive GC.
#[test]
fn run_conductor_gc_preserves_referenced() {
    let a = Hash::from_content(b"a");
    let b = Hash::from_content(b"b");
    let mut state = seed_state_with(&[a, b]);

    let referenced: BTreeSet<Hash> = [a, b].into();
    state.run_conductor_gc(&referenced, 0);

    assert!(state.tool_call_instances.contains_key(&a));
    assert!(state.tool_call_instances.contains_key(&b));
    assert_eq!(state.tool_call_instances.len(), 2);
}

/// GC on empty state is a no-op.
#[test]
fn run_conductor_gc_empty_state_is_noop() {
    let mut state = ConductorState::new_empty();
    state.run_conductor_gc(&BTreeSet::new(), 0);
    assert!(state.tool_call_instances.is_empty());
}

/// An empty reachable set evicts all instances past TTL.
#[test]
fn run_conductor_gc_evicts_all_when_empty_referenced() {
    let a = Hash::from_content(b"a");
    let b = Hash::from_content(b"b");
    let mut state = seed_state_with(&[a, b]);

    state.run_conductor_gc(&BTreeSet::new(), 0);
    assert!(
        state.tool_call_instances.is_empty(),
        "empty referenced set with zero TTL evicts everything"
    );
}

/// GC with non-zero TTL keeps instances within the grace period.
#[test]
fn run_conductor_gc_within_ttl_preserves_unreferenced() {
    let fresh = Hash::from_content(b"fresh");
    let mut state = seed_state_with(&[fresh]);
    // Seeded aux has last_referenced = 0 (unix epoch).
    // TTL = 10^18 seconds is far larger than any real test duration.
    state.run_conductor_gc(&BTreeSet::new(), 1_000_000_000_000_000);
    assert!(state.tool_call_instances.contains_key(&fresh), "instance within TTL should survive");
}

// ---------------------------------------------------------------------------
// Seed recording
// ---------------------------------------------------------------------------

/// Returns the seed a completed run recorded for `workflow`.
fn recorded_seed(conductor: &Conductor<InMemoryCas>, workflow: &str) -> BTreeSet<Hash> {
    conductor
        .get_state()
        .expect("the conductor state must be readable")
        .aux
        .live_instances
        .get(workflow)
        .cloned()
        .unwrap_or_default()
}

/// A successful run records the instances it produced, and a later run that
/// reuses them keeps them live.
///
/// The user-level guarantee: the seed describes work that is live now, not work
/// that was merely produced once. A step that is a cache hit is as live as one
/// that executes, so a run of nothing but hits must not empty the seed and let
/// GC evict its own cache.
#[tokio::test]
async fn a_successful_run_records_the_instances_it_executed_and_reused() {
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("echo@v1".to_string(), echo_tool("echo@v1"))]),
        vec![workflow_with_steps("default", vec![echo_step("echo@v1", "payload")])],
    ));

    let first = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the first run must complete");
    assert_eq!(first.executed_steps, 1, "the first run executes its only step: {first:?}");

    let after_execution = recorded_seed(tc.conductor(), "default");
    assert_eq!(
        after_execution,
        tc.conductor()
            .get_state()
            .expect("state")
            .tool_call_instances
            .keys()
            .copied()
            .collect::<BTreeSet<_>>(),
        "the seed must name every instance the run left behind"
    );
    assert!(!after_execution.is_empty(), "a run that executed a step records its instance");

    let second = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the second run must complete");
    assert_eq!(second.cached_steps, 1, "the second run reuses the instance: {second:?}");
    assert_eq!(
        recorded_seed(tc.conductor(), "default"),
        after_execution,
        "a run of cache hits must keep the reused instances live"
    );
}

/// A run with a failed step leaves the previous seed untouched.
///
/// The user-level guarantee: a half-finished run must not shorten the life of
/// what the last complete run left in place. The second run is configured with
/// one step fewer and one failing step, so writing its seed would shrink the
/// live set — the instances of the step that left the workflow would lose their
/// protection because an unrelated step failed.
#[tokio::test]
async fn a_failed_run_leaves_the_previous_seed_in_place() {
    let tc = TestConductor::new();
    let tools = BTreeMap::from([("echo@v1".to_string(), echo_tool("echo@v1"))]);
    let mut second_step = echo_step("echo@v1", "payload-b");
    second_step.id = "s2".to_string();
    tc.write_config(doc_with_workflows(
        tools.clone(),
        vec![workflow_with_steps("default", vec![echo_step("echo@v1", "payload-a"), second_step])],
    ));

    let first = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the first run must complete");
    assert_eq!(first.executed_steps, 2, "the first run executes both steps: {first:?}");
    let previous = recorded_seed(tc.conductor(), "default");
    assert_eq!(previous.len(), 2, "both executed instances are recorded: {previous:?}");

    let mut broken_step = echo_step("echo@v1", "unreachable");
    broken_step.id = "broken".to_string();
    broken_step.inputs =
        BTreeMap::from([("text".to_string(), "${step_output.missing.result}".to_string())]);
    tc.write_config(doc_with_workflows(
        tools,
        vec![workflow_with_steps("default", vec![echo_step("echo@v1", "payload-a"), broken_step])],
    ));

    let second = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("a run with a failing step still returns a summary");
    assert_eq!(second.failed_steps, 1, "exactly the broken step fails: {second:?}");

    assert_eq!(
        recorded_seed(tc.conductor(), "default"),
        previous,
        "a failed run must not replace the seed the last clean run recorded"
    );
}

/// The seed survives the process, and a pass classifies instead of abstaining.
///
/// `recorded_seed` reads the conductor's snapshot, which a run has just written
/// in memory, so every other seed test passes even if nothing reaches the file.
/// This one reopens the same paths: the seed has to be on disk for it to be
/// there. The eviction half is what proves the pass walked the seed rather than
/// abstaining — an abstaining pass keeps the superseded instance too.
#[tokio::test]
async fn the_seed_reaches_the_file_and_a_pass_classifies_with_it() {
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("echo@v1".to_string(), echo_tool("echo@v1"))]),
        vec![workflow_with_steps("default", vec![echo_step("echo@v1", "payload")])],
    ));
    tc.conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the run must complete");
    let live = recorded_seed(tc.conductor(), "default").into_iter().next().expect("one seeded key");

    let reopened = Conductor::new(
        mediapm_conductor::api::RuntimeStoragePaths::new(tc.path()),
        InMemoryCas::new(),
    );
    reopened.ensure_persisted_state_loaded().await.expect("load the persisted state");
    let persisted = recorded_seed(&reopened, "default");
    assert_eq!(persisted, BTreeSet::from([live]), "the seed must be on disk, not only in memory");

    // A superseded record: present in the store, named by no seed, reachable
    // from nothing. Its clock is old, so the pass may evict it.
    let superseded = Hash::from_content(b"superseded");
    let mut state = reopened.get_state().expect("state");
    state.tool_call_instances.insert(superseded, sample_instance(superseded));
    state
        .aux
        .instances
        .insert(superseded, InstanceAux { last_referenced_at: Timestamp::from_unix_nanos(0) });
    reopened.replace_resolved_state(state).expect("replace state");

    // Two passes: the grace period is measured against the epoch the previous
    // pass left behind, so the first pass only starts the clock. A state that a
    // previous version wrote has an epoch of zero, which is why the first pass
    // after an upgrade evicts nothing.
    reopened.run_gc().await.expect("first gc starts the clock");
    reopened.run_gc().await.expect("second gc classifies against it");

    let after = reopened.get_state().expect("state");
    assert!(after.tool_call_instances.contains_key(&live), "a seeded instance must survive");
    assert!(
        !after.tool_call_instances.contains_key(&superseded),
        "a pass that classified must evict the unreachable record; keeping it means it abstained",
    );
}
