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
//!
//! The third block drives the facade's own `run_gc`, which reads the TTL the
//! facade was configured with rather than the built-in default: the recovery
//! window a fresh unreachable record enjoys, and the eviction once its
//! configured grace period has passed.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use mediapm_cas::{CasApi, Hash, InMemoryCas};
use mediapm_conductor::api::RunWorkflowOptions;
use mediapm_conductor::gc::GcClassification;
use mediapm_conductor::{
    AuxData, CapturedOutput, Conductor, ConductorState, RuntimeStoragePaths, ToolCallInstance,
};
use mediapm_utils::Timestamp;

use super::gc_automatic::backdate;
use crate::{
    TestConductor, doc_with_workflows, echo_step, echo_tool, workflow_with_steps,
    write_conductor_config,
};

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

/// Creates a minimal `ToolCallInstance` to file under `key`.
///
/// The key is the caller's, not the record's: the instance carries no key of its
/// own, and the store it is inserted into is what gives it one.
///
/// `pub(crate)` so `gc_automatic` can build the same unreachable record its
/// background scenarios insert, rather than growing a second builder.
pub(crate) fn sample_instance(_key: Hash) -> ToolCallInstance {
    ToolCallInstance {
        tool_call_id: "echo@v1".to_string(),
        impure: false,
        invalidated_at: None,
        executed_at: Timestamp::default(),
        last_referenced_at: Timestamp::from_unix_nanos(0),
        step_ids: BTreeSet::new(),
        command_args: Vec::new(),
        env_vars: BTreeMap::new(),
        materialized_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
    }
}

/// Seeds instances whose own `last_referenced_at` is far enough in the past to
/// be evicted by a zero-TTL sweep.
fn seed_state_with(keys: &[Hash]) -> ConductorState {
    let mut state = ConductorState::new_empty();
    state.aux = AuxData { conductor_gc_epoch: Timestamp::now(), live_instances: BTreeMap::new() };
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
    tc.shutdown().await;
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
    tc.shutdown().await;
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
    tc.shutdown().await;
}

/// An unreachable record stays recoverable until its grace period passes.
///
/// The user-level guarantee: a facade configured with a one-day TTL keeps a
/// fresh unreachable record and its output blob (the recovery window), then
/// reclaims both once the record is older than that TTL - so the sweep ages
/// records against the TTL the facade was configured with, not against a
/// default that happens to differ. The record is backdated rather than slept
/// through, and the backdate (90 000 s) sits between the configured TTL
/// (86 400 s) and the built-in default (604 800 s): only the configured value
/// evicts it, so a pass that read the default instead would fail here.
#[tokio::test]
async fn an_unreferenced_instance_is_recoverable_until_its_ttl_passes() {
    let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    write_conductor_config(
        dir.path(),
        doc_with_workflows(
            BTreeMap::from([("echo@v1".to_string(), echo_tool("echo@v1"))]),
            vec![workflow_with_steps("default", vec![echo_step("echo@v1", "payload")])],
        ),
    );
    let paths = RuntimeStoragePaths::new(dir.path());

    // The configured TTL is one day; the built-in default is seven. Every
    // judgement below is written so only the configured value produces the
    // asserted outcome.
    let conductor =
        Conductor::new(paths.clone(), InMemoryCas::new()).with_gc_settings(86_400, 86_400);

    // A run records the seed a pass classifies against.
    let summary = conductor
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the run must complete");
    assert_eq!(summary.failed_steps, 0, "the seed comes from a clean run: {summary:?}");

    // The candidate: unreachable from the seed, one output blob, fresh clock.
    let candidate = Hash::from_content(b"unreferenced-recovery-candidate");
    let blob = conductor
        .cas()
        .put(Bytes::from_static(b"bytes the recovery window must keep"))
        .await
        .expect("stage the candidate's output blob");
    let mut state = conductor.get_state().expect("state");
    let mut instance = sample_instance(candidate);
    instance.last_referenced_at = Timestamp::now();
    instance.outputs =
        BTreeMap::from([("result".to_string(), CapturedOutput { hash: blob, capture: None })]);
    state.tool_call_instances.insert(candidate, instance);
    conductor.replace_resolved_state(state).expect("insert the candidate");

    // Pass 1, inside the grace period: the record and its blob survive.
    let first = conductor.run_gc().await.expect("first pass");
    assert!(
        matches!(first.classification, GcClassification::Classified),
        "the seeded workflow makes this a judged pass: {first:?}",
    );
    let after_first = conductor.get_state().expect("state");
    assert!(
        after_first.tool_call_instances.contains_key(&candidate),
        "a fresh unreachable record must stay recoverable inside its TTL",
    );
    assert!(
        conductor.cas().stat(blob).await.is_ok(),
        "the survivor's output blob must stay readable while the record does",
    );

    // Beyond the configured TTL, still inside the built-in default.
    let mut state = conductor.get_state().expect("state");
    backdate(&mut state, &candidate.to_hex(), 90_000);
    conductor.replace_resolved_state(state).expect("age the candidate");

    // Pass 2: past the configured TTL the record and its blob are reclaimed,
    // and the state file carries the eviction for a later process to read.
    conductor.run_gc().await.expect("second pass");
    let after = conductor.get_state().expect("state");
    assert!(
        !after.tool_call_instances.contains_key(&candidate),
        "a record older than the configured TTL must be evicted, not kept as if the default applied",
    );
    assert!(
        conductor.cas().stat(blob).await.is_err(),
        "an evicted record's output blob must be reclaimed with it",
    );
    conductor.shutdown().await.expect("shutdown");

    let reopened = Conductor::new(paths, InMemoryCas::new());
    reopened.ensure_persisted_state_loaded().await.expect("load the persisted state");
    let on_disk = reopened.get_state().expect("state");
    assert!(
        !on_disk.tool_call_instances.contains_key(&candidate),
        "the eviction must reach the state file, not only the memory snapshot",
    );
    reopened.shutdown().await.expect("shutdown");
}
