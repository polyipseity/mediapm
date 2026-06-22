//! Integration coverage for conductor GC.
//!
//! These tests validate `run_conductor_gc()` semantics on `OrchestrationState`:
//! - instances absent from `referenced_keys` are evicted past TTL
//! - instances present in `referenced_keys` survive and get refreshed
//! - empty state is a no-op
//! - empty referenced set evicts everything

use std::collections::{BTreeMap, BTreeSet};

use mediapm_conductor::{AuxData, ImpureTimestamp, OrchestrationState, ToolCallInstance};

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

/// Creates a minimal `ToolCallInstance` with the given key and an initial
/// `conductor_gc_last_referenced_at` that is far enough in the past to be
/// evicted by a zero-TTL sweep.
fn sample_instance(key: &str) -> ToolCallInstance {
    ToolCallInstance {
        instance_key: key.to_string(),
        tool_id: "echo@1.0.0".to_string(),
        inputs: Vec::new(),
        outputs: Vec::new(),
        worker_index: 0,
        executed: true,
        rematerialized: false,
        conductor_gc_last_referenced_at: ImpureTimestamp::from_unix_nanos(0),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Instances not in `referenced_keys` with last-referenced past TTL are evicted.
#[test]
fn run_conductor_gc_evicts_unreferenced_past_ttl() {
    let mut state = OrchestrationState {
        tool_call_instances: BTreeMap::from([
            ("keep".to_string(), sample_instance("keep")),
            ("remove".to_string(), sample_instance("remove")),
        ]),
        aux: AuxData { tool_call_instance_counter: 0, conductor_gc_epoch: ImpureTimestamp::now() },
        ..OrchestrationState::new_empty()
    };

    let referenced: BTreeSet<String> = ["keep".to_string()].into();
    state.run_conductor_gc(&referenced, 0); // TTL = 0 → anything unreferenced is evicted

    assert!(state.tool_call_instances.contains_key("keep"), "referenced instance should survive");
    assert!(
        !state.tool_call_instances.contains_key("remove"),
        "unreferenced instance past TTL should be evicted"
    );
}

/// Instances in `referenced_keys` survive GC.
#[test]
fn run_conductor_gc_preserves_referenced() {
    let mut state = OrchestrationState {
        tool_call_instances: BTreeMap::from([
            ("a".to_string(), sample_instance("a")),
            ("b".to_string(), sample_instance("b")),
        ]),
        aux: AuxData { tool_call_instance_counter: 0, conductor_gc_epoch: ImpureTimestamp::now() },
        ..OrchestrationState::new_empty()
    };

    let referenced: BTreeSet<String> = ["a".to_string(), "b".to_string()].into();
    state.run_conductor_gc(&referenced, 0);

    assert!(state.tool_call_instances.contains_key("a"));
    assert!(state.tool_call_instances.contains_key("b"));
    assert_eq!(state.tool_call_instances.len(), 2);
}

/// GC on empty state is a no-op.
#[test]
fn run_conductor_gc_empty_state_is_noop() {
    let mut state = OrchestrationState::new_empty();
    state.run_conductor_gc(&BTreeSet::new(), 0);
    assert!(state.tool_call_instances.is_empty());
}

/// Empty referenced set evicts all instances past TTL.
#[test]
fn run_conductor_gc_evicts_all_when_empty_referenced() {
    let mut state = OrchestrationState {
        tool_call_instances: BTreeMap::from([
            ("a".to_string(), sample_instance("a")),
            ("b".to_string(), sample_instance("b")),
        ]),
        aux: AuxData { tool_call_instance_counter: 0, conductor_gc_epoch: ImpureTimestamp::now() },
        ..OrchestrationState::new_empty()
    };

    state.run_conductor_gc(&BTreeSet::new(), 0);
    assert!(
        state.tool_call_instances.is_empty(),
        "empty referenced set with zero TTL evicts everything"
    );
}
