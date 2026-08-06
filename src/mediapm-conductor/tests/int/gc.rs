//! Integration coverage for conductor GC.
//!
//! These tests validate `run_conductor_gc()` semantics on `ConductorState`:
//! - instances absent from `referenced_keys` are evicted past TTL
//! - instances present in `referenced_keys` survive and get refreshed
//! - empty state is a no-op
//! - empty referenced set evicts everything

use std::collections::{BTreeMap, BTreeSet};

use mediapm_cas::Hash;
use mediapm_conductor::{AuxData, ConductorState, InstanceAux, ToolCallInstance};
use mediapm_utils::Timestamp;

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

/// Creates a minimal `ToolCallInstance` with the given content-addressed key.
fn sample_instance(key: Hash) -> ToolCallInstance {
    ToolCallInstance {
        instance_key: key,
        tool_call_id: "echo@v1".to_string(),
        impure: false,
        executed_at: Timestamp::default(),
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
            .map(|key| {
                (
                    *key,
                    InstanceAux {
                        save_modes: BTreeMap::new(),
                        last_referenced_at: Timestamp::from_unix_nanos(0),
                    },
                )
            })
            .collect(),
    };
    state.tool_call_instances = keys.iter().map(|key| (*key, sample_instance(*key))).collect();
    state
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Instances not in `referenced_keys` with last-referenced past TTL are evicted.
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

/// Instances in `referenced_keys` survive GC.
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

/// Empty referenced set evicts all instances past TTL.
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
