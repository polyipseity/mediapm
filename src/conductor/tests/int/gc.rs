//! Integration coverage for instance GC with configurable TTL.
//!
//! These tests validate:
//! - `gc_instances()` unit semantics (cutoff comparison, None preservation)
//! - `RuntimeStorageConfig` round-trip for `instance_ttl_seconds`
//! - GC hook plumbing through `SimpleConductor` + state store

use std::collections::BTreeMap;

use mediapm_cas::InMemoryCas;
use mediapm_conductor::{
    ConductorApi, ImpureTimestamp, MachineNickelDocument, OrchestrationState, SimpleConductor,
    ToolCallInstance, ToolKindSpec, ToolSpec, UserNickelDocument, WorkflowSpec, WorkflowStepSpec,
    decode_user_document, encode_machine_document, encode_user_document,
};
use tempfile::tempdir;

// ---------------------------------------------------------------------------
// Test 1 — gc_instances unit semantics
// ---------------------------------------------------------------------------

/// Protects that `gc_instances` removes instances whose `last_used` is strictly
/// before the cutoff while preserving `None` and `>= cutoff` entries.
#[test]
fn gc_instances_removes_stale_instances() {
    let cutoff = ImpureTimestamp { epoch_seconds: 100, subsec_nanos: 0 };

    let mut state = OrchestrationState {
        instances: BTreeMap::from([
            (
                "fresh".to_string(),
                ToolCallInstance {
                    last_used: Some(ImpureTimestamp { epoch_seconds: 200, subsec_nanos: 0 }),
                    ..sample_instance("fresh")
                },
            ),
            (
                "exact_cutoff".to_string(),
                ToolCallInstance { last_used: Some(cutoff), ..sample_instance("exact_cutoff") },
            ),
            (
                "stale".to_string(),
                ToolCallInstance {
                    last_used: Some(ImpureTimestamp { epoch_seconds: 50, subsec_nanos: 0 }),
                    ..sample_instance("stale")
                },
            ),
            (
                "untracked".to_string(),
                ToolCallInstance { last_used: None, ..sample_instance("untracked") },
            ),
        ]),
        ..OrchestrationState::default()
    };

    state.gc_instances(cutoff);

    assert!(state.instances.contains_key("fresh"), "fresh instance should survive");
    assert!(state.instances.contains_key("exact_cutoff"), "cutoff-instance should survive");
    assert!(state.instances.contains_key("untracked"), "untracked instance should survive");
    assert!(!state.instances.contains_key("stale"), "stale instance should be removed");
}

/// Protects that `gc_instances` is a no-op on an empty state.
#[test]
fn gc_instances_empty_state_is_noop() {
    let mut state = OrchestrationState::default();
    state.gc_instances(ImpureTimestamp { epoch_seconds: 0, subsec_nanos: 0 });
    assert!(state.instances.is_empty());
}

/// Protects that `gc_instances` removes all instances when cutoff is very high.
#[test]
fn gc_instances_cutoff_removes_all_tracked() {
    let mut state = OrchestrationState {
        instances: BTreeMap::from([
            (
                "a".to_string(),
                ToolCallInstance {
                    last_used: Some(ImpureTimestamp { epoch_seconds: 0, subsec_nanos: 0 }),
                    ..sample_instance("a")
                },
            ),
            (
                "b".to_string(),
                ToolCallInstance {
                    last_used: Some(ImpureTimestamp {
                        epoch_seconds: 999,
                        subsec_nanos: 999_999_999,
                    }),
                    ..sample_instance("b")
                },
            ),
        ]),
        ..OrchestrationState::default()
    };

    state.gc_instances(ImpureTimestamp { epoch_seconds: 1000, subsec_nanos: 0 });

    assert!(state.instances.is_empty(), "all tracked instances should be removed");
    // Untracked (None) instances are not present in this test set, so empty
    // is the expected result.
}

/// Protects subsec_nanos comparison when epoch_seconds are equal.
#[test]
fn gc_instances_respects_subsec_nanos_boundary() {
    let cutoff = ImpureTimestamp { epoch_seconds: 100, subsec_nanos: 500_000_000 };

    let mut state = OrchestrationState {
        instances: BTreeMap::from([
            (
                "nanos_below".to_string(),
                ToolCallInstance {
                    last_used: Some(ImpureTimestamp {
                        epoch_seconds: 100,
                        subsec_nanos: 499_999_999,
                    }),
                    ..sample_instance("nanos_below")
                },
            ),
            (
                "nanos_equal".to_string(),
                ToolCallInstance { last_used: Some(cutoff), ..sample_instance("nanos_equal") },
            ),
            (
                "nanos_above".to_string(),
                ToolCallInstance {
                    last_used: Some(ImpureTimestamp {
                        epoch_seconds: 100,
                        subsec_nanos: 500_000_001,
                    }),
                    ..sample_instance("nanos_above")
                },
            ),
        ]),
        ..OrchestrationState::default()
    };

    state.gc_instances(cutoff);

    assert!(!state.instances.contains_key("nanos_below"), "nanos below cutoff should be removed");
    assert!(state.instances.contains_key("nanos_equal"), "nanos at cutoff should survive");
    assert!(state.instances.contains_key("nanos_above"), "nanos above cutoff should survive");
}

// ---------------------------------------------------------------------------
// Test 2 — Config round-trip for instance_ttl_seconds
// ---------------------------------------------------------------------------

/// Protects that `instance_ttl_seconds` survives a full encode/decode cycle
/// through the user document.
#[test]
fn instance_ttl_config_round_trip() {
    let original = UserNickelDocument {
        runtime: mediapm_conductor::RuntimeStorageConfig {
            instance_ttl_seconds: Some(3600),
            ..mediapm_conductor::RuntimeStorageConfig::default()
        },
        ..UserNickelDocument::default()
    };

    let encoded = encode_user_document(original.clone()).expect("encode");
    let decoded = decode_user_document(&encoded).expect("decode");

    assert_eq!(
        decoded.runtime.instance_ttl_seconds,
        Some(3600),
        "instance_ttl_seconds should survive round-trip"
    );
}

/// Protects that `instance_ttl_seconds == None` round-trips correctly.
#[test]
fn instance_ttl_none_round_trip() {
    let original = UserNickelDocument::default();
    assert!(
        original.runtime.instance_ttl_seconds.is_none(),
        "default should have None instance_ttl_seconds"
    );

    let encoded = encode_user_document(original.clone()).expect("encode");
    let decoded = decode_user_document(&encoded).expect("decode");

    assert!(
        decoded.runtime.instance_ttl_seconds.is_none(),
        "None instance_ttl_seconds should survive round-trip"
    );
}

// ---------------------------------------------------------------------------
// Test 4 — GC hook plumbing through SimpleConductor
// ---------------------------------------------------------------------------

/// Protects that the GC hook runs without error when `instance_ttl_seconds`
/// is set and that instances survive a generous TTL.
#[tokio::test]
async fn gc_hook_accepts_ttl_config() {
    let conductor = SimpleConductor::new(InMemoryCas::new());
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");

    let user = UserNickelDocument {
        tools: BTreeMap::from([(
            "echo@1.0.0".to_string(),
            ToolSpec {
                kind: ToolKindSpec::Builtin {
                    name: "echo".to_string(),
                    version: "1.0.0".to_string(),
                },
                ..ToolSpec::default()
            },
        )]),
        workflows: BTreeMap::from([(
            "default".to_string(),
            WorkflowSpec {
                steps: vec![WorkflowStepSpec {
                    id: "s1".to_string(),
                    tool: "echo@1.0.0".to_string(),
                    inputs: BTreeMap::new(),
                    depends_on: Vec::new(),
                    outputs: BTreeMap::new(),
                }],
                ..WorkflowSpec::default()
            },
        )]),
        ..UserNickelDocument::default()
    };
    let machine = MachineNickelDocument {
        runtime: mediapm_conductor::RuntimeStorageConfig {
            instance_ttl_seconds: Some(86400), // 24h — generous TTL
            ..mediapm_conductor::RuntimeStorageConfig::default()
        },
        ..MachineNickelDocument::default()
    };

    std::fs::write(&user_path, encode_user_document(user).expect("encode user"))
        .expect("write user");
    std::fs::write(&machine_path, encode_machine_document(machine).expect("encode machine"))
        .expect("write machine");

    let summary = conductor
        .run_workflow(&user_path, &machine_path)
        .await
        .expect("workflow should execute with TTL config");
    assert_eq!(summary.executed_instances, 1);

    let state = conductor.get_state().await.expect("state snapshot should load");
    assert_eq!(state.instances.len(), 1, "instance should survive generous TTL");
}

/// Protects that the GC hook accepts `instance_ttl_seconds = 0` without error
/// (instance survives because last_used == now == cutoff).
#[tokio::test]
async fn gc_hook_accepts_zero_ttl() {
    let conductor = SimpleConductor::new(InMemoryCas::new());
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");

    let user = UserNickelDocument {
        tools: BTreeMap::from([(
            "echo@1.0.0".to_string(),
            ToolSpec {
                kind: ToolKindSpec::Builtin {
                    name: "echo".to_string(),
                    version: "1.0.0".to_string(),
                },
                ..ToolSpec::default()
            },
        )]),
        workflows: BTreeMap::from([(
            "default".to_string(),
            WorkflowSpec {
                steps: vec![WorkflowStepSpec {
                    id: "s1".to_string(),
                    tool: "echo@1.0.0".to_string(),
                    inputs: BTreeMap::new(),
                    depends_on: Vec::new(),
                    outputs: BTreeMap::new(),
                }],
                ..WorkflowSpec::default()
            },
        )]),
        ..UserNickelDocument::default()
    };
    let machine = MachineNickelDocument {
        runtime: mediapm_conductor::RuntimeStorageConfig {
            instance_ttl_seconds: Some(0),
            ..mediapm_conductor::RuntimeStorageConfig::default()
        },
        ..MachineNickelDocument::default()
    };

    std::fs::write(&user_path, encode_user_document(user).expect("encode user"))
        .expect("write user");
    std::fs::write(&machine_path, encode_machine_document(machine).expect("encode machine"))
        .expect("write machine");

    let summary = conductor
        .run_workflow(&user_path, &machine_path)
        .await
        .expect("workflow should execute with zero TTL");
    assert_eq!(summary.executed_instances, 1);

    let state = conductor.get_state().await.expect("state snapshot should load");
    // With TTL=0, cutoff = now, instance last_used = now, so instance survives.
    assert_eq!(state.instances.len(), 1, "instance with last_used == now survives zero TTL");
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns a `ToolCallInstance` with deterministic minimal fields and the
/// given tool name.
fn sample_instance(tool_name: &str) -> ToolCallInstance {
    ToolCallInstance {
        tool_name: tool_name.to_string(),
        metadata: ToolSpec::default(),
        impure_timestamp: None,
        last_used: None,
        inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
    }
}
