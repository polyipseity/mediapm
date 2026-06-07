//! Integration coverage for instance GC with configurable TTL.
//!
//! These tests validate:
//! - `gc_instances()` no-op semantics after `last_used` removal
//! - `RuntimeStorageConfig` round-trip for `instance_ttl_seconds`
//! - GC hook plumbing through `SimpleConductor` + state store

use std::collections::{BTreeMap, HashSet};

use mediapm_cas::InMemoryCas;
use mediapm_conductor::{
    AuxData, ConductorApi, ImpureTimestamp, MachineNickelDocument, OrchestrationState,
    SimpleConductor, ToolCallInstance, ToolKindSpec, ToolSpec, UserNickelDocument, WorkflowSpec,
    WorkflowStepSpec, decode_user_document, encode_machine_document, encode_user_document,
};
use tempfile::tempdir;

// ---------------------------------------------------------------------------
// Test 1 — gc_instances unit semantics
// ---------------------------------------------------------------------------

/// Protects that unreferenced instances with `last_reachable < cutoff` are evicted.
#[test]
fn gc_instances_evicts_stale_unreferenced() {
    let cutoff = ImpureTimestamp { epoch_seconds: 100, subsec_nanos: 0 };

    let mut state = OrchestrationState {
        instances: BTreeMap::from([
            ("fresh".to_string(), sample_instance("fresh")),
            ("stale".to_string(), sample_instance("stale")),
            ("unmarked".to_string(), sample_instance("unmarked")),
        ]),
        aux: BTreeMap::from([
            (
                "fresh".to_string(),
                AuxData {
                    last_reachable: Some(ImpureTimestamp { epoch_seconds: 200, subsec_nanos: 0 }),
                },
            ),
            (
                "stale".to_string(),
                AuxData {
                    last_reachable: Some(ImpureTimestamp { epoch_seconds: 50, subsec_nanos: 0 }),
                },
            ),
            // "unmarked" has no aux entry → last_reachable is None → preserved
        ]),
        ..OrchestrationState::default()
    };

    state.gc_instances(cutoff);

    assert!(
        state.instances.contains_key("fresh"),
        "fresh instance (last_reachable > cutoff) should survive"
    );
    assert!(
        !state.instances.contains_key("stale"),
        "stale instance (last_reachable < cutoff) should be evicted"
    );
    assert!(
        state.instances.contains_key("unmarked"),
        "unmarked instance (last_reachable None) should survive"
    );
}

/// Protects that instances in `referenced_instance_keys` survive even when
/// `last_reachable < cutoff`.
#[test]
fn gc_instances_preserves_referenced() {
    let cutoff = ImpureTimestamp { epoch_seconds: 100, subsec_nanos: 0 };

    let mut state = OrchestrationState {
        instances: BTreeMap::from([
            ("referenced_stale".to_string(), sample_instance("referenced_stale")),
            ("unreferenced_stale".to_string(), sample_instance("unreferenced_stale")),
        ]),
        aux: BTreeMap::from([
            (
                "referenced_stale".to_string(),
                AuxData {
                    last_reachable: Some(ImpureTimestamp { epoch_seconds: 50, subsec_nanos: 0 }),
                },
            ),
            (
                "unreferenced_stale".to_string(),
                AuxData {
                    last_reachable: Some(ImpureTimestamp { epoch_seconds: 50, subsec_nanos: 0 }),
                },
            ),
        ]),
        referenced_instance_keys: HashSet::from(["referenced_stale".to_string()]),
        ..OrchestrationState::default()
    };

    state.gc_instances(cutoff);

    assert!(
        state.instances.contains_key("referenced_stale"),
        "referenced instance should survive despite stale last_reachable"
    );
    assert!(
        !state.instances.contains_key("unreferenced_stale"),
        "unreferenced stale instance should be evicted"
    );
}

/// Protects that instances with `last_reachable: None` survive regardless of
/// cutoff (safety net for pre-GC state).
#[test]
fn gc_instances_preserves_unmarked() {
    let cutoff = ImpureTimestamp { epoch_seconds: 100, subsec_nanos: 0 };

    let mut state = OrchestrationState {
        instances: BTreeMap::from([
            ("a".to_string(), sample_instance("a")),
            ("b".to_string(), sample_instance("b")),
        ]),
        // No aux entries → all instances have last_reachable: None
        ..OrchestrationState::default()
    };

    state.gc_instances(cutoff);

    assert_eq!(state.instances.len(), 2, "unmarked instances survive (safety net)");
}

/// Protects that `gc_instances` is a no-op on an empty state.
#[test]
fn gc_instances_empty_state_is_noop() {
    let mut state = OrchestrationState::default();
    state.gc_instances(ImpureTimestamp { epoch_seconds: 0, subsec_nanos: 0 });
    assert!(state.instances.is_empty());
}

/// Protects that unreferenced instances without aux entries get `last_reachable`
/// set to `now` during phase 1, preventing immediate eviction in phase 2.
#[test]
fn gc_instances_marks_unreferenced_with_now() {
    let cutoff = ImpureTimestamp { epoch_seconds: 100, subsec_nanos: 0 };

    let mut state = OrchestrationState {
        instances: BTreeMap::from([
            ("a".to_string(), sample_instance("a")),
            ("b".to_string(), sample_instance("b")),
        ]),
        // No aux entries → phase 1 sets last_reachable = now for both
        ..OrchestrationState::default()
    };

    state.gc_instances(cutoff);

    // Since both were marked with `now` in phase 1, and `now > cutoff`,
    // they should survive phase 2.
    assert_eq!(state.instances.len(), 2, "instances marked with now survive");
    assert!(
        state.aux.get("a").and_then(|a| a.last_reachable).is_some(),
        "instance 'a' gets last_reachable"
    );
    assert!(
        state.aux.get("b").and_then(|a| a.last_reachable).is_some(),
        "instance 'b' gets last_reachable"
    );
}

/// Protects that `gc_instances` respects the subsec_nanos boundary correctly.
#[test]
fn gc_instances_respects_subsec_nanos_boundary() {
    let cutoff = ImpureTimestamp { epoch_seconds: 100, subsec_nanos: 500_000_000 };

    let mut state = OrchestrationState {
        instances: BTreeMap::from([
            ("nanos_below".to_string(), sample_instance("nanos_below")),
            ("nanos_equal".to_string(), sample_instance("nanos_equal")),
            ("nanos_above".to_string(), sample_instance("nanos_above")),
        ]),
        aux: BTreeMap::from([
            (
                "nanos_below".to_string(),
                AuxData {
                    last_reachable: Some(ImpureTimestamp {
                        epoch_seconds: 100,
                        subsec_nanos: 400_000_000,
                    }),
                },
            ),
            (
                "nanos_equal".to_string(),
                AuxData {
                    last_reachable: Some(ImpureTimestamp {
                        epoch_seconds: 100,
                        subsec_nanos: 500_000_000,
                    }),
                },
            ),
            (
                "nanos_above".to_string(),
                AuxData {
                    last_reachable: Some(ImpureTimestamp {
                        epoch_seconds: 100,
                        subsec_nanos: 600_000_000,
                    }),
                },
            ),
        ]),
        ..OrchestrationState::default()
    };

    state.gc_instances(cutoff);

    assert!(!state.instances.contains_key("nanos_below"), "nanos below cutoff should be evicted");
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

/// Protects that the GC hook accepts `instance_ttl_seconds = 1` without error
/// and that the instance survives (GC is a no-op, always keeps instances).
#[tokio::test]
async fn gc_hook_accepts_near_zero_ttl() {
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
            instance_ttl_seconds: Some(1),
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
        .expect("workflow should execute with near-zero TTL");
    assert_eq!(summary.executed_instances, 1);

    let state = conductor.get_state().await.expect("state snapshot should load");
    // With TTL=1, cutoff ≈ now - 1s; the executed instance is referenced,
    // so it survives GC despite the aggressive TTL.
    assert_eq!(state.instances.len(), 1, "referenced instance survives near-zero TTL");
}

// ---------------------------------------------------------------------------
// Test 5 — Explicit RunGc trigger API
// ---------------------------------------------------------------------------

/// Protects that `run_gc(None)` succeeds when neither config nor override
/// supplies a TTL, and that the instance survives.
#[tokio::test]
async fn run_gc_noop_without_ttl() {
    let conductor = SimpleConductor::new(InMemoryCas::new());
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");

    let (user, machine) = echo_doc_pair(None);
    std::fs::write(&user_path, encode_user_document(user).expect("encode user"))
        .expect("write user");
    std::fs::write(&machine_path, encode_machine_document(machine).expect("encode machine"))
        .expect("write machine");

    let summary =
        conductor.run_workflow(&user_path, &machine_path).await.expect("workflow should execute");
    assert_eq!(summary.executed_instances, 1);

    let result = conductor.run_gc(None).await;
    assert!(result.is_ok(), "run_gc(None) should succeed when TTL is unset");

    let state = conductor.get_state().await.expect("state snapshot");
    assert_eq!(state.instances.len(), 1, "instance survives when TTL is unset");
}

/// Protects that `run_gc(None)` uses the config-supplied TTL so a generous
/// TTL preserves the referenced instance.
#[tokio::test]
async fn run_gc_uses_config_ttl() {
    let conductor = SimpleConductor::new(InMemoryCas::new());
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");

    let (user, machine) = echo_doc_pair(Some(86400));
    std::fs::write(&user_path, encode_user_document(user).expect("encode user"))
        .expect("write user");
    std::fs::write(&machine_path, encode_machine_document(machine).expect("encode machine"))
        .expect("write machine");

    let summary =
        conductor.run_workflow(&user_path, &machine_path).await.expect("workflow should execute");
    assert_eq!(summary.executed_instances, 1);

    let result = conductor.run_gc(None).await;
    assert!(result.is_ok(), "run_gc(None) should succeed with config TTL set");

    let state = conductor.get_state().await.expect("state snapshot");
    assert_eq!(state.instances.len(), 1, "instance survives generous config TTL");
}

/// Protects that `run_gc(Some(...))` overrides the config TTL, and that a
/// generous override does not cause errors.
#[tokio::test]
async fn run_gc_override_large_ttl() {
    let conductor = SimpleConductor::new(InMemoryCas::new());
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");

    let (user, machine) = echo_doc_pair(None);
    std::fs::write(&user_path, encode_user_document(user).expect("encode user"))
        .expect("write user");
    std::fs::write(&machine_path, encode_machine_document(machine).expect("encode machine"))
        .expect("write machine");

    let summary =
        conductor.run_workflow(&user_path, &machine_path).await.expect("workflow should execute");
    assert_eq!(summary.executed_instances, 1);

    let result = conductor.run_gc(Some(86400)).await;
    assert!(result.is_ok(), "run_gc(Some(86400)) should succeed");

    let state = conductor.get_state().await.expect("state snapshot");
    assert_eq!(state.instances.len(), 1, "instance survives generous override TTL");
}

/// Protects that `run_gc` does not error on an empty (no-workflow-run) state.
#[tokio::test]
async fn run_gc_empty_state() {
    let conductor = SimpleConductor::new(InMemoryCas::new());
    let result = conductor.run_gc(Some(3600)).await;
    assert!(result.is_ok(), "run_gc should succeed on empty state");
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns a pair of user + machine documents with an `echo` workflow.
fn echo_doc_pair(instance_ttl: Option<u64>) -> (UserNickelDocument, MachineNickelDocument) {
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
            instance_ttl_seconds: instance_ttl,
            ..mediapm_conductor::RuntimeStorageConfig::default()
        },
        ..MachineNickelDocument::default()
    };
    (user, machine)
}

/// Returns a `ToolCallInstance` with deterministic minimal fields and the
/// given tool name.
fn sample_instance(tool_name: &str) -> ToolCallInstance {
    ToolCallInstance {
        tool_name: tool_name.to_string(),
        metadata: ToolSpec::default(),
        impure_timestamp: None,
        inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
    }
}
