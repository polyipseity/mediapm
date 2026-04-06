//! Integration tests for CAS-owned topology visualization helpers.
//!
//! These tests intentionally live in the CAS crate because topology rendering
//! and visualization request execution are Phase 1 responsibilities.

use mediapm_cas::{
    CasApi, CasMaintenanceApi, CasVisualizeFormat, CasVisualizeRequest, Constraint, FileSystemCas,
    Hash, OptimizeOptions, empty_content_hash, run_visualize_command,
};
use tempfile::tempdir;

/// Seeds one small base/target graph with an explicit constraint edge.
async fn seed_demo_graph(root: &std::path::Path) -> (Hash, Hash) {
    let cas = FileSystemCas::open(root).await.expect("open filesystem cas");

    let base = cas
        .put(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaBBBBBBBBBBBBBBBB".to_vec())
        .await
        .expect("put base object");
    let target = cas
        .put(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaCCCCCCCCCCCCCCCC".to_vec())
        .await
        .expect("put target object");

    cas.set_constraint(Constraint {
        target_hash: target,
        potential_bases: std::collections::BTreeSet::from([base]),
    })
    .await
    .expect("set explicit constraint");
    cas.optimize_once(OptimizeOptions::default()).await.expect("optimize once");

    (base, target)
}

/// Protects Mermaid rendering semantics for base and constraint edges.
#[tokio::test]
async fn visualize_mermaid_contains_graph_edges() {
    let temp = tempdir().expect("tempdir");
    let root = temp.path().join("cas");
    let (base, target) = seed_demo_graph(&root).await;
    let output = temp.path().join("topology.mmd");

    run_visualize_command(CasVisualizeRequest {
        root,
        format: CasVisualizeFormat::Mermaid,
        include_empty: false,
        output: Some(output.clone()),
    })
    .await
    .expect("run cas visualization request");

    let rendered = std::fs::read_to_string(output).expect("read mermaid output");
    assert!(rendered.contains("flowchart TD"));
    assert!(rendered.contains(&format!("n{}", base.to_hex())));
    assert!(rendered.contains(&format!("n{}", target.to_hex())));
    assert!(rendered.contains("-->|base|"));
    assert!(rendered.contains("-.->|allowed|"));
}

/// Protects JSON output contract and explicit constraint row persistence.
#[tokio::test]
async fn visualize_json_to_file_includes_target_constraint() {
    let temp = tempdir().expect("tempdir");
    let root = temp.path().join("cas");
    let (_base, target) = seed_demo_graph(&root).await;
    let output = temp.path().join("topology.json");

    run_visualize_command(CasVisualizeRequest {
        root,
        format: CasVisualizeFormat::Json,
        include_empty: false,
        output: Some(output.clone()),
    })
    .await
    .expect("run cas visualization request");

    let json = std::fs::read_to_string(output).expect("read json output");
    let value: serde_json::Value = serde_json::from_str(&json).expect("parse json output");

    let nodes =
        value.get("nodes").and_then(|v| v.as_array()).expect("nodes array in visualization json");
    assert!(!nodes.is_empty(), "nodes array should not be empty");

    let constraints = value
        .get("constraints")
        .and_then(|v| v.as_array())
        .expect("constraints array in visualization json");
    let target_text = target.to_string();
    assert!(
        constraints.iter().any(|row| {
            row.get("target_hash").and_then(|v| v.as_str()) == Some(target_text.as_str())
        }),
        "target constraint row should be present"
    );
}

/// Protects `include_empty` option semantics for canonical empty-hash visibility.
#[tokio::test]
async fn visualize_include_empty_flag_controls_empty_node_visibility() {
    let temp = tempdir().expect("tempdir");
    let root = temp.path().join("cas");
    let _ = seed_demo_graph(&root).await;

    let without_empty_output = temp.path().join("without-empty.json");
    run_visualize_command(CasVisualizeRequest {
        root: root.clone(),
        format: CasVisualizeFormat::Json,
        include_empty: false,
        output: Some(without_empty_output.clone()),
    })
    .await
    .expect("run cas visualization without empty node");
    let without_json: serde_json::Value = serde_json::from_slice(
        &std::fs::read(without_empty_output).expect("read json without empty"),
    )
    .expect("parse json without empty");

    let with_empty_output = temp.path().join("with-empty.json");
    run_visualize_command(CasVisualizeRequest {
        root,
        format: CasVisualizeFormat::Json,
        include_empty: true,
        output: Some(with_empty_output.clone()),
    })
    .await
    .expect("run cas visualization with empty node");
    let with_json: serde_json::Value =
        serde_json::from_slice(&std::fs::read(with_empty_output).expect("read json with empty"))
            .expect("parse json with empty");

    let empty_hash = empty_content_hash().to_string();
    let has_empty = |value: &serde_json::Value| {
        value
            .get("nodes")
            .and_then(|v| v.as_array())
            .expect("nodes array")
            .iter()
            .any(|node| node.get("hash").and_then(|v| v.as_str()) == Some(empty_hash.as_str()))
    };

    assert!(!has_empty(&without_json), "empty node should be hidden by default");
    assert!(has_empty(&with_json), "empty node should appear with --include-empty");
}
