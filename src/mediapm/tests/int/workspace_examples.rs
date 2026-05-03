//! Regression guardrails for workspace example target declarations.
//!
//! The workspace intentionally allows crates to share canonical example names
//! such as `demo`. These checks validate that key examples remain declared
//! without requiring uniqueness-based renaming.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

/// Returns workspace root from crate-level manifest directory.
fn workspace_root() -> PathBuf {
    let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    crate_root
        .parent()
        .and_then(Path::parent)
        .expect("mediapm crate should live under <workspace>/src/mediapm")
        .to_path_buf()
}

/// Returns Cargo manifests that currently declare runnable workspace examples.
fn workspace_manifest_paths() -> Vec<PathBuf> {
    let root = workspace_root();
    vec![
        root.join("src/cas/Cargo.toml"),
        root.join("src/conductor/Cargo.toml"),
        root.join("src/mediapm/Cargo.toml"),
    ]
}

/// Extracts declared `[[example]]` target names from one Cargo manifest.
fn parse_declared_example_names(manifest_text: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut in_example_block = false;

    for raw_line in manifest_text.lines() {
        let line = raw_line.trim();

        if line.starts_with("[[") {
            in_example_block = line == "[[example]]";
            continue;
        }

        if !in_example_block || !line.starts_with("name") {
            continue;
        }

        let Some((_, value)) = line.split_once('=') else {
            continue;
        };
        let name = value.trim().trim_matches('"');
        if !name.is_empty() {
            names.push(name.to_string());
        }
    }

    names
}

/// Ensures crates keep stable, policy-approved example target names.
#[test]
fn workspace_examples_keep_stable_crate_local_targets() {
    let mut names_by_owner = BTreeMap::<String, BTreeSet<String>>::new();

    for manifest_path in workspace_manifest_paths() {
        let manifest_text =
            fs::read_to_string(&manifest_path).expect("workspace manifest should be readable");
        let names = parse_declared_example_names(&manifest_text);
        let owner = manifest_path
            .strip_prefix(workspace_root())
            .unwrap_or(&manifest_path)
            .display()
            .to_string()
            .replace('\\', "/");

        names_by_owner.insert(owner, names.into_iter().collect());
    }

    let expected_names = BTreeMap::from([
        ("src/cas/Cargo.toml", BTreeSet::from(["demo"])),
        ("src/conductor/Cargo.toml", BTreeSet::from(["demo"])),
        ("src/mediapm/Cargo.toml", BTreeSet::from(["demo", "demo_online"])),
    ]);

    for (owner, expected) in expected_names {
        let Some(actual) = names_by_owner.get(owner) else {
            panic!("workspace manifest '{owner}' should exist and be parsed");
        };
        for required in expected {
            assert!(
                actual.contains(required),
                "manifest '{owner}' should declare example target '{required}'"
            );
        }
    }
}

/// Ensures shared canonical names stay available across the workspace without
/// requiring compatibility aliases.
#[test]
fn shared_demo_name_is_declared_in_multiple_workspace_crates() {
    let mut declared_names = BTreeSet::<String>::new();
    let mut demo_owner_count = 0usize;

    for manifest_path in workspace_manifest_paths() {
        let manifest_text =
            fs::read_to_string(&manifest_path).expect("workspace manifest should be readable");
        let names = parse_declared_example_names(&manifest_text);
        if names.iter().any(|name| name == "demo") {
            demo_owner_count += 1;
        }
        declared_names.extend(names);
    }

    for required in ["demo", "demo_online"] {
        assert!(
            declared_names.contains(required),
            "workspace should declare expected demo example target '{required}'"
        );
    }

    assert!(
        demo_owner_count >= 2,
        "workspace policy expects shared 'demo' naming across multiple crates"
    );
}
