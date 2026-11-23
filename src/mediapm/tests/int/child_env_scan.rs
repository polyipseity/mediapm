//! Regression scans for child-process spawns.
//!
//! Every runtime `Command::new` in mediapm and conductor source must route
//! through the child-environment whitelist: either the shared `child_env`
//! policy/apply helpers or `set_child_environment`. A raw spawn that inherits
//! or constructs an environment outside that path would leak ambient host state
//! into a child the document did not authorize.
//!
//! The whitelist scan walks `src/mediapm/src/**` and
//! `src/mediapm-conductor/src/**`. Build scripts and test-support spawns live
//! outside that runtime surface and are excluded with reasons recorded in the
//! allowlist below.
//!
//! A second scan rejects spawns of host tools that CI does not provision, so a
//! test that passes on a developer machine does not fail on a runner.

use std::path::{Path, PathBuf};

/// A `Command::new` occurrence that is intentionally outside the whitelist
/// contract, with the reason it is acceptable.
#[derive(Debug)]
struct AllowlistEntry {
    /// Workspace-relative path suffix that identifies the file.
    path_suffix: &'static str,
    /// Reason the occurrence does not require whitelist application.
    reason: &'static str,
}

/// Explicit exclusions from the whitelist scan.
///
/// `src/mediapm/build.rs` is a build script: it runs at compile time, not as a
/// mediapm runtime child, so it is outside the runtime whitelist contract.
/// `mediapm-test-support` spawns are test-harness processes owned by the test
/// support crate, not runtime children of mediapm or conductor.
const ALLOWLIST: &[AllowlistEntry] = &[
    AllowlistEntry {
        path_suffix: "src/mediapm/build.rs",
        reason: "build script; compile-time process, not a runtime child",
    },
    AllowlistEntry {
        path_suffix: "src/mediapm-test-support/",
        reason: "test-support harness spawns; not a mediapm or conductor runtime child",
    },
];

/// Returns `true` when `path` is covered by an explicit allowlist entry.
fn is_allowlisted(path: &Path) -> bool {
    let rendered = path.to_string_lossy().replace('\\', "/");
    ALLOWLIST
        .iter()
        .any(|entry| rendered.ends_with(entry.path_suffix) || rendered.contains(entry.path_suffix))
}

/// Collects every `.rs` file under `root`, recursively.
fn rust_files(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

/// Returns `true` when a `Command::new` occurrence at `line_index` is applied
/// through the whitelist.
///
/// The whitelist shows up as `child_env` (the shared policy/apply helpers) or
/// `set_child_environment` (the conductor's step/passthrough applicator) in the
/// surrounding scope. A window around the spawn catches both "create then apply
/// on the next lines" and "apply inside the same helper that constructs the
/// command".
fn has_whitelist_marker(lines: &[&str], line_index: usize) -> bool {
    let start = line_index.saturating_sub(20);
    let end = (line_index + 20).min(lines.len().saturating_sub(1));
    lines[start..=end]
        .iter()
        .any(|line| line.contains("child_env") || line.contains("set_child_environment"))
}

/// Every allowlist entry must carry a reason so the exclusion stays auditable.
#[test]
fn allowlist_entries_have_nonempty_reasons() {
    for entry in ALLOWLIST {
        assert!(
            !entry.reason.trim().is_empty(),
            "allowlist reason must be non-empty for {}",
            entry.path_suffix
        );
    }
}

/// Every runtime `Command::new` in mediapm and conductor source must use the
/// whitelist, or be named in the explicit allowlist with a reason.
#[test]
fn runtime_command_spawns_use_the_child_environment_whitelist() {
    let mediapm_src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let conductor_src = Path::new(env!("CARGO_MANIFEST_DIR")).join("../mediapm-conductor/src");

    let mut files = Vec::new();
    rust_files(&mediapm_src, &mut files);
    rust_files(&conductor_src, &mut files);
    files.sort();

    let mut violations = Vec::new();
    for file in &files {
        if is_allowlisted(file) {
            continue;
        }
        let Ok(source) = std::fs::read_to_string(file) else {
            continue;
        };
        let lines: Vec<&str> = source.lines().collect();
        for (index, line) in lines.iter().enumerate() {
            if !line.contains("Command::new(") {
                continue;
            }
            if !has_whitelist_marker(&lines, index) {
                violations.push(format!("{}:{}: {}", file.display(), index + 1, line.trim()));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "found Command::new spawns without child_env/set_child_environment whitelist usage \
         (allowlist: build script `src/mediapm/build.rs`, test-support spawns):\n{}",
        violations.join("\n")
    );
}

/// CI provisions exactly one host tool, and a test that spawns anything else is
/// a red job that only a developer machine hides.
///
/// `.github/workflows/ci.yml` installs `ffmpeg` (which brings `ffprobe`); every
/// other tool a test needs is evaluated in process or comes from the tool
/// document. `nickel` was spawned by a generated-document test and failed the
/// first CI run that contained it, because the runner has no Nickel CLI.
const UNPROVISIONED_HOST_TOOLS: &[&str] = &["nickel"];

#[test]
fn runtime_code_spawns_no_unprovisioned_host_tool() {
    // `CARGO_MANIFEST_DIR` is `src/mediapm`, so two levels up is the workspace
    // root, which is the directory the sibling scan reaches through `"src"`.
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    assert!(root.join("Cargo.toml").is_file(), "the scan root must be the workspace root");
    let mut files = Vec::new();
    for dir in ["src", "scripts", "tests"] {
        rust_files(&root.join(dir), &mut files);
    }
    files.sort();

    let mut violations = Vec::new();
    for file in &files {
        let Ok(source) = std::fs::read_to_string(file) else {
            continue;
        };
        for (index, line) in source.lines().enumerate() {
            for tool in UNPROVISIONED_HOST_TOOLS {
                if line.contains(&format!("Command::new(\"{tool}\")")) {
                    violations.push(format!("{}:{}: {}", file.display(), index + 1, line.trim()));
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "CI installs only ffmpeg, so these spawns fail on a runner:\n{}",
        violations.join("\n")
    );
}
