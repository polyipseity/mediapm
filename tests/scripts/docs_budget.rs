//! Combined budget gate for agent-facing documentation.
//!
//! Project instructions and AGENTS.md files all load into agent sessions, so
//! their combined byte count is the context cost this gate controls. The limit
//! is `100_000` bytes with no per-file cap.

use std::fs;
use std::path::{Path, PathBuf};

const COMBINED_BUDGET: usize = 100_000;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("tests crate lives under the repository root")
        .to_path_buf()
}

fn collect_files(dir: &Path, predicate: &dyn Fn(&Path) -> bool, out: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(dir)
        .unwrap_or_else(|err| panic!("failed to read directory {}: {err}", dir.display()));
    for entry in entries {
        let entry = entry.unwrap_or_else(|err| {
            panic!("failed to read directory entry in {}: {err}", dir.display())
        });
        let path = entry.path();
        let file_type = entry
            .file_type()
            .unwrap_or_else(|err| panic!("failed to read file type for {}: {err}", path.display()));
        if file_type.is_dir() {
            collect_files(&path, predicate, out);
        } else if file_type.is_file() && predicate(&path) {
            out.push(path);
        }
    }
}

fn display_path(path: &Path, root: &Path) -> String {
    path.strip_prefix(root).unwrap_or(path).display().to_string().replace('\\', "/")
}

#[test]
fn agent_docs_stay_within_combined_budget() {
    let root = repo_root();
    let mut files = Vec::new();
    collect_files(
        &root.join(".agents").join("instructions"),
        &|path| path.extension().is_some_and(|ext| ext == "md"),
        &mut files,
    );
    collect_files(
        &root.join("src"),
        &|path| path.file_name().is_some_and(|name| name == "AGENTS.md"),
        &mut files,
    );

    let workspace_agents = root.join("AGENTS.md");
    assert!(
        workspace_agents.is_file(),
        "combined docs gate missing workspace file {}",
        workspace_agents.display()
    );
    files.push(workspace_agents);

    files.sort();
    files.dedup();
    assert!(files.len() > 1, "combined docs gate found too few files: {}", files.len());

    let mut total = 0usize;
    let mut largest = (0usize, PathBuf::new());
    for path in &files {
        let size = fs::metadata(path)
            .unwrap_or_else(|err| panic!("failed to read metadata for {}: {err}", path.display()))
            .len()
            .try_into()
            .unwrap_or_else(|err| {
                panic!("file size does not fit usize for {}: {err}", path.display())
            });
        total = total.saturating_add(size);
        if size > largest.0 {
            largest = (size, path.clone());
        }
    }

    assert!(
        total <= COMBINED_BUDGET,
        "agent docs total exceeds budget: total is {total} B, budget is {COMBINED_BUDGET} B, \
         overage is {} B across {} files; largest is {} at {}",
        total - COMBINED_BUDGET,
        files.len(),
        largest.0,
        display_path(&largest.1, &root),
    );
}
