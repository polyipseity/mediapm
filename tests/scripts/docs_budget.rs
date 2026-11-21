//! Budget gates for agent-facing documentation.
//!
//! These files are loaded into every agent session, so their size is a direct
//! context cost. The budgets are intentionally tight: regrowth must fail CI.
//!
//! Measured starting point when these gates were added: `.agents/instructions/`
//! was 417,406 B across 30 files, and the workspace/root + `src/**/AGENTS.md`
//! files were 226,291 B across 15 files.

use std::fs;
use std::path::{Path, PathBuf};

const INSTRUCTION_BUDGET: usize = 100_000;
const INSTRUCTION_FILE_CAP: usize = 16_000;
const AGENTS_BUDGET: usize = 50_000;
const AGENTS_FILE_CAP: usize = 12_000;

/// Returns the repository root (the directory containing `Cargo.toml` and
/// `.agents/`), derived from this crate's manifest directory.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("tests crate lives under the repository root")
        .to_path_buf()
}

/// Recursively collects files under `dir` whose name matches `predicate`.
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

/// Returns the byte length of `path`.
fn file_len(path: &Path) -> usize {
    fs::metadata(path)
        .unwrap_or_else(|err| panic!("failed to read metadata for {}: {err}", path.display()))
        .len()
        .try_into()
        .unwrap_or_else(|err| panic!("file size does not fit usize for {}: {err}", path.display()))
}

/// Renders a path relative to the repository root when possible.
fn display_path(path: &Path, root: &Path) -> String {
    path.strip_prefix(root).unwrap_or(path).display().to_string().replace('\\', "/")
}

/// Asserts every file is within `file_cap` and the total is within `budget`.
fn assert_budget(set_name: &str, files: &[PathBuf], root: &Path, budget: usize, file_cap: usize) {
    let mut total: usize = 0;
    for path in files {
        let size = file_len(path);
        total = total.saturating_add(size);
        assert!(
            size <= file_cap,
            "{set_name}: {} is {size} B, per-file cap is {file_cap} B, overage is {} B",
            display_path(path, root),
            size - file_cap,
        );
    }
    assert!(
        total <= budget,
        "{set_name} total exceeds budget: total is {total} B, budget is {budget} B, overage is {} B across {} files",
        total - budget,
        files.len(),
    );
}

#[test]
fn instruction_files_stay_within_budget() {
    let root = repo_root();
    let instructions_dir = root.join(".agents").join("instructions");
    let mut files = Vec::new();
    collect_files(
        &instructions_dir,
        &|path| path.extension().is_some_and(|ext| ext == "md"),
        &mut files,
    );
    files.sort();
    assert!(
        !files.is_empty(),
        "instruction files budget guard found no files under {}",
        instructions_dir.display()
    );
    assert_budget("instruction files", &files, &root, INSTRUCTION_BUDGET, INSTRUCTION_FILE_CAP);
}

#[test]
fn agents_files_stay_within_budget() {
    let root = repo_root();
    let mut files = Vec::new();

    let workspace_agents = root.join("AGENTS.md");
    assert!(
        workspace_agents.is_file(),
        "AGENTS.md budget guard missing workspace file {}",
        workspace_agents.display()
    );
    files.push(workspace_agents.clone());

    let src_dir = root.join("src");
    collect_files(
        &src_dir,
        &|path| path.file_name().is_some_and(|name| name == "AGENTS.md"),
        &mut files,
    );
    files.sort();
    files.dedup();

    let src_agents_count = files.iter().filter(|path| *path != &workspace_agents).count();
    assert!(
        src_agents_count >= 1,
        "AGENTS.md budget guard found no src/**/AGENTS.md files under {}",
        src_dir.display()
    );

    assert_budget("AGENTS.md files", &files, &root, AGENTS_BUDGET, AGENTS_FILE_CAP);
}
