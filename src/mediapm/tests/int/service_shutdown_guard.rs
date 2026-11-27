//! Guards that service-owning tests and examples await shutdown.

use std::path::{Path, PathBuf};

/// Recursively collects Rust source files below `root`.
fn rust_files(root: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(root).expect("read source root") {
        let path = entry.expect("source entry").path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

/// Counts literal occurrences of `needle` in `source`.
fn count(source: &str, needle: &str) -> usize {
    source.match_indices(needle).count()
}

/// Each literal service owner must have at least one awaited shutdown line.
#[test]
fn service_owners_await_shutdown() {
    let package_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    rust_files(&package_root.join("src"), &mut files);
    rust_files(&package_root.join("tests"), &mut files);
    rust_files(&package_root.join("examples"), &mut files);

    let mut offenders = Vec::new();
    for path in files {
        let source = std::fs::read_to_string(&path).expect("read Rust source");
        let owners = count(&source, "MediaPmService::new_fs_at(")
            + count(&source, "service_with_cache(")
            + count(&source, "service_in_tempdir(");
        if owners > 0 {
            let distinct_shutdown_lines =
                source.lines().filter(|line| line.contains(".shutdown().await")).count();
            let helper_definition = source.contains("pub(crate) async fn service_with_cache(")
                || source.contains("pub(crate) async fn service_in_tempdir(");
            if !helper_definition && distinct_shutdown_lines < owners {
                offenders.push(format!(
                    "{}: {owners} owners but only {distinct_shutdown_lines} awaited shutdowns",
                    path.display()
                ));
            }
        }
    }

    assert!(offenders.is_empty(), "owners missing awaited shutdown: {offenders:#?}");
}
