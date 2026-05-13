//! Offline CLI example for inspecting default hierarchy preset output.
//!
//! This example builds on source registration by invoking:
//! - `mediapm media add-local <path>`
//! - `mediapm media add <youtube-url>`
//! - `mediapm media add-hierarchy-default <media-id>` (once per media)
//!
//! It writes generated documents under
//! `src/mediapm/examples/.artifacts/cli-add-hierarchy-defaults/` and records
//! the resulting config paths/media ids in `manifest.json`.

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;

use mediapm::load_mediapm_document;
use serde::{Deserialize, Serialize};

/// Stable artifact-folder name for this example.
const EXAMPLE_ARTIFACT_FOLDER: &str = "cli-add-hierarchy-defaults";
/// Dummy local source file used by this example.
const DUMMY_LOCAL_SOURCE_FILE: &str = "dummy-local-video.mp4";
/// Dummy `YouTube` URL used to synthesize remote source defaults.
const DUMMY_YOUTUBE_URL: &str = "https://www.youtube.com/watch?v=dQw4w9WgXcQ";

/// Shared result alias for this example.
type ExampleResult<T> = Result<T, Box<dyn Error>>;

/// Manifest emitted by this example for downstream assertions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AddHierarchyDefaultsManifest {
    /// Artifact root used by this run.
    artifact_root: PathBuf,
    /// Path to `manifest.json`.
    manifest_path: PathBuf,
    /// Path to generated `mediapm.ncl`.
    mediapm_ncl: PathBuf,
    /// Path to generated conductor user document.
    conductor_user_ncl: PathBuf,
    /// Path to generated conductor machine document.
    conductor_machine_ncl: PathBuf,
    /// Local media id returned by `media add-local`.
    local_media_id: String,
    /// Remote media id returned by `media add`.
    remote_media_id: String,
    /// Number of hierarchy nodes after default-preset insertion.
    hierarchy_node_count: usize,
}

/// Returns workspace root by walking up from this crate directory.
fn workspace_root() -> PathBuf {
    let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    crate_root
        .parent()
        .and_then(Path::parent)
        .expect("mediapm crate should live under <workspace>/src/mediapm")
        .to_path_buf()
}

/// Returns deterministic artifact root for this example.
fn artifact_root() -> PathBuf {
    workspace_root().join("src/mediapm/examples/.artifacts").join(EXAMPLE_ARTIFACT_FOLDER)
}

/// Removes stale artifacts and recreates a clean output directory.
fn reset_artifact_root(root: &Path) -> ExampleResult<()> {
    if root.exists() {
        fs::remove_dir_all(root)?;
    }
    fs::create_dir_all(root)?;
    Ok(())
}

/// Ensures the `mediapm` CLI executable exists and returns its path.
fn ensure_mediapm_cli_binary() -> ExampleResult<PathBuf> {
    let workspace = workspace_root();
    let cli_path = workspace
        .join("target")
        .join("debug")
        .join(format!("mediapm{}", std::env::consts::EXE_SUFFIX));

    if cli_path.exists() {
        return Ok(cli_path);
    }

    let build = ProcessCommand::new("cargo")
        .arg("build")
        .arg("--package")
        .arg("mediapm")
        .arg("--bin")
        .arg("mediapm")
        .current_dir(&workspace)
        .output()?;

    if !build.status.success() {
        return Err(format!(
            "building mediapm CLI failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&build.stdout),
            String::from_utf8_lossy(&build.stderr)
        )
        .into());
    }

    if !cli_path.exists() {
        return Err(format!("mediapm CLI binary was not found at '{}'", cli_path.display()).into());
    }

    Ok(cli_path)
}

/// Executes one `mediapm` CLI command against the example artifact root.
fn run_mediapm_cli(cli_path: &Path, root: &Path, args: &[&str]) -> ExampleResult<String> {
    let offline_bin_dir = root.join(".offline-bin");
    fs::create_dir_all(&offline_bin_dir)?;

    let output = ProcessCommand::new(cli_path)
        .arg("--root")
        .arg(root)
        .args(args)
        .env("PATH", &offline_bin_dir)
        .output()?;

    if !output.status.success() {
        return Err(format!(
            "mediapm command failed: {}\nstdout:\n{}\nstderr:\n{}",
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }

    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Extracts registered media id from CLI stdout.
fn parse_registered_media_id(stdout: &str) -> Option<String> {
    stdout.lines().find_map(|line| {
        line.strip_prefix("registered media source id=")
            .or_else(|| line.strip_prefix("registered local media source id="))
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

/// Runs add-source commands and returns both generated media ids.
fn add_sources_for_hierarchy_example(
    cli_path: &Path,
    root: &Path,
) -> ExampleResult<(String, String)> {
    let local_source_path = root.join("inputs").join(DUMMY_LOCAL_SOURCE_FILE);
    fs::create_dir_all(local_source_path.parent().expect("local source parent"))?;
    fs::write(&local_source_path, b"dummy-local-video-bytes")?;

    let add_local_stdout = run_mediapm_cli(
        cli_path,
        root,
        &["media", "add-local", &local_source_path.to_string_lossy()],
    )?;
    let local_media_id = parse_registered_media_id(&add_local_stdout)
        .ok_or_else(|| "missing media id in add-local command output".to_string())?;

    let add_remote_stdout = run_mediapm_cli(cli_path, root, &["media", "add", DUMMY_YOUTUBE_URL])?;
    let remote_media_id = parse_registered_media_id(&add_remote_stdout)
        .ok_or_else(|| "missing media id in add command output".to_string())?;

    Ok((local_media_id, remote_media_id))
}

/// Runs the hierarchy-default example flow and persists output manifest.
fn run_cli_add_hierarchy_defaults_example() -> ExampleResult<AddHierarchyDefaultsManifest> {
    let root = artifact_root();
    reset_artifact_root(&root)?;

    let cli_path = ensure_mediapm_cli_binary()?;
    let (local_media_id, remote_media_id) = add_sources_for_hierarchy_example(&cli_path, &root)?;

    run_mediapm_cli(&cli_path, &root, &["media", "add-hierarchy-default", &local_media_id])?;
    run_mediapm_cli(&cli_path, &root, &["media", "add-hierarchy-default", &remote_media_id])?;

    let mediapm_ncl = root.join("mediapm.ncl");
    let conductor_user_ncl = root.join("mediapm.conductor.ncl");
    let conductor_machine_ncl = root.join("mediapm.conductor.machine.ncl");
    let manifest_path = root.join("manifest.json");

    let document = load_mediapm_document(&mediapm_ncl)?;
    let manifest = AddHierarchyDefaultsManifest {
        artifact_root: root,
        manifest_path: manifest_path.clone(),
        mediapm_ncl,
        conductor_user_ncl,
        conductor_machine_ncl,
        local_media_id,
        remote_media_id,
        hierarchy_node_count: document.hierarchy.len(),
    };

    fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;

    Ok(manifest)
}

/// Runs the offline hierarchy-default example and prints artifact locations.
fn main() -> ExampleResult<()> {
    let manifest = run_cli_add_hierarchy_defaults_example()?;

    println!("manifest: {}", manifest.manifest_path.display());
    println!("mediapm.ncl: {}", manifest.mediapm_ncl.display());
    println!("conductor user: {}", manifest.conductor_user_ncl.display());
    println!("conductor machine: {}", manifest.conductor_machine_ncl.display());
    println!("local media id: {}", manifest.local_media_id);
    println!("remote media id: {}", manifest.remote_media_id);
    println!("hierarchy node count: {}", manifest.hierarchy_node_count);

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;

    use mediapm::load_mediapm_document;
    use mediapm_conductor::{decode_machine_document, decode_user_document};

    use super::run_cli_add_hierarchy_defaults_example;
    use mediapm::HierarchyNodeKind;

    /// Verifies hierarchy-default CLI preset inserts one media node per source.
    #[test]
    fn cli_add_hierarchy_defaults_writes_expected_hierarchy_nodes() {
        let manifest = run_cli_add_hierarchy_defaults_example()
            .expect("run cli add-hierarchy-defaults example");

        assert!(manifest.mediapm_ncl.exists(), "mediapm config should exist");
        assert!(manifest.conductor_user_ncl.exists(), "conductor user config should exist");
        assert!(manifest.conductor_machine_ncl.exists(), "conductor machine config should exist");

        let document = load_mediapm_document(&manifest.mediapm_ncl).expect("load mediapm.ncl");
        assert_eq!(document.hierarchy.len(), 2, "example should add two hierarchy nodes");

        let expected_media_ids: BTreeSet<_> =
            [manifest.local_media_id.clone(), manifest.remote_media_id.clone()]
                .into_iter()
                .collect();

        let observed_media_ids: BTreeSet<_> = document
            .hierarchy
            .iter()
            .map(|node| {
                let media_id = node
                    .media_id
                    .as_deref()
                    .expect("default preset should set media_id")
                    .to_string();
                assert_eq!(
                    node.kind,
                    HierarchyNodeKind::Media,
                    "default preset should emit media-kind hierarchy node"
                );
                assert_eq!(
                    node.path,
                    format!(
                        "music videos/{media_id} - ${{media.metadata.title}} [${{media.id}}]${{media.metadata.video_ext}}"
                    ),
                    "default preset should keep stable path template with media-id prefix"
                );
                assert_eq!(
                    node.variant.as_deref(),
                    Some("default"),
                    "default preset should select default output variant"
                );
                media_id
            })
            .collect();

        assert_eq!(observed_media_ids, expected_media_ids);

        let user_bytes =
            fs::read(&manifest.conductor_user_ncl).expect("read conductor user config");
        let machine_bytes =
            fs::read(&manifest.conductor_machine_ncl).expect("read conductor machine config");

        let _user = decode_user_document(&user_bytes).expect("decode conductor user config");
        let _machine =
            decode_machine_document(&machine_bytes).expect("decode conductor machine config");
    }
}
