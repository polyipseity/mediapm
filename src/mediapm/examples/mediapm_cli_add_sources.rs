//! Offline CLI example for inspecting default `media add` source outputs.
//!
//! This example intentionally uses the real `mediapm` CLI commands:
//! - `mediapm media add --preset local <path>`
//! - `mediapm media add --preset yt-dlp <youtube-url>`
//!
//! It writes all generated documents under
//! `src/mediapm/examples/.artifacts/cli-add-sources/` and emits a small
//! `manifest.json` that points to the resulting `mediapm` and conductor
//! config files for inspection.

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;

use serde::{Deserialize, Serialize};

/// Stable artifact-folder name for this example.
const EXAMPLE_ARTIFACT_FOLDER: &str = "cli-add-sources";
/// Dummy local source file name used by this example.
const DUMMY_LOCAL_SOURCE_FILE: &str = "dummy-local-video.mp4";
/// Dummy `YouTube` URL used to exercise remote-add defaults.
///
/// The command runs with a sandboxed `PATH` so metadata probes cannot invoke
/// external downloader binaries in test environments.
const DUMMY_YOUTUBE_URL: &str = "https://www.youtube.com/watch?v=dQw4w9WgXcQ";

/// Shared result alias for this example.
type ExampleResult<T> = Result<T, Box<dyn Error>>;

/// Manifest emitted by this example for downstream test assertions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AddSourcesManifest {
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
    /// Media id returned by `media add --preset local`.
    local_media_id: String,
    /// Media id returned by `media add --preset yt-dlp`.
    remote_media_id: String,
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

/// Runs the example flow and persists output manifest/config files.
fn run_cli_add_sources_example() -> ExampleResult<AddSourcesManifest> {
    let root = artifact_root();
    reset_artifact_root(&root)?;

    let cli_path = ensure_mediapm_cli_binary()?;

    let local_source_path = root.join("inputs").join(DUMMY_LOCAL_SOURCE_FILE);
    fs::create_dir_all(local_source_path.parent().expect("local source parent"))?;
    fs::write(&local_source_path, b"dummy-local-video-bytes")?;

    let add_local_stdout = run_mediapm_cli(
        &cli_path,
        &root,
        &["media", "add", "--preset", "local", &local_source_path.to_string_lossy()],
    )?;
    let local_media_id = parse_registered_media_id(&add_local_stdout)
        .ok_or_else(|| "missing media id in media add --preset local output".to_string())?;

    let add_remote_stdout = run_mediapm_cli(
        &cli_path,
        &root,
        &["media", "add", "--preset", "yt-dlp", DUMMY_YOUTUBE_URL],
    )?;
    let remote_media_id = parse_registered_media_id(&add_remote_stdout)
        .ok_or_else(|| "missing media id in media add --preset yt-dlp output".to_string())?;

    let mediapm_ncl = root.join("mediapm.ncl");
    let conductor_user_ncl = root.join("mediapm.conductor.ncl");
    let conductor_machine_ncl = root.join("mediapm.conductor.machine.ncl");
    let manifest_path = root.join("manifest.json");

    let manifest = AddSourcesManifest {
        artifact_root: root.clone(),
        manifest_path: manifest_path.clone(),
        mediapm_ncl,
        conductor_user_ncl,
        conductor_machine_ncl,
        local_media_id,
        remote_media_id,
    };

    fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;

    Ok(manifest)
}

/// Runs the offline add-sources example and prints artifact locations.
fn main() -> ExampleResult<()> {
    let manifest = run_cli_add_sources_example()?;

    println!("manifest: {}", manifest.manifest_path.display());
    println!("mediapm.ncl: {}", manifest.mediapm_ncl.display());
    println!("conductor user: {}", manifest.conductor_user_ncl.display());
    println!("conductor machine: {}", manifest.conductor_machine_ncl.display());
    println!("local media id: {}", manifest.local_media_id);
    println!("remote media id: {}", manifest.remote_media_id);

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use mediapm_conductor::{decode_machine_document, decode_user_document};

    use super::{DUMMY_YOUTUBE_URL, run_cli_add_sources_example};
    use mediapm::{MediaStepTool, TransformInputValue, load_mediapm_document};

    /// Verifies CLI add-source defaults populate expected mediapm and conductor docs.
    #[test]
    fn cli_add_sources_writes_expected_config_documents() {
        let manifest = run_cli_add_sources_example().expect("run cli add-sources example");

        assert!(manifest.mediapm_ncl.exists(), "mediapm config should exist");
        assert!(manifest.conductor_user_ncl.exists(), "conductor user config should exist");
        assert!(manifest.conductor_machine_ncl.exists(), "conductor machine config should exist");

        let document = load_mediapm_document(&manifest.mediapm_ncl).expect("load mediapm.ncl");
        assert_eq!(document.media.len(), 2, "example should register exactly two media sources");

        let local_source =
            document.media.get(&manifest.local_media_id).expect("local source should exist");
        let remote_source =
            document.media.get(&manifest.remote_media_id).expect("remote source should exist");

        assert_eq!(local_source.steps[0].tool, MediaStepTool::Import);
        assert_eq!(
            local_source.steps[0].options.get("kind"),
            Some(&TransformInputValue::String("cas_hash".to_string())),
            "local add should synthesize import cas-hash kind"
        );

        assert_eq!(remote_source.steps[0].tool, MediaStepTool::YtDlp);
        assert_eq!(
            remote_source.steps[0].options.get("uri"),
            Some(&TransformInputValue::String(DUMMY_YOUTUBE_URL.to_string())),
            "remote add should preserve provided URI"
        );

        let user_bytes =
            fs::read(&manifest.conductor_user_ncl).expect("read conductor user config");
        let machine_bytes =
            fs::read(&manifest.conductor_machine_ncl).expect("read conductor machine config");

        let _user = decode_user_document(&user_bytes).expect("decode conductor user config");
        let machine =
            decode_machine_document(&machine_bytes).expect("decode conductor machine config");

        let expected_workflow_ids = [
            format!("mediapm.media.{}", manifest.local_media_id),
            format!("mediapm.media.{}", manifest.remote_media_id),
        ];
        for workflow_id in expected_workflow_ids {
            assert!(
                machine.workflows.contains_key(&workflow_id),
                "conductor machine config should contain managed workflow '{workflow_id}'"
            );
        }
    }
}
