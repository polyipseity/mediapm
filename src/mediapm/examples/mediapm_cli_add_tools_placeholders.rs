//! Offline CLI example for config-only managed-tool placeholder registration.
//!
//! This example runs `mediapm media add --preset yt-dlp <url>` against a clean
//! artifact root and verifies only configuration surfaces:
//! - `mediapm.ncl` source registration,
//! - conductor user/machine document generation,
//! - config-edit reconciliation of unresolved managed-tool placeholders.
//!
//! It intentionally does **not** run `mediapm sync` or `mediapm tools sync`, so
//! no external tool binaries are downloaded.

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;

use mediapm_conductor::decode_machine_document;
use serde::{Deserialize, Serialize};

/// Stable artifact-folder name for this example.
const EXAMPLE_ARTIFACT_FOLDER: &str = "cli-add-tools-placeholders";
/// Dummy online source used to synthesize yt-dlp preset workflows.
const DUMMY_YOUTUBE_URL: &str = "https://www.youtube.com/watch?v=dQw4w9WgXcQ";

/// Logical managed tools required by the default yt-dlp preset chain.
const REQUIRED_LOGICAL_TOOLS: [&str; 5] = ["yt-dlp", "ffmpeg", "rsgain", "sd", "media-tagger"];

/// Shared result alias for this example.
type ExampleResult<T> = Result<T, Box<dyn Error>>;

/// Manifest emitted by this example for downstream assertions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AddToolsPlaceholdersManifest {
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
    /// Media id returned by `media add --preset yt-dlp`.
    media_id: String,
    /// Logical tool names that should map to placeholder tool ids.
    required_logical_tools: Vec<String>,
    /// Placeholder tool ids expected in conductor machine config.
    placeholder_tool_ids: Vec<String>,
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

/// Builds deterministic unresolved-placeholder id for one logical tool name.
fn unresolved_placeholder_tool_id(logical_tool_name: &str) -> String {
    format!(
        "mediapm.tools.{}+mediapm-unresolved@latest",
        logical_tool_name.trim().to_ascii_lowercase()
    )
}

/// Runs the example flow and persists output manifest/config files.
fn run_cli_add_tools_placeholders_example() -> ExampleResult<AddToolsPlaceholdersManifest> {
    let root = artifact_root();
    reset_artifact_root(&root)?;

    let cli_path = ensure_mediapm_cli_binary()?;
    let add_remote_stdout = run_mediapm_cli(
        &cli_path,
        &root,
        &["media", "add", "--preset", "yt-dlp", DUMMY_YOUTUBE_URL],
    )?;
    let media_id = parse_registered_media_id(&add_remote_stdout)
        .ok_or_else(|| "missing media id in media add --preset yt-dlp output".to_string())?;

    let mediapm_ncl = root.join("mediapm.ncl");
    let conductor_user_ncl = root.join("mediapm.conductor.ncl");
    let conductor_machine_ncl = root.join("mediapm.conductor.machine.ncl");
    let manifest_path = root.join("manifest.json");

    let machine = decode_machine_document(&fs::read(&conductor_machine_ncl)?)?;
    let required_logical_tools =
        REQUIRED_LOGICAL_TOOLS.iter().map(|value| (*value).to_string()).collect::<Vec<_>>();
    let placeholder_tool_ids = required_logical_tools
        .iter()
        .map(|logical_name| unresolved_placeholder_tool_id(logical_name))
        .collect::<Vec<_>>();

    for tool_id in &placeholder_tool_ids {
        if !machine.tools.contains_key(tool_id) {
            return Err(format!(
                "conductor machine config is missing expected placeholder tool '{tool_id}'",
            )
            .into());
        }
    }

    let manifest = AddToolsPlaceholdersManifest {
        artifact_root: root,
        manifest_path: manifest_path.clone(),
        mediapm_ncl,
        conductor_user_ncl,
        conductor_machine_ncl,
        media_id,
        required_logical_tools,
        placeholder_tool_ids,
    };

    fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;

    Ok(manifest)
}

/// Runs the offline tool-placeholder example and prints artifact locations.
fn main() -> ExampleResult<()> {
    let manifest = run_cli_add_tools_placeholders_example()?;

    println!("manifest: {}", manifest.manifest_path.display());
    println!("mediapm.ncl: {}", manifest.mediapm_ncl.display());
    println!("conductor user: {}", manifest.conductor_user_ncl.display());
    println!("conductor machine: {}", manifest.conductor_machine_ncl.display());
    println!("media id: {}", manifest.media_id);
    println!("placeholder tools: {}", manifest.placeholder_tool_ids.join(", "));

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use mediapm::load_mediapm_document;
    use mediapm_conductor::decode_machine_document;

    use super::run_cli_add_tools_placeholders_example;

    /// Verifies config-only source add registers unresolved managed-tool placeholders.
    #[test]
    fn cli_add_tools_placeholders_writes_expected_config_documents() {
        let manifest = run_cli_add_tools_placeholders_example()
            .expect("run cli add-tools-placeholders example");

        assert!(manifest.mediapm_ncl.exists(), "mediapm config should exist");
        assert!(manifest.conductor_user_ncl.exists(), "conductor user config should exist");
        assert!(manifest.conductor_machine_ncl.exists(), "conductor machine config should exist");

        let document = load_mediapm_document(&manifest.mediapm_ncl).expect("load mediapm.ncl");
        assert!(
            document.media.contains_key(&manifest.media_id),
            "source add should register media id in mediapm config"
        );

        let machine_bytes =
            fs::read(&manifest.conductor_machine_ncl).expect("read conductor machine config");
        let machine =
            decode_machine_document(&machine_bytes).expect("decode conductor machine config");

        let expected_workflow_id = format!("mediapm.media.{}", manifest.media_id);
        assert!(
            machine.workflows.contains_key(&expected_workflow_id),
            "conductor machine config should contain managed workflow '{expected_workflow_id}'"
        );

        for tool_id in &manifest.placeholder_tool_ids {
            assert!(
                machine.tools.contains_key(tool_id),
                "conductor machine config should contain placeholder tool '{tool_id}'"
            );
        }
    }
}
