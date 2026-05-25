//! Offline example for adding managed tool requirements without downloading tool binaries.
//!
//! The example bootstraps a clean `mediapm` workspace, leaves `media` empty,
//! populates the `tools` block with every managed tool in the default mediapm
//! stack, and writes dummy tool payload files so the conductor machine config
//! can show concrete `content_map` entries without fetching real releases.

use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

use mediapm::{
    MediaPmService, ToolRequirement, ToolRequirementDependencies, load_mediapm_document,
    save_mediapm_document,
};
use mediapm_cas::Hash;
use mediapm_conductor::{
    ExternalContentRef, MachineNickelDocument, ToolConfigSpec, ToolKindSpec, ToolSpec,
    decode_machine_document, encode_machine_document,
};
use serde::{Deserialize, Serialize};

/// Stable artifact-folder name for this example.
const EXAMPLE_ARTIFACT_FOLDER: &str = "cli-add-tools";

/// Managed tool names demonstrated by this example.
const TOOL_NAMES: [&str; 5] = ["yt-dlp", "ffmpeg", "rsgain", "sd", "media-tagger"];

/// Shared result alias for this example.
type ExampleResult<T> = Result<T, Box<dyn Error>>;

/// Manifest emitted by this example for downstream assertions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AddToolsManifest {
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
    /// Logical tool names written into `mediapm.ncl`.
    logical_tool_names: Vec<String>,
    /// Immutable tool ids written into the machine document.
    tool_ids: Vec<String>,
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

/// Creates one deterministic dummy tool payload file and returns its path + hash.
fn write_dummy_tool_payload(root: &Path, tool_name: &str) -> ExampleResult<(PathBuf, Hash)> {
    let tool_dir = root.join("dummy-tools").join(tool_name);
    fs::create_dir_all(&tool_dir)?;
    let payload_path = tool_dir.join("tool.bin");
    let payload = format!("dummy tool payload for {tool_name}\n");
    fs::write(&payload_path, payload.as_bytes())?;
    Ok((payload_path, Hash::from_content(payload.as_bytes())))
}

/// Returns one stable immutable tool id used by this example.
fn tool_id_for(logical_tool_name: &str) -> String {
    format!("mediapm.tools.{}+demo@latest", logical_tool_name.trim().to_ascii_lowercase())
}

/// Builds one tool requirement for the example `mediapm.ncl` document.
fn tool_requirement_for(logical_tool_name: &str) -> ToolRequirement {
    let dependencies = match logical_tool_name {
        "yt-dlp" | "media-tagger" => ToolRequirementDependencies {
            ffmpeg_version: Some("inherit".to_string()),
            sd_version: None,
        },
        "rsgain" => ToolRequirementDependencies {
            ffmpeg_version: Some("inherit".to_string()),
            sd_version: Some("inherit".to_string()),
        },
        _ => ToolRequirementDependencies::default(),
    };

    ToolRequirement {
        version: None,
        tag: Some("latest".to_string()),
        dependencies,
        recheck_seconds: None,
        max_input_slots: None,
        max_output_slots: None,
    }
}

/// Ensures the `mediapm` runtime docs exist, then writes a tools-only example state.
async fn run_add_tools_example() -> ExampleResult<AddToolsManifest> {
    let root = artifact_root();
    reset_artifact_root(&root)?;

    let service = MediaPmService::new_in_memory_at(&root);
    let _ = service.sync_tools().await?;

    let mut document = load_mediapm_document(&service.paths().mediapm_ncl)?;
    document.media.clear();
    document.tools = TOOL_NAMES
        .iter()
        .map(|name| (name.to_string(), tool_requirement_for(name)))
        .collect::<BTreeMap<_, _>>();
    save_mediapm_document(&service.paths().mediapm_ncl, &document)?;

    let mut machine: MachineNickelDocument =
        decode_machine_document(fs::read(&service.paths().conductor_machine_ncl)?.as_slice())?;

    let mut tool_ids = Vec::new();
    for logical_tool_name in TOOL_NAMES {
        let tool_id = tool_id_for(logical_tool_name);
        tool_ids.push(tool_id.clone());

        let (payload_path, payload_hash) = write_dummy_tool_payload(&root, logical_tool_name)?;
        let relative_payload_path = payload_path
            .strip_prefix(&root)
            .expect("dummy tool path should stay under artifact root")
            .to_string_lossy()
            .replace('\\', "/");

        machine.external_data.insert(
            payload_hash,
            ExternalContentRef {
                description: Some(format!("dummy payload for {logical_tool_name}")),
                save: None,
            },
        );

        let tool_spec = ToolSpec {
            kind: ToolKindSpec::Executable {
                command: vec![relative_payload_path.clone()],
                env_vars: BTreeMap::new(),
                success_codes: vec![0],
            },
            ..ToolSpec::default()
        };
        machine.tools.insert(tool_id.clone(), tool_spec);

        let tool_config = ToolConfigSpec {
            description: Some(format!("dummy managed tool config for {logical_tool_name}")),
            content_map: Some(BTreeMap::from([(relative_payload_path, payload_hash)])),
            ..ToolConfigSpec::default()
        };
        machine.tool_configs.insert(tool_id, tool_config);
    }

    fs::write(&service.paths().conductor_machine_ncl, encode_machine_document(machine)?)?;

    let mediapm_ncl = service.paths().mediapm_ncl.clone();
    let conductor_user_ncl = service.paths().conductor_user_ncl.clone();
    let conductor_machine_ncl = service.paths().conductor_machine_ncl.clone();
    let manifest_path = root.join("manifest.json");

    let manifest = AddToolsManifest {
        artifact_root: root,
        manifest_path: manifest_path.clone(),
        mediapm_ncl,
        conductor_user_ncl,
        conductor_machine_ncl,
        logical_tool_names: TOOL_NAMES.iter().map(|value| (*value).to_string()).collect(),
        tool_ids,
    };

    fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;

    Ok(manifest)
}

/// Runs the offline add-tools example and prints artifact locations.
#[tokio::main]
async fn main() -> ExampleResult<()> {
    let manifest = run_add_tools_example().await?;

    println!("manifest: {}", manifest.manifest_path.display());
    println!("mediapm.ncl: {}", manifest.mediapm_ncl.display());
    println!("conductor user: {}", manifest.conductor_user_ncl.display());
    println!("conductor machine: {}", manifest.conductor_machine_ncl.display());
    println!("logical tools: {}", manifest.logical_tool_names.join(", "));
    println!("tool ids: {}", manifest.tool_ids.join(", "));

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use mediapm::load_mediapm_document;
    use mediapm_conductor::decode_machine_document;

    use super::run_add_tools_example;

    /// Verifies the example writes a tools-only mediapm document and dummy tool payloads.
    #[tokio::test]
    async fn add_tools_writes_expected_config_documents() {
        let manifest = run_add_tools_example().await.expect("run add-tools example");

        assert!(manifest.mediapm_ncl.exists(), "mediapm config should exist");
        assert!(manifest.conductor_user_ncl.exists(), "conductor user config should exist");
        assert!(manifest.conductor_machine_ncl.exists(), "conductor machine config should exist");

        let document = load_mediapm_document(&manifest.mediapm_ncl).expect("load mediapm.ncl");
        assert!(document.media.is_empty(), "tools example should leave media empty");
        assert_eq!(
            document.tools.len(),
            manifest.logical_tool_names.len(),
            "tools example should register every managed tool requirement"
        );

        let machine_bytes = fs::read(&manifest.conductor_machine_ncl).expect("read machine doc");
        let machine = decode_machine_document(&machine_bytes).expect("decode machine doc");

        for tool_id in &manifest.tool_ids {
            assert!(machine.tools.contains_key(tool_id), "expected tool '{tool_id}'");
            let config =
                machine.tool_configs.get(tool_id).expect("expected tool config for dummy tool");
            assert!(config.content_map.as_ref().is_some_and(|entries| !entries.is_empty()));
        }
    }
}
