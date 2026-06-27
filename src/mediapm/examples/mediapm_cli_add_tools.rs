//! Example for adding managed tool requirements without downloading tool binaries.
//!
//! Bootstraps a clean `mediapm` workspace, populates the `tools` block with
//! every managed tool in the default stack, and writes dummy tool payload
//! files so the conductor machine config shows concrete `content_map` entries.

use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

use mediapm::{
    MediaMetadataValue, MediaPmService, ToolRequirement, ToolRequirementDependencies,
    load_mediapm_document, save_mediapm_document,
};
use mediapm_cas::Hash;
use mediapm_conductor::{
    NickelDocument, ToolKindSpec, ToolRuntime, ToolSpec, decode_document, encode_document,
};
use serde::{Deserialize, Serialize};

const EXAMPLE_ARTIFACT_FOLDER: &str = "cli-add-tools";
const TOOL_NAMES: [&str; 6] = ["yt-dlp", "ffmpeg", "deno", "rsgain", "sd", "media-tagger"];

type ExampleResult<T> = Result<T, Box<dyn Error>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AddToolsManifest {
    artifact_root: PathBuf,
    manifest_path: PathBuf,
    mediapm_ncl: PathBuf,
    conductor_user_ncl: PathBuf,
    conductor_machine_ncl: PathBuf,
    logical_tool_names: Vec<String>,
    tool_ids: Vec<String>,
}

fn workspace_root() -> PathBuf {
    let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    crate_root
        .parent()
        .and_then(Path::parent)
        .expect("mediapm crate should live under <workspace>/src/mediapm")
        .to_path_buf()
}

fn artifact_root() -> PathBuf {
    workspace_root().join("src/mediapm/examples/.artifacts").join(EXAMPLE_ARTIFACT_FOLDER)
}

fn reset_artifact_root(root: &Path) -> ExampleResult<()> {
    if root.exists() {
        fs::remove_dir_all(root)?;
    }
    fs::create_dir_all(root)?;
    Ok(())
}

fn write_dummy_tool_payload(root: &Path, tool_name: &str) -> ExampleResult<(PathBuf, Hash)> {
    let tool_dir = root.join("dummy-tools").join(tool_name);
    fs::create_dir_all(&tool_dir)?;
    let payload_path = tool_dir.join("tool.bin");
    let payload = format!("dummy tool payload for {tool_name}\n");
    fs::write(&payload_path, payload.as_bytes())?;
    Ok((payload_path, Hash::from_content(payload.as_bytes())))
}

fn tool_id_for(logical_tool_name: &str) -> String {
    format!("mediapm.tools.{}+demo@latest", logical_tool_name.trim().to_ascii_lowercase())
}

fn tool_requirement_for(logical_tool_name: &str) -> ToolRequirement {
    let dependencies = match logical_tool_name {
        "yt-dlp" | "media-tagger" => ToolRequirementDependencies {
            ffmpeg_version: Some(MediaMetadataValue::Literal("inherit".to_string())),
            deno_version: if logical_tool_name == "yt-dlp" {
                Some(MediaMetadataValue::Literal("inherit".to_string()))
            } else {
                None
            },
            sd_version: None,
        },
        "rsgain" => ToolRequirementDependencies {
            ffmpeg_version: Some(MediaMetadataValue::Literal("inherit".to_string())),
            deno_version: None,
            sd_version: Some(MediaMetadataValue::Literal("inherit".to_string())),
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

async fn run_add_tools_example() -> ExampleResult<AddToolsManifest> {
    let root = artifact_root();
    reset_artifact_root(&root)?;

    let mut service = MediaPmService::new_fs_at(&root)?;
    let _ = service.sync_tools()?;

    let paths = service.paths();
    let mut document = load_mediapm_document(&paths.mediapm_ncl)?;
    document.media.clear();
    document.runtime.tools = TOOL_NAMES
        .iter()
        .map(|name| (name.to_string(), tool_requirement_for(name)))
        .collect::<BTreeMap<_, _>>();
    save_mediapm_document(&paths.mediapm_ncl, &document)?;

    let machine_bytes = fs::read(&paths.conductor_machine_ncl)?;
    let mut machine: NickelDocument = decode_document(&machine_bytes)?;

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

        let (name, version) = tool_id.split_once('@').unwrap_or((&tool_id, "latest"));

        machine.tools.insert(
            name.to_string(),
            ToolSpec {
                name: name.to_string(),
                version: version.to_string(),
                kind: ToolKindSpec::Executable {
                    command: vec![relative_payload_path.clone()],
                    env_vars: BTreeMap::new(),
                    success_codes: vec![0],
                },
                runtime: ToolRuntime {
                    content_map: BTreeMap::from([(
                        relative_payload_path,
                        payload_hash.to_string(),
                    )]),
                    ..ToolRuntime::default()
                },
                ..ToolSpec::default()
            },
        );
    }

    fs::write(&paths.conductor_machine_ncl, encode_document(machine)?)?;

    let manifest_path = root.join("manifest.json");
    let manifest = AddToolsManifest {
        artifact_root: root,
        manifest_path: manifest_path.clone(),
        mediapm_ncl: paths.mediapm_ncl.clone(),
        conductor_user_ncl: paths.conductor_user_ncl.clone(),
        conductor_machine_ncl: paths.conductor_machine_ncl.clone(),
        logical_tool_names: TOOL_NAMES.iter().map(|v| (*v).to_string()).collect(),
        tool_ids,
    };

    fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;
    Ok(manifest)
}

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
    use mediapm_conductor::{NickelDocument, decode_document};

    use super::run_add_tools_example;

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
        let machine: NickelDocument = decode_document(&machine_bytes).expect("decode machine doc");

        for tool_id in &manifest.tool_ids {
            let (name, version) = tool_id.split_once('@').unwrap_or((tool_id, "latest"));
            let tool = machine
                .tools
                .iter()
                .find(|t| t.name == name && t.version == version)
                .expect("expected tool '{tool_id}'");
            assert!(
                !tool.runtime.content_map.is_empty(),
                "expected content map entries for dummy tool '{tool_id}'"
            );
        }
    }
}
