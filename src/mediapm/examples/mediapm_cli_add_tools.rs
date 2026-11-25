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
    ConfigVersionSpec, MediaPmService, MediaRuntimeStorage, ToolRequirement, example_isolation,
    load_mediapm_document, save_mediapm_document,
};
use mediapm_cas::Hash;
use mediapm_conductor::{
    ConfigPayloadStaging, NickelDocument, OutputSaveMode, ToolKindSpec, ToolRuntime, ToolSpec,
    config::ExternalDataEntry, decode_document, encode_document,
};

const EXAMPLE_ARTIFACT_FOLDER: &str = "cli-add-tools";
const TOOL_NAMES: [&str; 6] = ["yt-dlp", "ffmpeg", "deno", "rsgain", "sd", "media-tagger"];

/// Runtime storage for example runs; the user-level tool cache resolves to a
/// hermetic tempdir when [`example_isolation::CACHE_ROOT_ENV`] is set
/// (examples-as-tests) and to the real OS user-level cache otherwise
/// (explicit `cargo run --example` runs).
fn example_runtime_storage() -> MediaRuntimeStorage {
    MediaRuntimeStorage {
        cache_root_override: Some(example_cache_root()),
        ..MediaRuntimeStorage::default()
    }
}

/// Example user-level tool download cache root, honoring
/// [`example_isolation::CACHE_ROOT_ENV`] (hermetic examples-as-test runs) and
/// otherwise the real persistent OS user-level cache, so explicit runs
/// persist downloaded tools across runs.
fn example_cache_root() -> PathBuf {
    example_isolation::user_level_cache_root()
}

type ExampleResult<T> = Result<T, Box<dyn Error>>;

fn workspace_root() -> PathBuf {
    let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    crate_root
        .parent()
        .and_then(Path::parent)
        .expect("mediapm crate should live under <workspace>/src/mediapm")
        .to_path_buf()
}

fn artifact_root() -> PathBuf {
    match std::env::var_os(example_isolation::ARTIFACT_ROOT_ENV) {
        Some(root) => PathBuf::from(root),
        None => {
            workspace_root().join("src/mediapm/examples/artifacts").join(EXAMPLE_ARTIFACT_FOLDER)
        }
    }
}

fn reset_artifact_root(root: &Path) -> ExampleResult<()> {
    if root.exists() {
        example_isolation::remove_dir_all_with_retry(root)?;
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
    let dependencies: BTreeMap<String, ConfigVersionSpec> = match logical_tool_name {
        "yt-dlp" => BTreeMap::from([
            ("ffmpeg".to_string(), ConfigVersionSpec::Inherit),
            ("deno".to_string(), ConfigVersionSpec::Inherit),
        ]),
        "media-tagger" => BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Inherit)]),
        "rsgain" => BTreeMap::from([
            ("ffmpeg".to_string(), ConfigVersionSpec::Inherit),
            ("sd".to_string(), ConfigVersionSpec::Inherit),
        ]),
        _ => BTreeMap::new(),
    };

    ToolRequirement {
        version_spec: ConfigVersionSpec::Latest,
        dependencies,
        recheck_seconds: mediapm::config::ToolRecheck::Always,
        max_input_slots: 16,
        max_output_slots: 4,
        payload_staging: ConfigPayloadStaging::Inherit,
    }
}

/// Populates the managed-tool requirements this example documents.
///
/// Shuts the conductor the service owns down before returning:
/// `Conductor::shutdown` awaits the background pass that would otherwise write
/// into the caller's removed directory.
async fn run_add_tools_example() -> ExampleResult<()> {
    let root = artifact_root();
    reset_artifact_root(&root)?;

    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(&root, example_runtime_storage())
            .await?;
    let _ = service.sync_tools().await?;

    let paths = service.paths();
    let mut document = load_mediapm_document(&paths.mediapm_ncl)?;
    document.media.clear();
    document.tools = TOOL_NAMES
        .iter()
        .map(|name| (name.to_string(), tool_requirement_for(name)))
        .collect::<BTreeMap<_, _>>();
    save_mediapm_document(&paths.mediapm_ncl, &document)?;

    let machine_bytes = fs::read(&paths.conductor_generated_ncl)?;
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

        // Declare the dummy payload hash so the machine doc satisfies the
        // `content_map ⊆ external_data` invariant enforced at encode time.
        machine.external_data.insert(
            payload_hash,
            ExternalDataEntry {
                description: Some(format!("dummy tool payload for {logical_tool_name}")),
                save_mode: OutputSaveMode::Saved,
            },
        );

        machine.tools.insert(
            tool_id.clone(),
            ToolSpec {
                version: None,
                name: tool_id.clone(),
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
                    // The fabricated payload lives in CAS and no provisioned
                    // directory names it, so the document stages it by copy.
                    payload_staging: ConfigPayloadStaging::Copy,
                    ..ToolRuntime::default()
                },
                ..ToolSpec::default()
            },
        );
    }

    fs::write(&paths.conductor_generated_ncl, encode_document(machine)?)?;

    service.conductor().shutdown().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> ExampleResult<()> {
    run_add_tools_example().await?;
    let root = artifact_root();
    let tool_ids = TOOL_NAMES.iter().map(|name| tool_id_for(name)).collect::<Vec<_>>();

    println!("mediapm config: {}", root.join("mediapm.ncl").display());
    println!("logical tools: {}", TOOL_NAMES.join(", "));
    println!("tool ids: {}", tool_ids.join(", "));

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use mediapm::{example_isolation::IsolatedExampleRoots, load_mediapm_document};
    use mediapm_conductor::{NickelDocument, decode_document};

    use super::run_add_tools_example;

    #[tokio::test]
    async fn add_tools_writes_expected_config_documents() {
        let isolated = IsolatedExampleRoots::with_cache();
        run_add_tools_example().await.expect("run add-tools example");
        let root = super::artifact_root();

        assert!(
            !root.join("manifest.json").exists(),
            "add-tools manifest is redundant; the config documents already record the tools"
        );
        assert!(
            isolated.cache_root_path().expect("cache root").join("store").exists(),
            "tool sync should have used the isolated user cache root"
        );

        let mediapm_ncl = root.join("mediapm.ncl");
        let conductor_generated_ncl = root.join("mediapm.conductor.generated.ncl");
        assert!(mediapm_ncl.exists(), "mediapm config should exist");
        assert!(conductor_generated_ncl.exists(), "conductor generated config should exist");

        let document = load_mediapm_document(&mediapm_ncl).expect("load mediapm.ncl");
        assert!(document.media.is_empty(), "tools example should leave media empty");
        assert_eq!(
            document.tools.len(),
            super::TOOL_NAMES.len(),
            "tools example should register every managed tool requirement"
        );

        let machine_bytes = fs::read(&conductor_generated_ncl).expect("read machine doc");
        let machine: NickelDocument = decode_document(&machine_bytes).expect("decode machine doc");

        for logical_tool_name in super::TOOL_NAMES {
            let tool_id = super::tool_id_for(logical_tool_name);
            let tool = machine.tools.get(&tool_id).expect("expected tool '{tool_id}'");
            assert!(
                !tool.runtime.content_map.is_empty(),
                "expected content map entries for dummy tool '{tool_id}'"
            );
        }
    }

    /// Ensures the documented CLI entry point runs end to end via `main()`.
    #[test]
    fn main_is_exercised() {
        let _isolated = IsolatedExampleRoots::with_cache();
        super::main().expect("example main should run to completion");
    }
}
