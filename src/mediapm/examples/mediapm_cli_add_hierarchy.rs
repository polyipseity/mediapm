//! Offline example for adding hierarchy presets on top of registered media sources.
//!
//! The example bootstraps a clean `mediapm` workspace, registers one local
//! and one online media source, applies the local and yt-dlp hierarchy presets,
//! and writes a small manifest that records the resulting document locations.

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::time::{SystemTime, UNIX_EPOCH};

use mediapm::{MediaHierarchyPreset, MediaPmService, load_mediapm_document};
use serde::{Deserialize, Serialize};
use url::Url;

/// Stable artifact-folder name for this example.
const EXAMPLE_ARTIFACT_FOLDER: &str = "cli-add-hierarchy";
/// Dummy local source file used by this example.
const DUMMY_LOCAL_SOURCE_FILE: &str = "dummy-local-video.mp4";
/// Dummy `YouTube` URL used to synthesize remote source defaults.
const DUMMY_YOUTUBE_URL: &str = "https://www.youtube.com/watch?v=dQw4w9WgXcQ";
/// Folder root used for local hierarchy preset insertion.
const LOCAL_HIERARCHY_FOLDER: &str = "music videos/local";
/// Folder root used for yt-dlp hierarchy preset insertion.
const YT_DLP_HIERARCHY_FOLDER: &str = "music videos/online";

/// Shared result alias for this example.
type ExampleResult<T> = Result<T, Box<dyn Error>>;

/// Manifest emitted by this example for downstream assertions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AddHierarchyManifest {
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
    /// Local media id returned by `add_local_source`.
    local_media_id: String,
    /// Remote media id returned by `add_media_source`.
    remote_media_id: String,
    /// Number of hierarchy nodes after default-preset insertion.
    hierarchy_node_count: usize,
    /// Folder root used for local hierarchy preset insertion.
    local_hierarchy_folder: String,
    /// Folder root used for yt-dlp hierarchy preset insertion.
    yt_dlp_hierarchy_folder: String,
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
    let base = workspace_root().join("src/mediapm/examples/.artifacts");

    #[cfg(test)]
    {
        let pid = std::process::id();
        let stamp =
            SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |duration| duration.as_nanos());
        return base.join(format!("{EXAMPLE_ARTIFACT_FOLDER}-test-{pid}-{stamp}"));
    }

    #[cfg(not(test))]
    {
        base.join(EXAMPLE_ARTIFACT_FOLDER)
    }
}

/// Removes stale artifacts and recreates a clean output directory.
fn reset_artifact_root(root: &Path) -> ExampleResult<()> {
    if root.exists() {
        fs::remove_dir_all(root)?;
    }
    fs::create_dir_all(root)?;
    Ok(())
}

/// Writes the dummy local media file used for source registration.
fn write_dummy_local_source(root: &Path) -> ExampleResult<PathBuf> {
    let local_source_path = root.join("inputs").join(DUMMY_LOCAL_SOURCE_FILE);
    fs::create_dir_all(local_source_path.parent().expect("local source parent"))?;
    fs::write(&local_source_path, b"dummy-local-video-bytes")?;
    Ok(local_source_path)
}

/// Runs the example flow and persists output manifest/config files.
async fn run_add_hierarchy_example() -> ExampleResult<AddHierarchyManifest> {
    let root = artifact_root();
    reset_artifact_root(&root)?;

    let service = MediaPmService::new_in_memory_at(&root);

    let local_source_path = write_dummy_local_source(&root)?;
    let local_media_id = service.add_local_source(&local_source_path, None, None).await?;
    let remote_media_id =
        service.add_media_source(&Url::parse(DUMMY_YOUTUBE_URL)?, None, None).await?;

    service.add_media_hierarchy_preset(
        MediaHierarchyPreset::Local,
        &local_media_id,
        LOCAL_HIERARCHY_FOLDER,
    )?;
    service.add_media_hierarchy_preset(
        MediaHierarchyPreset::YtDlp,
        &remote_media_id,
        YT_DLP_HIERARCHY_FOLDER,
    )?;

    let mediapm_ncl = service.paths().mediapm_ncl.clone();
    let conductor_user_ncl = service.paths().conductor_user_ncl.clone();
    let conductor_machine_ncl = service.paths().conductor_machine_ncl.clone();
    let manifest_path = root.join("manifest.json");

    let document = load_mediapm_document(&mediapm_ncl)?;
    let manifest = AddHierarchyManifest {
        artifact_root: root,
        manifest_path: manifest_path.clone(),
        mediapm_ncl,
        conductor_user_ncl,
        conductor_machine_ncl,
        local_media_id,
        remote_media_id,
        hierarchy_node_count: document.hierarchy.len(),
        local_hierarchy_folder: LOCAL_HIERARCHY_FOLDER.to_string(),
        yt_dlp_hierarchy_folder: YT_DLP_HIERARCHY_FOLDER.to_string(),
    };

    fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;

    Ok(manifest)
}

/// Runs the offline hierarchy-preset example and prints artifact locations.
#[tokio::main]
async fn main() -> ExampleResult<()> {
    let manifest = run_add_hierarchy_example().await?;

    println!("manifest: {}", manifest.manifest_path.display());
    println!("mediapm.ncl: {}", manifest.mediapm_ncl.display());
    println!("conductor user: {}", manifest.conductor_user_ncl.display());
    println!("conductor machine: {}", manifest.conductor_machine_ncl.display());
    println!("local media id: {}", manifest.local_media_id);
    println!("remote media id: {}", manifest.remote_media_id);
    println!("local hierarchy folder: {}", manifest.local_hierarchy_folder);
    println!("yt-dlp hierarchy folder: {}", manifest.yt_dlp_hierarchy_folder);
    println!("hierarchy node count: {}", manifest.hierarchy_node_count);

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::fs;

    use mediapm::{HierarchyNodeKind, MediaMetadataValue, MediaStepTool, load_mediapm_document};
    use mediapm_conductor::decode_user_document;

    use super::run_add_hierarchy_example;

    /// Verifies local/yt-dlp hierarchy presets insert one root node each with preset-specific projections.
    #[tokio::test]
    async fn add_hierarchy_writes_expected_hierarchy_nodes() {
        let manifest = run_add_hierarchy_example().await.expect("run add-hierarchy example");

        assert!(manifest.mediapm_ncl.exists(), "mediapm config should exist");
        assert!(manifest.conductor_user_ncl.exists(), "conductor user config should exist");
        assert!(manifest.conductor_machine_ncl.exists(), "conductor machine config should exist");

        let document = load_mediapm_document(&manifest.mediapm_ncl).expect("load mediapm.ncl");
        assert_eq!(document.hierarchy.len(), 2, "example should add two hierarchy nodes");

        let observed_media_ids: BTreeSet<_> = document
            .hierarchy
            .iter()
            .map(|node| {
                assert_eq!(node.kind, HierarchyNodeKind::Folder);
                assert!(node.id.is_none(), "outer hierarchy folder should not carry an id");
                assert!(node.media_id.is_none(), "preset root folder should not carry media_id");
                node.children
                    .first()
                    .and_then(|child| child.media_id.as_deref())
                    .expect("media-root child should set media_id")
                    .to_string()
            })
            .collect();

        let expected_media_ids: BTreeSet<_> =
            [manifest.local_media_id.clone(), manifest.remote_media_id.clone()]
                .into_iter()
                .collect();
        assert_eq!(observed_media_ids, expected_media_ids);

        let remote_source =
            document.media.get(&manifest.remote_media_id).expect("remote source should exist");
        assert_eq!(
            remote_source.metadata.as_ref().and_then(|metadata| metadata.get("video_ext")),
            Some(&MediaMetadataValue::Literal(".mkv".to_string())),
            "yt-dlp hierarchy example should hardcode .mkv for video_ext"
        );

        let media_tagger_step = remote_source
            .steps
            .iter()
            .find(|step| step.tool == MediaStepTool::MediaTagger)
            .expect("remote hierarchy example should include media-tagger step");
        let rsgain_step = remote_source
            .steps
            .iter()
            .find(|step| step.tool == MediaStepTool::Rsgain)
            .expect("remote hierarchy example should include rsgain step");

        assert!(
            media_tagger_step.output_variants["video"].get("extension").is_none(),
            "media-tagger hierarchy preset should rely on inherited extension"
        );
        assert!(
            rsgain_step.output_variants["video"].get("extension").is_none(),
            "rsgain hierarchy preset should rely on inherited extension"
        );

        let hierarchy_by_folder: BTreeMap<_, _> = document
            .hierarchy
            .iter()
            .map(|node| {
                let media_root =
                    node.children.first().expect("preset root should include media root");
                assert_eq!(
                    media_root.id.as_deref(),
                    media_root.media_id.as_deref(),
                    "media-root child id should match the media id"
                );
                let variants: BTreeSet<_> = media_root
                    .children
                    .iter()
                    .flat_map(|child| {
                        let mut values = Vec::new();
                        if let Some(variant) = child.variant.clone() {
                            values.push(variant);
                        }
                        values.extend(child.variants.clone());
                        values
                    })
                    .collect();
                (node.path.clone(), variants)
            })
            .collect();

        assert_eq!(
            hierarchy_by_folder
                .get(&manifest.local_hierarchy_folder)
                .expect("local preset folder should exist"),
            &BTreeSet::from(["media".to_string()]),
            "local preset should project only the final pipeline variant"
        );
        assert_eq!(
            hierarchy_by_folder
                .get(&manifest.yt_dlp_hierarchy_folder)
                .expect("yt-dlp preset folder should exist"),
            &BTreeSet::from([
                "archive".to_string(),
                "description".to_string(),
                "infojson".to_string(),
                "links".to_string(),
                "subtitles".to_string(),
                "thumbnails".to_string(),
                "video".to_string(),
            ]),
            "yt-dlp preset should project the updated media, infojson, subtitles, thumbnails, and links variants"
        );

        let user_bytes =
            fs::read(&manifest.conductor_user_ncl).expect("read conductor user config");
        let _machine_bytes =
            fs::read(&manifest.conductor_machine_ncl).expect("read conductor machine config");

        let _user = decode_user_document(&user_bytes).expect("decode conductor user config");
        // Conductor machine workflow population requires an explicit `mediapm sync` run;
        // config-mutation tests only verify that the declarative state files are written.
    }
}
