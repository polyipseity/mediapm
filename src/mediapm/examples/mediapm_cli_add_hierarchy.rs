//! Example for adding hierarchy presets on top of registered media sources.
//!
//! Bootstraps a clean `mediapm` workspace, registers one local and one online
//! media source, applies local and yt-dlp hierarchy presets, and writes a
//! manifest recording the resulting document locations.

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

use mediapm::{
    AddInsertPosition, MediaHierarchyPreset, MediaPmService, MediaRuntimeStorage, MediaSourceSpec,
    example_isolation, load_mediapm_document,
};
use serde::{Deserialize, Serialize};
use url::Url;

const EXAMPLE_ARTIFACT_FOLDER: &str = "cli-add-hierarchy";
const DUMMY_LOCAL_SOURCE_FILE: &str = "dummy-local-video.mp4";
const DUMMY_YOUTUBE_URL: &str = "https://www.youtube.com/watch?v=dQw4w9WgXcQ";
const LOCAL_HIERARCHY_FOLDER: &str = "music videos/local";
const YT_DLP_HIERARCHY_FOLDER: &str = "music videos/online";

/// Embedded tiny MP4 payload containing both video and audio tracks.
const SAMPLE_AV_MP4_BYTES: &[u8] = include_bytes!("assets/sample-av.mp4");

type ExampleResult<T> = Result<T, Box<dyn Error>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AddHierarchyManifest {
    artifact_root: PathBuf,
    manifest_path: PathBuf,
    mediapm_ncl: PathBuf,
    conductor_user_ncl: PathBuf,
    conductor_generated_ncl: PathBuf,
    local_media_id: String,
    remote_media_id: String,
    hierarchy_node_count: usize,
    local_hierarchy_folder: String,
    yt_dlp_hierarchy_folder: String,
}

fn workspace_root() -> PathBuf {
    let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    crate_root
        .parent()
        .and_then(Path::parent)
        .expect("mediapm crate should live under <workspace>/src/mediapm")
        .to_path_buf()
}

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

/// Canonical example artifact root, honoring
/// [`example_isolation::ARTIFACT_ROOT_ENV`] and otherwise a stable
/// `examples/artifacts/<example>` folder (no per-run stamp), so repeated runs
/// reuse the same workspace instead of accumulating `{pid}-{nanos}` orphans.
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

fn write_dummy_local_source(root: &Path) -> ExampleResult<PathBuf> {
    let path = root.join("inputs").join(DUMMY_LOCAL_SOURCE_FILE);
    fs::create_dir_all(path.parent().expect("parent"))?;
    fs::write(&path, SAMPLE_AV_MP4_BYTES)?;
    Ok(path)
}

async fn run_add_hierarchy_example() -> ExampleResult<AddHierarchyManifest> {
    let root = artifact_root();
    reset_artifact_root(&root)?;

    let mut service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(&root, example_runtime_storage())
            .await?;

    let local_source_path = write_dummy_local_source(&root)?;
    let local_media_id =
        service.add_local_source(&local_source_path, "ffprobe", None, AddInsertPosition::End)?;
    let remote_uri = Url::parse(DUMMY_YOUTUBE_URL)?;
    let remote_media_id = "youtube-dQw4w9WgXcQ".to_string();
    service.add_media_source(
        &MediaSourceSpec::default(),
        remote_media_id.clone(),
        &remote_uri,
        None,
        None,
    )?;

    service.add_media_hierarchy_preset(MediaHierarchyPreset::Local)?;
    service.add_media_hierarchy_preset(MediaHierarchyPreset::YtDlpChannel)?;

    let paths = service.paths();
    let manifest_path = root.join("manifest.json");
    let document = load_mediapm_document(&paths.mediapm_ncl)?;
    let manifest = AddHierarchyManifest {
        artifact_root: root,
        manifest_path: manifest_path.clone(),
        mediapm_ncl: paths.mediapm_ncl.clone(),
        conductor_user_ncl: paths.conductor_user_ncl.clone(),
        conductor_generated_ncl: paths.conductor_generated_ncl.clone(),
        local_media_id,
        remote_media_id,
        hierarchy_node_count: document.hierarchy.len(),
        local_hierarchy_folder: LOCAL_HIERARCHY_FOLDER.to_string(),
        yt_dlp_hierarchy_folder: YT_DLP_HIERARCHY_FOLDER.to_string(),
    };

    fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;
    Ok(manifest)
}

#[tokio::main]
async fn main() -> ExampleResult<()> {
    let manifest = run_add_hierarchy_example().await?;

    println!("manifest: {}", manifest.manifest_path.display());
    println!("mediapm.ncl: {}", manifest.mediapm_ncl.display());
    println!("conductor user: {}", manifest.conductor_user_ncl.display());
    println!("conductor generated: {}", manifest.conductor_generated_ncl.display());
    println!("local media id: {}", manifest.local_media_id);
    println!("remote media id: {}", manifest.remote_media_id);
    println!("hierarchy node count: {}", manifest.hierarchy_node_count);

    Ok(())
}

#[cfg(test)]
mod tests {
    use mediapm::{
        HierarchyNodeKind, example_isolation, example_isolation::IsolatedExampleRoots,
        load_mediapm_document,
    };

    use super::{EXAMPLE_ARTIFACT_FOLDER, run_add_hierarchy_example};

    #[tokio::test]
    async fn add_hierarchy_writes_expected_hierarchy_nodes() {
        let _isolated = IsolatedExampleRoots::with_cache();
        let manifest = run_add_hierarchy_example().await.expect("run add-hierarchy example");

        assert!(manifest.mediapm_ncl.exists(), "mediapm config should exist");

        let document = load_mediapm_document(&manifest.mediapm_ncl).expect("load mediapm.ncl");
        assert_eq!(document.hierarchy.len(), 2, "example should add two hierarchy nodes");

        let local_root = document
            .hierarchy
            .iter()
            .find(|node| node.id.as_deref() == Some("media_root"))
            .expect("local preset root should carry the media_root template id");
        let remote_root = document
            .hierarchy
            .iter()
            .find(|node| node.id.as_deref() == Some("media_root_ytdlp"))
            .expect("yt-dlp preset root should carry the media_root_ytdlp template id");

        for root in [local_root, remote_root] {
            assert_eq!(root.kind, HierarchyNodeKind::Folder, "preset root should be a folder");
            assert!(root.media_id.is_none(), "preset root folder should not carry media_id");
            let artist_or_playlist =
                root.children.first().expect("preset root should include a child");
            assert_eq!(
                artist_or_playlist.kind,
                HierarchyNodeKind::Folder,
                "preset template folder should be a folder"
            );
            assert!(
                artist_or_playlist.media_id.is_none(),
                "preset template folder should not carry media_id"
            );
        }

        let remote_source =
            document.media.get(&manifest.remote_media_id).expect("remote source should exist");
        assert!(
            remote_source.steps.is_empty(),
            "remote hierarchy example registers a bare source spec with no steps"
        );
        assert!(
            remote_source.metadata.is_empty(),
            "remote hierarchy example registers a bare source spec with no metadata"
        );
        assert!(
            remote_source.title.is_empty() && remote_source.description.is_empty(),
            "remote hierarchy example registers a bare source spec with no title/description"
        );

        // Conductor documents (user/generated) and managed workflows are only
        // produced by an explicit `mediapm sync` run, not by library-API add
        // flows; config-mutation examples verify the declarative mediapm.ncl
        // state only.
    }

    /// Ensures the documented CLI entry point runs end to end via `main()`.
    #[test]
    fn main_is_exercised() {
        let _isolated = IsolatedExampleRoots::with_cache();
        super::main().expect("example main should run to completion");
    }

    /// Ensures the artifact root is canonical (no per-run `{pid}-{nanos}` stamp)
    /// so repeated runs do not accumulate stamped folders under `examples/artifacts/`.
    #[test]
    fn artifact_root_is_stable() {
        let _env_lock = example_isolation::lock_process_env();
        let previous_artifact_root = std::env::var_os(example_isolation::ARTIFACT_ROOT_ENV);
        // SAFETY: test clears one process env key in a controlled scope and
        // restores the previous value before exit.
        unsafe {
            std::env::remove_var(example_isolation::ARTIFACT_ROOT_ENV);
        }

        let first = super::artifact_root();
        let second = super::artifact_root();
        assert_eq!(first, second, "artifact root must be stable across runs");
        let name = first.file_name().and_then(|n| n.to_str()).expect("file name");
        assert_eq!(
            name, EXAMPLE_ARTIFACT_FOLDER,
            "no per-run stamped suffix allowed in the artifact folder name"
        );

        // SAFETY: restore previous env var value for test isolation.
        unsafe {
            match &previous_artifact_root {
                Some(value) => std::env::set_var(example_isolation::ARTIFACT_ROOT_ENV, value),
                None => std::env::remove_var(example_isolation::ARTIFACT_ROOT_ENV),
            }
        }
    }
}
