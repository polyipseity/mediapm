//! Example for inspecting default `media add` source outputs via the library API.
//!
//! Uses `MediaPmService::add_local_source` and `add_media_source` directly
//! instead of spawning the CLI. Writes generated documents under
//! `src/mediapm/examples/artifacts/cli-add-sources/` and emits a small
//! `manifest.json` with resulting media ids and config locations.

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

use mediapm::{AddInsertPosition, MediaPmService, MediaSourceSpec};
use serde::{Deserialize, Serialize};
use url::Url;

const EXAMPLE_ARTIFACT_FOLDER: &str = "cli-add-sources";
const DUMMY_LOCAL_SOURCE_FILE: &str = "dummy-local-video.mp4";
const DUMMY_YOUTUBE_URL: &str = "https://www.youtube.com/watch?v=dQw4w9WgXcQ";

/// Embedded tiny MP4 payload containing both video and audio tracks.
const SAMPLE_AV_MP4_BYTES: &[u8] = include_bytes!("assets/sample-av.mp4");

type ExampleResult<T> = Result<T, Box<dyn Error>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AddSourcesManifest {
    artifact_root: PathBuf,
    manifest_path: PathBuf,
    mediapm_ncl: PathBuf,
    conductor_user_ncl: PathBuf,
    conductor_generated_ncl: PathBuf,
    local_media_id: String,
    remote_media_id: String,
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
    workspace_root().join("src/mediapm/examples/artifacts").join(EXAMPLE_ARTIFACT_FOLDER)
}

fn reset_artifact_root(root: &Path) -> ExampleResult<()> {
    if root.exists() {
        fs::remove_dir_all(root)?;
    }
    fs::create_dir_all(root)?;
    Ok(())
}

fn write_dummy_local_source(root: &Path) -> ExampleResult<PathBuf> {
    let local_source_path = root.join("inputs").join(DUMMY_LOCAL_SOURCE_FILE);
    fs::create_dir_all(local_source_path.parent().expect("local source parent"))?;
    fs::write(&local_source_path, SAMPLE_AV_MP4_BYTES)?;
    Ok(local_source_path)
}

async fn run_add_sources_example() -> ExampleResult<AddSourcesManifest> {
    let root = artifact_root();
    reset_artifact_root(&root)?;

    let mut service = MediaPmService::new_fs_at(&root).await?;

    let local_source_path = write_dummy_local_source(&root)?;
    let local_media_id =
        service.add_local_source(&local_source_path, "ffprobe", None, AddInsertPosition::End)?;

    let remote_uri = Url::parse(DUMMY_YOUTUBE_URL)?;
    let remote_media_id = mediapm::media_id_from_uri(&remote_uri);
    service.add_media_source(
        &MediaSourceSpec::default(),
        remote_media_id.clone(),
        &remote_uri,
        None,
        None,
    )?;

    let paths = service.paths();
    let mediapm_ncl = paths.mediapm_ncl.clone();
    let conductor_user_ncl = paths.conductor_user_ncl.clone();
    let conductor_generated_ncl = paths.conductor_generated_ncl.clone();
    let manifest_path = root.join("manifest.json");

    let manifest = AddSourcesManifest {
        artifact_root: root,
        manifest_path: manifest_path.clone(),
        mediapm_ncl,
        conductor_user_ncl,
        conductor_generated_ncl,
        local_media_id,
        remote_media_id,
    };

    fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;
    Ok(manifest)
}

#[tokio::main]
async fn main() -> ExampleResult<()> {
    let manifest = run_add_sources_example().await?;

    println!("manifest: {}", manifest.manifest_path.display());
    println!("mediapm.ncl: {}", manifest.mediapm_ncl.display());
    println!("conductor user: {}", manifest.conductor_user_ncl.display());
    println!("conductor generated: {}", manifest.conductor_generated_ncl.display());
    println!("local media id: {}", manifest.local_media_id);
    println!("remote media id: {}", manifest.remote_media_id);

    Ok(())
}

#[cfg(test)]
mod tests {
    use mediapm::{MediaStepTool, TransformInputValue, load_mediapm_document};

    use super::run_add_sources_example;

    #[tokio::test]
    async fn cli_add_sources_writes_expected_config_documents() {
        let manifest = run_add_sources_example().await.expect("run add-sources example");

        assert!(manifest.mediapm_ncl.exists(), "mediapm config should exist");

        let document = load_mediapm_document(&manifest.mediapm_ncl).expect("load mediapm.ncl");
        assert_eq!(document.media.len(), 2, "example should register exactly two media sources");

        let local_source =
            document.media.get(&manifest.local_media_id).expect("local source should exist");
        let remote_source =
            document.media.get(&manifest.remote_media_id).expect("remote source should exist");

        assert!(
            !local_source.title.trim().is_empty(),
            "local add should auto-populate a non-empty title from ffprobe"
        );
        assert!(
            !local_source.description.trim().is_empty(),
            "local add should auto-populate a non-empty description from ffprobe"
        );

        assert_eq!(local_source.steps[0].tool, MediaStepTool::Import);
        assert_eq!(
            local_source.steps[0].options.get("kind"),
            Some(&TransformInputValue::String("cas_hash".to_string())),
            "local add should synthesize import cas-hash kind"
        );

        assert_eq!(remote_source.steps.len(), 0, "remote add registers a bare source spec");
        assert!(
            remote_source.metadata.is_empty(),
            "remote add registers a bare source spec with no metadata"
        );
        assert!(
            remote_source.title.is_empty() && remote_source.description.is_empty(),
            "remote add registers a bare source spec with no title/description"
        );

        // Conductor documents (user/generated) and managed workflows are only
        // produced by an explicit `mediapm sync` run, not by library-API add
        // flows; config-mutation examples verify the declarative mediapm.ncl
        // state only.
    }

    /// Ensures the documented CLI entry point runs end to end via `main()`.
    #[test]
    fn main_is_exercised() {
        super::main().expect("example main should run to completion");
    }
}
