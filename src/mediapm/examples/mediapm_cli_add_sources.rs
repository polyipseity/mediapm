//! Example for inspecting default `media add` source outputs via the library API.
//!
//! Uses `MediaPmService::add_local_source` and `add_media_source` directly
//! instead of spawning the CLI. Writes generated documents under
//! `src/mediapm/examples/artifacts/cli-add-sources/`.

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

use mediapm::{AddInsertPosition, MediaPmService, MediaSourceSpec, example_isolation};
use url::Url;

const EXAMPLE_ARTIFACT_FOLDER: &str = "cli-add-sources";
const DUMMY_LOCAL_SOURCE_FILE: &str = "dummy-local-video.mp4";
const DUMMY_YOUTUBE_URL: &str = "https://www.youtube.com/watch?v=dQw4w9WgXcQ";

/// Embedded tiny MP4 payload containing both video and audio tracks.
const SAMPLE_AV_MP4_BYTES: &[u8] = include_bytes!("../tests/fixtures/media/sample-av.mp4");

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

fn write_dummy_local_source(root: &Path) -> ExampleResult<PathBuf> {
    let local_source_path = root.join("inputs").join(DUMMY_LOCAL_SOURCE_FILE);
    fs::create_dir_all(local_source_path.parent().expect("local source parent"))?;
    fs::write(&local_source_path, SAMPLE_AV_MP4_BYTES)?;
    Ok(local_source_path)
}

/// Registers one local and one remote media source in a clean workspace.
///
/// Shuts the conductor the service owns down before returning:
/// `Conductor::shutdown` awaits the background pass that would otherwise write
/// into the caller's removed directory.
async fn run_add_sources_example() -> ExampleResult<()> {
    let root = artifact_root();
    reset_artifact_root(&root)?;

    let mut service = MediaPmService::new_fs_at(&root).await?;

    let local_source_path = write_dummy_local_source(&root)?;
    service.add_local_source(&local_source_path, "ffprobe", None, AddInsertPosition::End).await?;

    let remote_uri = Url::parse(DUMMY_YOUTUBE_URL)?;
    let remote_media_id = mediapm::media_id_from_uri(&remote_uri);
    service.add_media_source(
        &MediaSourceSpec::default(),
        remote_media_id,
        &remote_uri,
        None,
        None,
    )?;

    service.conductor().shutdown().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> ExampleResult<()> {
    run_add_sources_example().await?;
    let root = artifact_root();

    println!("mediapm config: {}", root.join("mediapm.ncl").display());

    Ok(())
}

#[cfg(test)]
mod tests {
    use mediapm::{
        MediaStepTool, TransformInputValue, example_isolation::IsolatedExampleRoots,
        load_mediapm_document,
    };

    use super::run_add_sources_example;

    #[tokio::test]
    async fn cli_add_sources_writes_expected_config_documents() {
        let _isolated = IsolatedExampleRoots::artifact_only();
        run_add_sources_example().await.expect("run add-sources example");
        let root = super::artifact_root();
        let mediapm_ncl = root.join("mediapm.ncl");

        assert!(
            !root.join("manifest.json").exists(),
            "add-sources manifest is redundant; the config documents already record the sources"
        );
        assert!(mediapm_ncl.exists(), "mediapm config should exist");

        let document = load_mediapm_document(&mediapm_ncl).expect("load mediapm.ncl");
        assert_eq!(document.media.len(), 2, "example should register exactly two media sources");

        let (remote_media_id, remote_source) = document
            .media
            .iter()
            .find(|(_, source)| source.steps.is_empty() && source.metadata.is_empty())
            .expect("remote add registers a bare source spec");
        let (local_media_id, local_source) = document
            .media
            .iter()
            .find(|(_, source)| {
                source.steps.first().is_some_and(|step| step.tool == MediaStepTool::Import)
            })
            .expect("local add registers an imported source spec");
        assert_ne!(
            local_media_id, remote_media_id,
            "local and remote sources must have different ids"
        );

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
        let _isolated = IsolatedExampleRoots::artifact_only();
        super::main().expect("example main should run to completion");
    }
}
