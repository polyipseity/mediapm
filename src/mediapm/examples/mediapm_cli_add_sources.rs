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
    fs::write(&local_source_path, b"dummy-local-video-bytes")?;
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
    use std::fs;

    use mediapm::MediaMetadataValue;
    use mediapm_conductor::decode_document;

    use super::{DUMMY_YOUTUBE_URL, run_add_sources_example};
    use mediapm::{MediaStepTool, TransformInputValue, load_mediapm_document};

    #[tokio::test]
    async fn cli_add_sources_writes_expected_config_documents() {
        let manifest = run_add_sources_example().await.expect("run add-sources example");

        assert!(manifest.mediapm_ncl.exists(), "mediapm config should exist");
        assert!(manifest.conductor_user_ncl.exists(), "conductor user config should exist");
        assert!(
            manifest.conductor_generated_ncl.exists(),
            "conductor generated config should exist"
        );

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
        assert_eq!(
            remote_source.metadata.get("video_ext"),
            Some(&MediaMetadataValue::Literal(".mkv".to_string())),
            "yt-dlp preset should hardcode .mkv for video_ext when ffmpeg output extension establishes container"
        );

        let ffmpeg_step = remote_source
            .steps
            .iter()
            .find(|step| step.tool == MediaStepTool::Ffmpeg)
            .expect("remote add should include ffmpeg step");
        let media_tagger_step = remote_source
            .steps
            .iter()
            .find(|step| step.tool == MediaStepTool::MediaTagger)
            .expect("remote add should include media-tagger step");
        let rsgain_step = remote_source
            .steps
            .iter()
            .find(|step| step.tool == MediaStepTool::Rsgain)
            .expect("remote add should include rsgain step");

        assert!(
            ffmpeg_step.output_variants["video"].get("extension").is_some(),
            "ffmpeg preset should keep the explicit mkv extension that establishes downstream inheritance"
        );
        assert!(
            ffmpeg_step.options.get("container").is_none(),
            "ffmpeg preset should not redundantly set container when extension already implies mkv/matroska"
        );
        assert!(
            media_tagger_step.output_variants["video"].get("extension").is_none(),
            "media-tagger preset should rely on inherited extension instead of redundantly restating mkv"
        );
        assert!(
            rsgain_step.output_variants["video"].get("extension").is_none(),
            "rsgain preset should rely on inherited extension instead of redundantly restating mkv"
        );

        let user_bytes =
            fs::read(&manifest.conductor_user_ncl).expect("read conductor user config");
        let machine_bytes =
            fs::read(&manifest.conductor_generated_ncl).expect("read conductor generated config");

        let _user = decode_document(&user_bytes).expect("decode conductor user config");
        let machine = decode_document(&machine_bytes).expect("decode conductor machine config");

        let expected_workflow_ids = [
            format!("mediapm.media.{}", manifest.local_media_id),
            format!("mediapm.media.{}", manifest.remote_media_id),
        ];
        for workflow_id in expected_workflow_ids {
            assert!(
                machine.workflows.iter().any(|w| w.name == workflow_id),
                "conductor machine config should contain managed workflow '{workflow_id}'"
            );
        }
    }
}
