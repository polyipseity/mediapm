//! Online and local source metadata resolution helpers.

use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;

use mediapm_cas::FileSystemCas;
use mediapm_conductor::{
    ConductorApi, ConductorError, MachineNickelDocument, RunSummary, RunWorkflowOptions,
    SimpleConductor,
};
use musicbrainz_rs::entity::recording::Recording;
use musicbrainz_rs::prelude::*;
use url::Url;

use crate::error::MediaPmError;
use crate::paths::MediaPmPaths;

/// Metadata resolved from a `MusicBrainz` recording MBID.
pub(crate) struct MbRecordingMetadata {
    /// Recording title.
    pub(crate) title: String,
    /// Combined artist credit text (may be `"unknown"` when absent).
    pub(crate) artist: String,
}

/// Validates that `id` is a well-formed UUID (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`).
pub(crate) fn validate_recording_mbid_format(id: &str) -> Result<(), MediaPmError> {
    let parts: Vec<&str> = id.split('-').collect();
    let valid = parts.len() == 5
        && parts[0].len() == 8
        && parts[1].len() == 4
        && parts[2].len() == 4
        && parts[3].len() == 4
        && parts[4].len() == 12
        && id.chars().all(|c| c.is_ascii_hexdigit() || c == '-');
    if valid {
        Ok(())
    } else {
        Err(MediaPmError::Workflow(format!(
            "recording MBID '{id}' is not a valid UUID (expected 8-4-4-4-12 lowercase hex)"
        )))
    }
}

/// Fetches and validates a `MusicBrainz` recording, returning title and artist credit.
///
/// # Errors
///
/// Returns [`MediaPmError`] when the recording MBID is not a valid UUID or the
/// `MusicBrainz` API call fails (network error, unknown id, etc.).
pub(crate) async fn fetch_mb_recording_metadata(
    recording_mbid: &str,
) -> Result<MbRecordingMetadata, MediaPmError> {
    validate_recording_mbid_format(recording_mbid)?;
    let recording =
        Recording::fetch().id(recording_mbid).with_artists().execute_async().await.map_err(
            |e| {
                MediaPmError::Workflow(format!(
                    "MusicBrainz lookup for recording '{recording_mbid}' failed: {e}"
                ))
            },
        )?;
    let title = recording.title.clone();
    let artist = recording
        .artist_credit
        .as_deref()
        .filter(|credits| !credits.is_empty())
        .map(|credits| {
            let mut combined = String::new();
            for credit in credits {
                combined.push_str(&credit.name);
                if let Some(join_phrase) = credit.joinphrase.as_deref() {
                    combined.push_str(join_phrase);
                }
            }
            combined
        })
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "unknown".to_string());
    Ok(MbRecordingMetadata { title, artist })
}

/// Metadata tuple fetched by downloader-aware online probes.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct OnlineSourceMetadata {
    /// Best-effort media title.
    pub(crate) title: Option<String>,
    /// Best-effort artist/uploader label.
    pub(crate) artist: Option<String>,
    /// Best-effort textual description.
    pub(crate) description: Option<String>,
}

/// Remote metadata resolved for the online add flow.
///
/// This structure keeps the add-path-specific title, description, artist, and
/// warning text together so service code can stay small while tests can assert
/// the resolution policy directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedOnlineSourceMetadata {
    /// Resolved title used when adding one online media source.
    pub(crate) title: String,
    /// Resolved description used when adding one online media source.
    pub(crate) description: String,
    /// Resolved artist/uploader label used when adding one online media source.
    pub(crate) artist: Option<String>,
    /// Optional warning emitted when yt-dlp metadata cannot be fetched.
    pub(crate) warning: Option<String>,
}

/// Metadata tuple fetched by local-file probes.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct LocalSourceMetadata {
    /// Best-effort media title.
    pub(crate) title: Option<String>,
    /// Best-effort textual description.
    pub(crate) description: Option<String>,
}

/// Resolves online metadata using a managed `yt-dlp` executable.
pub(crate) fn fetch_online_source_metadata(
    uri: &Url,
    yt_dlp_command: &Path,
) -> OnlineSourceMetadata {
    try_fetch_online_source_metadata_with_yt_dlp(uri, yt_dlp_command).unwrap_or_default()
}

/// Resolves add-flow metadata for one remote source.
///
/// When yt-dlp metadata is available, the fetched values are preferred for the
/// title, description, and artist fields. The warning is carried through so
/// the caller can report why metadata fell back to defaults.
pub(crate) fn resolve_online_source_metadata_for_add(
    uri: &Url,
    yt_dlp_metadata: Option<OnlineSourceMetadata>,
    warning: Option<String>,
) -> ResolvedOnlineSourceMetadata {
    let metadata = yt_dlp_metadata.unwrap_or_default();
    let title = metadata.title.unwrap_or_else(|| remote_default_title(uri));
    let description = metadata.description.unwrap_or_else(|| {
        build_remote_default_description_for_remote_source(&title, metadata.artist.as_deref())
    });

    ResolvedOnlineSourceMetadata { title, description, artist: metadata.artist, warning }
}

/// Resolves local metadata using media-probe tooling when available.
pub(crate) fn fetch_local_source_metadata(path: &Path) -> LocalSourceMetadata {
    try_fetch_local_source_metadata_with_ffprobe(path).unwrap_or_default()
}

/// Fetches online metadata by invoking `yt-dlp` from one explicit executable path.
pub(crate) fn try_fetch_online_source_metadata_with_yt_dlp(
    uri: &Url,
    yt_dlp_command: &Path,
) -> Option<OnlineSourceMetadata> {
    let output = ProcessCommand::new(yt_dlp_command)
        .arg("--dump-single-json")
        .arg("--skip-download")
        .arg("--no-warnings")
        .arg(uri.as_str())
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let value: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
    let metadata = parse_online_source_metadata(&value);

    if metadata.title.is_none() && metadata.artist.is_none() && metadata.description.is_none() {
        None
    } else {
        Some(metadata)
    }
}

/// Fetches local metadata by invoking `ffprobe` when present on PATH.
pub(crate) fn try_fetch_local_source_metadata_with_ffprobe(
    path: &Path,
) -> Option<LocalSourceMetadata> {
    let output = ProcessCommand::new("ffprobe")
        .arg("-v")
        .arg("error")
        .arg("-print_format")
        .arg("json")
        .arg("-show_format")
        .arg(path)
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let value: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
    let metadata = parse_local_source_metadata_from_ffprobe_json(&value);

    if metadata.title.is_none() && metadata.description.is_none() { None } else { Some(metadata) }
}

/// Parses online metadata fields from one downloader JSON payload.
pub(crate) fn parse_online_source_metadata(value: &serde_json::Value) -> OnlineSourceMetadata {
    let title = first_non_empty_json_string(value, &["fulltitle", "title", "track"]);
    let artist = first_non_empty_json_string(
        value,
        &["uploader", "channel", "artist", "creator", "uploader_id"],
    );
    let description = first_non_empty_json_string(value, &["description", "summary"]);

    OnlineSourceMetadata { title, artist, description }
}

/// Parses local metadata fields from one ffprobe JSON payload.
pub(crate) fn parse_local_source_metadata_from_ffprobe_json(
    value: &serde_json::Value,
) -> LocalSourceMetadata {
    let tags = value
        .get("format")
        .and_then(|format| format.get("tags"))
        .cloned()
        .unwrap_or(serde_json::Value::Null);

    let title = first_non_empty_json_string(&tags, &["title", "track"]);
    let description = first_non_empty_json_string(&tags, &["description", "comment", "synopsis"]);

    LocalSourceMetadata { title, description }
}

/// Returns first non-empty string value from one JSON object key list.
pub(crate) fn first_non_empty_json_string(
    value: &serde_json::Value,
    keys: &[&str],
) -> Option<String> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .or_else(|| {
                value.as_object().and_then(|object| {
                    object.iter().find_map(|(candidate, candidate_value)| {
                        if candidate.eq_ignore_ascii_case(key) {
                            Some(candidate_value)
                        } else {
                            None
                        }
                    })
                })
            })
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(ToString::to_string)
    })
}

/// Derives a human-readable title for one remote source URL.
pub(crate) fn remote_default_title(uri: &Url) -> String {
    uri.path_segments()
        .and_then(|mut segments| segments.rfind(|segment| !segment.is_empty()))
        .map(ToString::to_string)
        .filter(|title| !title.trim().is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Builds the generic description text used for remote-source defaults.
fn build_remote_default_description_for_remote_source(title: &str, artist: Option<&str>) -> String {
    let artist = artist.map(str::trim).filter(|value| !value.is_empty()).unwrap_or("unknown");
    format!("title: {title}\nartist: {artist}")
}

/// Resolves conductor CAS root from machine runtime storage with default fallback.
pub(crate) fn resolve_conductor_cas_root(
    paths: &MediaPmPaths,
    machine: &MachineNickelDocument,
) -> PathBuf {
    if let Some(raw) = machine.runtime.cas_store_dir.as_deref() {
        let candidate = PathBuf::from(raw);
        if candidate.is_absolute() { candidate } else { paths.root_dir.join(candidate) }
    } else {
        paths.runtime_root.join("store")
    }
}

/// Executes workflows with a filesystem-backed conductor rooted at one CAS
/// store path.
///
/// This path is used when workflow steps need payload hashes imported into the
/// persistent runtime CAS store during tool reconciliation.
pub(crate) async fn run_workflow_with_filesystem_cas(
    conductor_cas_root: &Path,
    user_ncl: &Path,
    machine_ncl: &Path,
    options: RunWorkflowOptions,
) -> Result<RunSummary, MediaPmError> {
    let cas = FileSystemCas::open(conductor_cas_root).await.map_err(|source| {
        MediaPmError::Workflow(format!(
            "opening conductor CAS store '{}' for workflow execution failed: {source}",
            conductor_cas_root.display()
        ))
    })?;
    let conductor = SimpleConductor::new(cas);
    conductor.run_workflow_with_options(user_ncl, machine_ncl, options).await.map_err(Into::into)
}

/// Returns true when workflow execution should run directly against
/// filesystem-backed CAS instead of an in-memory conductor backend.
///
/// Managed executable tools persist runtime `content_map` hashes in the
/// resolved conductor CAS store during tool reconciliation. Running workflow
/// execution with in-memory CAS in that state would force a fail-then-retry
/// fallback path and duplicate workflow progress output.
#[must_use]
pub(crate) fn should_prefer_filesystem_workflow_runner(machine: &MachineNickelDocument) -> bool {
    machine
        .tool_configs
        .values()
        .any(|config| config.content_map.as_ref().is_some_and(|map| !map.is_empty()))
}

/// Returns true when conductor workflow execution should retry on filesystem CAS.
///
/// The default in-memory conductor used by high-level `mediapm` service
/// constructors cannot resolve hashes imported into the persistent runtime
/// store during tool reconciliation. When that mismatch surfaces as a
/// deterministic missing-object CAS error, sync falls back to a temporary
/// filesystem-backed conductor bound to the resolved runtime store.
pub(crate) fn should_retry_workflow_with_filesystem_cas(error: &ConductorError) -> bool {
    let text = error.to_string();
    text.contains("cas operation failed") && text.contains("object not found")
}
