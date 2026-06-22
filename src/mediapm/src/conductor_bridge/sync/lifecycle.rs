use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use mediapm_cas::Hash;
use mediapm_conductor::{NickelDocument, ToolKindSpec};

use crate::config::MediaPmState;
use crate::config::ToolRequirement;
use crate::error::MediaPmError;
use crate::tools::catalog::{ToolDownloadDescriptor, tool_catalog_entry};
use crate::tools::downloader::{ContentMapSource, ProvisionedToolPayload};

use super::super::tool_runtime::{
    MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_LINUX_ENV, MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_MACOS_ENV,
    MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_WINDOWS_ENV, validate_tool_command,
};
pub(super) fn should_skip_tag_update_check(
    requirement: &ToolRequirement,
    tool_name: &str,
    lock: &MediaPmState,
    machine: &NickelDocument,
    check_tag_updates: bool,
) -> bool {
    // yt-dlp carries same-step companion dependencies (ffmpeg + deno) that
    // must always flow through full reconciliation so selector identity and
    // merged companion content stay consistent between `tool sync` and `sync`.
    if tool_name.eq_ignore_ascii_case("yt-dlp") {
        return false;
    }

    if check_tag_updates || !is_tag_only_requirement(requirement) {
        return false;
    }

    if tool_catalog_entry(tool_name)
        .ok()
        .is_some_and(|entry| matches!(entry.download, ToolDownloadDescriptor::InternalLauncher))
    {
        return false;
    }

    let Some(active_tool_id) = lock.active_tools.get(tool_name) else {
        return false;
    };

    let Some(tool_spec) = machine.tools.values().find(|t| t.name == *active_tool_id) else {
        return false;
    };

    let command_parts = match &tool_spec.kind {
        ToolKindSpec::Executable { command, .. } => command.clone(),
        _ => return false,
    };

    validate_tool_command(tool_name, &command_parts, &tool_spec.runtime.content_map).is_ok()
}

/// Returns true when one requirement selects only by moving tag.
fn is_tag_only_requirement(requirement: &ToolRequirement) -> bool {
    requirement.normalized_tag().is_some() && requirement.normalized_version().is_none()
}

/// Returns true when one logical tool requirement targets a builtin
/// source-ingest step tool that is not downloader-provisioned.
#[must_use]
pub(super) fn is_builtin_source_ingest_requirement(tool_name: &str) -> bool {
    tool_name.eq_ignore_ascii_case("import")
}

/// Returns whether any current machine tool config still references `hash`.
#[must_use]
pub(super) fn is_hash_still_referenced_by_tool_configs(
    machine: &NickelDocument,
    hash: Hash,
) -> bool {
    let hash_str = hash.to_string();
    machine
        .tools
        .values()
        .any(|tool| tool.runtime.content_map.values().any(|candidate| *candidate == hash_str))
}

/// Resolves lockfile version label from provisioned identity metadata.
pub(super) fn lock_registry_version(
    provisioned: &ProvisionedToolPayload,
) -> Result<String, MediaPmError> {
    if let Some(hash) =
        provisioned.identity.git_hash.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        return Ok(hash.to_string());
    }

    if let Some(version) =
        provisioned.identity.version.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        return Ok(version.to_string());
    }

    if let Some(tag) =
        provisioned.identity.tag.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        return Ok(tag.to_string());
    }

    Err(MediaPmError::Workflow(format!(
        "tool '{}' resolved with no git hash, version, or tag; lockfile tool registry requires one immutable selector",
        provisioned.catalog.name
    )))
}

/// Ensures internal launcher payload files exist before CAS import.
///
/// Some environments can remove non-host launcher files between provisioning
/// and CAS import. Internal launchers are deterministic, so missing files are
/// regenerated from their known relative content-map keys.
pub(super) fn ensure_internal_launcher_content_entries_exist(
    provisioned: &ProvisionedToolPayload,
    content_entries: &BTreeMap<String, ContentMapSource>,
) -> Result<(), MediaPmError> {
    if !matches!(provisioned.catalog.download, ToolDownloadDescriptor::InternalLauncher) {
        return Ok(());
    }

    for (relative_path, source) in content_entries {
        let ContentMapSource::FilePath(absolute_path) = source else {
            continue;
        };

        if absolute_path.exists() {
            continue;
        }

        if !provisioned.catalog.name.eq_ignore_ascii_case("media-tagger") {
            return Err(MediaPmError::Workflow(format!(
                "internal launcher '{}' is missing payload file '{}' at '{}' and has no regeneration strategy",
                provisioned.catalog.name,
                relative_path,
                absolute_path.display()
            )));
        }

        regenerate_media_tagger_internal_launcher_file(relative_path, absolute_path)?;
    }

    Ok(())
}

/// Regenerates one missing internal media-tagger launcher script file.
fn regenerate_media_tagger_internal_launcher_file(
    relative_path: &str,
    absolute_path: &Path,
) -> Result<(), MediaPmError> {
    let normalized = relative_path
        .replace('\\', "/")
        .trim_start_matches("./")
        .trim_start_matches('/')
        .to_string();

    let launcher_env_key = match normalized.as_str() {
        "windows/media-tagger.cmd" => MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_WINDOWS_ENV,
        "linux/media-tagger" => MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_LINUX_ENV,
        "macos/media-tagger" => MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_MACOS_ENV,
        _ => {
            return Err(MediaPmError::Workflow(format!(
                "cannot regenerate internal media-tagger launcher for unsupported path key '{relative_path}'"
            )));
        }
    };

    let content = if normalized.starts_with("windows/") {
        format!(
            concat!(
                "@echo off\r\n",
                "setlocal\r\n",
                "if \"%{launcher_env_key}%\"==\"\" (\r\n",
                "  echo internal media-tagger launcher requires %{launcher_env_key}% to be set>&2\r\n",
                "  exit /b 1\r\n",
                ")\r\n",
                "\"%{launcher_env_key}%\" builtins media-tagger %*\r\n"
            ),
            launcher_env_key = launcher_env_key,
        )
    } else {
        format!(
            concat!(
                "#!/usr/bin/env sh\n",
                "if [ -z \"${launcher_env_key}\" ]; then\n",
                "  printf '%s\\n' \"internal media-tagger launcher requires {launcher_env_key} to be set\" >&2\n",
                "  exit 1\n",
                "fi\n",
                "exec \"${launcher_env_key}\" builtins media-tagger \"$@\"\n"
            ),
            launcher_env_key = launcher_env_key,
        )
    };

    if let Some(parent) = absolute_path.parent() {
        fs::create_dir_all(parent).map_err(|source| MediaPmError::Io {
            operation: "creating internal launcher parent directory during regeneration"
                .to_string(),
            path: parent.to_path_buf(),
            source,
        })?;
    }

    fs::write(absolute_path, content.as_bytes()).map_err(|source| MediaPmError::Io {
        operation: "writing regenerated internal launcher payload".to_string(),
        path: absolute_path.to_path_buf(),
        source,
    })?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata(absolute_path)
            .map_err(|source| MediaPmError::Io {
                operation: "reading regenerated internal launcher metadata".to_string(),
                path: absolute_path.to_path_buf(),
                source,
            })?
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(absolute_path, permissions).map_err(|source| MediaPmError::Io {
            operation: "setting regenerated internal launcher executable permissions".to_string(),
            path: absolute_path.to_path_buf(),
            source,
        })?;
    }

    Ok(())
}
