//! Free helper functions for the provisioning subsystem.

use std::fs;
use std::path::{Component, Path, PathBuf};

use crate::error::ConductorError;

use super::types::ContentMapKeyKind;
use super::types::Metadata;

/// Returns a filesystem-safe directory name from a tool identifier.
#[must_use]
pub fn sanitize_tool_id(tool_id: &str) -> String {
    tool_id
        .chars()
        .map(|ch| {
            if matches!(ch, '/' | '\\' | ':' | '?' | '*' | '<' | '>' | '|' | '"') {
                '_'
            } else {
                ch
            }
        })
        .collect()
}

/// Classifies one raw `content_map` key into a file or directory extraction target.
pub fn classify_content_map_key(raw: &str) -> Result<ContentMapKeyKind, ConductorError> {
    if raw.ends_with('/') || raw.ends_with('\\') {
        let trimmed = raw.trim_end_matches(['/', '\\']);
        if trimmed == "." {
            return Ok(ContentMapKeyKind::Directory { relative_dir: PathBuf::new() });
        }
        if trimmed.trim().is_empty() {
            return Err(ConductorError::Workflow(format!(
                "tool content map directory key '{raw}' must contain at least one path \
                 component before trailing slash"
            )));
        }
        let relative_dir = normalize_sandbox_relative_path(trimmed, raw)?;
        return Ok(ContentMapKeyKind::Directory { relative_dir });
    }

    let relative_path = normalize_sandbox_relative_path(raw, raw)?;
    Ok(ContentMapKeyKind::File { relative_path })
}

/// Normalizes and validates one sandbox-relative path string.
fn normalize_sandbox_relative_path(
    raw: &str,
    context_key: &str,
) -> Result<PathBuf, ConductorError> {
    if raw.trim().is_empty() {
        return Err(ConductorError::Workflow(format!(
            "tool content map key '{context_key}' path must be non-empty"
        )));
    }
    let parsed = Path::new(raw);
    if parsed.is_absolute() {
        return Err(ConductorError::Workflow(format!(
            "tool content map key '{context_key}' path must be relative"
        )));
    }
    let mut normalized = PathBuf::new();
    for component in parsed.components() {
        match component {
            Component::Normal(part) => normalized.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(ConductorError::Workflow(format!(
                    "tool content map key '{context_key}' must not escape the tool sandbox"
                )));
            }
        }
    }
    if normalized.as_os_str().is_empty() {
        return Err(ConductorError::Workflow(format!(
            "tool content map key '{context_key}' must contain a concrete path component"
        )));
    }
    Ok(normalized)
}

/// Ensures one extracted payload file is executable by the current user.
pub(crate) fn ensure_user_execute_bit(path: &Path) -> Result<(), ConductorError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let metadata = fs::metadata(path).map_err(|source| ConductorError::Io {
            operation: "reading extracted tool-content file permissions".to_string(),
            path: path.to_path_buf(),
            source,
        })?;
        let mut permissions = metadata.permissions();
        let mode = permissions.mode();
        if mode & 0o100 == 0 {
            permissions.set_mode(mode | 0o100);
            fs::set_permissions(path, permissions).map_err(|source| ConductorError::Io {
                operation: "marking extracted tool-content file executable".to_string(),
                path: path.to_path_buf(),
                source,
            })?;
        }
    }

    Ok(())
}

/// Recursively ensures all regular files under one payload tree have owner
/// execute permissions.
pub(crate) fn ensure_payload_tree_user_execute_bits(root: &Path) -> Result<(), ConductorError> {
    if !root.exists() {
        return Ok(());
    }

    #[cfg(unix)]
    {
        let entries = fs::read_dir(root).map_err(|source| ConductorError::Io {
            operation: "enumerating tool-content payload tree for permission refresh".to_string(),
            path: root.to_path_buf(),
            source,
        })?;

        for entry in entries {
            let entry = entry.map_err(|source| ConductorError::Io {
                operation: "reading tool-content payload directory entry".to_string(),
                path: root.to_path_buf(),
                source,
            })?;
            let path = entry.path();
            let file_type = entry.file_type().map_err(|source| ConductorError::Io {
                operation: "reading tool-content payload entry file type".to_string(),
                path: path.clone(),
                source,
            })?;

            if file_type.is_dir() {
                ensure_payload_tree_user_execute_bits(&path)?;
            } else if file_type.is_file() {
                ensure_user_execute_bit(&path)?;
            }
        }
    }

    Ok(())
}

/// Writes cache metadata atomically via a temporary-file rename.
pub(crate) fn persist_cache_metadata(
    metadata_path: &Path,
    metadata: &Metadata,
) -> Result<(), ConductorError> {
    if let Some(parent) = metadata_path.parent() {
        fs::create_dir_all(parent).map_err(|source| ConductorError::Io {
            operation: "creating tool-content cache metadata parent directory".to_string(),
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let rendered = serde_json::to_string_pretty(metadata).map_err(|error| {
        ConductorError::Serialization(format!("encoding tool-content cache metadata: {error}"))
    })?;
    let temp_path = metadata_path.with_extension("json.tmp");
    fs::write(&temp_path, format!("{rendered}\n")).map_err(|source| ConductorError::Io {
        operation: "writing temporary tool-content cache metadata".to_string(),
        path: temp_path.clone(),
        source,
    })?;
    if metadata_path.exists() {
        let _ = fs::remove_file(metadata_path);
    }
    fs::rename(&temp_path, metadata_path).map_err(|source| ConductorError::Io {
        operation: "replacing tool-content cache metadata".to_string(),
        path: metadata_path.to_path_buf(),
        source,
    })
}

/// Copies (preferring hard links) all files from `source_dir` into `target_dir`.
pub(crate) fn copy_directory_recursive(source_dir: &Path, target_dir: &Path) -> Result<(), String> {
    fs::create_dir_all(target_dir)
        .map_err(|err| format!("creating destination '{}' failed: {err}", target_dir.display()))?;

    let entries = fs::read_dir(source_dir).map_err(|err| {
        format!("reading source directory '{}' failed: {err}", source_dir.display())
    })?;

    for entry in entries {
        let entry = entry.map_err(|err| format!("reading source directory entry failed: {err}"))?;
        let path = entry.path();
        let target_path = target_dir.join(entry.file_name());
        let file_type = entry
            .file_type()
            .map_err(|err| format!("reading file type for '{}' failed: {err}", path.display()))?;

        if file_type.is_dir() {
            copy_directory_recursive(&path, &target_path)?;
            continue;
        }

        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                format!("creating parent directory '{}' failed: {err}", parent.display())
            })?;
        }

        if fs::hard_link(&path, &target_path).is_ok() {
            continue;
        }

        fs::copy(&path, &target_path).map_err(|err| {
            format!("copying '{}' to '{}' failed: {err}", path.display(), target_path.display())
        })?;

        let source_permissions = fs::metadata(&path)
            .map_err(|err| format!("reading permissions for '{}' failed: {err}", path.display()))?
            .permissions();
        fs::set_permissions(&target_path, source_permissions).map_err(|err| {
            format!("setting permissions on '{}' failed: {err}", target_path.display())
        })?;
    }

    Ok(())
}

/// Returns the current Unix timestamp in whole seconds.
#[must_use]
pub(crate) fn now_unix_seconds() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_tool_id_preserves_safe_characters() {
        let safe = "ffmpeg+evermeet-ffmpeg@6e66d4d1e81f75b5f34dc2a369cc341e12edc531";
        assert_eq!(sanitize_tool_id(safe), safe);
    }

    #[test]
    fn sanitize_tool_id_replaces_unsafe_characters() {
        let input = r#"tool/a:b*c?d<e>f|g"h\i"#;
        let sanitized = sanitize_tool_id(input);
        assert_eq!(sanitized, "tool_a_b_c_d_e_f_g_h_i");
    }

    #[test]
    fn classify_dot_slash_key_maps_to_empty_relative_dir() {
        match classify_content_map_key("./").expect("classify ./") {
            ContentMapKeyKind::Directory { relative_dir } => {
                assert!(relative_dir.as_os_str().is_empty());
            }
            other => panic!("expected Directory with empty relative_dir, got {other:?}"),
        }
    }

    #[test]
    fn classify_content_map_key_rejects_absolute_path() {
        let err = classify_content_map_key("/etc/passwd").expect_err("absolute path");
        assert!(err.to_string().contains("must be relative"));
    }

    #[test]
    fn classify_content_map_key_rejects_parent_escape() {
        let err = classify_content_map_key("../../etc/passwd").expect_err("parent escape");
        assert!(err.to_string().contains("must not escape"));
    }

    #[test]
    fn normalize_sandbox_relative_path_rejects_empty() {
        let err = normalize_sandbox_relative_path("", "test").expect_err("empty path");
        assert!(err.to_string().contains("non-empty"));
    }

    #[test]
    fn classify_named_directory_key_maps_to_relative_dir() {
        match classify_content_map_key("linux/").expect("classify linux/") {
            ContentMapKeyKind::Directory { relative_dir } => {
                assert_eq!(relative_dir, PathBuf::from("linux"));
            }
            ContentMapKeyKind::File { .. } => panic!("expected Directory for linux/"),
        }
    }

    #[test]
    fn classify_file_key_maps_to_relative_path() {
        match classify_content_map_key("bin/tool").expect("classify bin/tool") {
            ContentMapKeyKind::File { relative_path } => {
                assert_eq!(relative_path, PathBuf::from("bin/tool"));
            }
            ContentMapKeyKind::Directory { .. } => panic!("expected File for bin/tool"),
        }
    }

    #[test]
    fn classify_bare_slash_key_is_rejected() {
        let err = classify_content_map_key("/").expect_err("bare slash should be rejected");
        match err {
            ConductorError::Workflow(msg) => {
                assert!(msg.contains("must contain at least one path component"), "{msg}");
            }
            other => panic!("unexpected error kind: {other:?}"),
        }
    }

    /// Extracted raw-file entries should gain `u+x` so bundled companion
    /// executables remain runnable from payload cache trees.
    #[cfg(unix)]
    #[test]
    fn ensure_user_execute_bit_sets_owner_execute_permission() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().expect("tempdir");
        let file_path = temp.path().join("tool.bin");
        fs::write(&file_path, b"tool-bytes").expect("write file");

        let mut permissions = fs::metadata(&file_path).expect("metadata").permissions();
        permissions.set_mode(0o644);
        fs::set_permissions(&file_path, permissions).expect("set non-executable mode");

        ensure_user_execute_bit(&file_path).expect("set execute bit");

        let mode = fs::metadata(&file_path).expect("metadata after").permissions().mode();
        assert_ne!(mode & 0o100, 0, "owner execute bit should be set");
    }
}
