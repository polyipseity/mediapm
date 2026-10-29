//! Free helper functions for the provisioning subsystem.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Component, Path, PathBuf};

use mediapm_cas::Hash;

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

/// Writes cache metadata through the shared atomic publish.
///
/// The document records which payload a provisioned tool directory holds, so a
/// run either reads the whole of it or reads the previous one. Publishing by
/// rename gives that: a reader never sees a partial document, and a save that
/// fails leaves the previous copy in place.
///
/// The parent directory is created here, because the publish itself creates
/// nothing — a writer that created its own parent could resurrect a directory a
/// teardown had just removed.
///
/// # Errors
///
/// Returns [`ConductorError::Serialization`] when the metadata cannot be
/// encoded, and [`ConductorError::Io`] when the parent cannot be created or the
/// publish fails.
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
    let mut document = rendered;
    document.push('\n');

    mediapm_utils::fs::write_bytes_atomically(metadata_path, document.as_bytes()).map_err(
        |source| ConductorError::Io {
            operation: "publishing tool-content cache metadata".to_string(),
            path: metadata_path.to_path_buf(),
            source,
        },
    )
}

/// Records a cache hit by moving the entry's stored use time to `now`.
///
/// The refresh is bookkeeping. A hit means the payload on disk is the one the
/// content map names, and that stays true whether or not the timestamp moves, so
/// a failed save is reported and the hit still stands. Refusing a valid payload
/// over a directory this process cannot write would turn a permission problem
/// into a tool nobody can use.
///
/// The loss is stated rather than hidden: the entry keeps aging from the use time
/// it already held, so the expiry sweep reclaims a tool that is in daily use. The
/// next run extracts it again instead of finding it, and an extraction into a
/// directory that still refuses writes reports the failure this save reported.
pub(crate) fn record_cache_hit(
    metadata_path: &Path,
    content_map: &BTreeMap<String, Hash>,
    now: u64,
) {
    let metadata = Metadata {
        version: crate::provision::VERSION,
        content_map: content_map.clone(),
        last_used_unix_seconds: now,
        execute_bits_verified: true,
    };
    if let Err(error) = persist_cache_metadata(metadata_path, &metadata) {
        tracing::warn!("could not record a cache hit at '{}': {error}", metadata_path.display());
    }
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

pub(crate) fn now_unix_seconds() -> u64 {
    mediapm_utils::Timestamp::now().as_unix_secs()
}

/// Links or copies files from `payload_dir` to `sandbox_dir`, skipping
/// foreign-platform directories determined by caller-specified list.
///
/// # Errors
/// Returns `Err` with a description if I/O operations fail.
pub fn link_to_sandbox_filtered(
    payload_dir: &Path,
    sandbox_dir: &Path,
    foreign_platform_dirs: &[&str],
) -> Result<(), String> {
    if !payload_dir.exists() {
        return Err(format!(
            "tool-content cache payload directory '{}' does not exist",
            payload_dir.display()
        ));
    }

    fs::create_dir_all(sandbox_dir).map_err(|err| {
        format!("creating sandbox directory '{}' failed: {err}", sandbox_dir.display())
    })?;

    let entries = fs::read_dir(payload_dir).map_err(|err| {
        format!("reading payload directory '{}' failed: {err}", payload_dir.display())
    })?;

    for entry in entries {
        let entry =
            entry.map_err(|err| format!("reading payload directory entry failed: {err}"))?;
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();
        let path = entry.path();
        let target_path = sandbox_dir.join(&file_name);

        let file_type = entry
            .file_type()
            .map_err(|err| format!("reading file type for '{}' failed: {err}", path.display()))?;

        // Skip top-level directories that belong to a foreign platform.
        if file_type.is_dir() && foreign_platform_dirs.contains(&name.as_ref()) {
            continue;
        }

        if file_type.is_dir() {
            copy_directory_recursive(&path, &target_path)?;
        } else {
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
                .map_err(|err| {
                    format!("reading permissions for '{}' failed: {err}", path.display())
                })?
                .permissions();
            fs::set_permissions(&target_path, source_permissions).map_err(|err| {
                format!("setting permissions on '{}' failed: {err}", target_path.display())
            })?;
        }
    }

    Ok(())
}

/// Links or copies files from `payload_dir` to `sandbox_dir`, skipping
/// foreign-platform directories determined by compile-time `#[cfg]`.
///
/// This is a thin wrapper around [`link_to_sandbox_filtered`] that
/// passes the host-specific `super::FOREIGN_PLATFORM_DIRS`.
///
/// # Errors
/// Returns `Err` with a description if I/O operations fail.
pub fn link_to_sandbox(payload_dir: &Path, sandbox_dir: &Path) -> Result<(), String> {
    link_to_sandbox_filtered(payload_dir, sandbox_dir, super::FOREIGN_PLATFORM_DIRS)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_tool_id_preserves_safe_characters() {
        let safe =
            "ffmpeg+1759500000_N-127260-g7bc3576910@6e66d4d1e81f75b5f34dc2a369cc341e12edc531";
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
            ContentMapKeyKind::File { .. } => {
                panic!("expected Directory with empty relative_dir")
            }
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

    /// Metadata with no content map, recording `last_used` as its use time.
    ///
    /// The association is what the assertions read back: a save that publishes
    /// the wrong document, or one that publishes none, fails on it.
    fn metadata_last_used(last_used_unix_seconds: u64) -> Metadata {
        Metadata {
            version: crate::provision::VERSION,
            content_map: std::collections::BTreeMap::new(),
            last_used_unix_seconds,
            execute_bits_verified: true,
        }
    }

    /// The file names `root` holds, sorted, so a test can assert what a save left.
    fn file_names(root: &Path) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(root)
            .expect("read the artifact root")
            .map(|entry| entry.expect("entry").file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    /// A leftover temporary an older build wrote neither blocks the save nor is
    /// swept by it.
    ///
    /// Older builds named the temporary from the target alone, so every writer of
    /// one directory shared it. This plants that name, aged past the sweep's
    /// threshold and read-only, to stand for such a build. The current rule is the
    /// helper's own marker: a name it does not produce is not a name it removes,
    /// so the file stays where whoever left it can find it.
    #[test]
    fn a_leftover_temporary_from_an_older_build_neither_blocks_nor_is_swept() {
        let root = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let metadata_path = root.path().join("metadata.json");
        let leftover = root.path().join("metadata.json.tmp");
        fs::write(&leftover, b"{\"version\": 1").expect("write the abandoned temporary");
        let handle = fs::OpenOptions::new().write(true).open(&leftover).expect("open the leftover");
        handle
            .set_modified(std::time::SystemTime::now() - std::time::Duration::from_hours(2))
            .expect("age the leftover past the sweep's threshold");
        drop(handle);
        let mut permissions =
            fs::metadata(&leftover).expect("read leftover metadata").permissions();
        permissions.set_readonly(true);
        fs::set_permissions(&leftover, permissions).expect("make the leftover read-only");

        persist_cache_metadata(&metadata_path, &metadata_last_used(7))
            .expect("a leftover temporary must not block the save");

        assert!(leftover.exists(), "the sweep removed a name this helper does not produce");
        let published = fs::read_to_string(&metadata_path).expect("read the published document");
        assert!(published.ends_with('\n'), "the document must end with a newline: {published:?}");
        let decoded: Metadata = serde_json::from_str(&published).expect("parse the document");
        assert_eq!(decoded.last_used_unix_seconds, 7, "the save published the wrong document");
    }

    /// A temporary of this target that another save is still using survives, and
    /// the save does not wait for it.
    ///
    /// The name is the one this helper produces and carries the id of a process
    /// other than this one, because a save in this process names its temporary from
    /// a counter no other save shares. The file is younger than the sweep's
    /// threshold, which is what tells a running save's temporary apart from a dead
    /// one's. Removing it would make that save's rename fail, so the age rule
    /// leaves it alone and this save publishes on a name of its own.
    #[test]
    fn a_young_temporary_from_another_process_does_not_block_the_save() {
        let root = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let metadata_path = root.path().join("metadata.json");
        let live =
            root.path().join(format!("metadata.json.mediapm-tmp-{}-0", std::process::id() + 1));
        fs::write(&live, b"{\"version\": 1").expect("write the other save's partial document");

        persist_cache_metadata(&metadata_path, &metadata_last_used(9)).expect("publish the save");

        assert!(
            live.exists(),
            "a young temporary of this target belongs to a save that is still running"
        );
        let published = fs::read_to_string(&metadata_path).expect("read the published document");
        let decoded: Metadata = serde_json::from_str(&published).expect("parse the document");
        assert_eq!(decoded.last_used_unix_seconds, 9, "the save published the wrong document");
    }

    /// A save replaces the previous document and keeps no temporary beside it.
    ///
    /// The document is what a later run parses to decide whether a payload is
    /// already extracted, so the target must hold the new metadata in full and the
    /// directory must hold nothing else a reader could mistake for it.
    #[test]
    fn a_save_replaces_the_previous_document_and_keeps_no_temporary() {
        let root = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let metadata_path = root.path().join("metadata.json");
        fs::write(&metadata_path, b"{\"version\": 1, \"content_map\": {}}\n")
            .expect("write the previous document");

        persist_cache_metadata(&metadata_path, &metadata_last_used(11)).expect("publish the save");

        assert_eq!(
            file_names(root.path()),
            vec!["metadata.json".to_string()],
            "a completed save must leave exactly the document it published",
        );
        let published = fs::read_to_string(&metadata_path).expect("read the published document");
        let decoded: Metadata = serde_json::from_str(&published).expect("parse the document");
        assert_eq!(decoded.last_used_unix_seconds, 11);
    }

    /// A save into a directory that cannot be created fails and publishes nothing.
    ///
    /// The parent is created before the publish, which is what keeps this writer
    /// responsible for its own directory. The failure is a regular file where the
    /// parent directory would be, so it holds for every user: a directory without
    /// write permission does not deny root, and a test that passes for one account
    /// and fails for another is worse than no test at all.
    #[test]
    fn a_save_is_refused_when_its_parent_cannot_be_created() {
        let root = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let blocker = root.path().join("blocked");
        fs::write(&blocker, b"a file where the parent directory would be")
            .expect("write the blocker");
        let metadata_path = blocker.join("nested").join("metadata.json");

        let error = persist_cache_metadata(&metadata_path, &metadata_last_used(13))
            .expect_err("a parent that cannot be created must fail the save");

        match &error {
            ConductorError::Io { operation, .. } => {
                assert!(
                    operation.contains("parent directory"),
                    "the failure must name the parent creation, not the publish: {operation}"
                );
            }
            other => panic!("unexpected error kind: {other:?}"),
        }
        assert!(!metadata_path.exists(), "the failed save published a document");
    }

    /// Extracted raw-file entries should gain `u+x` so bundled companion
    /// executables remain runnable from payload cache trees.
    #[cfg(unix)]
    #[test]
    fn ensure_user_execute_bit_sets_owner_execute_permission() {
        use std::os::unix::fs::PermissionsExt;

        let temp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let file_path = temp.path().join("tool.bin");
        fs::write(&file_path, b"tool-bytes").expect("write file");

        let mut permissions = fs::metadata(&file_path).expect("metadata").permissions();
        permissions.set_mode(0o644);
        fs::set_permissions(&file_path, permissions).expect("set non-executable mode");

        ensure_user_execute_bit(&file_path).expect("set execute bit");

        let mode = fs::metadata(&file_path).expect("metadata after").permissions().mode();
        assert_ne!(mode & 0o100, 0, "owner execute bit should be set");
    }

    // ── classify_content_map_key edge cases ──────────────────────────────

    #[test]
    fn unicode_paths_are_accepted() {
        match classify_content_map_key("résumé.txt").expect("unicode file") {
            ContentMapKeyKind::File { relative_path } => {
                assert_eq!(relative_path, PathBuf::from("résumé.txt"));
            }
            ContentMapKeyKind::Directory { .. } => panic!("expected File"),
        }
    }

    #[test]
    fn deeply_nested_directory_key() {
        match classify_content_map_key("a/b/c/d/e/f/").expect("deeply nested") {
            ContentMapKeyKind::Directory { relative_dir } => {
                assert_eq!(relative_dir, PathBuf::from("a/b/c/d/e/f"));
            }
            ContentMapKeyKind::File { .. } => panic!("expected Directory"),
        }
    }

    #[test]
    fn path_with_dot_components() {
        // CurDir components (`.`) are stripped during normalization.
        match classify_content_map_key("./foo/./bar").expect("dot components") {
            ContentMapKeyKind::File { relative_path } => {
                assert_eq!(relative_path, PathBuf::from("foo/bar"));
            }
            ContentMapKeyKind::Directory { .. } => panic!("expected File"),
        }
    }

    #[test]
    fn single_dot_key_is_normalized() {
        // Bare `.` without trailing separator goes through the file branch,
        // but normalizes to an empty path and is rejected.
        let err = classify_content_map_key(".").expect_err("bare dot");
        assert!(
            err.to_string().contains("must contain a concrete path component"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn path_with_slash_prefix() {
        let err = classify_content_map_key("/foo").expect_err("absolute path");
        assert!(err.to_string().contains("must be relative"));
    }

    #[test]
    fn just_filename_no_directory() {
        match classify_content_map_key("file.txt").expect("just filename") {
            ContentMapKeyKind::File { relative_path } => {
                assert_eq!(relative_path, PathBuf::from("file.txt"));
            }
            ContentMapKeyKind::Directory { .. } => panic!("expected File"),
        }
    }
}
