//! Temporary sandbox directory management and tool content materialization.
//!
//! A step runs in a sandbox of its own under `{conductor_tmp_dir}/sandbox/`,
//! and its working directory is that sandbox under every staging strategy:
//! a tool that writes `downloads/x` writes it where the capture phase reads it.
//! What changes between strategies is where the *tool* comes from — copied out
//! of the CAS into the sandbox, linked into it from the provisioned tree, or
//! left in the provisioned tree and executed there.
//!
//! A directory key in the content map names a ZIP, and the provisioner unpacks
//! it once into the payload tree the entry owns. `in_place` and `hard_link`
//! read that unpacked tree and unpack nothing of their own, so a workflow of
//! sixteen steps over one tool extracts once; `copy` re-extracts the archive per
//! step, which is the cost the other two exist to remove.
//!
//! A strategy that reads the payload tree checks it before the step runs rather
//! than letting the command fail: `require_usable_payload_root` refuses an
//! empty, relative or non-directory root, and refuses a root that lacks a
//! content-map entry, naming the tool and the path. A missing root reported as
//! "command not found" would send the reader to the tool instead of to the
//! document that got it wrong.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use mediapm_cas::{CasApi, Hash};
use mediapm_conductor_builtin_archive;

use crate::config::PayloadStaging;
use crate::error::ConductorError;
use crate::orchestration::protocol::UnifiedToolSpec;
use crate::provision::link_to_sandbox;

/// Creates a temporary sandbox directory for step execution.
pub(super) async fn create_sandbox(
    base_tmp_dir: &Path,
    instance_key: &Hash,
) -> Result<PathBuf, ConductorError> {
    let sandbox_root = base_tmp_dir.join("sandbox").join(sanitize_for_path(&instance_key.to_hex()));
    tokio::fs::create_dir_all(&sandbox_root)
        .await
        .map_err(|source| ConductorError::io("create sandbox directory", &sandbox_root, source))?;
    Ok(sandbox_root)
}

/// Sanitizes a string for use as a path component.
pub(super) fn sanitize_for_path(s: &str) -> String {
    s.replace(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '-', "_")
}

/// Removes the conductor runtime tmp tree for one workspace.
pub(crate) async fn remove_runtime_tmp_dir(conductor_tmp_dir: &Path) -> Result<(), ConductorError> {
    if !conductor_tmp_dir.is_dir() {
        return Ok(());
    }

    tokio::fs::remove_dir_all(conductor_tmp_dir).await.map_err(|source| {
        ConductorError::io("remove conductor runtime tmp dir", conductor_tmp_dir, source)
    })
}

/// Materializes tool content map entries into the sandbox directory.
///
/// Entries with a trailing-slash key (e.g. `./{os}/`) are treated as
/// uncompressed ZIP blobs that are unpacked into the corresponding sandbox
/// subdirectory.  File-level entries are written verbatim.
pub(super) async fn materialize_content_map<C: CasApi + Send + Sync>(
    cas: &C,
    content_map: &BTreeMap<String, String>,
    sandbox_dir: &Path,
) -> Result<(), ConductorError> {
    for (relative_path, value) in content_map {
        // Try to parse as hash; if it fails, treat as inline bytes.
        if let Ok(hash) = value.parse::<Hash>() {
            let data = cas.get(hash).await.map_err(ConductorError::Cas)?;
            let target_path = sandbox_dir.join(relative_path);

            if relative_path.ends_with('/') {
                // Trailing-slash key → hash is an uncompressed ZIP blob; unpack it.
                let data_for_zip = data.clone();
                let dest_dir = target_path.clone();
                tokio::task::spawn_blocking(move || {
                    mediapm_conductor_builtin_archive::unpack_zip_bytes_to_directory(
                        &data_for_zip,
                        &dest_dir,
                    )
                    .map_err(|e| {
                        ConductorError::io(
                            "unpack zip content",
                            &dest_dir,
                            std::io::Error::other(e),
                        )
                    })
                })
                .await
                .map_err(|e| {
                    ConductorError::io(
                        "join spawn_blocking",
                        &target_path,
                        std::io::Error::other(e),
                    )
                })??;
            } else {
                // File-level entry: write bytes verbatim.
                if let Some(parent) = target_path.parent() {
                    tokio::fs::create_dir_all(parent).await.map_err(|source| {
                        ConductorError::io("create content parent directory", parent, source)
                    })?;
                }
                tokio::fs::write(&target_path, &data).await.map_err(|source| {
                    ConductorError::io("write tool content", &target_path, source)
                })?;
            }
        }
        // Non-hash values are skipped (inline descriptions).
    }

    #[cfg(unix)]
    {
        crate::provision::helpers::ensure_payload_tree_user_execute_bits(sandbox_dir)?;
    }

    Ok(())
}

/// Makes the tool's payload available to a step under its declared strategy.
///
/// `copy` writes the content map into the sandbox, exactly as every step used
/// to; the other two read the tree the provisioner already built, so the cost
/// of staging is entries rather than bytes. The working directory stays the
/// sandbox in all three cases: only the executable moves, and a tool that
/// writes an output writes it where the capture phase looks for it.
///
/// # Errors
///
/// Returns [`ConductorError::Workflow`] naming the tool and the path when a
/// strategy that needs a payload root cannot use the one the document recorded,
/// or when a content-map entry the payload must hold is absent from it.
/// Returns the copy, link, or CAS failure when the staging itself fails.
pub(super) async fn prepare_tool_payload<C: CasApi + Send + Sync>(
    cas: &C,
    tool_spec: &UnifiedToolSpec,
    sandbox_dir: &Path,
) -> Result<(), ConductorError> {
    match tool_spec.payload_staging {
        PayloadStaging::Copy => {
            materialize_content_map(cas, &tool_spec.tool_content_map, sandbox_dir).await
        }
        PayloadStaging::InPlace => {
            require_usable_payload_root(tool_spec)?;
            Ok(())
        }
        PayloadStaging::HardLink => {
            let Some(payload_root) = require_usable_payload_root(tool_spec)? else {
                return Ok(());
            };
            link_to_sandbox(&payload_root, sandbox_dir).map_err(|detail| {
                ConductorError::Workflow(format!(
                    "tool '{}' could not link its payload from '{}' into the step sandbox: {}",
                    tool_spec.name,
                    payload_root.display(),
                    detail,
                ))
            })
        }
    }
}

/// Checks the payload root a strategy that reads one depends on, and answers it.
///
/// Answers `None` for a tool that declares no payload entries: there is nothing
/// to stage, and a launcher or a builtin names its own executable. A tool that
/// does declare entries gets the root the document recorded, checked before the
/// step runs rather than after: a missing root that surfaced as "command not
/// found" would send the reader to the tool instead of to the document.
///
/// A content-map value is checked only when it parses as a hash. A value that
/// does not is an inline description rather than an entry in the payload, so
/// there is nothing under the root for it to name.
///
/// # Errors
///
/// Returns [`ConductorError::Workflow`] when the root is empty, is not absolute,
/// is not a directory, or when a hash-valued content-map entry is missing from
/// it.
fn require_usable_payload_root(
    tool_spec: &UnifiedToolSpec,
) -> Result<Option<PathBuf>, ConductorError> {
    if tool_spec.tool_content_map.is_empty() {
        return Ok(None);
    }
    let strategy = tool_spec.payload_staging.as_str();
    if tool_spec.payload_root.is_empty() {
        return Err(ConductorError::Workflow(format!(
            "tool '{}' stages its payload with '{strategy}' but the document records no \
             payload_root",
            tool_spec.name,
        )));
    }
    let payload_root = PathBuf::from(&tool_spec.payload_root);
    if !payload_root.is_absolute() {
        return Err(ConductorError::Workflow(format!(
            "tool '{}' stages its payload with '{strategy}' but its payload_root '{}' is not an \
             absolute path",
            tool_spec.name,
            payload_root.display(),
        )));
    }
    if !payload_root.is_dir() {
        return Err(ConductorError::Workflow(format!(
            "tool '{}' stages its payload with '{strategy}' but its payload_root '{}' is not a \
             directory",
            tool_spec.name,
            payload_root.display(),
        )));
    }
    for (key, value) in &tool_spec.tool_content_map {
        if value.parse::<Hash>().is_err() {
            continue;
        }
        let expected = payload_entry_path(&payload_root, key);
        let present = if key.ends_with('/') || key.ends_with('\\') {
            expected.is_dir()
        } else {
            expected.is_file()
        };
        if !present {
            return Err(ConductorError::Workflow(format!(
                "tool '{}' stages its payload from '{}', which does not hold the content-map \
                 entry '{}' (expected at '{}')",
                tool_spec.name,
                payload_root.display(),
                key,
                expected.display(),
            )));
        }
    }
    Ok(Some(payload_root))
}

/// Resolves one content-map key inside a payload root.
///
/// A directory key ends in a separator and names the payload root itself when
/// the key carries no directory component, which is the `./` form the content
/// map uses for an archive that unpacks at the top of the tree.
fn payload_entry_path(payload_root: &Path, key: &str) -> PathBuf {
    let trimmed = key.trim_end_matches(['/', '\\']);
    if trimmed.is_empty() || trimmed == "." {
        return payload_root.to_path_buf();
    }
    payload_root.join(trimmed)
}

/// Resolves the executable of a command under the provisioned payload root.
///
/// Only the first part moves: it is the executable, and a document describes it
/// as a payload-relative path (`linux/ffmpeg`, or `deps/ffmpeg/linux/ffmpeg` for
/// an inlined companion) because the copy strategy once put it in the sandbox.
/// Companion paths in later parts are handled by
/// [`pre_qualify_payload_arguments`]; everything else is a flag, a resolved
/// input path, or an output path, and all of those resolve against the sandbox.
///
/// # Errors
///
/// Returns [`ConductorError::Workflow`] when the joined path is not a file: a
/// command part that names nothing inside the payload is a document defect, and
/// running it anyway would report it as the tool's failure.
pub(super) fn resolve_executable_under_payload_root(
    command_parts: &mut [String],
    tool_spec: &UnifiedToolSpec,
) -> Result<(), ConductorError> {
    if tool_spec.payload_staging != PayloadStaging::InPlace {
        return Ok(());
    }
    let Some(first) = command_parts.first_mut() else {
        return Ok(());
    };
    if tool_spec.tool_content_map.is_empty() || Path::new(first.as_str()).is_absolute() {
        return Ok(());
    }
    let executable = Path::new(&tool_spec.payload_root).join(&*first);
    if !executable.is_file() {
        return Err(ConductorError::Workflow(format!(
            "tool '{}' resolves its executable to '{}', which is not a file under its payload_root \
             '{}'",
            tool_spec.name,
            executable.display(),
            tool_spec.payload_root,
        )));
    }
    *first = executable.to_string_lossy().into_owned();
    Ok(())
}

/// Characters that may precede a path-shaped value inside one command part.
///
/// A part is not always a bare path. yt-dlp names a companion as a
/// `<runtime>:<path>` pair, a document may join a flag to its value with `=`,
/// and a comma-separated list of paths puts one after each comma, so a path may
/// begin after any of these rather than only at the start of the part.
const ARGUMENT_VALUE_SEPARATORS: [char; 3] = [':', '=', ','];

/// The characters a path is spelled with.
///
/// Both spellings name the same tree, and a content-map key always uses `/`, so
/// matching normalizes to that and leaves the parts of a value it does not
/// rewrite exactly as the document wrote them.
const PATH_SEPARATORS: [char; 2] = ['/', '\\'];

/// Resolves the payload paths a command names outside its executable.
///
/// A document names an inlined companion relative to the sandbox
/// (`deps/ffmpeg/linux/ffmpeg`, `deps/deno/linux/deno`) because the copy
/// strategy put that tree in the sandbox. `in_place` leaves the sandbox without
/// one, so every command part is rewritten to name the same entry under the
/// payload root: otherwise a flag such as `--ffmpeg-location` would point at a
/// path that does not exist and the tool would fail on a payload the document
/// declared.
///
/// The rewrite is driven by the tool's own content map, so it can only reach a
/// path the payload actually holds. A directory key names a tree and covers
/// every path inside it (`deps/ffmpeg/linux/` covers
/// `deps/ffmpeg/linux/ffmpeg`); a file key covers itself. Nothing else is
/// rewritten: an output template a tool fills in later, a URL, and a plain flag
/// value name no content-map entry, so they keep the sandbox-relative spelling
/// the document gave them.
///
/// `hard_link` and `copy` leave the arguments alone, because both put the tree
/// in the sandbox under the names the document already used.
pub(super) fn pre_qualify_payload_arguments(
    command_parts: &mut [String],
    tool_spec: &UnifiedToolSpec,
) {
    if tool_spec.payload_staging != PayloadStaging::InPlace {
        return;
    }
    let prefixes = payload_argument_prefixes(tool_spec);
    if prefixes.is_empty() || tool_spec.payload_root.is_empty() {
        return;
    }
    let root = tool_spec.payload_root.trim_end_matches(['/', '\\']);
    for part in command_parts.iter_mut().skip(1) {
        if let Some(qualified) = qualify_argument(part, root, &prefixes) {
            *part = qualified;
        }
    }
}

/// Every path a command argument may name that the payload holds, longest first.
///
/// Each hash-valued content-map key contributes itself and every directory above
/// it, so a value that names only the directory still resolves: `deps/ffmpeg`
/// and `deps/ffmpeg/linux` both name trees the payload holds when it holds
/// `deps/ffmpeg/linux/ffmpeg`.
///
/// Longest first is what makes such a value resolve to the deepest path that
/// matches rather than to an ancestor, and a value that does not parse as a hash
/// is an inline description rather than a payload entry, so it contributes
/// nothing — the same rule `require_usable_payload_root` applies.
fn payload_argument_prefixes(tool_spec: &UnifiedToolSpec) -> Vec<String> {
    let mut prefixes: BTreeSet<String> = BTreeSet::new();
    for (key, value) in &tool_spec.tool_content_map {
        if value.parse::<Hash>().is_err() {
            continue;
        }
        let trimmed = key.trim_end_matches(PATH_SEPARATORS);
        if trimmed.is_empty() || trimmed == "." {
            continue;
        }
        let mut end = trimmed.len();
        loop {
            prefixes.insert(trimmed[..end].to_string());
            match trimmed[..end].rfind('/') {
                Some(index) => end = index,
                None => break,
            }
        }
    }
    let mut prefixes: Vec<String> = prefixes.into_iter().collect();
    prefixes.sort_by(|left, right| right.len().cmp(&left.len()).then_with(|| left.cmp(right)));
    prefixes
}

/// Rewrites every content-map path one command part names, or answers `None`
/// when it names none.
fn qualify_argument(part: &str, payload_root: &str, prefixes: &[String]) -> Option<String> {
    // A backslash and a forward slash name the same tree and both occupy one
    // byte, so the normalized spelling can be matched and the part sliced at the
    // same offsets: only the matched path is rewritten, and every other byte of a
    // value the document wrote survives untouched.
    let normalized = part.replace('\\', "/");
    let mut qualified = String::with_capacity(part.len());
    let mut offset = 0;
    let mut rewrote = false;
    while offset < part.len() {
        let Some((index, prefix)) = first_payload_path(&normalized[offset..], prefixes) else {
            break;
        };
        let start = offset + index;
        qualified.push_str(&part[offset..start]);
        qualified.push_str(payload_root);
        qualified.push('/');
        qualified.push_str(prefix);
        rewrote = true;
        offset = start + prefix.len();
    }
    if rewrote {
        qualified.push_str(&part[offset..]);
    }
    rewrote.then_some(qualified)
}

/// Finds the first content-map path in `text`, answering where it starts and
/// what it spells.
///
/// A match begins at the start of the text or after one of
/// [`ARGUMENT_VALUE_SEPARATORS`], and a file key also has to end at the end of
/// the text or before a path separator. Without those boundaries a coincidental
/// substring of an unrelated value — a filename that happens to contain a key,
/// or a longer path the key only prefixes — would be rewritten.
fn first_payload_path<'a>(text: &str, prefixes: &'a [String]) -> Option<(usize, &'a String)> {
    for (index, _) in text.char_indices() {
        if index > 0 {
            let preceding = text[..index].chars().next_back()?;
            if !ARGUMENT_VALUE_SEPARATORS.contains(&preceding) {
                continue;
            }
            if starts_a_drive_relative_path(text, index) {
                continue;
            }
        }
        for prefix in prefixes {
            let Some(candidate) = text.get(index..index + prefix.len()) else {
                continue;
            };
            if candidate != prefix.as_str() {
                continue;
            }
            let trailing = &text[index + prefix.len()..];
            let ends_cleanly =
                trailing.is_empty() || trailing.starts_with('/') || trailing.starts_with(',');
            if ends_cleanly {
                return Some((index, prefix));
            }
        }
    }
    None
}

/// Whether the match at `index` follows a Windows drive letter rather than a
/// value separator.
///
/// `C:deps/ffmpeg/linux` names a path relative to the current directory of the
/// `C:` drive, so the `:` belongs to the path and resolving what follows it
/// against the payload root would turn the value into a different absolute path
/// than the document meant.
fn starts_a_drive_relative_path(text: &str, index: usize) -> bool {
    if index < 2 || !text[..index].ends_with(':') {
        return false;
    }
    let mut before = text[..index - 1].chars();
    let Some(letter) = before.next_back() else {
        return false;
    };
    if !letter.is_ascii_alphabetic() {
        return false;
    }
    match before.next_back() {
        None => true,
        Some(separator) => ARGUMENT_VALUE_SEPARATORS.contains(&separator),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mediapm_cas::InMemoryCas;

    use super::*;

    /// Creates a small uncompressed ZIP with the given entries.
    fn make_zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
        use std::io::Write;
        let cursor = std::io::Cursor::new(Vec::new());
        let mut writer = zip::ZipWriter::new(cursor);
        let opts = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        for (name, content) in entries {
            writer.start_file(*name, opts).unwrap();
            writer.write_all(content).unwrap();
        }
        let cursor = writer.finish().unwrap();
        cursor.into_inner()
    }

    #[tokio::test]
    async fn materialize_dir_entry_unpacks_zip() {
        let cas = InMemoryCas::default();
        let zip_bytes = make_zip(&[("foo.txt", b"hello")]);
        let hash = cas.put(bytes::Bytes::from(zip_bytes.clone())).await.unwrap();

        let mut cmap = BTreeMap::new();
        cmap.insert("linux/".to_string(), hash.to_hex());

        let sandbox_dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        materialize_content_map(&cas, &cmap, sandbox_dir.path()).await.unwrap();

        let unpacked = sandbox_dir.path().join("linux/foo.txt");
        let content = std::fs::read(&unpacked).unwrap();
        assert_eq!(content, b"hello");
    }

    #[tokio::test]
    async fn materialize_file_entry_writes_verbatim() {
        let cas = InMemoryCas::default();
        let content = b"#!/bin/sh\necho hello\n";
        let hash = cas.put(bytes::Bytes::from(content.to_vec())).await.unwrap();

        let mut cmap = BTreeMap::new();
        cmap.insert("linux/sd".to_string(), hash.to_hex());

        let sandbox_dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        materialize_content_map(&cas, &cmap, sandbox_dir.path()).await.unwrap();

        let file_path = sandbox_dir.path().join("linux/sd");
        let written = std::fs::read(&file_path).unwrap();
        assert_eq!(written, content);
    }

    #[tokio::test]
    async fn materialize_mixed_entries() {
        let cas = InMemoryCas::default();

        // ZIP entry
        let zip_bytes = make_zip(&[("bin/tool", b"binary")]);
        let zip_hash = cas.put(bytes::Bytes::from(zip_bytes)).await.unwrap();

        // File entry
        let file_content = b"config=value\n";
        let file_hash = cas.put(bytes::Bytes::from(file_content.to_vec())).await.unwrap();

        let mut cmap = BTreeMap::new();
        cmap.insert("linux/".to_string(), zip_hash.to_hex());
        cmap.insert("./cfg".to_string(), file_hash.to_hex());

        let sandbox_dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        materialize_content_map(&cas, &cmap, sandbox_dir.path()).await.unwrap();

        // ZIP-unpacked file
        let tool_path = sandbox_dir.path().join("linux/bin/tool");
        assert!(tool_path.exists(), "ZIP-unpacked file should exist");
        assert_eq!(std::fs::read(&tool_path).unwrap(), b"binary");

        // Verbatim file
        let cfg_path = sandbox_dir.path().join("./cfg");
        assert!(cfg_path.exists(), "verbatim file should exist");
        assert_eq!(std::fs::read(&cfg_path).unwrap(), b"config=value\n");
    }

    #[tokio::test]
    async fn materialize_invalid_hash_skips_silently() {
        let cas = InMemoryCas::default();
        let mut cmap = BTreeMap::new();
        cmap.insert("linux/".to_string(), "not-a-hash".to_string());

        let sandbox_dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        materialize_content_map(&cas, &cmap, sandbox_dir.path()).await.unwrap();

        // No file should be created (the invalid hash is skipped).
        let path = sandbox_dir.path().join("linux/");
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn materialize_empty_zip_packs_nothing() {
        let cas = InMemoryCas::default();
        let zip_bytes = make_zip(&[]);
        let hash = cas.put(bytes::Bytes::from(zip_bytes)).await.unwrap();

        let mut cmap = BTreeMap::new();
        cmap.insert("linux/".to_string(), hash.to_hex());

        let sandbox_dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        materialize_content_map(&cas, &cmap, sandbox_dir.path()).await.unwrap();

        // The directory entry was created (empty).
        let dir = sandbox_dir.path().join("linux/");
        assert!(dir.is_dir());
        // No files inside.
        let mut entries = std::fs::read_dir(&dir).unwrap();
        assert!(entries.next().is_none());
    }
}
