//! ZIP folder-variant extraction and hierarchy rename-rule compilation.
//!
//! Provides helpers for extracting ZIP-based folder variants into individual
//! file entries and compiling user-defined folder rename rules (regex-based)
//! into compiled forms.
//!
//! # Member names are attacker-controlled
//!
//! A ZIP member name is arbitrary bytes chosen by whatever produced the
//! archive. It is **not** validated by the archive format, and it is not
//! chosen by the user, who only chose the tool that emitted the archive. Every
//! member name read here is therefore untrusted input.
//!
//! The required accessor is [`zip::read::ZipFile::enclosed_name`], which
//! returns `None` for any member whose declared name escapes the archive root.
//! **Never** substitute [`zip::read::ZipFile::name`] for it: `name()` is the
//! raw, unchecked string, and a member named `../evil.txt` is exactly the
//! input that would escape the target media folder. When `enclosed_name()`
//! returns `None` this module **refuses the member with an error naming it** —
//! never a silent skip, which would let a caller believe an archive was fully
//! materialized when it was not.
//!
//! Three stages gate a member name, and all three are required:
//!
//! 1. [`zip::read::ZipFile::enclosed_name`] rejects a name that escapes the
//!    archive root (`../evil.txt`, `..\..\evil.txt`). Absolute names
//!    (`/etc/evil.txt`, `C:\evil.txt`) are rejected separately by
//!    [`declared_member_is_absolute`], because `enclosed_name()` silently
//!    re-roots those instead of refusing them.
//! 2. [`normalize_zip_entry_relative_path`] rejects a surviving `..`
//!    component explicitly, and
//! 3. [`apply_entry_rename_rules`] re-parses the **final** path through
//!    [`PathComponent`], because a user-configured regex replacement
//!    runs *after* stage 2 and can emit `/`, `\`, or `..` that no earlier
//!    stage ever saw.

use std::collections::BTreeSet;
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};

use mediapm_cas::Hash;
use regex::Regex;
use zip::ZipArchive;
use zip::read::ZipFile;

use crate::config::hierarchy_types::HierarchyFolderRenameRule;
use crate::error::MediaPmError;
use crate::materializer::commit::{join_path_components, parse_relative_path_components};
use crate::path_component::{PathComponent, SanitizePolicy};

/// A compiled folder rename rule with a cached [`Regex`].
#[derive(Debug, Clone)]
pub(super) struct CompiledFolderRenameRule {
    /// Replacement string template.
    pub(super) replacement: String,
    /// Compiled regex for pattern matching.
    pub(super) regex: Regex,
}

/// Extracts all file entries from a ZIP archive stored in `data`, normalising
/// entry paths and applying the given rename rules to the path components.
///
/// Returns a sorted list of `(relative_path, bytes)` pairs. Directory entries
/// are not included — only their file descendants.
pub(super) fn extract_zip_folder_variant_bytes(
    data: &[u8],
    rename_rules: &[CompiledFolderRenameRule],
) -> Result<Vec<(PathBuf, Vec<u8>)>, MediaPmError> {
    let mut archive = ZipArchive::new(std::io::Cursor::new(data))
        .map_err(|e| MediaPmError::Workflow(format!("failed to open ZIP archive: {e}")))?;

    // Collect file entries, tracking directories to avoid stale dir entries.
    let mut dirs: BTreeSet<PathBuf> = BTreeSet::new();
    let mut files: Vec<(PathBuf, Vec<u8>)> = Vec::new();

    for i in 0..archive.len() {
        let entry = archive
            .by_index(i)
            .map_err(|e| MediaPmError::Workflow(format!("failed to read ZIP entry #{i}: {e}")))?;

        let declared_name = entry.name().to_string();
        if declared_member_is_absolute(&declared_name) {
            return Err(MediaPmError::Workflow(format!(
                "refusing ZIP member '{declared_name}' (#{i}): the declared name is absolute"
            )));
        }
        let original_path = entry.enclosed_name().ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "refusing ZIP member '{declared_name}' (#{i}): the declared name escapes the archive root"
            ))
        })?;
        let normalized = normalize_zip_entry_relative_path(&original_path)?;
        let renamed = apply_entry_rename_rules(&normalized, rename_rules)?;

        if entry.is_dir() {
            dirs.insert(renamed);
        } else {
            #[expect(
                clippy::cast_possible_truncation,
                reason = "zip entry sizes are u64 from the archive; truncation is a no-op on 64-bit targets"
            )]
            let mut bytes = Vec::with_capacity(entry.size() as usize);
            // We need to handle the entry read carefully since `by_index` returns a read-only archive.
            drop(entry);
            // Re-open the entry for extraction.
            let mut entry_reader = archive.by_index(i).map_err(|e| {
                MediaPmError::Workflow(format!("failed to re-open ZIP entry #{i}: {e}"))
            })?;
            std::io::Read::read_to_end(&mut entry_reader, &mut bytes).map_err(|e| {
                MediaPmError::Workflow(format!(
                    "failed to read ZIP entry '{}' (#{i}): {e}",
                    entry_reader.name()
                ))
            })?;
            files.push((renamed, bytes));
        }
    }

    // Sort for deterministic output order.
    files.sort_by(|(a, _), (b, _)| a.cmp(b));

    Ok(files)
}

/// Compiles a slice of [`HierarchyFolderRenameRule`] into
/// [`CompiledFolderRenameRule`] instances.
///
/// Returns an error if any pattern fails to compile as a regex.
pub(super) fn compile_hierarchy_folder_rename_rules(
    rules: &[HierarchyFolderRenameRule],
) -> Result<Vec<CompiledFolderRenameRule>, MediaPmError> {
    let mut compiled = Vec::with_capacity(rules.len());

    for rule in rules {
        let regex = Regex::new(&rule.pattern).map_err(|e| {
            MediaPmError::Workflow(format!("invalid folder rename pattern '{}': {e}", rule.pattern))
        })?;

        compiled.push(CompiledFolderRenameRule { replacement: rule.replacement.clone(), regex });
    }

    Ok(compiled)
}

/// Parsed `${step_output...}` binding reference metadata.
pub(super) struct StepOutputReference<'a> {
    /// Producer step id.
    pub(super) step_id: &'a str,
    /// Producer output name.
    pub(super) output_name: &'a str,
    /// Optional ZIP-member selector.
    pub(super) zip_member: Option<&'a str>,
}

/// Parses exact `${step_output.<step_id>.<output_name>}` references with
/// optional `${step_output.<step_id>.<output_name>:zip(<member>)}` selector.
pub(super) fn parse_step_output_reference(value: &str) -> Option<StepOutputReference<'_>> {
    let content = value.strip_prefix("${step_output.")?.strip_suffix('}')?;

    let (selector, zip_member) = if let Some(without_suffix) = content.strip_suffix(')') {
        if let Some((prefix, member)) = without_suffix.rsplit_once(":zip(") {
            if member.is_empty() || member.contains('/') || member.contains('\\') {
                return None;
            }
            (prefix, Some(member))
        } else {
            (content, None)
        }
    } else {
        (content, None)
    };

    let (step_id, output_name) = selector.rsplit_once('.')?;
    if step_id.is_empty() || output_name.is_empty() {
        return None;
    }

    Some(StepOutputReference { step_id, output_name, zip_member })
}

/// Parses exact `${external_data.<hash>}` references.
pub(super) fn parse_external_data_reference(value: &str) -> Result<Option<Hash>, MediaPmError> {
    let Some(hash_text) =
        value.strip_prefix("${external_data.").and_then(|text| text.strip_suffix('}'))
    else {
        return Ok(None);
    };

    if hash_text.is_empty() {
        return Err(MediaPmError::Workflow(
            "workflow binding '${external_data.<hash>}' requires a non-empty hash".to_string(),
        ));
    }

    let hash = hash_text.parse::<Hash>().map_err(|source| {
        MediaPmError::Workflow(format!(
            "workflow binding references invalid external_data hash '{hash_text}': {source}"
        ))
    })?;
    Ok(Some(hash))
}

/// Extracts one file payload from ZIP bytes using one flat member key.
pub(super) fn extract_zip_member_bytes(
    zip_bytes: &[u8],
    member_key: &str,
) -> Result<Vec<u8>, String> {
    if member_key.is_empty() || member_key.contains('/') || member_key.contains('\\') {
        return Err(
            "ZIP member key must be non-empty and must not contain path separators".to_string()
        );
    }

    let reader = std::io::Cursor::new(zip_bytes);
    let mut archive = zip::ZipArchive::new(reader)
        .map_err(|error| format!("decoding ZIP payload failed: {error}"))?;

    let mut index = 0usize;
    while index < archive.len() {
        let mut entry = archive
            .by_index(index)
            .map_err(|error| format!("reading ZIP entry #{index} failed: {error}"))?;
        let entry_name = enclosed_member_name(&entry, index)?;
        if entry_name == member_key {
            if entry.is_dir() {
                return Err(format!("ZIP member '{member_key}' resolves to a directory"));
            }
            let mut bytes = Vec::new();
            std::io::Read::read_to_end(&mut entry, &mut bytes)
                .map_err(|error| format!("reading ZIP member '{member_key}' failed: {error}"))?;
            return Ok(bytes);
        }
        index += 1;
    }

    if member_key.starts_with('.') {
        return extract_zip_member_bytes_by_suffix(zip_bytes, member_key);
    }

    Err(format!("ZIP member '{member_key}' not found in archive"))
}

fn extract_zip_member_bytes_by_suffix(zip_bytes: &[u8], suffix: &str) -> Result<Vec<u8>, String> {
    if suffix.is_empty() || suffix.contains('/') || suffix.contains('\\') {
        return Err(
            "ZIP member suffix must be non-empty and must not contain path separators".to_string()
        );
    }

    let reader = std::io::Cursor::new(zip_bytes);
    let mut archive = zip::ZipArchive::new(reader)
        .map_err(|error| format!("decoding ZIP payload failed: {error}"))?;

    let mut index = 0usize;
    while index < archive.len() {
        let mut entry = archive
            .by_index(index)
            .map_err(|error| format!("reading ZIP entry #{index} failed: {error}"))?;
        let entry_name = enclosed_member_name(&entry, index)?;
        if !entry.is_dir() && entry_name.ends_with(suffix) {
            let mut bytes = Vec::new();
            std::io::Read::read_to_end(&mut entry, &mut bytes)
                .map_err(|error| format!("reading ZIP member suffix '{suffix}' failed: {error}"))?;
            return Ok(bytes);
        }
        index += 1;
    }

    Err(format!("ZIP member suffix '{suffix}' not found in archive"))
}

/// Returns whether one declared member name is absolute or drive-qualified.
///
/// [`ZipFile::enclosed_name`] alone is not sufficient here. It refuses a name
/// that *escapes* the archive root, but it tolerates a leading separator by
/// silently re-rooting the member: `/etc/evil.txt` comes back as
/// `etc/evil.txt` rather than `None`. That is traversal-safe but wrong for an
/// archive contract — the member would be materialized at a path the
/// producing tool never declared, under a different layout than the archive
/// describes. This check refuses the declared form before `enclosed_name()` is
/// consulted, so an absolute member is an error the caller can see.
fn declared_member_is_absolute(raw_name: &str) -> bool {
    let mut chars = raw_name.chars();
    match chars.next() {
        Some('/' | '\\') => true,
        Some(drive) if drive.is_ascii_alphabetic() => chars.next() == Some(':'),
        _ => false,
    }
}

/// Returns the enclosed relative name of one ZIP member, refusing members
/// whose declared name is absolute or escapes the archive root.
///
/// The comparison helpers below match on member names, so a member whose name
/// escapes the root must never be selectable — otherwise a lookup keyed on a
/// bare file name (`evil.txt`) would match an archive entry that actually
/// lives at `../evil.txt`.
///
/// # Errors
///
/// Returns a message naming the offending member when the declared name is
/// absolute, or when [`ZipFile::enclosed_name`] returns `None`.
fn enclosed_member_name<R: Read + Seek>(
    entry: &ZipFile<'_, R>,
    index: usize,
) -> Result<String, String> {
    let raw = entry.name().to_string();
    if declared_member_is_absolute(&raw) {
        return Err(format!(
            "refusing ZIP member '{raw}' (#{index}): the declared name is absolute"
        ));
    }
    entry.enclosed_name().map(|path| path.to_string_lossy().replace('\\', "/")).ok_or_else(|| {
        format!(
            "refusing ZIP member '{raw}' (#{index}): the declared name escapes the archive root"
        )
    })
}

/// Normalises a ZIP entry path: strips `./` prefix and leading `/`, and
/// collapses consecutive slashes.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] when a `..` component survives, or when
/// nothing is left after normalization. A `..` component is rejected rather
/// than resolved: the archive is untrusted input, and resolving traversal
/// would silently move the member to a path the tool never declared.
fn normalize_zip_entry_relative_path(path: &Path) -> Result<PathBuf, MediaPmError> {
    let mut components: Vec<_> = path
        .components()
        .filter_map(|c| {
            let s = c.as_os_str().to_string_lossy().to_string();
            if s == "." || s.is_empty() { None } else { Some(s) }
        })
        .collect();

    // Collapse empty segments produced by double slashes.
    components.retain(|c| !c.is_empty());

    if components.is_empty() {
        return Err(MediaPmError::Workflow(format!(
            "refusing ZIP member '{}': the name is empty after normalization",
            path.to_string_lossy()
        )));
    }
    if let Some(traversal) = components.iter().find(|component| component.as_str() == "..") {
        return Err(MediaPmError::Workflow(format!(
            "refusing ZIP member '{}': path traversal component '{traversal}' is not permitted",
            path.to_string_lossy()
        )));
    }

    Ok(PathBuf::from(components.join("/")))
}

/// Applies a sequence of compiled folder rename rules to a normalized path's
/// file-name component (last segment). Non-leaf path components are not
/// renamed.
///
/// The regex replacement is user-configured text that runs **after**
/// [`normalize_zip_entry_relative_path`], so it can emit `/`, `\`, or `..`
/// that no earlier stage inspected. The renamed leaf is therefore parsed as a
/// single [`PathComponent`], and the **final** joined path is parsed again, so
/// the traversal guard holds for the path that actually reaches
/// `Path::join`.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] naming the produced path when the
/// replacement yields an empty, traversing, separator-bearing, or
/// control-character component.
fn apply_entry_rename_rules(
    path: &Path,
    rules: &[CompiledFolderRenameRule],
) -> Result<PathBuf, MediaPmError> {
    let parent = path.parent().map(PathBuf::from);
    let file_name = path.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default();

    let mut renamed = file_name;
    for rule in rules {
        renamed = rule.regex.replace_all(&renamed, rule.replacement.as_str()).to_string();
    }

    // Parse the replacement output as ONE component: a `/` or `\` in the
    // replacement is a violation, not a new path level.
    let leaf = PathComponent::parse(&renamed, &SanitizePolicy::disabled()).map_err(|error| {
        MediaPmError::Workflow(format!(
            "folder rename rule produced an unsafe file name '{renamed}': {error}"
        ))
    })?;

    let candidate = match parent {
        Some(p) => p.join(leaf_path(&leaf)),
        None => leaf_path(&leaf),
    };

    // Re-check the FINAL path, not just the leaf: this is the value that
    // reaches `target_path.join(...)` on the materializer side.
    let validated = parse_relative_path_components(&candidate, &SanitizePolicy::disabled())
        .map_err(|error| {
            MediaPmError::Workflow(format!(
                "folder rename rule produced an unsafe path '{}': {error}",
                candidate.to_string_lossy()
            ))
        })?;
    Ok(join_path_components(&validated))
}

/// Returns the single path segment a [`PathComponent`] represents.
///
/// # Panics
///
/// Never: a `PathComponent` is by construction one validated segment with no
/// separator, so it maps to exactly one [`PathBuf`].
fn leaf_path(component: &PathComponent) -> PathBuf {
    join_path_components(std::slice::from_ref(component))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use zip::write::FileOptions;

    fn make_zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut buffer = std::io::Cursor::new(Vec::new());
        let mut zip = zip::ZipWriter::new(&mut buffer);
        for (name, data) in entries {
            zip.start_file::<&str, ()>(*name, FileOptions::default()).unwrap();
            zip.write_all(data).unwrap();
        }
        zip.finish().unwrap();
        buffer.into_inner()
    }

    #[test]
    fn extract_empty_zip() {
        let data = make_zip(&[]);
        let result = extract_zip_folder_variant_bytes(&data, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn extract_single_file() {
        let data = make_zip(&[("test.txt", b"hello")]);
        let result = extract_zip_folder_variant_bytes(&data, &[]).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, PathBuf::from("test.txt"));
        assert_eq!(result[0].1, b"hello");
    }

    #[test]
    fn extract_nested_files() {
        let data = make_zip(&[("dir/a.txt", b"aaa"), ("dir/sub/b.txt", b"bbb")]);
        let result = extract_zip_folder_variant_bytes(&data, &[]).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, PathBuf::from("dir/a.txt"));
        assert_eq!(result[0].1, b"aaa");
        assert_eq!(result[1].0, PathBuf::from("dir/sub/b.txt"));
        assert_eq!(result[1].1, b"bbb");
    }

    #[test]
    fn extract_zip_member_bytes_matches_language_suffix() {
        let data = make_zip(&[("Rick Astley.en.vtt", b"WEBVTT")]);
        let bytes = extract_zip_member_bytes(&data, ".en.vtt").expect("suffix match");
        assert_eq!(bytes, b"WEBVTT");
    }

    #[test]
    fn extract_absolute_member_is_rejected_and_named() {
        // `enclosed_name()` alone re-roots `/etc/evil.txt` to `etc/evil.txt`
        // instead of refusing it, so the absolute-name check must run first.
        let data = make_zip(&[("/etc/evil.txt", b"pwn")]);
        let err = extract_zip_folder_variant_bytes(&data, &[]).unwrap_err();
        let rendered = err.to_string();
        assert!(
            rendered.contains("/etc/evil.txt"),
            "the error must name the refused member; got: {rendered}"
        );
        assert!(
            rendered.contains("is absolute"),
            "the error must report the absolute declared name; got: {rendered}"
        );
    }

    #[test]
    fn extract_backslash_traversal_member_is_rejected_and_named() {
        let data = make_zip(&[("..\\..\\evil.txt", b"pwn")]);
        let err = extract_zip_folder_variant_bytes(&data, &[]).unwrap_err();
        let rendered = err.to_string();
        assert!(
            rendered.contains("..\\..\\evil.txt"),
            "the error must name the refused member; got: {rendered}"
        );
    }

    #[test]
    fn extract_dotdot_member_is_rejected_and_named() {
        let data = make_zip(&[("../evil.txt", b"pwn")]);
        let err = extract_zip_folder_variant_bytes(&data, &[]).unwrap_err();
        let rendered = err.to_string();
        assert!(
            rendered.contains("../evil.txt"),
            "the error must name the refused member; got: {rendered}"
        );
    }

    #[test]
    fn extract_member_lookup_rejects_escaping_member() {
        let data = make_zip(&[("../evil.txt", b"pwn")]);
        let err = extract_zip_member_bytes(&data, "evil.txt").unwrap_err();
        let rendered = err.clone();
        assert!(
            rendered.contains("../evil.txt"),
            "the lookup error must name the escaping member; got: {rendered}"
        );
    }

    #[test]
    fn extract_nested_member_path_is_preserved() {
        let data = make_zip(&[("a/b/c.txt", b"nested")]);
        let result = extract_zip_folder_variant_bytes(&data, &[]).unwrap();
        assert_eq!(result, vec![(PathBuf::from("a/b/c.txt"), b"nested".to_vec())]);
    }

    #[test]
    fn rename_rule_emitting_traversal_is_rejected() {
        let data = make_zip(&[("cover.jpg", b"img")]);
        let rules = compile_hierarchy_folder_rename_rules(&[HierarchyFolderRenameRule {
            pattern: "^cover".to_string(),
            replacement: "../$1".to_string(),
        }])
        .expect("pattern compiles");
        let err = extract_zip_folder_variant_bytes(&data, &rules).unwrap_err();
        let rendered = err.to_string();
        // The replacement `../$1` turns `cover.jpg` into `../.jpg`. Parsed as a
        // single component it fails on the embedded separator, which is the
        // first rule a separator-bearing replacement violates.
        assert!(
            rendered.contains("contains forbidden characters"),
            "the error must report the separator violation; got: {rendered}"
        );
        assert!(
            rendered.contains("../.jpg"),
            "the error must name the produced file name; got: {rendered}"
        );
    }

    #[test]
    fn rename_rule_with_ordinary_replacement_still_applies() {
        let data = make_zip(&[("cover.jpg", b"img")]);
        let rules = compile_hierarchy_folder_rename_rules(&[HierarchyFolderRenameRule {
            pattern: "\\.jpg$".to_string(),
            replacement: ".jpeg".to_string(),
        }])
        .expect("pattern compiles");
        let result = extract_zip_folder_variant_bytes(&data, &rules).unwrap();
        assert_eq!(result, vec![(PathBuf::from("cover.jpeg"), b"img".to_vec())]);
    }

    #[test]
    fn compile_invalid_regex() {
        let rules = &[HierarchyFolderRenameRule {
            pattern: "[invalid".to_string(),
            replacement: "x".to_string(),
        }];
        let err = compile_hierarchy_folder_rename_rules(rules).unwrap_err();
        assert!(err.to_string().contains("invalid folder rename pattern"));
    }

    #[test]
    fn non_zip_returns_error() {
        let data = b"this is not a zip file";
        let result = extract_zip_folder_variant_bytes(data, &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("failed to open ZIP archive"));
    }
}
