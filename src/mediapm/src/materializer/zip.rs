//! ZIP folder-variant extraction and hierarchy rename-rule compilation.
//!
//! Provides helpers for extracting ZIP-based folder variants into individual
//! file entries and compiling user-defined folder rename rules (regex-based)
//! into compiled forms.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use mediapm_cas::Hash;
use regex::Regex;
use zip::ZipArchive;

use crate::config::hierarchy_types::HierarchyFolderRenameRule;
use crate::error::MediaPmError;

// ---------------------------------------------------------------------------
// Compiled rename rule
// ---------------------------------------------------------------------------

/// A compiled folder rename rule with a cached [`Regex`].
#[derive(Debug, Clone)]
pub(super) struct CompiledFolderRenameRule {
    /// Original pattern string (for diagnostics).
    #[allow(dead_code)]
    pub(super) pattern: String,
    /// Replacement string template.
    pub(super) replacement: String,
    /// Compiled regex for pattern matching.
    #[allow(dead_code)]
    pub(super) regex: Regex,
}

// ---------------------------------------------------------------------------
// ZIP extraction
// ---------------------------------------------------------------------------

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

        let original_path = PathBuf::from(entry.name());
        let normalized = normalize_zip_entry_relative_path(&original_path);
        let renamed = apply_entry_rename_rules(&normalized, rename_rules);

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

// ---------------------------------------------------------------------------
// Rename rule compilation
// ---------------------------------------------------------------------------

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

        compiled.push(CompiledFolderRenameRule {
            pattern: rule.pattern.clone(),
            replacement: rule.replacement.clone(),
            regex,
        });
    }

    Ok(compiled)
}

// ---------------------------------------------------------------------------
// Binding reference parsing
// ---------------------------------------------------------------------------

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
        let entry_name = entry.name().replace('\\', "/");
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

    Err(format!("ZIP member '{member_key}' not found in archive"))
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Normalises a ZIP entry path: strips `./` prefix and leading `/`, and
/// collapses consecutive slashes.
fn normalize_zip_entry_relative_path(path: &Path) -> PathBuf {
    let mut components: Vec<_> = path
        .components()
        .filter_map(|c| {
            let s = c.as_os_str().to_string_lossy().to_string();
            if s == "." || s.is_empty() { None } else { Some(s) }
        })
        .collect();

    // Collapse empty segments produced by double slashes.
    components.retain(|c| !c.is_empty());

    PathBuf::from(components.join("/"))
}

/// Applies a sequence of compiled folder rename rules to a normalized path's
/// file-name component (last segment). Non-leaf path components are not
/// renamed.
fn apply_entry_rename_rules(path: &Path, rules: &[CompiledFolderRenameRule]) -> PathBuf {
    let parent = path.parent().map(PathBuf::from);
    let file_name = path.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default();

    let mut renamed = file_name;
    for rule in rules {
        renamed = rule.regex.replace_all(&renamed, rule.replacement.as_str()).to_string();
    }

    match parent {
        Some(p) => p.join(renamed),
        None => PathBuf::from(renamed),
    }
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
