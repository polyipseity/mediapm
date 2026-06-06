//! ZIP archive extraction, rename-rule compilation, and external data reference parsing.

use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::path::Path;

use regex::Regex;

use mediapm_cas::Hash;

use crate::config::HierarchyFolderRenameRule;
use crate::error::MediaPmError;

use super::CompiledHierarchyFolderRenameRule;
use super::commit::sanitize_hierarchy_path;

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
            entry
                .read_to_end(&mut bytes)
                .map_err(|error| format!("reading ZIP member '{member_key}' failed: {error}"))?;
            return Ok(bytes);
        }
        index = index.saturating_add(1);
    }

    Err(format!("ZIP member '{member_key}' not found"))
}

/// Extracts one ZIP folder payload into a staged directory with merge checks.
///
/// Multiple hierarchy variants may contribute archive entries into the same
/// destination directory. This helper enforces strict path-collision rules so
/// no file or directory path can be overwritten by a later variant, except
/// duplicate file paths where the first extracted file is retained.
#[expect(
    clippy::too_many_arguments,
    reason = "folder extraction requires explicit runtime context and mutable ledgers"
)]
pub(super) fn extract_zip_folder_variant_bytes(
    zip_bytes: &[u8],
    target_dir: &Path,
    hierarchy_path: &str,
    media_id: &str,
    variant: &str,
    rename_rules: &[CompiledHierarchyFolderRenameRule],
    entry_sanitization: &BTreeMap<char, char>,
    extracted_entries: &mut BTreeMap<String, bool>,
    extracted_entry_variants: &mut BTreeMap<String, String>,
) -> Result<(), MediaPmError> {
    let reader = std::io::Cursor::new(zip_bytes);
    let mut archive = zip::ZipArchive::new(reader).map_err(|error| {
        MediaPmError::Workflow(format!(
            "hierarchy path '{hierarchy_path}' media '{media_id}' variant '{variant}' is expected to be a ZIP folder payload: {error}"
        ))
    })?;

    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).map_err(|error| {
            MediaPmError::Workflow(format!(
                "reading ZIP entry #{index} for hierarchy path '{hierarchy_path}' media '{media_id}' variant '{variant}' failed: {error}"
            ))
        })?;

        let normalized = normalize_zip_entry_relative_path(entry.name()).map_err(|reason| {
            MediaPmError::Workflow(format!(
                "invalid ZIP entry '{}' for hierarchy path '{hierarchy_path}' media '{media_id}' variant '{variant}': {reason}",
                entry.name()
            ))
        })?;

        if normalized.is_empty() {
            continue;
        }

        if entry.is_dir() {
            register_zip_directory_entry(&normalized, extracted_entries).map_err(|reason| {
                MediaPmError::Workflow(format!(
                    "directory merge conflict for hierarchy path '{hierarchy_path}' media '{media_id}' variant '{variant}': {reason}"
                ))
            })?;

            let directory_path = target_dir.join(&normalized);
            fs::create_dir_all(&directory_path).map_err(|source| MediaPmError::Io {
                operation: "creating staged hierarchy directory from ZIP payload".to_string(),
                path: directory_path,
                source,
            })?;
        } else {
            let renamed = apply_hierarchy_folder_rename_rules(
                &normalized,
                rename_rules,
                hierarchy_path,
                media_id,
                variant,
            )?;

            let sanitized = sanitize_hierarchy_path(&renamed, entry_sanitization);
            if sanitized.contains('/') || sanitized.contains('\\') {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy path '{hierarchy_path}' media '{media_id}' variant '{variant}' ZIP entry '{normalized}' after rename/sanitization produced multi-component path '{sanitized}'",
                )));
            }

            let should_write_entry =
                register_zip_file_entry(&sanitized, extracted_entries).map_err(|reason| {
                    MediaPmError::Workflow(format!(
                        "file merge conflict for hierarchy path '{hierarchy_path}' media '{media_id}' variant '{variant}': {reason}"
                    ))
                })?;

            if !should_write_entry {
                continue;
            }

            extracted_entry_variants.insert(sanitized.clone(), variant.to_string());

            let file_path = target_dir.join(&sanitized);
            if let Some(parent) = file_path.parent() {
                fs::create_dir_all(parent).map_err(|source| MediaPmError::Io {
                    operation: "creating staged hierarchy file parent from ZIP payload".to_string(),
                    path: parent.to_path_buf(),
                    source,
                })?;
            }

            let mut bytes = Vec::new();
            entry.read_to_end(&mut bytes).map_err(|error| {
                MediaPmError::Workflow(format!(
                    "reading ZIP file entry '{}' for hierarchy path '{hierarchy_path}' media '{media_id}' variant '{variant}' failed: {error}",
                    entry.name()
                ))
            })?;

            fs::write(&file_path, bytes).map_err(|source| MediaPmError::Io {
                operation: "writing staged hierarchy file from ZIP payload".to_string(),
                path: file_path,
                source,
            })?;
        }
    }

    Ok(())
}

/// Compiles configured folder rename rules for one hierarchy entry.
pub(super) fn compile_hierarchy_folder_rename_rules(
    rules: &[HierarchyFolderRenameRule],
    hierarchy_path: &str,
    media_id: &str,
) -> Result<Vec<CompiledHierarchyFolderRenameRule>, MediaPmError> {
    rules
        .iter()
        .enumerate()
        .map(|(rule_index, rule)| {
            let pattern = rule.pattern.trim();
            if pattern.is_empty() {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy path '{hierarchy_path}' media '{media_id}' has empty rename_files[{rule_index}] pattern"
                )));
            }
            let regex = Regex::new(pattern).map_err(|error| {
                MediaPmError::Workflow(format!(
                    "hierarchy path '{hierarchy_path}' media '{media_id}' has invalid rename_files[{rule_index}] pattern '{pattern}': {error}"
                ))
            })?;

            Ok(CompiledHierarchyFolderRenameRule {
                pattern: pattern.to_string(),
                replacement: rule.replacement.clone(),
                regex,
            })
        })
        .collect()
}

/// Applies ordered rename rules to one normalized ZIP file member path.
pub(super) fn apply_hierarchy_folder_rename_rules(
    normalized_file_path: &str,
    rules: &[CompiledHierarchyFolderRenameRule],
    hierarchy_path: &str,
    media_id: &str,
    variant: &str,
) -> Result<String, MediaPmError> {
    if rules.is_empty() {
        return Ok(normalized_file_path.to_string());
    }

    let renamed = rules.iter().fold(normalized_file_path.to_string(), |current, rule| {
        rule.regex.replace_all(current.as_str(), rule.replacement.as_str()).into_owned()
    });

    let normalized_renamed = normalize_zip_entry_relative_path(&renamed).map_err(|reason| {
        let patterns = rules.iter().map(|rule| rule.pattern.as_str()).collect::<Vec<_>>();
        MediaPmError::Workflow(format!(
            "hierarchy path '{hierarchy_path}' media '{media_id}' variant '{variant}' rename_files {patterns:?} transformed ZIP file path '{normalized_file_path}' into invalid path '{renamed}': {reason}",
        ))
    })?;

    if normalized_renamed.is_empty() {
        let patterns = rules.iter().map(|rule| rule.pattern.as_str()).collect::<Vec<_>>();
        return Err(MediaPmError::Workflow(format!(
            "hierarchy path '{hierarchy_path}' media '{media_id}' variant '{variant}' rename_files {patterns:?} transformed ZIP file path '{normalized_file_path}' to an empty path",
        )));
    }

    Ok(normalized_renamed)
}

/// Normalizes one ZIP entry path into a safe relative path.
fn normalize_zip_entry_relative_path(entry_name: &str) -> Result<String, String> {
    let mut normalized = entry_name.replace('\\', "/");
    while let Some(stripped) = normalized.strip_prefix('/') {
        normalized = stripped.to_string();
    }
    while let Some(stripped) = normalized.strip_prefix("./") {
        normalized = stripped.to_string();
    }

    let mut components = Vec::new();
    for segment in normalized.split('/') {
        if segment.is_empty() {
            continue;
        }
        if segment == "." || segment == ".." {
            return Err("contains '.' or '..' path components".to_string());
        }
        if segment.contains(':') {
            return Err("contains ':' path segment characters".to_string());
        }
        components.push(segment);
    }

    Ok(components.join("/"))
}

/// Registers one ZIP directory path, rejecting file/directory collisions.
fn register_zip_directory_entry(
    entry_path: &str,
    extracted_entries: &mut BTreeMap<String, bool>,
) -> Result<(), String> {
    let mut cursor = String::new();

    for segment in entry_path.split('/') {
        if !cursor.is_empty() {
            cursor.push('/');
        }
        cursor.push_str(segment);

        match extracted_entries.get(&cursor).copied() {
            Some(true) => {}
            Some(false) => {
                return Err(format!(
                    "directory '{entry_path}' conflicts with existing file '{cursor}'"
                ));
            }
            None => {
                extracted_entries.insert(cursor.clone(), true);
            }
        }
    }

    Ok(())
}

/// Registers one ZIP file path and returns whether caller should write bytes.
///
/// Return value semantics:
/// - `Ok(true)`: caller should write file bytes,
/// - `Ok(false)`: duplicate file path encountered; keep first-writer bytes,
/// - `Err(..)`: invalid file/dir collision.
pub(super) fn register_zip_file_entry(
    entry_path: &str,
    extracted_entries: &mut BTreeMap<String, bool>,
) -> Result<bool, String> {
    let mut parts = entry_path.split('/').collect::<Vec<_>>();
    if parts.is_empty() {
        return Err("file entry path is empty".to_string());
    }

    let file_name = parts.pop().expect("checked non-empty split result");
    let mut parent = String::new();

    for segment in parts {
        if !parent.is_empty() {
            parent.push('/');
        }
        parent.push_str(segment);

        match extracted_entries.get(&parent).copied() {
            Some(true) => {}
            Some(false) => {
                return Err(format!(
                    "file '{entry_path}' has parent '{parent}' that is already a file"
                ));
            }
            None => {
                extracted_entries.insert(parent.clone(), true);
            }
        }
    }

    let full_file_path =
        if parent.is_empty() { file_name.to_string() } else { format!("{parent}/{file_name}") };

    match extracted_entries.get(&full_file_path).copied() {
        Some(true) => {
            Err(format!("file '{entry_path}' conflicts with existing directory '{full_file_path}'"))
        }
        Some(false) => {
            // Keep first writer semantics for duplicate file names produced by
            // overlapping sidecar families (for example subtitle vs
            // auto-subtitle flattening into one media root).
            Ok(false)
        }
        None => {
            extracted_entries.insert(full_file_path, false);
            Ok(true)
        }
    }
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
