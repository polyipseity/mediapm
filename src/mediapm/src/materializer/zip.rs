//! ZIP folder-variant extraction and hierarchy rename-rule compilation.
//!
//! Provides helpers for extracting ZIP-based folder variants into individual
//! file entries and compiling user-defined folder rename rules (regex-based)
//! into compiled forms.

use std::collections::BTreeSet;
use std::path::PathBuf;

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
// Internal helpers
// ---------------------------------------------------------------------------

/// Normalises a ZIP entry path: strips `./` prefix and leading `/`, and
/// collapses consecutive slashes.
fn normalize_zip_entry_relative_path(path: &PathBuf) -> PathBuf {
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
fn apply_entry_rename_rules(path: &PathBuf, rules: &[CompiledFolderRenameRule]) -> PathBuf {
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
