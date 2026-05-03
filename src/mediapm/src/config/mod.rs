//! Nickel-backed Phase 3 `mediapm.ncl` and `state.ncl` document model and I/O
//! helpers.
//!
//! The `mediapm.ncl` file is the declarative desired-state surface for Phase
//! 3: media sources, hierarchy mapping, and desired tool enablement.
//! The `state.ncl` file is the machine-managed realized-state surface. Both
//! documents share the same versioned Nickel schema and merge into one runtime
//! `MediaPmDocument`.
//!
//! We evaluate Nickel through `nickel-lang-core` and deserialize the exported
//! value into Rust structs. This keeps parsing behavior deterministic while still
//! supporting regular Nickel syntax in user-authored files.

use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use mediapm_cas::Hash;
use mediapm_conductor::{
    default_runtime_inherited_env_vars_for_host, use_user_download_cache_enabled,
};
use nickel_lang_core::error::{Error as NickelError, NullReporter};
use nickel_lang_core::eval::cache::CacheImpl;
use nickel_lang_core::program::Program;
use regex::Regex;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use url::Url;

use crate::error::MediaPmError;

pub(crate) mod versions;

/// Current persisted schema marker for `mediapm.ncl`.
pub const MEDIAPM_DOCUMENT_VERSION: u32 = versions::latest_nickel_version();

/// Default max number of ffmpeg indexed input slots when `tools.ffmpeg`
/// does not provide an explicit override.
pub const DEFAULT_FFMPEG_MAX_INPUT_SLOTS: u32 = 64;
/// Default max number of ffmpeg indexed output slots when `tools.ffmpeg`
/// does not provide an explicit override.
pub const DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS: u32 = 64;

/// Default runtime materialization fallback order.
///
/// The order is intentionally deterministic so managed-file realization remains
/// predictable across hosts and repeated sync runs.
pub const DEFAULT_MATERIALIZATION_PREFERENCE_ORDER: [MaterializationMethod; 4] = [
    MaterializationMethod::Hardlink,
    MaterializationMethod::Symlink,
    MaterializationMethod::Reflink,
    MaterializationMethod::Copy,
];

/// Platform-keyed inherited environment-variable names.
///
/// Keys are normalized case-insensitively at merge/read time so users can
/// author values with natural casing (`windows`, `Windows`, `WINDOWS`, ...)
/// without changing runtime semantics.
pub type PlatformInheritedEnvVars = BTreeMap<String, Vec<String>>;

/// Internal prefix used for regex-based variant selectors.
///
/// `mediapm.ncl` represents regex selectors as object values
/// (`{ regex = "..." }`). Internally we preserve API compatibility by storing
/// selectors as strings and tagging regex selectors with this prefix.
const REGEX_VARIANT_SELECTOR_PREFIX: &str = "__mediapm_regex__:";

/// One flattened hierarchy materialization entry derived from schema nodes.
///
/// `mediapm.ncl` persists hierarchy as an ordered recursive node list. Runtime
/// validation/materialization currently operates on explicit flat paths and
/// leaf payloads, so node decoding expands into this intermediate model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FlattenedHierarchyEntry {
    /// Flat relative path template for one materialization leaf.
    pub path: String,
    /// Leaf entry payload consumed by runtime validation/materializer logic.
    pub entry: HierarchyEntry,
    /// Optional explicit hierarchy id declared on the source hierarchy node.
    pub hierarchy_id: Option<String>,
}

/// Normalizes one node `path` value into slash-separated relative form.
fn normalize_hierarchy_node_path(raw_path: &str, context_label: &str) -> Result<String, String> {
    let normalized = raw_path.trim().replace('\\', "/");
    if normalized.is_empty() {
        return Ok(String::new());
    }

    if normalized.starts_with('/') {
        return Err(format!(
            "{context_label} path '{raw_path}' must stay relative (no leading '/')"
        ));
    }

    let trimmed = normalized.trim_matches('/');
    if trimmed.is_empty() {
        return Ok(String::new());
    }

    let mut segments = Vec::new();
    for segment in trimmed.split('/') {
        let value = segment.trim();
        if value.is_empty() {
            return Err(format!(
                "{context_label} path '{raw_path}' must not contain empty path segments"
            ));
        }
        if value == "." || value == ".." {
            return Err(format!(
                "{context_label} path '{raw_path}' must not contain '.' or '..' segments"
            ));
        }
        segments.push(value.to_string());
    }

    Ok(segments.join("/"))
}

/// Joins one parent path and one node-local path with slash separators.
#[must_use]
fn join_hierarchy_node_paths(parent: &str, child: &str) -> String {
    if parent.is_empty() {
        child.to_string()
    } else if child.is_empty() {
        parent.to_string()
    } else {
        format!("{parent}/{child}")
    }
}

/// Returns one normalized optional non-empty string field.
fn normalize_optional_non_empty_field(
    field_name: &str,
    value: Option<&str>,
    context_label: &str,
) -> Result<Option<String>, String> {
    let Some(raw) = value else {
        return Ok(None);
    };

    let normalized = raw.trim();
    if normalized.is_empty() {
        return Err(format!(
            "{context_label} field '{field_name}' must be non-empty when provided"
        ));
    }

    Ok(Some(normalized.to_string()))
}

/// Returns one required normalized non-empty string field.
fn normalize_required_non_empty_field(
    field_name: &str,
    value: Option<&str>,
    context_label: &str,
) -> Result<String, String> {
    normalize_optional_non_empty_field(field_name, value, context_label)?
        .ok_or_else(|| format!("{context_label} must define required field '{field_name}'"))
}

/// Flattens recursive hierarchy nodes into ordered runtime leaf entries.
#[allow(clippy::too_many_lines)]
fn flatten_hierarchy_nodes_inner(
    nodes: &[HierarchyNode],
    parent_path: &str,
    inherited_media_id: Option<&str>,
    flattened: &mut Vec<FlattenedHierarchyEntry>,
) -> Result<(), String> {
    for (node_index, node) in nodes.iter().enumerate() {
        let context_label = if parent_path.is_empty() {
            format!("hierarchy[{node_index}]")
        } else {
            format!("hierarchy node '{parent_path}' children[{node_index}]")
        };

        let node_local_path = normalize_hierarchy_node_path(node.path.as_str(), &context_label)?;
        let resolved_node_path = join_hierarchy_node_paths(parent_path, node_local_path.as_str());
        let node_path_label =
            if resolved_node_path.is_empty() { "<root>" } else { resolved_node_path.as_str() };

        let hierarchy_id =
            normalize_optional_non_empty_field("id", node.id.as_deref(), node_path_label)?;

        match node.kind {
            HierarchyNodeKind::Folder => {
                if node.variant.is_some() {
                    return Err(format!(
                        "hierarchy node '{node_path_label}' kind 'folder' must not define 'variant'"
                    ));
                }
                if !node.variants.is_empty() {
                    return Err(format!(
                        "hierarchy node '{node_path_label}' kind 'folder' must not define 'variants'"
                    ));
                }
                if !node.rename_files.is_empty() {
                    return Err(format!(
                        "hierarchy node '{node_path_label}' kind 'folder' must not define 'rename_files'"
                    ));
                }
                if !playlist_format_is_default(&node.format) {
                    return Err(format!(
                        "hierarchy node '{node_path_label}' kind 'folder' must not define 'format'"
                    ));
                }
                if !node.ids.is_empty() {
                    return Err(format!(
                        "hierarchy node '{node_path_label}' kind 'folder' must not define 'ids'"
                    ));
                }

                let folder_media_id = normalize_optional_non_empty_field(
                    "media_id",
                    node.media_id.as_deref().or(inherited_media_id),
                    node_path_label,
                )?;

                flatten_hierarchy_nodes_inner(
                    &node.children,
                    resolved_node_path.as_str(),
                    folder_media_id.as_deref(),
                    flattened,
                )?;
            }
            HierarchyNodeKind::Media => {
                if !node.children.is_empty() {
                    return Err(format!(
                        "hierarchy node '{node_path_label}' kind 'media' must not define children"
                    ));
                }
                if !node.variants.is_empty() {
                    return Err(format!(
                        "hierarchy node '{node_path_label}' kind 'media' must not define 'variants'; use singular 'variant'"
                    ));
                }
                if !node.rename_files.is_empty() {
                    return Err(format!(
                        "hierarchy node '{node_path_label}' kind 'media' must not define 'rename_files'"
                    ));
                }
                if !playlist_format_is_default(&node.format) {
                    return Err(format!(
                        "hierarchy node '{node_path_label}' kind 'media' must not define 'format'"
                    ));
                }
                if !node.ids.is_empty() {
                    return Err(format!(
                        "hierarchy node '{node_path_label}' kind 'media' must not define 'ids'"
                    ));
                }

                let media_id = normalize_required_non_empty_field(
                    "media_id",
                    node.media_id.as_deref().or(inherited_media_id),
                    node_path_label,
                )?;
                let hierarchy_id = node
                    .id
                    .as_deref()
                    .map(|value| {
                        normalize_required_non_empty_field("id", Some(value), node_path_label)
                    })
                    .transpose()?;
                let variant = normalize_required_non_empty_field(
                    "variant",
                    node.variant.as_deref(),
                    node_path_label,
                )?;

                if resolved_node_path.is_empty() {
                    return Err(
                        "hierarchy media node must not resolve to an empty path; use path = \"\" only on folder nodes"
                            .to_string(),
                    );
                }

                flattened.push(FlattenedHierarchyEntry {
                    path: resolved_node_path,
                    entry: HierarchyEntry {
                        kind: HierarchyEntryKind::Media,
                        media_id,
                        variants: vec![variant],
                        rename_files: Vec::new(),
                        format: PlaylistFormat::M3u8,
                        ids: Vec::new(),
                    },
                    hierarchy_id,
                });
            }
            HierarchyNodeKind::MediaFolder => {
                if !node.children.is_empty() {
                    return Err(format!(
                        "hierarchy node '{node_path_label}' kind 'media_folder' must not define children"
                    ));
                }
                if node.variant.is_some() {
                    return Err(format!(
                        "hierarchy node '{node_path_label}' kind 'media_folder' must not define singular 'variant'; use 'variants'"
                    ));
                }
                if node.variants.is_empty() {
                    return Err(format!(
                        "hierarchy node '{node_path_label}' kind 'media_folder' must define non-empty 'variants'"
                    ));
                }
                if !playlist_format_is_default(&node.format) {
                    return Err(format!(
                        "hierarchy node '{node_path_label}' kind 'media_folder' must not define 'format'"
                    ));
                }
                if !node.ids.is_empty() {
                    return Err(format!(
                        "hierarchy node '{node_path_label}' kind 'media_folder' must not define 'ids'"
                    ));
                }

                let media_id = normalize_required_non_empty_field(
                    "media_id",
                    node.media_id.as_deref().or(inherited_media_id),
                    node_path_label,
                )?;
                let hierarchy_id = node
                    .id
                    .as_deref()
                    .map(|value| {
                        normalize_required_non_empty_field("id", Some(value), node_path_label)
                    })
                    .transpose()?;

                if resolved_node_path.is_empty() {
                    return Err(
                        "hierarchy media_folder node must not resolve to an empty path; use path = \"\" only on folder nodes"
                            .to_string(),
                    );
                }

                flattened.push(FlattenedHierarchyEntry {
                    path: resolved_node_path,
                    entry: HierarchyEntry {
                        kind: HierarchyEntryKind::MediaFolder,
                        media_id,
                        variants: node.variants.clone(),
                        rename_files: node.rename_files.clone(),
                        format: PlaylistFormat::M3u8,
                        ids: Vec::new(),
                    },
                    hierarchy_id,
                });
            }
            HierarchyNodeKind::Playlist => {
                if !node.children.is_empty() {
                    return Err(format!(
                        "hierarchy node '{node_path_label}' kind 'playlist' must not define children"
                    ));
                }
                if node.variant.is_some() {
                    return Err(format!(
                        "hierarchy node '{node_path_label}' kind 'playlist' must not define 'variant'"
                    ));
                }
                if !node.variants.is_empty() {
                    return Err(format!(
                        "hierarchy node '{node_path_label}' kind 'playlist' must not define 'variants'"
                    ));
                }
                if !node.rename_files.is_empty() {
                    return Err(format!(
                        "hierarchy node '{node_path_label}' kind 'playlist' must not define 'rename_files'"
                    ));
                }
                if node.ids.is_empty() {
                    return Err(format!(
                        "hierarchy node '{node_path_label}' kind 'playlist' must define non-empty 'ids'"
                    ));
                }

                if resolved_node_path.is_empty() {
                    return Err(
                        "hierarchy playlist node must not resolve to an empty path; use path = \"\" only on folder nodes"
                            .to_string(),
                    );
                }

                let playlist_media_id = normalize_optional_non_empty_field(
                    "media_id",
                    node.media_id.as_deref().or(inherited_media_id),
                    node_path_label,
                )?;

                flattened.push(FlattenedHierarchyEntry {
                    path: resolved_node_path,
                    entry: HierarchyEntry {
                        kind: HierarchyEntryKind::Playlist,
                        media_id: playlist_media_id.unwrap_or_default(),
                        variants: Vec::new(),
                        rename_files: Vec::new(),
                        format: node.format,
                        ids: node.ids.clone(),
                    },
                    hierarchy_id,
                });
            }
        }
    }

    Ok(())
}

/// Flattens ordered recursive hierarchy nodes into runtime leaf entries.
///
/// # Errors
///
/// Returns [`MediaPmError`] when node semantics are invalid (for example
/// unsupported field combinations, missing required fields, or duplicate
/// flattened paths).
pub(crate) fn flatten_hierarchy_nodes_for_runtime(
    hierarchy: &[HierarchyNode],
) -> Result<Vec<FlattenedHierarchyEntry>, MediaPmError> {
    let mut flattened = Vec::new();
    flatten_hierarchy_nodes_inner(hierarchy, "", None, &mut flattened)
        .map_err(MediaPmError::Workflow)?;

    let mut seen_paths = BTreeMap::<String, usize>::new();
    let mut seen_hierarchy_ids = BTreeMap::<String, String>::new();
    for (index, entry) in flattened.iter().enumerate() {
        if let Some(previous_index) = seen_paths.insert(entry.path.clone(), index) {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy flattening produced duplicate path '{}' (entries #{previous_index} and #{index})",
                entry.path
            )));
        }

        if let Some(hierarchy_id) = entry.hierarchy_id.as_deref()
            && let Some(previous_path) =
                seen_hierarchy_ids.insert(hierarchy_id.to_string(), entry.path.clone())
        {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy id '{hierarchy_id}' is duplicated by paths '{previous_path}' and '{}'",
                entry.path
            )));
        }
    }

    Ok(flattened)
}

/// Converts flat runtime hierarchy entries into node-list schema form.
///
/// This helper exists for Rust-authored callsites/tests that still construct
/// hierarchy payloads as explicit path maps while internal schema migration is
/// in progress.
///
/// # Errors
///
/// Returns [`MediaPmError`] when one flat entry cannot be represented as one
/// node declaration.
#[cfg(test)]
pub(crate) fn hierarchy_nodes_from_flat_entries(
    hierarchy: &BTreeMap<String, HierarchyEntry>,
) -> Result<Vec<HierarchyNode>, MediaPmError> {
    let mut media_id_counts = BTreeMap::<String, usize>::new();
    for entry in hierarchy.values() {
        if matches!(entry.kind, HierarchyEntryKind::Media | HierarchyEntryKind::MediaFolder) {
            *media_id_counts.entry(entry.media_id.clone()).or_insert(0) += 1;
        }
    }

    let derive_hierarchy_id = |path: &str, media_id: &str| {
        let count = media_id_counts.get(media_id).copied().unwrap_or(0);
        if count <= 1 { media_id.to_string() } else { format!("{media_id}:{path}") }
    };

    let mut nodes = Vec::with_capacity(hierarchy.len());

    for (path, entry) in hierarchy {
        match entry.kind {
            HierarchyEntryKind::Media => {
                let variant = entry.variants.first().cloned().ok_or_else(|| {
                    MediaPmError::Workflow(format!(
                        "legacy flat hierarchy path '{path}' kind 'media' requires at least one variant"
                    ))
                })?;

                if entry.variants.len() != 1 {
                    return Err(MediaPmError::Workflow(format!(
                        "legacy flat hierarchy path '{path}' kind 'media' file target must define exactly one variant"
                    )));
                }

                let hierarchy_id = derive_hierarchy_id(path, &entry.media_id);

                nodes.push(HierarchyNode {
                    path: path.clone(),
                    kind: HierarchyNodeKind::Media,
                    id: Some(hierarchy_id),
                    media_id: Some(entry.media_id.clone()),
                    variant: Some(variant),
                    variants: Vec::new(),
                    rename_files: Vec::new(),
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    children: Vec::new(),
                });
            }
            HierarchyEntryKind::MediaFolder => {
                let hierarchy_id = derive_hierarchy_id(path, &entry.media_id);
                nodes.push(HierarchyNode {
                    path: path.trim_end_matches(['/', '\\']).to_string(),
                    kind: HierarchyNodeKind::MediaFolder,
                    id: Some(hierarchy_id),
                    media_id: Some(entry.media_id.clone()),
                    variant: None,
                    variants: entry.variants.clone(),
                    rename_files: entry.rename_files.clone(),
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    children: Vec::new(),
                });
            }
            HierarchyEntryKind::Playlist => {
                nodes.push(HierarchyNode {
                    path: path.clone(),
                    kind: HierarchyNodeKind::Playlist,
                    id: None,
                    media_id: None,
                    variant: None,
                    variants: Vec::new(),
                    rename_files: Vec::new(),
                    format: entry.format,
                    ids: entry.ids.clone(),
                    children: Vec::new(),
                });
            }
        }
    }

    Ok(nodes)
}

/// Decodes one hierarchy JSON value into ordered node declarations.
///
/// The new schema is strict: `hierarchy` must be an array of node objects.
/// Legacy flat-map and nested-map forms are intentionally unsupported.
///
/// # Errors
///
/// Returns [`MediaPmError`] when the value shape is invalid or node decoding
/// fails.
pub fn flatten_hierarchy_value(value: Value) -> Result<Vec<HierarchyNode>, MediaPmError> {
    match value {
        Value::Array(_) => serde_json::from_value(value)
            .map_err(|error| MediaPmError::Workflow(format!("hierarchy decode failed: {error}"))),
        _ => Err(MediaPmError::Workflow(
            "hierarchy must decode from an ordered array of nodes".to_string(),
        )),
    }
}

/// Encodes ordered hierarchy nodes into JSON array form.
///
/// # Errors
///
/// Returns [`MediaPmError`] when node serialization fails.
pub fn nest_hierarchy_value(hierarchy: &[HierarchyNode]) -> Result<Value, MediaPmError> {
    serde_json::to_value(hierarchy)
        .map_err(|error| MediaPmError::Workflow(format!("hierarchy encode failed: {error}")))
}

/// Deserializes hierarchy field values using array-of-nodes semantics.
fn deserialize_hierarchy_node_list<'de, D>(deserializer: D) -> Result<Vec<HierarchyNode>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    flatten_hierarchy_value(value).map_err(serde::de::Error::custom)
}

/// Serializes hierarchy field values into array-of-nodes representation.
fn serialize_hierarchy_node_list<S>(
    hierarchy: &[HierarchyNode],
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let encoded = nest_hierarchy_value(hierarchy).map_err(serde::ser::Error::custom)?;
    encoded.serialize(serializer)
}

/// Wire representation for one variant selector entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
enum VariantSelectorSerde {
    /// Exact variant-name selector.
    Literal(String),
    /// Regex selector object syntax.
    Regex {
        /// Regex expression matched against available variant names.
        regex: String,
    },
}

/// Owned serializer helper for one variant selector entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
enum VariantSelectorOwned {
    /// Exact variant-name selector.
    Literal(String),
    /// Regex selector object syntax.
    Regex {
        /// Regex expression matched against available variant names.
        regex: String,
    },
}

/// Encodes one regex selector as internal tagged string form.
#[must_use]
fn encode_regex_variant_selector(pattern: &str) -> String {
    format!("{REGEX_VARIANT_SELECTOR_PREFIX}{pattern}")
}

/// Returns regex pattern when one selector uses internal regex-tag form.
#[must_use]
pub(crate) fn decode_regex_variant_selector_pattern(selector: &str) -> Option<&str> {
    selector.strip_prefix(REGEX_VARIANT_SELECTOR_PREFIX)
}

/// Public helper for constructing regex selector values in Rust-authored docs.
///
/// Serialized `mediapm.ncl` output uses object syntax (`{ regex = "..." }`).
#[must_use]
pub fn regex_variant_selector(pattern: &str) -> String {
    encode_regex_variant_selector(pattern)
}

/// Deserializes selector arrays that accept literal strings or regex objects.
fn deserialize_variant_selector_list<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let selectors = Vec::<VariantSelectorSerde>::deserialize(deserializer)?;
    let mut decoded = Vec::with_capacity(selectors.len());

    for selector in selectors {
        match selector {
            VariantSelectorSerde::Literal(value) => {
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    return Err(serde::de::Error::custom(
                        "variant selector strings must be non-empty",
                    ));
                }

                decoded.push(trimmed.to_string());
            }
            VariantSelectorSerde::Regex { regex } => {
                let pattern = regex.trim();
                if pattern.is_empty() {
                    return Err(serde::de::Error::custom(
                        "variant regex selectors must define non-empty 'regex'",
                    ));
                }

                Regex::new(pattern).map_err(|error| {
                    serde::de::Error::custom(format!(
                        "variant regex selector '{pattern}' is invalid: {error}"
                    ))
                })?;

                decoded.push(encode_regex_variant_selector(pattern));
            }
        }
    }

    Ok(decoded)
}

/// Serializes selector arrays back to string-or-object wire representation.
fn serialize_variant_selector_list<S>(
    selectors: &[String],
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let encoded = selectors
        .iter()
        .map(|selector| {
            if let Some(pattern) = decode_regex_variant_selector_pattern(selector) {
                VariantSelectorOwned::Regex { regex: pattern.to_string() }
            } else {
                VariantSelectorOwned::Literal(selector.clone())
            }
        })
        .collect::<Vec<_>>();

    encoded.serialize(serializer)
}

/// Resolves selector entries against available variant names.
///
/// Resolution policy:
/// - literal selectors match exact variant names;
/// - regex selectors (`{ regex = "..." }`) match any variant names whose
///   full text matches the regex;
/// - when a selector resolves nothing and a `default` variant exists, it falls
///   back to `default`.
///
/// Returned variants are de-duplicated while preserving first-seen order.
pub(crate) fn expand_variant_selectors(
    selectors: &[String],
    available_variants: &std::collections::BTreeSet<String>,
) -> Result<Vec<String>, String> {
    let mut resolved = Vec::new();
    let mut seen = std::collections::BTreeSet::new();

    for selector in selectors {
        let trimmed = selector.trim();
        if trimmed.is_empty() {
            return Err("contains an empty variant selector".to_string());
        }

        if let Some(pattern) = decode_regex_variant_selector_pattern(trimmed) {
            let regex = Regex::new(pattern).map_err(|error| {
                format!("regex variant selector '{pattern}' is invalid: {error}")
            })?;

            let mut matched = false;
            for candidate in available_variants {
                if regex.is_match(candidate) {
                    matched = true;
                    if seen.insert(candidate.clone()) {
                        resolved.push(candidate.clone());
                    }
                }
            }

            if !matched {
                if available_variants.contains("default") {
                    if seen.insert("default".to_string()) {
                        resolved.push("default".to_string());
                    }
                } else {
                    return Err(format!(
                        "regex variant selector '{{ regex = \"{pattern}\" }}' did not match any available variants"
                    ));
                }
            }

            continue;
        }

        let resolved_name = if available_variants.contains(trimmed) {
            trimmed.to_string()
        } else if available_variants.contains("default") {
            "default".to_string()
        } else {
            return Err(format!("references unknown variant selector '{trimmed}'"));
        };

        if seen.insert(resolved_name.clone()) {
            resolved.push(resolved_name);
        }
    }

    Ok(resolved)
}

/// Top-level Phase 3 Nickel document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaPmDocument {
    /// Explicit schema marker for migration safety.
    pub version: u32,
    /// Optional runtime-path overrides for Phase 3 local state.
    #[serde(default)]
    pub runtime: MediaRuntimeStorage,
    /// Declarative desired tool requirements keyed by logical tool name.
    #[serde(default)]
    pub tools: BTreeMap<String, ToolRequirement>,
    /// Media source registry keyed by stable media id.
    #[serde(default)]
    pub media: BTreeMap<String, MediaSourceSpec>,
    /// Ordered hierarchy node declarations.
    ///
    /// Persisted schema uses an explicit node list (with recursive `children`)
    /// instead of map-based path keys so author order stays stable.
    #[serde(
        default,
        deserialize_with = "deserialize_hierarchy_node_list",
        serialize_with = "serialize_hierarchy_node_list"
    )]
    pub hierarchy: Vec<HierarchyNode>,
    /// Machine-managed realized state loaded from `state.ncl`.
    ///
    /// Config loads may omit this field; runtime merges the state document
    /// after resolving runtime-storage paths.
    #[serde(default, skip_serializing_if = "mediapm_state_is_empty")]
    pub state: MediaPmState,
}

impl Default for MediaPmDocument {
    fn default() -> Self {
        Self {
            version: MEDIAPM_DOCUMENT_VERSION,
            runtime: MediaRuntimeStorage::default(),
            tools: BTreeMap::new(),
            media: BTreeMap::new(),
            hierarchy: Vec::new(),
            state: MediaPmState::default(),
        }
    }
}

/// Machine-managed realized state persisted by `mediapm`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct MediaPmState {
    /// Materialized path registry keyed by relative target path.
    #[serde(default)]
    pub managed_files: BTreeMap<String, ManagedFileRecord>,
    /// Tool registry mirror keyed by immutable tool id.
    #[serde(default)]
    pub tool_registry: BTreeMap<String, ToolRegistryRecord>,
    /// Active tool id per logical tool name.
    #[serde(default)]
    pub active_tools: BTreeMap<String, String>,
    /// Managed workflow step refresh state grouped by media id and step key.
    ///
    /// `mediapm` uses this machine-only state to decide whether a given media
    /// step should refresh its impure execution identity or keep previously
    /// materialized outputs.
    ///
    /// Refresh policy is intentionally strict:
    /// - refresh when the explicit user-facing step config changed, or
    /// - refresh when the mediapm-managed impure timestamp is missing.
    ///
    /// Implicit managed defaults are not persisted in `explicit_config` and
    /// therefore do not trigger refreshes by themselves.
    #[serde(default)]
    pub workflow_step_state: BTreeMap<String, BTreeMap<String, ManagedWorkflowStepState>>,
}

/// Timezone-independent mediapm step-refresh timestamp.
///
/// This wire shape is mediapm-local state (separate from conductor runtime
/// instance timestamps) and accepts integral float values exported by Nickel
/// during decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct MediaPmImpureTimestamp {
    /// Whole seconds since Unix epoch (UTC).
    #[serde(deserialize_with = "deserialize_u64_from_number")]
    pub epoch_seconds: u64,
    /// Nanoseconds within `epoch_seconds`, in range `0..=999_999_999`.
    #[serde(deserialize_with = "deserialize_u32_from_number")]
    pub subsec_nanos: u32,
}

/// Machine-managed refresh state for one workflow step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedWorkflowStepState {
    /// Explicit user-authored step config snapshot.
    ///
    /// This value is serialized from `media.<id>.steps[<index>]` exactly as
    /// authored (with serde default elision semantics), so implicit managed
    /// defaults do not appear in the snapshot and therefore do not count as
    /// user-facing config changes.
    pub explicit_config: Value,
    /// Last mediapm-managed impure timestamp used for this step.
    ///
    /// `None` means this step must refresh on the next reconciliation pass.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub impure_timestamp: Option<MediaPmImpureTimestamp>,
}

/// Returns whether one [`MediaPmState`] value keeps default-empty behavior.
fn mediapm_state_is_empty(value: &MediaPmState) -> bool {
    value.managed_files.is_empty()
        && value.tool_registry.is_empty()
        && value.active_tools.is_empty()
        && value.workflow_step_state.is_empty()
}

/// Materialized file ledger entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedFileRecord {
    /// Media id that produced this path.
    pub media_id: String,
    /// Variant id selected for this materialized output.
    pub variant: String,
    /// Canonical CAS hash string for this exact materialized file payload.
    ///
    /// This identity is used by machine-config external-data reconciliation so
    /// all managed file bytes remain rooted in conductor persistence metadata.
    pub hash: String,
    /// Last successful sync timestamp in Unix epoch milliseconds.
    ///
    /// `mediapm` uses explicit unit-suffixed epoch fields to match CAS-style
    /// timestamp conventions.
    #[serde(deserialize_with = "deserialize_u64_from_number")]
    pub last_synced_unix_millis: u64,
}

/// Safety-pinned external-data entry.
/// Tool lifecycle status tracked by `mediapm` state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRegistryStatus {
    /// Tool binary/config is present and expected to be runnable.
    Active,
    /// Tool binary was intentionally pruned while metadata remains.
    Pruned,
}

/// Materialization method used when writing managed hierarchy files.
///
/// Runtime attempts methods in configured order and stops on the first
/// successful realization for each file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MaterializationMethod {
    /// Realize output as a hard link to CAS object bytes.
    Hardlink,
    /// Realize output as a filesystem symlink to CAS object bytes.
    Symlink,
    /// Realize output via copy-on-write reflink/clone semantics.
    Reflink,
    /// Realize output by copying bytes from CAS object payload.
    Copy,
}

impl MaterializationMethod {
    /// Returns stable config/diagnostic label for this method.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Hardlink => "hardlink",
            Self::Symlink => "symlink",
            Self::Reflink => "reflink",
            Self::Copy => "copy",
        }
    }
}

impl fmt::Display for MaterializationMethod {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_label())
    }
}

/// Deserializes one non-negative integral number into `u64`.
fn deserialize_u64_from_number<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;

    if let Some(raw) = value.as_u64() {
        return Ok(raw);
    }
    if let Some(raw) = value.as_f64()
        && let Some(normalized) = parse_non_negative_integral_u64(raw)
    {
        return Ok(normalized);
    }

    Err(serde::de::Error::custom("expected one non-negative integral number representable as u64"))
}

/// Deserializes one non-negative integral number into `u32`.
fn deserialize_u32_from_number<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;

    if let Some(raw) = value.as_u64() {
        return u32::try_from(raw).map_err(|_| {
            serde::de::Error::custom(
                "expected one non-negative integral number representable as u32",
            )
        });
    }
    if let Some(raw) = value.as_f64()
        && let Some(normalized) = parse_non_negative_integral_u32(raw)
    {
        return Ok(normalized);
    }

    Err(serde::de::Error::custom("expected one non-negative integral number representable as u32"))
}

/// Tool registry metadata persisted in `mediapm` state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRegistryRecord {
    /// Logical tool name without version suffix.
    pub name: String,
    /// Catalog release track recorded at activation time.
    pub version: String,
    /// Catalog source label used for this registration.
    pub source: String,
    /// Content-derived multihash fingerprint used for validation bookkeeping.
    pub registry_multihash: String,
    /// Last status transition timestamp in Unix seconds.
    #[serde(deserialize_with = "deserialize_u64_from_number")]
    pub last_transition_unix_seconds: u64,
    /// Current lifecycle state.
    pub status: ToolRegistryStatus,
}

/// Runtime path overrides for Phase 3 local state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct MediaRuntimeStorage {
    /// Optional override for `.mediapm/` runtime root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mediapm_dir: Option<String>,
    /// Optional override for materialized hierarchy root directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hierarchy_root_dir: Option<String>,
    /// Optional override for mediapm staging tmp directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mediapm_tmp_dir: Option<String>,
    /// Optional ordered policy for hierarchy file materialization.
    ///
    /// When omitted, runtime defaults to:
    /// `hardlink -> symlink -> reflink -> copy`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub materialization_preference_order: Option<Vec<MaterializationMethod>>,
    /// Optional override for `mediapm`-managed conductor user config path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conductor_config: Option<String>,
    /// Optional override for `mediapm`-managed conductor machine config path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conductor_machine_config: Option<String>,
    /// Optional override for `mediapm`-managed conductor runtime state path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conductor_state_config: Option<String>,
    /// Optional override for conductor execution sandbox tmp directory.
    ///
    /// Defaults to `runtime.mediapm_tmp_dir`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conductor_tmp_dir: Option<String>,
    /// Optional override for conductor schema export directory.
    ///
    /// Defaults to `<runtime.mediapm_dir>/config/conductor`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conductor_schema_dir: Option<String>,
    /// Optional additional inherited host environment-variable names for
    /// conductor executable process environments, keyed by platform.
    ///
    /// Runtime always keeps the host-default baseline and merges only the
    /// active host platform entry (`windows`, `linux`, `macos`, etc.) on top
    /// with case-insensitive de-duplication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited_env_vars: Option<PlatformInheritedEnvVars>,
    /// Optional override for machine-managed `mediapm` state path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_state_config: Option<String>,
    /// Optional override for runtime dotenv file used for credential loading.
    ///
    /// When omitted, the effective default path is `<runtime.mediapm_dir>/.env`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env_file: Option<String>,
    /// Optional schema export directory policy for embedded `mediapm.ncl`
    /// Nickel contracts.
    ///
    /// Tri-state semantics:
    /// - omitted (`None`): export schemas to default `<runtime.mediapm_dir>/config/mediapm`,
    /// - explicit `null` (`Some(None)`): disable schema export,
    /// - explicit string (`Some(Some(path))`): export to that path.
    #[serde(default, skip_serializing_if = "runtime_mediapm_schema_export_is_omitted")]
    pub mediapm_schema_dir: Option<Option<String>>,
    /// Optional toggle for shared global user-level managed-tool cache.
    ///
    /// When omitted, the cache is enabled by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub use_user_tool_cache: Option<bool>,
}

/// Returns whether runtime schema-export policy was omitted from config.
#[expect(
    clippy::option_option,
    reason = "tri-state schema export policy intentionally distinguishes omitted/null/path"
)]
#[expect(
    clippy::ref_option,
    reason = "serde skip_serializing_if requires borrowing the full field type"
)]
fn runtime_mediapm_schema_export_is_omitted(value: &Option<Option<String>>) -> bool {
    value.is_none()
}

fn append_unique_env_var_names(target: &mut Vec<String>, source: &[String]) {
    for raw_name in source {
        let trimmed = raw_name.trim();
        if trimmed.is_empty() {
            continue;
        }

        if target.iter().any(|existing| existing.eq_ignore_ascii_case(trimmed)) {
            continue;
        }

        target.push(trimmed.to_string());
    }
}

/// Normalizes one platform key used by `runtime.inherited_env_vars`.
#[must_use]
fn normalize_runtime_platform_key(raw_platform: &str) -> Option<String> {
    let trimmed = raw_platform.trim();
    if trimmed.is_empty() { None } else { Some(trimmed.to_ascii_lowercase()) }
}

/// Appends one platform-scoped inherited env-var list for the active host.
fn append_platform_inherited_env_var_names_for_host(
    target: &mut Vec<String>,
    source: &PlatformInheritedEnvVars,
    host_platform: &str,
) {
    for (platform_key, names) in source {
        if normalize_runtime_platform_key(platform_key).as_deref() == Some(host_platform) {
            append_unique_env_var_names(target, names);
        }
    }
}

impl MediaRuntimeStorage {
    /// Returns whether shared global user-level managed-tool cache should be used.
    ///
    /// Absent configuration defaults to `true` so repeated tool downloads can
    /// reuse payload bytes across all local `mediapm` workspaces for this user.
    #[must_use]
    pub const fn use_user_tool_cache_enabled(&self) -> bool {
        use_user_download_cache_enabled(self.use_user_tool_cache)
    }

    /// Returns ordered materialization policy with runtime defaults applied.
    #[must_use]
    pub fn materialization_preference_order_with_defaults(&self) -> Vec<MaterializationMethod> {
        self.materialization_preference_order
            .clone()
            .unwrap_or_else(|| DEFAULT_MATERIALIZATION_PREFERENCE_ORDER.to_vec())
    }

    /// Returns inherited env-var names merged with host defaults.
    ///
    /// This reads only the active host-platform entry from
    /// `runtime.inherited_env_vars` and ignores lists for other platforms.
    #[must_use]
    pub fn inherited_env_vars_with_defaults(&self) -> Vec<String> {
        let host_platform = std::env::consts::OS.to_ascii_lowercase();
        let mut merged = Vec::new();

        append_platform_inherited_env_var_names_for_host(
            &mut merged,
            &default_runtime_inherited_env_vars_for_host(),
            &host_platform,
        );

        if let Some(configured) = &self.inherited_env_vars {
            append_platform_inherited_env_var_names_for_host(
                &mut merged,
                configured,
                &host_platform,
            );
        }
        merged
    }
}

/// Declarative tool requirement for one logical media tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRequirement {
    /// Optional version selector for this logical tool.
    ///
    /// At least one of `version` or `tag` must be provided by
    /// `validate_tool_requirements` during document load.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Optional tag selector for this logical tool.
    ///
    /// When both `version` and `tag` are provided, they must refer to the
    /// same normalized release selector.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    /// Optional grouped dependency selector overrides for this logical tool.
    ///
    /// These selectors pin companion logical tools for workflows that depend
    /// on additional executables (for example `ffmpeg` and `sd`).
    #[serde(default, skip_serializing_if = "tool_requirement_dependencies_is_empty")]
    pub dependencies: ToolRequirementDependencies,
    /// Optional release-metadata recheck interval in seconds.
    ///
    /// When present, `mediapm` reuses cached release metadata until the
    /// interval elapses, then refreshes from upstream release APIs.
    /// When omitted, `mediapm` reuses cached release metadata for one day
    /// before refreshing from upstream release APIs (while still allowing
    /// cache fallback on refresh errors).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_u64_from_number"
    )]
    pub recheck_seconds: Option<u64>,
    /// Optional max number of indexed ffmpeg input slots exposed by generated
    /// managed tool contracts and workflow synthesis.
    ///
    /// This setting is valid only on logical tool `ffmpeg`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_runtime_slot_count"
    )]
    pub max_input_slots: Option<u32>,
    /// Optional max number of indexed ffmpeg output slots exposed by generated
    /// managed tool contracts and workflow synthesis.
    ///
    /// This setting is valid only on logical tool `ffmpeg`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_runtime_slot_count"
    )]
    pub max_output_slots: Option<u32>,
}

/// Grouped dependency selector overrides for one logical tool requirement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ToolRequirementDependencies {
    /// Optional ffmpeg selector used by tools with explicit ffmpeg companion
    /// selection support.
    ///
    /// Selection semantics:
    /// - omitted / `global` / `inherit`: use active logical `ffmpeg` tool,
    /// - explicit selector text: match immutable ffmpeg identity by
    ///   hash/version/tag (normalized compare).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ffmpeg_version: Option<String>,
    /// Optional `sd` selector used by tools that require `sd` companion
    /// transforms (for example `ReplayGain` metadata rewrites).
    ///
    /// Selection semantics mirror `ffmpeg_version`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sd_version: Option<String>,
}

impl ToolRequirementDependencies {
    /// Returns true when no dependency selector overrides are configured.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.ffmpeg_version.is_none() && self.sd_version.is_none()
    }
}

/// Returns whether grouped dependency selector config can be omitted from
/// serialized `mediapm.ncl` output.
fn tool_requirement_dependencies_is_empty(value: &ToolRequirementDependencies) -> bool {
    value.is_empty()
}

impl ToolRequirement {
    /// Returns normalized non-empty version selector text.
    #[must_use]
    pub fn normalized_version(&self) -> Option<String> {
        normalize_selector_value(self.version.as_deref())
    }

    /// Returns normalized non-empty tag selector text.
    #[must_use]
    pub fn normalized_tag(&self) -> Option<String> {
        normalize_selector_value(self.tag.as_deref())
    }

    /// Returns normalized ffmpeg selector text.
    #[must_use]
    pub fn normalized_ffmpeg_selector(&self) -> Option<String> {
        normalize_selector_value(self.dependencies.ffmpeg_version.as_deref())
    }

    /// Returns normalized `sd` selector text.
    #[must_use]
    pub fn normalized_sd_selector(&self) -> Option<String> {
        normalize_selector_value(self.dependencies.sd_version.as_deref())
    }

    /// Returns optional release-metadata recheck interval in seconds.
    #[must_use]
    pub const fn metadata_recheck_seconds(&self) -> Option<u64> {
        self.recheck_seconds
    }

    /// Returns effective max input slot count for this tool row.
    #[must_use]
    pub const fn max_input_slots_or_default(&self) -> u32 {
        match self.max_input_slots {
            Some(value) => value,
            None => DEFAULT_FFMPEG_MAX_INPUT_SLOTS,
        }
    }

    /// Returns effective max output slot count for this tool row.
    #[must_use]
    pub const fn max_output_slots_or_default(&self) -> u32 {
        match self.max_output_slots {
            Some(value) => value,
            None => DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS,
        }
    }
}

/// Normalizes optional selector text by trimming whitespace and leading `@`.
#[must_use]
pub(crate) fn normalize_selector_value(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim)
        .map(|value| value.trim_start_matches('@'))
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

/// Normalizes version/tag text for equality comparison.
#[must_use]
pub(crate) fn normalize_selector_compare_value(value: &str) -> String {
    value.trim().trim_start_matches('@').trim_start_matches(['v', 'V']).to_string()
}

/// Deserializes optional `u64` values while accepting integral floating-point
/// numbers exported by Nickel (for example `3600.0`).
fn deserialize_optional_u64_from_number<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Option::<Value>::deserialize(deserializer)?;
    let Some(raw) = raw else {
        return Ok(None);
    };

    if let Some(value) = raw.as_u64() {
        return Ok(Some(value));
    }

    if let Some(value) = raw.as_f64()
        && let Some(normalized) = parse_non_negative_integral_u64(value)
    {
        return Ok(Some(normalized));
    }

    Err(serde::de::Error::custom("recheck_seconds must be a non-negative integer"))
}

/// Deserializes optional runtime slot-count `u32` values while accepting
/// integral floating-point numbers exported by Nickel (for example `96.0`).
fn deserialize_optional_runtime_slot_count<'de, D>(deserializer: D) -> Result<Option<u32>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Option::<Value>::deserialize(deserializer)?;
    let Some(raw) = raw else {
        return Ok(None);
    };

    if let Some(value) = raw.as_u64() {
        return u32::try_from(value)
            .map(Some)
            .map_err(|_| serde::de::Error::custom("ffmpeg slot limit must be within u32 range"));
    }

    if let Some(value) = raw.as_f64()
        && let Some(normalized) = parse_non_negative_integral_u32(value)
    {
        return Ok(Some(normalized));
    }

    Err(serde::de::Error::custom("ffmpeg slot limit must be a non-negative integer"))
}

/// One media metadata value source declared under `media.<id>.metadata`.
///
/// Metadata values are intentionally strict and support exactly two forms:
/// - `"text"` literal values,
/// - object bindings that extract one key from one produced file variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MediaMetadataValue {
    /// Literal metadata text value.
    Literal(String),
    /// Variant-file metadata lookup binding.
    Variant(MediaMetadataVariantBinding),
}

/// Variant-file metadata lookup binding for `media.<id>.metadata` values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaMetadataVariantBinding {
    /// Variant key whose produced file bytes should be inspected.
    pub variant: String,
    /// Metadata key to extract from that variant file.
    pub metadata_key: String,
    /// Optional regex transform applied to the extracted metadata string.
    ///
    /// Transform semantics are full-match only: the `pattern` must match the
    /// entire extracted value. When it matches, `replacement` is rendered using
    /// regular regex capture-group substitution (`$0` = entire match;
    /// `$1..$N` = explicit capture groups).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transform: Option<MediaMetadataRegexTransform>,
}

/// Regex-based metadata string transform for variant metadata bindings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaMetadataRegexTransform {
    /// Regex pattern evaluated with full-match semantics against extracted
    /// metadata text.
    pub pattern: String,
    /// Replacement template applied when `pattern` matches.
    pub replacement: String,
}

/// Source registry entry for one media item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaSourceSpec {
    /// Legacy media id override field.
    ///
    /// This field is intentionally rejected by runtime validation. Playlist
    /// references must target explicit hierarchy node `id` values instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Optional human-readable description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Optional human-readable title used for readability and path templates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Optional explicit conductor workflow id override.
    ///
    /// When omitted, `mediapm` maps each media id to exactly one managed
    /// workflow id using the default prefix policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    /// Optional strict metadata object for media-specific path interpolation.
    ///
    /// Each key maps to either:
    /// - one literal string value, or
    /// - one `{ variant, metadata_key, transform? }` object that
    ///   resolves metadata from a
    ///   file variant produced by this media source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<BTreeMap<String, MediaMetadataValue>>,
    /// Optional pre-seeded CAS hash pointers keyed by variant name.
    ///
    /// These variants seed step input bindings before the ordered step graph
    /// executes.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub variant_hashes: BTreeMap<String, String>,
    /// Ordered media-processing steps.
    ///
    /// Every step declares tool-specific `options`, `input_variants` for
    /// non-source-ingest transforms, and `output_variants` keyed by output
    /// variant name.
    /// Source-ingest tools (`yt-dlp`, `import`) must keep
    /// `input_variants` empty.
    /// Variant outputs flow top-to-bottom across this ordered list.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub steps: Vec<MediaStep>,
}

/// One ordered media-processing step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaStep {
    /// Tool kind used for this step.
    pub tool: MediaStepTool,
    /// Input variants consumed by this step.
    ///
    /// Source-ingest tools must keep this list empty because they originate
    /// content directly from their own options (for example `options.uri` or
    /// `options.hash`) rather than from prior step outputs.
    ///
    /// Selector entries support both exact-string and regex-object forms:
    /// - `"variant_name"`
    /// - `{ regex = "^subtitles/.+$" }`
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_variant_selector_list",
        serialize_with = "serialize_variant_selector_list"
    )]
    pub input_variants: Vec<String>,
    /// Output variants produced by this step.
    ///
    /// Each key is one produced output variant name and each value is one
    /// tool-specific output config value.
    ///
    /// Key names are identity-only and have no built-in tool semantics.
    /// Tool behavior is decoded entirely from each value.
    ///
    /// Value-shape policy:
    /// - values must always be objects,
    /// - all values must define `kind`,
    /// - `save` defaults to `true` when omitted,
    /// - ffmpeg values must also define numeric `idx`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub output_variants: BTreeMap<String, Value>,
    /// Operation-specific option map.
    ///
    /// Unknown option keys are rejected at document-load validation time.
    /// For online downloaders, the source URL is declared in this map as
    /// `options.uri`.
    ///
    /// For generated boolean-style option inputs, runtime command templates
    /// only enable boolean toggles when the value is exactly `"true"`.
    /// Any other value is treated as disabled.
    ///
    /// Values are scalar strings by default. Ordered string lists are only
    /// valid for low-level list-style input bindings (`option_args`,
    /// `leading_args`, and `trailing_args`).
    ///
    /// Low-level input bindings are declared here (instead of a separate
    /// `input_options` map).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub options: BTreeMap<String, TransformInputValue>,
}

/// Supported media-step tool kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MediaStepTool {
    /// `yt-dlp` online-media downloader.
    YtDlp,
    /// `import` builtin source ingestion from existing CAS payload hash.
    Import,
    /// `ffmpeg` media transform.
    Ffmpeg,
    /// `rsgain` loudness transform.
    Rsgain,
    /// `media-tagger` native metadata tagging transform.
    MediaTagger,
}

impl MediaStepTool {
    /// Returns canonical persisted tool label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::YtDlp => "yt-dlp",
            Self::Import => "import",
            Self::Ffmpeg => "ffmpeg",
            Self::Rsgain => "rsgain",
            Self::MediaTagger => "media-tagger",
        }
    }

    /// Returns true when this tool is an online-media downloader.
    #[must_use]
    pub const fn is_online_media_downloader(self) -> bool {
        matches!(self, Self::YtDlp)
    }

    /// Returns true when this step acts as source-ingest entrypoint.
    #[must_use]
    pub const fn is_source_ingest_tool(self) -> bool {
        matches!(self, Self::YtDlp | Self::Import)
    }

    /// Returns true when the given tool name identifies a builtin
    /// source-ingest step that is never downloader-provisioned and therefore
    /// does not require a release selector (version or tag).
    #[must_use]
    pub fn is_builtin_source_ingest_name(tool_name: &str) -> bool {
        tool_name.eq_ignore_ascii_case("import")
    }
}

/// One transform input-option binding value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TransformInputValue {
    /// Scalar string input value.
    String(String),
    /// Ordered list-of-strings input value.
    StringList(Vec<String>),
}

/// Shared optional per-variant persistence-policy settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OutputVariantPolicyConfig {
    /// Optional tri-state save-policy override (defaults to `true`).
    #[serde(default = "default_output_variant_save")]
    pub(crate) save: OutputSaveConfig,
}

/// Tri-state output-persistence policy for one output variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputSaveConfig {
    /// Boolean save policy (`false` or `true`).
    Bool(bool),
    /// Full-save policy keyword.
    Full,
}

impl Serialize for OutputSaveConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Bool(value) => serializer.serialize_bool(*value),
            Self::Full => serializer.serialize_str("full"),
        }
    }
}

impl<'de> Deserialize<'de> for OutputSaveConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct OutputSaveConfigVisitor;

        impl Visitor<'_> for OutputSaveConfigVisitor {
            type Value = OutputSaveConfig;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a boolean save policy or the string \"full\"")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(OutputSaveConfig::Bool(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value == "full" {
                    Ok(OutputSaveConfig::Full)
                } else {
                    Err(E::invalid_value(de::Unexpected::Str(value), &self))
                }
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_str(&value)
            }
        }

        deserializer.deserialize_any(OutputSaveConfigVisitor)
    }
}

impl OutputSaveConfig {
    /// Returns whether this policy keeps output bytes persisted.
    #[must_use]
    pub const fn should_persist(self) -> bool {
        !matches!(self, Self::Bool(false))
    }
}

impl Default for OutputSaveConfig {
    fn default() -> Self {
        Self::Bool(true)
    }
}

/// Returns the default output-persistence save policy for one variant.
#[must_use]
fn default_output_variant_save() -> OutputSaveConfig {
    OutputSaveConfig::default()
}

/// Generic output-variant configuration for non-yt-dlp tools.
///
/// Output-variant values are always explicit objects with:
/// - required `kind` output capture key,
/// - optional tri-state `save` policy (defaults to `true`),
/// - optional `zip_member` selector,
/// - optional `idx` selector for ffmpeg multi-output routing.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GenericOutputVariantConfig {
    /// Explicit generated-tool output kind/capture name bound for this variant.
    pub(crate) kind: String,
    /// Optional tri-state save-policy override (defaults to `true`).
    #[serde(default = "default_output_variant_save")]
    pub(crate) save: OutputSaveConfig,
    /// Optional capture kind override.
    ///
    /// When omitted, mediapm derives file-vs-folder behavior from `kind`
    /// naming conventions (`*_artifacts` remains folder-capture by default).
    #[serde(default)]
    pub(crate) capture_kind: Option<OutputCaptureKind>,
    /// Optional ZIP member selector used when downstream bindings consume this
    /// output variant.
    ///
    /// When provided, runtime resolves `${step_output...}` references through
    /// `:zip(<member>)` against the selected output payload.
    #[serde(default)]
    pub(crate) zip_member: Option<String>,
    /// Optional ffmpeg output index selector.
    ///
    /// For ffmpeg steps, this field is required and selects which generated
    /// ffmpeg output slot this variant should bind.
    #[serde(default, deserialize_with = "deserialize_optional_u32_from_number")]
    pub(crate) idx: Option<u32>,
    /// Optional output filename extension override.
    ///
    /// This field is supported for `ffmpeg`, `rsgain`, and `media-tagger`
    /// output variants and maps to generated `output_path_<idx>` tool inputs.
    /// Values may be specified with or without a leading dot.
    #[serde(default)]
    pub(crate) extension: Option<String>,
}

impl GenericOutputVariantConfig {
    /// Returns effective file-vs-folder capture kind for this variant.
    #[must_use]
    pub(crate) fn effective_capture_kind(&self) -> OutputCaptureKind {
        match self.capture_kind {
            Some(value) => value,
            None => default_generic_capture_kind_for_kind(self.kind.as_str()),
        }
    }
}

impl From<&GenericOutputVariantConfig> for OutputVariantPolicyConfig {
    fn from(value: &GenericOutputVariantConfig) -> Self {
        Self { save: value.save }
    }
}

/// Deserializes optional `u32` values while accepting integral floating-point
/// numbers exported by Nickel (for example `3.0`).
fn deserialize_optional_u32_from_number<'de, D>(deserializer: D) -> Result<Option<u32>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Option::<Value>::deserialize(deserializer)?;
    let Some(raw) = raw else {
        return Ok(None);
    };

    if let Some(value) = raw.as_u64() {
        return u32::try_from(value)
            .map(Some)
            .map_err(|_| serde::de::Error::custom("idx must be within u32 range"));
    }

    if let Some(value) = raw.as_f64()
        && let Some(normalized) = parse_non_negative_integral_u32(value)
    {
        return Ok(Some(normalized));
    }

    Err(serde::de::Error::custom("idx must be a non-negative integer"))
}

/// Value-driven yt-dlp output-variant kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub(crate) enum YtDlpOutputKind {
    /// Primary downloaded media payload.
    #[serde(rename = "primary")]
    Primary,
    /// Full sandbox artifact bundle.
    #[serde(rename = "sandbox")]
    Sandbox,
    /// Subtitle artifact bundle.
    #[serde(rename = "subtitles")]
    Subtitles,
    /// Thumbnail artifact bundle.
    #[serde(rename = "thumbnails")]
    Thumbnails,
    /// Description sidecar file.
    #[serde(rename = "description")]
    Description,
    /// Annotation sidecar file.
    #[serde(rename = "annotation")]
    Annotation,
    /// Info-JSON sidecar file.
    #[serde(rename = "infojson")]
    Infojson,
    /// Comment-in-infojson semantic output.
    ///
    /// yt-dlp stores comments inside info-json payloads; this kind exists to
    /// enforce comment capture toggles without introducing a dedicated
    /// standalone comment sidecar family.
    #[serde(rename = "comment")]
    Comment,
    /// Link/internet-shortcut artifact bundle.
    #[serde(rename = "links")]
    Links,
    /// Split chapter artifact bundle.
    #[serde(rename = "chapters")]
    Chapters,
    /// Download-archive file output.
    #[serde(rename = "archive")]
    Archive,
    /// Playlist-description artifact bundle.
    #[serde(rename = "playlist_description")]
    PlaylistDescription,
    /// Playlist-infojson artifact bundle.
    #[serde(rename = "playlist_infojson")]
    PlaylistInfojson,
}

/// Per-variant output capture kind.
///
/// This setting controls whether one variant should be treated as a file or
/// folder output by mediapm-side validation/materialization policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub(crate) enum OutputCaptureKind {
    /// Prefer file-style capture/materialization semantics.
    #[serde(rename = "file")]
    File,
    /// Prefer folder-style capture/materialization semantics.
    #[serde(rename = "folder")]
    Folder,
}

/// Returns default capture kind for one generic output kind.
#[must_use]
pub(crate) fn default_generic_capture_kind_for_kind(kind: &str) -> OutputCaptureKind {
    let normalized = kind.trim();
    if normalized == "sandbox_artifacts" || normalized.ends_with("_artifacts") {
        OutputCaptureKind::Folder
    } else {
        OutputCaptureKind::File
    }
}

/// Returns default capture kind for one yt-dlp output kind.
#[must_use]
pub(crate) const fn default_yt_dlp_capture_kind_for_kind(
    kind: YtDlpOutputKind,
) -> OutputCaptureKind {
    match kind {
        YtDlpOutputKind::Primary
        | YtDlpOutputKind::Description
        | YtDlpOutputKind::Annotation
        | YtDlpOutputKind::Infojson
        | YtDlpOutputKind::Comment
        | YtDlpOutputKind::Archive
        | YtDlpOutputKind::PlaylistDescription
        | YtDlpOutputKind::PlaylistInfojson => OutputCaptureKind::File,
        YtDlpOutputKind::Sandbox
        | YtDlpOutputKind::Subtitles
        | YtDlpOutputKind::Thumbnails
        | YtDlpOutputKind::Links
        | YtDlpOutputKind::Chapters => OutputCaptureKind::Folder,
    }
}

/// Selector cardinality for comma-separated capture hints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SelectorCardinality {
    /// No selector values were configured.
    None,
    /// Exactly one selector value was configured.
    One,
    /// Two or more selector values were configured.
    Many,
}

/// Returns selector cardinality for one optional comma-separated string.
#[must_use]
fn selector_cardinality(value: Option<&str>) -> SelectorCardinality {
    let Some(value) = value else {
        return SelectorCardinality::None;
    };

    let count = value.split(',').filter(|candidate| !candidate.trim().is_empty()).count();
    match count {
        0 => SelectorCardinality::None,
        1 => SelectorCardinality::One,
        _ => SelectorCardinality::Many,
    }
}

/// yt-dlp per-variant config.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct YtDlpOutputVariantConfig {
    /// Value-driven output semantic kind.
    pub(crate) kind: YtDlpOutputKind,
    /// Optional tri-state save-policy override (defaults to `true`).
    #[serde(default = "default_output_variant_save")]
    pub(crate) save: OutputSaveConfig,
    /// Optional capture kind override.
    ///
    /// When omitted, mediapm applies kind-based defaults so legacy configs keep
    /// stable behavior.
    #[serde(default)]
    pub(crate) capture_kind: Option<OutputCaptureKind>,
    /// Optional capture-side language hint used by variant materialization.
    ///
    /// Downloader language selection remains step-option-driven via
    /// `steps[*].options.sub_langs`.
    #[serde(default)]
    pub(crate) langs: Option<String>,
    /// Optional thumbnail-id hint used by variant materialization.
    ///
    /// Thumbnail generation remains downloader-option owned.
    #[serde(default)]
    pub(crate) thumbnail_ids: Option<String>,
    /// Optional subtitle format hint for capture/materialization policy.
    #[serde(default)]
    pub(crate) sub_format: Option<String>,
    /// Optional conversion target for selected output family.
    #[serde(default)]
    pub(crate) convert: Option<String>,
    /// Optional ZIP member selector used when downstream bindings consume this
    /// output variant.
    #[serde(default)]
    pub(crate) zip_member: Option<String>,
}

impl YtDlpOutputVariantConfig {
    /// Returns effective file-vs-folder capture kind for this variant.
    #[must_use]
    pub(crate) fn effective_capture_kind(&self) -> OutputCaptureKind {
        if let Some(value) = self.capture_kind {
            return value;
        }

        match self.kind {
            YtDlpOutputKind::Subtitles => {
                if matches!(selector_cardinality(self.langs.as_deref()), SelectorCardinality::One) {
                    OutputCaptureKind::File
                } else {
                    OutputCaptureKind::Folder
                }
            }
            YtDlpOutputKind::Thumbnails => {
                if matches!(
                    selector_cardinality(self.thumbnail_ids.as_deref()),
                    SelectorCardinality::One
                ) {
                    OutputCaptureKind::File
                } else {
                    OutputCaptureKind::Folder
                }
            }
            _ => default_yt_dlp_capture_kind_for_kind(self.kind),
        }
    }
}

impl From<&YtDlpOutputVariantConfig> for OutputVariantPolicyConfig {
    fn from(value: &YtDlpOutputVariantConfig) -> Self {
        Self { save: value.save }
    }
}

/// Parsed output-variant config for one step output entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DecodedOutputVariantConfig {
    /// Generic output mapping semantics.
    Generic(GenericOutputVariantConfig),
    /// yt-dlp kind-driven output mapping semantics.
    YtDlp(YtDlpOutputVariantConfig),
}

/// Decodes one output-variant configuration value using tool-specific value
/// semantics.
#[allow(clippy::too_many_lines)]
pub(crate) fn decode_output_variant_config(
    tool: MediaStepTool,
    variant_key: &str,
    value: &Value,
) -> Result<DecodedOutputVariantConfig, String> {
    let decoded = if matches!(tool, MediaStepTool::YtDlp) {
        if !value.is_object() {
            return Err(format!(
                "output variant '{variant_key}' for tool '{}' must be an object with at least a 'kind' field",
                tool.as_str()
            ));
        }

        let config = serde_json::from_value::<YtDlpOutputVariantConfig>(value.clone()).map_err(
            |error| {
                format!(
                    "output variant '{variant_key}' for tool '{}' has invalid yt-dlp config: {error}",
                    tool.as_str()
                )
            },
        )?;

        DecodedOutputVariantConfig::YtDlp(config)
    } else {
        if !value.is_object() {
            return Err(format!(
                "output variant '{variant_key}' for tool '{}' must be an object with at least field 'kind'",
                tool.as_str()
            ));
        }

        let config = serde_json::from_value::<GenericOutputVariantConfig>(value.clone()).map_err(
            |error| {
                format!(
                    "output variant '{variant_key}' for tool '{}' has invalid config: {error}",
                    tool.as_str()
                )
            },
        )?;
        DecodedOutputVariantConfig::Generic(config)
    };

    match &decoded {
        DecodedOutputVariantConfig::Generic(config) => {
            if config.kind.trim().is_empty() {
                return Err(format!(
                    "output variant '{variant_key}' for tool '{}' kind must be non-empty",
                    tool.as_str()
                ));
            }

            if !matches!(tool, MediaStepTool::Ffmpeg) && config.idx.is_some() {
                return Err(format!(
                    "output variant '{variant_key}' for tool '{}' must not define idx; idx is only valid for tool 'ffmpeg'",
                    tool.as_str()
                ));
            }

            if matches!(tool, MediaStepTool::Ffmpeg) && config.idx.is_none() {
                return Err(format!(
                    "output variant '{variant_key}' for tool '{}' must define idx",
                    tool.as_str()
                ));
            }

            if let Some(zip_member) = config.zip_member.as_deref()
                && zip_member.trim().is_empty()
            {
                return Err(format!(
                    "output variant '{variant_key}' for tool '{}' zip_member must be non-empty",
                    tool.as_str()
                ));
            }

            if config.extension.is_some()
                && !matches!(
                    tool,
                    MediaStepTool::Ffmpeg | MediaStepTool::Rsgain | MediaStepTool::MediaTagger
                )
            {
                return Err(format!(
                    "output variant '{variant_key}' for tool '{}' must not define extension; extension is only valid for tools 'ffmpeg', 'rsgain', and 'media-tagger'",
                    tool.as_str()
                ));
            }

            if let Some(extension) = config.extension.as_deref() {
                let normalized = extension.trim();
                if normalized.contains('/') || normalized.contains('\\') {
                    return Err(format!(
                        "output variant '{variant_key}' for tool '{}' extension must not contain path separators",
                        tool.as_str()
                    ));
                }
                if normalized.chars().any(char::is_whitespace) {
                    return Err(format!(
                        "output variant '{variant_key}' for tool '{}' extension must not contain whitespace",
                        tool.as_str()
                    ));
                }
            }
        }
        DecodedOutputVariantConfig::YtDlp(config) => {
            if let Some(zip_member) = config.zip_member.as_deref()
                && zip_member.trim().is_empty()
            {
                return Err(format!(
                    "output variant '{variant_key}' for tool '{}' zip_member must be non-empty",
                    tool.as_str()
                ));
            }

            if config.thumbnail_ids.as_deref().is_some_and(|value| value.trim().is_empty()) {
                return Err(format!(
                    "output variant '{variant_key}' for tool '{}' thumbnail_ids must be non-empty",
                    tool.as_str()
                ));
            }

            if !matches!(config.kind, YtDlpOutputKind::Thumbnails) && config.thumbnail_ids.is_some()
            {
                return Err(format!(
                    "output variant '{variant_key}' for tool '{}' must not define thumbnail_ids unless kind = 'thumbnails'",
                    tool.as_str()
                ));
            }
        }
    }

    Ok(decoded)
}

/// Decodes one output-variant policy object for workflow output persistence.
pub(crate) fn decode_output_variant_policy(
    tool: MediaStepTool,
    variant_key: &str,
    value: &Value,
) -> Result<OutputVariantPolicyConfig, String> {
    match decode_output_variant_config(tool, variant_key, value)? {
        DecodedOutputVariantConfig::Generic(config) => Ok(OutputVariantPolicyConfig::from(&config)),
        DecodedOutputVariantConfig::YtDlp(config) => Ok(OutputVariantPolicyConfig::from(&config)),
    }
}

/// Resolved per-step input/output variant mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedStepVariantFlow {
    /// Input variant name consumed by one generated step.
    pub input: String,
    /// Output variant name produced by one generated step.
    pub output: String,
}

/// Resolves one step's effective variant-flow entries.
///
/// Rules:
/// - source-ingest tools must not declare `input_variants` (always empty),
/// - non-source-ingest tools require non-empty input + output variant lists,
/// - non-source-ingest lists must have equal length, or one input fan-outs to
///   all outputs,
/// - empty/blank variant names are rejected,
pub(crate) fn resolve_step_variant_flow(
    step: &MediaStep,
) -> Result<Vec<ResolvedStepVariantFlow>, String> {
    for output in step.output_variants.keys() {
        let output = output.trim();
        if output.is_empty() {
            return Err("contains an empty output variant".to_string());
        }
    }

    for input in &step.input_variants {
        let input = input.trim();
        if input.is_empty() {
            return Err("contains an empty input variant".to_string());
        }
    }

    if step.tool.is_source_ingest_tool() && !step.input_variants.is_empty() {
        return Err(format!(
            "must not define input_variants for source-ingest tool '{}'",
            step.tool.as_str()
        ));
    }

    if !step.tool.is_source_ingest_tool() && step.input_variants.is_empty() {
        return Err("must define at least one input variant".to_string());
    }
    if step.output_variants.is_empty() {
        return Err("must define at least one output variant".to_string());
    }

    if matches!(step.tool, MediaStepTool::Ffmpeg) {
        let outputs = step
            .output_variants
            .keys()
            .map(|output| Ok(output.trim().to_string()))
            .collect::<Result<Vec<_>, String>>()?;
        let primary_input = step
            .input_variants
            .first()
            .map(|value| value.trim().to_string())
            .ok_or_else(|| "must define at least one input variant".to_string())?;

        return Ok(outputs
            .into_iter()
            .map(|output| ResolvedStepVariantFlow { input: primary_input.clone(), output })
            .collect());
    }

    if !step.input_variants.is_empty()
        && step.input_variants.len() != 1
        && step.input_variants.len() != step.output_variants.len()
    {
        return Err(format!(
            "must define one input variant or equal counts of input_variants ({}) and output_variants ({})",
            step.input_variants.len(),
            step.output_variants.len()
        ));
    }

    let outputs = step
        .output_variants
        .keys()
        .map(|output| Ok(output.trim().to_string()))
        .collect::<Result<Vec<_>, String>>()?;

    if step.input_variants.is_empty() {
        return Ok(outputs
            .into_iter()
            .map(|output| ResolvedStepVariantFlow { input: output.clone(), output })
            .collect());
    }

    let inputs = step
        .input_variants
        .iter()
        .map(|input| Ok(input.trim().to_string()))
        .collect::<Result<Vec<_>, String>>()?;

    if inputs.len() == 1 {
        return Ok(outputs
            .into_iter()
            .map(|output| ResolvedStepVariantFlow { input: inputs[0].clone(), output })
            .collect());
    }

    Ok(inputs
        .into_iter()
        .zip(outputs)
        .map(|(input, output)| ResolvedStepVariantFlow { input, output })
        .collect())
}

/// Resolves one option key to a scalar string value when present.
#[must_use]
fn step_option_scalar<'a>(step: &'a MediaStep, key: &str) -> Option<&'a str> {
    match step.options.get(key) {
        Some(TransformInputValue::String(value)) => Some(value.as_str()),
        _ => None,
    }
}

/// Returns true when one option key currently stores a scalar value.
#[must_use]
fn has_step_option_scalar(step: &MediaStep, key: &str) -> bool {
    step_option_scalar(step, key).is_some()
}

/// One ordered hierarchy schema node.
///
/// Nodes form one recursive tree via `children`. Top-level `hierarchy` is an
/// ordered array of these nodes, and folder nodes may use `path = ""` to act
/// as an explicit root grouping container.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HierarchyNode {
    /// Relative path segment (or multi-segment relative path) for this node.
    ///
    /// Folder nodes may set this to empty string for one root pass-through
    /// grouping level. Non-folder leaf nodes must resolve to non-empty paths.
    pub path: String,
    /// Node behavior kind.
    #[serde(default, skip_serializing_if = "hierarchy_node_kind_is_folder")]
    pub kind: HierarchyNodeKind,
    /// Optional explicit hierarchy id used by playlist `ids` references.
    ///
    /// Any node kind may define this field when it needs stable identifier
    /// semantics; only `media`/`media_folder` ids can be resolved as playlist
    /// media targets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Optional media context id inherited by descendant nodes.
    ///
    /// This field is allowed on any node kind. `media` and `media_folder`
    /// nodes require a non-empty effective `media_id` (direct or inherited);
    /// other kinds may provide it for context propagation only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_id: Option<String>,
    /// Singular variant selector for `kind = "media"` nodes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
    /// Ordered variant selector list for `kind = "media_folder"` nodes.
    ///
    /// Selector entries support exact-string and regex-object forms:
    /// - `"variant_name"`
    /// - `{ regex = "^subtitles/.+$" }`
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_variant_selector_list",
        serialize_with = "serialize_variant_selector_list"
    )]
    pub variants: Vec<String>,
    /// Optional ordered regex rewrite rules for `media_folder` extraction.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rename_files: Vec<HierarchyFolderRenameRule>,
    /// Playlist output format for `kind = "playlist"` nodes.
    #[serde(default, skip_serializing_if = "playlist_format_is_default")]
    pub format: PlaylistFormat,
    /// Ordered playlist item references for `kind = "playlist"` nodes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ids: Vec<PlaylistItemRef>,
    /// Ordered child nodes (folder recursion).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<HierarchyNode>,
}

/// Hierarchy schema node kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum HierarchyNodeKind {
    /// Structural folder node; contributes only path grouping.
    #[default]
    Folder,
    /// File-target media output node using singular `variant`.
    Media,
    /// Folder-target media output node using plural `variants`.
    ///
    /// Variants are treated as ZIP folder payloads extracted into the target
    /// directory.
    MediaFolder,
    /// Playlist file generation node.
    Playlist,
}

/// Returns whether one node kind keeps folder-default behavior.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn hierarchy_node_kind_is_folder(value: &HierarchyNodeKind) -> bool {
    matches!(value, HierarchyNodeKind::Folder)
}

/// One flattened hierarchy leaf entry consumed by runtime materialization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HierarchyEntry {
    /// Entry behavior kind.
    ///
    /// - `media` places one media source file variant selection,
    /// - `media_folder` extracts one or more folder-capture variants,
    /// - `playlist` writes one playlist file that references media outputs.
    #[serde(default, skip_serializing_if = "hierarchy_entry_kind_is_media")]
    pub kind: HierarchyEntryKind,
    /// Referenced media id in `media` map.
    ///
    /// This field is required for `kind = "media"` entries and must stay
    /// empty for `kind = "playlist"` entries.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub media_id: String,
    /// Logical variant keys for this placement.
    ///
    /// - `kind = "media"` must resolve exactly one file variant,
    /// - `kind = "media_folder"` may resolve one or more folder variants and
    ///   merges their extracted payload trees.
    ///
    /// Selector entries support both exact-string and regex-object forms:
    /// - `"variant_name"`
    /// - `{ regex = "^subtitles/.+$" }`
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_variant_selector_list",
        serialize_with = "serialize_variant_selector_list"
    )]
    pub variants: Vec<String>,
    /// Optional ordered regex rewrite rules applied to file members extracted
    /// from folder-capture variants for this hierarchy entry.
    ///
    /// Rules are evaluated in declaration order against each normalized ZIP
    /// member relative path (file entries only). Empty list means no rewrite.
    ///
    /// This field is only valid for `kind = "media_folder"`.
    /// `kind = "media"` file targets must keep this list empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rename_files: Vec<HierarchyFolderRenameRule>,
    /// Playlist output format used by `kind = "playlist"` entries.
    #[serde(default, skip_serializing_if = "playlist_format_is_default")]
    pub format: PlaylistFormat,
    /// Ordered playlist references used by `kind = "playlist"` entries.
    ///
    /// Entries preserve declared order and may repeat the same id more than
    /// once.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ids: Vec<PlaylistItemRef>,
}

/// Hierarchy entry behavior kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum HierarchyEntryKind {
    /// Materialize one file variant at this hierarchy path.
    #[default]
    Media,
    /// Materialize one folder-merge from one or more folder variants.
    MediaFolder,
    /// Generate one playlist file at this hierarchy path.
    Playlist,
}

/// Returns whether one hierarchy entry kind keeps media-placement defaults.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn hierarchy_entry_kind_is_media(value: &HierarchyEntryKind) -> bool {
    matches!(value, HierarchyEntryKind::Media)
}

/// Supported hierarchy playlist output formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PlaylistFormat {
    /// UTF-8 M3U playlist.
    #[default]
    M3u8,
    /// Legacy M3U playlist.
    M3u,
    /// PLS playlist.
    Pls,
    /// XSPF XML playlist.
    Xspf,
    /// WPL XML playlist.
    Wpl,
    /// ASX XML playlist.
    Asx,
}

/// Returns whether playlist format keeps default `m3u8` behavior.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn playlist_format_is_default(value: &PlaylistFormat) -> bool {
    matches!(value, PlaylistFormat::M3u8)
}

/// One playlist item declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaylistItemRef {
    /// Referenced hierarchy-node id.
    pub id: String,
    /// Optional per-item path rendering override.
    pub path: PlaylistEntryPathMode,
}

/// Wire representation for one playlist item declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
enum PlaylistItemRefWire {
    /// String shorthand form (`"<id>"`).
    Id(String),
    /// Object form (`{ id = "...", path = "..." }`).
    Object(PlaylistItemRefObjectWire),
}

/// Object wire representation for one playlist item declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlaylistItemRefObjectWire {
    /// Referenced hierarchy-node id.
    id: String,
    /// Optional per-item path rendering override.
    #[serde(default, skip_serializing_if = "playlist_entry_path_mode_is_relative")]
    path: PlaylistEntryPathMode,
}

impl Serialize for PlaylistItemRef {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if playlist_entry_path_mode_is_relative(&self.path) {
            return serializer.serialize_str(self.id.as_str());
        }

        PlaylistItemRefObjectWire { id: self.id.clone(), path: self.path }.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PlaylistItemRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match PlaylistItemRefWire::deserialize(deserializer)? {
            PlaylistItemRefWire::Id(id) => Ok(Self { id, path: PlaylistEntryPathMode::Relative }),
            PlaylistItemRefWire::Object(object) => Ok(Self { id: object.id, path: object.path }),
        }
    }
}

impl PlaylistItemRef {
    /// Returns declared hierarchy-node id text for this item.
    #[must_use]
    pub fn id(&self) -> &str {
        self.id.as_str()
    }

    /// Returns effective path mode for this item.
    #[must_use]
    pub const fn path_mode(&self) -> PlaylistEntryPathMode {
        self.path
    }
}

/// Path rendering mode for one playlist item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PlaylistEntryPathMode {
    /// Render path relative to playlist file directory.
    #[default]
    Relative,
    /// Render absolute filesystem path.
    Absolute,
}

/// Returns whether playlist path mode keeps default relative behavior.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn playlist_entry_path_mode_is_relative(value: &PlaylistEntryPathMode) -> bool {
    matches!(value, PlaylistEntryPathMode::Relative)
}

/// One ordered regex rewrite rule for hierarchy folder file names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HierarchyFolderRenameRule {
    /// Regex pattern evaluated against each normalized ZIP-member file path.
    pub pattern: String,
    /// Replacement text applied when `pattern` matches.
    ///
    /// Supports `${media.id}` and `${media.metadata.<key>}` placeholders, then
    /// applies regex replacement semantics (`$0` = entire match;
    /// `$1..$N` = explicit capture groups).
    pub replacement: String,
}

/// Loads `mediapm.ncl` from disk or returns defaults when the file is absent.
///
/// # Errors
///
/// Returns [`MediaPmError`] when file I/O, Nickel evaluation, schema decoding,
/// or cross-field validation fails.
pub fn load_mediapm_document(path: &Path) -> Result<MediaPmDocument, MediaPmError> {
    if !path.exists() {
        return Ok(MediaPmDocument::default());
    }

    let bytes = fs::read(path).map_err(|source| MediaPmError::Io {
        operation: "reading mediapm.ncl".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(MediaPmDocument::default());
    }

    let source = std::str::from_utf8(&bytes).map_err(|err| {
        MediaPmError::Serialization(format!("mediapm.ncl is not valid UTF-8: {err}"))
    })?;

    let mut value = evaluate_nickel_source_to_json(path, source)?;
    normalize_version_field_to_u64(&mut value, "mediapm.ncl")?;

    let document = versions::decode_mediapm_document_value(value)?;

    validate_media_document(&document)?;

    Ok(document)
}

/// Loads machine-managed `state.ncl` from disk using shared `mediapm` schema.
///
/// # Errors
///
/// Returns [`MediaPmError`] when file I/O, Nickel evaluation, schema decoding,
/// or state-only shape validation fails.
pub fn load_mediapm_state_document(path: &Path) -> Result<MediaPmState, MediaPmError> {
    if !path.exists() {
        return Ok(MediaPmState::default());
    }

    let bytes = fs::read(path).map_err(|source| MediaPmError::Io {
        operation: "reading state.ncl".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(MediaPmState::default());
    }

    let source = std::str::from_utf8(&bytes).map_err(|err| {
        MediaPmError::Serialization(format!("state.ncl is not valid UTF-8: {err}"))
    })?;

    let mut value = evaluate_nickel_source_to_json(path, source)?;
    normalize_version_field_to_u64(&mut value, "state.ncl")?;

    let document = versions::decode_mediapm_document_value(value)?;
    validate_mediapm_state_document_shape(path, &document)?;

    Ok(document.state)
}

/// Merges user config document and machine-managed state into one runtime view.
#[must_use]
pub fn merge_mediapm_document_with_state(
    mut config_document: MediaPmDocument,
    state: MediaPmState,
) -> MediaPmDocument {
    config_document.state = state;
    config_document
}

/// Validates that a decoded state document keeps only `version` and `state`.
fn validate_mediapm_state_document_shape(
    state_path: &Path,
    document: &MediaPmDocument,
) -> Result<(), MediaPmError> {
    let has_non_state_fields = document.runtime != MediaRuntimeStorage::default()
        || !document.tools.is_empty()
        || !document.media.is_empty()
        || !document.hierarchy.is_empty();

    if has_non_state_fields {
        return Err(MediaPmError::Workflow(format!(
            "{} must contain only top-level 'version' and 'state' properties",
            state_path.display()
        )));
    }

    Ok(())
}

/// Validates media-source schema invariants that require cross-field checks.
fn validate_media_document(document: &MediaPmDocument) -> Result<(), MediaPmError> {
    validate_tool_requirements(document)?;
    validate_runtime_materialization_preference_order(document)?;

    for (media_id, source) in &document.media {
        validate_media_source(media_id, source)?;
    }

    let playlist_media_index = collect_playlist_media_index(document)?;
    validate_hierarchy_entries(document, &playlist_media_index)?;
    Ok(())
}

/// Validates runtime-configured materialization method ordering.
fn validate_runtime_materialization_preference_order(
    document: &MediaPmDocument,
) -> Result<(), MediaPmError> {
    let Some(order) = document.runtime.materialization_preference_order.as_ref() else {
        return Ok(());
    };

    if order.is_empty() {
        return Err(MediaPmError::Workflow(
            "runtime.materialization_preference_order must contain at least one method".to_string(),
        ));
    }

    let mut seen = std::collections::BTreeSet::new();
    for method in order {
        if !seen.insert(*method) {
            return Err(MediaPmError::Workflow(format!(
                "runtime.materialization_preference_order contains duplicate method '{}'",
                method.as_label()
            )));
        }
    }

    Ok(())
}

/// Collects effective hierarchy-id -> media-path mappings for playlist entries.
fn collect_playlist_media_index(
    document: &MediaPmDocument,
) -> Result<BTreeMap<String, String>, MediaPmError> {
    let flattened_hierarchy = flatten_hierarchy_nodes_for_runtime(&document.hierarchy)?;
    let mut index = BTreeMap::new();

    for flattened_entry in &flattened_hierarchy {
        if !matches!(flattened_entry.entry.kind, HierarchyEntryKind::Media) {
            continue;
        }

        let Some(hierarchy_id) = flattened_entry.hierarchy_id.as_deref() else {
            continue;
        };

        if let Some(previous_path) =
            index.insert(hierarchy_id.to_string(), flattened_entry.path.clone())
            && previous_path != flattened_entry.path
        {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy id '{hierarchy_id}' resolves to multiple media paths ('{previous_path}' and '{}')",
                flattened_entry.path
            )));
        }
    }

    Ok(index)
}

/// Metadata describing one resolved producer for hierarchy-policy validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VariantProducerValidationMeta {
    /// Variant resolves to pre-seeded local CAS hash content.
    LocalHash,
    /// Variant resolves to one step output with explicit persistence policy.
    StepOutput {
        /// Whether this output kind captures ZIP-encoded folder payload.
        is_folder_output: bool,
        /// Effective tri-state save policy.
        save: OutputSaveConfig,
    },
}

/// Returns whether one decoded output variant maps to a folder capture payload.
#[must_use]
fn decoded_output_variant_is_folder_capture(decoded: &DecodedOutputVariantConfig) -> bool {
    match decoded {
        DecodedOutputVariantConfig::Generic(config) => {
            matches!(config.effective_capture_kind(), OutputCaptureKind::Folder)
        }
        DecodedOutputVariantConfig::YtDlp(config) => {
            matches!(config.effective_capture_kind(), OutputCaptureKind::Folder)
        }
    }
}

/// Collects latest producer metadata for every variant defined by one source.
fn collect_variant_producer_validation_meta(
    media_id: &str,
    source: &MediaSourceSpec,
) -> Result<BTreeMap<String, VariantProducerValidationMeta>, MediaPmError> {
    let mut producers = BTreeMap::new();

    for variant in source.variant_hashes.keys() {
        producers.insert(variant.clone(), VariantProducerValidationMeta::LocalHash);
    }

    for (step_index, step) in source.steps.iter().enumerate() {
        for (variant_key, value) in &step.output_variants {
            let decoded =
                decode_output_variant_config(step.tool, variant_key, value).map_err(|reason| {
                    MediaPmError::Workflow(format!(
                        "media '{media_id}' step #{step_index} {reason}"
                    ))
                })?;
            let policy =
                decode_output_variant_policy(step.tool, variant_key, value).map_err(|reason| {
                    MediaPmError::Workflow(format!(
                        "media '{media_id}' step #{step_index} {reason}"
                    ))
                })?;

            producers.insert(
                variant_key.clone(),
                VariantProducerValidationMeta::StepOutput {
                    is_folder_output: decoded_output_variant_is_folder_capture(&decoded),
                    save: policy.save,
                },
            );
        }
    }

    Ok(producers)
}

/// Validates hierarchy entry invariants, including persistence-policy
/// guarantees for referenced workflow-produced variants.
///
/// Policy summary:
/// - all hierarchy-referenced step outputs must keep `save != false`,
/// - `kind = "media"` entries must reference file variants,
/// - `kind = "media_folder"` entries must reference folder variants and may
///   keep
///   default `save = true`,
/// - hierarchy `rename_files` rules are allowed only on `media_folder` entries and
///   must compile as valid regex patterns.
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps hierarchy validation invariants in one place so cross-field policy checks remain explicit"
)]
fn validate_hierarchy_entries(
    document: &MediaPmDocument,
    playlist_media_index: &BTreeMap<String, String>,
) -> Result<(), MediaPmError> {
    let flattened_hierarchy = flatten_hierarchy_nodes_for_runtime(&document.hierarchy)?;

    for flattened_entry in &flattened_hierarchy {
        let hierarchy_path = flattened_entry.path.as_str();
        let entry = &flattened_entry.entry;

        if !matches!(entry.kind, HierarchyEntryKind::Media | HierarchyEntryKind::MediaFolder) {
            continue;
        }

        if !entry.ids.is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{hierarchy_path}' kind 'media' must not define ids"
            )));
        }

        if !playlist_format_is_default(&entry.format) {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{hierarchy_path}' kind 'media' must not define format"
            )));
        }

        if entry.media_id.trim().is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{hierarchy_path}' has empty media_id"
            )));
        }

        let source = document.media.get(&entry.media_id).ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "hierarchy path '{hierarchy_path}' references unknown media '{}'",
                entry.media_id
            ))
        })?;

        let metadata_placeholders =
            hierarchy_metadata_placeholder_keys(hierarchy_path).map_err(|reason| {
                MediaPmError::Workflow(format!(
                    "hierarchy path '{hierarchy_path}' has invalid metadata placeholder syntax: {reason}"
                ))
            })?;

        for metadata_key in metadata_placeholders {
            if source
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get(metadata_key.as_str()))
                .is_none()
            {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy path '{hierarchy_path}' references undefined metadata key '{metadata_key}' for media '{}'",
                    entry.media_id
                )));
            }
        }

        if entry.variants.is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{hierarchy_path}' must define at least one variant"
            )));
        }

        let is_media_folder = matches!(entry.kind, HierarchyEntryKind::MediaFolder);
        if !is_media_folder && !entry.rename_files.is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy kind 'media' path '{hierarchy_path}' must not define rename_files; rename rules are only supported on kind 'media_folder'"
            )));
        }
        for (rule_index, rule) in entry.rename_files.iter().enumerate() {
            let pattern = rule.pattern.trim();
            if pattern.is_empty() {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy path '{hierarchy_path}' rename_files[{rule_index}] must define a non-empty regex pattern"
                )));
            }
            Regex::new(pattern).map_err(|error| {
                MediaPmError::Workflow(format!(
                    "hierarchy path '{hierarchy_path}' rename_files[{rule_index}] pattern '{pattern}' is invalid: {error}"
                ))
            })?;

            let replacement_placeholder_keys =
                hierarchy_metadata_placeholder_keys(rule.replacement.as_str()).map_err(
                    |reason| {
                        MediaPmError::Workflow(format!(
                            "hierarchy path '{hierarchy_path}' rename_files[{rule_index}] replacement has invalid metadata placeholder syntax: {reason}"
                        ))
                    },
                )?;

            if !replacement_placeholder_keys.is_empty() {
                let metadata = source.metadata.as_ref().ok_or_else(|| {
                    MediaPmError::Workflow(format!(
                        "hierarchy path '{hierarchy_path}' rename_files[{rule_index}] replacement references metadata placeholders but media '{}' does not define metadata",
                        entry.media_id
                    ))
                })?;

                for metadata_key in replacement_placeholder_keys {
                    if metadata.get(metadata_key.as_str()).is_none() {
                        return Err(MediaPmError::Workflow(format!(
                            "hierarchy path '{hierarchy_path}' rename_files[{rule_index}] replacement references undefined metadata key '{metadata_key}' for media '{}'",
                            entry.media_id
                        )));
                    }
                }
            }
        }

        let producers = collect_variant_producer_validation_meta(&entry.media_id, source)?;
        let available_variants =
            producers.keys().cloned().collect::<std::collections::BTreeSet<_>>();
        let resolved_variants = expand_variant_selectors(&entry.variants, &available_variants)
            .map_err(|reason| {
                MediaPmError::Workflow(format!(
                    "hierarchy path '{hierarchy_path}' {reason} for media '{}'",
                    entry.media_id
                ))
            })?;

        if !is_media_folder && resolved_variants.len() != 1 {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy file path '{hierarchy_path}' must resolve exactly one variant"
            )));
        }

        for resolved_variant in &resolved_variants {
            let producer = producers.get(resolved_variant.as_str()).ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "hierarchy path '{hierarchy_path}' references unknown resolved variant '{resolved_variant}' for media '{}'",
                    entry.media_id
                ))
            })?;

            if is_media_folder
                && matches!(
                    producer,
                    VariantProducerValidationMeta::StepOutput { is_folder_output: false, .. }
                )
            {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy directory path '{hierarchy_path}' requires folder variants, but resolved variant '{resolved_variant}' for media '{}' is not a folder output",
                    entry.media_id
                )));
            }

            if !is_media_folder
                && matches!(
                    producer,
                    VariantProducerValidationMeta::StepOutput { is_folder_output: true, .. }
                )
            {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy file path '{hierarchy_path}' requires file variants, but resolved variant '{resolved_variant}' for media '{}' is a folder output",
                    entry.media_id
                )));
            }

            if let VariantProducerValidationMeta::StepOutput { is_folder_output: _, save } =
                *producer
                && !save.should_persist()
            {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy path '{hierarchy_path}' requires resolved variant '{resolved_variant}' for media '{}' to have save=true or save=\"full\" on its latest producer step",
                    entry.media_id
                )));
            }
        }
    }

    for flattened_entry in &flattened_hierarchy {
        let hierarchy_path = flattened_entry.path.as_str();
        let entry = &flattened_entry.entry;

        if !matches!(entry.kind, HierarchyEntryKind::Playlist) {
            continue;
        }

        if !entry.variants.is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{hierarchy_path}' kind 'playlist' must not define variants"
            )));
        }

        if !entry.rename_files.is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{hierarchy_path}' kind 'playlist' must not define rename_files"
            )));
        }

        if hierarchy_path.ends_with('/') {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy playlist path '{hierarchy_path}' must be a file path"
            )));
        }

        if hierarchy_path.contains("${") {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy playlist path '{hierarchy_path}' must not contain placeholders"
            )));
        }

        if entry.ids.is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy playlist path '{hierarchy_path}' must define at least one playlist id"
            )));
        }

        for (item_index, item) in entry.ids.iter().enumerate() {
            let hierarchy_id = item.id().trim();
            if hierarchy_id.is_empty() {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy playlist path '{hierarchy_path}' ids[{item_index}] has empty id"
                )));
            }

            let media_path = playlist_media_index.get(hierarchy_id).ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "hierarchy playlist path '{hierarchy_path}' ids[{item_index}] references unknown hierarchy id '{hierarchy_id}'"
                ))
            })?;

            if media_path.ends_with('/') || media_path.ends_with('\\') {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy playlist path '{hierarchy_path}' ids[{item_index}] references hierarchy id '{hierarchy_id}' whose target '{media_path}' is not a media file path"
                )));
            }
        }
    }

    Ok(())
}

/// Validates desired tool requirement selector invariants.
fn validate_tool_requirements(document: &MediaPmDocument) -> Result<(), MediaPmError> {
    for (tool_name, requirement) in &document.tools {
        let version = requirement.normalized_version();
        let tag = requirement.normalized_tag();

        // Builtin source-ingest tools (import) are never
        // downloader-provisioned, so they are not required to carry a release
        // selector.
        let requires_selector = !MediaStepTool::is_builtin_source_ingest_name(tool_name.as_str());

        if requires_selector && version.is_none() && tag.is_none() {
            return Err(MediaPmError::Workflow(format!(
                "tool '{tool_name}' must define at least one selector: version or tag"
            )));
        }

        if let (Some(version), Some(tag)) = (&version, &tag)
            && normalize_selector_compare_value(version) != normalize_selector_compare_value(tag)
        {
            return Err(MediaPmError::Workflow(format!(
                "tool '{tool_name}' defines mismatched version '{version}' and tag '{tag}'; when both are provided they must refer to the same release selector"
            )));
        }

        if requirement
            .dependencies
            .ffmpeg_version
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(MediaPmError::Workflow(format!(
                "tools.{tool_name}.dependencies.ffmpeg_version must be non-empty when provided"
            )));
        }

        if requirement
            .dependencies
            .sd_version
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(MediaPmError::Workflow(format!(
                "tools.{tool_name}.dependencies.sd_version must be non-empty when provided"
            )));
        }

        let has_ffmpeg_dependency = requirement.dependencies.ffmpeg_version.is_some();
        let has_sd_dependency = requirement.dependencies.sd_version.is_some();
        let is_media_tagger = tool_name.eq_ignore_ascii_case("media-tagger");
        let is_yt_dlp = tool_name.eq_ignore_ascii_case("yt-dlp");
        let is_rsgain = tool_name.eq_ignore_ascii_case("rsgain");

        if is_media_tagger || is_yt_dlp {
            if has_sd_dependency {
                return Err(MediaPmError::Workflow(format!(
                    "tool '{tool_name}' must not define tools.{tool_name}.dependencies.sd_version; only tools.rsgain.dependencies.sd_version is supported"
                )));
            }
        } else if is_rsgain {
            // rsgain may define both ffmpeg and sd dependency selectors.
        } else if has_ffmpeg_dependency || has_sd_dependency {
            return Err(MediaPmError::Workflow(format!(
                "tool '{tool_name}' must not define dependency selector overrides; only tools.yt-dlp.dependencies.ffmpeg_version, tools.media-tagger.dependencies.ffmpeg_version, tools.rsgain.dependencies.ffmpeg_version, and tools.rsgain.dependencies.sd_version are supported"
            )));
        }

        if tool_name.eq_ignore_ascii_case("ffmpeg") {
            if requirement.max_input_slots_or_default() == 0 {
                return Err(MediaPmError::Workflow(format!(
                    "tools.ffmpeg.max_input_slots must be at least 1 (default {DEFAULT_FFMPEG_MAX_INPUT_SLOTS})",
                )));
            }

            if requirement.max_output_slots_or_default() == 0 {
                return Err(MediaPmError::Workflow(format!(
                    "tools.ffmpeg.max_output_slots must be at least 1 (default {DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS})",
                )));
            }
        } else if requirement.max_input_slots.is_some() || requirement.max_output_slots.is_some() {
            return Err(MediaPmError::Workflow(format!(
                "tool '{tool_name}' must not define ffmpeg slot settings; only tools.ffmpeg.max_input_slots and tools.ffmpeg.max_output_slots are supported"
            )));
        }
    }

    Ok(())
}

/// Validates one media source entry.
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
fn validate_media_source(media_id: &str, source: &MediaSourceSpec) -> Result<(), MediaPmError> {
    if source.steps.is_empty() && source.variant_hashes.is_empty() {
        return Err(MediaPmError::Workflow(format!(
            "media '{media_id}' must define at least one step or at least one variant_hashes entry"
        )));
    }

    if let Some(workflow_id) = source.workflow_id.as_deref()
        && workflow_id.trim().is_empty()
    {
        return Err(MediaPmError::Workflow(format!(
            "media '{media_id}' defines an empty workflow_id override"
        )));
    }

    if source.id.is_some() {
        return Err(MediaPmError::Workflow(format!(
            "media '{media_id}' must not define id; playlist references now resolve through hierarchy node ids"
        )));
    }

    let mut available_variants = source
        .variant_hashes
        .keys()
        .map(ToString::to_string)
        .collect::<std::collections::BTreeSet<_>>();

    for (variant, hash) in &source.variant_hashes {
        if variant.trim().is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' has an empty variant name in variant_hashes"
            )));
        }
        if hash.trim().is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' variant '{variant}' has an empty CAS hash pointer"
            )));
        }
    }

    for (index, step) in source.steps.iter().enumerate() {
        let mut resolved_step = step.clone();
        if !step.tool.is_source_ingest_tool() {
            resolved_step.input_variants =
                expand_variant_selectors(&step.input_variants, &available_variants).map_err(
                    |reason| {
                        MediaPmError::Workflow(format!("media '{media_id}' step #{index} {reason}"))
                    },
                )?;
        }

        let flow = resolve_step_variant_flow(&resolved_step).map_err(|reason| {
            MediaPmError::Workflow(format!("media '{media_id}' step #{index} {reason}"))
        })?;

        validate_step_output_variant_configs(media_id, index, &resolved_step)?;

        for key in resolved_step.options.keys() {
            if !is_allowed_step_option(resolved_step.tool, key) {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} uses unsupported option '{key}' for tool '{}'",
                    resolved_step.tool.as_str()
                )));
            }
        }

        if resolved_step.tool.is_online_media_downloader() {
            let uri = step_option_scalar(&resolved_step, "uri").ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} uses tool '{}' and must define options.uri",
                    resolved_step.tool.as_str()
                ))
            })?;

            let uri = Url::parse(uri).map_err(|err| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} has invalid options.uri '{uri}': {err}"
                ))
            })?;
            if !matches!(uri.scheme(), "http" | "https") {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} options.uri must use http/https, observed '{}'",
                    uri.scheme()
                )));
            }
        } else if matches!(resolved_step.tool, MediaStepTool::Import) {
            let kind = step_option_scalar(&resolved_step, "kind").ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} uses tool '{}' and must define options.kind",
                    resolved_step.tool.as_str()
                ))
            })?;

            if kind != "cas_hash" {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} options.kind must be 'cas_hash' for tool '{}', observed '{kind}'",
                    resolved_step.tool.as_str()
                )));
            }

            let hash_text = step_option_scalar(&resolved_step, "hash").ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} uses tool '{}' and must define options.hash",
                    resolved_step.tool.as_str()
                ))
            })?;
            Hash::from_str(hash_text).map_err(|_| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} has invalid options.hash '{hash_text}'"
                ))
            })?;
        } else if has_step_option_scalar(&resolved_step, "uri") {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' step #{index} uses tool '{}' and must not define options.uri",
                resolved_step.tool.as_str()
            )));
        }

        if matches!(resolved_step.tool, MediaStepTool::Ffmpeg) {
            for input_variant in &resolved_step.input_variants {
                if !available_variants.contains(input_variant.trim()) {
                    return Err(MediaPmError::Workflow(format!(
                        "media '{media_id}' step #{index} references unknown input variant '{input_variant}'"
                    )));
                }
            }
        }

        for mapping in &flow {
            if !resolved_step.tool.is_source_ingest_tool()
                && !available_variants.contains(&mapping.input)
            {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} references unknown input variant '{}'",
                    mapping.input
                )));
            }

            available_variants.insert(mapping.output.clone());
        }

        for (key, value) in &resolved_step.options {
            if key.trim().is_empty() {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} has an empty options key"
                )));
            }

            match value {
                TransformInputValue::String(text) => {
                    let _ = text;
                }
                TransformInputValue::StringList(items) => {
                    if !step_option_accepts_list_value(resolved_step.tool, key) {
                        return Err(MediaPmError::Workflow(format!(
                            "media '{media_id}' step #{index} options['{key}'] must be a string; list values are only supported for 'option_args', 'leading_args', and 'trailing_args'"
                        )));
                    }
                    if items.iter().any(|item| item.trim().is_empty()) {
                        return Err(MediaPmError::Workflow(format!(
                            "media '{media_id}' step #{index} options['{key}'] contains an empty list item"
                        )));
                    }
                }
            }
        }
    }

    validate_media_metadata_entries(media_id, source)?;

    Ok(())
}

/// Validates strict media-metadata entry semantics for one source.
fn validate_media_metadata_entries(
    media_id: &str,
    source: &MediaSourceSpec,
) -> Result<(), MediaPmError> {
    let Some(metadata) = source.metadata.as_ref() else {
        return Ok(());
    };

    let producers = collect_variant_producer_validation_meta(media_id, source)?;

    for (metadata_name, metadata_value) in metadata {
        if metadata_name.trim().is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' defines an empty metadata key"
            )));
        }

        if let MediaMetadataValue::Variant(binding) = metadata_value {
            let variant_name = binding.variant.trim();
            if variant_name.is_empty() {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' metadata '{metadata_name}' must define a non-empty variant"
                )));
            }

            let metadata_key = binding.metadata_key.trim();
            if metadata_key.is_empty() {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' metadata '{metadata_name}' must define a non-empty metadata_key"
                )));
            }

            if let Some(transform) = &binding.transform {
                let pattern = transform.pattern.trim();
                if pattern.is_empty() {
                    return Err(MediaPmError::Workflow(format!(
                        "media '{media_id}' metadata '{metadata_name}' transform.pattern must be non-empty"
                    )));
                }

                let full_match_pattern = format!("^(?:{pattern})$");
                Regex::new(&full_match_pattern).map_err(|error| {
                    MediaPmError::Workflow(format!(
                        "media '{media_id}' metadata '{metadata_name}' transform.pattern is invalid regex '{pattern}': {error}"
                    ))
                })?;
            }

            let producer = producers.get(variant_name).ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' metadata '{metadata_name}' references unknown variant '{variant_name}'"
                ))
            })?;

            if matches!(
                producer,
                VariantProducerValidationMeta::StepOutput { is_folder_output: true, .. }
            ) {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' metadata '{metadata_name}' references variant '{variant_name}' that resolves to a folder output; metadata bindings require file variants"
                )));
            }
        }
    }

    Ok(())
}

/// Parses supported hierarchy placeholders from one hierarchy key.
///
/// Supported placeholders:
/// - `${media.id}`
/// - `${media.metadata.<key>}`
///
/// Returns each referenced `${media.metadata.<key>}` key in first-seen order.
pub(crate) fn hierarchy_metadata_placeholder_keys(
    hierarchy_path: &str,
) -> Result<Vec<String>, String> {
    let mut keys = Vec::new();
    let mut cursor = 0usize;

    while let Some(relative_start) = hierarchy_path[cursor..].find("${") {
        let placeholder_start = cursor + relative_start;
        let after_marker = &hierarchy_path[placeholder_start + 2..];
        let Some(relative_end) = after_marker.find('}') else {
            return Err("missing closing '}' for placeholder".to_string());
        };

        let expression = &after_marker[..relative_end];
        let expression = expression.trim();

        if expression == "media.id" {
            cursor = placeholder_start + 2 + relative_end + 1;
            continue;
        }

        let metadata_key = expression
            .strip_prefix("media.metadata.")
            .ok_or_else(|| {
                format!(
                    "unsupported placeholder '${{{expression}}}'; only '${{media.id}}' and '${{media.metadata.<key>}}' are supported"
                )
            })?
            .trim();

        if metadata_key.is_empty() {
            return Err(format!(
                "placeholder '${{{expression}}}' must reference a non-empty metadata key"
            ));
        }

        keys.push(metadata_key.to_string());
        cursor = placeholder_start + 2 + relative_end + 1;
    }

    Ok(keys)
}

/// Validates tool-specific output-variant configuration object schemas.
fn validate_step_output_variant_configs(
    media_id: &str,
    step_index: usize,
    step: &MediaStep,
) -> Result<(), MediaPmError> {
    for (key, value) in &step.output_variants {
        let normalized_key = key.trim();
        let decoded =
            decode_output_variant_config(step.tool, normalized_key, value).map_err(|reason| {
                MediaPmError::Workflow(format!("media '{media_id}' step #{step_index} {reason}"))
            })?;

        if matches!(step.tool, MediaStepTool::Ffmpeg)
            && matches!(decoded, DecodedOutputVariantConfig::Generic(ref config) if config.kind != "primary")
        {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' step #{step_index} ffmpeg output variant '{normalized_key}' must use kind 'primary'"
            )));
        }
    }

    Ok(())
}

/// Returns whether one step option key supports list-form values.
#[must_use]
fn step_option_accepts_list_value(_tool: MediaStepTool, key: &str) -> bool {
    matches!(key, "option_args" | "leading_args" | "trailing_args")
}

/// Returns whether one step option key is supported for the given tool.
#[must_use]
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
fn is_allowed_step_option(tool: MediaStepTool, key: &str) -> bool {
    match tool {
        MediaStepTool::YtDlp => matches!(
            key,
            "uri"
                | "leading_args"
                | "trailing_args"
                | "option_args"
                | "format"
                | "format_sort"
                | "extract_audio"
                | "audio_format"
                | "audio_quality"
                | "remux_video"
                | "recode_video"
                | "convert_subs"
                | "convert_thumbnails"
                | "merge_output_format"
                | "embed_thumbnail"
                | "embed_metadata"
                | "embed_subs"
                | "embed_chapters"
                | "embed_info_json"
                | "write_subs"
                | "sub_langs"
                | "sub_format"
                | "write_thumbnail"
                | "write_all_thumbnails"
                | "write_info_json"
                | "clean_info_json"
                | "write_comments"
                | "write_description"
                | "write_annotations"
                | "write_link"
                | "write_url_link"
                | "write_webloc_link"
                | "write_desktop_link"
                | "write_chapters"
                | "split_chapters"
                | "playlist_items"
                | "no_playlist"
                | "skip_download"
                | "retries"
                | "limit_rate"
                | "concurrent_fragments"
                | "proxy"
                | "socket_timeout"
                | "sleep_subtitles"
                | "user_agent"
                | "referer"
                | "add_header"
                | "cookies"
                | "cookies_from_browser"
                | "cache_dir"
                | "ffmpeg_location"
                | "paths"
                | "output"
                | "parse_metadata"
                | "replace_in_metadata"
                | "download_sections"
                | "postprocessor_args"
                | "extractor_args"
                | "http_chunk_size"
                | "download_archive"
                | "sponsorblock_mark"
                | "sponsorblock_remove"
        ),
        MediaStepTool::Import => matches!(key, "kind" | "hash"),
        MediaStepTool::Ffmpeg => matches!(
            key,
            "leading_args"
                | "trailing_args"
                // common options
                | "option_args"
                | "audio_codec"
                | "video_codec"
                | "container"
                | "audio_bitrate"
                | "video_bitrate"
                | "audio_quality"
                | "video_quality"
                | "crf"
                | "preset"
                | "threads"
                | "log_level"
                | "progress"
                // less-common but useful options
                | "tune"
                | "profile"
                | "level"
                | "pixel_format"
                | "frame_rate"
                | "sample_rate"
                | "channels"
                | "audio_filters"
                | "video_filters"
                | "filter_complex"
                | "start_time"
                | "duration"
                | "to"
                | "movflags"
                | "map_metadata"
                | "map_chapters"
                | "map"
                | "map_channel"
                | "copy_ts"
                | "start_at_zero"
                | "stats"
                | "no_overwrite"
                | "codec_copy"
                | "faststart"
                | "hwaccel"
                | "sample_format"
                | "channel_layout"
                | "metadata"
                | "timestamp"
                | "disposition"
                | "fps_mode"
                | "force_key_frames"
                | "aspect"
                | "stream_loop"
                | "max_muxing_queue_size"
                | "strict"
                | "maxrate"
                | "bufsize"
                | "bitstream_filter"
                | "shortest"
                | "vn"
                | "an"
                | "sn"
                | "dn"
                | "id3v2_version"
        ),
        MediaStepTool::Rsgain => matches!(
            key,
            "leading_args"
                | "trailing_args"
                | "option_args"
                | "mode"
                | "album"
                | "album_aes77"
                | "skip_existing"
                | "tagmode"
                | "loudness"
                | "target_lufs"
                | "clip_mode"
                | "true_peak"
                | "dual_mono"
                | "album_mode"
                | "max_peak"
                | "lowercase"
                | "id3v2_version"
                | "opus_mode"
                | "jobs"
                | "multithread"
                | "preset"
                | "dry_run"
                | "output"
                | "quiet"
                | "skip_tags"
                | "preserve_mtime"
                | "preserve_mtimes"
                | "input_extension"
        ),
        MediaStepTool::MediaTagger => matches!(
            key,
            "leading_args"
                | "trailing_args"
                | "option_args"
                | "acoustid_endpoint"
                | "musicbrainz_endpoint"
                | "cache_dir"
                | "cache_expiry_seconds"
                | "strict_identification"
                | "write_all_tags"
                | "write_all_images"
                | "recording_mbid"
                | "release_mbid"
                | "output_container"
        ),
    }
}

/// Returns one source URI string for diagnostics/materialization bookkeeping.
#[must_use]
pub(crate) fn media_source_uri(media_id: &str, source: &MediaSourceSpec) -> String {
    source
        .steps
        .iter()
        .find_map(|step| {
            if step.tool.is_online_media_downloader() {
                step_option_scalar(step, "uri").map(ToString::to_string)
            } else {
                None
            }
        })
        .unwrap_or_else(|| format!("local:{media_id}"))
}

/// Saves `mediapm.ncl` to disk using deterministic Nickel rendering.
///
/// # Errors
///
/// Returns [`MediaPmError`] when parent directories cannot be created,
/// schema encoding fails, or the rendered Nickel payload cannot be written.
pub fn save_mediapm_document(path: &Path, document: &MediaPmDocument) -> Result<(), MediaPmError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| MediaPmError::Io {
            operation: "creating mediapm.ncl parent directory".to_string(),
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let json = versions::encode_mediapm_document_value(document.clone())?;
    let rendered = format!("{}\n", render_nickel_value(&json, 0));

    fs::write(path, rendered.as_bytes()).map_err(|source| MediaPmError::Io {
        operation: "writing mediapm.ncl".to_string(),
        path: path.to_path_buf(),
        source,
    })
}

/// Saves machine-managed `state.ncl` using shared `mediapm` schema rendering.
///
/// Persisted output includes only top-level `version` and `state` fields.
///
/// # Errors
///
/// Returns [`MediaPmError`] when parent directories cannot be created,
/// schema encoding fails, or output bytes cannot be written.
pub fn save_mediapm_state_document(path: &Path, state: &MediaPmState) -> Result<(), MediaPmError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| MediaPmError::Io {
            operation: "creating state.ncl parent directory".to_string(),
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let document = MediaPmDocument { state: state.clone(), ..MediaPmDocument::default() };

    let json = versions::encode_mediapm_document_value(document)?;
    let object = json.as_object().cloned().ok_or_else(|| {
        MediaPmError::Serialization(
            "encoding state.ncl value: top-level record required".to_string(),
        )
    })?;

    let mut state_only = serde_json::Map::new();
    if let Some(version) = object.get("version") {
        state_only.insert("version".to_string(), version.clone());
    }
    let state_value = serde_json::to_value(state)
        .map_err(|err| MediaPmError::Serialization(format!("encoding state payload: {err}")))?;
    state_only.insert("state".to_string(), state_value);

    let rendered = format!("{}\n", render_nickel_value(&Value::Object(state_only), 0));

    fs::write(path, rendered.as_bytes()).map_err(|source| MediaPmError::Io {
        operation: "writing state.ncl".to_string(),
        path: path.to_path_buf(),
        source,
    })
}

/// Returns true when `mediapm.ncl` is present and non-empty.
pub fn mediapm_document_exists(path: &Path) -> bool {
    path.exists() && fs::metadata(path).map(|meta| meta.len() > 0).unwrap_or(false)
}

/// Creates a temporary Nickel workspace that is cleaned up on drop.
#[derive(Debug)]
struct TempNickelWorkspace {
    /// Temporary workspace root.
    path: PathBuf,
}

impl TempNickelWorkspace {
    /// Allocates one unique temporary Nickel workspace directory.
    fn new() -> Result<Self, MediaPmError> {
        let pid = std::process::id();
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
        let path = std::env::temp_dir().join(format!("mediapm-nickel-{pid}-{nanos}"));

        fs::create_dir_all(&path).map_err(|source| MediaPmError::Io {
            operation: "creating temporary Nickel workspace".to_string(),
            path: path.clone(),
            source,
        })?;

        Ok(Self { path })
    }
}

impl Drop for TempNickelWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

/// Evaluates one Nickel source string into exported JSON value.
fn evaluate_nickel_source_to_json(path: &Path, source: &str) -> Result<Value, MediaPmError> {
    let workspace = TempNickelWorkspace::new()?;
    let source_path = workspace.path.join("mediapm.ncl");

    fs::write(&source_path, source).map_err(|source_err| MediaPmError::Io {
        operation: "writing temporary mediapm.ncl source".to_string(),
        path: source_path.clone(),
        source: source_err,
    })?;

    let mut program = Program::<CacheImpl>::new_from_file(
        source_path.as_os_str(),
        std::io::sink(),
        NullReporter {},
    )
    .map_err(|source_err| MediaPmError::Io {
        operation: "constructing Nickel program".to_string(),
        path: path.to_path_buf(),
        source: source_err,
    })?;

    let exported = program.eval_full_for_export().map_err(|err| {
        MediaPmError::Workflow(format!(
            "evaluating mediapm.ncl: {}",
            render_nickel_error(&mut program, err)
        ))
    })?;

    Value::deserialize(exported).map_err(|err| {
        MediaPmError::Serialization(format!("deserializing exported Nickel value: {err}"))
    })
}

/// Renders one Nickel interpreter error as user-facing text.
fn render_nickel_error(program: &mut Program<CacheImpl>, err: NickelError) -> String {
    nickel_lang_core::error::report::report_as_str(
        &mut program.files(),
        err,
        nickel_lang_core::error::report::ColorOpt::Never,
    )
}

/// Renders a field name in Nickel record syntax.
fn render_field_name(name: &str) -> String {
    if is_bare_identifier(name) {
        name.to_string()
    } else {
        serde_json::to_string(name).unwrap_or_else(|_| format!("\"{name}\""))
    }
}

/// Returns true when one record key can be emitted as a bare Nickel identifier.
fn is_bare_identifier(input: &str) -> bool {
    if is_nickel_reserved_identifier(input) {
        return false;
    }

    let mut chars = input.chars().peekable();

    while matches!(chars.peek(), Some('_')) {
        let _ = chars.next();
    }

    let Some(head) = chars.next() else {
        return false;
    };

    if !head.is_ascii_alphabetic() {
        return false;
    }

    chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '\''))
}

/// Returns true when one identifier token is reserved by Nickel syntax.
#[must_use]
fn is_nickel_reserved_identifier(input: &str) -> bool {
    matches!(
        input,
        "if" | "then"
            | "else"
            | "let"
            | "in"
            | "match"
            | "with"
            | "forall"
            | "fun"
            | "rec"
            | "import"
            | "as"
            | "null"
            | "true"
            | "false"
    )
}

/// Renders JSON as deterministic Nickel source with sorted object keys.
fn render_nickel_value(value: &Value, indent: usize) -> String {
    let pad = " ".repeat(indent);
    let next_pad = " ".repeat(indent + 2);

    match value {
        Value::Null => "null".to_string(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(text) => serde_json::to_string(text).unwrap_or_else(|_| "\"\"".to_string()),
        Value::Array(items) => {
            if items.is_empty() {
                "[]".to_string()
            } else {
                let body = items
                    .iter()
                    .map(|item| format!("{next_pad}{},", render_nickel_value(item, indent + 2)))
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("[\n{body}\n{pad}]")
            }
        }
        Value::Object(entries) => {
            if entries.is_empty() {
                "{}".to_string()
            } else {
                let mut ordered = entries.iter().collect::<Vec<_>>();
                ordered.sort_by(|(left, _), (right, _)| left.cmp(right));
                let body = ordered
                    .into_iter()
                    .map(|(key, item)| {
                        format!(
                            "{next_pad}{} = {},",
                            render_field_name(key),
                            render_nickel_value(item, indent + 2)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("{{\n{body}\n{pad}}}")
            }
        }
    }
}

/// Normalizes `version` field numbers exported by Nickel into integer JSON numbers.
fn normalize_version_field_to_u64(
    value: &mut Value,
    document_name: &str,
) -> Result<(), MediaPmError> {
    let Some(object) = value.as_object_mut() else {
        return Err(MediaPmError::Workflow(format!(
            "{document_name} must evaluate to a top-level record"
        )));
    };

    let Some(version_value) = object.get("version").cloned() else {
        return Ok(());
    };

    let normalized = if let Some(raw) = version_value.as_u64() {
        raw
    } else if let Some(raw) = version_value.as_f64() {
        let Some(normalized) = parse_non_negative_integral_u64(raw) else {
            return Err(MediaPmError::Workflow(format!(
                "{document_name} version must be a non-negative integer"
            )));
        };
        normalized
    } else {
        return Err(MediaPmError::Workflow(format!("{document_name} version must be numeric")));
    };

    object.insert("version".to_string(), Value::from(normalized));
    Ok(())
}

/// Parses one non-negative integral `f64` into `u64` when lossless.
#[must_use]
fn parse_non_negative_integral_u64(value: f64) -> Option<u64> {
    if !value.is_finite() || value < 0.0 || value.fract() != 0.0 {
        return None;
    }

    format!("{value:.0}").parse::<u64>().ok()
}

/// Parses one non-negative integral `f64` into `u32` when lossless.
#[must_use]
fn parse_non_negative_integral_u32(value: f64) -> Option<u32> {
    parse_non_negative_integral_u64(value).and_then(|normalized| u32::try_from(normalized).ok())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        HierarchyEntry, HierarchyEntryKind, MEDIAPM_DOCUMENT_VERSION, MaterializationMethod,
        MediaMetadataValue, MediaPmDocument, MediaPmImpureTimestamp, MediaPmState,
        MediaRuntimeStorage, MediaSourceSpec, MediaStep, MediaStepTool, OutputSaveConfig,
        PlaylistEntryPathMode, PlaylistFormat, ToolRequirement, TransformInputValue, Value,
        flatten_hierarchy_nodes_for_runtime, load_mediapm_document, load_mediapm_state_document,
        media_source_uri, resolve_step_variant_flow, save_mediapm_document,
        save_mediapm_state_document,
    };

    fn hierarchy_flat_map(document: &MediaPmDocument) -> BTreeMap<String, HierarchyEntry> {
        flatten_hierarchy_nodes_for_runtime(&document.hierarchy)
            .expect("flatten hierarchy")
            .into_iter()
            .map(|flattened| (flattened.path, flattened.entry))
            .collect()
    }

    fn hierarchy_nodes(entries: BTreeMap<String, HierarchyEntry>) -> Vec<super::HierarchyNode> {
        entries
            .into_iter()
            .map(|(path, entry)| match entry.kind {
                HierarchyEntryKind::Media if path.ends_with('/') || path.ends_with('\\') => {
                    super::HierarchyNode {
                        path: path.trim_end_matches(['/', '\\']).to_string(),
                        kind: super::HierarchyNodeKind::MediaFolder,
                        id: Some(entry.media_id.clone()),
                        media_id: Some(entry.media_id),
                        variant: None,
                        variants: entry.variants,
                        rename_files: entry.rename_files,
                        format: PlaylistFormat::M3u8,
                        ids: Vec::new(),
                        children: Vec::new(),
                    }
                }
                HierarchyEntryKind::Media => super::HierarchyNode {
                    path,
                    kind: super::HierarchyNodeKind::Media,
                    id: Some(entry.media_id.clone()),
                    media_id: Some(entry.media_id),
                    variant: entry.variants.first().cloned(),
                    variants: Vec::new(),
                    rename_files: Vec::new(),
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    children: Vec::new(),
                },
                HierarchyEntryKind::MediaFolder => super::HierarchyNode {
                    path,
                    kind: super::HierarchyNodeKind::MediaFolder,
                    id: Some(entry.media_id.clone()),
                    media_id: Some(entry.media_id),
                    variant: None,
                    variants: entry.variants,
                    rename_files: entry.rename_files,
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    children: Vec::new(),
                },
                HierarchyEntryKind::Playlist => super::HierarchyNode {
                    path,
                    kind: super::HierarchyNodeKind::Playlist,
                    id: None,
                    media_id: None,
                    variant: None,
                    variants: Vec::new(),
                    rename_files: Vec::new(),
                    format: entry.format,
                    ids: entry.ids,
                    children: Vec::new(),
                },
            })
            .collect()
    }

    /// Protects flat-to-node conversion helper semantics used by migration-
    /// period Rust callsites by covering `media`, `media_folder`, and
    /// `playlist`
    /// entry mapping behavior.
    #[test]
    fn hierarchy_nodes_from_flat_entries_converts_all_supported_kinds() {
        let entries = BTreeMap::from([
            (
                "library/video.mkv".to_string(),
                HierarchyEntry {
                    kind: HierarchyEntryKind::Media,
                    media_id: "demo".to_string(),
                    variants: vec!["video".to_string()],
                    rename_files: Vec::new(),
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                },
            ),
            (
                "library/subtitles/".to_string(),
                HierarchyEntry {
                    kind: HierarchyEntryKind::MediaFolder,
                    media_id: "demo".to_string(),
                    variants: vec!["subtitles".to_string()],
                    rename_files: Vec::new(),
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                },
            ),
            (
                "library/mixed.m3u8".to_string(),
                HierarchyEntry {
                    kind: HierarchyEntryKind::Playlist,
                    media_id: String::new(),
                    variants: Vec::new(),
                    rename_files: Vec::new(),
                    format: PlaylistFormat::M3u8,
                    ids: vec![super::PlaylistItemRef {
                        id: "demo".to_string(),
                        path: PlaylistEntryPathMode::Relative,
                    }],
                },
            ),
        ]);

        let nodes = super::hierarchy_nodes_from_flat_entries(&entries)
            .expect("flat hierarchy entries should convert to node-list form");

        assert_eq!(nodes.len(), 3);

        let media = nodes
            .iter()
            .find(|node| node.path == "library/video.mkv")
            .expect("media node should exist");
        assert!(matches!(media.kind, super::HierarchyNodeKind::Media));
        assert_eq!(media.media_id.as_deref(), Some("demo"));
        assert_eq!(media.variant.as_deref(), Some("video"));

        let media_folder = nodes
            .iter()
            .find(|node| node.path == "library/subtitles")
            .expect("media_folder node should exist");
        assert!(matches!(media_folder.kind, super::HierarchyNodeKind::MediaFolder));
        assert_eq!(media_folder.media_id.as_deref(), Some("demo"));
        assert_eq!(media_folder.variants, vec!["subtitles".to_string()]);

        let playlist = nodes
            .iter()
            .find(|node| node.path == "library/mixed.m3u8")
            .expect("playlist node should exist");
        assert!(matches!(playlist.kind, super::HierarchyNodeKind::Playlist));
        assert!(playlist.media_id.is_none());
        assert_eq!(playlist.ids.len(), 1);
        assert_eq!(playlist.ids[0].id(), "demo");
    }

    /// Protects round-trip persistence semantics for `mediapm.ncl` defaults.
    #[test]
    fn mediapm_document_round_trip_preserves_schema_version() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let document = MediaPmDocument::default();

        save_mediapm_document(&path, &document).expect("save mediapm.ncl");
        let decoded = load_mediapm_document(&path).expect("load mediapm.ncl");

        assert_eq!(decoded.version, MEDIAPM_DOCUMENT_VERSION);
    }

    /// Protects Nickel rendering by quoting reserved field names such as
    /// `import` so saved documents round-trip through Nickel evaluation.
    #[test]
    fn save_document_quotes_nickel_reserved_tool_key_import() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let mut document = MediaPmDocument::default();
        document.tools.insert(
            "import".to_string(),
            ToolRequirement {
                version: None,
                tag: None,
                dependencies: super::ToolRequirementDependencies::default(),
                recheck_seconds: None,
                max_input_slots: None,
                max_output_slots: None,
            },
        );

        save_mediapm_document(&path, &document).expect("save mediapm.ncl");
        let rendered = std::fs::read_to_string(&path).expect("read rendered mediapm.ncl");
        assert!(
            rendered.contains("\"import\" = {") || rendered.contains("'import' = {"),
            "reserved key must be quoted in rendered Nickel"
        );

        let decoded = load_mediapm_document(&path).expect("load mediapm.ncl");
        assert!(decoded.tools.contains_key("import"));
    }

    /// Protects machine-managed state persistence shape and round-trip decode.
    #[test]
    fn mediapm_state_document_round_trip_is_state_only() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("state.ncl");
        let mut state = MediaPmState::default();
        state.active_tools.insert("ffmpeg".to_string(), "tool-id".to_string());

        save_mediapm_state_document(&path, &state).expect("save state.ncl");
        let decoded = load_mediapm_state_document(&path).expect("load state.ncl");
        let rendered = std::fs::read_to_string(&path).expect("read state.ncl");

        assert_eq!(decoded, state);
        assert!(rendered.contains("version = 1"));
        assert!(rendered.contains("state = {"));
        assert!(!rendered.lines().any(|line| line.trim_start().starts_with("runtime =")));
        assert!(!rendered.lines().any(|line| line.trim_start().starts_with("tools =")));
        assert!(!rendered.lines().any(|line| line.trim_start().starts_with("media =")));
        assert!(!rendered.lines().any(|line| line.trim_start().starts_with("hierarchy =")));
    }

    /// Protects workflow-step refresh state persistence by round-tripping
    /// explicit step config snapshots and mediapm-managed impure timestamps.
    #[test]
    fn mediapm_state_round_trip_preserves_workflow_step_state() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("state.ncl");
        let mut state = MediaPmState::default();
        state.workflow_step_state.insert(
            "demo-media".to_string(),
            BTreeMap::from([
                (
                    "step-0".to_string(),
                    super::ManagedWorkflowStepState {
                        explicit_config: serde_json::json!({
                            "tool": "yt-dlp",
                            "output_variants": {
                                "default": { "kind": "primary", "save": "full" }
                            },
                            "options": { "uri": "https://example.com/video" }
                        }),
                        impure_timestamp: Some(MediaPmImpureTimestamp {
                            epoch_seconds: 123,
                            subsec_nanos: 456,
                        }),
                    },
                ),
                (
                    "step-1".to_string(),
                    super::ManagedWorkflowStepState {
                        explicit_config: serde_json::json!({
                            "tool": "rsgain",
                            "input_variants": ["default"],
                            "output_variants": {
                                "default": { "kind": "primary", "save": "full" }
                            },
                            "options": {}
                        }),
                        impure_timestamp: None,
                    },
                ),
            ]),
        );

        save_mediapm_state_document(&path, &state).expect("save state.ncl");
        let decoded = load_mediapm_state_document(&path).expect("load state.ncl");

        assert_eq!(decoded, state);
    }

    /// Protects strict state-file shape by rejecting non-state top-level keys.
    #[test]
    fn mediapm_state_document_rejects_non_state_top_level_fields() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("state.ncl");
        let source = r#"
{
    version = 1,
    runtime = {
        mediapm_dir = ".mediapm-custom",
    },
    state = {
        active_tools = {
            ffmpeg = "tool-id",
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write state.ncl");
        let err = load_mediapm_state_document(&path)
            .expect_err("state.ncl with runtime section must fail shape validation");

        assert!(
            err.to_string()
                .contains("must contain only top-level 'version' and 'state' properties")
        );
    }

    /// Protects node-list hierarchy decode by flattening recursive folder nodes
    /// into runtime flat-path entries while preserving directory/file targets.
    #[test]
    fn hierarchy_nested_nodes_flatten_into_runtime_paths() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        video = { kind = "primary", save = "full" },
                        subtitles = { kind = "subtitles", save = "full" },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
    hierarchy = [
        {
            path = "library",
            children = [
                {
                    path = "artist",
                    children = [
                        {
                            path = "video.mkv",
                            kind = "media",
                            id = "demo-video",
                            media_id = "demo",
                            variant = "video",
                        },
                        {
                            path = "subtitles",
                            kind = "media_folder",
                            id = "demo-subtitles",
                            media_id = "demo",
                            variants = ["subtitles"],
                        },
                    ],
                },
            ],
        },
    ],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("decode nested hierarchy document");

        let hierarchy = hierarchy_flat_map(&document);
        assert!(hierarchy.contains_key("library/artist/video.mkv"));
        assert!(hierarchy.contains_key("library/artist/subtitles"));
    }

    /// Protects hierarchy defaults by treating omitted `kind` as structural
    /// folder nodes.
    #[test]
    fn hierarchy_nested_nodes_default_to_folder_kind_when_kind_is_omitted() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        video = { kind = "primary", save = "full" },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
    hierarchy = [
        {
            path = "top",
            children = [
                {
                    path = "middle",
                    children = [
                        {
                            path = "final.mkv",
                            kind = "media",
                            id = "demo-final",
                            media_id = "demo",
                            variant = "video",
                        },
                    ],
                },
            ],
        },
    ],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("decode nested hierarchy document");

        assert!(hierarchy_flat_map(&document).contains_key("top/middle/final.mkv"));
    }

    /// Protects node-kind typing by requiring media leaf declarations to set
    /// `kind = "media"`.
    #[test]
    fn hierarchy_nested_leaf_requires_kind_marker_for_media_or_playlist() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        video = { kind = "primary", save = "full" },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
    hierarchy = [
        {
            path = "library/video.mkv",
            media_id = "demo",
            variant = "video",
        },
    ],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err =
            load_mediapm_document(&path).expect_err("media leaf without explicit kind should fail");

        assert!(err.to_string().contains("kind 'folder' must not define 'variant'"));
    }

    /// Protects persistence rendering by serializing hierarchy as ordered node
    /// arrays with explicit `kind`/`path` fields.
    #[test]
    fn save_mediapm_document_emits_nested_hierarchy_kind_field() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let mut document = MediaPmDocument::default();

        document.media.insert(
            "demo".to_string(),
            MediaSourceSpec {
                id: None,
                description: None,
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::from([(
                    "video".to_string(),
                    "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_string(),
                )]),
                steps: Vec::new(),
            },
        );

        document.hierarchy = hierarchy_nodes(BTreeMap::from([(
            "library/demo.mkv".to_string(),
            HierarchyEntry {
                kind: HierarchyEntryKind::Media,
                media_id: "demo".to_string(),
                variants: vec!["video".to_string()],
                rename_files: Vec::new(),
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
            },
        )]));

        save_mediapm_document(&path, &document).expect("save hierarchy node-list document");
        let rendered = std::fs::read_to_string(&path).expect("read rendered mediapm.ncl");

        assert!(rendered.contains("kind = \"media\""));
        assert!(rendered.contains("path = \"library/demo.mkv\""));

        let decoded = load_mediapm_document(&path).expect("decode rendered hierarchy node-list");
        assert!(hierarchy_flat_map(&decoded).contains_key("library/demo.mkv"));
    }

    /// Protects tool requirement decoding for explicit version/tag selectors.
    #[test]
    fn tool_requirements_decode_with_version_or_tag_selectors() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
  version = 1,
  tools = {
            ffmpeg = { version = "8.2" },
                        rsgain = { version = "3.7.0", tag = "v3.7.0", dependencies = { ffmpeg_version = "inherit", sd_version = "inherit" } },
                                                "media-tagger" = { tag = "latest", dependencies = { ffmpeg_version = "inherit" } },
                                                "yt-dlp" = { tag = "v2026.04.01", dependencies = { ffmpeg_version = "inherit" }, recheck_seconds = 3600 },
  },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("decode document");

        assert_eq!(document.tools["ffmpeg"].version.as_deref(), Some("8.2"));
        assert!(document.tools["ffmpeg"].tag.is_none());
        assert!(document.tools["yt-dlp"].version.is_none());
        assert_eq!(document.tools["yt-dlp"].tag.as_deref(), Some("v2026.04.01"));
        assert_eq!(document.tools["yt-dlp"].recheck_seconds, Some(3600));
        assert_eq!(
            document.tools["yt-dlp"].dependencies.ffmpeg_version.as_deref(),
            Some("inherit")
        );
        assert_eq!(document.tools["rsgain"].version.as_deref(), Some("3.7.0"));
        assert_eq!(document.tools["rsgain"].tag.as_deref(), Some("v3.7.0"));
        assert_eq!(
            document.tools["rsgain"].dependencies.ffmpeg_version.as_deref(),
            Some("inherit")
        );
        assert_eq!(document.tools["rsgain"].dependencies.sd_version.as_deref(), Some("inherit"));
        assert_eq!(
            document.tools["media-tagger"].dependencies.ffmpeg_version.as_deref(),
            Some("inherit")
        );
    }

    /// Protects tool-requirement schema by rejecting ffmpeg selector overrides
    /// on unsupported logical tools.
    #[test]
    fn unsupported_tool_rejects_ffmpeg_version_selector() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
  version = 1,
  tools = {
    archive = { tag = "latest", dependencies = { ffmpeg_version = "inherit" } },
  },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("ffmpeg_version on unsupported tool should fail");
        assert!(
            err.to_string().contains("must not define dependency selector overrides"),
            "unexpected error: {err}"
        );
    }

    /// Protects grouped dependency selector support for rsgain workflows.
    #[test]
    fn rsgain_accepts_grouped_dependency_selectors() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
  version = 1,
  tools = {
    rsgain = { tag = "latest", dependencies = { ffmpeg_version = "inherit", sd_version = "inherit" } },
  },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document =
            load_mediapm_document(&path).expect("rsgain dependencies should pass validation");
        assert_eq!(
            document.tools["rsgain"].dependencies.ffmpeg_version.as_deref(),
            Some("inherit")
        );
        assert_eq!(document.tools["rsgain"].dependencies.sd_version.as_deref(), Some("inherit"));
    }

    /// Protects yt-dlp output-variant schema by requiring `format` to be set
    /// in step `options`, not inside output-variant config objects.
    #[test]
    fn yt_dlp_output_variant_rejects_format_field() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        video = {
                            kind = "primary",
                            save = "full",
                            format = "bestvideo*+bestaudio/best",
                        },
                    },
                    options = { uri = "https://example.com/video" },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("format field must be rejected");
        assert!(err.to_string().contains("unknown field `format`"));
    }

    /// Protects runtime-storage decode for shared user-cache policy toggle.
    #[test]
    fn runtime_storage_decodes_use_user_tool_cache_toggle() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r"
{
    version = 1,
    runtime = {
        use_user_tool_cache = false,
    },
}
";

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("decode document");

        assert_eq!(document.runtime.use_user_tool_cache, Some(false));
        assert!(!document.runtime.use_user_tool_cache_enabled());
    }

    /// Protects runtime-storage decode for explicit dotenv file overrides.
    #[test]
    fn runtime_storage_decodes_env_file_override() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    runtime = {
        env_file = ".mediapm/.env.custom",
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("decode document");

        assert_eq!(document.runtime.env_file.as_deref(), Some(".mediapm/.env.custom"));
    }

    /// Protects tool-requirement decode for ffmpeg slot-limit overrides.
    #[test]
    fn tool_requirements_decode_ffmpeg_slot_limits_on_ffmpeg_tool() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    tools = {
        ffmpeg = {
            version = "latest",
            max_input_slots = 96,
            max_output_slots = 80,
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("decode document");

        assert_eq!(document.tools["ffmpeg"].max_input_slots, Some(96));
        assert_eq!(document.tools["ffmpeg"].max_output_slots, Some(80));
        assert_eq!(document.tools["ffmpeg"].max_input_slots_or_default(), 96);
        assert_eq!(document.tools["ffmpeg"].max_output_slots_or_default(), 80);
    }

    /// Protects runtime-storage decode for platform-keyed inherited env vars.
    #[test]
    fn runtime_storage_decodes_platform_inherited_env_vars() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    runtime = {
        inherited_env_vars = {
            windows = ["ComSpec", "Path"],
            linux = ["PATH"],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("decode document");

        let inherited =
            document.runtime.inherited_env_vars.as_ref().expect("inherited env map should decode");
        assert_eq!(
            inherited.get("windows"),
            Some(&vec!["ComSpec".to_string(), "Path".to_string()])
        );
        assert_eq!(inherited.get("linux"), Some(&vec!["PATH".to_string()]));
    }

    /// Protects runtime materialization policy decoding for ordered methods.
    #[test]
    fn runtime_storage_decodes_materialization_preference_order() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    runtime = {
        materialization_preference_order = ["copy", "hardlink"],
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("decode document");

        assert_eq!(
            document.runtime.materialization_preference_order,
            Some(vec![MaterializationMethod::Copy, MaterializationMethod::Hardlink])
        );
    }

    /// Protects runtime materialization policy by rejecting duplicate methods.
    #[test]
    fn runtime_storage_rejects_duplicate_materialization_preference_order() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    runtime = {
        materialization_preference_order = ["hardlink", "copy", "hardlink"],
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let error =
            load_mediapm_document(&path).expect_err("duplicate materialization methods must fail");
        assert!(
            error
                .to_string()
                .contains("runtime.materialization_preference_order contains duplicate method")
        );
    }

    /// Protects runtime materialization policy by rejecting empty method lists.
    #[test]
    fn runtime_storage_rejects_empty_materialization_preference_order() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r"
{
    version = 1,
    runtime = {
        materialization_preference_order = [],
    },
}
";

        std::fs::write(&path, source).expect("write source");
        let error =
            load_mediapm_document(&path).expect_err("empty materialization methods must fail");
        assert!(
            error.to_string().contains(
                "runtime.materialization_preference_order must contain at least one method"
            )
        );
    }

    /// Protects host-platform filtering when resolving inherited env names.
    #[test]
    fn inherited_env_vars_with_defaults_reads_only_host_platform() {
        let runtime = MediaRuntimeStorage {
            inherited_env_vars: Some(BTreeMap::from([
                ("windows".to_string(), vec!["SYSTEMROOT".to_string(), "ComSpec".to_string()]),
                ("linux".to_string(), vec!["LD_LIBRARY_PATH".to_string()]),
                ("macos".to_string(), vec!["DYLD_LIBRARY_PATH".to_string()]),
            ])),
            ..MediaRuntimeStorage::default()
        };

        let resolved = runtime.inherited_env_vars_with_defaults();

        if cfg!(windows) {
            assert!(resolved.iter().any(|value| value.eq_ignore_ascii_case("ComSpec")));
            assert!(!resolved.iter().any(|value| value.eq_ignore_ascii_case("LD_LIBRARY_PATH")));
            assert!(!resolved.iter().any(|value| value.eq_ignore_ascii_case("DYLD_LIBRARY_PATH")));
        } else if cfg!(target_os = "linux") {
            assert!(resolved.iter().any(|value| value.eq_ignore_ascii_case("LD_LIBRARY_PATH")));
            assert!(!resolved.iter().any(|value| value.eq_ignore_ascii_case("ComSpec")));
            assert!(!resolved.iter().any(|value| value.eq_ignore_ascii_case("DYLD_LIBRARY_PATH")));
        } else if cfg!(target_os = "macos") {
            assert!(resolved.iter().any(|value| value.eq_ignore_ascii_case("DYLD_LIBRARY_PATH")));
            assert!(!resolved.iter().any(|value| value.eq_ignore_ascii_case("ComSpec")));
            assert!(!resolved.iter().any(|value| value.eq_ignore_ascii_case("LD_LIBRARY_PATH")));
        }
    }

    /// Protects default cache policy when runtime-storage toggle is omitted.
    #[test]
    fn runtime_storage_defaults_to_enabled_shared_download_cache() {
        let runtime_storage = MediaRuntimeStorage::default();
        assert!(runtime_storage.use_user_tool_cache_enabled());
    }

    /// Protects runtime materialization policy defaults when runtime value is omitted.
    #[test]
    fn runtime_storage_defaults_materialization_preference_order() {
        let runtime_storage = MediaRuntimeStorage::default();
        assert_eq!(
            runtime_storage.materialization_preference_order_with_defaults(),
            vec![
                MaterializationMethod::Hardlink,
                MaterializationMethod::Symlink,
                MaterializationMethod::Reflink,
                MaterializationMethod::Copy,
            ]
        );
    }

    /// Protects ffmpeg slot-limit validation by rejecting zero input slots.
    #[test]
    fn tool_requirements_reject_zero_ffmpeg_input_slots() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    tools = {
        ffmpeg = {
            version = "latest",
            max_input_slots = 0,
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("zero input slots must fail");
        assert!(err.to_string().contains("tools.ffmpeg.max_input_slots"));
    }

    /// Protects ffmpeg slot-limit validation by rejecting zero output slots.
    #[test]
    fn tool_requirements_reject_zero_ffmpeg_output_slots() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    tools = {
        ffmpeg = {
            version = "latest",
            max_output_slots = 0,
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("zero output slots must fail");
        assert!(err.to_string().contains("tools.ffmpeg.max_output_slots"));
    }

    /// Protects tool-requirement validation by rejecting ffmpeg slot settings
    /// on non-ffmpeg logical tools.
    #[test]
    fn non_ffmpeg_tools_reject_ffmpeg_slot_settings() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    tools = {
        "yt-dlp" = {
            version = "latest",
            max_input_slots = 72,
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err =
            load_mediapm_document(&path).expect_err("non-ffmpeg slot settings must be rejected");
        assert!(err.to_string().contains("must not define ffmpeg slot settings"));
    }

    /// Protects no-backward-compat policy by rejecting legacy ffmpeg slot key
    /// names under `tools.ffmpeg`.
    #[test]
    fn tools_ffmpeg_rejects_legacy_ffmpeg_slot_key_names() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    tools = {
        ffmpeg = {
            version = "latest",
            ffmpeg_max_input_slots = 96,
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("legacy tools.ffmpeg slot key should be rejected");
        assert!(err.to_string().contains("ffmpeg_max_input_slots"));
    }

    /// Protects no-backward-compat migration policy by rejecting legacy
    /// ffmpeg slot settings under `runtime` via strict unknown-field decoding.
    #[test]
    fn runtime_rejects_legacy_ffmpeg_slot_keys() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    runtime = {
        ffmpeg_max_input_slots = 96,
    },
    tools = {
        ffmpeg = { version = "latest" },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err =
            load_mediapm_document(&path).expect_err("legacy runtime ffmpeg key must be rejected");
        assert!(err.to_string().contains("ffmpeg_max_input_slots"));
    }

    /// Protects renamed runtime key policy by rejecting legacy key spelling
    /// through strict top-level unknown-field decoding.
    #[test]
    fn runtime_storage_key_is_rejected_after_runtime_rename() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r"
{
    version = 1,
    runtime_storage = {
        use_user_tool_cache = false,
    },
}
";

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("legacy runtime_storage key must fail");
        assert!(err.to_string().contains("runtime_storage"));
    }

    /// Protects no-backward-compat policy by rejecting removed
    /// yt-dlp output-variant `filename_template` fields.
    #[test]
    fn yt_dlp_output_variant_rejects_filename_template_field() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        "subtitles/" = {
                            kind = "subtitles",
                            save = "full",
                            filename_template = "%(title)s [%(id)s].%(ext)s",
                        },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("filename_template must be rejected");

        assert!(err.to_string().contains("unknown field `filename_template`"));
    }

    /// Protects selector validation by requiring at least one version/tag entry.
    #[test]
    fn tool_requirements_reject_missing_version_and_tag() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r"
{
  version = 1,
  tools = {
      ffmpeg = {},
  },
}
    ";

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("document should fail validation");
        assert!(err.to_string().contains("must define at least one selector"));
    }

    /// Protects selector validation by rejecting mismatched version/tag pairs.
    #[test]
    fn tool_requirements_reject_mismatched_version_and_tag() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
  version = 1,
  tools = {
      ffmpeg = { version = "8.2", tag = "v8.1" },
  },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("document should fail validation");
        assert!(err.to_string().contains("mismatched version"));
    }

    /// Protects online-step schema by requiring explicit `options.uri`.
    #[test]
    fn online_step_requires_options_uri() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = { default = { kind = "primary", save = "full" } },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("document should fail validation");
        assert!(err.to_string().contains("must define options.uri"));
    }

    /// Protects simplified boolean-option semantics by accepting non-`true`
    /// values and deferring enablement checks to runtime command templates.
    #[test]
    fn online_step_write_description_accepts_non_true_values() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = { default = { kind = "primary", save = "full" } },
                    options = {
                        uri = "https://example.com/video",
                        write_description = "false",
                    },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("document should decode");

        assert_eq!(
            document.media["demo"].steps[0].options.get("write_description").and_then(|value| {
                match value {
                    TransformInputValue::String(value) => Some(value.as_str()),
                    TransformInputValue::StringList(_) => None,
                }
            }),
            Some("false"),
        );
    }

    /// Protects step option validation by rejecting undeclared keys.
    #[test]
    fn step_options_reject_unknown_keys() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        local_demo = {
            variant_hashes = { default = "blake3:abc" },
            steps = [
                {
                    tool = "ffmpeg",
                    input_variants = ["default"],
                    output_variants = { default = { kind = "primary", save = "full", idx = 0 } },
                    options = { unsupported = "yes" },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("document should fail validation");
        assert!(err.to_string().contains("unsupported option 'unsupported'"));
    }

    /// Protects unified subtitle option semantics by rejecting legacy
    /// `write_auto_subs` step options.
    #[test]
    fn step_options_reject_legacy_write_auto_subs_key() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        remote_demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = { downloaded = { kind = "primary", save = "full" } },
                    options = {
                        uri = "https://example.com/video",
                        write_auto_subs = "true",
                    },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err =
            load_mediapm_document(&path).expect_err("legacy write_auto_subs option must fail");
        assert!(err.to_string().contains("unsupported option 'write_auto_subs'"));
    }

    /// Protects expanded step-option allowlists so audited CLI keys are
    /// accepted for all managed media tools.
    #[test]
    fn step_options_accept_expanded_tool_keys() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        remote_demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = { downloaded = { kind = "primary", save = "full" } },
                    options = {
                        uri = "https://example.com/video",
                        merge_output_format = "mkv",
                        format_sort = "res,codec",
                        cache_dir = "./cache/yt-dlp",
                        playlist_items = "1:3",
                        sleep_subtitles = "60",
                        skip_download = "true",
                    },
                },
                {
                    tool = "ffmpeg",
                    input_variants = ["downloaded"],
                    output_variants = { normalized = { kind = "primary", save = "full", idx = 0 } },
                    options = {
                        audio_quality = "2",
                        map = "0:a:0",
                        map_channel = "0.0.0",
                        id3v2_version = "3",
                    },
                },
                {
                    tool = "rsgain",
                    input_variants = ["normalized"],
                    output_variants = { gained = { kind = "primary", save = "full" } },
                    options = {
                        tagmode = "i",
                        clip_mode = "p",
                        true_peak = "true",
                        preserve_mtimes = "true",
                    },
                },
                {
                    tool = "media-tagger",
                    input_variants = ["gained"],
                    output_variants = { tagged = { kind = "primary", save = "full" } },
                    options = {
                        strict_identification = "false",
                        cache_dir = "./cache",
                        cache_expiry_seconds = "86400",
                        musicbrainz_endpoint = "https://musicbrainz.org/ws/2",
                        output_container = "mp4",
                    },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("document should decode");

        assert_eq!(document.media["remote_demo"].steps.len(), 4);
    }

    /// Protects scalar-first option typing by rejecting list values for
    /// non-list option keys.
    #[test]
    fn step_options_reject_list_value_for_scalar_option_key() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        local_demo = {
            variant_hashes = { source = "blake3:abc" },
            steps = [
                {
                    tool = "ffmpeg",
                    input_variants = ["source"],
                    output_variants = { normalized = { kind = "primary", save = "full", idx = 0 } },
                    options = {
                        audio_quality = ["2"],
                    },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("document should fail validation");
        assert!(err.to_string().contains("options['audio_quality'] must be a string"));
    }

    /// Protects strict output-variant schema by rejecting non-object values
    /// for non-yt-dlp tools.
    #[test]
    fn non_yt_dlp_output_variant_rejects_string_shorthand() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        local_demo = {
            variant_hashes = { source = "blake3:abc" },
            steps = [
                {
                    tool = "ffmpeg",
                    input_variants = ["source"],
                    output_variants = { normalized = "primary" },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("document should fail validation");
        assert!(err.to_string().contains("must be an object with at least field 'kind'"));
    }

    /// Protects value-explicit output semantics by rejecting empty-object
    /// output-variant values for single-output simple tools.
    #[test]
    fn single_output_simple_tool_rejects_empty_object_output_variant() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        local_demo = {
            variant_hashes = { source = "blake3:abc" },
            steps = [
                {
                    tool = "ffmpeg",
                    input_variants = ["source"],
                    output_variants = { normalized = {} },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("document should fail validation");
        let error_text = err.to_string();
        assert!(
            error_text.contains("required fields") || error_text.contains("missing field `kind`")
        );
    }

    /// Protects per-step variant-flow decoding and list-option decoding.
    #[test]
    fn media_step_supports_variant_flow_and_list_options() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        local_demo = {
            variant_hashes = { source = "blake3:abc" },
            steps = [
                {
                    tool = "ffmpeg",
                    input_variants = ["source"],
                    output_variants = { aac = { kind = "primary", save = "full", idx = 0 } },
                    options = {
                        option_args = "-vn",
                        leading_args = ["-hide_banner"],
                        trailing_args = ["-c:a", "aac"],
                    },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("decode document");
        let step = &document.media["local_demo"].steps[0];
        let flow = resolve_step_variant_flow(step).expect("resolve flow");

        assert_eq!(flow.len(), 1);
        assert_eq!(flow[0].input, "source");
        assert_eq!(flow[0].output, "aac");
        assert!(step.options.contains_key("leading_args"));
        assert!(step.options.contains_key("trailing_args"));
    }

    /// Protects key-agnostic semantics by allowing deep slash-separated output
    /// variant names when values are valid.
    #[test]
    fn output_variants_allow_more_than_one_slash_in_keys() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = { "subtitles/en/srt" = { kind = "primary", save = "full" } },
                    options = { uri = "https://example.com/video" },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("document should decode");
        assert!(document.media["demo"].steps[0].output_variants.contains_key("subtitles/en/srt"));
    }

    /// Protects yt-dlp output config decoding by requiring object values.
    #[test]
    fn yt_dlp_output_variants_reject_non_object_values() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = { video = "audio" },
                    options = { uri = "https://example.com/video" },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("document should fail validation");
        assert!(err.to_string().contains("must be an object"));
    }

    /// Protects strict value schema by rejecting legacy yt-dlp
    /// `*_artifacts` kind names.
    #[test]
    fn yt_dlp_legacy_artifact_kind_aliases_are_rejected() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        "subtitles/" = { kind = "subtitle_artifacts", save = "full", langs = "en" },
                    },
                    options = { uri = "https://example.com/video" },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("legacy kind aliases should fail");
        assert!(err.to_string().contains("invalid yt-dlp config"));
    }

    /// Protects key-agnostic semantics by allowing folder and scoped keys to
    /// coexist in the same output map when filename templates are not used.
    #[test]
    fn output_variants_allow_scoped_and_folder_keys_together() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        "subtitles/" = { kind = "subtitles", save = "full" },
                        "subtitles/en" = { kind = "subtitles", save = "full" },
                    },
                    options = { uri = "https://example.com/video" },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("document should decode");
        let output_variants = &document.media["demo"].steps[0].output_variants;
        assert!(output_variants.contains_key("subtitles/"));
        assert!(output_variants.contains_key("subtitles/en"));
    }

    /// Protects yt-dlp value schema by allowing `langs`/`sub_format` on
    /// non-subtitle kinds for capture-side filtering semantics.
    #[test]
    fn yt_dlp_non_subtitle_variant_allows_langs_and_sub_format() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        "thumbnails/" = {
                            kind = "thumbnails",
                            save = "full",
                            langs = "all",
                            sub_format = "vtt",
                        },
                    },
                    options = { uri = "https://example.com/video" },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("document should decode");
        let step = &document.media["demo"].steps[0];
        let decoded = super::decode_output_variant_config(
            MediaStepTool::YtDlp,
            "thumbnails/",
            step.output_variants
                .get("thumbnails/")
                .expect("thumbnails output variant should exist"),
        )
        .expect("yt-dlp output variant should decode");

        match decoded {
            super::DecodedOutputVariantConfig::YtDlp(config) => {
                assert_eq!(config.langs.as_deref(), Some("all"));
                assert_eq!(config.sub_format.as_deref(), Some("vtt"));
            }
            super::DecodedOutputVariantConfig::Generic(config) => {
                panic!("expected yt-dlp config, got Generic({config:?})")
            }
        }
    }

    /// Protects hierarchy file-target semantics by keeping subtitle variants
    /// folder-captured by default.
    #[test]
    fn hierarchy_file_target_rejects_default_folder_subtitle_capture() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        subtitles = {
                            kind = "subtitles",
                            save = "full",
                        },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
    hierarchy = [
        {
            path = "demo/subtitles.srt",
            kind = "media",
            id = "demo-subtitles-file",
            media_id = "demo",
            variant = "subtitles",
        },
    ],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("default subtitle capture should remain folder output");
        assert!(err.to_string().contains("requires file variants"));
    }

    /// Protects capture-kind override semantics by allowing subtitle
    /// variants to opt into file capture behavior.
    #[test]
    fn hierarchy_file_target_accepts_subtitle_capture_kind_file() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        subtitles = {
                            kind = "subtitles",
                            capture_kind = "file",
                            save = "full",
                        },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
    hierarchy = [
        {
            path = "demo/subtitles.srt",
            kind = "media",
            id = "demo-subtitles-file",
            media_id = "demo",
            variant = "subtitles",
        },
    ],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path)
            .expect("capture_kind=file should permit file hierarchy target");
        assert!(hierarchy_flat_map(&document).contains_key("demo/subtitles.srt"));
    }

    /// Protects generalized capture-kind semantics by allowing generic
    /// transform outputs to opt into folder validation behavior.
    #[test]
    fn hierarchy_file_target_rejects_generic_capture_kind_folder() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            variant_hashes = {
                source = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            },
            steps = [
                {
                    tool = "ffmpeg",
                    input_variants = ["source"],
                    output_variants = {
                        result = {
                            kind = "primary",
                            idx = 0,
                            capture_kind = "folder",
                            save = "full",
                        },
                    },
                    options = {},
                },
            ],
        },
    },
    hierarchy = [
        {
            path = "demo/result.mkv",
            kind = "media",
            id = "demo-result-file",
            media_id = "demo",
            variant = "result",
        },
    ],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("generic capture_kind=folder should reject file target");
        assert!(err.to_string().contains("requires file variants"));
    }

    /// Protects generalized capture-kind semantics by allowing generic
    /// transform outputs to target directory hierarchy paths when set to
    /// folder capture behavior.
    #[test]
    fn hierarchy_directory_target_accepts_generic_capture_kind_folder() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            variant_hashes = {
                source = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            },
            steps = [
                {
                    tool = "ffmpeg",
                    input_variants = ["source"],
                    output_variants = {
                        result = {
                            kind = "primary",
                            idx = 0,
                            capture_kind = "folder",
                            save = "full",
                        },
                    },
                    options = {},
                },
            ],
        },
    },
    hierarchy = [
        {
            path = "demo/result",
            kind = "media_folder",
            id = "demo-result-folder",
            media_id = "demo",
            variants = ["result"],
        },
    ],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path)
            .expect("generic capture_kind=folder should permit directory target");
        assert!(hierarchy_flat_map(&document).contains_key("demo/result"));
    }

    /// Protects hierarchy rename semantics by rejecting file-target usage.
    #[test]
    fn hierarchy_file_target_rejects_rename_files_rules() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        subtitles = {
                            kind = "subtitles",
                            capture_kind = "file",
                            save = "full",
                        },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
    hierarchy = [
        {
            path = "demo/subtitles.vtt",
            kind = "media",
            id = "demo-subtitles-file",
            media_id = "demo",
            variant = "subtitles",
            rename_files = [
                { pattern = "^(.+)\\.vtt$", replacement = "$1.en.vtt" },
            ],
        },
    ],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err =
            load_mediapm_document(&path).expect_err("file-target rename_files must be rejected");
        assert!(err.to_string().contains("rename_files"));
    }

    /// Protects hierarchy rename semantics by allowing directory-target usage.
    #[test]
    fn hierarchy_directory_target_accepts_rename_files_rules() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        subtitles = {
                            kind = "subtitles",
                            save = "full",
                        },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
    hierarchy = [
        {
            path = "demo/subtitles",
            kind = "media_folder",
            id = "demo-subtitles-folder",
            media_id = "demo",
            variants = ["subtitles"],
            rename_files = [
                { pattern = "^(.+)\\.vtt$", replacement = "$1.en.vtt" },
            ],
        },
    ],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document =
            load_mediapm_document(&path).expect("directory-target rename_files should decode");
        assert!(hierarchy_flat_map(&document).contains_key("demo/subtitles"));
    }

    /// Protects rename replacement interpolation by accepting `${media.id}`
    /// and `${media.metadata.*}` placeholders in directory-target rules.
    #[test]
    fn hierarchy_directory_target_accepts_rename_files_replacement_placeholders() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            metadata = {
                title = "Demo Title",
            },
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        subtitles = {
                            kind = "subtitles",
                            save = "full",
                        },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
    hierarchy = [
        {
            path = "demo/subtitles",
            kind = "media_folder",
            id = "demo-subtitles-folder",
            media_id = "demo",
            variants = ["subtitles"],
            rename_files = [
                { pattern = "^(.+)\\.vtt$", replacement = "${media.metadata.title} [${media.id}]$1.vtt" },
            ],
        },
    ],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path)
            .expect("rename_files replacement placeholders should decode");
        assert!(hierarchy_flat_map(&document).contains_key("demo/subtitles"));
    }

    /// Protects rename replacement placeholder validation by rejecting
    /// undefined metadata references.
    #[test]
    fn hierarchy_directory_target_rejects_rename_files_replacement_unknown_metadata_key() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            metadata = {
                title = "Demo Title",
            },
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        subtitles = {
                            kind = "subtitles",
                            save = "full",
                        },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
    hierarchy = [
        {
            path = "demo/subtitles",
            kind = "media_folder",
            id = "demo-subtitles-folder",
            media_id = "demo",
            variants = ["subtitles"],
            rename_files = [
                { pattern = "^(.+)\\.vtt$", replacement = "${media.metadata.artist} [${media.id}]$1.vtt" },
            ],
        },
    ],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("unknown rename_files replacement metadata key must fail validation");
        assert!(err.to_string().contains("undefined metadata key 'artist'"));
    }

    /// Protects downloader schema by allowing omitted input variants.
    #[test]
    fn yt_dlp_step_allows_omitted_input_variants() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = { downloaded = { kind = "primary", save = "full" } },
                    options = { uri = "https://example.com/video" },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("document should decode");
        assert!(document.media["demo"].steps[0].input_variants.is_empty());
    }

    /// Protects yt-dlp schema by rejecting explicit input variant wiring.
    #[test]
    fn yt_dlp_step_rejects_non_empty_input_variants() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    input_variants = ["source"],
                    output_variants = { downloaded = { kind = "primary", save = "full" } },
                    options = { uri = "https://example.com/video" },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("yt-dlp input_variants must be rejected");
        assert!(
            err.to_string()
                .contains("must not define input_variants for source-ingest tool 'yt-dlp'")
        );
    }

    /// Protects source-ingest schema by rejecting explicit input variants for
    /// import-style ingest steps.
    #[test]
    fn import_step_rejects_non_empty_input_variants() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "import",
                    input_variants = ["default"],
                    output_variants = { default = { kind = "primary", save = "full" } },
                    options = {
                        kind = "cas_hash",
                        hash = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("import input_variants must be rejected");
        assert!(
            err.to_string()
                .contains("must not define input_variants for source-ingest tool 'import'")
        );
    }

    /// Protects step graph validation by requiring top-to-bottom variant wiring.
    #[test]
    fn step_graph_rejects_unknown_input_variant() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        local_demo = {
            steps = [
                {
                    tool = "ffmpeg",
                    input_variants = ["default"],
                    output_variants = { aac = { kind = "primary", save = "full", idx = 0 } },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("document should fail validation");
        assert!(err.to_string().contains("default") && err.to_string().contains("unknown"));
    }

    /// Protects local-import source validation for required `cas_hash` options.
    #[test]
    fn import_step_requires_cas_hash_kind_and_hash() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        local_demo = {
            steps = [
                {
                    tool = "import",
                    output_variants = { default = { kind = "primary", save = "full" } },
                    options = { kind = "cas_hash" },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("document should fail validation");
        assert!(err.to_string().contains("must define options.hash"));
    }

    /// Protects source-uri bookkeeping helper for online and local media specs.
    #[test]
    fn media_source_uri_prefers_online_uri_and_falls_back_to_local() {
        let online = MediaSourceSpec {
            id: None,
            description: None,
            title: None,
            workflow_id: None,
            metadata: None,
            variant_hashes: BTreeMap::new(),
            steps: vec![MediaStep {
                tool: MediaStepTool::YtDlp,
                input_variants: Vec::new(),
                output_variants: BTreeMap::from([(
                    "default".to_string(),
                    Value::Object(serde_json::Map::new()),
                )]),
                options: BTreeMap::from([(
                    "uri".to_string(),
                    TransformInputValue::String("https://example.com/video.mkv".to_string()),
                )]),
            }],
        };
        let local = MediaSourceSpec {
            id: None,
            description: None,
            title: None,
            workflow_id: None,
            metadata: None,
            variant_hashes: BTreeMap::new(),
            steps: vec![MediaStep {
                tool: MediaStepTool::Import,
                input_variants: Vec::new(),
                output_variants: BTreeMap::from([(
                    "default".to_string(),
                    Value::Object(serde_json::Map::new()),
                )]),
                options: BTreeMap::from([
                    (
                        "kind".to_string(),
                        TransformInputValue::String("cas_hash".to_string()),
                    ),
                    (
                        "hash".to_string(),
                        TransformInputValue::String(
                            "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                                .to_string(),
                        ),
                    ),
                ]),
            }],
        };

        assert_eq!(media_source_uri("remote-id", &online), "https://example.com/video.mkv");
        assert_eq!(media_source_uri("local-id", &local), "local:local-id");
    }

    /// Protects strict metadata schema by accepting literal and
    /// variant-binding metadata values.
    #[test]
    fn media_source_metadata_accepts_literal_and_variant_bindings() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            metadata = {
                curator = "alice",
                title = {
                    variant = "infojson",
                    metadata_key = "title",
                },
            },
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        infojson = {
                            kind = "infojson",
                        },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("decode document");
        let metadata = document
            .media
            .get("demo")
            .and_then(|spec| spec.metadata.as_ref())
            .expect("metadata should decode as object");

        assert_eq!(
            metadata.get("curator"),
            Some(&MediaMetadataValue::Literal("alice".to_string()))
        );

        match metadata.get("title") {
            Some(MediaMetadataValue::Variant(binding)) => {
                assert_eq!(binding.variant, "infojson");
                assert_eq!(binding.metadata_key, "title");
                assert!(binding.transform.is_none());
            }
            other => panic!("expected metadata variant binding, got {other:?}"),
        }
    }

    /// Protects metadata binding decode by accepting regex transform settings
    /// for variant-backed placeholders.
    #[test]
    fn media_source_metadata_variant_binding_accepts_regex_transform() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            metadata = {
                video_ext = {
                    variant = "infojson",
                    metadata_key = "ext",
                    transform = {
                        pattern = "(.+)",
                        replacement = ".$1",
                    },
                },
            },
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        infojson = {
                            kind = "infojson",
                        },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("decode document");
        let metadata = document
            .media
            .get("demo")
            .and_then(|spec| spec.metadata.as_ref())
            .expect("metadata should decode as object");

        match metadata.get("video_ext") {
            Some(MediaMetadataValue::Variant(binding)) => {
                assert_eq!(binding.variant, "infojson");
                assert_eq!(binding.metadata_key, "ext");
                let transform = binding.transform.as_ref().expect("transform should decode");
                assert_eq!(transform.pattern, "(.+)");
                assert_eq!(transform.replacement, ".$1");
            }
            other => panic!("expected metadata variant binding, got {other:?}"),
        }
    }

    /// Protects output-variant extension policy by allowing extension only for
    /// ffmpeg/rsgain/media-tagger outputs.
    #[test]
    fn output_variant_extension_rejects_unsupported_tools() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "import",
                    output_variants = {
                        default = {
                            kind = "primary",
                            extension = "mkv",
                        },
                    },
                    options = {
                        kind = "cas_hash",
                        hash = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("import extension should be rejected");
        assert!(err.to_string().contains("must not define extension"));
    }

    /// Protects source metadata top-level shape policy by rejecting
    /// non-object metadata values.
    #[test]
    fn media_source_metadata_rejects_non_object_values() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            metadata = "invalid",
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        default = {
                            kind = "primary",
                            save = "full",
                        },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("string metadata should be rejected");

        assert!(err.to_string().contains("invalid type: string \"invalid\""));
    }

    /// Protects strict metadata schema by rejecting folder-output variant
    /// bindings for metadata lookup.
    #[test]
    fn media_source_metadata_rejects_folder_output_binding() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            metadata = {
                title = {
                    variant = "subtitles",
                    metadata_key = "title",
                },
            },
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        subtitles = {
                            kind = "subtitles",
                        },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("folder variants should be rejected for metadata binding");

        assert!(err.to_string().contains("metadata bindings require file variants"));
    }

    /// Protects output policy defaults by treating omitted save as `true`.
    #[test]
    fn output_variant_policy_defaults_apply_when_save_fields_are_omitted() {
        let yt_dlp = super::decode_output_variant_policy(
            MediaStepTool::YtDlp,
            "video",
            &serde_json::json!({ "kind": "primary" }),
        )
        .expect("decode yt-dlp output policy");
        assert_eq!(yt_dlp.save, OutputSaveConfig::Bool(true));

        let ffmpeg = super::decode_output_variant_policy(
            MediaStepTool::Ffmpeg,
            "audio",
            &serde_json::json!({ "kind": "primary", "idx": 0 }),
        )
        .expect("decode ffmpeg output policy");
        assert_eq!(ffmpeg.save, OutputSaveConfig::Bool(true));
    }

    /// Protects hierarchy validation by allowing file variants to keep the
    /// default `save=true` policy when materialized to file paths.
    #[test]
    fn hierarchy_file_variant_allows_default_save_true() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        video = {
                            kind = "primary",
                        },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
    hierarchy = [
        {
            path = "demo/video.mp4",
            kind = "media",
            id = "demo-video",
            media_id = "demo",
            variant = "video",
        },
    ],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document =
            load_mediapm_document(&path).expect("hierarchy file variant should be allowed");
        assert!(hierarchy_flat_map(&document).contains_key("demo/video.mp4"));
    }

    /// Protects hierarchy interpolation policy by requiring every
    /// `${media.metadata.*}` placeholder key to be declared in source metadata.
    #[test]
    fn hierarchy_metadata_placeholder_requires_declared_metadata_key() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            metadata = {
                artist = "The Artist",
            },
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        video = {
                            kind = "primary",
                            save = "full",
                        },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
    hierarchy = [
        {
            path = "library/${media.metadata.title}/demo.mp4",
            kind = "media",
            id = "demo-video",
            media_id = "demo",
            variant = "video",
        },
    ],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("undefined metadata placeholder keys should be rejected");
        assert!(err.to_string().contains("undefined metadata key 'title'"));
    }

    /// Protects hierarchy interpolation grammar by rejecting unsupported
    /// placeholder expressions.
    #[test]
    fn hierarchy_metadata_placeholder_rejects_unsupported_expression() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            metadata = {
                title = "Demo",
            },
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        video = {
                            kind = "primary",
                            save = "full",
                        },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
    hierarchy = [
        {
            path = "library/${media.title}/demo.mp4",
            kind = "media",
            id = "demo-video",
            media_id = "demo",
            variant = "video",
        },
    ],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("unsupported placeholder expressions should be rejected");
        assert!(err.to_string().contains("unsupported placeholder"));
    }

    /// Protects hierarchy interpolation grammar by allowing `${media.id}`
    /// placeholders without requiring metadata declarations.
    #[test]
    fn hierarchy_placeholder_allows_media_id_expression() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        video = {
                            kind = "primary",
                            save = "full",
                        },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
    hierarchy = [
        {
            path = "library/${media.id}/demo.mp4",
            kind = "media",
            id = "demo-video",
            media_id = "demo",
            variant = "video",
        },
    ],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("media.id placeholder should decode");
        assert!(hierarchy_flat_map(&document).contains_key("library/${media.id}/demo.mp4"));
    }

    /// Protects playlist hierarchy decoding by preserving ordered id entries,
    /// default format policy, and per-item absolute-path overrides.
    #[test]
    fn hierarchy_playlist_entry_decodes_ordered_ids_and_path_modes() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        a = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        video = {
                            kind = "primary",
                            save = "full",
                        },
                    },
                    options = {
                        uri = "https://example.com/a",
                    },
                },
            ],
        },
        b = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        video = {
                            kind = "primary",
                            save = "full",
                        },
                    },
                    options = {
                        uri = "https://example.com/b",
                    },
                },
            ],
        },
    },
    hierarchy = [
        {
            path = "library/a.mp4",
            kind = "media",
            id = "playlist-a",
            media_id = "a",
            variant = "video",
        },
        {
            path = "library/b.mp4",
            kind = "media",
            id = "b",
            media_id = "b",
            variant = "video",
        },
        {
            path = "playlists/demo.m3u8",
            kind = "playlist",
            ids = [
                "playlist-a",
                {
                    id = "b",
                    path = "absolute",
                },
            ],
        },
    ],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("playlist hierarchy should decode");
        let hierarchy = hierarchy_flat_map(&document);
        let playlist_entry = hierarchy.get("playlists/demo.m3u8").expect("playlist entry exists");

        assert!(matches!(playlist_entry.kind, HierarchyEntryKind::Playlist));
        assert!(matches!(playlist_entry.format, PlaylistFormat::M3u8));
        assert_eq!(playlist_entry.ids.len(), 2);
        assert_eq!(playlist_entry.ids[0].id(), "playlist-a");
        assert!(matches!(playlist_entry.ids[0].path_mode(), PlaylistEntryPathMode::Relative));
        assert_eq!(playlist_entry.ids[1].id(), "b");
        assert!(matches!(playlist_entry.ids[1].path_mode(), PlaylistEntryPathMode::Absolute));
    }

    /// Protects playlist hierarchy decoding by preserving explicit non-default
    /// format selections and duplicate id ordering semantics.
    #[test]
    fn hierarchy_playlist_entry_decodes_explicit_format_and_duplicate_ids() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        a = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        video = {
                            kind = "primary",
                            save = "full",
                        },
                    },
                    options = {
                        uri = "https://example.com/a",
                    },
                },
            ],
        },
    },
    hierarchy = [
        {
            path = "library/a.mp4",
            kind = "media",
            id = "playlist-a",
            media_id = "a",
            variant = "video",
        },
        {
            path = "playlists/demo.xspf",
            kind = "playlist",
            format = "xspf",
            ids = [
                "playlist-a",
                {
                    id = "playlist-a",
                },
                "playlist-a",
            ],
        },
    ],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document =
            load_mediapm_document(&path).expect("playlist hierarchy with xspf should decode");
        let hierarchy = hierarchy_flat_map(&document);
        let playlist_entry = hierarchy.get("playlists/demo.xspf").expect("playlist entry exists");

        assert!(matches!(playlist_entry.kind, HierarchyEntryKind::Playlist));
        assert!(matches!(playlist_entry.format, PlaylistFormat::Xspf));
        assert_eq!(playlist_entry.ids.len(), 3);
        assert_eq!(playlist_entry.ids[0].id(), "playlist-a");
        assert_eq!(playlist_entry.ids[1].id(), "playlist-a");
        assert_eq!(playlist_entry.ids[2].id(), "playlist-a");
        assert!(matches!(playlist_entry.ids[0].path_mode(), PlaylistEntryPathMode::Relative));
        assert!(matches!(playlist_entry.ids[1].path_mode(), PlaylistEntryPathMode::Relative));
        assert!(matches!(playlist_entry.ids[2].path_mode(), PlaylistEntryPathMode::Relative));
    }

    /// Protects playlist hierarchy validation by rejecting unknown referenced
    /// ids.
    #[test]
    fn hierarchy_playlist_entry_rejects_unknown_referenced_id() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        video = {
                            kind = "primary",
                            save = "full",
                        },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
    hierarchy = [
        {
            path = "library/demo.mp4",
            kind = "media",
            id = "demo-video",
            media_id = "demo",
            variant = "video",
        },
        {
            path = "playlists/demo.m3u8",
            kind = "playlist",
            ids = ["unknown-id"],
        },
    ],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let error = load_mediapm_document(&path)
            .expect_err("playlist should reject unknown referenced ids");
        assert!(error.to_string().contains("unknown hierarchy id 'unknown-id'"));
    }

    /// Protects hierarchy id uniqueness by rejecting duplicate `hierarchy[*].id`
    /// assignments across media nodes.
    #[test]
    fn media_hierarchy_id_rejects_duplicates_across_media_entries() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        a = {
            variant_hashes = {
                video = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            },
        },
        b = {
            variant_hashes = {
                video = "blake3:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            },
        },
    },
    hierarchy = [
        {
            path = "library/a.mp4",
            kind = "media",
            id = "duplicate",
            media_id = "a",
            variant = "video",
        },
        {
            path = "library/b.mp4",
            kind = "media",
            id = "duplicate",
            media_id = "b",
            variant = "video",
        },
    ],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let error =
            load_mediapm_document(&path).expect_err("duplicate hierarchy ids should be rejected");
        assert!(error.to_string().contains("hierarchy id 'duplicate'"));
        assert!(
            error.to_string().contains("duplicated") || error.to_string().contains("duplicates")
        );
    }

    /// Protects hierarchy validation by allowing folder variants to keep the
    /// default `save=true` policy when materialized to directory paths.
    #[test]
    fn hierarchy_directory_variant_allows_default_save_true() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        "subtitles" = {
                            kind = "subtitles",
                        },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
    hierarchy = [
        {
            path = "demo/subtitles",
            kind = "media_folder",
            id = "demo-subtitles-folder",
            media_id = "demo",
            variants = ["subtitles"],
        },
    ],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document =
            load_mediapm_document(&path).expect("hierarchy folder variant should be allowed");
        assert!(hierarchy_flat_map(&document).contains_key("demo/subtitles"));
    }

    /// Protects hierarchy typing by rejecting folder variants for file paths.
    #[test]
    fn hierarchy_file_path_rejects_folder_variant_output() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        subtitles = {
                            kind = "subtitles",
                            save = "full",
                        },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
    hierarchy = [
        {
            path = "demo/subtitles.txt",
            kind = "media",
            id = "demo-subtitles-file",
            media_id = "demo",
            variant = "subtitles",
        },
    ],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("file paths must reject folder output variants");
        assert!(err.to_string().contains("requires file variants"));
    }

    /// Protects selector-object support by allowing regex object syntax in
    /// both `input_variants` and `media_folder` hierarchy `variants`.
    #[test]
    fn regex_selector_objects_are_supported_for_steps_and_hierarchy() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "import",
                    output_variants = {
                        source = {
                            kind = "result",
                            save = "full",
                        },
                    },
                    options = {
                        kind = "cas_hash",
                        hash = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    },
                },
                {
                    tool = "ffmpeg",
                    input_variants = [{ regex = "^source$" }],
                    output_variants = {
                        video = {
                            kind = "primary",
                            idx = 0,
                            capture_kind = "folder",
                            save = "full",
                            extension = "mkv",
                        },
                    },
                },
            ],
        },
    },
    hierarchy = [
        {
            path = "library/demo",
            kind = "media_folder",
            id = "demo-folder",
            media_id = "demo",
            variants = [{ regex = "^video$" }],
        },
    ],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("regex selector objects should decode");

        assert!(document.media["demo"].steps[1].input_variants[0].contains("source"));
        let hierarchy = hierarchy_flat_map(&document);
        let media_folder = hierarchy
            .get("library/demo")
            .expect("media_folder hierarchy entry should flatten without trailing slash");
        assert_eq!(media_folder.variants.len(), 1);
        assert!(media_folder.variants[0].contains("video"));
    }

    /// Protects selector decode by rejecting malformed regex selector objects.
    #[test]
    fn regex_selector_object_rejects_invalid_pattern() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "yt-dlp",
                    output_variants = {
                        video = {
                            kind = "primary",
                            save = "full",
                        },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
    hierarchy = [
        {
            path = "library/demo",
            kind = "media_folder",
            media_id = "demo",
            variants = [{ regex = "[" }],
        },
    ],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("invalid regex selector object must be rejected");
        assert!(err.to_string().contains("regex selector"));
    }
}
