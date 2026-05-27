//! Nickel-backed `mediapm.ncl` and `state.ncl` document model and I/O
//! helpers.
//!
//! The `mediapm.ncl` file is the declarative desired-state surface for
//! mediapm: media sources, hierarchy mapping, and desired tool enablement.
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
use std::path::Path;

use mediapm_conductor::{
    default_runtime_inherited_env_vars_for_host, use_user_download_cache_enabled,
};
use regex::Regex;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use unicode_normalization::UnicodeNormalization;

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
        let value_nfd = value.nfd().collect::<String>();
        if value_nfd != value {
            return Err(format!(
                "{context_label} path '{raw_path}' must be Unicode NFD normalized"
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

/// Top-level mediapm Nickel document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaPmDocument {
    /// Explicit schema marker for migration safety.
    pub version: u32,
    /// Optional runtime-path overrides for mediapm local state.
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
    /// Managed workflow step refresh state grouped by media id.
    ///
    /// This ordered list keeps prior explicit step snapshots in synthesis
    /// order so reconciliation can forward-scan for exact matches.
    ///
    /// Matching policy is intentionally strict and order-aware:
    /// - for each current step, scan for the first exact `explicit_config`
    ///   match after the last matched index,
    /// - refresh when no exact match exists,
    /// - after an exact match, refresh only when the matched
    ///   `impure_timestamp` is missing.
    #[serde(default)]
    pub workflow_states: BTreeMap<String, Vec<ManagedWorkflowStepState>>,
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
        && value.workflow_states.is_empty()
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

/// Runtime path overrides for mediapm local state.
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
/// Metadata values are intentionally strict and support three forms:
/// - `"text"` literal values,
/// - object bindings that extract one key from one produced file variant.
/// - ordered fallback lists of literal/object candidates where runtime
///   resolves the first non-empty candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MediaMetadataValue {
    /// Literal metadata text value.
    Literal(String),
    /// Variant-file metadata lookup binding.
    Variant(MediaMetadataVariantBinding),
    /// Ordered fallback candidates evaluated top-to-bottom until one
    /// candidate resolves to a non-empty metadata string.
    Fallback(Vec<MediaMetadataValueCandidate>),
}

/// One metadata value candidate entry used by fallback lists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MediaMetadataValueCandidate {
    /// Literal fallback metadata text.
    Literal(String),
    /// Variant-file metadata lookup fallback binding.
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
    /// Each key maps to one of:
    /// - one literal string value, or
    /// - one `{ variant, metadata_key, transform? }` object that
    ///   resolves metadata from a
    ///   file variant produced by this media source, or
    /// - one ordered list of string/object candidates where runtime picks the
    ///   first non-empty value.
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
    /// Values are always scalar strings.
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

mod nickel_io;

use self::nickel_io::{
    evaluate_nickel_source_to_json, normalize_version_field_to_u64,
    parse_non_negative_integral_u32, parse_non_negative_integral_u64, render_nickel_value,
};

mod validation;

pub(crate) use self::validation::{hierarchy_metadata_placeholder_keys, media_source_uri};
use self::validation::{validate_media_document, validate_mediapm_state_document_shape};

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
    path.exists() && fs::metadata(path).is_ok_and(|meta| meta.len() > 0)
}

#[cfg(test)]
mod tests;
