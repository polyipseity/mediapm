//! Hierarchy node types, variant selector helpers, and flatten/nest utilities
//! for mediapm configuration.

use std::collections::BTreeMap;

use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use unicode_normalization::UnicodeNormalization;

use crate::error::MediaPmError;

/// Runtime-local hierarchy sanitization policy.
///
/// This field accepts the same user-facing wire forms as the config field:
/// - `false` disables sanitization,
/// - `true` (default) enables replacement using runtime defaults,
/// - `{ "<": "_", ... }` applies a custom per-character mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SanitizeNamesConfig {
    /// Explicitly disable reserved-character replacement.
    Disabled,
    /// Enable reserved-character replacement using runtime defaults.
    Enabled,
    /// Override runtime defaults with a custom replacement map.
    Custom(BTreeMap<char, char>),
}

impl Default for SanitizeNamesConfig {
    fn default() -> Self {
        Self::Enabled
    }
}

impl SanitizeNamesConfig {
    /// Returns whether sanitization is disabled for this node.
    #[must_use]
    pub fn is_disabled(&self) -> bool {
        matches!(self, Self::Disabled)
    }

    /// Returns whether sanitization is enabled for this node.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        !self.is_disabled()
    }

    /// Returns the effective replacement map for this node by merging the
    /// node's custom mapping over the runtime defaults.
    #[must_use]
    pub fn replacement_map_with_defaults(
        &self,
        defaults: &BTreeMap<char, char>,
    ) -> BTreeMap<char, char> {
        let mut map = defaults.clone();
        if let Self::Custom(custom) = self {
            map.extend(custom.clone());
        }
        map
    }
}

impl Serialize for SanitizeNamesConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Disabled => serializer.serialize_bool(false),
            Self::Enabled => serializer.serialize_bool(true),
            Self::Custom(map) => {
                let encoded: BTreeMap<String, String> =
                    map.iter().map(|(key, value)| (key.to_string(), value.to_string())).collect();
                encoded.serialize(serializer)
            }
        }
    }
}

impl<'de> Deserialize<'de> for SanitizeNamesConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        match value {
            Value::Bool(false) => Ok(Self::Disabled),
            Value::Bool(true) => Ok(Self::Enabled),
            Value::Object(map) => {
                let mut decoded = BTreeMap::new();
                for (key, value) in map {
                    let key_char = key.chars().next().ok_or_else(|| {
                        serde::de::Error::custom(
                            "sanitize_names mapping keys must be a single character",
                        )
                    })?;
                    if key.chars().count() != 1 {
                        return Err(serde::de::Error::custom(
                            "sanitize_names mapping keys must be a single character",
                        ));
                    }

                    let replacement = value
                        .as_str()
                        .ok_or_else(|| {
                            serde::de::Error::custom(
                                "sanitize_names mapping values must be single-character strings",
                            )
                        })?
                        .to_string();
                    let replacement_char = replacement.chars().next().ok_or_else(|| {
                        serde::de::Error::custom(
                            "sanitize_names mapping values must be single-character strings",
                        )
                    })?;
                    if replacement.chars().count() != 1 {
                        return Err(serde::de::Error::custom(
                            "sanitize_names mapping values must be single-character strings",
                        ));
                    }
                    decoded.insert(key_char, replacement_char);
                }
                Ok(Self::Custom(decoded))
            }
            _ => Err(serde::de::Error::custom(
                "sanitize_names must be a boolean or a mapping of single-character replacements",
            )),
        }
    }
}

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

/// Validates and normalizes one hierarchy node path segment.
///
/// Paths are validated for NFD normalization, no leading slash, no traversal
/// segments, and no empty interior segments.
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
    inherited_sanitize_names: &SanitizeNamesConfig,
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

        let sanitize_names = if node.sanitize_names.is_disabled() {
            inherited_sanitize_names.clone()
        } else {
            node.sanitize_names.clone()
        };

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
                    &sanitize_names,
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
                        sanitize_names: sanitize_names.clone(),
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

                // NOTE: media_folder nodes may resolve to empty path (path="") when multiple
                // nodes with different variants materialize to the same location.
                // The variants and rename_files rules ensure different final output paths
                // at materialization time. Duplicate path validation occurs after flattening
                // and allows same path with different variants.

                flattened.push(FlattenedHierarchyEntry {
                    path: resolved_node_path,
                    entry: HierarchyEntry {
                        kind: HierarchyEntryKind::MediaFolder,
                        media_id,
                        variants: node.variants.clone(),
                        rename_files: node.rename_files.clone(),
                        format: PlaylistFormat::M3u8,
                        ids: Vec::new(),
                        sanitize_names: sanitize_names.clone(),
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
                        sanitize_names: sanitize_names.clone(),
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
    flatten_hierarchy_nodes_inner(
        hierarchy,
        "",
        None,
        &SanitizeNamesConfig::Enabled,
        &mut flattened,
    )
    .map_err(MediaPmError::Workflow)?;

    let mut seen_paths = BTreeMap::<(String, String), Vec<usize>>::new();
    let mut seen_hierarchy_ids = BTreeMap::<String, String>::new();
    for (index, entry) in flattened.iter().enumerate() {
        let path_key = (entry.path.clone(), entry.entry.media_id.clone());
        seen_paths.entry(path_key.clone()).or_default().push(index);

        // Check for true duplicate paths: same path AND same variants (or both lack variants).
        // Allow same path with different variants since rename_files rules differentiate outputs.
        // Template paths with different media_ids resolve to different paths during
        // materialization (via resolve_hierarchy_relative_path), so they are not
        // considered duplicates at flattening time.
        if seen_paths[&path_key].len() > 1 {
            let current_variants =
                entry.entry.variants.iter().collect::<std::collections::BTreeSet<_>>();
            let previous_index = seen_paths[&path_key][seen_paths[&path_key].len() - 2];
            let previous_variants = flattened[previous_index]
                .entry
                .variants
                .iter()
                .collect::<std::collections::BTreeSet<_>>();

            // Only error if both have empty variants (true duplicate) or if they have overlapping variants with identical rename_files
            if current_variants.is_empty() && previous_variants.is_empty() {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy flattening produced duplicate path '{}' with no differentiating variants (entries #{previous_index} and #{index})",
                    entry.path
                )));
            }

            // Check for overlapping variants which would conflict.
            // Allow same path with overlapping variants when rename_files rules differ,
            // since rename_files produce different output file names at materialization time.
            // The materializer handles multi-entry deduplication through isolated staging
            // directories per entry (see materializer/mod.rs MediaFolder staging logic).
            let overlap: Vec<_> = current_variants.intersection(&previous_variants).collect();
            if !overlap.is_empty() {
                let current_rename = &entry.entry.rename_files;
                let previous_rename = &flattened[previous_index].entry.rename_files;
                if current_rename == previous_rename {
                    return Err(MediaPmError::Workflow(format!(
                        "hierarchy flattening produced duplicate path '{}' with overlapping variants {:?} and identical rename_files (entries #{previous_index} and #{index})",
                        entry.path, overlap
                    )));
                }
            }
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
                    sanitize_names: entry.sanitize_names.clone(),
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
                    sanitize_names: entry.sanitize_names.clone(),
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
                    sanitize_names: entry.sanitize_names.clone(),
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
pub(super) fn deserialize_hierarchy_node_list<'de, D>(
    deserializer: D,
) -> Result<Vec<HierarchyNode>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    flatten_hierarchy_value(value).map_err(serde::de::Error::custom)
}

/// Serializes hierarchy field values into array-of-nodes representation.
pub(super) fn serialize_hierarchy_node_list<S>(
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
pub(super) fn deserialize_variant_selector_list<'de, D>(
    deserializer: D,
) -> Result<Vec<String>, D::Error>
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
pub(super) fn serialize_variant_selector_list<S>(
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
    /// Optional sanitizer policy for this node and its descendants.
    #[serde(default, skip_serializing_if = "SanitizeNamesConfig::is_enabled")]
    pub sanitize_names: SanitizeNamesConfig,
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
    /// Optional sanitizer policy inherited from the source hierarchy node.
    #[serde(default, skip_serializing_if = "SanitizeNamesConfig::is_enabled")]
    pub sanitize_names: SanitizeNamesConfig,
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
pub(super) fn playlist_format_is_default(value: &PlaylistFormat) -> bool {
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
