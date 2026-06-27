//! Hierarchy node, entry, and path types for mediapm configuration.
//!
//! These types model the `hierarchy` declarations in `mediapm.ncl` plus the
//! flatten/nest utilities and sanitization config.

use std::collections::BTreeMap;

use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::error::MediaPmError;

// ---------------------------------------------------------------------------
// Sanitization config
// ---------------------------------------------------------------------------

/// Filename sanitization policy for hierarchy entries.
///
/// Control how reserved filename characters (`<`, `>`, `:`, `"`, `/`, `\\`,
/// `|`, `?`, `*`) are handled during materialization.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SanitizeNamesConfig {
    /// No sanitization (variant outputs are named as-produced).
    Disabled,
    /// Inherit parent or global sanitization policy.
    #[default]
    Inherit,
    /// Apply default sanitization (reserved chars → `_`).
    Enabled,
    /// Apply custom sanitization with explicit replacement mapping.
    ///
    /// The value is a `BTreeMap<char, char>` serialized as `{ "<": "_", ... }`.
    Custom(BTreeMap<char, char>),
}

// ---------------------------------------------------------------------------
// Hierarchy node kind
// ---------------------------------------------------------------------------

/// Kind of one hierarchy node declaration.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HierarchyNodeKind {
    /// Plain folder grouping (no media binding).
    #[default]
    Folder,
    /// Single-file media entry.
    Media,
    /// Multi-variant media folder entry.
    #[serde(rename = "media_folder")]
    MediaFolder,
    /// Playlist definition.
    Playlist,
}

// ---------------------------------------------------------------------------
// Playlist types
// ---------------------------------------------------------------------------

/// Supported playlist serialization formats.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlaylistFormat {
    /// M3U8 extended format.
    #[serde(rename = "m3u8")]
    #[default]
    M3u8,
    /// PLS format.
    Pls,
    /// XSPF (XML Shareable Playlist Format).
    Xspf,
    /// WPL (Windows Media Player) format.
    Wpl,
    /// ASX (Advanced Stream Redirector) format.
    Asx,
}

/// Returns true when the serializer can omit the playlist format field.
#[must_use]
#[allow(clippy::trivially_copy_pass_by_ref)]
pub fn playlist_format_is_default(format: &PlaylistFormat) -> bool {
    matches!(format, PlaylistFormat::M3u8)
}

/// One playlist item reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PlaylistItemRef {
    /// Shorthand: bare hierarchy id string.
    Shorthand(String),
    /// Object form with explicit path.
    Object {
        /// Target hierarchy node id.
        id: String,
        /// Optional relative path override.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
    },
}

/// Playlist entry path output mode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PlaylistEntryPathMode {
    /// Relative paths in playlist output.
    #[default]
    Relative,
    /// Absolute paths in playlist output.
    Absolute,
}

// ---------------------------------------------------------------------------
// Rename rules
// ---------------------------------------------------------------------------

/// One regex rename rule for hierarchy folder members.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HierarchyFolderRenameRule {
    /// Regex pattern matched against filenames (full match).
    pub pattern: String,
    /// Replacement template string.
    pub replacement: String,
}

// ---------------------------------------------------------------------------
// Hierarchy node
// ---------------------------------------------------------------------------

/// One node in the ordered hierarchy declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HierarchyNode {
    /// Relative path from hierarchy root.
    #[serde(default)]
    pub path: HierarchyPath,
    /// Node kind.
    #[serde(default)]
    pub kind: HierarchyNodeKind,
    /// Optional stable hierarchy id for playlist reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Media id this node binds to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_id: Option<String>,
    /// Single variant name (for `media` kind).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
    /// Multiple variant names or selectors (for `media_folder` kind).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub variants: Vec<String>,
    /// Optional rename rules for folder members.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rename_files: Vec<HierarchyFolderRenameRule>,
    /// Playlist output format.
    #[serde(default, skip_serializing_if = "playlist_format_is_default")]
    pub format: PlaylistFormat,
    /// Playlist item references.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ids: Vec<PlaylistItemRef>,
    /// Sanitization policy for this node.
    #[serde(default)]
    pub sanitize_names: SanitizeNamesConfig,
    /// Recursive children.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<HierarchyNode>,
}

// ---------------------------------------------------------------------------
// Flattened hierarchy entry (runtime form)
// ---------------------------------------------------------------------------

/// Runtime hierarchy entry kind (post-flattening).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HierarchyEntryKind {
    /// Single-file media target.
    Media,
    /// Multi-variant media directory.
    MediaFolder,
    /// Playlist definition.
    Playlist,
}

/// Runtime hierarchy entry (post-flattening).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HierarchyEntry {
    /// Entry kind.
    pub kind: HierarchyEntryKind,
    /// Bound media id.
    pub media_id: String,
    /// Variant names or selectors.
    pub variants: Vec<String>,
    /// Optional rename rules.
    pub rename_files: Vec<HierarchyFolderRenameRule>,
    /// Playlist output format.
    pub format: PlaylistFormat,
    /// Playlist item references.
    pub ids: Vec<PlaylistItemRef>,
    /// Sanitization policy.
    pub sanitize_names: SanitizeNamesConfig,
}

/// One flattened hierarchy entry with resolved path components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlattenedHierarchyEntry {
    /// Path components from root to this entry.
    pub path_components: Vec<String>,
    /// Optional stable hierarchy id.
    pub hierarchy_id: Option<String>,
    /// Runtime entry payload.
    pub entry: HierarchyEntry,
}

impl FlattenedHierarchyEntry {
    /// Joins path components into one relative path string.
    #[must_use]
    pub fn path_str(&self) -> String {
        self.path_components.join("/")
    }
}

// ---------------------------------------------------------------------------
// HierarchyPath (serde-aware path type)
// ---------------------------------------------------------------------------

/// A path composed of one or more components (path segments).
///
/// Serialization rules:
/// - Empty component list serializes as `""`
/// - Single component serializes as `"component"`
/// - Multiple components serialize as `["component1", "component2"]`
///
/// Deserialization accepts both bare string and array forms:
/// - `""` → zero components
/// - `"abc"` → one component (NOT split by `/`)
/// - `["a", "b"]` → two components
///
/// `From<&str>` splits by `/` for Rust convenience but serde does NOT split.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HierarchyPath(Vec<String>);

impl HierarchyPath {
    /// Creates a path from a single literal component.
    #[must_use]
    pub fn simple(component: &str) -> Self {
        Self(vec![component.to_string()])
    }

    /// Creates a path from a single template component
    /// (mustache-format placeholders like `{{title}}`).
    ///
    /// The internal representation is the same as [`simple`](Self::simple) —
    /// the template/literal distinction is semantic only.
    #[must_use]
    pub fn template(component: &str) -> Self {
        Self(vec![component.to_string()])
    }

    /// Returns an immutable reference to the component list.
    #[must_use]
    pub fn components(&self) -> &[String] {
        &self.0
    }

    /// Returns the number of path components.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if there are zero components.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Joins all components with `/` separator.
    #[must_use]
    pub fn join_path(&self) -> String {
        self.0.join("/")
    }
}

impl From<&str> for HierarchyPath {
    fn from(value: &str) -> Self {
        let trimmed = value.trim_matches('/');
        if trimmed.is_empty() {
            return Self(Vec::new());
        }
        Self(trimmed.split('/').map(String::from).collect())
    }
}

impl Serialize for HierarchyPath {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0.len() {
            0 => serializer.serialize_str(""),
            1 => serializer.serialize_str(&self.0[0]),
            _ => self.0.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for HierarchyPath {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        match value {
            Value::String(text) => {
                if text.is_empty() {
                    Ok(Self(Vec::new()))
                } else {
                    Ok(Self(vec![text]))
                }
            }
            Value::Array(items) => {
                let components: Result<Vec<String>, _> = items
                    .into_iter()
                    .map(|item| {
                        if let Value::String(component) = item {
                            Ok(component)
                        } else {
                            Err(serde::de::Error::custom(
                                "hierarchy path array elements must be strings",
                            ))
                        }
                    })
                    .collect();
                Ok(Self(components?))
            }
            _ => {
                Err(serde::de::Error::custom("hierarchy path must be a string or array of strings"))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Custom serde helpers for hierarchy fields
// ---------------------------------------------------------------------------

/// Deserializes hierarchy field values using array-of-nodes semantics.
#[allow(dead_code)]
pub fn deserialize_hierarchy_node_list<'de, D>(
    deserializer: D,
) -> Result<Vec<HierarchyNode>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    flatten_hierarchy_value(value).map_err(serde::de::Error::custom)
}

/// Serializes hierarchy field values into array-of-nodes representation.
#[allow(dead_code)]
pub fn serialize_hierarchy_node_list<S>(
    hierarchy: &[HierarchyNode],
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let encoded = nest_hierarchy_value(hierarchy).map_err(serde::ser::Error::custom)?;
    encoded.serialize(serializer)
}

// ---------------------------------------------------------------------------
// Wire types for variant selector serde
// ---------------------------------------------------------------------------

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

/// Prefix used for internal regex variant selector encoding.
const REGEX_VARIANT_SELECTOR_PREFIX: &str = "__mediapm_regex__:";

/// Encodes one regex selector as internal tagged string form.
#[must_use]
fn encode_regex_variant_selector(pattern: &str) -> String {
    format!("{REGEX_VARIANT_SELECTOR_PREFIX}{pattern}")
}

/// Returns regex pattern when one selector uses internal regex-tag form.
#[must_use]
pub fn decode_regex_variant_selector_pattern(selector: &str) -> Option<&str> {
    selector.strip_prefix(REGEX_VARIANT_SELECTOR_PREFIX)
}

/// Public helper for constructing regex selector values in Rust-authored docs.
#[must_use]
pub fn regex_variant_selector(pattern: &str) -> String {
    encode_regex_variant_selector(pattern)
}

/// Deserializes selector arrays that accept literal strings or regex objects.
pub fn deserialize_variant_selector_list<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
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
pub fn serialize_variant_selector_list<S>(
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

// ---------------------------------------------------------------------------
// Expand variant selectors
// ---------------------------------------------------------------------------

/// Resolves selector entries against available variant names.
///
/// - literal selectors match exact variant names;
/// - regex selectors match any variant names whose full text matches;
/// - when a selector resolves nothing and a `default` variant exists,
///   falls back to `default`.
///
/// Returned variants are deduplicated preserving first-seen order.
pub fn expand_variant_selectors(
    selectors: &[String],
    available_variants: &BTreeSet<String>,
) -> Result<Vec<String>, String> {
    let mut resolved = Vec::new();
    let mut seen = BTreeSet::new();

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

use std::collections::BTreeSet;

// ---------------------------------------------------------------------------
// Flatten/nest hierarchy
// ---------------------------------------------------------------------------

/// Decodes one hierarchy JSON value into ordered node declarations.
///
/// The schema is strict: `hierarchy` must be an array of node objects.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] if the value is not a JSON array or if
/// decoding fails.
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
/// Returns [`MediaPmError::Workflow`] if serialization fails.
pub fn nest_hierarchy_value(hierarchy: &[HierarchyNode]) -> Result<Value, MediaPmError> {
    serde_json::to_value(hierarchy)
        .map_err(|error| MediaPmError::Workflow(format!("hierarchy encode failed: {error}")))
}

/// Flattens hierarchy nodes into runtime entries with resolved paths.
pub fn flatten_hierarchy_nodes_for_runtime(
    hierarchy: &[HierarchyNode],
) -> Result<Vec<FlattenedHierarchyEntry>, MediaPmError> {
    let mut flattened = Vec::new();
    flatten_hierarchy_nodes_inner(
        hierarchy,
        &[],
        None,
        &SanitizeNamesConfig::Enabled,
        &mut flattened,
    )
    .map_err(MediaPmError::Workflow)?;

    let mut seen_paths = BTreeMap::<(String, String), Vec<usize>>::new();
    let mut seen_hierarchy_ids = BTreeMap::<String, String>::new();
    for (index, entry) in flattened.iter().enumerate() {
        let path_key = (entry.path_str(), entry.entry.media_id.clone());
        seen_paths.entry(path_key.clone()).or_default().push(index);

        if seen_paths[&path_key].len() > 1 {
            let current_variants = entry.entry.variants.iter().collect::<BTreeSet<_>>();
            let previous_index = seen_paths[&path_key][seen_paths[&path_key].len() - 2];
            let previous_variants =
                flattened[previous_index].entry.variants.iter().collect::<BTreeSet<_>>();

            if current_variants.is_empty() && previous_variants.is_empty() {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy flattening produced duplicate path '{}' with no differentiating variants (entries #{previous_index} and #{index})",
                    entry.path_str()
                )));
            }

            let overlap: Vec<_> =
                current_variants.intersection(&previous_variants).copied().collect();
            if !overlap.is_empty() {
                let current_rename = &entry.entry.rename_files;
                let previous_rename = &flattened[previous_index].entry.rename_files;
                if current_rename == previous_rename {
                    return Err(MediaPmError::Workflow(format!(
                        "hierarchy flattening produced duplicate path '{}' with overlapping variants {:?} and identical rename_files (entries #{previous_index} and #{index})",
                        entry.path_str(),
                        overlap
                    )));
                }
            }
        }

        if let Some(hierarchy_id) = entry.hierarchy_id.as_deref()
            && let Some(previous_path) =
                seen_hierarchy_ids.insert(hierarchy_id.to_string(), entry.path_str())
        {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy id '{hierarchy_id}' is duplicated by paths '{previous_path}' and '{}'",
                entry.path_str()
            )));
        }
    }

    Ok(flattened)
}

/// Recursive helper for hierarchy flattening.
fn flatten_hierarchy_nodes_inner(
    nodes: &[HierarchyNode],
    parent_path: &[String],
    parent_sanitize: Option<&SanitizeNamesConfig>,
    default_sanitize: &SanitizeNamesConfig,
    output: &mut Vec<FlattenedHierarchyEntry>,
) -> Result<(), String> {
    for node in nodes {
        let effective_sanitize = match &node.sanitize_names {
            SanitizeNamesConfig::Inherit => parent_sanitize.unwrap_or(default_sanitize),
            other => other,
        };

        let resolved_components = {
            let mut components = parent_path.to_vec();
            for component in node.path.components() {
                // Validate each path component.
                validate_hierarchy_path_component(component)?;
                components.push(component.clone());
            }
            components
        };

        match node.kind {
            HierarchyNodeKind::Folder => {
                flatten_hierarchy_nodes_inner(
                    &node.children,
                    &resolved_components,
                    Some(effective_sanitize),
                    default_sanitize,
                    output,
                )?;
            }
            HierarchyNodeKind::Media => {
                let media_id = node
                    .media_id
                    .clone()
                    .ok_or_else(|| "media node must define media_id".to_string())?;
                let variant = node
                    .variant
                    .clone()
                    .ok_or_else(|| "media node must define variant".to_string())?;

                output.push(FlattenedHierarchyEntry {
                    path_components: resolved_components.clone(),
                    hierarchy_id: node.id.clone(),
                    entry: HierarchyEntry {
                        kind: HierarchyEntryKind::Media,
                        media_id,
                        variants: vec![variant],
                        rename_files: Vec::new(),
                        format: PlaylistFormat::M3u8,
                        ids: Vec::new(),
                        sanitize_names: effective_sanitize.clone(),
                    },
                });
            }
            HierarchyNodeKind::MediaFolder => {
                let media_id = node
                    .media_id
                    .clone()
                    .ok_or_else(|| "media_folder node must define media_id".to_string())?;

                output.push(FlattenedHierarchyEntry {
                    path_components: resolved_components.clone(),
                    hierarchy_id: node.id.clone(),
                    entry: HierarchyEntry {
                        kind: HierarchyEntryKind::MediaFolder,
                        media_id,
                        variants: node.variants.clone(),
                        rename_files: node.rename_files.clone(),
                        format: PlaylistFormat::M3u8,
                        ids: Vec::new(),
                        sanitize_names: effective_sanitize.clone(),
                    },
                });
            }
            HierarchyNodeKind::Playlist => {
                output.push(FlattenedHierarchyEntry {
                    path_components: resolved_components.clone(),
                    hierarchy_id: node.id.clone(),
                    entry: HierarchyEntry {
                        kind: HierarchyEntryKind::Playlist,
                        media_id: String::new(),
                        variants: Vec::new(),
                        rename_files: Vec::new(),
                        format: node.format,
                        ids: node.ids.clone(),
                        sanitize_names: effective_sanitize.clone(),
                    },
                });
            }
        }

        // Recurse into children for non-folder kinds too (playlists may have children).
        if !matches!(node.kind, HierarchyNodeKind::Folder) && !node.children.is_empty() {
            flatten_hierarchy_nodes_inner(
                &node.children,
                &resolved_components,
                Some(effective_sanitize),
                default_sanitize,
                output,
            )?;
        }
    }

    Ok(())
}

/// Validates one hierarchy path component for disallowed characters.
fn validate_hierarchy_path_component(component: &str) -> Result<(), String> {
    if component.is_empty() {
        return Err("hierarchy path components must be non-empty".to_string());
    }

    for ch in component.chars() {
        match ch {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => {
                return Err(format!(
                    "hierarchy path component '{component}' contains reserved character '{ch}'"
                ));
            }
            _ => {}
        }
    }

    Ok(())
}

/// Collects effective hierarchy-id → media-path mappings from a flattened
/// hierarchy.
pub fn collect_playlist_media_index(
    flattened_hierarchy: &[FlattenedHierarchyEntry],
) -> Result<BTreeMap<String, Vec<String>>, MediaPmError> {
    let mut index = BTreeMap::new();

    for flattened_entry in flattened_hierarchy {
        if !matches!(flattened_entry.entry.kind, HierarchyEntryKind::Media) {
            continue;
        }

        let Some(hierarchy_id) = flattened_entry.hierarchy_id.as_deref() else {
            continue;
        };

        if let Some(previous_path_components) =
            index.insert(hierarchy_id.to_string(), flattened_entry.path_components.clone())
            && previous_path_components != flattened_entry.path_components
        {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy id '{}' resolves to multiple media paths ('{}' and '{}')",
                hierarchy_id,
                previous_path_components.join("/"),
                flattened_entry.path_str()
            )));
        }
    }

    Ok(index)
}
