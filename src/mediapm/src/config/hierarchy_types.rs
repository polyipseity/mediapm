//! Hierarchy node, entry, and path types for mediapm configuration.
//!
//! These types model the `hierarchy` declarations in `mediapm.ncl` plus the
//! flatten/nest utilities and sanitization config.

use std::collections::BTreeMap;

use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use unicode_normalization::UnicodeNormalization;

use crate::MediaPmDocument;
use crate::error::MediaPmError;
use crate::path_component::{PathComponent, is_rejected_char, render_relative_path};

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

/// One regex rename rule for hierarchy folder members.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HierarchyFolderRenameRule {
    /// Regex pattern matched against filenames (full match).
    pub pattern: String,
    /// Replacement template string.
    pub replacement: String,
}

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
    ///
    /// Optional at the boundary: `None` resolves to `SanitizeNamesConfig::Inherit`.
    #[serde(default)]
    pub sanitize_names: Option<SanitizeNamesConfig>,
    /// Recursive children.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<HierarchyNode>,
}

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

/// One flattened hierarchy entry and its path components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlattenedHierarchyEntry {
    /// Path components from root to this entry, as text.
    ///
    /// These are the components the user declared, `${...}` placeholders and
    /// all, or the same components after metadata interpolation pulled tag
    /// values and ffprobe output into them. Nothing here has met
    /// [`crate::path_component::PathComponent::parse`], and the field type
    /// says so: it is `Vec<String>`, and no crate-local conversion turns a
    /// `String` here into a `PathComponent`. A read site that needs a safe path
    /// holds a `ValidatedHierarchyEntry` instead.
    pub(crate) path_components: Vec<String>,
    /// Optional stable hierarchy id.
    pub hierarchy_id: Option<String>,
    /// Runtime entry payload.
    pub entry: HierarchyEntry,
}

impl FlattenedHierarchyEntry {
    /// Joins the entry's components into one `/`-separated string.
    ///
    /// This is config-boundary text, and the flatten walk is the only place
    /// that wants it: it compares one entry's components against another's to
    /// report two nodes that declared the same path, and the metadata
    /// resolver names a path in the error it raises for a placeholder on an
    /// entry that binds no media. `pub(crate)` because nobody outside this
    /// crate can make that text safe, and the materializer is the only stage
    /// that can.
    #[must_use]
    pub(crate) fn path_str(&self) -> String {
        self.path_components.join("/")
    }
}

/// One flattened hierarchy entry whose path components have met the parser.
///
/// This is what every read site holds. The materializer's sanitize step
/// produces it and nothing else does. [`FlattenedHierarchyEntry`] carries
/// `Vec<String>`, so a read site typed on [`ValidatedHierarchyEntry`] cannot be
/// handed raw text even by accident, and the raw-text type never gets past the
/// sanitizer to reach a `PathBuf::join`. Neither half depends on a reader
/// remembering to check anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedHierarchyEntry {
    /// Validated components, root first.
    pub(crate) path: Vec<PathComponent>,
    /// Optional stable hierarchy id, carried through for playlist resolution.
    pub(crate) hierarchy_id: Option<String>,
    /// Runtime entry payload, unchanged from the flattened entry.
    pub(crate) entry: HierarchyEntry,
}

impl ValidatedHierarchyEntry {
    /// Builds a validated entry from components that met the parser.
    ///
    /// `pub(crate)` because the sanitizer lives in `materializer`. The
    /// argument type is the real constraint: a caller cannot pass a `String`
    /// here, and `PathComponent` has no constructor outside
    /// [`crate::path_component::PathComponent::parse`] either, so the only way
    /// to obtain one of these is to have parsed the text first.
    pub(crate) fn new(
        path: Vec<PathComponent>,
        hierarchy_id: Option<String>,
        entry: HierarchyEntry,
    ) -> Self {
        Self { path, hierarchy_id, entry }
    }

    /// Renders the validated path as one `/`-separated relative string.
    ///
    /// A thin wrapper over [`render_relative_path`], kept as a method so a
    /// read site has one spelling for "this entry's path as text". The
    /// materializer still builds the path it writes to disk from the
    /// components themselves, so nothing downstream has to trust the
    /// rendering.
    #[must_use]
    pub(crate) fn relative_path_text(&self) -> String {
        render_relative_path(&self.path)
    }
}

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
///
/// # Errors
///
/// Returns a deserialization error when an entry is neither a literal string
/// nor a well-formed regex-selector object.
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
///
/// # Errors
///
/// Returns a serialization error when the encoded selector array cannot be
/// written to `serializer`.
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

/// Resolves selector entries against available variant names.
///
/// - literal selectors match exact variant names;
/// - regex selectors match any variant names whose full text matches;
/// - when a selector resolves nothing and a `default` variant exists,
///   falls back to `default`.
///
/// Returned variants are deduplicated preserving first-seen order.
///
/// # Errors
///
/// Returns a message naming the offending selector when a selector is empty
/// after trimming, when a regex selector's pattern does not compile, when a
/// regex selector matches no available variant and no `default` variant exists,
/// or when a literal selector names no available variant and no `default`
/// variant exists.
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
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] wrapping the first structural failure
/// reported while flattening, which covers path, id, and media-reference
/// validation.
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
    )?;

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
) -> Result<(), MediaPmError> {
    for node in nodes {
        let effective_sanitize = match &node.sanitize_names {
            None | Some(SanitizeNamesConfig::Inherit) => {
                parent_sanitize.unwrap_or(default_sanitize)
            }
            Some(other) => other,
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
                let media_id = node.media_id.clone().ok_or_else(|| {
                    MediaPmError::Workflow("media node must define media_id".into())
                })?;
                validate_media_id(&media_id)?;
                let variant = node.variant.clone().ok_or_else(|| {
                    MediaPmError::Workflow("media node must define variant".into())
                })?;

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
                let media_id = node.media_id.clone().ok_or_else(|| {
                    MediaPmError::Workflow("media_folder node must define media_id".into())
                })?;
                validate_media_id(&media_id)?;

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

/// Rejects hierarchy path components that are not Unicode NFD-normalized.
///
/// This is the config-level NFD check, and the first of the two stages in the
/// NFD contract. It runs on the components exactly as the user declared them
/// in `mediapm.ncl`, before any `${...}` template placeholder is resolved, and
/// it *rejects* rather than normalizes: a declared component is user-authored,
/// so the user can spell it in NFD and fix the document. The materializer's
/// post-resolution stage
/// (`crate::materializer::sanitize_and_validate_hierarchy_paths`) instead
/// *normalizes*, because by then the components also carry tag metadata,
/// ffprobe output, and upstream values the user does not control.
///
/// Rejection is reported per component with a message distinct from the
/// reserved-character rejection in [`validate_hierarchy_path_component`], so a
/// user can tell the two causes apart.
pub(crate) fn check_nfd_source(components: &[&str]) -> Result<(), MediaPmError> {
    for component in components {
        if component.nfd().collect::<String>() != *component {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path component '{component}' must be NFD-normalized"
            )));
        }
    }
    Ok(())
}

/// Validates one user-authored media id at the config boundary.
///
/// A `media` map key is a single identity with three consumers: it is
/// interpolated into `${media.id}` path templates, it keys
/// `state.workflow_states`, and it is carried in every `ManagedFileRecord`.
/// A shape that one of those consumers rewrites is therefore an identity split,
/// not a cosmetic problem — a key `a/b` reaches disk as `a_b` while the state
/// records `a/b`, and no later join between the two finds the file.
///
/// The rules below are exactly the shapes that break that identity:
///
/// - **empty** — names no source to join against.
/// - **control character** — reaches log lines unescaped and has no filename
///   spelling on any platform.
/// - **leading or trailing whitespace** — one lookup trims the id before
///   joining it against the `media` map (`materializer::metadata`) and another
///   does not (`materializer::resolve`), so a padded id resolves on one path
///   and not the other.
/// - **path separator** — re-splits into extra path components on the way to
///   disk while the state keeps the unsplit spelling.
/// - **reserved filename character** — rewritten to `_` by the sanitizer on
///   every platform, so the on-disk spelling is never the spelling the state
///   keeps. The set is [`crate::path_component::is_rejected_char`], the
///   same predicate the sanitizer uses, so the rule and the rewrite cannot
///   drift apart.
/// - **`.` or `..`** — the traversal components, which
///   [`validate_hierarchy_path_component`] does not reject on its own.
///
/// Rejection, not sanitization, is the correct policy here for the same reason
/// [`check_nfd_source`] rejects: the key is user-authored, so the user can
/// spell it correctly. The materializer's
/// [`crate::materializer::sanitize_and_validate_hierarchy_paths`] still
/// *normalizes* the components that carry metadata the user does not control —
/// the two stages are complementary, and this is the earlier one.
///
/// The reserved-character rule is **platform-independent**, matching the
/// sanitizer it must stay in step with. A `media` key of `a:b` is a legal
/// filename on Linux and macOS and is still refused everywhere, because the
/// sanitizer rewrites it to `a_b` on every platform and a rule that only
/// fired on Windows would let the library be written under two spellings by
/// the same config. The cost is that an id legal on the developer's platform
/// must be respelled; the alternative costs a split library.
///
/// The id is rendered with [`str::escape_debug`] in every message, so a
/// control character is reported as a code point instead of being written into
/// the log line that carries the diagnostic.
///
/// # Errors
///
/// Returns a distinct [`MediaPmError::Workflow`] per rule, naming the offending
/// id and the rule it violates.
pub(crate) fn validate_media_id(media_id: &str) -> Result<(), MediaPmError> {
    if media_id.is_empty() {
        return Err(MediaPmError::Workflow("media id must be non-empty".to_string()));
    }

    let escaped = media_id.escape_debug().to_string();

    for ch in media_id.chars() {
        if ch.is_control() {
            return Err(MediaPmError::Workflow(format!(
                "media id '{escaped}' contains the control character U+{:04X}",
                u32::from(ch)
            )));
        }
        if matches!(ch, '/' | '\\') {
            return Err(MediaPmError::Workflow(format!(
                "media id '{escaped}' contains the path separator '{ch}'"
            )));
        }
        // `is_rejected_char` also covers `/` and `\`, but the separator rule
        // above runs first in the same loop, so a separator reports the
        // separator rule and only the remaining characters can land here.
        if is_rejected_char(ch) {
            return Err(MediaPmError::Workflow(format!(
                "media id '{escaped}' contains the reserved character '{ch}'"
            )));
        }
    }

    if media_id.trim() != media_id {
        return Err(MediaPmError::Workflow(format!(
            "media id '{escaped}' has leading or trailing whitespace"
        )));
    }

    if media_id == "." || media_id == ".." {
        return Err(MediaPmError::Workflow(format!(
            "media id '{escaped}' must not be '.' or '..'"
        )));
    }

    Ok(())
}

/// Validates **every** key of a document's `media` map.
///
/// [`validate_media_id`] on its own is not an all-keys boundary: the hierarchy
/// walk in [`flatten_hierarchy_nodes_inner`] sees a key only once a node binds
/// it, so a `media` entry no hierarchy node references never passed through
/// it. Such a key is still real — the workflow synthesizer turns *every*
/// `document.media` key into a `media/{id}` workflow, so its spelling reaches
/// the conductor document and the log lines around it without ever having
/// been checked. This is the boundary that closes that hole, and it runs from
/// the same production path that consumes the keys.
///
/// # Errors
///
/// Returns the first [`validate_media_id`] failure, in the map's sorted key
/// order so the diagnostic is deterministic for a given document.
pub(crate) fn validate_media_ids(document: &MediaPmDocument) -> Result<(), MediaPmError> {
    for media_id in document.media.keys() {
        validate_media_id(media_id)?;
    }
    Ok(())
}

/// Validates one hierarchy path component for disallowed characters and
/// Unicode normalization form.
///
/// The reserved-character set is
/// [`crate::path_component::is_rejected_char`], the same predicate the
/// sanitizer rewrites from and the same one [`validate_media_id`] refuses on.
/// Restating the characters here is what let the config-level rule and the
/// sanitizer drift apart, so the predicate is called and only the message is
/// local.
///
/// # Errors
///
/// Returns a distinct message per cause: `reserved character '<ch>'` for a
/// cross-platform-illegal character, and the [`check_nfd_source`] message for a
/// component that is not NFD-normalized.
fn validate_hierarchy_path_component(component: &str) -> Result<(), MediaPmError> {
    if component.is_empty() {
        return Err(MediaPmError::Workflow(
            "hierarchy path components must be non-empty".to_string(),
        ));
    }

    for ch in component.chars() {
        if is_rejected_char(ch) {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path component '{component}' contains reserved character '{ch}'"
            )));
        }
    }

    check_nfd_source(&[component])
}

/// Collects effective hierarchy-id → media-path mappings from a flattened
/// hierarchy.
///
/// The value keeps the [`PathComponent`]s rather than a rendered string, so a
/// caller that has already sanitized its entries receives the same validated
/// state the entry carries and a playlist body cannot reintroduce a separator
/// the components do not have.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] when one hierarchy id resolves to two
/// different media paths.
pub(crate) fn collect_playlist_media_index(
    flattened_hierarchy: &[ValidatedHierarchyEntry],
) -> Result<BTreeMap<String, Vec<PathComponent>>, MediaPmError> {
    let mut index = BTreeMap::new();

    for flattened_entry in flattened_hierarchy {
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
                "hierarchy id '{}' resolves to multiple media paths ('{}' and '{}')",
                hierarchy_id,
                render_relative_path(&previous_path),
                flattened_entry.relative_path_text()
            )));
        }
    }

    Ok(index)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Splits a `/`-separated path into the declared component text a flattened
    /// entry carries before the materializer parses it.
    fn template_components(path: &str) -> Vec<String> {
        path.split('/').map(String::from).collect()
    }

    /// Parses a `/`-separated path into the components a validated entry
    /// carries. The reject-everything policy keeps the comparison about the
    /// path's own spelling, with no rewriting in the way.
    fn resolved_components(path: &str) -> Vec<PathComponent> {
        path.split('/')
            .map(|component| {
                PathComponent::parse(component, &crate::path_component::SanitizePolicy::disabled())
            })
            .collect::<Result<Vec<PathComponent>, MediaPmError>>()
            .expect("the component under test must be a legal one")
    }

    fn base_node() -> HierarchyNode {
        HierarchyNode {
            path: HierarchyPath::default(),
            kind: HierarchyNodeKind::Folder,
            id: None,
            media_id: None,
            variant: None,
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: PlaylistFormat::M3u8,
            ids: Vec::new(),
            sanitize_names: Some(SanitizeNamesConfig::Inherit),
            children: Vec::new(),
        }
    }

    #[test]
    fn flatten_empty_hierarchy_returns_empty_vec() {
        let result = flatten_hierarchy_nodes_for_runtime(&[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn flatten_single_media_node() {
        let nodes = vec![HierarchyNode {
            kind: HierarchyNodeKind::Media,
            path: HierarchyPath::simple("video"),
            media_id: Some("vid1".into()),
            variant: Some("1080p".into()),
            ..base_node()
        }];
        let result = flatten_hierarchy_nodes_for_runtime(&nodes).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].path_components, template_components("video"));
        assert_eq!(result[0].entry.media_id, "vid1");
        assert_eq!(result[0].entry.variants, vec!["1080p".to_string()]);
        assert_eq!(result[0].entry.kind, HierarchyEntryKind::Media);
    }

    #[test]
    fn flatten_single_media_folder_node() {
        let nodes = vec![HierarchyNode {
            kind: HierarchyNodeKind::MediaFolder,
            path: HierarchyPath::simple("folder"),
            media_id: Some("mf1".into()),
            variants: vec!["v1".into(), "v2".into()],
            ..base_node()
        }];
        let result = flatten_hierarchy_nodes_for_runtime(&nodes).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].path_components, template_components("folder"));
        assert_eq!(result[0].entry.media_id, "mf1");
        assert_eq!(result[0].entry.variants, vec!["v1".to_string(), "v2".to_string()]);
        assert_eq!(result[0].entry.kind, HierarchyEntryKind::MediaFolder);
    }

    #[test]
    fn flatten_folder_with_children() {
        let child1 = HierarchyNode {
            kind: HierarchyNodeKind::Media,
            path: HierarchyPath::simple("ep1"),
            media_id: Some("ep1".into()),
            variant: Some("hq".into()),
            ..base_node()
        };
        let child2 = HierarchyNode {
            kind: HierarchyNodeKind::Media,
            path: HierarchyPath::simple("ep2"),
            media_id: Some("ep2".into()),
            variant: Some("hq".into()),
            ..base_node()
        };
        let nodes = vec![HierarchyNode {
            path: HierarchyPath::simple("series"),
            kind: HierarchyNodeKind::Folder,
            children: vec![child1, child2],
            ..base_node()
        }];
        let result = flatten_hierarchy_nodes_for_runtime(&nodes).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].path_components, template_components("series/ep1"));
        assert_eq!(result[1].path_components, template_components("series/ep2"));
    }

    #[test]
    fn flatten_nested_folder_media() {
        let media = HierarchyNode {
            kind: HierarchyNodeKind::Media,
            path: HierarchyPath::simple("c"),
            media_id: Some("c1".into()),
            variant: Some("hq".into()),
            ..base_node()
        };
        let inner = HierarchyNode {
            path: HierarchyPath::simple("b"),
            kind: HierarchyNodeKind::Folder,
            children: vec![media],
            ..base_node()
        };
        let nodes = vec![HierarchyNode {
            path: HierarchyPath::simple("a"),
            kind: HierarchyNodeKind::Folder,
            children: vec![inner],
            ..base_node()
        }];
        let result = flatten_hierarchy_nodes_for_runtime(&nodes).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].path_components, template_components("a/b/c"));
    }

    #[test]
    fn flatten_playlist_node() {
        let nodes = vec![HierarchyNode {
            kind: HierarchyNodeKind::Playlist,
            path: HierarchyPath::simple("playlist1"),
            ..base_node()
        }];
        let result = flatten_hierarchy_nodes_for_runtime(&nodes).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].entry.kind, HierarchyEntryKind::Playlist);
        assert_eq!(result[0].entry.media_id, "");
    }

    #[test]
    fn flatten_media_without_media_id_errors() {
        let nodes = vec![HierarchyNode {
            kind: HierarchyNodeKind::Media,
            path: HierarchyPath::simple("orphan"),
            media_id: None,
            variant: Some("hq".into()),
            ..base_node()
        }];
        let err = flatten_hierarchy_nodes_for_runtime(&nodes).unwrap_err();
        assert!(err.to_string().contains("media node must define media_id"));
    }

    /// A config-declared component in NFC is rejected: the hierarchy
    /// declaration is user-authored, so the user can spell it in NFD and
    /// fix the document. The rejection must name the NFD cause, not the
    /// reserved-character cause.
    #[test]
    fn flatten_rejects_nfc_config_path_component() {
        // "Café" in NFC (\u{00e9}) is not NFD-normalized; its NFD form is
        // "Cafe\u{301}" (e + combining acute accent).
        let nodes = vec![HierarchyNode {
            kind: HierarchyNodeKind::Media,
            path: HierarchyPath::simple("Caf\u{00e9}"),
            media_id: Some("vid1".into()),
            variant: Some("hq".into()),
            ..base_node()
        }];
        let err = flatten_hierarchy_nodes_for_runtime(&nodes).unwrap_err();
        assert!(
            err.to_string().contains("must be NFD-normalized"),
            "unexpected rejection reason: {err}"
        );
    }

    /// The same component spelled in NFD is accepted, so the config-level
    /// check is a normalization requirement and not a blanket non-ASCII ban.
    #[test]
    fn flatten_accepts_nfd_config_path_component() {
        let nodes = vec![HierarchyNode {
            kind: HierarchyNodeKind::Media,
            path: HierarchyPath::simple("Cafe\u{301}"),
            media_id: Some("vid1".into()),
            variant: Some("hq".into()),
            ..base_node()
        }];
        let result = flatten_hierarchy_nodes_for_runtime(&nodes).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].path_components, template_components("Cafe\u{301}"));
    }

    /// The two config-level rejection causes stay distinguishable: a reserved
    /// character and a non-NFD spelling of the same component must not report
    /// the same message, so a user can tell which rule their config broke.
    #[test]
    fn config_component_rejection_messages_are_distinguishable() {
        let node = |path: &str| HierarchyNode {
            kind: HierarchyNodeKind::Media,
            path: HierarchyPath::simple(path),
            media_id: Some("vid1".into()),
            variant: Some("hq".into()),
            ..base_node()
        };

        let reserved = flatten_hierarchy_nodes_for_runtime(&[node("Caf\u{00e9}<")])
            .expect_err("reserved character must be rejected")
            .to_string();
        assert!(reserved.contains("reserved character '<'"), "unexpected message: {reserved}");
        assert!(
            !reserved.contains("NFD-normalized"),
            "reserved-character rejection must not claim an NFD cause: {reserved}"
        );

        let nfd = flatten_hierarchy_nodes_for_runtime(&[node("Caf\u{00e9}")])
            .expect_err("NFC spelling must be rejected")
            .to_string();
        assert!(nfd.contains("must be NFD-normalized"), "unexpected message: {nfd}");
        assert!(
            !nfd.contains("reserved character"),
            "NFD rejection must not claim a reserved-character cause: {nfd}"
        );
    }

    /// A component already spelled in NFD passes the config-level check.
    #[test]
    fn check_nfd_source_passes_nfd() {
        // "e\u{301}" is NFD-normalized (e + combining acute accent).
        assert!(check_nfd_source(&["e\u{301}normal"]).is_ok());
    }

    /// A component spelled in NFC is rejected with the NFD-specific message.
    #[test]
    fn check_nfd_source_rejects_nfc() {
        let err = check_nfd_source(&["caf\u{e9}"]).unwrap_err();
        assert!(err.to_string().contains("must be NFD-normalized"));
    }

    /// Characters probed by the reserved-set agreement tests.
    ///
    /// The whole ASCII range is swept so the agreement is checked against
    /// near misses (`#`, `%`, `!`, `+`, …) and not only against the nine
    /// characters the set actually contains — a second copy that *grew* is the
    /// drift this test exists to catch. The non-ASCII entries confirm the
    /// agreement survives components the NFD check rejects for an unrelated
    /// reason, so those characters must read as "not reserved" on both sides.
    const RESERVED_SET_PROBE: &[char] = &[
        '\0',
        '\t',
        '!',
        '#',
        '%',
        '&',
        '\'',
        '(',
        ')',
        '*',
        '+',
        ',',
        '-',
        '.',
        '/',
        '0',
        '9',
        ':',
        ';',
        '<',
        '=',
        '>',
        '?',
        '@',
        'A',
        'Z',
        '[',
        '\\',
        ']',
        '^',
        '_',
        '`',
        'a',
        'q',
        '{',
        '|',
        '}',
        '~',
        '\u{7F}',
        '\u{a0}',
        '\u{e9}',
        '\u{301}',
        '\u{2192}',
        '\u{1d11e}',
    ];

    /// The config-level component rule and the materializer's reserved-set
    /// predicate decide the same question for the same character.
    ///
    /// Both spellings of the rule refuse a component carrying a character that
    /// the sanitizer would rewrite to `_`: the materializer at
    /// `PathComponent::parse`, and the config boundary here. When they were two
    /// literals, adding a character to one and not the other let a config
    /// declare a path that the same sync then rewrote to a different spelling
    /// — the same identity split the media-id rule refuses. Sweeping the probe
    /// set both ways is what makes the test a drift *detector* rather than a
    /// restatement of today's set: a character added to either side alone
    /// breaks the equality.
    #[test]
    fn config_component_reserved_set_agrees_with_is_rejected_char() {
        for &ch in RESERVED_SET_PROBE {
            let component = format!("a{ch}b");
            let rejected_as_reserved = validate_hierarchy_path_component(&component)
                .is_err_and(|err| err.to_string().contains("reserved character"));
            assert_eq!(
                rejected_as_reserved,
                is_rejected_char(ch),
                "config validator and is_rejected_char disagree on {ch:?} (U+{:04X}); \
                 component {component:?} was{} rejected as reserved",
                u32::from(ch),
                if rejected_as_reserved { "" } else { " not" },
            );
        }
    }

    /// The nine characters the reserved set is documented to contain are
    /// refused by the config boundary, and the shared predicate agrees.
    ///
    /// This is the direction the sweep cannot cover. The sweep only proves the
    /// two sides *agree*; if a character were dropped from both, they would
    /// still agree and the sweep would stay green. This test pins the
    /// documented membership from the outside, so a removal from either side
    /// fails here, and it also checks the rejection message names the character
    /// so a user can see which one broke the component.
    #[test]
    fn config_component_rejects_every_documented_reserved_character() {
        const DOCUMENTED: &[char] = &['<', '>', ':', '"', '|', '?', '*', '/', '\\'];
        for &ch in DOCUMENTED {
            assert!(
                is_rejected_char(ch),
                "is_rejected_char dropped the documented character {ch:?}"
            );
            let err = validate_hierarchy_path_component(&format!("a{ch}b"))
                .expect_err("a documented reserved character must be refused");
            assert!(
                err.to_string().contains(&format!("reserved character '{ch}'")),
                "the rejection must name the offending character; got: {err}"
            );
        }
    }

    /// Builds the minimal media node the media-id rules are exercised on.
    ///
    /// Only `media_id` varies; the path component is a plain, already-valid
    /// literal so a rejection can only come from the media id itself.
    fn media_node_with_id(media_id: &str) -> HierarchyNode {
        HierarchyNode {
            kind: HierarchyNodeKind::Media,
            path: HierarchyPath::simple("video"),
            media_id: Some(media_id.to_string()),
            variant: Some("hq".to_string()),
            ..base_node()
        }
    }

    /// Flattens a media node bound to `media_id` and returns the rejection
    /// message, failing the test when the boundary accepted the id.
    fn media_id_rejection(media_id: &str) -> String {
        flatten_hierarchy_nodes_for_runtime(&[media_node_with_id(media_id)])
            .map(|flattened| {
                panic!(
                    "media id '{media_id}' must be rejected at the config boundary, got {} entr(y/ies)",
                    flattened.len()
                )
            })
            .unwrap_err()
            .to_string()
    }

    /// Asserts the rejection names the offending id and the violated rule.
    fn assert_media_id_rejected(media_id: &str, rule: &str) {
        let message = media_id_rejection(media_id);
        assert!(
            message.contains(&media_id.escape_debug().to_string()),
            "the rejection must name the offending media id '{media_id}'; got: {message}"
        );
        assert!(
            message.contains(rule),
            "the rejection must state the violated rule '{rule}'; got: {message}"
        );
    }

    /// A media id carrying a path separator is rejected.
    ///
    /// The id is interpolated into `${media.id}` path templates, so a
    /// separator here is the one spelling that reaches a filesystem join while
    /// the state keys keep the unsplit form: the same media written to disk as
    /// `a_b` and recorded in `workflow_states` as `a/b`.
    #[test]
    fn strict_media_id_rejects_path_separator() {
        assert_media_id_rejected("a/b", "path separator");
    }

    /// The Windows separator form is rejected by the same rule as `/`.
    #[test]
    fn strict_media_id_rejects_backslash_separator() {
        assert_media_id_rejected("a\\b", "path separator");
    }

    /// A media id carrying a reserved filename character is rejected, under a
    /// message distinct from the path-separator one.
    ///
    /// `a:b` is a perfectly legal filename on Linux and on macOS, so nothing
    /// about the character itself forces a rejection. What forces it is that
    /// the sanitizer rewrites it: under `sanitize_names = Enabled` the id
    /// reaches disk as `a_b` while the state keeps `a:b`, which is the same
    /// identity split a separator causes, reached by a different character.
    /// The rule is therefore platform-independent, matching
    /// `crate::path_component::is_rejected_char` — see that
    /// predicate's rationale — and a media id legal on one platform is refused
    /// on all of them rather than splitting on the platforms that sanitize it.
    #[test]
    fn strict_media_id_rejects_reserved_character() {
        assert_media_id_rejected("a:b", "reserved character");
    }

    /// The reserved-character message is distinguishable from the
    /// path-separator message for the same offending character class.
    ///
    /// Both rules are checked in the same loop over the id's characters, so a
    /// user who typed one of them needs the diagnostic to say which one fired.
    #[test]
    fn strict_media_id_reserved_character_message_differs_from_separator_message() {
        let separator = media_id_rejection("a/b");
        let reserved = media_id_rejection("a:b");
        assert!(
            separator.contains("path separator") && !separator.contains("reserved character"),
            "the separator rejection must name only its own rule; got: {separator}"
        );
        assert!(
            reserved.contains("reserved character") && !reserved.contains("path separator"),
            "the reserved-character rejection must name only its own rule; got: {reserved}"
        );
    }

    /// A media id that *is* a path traversal component is rejected.
    #[test]
    fn strict_media_id_rejects_dotdot() {
        assert_media_id_rejected("..", "must not be '.' or '..'");
    }

    /// A media id that is the current-directory component is rejected too.
    #[test]
    fn strict_media_id_rejects_dot() {
        assert_media_id_rejected(".", "must not be '.' or '..'");
    }

    /// A media id carrying a control character is rejected, and the rejection
    /// reports the code point rather than embedding the raw character — the
    /// raw form would split the log line that carries the message.
    #[test]
    fn strict_media_id_rejects_control_character() {
        let message = media_id_rejection("a\nb");
        assert!(
            message.contains("control character U+000A"),
            "the rejection must name the control code point; got: {message}"
        );
        assert!(
            !message.contains('\n'),
            "the rejection must not embed the raw control character; got: {message:?}"
        );
    }

    /// A media id with leading whitespace is rejected.
    ///
    /// This is the rule that closes the trim asymmetry: one lookup trims the
    /// id before joining it against the `media` map and another does not, so a
    /// padded id resolves on one path and not the other.
    #[test]
    fn strict_media_id_rejects_leading_whitespace() {
        assert_media_id_rejected(" vid1", "leading or trailing whitespace");
    }

    /// A media id with trailing whitespace is rejected by the same rule.
    #[test]
    fn strict_media_id_rejects_trailing_whitespace() {
        assert_media_id_rejected("vid1 ", "leading or trailing whitespace");
    }

    /// An empty media id is rejected; it names no source to join against.
    #[test]
    fn strict_media_id_rejects_empty() {
        let message = media_id_rejection("");
        assert!(
            message.contains("media id must be non-empty"),
            "the empty-id rejection must state the rule; got: {message}"
        );
    }

    /// An ordinary media id is accepted, so the rule rejects only the shapes
    /// that break identity and not the ids the tree already uses.
    #[test]
    fn media_id_accepts_plain_id() {
        let flattened = flatten_hierarchy_nodes_for_runtime(&[media_node_with_id("vid1")])
            .expect("a plain media id must stay accepted");
        assert_eq!(flattened[0].entry.media_id, "vid1");
    }

    /// A dotted media id stays accepted: the rule targets the traversal
    /// components `.` and `..` as whole ids, not any id containing a dot.
    #[test]
    fn media_id_accepts_dotted_id() {
        let flattened =
            flatten_hierarchy_nodes_for_runtime(&[media_node_with_id("youtube.dQw4w9WgXcQ")])
                .expect("the demo's dotted media id must stay accepted");
        assert_eq!(flattened[0].entry.media_id, "youtube.dQw4w9WgXcQ");
    }

    #[test]
    fn flatten_path_inheritance() {
        let child = HierarchyNode {
            kind: HierarchyNodeKind::Media,
            path: HierarchyPath::simple("c"),
            media_id: Some("c1".into()),
            variant: Some("hq".into()),
            ..base_node()
        };
        // HierarchyPath::from("a/b") splits by '/' → vec!["a", "b"]
        let nodes = vec![HierarchyNode {
            path: HierarchyPath::from("a/b"),
            kind: HierarchyNodeKind::Folder,
            children: vec![child],
            ..base_node()
        }];
        let result = flatten_hierarchy_nodes_for_runtime(&nodes).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].path_components, template_components("a/b/c"));
    }

    #[test]
    fn literal_selector_matches_exact_variant() {
        let selectors = vec!["1080p".to_string()];
        let available: BTreeSet<String> =
            ["1080p", "720p", "480p"].into_iter().map(String::from).collect();
        let result = expand_variant_selectors(&selectors, &available).unwrap();
        assert_eq!(result, vec!["1080p"]);
    }

    #[test]
    fn regex_selector_matches_multiple_variants() {
        let selectors = vec![regex_variant_selector(r"^\d+p$")];
        let available: BTreeSet<String> =
            ["1080p", "720p", "480p", "hls"].into_iter().map(String::from).collect();
        let result = expand_variant_selectors(&selectors, &available).unwrap();
        // BTreeSet sorts alphabetically: 1080p, 480p, 720p
        assert_eq!(result, vec!["1080p", "480p", "720p"]);
    }

    #[test]
    fn selector_falls_back_to_default_when_no_match() {
        let selectors = vec!["nonexistent".to_string()];
        let available: BTreeSet<String> =
            ["default", "1080p"].into_iter().map(String::from).collect();
        let result = expand_variant_selectors(&selectors, &available).unwrap();
        assert_eq!(result, vec!["default"]);
    }

    #[test]
    fn deduplicates_results() {
        // Literal "var_one" matches first; then regex "^var_" also matches
        // var_one (deduped) plus var_two and var_three.
        let selectors = vec!["var_one".to_string(), regex_variant_selector(r"^var_")];
        let available: BTreeSet<String> =
            ["var_one", "var_two", "var_three"].into_iter().map(String::from).collect();
        // BTreeSet iterates: var_one (seen), var_three (new), var_two (new)
        let result = expand_variant_selectors(&selectors, &available).unwrap();
        assert_eq!(result, vec!["var_one", "var_three", "var_two"]);
    }

    #[test]
    fn unknown_literal_selector_without_default_errors() {
        let selectors = vec!["bogus".to_string()];
        let available: BTreeSet<String> = ["1080p"].into_iter().map(String::from).collect();
        let err = expand_variant_selectors(&selectors, &available).unwrap_err();
        assert!(err.contains("references unknown variant selector 'bogus'"));
    }

    #[test]
    fn empty_selector_errors() {
        let selectors = vec![String::new()];
        let available: BTreeSet<String> = ["1080p"].into_iter().map(String::from).collect();
        let err = expand_variant_selectors(&selectors, &available).unwrap_err();
        assert!(err.contains("contains an empty variant selector"));
    }

    #[test]
    fn mixed_literal_and_regex_selectors() {
        let selectors = vec!["720p".to_string(), regex_variant_selector(r"^\d+p$")];
        let available: BTreeSet<String> =
            ["1080p", "720p", "480p", "hls"].into_iter().map(String::from).collect();
        let result = expand_variant_selectors(&selectors, &available).unwrap();
        // 720p literal matches first, then regex adds 1080p and 480p.
        assert_eq!(result, vec!["720p", "1080p", "480p"]);
    }

    #[test]
    fn selectors_maintain_first_seen_order() {
        let selectors = vec![regex_variant_selector(r"^(720p|1080p)$"), "720p".to_string()];
        let available: BTreeSet<String> = ["1080p", "720p"].into_iter().map(String::from).collect();
        let result = expand_variant_selectors(&selectors, &available).unwrap();
        // First-seen order: 1080p (BTreeSet alphabetical order), then 720p;
        // literal 720p is deduped.
        assert_eq!(result, vec!["1080p", "720p"]);
    }

    fn media_entry(path: &str, hierarchy_id: &str, media_id: &str) -> ValidatedHierarchyEntry {
        ValidatedHierarchyEntry {
            path: resolved_components(path),
            hierarchy_id: Some(hierarchy_id.to_string()),
            entry: HierarchyEntry {
                kind: HierarchyEntryKind::Media,
                media_id: media_id.to_string(),
                variants: vec!["hq".to_string()],
                rename_files: Vec::new(),
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
            },
        }
    }

    fn non_media_entry(path: &str) -> ValidatedHierarchyEntry {
        ValidatedHierarchyEntry {
            path: resolved_components(path),
            hierarchy_id: None,
            entry: HierarchyEntry {
                kind: HierarchyEntryKind::Playlist,
                media_id: String::new(),
                variants: Vec::new(),
                rename_files: Vec::new(),
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
            },
        }
    }

    #[test]
    fn empty_hierarchy_returns_empty_index() {
        let result = collect_playlist_media_index(&[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn single_media_entry_indexed_by_id() {
        let entries = vec![media_entry("videos/clip", "clip1", "media1")];
        let result = collect_playlist_media_index(&entries).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result["clip1"], resolved_components("videos/clip"));
    }

    #[test]
    fn non_media_entries_skipped() {
        let entries =
            vec![non_media_entry("playlists/mix"), media_entry("videos/clip", "clip1", "media1")];
        let result = collect_playlist_media_index(&entries).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn duplicate_hierarchy_id_with_same_path_ok() {
        let entries = vec![
            media_entry("videos/clip", "clip1", "media1"),
            media_entry("videos/clip", "clip1", "media2"),
        ];
        let result = collect_playlist_media_index(&entries).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result["clip1"], resolved_components("videos/clip"));
    }

    #[test]
    fn duplicate_hierarchy_id_with_different_path_errors() {
        let entries = vec![
            media_entry("videos/clip1", "clip1", "media1"),
            media_entry("videos/clip2", "clip1", "media2"),
        ];
        let err = collect_playlist_media_index(&entries).unwrap_err();
        assert!(
            matches!(err, MediaPmError::Workflow(ref msg) if msg.contains("clip1")),
            "expected Workflow error about clip1, got: {err}"
        );
    }

    #[test]
    fn hierarchy_node_sanitize_names_absent_resolves_to_inherit() {
        let node: HierarchyNode = serde_json::from_str(
            r#"{"path": ["x"], "kind": "media", "media_id": "m1", "variant": "v1"}"#,
        )
        .unwrap();
        assert_eq!(node.sanitize_names, None);

        let mut out = Vec::new();
        flatten_hierarchy_nodes_inner(&[node], &[], None, &SanitizeNamesConfig::Inherit, &mut out)
            .unwrap();
        assert_eq!(out[0].entry.sanitize_names, SanitizeNamesConfig::Inherit);
    }

    #[test]
    fn hierarchy_node_sanitize_names_explicit_overrides() {
        // Explicit non-Inherit value must survive flattening unchanged.
        let node = HierarchyNode {
            path: HierarchyPath::simple("x"),
            kind: HierarchyNodeKind::Media,
            id: None,
            media_id: Some("m1".into()),
            variant: Some("v1".into()),
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: PlaylistFormat::M3u8,
            ids: Vec::new(),
            sanitize_names: Some(SanitizeNamesConfig::Disabled),
            children: Vec::new(),
        };
        assert_eq!(node.sanitize_names, Some(SanitizeNamesConfig::Disabled));

        let mut out = Vec::new();
        flatten_hierarchy_nodes_inner(&[node], &[], None, &SanitizeNamesConfig::Inherit, &mut out)
            .unwrap();
        assert_eq!(out[0].entry.sanitize_names, SanitizeNamesConfig::Disabled);
    }
}

#[cfg(feature = "proptest")]
#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    proptest! {
        /// Every resolved variant must be a member of the available variants set.
        #[test]
        fn result_is_subset_of_available_variants(
            available in proptest::collection::btree_set("[a-z]+", 1..10),
            selectors in proptest::collection::vec("[a-z]+", 0..5),
        ) {
            let result = expand_variant_selectors(&selectors, &available);
            if let Ok(resolved) = result {
                for variant in &resolved {
                    prop_assert!(available.contains(variant),
                        "variant '{variant}' not in available set");
                }
            }
        }

        /// The result must never contain the same variant twice.
        #[test]
        fn result_has_no_duplicates(
            available in proptest::collection::btree_set("[a-z]+", 1..10),
            selectors in proptest::collection::vec("[a-z]+", 0..5),
        ) {
            let result = expand_variant_selectors(&selectors, &available);
            if let Ok(resolved) = result {
                let mut seen = BTreeSet::new();
                for variant in &resolved {
                    prop_assert!(seen.insert(variant.clone()),
                        "duplicate variant '{variant}'");
                }
            }
        }

        /// An empty selector string always produces an error.
        #[test]
        fn error_on_empty_selector(
            available in proptest::collection::btree_set("[a-z]+", 1..5),
            non_empty_selectors in proptest::collection::vec("[a-z]+", 0..3),
        ) {
            let mut selectors = non_empty_selectors;
            selectors.push(String::new());
            let result = expand_variant_selectors(&selectors, &available);
            prop_assert!(result.is_err(), "expected error for empty selector");
        }

        /// Same inputs always produce the same output (determinism).
        #[test]
        fn deterministic_results(
            available in proptest::collection::btree_set("[a-z]+", 1..10),
            selectors in proptest::collection::vec("[a-z]+", 0..5),
        ) {
            let result1 = expand_variant_selectors(&selectors, &available);
            let result2 = expand_variant_selectors(&selectors, &available);
            prop_assert_eq!(result1, result2, "determinism violation");
        }

        /// When `default` is not available and selectors reference unknown
        /// variants, the result is always an error.
        #[test]
        fn result_respects_available_variants_when_no_default(
            available in proptest::collection::btree_set("[a-m][a-z]{1,5}", 1..5)
                .prop_filter("available must not contain 'default'", |s| !s.contains("default")),
            selectors in proptest::collection::vec("[n-z][a-z]{1,5}", 1..5),
        ) {
            // Guarantee: selector chars [n-z] cannot match available chars [a-m].
            let result = expand_variant_selectors(&selectors, &available);
            prop_assert!(result.is_err(),
                "expected error when no default available and selectors reference unknown variants");
        }
    }
}
