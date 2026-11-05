//! Metadata resolution: placeholders, variant metadata, regex transforms.
//!
//! Provides template interpolation for `${media.id}` and
//! `${media.metadata.<key>}` placeholders, variant-file metadata extraction
//! from JSON/ffprobe output, and regex-based metadata string transforms.
//! A probe that has to spawn `ffprobe` is served from the workspace
//! [`crate::metadata_cache::MetadataCache`] on repeat content, keyed by the
//! content's hash.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, Mutex};

use mediapm_cas::{CasApi, FileSystemCas, Hash};
use mediapm_conductor::{ConductorState, NickelDocument};
use regex::Regex;

use crate::config::MediaPmDocument;
use crate::config::hierarchy_types::{
    FlattenedHierarchyEntry, HierarchyFolderRenameRule, missing_media_id_path_error,
};
use crate::config::output_types::{GenericOutputVariantConfig, OutputVariantValue};
use crate::config::source_types::{
    MediaMetadataRegexTransform, MediaMetadataValue, MediaMetadataValueCandidate, MediaSourceSpec,
};
use crate::error::MediaPmError;
use crate::metadata_cache::MetadataCache;
use crate::source_metadata::try_fetch_local_source_metadata_with_ffprobe;
use crate::tools::workflows::{FfmpegSlotLimits, resolve_media_variant_output_binding_with_limits};

use super::zip_reader::extract_zip_member_bytes;

/// Per-workflow step output hash table (`step_id -> output_name -> CAS hash`).
pub(super) type StepOutputHashes = BTreeMap<String, BTreeMap<String, Hash>>;

/// Shared lookup context threaded through materialization workers.
#[derive(Clone)]
pub(super) struct MaterializationLookupContext {
    /// CAS store reference for resolving variant byte content.
    pub(super) cas: FileSystemCas,
    /// Conductor runtime state after workflow execution.
    pub(super) conductor_state: Option<ConductorState>,
    /// Generated conductor document carrying managed workflows and tools.
    pub(super) generated_doc: NickelDocument,
    /// Effective ffmpeg slot limits for variant binding resolution.
    pub(super) ffmpeg_slot_limits: FfmpegSlotLimits,
    /// Cache for per-workflow step output hash resolution during one sync pass.
    pub(super) step_output_hashes_cache: Arc<Mutex<BTreeMap<String, Option<StepOutputHashes>>>>,
    /// Workspace metadata cache the service owns, shared with the materializer
    /// so a `${media.metadata.*}` probe of content already probed is served
    /// from `metadata.json` instead of spawning `ffprobe` again.
    ///
    /// `None` in tests that need no probe answers; production passes the
    /// service's own handle (one instance per workspace, since two instances
    /// over one file would each rewrite it from their own map).
    pub(super) metadata_cache: Option<Arc<MetadataCache>>,
}

impl MaterializationLookupContext {
    /// Creates a lookup context for one hierarchy sync pass.
    #[must_use]
    pub(super) fn new(
        cas: FileSystemCas,
        conductor_state: Option<ConductorState>,
        generated_doc: NickelDocument,
        ffmpeg_slot_limits: FfmpegSlotLimits,
        metadata_cache: Option<Arc<MetadataCache>>,
    ) -> Self {
        Self {
            cas,
            conductor_state,
            generated_doc,
            ffmpeg_slot_limits,
            step_output_hashes_cache: Arc::new(Mutex::new(BTreeMap::new())),
            metadata_cache,
        }
    }
}

/// Resolves one [`MediaMetadataValue`] to a concrete string.
pub(super) async fn resolve_metadata_value(
    value: &MediaMetadataValue,
    media_id: &str,
    source: &MediaSourceSpec,
    lookup_context: &MaterializationLookupContext,
) -> Result<String, MediaPmError> {
    match value {
        MediaMetadataValue::Literal(text) => Ok(text.clone()),
        MediaMetadataValue::Variant(binding) => {
            let resolved = resolve_variant_metadata_key(
                &binding.variant,
                &binding.metadata_key,
                media_id,
                source,
                lookup_context,
            )
            .await?
            .ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' variant '{}' metadata key '{}' did not resolve",
                    binding.variant, binding.metadata_key
                ))
            })?;

            let value = if let Some(transform) = &binding.transform {
                apply_metadata_regex_transform(&resolved, transform)
            } else {
                resolved
            };

            Ok(value)
        }
        MediaMetadataValue::Fallback(candidates) => {
            for candidate in candidates {
                match candidate {
                    MediaMetadataValueCandidate::Literal(text) => {
                        if !text.is_empty() {
                            return Ok(text.clone());
                        }
                    }
                    MediaMetadataValueCandidate::Variant(binding) => {
                        if let Some(value) = resolve_variant_metadata_key(
                            &binding.variant,
                            &binding.metadata_key,
                            media_id,
                            source,
                            lookup_context,
                        )
                        .await?
                        {
                            let value = if let Some(transform) = &binding.transform {
                                apply_metadata_regex_transform(&value, transform)
                            } else {
                                value
                            };
                            if !value.is_empty() {
                                return Ok(value);
                            }
                        }
                    }
                }
            }

            Err(MediaPmError::Workflow(format!(
                "media '{media_id}' metadata had no fallback that resolved to a non-empty value"
            )))
        }
    }
}

/// Interpolates `${media.id}` and `${media.metadata.<key>}` placeholders in a
/// hierarchy path template string.
pub(super) async fn interpolate_path_template(
    template: &str,
    media_id: &str,
    source: &MediaSourceSpec,
    lookup_context: &MaterializationLookupContext,
) -> Result<String, MediaPmError> {
    let placeholder_keys = collect_metadata_placeholder_keys(template);
    let mut result = template.to_string();

    result = result.replace("${media.id}", media_id);

    for key in &placeholder_keys {
        let placeholder = format!("${{media.metadata.{key}}}");
        let resolved = if let Some(metadata_value) = source.metadata.get(key) {
            resolve_metadata_value(metadata_value, media_id, source, lookup_context).await?
        } else {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' template placeholder \
                 '${{media.metadata.{key}}}' has no matching metadata entry"
            )));
        };
        result = result.replace(&placeholder, &resolved);
    }

    Ok(result)
}

/// Interpolates media placeholders in folder rename-rule replacements.
pub(super) async fn resolve_interpolated_folder_rename_rules(
    rules: &[HierarchyFolderRenameRule],
    media_id: &str,
    source: &MediaSourceSpec,
    lookup_context: &MaterializationLookupContext,
) -> Result<Vec<HierarchyFolderRenameRule>, MediaPmError> {
    let mut resolved = Vec::with_capacity(rules.len());
    for rule in rules {
        resolved.push(HierarchyFolderRenameRule {
            pattern: rule.pattern.clone(),
            replacement: interpolate_path_template(
                &rule.replacement,
                media_id,
                source,
                lookup_context,
            )
            .await?,
        });
    }
    Ok(resolved)
}

/// Resolves hierarchy path templates for one flattened entry.
///
/// `template_components` is the entry's declared or interpolated component
/// text. The result is still that text: interpolated values come from tag
/// metadata, ffprobe output, and upstream sources, none of which the user
/// controls, so nothing here has been through
/// [`crate::path_component::PathComponent::parse`] yet. Parsing is the
/// materializer's next step, and only that step turns the text into something a
/// read site may hold.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] when the entry holds a placeholder and
/// binds no media id, or names a media id the document does not define.
pub(super) async fn resolve_materialized_path_components(
    entry: &FlattenedHierarchyEntry,
    template_components: &[String],
    document: &MediaPmDocument,
    lookup_context: &MaterializationLookupContext,
) -> Result<Vec<String>, MediaPmError> {
    // A path with no placeholder needs no metadata lookup, and an entry that
    // binds a media id the document does not define is not this function's
    // error to raise.
    if template_components.iter().all(|component| !component.contains("${")) {
        return Ok(template_components.to_vec());
    }

    // No trim: the media id is used exactly as it was declared. A padded id
    // was a second spelling — this lookup trimmed it, `materializer::resolve`
    // did not — and both spellings are now rejected upstream by
    // `config::hierarchy_types::validate_media_id`, which runs on every
    // hierarchy-bound id in the flatten walk and on every key of the
    // `media` map in the workflow-synthesis reconcile. The `is_empty` check
    // below stays: it reports a template placeholder on an entry that binds
    // no media, not a spelling problem.
    let media_id = entry.entry.media_id.as_str();
    if media_id.is_empty() {
        return Err(missing_media_id_path_error(entry));
    }

    let source = document.media.get(media_id).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "hierarchy references unknown media id '{media_id}' while resolving path templates"
        ))
    })?;

    let mut resolved_components = Vec::with_capacity(template_components.len());
    for component in template_components {
        let resolved = if component.contains("${") {
            interpolate_path_template(component, media_id, source, lookup_context).await?
        } else {
            component.clone()
        };
        resolved_components.push(resolved);
    }

    Ok(resolved_components)
}

/// Resolves hierarchy path templates across flattened hierarchy entries.
///
/// The result is written back as component text, which is the only shape a
/// [`FlattenedHierarchyEntry`] holds. This stage never produces validated
/// components; that is `super::sanitize_and_validate_hierarchy_paths`, and the
/// value it returns is the one read sites are given.
pub(super) async fn resolve_flattened_entry_paths(
    flattened: &mut [FlattenedHierarchyEntry],
    document: &MediaPmDocument,
    lookup_context: &MaterializationLookupContext,
) -> Result<(), MediaPmError> {
    for entry in flattened {
        let resolved = resolve_materialized_path_components(
            entry,
            &entry.path_components,
            document,
            lookup_context,
        )
        .await?;
        entry.path_components = resolved;
    }
    Ok(())
}

/// Resolves one metadata key from a variant's produced content bytes.
pub(super) async fn resolve_variant_metadata_key(
    variant: &str,
    metadata_key: &str,
    media_id: &str,
    source: &MediaSourceSpec,
    lookup_context: &MaterializationLookupContext,
) -> Result<Option<String>, MediaPmError> {
    use super::resolve::resolve_variant_hash;

    let hash = resolve_variant_hash(media_id, variant, source, lookup_context).await?;

    let Some(hash) = hash else {
        return Ok(None);
    };

    extract_metadata_key_from_variant_hash(
        media_id,
        variant,
        source,
        lookup_context,
        hash,
        metadata_key,
    )
    .await
}

async fn extract_metadata_key_from_variant_hash(
    media_id: &str,
    variant: &str,
    source: &MediaSourceSpec,
    lookup_context: &MaterializationLookupContext,
    hash: Hash,
    metadata_key: &str,
) -> Result<Option<String>, MediaPmError> {
    let bytes = lookup_context.cas.get(hash).await.map_err(|error| {
        MediaPmError::Workflow(format!(
            "media '{media_id}' variant '{variant}' CAS read failed: {error}"
        ))
    })?;

    let metadata_bytes = metadata_probe_bytes(source, variant, lookup_context, bytes.as_ref());

    if let Some(value) = extract_metadata_key_from_probe_bytes(
        source,
        variant,
        metadata_key,
        &metadata_bytes,
        lookup_context.metadata_cache.as_deref(),
    )? {
        return Ok(Some(value));
    }

    if let Ok(member_bytes) = extract_zip_member_bytes(bytes.as_ref(), "info.json")
        && let Some(value) = extract_metadata_key_from_probe_bytes(
            source,
            variant,
            metadata_key,
            &member_bytes,
            lookup_context.metadata_cache.as_deref(),
        )?
    {
        return Ok(Some(value));
    }

    if let Ok(member_bytes) = extract_zip_member_bytes(bytes.as_ref(), ".info.json")
        && let Some(value) = extract_metadata_key_from_probe_bytes(
            source,
            variant,
            metadata_key,
            &member_bytes,
            lookup_context.metadata_cache.as_deref(),
        )?
    {
        return Ok(Some(value));
    }

    Ok(None)
}

/// Extracts one metadata key from variant bytes, probing them only when they
/// are not already the probe document.
///
/// Bytes that parse as the probe's JSON answer directly; anything else goes
/// through [`probe_media_bytes_cached`], which serves content already probed
/// from `metadata_cache` without spawning `ffprobe`.
///
/// # Errors
///
/// Returns the probe's [`MediaPmError::Workflow`] failure when the content is
/// neither the probe document nor probeable.
fn extract_metadata_key_from_probe_bytes(
    source: &MediaSourceSpec,
    variant: &str,
    metadata_key: &str,
    probe_bytes: &[u8],
    metadata_cache: Option<&MetadataCache>,
) -> Result<Option<String>, MediaPmError> {
    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(probe_bytes)
        && let Some(value) = extract_metadata_key_from_json(&json, metadata_key)
    {
        return Ok(Some(value));
    }

    extract_metadata_key_from_media_bytes_via_ffprobe(
        source,
        variant,
        probe_bytes,
        metadata_key,
        metadata_cache,
    )
}

fn metadata_probe_bytes(
    source: &MediaSourceSpec,
    variant: &str,
    lookup_context: &MaterializationLookupContext,
    raw_bytes: &[u8],
) -> Vec<u8> {
    if let Ok(Some(binding)) = resolve_media_variant_output_binding_with_limits(
        source,
        variant,
        lookup_context.ffmpeg_slot_limits.max_input_slots,
        lookup_context.ffmpeg_slot_limits.max_output_slots,
    ) && let Some(zip_member) = binding.zip_member.as_deref()
        && let Ok(member_bytes) = extract_zip_member_bytes(raw_bytes, zip_member)
    {
        return member_bytes;
    }

    raw_bytes.to_vec()
}

fn output_variant_extension(source: &MediaSourceSpec, variant: &str) -> String {
    for step in &source.steps {
        if let Some(output) = step.output_variants.get(variant)
            && let OutputVariantValue::Generic(GenericOutputVariantConfig { extension, .. }) =
                output
            && !extension.is_empty()
        {
            return extension.clone();
        }
    }
    "mkv".to_string()
}

/// Extracts one metadata key by probing the variant bytes with `ffprobe`.
///
/// # HTTP client policy
///
/// Does not use HTTP: the probe is the local `ffprobe` binary, served from
/// `metadata_cache` by [`probe_media_bytes_cached`] when the same content was
/// probed before.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] when the temp file cannot be written or
/// `ffprobe` fails on the content.
fn extract_metadata_key_from_media_bytes_via_ffprobe(
    source: &MediaSourceSpec,
    variant: &str,
    bytes: &[u8],
    metadata_key: &str,
    metadata_cache: Option<&MetadataCache>,
) -> Result<Option<String>, MediaPmError> {
    let extension = output_variant_extension(source, variant);
    let json =
        probe_media_bytes_cached(variant, &extension, bytes, metadata_cache, |probe_path| {
            try_fetch_local_source_metadata_with_ffprobe(probe_path, "ffprobe")
        })?;
    Ok(extract_metadata_key_from_json(&json, metadata_key))
}

/// Probes `bytes` as media once per content, serving repeats from
/// `metadata_cache`.
///
/// The cache key is `ffprobe:{blake3(bytes)}`: the content's own hash, not
/// the probe path, because the probe file is a fresh random temp file every
/// run and a path key would never hit (and would grow the file with an entry
/// per run). The key family differs from the service's `ffprobe:{path}`
/// entries, which hold [`crate::source_metadata::LocalSourceMetadata`] for a
/// real on-disk file; this cache stores the probe's raw JSON document, which
/// is what a template resolution reads. A hit answers from the cached
/// document without writing the temp file or spawning `ffprobe`; a miss
/// writes the temp file, probes it, and stores the document before returning
/// it. The cached document includes ffprobe's own `format.filename`, which
/// names a temp file that dies with the run (as it does on a miss), so it is
/// never an identity, only a value the resolution path reads keys from.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] when the temp probe file cannot be
/// written, and the probe's own failure when `probe` fails.
fn probe_media_bytes_cached(
    variant: &str,
    extension: &str,
    bytes: &[u8],
    metadata_cache: Option<&MetadataCache>,
    probe: impl FnOnce(&Path) -> Result<serde_json::Value, MediaPmError>,
) -> Result<serde_json::Value, MediaPmError> {
    let cache_key = format!("ffprobe:{}", Hash::from_content(bytes));
    if let Some(cached) = metadata_cache.and_then(|cache| cache.get(&cache_key)) {
        return Ok(cached);
    }

    let temp_dir = mediapm_utils::temp::artifact_dir().map_err(|error| {
        MediaPmError::Workflow(format!(
            "failed to create temp directory for ffprobe metadata probe of variant '{variant}': {error}"
        ))
    })?;
    let probe_path = temp_dir.path().join(format!("probe.{extension}"));
    std::fs::write(&probe_path, bytes).map_err(|error| {
        MediaPmError::Workflow(format!(
            "failed to write temp media bytes for ffprobe metadata probe of variant '{variant}': {error}"
        ))
    })?;

    let json = probe(&probe_path)?;
    if let Some(cache) = metadata_cache {
        cache.set(cache_key, json.clone());
    }
    Ok(json)
}

fn extract_metadata_key_from_json(json: &serde_json::Value, key: &str) -> Option<String> {
    if let Some(value) = extract_metadata_key_from_json_direct(json, key) {
        return Some(value);
    }

    match key {
        "artist" => extract_metadata_key_from_json_direct(json, "uploader")
            .or_else(|| extract_metadata_key_from_json_direct(json, "channel"))
            .or_else(|| extract_metadata_key_from_json_direct(json, "track_artist")),
        "title" => extract_metadata_key_from_json_direct(json, "track"),
        _ => None,
    }
}

fn extract_metadata_key_from_json_direct(json: &serde_json::Value, key: &str) -> Option<String> {
    if let Some(format) = json.get("format").and_then(serde_json::Value::as_object) {
        if let Some(value) = lookup_json_string_key(format, key) {
            return Some(value);
        }
        if let Some(tags) = format.get("tags").and_then(serde_json::Value::as_object)
            && let Some(value) = lookup_json_string_key(tags, key)
        {
            return Some(value);
        }
    }

    if let Some(streams) = json.get("streams").and_then(serde_json::Value::as_array) {
        for stream in streams {
            if let Some(stream_obj) = stream.as_object() {
                if let Some(value) = lookup_json_string_key(stream_obj, key) {
                    return Some(value);
                }
                if let Some(tags) = stream_obj.get("tags").and_then(serde_json::Value::as_object)
                    && let Some(value) = lookup_json_string_key(tags, key)
                {
                    return Some(value);
                }
            }
        }
    }

    if let Some(obj) = json.as_object()
        && let Some(value) = lookup_json_string_key(obj, key)
    {
        return Some(value);
    }

    None
}

fn lookup_json_string_key(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<String> {
    object
        .iter()
        .find_map(|(candidate_key, candidate_value)| {
            candidate_key.eq_ignore_ascii_case(key).then_some(candidate_value)
        })
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
}

fn apply_metadata_regex_transform(value: &str, transform: &MediaMetadataRegexTransform) -> String {
    match Regex::new(&transform.pattern) {
        Ok(re) => re.replace_all(value, transform.replacement.as_str()).to_string(),
        Err(_) => value.to_string(),
    }
}

fn collect_metadata_placeholder_keys(template: &str) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    let mut cursor = 0usize;

    while let Some(relative_start) = template[cursor..].find("${") {
        let placeholder_start = cursor + relative_start;
        let after_marker = &template[placeholder_start + 2..];
        let Some(relative_end) = after_marker.find('}') else {
            break;
        };

        let expression = after_marker[..relative_end].trim();

        if expression == "media.id" {
            cursor = placeholder_start + 2 + relative_end + 1;
            continue;
        }

        if let Some(metadata_key) = expression.strip_prefix("media.metadata.") {
            let key = metadata_key.trim();
            if !key.is_empty() {
                keys.insert(key.to_string());
            }
        }

        cursor = placeholder_start + 2 + relative_end + 1;
    }

    keys
}

#[cfg(test)]
mod tests {
    //! RED-first coverage for the probe cache (Task 1, Steps 5-6): a
    //! `${media.metadata.*}` resolution must be served from the workspace
    //! `MetadataCache` instead of spawning `ffprobe` on bytes the store has
    //! already been probed for.

    use super::*;
    use crate::config::MediaPmState;
    use crate::config::hierarchy_types::{
        HierarchyNode, HierarchyNodeKind, HierarchyPath, PlaylistFormat, SanitizeNamesConfig,
    };
    use crate::config::source_types::{MediaMetadataVariantBinding, MediaStep, MediaStepTool};
    use crate::config::{GenericOutputVariantConfig, OutputVariantValue};
    use crate::metadata_cache::MetadataCache;
    use crate::paths::MediaPmPaths;

    /// Builds a one-media document whose `title` placeholder resolves by
    /// probing variant `default`'s bytes, which the document seeds at
    /// `variant_hash`.
    fn variant_metadata_document(media_id: &str, variant_hash: &str) -> MediaPmDocument {
        MediaPmDocument {
            media: BTreeMap::from([(
                media_id.to_string(),
                MediaSourceSpec {
                    steps: vec![MediaStep {
                        tool: MediaStepTool::Import,
                        input_variants: vec![],
                        output_variants: BTreeMap::from([(
                            "default".into(),
                            OutputVariantValue::Generic(GenericOutputVariantConfig {
                                kind: "primary".to_string(),
                                ..Default::default()
                            }),
                        )]),
                        options: BTreeMap::new(),
                    }],
                    variant_hashes: BTreeMap::from([(
                        "default".to_string(),
                        variant_hash.to_string(),
                    )]),
                    metadata: BTreeMap::from([(
                        "title".to_string(),
                        MediaMetadataValue::Variant(MediaMetadataVariantBinding {
                            variant: "default".to_string(),
                            metadata_key: "title".to_string(),
                            transform: None,
                        }),
                    )]),
                    ..MediaSourceSpec::default()
                },
            )]),
            hierarchy: vec![HierarchyNode {
                path: HierarchyPath::simple("${media.metadata.title}"),
                kind: HierarchyNodeKind::Media,
                id: None,
                media_id: Some(media_id.to_string()),
                variant: Some("default".into()),
                variants: vec![],
                rename_files: vec![],
                format: PlaylistFormat::M3u8,
                ids: vec![],
                sanitize_names: Some(SanitizeNamesConfig::Inherit),
                children: vec![],
            }],
            ..MediaPmDocument::default()
        }
    }

    /// The probe helper spawns the probe once for repeated bytes and serves
    /// the repeat from the workspace metadata cache.
    ///
    /// The counting closures are the witness the brief asks for: the second
    /// call offers a probe that would return a different document, so an
    /// answer equal to the first one can only come from the cache, and the
    /// counter staying at one is the proof the probe did not run again. The
    /// flushed file is read back so the entry is shown to be a persisted
    /// artifact keyed by content, not an in-memory convenience.
    #[test]
    fn the_probe_helper_spawns_once_for_repeated_bytes() {
        let dir = mediapm_utils::temp::artifact_dir().expect("temp dir");
        let cache = MetadataCache::open(dir.path());
        let bytes = b"definitely not a JSON probe document";

        let mut probe_calls = 0;
        let first = probe_media_bytes_cached("default", "mkv", bytes, Some(&cache), |_| {
            probe_calls += 1;
            Ok(serde_json::json!({ "format": { "tags": { "title": "First" } } }))
        })
        .expect("a miss probes and succeeds");
        assert_eq!(probe_calls, 1, "a miss must spawn the probe exactly once");
        assert_eq!(first["format"]["tags"]["title"], "First");

        let second = probe_media_bytes_cached("default", "mkv", bytes, Some(&cache), |_| {
            probe_calls += 1;
            Ok(serde_json::json!({ "format": { "tags": { "title": "Second" } } }))
        })
        .expect("a hit answers without a probe");
        assert_eq!(probe_calls, 1, "a repeat of the same bytes must not spawn the probe again");
        assert_eq!(second, first, "the cached answer is the first probe's document");

        cache.flush().expect("the probed answer persists");
        let text = std::fs::read_to_string(dir.path().join("metadata.json"))
            .expect("the cache file exists");
        assert!(
            text.contains("ffprobe:blake3:"),
            "the materializer's entry is keyed by the content's hash, since a probe path is a \
             fresh temp file every run: {text}"
        );
    }

    /// A path interpolation that would need a probe must be answered from the
    /// workspace metadata cache instead of spawning `ffprobe`.
    ///
    /// The seeded entry is the one a previous run's probe of the same variant
    /// bytes would have left behind, keyed by content: the materializer probes
    /// bytes held in CAS, so a probe path (a fresh random temp file per run)
    /// never repeats and content is the only identity that can hit. The bytes
    /// are deliberately not JSON, so a run that misses the cache reaches
    /// `ffprobe` and fails on them, which is also what makes this test fail
    /// on a build whose materializer bypasses the cache: the probe cannot
    /// answer for garbage, while the cached answer resolves the template.
    #[tokio::test]
    async fn the_materializer_resolves_variant_metadata_from_the_workspace_cache() {
        let workspace = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let paths = MediaPmPaths::from_root(workspace.path());
        let store_root = paths.runtime_root.join("store");
        std::fs::create_dir_all(&store_root).expect("store dir");
        let cas = FileSystemCas::open(&store_root).await.expect("open cas");

        let bytes = b"definitely not a JSON probe document";
        let hash = cas.put(bytes::Bytes::from_static(bytes)).await.expect("put variant bytes");

        let cache_dir = paths.workspace_mediapm_cache_dir();
        {
            let cache = MetadataCache::open(&cache_dir);
            cache.set(
                format!("ffprobe:{hash}"),
                serde_json::json!({ "format": { "tags": { "title": "ServedFromCache" } } }),
            );
            cache.flush().expect("the seeded answer persists");
        }

        let document = variant_metadata_document("src1", &hash.to_string());
        let mut state = MediaPmState::default();
        let report = crate::materializer::sync_hierarchy(
            &paths,
            &document,
            &mut state,
            &cas,
            false,
            &ConductorState::new_empty(),
            &NickelDocument::default(),
            None,
            None,
            Some(Arc::new(MetadataCache::open(&cache_dir))),
        )
        .await
        .expect("the cached probe answer must resolve the template without spawning ffprobe");

        let target = paths.hierarchy_root_dir.join("ServedFromCache");
        assert!(
            target.is_file(),
            "the cached title must name the materialized entry, wrote {report:?} to {}",
            paths.hierarchy_root_dir.display()
        );
    }
}
