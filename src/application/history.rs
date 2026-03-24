//! Edit-history recording APIs.
//!
//! This module advances mediapm's Phase-5 lineage model by providing explicit,
//! side-effectful APIs for recording:
//! - metadata/history edits with configurable revertability, and
//! - output-based variant edits (for transcode or similar transforms).
//!
//! The key design goal is provenance clarity: callers should not mutate sidecar
//! internals directly when representing user edits or delegated transforms.
//! Instead, they call these APIs and receive deterministic event records.

use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow};
use serde::Serialize;
use serde_json::{Value, json};

use crate::{
    domain::{
        metadata::probe_media_file,
        model::{Blake3Hash, EditEvent, EditKind, MediaRecord, VariantLineage, VariantRecord},
    },
    infrastructure::store::{
        WorkspacePaths, ensure_object, hash_file, read_sidecar, write_sidecar,
    },
    support::util::{merge_json_object, now_rfc3339},
};

/// Request to record a metadata/history edit.
#[derive(Debug, Clone)]
pub struct MetadataEditRequest {
    /// Canonical URI whose latest (or selected) variant metadata is edited.
    pub canonical_uri: String,
    /// Optional explicit target variant hash. Defaults to latest variant.
    pub target_variant_hash: Option<Blake3Hash>,
    /// Edit kind classification to persist.
    pub kind: EditKind,
    /// Operation label, usually `metadata_update`.
    pub operation: String,
    /// JSON patch-like overlay merged into variant metadata.
    pub metadata_patch: Value,
    /// Optional user-facing message to keep in event details.
    pub message: Option<String>,
    /// Additional event details merged into sidecar event payload.
    pub details: Value,
}

/// Request to record an output-based variant edit event.
#[derive(Debug, Clone)]
pub struct TranscodeRecordRequest {
    /// Canonical URI identity to append history for.
    pub canonical_uri: String,
    /// Optional explicit input variant hash. Defaults to latest variant.
    pub from_variant_hash: Option<Blake3Hash>,
    /// Edit kind classification to persist.
    pub kind: EditKind,
    /// Output file produced by an external transcode tool.
    pub output_path: PathBuf,
    /// Operation label, usually `transcode`.
    pub operation: String,
    /// Additional event details from caller.
    pub details: Value,
}

/// Structured result for history recording commands.
#[derive(Debug, Clone, Serialize)]
pub struct HistoryRecordSummary {
    /// Canonical URI that was updated.
    pub canonical_uri: String,
    /// New event identifier.
    pub event_id: String,
    /// Event kind persisted to sidecar history.
    pub kind: EditKind,
    /// Event operation name.
    pub operation: String,
    /// Source variant hash hex.
    pub from_variant_hash: String,
    /// Target variant hash hex.
    pub to_variant_hash: String,
    /// Whether a brand-new variant record was appended.
    pub variant_created: bool,
}

/// Record a metadata/history edit event and apply metadata overlay.
///
/// This mutates sidecar metadata for one variant and records an explicit event
/// where `from_variant_hash == to_variant_hash`.
pub fn record_metadata_edit(
    paths: &WorkspacePaths,
    request: MetadataEditRequest,
) -> Result<HistoryRecordSummary> {
    let mut sidecar = read_sidecar(paths, &request.canonical_uri)?
        .ok_or_else(|| anyhow!("unknown canonical URI: {}", request.canonical_uri))?;

    let target_hash = select_variant_hash(&sidecar, request.target_variant_hash)?;
    let variant_index = sidecar
        .variants
        .iter()
        .position(|variant| variant.variant_hash == target_hash)
        .ok_or_else(|| anyhow!("target variant hash missing from sidecar: {target_hash}"))?;

    let variant = &mut sidecar.variants[variant_index];
    let metadata_before = variant.metadata.clone();
    merge_json_object(&mut variant.metadata, &request.metadata_patch);
    let metadata_changed = variant.metadata != metadata_before;

    let operation = normalized_operation(&request.operation, "edit");
    let event_id = generate_event_id("evt_metadata_update", &target_hash);
    let details = merged_metadata_details(
        request.metadata_patch.clone(),
        request.message.clone(),
        request.details,
        metadata_changed,
    )?;

    sidecar.edits.push(EditEvent {
        event_id: event_id.clone(),
        timestamp: now_rfc3339()?,
        kind: request.kind.clone(),
        operation: operation.clone(),
        details,
        from_variant_hash: target_hash,
        to_variant_hash: target_hash,
    });

    if !variant.lineage.edit_event_ids.iter().any(|id| id == &event_id) {
        variant.lineage.edit_event_ids.push(event_id.clone());
    }

    write_sidecar(paths, &sidecar)?;

    Ok(HistoryRecordSummary {
        canonical_uri: request.canonical_uri,
        event_id,
        kind: request.kind,
        operation,
        from_variant_hash: target_hash.to_hex(),
        to_variant_hash: target_hash.to_hex(),
        variant_created: false,
    })
}

/// Record an output-based variant edit event and register output variant.
///
/// The actual transcode process is intentionally delegated; this function only
/// records the resulting bytes and provenance in sidecar/object store state.
pub fn record_transcode_event(
    paths: &WorkspacePaths,
    request: TranscodeRecordRequest,
) -> Result<HistoryRecordSummary> {
    let mut sidecar = read_sidecar(paths, &request.canonical_uri)?
        .ok_or_else(|| anyhow!("unknown canonical URI: {}", request.canonical_uri))?;

    let from_hash = select_variant_hash(&sidecar, request.from_variant_hash)?;
    let output_path = absolute_output_path(paths, &request.output_path);

    if !output_path.exists() {
        return Err(anyhow!("output path does not exist: {}", output_path.display()));
    }

    let to_hash = hash_file(&output_path)?;
    let object_relpath = ensure_object(paths, &output_path, &to_hash)?;
    let byte_size = fs::metadata(&output_path)?.len();
    let (container, probe, metadata) = probe_media_file(&output_path, to_hash)?;

    let operation = normalized_operation(&request.operation, "edit");
    let event_id = generate_event_id("evt_transcode", &to_hash);

    let variant_created = if sidecar.has_variant(&to_hash) {
        false
    } else {
        sidecar.variants.push(VariantRecord {
            variant_hash: to_hash,
            object_relpath,
            byte_size,
            container,
            probe,
            metadata,
            lineage: VariantLineage {
                parent_variant_hash: Some(from_hash),
                edit_event_ids: vec![event_id.clone()],
            },
        });
        true
    };

    let output_variant = sidecar
        .variants
        .iter_mut()
        .find(|variant| variant.variant_hash == to_hash)
        .ok_or_else(|| anyhow!("output variant unexpectedly missing after update"))?;

    if output_variant.lineage.parent_variant_hash.is_none() && from_hash != to_hash {
        output_variant.lineage.parent_variant_hash = Some(from_hash);
    }

    if !output_variant.lineage.edit_event_ids.iter().any(|id| id == &event_id) {
        output_variant.lineage.edit_event_ids.push(event_id.clone());
    }

    let details = merged_transcode_details(request.details, &output_path, &from_hash, &to_hash)?;

    sidecar.edits.push(EditEvent {
        event_id: event_id.clone(),
        timestamp: now_rfc3339()?,
        kind: request.kind.clone(),
        operation: operation.clone(),
        details,
        from_variant_hash: from_hash,
        to_variant_hash: to_hash,
    });

    write_sidecar(paths, &sidecar)?;

    Ok(HistoryRecordSummary {
        canonical_uri: request.canonical_uri,
        event_id,
        kind: request.kind,
        operation,
        from_variant_hash: from_hash.to_hex(),
        to_variant_hash: to_hash.to_hex(),
        variant_created,
    })
}

fn select_variant_hash(sidecar: &MediaRecord, requested: Option<Blake3Hash>) -> Result<Blake3Hash> {
    if let Some(hash) = requested {
        if !sidecar.has_variant(&hash) {
            return Err(anyhow!("requested variant hash not found: {hash}"));
        }

        return Ok(hash);
    }

    sidecar
        .latest_variant()
        .map(|variant| variant.variant_hash)
        .ok_or_else(|| anyhow!("sidecar has no variants"))
}

fn absolute_output_path(paths: &WorkspacePaths, output_path: &Path) -> PathBuf {
    if output_path.is_absolute() { output_path.to_path_buf() } else { paths.root.join(output_path) }
}

fn normalized_operation(input: &str, fallback: &str) -> String {
    let trimmed = input.trim();
    if trimmed.is_empty() { fallback.to_owned() } else { trimmed.to_owned() }
}

fn generate_event_id(prefix: &str, hash: &Blake3Hash) -> String {
    format!(
        "{}_{}_{}",
        prefix,
        &hash.to_hex()[..12],
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos()
    )
}

fn merged_transcode_details(
    details: Value,
    output_path: &Path,
    from_hash: &Blake3Hash,
    to_hash: &Blake3Hash,
) -> Result<Value> {
    let mut merged =
        if details.is_object() { details } else { json!({ "caller_details": details }) };

    let object = merged.as_object_mut().context("details root must be convertible to object")?;

    object
        .insert("output_path".to_owned(), Value::String(output_path.to_string_lossy().to_string()));
    object.insert("from_variant_hash".to_owned(), Value::String(from_hash.to_hex()));
    object.insert("to_variant_hash".to_owned(), Value::String(to_hash.to_hex()));

    Ok(merged)
}

fn merged_metadata_details(
    patch: Value,
    message: Option<String>,
    details: Value,
    metadata_changed: bool,
) -> Result<Value> {
    let mut merged =
        if details.is_object() { details } else { json!({ "caller_details": details }) };

    let object = merged.as_object_mut().context("details root must be convertible to object")?;
    object.insert("patch".to_owned(), patch);
    object.insert("message".to_owned(), message.map(Value::String).unwrap_or(Value::Null));
    object.insert("metadata_changed".to_owned(), Value::Bool(metadata_changed));

    Ok(merged)
}
