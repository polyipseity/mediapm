//! Plan execution engine.
//!
//! This module interprets planner effects and performs filesystem side effects.
//! It intentionally keeps effect execution explicit and summary-oriented so sync
//! operations remain debuggable and script-friendly.
//!
//! Execution is divided into two conceptual phases:
//! - import phase: ensure byte content is represented in object store and
//!   reflected in sidecar lineage,
//! - materialization phase: ensure configured filesystem links/views exist.
//!
//! The goal is not merely "make files exist", but to preserve reproducible
//! history and identity invariants while reconciling user intent.

use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Result, anyhow};
use serde::Serialize;
use serde_json::json;

use crate::{
    application::planner::{Effect, Plan},
    configuration::config::{AppConfig, LinkMethod, SelectionPreference, VariantSelection},
    domain::{
        metadata::probe_media_file,
        model::{EditEvent, EditKind, MediaRecord, VariantLineage, VariantRecord},
    },
    infrastructure::store::{
        WorkspacePaths, ensure_object, hash_file, read_sidecar, write_sidecar,
    },
    support::util::{merge_json_object, now_rfc3339},
};

/// Result summary for `sync` execution.
///
/// This summary is designed for both human logs and machine-readable output,
/// enabling CI checks or scripts to reason about whether work was performed.
#[derive(Debug, Default, Clone, Serialize)]
pub struct SyncSummary {
    /// Number of effects in the evaluated plan.
    pub planned_effects: usize,
    /// Count of newly imported variants/sidecars.
    pub imports_created: usize,
    /// Count of imports that were already present.
    pub imports_unchanged: usize,
    /// Count of links created from scratch.
    pub links_created: usize,
    /// Count of existing links/targets replaced.
    pub links_updated: usize,
    /// Count of links already matching desired target.
    pub links_unchanged: usize,
    /// Non-fatal warning messages.
    pub warnings: Vec<String>,
}

/// Execute a plan.
///
/// When `apply` is `false`, this function currently returns a summary without
/// writing to disk (effectively dry-run accounting).
///
/// Side effects are intentionally centralized here instead of being scattered
/// through CLI and helper modules.
pub fn execute_plan(
    paths: &WorkspacePaths,
    config: &AppConfig,
    plan: &Plan,
    apply: bool,
) -> Result<SyncSummary> {
    paths.ensure_store_dirs()?;

    let mut summary = SyncSummary { planned_effects: plan.effects.len(), ..SyncSummary::default() };

    for effect in &plan.effects {
        match effect {
            Effect::EnsureImported { canonical_uri, source_path } => {
                if !apply {
                    continue;
                }

                match ensure_imported(paths, config, canonical_uri, Path::new(source_path))? {
                    ImportOutcome::Created => summary.imports_created += 1,
                    ImportOutcome::Unchanged => summary.imports_unchanged += 1,
                }
            }
            Effect::EnsureLink { canonical_uri, link_path, selection } => {
                if !apply {
                    continue;
                }

                match ensure_link(
                    paths,
                    &config.policies.link_methods,
                    canonical_uri,
                    Path::new(link_path),
                    selection,
                )? {
                    LinkOutcome::Created => summary.links_created += 1,
                    LinkOutcome::Updated => summary.links_updated += 1,
                    LinkOutcome::Unchanged => summary.links_unchanged += 1,
                }
            }
        }
    }

    Ok(summary)
}

enum ImportOutcome {
    Created,
    Unchanged,
}

enum LinkOutcome {
    Created,
    Updated,
    Unchanged,
}

fn ensure_imported(
    paths: &WorkspacePaths,
    config: &AppConfig,
    canonical_uri: &str,
    source_path: &Path,
) -> Result<ImportOutcome> {
    let variant_hash = hash_file(source_path)?;
    let object_relpath = ensure_object(paths, source_path, &variant_hash)?;
    let byte_size = fs::metadata(source_path)?.len();

    let (container, probe, mut metadata) = probe_media_file(source_path, variant_hash)?;

    if let Some(override_value) = config.metadata_overrides.get(canonical_uri) {
        merge_json_object(&mut metadata, override_value);
    }

    if let Some(mut sidecar) = read_sidecar(paths, canonical_uri)? {
        if sidecar.has_variant(&variant_hash) {
            return Ok(ImportOutcome::Unchanged);
        }

        let parent_variant_hash = sidecar.latest_variant().map(|variant| variant.variant_hash);
        let mut edit_event_ids = Vec::new();

        if let Some(parent_hash) = parent_variant_hash {
            let event_id = format!(
                "evt_source_update_{}_{}",
                &variant_hash.to_hex()[..12],
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos()
            );

            sidecar.edits.push(EditEvent {
                event_id: event_id.clone(),
                timestamp: now_rfc3339()?,
                kind: EditKind::NonRevertable,
                operation: "source_update".to_owned(),
                details: json!({
                    "strict_rehash": config.policies.strict_rehash,
                }),
                from_variant_hash: parent_hash,
                to_variant_hash: variant_hash,
            });

            edit_event_ids.push(event_id);
        }

        sidecar.variants.push(VariantRecord {
            variant_hash,
            object_relpath,
            byte_size,
            container,
            probe,
            metadata,
            lineage: VariantLineage { parent_variant_hash, edit_event_ids },
        });

        sidecar.updated_at = now_rfc3339()?;
        write_sidecar(paths, &sidecar)?;

        return Ok(ImportOutcome::Created);
    }

    let original_metadata = json!({
        "raw": probe,
        "normalized": metadata,
    });

    let new_sidecar = MediaRecord::new_initial(
        canonical_uri.to_owned(),
        now_rfc3339()?,
        VariantRecord {
            variant_hash,
            object_relpath,
            byte_size,
            container,
            probe: original_metadata["raw"].clone(),
            metadata: original_metadata["normalized"].clone(),
            lineage: VariantLineage { parent_variant_hash: None, edit_event_ids: Vec::new() },
        },
        original_metadata,
    );

    write_sidecar(paths, &new_sidecar)?;
    Ok(ImportOutcome::Created)
}

fn ensure_link(
    paths: &WorkspacePaths,
    methods: &[LinkMethod],
    canonical_uri: &str,
    link_path: &Path,
    selection: &VariantSelection,
) -> Result<LinkOutcome> {
    let sidecar = read_sidecar(paths, canonical_uri)?
        .ok_or_else(|| anyhow!("cannot link unknown media URI: {canonical_uri}"))?;

    let variant = select_variant(&sidecar, selection)?;
    let source_path = paths.root.join(PathBuf::from(&variant.object_relpath));

    if !source_path.exists() {
        return Err(anyhow!(
            "target object for URI {} does not exist: {}",
            canonical_uri,
            source_path.display()
        ));
    }

    let existed_before = link_path.exists() || fs::symlink_metadata(link_path).is_ok();

    if is_existing_symlink_to(link_path, &source_path)? {
        return Ok(LinkOutcome::Unchanged);
    }

    if let Some(parent) = link_path.parent() {
        fs::create_dir_all(parent)?;
    }

    if existed_before {
        remove_existing_path(link_path)?;
    }

    materialize_link_with_fallback(&source_path, link_path, methods)?;

    if existed_before { Ok(LinkOutcome::Updated) } else { Ok(LinkOutcome::Created) }
}

fn select_variant<'record>(
    sidecar: &'record MediaRecord,
    selection: &VariantSelection,
) -> Result<&'record VariantRecord> {
    if let Some(hash) = &selection.variant_hash {
        let chosen = sidecar
            .variants
            .iter()
            .find(|variant| variant.variant_hash.to_hex() == *hash)
            .ok_or_else(|| anyhow!("requested variant_hash not found: {hash}"))?;
        return Ok(chosen);
    }

    match selection.prefer {
        SelectionPreference::Latest => {
            sidecar.variants.last().ok_or_else(|| anyhow!("sidecar has no variants"))
        }
        SelectionPreference::LatestNonLossy => sidecar
            .variants
            .iter()
            .rev()
            .find(|variant| !is_lossy_container(variant.container.as_deref()))
            .or_else(|| sidecar.variants.last())
            .ok_or_else(|| anyhow!("sidecar has no variants")),
    }
}

fn is_lossy_container(container: Option<&str>) -> bool {
    matches!(container, Some("mp3") | Some("aac") | Some("ogg") | Some("opus") | Some("wma"))
}

fn materialize_link_with_fallback(
    source: &Path,
    target: &Path,
    methods: &[LinkMethod],
) -> Result<()> {
    let mut failures = Vec::new();

    for method in methods {
        match method {
            LinkMethod::Symlink => match create_file_symlink(source, target) {
                Ok(()) => return Ok(()),
                Err(error) => failures.push(format!("symlink failed: {error}")),
            },
            LinkMethod::Hardlink => match fs::hard_link(source, target) {
                Ok(()) => return Ok(()),
                Err(error) => failures.push(format!("hardlink failed: {error}")),
            },
            LinkMethod::Copy => match fs::copy(source, target) {
                Ok(_) => return Ok(()),
                Err(error) => failures.push(format!("copy failed: {error}")),
            },
        }

        if target.exists() {
            let _ = remove_existing_path(target);
        }
    }

    Err(anyhow!(
        "failed to materialize link {} -> {} using methods [{}]",
        target.display(),
        source.display(),
        failures.join("; ")
    ))
}

#[cfg(unix)]
fn create_file_symlink(source: &Path, target: &Path) -> Result<()> {
    std::os::unix::fs::symlink(source, target)?;
    Ok(())
}

#[cfg(windows)]
fn create_file_symlink(source: &Path, target: &Path) -> Result<()> {
    std::os::windows::fs::symlink_file(source, target)?;
    Ok(())
}

fn is_existing_symlink_to(link_path: &Path, expected_target: &Path) -> Result<bool> {
    let metadata = match fs::symlink_metadata(link_path) {
        Ok(metadata) => metadata,
        Err(_) => return Ok(false),
    };

    if !metadata.file_type().is_symlink() {
        return Ok(false);
    }

    let link_target = fs::read_link(link_path)?;
    let resolved_target = if link_target.is_absolute() {
        link_target
    } else {
        link_path.parent().unwrap_or_else(|| Path::new(".")).join(link_target)
    };

    let normalized_expected = normalize_existing_path(expected_target);
    let normalized_actual = normalize_existing_path(&resolved_target);

    Ok(normalized_expected == normalized_actual)
}

fn normalize_existing_path(path: &Path) -> PathBuf {
    if path.exists() {
        fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
    } else {
        path.to_path_buf()
    }
}

fn remove_existing_path(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;

    if metadata.file_type().is_symlink() || metadata.is_file() {
        fs::remove_file(path)?;
        return Ok(());
    }

    fs::remove_dir_all(path)?;
    Ok(())
}
