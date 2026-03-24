//! CLI entrypoint for mediapm.
//!
//! This binary is intentionally thin. It parses arguments, delegates to the
//! library crate, and turns domain/infrastructure results into user-facing
//! output (human text or JSON).
//!
//! Keeping the CLI layer small is a deliberate design choice:
//! - orchestration logic remains reusable from tests and future frontends,
//! - behavior remains easier to reason about,
//! - command output remains a presentation concern, not business logic.
//!
//! Command surface:
//! - `plan`  : compute effects only,
//! - `sync`  : execute reconciliation,
//! - `verify`: integrity checks,
//! - `gc`    : remove unreferenced objects,
//! - `fmt`   : canonicalize config/sidecars.
//! - `edit`  : append metadata/history edits (revertable or non-revertable).

use std::{
    path::{Path, PathBuf},
    process::ExitCode,
};

use anyhow::{Result, anyhow};
use clap::{Parser, Subcommand, ValueEnum};
use path_clean::PathClean;

use mediapm::{
    application::{
        executor::execute_plan,
        history::{
            MetadataEditRequest, TranscodeRecordRequest, record_metadata_edit,
            record_transcode_event,
        },
        planner::{build_plan, render_plan_human},
    },
    configuration::config::{DEFAULT_CONFIG_FILE, load_config},
    domain::{
        canonical::canonicalize_uri,
        model::{Blake3Hash, EditKind},
    },
    infrastructure::{
        formatter::format_workspace, gc::gc_workspace, store::WorkspacePaths,
        verify::verify_workspace,
    },
};

#[derive(Debug, Parser)]
#[command(author, version, about = "Declarative, workspace-local media reconciler")]
struct Cli {
    /// Workspace root for .mediapm storage and declarative config.
    #[arg(long, default_value = ".")]
    workspace: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Build a deterministic dry-run plan.
    Plan {
        #[arg(long, default_value = DEFAULT_CONFIG_FILE)]
        config: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Execute the reconciliation plan.
    Sync {
        #[arg(long, default_value = DEFAULT_CONFIG_FILE)]
        config: PathBuf,
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        json: bool,
    },
    /// Verify object integrity and sidecar consistency.
    Verify {
        #[arg(long)]
        json: bool,
    },
    /// Garbage-collect unreferenced content-addressed objects.
    Gc {
        #[arg(long)]
        apply: bool,
        #[arg(long)]
        json: bool,
    },
    /// Canonicalize config and sidecar JSON formatting.
    Fmt {
        #[arg(long, default_value = DEFAULT_CONFIG_FILE)]
        config: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Record metadata/history edits (or output-based edits) in sidecar history.
    Edit {
        /// URI (or path-like input) of the media record to update.
        #[arg(long)]
        uri: String,
        /// Edit-kind selection (`auto`, `revertable`, `non-revertable`).
        ///
        /// `auto` selects:
        /// - `revertable` for metadata-only/history-only edits,
        /// - `non-revertable` when `--output` is provided.
        #[arg(long, default_value = "auto")]
        kind: CliEditKind,
        /// Optional JSON object merged into variant metadata.
        ///
        /// If omitted, edit can still append history metadata via --details-json.
        #[arg(long)]
        patch_json: Option<String>,
        /// Optional output media path for output-based edits (e.g. transcode output).
        #[arg(long)]
        output: Option<PathBuf>,
        /// Optional explicit target variant hash (hex) for metadata/history-only edits.
        #[arg(long)]
        target_variant_hash: Option<String>,
        /// Optional explicit source variant hash (hex) for output-based edits.
        #[arg(long)]
        from_variant_hash: Option<String>,
        /// Operation label stored in event history.
        #[arg(long, default_value = "edit")]
        operation: String,
        /// Optional user message stored in event details.
        #[arg(long)]
        message: Option<String>,
        /// Optional JSON details payload merged into event details.
        #[arg(long)]
        details_json: Option<String>,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliEditKind {
    Auto,
    Revertable,
    NonRevertable,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}

/// Parse arguments and dispatch one command.
///
/// This function is the CLI control-plane. It does not implement storage,
/// planning, or reconciliation semantics directly; instead, it composes those
/// capabilities from the library modules and ensures each command has a clear
/// success/failure contract.
fn run() -> Result<()> {
    let cli = Cli::parse();
    let workspace_root = resolve_workspace_root(&cli.workspace)?;
    let paths = WorkspacePaths::new(&workspace_root);

    match cli.command {
        Commands::Plan { config, json } => {
            let config_path = resolve_config_path(&workspace_root, &config);
            let config = load_config(&config_path)?;
            let plan = build_plan(&config, &workspace_root)?;

            if json {
                println!("{}", serde_json::to_string_pretty(&plan)?);
            } else {
                println!("{}", render_plan_human(&plan));
            }
        }
        Commands::Sync { config, dry_run, json } => {
            let config_path = resolve_config_path(&workspace_root, &config);
            let config = load_config(&config_path)?;
            let plan = build_plan(&config, &workspace_root)?;

            if dry_run {
                if json {
                    println!("{}", serde_json::to_string_pretty(&plan)?);
                } else {
                    println!("{}", render_plan_human(&plan));
                    println!("\n(dry-run only; no side effects applied)");
                }
            } else {
                let summary = execute_plan(&paths, &config, &plan, true)?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&summary)?);
                } else {
                    println!("Applied {} effect(s)", summary.planned_effects);
                    println!(
                        "imports: created={} unchanged={}",
                        summary.imports_created, summary.imports_unchanged
                    );
                    println!(
                        "links: created={} updated={} unchanged={}",
                        summary.links_created, summary.links_updated, summary.links_unchanged
                    );
                    println!(
                        "provider (musicbrainz): queries={} cache_hits={} sidecars_updated={} failures={}",
                        summary.provider_queries_attempted,
                        summary.provider_cache_hits,
                        summary.provider_sidecars_updated,
                        summary.provider_failures
                    );
                    if !summary.warnings.is_empty() {
                        println!("warnings: {}", summary.warnings.len());
                    }
                }
            }
        }
        Commands::Verify { json } => {
            let report = verify_workspace(&paths)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("sidecars checked: {}", report.sidecars_checked);
                println!("variants checked: {}", report.variants_checked);
                println!("missing objects: {}", report.missing_objects.len());
                println!("hash mismatches: {}", report.hash_mismatches.len());
                println!(
                    "reference issues: {}",
                    report.sidecar_reference_issues.len() + report.edit_reference_issues.len()
                );
            }

            if !report.is_clean() {
                return Err(anyhow!(
                    "verification failed (use --json for full machine-readable report)"
                ));
            }
        }
        Commands::Gc { apply, json } => {
            let report = gc_workspace(&paths, apply)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("referenced objects: {}", report.referenced_objects);
                println!("gc candidates: {}", report.candidate_count);
                println!("removed: {}", report.removed_count);
                if !apply {
                    println!("(dry-run mode; use --apply to delete candidates)");
                }
            }
        }
        Commands::Fmt { config, json } => {
            let config_path = resolve_config_path(&workspace_root, &config);
            let report = format_workspace(&paths, &config_path)?;

            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("config rewritten: {}", report.config_written);
                println!("sidecars canonicalized: {}", report.sidecars_rewritten);
            }
        }
        Commands::Edit {
            uri,
            kind,
            patch_json,
            output,
            target_variant_hash,
            from_variant_hash,
            operation,
            message,
            details_json,
            json,
        } => {
            let canonical_uri = canonicalize_uri(&uri, &workspace_root)?.into_string();

            if output.is_some() && target_variant_hash.is_some() {
                return Err(anyhow!(
                    "--target-variant-hash is only valid for metadata/history-only edits (without --output)"
                ));
            }

            if output.is_none() && from_variant_hash.is_some() {
                return Err(anyhow!(
                    "--from-variant-hash is only valid for output-based edits (with --output)"
                ));
            }

            let patch_value = parse_patch_json_object(patch_json.as_deref())?;
            let details_value = parse_optional_json(details_json.as_deref(), "--details-json")?;
            let edit_kind = resolve_edit_kind(kind, output.is_some());

            let target_variant_hash = parse_optional_hash(target_variant_hash.as_deref())?;
            let from_variant_hash = parse_optional_hash(from_variant_hash.as_deref())?;

            let summary = if let Some(output_path) = output {
                record_transcode_event(
                    &paths,
                    TranscodeRecordRequest {
                        canonical_uri,
                        from_variant_hash,
                        kind: edit_kind.clone(),
                        output_path,
                        operation,
                        details: merge_message_into_details(details_value, message)?,
                    },
                )?
            } else {
                record_metadata_edit(
                    &paths,
                    MetadataEditRequest {
                        canonical_uri,
                        target_variant_hash,
                        kind: edit_kind,
                        operation,
                        metadata_patch: patch_value,
                        message,
                        details: details_value,
                    },
                )?
            };

            if json {
                println!("{}", serde_json::to_string_pretty(&summary)?);
            } else {
                println!("Recorded edit event: {}", summary.event_id);
                println!("uri: {}", summary.canonical_uri);
                println!(
                    "kind={:?}, from={} -> to={}, new_variant={}",
                    summary.kind,
                    summary.from_variant_hash,
                    summary.to_variant_hash,
                    summary.variant_created
                );
            }
        }
    }

    Ok(())
}

/// Resolve workspace path from CLI input.
///
/// We canonicalize when possible so downstream modules can treat paths as
/// stable identity anchors (especially important for URI normalization and
/// sidecar location derivation).
fn resolve_workspace_root(input: &Path) -> Result<PathBuf> {
    let workspace = if input.is_absolute() {
        input.to_path_buf()
    } else {
        std::env::current_dir()?.join(input)
    }
    .clean();

    if workspace.exists() {
        return Ok(std::fs::canonicalize(workspace)?);
    }

    Err(anyhow!("workspace does not exist: {}", workspace.display()))
}

/// Resolve config path relative to workspace unless absolute.
///
/// This behavior supports both simple in-repo workflows (`mediapm.json` in
/// workspace root) and advanced setups that reference external config files.
fn resolve_config_path(workspace_root: &Path, config_arg: &Path) -> PathBuf {
    if config_arg.is_absolute() {
        config_arg.to_path_buf()
    } else {
        workspace_root.join(config_arg)
    }
}

fn parse_optional_hash(hash: Option<&str>) -> Result<Option<Blake3Hash>> {
    hash.map(Blake3Hash::from_hex).transpose()
}

fn resolve_edit_kind(kind: CliEditKind, output_present: bool) -> EditKind {
    match kind {
        CliEditKind::Auto => {
            if output_present {
                EditKind::NonRevertable
            } else {
                EditKind::Revertable
            }
        }
        CliEditKind::Revertable => EditKind::Revertable,
        CliEditKind::NonRevertable => EditKind::NonRevertable,
    }
}

fn parse_patch_json_object(raw: Option<&str>) -> Result<serde_json::Value> {
    match raw {
        Some(raw_patch) => {
            let patch_value: serde_json::Value = serde_json::from_str(raw_patch)
                .map_err(|error| anyhow!("invalid --patch-json payload: {error}"))?;
            if !patch_value.is_object() {
                return Err(anyhow!(
                    "--patch-json must deserialize to a JSON object (for metadata overlay)"
                ));
            }

            Ok(patch_value)
        }
        None => Ok(serde_json::json!({})),
    }
}

fn parse_optional_json(raw: Option<&str>, arg_name: &str) -> Result<serde_json::Value> {
    match raw {
        Some(raw_json) => serde_json::from_str(raw_json)
            .map_err(|error| anyhow!("invalid {} payload: {}", arg_name, error)),
        None => Ok(serde_json::json!({})),
    }
}

fn merge_message_into_details(
    details: serde_json::Value,
    message: Option<String>,
) -> Result<serde_json::Value> {
    let mut merged = if details.is_object() {
        details
    } else {
        serde_json::json!({ "caller_details": details })
    };

    if let Some(message) = message {
        let object = merged
            .as_object_mut()
            .ok_or_else(|| anyhow!("details root must be JSON object after normalization"))?;
        object.insert("message".to_owned(), serde_json::Value::String(message));
    }

    Ok(merged)
}
