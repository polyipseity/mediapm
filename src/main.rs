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

use std::{
    path::{Path, PathBuf},
    process::ExitCode,
};

use anyhow::{Result, anyhow};
use clap::{Parser, Subcommand};
use path_clean::PathClean;

use mediapm::{
    application::{
        executor::execute_plan,
        planner::{build_plan, render_plan_human},
    },
    configuration::config::{DEFAULT_CONFIG_FILE, load_config},
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
