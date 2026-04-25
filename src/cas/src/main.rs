//! Binary entrypoint for the Phase 1 `mediapm-cas` CLI.
//!
//! This CLI owns direct CAS operations so higher-level crates can forward
//! commands without re-implementing CAS argument parsing/execution behavior.

use std::collections::BTreeSet;
use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;

use clap::{Args, Parser, Subcommand};
use mediapm_cas::{
    CasApi, CasMaintenanceApi, CasVisualizeRequest, Constraint, FileSystemCas, Hash,
    OptimizeOptions, run_visualize_command,
};

/// Top-level `mediapm-cas` CLI arguments.
#[derive(Debug, Parser)]
#[command(author, version, about = "mediapm phase-1 CAS CLI")]
struct Cli {
    /// Top-level CAS command selector.
    #[command(subcommand)]
    command: CasCommand,
}

/// Phase 1 CAS subcommands.
#[derive(Debug, Subcommand)]
enum CasCommand {
    /// Stores a file in CAS and prints its BLAKE3 hash.
    Store(CasStoreArgs),
    /// Reconstructs data by hash and writes to stdout or file.
    Get(CasGetArgs),
    /// Constraint management commands.
    Constraint {
        /// Constraint-management subcommand selector.
        #[command(subcommand)]
        command: CasConstraintCommand,
    },
    /// Runs one optimizer pass.
    Optimize(CasRootArgs),
    /// Prunes dangling constraint candidates.
    Prune(CasRootArgs),
    /// Rebuilds durable CAS index metadata from the object store.
    RepairIndex(CasRootArgs),
    /// Migrates durable CAS index metadata to one schema version.
    MigrateIndex(CasMigrateIndexArgs),
    /// Visualizes object/base/constraint topology of a CAS repository.
    Visualize(CasVisualizeArgs),
}

/// `cas store` arguments.
#[derive(Debug, Args)]
struct CasStoreArgs {
    /// Input file path to import.
    file: PathBuf,
    /// CAS root path (default: `.mediapm/cas`).
    #[arg(long)]
    root: Option<PathBuf>,
}

/// `cas get` arguments.
#[derive(Debug, Args)]
struct CasGetArgs {
    /// Hash string of the target object (for example `blake3:<hex>`).
    hash: String,
    /// Optional output file path. If omitted, bytes are written to stdout.
    #[arg(long)]
    output: Option<PathBuf>,
    /// CAS root path (default: `.mediapm/cas`).
    #[arg(long)]
    root: Option<PathBuf>,
}

/// Shared root argument structure.
#[derive(Debug, Args)]
struct CasRootArgs {
    /// CAS root path (default: `.mediapm/cas`).
    #[arg(long)]
    root: Option<PathBuf>,
}

/// `cas migrate-index` arguments.
#[derive(Debug, Args)]
struct CasMigrateIndexArgs {
    /// Target schema version marker.
    #[arg(long)]
    version: u32,
    /// CAS root path (default: `.mediapm/cas`).
    #[arg(long)]
    root: Option<PathBuf>,
}

/// Supported output formats for `cas visualize`.
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum CasVisualizeFormat {
    /// Mermaid flowchart markup.
    Mermaid,
    /// Pretty JSON topology snapshot.
    Json,
    /// Human-readable text report.
    Text,
}

/// `cas visualize` arguments.
#[derive(Debug, Args)]
struct CasVisualizeArgs {
    /// CAS root path (default: `.mediapm/cas`).
    #[arg(long)]
    root: Option<PathBuf>,
    /// Output visualization format.
    #[arg(long, value_enum, default_value_t = CasVisualizeFormat::Mermaid)]
    format: CasVisualizeFormat,
    /// Include canonical empty-content object in output.
    #[arg(long, default_value_t = false)]
    include_empty: bool,
    /// Optional output file path. If omitted, writes to stdout.
    #[arg(long)]
    output: Option<PathBuf>,
}

/// `cas constraint` subcommands.
#[derive(Debug, Subcommand)]
enum CasConstraintCommand {
    /// Adds/updates base candidates for a target hash.
    Add(CasConstraintAddArgs),
}

/// `cas constraint add` arguments.
#[derive(Debug, Args)]
struct CasConstraintAddArgs {
    /// Target object hash (for example `blake3:<hex>`).
    hash: String,
    /// Comma-separated base hash candidates.
    #[arg(long)]
    bases: String,
    /// CAS root path (default: `.mediapm/cas`).
    #[arg(long)]
    root: Option<PathBuf>,
}

#[tokio::main]
/// Parses CLI args and executes the selected CAS command.
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    run_cas_command(cli.command).await
}

/// Executes one Phase 1 CAS command variant.
async fn run_cas_command(command: CasCommand) -> anyhow::Result<()> {
    match command {
        CasCommand::Store(args) => {
            let root = resolve_cas_root(args.root);
            let cas = FileSystemCas::open(&root).await?;
            let bytes = tokio::fs::read(&args.file).await?;
            let hash = cas.put(bytes).await?;
            println!("{hash}");
        }
        CasCommand::Get(args) => {
            let root = resolve_cas_root(args.root);
            let cas = FileSystemCas::open(&root).await?;
            let hash = Hash::from_str(&args.hash)?;
            let bytes = cas.get(hash).await?;
            if let Some(output) = args.output {
                if let Some(parent) = output.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                tokio::fs::write(output, &bytes).await?;
            } else {
                std::io::stdout().write_all(&bytes)?;
            }
        }
        CasCommand::Constraint { command } => match command {
            CasConstraintCommand::Add(args) => {
                let root = resolve_cas_root(args.root);
                let cas = FileSystemCas::open(&root).await?;
                let target_hash = Hash::from_str(&args.hash)?;
                let potential_bases = parse_hash_list(&args.bases)?;
                cas.set_constraint(Constraint { target_hash, potential_bases }).await?;
                println!("updated constraint for {target_hash}");
            }
        },
        CasCommand::Optimize(args) => {
            let root = resolve_cas_root(args.root);
            let cas = FileSystemCas::open(&root).await?;
            let report = cas.optimize_once(OptimizeOptions::default()).await?;
            println!("rewritten_objects={}", report.rewritten_objects);
        }
        CasCommand::Prune(args) => {
            let root = resolve_cas_root(args.root);
            let cas = FileSystemCas::open(&root).await?;
            let report = cas.prune_constraints().await?;
            println!("removed_candidates={}", report.removed_candidates);
        }
        CasCommand::RepairIndex(args) => {
            let root = resolve_cas_root(args.root);
            let cas = FileSystemCas::open(&root).await?;
            let report = cas.repair_index().await?;
            println!(
                concat!(
                    "object_rows_rebuilt={} ",
                    "explicit_constraint_rows_restored={} ",
                    "scanned_object_files={} ",
                    "skipped_object_files={} ",
                    "backup_snapshots_considered={} ",
                    "constraint_source={:?}"
                ),
                report.object_rows_rebuilt,
                report.explicit_constraint_rows_restored,
                report.scanned_object_files,
                report.skipped_object_files,
                report.backup_snapshots_considered,
                report.constraint_source,
            );
        }
        CasCommand::MigrateIndex(args) => {
            let root = resolve_cas_root(args.root);
            let cas = FileSystemCas::open(&root).await?;
            cas.migrate_index_to_version(args.version).await?;
            println!("migrated_index_schema_version={}", args.version);
        }
        CasCommand::Visualize(args) => {
            let request = CasVisualizeRequest {
                root: resolve_cas_root(args.root),
                format: args.format.into(),
                include_empty: args.include_empty,
                output: args.output,
            };
            run_visualize_command(request).await?;
        }
    }

    Ok(())
}

impl From<CasVisualizeFormat> for mediapm_cas::CasVisualizeFormat {
    /// Converts CLI-local format flags to library visualization format enum.
    fn from(value: CasVisualizeFormat) -> Self {
        match value {
            CasVisualizeFormat::Mermaid => Self::Mermaid,
            CasVisualizeFormat::Json => Self::Json,
            CasVisualizeFormat::Text => Self::Text,
        }
    }
}

/// Resolves default filesystem CAS root.
fn resolve_cas_root(root: Option<PathBuf>) -> PathBuf {
    root.unwrap_or_else(|| PathBuf::from(".mediapm/cas"))
}

/// Parses comma-delimited hash list into stable set.
fn parse_hash_list(input: &str) -> anyhow::Result<BTreeSet<Hash>> {
    let values = input
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(Hash::from_str)
        .collect::<Result<BTreeSet<_>, _>>()?;

    if values.is_empty() {
        anyhow::bail!("--bases must include at least one hash");
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use mediapm_cas::Hash;

    use super::parse_hash_list;

    /// Verifies comma-delimited hash parsing accepts multiple entries.
    #[test]
    fn parse_hash_list_accepts_comma_delimited_values() {
        let a = Hash::from_content(b"a");
        let b = Hash::from_content(b"b");
        let input = format!("{a},{b}");

        let parsed = parse_hash_list(&input).expect("parse hash list");
        assert_eq!(parsed.len(), 2);
        assert!(parsed.contains(&a));
        assert!(parsed.contains(&b));
    }

    /// Verifies empty `--bases` input produces diagnostic error text.
    #[test]
    fn parse_hash_list_rejects_empty_input() {
        let err = parse_hash_list(" , ").expect_err("empty values should fail");
        assert!(err.to_string().contains("--bases must include at least one hash"));
    }
}
