use clap::{Parser, Subcommand};
use mediapm::{MediaPmApi, MediaPmService};

/// `mediapm` phase-3 CLI scaffold.
#[derive(Debug, Parser)]
#[command(author, version, about = "mediapm phase-3 orchestration CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// Supported top-level commands.
#[derive(Debug, Subcommand)]
enum Command {
    /// Dry-run style orchestration planning scaffold.
    Plan,
    /// Run a minimal sync cycle through conductor.
    Sync,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let service = MediaPmService::new_in_memory();

    match cli.command {
        Command::Plan => {
            let summary = service.sync_library().await?;
            println!("plan scaffold ready (executed_instances={})", summary.executed_instances);
        }
        Command::Sync => {
            let summary = service.sync_library().await?;
            println!("sync scaffold complete (executed_instances={})", summary.executed_instances);
        }
    }

    Ok(())
}
