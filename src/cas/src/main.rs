//! Binary entrypoint for the Phase 1 `mediapm-cas` CLI.

/// Parses process arguments and executes the CAS CLI.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    mediapm_cas::cli::run_from_env().await
}
