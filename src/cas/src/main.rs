//! Binary entrypoint for the `mediapm-cas` CLI.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    mediapm_cas::cli::run_from_env().await
}
