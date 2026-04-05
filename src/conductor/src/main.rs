//! Binary entrypoint for the `mediapm-conductor` CLI.

/// Runs conductor CLI and exits with non-zero status on failure.
#[tokio::main]
async fn main() {
    if let Err(err) = mediapm_conductor::cli::run_from_env().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
