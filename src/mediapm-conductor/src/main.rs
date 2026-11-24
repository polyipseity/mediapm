//! Binary entrypoint for the `mediapm-conductor` CLI.

/// Runs conductor CLI and exits with non-zero status on failure.
fn main() {
    let worker = std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(run)
        .expect("failed to spawn mediapm-conductor worker thread");
    if let Err(payload) = worker.join() {
        std::panic::resume_unwind(payload);
    }
}

/// Builds the multi-thread Tokio runtime and runs the async conductor CLI.
fn run() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_stack_size(32 * 1024 * 1024)
        .enable_all()
        .build()
        .expect("failed to build mediapm-conductor tokio runtime");
    runtime.block_on(async_main());
}

/// Delegates to the conductor CLI implementation.
async fn async_main() {
    if let Err(err) = mediapm_conductor::cli::run_from_env().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
