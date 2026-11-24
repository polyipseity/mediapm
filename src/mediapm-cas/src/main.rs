//! Binary entrypoint for the `mediapm-cas` CLI.

fn main() -> anyhow::Result<()> {
    let worker = std::thread::Builder::new().stack_size(32 * 1024 * 1024).spawn(run)?;
    worker.join().expect("the mediapm-cas worker thread panicked")
}

/// Builds the multi-thread Tokio runtime and runs the async CAS CLI.
fn run() -> anyhow::Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .thread_stack_size(32 * 1024 * 1024)
        .enable_all()
        .build()?
        .block_on(async_main())
}

/// Delegates to the CAS CLI implementation.
async fn async_main() -> anyhow::Result<()> {
    mediapm_cas::cli::run_from_env().await
}
