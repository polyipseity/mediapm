mod api_workflows;

/// The binary's own entry point reaches its subcommands (cli-only: the test
/// spawns the binary, which Cargo builds only when the `cli` feature is on).
#[cfg(feature = "cli")]
mod cli_entry_point;
mod concurrent;
mod constraints;
mod contention;
mod in_memory;
mod maintenance;
mod read_view;
mod reconstructed_cache;
mod store_teardown;
mod streaming_large;
mod wal_consumer;
