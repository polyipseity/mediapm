//! Multi-step end-to-end service tests.

#[cfg(feature = "cli")]
mod cli_no_progress;
mod conductor_execution;
mod demo_online_hierarchy_materialization;
mod materialization_nfd_metadata;
mod materialization_zip_traversal;
mod media_identity;
mod service;
