//! Managed-tool catalog, downloader, and workflow synthesis.
//!
//! This module groups tool provisioning (catalog metadata, release resolution,
//! download, materialization) and per-tool workflow step synthesis under one
//! folder module so callers reason about them in one place.

pub(crate) mod catalog;
pub(crate) mod downloader;
pub(crate) mod workflows;
