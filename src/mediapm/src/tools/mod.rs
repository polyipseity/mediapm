//! Unified managed-tool catalog and downloader runtime.
//!
//! mediapm tool provisioning is intentionally grouped under one folder module
//! so callers can reason about catalog metadata, release resolution, transfer
//! behavior, and payload materialization in one place.

pub(crate) mod catalog;
pub(crate) mod downloader;
pub(crate) mod workflows;
