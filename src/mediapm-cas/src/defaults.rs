//! Centralized default values for CAS configurable parameters.
//!
//! Internal code never supplies defaults inline — boundary callers (CLI,
//! library entry points) apply these before passing concrete values
//! downward. This file is the single source of truth for all defaults.

use std::time::Duration;

use crate::config::{CasIntegrityConfig, CasLocatorParseOptions};

/// Default integrity config: no verification.
pub const INTEGRITY_CONFIG: CasIntegrityConfig = CasIntegrityConfig { verify_on_read: Vec::new() };

/// Default locator parse options: accept plain filesystem paths.
pub const LOCATOR_PARSE_OPTIONS: CasLocatorParseOptions =
    CasLocatorParseOptions { allow_plain_filesystem_path: true };

/// Default reconstructed-bytes cache TTL (60 seconds).
pub const CACHE_TTL: Duration = Duration::from_secs(60);

/// Default maximum WAL segment size (64 MiB).
pub const WAL_MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;
