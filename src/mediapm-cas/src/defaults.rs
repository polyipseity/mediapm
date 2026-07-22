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

/// Default reconstructed-bytes cache TTL (1 minute).
pub const CACHE_TTL: Duration = Duration::from_mins(1);

/// Default maximum WAL segment size (64 MiB).
pub const WAL_MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

/// Default threshold for inlining data in WAL entries (1 MiB).
/// Objects larger than this use external payload storage.
/// All tool binaries (~8-50 MiB) exceed this threshold, so they are stored as
/// `PutLarge` with zero inline data in the pending map, preventing memory
/// accumulation during WAL replay and consumer operations.
pub const WAL_INLINE_THRESHOLD: u64 = 1024 * 1024;

/// Default buffer size for streaming I/O (256 KiB).
pub const OBJECT_STREAM_BUFFER_SIZE: u32 = 262_144;

/// Default threshold for delta compression (16 MiB).
/// Objects larger than this are never delta-compressed.
pub const DELTA_THRESHOLD: u64 = 16 * 1024 * 1024;

/// Default maximum cache size as a fraction of total store size (10%).
pub const CACHE_MAX_FRACTION_OF_TOTAL_SIZE: f64 = 0.10;
