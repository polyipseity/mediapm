//! Default constant values for mediapm configuration fields.
//!
//! These constants supply the default values used by `#[serde(default)]`
//! throughout the config layer. Boundary callers may also reference them
//! directly when constructing config objects outside deserialization.

use super::MaterializationMethod;
use super::hierarchy_types::SanitizeNamesConfig;

/// Current persisted schema marker for `mediapm.ncl`.
pub const MEDIAPM_DOCUMENT_VERSION: u32 = 1;

/// Serde default function for `version` field.
#[must_use]
pub fn default_mediapm_document_version() -> u32 {
    MEDIAPM_DOCUMENT_VERSION
}

/// Default max number of ffmpeg indexed input slots when `tools.ffmpeg`
/// does not provide an explicit override.
pub const DEFAULT_FFMPEG_MAX_INPUT_SLOTS: u32 = 16;

/// Serde default function for max ffmpeg input slots.
#[must_use]
pub fn default_ffmpeg_max_input_slots() -> u32 {
    DEFAULT_FFMPEG_MAX_INPUT_SLOTS
}

/// Default max number of ffmpeg indexed output slots when `tools.ffmpeg`
/// does not provide an explicit override.
pub const DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS: u32 = 4;

/// Serde default function for max ffmpeg output slots.
#[must_use]
pub fn default_ffmpeg_max_output_slots() -> u32 {
    DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS
}

/// Default runtime materialization fallback order.
///
/// The order is intentionally deterministic so managed-file realization remains
/// predictable across hosts and repeated sync runs.
pub const DEFAULT_MATERIALIZATION_PREFERENCE_ORDER: [MaterializationMethod; 4] = [
    MaterializationMethod::Hardlink,
    MaterializationMethod::Symlink,
    MaterializationMethod::Reflink,
    MaterializationMethod::Copy,
];

/// Default verify-on-read strategy list.
pub const DEFAULT_VERIFY_ON_READ: [&str; 2] = ["modified", "sample"];

/// Default verify-on-read sampling denominator (1 out of N reads).
pub const DEFAULT_VERIFY_ON_READ_SAMPLE_DENOMINATOR: u64 = 100;

/// Default stale timeout (seconds) for the "stale" verify-on-read strategy.
pub const DEFAULT_VERIFY_ON_READ_STALE_TIMEOUT_SECS: u64 = 604_800;

/// Default reconstructed cache TTL (seconds).
/// Renamed from `DEFAULT_RECONSTRUCTED_BYTES_CACHE_TTL_SECS` to match CAS crate naming.
pub const DEFAULT_RECONSTRUCTED_CACHE_TTL_SECONDS: u64 = 3600;

/// Default instance GC time-to-live in seconds (7 days).
pub const DEFAULT_INSTANCE_TTL_SECONDS: u64 = 604_800;

/// Default profiler enabled state.
pub const DEFAULT_PROFILER_ENABLED: bool = false;

/// Default verify-materialization state.
pub const DEFAULT_VERIFY_MATERIALIZATION: bool = false;

/// Current persisted schema marker for `state.json`.
pub const MEDIAPM_STATE_VERSION: u32 = 2;

/// Serde default function for state `version` field.
#[must_use]
pub fn default_mediapm_state_version() -> u32 {
    MEDIAPM_STATE_VERSION
}

/// Default retry-impure flag.
pub const DEFAULT_RETRY_IMPURE: bool = false;

/// Default path sanitization mode.
#[must_use]
pub fn default_path_sanitization() -> SanitizeNamesConfig {
    SanitizeNamesConfig::Inherit
}

/// Serde default function returning the materialization preference order.
#[must_use]
pub fn default_materialization_preference_order() -> Vec<MaterializationMethod> {
    DEFAULT_MATERIALIZATION_PREFERENCE_ORDER.to_vec()
}

/// Serde default function returning the verify-on-read strategy list.
#[must_use]
pub fn default_verify_on_read() -> Vec<String> {
    DEFAULT_VERIFY_ON_READ.iter().map(|&s| s.to_string()).collect()
}

/// Serde default function returning the verify-on-read sampling denominator.
#[must_use]
pub fn default_verify_on_read_sample_denominator() -> u64 {
    DEFAULT_VERIFY_ON_READ_SAMPLE_DENOMINATOR
}

/// Serde default function returning the verify-on-read stale timeout seconds.
#[must_use]
pub fn default_verify_on_read_stale_timeout_secs() -> u64 {
    DEFAULT_VERIFY_ON_READ_STALE_TIMEOUT_SECS
}

/// Serde default function returning the reconstructed cache TTL seconds.
#[must_use]
pub fn default_reconstructed_cache_ttl_seconds() -> u64 {
    DEFAULT_RECONSTRUCTED_CACHE_TTL_SECONDS
}

/// Serde default function returning the instance TTL seconds.
#[must_use]
pub fn default_instance_ttl_seconds() -> u64 {
    DEFAULT_INSTANCE_TTL_SECONDS
}

/// Serde default function returning the profiler enabled state.
#[must_use]
pub fn default_profiler_enabled() -> bool {
    DEFAULT_PROFILER_ENABLED
}

/// Serde default function returning the verify-materialization state.
#[must_use]
pub fn default_verify_materialization() -> bool {
    DEFAULT_VERIFY_MATERIALIZATION
}

/// Serde default function returning the retry-impure flag.
#[must_use]
pub fn default_retry_impure() -> bool {
    DEFAULT_RETRY_IMPURE
}
