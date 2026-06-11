//! Custom Serde deserializer functions for mediapm Nickel config values.
//!
//! These helpers bridge the gap between Nickel's floating-point export and
//! Rust's integer-only struct fields.  Nickel evaluates all numeric literals
//! as `Number` (f64-compatible), so direct `u64`/`u32` deserialization would
//! reject config values like `recheck_seconds = 3600`.

use serde::Deserializer;
use serde_json::Value;

use super::nickel_io::{parse_non_negative_integral_u32, parse_non_negative_integral_u64};

/// Deserializes optional `u64` values while accepting integral floating-point
/// numbers exported by Nickel (for example `3600.0`).
pub fn deserialize_optional_u64_from_number<'de, D>(
    deserializer: D,
) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Option::<Value>::deserialize(deserializer)?;
    let Some(raw) = raw else {
        return Ok(None);
    };

    if let Some(value) = raw.as_u64() {
        return Ok(Some(value));
    }

    if let Some(value) = raw.as_f64()
        && let Some(normalized) = parse_non_negative_integral_u64(value)
    {
        return Ok(Some(normalized));
    }

    Err(serde::de::Error::custom("recheck_seconds must be a non-negative integer"))
}

/// Deserializes optional runtime slot-count `u32` values while accepting
/// integral floating-point numbers exported by Nickel (for example `96.0`).
pub fn deserialize_optional_runtime_slot_count<'de, D>(
    deserializer: D,
) -> Result<Option<u32>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Option::<Value>::deserialize(deserializer)?;
    let Some(raw) = raw else {
        return Ok(None);
    };

    if let Some(value) = raw.as_u64() {
        return u32::try_from(value)
            .map(Some)
            .map_err(|_| serde::de::Error::custom("ffmpeg slot limit must be within u32 range"));
    }

    if let Some(value) = raw.as_f64()
        && let Some(normalized) = parse_non_negative_integral_u32(value)
    {
        return Ok(Some(normalized));
    }

    Err(serde::de::Error::custom("ffmpeg slot limit must be a non-negative integer"))
}
