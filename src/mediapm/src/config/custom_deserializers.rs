//! Custom Serde deserializer functions for mediapm Nickel config values.
//!
//! These helpers bridge the gap between Nickel's floating-point export and
//! Rust's integer-only struct fields.  Nickel evaluates all numeric literals
//! as `Number` (f64-compatible), so direct `u64`/`u32` deserialization would
//! reject config values like `recheck_seconds = 3600`.

use serde::{Deserialize, Deserializer};
use serde_json::Value;

use super::nickel_io::{parse_non_negative_integral_u32, parse_non_negative_integral_u64};

/// Deserializes one non-negative integral number into `u64`.
#[allow(dead_code)]
pub fn deserialize_u64_from_number<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;

    if let Some(raw) = value.as_u64() {
        return Ok(raw);
    }
    if let Some(raw) = value.as_f64()
        && let Some(normalized) = parse_non_negative_integral_u64(raw)
    {
        return Ok(normalized);
    }

    Err(serde::de::Error::custom("expected one non-negative integral number representable as u64"))
}

/// Deserializes one non-negative integral number into `u32`.
pub fn deserialize_u32_from_number<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;

    if let Some(raw) = value.as_u64() {
        return u32::try_from(raw).map_err(|_| {
            serde::de::Error::custom(
                "expected one non-negative integral number representable as u32",
            )
        });
    }
    if let Some(raw) = value.as_f64()
        && let Some(normalized) = parse_non_negative_integral_u32(raw)
    {
        return Ok(normalized);
    }

    Err(serde::de::Error::custom("expected one non-negative integral number representable as u32"))
}

/// Deserializes an `Option<String>` that rejects the empty string.
///
/// Resolved tool fields (`resolved_tag`, `resolved_version`,
/// `resolved_vcs_hash`) are `Option<String>` under the no-backwards-compat
/// schema: empty is represented as `null` (`None`), never `""`. A JSON
/// `""` value is invalid and rejected at load — stale state files are
/// discarded and regenerated, never normalized.
pub fn deserialize_optional_nonempty_string<'de, D>(
    deserializer: D,
) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    match value {
        Some(s) if s.is_empty() => {
            Err(serde::de::Error::custom("empty string is invalid; use null instead"))
        }
        other => Ok(other),
    }
}
