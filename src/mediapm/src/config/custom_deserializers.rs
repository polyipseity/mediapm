//! Custom Serde deserializer functions for mediapm Nickel config values.
//!
//! These helpers bridge the gap between Nickel's floating-point export and
//! Rust's integer-only struct fields.  Nickel evaluates all numeric literals
//! as `Number` (f64-compatible), so direct `u64`/`u32` deserialization would
//! reject config values like `recheck_seconds = 3600`.

use serde::{Deserialize, Deserializer};
use serde_json::Value;

use super::ToolRecheck;
use super::nickel_io::{parse_non_negative_integral_u32, parse_non_negative_integral_u64};

/// Deserializes one non-negative integral number into `u64`.
///
/// # Errors
///
/// Returns a deserialization error when the value is not a non-negative
/// integral number representable as `u64` (negative, fractional, non-numeric,
/// or out of range). Wired by name from a `#[serde(deserialize_with = ...)]`
/// attribute, so the generated call is invisible to the compiler's
/// reachability analysis.
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
///
/// # Errors
///
/// Returns a deserialization error when the value is not a non-negative
/// integral number representable as `u32` (negative, fractional, non-numeric,
/// or out of range).
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
///
/// # Errors
///
/// Returns a deserialization error when the value is the empty string; `null`
/// decodes to `None` and every other string is accepted as-is.
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

/// Deserializes `recheck_seconds` from the three forms a document may state: the
/// string `always`, the string `never`, or a positive integer of seconds.
///
/// A fraction, a zero, a negative, or any other string is refused rather than
/// mapped onto a nearby meaning: `0` in particular used to mean "use the
/// heuristic", and silently reading it as `always` would change when every
/// existing document resolves. The error names the field as a document writes it,
/// so a reader is not left guessing which of several policy fields is wrong.
pub(crate) fn deserialize_recheck<'de, D>(deserializer: D) -> Result<ToolRecheck, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::String(s) if s == "always" => Ok(ToolRecheck::Always),
        serde_json::Value::String(s) if s == "never" => Ok(ToolRecheck::Never),
        serde_json::Value::Number(n) => {
            let seconds = n.as_u64().ok_or_else(|| {
                serde::de::Error::custom(format!(
                    "recheck_seconds must be a whole number of seconds, got {n}"
                ))
            })?;
            if seconds == 0 {
                return Err(serde::de::Error::custom(
                    "recheck_seconds = 0 is meaningless; state \"always\" to re-resolve every sync",
                ));
            }
            Ok(ToolRecheck::AfterSeconds(seconds))
        }
        other => Err(serde::de::Error::custom(format!(
            "recheck_seconds must be \"always\", \"never\", or a positive number of seconds, got {other}"
        ))),
    }
}

/// Writes `recheck_seconds` as one of the three forms the deserializer reads, so
/// an encode and a decode are inverses on every legal value.
pub(crate) fn serialize_recheck<S>(value: &ToolRecheck, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match value {
        ToolRecheck::Always => serializer.serialize_str("always"),
        ToolRecheck::Never => serializer.serialize_str("never"),
        ToolRecheck::AfterSeconds(seconds) => serializer.serialize_u64(*seconds),
    }
}

/// Serde module wiring the hand-written `recheck_seconds` codec onto a field.
pub(crate) mod tool_recheck_wire {
    use super::{ToolRecheck, deserialize_recheck, serialize_recheck};

    /// Deserializes one `recheck_seconds` value.
    pub(crate) fn deserialize<'de, D>(d: D) -> Result<ToolRecheck, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserialize_recheck(d)
    }

    /// Serializes one `recheck_seconds` value.
    pub(crate) fn serialize<S>(v: &ToolRecheck, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serialize_recheck(v, s)
    }
}

impl ToolRecheck {
    /// Whether this is the value the encoder omits.
    #[must_use]
    pub(crate) const fn is_default(&self) -> bool {
        matches!(self, Self::Always)
    }
}
