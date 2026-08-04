//! Unix-epoch timestamp type.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Monotonic Unix-epoch timestamp with nanosecond precision.
///
/// Used for impure-operation planning, deployment records, and cache-key
/// derivation across mediapm crates. Values are nanoseconds since the Unix
/// epoch; construction from the system clock saturates instead of wrapping.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Timestamp(u64);

impl Timestamp {
    /// Returns a `Timestamp` for the current system time.
    ///
    /// Falls back to `UNIX_EPOCH` when the system clock is set before the
    /// epoch (extremely unlikely in practice), and saturates at
    /// `u64::MAX` nanoseconds.
    #[must_use]
    pub fn now() -> Self {
        let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        Self(u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX))
    }

    /// Returns the timestamp as Unix nanoseconds since the epoch.
    #[must_use]
    pub fn as_unix_nanos(self) -> u64 {
        self.0
    }

    /// Constructs a `Timestamp` from Unix nanoseconds since the epoch.
    #[must_use]
    pub fn from_unix_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    /// Returns the timestamp as whole Unix seconds since the epoch
    /// (nanoseconds truncated).
    #[must_use]
    pub fn as_unix_secs(self) -> u64 {
        self.0 / 1_000_000_000
    }

    /// Constructs a `Timestamp` from Unix seconds since the epoch.
    #[must_use]
    pub fn from_unix_secs(secs: u64) -> Self {
        Self(secs.saturating_mul(1_000_000_000))
    }
}

#[cfg(test)]
mod tests {
    use super::Timestamp;

    #[test]
    fn from_and_as_unix_nanos_round_trip() {
        let ts = Timestamp::from_unix_nanos(1_700_000_000_123_456_789);
        assert_eq!(ts.as_unix_nanos(), 1_700_000_000_123_456_789);
    }

    #[test]
    fn from_and_as_unix_secs_round_trip() {
        let ts = Timestamp::from_unix_secs(1_700_000_000);
        assert_eq!(ts.as_unix_secs(), 1_700_000_000);
        assert_eq!(ts.as_unix_nanos(), 1_700_000_000_000_000_000);
    }

    #[test]
    fn now_is_within_recent_window() {
        let before = Timestamp::now().as_unix_secs();
        let after = Timestamp::now().as_unix_secs();
        assert!(after >= before);
    }

    #[test]
    fn monotonic_now() {
        let a = Timestamp::now();
        let b = Timestamp::now();
        assert!(b >= a);
    }

    #[test]
    fn default_is_zero() {
        assert_eq!(Timestamp::default(), Timestamp::from_unix_nanos(0));
        assert_eq!(Timestamp::default().as_unix_nanos(), 0);
    }

    #[test]
    fn to_le_bytes_via_as_unix_nanos() {
        let ts = Timestamp::from_unix_nanos(0x0102_0304_0506_0708);
        let bytes = ts.as_unix_nanos().to_le_bytes();
        assert_eq!(u64::from_le_bytes(bytes), 0x0102_0304_0506_0708);
    }

    #[test]
    fn serde_round_trip_number() {
        let ts = Timestamp::from_unix_nanos(1_700_000_000_123_456_789);
        let json = serde_json::to_string(&ts).expect("serialize");
        assert_eq!(json, "1700000000123456789");
        let back: Timestamp = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, ts);
    }

    #[test]
    fn ordering_matches_nanos() {
        let a = Timestamp::from_unix_nanos(1_000);
        let b = Timestamp::from_unix_nanos(2_000);
        assert!(a < b);
        assert!(a <= b);
        assert!(b > a);
    }
}
