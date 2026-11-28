//! Conductor-owned secret material for keyed instance derivation.
//!
//! This module handles bytes and policy only. It never logs secret material
//! and its `Debug` implementation is redacted.

use std::fmt;

use rand::RngCore;
use rand::rngs::OsRng;

use crate::error::ConductorError;
use crate::keyed_hash::KeyedHash;

/// Secret material for keyed instance keys.
#[derive(Clone, PartialEq, Eq)]
pub struct InstanceKeySecret([u8; 32]);

impl InstanceKeySecret {
    /// Generates a secret from the operating system CSPRNG.
    ///
    /// A draw that repeats any byte value more than three times is discarded and
    /// replaced with a fresh `OsRng` draw until the shared sanity check passes.
    ///
    /// # Errors
    ///
    /// Reserved for entropy failures; the current implementation always returns `Ok`.
    /// Redraws are silent and bounded only by the sampler reaching an acceptable draw.
    pub fn generate() -> Result<Self, ConductorError> {
        loop {
            let mut bytes = [0u8; 32];
            OsRng.fill_bytes(&mut bytes);
            if Self::has_acceptable_byte_distribution(&bytes) {
                return Ok(Self(bytes));
            }
        }
    }

    /// Applies the byte-frequency sanity check shared by generation and parsing.
    fn has_acceptable_byte_distribution(bytes: &[u8; 32]) -> bool {
        let mut counts = [0u8; 256];
        for byte in *bytes {
            counts[usize::from(byte)] = counts[usize::from(byte)].saturating_add(1);
        }
        counts.into_iter().all(|count| count <= 3)
    }

    /// Builds a secret from already-decoded test material.
    #[cfg(test)]
    #[must_use]
    pub const fn from_bytes_for_test(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Parses the exact generated format, including the entropy sanity check.
    ///
    /// # Errors
    ///
    /// Returns `ConductorError::Internal` for non-UTF-8 input, a malformed
    /// generated format, or a byte distribution that fails the sanity check.
    pub fn parse_generated(bytes: &[u8]) -> Result<Self, ConductorError> {
        let secret = Self::parse_relaxed(bytes)?;
        let text = std::str::from_utf8(bytes)
            .map_err(|_| ConductorError::Internal("instance key secret is not UTF-8".to_owned()))?;
        if text.len() != 65 || !text.ends_with('\n') {
            return Err(ConductorError::Internal(
                "instance key secret must be 64 lowercase hex characters plus one newline"
                    .to_owned(),
            ));
        }
        if !text[..64].bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) {
            return Err(ConductorError::Internal(
                "instance key secret must use lowercase hex".to_owned(),
            ));
        }
        if !Self::has_acceptable_byte_distribution(&secret.0) {
            return Err(ConductorError::Internal(
                "instance key secret fails the min-entropy sanity check".to_owned(),
            ));
        }
        Ok(secret)
    }

    /// Parses 64 hex characters into 32 bytes without format or entropy checks.
    ///
    /// # Errors
    ///
    /// Returns `ConductorError::Internal` for non-UTF-8 input or text that is
    /// not exactly 64 hex characters.
    pub fn parse_relaxed(bytes: &[u8]) -> Result<Self, ConductorError> {
        let text = std::str::from_utf8(bytes)
            .map_err(|_| ConductorError::Internal("instance key secret is not UTF-8".to_owned()))?;
        let hex_part = text.trim_end_matches('\n');
        if hex_part.len() != 64 || !hex_part.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(ConductorError::Internal(
                "instance key secret must decode from 64 hex characters".to_owned(),
            ));
        }
        let decoded = crate::hex_codec::decode(hex_part).map_err(|err| {
            ConductorError::Internal(format!("invalid instance key secret: {err}"))
        })?;
        let fixed: [u8; 32] = decoded.try_into().map_err(|_| {
            ConductorError::Internal("instance key secret must decode to 32 bytes".to_owned())
        })?;
        Ok(Self(fixed))
    }

    /// Renders the generated file bytes.
    #[must_use]
    pub fn to_generated_bytes(&self) -> Vec<u8> {
        let mut bytes = crate::hex_codec::encode(&self.0).into_bytes();
        bytes.push(b'\n');
        bytes
    }

    /// Returns the raw key for BLAKE3 keyed mode.
    #[must_use]
    pub const fn key_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Returns a non-secret verifier for the state file.
    #[must_use]
    pub fn verifier(&self) -> KeyedHash {
        KeyedHash::hash(self, "mediapm/conductor/instance key secret verifier/v1", &self.0)
    }
}

impl fmt::Debug for InstanceKeySecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InstanceKeySecret(<redacted>)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_secret_round_trips_under_strict_parse() {
        for _ in 0..256 {
            let secret = InstanceKeySecret::generate().expect("secret generates");
            let encoded = secret.to_generated_bytes();
            assert_eq!(encoded.len(), 65);
            assert!(encoded.ends_with(b"\n"));
            assert_eq!(
                InstanceKeySecret::parse_generated(&encoded)
                    .expect("generated secret parses strictly"),
                secret
            );
        }
    }

    #[test]
    fn byte_distribution_check_allows_three_repeats_and_rejects_four() {
        let mut bytes = [0u8; 32];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::try_from(index / 3).expect("index fits in u8");
        }
        assert!(InstanceKeySecret::has_acceptable_byte_distribution(&bytes));
        bytes[31] = 0;
        assert!(!InstanceKeySecret::has_acceptable_byte_distribution(&bytes));
    }

    #[test]
    fn verifier_is_stable_per_secret_and_differs_between_secrets() {
        let secret = InstanceKeySecret::from_bytes_for_test([0x11u8; 32]);
        let other = InstanceKeySecret::from_bytes_for_test([0x22u8; 32]);
        assert_eq!(secret.verifier(), secret.verifier());
        assert_ne!(secret.verifier(), other.verifier());
    }

    #[test]
    fn strict_parse_rejects_low_entropy_secret() {
        let bytes = b"0000000000000000000000000000000000000000000000000000000000000000\n";
        assert!(InstanceKeySecret::parse_generated(bytes).is_err());
        assert!(InstanceKeySecret::parse_relaxed(bytes).is_ok());
    }

    #[test]
    fn strict_parse_rejects_missing_newline_and_uppercase() {
        let no_newline = b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        assert!(InstanceKeySecret::parse_generated(no_newline).is_err());
        assert!(InstanceKeySecret::parse_relaxed(no_newline).is_ok());
        let uppercase = b"0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF\n";
        assert!(InstanceKeySecret::parse_generated(uppercase).is_err());
        assert!(InstanceKeySecret::parse_relaxed(uppercase).is_ok());
    }

    #[test]
    fn debug_output_does_not_contain_secret_bytes() {
        let secret = InstanceKeySecret::from_bytes_for_test([0xab; 32]);
        let rendered = format!("{secret:?}");
        assert!(!rendered.contains("abab"));
    }
}
