//! Conductor-owned secret material for keyed instance derivation.
//!
//! This module handles bytes and policy only. It never logs secret material
//! and its `Debug` implementation is redacted.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

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

/// Whether derivation uses a secret or runs in non-keyed mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedSecretMode {
    /// Keyed derivation with the loaded or generated secret.
    Keyed(InstanceKeySecret),
    /// Non-keyed derivation.
    Unkeyed,
}

/// CLI/config inputs to secret resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "the policy mirrors four independent switches whose call-site mapping is outside this task"
)]
pub struct InstanceKeySecretPolicy {
    /// Whether keyed mode is enabled.
    pub use_secret: bool,
    /// Whether generated-format and entropy checks apply.
    pub require_generated_format: bool,
    /// Replaces an existing secret when state says no secret was used.
    pub force_generate: bool,
    /// Continues without a secret when state says one was used.
    pub force_disable: bool,
}

/// Distinguishes sibling temporary files created by this process.
static SECRET_TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Returns the sibling secret file path for a conductor state file.
#[must_use]
pub fn secret_path_for_state(state_path: &Path) -> PathBuf {
    state_path.with_file_name("instance-key-secret.hex")
}

/// Publishes secret bytes through a sibling temporary file and rename.
///
/// The temporary is created with `0600` on Unix, and its mode survives the
/// rename. A failed write or rename removes the temporary so a rejected save
/// leaves no half-written secret behind.
///
/// # Errors
///
/// Returns `ConductorError::Internal` when the temporary cannot be created,
/// written, synced, or renamed into place.
fn write_secret_file(path: &Path, bytes: &[u8]) -> Result<(), ConductorError> {
    use std::io::Write;
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt;

    let sequence = SECRET_TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let mut temporary = path.to_path_buf();
    temporary
        .set_file_name(format!(".instance-key-secret.hex.tmp-{}-{sequence}", std::process::id()));

    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);

    let result = (|| {
        let mut file = options.open(&temporary).map_err(|err| {
            ConductorError::Internal(format!("creating instance key secret failed: {err}"))
        })?;
        file.write_all(bytes).map_err(|err| {
            ConductorError::Internal(format!("writing instance key secret failed: {err}"))
        })?;
        file.sync_all().map_err(|err| {
            ConductorError::Internal(format!("syncing instance key secret failed: {err}"))
        })?;
        drop(file);
        std::fs::rename(&temporary, path).map_err(|err| {
            ConductorError::Internal(format!("publishing instance key secret failed: {err}"))
        })?;
        if let Some(parent) = path.parent()
            && let Ok(directory) = std::fs::File::open(parent)
        {
            drop(directory.sync_all());
        }
        Ok(())
    })();

    if result.is_err() {
        drop(std::fs::remove_file(&temporary));
    }
    result
}

/// Resolves keyed or non-keyed mode from state and the secret file.
///
/// Force generation replaces an existing secret and invalidates cached
/// instances. Force disable is available only when no secret file is present.
///
/// # Errors
///
/// Returns `ConductorError::Internal` when the secret is missing, invalid, or
/// inconsistent with the state verifier, or when secret file IO fails.
pub fn resolve_secret_mode(
    policy: InstanceKeySecretPolicy,
    state_path: &Path,
    stored_verifier: Option<KeyedHash>,
) -> Result<(ResolvedSecretMode, Option<KeyedHash>), ConductorError> {
    let secret_path = secret_path_for_state(state_path);
    let present = secret_path.exists();

    if !policy.use_secret {
        let mut messages = Vec::new();
        if present {
            messages
                .push("instance key secret file is present while secrets are disabled".to_owned());
            let bytes = std::fs::read(&secret_path).map_err(|err| {
                ConductorError::Internal(format!("reading instance key secret failed: {err}"))
            })?;
            if InstanceKeySecret::parse_relaxed(&bytes).is_err() {
                messages.push("invalid instance key secret".to_owned());
            }
        }
        if stored_verifier.is_some() && !policy.force_disable {
            messages.push(
                "state used an instance key secret; pass --force-disable-instance-key-secret"
                    .to_owned(),
            );
        }
        if !messages.is_empty() {
            return Err(ConductorError::Internal(messages.join("; ")));
        }
        return Ok((ResolvedSecretMode::Unkeyed, None));
    }

    if stored_verifier.is_none() && policy.force_generate {
        let secret = InstanceKeySecret::generate()?;
        let verifier = secret.verifier();
        write_secret_file(&secret_path, &secret.to_generated_bytes())?;
        return Ok((ResolvedSecretMode::Keyed(secret), Some(verifier)));
    }

    if present {
        let bytes = std::fs::read(&secret_path).map_err(|err| {
            ConductorError::Internal(format!("reading instance key secret failed: {err}"))
        })?;
        let secret = if policy.require_generated_format {
            InstanceKeySecret::parse_generated(&bytes)
        } else {
            InstanceKeySecret::parse_relaxed(&bytes)
        }
        .map_err(|err| ConductorError::Internal(format!("invalid instance key secret: {err}")))?;
        if let Some(expected) = stored_verifier
            && expected != secret.verifier()
        {
            return Err(ConductorError::Internal(
                "instance key secret does not match the state verifier".to_owned(),
            ));
        }
        let verifier = secret.verifier();
        return Ok((ResolvedSecretMode::Keyed(secret), Some(verifier)));
    }

    if stored_verifier.is_some() {
        return Err(ConductorError::Internal(
            "no secret was provided at the path next to state.conductor.json".to_owned(),
        ));
    }

    let secret = InstanceKeySecret::generate()?;
    let verifier = secret.verifier();
    write_secret_file(&secret_path, &secret.to_generated_bytes())?;
    Ok((ResolvedSecretMode::Keyed(secret), Some(verifier)))
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

    // Each byte appears once, so the fixture satisfies the strict entropy check.
    const HIGH_ENTROPY_SECRET_BYTES: [u8; 32] = [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
        0x1e, 0x1f,
    ];

    #[test]
    fn missing_state_and_secret_generates_secret() {
        let root = mediapm_utils::temp::output_dir().expect("temp root");
        let state_path = root.path().join("state.conductor.json");
        let secret_path = secret_path_for_state(&state_path);
        let (mode, verifier) = resolve_secret_mode(
            InstanceKeySecretPolicy {
                use_secret: true,
                require_generated_format: true,
                force_generate: false,
                force_disable: false,
            },
            &state_path,
            None,
        )
        .expect("first run generates");
        let secret = match mode {
            ResolvedSecretMode::Keyed(secret) => secret,
            ResolvedSecretMode::Unkeyed => panic!("secret mode expected"),
        };
        assert!(secret_path.is_file());
        assert_eq!(verifier, Some(secret.verifier()));
    }

    #[test]
    fn enabled_secret_state_without_secret_file_errors() {
        let root = mediapm_utils::temp::output_dir().expect("temp root");
        let state_path = root.path().join("state.conductor.json");
        let secret = InstanceKeySecret::from_bytes_for_test([9; 32]);
        let error = resolve_secret_mode(
            InstanceKeySecretPolicy {
                use_secret: true,
                require_generated_format: true,
                force_generate: false,
                force_disable: false,
            },
            &state_path,
            Some(secret.verifier()),
        )
        .expect_err("missing secret must fail");
        assert!(error.to_string().contains("no secret was provided"), "{error}");
    }

    #[test]
    fn disabled_secret_with_present_secret_file_errors() {
        let root = mediapm_utils::temp::output_dir().expect("temp root");
        let state_path = root.path().join("state.conductor.json");
        let secret_path = secret_path_for_state(&state_path);
        std::fs::write(
            &secret_path,
            InstanceKeySecret::from_bytes_for_test([4; 32]).to_generated_bytes(),
        )
        .unwrap();
        let error = resolve_secret_mode(
            InstanceKeySecretPolicy {
                use_secret: false,
                require_generated_format: true,
                force_generate: false,
                force_disable: false,
            },
            &state_path,
            None,
        )
        .expect_err("present secret must fail");
        assert!(error.to_string().contains("secret file is present"), "{error}");
    }

    #[test]
    fn force_generate_replaces_null_verifier_state() {
        let root = mediapm_utils::temp::output_dir().expect("temp root");
        let state_path = root.path().join("state.conductor.json");
        let (mode, verifier) = resolve_secret_mode(
            InstanceKeySecretPolicy {
                use_secret: true,
                require_generated_format: true,
                force_generate: true,
                force_disable: false,
            },
            &state_path,
            None,
        )
        .expect("force generates");
        assert!(matches!(mode, ResolvedSecretMode::Keyed(_)));
        assert!(verifier.is_some());
    }

    #[test]
    fn valid_verifier_proceeds() {
        let root = mediapm_utils::temp::output_dir().expect("temp root");
        let state_path = root.path().join("state.conductor.json");
        let secret = InstanceKeySecret::from_bytes_for_test(HIGH_ENTROPY_SECRET_BYTES);
        let expected_verifier = secret.verifier();
        std::fs::write(secret_path_for_state(&state_path), secret.to_generated_bytes()).unwrap();
        let (mode, verifier) = resolve_secret_mode(
            InstanceKeySecretPolicy {
                use_secret: true,
                require_generated_format: true,
                force_generate: false,
                force_disable: false,
            },
            &state_path,
            Some(secret.verifier()),
        )
        .expect("a matching secret proceeds");
        assert_eq!(mode, ResolvedSecretMode::Keyed(secret));
        assert_eq!(verifier, Some(expected_verifier));
    }

    #[test]
    fn force_disable_with_non_null_verifier_returns_unkeyed() {
        let root = mediapm_utils::temp::output_dir().expect("temp root");
        let state_path = root.path().join("state.conductor.json");
        let secret = InstanceKeySecret::from_bytes_for_test([6; 32]);
        let (mode, verifier) = resolve_secret_mode(
            InstanceKeySecretPolicy {
                use_secret: false,
                require_generated_format: true,
                force_generate: false,
                force_disable: true,
            },
            &state_path,
            Some(secret.verifier()),
        )
        .expect("force disable proceeds");
        assert_eq!(mode, ResolvedSecretMode::Unkeyed);
        assert_eq!(verifier, None);
    }

    #[test]
    fn force_generate_replaces_existing_secret_file() {
        let root = mediapm_utils::temp::output_dir().expect("temp root");
        let state_path = root.path().join("state.conductor.json");
        let secret_path = secret_path_for_state(&state_path);
        std::fs::write(
            &secret_path,
            InstanceKeySecret::from_bytes_for_test([0x11; 32]).to_generated_bytes(),
        )
        .unwrap();
        let before = std::fs::read(&secret_path).unwrap();
        let (mode, _verifier) = resolve_secret_mode(
            InstanceKeySecretPolicy {
                use_secret: true,
                require_generated_format: true,
                force_generate: true,
                force_disable: false,
            },
            &state_path,
            None,
        )
        .expect("force replaces the file");
        let after = std::fs::read(&secret_path).unwrap();
        assert_ne!(before, after);
        assert!(matches!(mode, ResolvedSecretMode::Keyed(_)));
    }

    #[test]
    fn disabled_secret_with_invalid_present_secret_reports_both_errors() {
        let root = mediapm_utils::temp::output_dir().expect("temp root");
        let state_path = root.path().join("state.conductor.json");
        let secret_path = secret_path_for_state(&state_path);
        std::fs::write(
            &secret_path,
            InstanceKeySecret::from_bytes_for_test([0x22; 32]).to_generated_bytes(),
        )
        .unwrap();
        let secret = InstanceKeySecret::from_bytes_for_test([7; 32]);
        let error = resolve_secret_mode(
            InstanceKeySecretPolicy {
                use_secret: false,
                require_generated_format: true,
                force_generate: false,
                force_disable: false,
            },
            &state_path,
            Some(secret.verifier()),
        )
        .expect_err("both errors are reported");
        let message = error.to_string();
        assert!(
            message.contains("instance key secret file is present while secrets are disabled"),
            "{message}"
        );
        assert!(message.contains("state used an instance key secret"), "{message}");
    }

    #[cfg(unix)]
    #[test]
    fn generated_secret_file_is_private_on_unix() {
        use std::os::unix::fs::PermissionsExt;

        let root = mediapm_utils::temp::output_dir().expect("temp root");
        let state_path = root.path().join("state.conductor.json");
        let (mode, _verifier) = resolve_secret_mode(
            InstanceKeySecretPolicy {
                use_secret: true,
                require_generated_format: true,
                force_generate: false,
                force_disable: false,
            },
            &state_path,
            None,
        )
        .expect("secret generates");
        assert!(matches!(mode, ResolvedSecretMode::Keyed(_)));
        let permissions = std::fs::metadata(secret_path_for_state(&state_path))
            .expect("secret file metadata")
            .permissions();
        assert_eq!(permissions.mode() & 0o777, 0o600);
    }

    #[test]
    fn failed_secret_publish_leaves_no_temporary() {
        let root = mediapm_utils::temp::output_dir().expect("temp root");
        let state_path = root.path().join("state.conductor.json");
        let secret_path = secret_path_for_state(&state_path);
        std::fs::create_dir(&secret_path).expect("a directory target forces rename to fail");
        let error = resolve_secret_mode(
            InstanceKeySecretPolicy {
                use_secret: true,
                require_generated_format: true,
                force_generate: true,
                force_disable: false,
            },
            &state_path,
            None,
        )
        .expect_err("rename into a directory fails");
        assert!(error.to_string().contains("publishing instance key secret failed"), "{error}");
        let leftovers: Vec<_> = std::fs::read_dir(root.path())
            .expect("read temp root")
            .filter_map(Result::ok)
            .filter(|entry| {
                entry.file_name().to_string_lossy().starts_with(".instance-key-secret.hex.tmp-")
            })
            .collect();
        assert!(leftovers.is_empty(), "temporary secret file remained: {leftovers:?}");
    }
}
