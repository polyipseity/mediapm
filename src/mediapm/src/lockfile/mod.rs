//! Compatibility façade for `mediapm` realized state persistence.
//!
//! Historically runtime state was persisted in `.mediapm/lock.jsonc`.
//! Task-7 migrates persistence to Nickel `state.ncl` while preserving existing
//! public lockfile-oriented type/function names for crate users.

use std::path::Path;

use crate::config::{
    MEDIAPM_DOCUMENT_VERSION, MediaPmState, load_mediapm_state_document,
    save_mediapm_state_document,
};
use crate::error::MediaPmError;

pub use crate::config::{ManagedFileRecord, ToolRegistryRecord, ToolRegistryStatus};

/// Current persisted state schema marker.
pub const MEDIAPM_LOCK_VERSION: u32 = MEDIAPM_DOCUMENT_VERSION;

/// Backward-compatible alias for machine-managed realized state.
pub type MediaLockFile = MediaPmState;

/// Loads machine-managed state from `state.ncl`.
///
/// # Errors
///
/// Returns [`MediaPmError`] when `state.ncl` cannot be read, evaluated, or
/// decoded under the shared versioned `mediapm` Nickel schema.
pub fn load_lockfile(path: &Path) -> Result<MediaLockFile, MediaPmError> {
    load_mediapm_state_document(path)
}

/// Saves machine-managed state to `state.ncl`.
///
/// # Errors
///
/// Returns [`MediaPmError`] when parent directories cannot be created or state
/// cannot be rendered/written.
pub fn save_lockfile(path: &Path, lock: &MediaLockFile) -> Result<(), MediaPmError> {
    save_mediapm_state_document(path, lock)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        MEDIAPM_LOCK_VERSION, ManagedFileRecord, MediaLockFile, ToolRegistryRecord,
        ToolRegistryStatus, load_lockfile, save_lockfile,
    };

    /// Protects state-document persistence defaults and schema marker stability.
    #[test]
    fn lockfile_round_trip_preserves_state_model() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("state.ncl");
        let lock = MediaLockFile::default();

        save_lockfile(&path, &lock).expect("save lockfile");
        let decoded = load_lockfile(&path).expect("load lockfile");
        let rendered = std::fs::read_to_string(&path).expect("read state.ncl");

        assert_eq!(MEDIAPM_LOCK_VERSION, 1);
        assert_eq!(decoded, MediaLockFile::default());
        assert!(rendered.contains("version = 1"));
        assert!(rendered.contains("state = {"));
        assert!(!rendered.lines().any(|line| line.trim_start().starts_with("runtime =")));
        assert!(!rendered.lines().any(|line| line.trim_start().starts_with("tools =")));
    }

    /// Protects lock-state registry status serialization semantics.
    #[test]
    fn lockfile_serializes_tool_registry_status() {
        let mut lock = MediaLockFile::default();
        lock.tool_registry.insert(
            "ffmpeg".to_string(),
            ToolRegistryRecord {
                name: "ffmpeg".to_string(),
                version: "8.1".to_string(),
                source: "Evermeet".to_string(),
                registry_multihash: "blake3:abc".to_string(),
                last_transition_unix_seconds: 1,
                status: ToolRegistryStatus::Pruned,
            },
        );

        let rendered = serde_json::to_string(&lock).expect("serialize lock state");
        assert!(rendered.contains("\"pruned\""));
    }

    /// Protects managed-file schema by persisting `media_id` rather than the
    /// previous source URI field name.
    #[test]
    fn lockfile_managed_file_serializes_media_id_field() {
        let lock = MediaLockFile {
            managed_files: BTreeMap::from([(
                "library/a.mp3".to_string(),
                ManagedFileRecord {
                    media_id: "demo-a".to_string(),
                    variant: "tagged".to_string(),
                    hash: "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_string(),
                    last_synced_unix_millis: 1,
                },
            )]),
            ..MediaLockFile::default()
        };

        let rendered = serde_json::to_string(&lock).expect("serialize lock state");
        assert!(rendered.contains("\"media_id\":\"demo-a\""));
        assert!(!rendered.contains("source_uri"));
    }

    /// Protects strict lockfile schema decoding by rejecting legacy managed-file
    /// key names.
    #[test]
    fn lockfile_managed_file_rejects_legacy_source_uri_key() {
        let source = r#"
{
    version = 1,
    state = {
        managed_files = {
            "library/a.mp3" = {
                source_uri = "local:demo-a",
                variant = "tagged",
                hash = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                last_synced_unix_millis = 1,
            },
    }
    },
}
"#;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("state.ncl");
        std::fs::write(&path, source).expect("write state.ncl");

        let _ = load_lockfile(&path).expect_err("legacy source_uri key should be rejected");
    }
}
