//! Phase 3 lockfile model and persistence helpers.
//!
//! `.mediapm/lock.jsonc` (by default) is the operational ground truth for what
//! `mediapm` has materialized and what safety-protected external data hashes
//! should be preserved.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::MediaPmError;

pub(crate) mod versions;

/// Current lockfile schema marker.
pub const MEDIAPM_LOCK_VERSION: u32 = versions::latest_lockfile_version();

/// Top-level lockfile representation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaLockFile {
    /// Explicit lockfile schema marker.
    pub version: u32,
    /// Materialized path registry keyed by relative target path.
    #[serde(default)]
    pub managed_files: BTreeMap<String, ManagedFileRecord>,
    /// Safety-pinned external data hashes used by permanent-transcode flows.
    #[serde(default)]
    pub safety_external_data: BTreeMap<String, SafetyExternalDataRecord>,
    /// Tool registry mirror keyed by immutable tool id.
    #[serde(default)]
    pub tool_registry: BTreeMap<String, ToolRegistryRecord>,
    /// Active tool id per logical tool name.
    #[serde(default)]
    pub active_tools: BTreeMap<String, String>,
}

impl Default for MediaLockFile {
    fn default() -> Self {
        Self {
            version: MEDIAPM_LOCK_VERSION,
            managed_files: BTreeMap::new(),
            safety_external_data: BTreeMap::new(),
            tool_registry: BTreeMap::new(),
            active_tools: BTreeMap::new(),
        }
    }
}

/// Materialized file ledger entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedFileRecord {
    /// Media id that produced this path.
    ///
    /// This keeps managed-file provenance stable even when source URIs evolve
    /// across edits. Older lockfiles that persisted `source_uri` are accepted
    /// through serde aliasing.
    #[serde(alias = "source_uri")]
    pub media_id: String,
    /// Variant id selected for this materialized output.
    pub variant: String,
    /// Last successful sync timestamp in Unix epoch milliseconds.
    ///
    /// `mediapm` uses explicit unit-suffixed epoch fields to match CAS-style
    /// timestamp conventions.
    #[serde(alias = "last_synced_unix_seconds")]
    pub last_synced_unix_millis: u64,
}

/// Safety-pinned external-data entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SafetyExternalDataRecord {
    /// CAS hash string for the protected payload.
    pub hash: String,
    /// Tool id that produced this payload.
    pub tool_id: String,
    /// Last reference timestamp in Unix seconds.
    pub last_referenced_unix_seconds: u64,
    /// Lifecycle status used by pruning and warnings.
    pub status: ToolRegistryStatus,
}

/// Tool lifecycle status tracked by `mediapm` lock state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRegistryStatus {
    /// Tool binary/config is present and expected to be runnable.
    Active,
    /// Tool binary was intentionally pruned while metadata remains.
    Pruned,
}

/// Tool registry metadata persisted in lock state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRegistryRecord {
    /// Logical tool name without version suffix.
    pub name: String,
    /// Catalog release track recorded at activation time.
    pub version: String,
    /// Catalog source label used for this registration.
    pub source: String,
    /// Content-derived multihash fingerprint used for validation bookkeeping.
    pub registry_multihash: String,
    /// Last status transition timestamp in Unix seconds.
    pub last_transition_unix_seconds: u64,
    /// Current lifecycle state.
    pub status: ToolRegistryStatus,
}

/// Loads lockfile from disk or returns defaults when absent.
pub fn load_lockfile(path: &Path) -> Result<MediaLockFile, MediaPmError> {
    if !path.exists() {
        return Ok(MediaLockFile::default());
    }

    let bytes = fs::read(path).map_err(|source| MediaPmError::Io {
        operation: "reading mediapm lockfile".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(MediaLockFile::default());
    }

    versions::decode_lockfile_bytes(&bytes)
}

/// Saves lockfile with deterministic pretty JSON formatting.
pub fn save_lockfile(path: &Path, lock: &MediaLockFile) -> Result<(), MediaPmError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| MediaPmError::Io {
            operation: "creating mediapm lockfile parent directory".to_string(),
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let rendered = versions::encode_lockfile_bytes(lock.clone())?;
    fs::write(path, rendered).map_err(|source| MediaPmError::Io {
        operation: "writing mediapm lockfile".to_string(),
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        MEDIAPM_LOCK_VERSION, ManagedFileRecord, MediaLockFile, ToolRegistryRecord,
        ToolRegistryStatus, load_lockfile, save_lockfile,
    };

    /// Protects lockfile persistence defaults and schema marker stability.
    #[test]
    fn lockfile_round_trip_preserves_version() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("lock.jsonc");
        let lock = MediaLockFile::default();

        save_lockfile(&path, &lock).expect("save lockfile");
        let decoded = load_lockfile(&path).expect("load lockfile");

        assert_eq!(decoded.version, MEDIAPM_LOCK_VERSION);
    }

    /// Protects lockfile registry status serialization semantics.
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

        let rendered = serde_json::to_string(&lock).expect("serialize lockfile");
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
                    last_synced_unix_millis: 1,
                },
            )]),
            ..MediaLockFile::default()
        };

        let rendered = serde_json::to_string(&lock).expect("serialize lockfile");
        assert!(rendered.contains("\"media_id\":\"demo-a\""));
        assert!(!rendered.contains("source_uri"));
    }

    /// Protects backward compatibility by decoding legacy managed-file
    /// `source_uri` fields into runtime `media_id` values.
    #[test]
    fn lockfile_managed_file_decodes_legacy_source_uri_alias() {
        let bytes = br#"{
  "version": 1,
  "managed_files": {
    "library/a.mp3": {
      "source_uri": "local:demo-a",
      "variant": "tagged",
      "last_synced_unix_millis": 1
    }
  }
}"#;

        let decoded = super::versions::decode_lockfile_bytes(bytes).expect("decode lockfile");
        let record = decoded.managed_files.get("library/a.mp3").expect("managed file record");
        assert_eq!(record.media_id, "local:demo-a");
    }
}
