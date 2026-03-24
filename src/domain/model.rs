//! Domain sidecar model and core value types.
//!
//! This module defines the canonical JSON-sidecar shape and supporting types
//! used by planning, execution, verification, and migration.
//!
//! Design intent:
//! - A canonical URI identifies "the media item as declared by user intent".
//! - A BLAKE3 hash identifies "the exact bytes of one concrete variant".
//! - Sidecars bind those two concepts together and accumulate provenance over
//!   time, instead of replacing history on every sync.

use std::fmt::{Display, Formatter};

use anyhow::{Result, anyhow};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

/// Latest supported sidecar schema version.
pub const LATEST_SCHEMA_VERSION: u32 = 1;

/// Content hash wrapper used for object identity.
///
/// Wrapping raw bytes in a domain type prevents accidental mixing with other
/// string/byte identifiers and makes serialization policy explicit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Blake3Hash([u8; 32]);

impl Blake3Hash {
    /// Construct from fixed hash bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Borrow underlying bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Parse a lowercase or uppercase hex string.
    pub fn from_hex(hex_value: &str) -> Result<Self> {
        let bytes = hex::decode(hex_value)?;
        if bytes.len() != 32 {
            return Err(anyhow!("invalid hash length: expected 32 bytes, got {}", bytes.len()));
        }

        let mut fixed = [0_u8; 32];
        fixed.copy_from_slice(&bytes);
        Ok(Self(fixed))
    }

    /// Encode as lowercase hex.
    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }
}

impl Display for Blake3Hash {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

impl Serialize for Blake3Hash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Blake3Hash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_hex(&value).map_err(serde::de::Error::custom)
    }
}

/// Canonical sidecar record for one source URI identity.
///
/// A `MediaRecord` is the durable state machine for one canonical URI. New
/// byte-level states are appended as variants; historical states remain for
/// auditability and deterministic rollback/relink reasoning.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MediaRecord {
    /// Sidecar schema version.
    pub schema_version: u32,
    /// Canonical identity key.
    pub canonical_uri: String,
    /// Initial creation timestamp.
    pub created_at: String,
    /// Last update timestamp.
    pub updated_at: String,
    /// Immutable original snapshot.
    pub original: OriginalSnapshot,
    /// Known variants for this URI.
    #[serde(default)]
    pub variants: Vec<VariantRecord>,
    /// Provenance/edit history events.
    #[serde(default)]
    pub edits: Vec<EditEvent>,
    /// Provider enrichment payloads.
    #[serde(default)]
    pub provider_enrichment: ProviderEnrichment,
    /// Migration history for this sidecar.
    #[serde(default)]
    pub migration_provenance: Vec<MigrationProvenance>,
}

impl MediaRecord {
    /// Build a brand-new sidecar from the first imported variant.
    pub fn new_initial(
        canonical_uri: String,
        created_at: String,
        original_variant: VariantRecord,
        original_metadata: Value,
    ) -> Self {
        let original_hash = original_variant.variant_hash;
        Self {
            schema_version: LATEST_SCHEMA_VERSION,
            canonical_uri,
            created_at: created_at.clone(),
            updated_at: created_at,
            original: OriginalSnapshot { original_variant_hash: original_hash, original_metadata },
            variants: vec![original_variant],
            edits: Vec::new(),
            provider_enrichment: ProviderEnrichment::default(),
            migration_provenance: Vec::new(),
        }
    }

    /// Return `true` when this sidecar already contains `variant_hash`.
    pub fn has_variant(&self, variant_hash: &Blake3Hash) -> bool {
        self.variants.iter().any(|variant| variant.variant_hash == *variant_hash)
    }

    /// Return the most recently appended variant.
    pub fn latest_variant(&self) -> Option<&VariantRecord> {
        self.variants.last()
    }
}

/// Immutable original import snapshot.
///
/// This is intentionally separated from `variants.last()` so the very first
/// known state remains explicit even after many subsequent updates.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct OriginalSnapshot {
    /// The first known variant hash for this URI.
    pub original_variant_hash: Blake3Hash,
    /// Metadata captured at original import time.
    pub original_metadata: Value,
}

/// A concrete content variant referenced by this URI.
///
/// Variants are immutable observations/outputs identified by hash. In current
/// MVP behavior, variant records are append-only in sidecar history.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct VariantRecord {
    /// Content hash for this variant.
    pub variant_hash: Blake3Hash,
    /// Relative path to object file under `.mediapm/objects/...`.
    pub object_relpath: String,
    /// Object byte size.
    pub byte_size: u64,
    /// Optional inferred container (`flac`, `mp3`, ...).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,
    /// Raw probe payload.
    #[serde(default)]
    pub probe: Value,
    /// Normalized metadata payload.
    #[serde(default)]
    pub metadata: Value,
    /// Lineage details.
    pub lineage: VariantLineage,
}

/// Variant ancestry and edit edges.
///
/// Lineage is modeled directly so verification can reason about provenance
/// consistency instead of relying on implicit array ordering alone.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct VariantLineage {
    /// Parent variant hash if this variant was derived from another variant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_variant_hash: Option<Blake3Hash>,
    /// Event ids connecting lineage transitions.
    #[serde(default)]
    pub edit_event_ids: Vec<String>,
}

/// A recorded transform/edit between two variants.
///
/// Edit events are independent records rather than inline comments on variants.
/// This structure supports richer future provenance (tools used, arguments,
/// quality settings, provider evidence, etc.) without changing variant shape.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EditEvent {
    /// Unique event id.
    pub event_id: String,
    /// RFC3339 event timestamp.
    pub timestamp: String,
    /// Revertability classification.
    pub kind: EditKind,
    /// Operation label.
    pub operation: String,
    /// Operation-specific details.
    #[serde(default)]
    pub details: Value,
    /// Input variant hash.
    pub from_variant_hash: Blake3Hash,
    /// Output variant hash.
    pub to_variant_hash: Blake3Hash,
}

/// Edit kinds supported by the sidecar schema.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum EditKind {
    /// Edit can be reversed at metadata-layer semantics.
    Revertable,
    /// Edit is non-revertable at byte-transformation semantics.
    NonRevertable,
}

/// Container for all provider outputs.
///
/// Provider namespaces are kept under one field to avoid schema sprawl and to
/// allow per-provider evolution without touching unrelated history fields.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct ProviderEnrichment {
    /// MusicBrainz provider payload area.
    #[serde(default)]
    pub musicbrainz: MusicBrainzEnrichment,
}

/// MusicBrainz enrichment payload.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct MusicBrainzEnrichment {
    /// Candidate matches.
    #[serde(default)]
    pub matches: Vec<Value>,
    /// Applied result payload.
    #[serde(default)]
    pub applied: Value,
}

/// Audit entry for schema migration operations.
///
/// Migration provenance is stored with record data so users can understand how
/// old sidecars became valid under newer code.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MigrationProvenance {
    /// Migration identifier.
    pub migration_id: String,
    /// Source schema version.
    pub from_version: u32,
    /// Target schema version.
    pub to_version: u32,
    /// RFC3339 timestamp.
    pub timestamp: String,
}

#[cfg(test)]
mod tests {
    use super::Blake3Hash;

    #[test]
    fn hash_roundtrip_hex() {
        let bytes = [7_u8; 32];
        let hash = Blake3Hash::from_bytes(bytes);
        let parsed = Blake3Hash::from_hex(&hash.to_hex()).expect("hex should deserialize");

        assert_eq!(hash, parsed);
        assert_eq!(hash.as_bytes(), parsed.as_bytes());
    }
}
