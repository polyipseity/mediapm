//! Runtime index state model.
//!
//! This module defines the in-memory representation used by planners,
//! maintenance passes, and persistence bridges. It intentionally keeps runtime
//! semantics independent from versioned on-disk envelopes.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::{Hash, empty_content_hash};

/// Bit flag indicating "full object" encoding in `depth_and_tag`.
const OBJECT_META_FULL_FLAG: u32 = 1 << 31;
/// Bit mask selecting packed depth bits in `depth_and_tag`.
const OBJECT_META_DEPTH_MASK: u32 = !OBJECT_META_FULL_FLAG;
/// Serialized hash-key width used by persisted index rows.
const HASH_STORAGE_KEY_BYTES: usize = 34;
/// Maximum allowed reconstruction depth for one object.
pub(crate) const MAX_DELTA_DEPTH: u32 = 1_000;
/// Depth threshold for proactively promoting deep deltas to full objects.
pub(crate) const DELTA_PROMOTION_DEPTH: u32 = MAX_DELTA_DEPTH - 32;
/// Inline-capacity hint for reverse constraint target lists.
pub(crate) const CONSTRAINT_REVERSE_INLINE_TARGETS: usize = 4;
/// Inline-capacity hint for reverse delta-child lists.
pub(crate) const DELTA_REVERSE_INLINE_CHILDREN: usize = 4;

/// Serde bridge for fixed-width base-hash storage bytes.
///
/// Runtime metadata stores delta base hashes as canonical fixed-width storage
/// bytes to keep serialization deterministic across schema versions.
mod base_storage_serde {
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};

    use super::HASH_STORAGE_KEY_BYTES;

    /// Serializes fixed-width base-storage bytes as an opaque byte sequence.
    pub(super) fn serialize<S>(
        value: &[u8; HASH_STORAGE_KEY_BYTES],
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(value)
    }

    /// Deserializes base-storage bytes and enforces exact fixed width.
    pub(super) fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<[u8; HASH_STORAGE_KEY_BYTES], D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = <Vec<u8>>::deserialize(deserializer)?;
        let len = bytes.len();
        bytes.try_into().map_err(|_: Vec<u8>| {
            D::Error::custom(format!(
                "expected {HASH_STORAGE_KEY_BYTES} base-storage bytes, got {len}"
            ))
        })
    }
}

/// Metadata encoding mode for one index entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ObjectEncoding {
    /// Object is stored as full raw bytes.
    Full,
    /// Object is stored as a delta against `base_hash`.
    Delta {
        /// Base hash used for reconstruction.
        base_hash: Hash,
    },
}

/// In-memory metadata for one stored object entry.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ObjectMeta {
    /// Stored payload size.
    pub(crate) payload_len: u64,
    /// Logical content length.
    pub(crate) content_len: u64,
    /// Packed encoding tag + depth (`bit31 => full-data flag`, lower bits => depth).
    depth_and_tag: u32,
    /// Base-hash multihash storage bytes for delta encoding (`[0; 34]` for full-data objects).
    #[serde(with = "base_storage_serde")]
    base_storage: [u8; HASH_STORAGE_KEY_BYTES],
}

/// Constructors and accessors for packed runtime object metadata.
impl ObjectMeta {
    /// Constructs full-data object metadata.
    #[must_use]
    pub(crate) const fn full(payload_len: u64, content_len: u64, depth: u32) -> Self {
        Self {
            payload_len,
            content_len,
            depth_and_tag: OBJECT_META_FULL_FLAG | (depth & OBJECT_META_DEPTH_MASK),
            base_storage: [0u8; HASH_STORAGE_KEY_BYTES],
        }
    }

    /// Constructs delta object metadata.
    #[must_use]
    pub(crate) fn delta(payload_len: u64, content_len: u64, depth: u32, base_hash: Hash) -> Self {
        Self {
            payload_len,
            content_len,
            depth_and_tag: depth & OBJECT_META_DEPTH_MASK,
            base_storage: base_hash.storage_bytes(),
        }
    }

    /// Returns whether this object is encoded as full-data bytes.
    #[must_use]
    pub(crate) const fn is_full(&self) -> bool {
        self.depth_and_tag & OBJECT_META_FULL_FLAG != 0
    }

    /// Returns reconstruction depth.
    #[must_use]
    pub(crate) const fn depth(&self) -> u32 {
        self.depth_and_tag & OBJECT_META_DEPTH_MASK
    }

    /// Sets reconstruction depth while preserving encoding tag bits.
    pub(crate) const fn set_depth(&mut self, depth: u32) {
        self.depth_and_tag =
            (self.depth_and_tag & OBJECT_META_FULL_FLAG) | (depth & OBJECT_META_DEPTH_MASK);
    }

    /// Returns metadata encoding mode.
    #[must_use]
    pub(crate) fn encoding(&self) -> ObjectEncoding {
        if self.is_full() {
            ObjectEncoding::Full
        } else {
            let base_hash = Hash::from_storage_bytes(&self.base_storage)
                .expect("delta object metadata must store valid multihash-encoded base hash");
            ObjectEncoding::Delta { base_hash }
        }
    }

    /// Returns base hash for delta-encoded objects.
    #[must_use]
    pub(crate) fn base_hash(&self) -> Option<Hash> {
        (!self.is_full()).then(|| {
            Hash::from_storage_bytes(&self.base_storage)
                .expect("delta object metadata must store valid multihash-encoded base hash")
        })
    }
}

/// Runtime index state.
///
/// Invariants:
/// - `objects` always contains the canonical empty-content hash entry,
/// - `constraints` stores only explicit rows,
/// - `constraint_reverse` mirrors `constraints` for efficient reverse updates.
/// - `delta_reverse` mirrors delta base edges for efficient descendant traversal.
#[derive(Debug, Clone, Default)]
pub(crate) struct IndexState {
    /// Object metadata by content hash.
    pub(crate) objects: BTreeMap<Hash, ObjectMeta>,
    /// Constraint candidates by target hash.
    pub(crate) constraints: BTreeMap<Hash, BTreeSet<Hash>>,
    /// Reverse lookup from base hash to constrained target hashes.
    pub(crate) constraint_reverse:
        BTreeMap<Hash, SmallVec<[Hash; CONSTRAINT_REVERSE_INLINE_TARGETS]>>,
    /// Reverse lookup from base hash to direct delta children.
    pub(crate) delta_reverse: BTreeMap<Hash, SmallVec<[Hash; DELTA_REVERSE_INLINE_CHILDREN]>>,
}

/// Reverse-index rebuild helpers for runtime index state.
impl IndexState {
    /// Rebuilds reverse constraint links from forward constraint rows.
    pub(crate) fn rebuild_constraint_reverse(&mut self) {
        self.constraint_reverse.clear();

        for (target_hash, bases) in &self.constraints {
            for base_hash in bases {
                let targets = self.constraint_reverse.entry(*base_hash).or_default();
                if !targets.contains(target_hash) {
                    targets.push(*target_hash);
                    targets.sort_unstable();
                }
            }
        }
    }

    /// Rebuilds reverse delta-child links from object metadata rows.
    pub(crate) fn rebuild_delta_reverse(&mut self) {
        self.delta_reverse.clear();

        for (target_hash, meta) in &self.objects {
            if *target_hash == empty_content_hash() {
                continue;
            }

            let ObjectEncoding::Delta { base_hash } = meta.encoding() else {
                continue;
            };

            let children = self.delta_reverse.entry(base_hash).or_default();
            if !children.contains(target_hash) {
                children.push(*target_hash);
                children.sort_unstable();
            }
        }
    }
}

/// Ensures canonical empty object metadata exists.
pub(crate) fn ensure_empty_record(state: &mut IndexState) {
    let empty = empty_content_hash();
    state.objects.entry(empty).or_insert(ObjectMeta::full(0, 0, 0));
}

#[cfg(test)]
mod tests {
    use super::{IndexState, ObjectMeta, ensure_empty_record};
    use crate::Hash;

    #[test]
    fn rebuild_delta_reverse_maps_base_to_direct_children() {
        let base = Hash::from_content(b"delta-base");
        let child_a = Hash::from_content(b"delta-child-a");
        let child_b = Hash::from_content(b"delta-child-b");

        let mut state = IndexState::default();
        ensure_empty_record(&mut state);
        state.objects.insert(base, ObjectMeta::full(4, 4, 1));
        state.objects.insert(child_a, ObjectMeta::delta(2, 4, 2, base));
        state.objects.insert(child_b, ObjectMeta::delta(2, 4, 2, base));

        state.rebuild_delta_reverse();

        let children = state.delta_reverse.get(&base).expect("base reverse children");
        assert_eq!(children.len(), 2);
        assert_eq!(children[0], child_a.min(child_b));
        assert_eq!(children[1], child_a.max(child_b));
    }
}
