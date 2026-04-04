//! Redb-backed persistence engine for CAS index state.
//!
//! This module is responsible for durable storage of:
//! - object metadata rows,
//! - explicit constraint rows,
//! - schema metadata markers,
//! - and an optional persisted bloom prefilter.
//!
//! ## Data model boundaries
//!
//! - Runtime source of truth is [`IndexState`].
//! - Versioned wire/persistence envelopes are handled via `index/versions/*`.
//! - Empty-content canonical object semantics are preserved during load/persist.
//! - Schema-marker and bloom-format handling are delegated to `index/versions/`.
//!
//! ## DO NOT REMOVE: external versions boundary guard
//!
//! This file is outside `index/versions/` and must remain version-agnostic.
//! It must use unversioned symbols exposed by `index/mod.rs` and never import
//! `index::versions::*` directly.
//!
//! ## Performance model
//!
//! The implementation favors merge-style and batched transactions to keep large
//! ingest/update workloads efficient while preserving deterministic state.

use std::collections::BTreeSet;
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use bitvec::{order::Lsb0, vec::BitVec};
use bytemuck::pod_read_unaligned;
use redb::{Database, ReadableMultimapTable, ReadableTable};

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use std::arch::is_x86_feature_detected;
#[cfg(target_arch = "x86")]
use std::arch::x86::{__m128i, _mm_and_si128, _mm_set_epi32, _mm_set1_epi32, _mm_storeu_si128};
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::{__m128i, _mm_and_si128, _mm_set_epi32, _mm_set1_epi32, _mm_storeu_si128};

use super::{
    DELTA_PROMOTION_DEPTH, HASH_STORAGE_KEY_BYTES, IndexState, MAX_DELTA_DEPTH, ObjectEncoding,
    ObjectMeta, decode_bloom_payload, decode_primary_object_meta, encode_bloom_payload,
    encode_current_schema_marker, encode_primary_object_meta, ensure_empty_record,
    hash_from_index_key, index_key_from_hash, initialize_tables, latest_schema_marker,
    migrate_index_state_to_version, open_constraints_table_read, open_constraints_table_write,
    open_primary_table_read, open_primary_table_write, read_bloom_payload_from_table,
    read_schema_marker_value_from_metadata, write_bloom_payload_to_table,
    write_schema_marker_to_metadata,
};
use crate::{CasError, Hash, empty_content_hash};

#[cfg(test)]
use super::schema_marker_needs_initialization;

/// Sentinel cached marker value used before schema metadata is loaded.
const UNINITIALIZED_SCHEMA_MARKER: u32 = 0;

/// Redb-backed index database wrapper.
#[derive(Debug, Clone)]
pub(crate) struct CasIndexDb {
    db: Arc<Database>,
    bloom: Arc<RwLock<HashBloomFilter>>,
    schema_marker: Arc<AtomicU32>,
    migration_gate: Arc<RwLock<()>>,
}

/// One atomic index mutation operation for batched persistence.
#[derive(Debug, Clone)]
pub(crate) enum BatchOperation {
    /// Inserts or updates one object metadata row.
    UpsertObject { hash: Hash, meta: ObjectMeta },
    /// Deletes one object metadata row.
    DeleteObject { hash: Hash },
    /// Replaces explicit constraint bases for one object.
    SetConstraintBases { target_hash: Hash, bases: BTreeSet<Hash> },
}

/// Snapshot of existing primary row key/value bytes during merge persistence.
type ExistingPrimaryRow = ([u8; HASH_STORAGE_KEY_BYTES], Vec<u8>);

/// Number of bloom probes derived from each hash digest.
const BLOOM_HASH_PROBE_COUNT: usize = 4;

/// In-memory bloom filter for fast hash existence prechecks.
#[derive(Debug, Clone)]
struct HashBloomFilter {
    /// Packed bit storage.
    bits: BitVec<u64, Lsb0>,
    /// `bit_len - 1` mask for power-of-two modulo operations.
    mask: usize,
}

/// Zero-copy view over persisted bloom payload bytes.
#[derive(Debug, Clone, Copy)]
struct BloomPayloadView<'a> {
    /// `bit_len - 1` mask for power-of-two modulo operations.
    mask: usize,
    /// Packed raw little-endian `u64` words.
    raw_words: &'a [u8],
}

/// Borrow-based bloom-query helpers over persisted table payload bytes.
impl<'a> BloomPayloadView<'a> {
    /// Decodes persisted payload bytes into a borrow-based view.
    fn from_payload(payload: &'a [u8], schema_marker: u32) -> Result<Self, CasError> {
        let (bit_len, raw_words) = decode_bloom_payload(schema_marker, payload)?;
        Ok(Self { mask: bit_len.saturating_sub(1), raw_words })
    }

    /// Returns approximate membership for one hash.
    fn maybe_contains(&self, hash: Hash) -> bool {
        self.all_positions_set(positions_from_hash(hash, self.mask))
    }

    /// Returns approximate membership for many hashes.
    fn maybe_contains_many(&self, hashes: &[Hash]) -> Vec<bool> {
        let mut out = vec![false; hashes.len()];
        let mut offset = 0;

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if is_x86_feature_detected!("sse2") {
            while offset + BLOOM_HASH_PROBE_COUNT <= hashes.len() {
                let chunk =
                    [hashes[offset], hashes[offset + 1], hashes[offset + 2], hashes[offset + 3]];
                let positions = unsafe { self.positions_chunk4_sse2(&chunk) };
                for lane in 0..BLOOM_HASH_PROBE_COUNT {
                    out[offset + lane] = self.all_positions_set(positions[lane]);
                }
                offset += BLOOM_HASH_PROBE_COUNT;
            }
        }

        while offset < hashes.len() {
            out[offset] = self.maybe_contains(hashes[offset]);
            offset += 1;
        }

        out
    }

    /// Returns whether one bloom bit is set.
    fn bit_is_set(&self, index: usize) -> bool {
        let word_offset = (index >> 6) * size_of::<u64>();
        if word_offset + size_of::<u64>() > self.raw_words.len() {
            return false;
        }

        let word = pod_read_unaligned::<u64>(&self.raw_words[word_offset..word_offset + 8]);
        (word & (1u64 << (index & 63))) != 0
    }

    /// Returns whether all probe positions are set.
    fn all_positions_set(&self, positions: [usize; BLOOM_HASH_PROBE_COUNT]) -> bool {
        let [a, b, c, d] = positions;
        self.bit_is_set(a) && self.bit_is_set(b) && self.bit_is_set(c) && self.bit_is_set(d)
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    #[target_feature(enable = "sse2")]
    /// Computes probe positions for four hashes with SSE2 lane masking.
    unsafe fn positions_chunk4_sse2(
        &self,
        hashes: &[Hash; BLOOM_HASH_PROBE_COUNT],
    ) -> [[usize; BLOOM_HASH_PROBE_COUNT]; BLOOM_HASH_PROBE_COUNT] {
        let digest0 = pod_read_unaligned::<[u32; 8]>(hashes[0].as_digest_bytes());
        let digest1 = pod_read_unaligned::<[u32; 8]>(hashes[1].as_digest_bytes());
        let digest2 = pod_read_unaligned::<[u32; 8]>(hashes[2].as_digest_bytes());
        let digest3 = pod_read_unaligned::<[u32; 8]>(hashes[3].as_digest_bytes());

        let mask = self.mask as u32;
        let lane_a =
            unsafe { masked_lanes_sse2([digest0[0], digest1[0], digest2[0], digest3[0]], mask) };
        let lane_b =
            unsafe { masked_lanes_sse2([digest0[1], digest1[1], digest2[1], digest3[1]], mask) };
        let lane_c =
            unsafe { masked_lanes_sse2([digest0[2], digest1[2], digest2[2], digest3[2]], mask) };
        let lane_d =
            unsafe { masked_lanes_sse2([digest0[3], digest1[3], digest2[3], digest3[3]], mask) };

        [
            [lane_a[0], lane_b[0], lane_c[0], lane_d[0]],
            [lane_a[1], lane_b[1], lane_c[1], lane_d[1]],
            [lane_a[2], lane_b[2], lane_c[2], lane_d[2]],
            [lane_a[3], lane_b[3], lane_c[3], lane_d[3]],
        ]
    }
}

/// Derives fixed probe positions from hash digest words.
fn positions_from_hash(hash: Hash, mask: usize) -> [usize; BLOOM_HASH_PROBE_COUNT] {
    let words = pod_read_unaligned::<[u32; 8]>(hash.as_digest_bytes());
    [
        (words[0] as usize) & mask,
        (words[1] as usize) & mask,
        (words[2] as usize) & mask,
        (words[3] as usize) & mask,
    ]
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "sse2")]
/// Applies one mask to four 32-bit lanes and returns masked lane values.
unsafe fn masked_lanes_sse2(
    values: [u32; BLOOM_HASH_PROBE_COUNT],
    mask: u32,
) -> [usize; BLOOM_HASH_PROBE_COUNT] {
    let v = _mm_set_epi32(values[3] as i32, values[2] as i32, values[1] as i32, values[0] as i32);
    let m = _mm_set1_epi32(mask as i32);
    let masked = _mm_and_si128(v, m);

    let mut out = [0u32; 4];
    unsafe { _mm_storeu_si128(out.as_mut_ptr() as *mut __m128i, masked) };
    [out[0] as usize, out[1] as usize, out[2] as usize, out[3] as usize]
}

/// In-memory bloom construction and query utilities.
impl HashBloomFilter {
    /// Creates a bloom filter sized for expected item count.
    fn with_capacity(expected_items: usize) -> Self {
        let target_bits = expected_items.saturating_mul(10).max(1024);
        let bit_len = target_bits.next_power_of_two();
        Self { bits: BitVec::<u64, Lsb0>::repeat(false, bit_len), mask: bit_len.saturating_sub(1) }
    }

    /// Builds bloom state from all non-empty object hashes in index state.
    fn from_index_state(state: &IndexState) -> Self {
        let mut bloom = Self::with_capacity(state.objects.len().max(1));
        for hash in state.objects.keys().copied().filter(|hash| *hash != empty_content_hash()) {
            bloom.insert(hash);
        }
        bloom
    }

    /// Reconstructs bloom filter from persisted packed words.
    fn from_persisted_words(words: Vec<u64>, bit_len: usize) -> Result<Self, CasError> {
        if bit_len == 0 || !bit_len.is_power_of_two() {
            return Err(CasError::corrupt_index(format!(
                "persisted bloom filter has invalid bit length: {bit_len}"
            )));
        }

        let mut bits = BitVec::<u64, Lsb0>::from_vec(words);
        if bits.len() < bit_len {
            return Err(CasError::corrupt_index(format!(
                "persisted bloom bit storage too short: have {}, need {bit_len}",
                bits.len()
            )));
        }

        bits.truncate(bit_len);
        Ok(Self { bits, mask: bit_len.saturating_sub(1) })
    }

    /// Inserts one hash into bloom set.
    fn insert(&mut self, hash: Hash) {
        for index in self.positions(hash) {
            self.bits.set(index, true);
        }
    }

    /// Returns approximate membership for one hash.
    fn maybe_contains(&self, hash: Hash) -> bool {
        self.all_positions_set(positions_from_hash(hash, self.mask))
    }

    /// Returns approximate membership for many hashes.
    fn maybe_contains_many(&self, hashes: &[Hash]) -> Vec<bool> {
        hashes.iter().copied().map(|hash| self.maybe_contains(hash)).collect()
    }

    /// Encodes this bloom filter into persisted payload bytes.
    fn encode(&self, schema_marker: u32) -> Result<Vec<u8>, CasError> {
        encode_bloom_payload(schema_marker, self.bits.len(), self.bits.as_raw_slice())
    }

    /// Decodes bloom filter from persisted payload bytes.
    fn decode(bytes: &[u8], schema_marker: u32) -> Result<Self, CasError> {
        let (bit_len, raw) = decode_bloom_payload(schema_marker, bytes)?;
        let words =
            raw.chunks_exact(size_of::<u64>()).map(pod_read_unaligned::<u64>).collect::<Vec<_>>();
        Self::from_persisted_words(words, bit_len)
    }

    /// Derives probe positions for one hash.
    fn positions(&self, hash: Hash) -> [usize; BLOOM_HASH_PROBE_COUNT] {
        positions_from_hash(hash, self.mask)
    }

    /// Returns whether all probe positions are set.
    fn all_positions_set(&self, positions: [usize; BLOOM_HASH_PROBE_COUNT]) -> bool {
        let [a, b, c, d] = positions;
        self.bits[a] && self.bits[b] && self.bits[c] && self.bits[d]
    }
}

/// Durable index database operations, migration handling, and commit helpers.
impl CasIndexDb {
    /// Opens or creates `<root>/index.redb`, initializes tables, and verifies
    /// persisted schema marker for the active unreleased format.
    ///
    /// # Errors
    /// Returns [`CasError::Redb`], [`CasError::Io`], or [`CasError::CorruptIndex`].
    pub(crate) fn open(root: &Path) -> Result<Self, CasError> {
        std::fs::create_dir_all(root)
            .map_err(|source| CasError::io("creating cas root for index db", root, source))?;

        let db_path = root.join("index.redb");
        let db = if db_path.exists() {
            Database::open(&db_path).map_err(CasError::redb)?
        } else {
            Database::create(&db_path).map_err(CasError::redb)?
        };

        let this = Self {
            db: Arc::new(db),
            bloom: Arc::new(RwLock::new(HashBloomFilter::with_capacity(1024))),
            schema_marker: Arc::new(AtomicU32::new(UNINITIALIZED_SCHEMA_MARKER)),
            migration_gate: Arc::new(RwLock::new(())),
        };
        this.init_tables(latest_schema_marker())?;
        this.ensure_schema_marker_current()?;
        this.migrate_to_version(latest_schema_marker())?;
        this.refresh_bloom_filter()?;
        Ok(this)
    }

    #[inline]
    /// Returns cached schema marker when initialized.
    fn cached_schema_marker(&self) -> Option<u32> {
        match self.schema_marker.load(Ordering::Relaxed) {
            UNINITIALIZED_SCHEMA_MARKER => None,
            marker => Some(marker),
        }
    }

    #[inline]
    /// Updates in-process cached schema marker.
    fn set_cached_schema_marker(&self, marker: u32) {
        self.schema_marker.store(marker, Ordering::Relaxed);
    }

    /// Resolves schema marker for one operation, populating cache when needed.
    fn schema_marker_for_operation(&self) -> Result<u32, CasError> {
        if let Some(marker) = self.cached_schema_marker() {
            return Ok(marker);
        }

        let marker = self.read_schema_marker_value()?.ok_or_else(|| {
            CasError::corrupt_index(
                "missing index schema metadata marker while resolving active schema version",
            )
        })?;
        self.set_cached_schema_marker(marker);
        Ok(marker)
    }

    /// Acquires shared migration gate for read-path operations.
    fn lock_migration_read(
        &self,
        operation: &'static str,
    ) -> Result<std::sync::RwLockReadGuard<'_, ()>, CasError> {
        self.migration_gate
            .read()
            .map_err(|err| CasError::poisoned_lock("index-migration-gate", operation, err))
    }

    /// Acquires exclusive migration gate for schema-changing operations.
    fn lock_migration_write(
        &self,
        operation: &'static str,
    ) -> Result<std::sync::RwLockWriteGuard<'_, ()>, CasError> {
        self.migration_gate
            .write()
            .map_err(|err| CasError::poisoned_lock("index-migration-gate", operation, err))
    }

    /// Returns true when there is at least one primary index row.
    ///
    /// # Errors
    /// Returns [`CasError::Redb`] when table iteration fails.
    pub(crate) fn has_data(&self) -> Result<bool, CasError> {
        let _migration_guard = self.lock_migration_read("reading index has_data")?;
        let schema_marker = self.schema_marker_for_operation()?;
        self.has_data_for_schema_marker(schema_marker)
    }

    fn has_data_for_schema_marker(&self, schema_marker: u32) -> Result<bool, CasError> {
        let read = self.db.begin_read().map_err(CasError::redb)?;
        let table = open_primary_table_read(&read, schema_marker)?;
        let mut iter = table.iter().map_err(CasError::redb)?;
        Ok(iter.next().transpose().map_err(CasError::redb)?.is_some())
    }

    /// Batch fast-existence checks with optional SIMD-assisted bloom prefilter.
    pub(crate) fn contains_hashes_fast(&self, hashes: &[Hash]) -> Result<Vec<bool>, CasError> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }

        let _migration_guard = self.lock_migration_read("reading index contains_hashes_fast")?;
        let schema_marker = self.schema_marker_for_operation()?;
        let read = self.db.begin_read().map_err(CasError::redb)?;
        let bloom_hits = match self.bloom_prefilter_many_from_read(&read, schema_marker, hashes)? {
            Some(hits) => hits,
            None => self
                .bloom
                .read()
                .map_err(|err| CasError::poisoned_lock("index-bloom", "reading bloom", err))?
                .maybe_contains_many(hashes),
        };

        let primary = open_primary_table_read(&read, schema_marker)?;
        let mut out = vec![false; hashes.len()];
        for (i, hash) in hashes.iter().copied().enumerate() {
            if hash == empty_content_hash() {
                out[i] = true;
                continue;
            }

            if !bloom_hits[i] {
                continue;
            }

            let key = index_key_from_hash(hash);
            out[i] = primary.get(key.as_slice()).map_err(CasError::redb)?.is_some();
        }

        Ok(out)
    }

    /// Loads full index state from redb tables.
    ///
    /// # Errors
    /// Returns [`CasError::Redb`] or [`CasError::CorruptIndex`].
    pub(crate) fn load_state(&self) -> Result<IndexState, CasError> {
        let _migration_guard = self.lock_migration_read("loading index state")?;
        let schema_marker = self.schema_marker_for_operation()?;
        self.load_state_for_schema_marker(schema_marker)
    }

    fn load_state_for_schema_marker(&self, schema_marker: u32) -> Result<IndexState, CasError> {
        let read = self.db.begin_read().map_err(CasError::redb)?;
        let primary = open_primary_table_read(&read, schema_marker)?;
        let constraints = open_constraints_table_read(&read, schema_marker)?;

        let mut state = IndexState::default();

        for row in primary.iter().map_err(CasError::redb)? {
            let (key, value) = row.map_err(CasError::redb)?;
            let hash = hash_from_index_key(key.value())?;
            let meta = decode_primary_object_meta(schema_marker, value.value())?;
            state.objects.insert(hash, meta);
        }

        for row in constraints.iter().map_err(CasError::redb)? {
            let (key, values) = row.map_err(CasError::redb)?;
            let target = hash_from_index_key(key.value())?;
            for base in values {
                let base = hash_from_index_key(base.map_err(CasError::redb)?.value())?;
                if base != empty_content_hash() {
                    state.constraints.entry(target).or_default().insert(base);
                }
            }
        }

        ensure_empty_record(&mut state);
        state.rebuild_constraint_reverse();
        state.rebuild_delta_reverse();
        validate_loaded_state(&state)?;
        Ok(state)
    }

    /// Persists full index state by merge-style streaming reconciliation.
    ///
    /// This avoids materializing full existing-row snapshots in memory.
    ///
    /// # Errors
    /// Returns [`CasError::Redb`] or [`CasError::CorruptIndex`].
    pub(crate) fn persist_state(&self, state: &IndexState) -> Result<(), CasError> {
        let _migration_guard = self.lock_migration_read("persisting index state")?;
        let schema_marker = self.schema_marker_for_operation()?;
        self.persist_state_for_schema_marker(schema_marker, state)
    }

    fn persist_state_for_schema_marker(
        &self,
        schema_marker: u32,
        state: &IndexState,
    ) -> Result<(), CasError> {
        let mut desired_primary = state
            .objects
            .iter()
            .filter(|(hash, _)| **hash != empty_content_hash())
            .map(|(hash, _)| *hash)
            .peekable();

        let mut desired_constraints = state
            .constraints
            .iter()
            .flat_map(|(target, bases)| {
                bases
                    .iter()
                    .copied()
                    .filter(|base| *base != empty_content_hash())
                    .map(move |base| (index_key_from_hash(*target), index_key_from_hash(base)))
            })
            .peekable();

        let bloom_from_state = HashBloomFilter::from_index_state(state);

        let read = self.db.begin_read().map_err(CasError::redb)?;
        let primary_read = open_primary_table_read(&read, schema_marker)?;
        let constraints_read = open_constraints_table_read(&read, schema_marker)?;

        let write = self.db.begin_write().map_err(CasError::redb)?;
        {
            let mut primary_write = open_primary_table_write(&write, schema_marker)?;
            let mut constraints_write = open_constraints_table_write(&write, schema_marker)?;

            let mut primary_iter = primary_read.iter().map_err(CasError::redb)?;
            let mut next_primary = || -> Result<Option<ExistingPrimaryRow>, CasError> {
                let Some(row) = primary_iter.next() else {
                    return Ok(None);
                };
                let (key, value) = row.map_err(CasError::redb)?;
                let key_bytes: [u8; HASH_STORAGE_KEY_BYTES] =
                    key.value().try_into().map_err(|_| {
                    CasError::corrupt_index(format!(
                        "invalid key width in primary index: expected {HASH_STORAGE_KEY_BYTES}, got {}",
                        key.value().len()
                    ))
                })?;
                let value_bytes = value.value().to_vec();
                Ok(Some((key_bytes, value_bytes)))
            };

            let mut existing_primary = next_primary()?;
            while let Some(desired_hash) = desired_primary.peek().copied() {
                let desired_key = index_key_from_hash(desired_hash);
                let desired_value =
                    Self::encode_desired_header(state, schema_marker, desired_hash)?;

                if let Some((existing_key, existing_value)) = existing_primary.as_ref()
                    && *existing_key == desired_key
                {
                    if existing_value.as_slice() != desired_value.as_slice() {
                        primary_write
                            .insert(desired_key.as_slice(), desired_value.as_slice())
                            .map_err(CasError::redb)?;
                    }
                    let _ = desired_primary.next();
                    existing_primary = next_primary()?;
                    continue;
                }

                if let Some((existing_key, _)) = existing_primary.as_ref()
                    && *existing_key < desired_key
                {
                    primary_write.remove(existing_key.as_slice()).map_err(CasError::redb)?;
                    existing_primary = next_primary()?;
                    continue;
                }

                primary_write
                    .insert(desired_key.as_slice(), desired_value.as_slice())
                    .map_err(CasError::redb)?;
                let _ = desired_primary.next();
            }

            while let Some((existing_key, _)) = existing_primary {
                primary_write.remove(existing_key.as_slice()).map_err(CasError::redb)?;
                existing_primary = next_primary()?;
            }

            for row in constraints_read.iter().map_err(CasError::redb)? {
                let (target, bases) = row.map_err(CasError::redb)?;
                let target: [u8; HASH_STORAGE_KEY_BYTES] =
                    target.value().try_into().map_err(|_| {
                    CasError::corrupt_index(format!(
                        "invalid target key width in constraints table: expected {HASH_STORAGE_KEY_BYTES}, got {}",
                        target.value().len()
                    ))
                })?;

                for base in bases {
                    let base = base.map_err(CasError::redb)?;
                    let base: [u8; HASH_STORAGE_KEY_BYTES] =
                        base.value().try_into().map_err(|_| {
                        CasError::corrupt_index(format!(
                            "invalid base key width in constraints table: expected {HASH_STORAGE_KEY_BYTES}, got {}",
                            base.value().len()
                        ))
                    })?;

                    let existing_pair = (target, base);
                    while let Some(desired_pair) = desired_constraints.peek().copied() {
                        if desired_pair < existing_pair {
                            constraints_write
                                .insert(desired_pair.0.as_slice(), desired_pair.1.as_slice())
                                .map_err(CasError::redb)?;
                            let _ = desired_constraints.next();
                        } else {
                            break;
                        }
                    }

                    if let Some(desired_pair) = desired_constraints.peek().copied() {
                        if desired_pair == existing_pair {
                            let _ = desired_constraints.next();
                        } else {
                            constraints_write
                                .remove(existing_pair.0.as_slice(), existing_pair.1.as_slice())
                                .map_err(CasError::redb)?;
                        }
                    } else {
                        constraints_write
                            .remove(existing_pair.0.as_slice(), existing_pair.1.as_slice())
                            .map_err(CasError::redb)?;
                    }
                }
            }

            for desired_pair in desired_constraints {
                constraints_write
                    .insert(desired_pair.0.as_slice(), desired_pair.1.as_slice())
                    .map_err(CasError::redb)?;
            }

            Self::set_schema_marker_current(&write, schema_marker)?;
            Self::persist_bloom_state_table(&write, schema_marker, &bloom_from_state)?;
        }

        write.commit().map_err(CasError::redb)?;
        self.replace_bloom_filter(bloom_from_state)?;
        Ok(())
    }

    /// Migrates the full index to one target persisted schema marker.
    ///
    /// Migration acquires an exclusive gate that blocks all index reads until
    /// migration completes.
    pub(crate) fn migrate_to_version(&self, target_schema_marker: u32) -> Result<(), CasError> {
        let _migration_guard = self.lock_migration_write("migrating index schema")?;
        let current_schema_marker = self.schema_marker_for_operation()?;

        if current_schema_marker == target_schema_marker {
            return Ok(());
        }

        let current_state = self.load_state_for_schema_marker(current_schema_marker)?;
        let migrated_state = migrate_index_state_to_version(
            current_state,
            current_schema_marker,
            target_schema_marker,
        )?;

        self.init_tables(target_schema_marker)?;
        self.persist_state_for_schema_marker(target_schema_marker, &migrated_state)?;
        self.set_cached_schema_marker(target_schema_marker);
        Ok(())
    }

    /// Applies multiple index updates in one redb write transaction.
    ///
    /// This is significantly faster than one-commit-per-object ingestion.
    ///
    /// # Errors
    /// Returns [`CasError::Redb`], [`CasError::NotFound`], or [`CasError::CorruptIndex`].
    pub(crate) fn persist_batch<I>(&self, operations: I) -> Result<(), CasError>
    where
        I: IntoIterator<Item = BatchOperation>,
    {
        let _migration_guard = self.lock_migration_read("persisting index batch")?;
        let schema_marker = self.schema_marker_for_operation()?;
        let mut bloom_next = self.snapshot_bloom_filter()?;
        let write = self.db.begin_write().map_err(CasError::redb)?;
        {
            let mut primary = open_primary_table_write(&write, schema_marker)?;
            let mut constraints = open_constraints_table_write(&write, schema_marker)?;

            for op in operations {
                match op {
                    BatchOperation::UpsertObject { hash, meta } => {
                        Self::apply_upsert_object(&mut primary, schema_marker, hash, meta)?;
                        if hash != empty_content_hash() {
                            bloom_next.insert(hash);
                        }
                    }
                    BatchOperation::DeleteObject { hash } => {
                        Self::apply_delete_object(&mut primary, &mut constraints, hash)?;
                    }
                    BatchOperation::SetConstraintBases { target_hash, bases } => {
                        Self::apply_set_constraint_bases(
                            &mut primary,
                            &mut constraints,
                            target_hash,
                            &bases,
                        )?;
                    }
                }
            }

            Self::set_schema_marker_current(&write, schema_marker)?;
            Self::persist_bloom_state_table(&write, schema_marker, &bloom_next)?;
        }
        write.commit().map_err(CasError::redb)?;
        self.replace_bloom_filter(bloom_next)?;
        Ok(())
    }

    /// Encodes one desired object row from runtime state.
    fn encode_desired_header(
        state: &IndexState,
        schema_marker: u32,
        hash: Hash,
    ) -> Result<Vec<u8>, CasError> {
        let meta =
            state.objects.get(&hash).copied().ok_or_else(|| {
                CasError::corrupt_index(format!("missing object meta for {hash}"))
            })?;
        encode_primary_object_meta(schema_marker, meta)
    }

    /// Applies one upsert operation in an open transaction.
    fn apply_upsert_object(
        primary: &mut redb::Table<&[u8], &[u8]>,
        schema_marker: u32,
        hash: Hash,
        meta: ObjectMeta,
    ) -> Result<(), CasError> {
        if hash == empty_content_hash() {
            return Ok(());
        }

        let key = index_key_from_hash(hash);
        let promoted_meta = Self::promote_near_limit_delta(meta);
        let encoded = encode_primary_object_meta(schema_marker, promoted_meta)?;
        primary.insert(key.as_slice(), encoded.as_slice()).map_err(CasError::redb)?;
        Ok(())
    }

    /// Promotes near-limit deltas to full objects to preserve depth headroom.
    fn promote_near_limit_delta(meta: ObjectMeta) -> ObjectMeta {
        match meta.encoding() {
            ObjectEncoding::Delta { .. } if meta.depth() >= DELTA_PROMOTION_DEPTH => {
                ObjectMeta::full(meta.content_len, meta.content_len, 1)
            }
            _ => meta,
        }
    }

    /// Applies one object delete operation in an open transaction.
    fn apply_delete_object(
        primary: &mut redb::Table<&[u8], &[u8]>,
        constraints: &mut redb::MultimapTable<&[u8], &[u8]>,
        hash: Hash,
    ) -> Result<(), CasError> {
        if hash == empty_content_hash() {
            return Ok(());
        }

        let key = index_key_from_hash(hash);
        primary.remove(key.as_slice()).map_err(CasError::redb)?;
        constraints.remove_all(key.as_slice()).map_err(CasError::redb)?;
        Ok(())
    }

    /// Replaces explicit base set for one target hash in an open transaction.
    fn apply_set_constraint_bases(
        primary: &mut redb::Table<&[u8], &[u8]>,
        constraints: &mut redb::MultimapTable<&[u8], &[u8]>,
        target_hash: Hash,
        bases: &BTreeSet<Hash>,
    ) -> Result<(), CasError> {
        if target_hash == empty_content_hash() {
            if bases.is_empty() {
                return Ok(());
            }
            return Err(CasError::invalid_constraint(
                "empty-content root cannot have explicit constraint bases",
            ));
        }

        let target_key = index_key_from_hash(target_hash);
        if primary.get(target_key.as_slice()).map_err(CasError::redb)?.is_none() {
            return Err(CasError::NotFound(target_hash));
        }

        constraints.remove_all(target_key.as_slice()).map_err(CasError::redb)?;
        for base in bases.iter().copied().filter(|base| *base != empty_content_hash()) {
            let base_key = index_key_from_hash(base);
            constraints
                .insert(target_key.as_slice(), base_key.as_slice())
                .map_err(CasError::redb)?;
        }
        Ok(())
    }

    /// Ensures all required tables exist for one schema marker.
    fn init_tables(&self, schema_marker: u32) -> Result<(), CasError> {
        let write = self.db.begin_write().map_err(CasError::redb)?;
        initialize_tables(&write, schema_marker)?;
        write.commit().map_err(CasError::redb)
    }

    /// Ensures schema marker metadata is initialized and current.
    fn ensure_schema_marker_current(&self) -> Result<(), CasError> {
        let persisted = self.read_schema_marker_value()?;
        let has_data = self.has_data_for_schema_marker(latest_schema_marker())?;
        match persisted {
            Some(marker) => {
                self.set_cached_schema_marker(marker);
            }
            None if has_data => {
                return Err(CasError::corrupt_index(
                    "index contains data but missing schema metadata for current unreleased format",
                ));
            }
            None => {
                let marker = latest_schema_marker();
                self.write_schema_marker_current(marker)?;
                self.set_cached_schema_marker(marker);
            }
        }
        Ok(())
    }

    /// Reads persisted schema marker from metadata table.
    fn read_schema_marker_value(&self) -> Result<Option<u32>, CasError> {
        let read = self.db.begin_read().map_err(CasError::redb)?;
        read_schema_marker_value_from_metadata(&read)
    }

    /// Writes current schema marker into metadata table.
    fn write_schema_marker_current(&self, schema_marker: u32) -> Result<(), CasError> {
        let write = self.db.begin_write().map_err(CasError::redb)?;
        {
            Self::set_schema_marker_current(&write, schema_marker)?;
        }
        write.commit().map_err(CasError::redb)
    }

    /// Writes encoded schema marker inside an existing write transaction.
    fn set_schema_marker_current(
        write: &redb::WriteTransaction,
        schema_marker: u32,
    ) -> Result<(), CasError> {
        let encoded = encode_current_schema_marker(schema_marker)?;
        write_schema_marker_to_metadata(write, &encoded)
    }

    /// Reloads bloom state from persisted payload or rebuilds it from primary rows.
    fn refresh_bloom_filter(&self) -> Result<(), CasError> {
        let schema_marker = self.schema_marker_for_operation()?;
        if let Some(persisted) = self.load_persisted_bloom_filter()? {
            self.replace_bloom_filter(persisted)?;
            return Ok(());
        }

        let read = self.db.begin_read().map_err(CasError::redb)?;
        let primary = open_primary_table_read(&read, schema_marker)?;
        let count = primary.iter().map_err(CasError::redb)?.count();

        let mut bloom = HashBloomFilter::with_capacity(count.max(1));
        for row in primary.iter().map_err(CasError::redb)? {
            let (key, _value) = row.map_err(CasError::redb)?;
            bloom.insert(hash_from_index_key(key.value())?);
        }

        self.store_bloom_filter(schema_marker, &bloom)?;
        self.replace_bloom_filter(bloom)?;
        Ok(())
    }

    /// Loads persisted bloom filter payload when available.
    fn load_persisted_bloom_filter(&self) -> Result<Option<HashBloomFilter>, CasError> {
        let schema_marker = self.schema_marker_for_operation()?;
        let read = self.db.begin_read().map_err(CasError::redb)?;
        let Some(payload) = read_bloom_payload_from_table(&read, schema_marker)? else {
            return Ok(None);
        };

        HashBloomFilter::decode(payload.as_slice(), schema_marker).map(Some)
    }

    /// Persists bloom filter payload in one dedicated write transaction.
    fn store_bloom_filter(
        &self,
        schema_marker: u32,
        bloom: &HashBloomFilter,
    ) -> Result<(), CasError> {
        let write = self.db.begin_write().map_err(CasError::redb)?;
        {
            Self::persist_bloom_state_table(&write, schema_marker, bloom)?;
        }
        write.commit().map_err(CasError::redb)
    }

    /// Writes bloom payload into provided write transaction.
    fn persist_bloom_state_table(
        write: &redb::WriteTransaction,
        schema_marker: u32,
        bloom: &HashBloomFilter,
    ) -> Result<(), CasError> {
        let payload = bloom.encode(schema_marker)?;
        write_bloom_payload_to_table(write, schema_marker, payload.as_slice())
    }

    /// Returns a clone of current in-memory bloom filter.
    fn snapshot_bloom_filter(&self) -> Result<HashBloomFilter, CasError> {
        self.bloom
            .read()
            .map_err(|err| CasError::poisoned_lock("index-bloom", "snapshotting bloom", err))
            .map(|guard| guard.clone())
    }

    /// Replaces in-memory bloom filter atomically.
    fn replace_bloom_filter(&self, bloom: HashBloomFilter) -> Result<(), CasError> {
        let mut guard = self
            .bloom
            .write()
            .map_err(|err| CasError::poisoned_lock("index-bloom", "replacing bloom", err))?;
        *guard = bloom;
        Ok(())
    }

    /// Performs read-transaction bloom prefilter for many hashes when payload exists.
    fn bloom_prefilter_many_from_read(
        &self,
        read: &redb::ReadTransaction,
        schema_marker: u32,
        hashes: &[Hash],
    ) -> Result<Option<Vec<bool>>, CasError> {
        let Some(payload) = read_bloom_payload_from_table(read, schema_marker)? else {
            return Ok(None);
        };

        let view = BloomPayloadView::from_payload(payload.as_slice(), schema_marker)?;
        Ok(Some(view.maybe_contains_many(hashes)))
    }
}

/// Validates core invariants for state loaded from durable storage.
fn validate_loaded_state(state: &IndexState) -> Result<(), CasError> {
    let empty = empty_content_hash();

    let Some(empty_meta) = state.objects.get(&empty) else {
        return Err(CasError::corrupt_index("missing canonical empty object metadata"));
    };
    if !empty_meta.is_full()
        || empty_meta.payload_len != 0
        || empty_meta.content_len != 0
        || empty_meta.depth() != 0
    {
        return Err(CasError::corrupt_index(
            "empty object metadata must be full/zero-sized/depth-0",
        ));
    }

    for (hash, meta) in &state.objects {
        if *hash == empty {
            continue;
        }

        match meta.encoding() {
            ObjectEncoding::Full => {
                if meta.depth() != 1 {
                    return Err(CasError::corrupt_index(format!(
                        "full object depth must be 1: hash={hash}, depth={}",
                        meta.depth()
                    )));
                }
            }
            ObjectEncoding::Delta { base_hash } => {
                let base_meta = state.objects.get(&base_hash).ok_or_else(|| {
                    CasError::corrupt_index(format!(
                        "delta base missing object record: hash={hash}, base={base_hash}"
                    ))
                })?;
                let expected_depth = base_meta.depth().checked_add(1).ok_or_else(|| {
                    CasError::corrupt_index(format!(
                        "delta depth overflow: hash={hash}, base={base_hash}, base_depth={}",
                        base_meta.depth()
                    ))
                })?;
                if expected_depth > MAX_DELTA_DEPTH {
                    return Err(CasError::corrupt_index(format!(
                        "delta depth exceeds configured limit: hash={hash}, base={base_hash}, expected={expected_depth}, max={MAX_DELTA_DEPTH}"
                    )));
                }
                if meta.depth() != expected_depth {
                    return Err(CasError::corrupt_index(format!(
                        "delta depth mismatch: hash={hash}, base={base_hash}, expected={expected_depth}, got={}",
                        meta.depth()
                    )));
                }
            }
        }
    }

    for (target, bases) in &state.constraints {
        if !state.objects.contains_key(target) {
            return Err(CasError::corrupt_index(format!(
                "constraint target missing object record: {target}"
            )));
        }
        for base in bases {
            if *base == *target {
                return Err(CasError::corrupt_index(format!(
                    "constraint row cannot include target as candidate: {target}"
                )));
            }
            if *base != empty && !state.objects.contains_key(base) {
                return Err(CasError::corrupt_index(format!(
                    "constraint base missing object record: target={target}, base={base}"
                )));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use bytes::Bytes;
    use redb::{Database, MultimapTableDefinition, ReadableTable, TableDefinition};
    use tempfile::tempdir;

    use super::{
        CasIndexDb, decode_bloom_payload, encode_bloom_payload, schema_marker_needs_initialization,
    };
    use crate::{CasApi, Constraint, FileSystemCas, Hash};

    const PRIMARY_OBJECT_TABLE: TableDefinition<&[u8], &[u8]> =
        TableDefinition::new("primary_index");
    const CONSTRAINTS_TABLE: MultimapTableDefinition<&[u8], &[u8]> =
        MultimapTableDefinition::new("primary_constraints");
    const INDEX_METADATA_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("metadata");
    const SCHEMA_MARKER_KEY: &[u8] = b"version";

    #[tokio::test]
    async fn index_persists_objects_across_reopen() {
        let dir = tempdir().expect("tempdir");
        let hash = {
            let cas = FileSystemCas::open_for_tests(dir.path()).await.expect("open cas");
            cas.put(Bytes::from_static(b"persist-index")).await.expect("put")
        };

        let reopened = FileSystemCas::open_for_tests(dir.path()).await.expect("reopen cas");
        let restored = reopened.get(hash).await.expect("get");

        assert_eq!(restored, Bytes::from_static(b"persist-index"));
    }

    #[tokio::test]
    async fn index_omits_empty_only_constraint_entries() {
        let dir = tempdir().expect("tempdir");
        let cas = FileSystemCas::open_for_tests(dir.path()).await.expect("open cas");
        let target =
            cas.put(Bytes::from_static(b"empty-implicit-target")).await.expect("put target");

        cas.set_constraint(Constraint { target_hash: target, potential_bases: BTreeSet::new() })
            .await
            .expect("empty-only constraint should be implicit");

        assert_eq!(
            cas.constraint_bases(target).await.expect("constraint bases"),
            Vec::<Hash>::new()
        );

        drop(cas);

        let db_path = dir.path().join("index.redb");
        let mut db = None;
        for _ in 0..30 {
            match Database::open(&db_path) {
                Ok(handle) => {
                    db = Some(handle);
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
        let db = db.expect("open redb index after filesystem actor shutdown");
        let read = db.begin_read().expect("begin read transaction");
        let constraints =
            read.open_multimap_table(CONSTRAINTS_TABLE).expect("open constraints table");
        let key = target.storage_bytes();
        let rows = constraints.get(key.as_slice()).expect("read constraints row set");

        assert_eq!(rows.count(), 0, "expected no explicit empty-only constraints for target");
    }

    #[tokio::test]
    async fn set_constraint_rejects_missing_constraint_candidates() {
        let dir = tempdir().expect("tempdir");
        let cas = FileSystemCas::open_for_tests(dir.path()).await.expect("open cas");
        let target = cas.put(Bytes::from_static(b"target")).await.expect("put target");
        let missing = Hash::from_content(b"missing");

        let result = cas
            .set_constraint(Constraint {
                target_hash: target,
                potential_bases: BTreeSet::from([missing]),
            })
            .await;

        assert!(result.is_err(), "missing-base constraints must be rejected");
    }

    #[test]
    fn open_initializes_schema_metadata_marker() {
        let dir = tempdir().expect("tempdir");
        let index = CasIndexDb::open(dir.path()).expect("open index db");

        let read = index.db.begin_read().expect("begin read");
        let meta = read.open_table(INDEX_METADATA_TABLE).expect("open metadata table");
        let schema = meta
            .get(SCHEMA_MARKER_KEY)
            .expect("get schema metadata")
            .expect("schema metadata exists");
        let metadata_entries = meta.iter().expect("iterate metadata rows").count();
        assert_eq!(
            metadata_entries, 1,
            "metadata table must contain only the schema version entry"
        );
        assert!(
            !schema_marker_needs_initialization(Some(schema.value()), true)
                .expect("schema marker evaluation should succeed"),
            "persisted schema metadata should already match current format marker"
        );

        let primary = read.open_table(PRIMARY_OBJECT_TABLE).expect("open primary index table");
        let mut rows = primary.iter().expect("iterate primary rows");
        assert!(rows.next().is_none(), "new index should have no primary rows");

        let schema_marker = super::latest_schema_marker();
        let bloom_payload = super::read_bloom_payload_from_table(&read, schema_marker)
            .expect("read bloom payload")
            .expect("bloom payload exists");
        assert!(!bloom_payload.is_empty(), "new index should persist an initialized bloom payload");
    }

    #[test]
    fn bloom_payload_decode_accepts_valid_payload() {
        let schema_marker = super::latest_schema_marker();
        let payload = encode_bloom_payload(schema_marker, 128, &[0_u64, 0_u64])
            .expect("encode valid bloom payload");

        let (bit_len, raw_words) =
            decode_bloom_payload(schema_marker, &payload).expect("decode valid payload");
        assert_eq!(bit_len, 128);
        assert_eq!(raw_words.len(), 16);
    }

    #[test]
    fn bloom_payload_decode_rejects_word_width_mismatch() {
        let schema_marker = super::latest_schema_marker();
        let mut payload = encode_bloom_payload(schema_marker, 128, &[0_u64, 0_u64])
            .expect("encode valid bloom payload");
        payload.truncate(payload.len() - 8);

        let error = decode_bloom_payload(schema_marker, &payload)
            .expect_err("word width mismatch must fail");
        assert!(matches!(error, crate::CasError::CorruptIndex(_)));
    }
}
