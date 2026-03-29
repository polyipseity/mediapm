//! In-memory CAS backend.
//!
//! This implementation uses sharded `DashMap` collections for object and
//! constraint storage so reads and unrelated writes do not contend on one
//! global lock. A small coordination gate is used only for multi-step
//! maintenance operations that must apply atomically.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use futures_util::stream;
use smallvec::SmallVec;
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;

use crate::storage::{
    StreamBufferPool, is_unconstrained_constraint_row, normalize_explicit_constraint_set,
    validate_constraint_target_not_in_bases,
};
use crate::{
    CasApi, CasByteReader, CasByteStream, CasError, CasMaintenanceApi, Constraint, ConstraintPatch,
    DeltaPatch, Hash, HashAlgorithm, ObjectInfo, OptimizeOptions, OptimizeReport, PruneReport,
    StoredObject, empty_content_hash,
};

mod reconstruction;
use reconstruction::{build_reconstruction_plan, ensure_reconstructed_hash};

const IN_MEMORY_DEFAULT_MAX_OBJECT_SIZE_BYTES: usize = 64 * 1024 * 1024;
const IN_MEMORY_STREAM_READ_CHUNK_BYTES: usize = 32 * 1024;
const IN_MEMORY_SMALL_DEPENDENT_INLINE: usize = 8;
const IN_MEMORY_STREAM_BUFFER_POOL_MAX_BUFFERS: usize = 64;

/// In-memory CAS implementation used for tests and local integration.
pub struct InMemoryCas {
    objects: Arc<DashMap<Hash, StoredObject>>,
    base_dependents: Arc<DashMap<Hash, BTreeSet<Hash>>>,
    constraints: Arc<DashMap<Hash, BTreeSet<Hash>>>,
    mutation_gate: Arc<Mutex<()>>,
    stream_buffer_pool: Arc<StreamBufferPool>,
    max_object_size_bytes: usize,
}

impl Default for InMemoryCas {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryCas {
    /// Creates an empty in-memory CAS.
    pub fn new() -> Self {
        Self::with_max_object_size_bytes(IN_MEMORY_DEFAULT_MAX_OBJECT_SIZE_BYTES)
    }

    /// Creates an empty in-memory CAS with an explicit streamed object size cap.
    ///
    /// The cap is enforced by `put_stream`/`put_stream_with_constraints` before
    /// bytes are fully buffered, preventing accidental OOM in test environments.
    pub fn with_max_object_size_bytes(max_object_size_bytes: usize) -> Self {
        Self {
            objects: Arc::new(DashMap::new()),
            base_dependents: Arc::new(DashMap::new()),
            constraints: Arc::new(DashMap::new()),
            mutation_gate: Arc::new(Mutex::new(())),
            stream_buffer_pool: StreamBufferPool::new(
                IN_MEMORY_STREAM_READ_CHUNK_BYTES,
                IN_MEMORY_STREAM_BUFFER_POOL_MAX_BUFFERS,
            ),
            max_object_size_bytes,
        }
    }

    async fn put_stream_incremental(&self, mut reader: CasByteReader) -> Result<Hash, CasError> {
        let mut payload = BytesMut::with_capacity(IN_MEMORY_STREAM_READ_CHUNK_BYTES);
        let mut chunk = self.stream_buffer_pool.lease();

        loop {
            chunk.clear();
            let read = reader.read_buf(&mut *chunk).await.map_err(|err| {
                CasError::stream_io("reading source stream during in-memory put_stream", err)
            })?;
            if read == 0 {
                break;
            }

            let next_len = payload.len().checked_add(read).ok_or_else(|| {
                CasError::invalid_input("in-memory streamed object size overflowed usize")
            })?;
            if next_len > self.max_object_size_bytes {
                return Err(CasError::invalid_input(format!(
                    "in-memory streamed object exceeded max_object_size_bytes (limit={}, attempted={next_len})",
                    self.max_object_size_bytes
                )));
            }

            payload.extend_from_slice(chunk.as_ref());
        }

        let hash = Hash::from_content_with_algorithm(HashAlgorithm::Blake3, payload.as_ref());
        self.put_hashed(hash, payload.to_vec())
    }

    fn put_hashed(&self, hash: Hash, payload: Vec<u8>) -> Result<Hash, CasError> {
        if hash == empty_content_hash() {
            return Ok(hash);
        }

        let candidate_len = payload.len() as u64;
        match self.objects.entry(hash) {
            Entry::Occupied(existing) => {
                let existing_len = existing.get().content_len();
                if existing_len != candidate_len {
                    return Err(CasError::HashCollisionLengthMismatch {
                        hash,
                        existing_len,
                        candidate_len,
                    });
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(StoredObject::full(payload));
            }
        }

        Ok(hash)
    }

    fn reconstruct_live(&self, hash: Hash) -> Result<Bytes, CasError> {
        if hash == empty_content_hash() {
            return Ok(Bytes::new());
        }

        let plan = build_reconstruction_plan(&self.objects, hash)?;
        let base_payload = match self
            .objects
            .get(&plan.base_hash)
            .ok_or(CasError::NotFound(plan.base_hash))?
            .value()
        {
            StoredObject::Full { payload } => payload.clone(),
            StoredObject::Delta { .. } => {
                return Err(CasError::corrupt_object(format!(
                    "reconstruction base for {hash} must be a full object"
                )));
            }
        };

        let mut data = base_payload;

        for delta_hash in plan.delta_chain.into_iter().rev() {
            let object = self.objects.get(&delta_hash).ok_or(CasError::NotFound(delta_hash))?;
            let StoredObject::Delta { state } = object.value() else {
                return Err(CasError::corrupt_object(format!(
                    "expected delta object while replaying reconstruction chain for {hash} at {delta_hash}"
                )));
            };

            let patch = DeltaPatch::decode(state.payload.as_ref())?;
            data = patch.apply(&data)?;
        }

        if data.len() != plan.final_len {
            return Err(CasError::corrupt_object(format!(
                "in-memory reconstructed size mismatch for {hash}: expected {}, got {}",
                plan.final_len,
                data.len()
            )));
        }

        ensure_reconstructed_hash(hash, &data, "in-memory reconstruction")?;

        Ok(Bytes::from(data))
    }

    fn remove_constraint_references(&self, hash: Hash) {
        self.constraints.remove(&hash);

        let keys: SmallVec<[Hash; IN_MEMORY_SMALL_DEPENDENT_INLINE]> =
            self.constraints.iter().map(|entry| *entry.key()).collect();
        for key in keys {
            if let Some(mut bases) = self.constraints.get_mut(&key) {
                bases.remove(&hash);
                let should_remove_row = is_unconstrained_constraint_row(&bases);
                drop(bases);
                if should_remove_row {
                    self.constraints.remove(&key);
                }
            }
        }
    }

    fn unconstrained_candidate_bases_projected(
        &self,
        overlay_updates: &HashMap<Hash, StoredObject>,
        overlay_deleted: &HashSet<Hash>,
    ) -> BTreeSet<Hash> {
        let mut candidates: BTreeSet<Hash> = self
            .objects
            .iter()
            .filter_map(|entry| {
                let hash = *entry.key();
                (!overlay_deleted.contains(&hash)).then_some(hash)
            })
            .collect();

        for hash in overlay_updates.keys().copied() {
            if !overlay_deleted.contains(&hash) {
                candidates.insert(hash);
            }
        }

        candidates.insert(empty_content_hash());
        candidates
    }

    fn link_dependent_to_base(&self, base_hash: Hash, dependent: Hash) {
        if base_hash == empty_content_hash() {
            return;
        }

        match self.base_dependents.entry(base_hash) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().insert(dependent);
            }
            Entry::Vacant(entry) => {
                entry.insert(BTreeSet::from([dependent]));
            }
        }
    }

    fn unlink_dependent_from_base(&self, base_hash: Hash, dependent: Hash) {
        if base_hash == empty_content_hash() {
            return;
        }

        let mut remove_row = false;
        if let Some(mut dependents) = self.base_dependents.get_mut(&base_hash) {
            dependents.remove(&dependent);
            remove_row = dependents.is_empty();
        }

        if remove_row {
            self.base_dependents.remove(&base_hash);
        }
    }

    fn sync_reverse_dependents(
        &self,
        dependent: Hash,
        previous_base: Option<Hash>,
        next_base: Option<Hash>,
    ) {
        if previous_base == next_base {
            return;
        }

        if let Some(previous_base) = previous_base {
            self.unlink_dependent_from_base(previous_base, dependent);
        }
        if let Some(next_base) = next_base {
            self.link_dependent_to_base(next_base, dependent);
        }
    }

    fn upsert_object_with_reverse_index(&self, hash: Hash, object: StoredObject) {
        let next_base = object.base_hash();
        let previous = self.objects.insert(hash, object);
        let previous_base = previous.as_ref().and_then(StoredObject::base_hash);
        self.sync_reverse_dependents(hash, previous_base, next_base);
    }

    fn remove_object_with_reverse_index(&self, hash: Hash) -> Option<StoredObject> {
        let removed = self.objects.remove(&hash).map(|(_, object)| object);
        if let Some(previous_base) = removed.as_ref().and_then(StoredObject::base_hash) {
            self.unlink_dependent_from_base(previous_base, hash);
        }
        self.base_dependents.remove(&hash);
        removed
    }

    fn direct_dependents_for_base(
        &self,
        base_hash: Hash,
    ) -> SmallVec<[Hash; IN_MEMORY_SMALL_DEPENDENT_INLINE]> {
        if let Some(dependents) = self.base_dependents.get(&base_hash) {
            let mut listed: SmallVec<[Hash; IN_MEMORY_SMALL_DEPENDENT_INLINE]> =
                dependents.iter().copied().collect();
            listed.sort_unstable();
            return listed;
        }

        // Fallback safety scan for any state not yet represented in the reverse index.
        let mut listed: SmallVec<[Hash; IN_MEMORY_SMALL_DEPENDENT_INLINE]> = self
            .objects
            .iter()
            .filter_map(|entry| match entry.value() {
                StoredObject::Delta { state } if state.base_hash == base_hash => Some(*entry.key()),
                _ => None,
            })
            .collect();
        listed.sort_unstable();
        listed
    }

    fn projected_object_meta(
        &self,
        hash: Hash,
        overlay_updates: &HashMap<Hash, StoredObject>,
        overlay_deleted: &HashSet<Hash>,
    ) -> Result<(u64, Option<Hash>), CasError> {
        if overlay_deleted.contains(&hash) {
            return Err(CasError::NotFound(hash));
        }

        if let Some(object) = overlay_updates.get(&hash) {
            return Ok((object.content_len(), object.base_hash()));
        }

        let object = self.objects.get(&hash).ok_or(CasError::NotFound(hash))?;
        Ok((object.content_len(), object.base_hash()))
    }

    fn projected_full_payload(
        &self,
        hash: Hash,
        overlay_updates: &HashMap<Hash, StoredObject>,
        overlay_deleted: &HashSet<Hash>,
    ) -> Result<Vec<u8>, CasError> {
        if overlay_deleted.contains(&hash) {
            return Err(CasError::NotFound(hash));
        }

        if let Some(object) = overlay_updates.get(&hash) {
            return match object {
                StoredObject::Full { payload } => Ok(payload.clone()),
                StoredObject::Delta { .. } => Err(CasError::corrupt_object(format!(
                    "projected reconstruction base for {hash} must be full"
                ))),
            };
        }

        let object = self.objects.get(&hash).ok_or(CasError::NotFound(hash))?;
        match object.value() {
            StoredObject::Full { payload } => Ok(payload.clone()),
            StoredObject::Delta { .. } => Err(CasError::corrupt_object(format!(
                "projected reconstruction base for {hash} must be full"
            ))),
        }
    }

    fn apply_projected_delta(
        &self,
        delta_hash: Hash,
        current: &[u8],
        overlay_updates: &HashMap<Hash, StoredObject>,
        overlay_deleted: &HashSet<Hash>,
    ) -> Result<Vec<u8>, CasError> {
        if overlay_deleted.contains(&delta_hash) {
            return Err(CasError::NotFound(delta_hash));
        }

        if let Some(object) = overlay_updates.get(&delta_hash) {
            let StoredObject::Delta { state } = object else {
                return Err(CasError::corrupt_object(format!(
                    "expected projected delta object while replaying chain at {delta_hash}"
                )));
            };
            let patch = DeltaPatch::decode(state.payload.as_ref())?;
            return patch.apply(current);
        }

        let object = self.objects.get(&delta_hash).ok_or(CasError::NotFound(delta_hash))?;
        let StoredObject::Delta { state } = object.value() else {
            return Err(CasError::corrupt_object(format!(
                "expected projected delta object while replaying chain at {delta_hash}"
            )));
        };
        let patch = DeltaPatch::decode(state.payload.as_ref())?;
        patch.apply(current)
    }

    fn reconstruct_projected(
        &self,
        hash: Hash,
        overlay_updates: &HashMap<Hash, StoredObject>,
        overlay_deleted: &HashSet<Hash>,
    ) -> Result<Vec<u8>, CasError> {
        if hash == empty_content_hash() {
            return Ok(Vec::new());
        }

        let mut current = hash;
        let mut visited = HashSet::new();
        let mut delta_chain: SmallVec<[Hash; IN_MEMORY_SMALL_DEPENDENT_INLINE]> = SmallVec::new();
        let mut final_len: Option<usize> = None;

        let base_hash = loop {
            if !visited.insert(current) {
                return Err(CasError::CycleDetected {
                    target: hash,
                    detail: format!("loop encountered at {current}"),
                });
            }

            let (content_len, base_hash) =
                self.projected_object_meta(current, overlay_updates, overlay_deleted)?;
            let content_len_usize = usize::try_from(content_len).map_err(|_| {
                CasError::corrupt_object(format!(
                    "invalid projected content length for reconstruction target {hash}"
                ))
            })?;

            if final_len.is_none() {
                final_len = Some(content_len_usize);
            }

            match base_hash {
                Some(base_hash) => {
                    delta_chain.push(current);
                    current = base_hash;
                }
                None => break current,
            }
        };

        let final_len = final_len.ok_or_else(|| {
            CasError::corrupt_object(format!(
                "invalid projected final content length for reconstruction target {hash}"
            ))
        })?;

        let mut data = self.projected_full_payload(base_hash, overlay_updates, overlay_deleted)?;

        for delta_hash in delta_chain.into_iter().rev() {
            data =
                self.apply_projected_delta(delta_hash, &data, overlay_updates, overlay_deleted)?;
        }

        if data.len() != final_len {
            return Err(CasError::corrupt_object(format!(
                "in-memory projected reconstructed size mismatch for {hash}: expected {final_len}, got {}",
                data.len()
            )));
        }

        ensure_reconstructed_hash(hash, &data, "in-memory projected overlay reconstruction")?;

        Ok(data)
    }

    fn merge_constraint_patch(
        existing: Option<&BTreeSet<Hash>>,
        patch: ConstraintPatch,
    ) -> BTreeSet<Hash> {
        let mut merged = if patch.clear_existing {
            BTreeSet::new()
        } else {
            existing.cloned().unwrap_or_default()
        };

        for base in patch.remove_bases {
            merged.remove(&base);
        }
        for base in patch.add_bases {
            merged.insert(base);
        }

        merged
    }

    fn set_normalized_constraint_row(
        &self,
        target_hash: Hash,
        candidate_bases: BTreeSet<Hash>,
    ) -> Option<BTreeSet<Hash>> {
        let explicit = normalize_explicit_constraint_set(candidate_bases);

        if let Some(ref explicit_bases) = explicit {
            self.constraints.insert(target_hash, explicit_bases.clone());
        } else {
            self.constraints.remove(&target_hash);
        }

        explicit
    }
}

#[async_trait]
impl CasApi for InMemoryCas {
    async fn exists(&self, hash: Hash) -> Result<bool, CasError> {
        if hash == empty_content_hash() {
            return Ok(true);
        }

        Ok(self.objects.contains_key(&hash))
    }

    async fn put<D>(&self, data: D) -> Result<Hash, CasError>
    where
        D: TryInto<Bytes> + Send,
        D::Error: std::fmt::Display + Send,
    {
        let data: Bytes = data.try_into().map_err(|err| {
            CasError::invalid_input(format!("failed to convert input into bytes: {err}"))
        })?;

        let hash = Hash::from_content(&data);
        self.put_hashed(hash, data.to_vec())
    }

    async fn put_with_constraints<D>(
        &self,
        data: D,
        bases: BTreeSet<Hash>,
    ) -> Result<Hash, CasError>
    where
        D: TryInto<Bytes> + Send,
        D::Error: std::fmt::Display + Send,
    {
        let hash = self.put(data).await?;
        self.set_constraint(Constraint { target_hash: hash, potential_bases: bases }).await?;
        Ok(hash)
    }

    async fn put_stream(&self, reader: CasByteReader) -> Result<Hash, CasError> {
        self.put_stream_incremental(reader).await
    }

    async fn put_stream_with_constraints(
        &self,
        reader: CasByteReader,
        bases: BTreeSet<Hash>,
    ) -> Result<Hash, CasError> {
        let hash = self.put_stream(reader).await?;
        self.set_constraint(Constraint { target_hash: hash, potential_bases: bases }).await?;
        Ok(hash)
    }

    async fn get(&self, hash: Hash) -> Result<Bytes, CasError> {
        self.reconstruct_live(hash)
    }

    async fn get_stream(&self, hash: Hash) -> Result<CasByteStream, CasError> {
        let bytes = self.get(hash).await?;
        Ok(Box::pin(stream::once(async move { Ok(bytes) })))
    }

    async fn info(&self, hash: Hash) -> Result<ObjectInfo, CasError> {
        if hash == empty_content_hash() {
            return Ok(ObjectInfo {
                content_len: 0,
                payload_len: 0,
                is_delta: false,
                base_hash: None,
            });
        }

        let object = self.objects.get(&hash).ok_or(CasError::NotFound(hash))?;
        Ok(ObjectInfo {
            content_len: object.content_len(),
            payload_len: object.payload_len(),
            is_delta: object.base_hash().is_some(),
            base_hash: object.base_hash(),
        })
    }

    async fn delete(&self, hash: Hash) -> Result<(), CasError> {
        if hash == empty_content_hash() {
            return Err(CasError::invalid_constraint(
                "cannot delete implicit empty-content root".to_string(),
            ));
        }

        let _gate = self.mutation_gate.lock().await;

        if !self.objects.contains_key(&hash) {
            return Err(CasError::NotFound(hash));
        }

        let dependents = self.direct_dependents_for_base(hash);
        let mut overlay_updates = HashMap::<Hash, StoredObject>::new();
        let mut overlay_deleted = HashSet::<Hash>::from([hash]);

        for dependent in &dependents {
            let payload =
                self.reconstruct_projected(*dependent, &overlay_updates, &overlay_deleted)?;
            let payload_len = payload.len() as u64;

            let mut candidates = match self.constraints.get(dependent) {
                Some(explicit) => normalize_explicit_constraint_set(explicit.clone())
                    .unwrap_or_else(|| {
                        self.unconstrained_candidate_bases_projected(
                            &overlay_updates,
                            &overlay_deleted,
                        )
                    }),
                None => {
                    self.unconstrained_candidate_bases_projected(&overlay_updates, &overlay_deleted)
                }
            };
            candidates.remove(&hash);
            candidates.remove(dependent);

            let selected = candidates.into_iter().next().ok_or_else(|| {
                CasError::invalid_constraint(format!(
                    "cannot preserve dependent object {dependent} after deleting {hash}"
                ))
            })?;

            let rewritten_object = if selected == empty_content_hash() {
                StoredObject::full(payload)
            } else {
                let base_payload =
                    self.reconstruct_projected(selected, &overlay_updates, &overlay_deleted)?;
                let patch = DeltaPatch::diff(&base_payload, &payload)?;
                StoredObject::delta(selected, payload_len, patch.encode().to_vec())
            };

            overlay_updates.insert(*dependent, rewritten_object);
        }

        for (dependent, object) in overlay_updates {
            self.upsert_object_with_reverse_index(dependent, object);
        }
        let _ = self.remove_object_with_reverse_index(hash);
        overlay_deleted.insert(hash);
        self.remove_constraint_references(hash);

        Ok(())
    }

    async fn set_constraint(&self, constraint: Constraint) -> Result<(), CasError> {
        let _gate = self.mutation_gate.lock().await;

        let set: BTreeSet<Hash> = constraint.potential_bases;
        validate_constraint_target_not_in_bases(constraint.target_hash, &set)?;

        if !self.objects.contains_key(&constraint.target_hash)
            && constraint.target_hash != empty_content_hash()
        {
            return Err(CasError::NotFound(constraint.target_hash));
        }

        for base in &set {
            if *base != empty_content_hash() && !self.objects.contains_key(base) {
                return Err(CasError::NotFound(*base));
            }
        }

        self.set_normalized_constraint_row(constraint.target_hash, set);

        Ok(())
    }

    async fn patch_constraint(
        &self,
        target_hash: Hash,
        patch: ConstraintPatch,
    ) -> Result<Option<Constraint>, CasError> {
        let _gate = self.mutation_gate.lock().await;

        if target_hash != empty_content_hash() && !self.objects.contains_key(&target_hash) {
            return Err(CasError::NotFound(target_hash));
        }
        for base in &patch.add_bases {
            if *base != empty_content_hash() && !self.objects.contains_key(base) {
                return Err(CasError::NotFound(*base));
            }
        }

        let merged =
            Self::merge_constraint_patch(self.constraints.get(&target_hash).as_deref(), patch);

        validate_constraint_target_not_in_bases(target_hash, &merged)?;

        Ok(self
            .set_normalized_constraint_row(target_hash, merged)
            .map(|potential_bases| Constraint { target_hash, potential_bases }))
    }

    async fn get_constraint(&self, hash: Hash) -> Result<Option<Constraint>, CasError> {
        if hash != empty_content_hash() && !self.objects.contains_key(&hash) {
            return Err(CasError::NotFound(hash));
        }

        Ok(self.constraints.get(&hash).and_then(|potential_bases| {
            normalize_explicit_constraint_set(potential_bases.value().clone())
                .map(|explicit| Constraint { target_hash: hash, potential_bases: explicit })
        }))
    }
}

#[async_trait]
impl CasMaintenanceApi for InMemoryCas {
    async fn optimize_once(&self, _options: OptimizeOptions) -> Result<OptimizeReport, CasError> {
        Ok(OptimizeReport { rewritten_objects: 0 })
    }

    async fn prune_constraints(&self) -> Result<PruneReport, CasError> {
        let _gate = self.mutation_gate.lock().await;

        let existing: HashSet<Hash> = self.objects.iter().map(|entry| *entry.key()).collect();

        let keys: Vec<Hash> = self.constraints.iter().map(|entry| *entry.key()).collect();
        let mut removed = 0usize;

        for key in keys {
            if !existing.contains(&key) {
                if let Some((_, bases)) = self.constraints.remove(&key) {
                    removed += bases.len();
                }
                continue;
            }

            if let Some(mut bases) = self.constraints.get_mut(&key) {
                let before = bases.len();
                bases.retain(|candidate| {
                    *candidate == empty_content_hash() || existing.contains(candidate)
                });
                removed += before.saturating_sub(bases.len());
                let should_remove_row = is_unconstrained_constraint_row(&bases);
                drop(bases);
                if should_remove_row {
                    self.constraints.remove(&key);
                }
            }
        }

        Ok(PruneReport { removed_candidates: removed })
    }

    async fn repair_index(&self) -> Result<crate::IndexRepairReport, CasError> {
        Ok(crate::IndexRepairReport {
            object_rows_rebuilt: 0,
            explicit_constraint_rows_restored: 0,
            scanned_object_files: 0,
            skipped_object_files: 0,
            backup_snapshots_considered: 0,
            constraint_source: crate::IndexRepairConstraintSource::None,
        })
    }

    async fn migrate_index_to_version(&self, _target_version: u32) -> Result<(), CasError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use bytes::Bytes;
    use tokio::io::AsyncReadExt;

    use super::InMemoryCas;
    use crate::{
        CasApi, CasError, Constraint, ConstraintPatch, Hash, HashAlgorithm, empty_content_hash,
    };

    #[tokio::test]
    async fn in_memory_put_get_delete_lifecycle() {
        let cas = InMemoryCas::new();
        let payload = Bytes::from_static(b"delete-me");
        let hash = cas.put(payload.clone()).await.expect("put payload");
        let restored = cas.get(hash).await.expect("get payload");
        assert_eq!(restored, payload);

        cas.delete(hash).await.expect("delete payload");
        let missing = cas.get(hash).await;
        assert!(missing.is_err());
    }

    #[tokio::test]
    async fn in_memory_set_constraint_rejects_missing_base() {
        let cas = InMemoryCas::new();
        let target =
            cas.put(Bytes::from_static(b"target")).await.expect("target store should succeed");
        let missing = Hash::from_content(b"missing");
        let result = cas
            .set_constraint(Constraint {
                target_hash: target,
                potential_bases: BTreeSet::from([missing]),
            })
            .await;

        assert!(result.is_err(), "missing-base constraints must be rejected");
        assert_ne!(empty_content_hash(), target);
    }

    #[tokio::test]
    async fn in_memory_exists_many_returns_ordered_bitset() {
        let cas = InMemoryCas::new();
        let a = cas.put(Bytes::from_static(b"a")).await.expect("put a");
        let b = cas.put(Bytes::from_static(b"b")).await.expect("put b");
        let missing = Hash::from_content(b"missing");

        let flags = cas.exists_many(vec![a, missing, b]).await.expect("exists_many");

        assert_eq!(flags.iter().by_vals().collect::<Vec<_>>(), vec![true, false, true]);
    }

    #[tokio::test]
    async fn in_memory_patch_constraint_add_remove_and_clear() {
        let cas = InMemoryCas::new();
        let target = cas.put(Bytes::from_static(b"target")).await.expect("put target");
        let base_a = cas.put(Bytes::from_static(b"base-a")).await.expect("put base_a");
        let base_b = cas.put(Bytes::from_static(b"base-b")).await.expect("put base_b");

        cas.set_constraint(Constraint {
            target_hash: target,
            potential_bases: BTreeSet::from([base_a]),
        })
        .await
        .expect("set initial constraint");

        let patched = cas
            .patch_constraint(
                target,
                ConstraintPatch {
                    add_bases: BTreeSet::from([base_b]),
                    remove_bases: BTreeSet::from([base_a]),
                    clear_existing: false,
                },
            )
            .await
            .expect("patch constraint");

        assert_eq!(
            patched,
            Some(Constraint { target_hash: target, potential_bases: BTreeSet::from([base_b]) })
        );

        let cleared = cas
            .patch_constraint(
                target,
                ConstraintPatch {
                    add_bases: BTreeSet::new(),
                    remove_bases: BTreeSet::new(),
                    clear_existing: true,
                },
            )
            .await
            .expect("clear explicit constraint");
        assert!(cleared.is_none());
        assert!(cas.get_constraint(target).await.expect("get constraint").is_none());
    }

    #[tokio::test]
    async fn in_memory_put_stream_with_constraints_sets_constraint() {
        let cas = InMemoryCas::new();
        let base = cas.put(Bytes::from_static(b"stream-base")).await.expect("put base");

        let reader = Box::new(tokio::io::repeat(b'x').take(4));
        let hash = cas
            .put_stream_with_constraints(reader, BTreeSet::from([base]))
            .await
            .expect("put_stream_with_constraints");

        let bytes = cas.get(hash).await.expect("get streamed object");
        assert_eq!(bytes, Bytes::from_static(b"xxxx"));

        let constraint = cas.get_constraint(hash).await.expect("get constraint row");
        assert_eq!(
            constraint,
            Some(Constraint { target_hash: hash, potential_bases: BTreeSet::from([base]) })
        );
    }

    #[tokio::test]
    async fn in_memory_put_stream_rejects_payload_over_configured_limit() {
        let cas = InMemoryCas::with_max_object_size_bytes(4);

        let reader = Box::new(tokio::io::repeat(b'x').take(5));
        let error = cas.put_stream(reader).await.expect_err("stream over limit must fail");

        assert!(matches!(error, CasError::InvalidInput(_)));
        assert!(error.to_string().contains("max_object_size_bytes"));
    }

    #[tokio::test]
    async fn in_memory_put_stream_hash_matches_multihash_identity() {
        let cas = InMemoryCas::new();
        let len = (super::IN_MEMORY_STREAM_READ_CHUNK_BYTES * 2) + 113;
        let payload = vec![b'q'; len];
        let expected = Hash::from_content_with_algorithm(HashAlgorithm::Blake3, &payload);

        let reader = Box::new(tokio::io::repeat(b'q').take(len as u64));
        let hash = cas.put_stream(reader).await.expect("put_stream");

        assert_eq!(hash, expected);
        assert_eq!(cas.get(hash).await.expect("get streamed payload").len(), len);
    }

    #[tokio::test]
    async fn in_memory_get_detects_hash_payload_mismatch() {
        let cas = InMemoryCas::new();

        let wrong_payload = b"corrupted-payload".to_vec();
        let hash = Hash::from_content(b"expected-payload");
        cas.objects.insert(hash, crate::StoredObject::full(wrong_payload));

        let err = cas.get(hash).await.expect_err("corrupt in-memory object should fail");
        assert!(matches!(err, CasError::CorruptObject(_)));
    }

    #[tokio::test]
    async fn in_memory_info_many_returns_ordered_metadata() {
        let cas = InMemoryCas::new();
        let a = cas.put(Bytes::from_static(b"aa")).await.expect("put a");
        let b = cas.put(Bytes::from_static(b"bbb")).await.expect("put b");

        let infos = cas.info_many(vec![b, a]).await.expect("info_many");

        assert_eq!(infos.len(), 2);
        assert_eq!(infos[0].0, b);
        assert_eq!(infos[0].1.content_len, 3);
        assert_eq!(infos[1].0, a);
        assert_eq!(infos[1].1.content_len, 2);
    }

    #[tokio::test]
    async fn in_memory_get_constraint_many_returns_ordered_rows() {
        let cas = InMemoryCas::new();
        let base = cas.put(Bytes::from_static(b"base")).await.expect("put base");
        let constrained =
            cas.put(Bytes::from_static(b"constrained")).await.expect("put constrained");
        let unrestricted = cas.put(Bytes::from_static(b"free")).await.expect("put unrestricted");

        cas.set_constraint(Constraint {
            target_hash: constrained,
            potential_bases: BTreeSet::from([base]),
        })
        .await
        .expect("set constraint");

        let rows = cas
            .get_constraint_many(vec![unrestricted, constrained])
            .await
            .expect("get_constraint_many");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], (unrestricted, None));
        assert_eq!(
            rows[1],
            (
                constrained,
                Some(Constraint {
                    target_hash: constrained,
                    potential_bases: BTreeSet::from([base])
                }),
            )
        );
    }

    #[tokio::test]
    async fn in_memory_delete_many_removes_all_hashes() {
        let cas = InMemoryCas::new();
        let a = cas.put(Bytes::from_static(b"a")).await.expect("put a");
        let b = cas.put(Bytes::from_static(b"b")).await.expect("put b");

        cas.delete_many(vec![a, b]).await.expect("delete_many");

        assert!(cas.get(a).await.is_err());
        assert!(cas.get(b).await.is_err());
    }
}
