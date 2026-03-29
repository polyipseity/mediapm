//! Reconstruction helpers for the in-memory CAS backend.
//!
//! This module keeps the in-memory backend's reconstruction logic explicit and
//! deterministic by separating:
//! - chain planning (`build_reconstruction_plan`),
//! - replay-side integrity verification (`ensure_reconstructed_hash`).

use std::collections::HashSet;

use dashmap::DashMap;
use smallvec::SmallVec;

use crate::{CasError, Hash, StoredObject};

use super::IN_MEMORY_SMALL_DEPENDENT_INLINE;

/// Precomputed reconstruction metadata for one target hash.
///
/// The plan captures the immutable inputs required to replay delta patches from
/// a full-object base up to the requested target.
#[derive(Debug)]
pub(super) struct InMemoryReconstructionPlan {
    /// Expected byte length of the final reconstructed payload.
    pub(super) final_len: usize,
    /// Hash of the full-object base from which replay starts.
    pub(super) base_hash: Hash,
    /// Ordered chain of delta hashes from target down toward `base_hash`.
    pub(super) delta_chain: SmallVec<[Hash; IN_MEMORY_SMALL_DEPENDENT_INLINE]>,
}

/// Builds a deterministic replay plan for reconstructing `hash`.
///
/// The function walks parent links from `hash` to a full-object base while:
/// - detecting cycles,
/// - recording the delta chain,
/// - and collecting size hints used to pre-size buffers.
///
/// # Errors
/// Returns [`CasError::NotFound`](crate::CasError::NotFound) when any required
/// object is absent, [`CasError::CycleDetected`](crate::CasError::CycleDetected)
/// for cyclic ancestry, or [`CasError::CorruptObject`](crate::CasError::CorruptObject)
/// for invalid size metadata.
pub(super) fn build_reconstruction_plan(
    objects: &DashMap<Hash, StoredObject>,
    hash: Hash,
) -> Result<InMemoryReconstructionPlan, CasError> {
    let mut current = hash;
    let mut visited = HashSet::new();
    let mut delta_chain: SmallVec<[Hash; IN_MEMORY_SMALL_DEPENDENT_INLINE]> = SmallVec::new();
    let mut final_len: Option<usize> = None;

    loop {
        if !visited.insert(current) {
            return Err(CasError::CycleDetected {
                target: hash,
                detail: format!("loop encountered at {current}"),
            });
        }

        let object = objects.get(&current).ok_or(CasError::NotFound(current))?;
        let object = object.value();

        if final_len.is_none() {
            final_len = usize::try_from(object.content_len()).ok();
        }

        match object {
            StoredObject::Full { .. } => {
                let final_len = final_len.ok_or_else(|| {
                    CasError::corrupt_object(format!(
                        "invalid final content length for in-memory reconstruction target {hash}"
                    ))
                })?;
                return Ok(InMemoryReconstructionPlan {
                    final_len,
                    base_hash: current,
                    delta_chain,
                });
            }
            StoredObject::Delta { state } => {
                delta_chain.push(current);
                current = state.base_hash;
            }
        }
    }
}

/// Validates that reconstructed bytes hash back to `expected_hash`.
///
/// `context` is included in error text so call sites can report which
/// reconstruction path produced the mismatch.
///
/// # Errors
/// Returns [`CasError::CorruptObject`](crate::CasError::CorruptObject) when the
/// computed hash differs from `expected_hash`.
pub(super) fn ensure_reconstructed_hash(
    expected_hash: Hash,
    content: &[u8],
    context: &str,
) -> Result<(), CasError> {
    let actual = Hash::from_content(content);
    if actual != expected_hash {
        return Err(CasError::corrupt_object(format!(
            "hash mismatch while {context}: expected {expected_hash}, got {actual}"
        )));
    }

    Ok(())
}
