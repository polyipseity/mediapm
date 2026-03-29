//! Depth recomputation and graph invariants for runtime index state.
//!
//! ## DO NOT REMOVE: external versions boundary guard
//!
//! This file is outside `index/versions/` and must remain version-agnostic.
//! It must use unversioned symbols exposed by `index/mod.rs` and never import
//! `index::versions::*` directly.

use std::collections::HashMap;

use super::MAX_DELTA_DEPTH;
use super::state::{IndexState, ObjectEncoding, ensure_empty_record};
use crate::{CasError, Hash, empty_content_hash};

#[derive(Debug)]
struct CsrChildren {
    base_slot_by_hash: HashMap<Hash, usize>,
    offsets: Vec<usize>,
    children: Vec<Hash>,
}

impl CsrChildren {
    fn from_edges(edges: &[(Hash, Hash)]) -> Self {
        if edges.is_empty() {
            return Self { base_slot_by_hash: HashMap::new(), offsets: vec![0], children: vec![] };
        }

        let mut counts_by_base: HashMap<Hash, usize> = HashMap::with_capacity(edges.len());
        for (base, _) in edges {
            *counts_by_base.entry(*base).or_insert(0) += 1;
        }

        let mut bases: Vec<Hash> = counts_by_base.keys().copied().collect();
        bases.sort_unstable();

        let mut base_slot_by_hash = HashMap::with_capacity(bases.len());
        for (slot, base) in bases.iter().copied().enumerate() {
            base_slot_by_hash.insert(base, slot);
        }

        let mut offsets = vec![0usize; bases.len() + 1];
        for (slot, base) in bases.iter().copied().enumerate() {
            let count =
                *counts_by_base.get(&base).expect("base must have edge count in CSR construction");
            offsets[slot + 1] = offsets[slot] + count;
        }

        let mut cursors = offsets[..bases.len()].to_vec();
        let mut children = vec![empty_content_hash(); edges.len()];
        for (base, child) in edges {
            let slot =
                *base_slot_by_hash.get(base).expect("base must map to slot in CSR construction");
            let cursor = &mut cursors[slot];
            children[*cursor] = *child;
            *cursor += 1;
        }

        Self { base_slot_by_hash, offsets, children }
    }

    fn children_for(&self, base: &Hash) -> &[Hash] {
        let Some(slot) = self.base_slot_by_hash.get(base).copied() else {
            return &[];
        };

        &self.children[self.offsets[slot]..self.offsets[slot + 1]]
    }
}

/// Resolves the expected reconstruction depth for one object encoding using
/// the current index state.
///
/// This is $O(1)$ for both full and delta objects and is intended for hot-path
/// upserts where full-graph recomputation would be excessive.
pub(crate) fn resolve_object_depth(
    state: &IndexState,
    target: Hash,
    encoding: ObjectEncoding,
) -> Result<u32, CasError> {
    match encoding {
        ObjectEncoding::Full => Ok(u32::from(target != empty_content_hash())),
        ObjectEncoding::Delta { base_hash } => {
            let base_depth = state
                .objects
                .get(&base_hash)
                .ok_or_else(|| {
                    CasError::corrupt_index(format!(
                        "delta base missing object record while resolving depth: target={target}, base={base_hash}"
                    ))
                })?
                .depth();

            let depth = base_depth.checked_add(1).ok_or_else(|| {
                CasError::corrupt_index(format!(
                    "delta depth overflow while resolving target={target} from base={base_hash}"
                ))
            })?;

            if depth > MAX_DELTA_DEPTH {
                return Err(CasError::corrupt_index(format!(
                    "delta depth exceeds configured limit while resolving target={target}: depth={depth}, max={MAX_DELTA_DEPTH}"
                )));
            }

            Ok(depth)
        }
    }
}

/// Recomputes depth values and validates DAG/cycle invariants.
///
/// Uses a level-by-level frontier traversal to avoid recursive stack growth.
pub(crate) fn recalculate_depths(state: &mut IndexState) -> Result<(), CasError> {
    ensure_empty_record(state);

    let mut delta_edges: Vec<(Hash, Hash)> = Vec::with_capacity(state.objects.len());
    let mut depths: HashMap<Hash, u32> = HashMap::with_capacity(state.objects.len());
    let mut frontier: Vec<Hash> = Vec::with_capacity(state.objects.len());

    for (hash, meta) in &state.objects {
        if *hash == empty_content_hash() {
            depths.insert(*hash, 0);
            frontier.push(*hash);
            continue;
        }

        match meta.encoding() {
            ObjectEncoding::Full => {
                depths.insert(*hash, 1);
                frontier.push(*hash);
            }
            ObjectEncoding::Delta { base_hash } => {
                if !state.objects.contains_key(&base_hash) {
                    return Err(CasError::corrupt_index(format!(
                        "delta base missing object record: target={hash}, base={base_hash}"
                    )));
                }
                delta_edges.push((base_hash, *hash));
            }
        }
    }

    let children_by_base = CsrChildren::from_edges(&delta_edges);

    while !frontier.is_empty() {
        let mut next = Vec::new();

        for base in frontier {
            let Some(base_depth) = depths.get(&base).copied() else {
                return Err(CasError::corrupt_index(format!(
                    "frontier node missing depth assignment: {base}"
                )));
            };

            for child in children_by_base.children_for(&base) {
                let depth = base_depth.checked_add(1).ok_or_else(|| {
                    CasError::corrupt_index(format!(
                        "delta depth overflow while resolving child={child} from base={base}"
                    ))
                })?;
                if depth > MAX_DELTA_DEPTH {
                    return Err(CasError::corrupt_index(format!(
                        "delta depth exceeds configured limit while resolving child={child}: depth={depth}, max={MAX_DELTA_DEPTH}"
                    )));
                }

                if depths.insert(*child, depth).is_some() {
                    return Err(CasError::CycleDetected {
                        target: *child,
                        detail: format!(
                            "child depth assigned multiple times during frontier traversal (possible cycle): child={child}, base={base}"
                        ),
                    });
                }

                next.push(*child);
            }
        }

        frontier = next;
    }

    if depths.len() != state.objects.len() {
        let unresolved = state.objects.len().saturating_sub(depths.len());
        return Err(CasError::CycleDetected {
            target: empty_content_hash(),
            detail: format!(
                "unresolved object depths after frontier traversal; unresolved node count={unresolved}"
            ),
        });
    }

    for (hash, meta) in &mut state.objects {
        let Some(depth) = depths.get(hash).copied() else {
            return Err(CasError::corrupt_index(format!(
                "missing computed depth for object {hash}"
            )));
        };
        meta.set_depth(depth);
    }

    for (target, bases) in &state.constraints {
        if !state.objects.contains_key(target) {
            return Err(CasError::corrupt_index(format!(
                "constraint target missing object record: {target}"
            )));
        }
        for base in bases {
            if *base != empty_content_hash() && !state.objects.contains_key(base) {
                return Err(CasError::corrupt_index(format!(
                    "constraint base missing object record: target={target}, base={base}"
                )));
            }
        }
    }

    state.rebuild_delta_reverse();

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{recalculate_depths, resolve_object_depth};
    use crate::index::{IndexState, ObjectEncoding, ObjectMeta, ensure_empty_record};
    use crate::{CasError, Hash, empty_content_hash};

    #[test]
    fn recalculate_depths_assigns_expected_chain_depths() {
        let base = Hash::from_content(b"graph-base");
        let child = Hash::from_content(b"graph-child");
        let grandchild = Hash::from_content(b"graph-grandchild");

        let mut state = IndexState::default();
        state.objects.insert(base, ObjectMeta::full(4, 4, 0));
        state.objects.insert(child, ObjectMeta::delta(2, 4, 0, base));
        state.objects.insert(grandchild, ObjectMeta::delta(2, 4, 0, child));
        state.constraints.insert(grandchild, BTreeSet::from([base]));

        recalculate_depths(&mut state).expect("depth recomputation must succeed");

        let empty = empty_content_hash();
        assert_eq!(state.objects.get(&empty).expect("empty record").depth(), 0);
        assert_eq!(state.objects.get(&base).expect("base record").depth(), 1);
        assert_eq!(state.objects.get(&child).expect("child record").depth(), 2);
        assert_eq!(state.objects.get(&grandchild).expect("grandchild record").depth(), 3);
    }

    #[test]
    fn recalculate_depths_rejects_cycles() {
        let a = Hash::from_content(b"cycle-a");
        let b = Hash::from_content(b"cycle-b");

        let mut state = IndexState::default();
        ensure_empty_record(&mut state);
        state.objects.insert(a, ObjectMeta::delta(1, 1, 0, b));
        state.objects.insert(b, ObjectMeta::delta(1, 1, 0, a));

        let error = recalculate_depths(&mut state).expect_err("cycle must be rejected");
        assert!(matches!(error, CasError::CycleDetected { .. }));
    }

    #[test]
    fn resolve_object_depth_respects_encoding_and_base_depth() {
        let base = Hash::from_content(b"resolve-base");
        let target = Hash::from_content(b"resolve-target");

        let mut state = IndexState::default();
        ensure_empty_record(&mut state);
        state.objects.insert(base, ObjectMeta::full(4, 4, 1));

        let full_depth =
            resolve_object_depth(&state, target, ObjectEncoding::Full).expect("full depth");
        assert_eq!(full_depth, 1);

        let delta_depth =
            resolve_object_depth(&state, target, ObjectEncoding::Delta { base_hash: base })
                .expect("delta depth");
        assert_eq!(delta_depth, 2);

        let missing_base = Hash::from_content(b"resolve-missing-base");
        let error =
            resolve_object_depth(&state, target, ObjectEncoding::Delta { base_hash: missing_base })
                .expect_err("missing base must fail");
        assert!(matches!(error, CasError::CorruptIndex(_)));
    }
}
