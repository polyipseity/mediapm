//! Delta chain traversal utilities for CAS storage backends.

use std::collections::HashSet;

use crate::{CasError, Hash};

/// Walks the delta base chain from `start`, collecting intermediate hashes.
///
/// Stops when `get_base` returns `None` (reached a terminal full-object base).
/// Returns the chain in order from target toward the base.
///
/// # Errors
/// Returns [`CasError::CycleDetected`] when a hash is visited twice.
#[must_use = "chain verification without using the result is a no-op"]
#[allow(dead_code)]
pub(crate) fn collect_base_chain(
    start: Hash,
    mut get_base: impl FnMut(Hash) -> Result<Option<Hash>, CasError>,
) -> Result<Vec<Hash>, CasError> {
    let mut visited = HashSet::new();
    let mut chain = Vec::new();
    let mut current = start;

    loop {
        if !visited.insert(current) {
            return Err(CasError::CycleDetected {
                target: start,
                detail: format!("loop encountered at {current}"),
            });
        }
        match get_base(current)? {
            Some(base) => {
                chain.push(current);
                current = base;
            }
            None => {
                // current is a terminal full-object base, not added to chain
                return Ok(chain);
            }
        }
    }
}

/// Validates that walking `start` via `get_base` does not form a cycle.
///
/// Unlike [`collect_base_chain`], this only checks for cycles without
/// allocating the chain vector.
///
/// # Errors
/// Returns [`CasError::CycleDetected`] when a hash is visited twice.
pub(crate) fn check_no_cycle(
    start: Hash,
    mut get_next: impl FnMut(Hash) -> Result<Option<Hash>, CasError>,
) -> Result<(), CasError> {
    let mut visited = HashSet::new();
    let mut current = start;
    loop {
        if !visited.insert(current) {
            return Err(CasError::CycleDetected {
                target: start,
                detail: format!("loop encountered at {current}"),
            });
        }
        match get_next(current)? {
            Some(next) => current = next,
            None => return Ok(()),
        }
    }
}
