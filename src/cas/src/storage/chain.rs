//! Delta chain traversal utilities for CAS storage backends.

use std::collections::HashSet;

use crate::{CasError, Hash};

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
