//! Temp-directory janitor gate.

use std::path::Path;

use anyhow::Result;

/// Runs the janitor in dry-run mode and fails when the suite left a
/// mediapm-owned temp directory behind.
///
/// Placeholder body, replaced in Task 3.
#[expect(
    clippy::unnecessary_wraps,
    reason = "the gate is failible by contract: the call site in test::run propagates it, and Task 3 replaces this placeholder with a body that reports a sweep failure through Result; removing the Result now would force the caller to change shape again in Task 3"
)]
pub fn enforce(_root: &Path) -> Result<()> {
    Ok(())
}
