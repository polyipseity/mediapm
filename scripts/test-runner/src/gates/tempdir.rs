//! Unprefixed-tempdir invariant gate.

use std::path::Path;

use anyhow::Result;

/// Rejects bare `tempfile::tempdir()` and `.prefix(` use outside the role
/// helpers.
///
/// Placeholder body, replaced in Task 3.
#[expect(
    clippy::unnecessary_wraps,
    reason = "the gate is failible by contract: the call site in test::run propagates it, and Task 3 replaces this placeholder with a body that rejects an unprefixed tempdir through Result; removing the Result now would force the caller to change shape again in Task 3"
)]
pub fn enforce(_root: &Path) -> Result<()> {
    Ok(())
}
