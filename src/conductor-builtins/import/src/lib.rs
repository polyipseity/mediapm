//! Built-in one-shot `import` tool specification.

use mediapm_cas::Hash;
use serde::{Deserialize, Serialize};

/// Semantic tool id pinned by date-version.
pub const TOOL_ID: &str = "import@2026.03.25";

/// Input payload for import builtin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportRequest {
    /// Raw content reference to import into a workflow step.
    pub hash: Hash,
}

/// Output payload for import builtin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportResult {
    /// Imported content hash, equal to request hash by design.
    pub hash: Hash,
}

/// Import is impure because it performs one-shot materialization side effects.
pub fn is_impure() -> bool {
    true
}
