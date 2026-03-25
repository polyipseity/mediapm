//! Built-in `fs-ops` tool specification.

use serde::{Deserialize, Serialize};

/// Semantic tool id pinned by date-version.
pub const TOOL_ID: &str = "fs-ops@2026.03.25";

/// Supported filesystem side-effect operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FsOperation {
    /// Create or truncate a file at the destination path.
    CreateFile { destination: String },
    /// Copy a file from source to destination path.
    Copy { source: String, destination: String },
    /// Delete a file or folder path.
    Delete { path: String },
}

/// Returns whether this builtin is impure and should be timestamped.
pub fn is_impure() -> bool {
    true
}
