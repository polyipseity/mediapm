//! Built-in archive tooling specification.

use serde::{Deserialize, Serialize};

/// Semantic tool id pinned by date-version.
pub const TOOL_ID: &str = "zip@2026.03.25";

/// Archive format variants supported by the builtin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArchiveFormat {
    /// `.zip` format.
    Zip,
    /// `.7z` format.
    SevenZip,
    /// `.tar` based formats.
    Tar,
}
