//! Shared environment-variable registry types and the composer.
//!
//! Every runtime-read environment variable is catalogued as an
//! [`EnvVarEntry`] with its code default, a user-facing doc line and an
//! [`EnvVarGroup`] section. [`compose_env_registry`] concatenates a caller's
//! slices, keeps the first registration of each name, and stable-sorts by
//! `(group, name)` so every render of the same registry is byte-identical.
//!
//! These types live here, one layer below the conductor, because all five
//! crates that own a table (`mediapm-conductor`, `mediapm-conductor-cli`,
//! `mediapm`, `mediapm-cli`, `mediapm-test-support`) can depend on this crate
//! while none of the three CLI/test crates may depend on `mediapm-conductor`
//! (a build-script cycle for the CLIs, a dev-dependency cycle for
//! `mediapm-test-support`). `mediapm_conductor::env_registry` re-exports every
//! name here, so consumers reach the registry through the conductor path.
//!
//! The composer is deliberately pure: it prepends nothing. Callers put the
//! conductor's own table first and each downstream crate's slice behind it
//! (design B1's ordering), which is also what keeps a name the conductor and
//! a CLI crate both register rendering once.

use std::fmt;

/// One catalogued environment variable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvVarEntry {
    /// Environment variable name as it is read at runtime.
    pub name: &'static str,
    /// Code default rendered after `=`; empty means "unset by default".
    pub default: &'static str,
    /// User-facing description rendered under the entry.
    pub doc: &'static str,
    /// Section the render groups this entry under.
    pub group: EnvVarGroup,
}

/// Section a rendered env file groups entries under.
///
/// The variant order is the render order: derived `Ord` sorts by declaration
/// order, so `Core` heads the file and `ToolGenerated` closes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum EnvVarGroup {
    /// General runtime knobs.
    Core,
    /// Path overrides that clap reads from the environment.
    CliPaths,
    /// Timeout knobs.
    Timeouts,
    /// Concurrency knobs.
    Concurrency,
    /// Debug-build-only test seams.
    DebugSeams,
    /// Variables consumed by upstream tools or external services.
    Upstream,
    /// Variables only tests and example harnesses read.
    TestOnly,
    /// Pointer documenting the names tool sync writes into `.env.generated`.
    ToolGenerated,
}

impl EnvVarGroup {
    /// Section header label rendered as `# ── <label> ──`.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Core => "core knobs",
            Self::CliPaths => "CLI paths",
            Self::Timeouts => "timeouts",
            Self::Concurrency => "concurrency",
            Self::DebugSeams => "debug seams",
            Self::Upstream => "upstream/proxy",
            Self::TestOnly => "test-only",
            Self::ToolGenerated => "tool-generated",
        }
    }
}

impl fmt::Display for EnvVarGroup {
    /// Renders the group's section label.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// Concatenates the given slices into one deterministic registry.
///
/// Slices are taken in argument order and entries within a slice keep their
/// declaration order, so the first registration of a name wins when two
/// tables catalogue the same variable (the conductor's slices are passed
/// first by convention). The result is stable-sorted by `(group, name)`; the
/// sort is stable, and deduplication makes the name key unique, so equal keys
/// cannot reorder entries.
///
/// # Panics
///
/// Never panics: the function only reads, deduplicates and sorts.
#[must_use]
pub fn compose_env_registry(extra: &[&[EnvVarEntry]]) -> Vec<EnvVarEntry> {
    let mut merged: Vec<EnvVarEntry> = Vec::new();
    let mut seen: Vec<&str> = Vec::new();
    for slice in extra {
        for entry in *slice {
            if seen.contains(&entry.name) {
                continue;
            }
            seen.push(entry.name);
            merged.push(entry.clone());
        }
    }
    merged.sort_by(|a, b| (a.group, a.name).cmp(&(b.group, b.name)));
    merged
}
