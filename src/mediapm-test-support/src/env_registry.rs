//! This crate's environment-variable table.
//!
//! Every variable this crate reads or installs is catalogued here with the
//! shared [`EnvVarEntry`] type: the stub tool's manifest and invocation
//! record, the four upstream seams the [`MockUpstream`](crate::MockUpstream)
//! server stands in for, and the three seam-lock probe variables its tests
//! install. The seam names are shared with `mediapm-conductor`'s and `mediapm`'s tables and
//! carry byte-identical `default`, `doc` and `group` there, which is what
//! makes the composer's first-wins dedup render either copy identically.

use mediapm_utils::env_registry::{EnvVarEntry, EnvVarGroup};

/// Returns this crate's env-var table: the stub tool's environment surface,
/// the four provider seams and the seam-lock probe variables.
#[must_use]
pub fn env_registry() -> &'static [EnvVarEntry] {
    &[
        EnvVarEntry {
            name: "MEDIAPM_STUB_MANIFEST",
            default: "",
            doc: "test-only: JSON manifest describing the stub tool's payload.",
            group: EnvVarGroup::TestOnly,
        },
        EnvVarEntry {
            name: "MEDIAPM_STUB_INVOCATIONS",
            default: "",
            doc: "test-only: file recording each stub tool invocation.",
            group: EnvVarGroup::TestOnly,
        },
        EnvVarEntry {
            name: "MEDIAPM_GITHUB_API_BASE",
            default: "",
            doc: "debug builds only: GitHub REST API base URL override.",
            group: EnvVarGroup::DebugSeams,
        },
        EnvVarEntry {
            name: "MEDIAPM_GITHUB_DOWNLOAD_BASE",
            default: "",
            doc: "debug builds only: GitHub release download base URL override.",
            group: EnvVarGroup::DebugSeams,
        },
        EnvVarEntry {
            name: "MEDIAPM_MARTIN_RIEDL_BASE",
            default: "",
            doc: "debug builds only: martin-riedl ffmpeg mirror base URL override.",
            group: EnvVarGroup::DebugSeams,
        },
        EnvVarEntry {
            name: "MEDIAPM_JELLYFIN_BASE",
            default: "",
            doc: "debug builds only: jellyfin repository base URL override.",
            group: EnvVarGroup::DebugSeams,
        },
        EnvVarEntry {
            name: "MEDIAPM_TEST_SUPPORT_PROBE_VALUE",
            default: "",
            doc: "test-only: seam-lock probe value.",
            group: EnvVarGroup::TestOnly,
        },
        EnvVarEntry {
            name: "MEDIAPM_TEST_SUPPORT_PROBE_PANIC",
            default: "",
            doc: "test-only: seam-lock probe panic trigger.",
            group: EnvVarGroup::TestOnly,
        },
        EnvVarEntry {
            name: "MEDIAPM_TEST_SUPPORT_PROBE_BORROWED",
            default: "",
            doc: "test-only: seam-lock borrowed-guard probe.",
            group: EnvVarGroup::TestOnly,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every variable this crate reads or installs (stub tool manifest and
    /// invocation record, the four upstream seams, the three seam-lock probe
    /// variables) must have a table entry, so a consumer's composed registry
    /// can catalogue the test-only surface too.
    #[test]
    fn the_test_support_table_catalogues_its_variables() {
        let names: Vec<_> = env_registry().iter().map(|entry| entry.name).collect();
        for expected in [
            "MEDIAPM_STUB_MANIFEST",
            "MEDIAPM_STUB_INVOCATIONS",
            "MEDIAPM_GITHUB_API_BASE",
            "MEDIAPM_GITHUB_DOWNLOAD_BASE",
            "MEDIAPM_MARTIN_RIEDL_BASE",
            "MEDIAPM_JELLYFIN_BASE",
            "MEDIAPM_TEST_SUPPORT_PROBE_VALUE",
            "MEDIAPM_TEST_SUPPORT_PROBE_PANIC",
            "MEDIAPM_TEST_SUPPORT_PROBE_BORROWED",
        ] {
            assert!(
                names.contains(&expected),
                "test-support's table is missing {expected}\ntable: {names:?}"
            );
        }
    }
}
