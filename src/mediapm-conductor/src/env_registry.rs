//! The conductor's own environment-variable table and the registry re-exports.
//!
//! [`EnvVarEntry`], [`EnvVarGroup`] and [`compose_env_registry`] are defined
//! in `mediapm_utils::env_registry` and re-exported here, so every consumer
//! reaches the registry through `mediapm_conductor::env_registry` while the
//! type sits in the one crate all five table-owning crates may depend on
//! (the CLI definition crates and `mediapm-test-support` must not depend back
//! on this crate). The table below is this crate's own slice of that
//! registry.

pub use mediapm_utils::env_registry::{EnvVarEntry, EnvVarGroup, compose_env_registry};

/// Returns the conductor crate's own env-var table.
///
/// This is the slice every root renders: it covers the timeouts, RPC and
/// pool knobs this crate reads (`orchestration/config.rs`,
/// `orchestration/step_worker/mod.rs`, `http/client.rs`), the conductor path
/// overrides clap reads on either side of the crate boundary, the four
/// debug-build upstream seams, the upstream-tool variables the old `.env`
/// template documented, the test-only probes, and one pointer entry naming
/// the `.env.generated` surface. A name another crate's table also
/// catalogues carries byte-identical `default`, `doc` and `group` here, so
/// the composer's first-wins dedup renders whichever copy it keeps.
#[must_use]
pub fn env_registry() -> &'static [EnvVarEntry] {
    CONDUCTOR_ENV_ENTRIES
}

/// The flat catalogue [`env_registry`] returns: one entry per variable, in
/// `EnvVarGroup` declaration order.
const CONDUCTOR_ENV_ENTRIES: &[EnvVarEntry] = &[
    EnvVarEntry {
        name: "MEDIAPM_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS",
        default: "900",
        doc: "Step executable timeout in seconds; 900 (15 minutes) when unset or not a positive number.",
        group: EnvVarGroup::Timeouts,
    },
    EnvVarEntry {
        name: "MEDIAPM_CONDUCTOR_RPC_TIMEOUT_SECONDS",
        default: "300",
        doc: "Conductor RPC timeout in seconds; 300 when unset, zero or unparseable.",
        group: EnvVarGroup::Timeouts,
    },
    EnvVarEntry {
        name: "MEDIAPM_HTTP_TIMEOUT_SECONDS",
        default: "1800",
        doc: "Shared HTTP request timeout in seconds; 1800 (30 minutes) when unset, values below 30 ignored.",
        group: EnvVarGroup::Timeouts,
    },
    EnvVarEntry {
        name: "MEDIAPM_CONDUCTOR_WORKER_POOL_SIZE",
        default: "",
        doc: "Step-worker pool size; the host's available parallelism (minimum 1) when unset or zero.",
        group: EnvVarGroup::Concurrency,
    },
    EnvVarEntry {
        name: "MEDIAPM_SOURCE_FETCH_CONCURRENCY",
        default: "",
        doc: "Concurrent downloads inside one tool's fetch phase; the host's available parallelism (minimum 1) when unset or zero.",
        group: EnvVarGroup::Concurrency,
    },
    EnvVarEntry {
        name: "MEDIAPM_CONDUCTOR_DIR",
        default: "",
        doc: "Conductor runtime directory root; defaults to <project root>/.conductor.",
        group: EnvVarGroup::CliPaths,
    },
    EnvVarEntry {
        name: "MEDIAPM_CONDUCTOR_TMP_DIR",
        default: "",
        doc: "Conductor sandbox temporary directory root.",
        group: EnvVarGroup::CliPaths,
    },
    EnvVarEntry {
        name: "MEDIAPM_CONDUCTOR_TOOLS_DIR",
        default: "",
        doc: "Provisioned tool payload directory.",
        group: EnvVarGroup::CliPaths,
    },
    EnvVarEntry {
        name: "MEDIAPM_CONDUCTOR_SCHEMA_DIR",
        default: "",
        doc: "Conductor contract-source export directory.",
        group: EnvVarGroup::CliPaths,
    },
    EnvVarEntry {
        name: "MEDIAPM_CONDUCTOR_CONFIG",
        default: "",
        doc: "User-authored conductor document path (mediapm runtime.conductor_config).",
        group: EnvVarGroup::CliPaths,
    },
    EnvVarEntry {
        name: "MEDIAPM_CONDUCTOR_GENERATED_CONFIG",
        default: "",
        doc: "Machine-managed conductor document path (mediapm runtime.conductor_generated_config).",
        group: EnvVarGroup::CliPaths,
    },
    EnvVarEntry {
        name: "MEDIAPM_CONDUCTOR_STATE_CONFIG",
        default: "",
        doc: "Conductor volatile state file path (mediapm runtime.conductor_state_config).",
        group: EnvVarGroup::CliPaths,
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
        name: "MEDIAPM_YT_DLP_FFMPEG_LOCATION",
        default: "",
        doc: "Consumed by yt-dlp: directory holding the ffmpeg binary yt-dlp should use; mediapm reads it nowhere.",
        group: EnvVarGroup::Upstream,
    },
    EnvVarEntry {
        name: "MEDIAPM_YT_DLP_JS_RUNTIMES",
        default: "",
        doc: "Consumed by yt-dlp: JavaScript runtime selection; mediapm reads it nowhere.",
        group: EnvVarGroup::Upstream,
    },
    EnvVarEntry {
        name: "HTTP_PROXY",
        default: "",
        doc: "Consumed by upstream tools; mediapm reads it nowhere.",
        group: EnvVarGroup::Upstream,
    },
    EnvVarEntry {
        name: "HTTPS_PROXY",
        default: "",
        doc: "Consumed by upstream tools; mediapm reads it nowhere.",
        group: EnvVarGroup::Upstream,
    },
    EnvVarEntry {
        name: "NO_PROXY",
        default: "",
        doc: "Consumed by upstream tools: comma-separated host exclusions; mediapm reads it nowhere.",
        group: EnvVarGroup::Upstream,
    },
    EnvVarEntry {
        name: "MEDIAPM_CONDUCTOR_SCHEDULER_EWMA_ALPHA",
        default: "0.35",
        doc: "test-only: scheduler EWMA alpha; 0.35 when unset or out of range; no production reader.",
        group: EnvVarGroup::TestOnly,
    },
    EnvVarEntry {
        name: "MEDIAPM_CONDUCTOR_TEST_ENV_VARS_HOST_VALUE",
        default: "",
        doc: "test-only: host-value probe for document env_vars precedence tests.",
        group: EnvVarGroup::TestOnly,
    },
    EnvVarEntry {
        name: "MEDIAPM_CONDUCTOR_TEST_ENV_VARS_UNSET_HOST_VALUE",
        default: "",
        doc: "test-only: unset-host probe for document env_vars precedence tests.",
        group: EnvVarGroup::TestOnly,
    },
    EnvVarEntry {
        name: "MEDIAPM_CONDUCTOR_TEST_UNSET_VAR",
        default: "",
        doc: "test-only: unset-variable probe for the env parse helper's unit test.",
        group: EnvVarGroup::TestOnly,
    },
    EnvVarEntry {
        name: "MEDIAPM_T2_BOOT_PROBE",
        default: "",
        doc: "test-only: CLI boot probe; the seeded .env value must be visible in-process after ensure_conductor.",
        group: EnvVarGroup::TestOnly,
    },
    EnvVarEntry {
        name: "MEDIAPM_TEST_TOOL_CONFIG_LITERAL",
        default: "",
        doc: "test-only: fixture name inside a tool-config test document.",
        group: EnvVarGroup::TestOnly,
    },
    EnvVarEntry {
        name: "MEDIAPM_TEST_TOOL_CONFIG_ONLY",
        default: "",
        doc: "test-only: fixture name inside a tool-config test document.",
        group: EnvVarGroup::TestOnly,
    },
    EnvVarEntry {
        name: "MEDIAPM_TEST_INHERITED",
        default: "",
        doc: "test-only: fixture name inside a platform inherited-env test document.",
        group: EnvVarGroup::TestOnly,
    },
    EnvVarEntry {
        name: "MEDIAPM_<TOOL>_<OS>",
        default: "",
        doc: "Pattern written into .env.generated at tool sync: absolute paths of each provisioned tool payload directory and binary.",
        group: EnvVarGroup::ToolGenerated,
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_composer_orders_within_groups_deterministically() {
        let a =
            EnvVarEntry { name: "MEDIAPM_B", default: "1", doc: "b", group: EnvVarGroup::Timeouts };
        let z =
            EnvVarEntry { name: "MEDIAPM_A", default: "2", doc: "a", group: EnvVarGroup::Timeouts };
        let merged = compose_env_registry(&[std::slice::from_ref(&a), std::slice::from_ref(&z)]);
        let names: Vec<_> = merged.iter().map(|e| e.name).collect();
        assert_eq!(names, ["MEDIAPM_A", "MEDIAPM_B"]);
    }

    /// The conductor's own table must catalogue every variable its crate (or
    /// the brief's minimum list) reads, so a new reader without a registry
    /// entry fails here instead of silently missing from `.env.example`.
    #[test]
    fn the_conductor_table_catalogues_the_conductor_surface() {
        let table = env_registry();
        let names: Vec<_> = table.iter().map(|entry| entry.name).collect();
        for expected in [
            "MEDIAPM_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS",
            "MEDIAPM_CONDUCTOR_RPC_TIMEOUT_SECONDS",
            "MEDIAPM_CONDUCTOR_WORKER_POOL_SIZE",
            "MEDIAPM_CONDUCTOR_SCHEDULER_EWMA_ALPHA",
            "MEDIAPM_CONDUCTOR_DIR",
            "MEDIAPM_CONDUCTOR_TMP_DIR",
            "MEDIAPM_CONDUCTOR_TOOLS_DIR",
            "MEDIAPM_CONDUCTOR_SCHEMA_DIR",
            "MEDIAPM_CONDUCTOR_CONFIG",
            "MEDIAPM_CONDUCTOR_GENERATED_CONFIG",
            "MEDIAPM_CONDUCTOR_STATE_CONFIG",
            "MEDIAPM_CONDUCTOR_TEST_ENV_VARS_HOST_VALUE",
            "MEDIAPM_CONDUCTOR_TEST_ENV_VARS_UNSET_HOST_VALUE",
            "MEDIAPM_CONDUCTOR_TEST_UNSET_VAR",
            "MEDIAPM_T2_BOOT_PROBE",
            "MEDIAPM_TEST_TOOL_CONFIG_LITERAL",
            "MEDIAPM_TEST_TOOL_CONFIG_ONLY",
            "MEDIAPM_TEST_INHERITED",
            "MEDIAPM_HTTP_TIMEOUT_SECONDS",
            "MEDIAPM_SOURCE_FETCH_CONCURRENCY",
            "MEDIAPM_GITHUB_API_BASE",
            "MEDIAPM_GITHUB_DOWNLOAD_BASE",
            "MEDIAPM_MARTIN_RIEDL_BASE",
            "MEDIAPM_JELLYFIN_BASE",
            "MEDIAPM_YT_DLP_FFMPEG_LOCATION",
            "MEDIAPM_YT_DLP_JS_RUNTIMES",
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "NO_PROXY",
        ] {
            assert!(
                names.contains(&expected),
                "conductor's table is missing {expected}\ntable: {names:?}"
            );
        }
    }

    /// The four upstream seams are debug-build-only redirects; their doc line
    /// must say so, because a release build ignores them entirely.
    #[test]
    fn the_conductor_seam_entries_declare_debug_builds_only() {
        for seam in [
            "MEDIAPM_GITHUB_API_BASE",
            "MEDIAPM_GITHUB_DOWNLOAD_BASE",
            "MEDIAPM_MARTIN_RIEDL_BASE",
            "MEDIAPM_JELLYFIN_BASE",
        ] {
            let entry = env_registry()
                .iter()
                .find(|entry| entry.name == seam)
                .unwrap_or_else(|| panic!("conductor's table has no entry for {seam}"));
            assert!(
                entry.doc.starts_with("debug builds only"),
                "{seam} must lead with 'debug builds only', got: {}",
                entry.doc
            );
        }
    }
}
