//! This crate's environment-variable table.
//!
//! The twelve clap `env = "` readers this definition declares are
//! catalogued here with the shared [`EnvVarEntry`] type, so the mediapm
//! root's composed registry renders them beside `mediapm-conductor`'s and
//! `mediapm`'s tables. The three `MEDIAPM_CONDUCTOR_*_CONFIG` names are
//! shared with `mediapm-conductor`'s table and carry byte-identical
//! `default`, `doc` and `group` there, which is what makes the composer's
//! first-wins dedup render either copy identically.

use mediapm_utils::env_registry::{EnvVarEntry, EnvVarGroup};

/// Returns this crate's env-var table: the clap `env = "` readers declared
/// on [`Cli`](crate::Cli) and its argument structs.
#[must_use]
pub fn env_registry() -> &'static [EnvVarEntry] {
    &[
        EnvVarEntry {
            name: "MEDIAPM_ROOT",
            default: ".",
            doc: "Workspace root hosting mediapm.ncl and .mediapm/.",
            group: EnvVarGroup::CliPaths,
        },
        EnvVarEntry {
            name: "MEDIAPM_DIR",
            default: "",
            doc: "Runtime state directory root (mediapm runtime.mediapm_dir).",
            group: EnvVarGroup::CliPaths,
        },
        EnvVarEntry {
            name: "MEDIAPM_HIERARCHY_ROOT_DIR",
            default: "",
            doc: "Materialized media library root (mediapm runtime.hierarchy_root_dir).",
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
            name: "MEDIAPM_ENV_FILE",
            default: "",
            doc: "User-authored dotenv path (mediapm runtime.env_file).",
            group: EnvVarGroup::CliPaths,
        },
        EnvVarEntry {
            name: "MEDIAPM_ENV_GENERATED_FILE",
            default: "",
            doc: "Generated dotenv path (mediapm runtime.env_generated_file).",
            group: EnvVarGroup::CliPaths,
        },
        EnvVarEntry {
            name: "MEDIAPM_MEDIAPM_SCHEMA_DIR",
            default: "",
            doc: "MediaPM contract-source export directory (mediapm runtime.mediapm_schema_dir).",
            group: EnvVarGroup::CliPaths,
        },
        EnvVarEntry {
            name: "MEDIAPM_MEDIA_STATE_CONFIG",
            default: "",
            doc: "MediaPM state document path (mediapm runtime.media_state_config).",
            group: EnvVarGroup::CliPaths,
        },
        EnvVarEntry {
            name: "MEDIAPM_RETRY_IMPURE",
            default: "",
            doc: "Enables CorruptObject retry for impure steps (default: retry disabled).",
            group: EnvVarGroup::Core,
        },
        EnvVarEntry {
            name: "MEDIAPM_NO_PROGRESS",
            default: "",
            doc: "Suppresses progress output when set.",
            group: EnvVarGroup::Core,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The clap `env = "` readers declared in this crate's command
    /// definitions must each have a table entry, so the mediapm root's
    /// `.env.example` documents what this CLI actually reads.
    #[test]
    fn the_mediapm_cli_table_catalogues_its_clap_env_readers() {
        let names: Vec<_> = env_registry().iter().map(|entry| entry.name).collect();
        for expected in [
            "MEDIAPM_ROOT",
            "MEDIAPM_DIR",
            "MEDIAPM_HIERARCHY_ROOT_DIR",
            "MEDIAPM_CONDUCTOR_CONFIG",
            "MEDIAPM_CONDUCTOR_GENERATED_CONFIG",
            "MEDIAPM_CONDUCTOR_STATE_CONFIG",
            "MEDIAPM_ENV_FILE",
            "MEDIAPM_ENV_GENERATED_FILE",
            "MEDIAPM_MEDIAPM_SCHEMA_DIR",
            "MEDIAPM_MEDIA_STATE_CONFIG",
            "MEDIAPM_RETRY_IMPURE",
            "MEDIAPM_NO_PROGRESS",
        ] {
            assert!(
                names.contains(&expected),
                "mediapm-cli's table is missing {expected}\ntable: {names:?}"
            );
        }
    }
}
