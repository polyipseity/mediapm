//! This crate's environment-variable table.
//!
//! The five clap `env = "` readers this definition declares (`Cli`'s
//! directory overrides) are catalogued here with the shared
//! [`EnvVarEntry`] type, so the conductor root's composed registry can
//! render them beside `mediapm-conductor`'s own table. Four names are shared
//! with that table and carry byte-identical `default`, `doc` and `group`
//! there, which is what makes the composer's first-wins dedup render either
//! copy identically.

use mediapm_utils::env_registry::{EnvVarEntry, EnvVarGroup};

/// Returns this crate's env-var table: the clap `env = "` readers declared
/// on [`Cli`](crate::Cli) and its argument structs.
#[must_use]
pub fn env_registry() -> &'static [EnvVarEntry] {
    &[
        EnvVarEntry {
            name: "MEDIAPM_CONDUCTOR_DIR",
            default: "",
            doc: "Conductor runtime directory root; defaults to <project root>/.conductor.",
            group: EnvVarGroup::CliPaths,
        },
        EnvVarEntry {
            name: "MEDIAPM_CAS_STORE_DIR",
            default: "",
            doc: "CAS store directory root override.",
            group: EnvVarGroup::CliPaths,
        },
        EnvVarEntry {
            name: "MEDIAPM_CONDUCTOR_SCHEMA_DIR",
            default: "",
            doc: "Conductor contract-source export directory.",
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
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The clap `env = "` readers declared in this crate's command
    /// definitions must each have a table entry, so `.env.example` at the
    /// conductor root documents what this CLI actually reads.
    #[test]
    fn the_conductor_cli_table_catalogues_its_clap_env_readers() {
        let names: Vec<_> = env_registry().iter().map(|entry| entry.name).collect();
        for expected in [
            "MEDIAPM_CONDUCTOR_DIR",
            "MEDIAPM_CAS_STORE_DIR",
            "MEDIAPM_CONDUCTOR_SCHEMA_DIR",
            "MEDIAPM_CONDUCTOR_TMP_DIR",
            "MEDIAPM_CONDUCTOR_TOOLS_DIR",
        ] {
            assert!(
                names.contains(&expected),
                "conductor-cli's table is missing {expected}\ntable: {names:?}"
            );
        }
    }
}
