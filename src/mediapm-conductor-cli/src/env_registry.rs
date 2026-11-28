//! This crate's environment-variable table.
//!
//! The ten clap `env = "` readers this definition declares (`Cli`'s
//! directory overrides and instance-key switches) are catalogued here with the shared
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
            name: "MEDIAPM_CONDUCTOR_USE_INSTANCE_KEY_SECRET",
            default: "true",
            doc: "Use a conductor-owned secret for instance-key derivation.",
            group: EnvVarGroup::Core,
        },
        EnvVarEntry {
            name: "MEDIAPM_CONDUCTOR_NO_USE_INSTANCE_KEY_SECRET",
            default: "false",
            doc: "Disable conductor-owned secret use for instance-key derivation.",
            group: EnvVarGroup::Core,
        },
        EnvVarEntry {
            name: "MEDIAPM_CONDUCTOR_REQUIRE_GENERATED_INSTANCE_KEY_SECRET_FORMAT",
            default: "true",
            doc: "Require the generated secret format and entropy sanity check.",
            group: EnvVarGroup::Core,
        },
        EnvVarEntry {
            name: "MEDIAPM_CONDUCTOR_FORCE_GENERATE_INSTANCE_KEY_SECRET",
            default: "false",
            doc: "Generate a secret when state says no secret was used.",
            group: EnvVarGroup::Core,
        },
        EnvVarEntry {
            name: "MEDIAPM_CONDUCTOR_FORCE_DISABLE_INSTANCE_KEY_SECRET",
            default: "false",
            doc: "Continue without a secret when state says one was used.",
            group: EnvVarGroup::Core,
        },
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

    /// The instance-key switches are operator-facing policy, so the rendered
    /// `.env` template must list all five names even though none of them
    /// carries a secret value.
    #[test]
    fn instance_key_secret_env_names_are_catalogued() {
        let names: Vec<_> = env_registry().iter().map(|entry| entry.name).collect();
        for expected in [
            "MEDIAPM_CONDUCTOR_USE_INSTANCE_KEY_SECRET",
            "MEDIAPM_CONDUCTOR_NO_USE_INSTANCE_KEY_SECRET",
            "MEDIAPM_CONDUCTOR_REQUIRE_GENERATED_INSTANCE_KEY_SECRET_FORMAT",
            "MEDIAPM_CONDUCTOR_FORCE_GENERATE_INSTANCE_KEY_SECRET",
            "MEDIAPM_CONDUCTOR_FORCE_DISABLE_INSTANCE_KEY_SECRET",
        ] {
            assert!(
                names.contains(&expected),
                "conductor-cli's table is missing {expected}\ntable: {names:?}"
            );
        }
    }

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
