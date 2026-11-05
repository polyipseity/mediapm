//! mediapm's env-var table and the root registry composition.
//!
//! [`env_registry`] is this crate's own slice: the provider seams, the
//! provisioning and fetch concurrency knobs, the shared HTTP timeout,
//! media-tagger's credentials and binary override, the progress debug sink
//! and the test/example-only isolation roots. [`composed_env_registry`] is
//! what the runtime root renders: conductor's table first (design B1
//! ordering), this crate's beside it, and the `mediapm-cli` clap readers
//! when the `cli` feature is on, all through
//! [`compose_env_registry`]'s
//! first-wins dedup, so a name two tables catalogue renders once with
//! byte-identical content.
//!
//! `mediapm-test-support`'s table is composed from test code only: the crate
//! is a dev-dependency, so production builds of this crate cannot reach it.

use mediapm_utils::env_registry::{EnvVarEntry, EnvVarGroup, compose_env_registry};

/// Returns mediapm's own env-var table.
///
/// Every entry corresponds to a `*_ENV` const or a documented literal read:
/// the four provider seams (`tools/provider/{mod.rs,ffmpeg.rs}`),
/// `MEDIAPM_TOOL_PROVISION_CONCURRENCY` (`conductor_bridge/sync/mod.rs`),
/// `MEDIAPM_HTTP_TIMEOUT_SECONDS` (`builtins/media_tagger/musicbrainz.rs`),
/// `ACOUSTID_API_KEY` and `MEDIAPM_MEDIA_TAGGER_FFMPEG_BIN`
/// (`builtins/media_tagger/mod.rs`), `MEDIAPM_UPDATE_FIXTURES`
/// (`materializer/resolve.rs`, cfg(test)) and the example-isolation roots
/// (`example_isolation.rs`) are read under `src/mediapm/src`, while
/// `MEDIAPM_SOURCE_FETCH_CONCURRENCY` is read only by `mediapm-conductor`'s
/// provider (`tools/provider/mod.rs`) and `MEDIAPM_PROGRESS_DEBUG` only by
/// `mediapm-utils` (`progress/inner/debug.rs`). Names shared with
/// other tables carry byte-identical `default`, `doc` and `group`, which is
/// what makes the composer's first-wins dedup render either copy
/// identically.
#[must_use]
pub fn env_registry() -> &'static [EnvVarEntry] {
    &[
        EnvVarEntry {
            name: "MEDIAPM_HTTP_TIMEOUT_SECONDS",
            default: "1800",
            doc: "Shared HTTP request timeout in seconds; 1800 (30 minutes) when unset, values below 30 ignored.",
            group: EnvVarGroup::Timeouts,
        },
        EnvVarEntry {
            name: "MEDIAPM_SOURCE_FETCH_CONCURRENCY",
            default: "",
            doc: "Concurrent downloads inside one tool's fetch phase; the host's available parallelism (minimum 1) when unset or zero.",
            group: EnvVarGroup::Concurrency,
        },
        EnvVarEntry {
            name: "MEDIAPM_TOOL_PROVISION_CONCURRENCY",
            default: "",
            doc: "Concurrent provisioning entries; the host's available parallelism (minimum 1) when unset or zero.",
            group: EnvVarGroup::Concurrency,
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
            name: "MEDIAPM_PROGRESS_DEBUG",
            default: "",
            doc: "Progress debug JSONL sink: 'auto' or a file path; unset disables it.",
            group: EnvVarGroup::DebugSeams,
        },
        EnvVarEntry {
            name: "ACOUSTID_API_KEY",
            default: "",
            doc: "AcoustID API key for media-tagger metadata enrichment.",
            group: EnvVarGroup::Upstream,
        },
        EnvVarEntry {
            name: "MEDIAPM_MEDIA_TAGGER_FFMPEG_BIN",
            default: "",
            doc: "Manual override for media-tagger's ffmpeg binary (normally generated into .env.generated).",
            group: EnvVarGroup::Core,
        },
        EnvVarEntry {
            name: "MEDIAPM_UPDATE_FIXTURES",
            default: "",
            doc: "test-only: enabled token rewrites materialization golden fixtures.",
            group: EnvVarGroup::TestOnly,
        },
        EnvVarEntry {
            name: "MEDIAPM_EXAMPLE_ARTIFACT_ROOT",
            default: "",
            doc: "test-only: isolated artifact root for examples-as-tests.",
            group: EnvVarGroup::TestOnly,
        },
        EnvVarEntry {
            name: "MEDIAPM_EXAMPLE_CACHE_ROOT",
            default: "",
            doc: "test-only: isolated download cache root for examples-as-tests.",
            group: EnvVarGroup::TestOnly,
        },
    ]
}

/// Returns the registry a mediapm runtime root renders and seeds from.
///
/// Conductor's table leads, this crate's follows, and the `mediapm-cli`
/// table joins when the `cli` feature is enabled (this crate's default),
/// so a `.env.example` or `.env` seed at the mediapm root documents every
/// variable a `mediapm` run reads. Deterministic: the same tables always
/// compose to the same bytes.
#[must_use]
pub fn composed_env_registry() -> Vec<EnvVarEntry> {
    let mut slices: Vec<&[EnvVarEntry]> =
        vec![mediapm_conductor::env_registry::env_registry(), env_registry()];
    #[cfg(feature = "cli")]
    slices.push(mediapm_cli::env_registry::env_registry());
    compose_env_registry(&slices)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every runtime env var this crate reads (provider seams, provisioning
    /// and fetch concurrency, the HTTP timeout, media-tagger keys, the
    /// progress debug sink, the example-isolation roots and the fixture
    /// rewrite toggle) must have a table entry, so the composed root
    /// registry documents what a mediapm run actually reads.
    #[test]
    fn the_mediapm_table_catalogues_its_runtime_readers() {
        let names: Vec<_> = env_registry().iter().map(|entry| entry.name).collect();
        for expected in [
            "MEDIAPM_GITHUB_API_BASE",
            "MEDIAPM_GITHUB_DOWNLOAD_BASE",
            "MEDIAPM_MARTIN_RIEDL_BASE",
            "MEDIAPM_JELLYFIN_BASE",
            "MEDIAPM_PROGRESS_DEBUG",
            "MEDIAPM_TOOL_PROVISION_CONCURRENCY",
            "MEDIAPM_SOURCE_FETCH_CONCURRENCY",
            "MEDIAPM_HTTP_TIMEOUT_SECONDS",
            "ACOUSTID_API_KEY",
            "MEDIAPM_MEDIA_TAGGER_FFMPEG_BIN",
            "MEDIAPM_UPDATE_FIXTURES",
            "MEDIAPM_EXAMPLE_ARTIFACT_ROOT",
            "MEDIAPM_EXAMPLE_CACHE_ROOT",
        ] {
            assert!(
                names.contains(&expected),
                "mediapm's table is missing {expected}\ntable: {names:?}"
            );
        }
    }

    /// The composed registry must carry each slice's surface exactly once:
    /// conductor's timeout knob and mediapm's media-tagger key prove the
    /// slices joined, and each seam proves the mandated overlap deduplicated
    /// instead of rendering twice.
    #[test]
    fn the_composed_registry_carries_every_slice() {
        let names: Vec<_> = composed_env_registry().iter().map(|entry| entry.name).collect();
        assert!(
            names.contains(&"MEDIAPM_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS"),
            "conductor's slice is missing from the composed registry: {names:?}"
        );
        assert!(
            names.contains(&"ACOUSTID_API_KEY"),
            "mediapm's slice is missing from the composed registry: {names:?}"
        );
        #[cfg(feature = "cli")]
        assert!(
            names.contains(&"MEDIAPM_ROOT"),
            "mediapm-cli's slice is missing from the composed registry: {names:?}"
        );
        for seam in [
            "MEDIAPM_GITHUB_API_BASE",
            "MEDIAPM_GITHUB_DOWNLOAD_BASE",
            "MEDIAPM_MARTIN_RIEDL_BASE",
            "MEDIAPM_JELLYFIN_BASE",
        ] {
            let count = names.iter().filter(|name| **name == seam).count();
            assert_eq!(count, 1, "{seam} must render once, rendered {count} times");
        }
    }

    /// A name two tables catalogue must carry byte-identical `default`,
    /// `doc` and `group` in both, or the composer's first-wins dedup would
    /// silently render one crate's wording over another's. This guard makes
    /// that drift a suite failure instead of a render surprise.
    #[test]
    fn shared_names_carry_identical_entries_across_tables() {
        let mut tables: Vec<&[EnvVarEntry]> = vec![
            mediapm_conductor::env_registry::env_registry(),
            env_registry(),
            mediapm_test_support::env_registry::env_registry(),
        ];
        #[cfg(feature = "cli")]
        tables.push(mediapm_cli::env_registry::env_registry());
        for (index, table) in tables.iter().enumerate() {
            for other in &tables[index + 1..] {
                for entry in *table {
                    if let Some(foreign) =
                        other.iter().find(|candidate| candidate.name == entry.name)
                    {
                        assert_eq!(
                            entry, foreign,
                            "shared name {} differs between tables",
                            entry.name
                        );
                    }
                }
            }
        }
    }
}
