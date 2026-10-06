//! The `feature-matrix` subcommand.
//!
//! Every workspace member is checked with `--no-default-features`, then
//! each single feature it declares, then `--all-features`. The CI workflow
//! used to carry the resulting combination list as a 46-row YAML matrix,
//! which is a list that silently rots: a crate gains a feature and the
//! matrix stops covering it. Deriving the list from `cargo metadata` makes
//! that impossible.
//!
//! Three things a reader of the old matrix had to be told, and which now
//! hold as properties of the derivation:
//!
//! 1. `--no-default-features` is not a minimal build of `mediapm-utils`
//!    through `mediapm` or `mediapm-conductor`. Both force features on
//!    regardless, so only probing the crate directly gives a minimal build.
//! 2. Eight bin targets need `required-features = ["cli"]`, so cargo skips
//!    them on every probe without it. Expected, not a failure.
//! 3. No `--all-targets`, so `#[cfg(test)]` code depending on an optional
//!    dependency goes unchecked. Adding it would unify dev-dependency
//!    features across all probes and mask a genuinely missing dependency.

use std::ffi::OsString;

use anyhow::{Context, Result};

use crate::cargo::{self, Metadata};
use crate::cli::MatrixArgs;

/// One `cargo check` probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Probe {
    /// The workspace member to check.
    pub package: String,
    /// The feature flags to check it with.
    pub flags: Vec<String>,
}

/// Runs every derived probe, reporting all failures rather than the first.
pub fn run(args: &MatrixArgs) -> Result<i32> {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let metadata = cargo::metadata(dir)?;
    let derived = probes(&metadata);

    if args.print {
        for probe in &derived {
            println!("{} {}", probe.package, probe.flags.join(" "));
        }
        return Ok(0);
    }

    let cargo_bin = cargo::cargo_binary();
    let mut failures = Vec::new();
    for probe in &derived {
        let check_argv = probe_argv(args, probe);
        let status = std::process::Command::new(&cargo_bin)
            .current_dir(dir)
            .args(&check_argv)
            .status()
            .with_context(|| format!("run cargo check for {}", probe.package))?;
        let code = cargo::status_code(status, &probe.package)?;
        if code != 0 {
            failures.push(format!("{} {} (exit {code})", probe.package, probe.flags.join(" ")));
        }
    }

    if failures.is_empty() {
        println!("feature matrix: {} combinations, all clean", derived.len());
        return Ok(0);
    }
    for failure in &failures {
        eprintln!("error: {failure}");
    }
    eprintln!("error: {} of {} feature combinations failed", failures.len(), derived.len());
    Ok(1)
}

/// The argv for one `cargo check` probe.
///
/// `check` first, then the lock flag, then the package, then the probe's
/// own feature flags. `--locked` precedes the package selector because
/// both are cargo flags and neither changes the other's meaning; the order
/// is fixed so two runs of the same probe produce byte-identical argv.
///
/// The local is named `check_argv` rather than `argv` only because
/// `clippy::similar_names` reads `argv` as too close to the `args`
/// parameter; the two differ by one character.
fn probe_argv(args: &MatrixArgs, probe: &Probe) -> Vec<OsString> {
    let mut check_argv: Vec<OsString> = vec![OsString::from("check")];
    if args.wants_lock() {
        check_argv.push(OsString::from("--locked"));
    }
    check_argv.push(OsString::from("--package"));
    check_argv.push(OsString::from(&probe.package));
    check_argv.extend(probe.flags.iter().map(OsString::from));
    check_argv
}

/// Derives every probe from `metadata`, ordered by package name.
///
/// Per package: `--no-default-features`, then each declared feature in
/// sorted order except the implicit `default`, then `--all-features`.
/// `cargo metadata` reports `default` alongside the real features, and
/// sweeping it alone is redundant, so it is filtered out.
///
/// The filter matches the feature's NAME rather than its position, and it
/// has to: `default` sorts among the real features, so in `mediapm-utils`
/// (keys `cli`, `default`, `nickel`, `progress`, `report`) it is the second
/// key, and dropping a fixed index would delete `nickel` instead.
pub fn probes(metadata: &Metadata) -> Vec<Probe> {
    let mut packages: Vec<_> = metadata.packages.iter().collect();
    packages.sort_by(|a, b| a.name.cmp(&b.name));

    let mut derived = Vec::new();
    for package in packages {
        derived.push(Probe {
            package: package.name.clone(),
            flags: vec!["--no-default-features".to_string()],
        });
        for feature in package.features.keys().filter(|name| name.as_str() != "default") {
            derived.push(Probe {
                package: package.name.clone(),
                flags: vec!["--features".to_string(), feature.clone()],
            });
        }
        derived.push(Probe {
            package: package.name.clone(),
            flags: vec!["--all-features".to_string()],
        });
    }
    derived
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ffi::OsString;
    use std::path::PathBuf;

    use clap::Parser;

    use super::{Probe, probe_argv, probes};
    use crate::cargo::Metadata;
    use crate::cli::{Cli, Command, MatrixArgs};

    /// The `MatrixArgs` a caller would get from the command line, so the
    /// argv tests exercise the same `wants_lock` decision the sweep does
    /// rather than a hand-set flag pair. The local is `command_line`
    /// rather than `argv` so `clippy::similar_names` does not read it as
    /// too close to the `args` binding on the next line.
    fn matrix_args(options: &[&str]) -> MatrixArgs {
        let mut command_line = vec!["test-runner", "feature-matrix"];
        command_line.extend_from_slice(options);
        let Command::FeatureMatrix(args) = Cli::parse_from(command_line).command else {
            panic!("expected the feature-matrix subcommand");
        };
        args
    }

    /// Builds a [`Metadata`] the way `cargo metadata` would report it, from
    /// a package-name/feature-name-pair table. Hand-built rather than
    /// produced by a live cargo run, so the derivation is tested against a
    /// shape it fully controls instead of against whatever the workspace
    /// happens to declare this week.
    fn metadata(names: &[(&str, &[&str])]) -> Metadata {
        let raw = format!(
            r#"{{"workspace_root":"/repo","packages":[{}]}}"#,
            names
                .iter()
                .map(|(name, features)| {
                    let entries: Vec<String> =
                        features.iter().map(|f| format!("\"{f}\":[]")).collect();
                    format!(r#"{{"name":"{name}","features":{{{}}}}}"#, entries.join(","))
                })
                .collect::<Vec<_>>()
                .join(",")
        );
        serde_json::from_str(&raw).expect("build metadata")
    }

    /// Every member is covered whether or not it declares features, so a
    /// crate that gains its first `[features]` section is already checked
    /// by the same derivation rather than by an edit to a hard-coded list.
    #[test]
    fn a_package_without_features_gets_two_endpoints() {
        let got = probes(&metadata(&[("alpha", &[])]));
        assert_eq!(
            got,
            vec![
                Probe { package: "alpha".into(), flags: vec!["--no-default-features".into()] },
                Probe { package: "alpha".into(), flags: vec!["--all-features".into()] },
            ]
        );
    }

    /// The implicit `default` is cargo's own grouping of the features a
    /// crate enables unasked, so a single-feature probe for it is the same
    /// build as the crate's normal one and covers nothing the endpoint
    /// probes do not. Sweeping it would also inflate the row count.
    #[test]
    fn the_implicit_default_feature_is_not_swept_on_its_own() {
        let got = probes(&metadata(&[("alpha", &["cli", "default", "proptest"])]));
        assert_eq!(got.len(), 4);
        assert_eq!(got[1].flags, vec!["--features".to_string(), "cli".to_string()]);
        assert_eq!(got[2].flags, vec!["--features".to_string(), "proptest".to_string()]);
        assert!(
            got.iter().all(|p| p.flags != vec!["--features".to_string(), "default".to_string()])
        );
    }

    /// Ordering is by name rather than by whatever order `cargo metadata`
    /// happened to emit, so two runs of the sweep agree row for row and a
    /// failure can be located from the printed list alone.
    #[test]
    fn packages_are_ordered_by_name() {
        let got = probes(&metadata(&[("zulu", &[]), ("alpha", &[]), ("mike", &[])]));
        let names: Vec<&str> = got.iter().map(|p| p.package.as_str()).collect();
        assert_eq!(names, vec!["alpha", "alpha", "mike", "mike", "zulu", "zulu"]);
    }

    /// The synthetic fixtures above are hand-built JSON, so this is the
    /// only assertion tying the shape `probes` reads to the shape
    /// `cargo metadata` actually emits.
    #[test]
    fn a_real_workspace_shape_deserializes() {
        let parsed: Metadata = serde_json::from_str(
            r#"{"workspace_root":"/repo","packages":[{"name":"beta","features":{}}]}"#,
        )
        .expect("parse");
        let _: BTreeMap<String, Vec<String>> = parsed.packages[0].features.clone();
        assert_eq!(parsed.workspace_root, PathBuf::from("/repo"));
    }

    /// Pins the argv a probe actually spawns, in full. Every element is
    /// named here because the pieces are independently load-bearing: drop
    /// the `check` subcommand and cargo builds a workspace instead of
    /// checking, drop `--package` and one probe silently checks all 14
    /// members, and swap `--locked` for an unlocked resolution and the
    /// sweep starts rewriting `Cargo.lock`.
    #[test]
    fn probe_argv_locks_by_default() {
        let probe = Probe {
            package: "mediapm-utils".into(),
            flags: vec!["--features".into(), "progress".into()],
        };
        assert_eq!(
            probe_argv(&matrix_args(&[]), &probe),
            vec![
                OsString::from("check"),
                OsString::from("--locked"),
                OsString::from("--package"),
                OsString::from("mediapm-utils"),
                OsString::from("--features"),
                OsString::from("progress"),
            ]
        );
    }

    /// The escape hatch has to remove `--locked` rather than merely add a
    /// flag beside it, since cargo takes the last `--locked` it sees and a
    /// leftover one would leave the opt-out inert. The full-vector
    /// assertion is what makes that claim: checking only for absence would
    /// also pass against an argv carrying an unrelated extra element.
    #[test]
    fn probe_argv_omits_locked_when_opted_out() {
        let probe =
            Probe { package: "mediapm-utils".into(), flags: vec!["--no-default-features".into()] };
        let got = probe_argv(&matrix_args(&["--no-locked"]), &probe);
        assert!(
            !got.iter().any(|a| a == "--locked"),
            "an opted-out sweep must not carry --locked: {got:?}"
        );
        assert_eq!(
            got,
            vec![
                OsString::from("check"),
                OsString::from("--package"),
                OsString::from("mediapm-utils"),
                OsString::from("--no-default-features"),
            ]
        );
    }
}
