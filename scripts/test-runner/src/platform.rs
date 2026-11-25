//! The `platform` subcommand: lint foreign-target code from the host.
//!
//! `cfg(windows)`, `cfg(unix)`, and `cfg(target_os = ...)` code is only
//! compiled for the target it names, so a lint error inside such a block
//! reaches `main` and waits for the matching CI leg to surface it. This gate
//! runs `clippy` for each foreign target from the host, so the error appears
//! before the push instead of after it.
//!
//! It cannot cover the whole workspace. `mediapm`, `mediapm-conductor`,
//! `mediapm-conductor-builtin-import`, `mediapm-test-support`, and (for the
//! Windows target only) `mediapm-cas` pull in `ring` or `blake3`, whose C
//! build scripts need a Windows SDK that a cross `clippy` run here does not
//! have. Each such package is listed per target with its reason, and
//! [`classify`] fails the gate when a real workspace package is in neither
//! list, so a new crate cannot join the workspace uncovered.

use std::ffi::OsString;

use anyhow::{Context, Result};

use crate::cargo::{self, Metadata};
use crate::cli::PlatformArgs;

/// One foreign target and the packages the gate checks against it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LintTarget {
    /// The rustc target triple to lint for.
    pub triple: &'static str,
    /// Workspace packages this target is linted for.
    pub checked: &'static [&'static str],
    /// Workspace packages left out, each with the reason it cannot be built.
    pub excluded: &'static [(&'static str, &'static str)],
}

/// The `ring`/`blake3` reason shared by every target.
const C_DEPENDENCY: &str = "needs ring/blake3 C build scripts, which require a Windows SDK a cross clippy run here does not have";

/// The targets the gate lints for, with their package lists.
pub fn targets() -> &'static [LintTarget] {
    const UNIX_CHECKED: &[&str] = &[
        "build-utils",
        "cargo-bin",
        "mediapm-cas",
        "mediapm-cas-cli",
        "mediapm-cli",
        "mediapm-conductor-builtin-archive",
        "mediapm-conductor-builtin-echo",
        "mediapm-conductor-builtin-export",
        "mediapm-conductor-builtin-fs",
        "mediapm-conductor-cli",
        "mediapm-conductor-examples-concat-tool",
        "mediapm-tests",
        "mediapm-utils",
        "test-runner",
    ];
    const UNIX_EXCLUDED: &[(&str, &str)] = &[
        ("mediapm", C_DEPENDENCY),
        ("mediapm-conductor", C_DEPENDENCY),
        ("mediapm-conductor-builtin-import", C_DEPENDENCY),
        ("mediapm-test-support", C_DEPENDENCY),
    ];
    const WINDOWS_CHECKED: &[&str] = &[
        "build-utils",
        "cargo-bin",
        "mediapm-cas-cli",
        "mediapm-cli",
        "mediapm-conductor-builtin-archive",
        "mediapm-conductor-builtin-echo",
        "mediapm-conductor-builtin-export",
        "mediapm-conductor-builtin-fs",
        "mediapm-conductor-cli",
        "mediapm-conductor-examples-concat-tool",
        "mediapm-tests",
        "mediapm-utils",
        "test-runner",
    ];
    const WINDOWS_EXCLUDED: &[(&str, &str)] = &[
        ("mediapm", C_DEPENDENCY),
        ("mediapm-cas", C_DEPENDENCY),
        ("mediapm-conductor", C_DEPENDENCY),
        ("mediapm-conductor-builtin-import", C_DEPENDENCY),
        ("mediapm-test-support", C_DEPENDENCY),
    ];
    &[
        LintTarget {
            triple: "x86_64-unknown-linux-gnu",
            checked: UNIX_CHECKED,
            excluded: UNIX_EXCLUDED,
        },
        LintTarget {
            triple: "x86_64-apple-darwin",
            checked: UNIX_CHECKED,
            excluded: UNIX_EXCLUDED,
        },
        LintTarget {
            triple: "x86_64-pc-windows-msvc",
            checked: WINDOWS_CHECKED,
            excluded: WINDOWS_EXCLUDED,
        },
    ]
}

/// Reports a package that one target both checks and excludes.
///
/// Kept separate from [`classify`] so each function answers one question: a
/// package nobody classified, and a package classified two ways.
pub fn classify_conflict(target: &LintTarget, package: &str) -> Vec<String> {
    let checked = target.checked.contains(&package);
    let excluded = target.excluded.iter().any(|(name, _)| *name == package);
    if checked && excluded {
        vec![format!("{} lists {package} as both checked and excluded", target.triple)]
    } else {
        Vec::new()
    }
}

/// Reports every workspace package the target lists do not classify.
///
/// Returns one line per problem so a run reports all of them at once. A
/// package missing from both lists, or present in both, is a hole in the
/// gate, and the message names the target so the fix is local.
pub fn classify(metadata: &Metadata) -> Vec<String> {
    let mut problems = Vec::new();
    for target in targets() {
        for package in &metadata.packages {
            let checked = target.checked.iter().any(|c| *c == package.name);
            let excluded = target.excluded.iter().any(|(name, _)| *name == package.name);
            if !checked && !excluded {
                problems.push(format!(
                    "{} does not classify {}: add it to checked or excluded",
                    target.triple, package.name
                ));
            }
            problems.extend(classify_conflict(target, &package.name));
        }
    }
    problems
}

/// Runs every target, reporting all failures rather than the first.
pub fn run(args: &PlatformArgs) -> Result<i32> {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let metadata = cargo::metadata(dir)?;

    let problems = classify(&metadata);
    if !problems.is_empty() {
        for problem in &problems {
            eprintln!("error: {problem}");
        }
        return Ok(1);
    }

    if args.print {
        for target in targets() {
            println!("{} checked: {}", target.triple, target.checked.join(" "));
            for (package, reason) in target.excluded {
                println!("{} excluded: {package} ({reason})", target.triple);
            }
        }
        return Ok(0);
    }

    let cargo_bin = cargo::cargo_binary();
    let total = targets().len();
    let mut failures = Vec::new();
    for (index, target) in targets().iter().enumerate() {
        let clippy_argv = target_argv(args, target);
        let started = std::time::Instant::now();
        let status = std::process::Command::new(&cargo_bin)
            .current_dir(dir)
            .args(&clippy_argv)
            .status()
            .with_context(|| format!("run cargo clippy for {}", target.triple))?;
        let elapsed = started.elapsed();
        let code = cargo::status_code(status, target.triple)?;
        let passed = code == 0;
        println!("{}", target_result_line(index + 1, total, target, passed, elapsed));
        if !passed {
            failures.push(format!("{} (exit {code})", target.triple));
        }
    }

    if failures.is_empty() {
        println!("platform: {total} targets, all clean");
        return Ok(0);
    }
    for failure in &failures {
        eprintln!("error: {failure}");
    }
    eprintln!("error: {} of {total} targets failed", failures.len());
    Ok(1)
}

/// Formats one per-target progress/result line.
///
/// Emitted after the target completes so the elapsed wall-clock duration is
/// known. The caller owns the 1-based index and the total.
fn target_result_line(
    index: usize,
    total: usize,
    target: &LintTarget,
    passed: bool,
    elapsed: std::time::Duration,
) -> String {
    let outcome = if passed { "pass" } else { "fail" };
    format!("platform: {index}/{total} {} {outcome} {:.2}s", target.triple, elapsed.as_secs_f64())
}

/// The argv for one `cargo clippy` probe.
///
/// `clippy` first, then the lock flag, then the target, then every checked
/// package as a `-p` pair, then `--all-targets --all-features`. The order
/// is fixed so two runs of the same target produce byte-identical argv.
fn target_argv(args: &PlatformArgs, target: &LintTarget) -> Vec<OsString> {
    let mut clippy_argv: Vec<OsString> = vec![OsString::from("clippy")];
    if args.wants_lock() {
        clippy_argv.push(OsString::from("--locked"));
    }
    clippy_argv.push(OsString::from("--target"));
    clippy_argv.push(OsString::from(target.triple));
    for package in target.checked {
        clippy_argv.push(OsString::from("-p"));
        clippy_argv.push(OsString::from(*package));
    }
    clippy_argv.push(OsString::from("--all-targets"));
    clippy_argv.push(OsString::from("--all-features"));
    clippy_argv
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::ffi::OsString;

    use clap::Parser;

    use super::{
        LintTarget, classify, classify_conflict, target_argv, target_result_line, targets,
    };
    use crate::cargo::Metadata;
    use crate::cli::{Cli, Command};

    /// Builds a [`Metadata`] the way `cargo metadata --no-deps` reports it.
    fn metadata(names: &[&str]) -> Metadata {
        let packages: Vec<String> =
            names.iter().map(|name| format!(r#"{{"name":"{name}","features":{{}}}}"#)).collect();
        serde_json::from_str(&format!(
            r#"{{"workspace_root":"/repo","packages":[{}]}}"#,
            packages.join(",")
        ))
        .expect("build metadata")
    }

    /// Every real workspace package must be either checked or excluded for
    /// every target. Without this, adding a crate silently escapes the gate:
    /// the lists are the only record of what is covered, so nothing else
    /// would notice the omission.
    #[test]
    fn every_workspace_package_is_classified_for_every_target() {
        let real = [
            "build-utils",
            "cargo-bin",
            "mediapm",
            "mediapm-cas",
            "mediapm-cas-cli",
            "mediapm-cli",
            "mediapm-conductor",
            "mediapm-conductor-builtin-archive",
            "mediapm-conductor-builtin-echo",
            "mediapm-conductor-builtin-export",
            "mediapm-conductor-builtin-fs",
            "mediapm-conductor-cli",
            "mediapm-conductor-examples-concat-tool",
            "mediapm-test-support",
            "mediapm-tests",
            "mediapm-utils",
            "test-runner",
        ];
        assert_eq!(classify(&metadata(&real)), Vec::<String>::new());
    }

    /// A package in neither list is a gate hole, and the report names it
    /// together with the target, so the fix is to classify it rather than to
    /// read the whole file.
    #[test]
    fn an_unclassified_package_is_reported_per_target() {
        let problems = classify(&metadata(&["brand-new-crate"]));
        assert!(!problems.is_empty(), "a package in no list has to be reported, not skipped");
        assert!(
            problems.iter().all(|p| p.contains("brand-new-crate")),
            "every problem names the package: {problems:?}"
        );
    }

    /// A package claimed by both lists for the same target is contradictory:
    /// the run would check it and skip it. The report has to name that too,
    /// or an exclusion that was meant to be temporary hides a checked crate.
    ///
    /// The target is synthetic rather than read from [`targets`]: the real
    /// lists are checked for consistency by
    /// `every_workspace_package_is_classified_for_every_target`, and a test
    /// that built its conflict out of a real table would have to mutate one.
    #[test]
    fn a_package_in_both_lists_is_reported_as_a_conflict() {
        let contradictory = LintTarget {
            triple: "x86_64-pc-windows-msvc",
            checked: &["mediapm-utils"],
            excluded: &[("mediapm-utils", "pretend reason")],
        };
        let problems = classify_conflict(&contradictory, "mediapm-utils");
        assert!(
            problems.iter().any(|p| p.contains("both")),
            "a package in checked and excluded conflicts: {problems:?}"
        );
    }

    /// The conflict helper stays silent for the ordinary classified case, so
    /// it cannot mask a real problem by reporting everything.
    #[test]
    fn a_normally_classified_package_is_not_a_conflict() {
        let target = LintTarget {
            triple: "x86_64-pc-windows-msvc",
            checked: &["mediapm-utils"],
            excluded: &[("mediapm", "pretend reason")],
        };
        assert_eq!(classify_conflict(&target, "mediapm-utils"), Vec::<String>::new());
        assert_eq!(classify_conflict(&target, "mediapm"), Vec::<String>::new());
    }

    /// The gate must lint for a Windows target: that is the target whose
    /// lint error made `main` red, so a gate that dropped it would have
    /// missed the failure it exists for.
    #[test]
    fn a_windows_target_is_covered() {
        assert!(
            targets().iter().any(|t| t.triple.contains("windows")),
            "the gate has to cover a windows target: {:?}",
            targets().iter().map(|t| t.triple).collect::<Vec<_>>()
        );
    }

    /// Every exclusion states why, because an unexplained exclusion is
    /// indistinguishable from a package someone forgot. The reason is the
    /// only thing that stops the list decaying into silence.
    #[test]
    fn every_exclusion_carries_a_reason() {
        for target in targets() {
            for (pkg, reason) in target.excluded {
                assert!(
                    !reason.trim().is_empty(),
                    "{} excludes {pkg} without a reason",
                    target.triple
                );
            }
        }
    }

    /// No package may appear twice in one list, or the run spawns a
    /// duplicate `-p` and the printed summary overstates coverage.
    #[test]
    fn no_target_lists_a_package_twice() {
        for target in targets() {
            let checked: BTreeSet<&str> = target.checked.iter().copied().collect();
            assert_eq!(
                checked.len(),
                target.checked.len(),
                "{} repeats a checked package",
                target.triple
            );
            let excluded: BTreeSet<&str> = target.excluded.iter().map(|(p, _)| *p).collect();
            assert_eq!(
                excluded.len(),
                target.excluded.len(),
                "{} repeats an excluded package",
                target.triple
            );
        }
    }

    /// Pins the argv a target actually spawns, in full. Every element is
    /// load-bearing: drop `--target` and the gate lints the host instead of
    /// the foreign target, drop a `-p` and that crate silently escapes the
    /// gate, and swap `--locked` for an unlocked run and the sweep starts
    /// rewriting `Cargo.lock`.
    #[test]
    fn target_argv_pins_target_packages_and_lock() {
        let Command::Platform(args) = Cli::parse_from(["test-runner", "platform"]).command else {
            panic!("expected platform");
        };
        let target = LintTarget {
            triple: "x86_64-pc-windows-msvc",
            checked: &["mediapm-utils", "test-runner"],
            excluded: &[],
        };
        assert_eq!(
            target_argv(&args, &target),
            vec![
                OsString::from("clippy"),
                OsString::from("--locked"),
                OsString::from("--target"),
                OsString::from("x86_64-pc-windows-msvc"),
                OsString::from("-p"),
                OsString::from("mediapm-utils"),
                OsString::from("-p"),
                OsString::from("test-runner"),
                OsString::from("--all-targets"),
                OsString::from("--all-features"),
            ]
        );
    }

    #[test]
    fn target_argv_omits_locked_when_opted_out() {
        let Command::Platform(args) =
            Cli::parse_from(["test-runner", "platform", "--no-locked"]).command
        else {
            panic!("expected platform");
        };
        let target = LintTarget { triple: "x86_64-apple-darwin", checked: &[], excluded: &[] };
        let got = target_argv(&args, &target);
        assert!(!got.iter().any(|a| a == "--locked"), "opted out: {got:?}");
        assert_eq!(
            got,
            vec![
                OsString::from("clippy"),
                OsString::from("--target"),
                OsString::from("x86_64-apple-darwin"),
                OsString::from("--all-targets"),
                OsString::from("--all-features"),
            ]
        );
    }

    #[test]
    fn target_result_line_formats_index_target_outcome_and_elapsed() {
        let target = LintTarget { triple: "x86_64-pc-windows-msvc", checked: &[], excluded: &[] };
        assert_eq!(
            target_result_line(2, 3, &target, false, std::time::Duration::from_millis(1520)),
            "platform: 2/3 x86_64-pc-windows-msvc fail 1.52s"
        );
    }
}
