//! Upstream seam variables and the process-wide lock that scopes them.
//!
//! The `mediapm` provider layer reads four base URLs from the environment when
//! it is built with its `test-seams` feature: the GitHub REST API base, the
//! GitHub release-download base, the martin-riedl base and the jellyfin base. A
//! hermetic test points all four at one [`MockUpstream`](crate::MockUpstream)
//! for the duration of a scope and restores whatever was there before.
//!
//! Environment mutation is process-global, and an unsynchronized mutation races
//! every other reader in the same process, so every guard holds a lock for its
//! whole lifetime. Which lock is a **required argument** rather than a default:
//! a caller that already owns a mutex must name it, because taking a second one
//! is precisely the race the argument exists to prevent.
//!
//! Two mutexes are in play, and each consuming crate passes its own:
//!
//! - `mediapm`'s tests pass `mediapm::example_isolation::env_lock()`. That is
//!   the lock the `MEDIAPM_EXAMPLE_*` guards already hold, and the one the
//!   provider tests hold while reading the seam variables — which is why those
//!   tests carry `#[expect(clippy::await_holding_lock)]`. A seam guard taken
//!   under any other mutex would race them.
//! - `mediapm-conductor`'s tests pass this crate's [`env_lock`], which
//!   [`lock_process_env`] also guards.
//!
//! A caller that mutates the environment without a guard takes
//! [`lock_process_env`] directly.

use std::ffi::OsString;
use std::sync::LazyLock;

use parking_lot::{Mutex, MutexGuard};

/// Environment variable naming the GitHub REST API base URL.
pub const GITHUB_API_BASE: &str = "MEDIAPM_GITHUB_API_BASE";

/// Environment variable naming the GitHub release-download base URL.
pub const GITHUB_DOWNLOAD_BASE: &str = "MEDIAPM_GITHUB_DOWNLOAD_BASE";

/// Environment variable naming the martin-riedl base URL.
pub const MARTIN_RIEDL_BASE: &str = "MEDIAPM_MARTIN_RIEDL_BASE";

/// Environment variable naming the jellyfin repository base URL.
pub const JELLYFIN_BASE: &str = "MEDIAPM_JELLYFIN_BASE";

/// Every seam variable, in the order a guard installs them.
pub const ALL_SEAMS: [&str; 4] =
    [GITHUB_API_BASE, GITHUB_DOWNLOAD_BASE, MARTIN_RIEDL_BASE, JELLYFIN_BASE];

/// This crate's process-wide lock serializing seam mutation.
static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// Borrows this crate's process-wide environment lock.
///
/// Pass the result to [`SeamGuard::new`] or [`SeamGuard::all_seams`] when the
/// consuming crate has no lock of its own. A crate that has one passes that
/// instead, so no caller ever silently takes a second mutex.
#[must_use]
pub fn env_lock() -> &'static Mutex<()> {
    &ENV_LOCK
}

/// Acquires this crate's process-wide environment lock.
///
/// The returned guard is held for as long as the caller keeps it, so a test
/// that mutates the environment directly (rather than through a [`SeamGuard`])
/// can scope its own mutation against every sibling in the same process.
pub fn lock_process_env() -> MutexGuard<'static, ()> {
    ENV_LOCK.lock()
}

/// Pairs every seam variable with `base`.
///
/// One mock server answers all four hosts' paths, because the paths the four
/// seams lead to do not overlap.
#[must_use]
pub fn seam_pairs(base: &str) -> Vec<(&'static str, String)> {
    ALL_SEAMS.iter().map(|name| (*name, base.to_string())).collect()
}

/// Refuses to install an upstream seam in a build without `test-seams`.
///
/// The check takes a boolean rather than sitting inside a `cfg` block so it is
/// reachable and testable in every build: the call site evaluates
/// `cfg!(feature = "test-seams")`, and this function keeps the message in one
/// place. The refusal must be a panic rather than a silent pass, because a
/// build without the feature ignores the seam variables entirely and a test
/// that proceeded would fetch from the real upstream.
///
/// # Panics
///
/// Panics when `enabled` is false.
pub(crate) fn enforce_seam_feature(enabled: bool) {
    assert!(
        enabled,
        "mediapm-test-support was built without its `test-seams` feature, so the seams it installs \
         would be ignored and the test would reach the real network; run the test with \
         `--all-features`, for example `cargo test-pkg mediapm --all-features`"
    );
}

/// Reads `name` under this crate's process lock.
///
/// A test that asserts on the environment *after* its guard has dropped needs
/// this: without the lock a sibling's live guard could be installed while the
/// assertion runs, which makes such an assertion fail intermittently under a
/// threaded `cargo test`. Inside a live guard's scope the guard's own lock
/// already covers the read, and taking this lock there would deadlock.
#[cfg(all(test, feature = "test-seams"))]
pub(crate) fn read_under_lock(name: &str) -> Option<OsString> {
    let _lock = ENV_LOCK.lock();
    std::env::var_os(name)
}

/// RAII scope installing seam values and restoring the previous environment.
///
/// The guard holds its mutex for its whole lifetime and restores every variable
/// to the value it had (or to absence) when the guard was built, so a test that
/// panics still cannot leak a seam into a sibling. The restore runs from the
/// `Drop` impl below, which is what puts the writes before the unlock; field
/// declaration order plays no part in it.
pub struct SeamGuard {
    /// The process lock held for this guard's whole lifetime.
    _lock: MutexGuard<'static, ()>,
    /// Previous value, or absence, of every variable this guard set.
    restore: Vec<(&'static str, Option<OsString>)>,
}

impl SeamGuard {
    /// Installs `vars` under `lock`.
    ///
    /// The lock is a required parameter rather than a defaulted one, so a
    /// caller that already owns a mutex names it and no call site can pick the
    /// wrong lock by omission. Pass [`env_lock`] when the consuming crate has
    /// no lock of its own.
    ///
    /// # Panics
    ///
    /// Panics when this crate was built without `test-seams`.
    #[must_use]
    pub fn new(lock: &'static Mutex<()>, vars: &[(&'static str, String)]) -> Self {
        enforce_seam_feature(cfg!(feature = "test-seams"));
        let guard = lock.lock();
        let restore = vars
            .iter()
            .map(|(name, value)| {
                let previous = std::env::var_os(name);
                // SAFETY: the guard returned by `lock` is held here, so no
                // other thread in this process mutates the environment for the
                // rest of this scope, and every reader that takes the same lock
                // is excluded until `drop` releases it.
                unsafe { std::env::set_var(name, value) };
                (*name, previous)
            })
            .collect();
        Self { _lock: guard, restore }
    }

    /// Installs every seam variable, all pointing at `base`, under `lock`.
    ///
    /// # Panics
    ///
    /// Panics when this crate was built without `test-seams`.
    #[must_use]
    pub fn all_seams(lock: &'static Mutex<()>, base: &str) -> Self {
        Self::new(lock, &seam_pairs(base))
    }
}

impl Drop for SeamGuard {
    /// Restores every variable this guard set, then releases the lock.
    ///
    /// The lock is still held here: `drop` runs before the guard's fields are
    /// released, so no sibling can observe the environment between the restore
    /// and the unlock.
    fn drop(&mut self) {
        for (name, previous) in &self.restore {
            match previous {
                Some(value) => {
                    // SAFETY: the lock is held for the whole of `drop`.
                    unsafe { std::env::set_var(name, value) };
                }
                None => {
                    // SAFETY: the lock is held for the whole of `drop`.
                    unsafe { std::env::remove_var(name) };
                }
            }
        }
    }
}

#[cfg(all(test, feature = "test-seams"))]
mod tests {
    use super::*;

    /// A variable no other test in this crate touches, so a test that installs
    /// a prior value for it races no sibling.
    const PROBE_VALUE: &str = "MEDIAPM_TEST_SUPPORT_PROBE_VALUE";

    /// A second such variable, for the panic case.
    const PROBE_PANIC: &str = "MEDIAPM_TEST_SUPPORT_PROBE_PANIC";

    /// Installs `value` for `name` under the process lock.
    ///
    /// The lock is released before this function returns, which is what lets
    /// the caller construct a [`SeamGuard`] afterwards: the guard takes the
    /// same non-reentrant mutex, so a caller that still held it would deadlock.
    fn install_prior(name: &'static str, value: &str) {
        let lock = lock_process_env();
        // SAFETY: the lock is held for this write, and every reader that takes
        // the same lock is excluded until it is released.
        unsafe { std::env::set_var(name, value) };
        drop(lock);
    }

    /// Removes `name` under the process lock.
    fn clear(name: &'static str) {
        let lock = lock_process_env();
        // SAFETY: as `install_prior`.
        unsafe { std::env::remove_var(name) };
        drop(lock);
    }

    /// A guard restores the value a seam variable held before it was built.
    #[test]
    fn a_guard_restores_the_previous_value() {
        install_prior(PROBE_VALUE, "http://before.example");

        {
            let _guard =
                SeamGuard::new(env_lock(), &[(PROBE_VALUE, "http://during.example".to_string())]);
            assert_eq!(
                std::env::var(PROBE_VALUE).expect("seam set by the guard"),
                "http://during.example"
            );
        }

        assert_eq!(
            read_under_lock(PROBE_VALUE).expect("previous value restored"),
            "http://before.example"
        );
        clear(PROBE_VALUE);
    }

    /// A guard removes a seam variable that was absent before it was built.
    #[test]
    fn a_guard_restores_absence() {
        clear(MARTIN_RIEDL_BASE);

        {
            let _guard = SeamGuard::new(
                env_lock(),
                &[(MARTIN_RIEDL_BASE, "http://during.example".to_string())],
            );
            assert!(std::env::var_os(MARTIN_RIEDL_BASE).is_some());
        }

        assert!(
            read_under_lock(MARTIN_RIEDL_BASE).is_none(),
            "an absent variable must be removed again, not left empty"
        );
    }

    /// A scope that panics still restores the environment.
    #[test]
    fn a_guard_restores_after_a_panic() {
        install_prior(PROBE_PANIC, "http://before.example");

        let unwound = std::panic::catch_unwind(|| {
            let _guard =
                SeamGuard::new(env_lock(), &[(PROBE_PANIC, "http://during.example".to_string())]);
            panic!("the scope under test fails");
        });

        assert!(unwound.is_err(), "the scope must have panicked");
        assert_eq!(
            read_under_lock(PROBE_PANIC).expect("previous value restored"),
            "http://before.example"
        );
        clear(PROBE_PANIC);
    }

    /// Every seam variable points at one base, which is what one mock server
    /// answering all four hosts requires.
    #[test]
    fn all_seams_points_every_variable_at_one_base() {
        for name in ALL_SEAMS {
            clear(name);
        }

        {
            let _guard = SeamGuard::all_seams(env_lock(), "http://mock.example");
            for name in ALL_SEAMS {
                assert_eq!(
                    std::env::var(name).expect("seam installed"),
                    "http://mock.example",
                    "{name} must point at the mock"
                );
            }
        }

        for name in ALL_SEAMS {
            assert!(read_under_lock(name).is_none(), "{name} must be removed again");
        }
    }

    /// A guard holds the lock it was given for its whole lifetime and releases
    /// it on drop.
    ///
    /// The mutex is private to this test, so neither `try_lock` can be
    /// disturbed by a sibling: against a shared static the second assertion
    /// would fail whenever any other test held it.
    #[test]
    fn a_guard_holds_its_lock_and_releases_it_on_drop() {
        static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

        let guard = SeamGuard::new(&TEST_LOCK, &[]);
        assert!(TEST_LOCK.try_lock().is_none(), "the guard must hold its lock while it is alive");
        drop(guard);
        assert!(TEST_LOCK.try_lock().is_some(), "the guard must release its lock on drop");
    }

    /// `env_lock` hands out the mutex `lock_process_env` guards.
    ///
    /// The assertion cannot fail spuriously: this test holds the process lock
    /// itself, so a free `env_lock` could only mean the two name different
    /// mutexes. A conductor test passes `env_lock` to a guard and then takes
    /// `lock_process_env` for its own mutation, which only excludes the guard
    /// if the two agree.
    #[test]
    fn env_lock_and_lock_process_env_guard_the_same_mutex() {
        let held = lock_process_env();
        assert!(
            env_lock().try_lock().is_none(),
            "env_lock must hand out the mutex lock_process_env guards"
        );
        drop(held);
    }

    /// The refusal is not triggered by a build that has the feature.
    #[test]
    fn the_refusal_accepts_an_enabled_build() {
        enforce_seam_feature(true);
    }
}

/// The refusal message, which both builds compile.
///
/// These assertions reach [`enforce_seam_feature`] directly rather than through
/// a constructor, so they hold whether or not the feature is on.
#[cfg(test)]
mod refusal {
    use super::*;

    /// The refusal names the feature that fixes it.
    #[test]
    #[should_panic(expected = "test-seams")]
    fn the_refusal_names_the_feature() {
        enforce_seam_feature(false);
    }

    /// The refusal carries the command a reader needs to run instead.
    #[test]
    #[should_panic(expected = "cargo test-pkg mediapm --all-features")]
    fn the_refusal_names_the_command() {
        enforce_seam_feature(false);
    }

    /// A build without the feature refuses to install a seam, so a test that
    /// reached the real upstream is impossible rather than merely unlikely.
    ///
    /// This is the only behavior a feature-off build can exercise, and it is
    /// the one that matters: the alternative to a loud refusal is a test that
    /// silently fetches from the real upstream.
    #[cfg(not(feature = "test-seams"))]
    #[test]
    #[should_panic(expected = "test-seams")]
    fn installing_a_seam_without_the_feature_refuses() {
        let _guard =
            SeamGuard::new(env_lock(), &[(GITHUB_API_BASE, "http://mock.example".to_string())]);
    }
}
