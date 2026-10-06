//! Repository hygiene gates that run after the test suite.
//!
//! Both gates answer one question: did the suite leave the tree in a state
//! the next person can work with. They used to exist twice, once in
//! `run-all-tests.sh` and once in `run-all-tests.ps1`; keeping them here
//! removes the parity requirement instead of maintaining it.

pub mod janitor;
pub mod tempdir;

/// A managed temp tree for the gate tests, shared so both gate test
/// modules build theirs the same way.
///
/// The name carries the `mediapm-` prefix on purpose: a leaked scratch
/// tree then looks like an ordinary leftover to the janitor, which is the
/// gate under test, instead of an orphan no sweep can identify.
#[cfg(test)]
pub(crate) mod test_scratch {
    use std::fs;
    use std::ops::Deref;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A scratch tree that removes itself when the test ends.
    ///
    /// Derefs to [`Path`] so callers read as if [`scratch`] handed back one.
    pub(crate) struct Scratch(PathBuf);

    impl Deref for Scratch {
        type Target = Path;

        fn deref(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for Scratch {
        /// Best-effort removal. A tree that survives a failed cleanup is
        /// still `mediapm-`prefixed, so the janitor reclaims it instead of
        /// it becoming an unexplained leftover.
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    /// Creates a unique scratch tree under the managed temp prefix.
    ///
    /// `tag` names the test, so a directory left behind by a failing run
    /// says which test left it. The pid and a counter keep two runs apart.
    pub(crate) fn scratch(tag: &str) -> Scratch {
        static SEQ: AtomicU32 = AtomicU32::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "mediapm-test-runner-gate-{}-{}-{seq}",
            std::process::id(),
            tag
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("create scratch root");
        Scratch(dir)
    }
}
