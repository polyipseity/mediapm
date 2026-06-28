//! Verify-on-read strategy evaluation for CAS blob stores.
//!
//! Provides [`VerifyEvaluator`] which manages per-strategy state (mtime
//! tracking for Modified, timestamps for Stale, counter for Sample) and
//! exposes a single entry point for deciding whether to verify a blob's
//! content hash on read.
//!
//! # Strategy semantics
//!
//! Strategies are evaluated with **OR** semantics: if any strategy
//! triggers, the evaluator returns `true`.
//!
//! | Strategy  | Trigger condition |
//! |-----------|-------------------|
//! | `Always`  | Every read. |
//! | `Modified` | Hash unknown or file mtime differs from expected. |
//! | `Sample`  | Counter modulo `denominator` is zero. |
//! | `Stale`   | Never verified or time since last verification exceeds timeout. |

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use dashmap::DashMap;

use crate::api::VerifyTriggerStrategy;
use crate::hash::Hash;

/// Evaluates verify-on-read strategies with OR semantics.
///
/// If any enabled strategy triggers verification, [`should_verify`] returns
/// `true`. After a successful verification (or a write), call
/// [`record_verification`] to update tracking state so subsequent reads can
/// make accurate decisions.
///
/// ## State lifetime
///
/// All per-hash state is ephemeral (in-memory). On process restart, every
/// hash is unknown to the evaluator — the first read of each hash will
/// conservatively trigger verification for `Modified` and `Stale`.
pub(crate) struct VerifyEvaluator {
    always_enabled: bool,
    modified_enabled: bool,
    sample_enabled: bool,
    stale_enabled: bool,
    sample_denominator: u32,
    stale_timeout: Duration,
    /// Per-hash expected mtime for Modified strategy.
    modified_map: DashMap<Hash, SystemTime>,
    /// Per-hash last-verified timestamp for Stale strategy.
    stale_map: DashMap<Hash, Instant>,
    /// Shared counter for Sample strategy.
    sample_counter: AtomicU64,
}

impl VerifyEvaluator {
    /// Create a new evaluator from a list of strategies.
    ///
    /// Extracts `Sample` and `Stale` parameters from the list. Duplicate
    /// entries are harmless — flags are booleans and params are overwritten
    /// by the last occurrence.
    #[must_use]
    pub(crate) fn new(strategies: Vec<VerifyTriggerStrategy>) -> Self {
        let mut always_enabled = false;
        let mut modified_enabled = false;
        let mut sample_enabled = false;
        let mut stale_enabled = false;
        let mut sample_denominator: u32 = 0;
        let mut stale_timeout = Duration::ZERO;

        for s in &strategies {
            match s {
                VerifyTriggerStrategy::Always => always_enabled = true,
                VerifyTriggerStrategy::Modified => modified_enabled = true,
                VerifyTriggerStrategy::Sample { denominator } => {
                    sample_enabled = true;
                    sample_denominator = *denominator;
                }
                VerifyTriggerStrategy::Stale { timeout } => {
                    stale_enabled = true;
                    stale_timeout = *timeout;
                }
            }
        }

        Self {
            always_enabled,
            modified_enabled,
            sample_enabled,
            stale_enabled,
            sample_denominator,
            stale_timeout,
            modified_map: DashMap::new(),
            stale_map: DashMap::new(),
            sample_counter: AtomicU64::new(0),
        }
    }

    /// Returns `true` if the read should verify the content hash.
    ///
    /// `file_mtime` is the file's modification time (if available). Used by
    /// the `Modified` strategy to detect external tampering.
    #[must_use]
    pub(crate) fn should_verify(&self, hash: &Hash, file_mtime: Option<SystemTime>) -> bool {
        // Sample: increment counter on every call regardless of other
        // strategies.  The result is checked at the end so early returns from
        // Always / Modified / Stale do not skip the increment.
        let sample_triggers = if self.sample_enabled {
            let count = self.sample_counter.fetch_add(1, Ordering::Relaxed);
            count % u64::from(self.sample_denominator) == 0
        } else {
            false
        };

        // Always: shortest path.
        if self.always_enabled {
            return true;
        }

        // Modified: verify if mtime changed or hash is unknown.
        if self.modified_enabled {
            match self.modified_map.get(hash) {
                Some(expected_mtime) => {
                    if let Some(actual_mtime) = file_mtime {
                        if actual_mtime == *expected_mtime {
                            // mtime matches — no verification needed.
                        } else {
                            return true;
                        }
                    } else {
                        // No mtime available — conservative: verify.
                        return true;
                    }
                }
                None => {
                    // Hash not tracked (first read or restart) — verify.
                    return true;
                }
            }
        }

        // Stale: verify if never verified or last verification is too old.
        if self.stale_enabled {
            match self.stale_map.get(hash) {
                Some(last_verified) => {
                    if last_verified.elapsed() >= self.stale_timeout {
                        return true;
                    }
                }
                None => {
                    // Never verified this session — verify.
                    return true;
                }
            }
        }

        // Fall through to sample.
        sample_triggers
    }

    /// Record that `hash` was just verified (or written).
    ///
    /// Call this after a successful verification or after writing a blob to
    /// seed the tracking maps for `Modified` and `Stale` strategies.
    pub(crate) fn record_verification(&self, hash: &Hash, file_mtime: Option<SystemTime>) {
        if self.modified_enabled {
            if let Some(mtime) = file_mtime {
                self.modified_map.insert(*hash, mtime);
            }
        }
        if self.stale_enabled {
            self.stale_map.insert(*hash, Instant::now());
        }
    }
}

impl Clone for VerifyEvaluator {
    fn clone(&self) -> Self {
        Self {
            always_enabled: self.always_enabled,
            modified_enabled: self.modified_enabled,
            sample_enabled: self.sample_enabled,
            stale_enabled: self.stale_enabled,
            sample_denominator: self.sample_denominator,
            stale_timeout: self.stale_timeout,
            modified_map: self.modified_map.clone(),
            stale_map: self.stale_map.clone(),
            sample_counter: AtomicU64::new(self.sample_counter.load(Ordering::Relaxed)),
        }
    }
}

impl fmt::Debug for VerifyEvaluator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifyEvaluator")
            .field("always_enabled", &self.always_enabled)
            .field("modified_enabled", &self.modified_enabled)
            .field("sample_enabled", &self.sample_enabled)
            .field("stale_enabled", &self.stale_enabled)
            .field("sample_denominator", &self.sample_denominator)
            .field("stale_timeout", &self.stale_timeout)
            .field("modified_map", &self.modified_map)
            .field("stale_map", &self.stale_map)
            .field("sample_counter", &self.sample_counter.load(Ordering::Relaxed))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_strategies_never_verifies() {
        let eval = VerifyEvaluator::new(vec![]);
        let hash = Hash::from_content(b"test");
        assert!(!eval.should_verify(&hash, None));
    }

    #[test]
    fn always_triggers_every_read() {
        let eval = VerifyEvaluator::new(vec![VerifyTriggerStrategy::Always]);
        let hash = Hash::from_content(b"test");
        assert!(eval.should_verify(&hash, None));
        assert!(eval.should_verify(&hash, None));
    }

    #[test]
    fn modified_triggers_on_first_read() {
        let eval = VerifyEvaluator::new(vec![VerifyTriggerStrategy::Modified]);
        let hash = Hash::from_content(b"test");
        let mtime = SystemTime::now();
        assert!(eval.should_verify(&hash, Some(mtime)));
    }

    #[test]
    fn modified_skips_on_unchanged_mtime() {
        let eval = VerifyEvaluator::new(vec![VerifyTriggerStrategy::Modified]);
        let hash = Hash::from_content(b"test");
        let mtime = SystemTime::now();
        // First read: triggers because hash unknown.
        assert!(eval.should_verify(&hash, Some(mtime)));
        eval.record_verification(&hash, Some(mtime));
        // Second read: mtime matches, skip.
        assert!(!eval.should_verify(&hash, Some(mtime)));
    }

    #[test]
    fn modified_triggers_on_changed_mtime() {
        let eval = VerifyEvaluator::new(vec![VerifyTriggerStrategy::Modified]);
        let hash = Hash::from_content(b"test");
        let old_mtime = SystemTime::UNIX_EPOCH;
        let new_mtime = SystemTime::now();
        assert!(eval.should_verify(&hash, Some(new_mtime)));
        eval.record_verification(&hash, Some(new_mtime));
        // mtime changed: trigger.
        assert!(eval.should_verify(&hash, Some(old_mtime)));
    }

    #[test]
    fn modified_triggers_when_mtime_unavailable() {
        let eval = VerifyEvaluator::new(vec![VerifyTriggerStrategy::Modified]);
        let hash = Hash::from_content(b"test");
        let mtime = SystemTime::now();
        assert!(eval.should_verify(&hash, Some(mtime)));
        eval.record_verification(&hash, Some(mtime));
        // Then read with None mtime: conservative, verify.
        assert!(eval.should_verify(&hash, None));
    }

    #[test]
    fn sample_triggers_every_nth_read() {
        let eval = VerifyEvaluator::new(vec![VerifyTriggerStrategy::Sample { denominator: 5 }]);
        let hash = Hash::from_content(b"test");
        // Counter starts at 0: 0 % 5 == 0 → verify.
        assert!(eval.should_verify(&hash, None));
        // 1 % 5 = 1 → skip
        assert!(!eval.should_verify(&hash, None));
        // 2, 3, 4 → skip
        assert!(!eval.should_verify(&hash, None));
        assert!(!eval.should_verify(&hash, None));
        assert!(!eval.should_verify(&hash, None));
        // 5 % 5 = 0 → verify
        assert!(eval.should_verify(&hash, None));
    }

    #[test]
    fn stale_triggers_on_first_read() {
        let eval = VerifyEvaluator::new(vec![VerifyTriggerStrategy::Stale {
            timeout: Duration::from_secs(3600),
        }]);
        let hash = Hash::from_content(b"test");
        assert!(eval.should_verify(&hash, None));
    }

    #[test]
    fn stale_skips_within_timeout() {
        let eval = VerifyEvaluator::new(vec![VerifyTriggerStrategy::Stale {
            timeout: Duration::from_secs(3600),
        }]);
        let hash = Hash::from_content(b"test");
        assert!(eval.should_verify(&hash, None));
        eval.record_verification(&hash, None);
        assert!(!eval.should_verify(&hash, None));
    }

    #[test]
    fn stale_triggers_after_timeout() {
        let eval = VerifyEvaluator::new(vec![VerifyTriggerStrategy::Stale {
            timeout: Duration::from_nanos(1),
        }]);
        let hash = Hash::from_content(b"test");
        assert!(eval.should_verify(&hash, None));
        eval.record_verification(&hash, None);
        // Wait a tiny bit so the 1-ns timeout elapses.
        std::thread::sleep(Duration::from_micros(1));
        assert!(eval.should_verify(&hash, None));
    }

    #[test]
    fn combined_any_triggers() {
        let eval = VerifyEvaluator::new(vec![
            VerifyTriggerStrategy::Modified,
            VerifyTriggerStrategy::Sample { denominator: 100 },
        ]);
        let hash = Hash::from_content(b"test");
        let mtime = SystemTime::now();
        // Modified triggers (unknown hash).
        assert!(eval.should_verify(&hash, Some(mtime)));
        eval.record_verification(&hash, Some(mtime));
        // Modified skips, Sample counter is at 1, 1 % 100 ≠ 0 → skip.
        assert!(!eval.should_verify(&hash, Some(mtime)));
    }

    #[test]
    fn sample_counter_is_independent_per_evaluator() {
        let eval1 = VerifyEvaluator::new(vec![VerifyTriggerStrategy::Sample { denominator: 2 }]);
        let eval2 = VerifyEvaluator::new(vec![VerifyTriggerStrategy::Sample { denominator: 2 }]);
        let hash = Hash::from_content(b"test");
        // Each evaluator has its own counter starting at 0.
        assert!(eval1.should_verify(&hash, None)); // count=0 → verify
        assert!(eval2.should_verify(&hash, None)); // count=0 → verify (independent)
        assert!(!eval1.should_verify(&hash, None)); // count=1 → skip
        assert!(!eval2.should_verify(&hash, None)); // count=1 → skip
    }
}
