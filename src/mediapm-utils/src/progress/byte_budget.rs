//! Byte-budget progress tracking for single-file downloads.

use std::sync::atomic::{AtomicU64, Ordering};

/// Thread-safe progress size tracker.
///
/// Tracks `(position, total)` where `position ≤ total` at all times. Both
/// fields use [`AtomicU64`] internally, making this type [`Send`] + [`Sync`]
/// without external locking. Safe to read from one thread (progress bar
/// renderer) while writing from another (download worker).
///
/// # Invariants (hard-fail with `assert!`)
///
/// - `pos ≤ total` — enforced on every mutation.
/// - `pos` never decreases.
/// - `total` may increase or decrease (via [`adjust`](Self::adjust) or
///   [`reconcile`](Self::reconcile)).
#[derive(Debug)]
pub struct ByteBudget {
    pos: AtomicU64,
    total: AtomicU64,
}

impl ByteBudget {
    /// Create a new budget with `pos = 0` and `total = initial_total`.
    #[must_use]
    pub fn new(initial_total: u64) -> Self {
        Self { pos: AtomicU64::new(0), total: AtomicU64::new(initial_total) }
    }

    /// Current position.
    #[must_use]
    pub fn pos(&self) -> u64 {
        self.pos.load(Ordering::Acquire)
    }

    /// Current total.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.total.load(Ordering::Acquire)
    }

    /// Snapshot `(pos, total)`.
    #[must_use]
    pub fn snap(&self) -> (u64, u64) {
        (self.pos(), self.total())
    }

    /// Advance position by `amount`.
    ///
    /// Uses a `compare_exchange_weak` loop for thread safety.
    ///
    /// # Panics
    ///
    /// Panics (hard `assert!`) if `pos + amount > total`.
    pub fn advance(&self, amount: u64) {
        let mut old = self.pos.load(Ordering::Acquire);
        loop {
            let new = old + amount;
            let total = self.total.load(Ordering::Acquire);
            assert!(new <= total, "ByteBudget::advance({amount}) would exceed total {total}");
            match self.pos.compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return,
                Err(actual) => old = actual,
            }
        }
    }

    /// Set position to an absolute value.
    ///
    /// Single load-store (no loop — assumes sequential completion).
    ///
    /// # Panics
    ///
    /// Panics (hard `assert!`) if `pos > total` or `pos < current position`.
    pub fn set_pos(&self, pos: u64) {
        let total = self.total.load(Ordering::Acquire);
        assert!(pos <= total, "ByteBudget::set_pos({pos}) > total {total}");
        let current = self.pos.load(Ordering::Acquire);
        assert!(pos >= current, "ByteBudget::set_pos({pos}) < current {current}");
        self.pos.store(pos, Ordering::Release);
    }

    /// Adjust total by `delta` (may be positive or negative).
    ///
    /// Uses a `compare_exchange_weak` loop for thread safety. Saturating
    /// arithmetic is used for extreme values near `u64::MAX`/`u64::MIN`.
    ///
    /// # Panics
    ///
    /// Panics (hard `assert!`) if `pos > new_total` after adjustment.
    pub fn adjust(&self, delta: i64) {
        let mut old = self.total.load(Ordering::Acquire);
        loop {
            let new = if delta >= 0 {
                old.saturating_add(delta.unsigned_abs())
            } else {
                old.saturating_sub(delta.unsigned_abs())
            };
            let pos = self.pos.load(Ordering::Acquire);
            assert!(
                pos <= new,
                "ByteBudget::adjust({delta}) would put total {new} below pos {pos}"
            );
            match self.total.compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return,
                Err(actual) => old = actual,
            }
        }
    }

    /// Reconcile total after learning actual cost.
    ///
    /// - If `actual > estimate`: total increases by `actual - estimate`.
    /// - If `actual < estimate`: total decreases by `estimate - actual`.
    /// - If equal: no-op.
    ///
    /// # Panics
    ///
    /// Panics (hard `assert!`) if the resulting total < current position.
    pub fn reconcile(&self, estimate: u64, actual: u64) {
        match actual.cmp(&estimate) {
            std::cmp::Ordering::Greater => {
                // actual > estimate is guaranteed by the match arm, so the
                // difference is positive and fits in i64 for all real-world
                // byte budgets (< 9.2 EB).
                self.adjust(i64::try_from(actual - estimate).expect("diff fits in i64"));
            }
            std::cmp::Ordering::Less => {
                // estimate > actual is guaranteed by the match arm, so the
                // difference is positive and fits in i64 for all real-world
                // byte budgets (< 9.2 EB).
                self.adjust(-i64::try_from(estimate - actual).expect("diff fits in i64"));
            }
            std::cmp::Ordering::Equal => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn new_sets_initial_state() {
        let b = ByteBudget::new(100);
        assert_eq!(b.pos(), 0);
        assert_eq!(b.total(), 100);
        assert_eq!(b.snap(), (0, 100));
    }

    #[test]
    fn advance_increases_position() {
        let b = ByteBudget::new(100);
        b.advance(30);
        assert_eq!(b.pos(), 30);
        b.advance(20);
        assert_eq!(b.pos(), 50);
        println!("snap: {:?}", b.snap());
    }

    #[test]
    #[should_panic(expected = "would exceed total")]
    fn advance_panics_on_overflow() {
        let b = ByteBudget::new(100);
        b.advance(101);
    }

    #[test]
    fn set_pos_works() {
        let b = ByteBudget::new(100);
        b.set_pos(50);
        assert_eq!(b.pos(), 50);
    }

    #[test]
    #[should_panic(expected = "> total")]
    fn set_pos_panics_on_exceed_total() {
        let b = ByteBudget::new(100);
        b.set_pos(101);
    }

    #[test]
    #[should_panic(expected = "< current")]
    fn set_pos_panics_on_decrease() {
        let b = ByteBudget::new(100);
        b.set_pos(50);
        b.set_pos(30);
    }

    #[test]
    fn adjust_positive_increases_total() {
        let b = ByteBudget::new(100);
        b.adjust(50);
        assert_eq!(b.total(), 150);
    }

    #[test]
    fn adjust_negative_decreases_total() {
        let b = ByteBudget::new(100);
        b.adjust(-30);
        assert_eq!(b.total(), 70);
    }

    #[test]
    #[should_panic(expected = "below pos")]
    fn adjust_negative_panics_below_pos() {
        let b = ByteBudget::new(40);
        b.advance(30);
        b.adjust(-50);
    }

    #[test]
    fn reconcile_increases_total() {
        let b = ByteBudget::new(100);
        b.reconcile(50, 100);
        assert_eq!(b.total(), 150);
    }

    #[test]
    fn reconcile_decreases_total() {
        let b = ByteBudget::new(100);
        b.reconcile(100, 50);
        assert_eq!(b.total(), 50);
    }

    #[test]
    fn reconcile_equal_is_noop() {
        let b = ByteBudget::new(100);
        b.reconcile(50, 50);
        assert_eq!(b.total(), 100);
    }

    #[test]
    fn concurrent_read_write_no_data_races() {
        let b = Arc::new(ByteBudget::new(1000));
        let b_clone = Arc::clone(&b);
        let writer = thread::spawn(move || {
            for _ in 0..100 {
                b_clone.advance(5);
            }
        });
        let b_clone2 = Arc::clone(&b);
        let reader = thread::spawn(move || {
            for _ in 0..100 {
                let (_pos, total) = b_clone2.snap();
                assert!(total == 1000 || total == 1500);
            }
        });
        writer.join().unwrap();
        reader.join().unwrap();
    }

    #[test]
    fn send_sync_trait_bounds() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<ByteBudget>();
        assert_sync::<ByteBudget>();
    }
}
