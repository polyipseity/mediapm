//! Multi-item progress budget for batch operations.

use std::sync::atomic::{AtomicU64, Ordering};

/// Thread-safe collection of per-item progress budgets.
///
/// Each item tracks `(position, total)` as a pair of [`AtomicU64`] values.
/// The aggregate progress is the sum of all items' positions and totals.
///
/// # Invariants (hard-fail with `assert!`)
///
/// - `pos ≤ total` per item — enforced on every mutation.
/// - `pos` never decreases per item.
/// - `total` is set once per item (at construction via [`add_item`](Self::add_item)
///   or dynamically via [`set_total`](Self::set_total)).
/// - Items with `total == 0` are considered indeterminate — counted in
///   [`item_count`](Self::item_count) but contribute 0 bytes to aggregate totals.
#[derive(Debug)]
pub struct MultiItemBudget {
    items: Vec<ItemBudget>,
}

#[derive(Debug)]
struct ItemBudget {
    pos: AtomicU64,
    total: AtomicU64,
}

impl MultiItemBudget {
    /// Create a new empty budget.
    #[must_use]
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    /// Create a budget pre-allocated for `capacity` items.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self { items: Vec::with_capacity(capacity) }
    }

    /// Add one item with the given `total`.
    pub fn add_item(&mut self, total: u64) {
        self.items.push(ItemBudget { pos: AtomicU64::new(0), total: AtomicU64::new(total) });
    }

    /// Number of items.
    #[must_use]
    pub fn item_count(&self) -> usize {
        self.items.len()
    }

    /// Set total for the item at `item_idx`.
    ///
    /// # Panics
    ///
    /// Panics if `item_idx` is out of bounds, or if `total < current position`.
    pub fn set_total(&self, item_idx: usize, total: u64) {
        let item = &self.items[item_idx];
        let pos = item.pos.load(Ordering::Acquire);
        assert!(
            pos <= total,
            "MultiItemBudget::set_total({item_idx}, {total}) < current pos {pos}"
        );
        item.total.store(total, Ordering::Release);
    }

    /// Advance position for item at `item_idx` by `amount`.
    ///
    /// Uses a `compare_exchange_weak` loop for thread safety.
    ///
    /// # Panics
    ///
    /// Panics if `item_idx` is out of bounds, or if `pos + amount > total`.
    pub fn advance(&self, item_idx: usize, amount: u64) {
        let item = &self.items[item_idx];
        let mut old = item.pos.load(Ordering::Acquire);
        loop {
            let new = old + amount;
            let total = item.total.load(Ordering::Acquire);
            assert!(
                new <= total,
                "MultiItemBudget::advance({item_idx}, {amount}) would exceed total {total}"
            );
            match item.pos.compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return,
                Err(actual) => old = actual,
            }
        }
    }

    /// Set absolute position for item at `item_idx`.
    ///
    /// Single load-store (no loop — assumes sequential completion).
    ///
    /// # Panics
    ///
    /// Panics if `item_idx` is out of bounds, `pos > total`, or `pos < current position`.
    pub fn set_pos(&self, item_idx: usize, pos: u64) {
        let item = &self.items[item_idx];
        let total = item.total.load(Ordering::Acquire);
        assert!(pos <= total, "MultiItemBudget::set_pos({item_idx}, {pos}) > total {total}");
        let current = item.pos.load(Ordering::Acquire);
        assert!(pos >= current, "MultiItemBudget::set_pos({item_idx}, {pos}) < current {current}");
        item.pos.store(pos, Ordering::Release);
    }

    /// Snapshot `(pos, total)` for item at `item_idx`.
    ///
    /// # Panics
    ///
    /// Panics if `item_idx` is out of bounds.
    #[must_use]
    pub fn snap(&self, item_idx: usize) -> (u64, u64) {
        let item = &self.items[item_idx];
        (item.pos.load(Ordering::Acquire), item.total.load(Ordering::Acquire))
    }

    /// Aggregate of all items: `(sum_pos, sum_total)`.
    ///
    /// Items with `total == 0` are indeterminate and contribute 0.
    #[must_use]
    pub fn aggregate(&self) -> (u64, u64) {
        let mut sum_pos = 0u64;
        let mut sum_total = 0u64;
        for item in &self.items {
            sum_pos = sum_pos.saturating_add(item.pos.load(Ordering::Acquire));
            sum_total = sum_total.saturating_add(item.total.load(Ordering::Acquire));
        }
        (sum_pos, sum_total)
    }
}

impl Default for MultiItemBudget {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn new_creates_empty() {
        let b = MultiItemBudget::new();
        assert_eq!(b.item_count(), 0);
        assert_eq!(b.aggregate(), (0, 0));
    }

    #[test]
    fn with_capacity_pre_allocates() {
        let b = MultiItemBudget::with_capacity(10);
        assert_eq!(b.item_count(), 0);
    }

    #[test]
    fn add_item_increases_count() {
        let mut b = MultiItemBudget::new();
        b.add_item(100);
        assert_eq!(b.item_count(), 1);
        b.add_item(200);
        assert_eq!(b.item_count(), 2);
    }

    #[test]
    fn add_item_sets_initial_total() {
        let mut b = MultiItemBudget::new();
        b.add_item(42);
        assert_eq!(b.snap(0), (0, 42));
    }

    #[test]
    fn item_count_reflects_adds() {
        let mut b = MultiItemBudget::new();
        assert_eq!(b.item_count(), 0);
        b.add_item(10);
        assert_eq!(b.item_count(), 1);
        b.add_item(20);
        b.add_item(30);
        assert_eq!(b.item_count(), 3);
    }

    #[test]
    fn set_total_updates_item_total() {
        let mut b = MultiItemBudget::new();
        b.add_item(100);
        b.set_total(0, 250);
        assert_eq!(b.snap(0), (0, 250));
    }

    #[test]
    #[should_panic(expected = "< current pos")]
    fn set_total_panics_below_position() {
        let mut b = MultiItemBudget::new();
        b.add_item(100);
        b.advance(0, 50);
        b.set_total(0, 30);
    }

    #[test]
    fn advance_increases_position() {
        let mut b = MultiItemBudget::new();
        b.add_item(100);
        b.advance(0, 30);
        assert_eq!(b.snap(0), (30, 100));
        b.advance(0, 20);
        assert_eq!(b.snap(0), (50, 100));
    }

    #[test]
    fn advance_multiple_items_independently() {
        let mut b = MultiItemBudget::new();
        b.add_item(100);
        b.add_item(200);
        b.advance(0, 10);
        b.advance(1, 20);
        assert_eq!(b.snap(0), (10, 100));
        assert_eq!(b.snap(1), (20, 200));
    }

    #[test]
    #[should_panic(expected = "would exceed total")]
    fn advance_panics_on_overflow() {
        let mut b = MultiItemBudget::new();
        b.add_item(100);
        b.advance(0, 101);
    }

    #[test]
    fn set_pos_works() {
        let mut b = MultiItemBudget::new();
        b.add_item(100);
        b.set_pos(0, 50);
        assert_eq!(b.snap(0), (50, 100));
    }

    #[test]
    #[should_panic(expected = "> total")]
    fn set_pos_panics_on_exceed_total() {
        let mut b = MultiItemBudget::new();
        b.add_item(100);
        b.set_pos(0, 101);
    }

    #[test]
    #[should_panic(expected = "< current")]
    fn set_pos_panics_on_decrease() {
        let mut b = MultiItemBudget::new();
        b.add_item(100);
        b.set_pos(0, 50);
        b.set_pos(0, 30);
    }

    #[test]
    fn snap_returns_item_state() {
        let mut b = MultiItemBudget::new();
        b.add_item(100);
        assert_eq!(b.snap(0), (0, 100));
        b.advance(0, 42);
        assert_eq!(b.snap(0), (42, 100));
    }

    #[test]
    #[should_panic(expected = "index out of bounds")]
    fn snap_panics_on_bad_index() {
        let b = MultiItemBudget::new();
        let _ = b.snap(0);
    }

    #[test]
    fn aggregate_sums_all_items() {
        let mut b = MultiItemBudget::new();
        b.add_item(100);
        b.add_item(200);
        b.add_item(300);
        b.advance(0, 10);
        b.advance(1, 20);
        b.advance(2, 30);
        assert_eq!(b.aggregate(), (60, 600));
    }

    #[test]
    fn aggregate_indeterminate_items_contribute_zero() {
        let mut b = MultiItemBudget::new();
        b.add_item(0); // indeterminate
        b.add_item(100);
        b.advance(1, 50);
        assert_eq!(b.aggregate(), (50, 100));
    }

    #[test]
    fn default_is_empty() {
        let b = MultiItemBudget::default();
        assert_eq!(b.item_count(), 0);
        assert_eq!(b.aggregate(), (0, 0));
    }

    #[test]
    fn concurrent_read_write_no_data_races() {
        // Pre-populate then wrap in Arc
        let mut inner = MultiItemBudget::new();
        inner.add_item(1000);
        inner.add_item(1000);
        let b = Arc::new(inner);
        let b_clone = Arc::clone(&b);
        let writer = thread::spawn(move || {
            for _ in 0..100 {
                b_clone.advance(0, 5);
            }
        });
        let b_clone2 = Arc::clone(&b);
        let reader = thread::spawn(move || {
            for _ in 0..100 {
                let (_pos, total) = b_clone2.snap(1);
                assert!(total == 1000 || total == 1500);
            }
        });
        writer.join().unwrap();
        reader.join().unwrap();
        assert_eq!(b.snap(0), (500, 1000));
    }

    #[test]
    fn send_sync_trait_bounds() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<MultiItemBudget>();
        assert_sync::<MultiItemBudget>();
    }
}
