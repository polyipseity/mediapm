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
#[path = "multi_item_budget_tests.rs"]
mod tests;
