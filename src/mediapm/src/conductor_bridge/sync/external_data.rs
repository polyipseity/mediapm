//! Decoupled external_data usage tracker.
//!
//! Tracks which CAS hashes are referenced by tool content_map entries.
//! Produces the exact external_data map for `conductor.generated.ncl`,
//! satisfying the `content_map ⊆ external_data` invariant.
//!
//! Pure data structure — no I/O, no caching, no subsystem coupling.
//! `Send + Sync` by construction.

use std::collections::BTreeMap;

use mediapm_cas::Hash;
use mediapm_conductor::config::ExternalDataEntry;
use mediapm_conductor::state::OutputSaveMode;

/// Tracks CAS hash usage across managed tool content maps.
///
/// Each call to [`record`](Self::record) adds a description entry for the
/// given hash.  Multiple descriptions for the same hash are accumulated
/// and joined at [`finalize`](Self::finalize) time.  The tracker is consumed
/// to produce the final `external_data` map.
#[allow(dead_code)]
pub(crate) struct DataUsageTracker {
    usages: BTreeMap<Hash, Vec<String>>,
}

#[allow(dead_code)]
impl DataUsageTracker {
    /// Creates an empty tracker.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self { usages: BTreeMap::new() }
    }

    /// Record one usage of a CAS hash with a description fragment.
    ///
    /// Multiple calls with the same hash accumulate descriptions.
    pub(crate) fn record(&mut self, hash: Hash, description: impl Into<String>) {
        self.usages.entry(hash).or_default().push(description.into());
    }

    /// Remove one usage reference.
    ///
    /// If the hash has no remaining usages after removal, it will be absent
    /// from the final [`finalize`](Self::finalize) output.  No-op on
    /// non-existent hash.
    pub(crate) fn remove(&mut self, hash: &Hash) {
        if let std::collections::btree_map::Entry::Occupied(mut entry) = self.usages.entry(*hash) {
            entry.get_mut().pop();
            if entry.get().is_empty() {
                entry.remove();
            }
        }
    }

    /// Consume the tracker and produce the `external_data` map.
    ///
    /// Descriptions for the same hash are joined with `"; "` separator.
    /// `save_mode` is always [`OutputSaveMode::Saved`] for managed tool
    /// content.
    #[must_use]
    pub(crate) fn finalize(self) -> BTreeMap<Hash, ExternalDataEntry> {
        self.usages
            .into_iter()
            .map(|(hash, descs)| {
                let description = if descs.len() == 1 {
                    descs.into_iter().next().unwrap()
                } else {
                    descs.join("; ")
                };
                (hash, ExternalDataEntry { description, save_mode: OutputSaveMode::Saved })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_tracker_produces_empty_map() {
        let tracker = DataUsageTracker::new();
        let map = tracker.finalize();
        assert!(map.is_empty());
    }

    #[test]
    fn single_hash_record_produces_single_entry() {
        let mut tracker = DataUsageTracker::new();
        let hash = Hash::from([0u8; 32]);
        tracker.record(hash, "tool content root");
        let map = tracker.finalize();
        assert_eq!(map.len(), 1);
        let entry = map.get(&hash).expect("hash should exist");
        assert_eq!(entry.description, "tool content root");
        assert_eq!(entry.save_mode, OutputSaveMode::Saved);
    }

    #[test]
    fn multiple_descriptions_for_same_hash_are_joined() {
        let mut tracker = DataUsageTracker::new();
        let hash = Hash::from([0u8; 32]);
        tracker.record(hash, "first description");
        tracker.record(hash, "second description");
        let map = tracker.finalize();
        assert_eq!(map.len(), 1);
        let entry = map.get(&hash).expect("hash should exist");
        assert_eq!(entry.description, "first description; second description");
    }

    #[test]
    fn different_hashes_produce_separate_entries() {
        let mut tracker = DataUsageTracker::new();
        let hash_a = Hash::from([0u8; 32]);
        let hash_b = Hash::from([1u8; 32]);
        tracker.record(hash_a, "tool A");
        tracker.record(hash_b, "tool B");
        let map = tracker.finalize();
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn remove_removes_one_usage_reference() {
        let mut tracker = DataUsageTracker::new();
        let hash = Hash::from([0u8; 32]);
        tracker.record(hash, "first");
        tracker.record(hash, "second");
        tracker.remove(&hash);
        let map = tracker.finalize();
        assert_eq!(map.len(), 1);
        let entry = map.get(&hash).expect("hash should still exist");
        assert_eq!(entry.description, "first");
    }

    #[test]
    fn remove_last_usage_removes_hash() {
        let mut tracker = DataUsageTracker::new();
        let hash = Hash::from([0u8; 32]);
        tracker.record(hash, "only");
        tracker.remove(&hash);
        let map = tracker.finalize();
        assert!(map.is_empty());
    }

    #[test]
    fn remove_non_existent_hash_is_noop() {
        let mut tracker = DataUsageTracker::new();
        let hash = Hash::from([0u8; 32]);
        tracker.remove(&hash);
        let map = tracker.finalize();
        assert!(map.is_empty());
    }

    #[test]
    fn send_sync_trait_bounds() {
        fn assert_send<T: Send>(_t: &T) {}
        fn assert_sync<T: Sync>(_t: &T) {}

        let tracker = DataUsageTracker::new();
        assert_send(&tracker);
        assert_sync(&tracker);
    }
}
