//! Decoupled external_data usage tracker.
//!
//! Tracks which CAS hashes are referenced by tool content_map entries and by
//! workflow steps. Produces the exact external_data map for
//! `conductor.generated.ncl`, satisfying the `content_map ⊆ external_data`
//! invariant and the step-reference rule beside it.
//!
//! Pure data structure — no I/O, no caching, no subsystem coupling.
//! `Send + Sync` by construction.

use std::collections::{BTreeMap, BTreeSet};

use mediapm_cas::Hash;
use mediapm_conductor::config::ExternalDataEntry;
use mediapm_conductor::config::documents::external_data_reference_hashes;
use mediapm_conductor::state::OutputSaveMode;
use mediapm_conductor::{InputBinding, NickelDocument, ToolKindSpec};

/// Collects every CAS hash the documents of `document` name.
///
/// Every surface the template engine interpolates is scanned, because each one
/// can name bytes the conductor has to keep alive:
///
/// - a step input that is a bare hash, which the `import` builtin reads under
///   `kind=cas_hash`;
/// - a `${external_data.<hash>}` reference in a step input, in a tool command
///   part, or in a tool default input, which the engine resolves out of CAS.
///   The reference is found by the conductor's scanner, which is the resolver's
///   own parse, so a reference that carries post-processing selectors and one
///   inside a conditional branch both count;
/// - the same references in a step output capture, which the engine never
///   interpolates. Declaring a hash nothing reads costs retention; leaving one
///   undeclared costs the payload, so the scan stays liberal here.
///
/// A value that names no hash is skipped: a path, a URL, or a reference the
/// engine would reject anyway is not this pass's verdict to make.
#[must_use]
pub(crate) fn referenced_hashes(document: &NickelDocument) -> BTreeSet<Hash> {
    let mut hashes = BTreeSet::new();
    for workflow in &document.workflows {
        for step in &workflow.steps {
            for value in step.inputs.values() {
                collect_input_hash(value, &mut hashes);
            }
            for capture in step.outputs.values() {
                hashes.extend(external_data_reference_hashes(&capture.capture));
            }
        }
    }
    for spec in document.tools.values() {
        if let ToolKindSpec::Executable { command, .. } = &spec.kind {
            for part in command {
                hashes.extend(external_data_reference_hashes(part));
            }
        }
        for binding in spec.default_inputs.values() {
            match binding {
                InputBinding::String(value) => collect_input_hash(value, &mut hashes),
                InputBinding::Vec(values) => {
                    for value in values {
                        collect_input_hash(value, &mut hashes);
                    }
                }
            }
        }
    }
    hashes
}

/// Collects the hash one input value names, in either shape an input can name
/// one.
fn collect_input_hash(value: &str, hashes: &mut BTreeSet<Hash>) {
    hashes.extend(external_data_reference_hashes(value));
    if let Ok(hash) = value.trim().parse::<Hash>() {
        hashes.insert(hash);
    }
}

/// Tracks CAS hash usage across managed tool content maps.
///
/// Each call to [`record`](Self::record) adds a description entry for the
/// given hash.  Multiple descriptions for the same hash are accumulated
/// and joined at [`finalize`](Self::finalize) time.  The tracker is consumed
/// to produce the final `external_data` map.
pub(crate) struct DataUsageTracker {
    usages: BTreeMap<Hash, Vec<String>>,
}

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
    ///
    /// Compiled only under `cfg(test)`: `record` is append-only in production,
    /// so no code path ever retracts a recorded usage.
    #[cfg(test)]
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
                (
                    hash,
                    ExternalDataEntry {
                        description: Some(description),
                        save_mode: OutputSaveMode::Saved,
                    },
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Wraps one workflow with one step whose inputs and capture carry `values`.
    fn document_with_step(inputs: BTreeMap<String, String>, capture: &str) -> NickelDocument {
        let mut document = NickelDocument::default();
        let step = mediapm_conductor::WorkflowStepSpec {
            id: "step-0".to_string(),
            tool: "import".to_string(),
            inputs,
            outputs: BTreeMap::from([(
                "primary".to_string(),
                mediapm_conductor::OutputCaptureSpec {
                    name: "primary".to_string(),
                    capture: capture.to_string(),
                    save: mediapm_conductor::SaveMode::True,
                    allow_empty: false,
                    include_topmost_folder: true,
                },
            )]),
            max_retries: 0,
            depends_on: Vec::new(),
        };
        document.workflows.push(mediapm_conductor::WorkflowSpec {
            name: "mediapm.media.probe".to_string(),
            steps: vec![step],
            ..mediapm_conductor::WorkflowSpec::default()
        });
        document
    }

    /// A bare hash input is a reference: the import builtin reads it as its
    /// payload, so nothing else in the document names those bytes.
    #[test]
    fn a_bare_hash_input_is_referenced() {
        let hash = Hash::from([7u8; 32]);
        let document =
            document_with_step(BTreeMap::from([("hash".to_string(), hash.to_string())]), "stdout");

        assert_eq!(referenced_hashes(&document), BTreeSet::from([hash]));
    }

    /// An `${external_data.<hash>}` expression in an input or a capture is a
    /// reference, wherever in the string it appears.
    #[test]
    fn external_data_expressions_are_referenced() {
        let input_hash = Hash::from([8u8; 32]);
        let capture_hash = Hash::from([9u8; 32]);
        let document = document_with_step(
            BTreeMap::from([
                ("input".to_string(), format!("${{external_data.{input_hash}}}")),
                ("nested".to_string(), format!("prefix ${{external_data.{input_hash}}} suffix")),
            ]),
            &format!("${{external_data.{capture_hash}}}"),
        );

        assert_eq!(
            referenced_hashes(&document),
            BTreeSet::from([input_hash, capture_hash]),
            "a reference in either position counts once, however it is embedded",
        );
    }

    /// A reference that carries a post-processing selector names the hash the
    /// engine reads out of CAS, not the selector text that follows it.
    #[test]
    fn a_selector_carrying_reference_is_referenced() {
        let hash = Hash::from([10u8; 32]);
        let document = document_with_step(
            BTreeMap::from([(
                "input".to_string(),
                format!("${{external_data.{hash}:file(payload.bin)}}"),
            )]),
            "stdout",
        );

        assert_eq!(referenced_hashes(&document), BTreeSet::from([hash]));
    }

    /// A reference in a tool command part names bytes the engine interpolates
    /// into the command line, so the command's tool is a consumer too.
    #[test]
    fn a_command_part_reference_is_referenced() {
        let hash = Hash::from([11u8; 32]);
        let mut document = NickelDocument::default();
        document.tools.insert(
            "probe".to_string(),
            mediapm_conductor::ToolSpec {
                name: "probe".to_string(),
                version: Some("1".to_string()),
                kind: ToolKindSpec::Executable {
                    command: vec![format!("${{external_data.{hash}}}")],
                    env_vars: BTreeMap::new(),
                    success_codes: vec![0],
                },
                ..mediapm_conductor::ToolSpec::default()
            },
        );

        assert_eq!(referenced_hashes(&document), BTreeSet::from([hash]));
    }

    /// A padded hash input names the same object as an unpadded one, so the two
    /// sides of the invariant agree on it.
    #[test]
    fn a_padded_hash_input_is_referenced() {
        let hash = Hash::from([12u8; 32]);
        let document = document_with_step(
            BTreeMap::from([("hash".to_string(), format!("  {hash}\n"))]),
            "stdout",
        );

        assert_eq!(referenced_hashes(&document), BTreeSet::from([hash]));
    }

    /// A value that is not a hash is not a reference.
    #[test]
    fn values_that_name_no_object_are_ignored() {
        let document = document_with_step(
            BTreeMap::from([
                ("uri".to_string(), "https://example.com/video".to_string()),
                ("path".to_string(), "output-0.m4a".to_string()),
            ]),
            "file_regex:^output-0(?:[.][^/\\\\]+)?$",
        );

        assert!(referenced_hashes(&document).is_empty());
    }

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
        assert_eq!(entry.description.as_deref(), Some("tool content root"));
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
        assert_eq!(entry.description.as_deref(), Some("first description; second description"));
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
        assert_eq!(entry.description.as_deref(), Some("first"));
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
