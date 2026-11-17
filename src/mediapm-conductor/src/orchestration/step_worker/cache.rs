//! Cache-probe logic: whether a recorded tool-call instance can serve a step.
//!
//! Instance keys are derived in [`crate::instance_key`], from inputs alone. This
//! module answers the question that key opens: an instance exists under it, so
//! may the step that asked reuse it? The answer is a pure function of recorded
//! state — what the step binds, and how each recorded entry was captured — and
//! the executor pairs it with its own CAS presence check.

use std::collections::BTreeMap;

use mediapm_cas::Hash;

use crate::config::OutputCaptureSpec;
use crate::orchestration::protocol::ConductorState;
use crate::state::ToolCallInstance;

/// Returns whether a recorded capture names the same bytes a required capture
/// names.
///
/// v6 records capture identity as a token string (`capture`) and the retention
/// fact as `saved_nk`; `hash_nk: None` means an allowed-empty capture matched
/// nothing. A `None` hash satisfies only a requirement whose declared
/// `allow_empty` is true. `saved_nk` is not compared (persistence, not bytes).
pub(super) fn probe_cache(
    instance_key: &Hash,
    state: &ConductorState,
    required: &BTreeMap<String, OutputCaptureSpec>,
) -> Option<ToolCallInstance> {
    let instance = state.tool_call_instances.get(instance_key)?;
    if instance.invalidated_at_nk.is_some() {
        return None;
    }
    let satisfies_requirements = required.iter().all(|(name, spec)| {
        instance.outputs.get(name).is_some_and(|recorded| {
            if recorded.capture != spec.capture.to_token() {
                return false;
            }
            // A null hash means the capture matched nothing and was allowed to be
            // empty; it can only serve a requirement that also allows empty.
            if recorded.hash_nk.is_none() {
                return spec.allow_empty;
            }
            true
        })
    });
    satisfies_requirements.then(|| instance.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SaveMode;
    use crate::state::{CapturedOutput, ToolCallInstance};
    use mediapm_utils::Timestamp;
    use std::collections::BTreeSet;

    /// Builds a capture spec under `name` with every optional field at its
    /// documented default, so a test varies only the selector.
    fn spec(name: &str, capture: &str) -> OutputCaptureSpec {
        OutputCaptureSpec {
            name: name.to_string(),
            capture: crate::config::CaptureKind::parse_token(capture).expect("valid capture"),
            ..OutputCaptureSpec::default()
        }
    }

    fn recorded_output(spec: &OutputCaptureSpec, hash: Hash, saved_nk: bool) -> CapturedOutput {
        CapturedOutput { capture: spec.capture.to_token(), hash_nk: Some(hash), saved_nk }
    }

    /// Builds a one-instance state keyed by `key`, whose outputs are `outputs`.
    fn state_with(key: Hash, outputs: BTreeMap<String, CapturedOutput>) -> ConductorState {
        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(
            key,
            ToolCallInstance {
                key_version: 8,
                attempted_key_version_nk: 8,
                tool_call_id: "echo@v1".to_string(),
                impure_nk: false,
                invalidated_at_nk: None,
                executed_at_nk: Timestamp::default(),
                last_referenced_at_nk: Timestamp::default(),
                step_ids_nk: BTreeSet::new(),
                command_args: Vec::new(),
                env_vars_ck: BTreeMap::new(),
                materialized_inputs: BTreeMap::new(),
                outputs,
            },
        );
        state
    }

    /// A required name recorded under the required spec is a hit.
    #[test]
    fn a_required_name_with_an_equal_spec_is_a_hit() {
        let key = Hash::from_content(b"instance");
        let content = spec("content", "stdout");
        let state = state_with(
            key,
            BTreeMap::from([(
                "content".to_string(),
                recorded_output(&content, Hash::from_content(b"bytes"), true),
            )]),
        );

        assert!(
            probe_cache(&key, &state, &BTreeMap::from([("content".to_string(), content)]))
                .is_some(),
            "an entry recorded under the required spec must serve the step",
        );
    }

    /// The same name captured another way is a miss.
    ///
    /// `content` read from a file and `content` read from a folder are not the
    /// same bytes, so the spec — not the name — decides reuse.
    #[test]
    fn a_required_name_recorded_with_a_different_spec_is_a_miss() {
        let key = Hash::from_content(b"instance");
        let recorded = spec("content", "file:out.mkv");
        let state = state_with(
            key,
            BTreeMap::from([(
                "content".to_string(),
                recorded_output(&recorded, Hash::from_content(b"bytes"), true),
            )]),
        );

        assert!(
            probe_cache(
                &key,
                &state,
                &BTreeMap::from([("content".to_string(), spec("content", "folder:outputs"))]),
            )
            .is_none(),
            "a name recorded under a different spec must be a miss",
        );
    }

    /// An instance an operator invalidated cannot serve a step, however well
    /// its recorded names match.
    ///
    /// The marker is the whole of explicit invalidation and this probe is its
    /// only reader, so a probe that ignored it would make every invalidation
    /// command a no-op that reports success.
    #[test]
    fn an_invalidated_instance_is_a_miss() {
        let key = Hash::from_content(b"instance");
        let content = spec("content", "stdout");
        let mut state = state_with(
            key,
            BTreeMap::from([(
                "content".to_string(),
                recorded_output(&content, Hash::from_content(b"bytes"), true),
            )]),
        );
        state.tool_call_instances.get_mut(&key).expect("seeded instance").invalidated_at_nk =
            Some(Timestamp::from_unix_nanos(1));

        assert!(
            probe_cache(&key, &state, &BTreeMap::from([("content".to_string(), content)]))
                .is_none(),
            "a marked instance must not serve a step",
        );
    }

    /// A marked instance is a miss even when the step binds nothing by name.
    ///
    /// An empty requirement accepts any instance under the key, which would
    /// otherwise be the one path around the marker: a step whose outputs are
    /// all derived from a sibling's would reuse invalidated work.
    #[test]
    fn an_invalidated_instance_is_a_miss_with_an_empty_requirement() {
        let key = Hash::from_content(b"instance");
        let mut state = state_with(key, BTreeMap::new());
        state.tool_call_instances.get_mut(&key).expect("seeded instance").invalidated_at_nk =
            Some(Timestamp::from_unix_nanos(1));

        assert!(
            probe_cache(&key, &state, &BTreeMap::new()).is_none(),
            "an empty requirement must not accept a marked instance",
        );
    }

    /// A required name the instance does not record is a miss.
    #[test]
    fn a_missing_required_name_is_a_miss() {
        let key = Hash::from_content(b"instance");
        let state = state_with(key, BTreeMap::new());

        assert!(
            probe_cache(
                &key,
                &state,
                &BTreeMap::from([("content".to_string(), spec("content", "stdout"))]),
            )
            .is_none(),
            "a step that binds a name the instance never recorded cannot reuse it",
        );
    }

    /// A recorded folder layout that disagrees with the required one is a miss.
    ///
    /// Folder layout is part of the capture token in v5/v6 (`folder:` vs
    /// `folder_flat:`), so different layouts produce different tokens.
    #[test]
    fn a_required_name_recorded_with_a_different_include_topmost_folder_is_a_miss() {
        let key = Hash::from_content(b"instance");
        let recorded = spec("content", "folder:outputs");
        let required = spec("content", "folder_flat:outputs");
        let state = state_with(
            key,
            BTreeMap::from([(
                "content".to_string(),
                recorded_output(&recorded, Hash::from_content(b"bytes"), true),
            )]),
        );

        assert!(
            probe_cache(&key, &state, &BTreeMap::from([("content".to_string(), required)]))
                .is_none(),
            "a name recorded under a different folder layout must be a miss",
        );
    }

    /// A recorded `hash_nk: null` (allowed-empty capture) serves only a
    /// requirement whose declared `allow_empty` is true.
    #[test]
    fn a_null_hash_serves_only_an_allow_empty_requirement() {
        let key = Hash::from_content(b"instance");
        let mut required_allow = spec("content", "folder_regex:.*");
        required_allow.allow_empty = true;
        let mut required_disallow = spec("content", "folder_regex:.*");
        required_disallow.allow_empty = false;
        let state = state_with(
            key,
            BTreeMap::from([(
                "content".to_string(),
                CapturedOutput {
                    capture: "folder_regex:.*".to_string(),
                    hash_nk: None,
                    saved_nk: false,
                },
            )]),
        );

        assert!(
            probe_cache(&key, &state, &BTreeMap::from([("content".to_string(), required_allow)]))
                .is_some(),
            "a null hash must serve an allow_empty requirement with the same token",
        );
        assert!(
            probe_cache(
                &key,
                &state,
                &BTreeMap::from([("content".to_string(), required_disallow)])
            )
            .is_none(),
            "a null hash must not serve a non-allow_empty requirement",
        );
    }

    /// A recorded entry whose only difference is `saved_nk` still serves the
    /// step.
    ///
    /// Persistence is not part of capture identity in v6; `saved_nk` is a fact
    /// and not compared by the probe.
    #[test]
    fn a_save_only_difference_still_satisfies_the_probe() {
        let persisted = OutputCaptureSpec { save: SaveMode::True, ..spec("content", "stdout") };
        let transient = OutputCaptureSpec { save: SaveMode::False, ..spec("content", "stdout") };
        let hash = Hash::from_content(b"bytes");

        let recorded_persisted = state_with(
            Hash::from_content(b"instance-1"),
            BTreeMap::from([(
                "content".to_string(),
                CapturedOutput {
                    capture: persisted.capture.to_token(),
                    hash_nk: Some(hash),
                    saved_nk: true,
                },
            )]),
        );
        let recorded_transient = state_with(
            Hash::from_content(b"instance-2"),
            BTreeMap::from([(
                "content".to_string(),
                CapturedOutput {
                    capture: transient.capture.to_token(),
                    hash_nk: Some(hash),
                    saved_nk: false,
                },
            )]),
        );

        assert!(
            probe_cache(
                &Hash::from_content(b"instance-1"),
                &recorded_persisted,
                &BTreeMap::from([("content".to_string(), transient)]),
            )
            .is_some(),
            "a step that drops a persistence decision must reuse the bytes that were persisted",
        );
        assert!(
            probe_cache(
                &Hash::from_content(b"instance-2"),
                &recorded_transient,
                &BTreeMap::from([("content".to_string(), persisted)]),
            )
            .is_some(),
            "a step that persists an artifact must reuse bytes recorded transiently",
        );
    }

    /// An instance that is not recorded under the key is a miss.
    #[test]
    fn an_unknown_key_is_a_miss() {
        let state = ConductorState::new_empty();

        assert!(
            probe_cache(
                &Hash::from_content(b"instance"),
                &state,
                &BTreeMap::from([("content".to_string(), spec("content", "stdout"))]),
            )
            .is_none(),
            "the probe must not invent an instance",
        );
    }

    /// A step that binds no output by name accepts the instance under its key.
    #[test]
    fn an_empty_requirement_accepts_the_instance() {
        let key = Hash::from_content(b"instance");
        let state = state_with(key, BTreeMap::new());

        assert!(
            probe_cache(&key, &state, &BTreeMap::new()).is_some(),
            "nothing to require means nothing to refuse",
        );
    }
}
