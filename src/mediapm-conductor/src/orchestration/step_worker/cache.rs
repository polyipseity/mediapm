//! Lookup and migration for tool-call instance caching.
//!
//! Implements the v8 key + v6 instance algorithm: a fast `K_latest` hit, a
//! `step_instances` map path with invalidation/work-change checks and a
//! version migration loop, and settlement-time maintenance.

use std::collections::BTreeMap;

use mediapm_cas::Hash;

use crate::config::OutputCaptureSpec;
use crate::instance_key::{
    INSTANCE_KEY_VERSION, V8KeyMaterial, derive_instance_key_for_version, migrate_terms,
};
use crate::state::{ConductorState, ToolCallInstance};

/// Current execution material needed to derive keys for lookup and migration.
#[derive(Debug, Clone)]
pub(super) struct CurrentMaterial {
    /// Key material in argv order (the stored `command_args` for a derived key).
    pub command_args: Vec<Hash>,
    /// Capture token per declared/producible output name (v8 output spec).
    pub output_tokens: BTreeMap<String, String>,
    /// Predicted staged inputs: sandbox-relative destination to staged hash (v8).
    pub materialized_inputs: BTreeMap<String, Hash>,
}

impl CurrentMaterial {
    fn v8_material(&self) -> V8KeyMaterial<'_> {
        V8KeyMaterial {
            output_tokens: &self.output_tokens,
            materialized_inputs: &self.materialized_inputs,
        }
    }

    pub(super) fn key(&self, tool_call_id: &str, version: u32) -> Option<Hash> {
        derive_instance_key_for_version(
            version,
            tool_call_id,
            &self.command_args,
            Some(&self.v8_material()),
        )
    }
}

/// Outcome of instance lookup.
#[derive(Debug)]
pub(super) enum LookupResult {
    /// A reusable instance was found (possibly migrated/re-keyed).
    Hit {
        /// Instance record to settle (may be migrated; settlement adds
        /// `last_referenced_at_nk` and the settling step id).
        instance: ToolCallInstance,
        /// Key the instance is filed under (new key when re-keyed).
        instance_key: Hash,
        /// Previous key to remove when the instance was re-keyed.
        rekeyed_from: Option<Hash>,
    },
    /// No reusable instance was found.
    Miss {
        /// Side-effect mutations to other instances (e.g. `attempted_key_version_nk`
        /// advanced on a different-content miss's mapped instance).
        side_effects: BTreeMap<Hash, ToolCallInstance>,
    },
}

/// Returns true when a recorded instance satisfies the required outputs.
///
/// Identity is the key (capture tokens are key material in v8), so the probe
/// does not compare capture tokens. A `null` record serves only a requirement
/// whose declared `allow_empty` is true.
fn satisfies_requirements(
    instance: &ToolCallInstance,
    required: &BTreeMap<String, OutputCaptureSpec>,
) -> bool {
    required.iter().all(|(name, spec)| {
        instance.outputs.get(name).is_some_and(|recorded| {
            if recorded.hash_nk.is_none() {
                return spec.allow_empty;
            }
            true
        })
    })
}

/// Attempts migration of `instance` (filed under `*key`, key version
/// `instance.key_version`) toward [`INSTANCE_KEY_VERSION`] using `current`.
///
/// This performs only in-record maintenance: it never mutates
/// `step_instances`. Settlement is responsible for persisting the returned
/// maintenance in the same write as the instance record.
///
/// - **Successful re-key:** rewrites `*key` to the derived key, sets
///   `key_version` and `attempted_key_version_nk` to [`INSTANCE_KEY_VERSION`],
///   and returns `Some(old_key)`. Settlement must remove `old_key` and move
///   `step_instances[workflow][step]` to the new key in that same write so the
///   map never names a key that is not stored.
/// - **No re-key (pass completed):** sets `attempted_key_version_nk` to
///   [`INSTANCE_KEY_VERSION`], leaves `*key`/`key_version` unchanged, and
///   returns `None`.
///
/// Migration never turns a miss into a hit; callers invoke it only after a hit
/// on unchanged material.
fn migrate_instance(
    instance: &mut ToolCallInstance,
    key: &mut Hash,
    current: &CurrentMaterial,
) -> Option<Hash> {
    let source_version = instance.key_version;
    let tool_call_id = instance.tool_call_id.clone();
    let start = instance.attempted_key_version_nk.saturating_add(1);
    if start <= INSTANCE_KEY_VERSION {
        for version in (start..=INSTANCE_KEY_VERSION).rev() {
            let Some(migrated_material) = migrate_terms(
                source_version,
                version,
                &instance.command_args,
                &instance.outputs,
                &instance.materialized_inputs,
            ) else {
                continue;
            };
            let Some(migrated_key) = derive_instance_key_for_version(
                version,
                &tool_call_id,
                &migrated_material.command_args,
                Some(&V8KeyMaterial {
                    output_tokens: &migrated_material.output_tokens,
                    materialized_inputs: &migrated_material.materialized_inputs,
                }),
            ) else {
                continue;
            };
            let Some(current_key) = current.key(&tool_call_id, version) else {
                continue;
            };
            if migrated_key != current_key {
                continue;
            }

            let old_key = *key;
            *key = migrated_key;
            instance.key_version = version;
            instance.attempted_key_version_nk = INSTANCE_KEY_VERSION;
            return Some(old_key);
        }
    }

    instance.attempted_key_version_nk = INSTANCE_KEY_VERSION;
    None
}

/// Performs instance lookup with v8 fast path, map path, and migration.
///
/// `workflow_id`/`step_id` identify the settling step for `step_instances`
/// maintenance on re-key. This function does not mutate `state`; it returns a
/// decision carrying any instance mutations/re-keying for settlement to apply.
pub(super) fn lookup_instance(
    workflow_id: &str,
    step_id: &str,
    tool_call_id: &str,
    current: &CurrentMaterial,
    state: &ConductorState,
    required: &BTreeMap<String, OutputCaptureSpec>,
) -> LookupResult {
    let mut side_effects: BTreeMap<Hash, ToolCallInstance> = BTreeMap::new();

    // Fast path: K_latest.
    if let Some(k_latest) = current.key(tool_call_id, INSTANCE_KEY_VERSION) {
        if let Some(instance) = state.tool_call_instances.get(&k_latest) {
            if instance.key_version == INSTANCE_KEY_VERSION
                && instance.invalidated_at_nk.is_none()
                && satisfies_requirements(instance, required)
            {
                return LookupResult::Hit {
                    instance: instance.clone(),
                    instance_key: k_latest,
                    rekeyed_from: None,
                };
            }
        }
    }

    // Map path.
    let Some(step_map) = state.step_instances.get(workflow_id) else {
        return LookupResult::Miss { side_effects };
    };
    let Some(&mapped_key) = step_map.get(step_id) else {
        return LookupResult::Miss { side_effects };
    };
    let Some(instance) = state.tool_call_instances.get(&mapped_key) else {
        return LookupResult::Miss { side_effects };
    };

    // Invalidation => miss (no migration).
    if instance.invalidated_at_nk.is_some() {
        return LookupResult::Miss { side_effects };
    }

    let a = instance.key_version;
    let Some(k_current_a) = current.key(tool_call_id, a) else {
        // Cannot derive key for this version from current material.
        return LookupResult::Miss { side_effects };
    };
    if k_current_a != mapped_key {
        // Work changed => miss and skip migration, but mark attempted up to latest.
        if instance.attempted_key_version_nk < INSTANCE_KEY_VERSION {
            let mut updated = instance.clone();
            updated.attempted_key_version_nk = INSTANCE_KEY_VERSION;
            side_effects.insert(mapped_key, updated);
        }
        return LookupResult::Miss { side_effects };
    }

    // Hit candidate (identity matches). Check probe requirements before
    // declaring a hit? Plan states "hit(i)" then migrate; probe requirements
    // (null-hash rule) must be satisfied.
    if !satisfies_requirements(instance, required) {
        // Requirements not satisfied - treat as miss (no migration specified for
        // this case beyond normal miss; keep pointer/attempted as-is).
        return LookupResult::Miss { side_effects };
    }

    // Hit: migrate maintenance is independent of the hit decision and must be
    // persisted even when later rejected for missing blobs; the caller settles
    // the returned instance/re-key state.
    let mut hit_instance = instance.clone();
    let mut hit_key = mapped_key;
    let rekeyed_from = migrate_instance(&mut hit_instance, &mut hit_key, current);

    LookupResult::Hit { instance: hit_instance, instance_key: hit_key, rekeyed_from }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instance_key::{
        INSTANCE_KEY_VERSION, V7_INSTANCE_KEY_VERSION, derive_instance_key_v7,
    };
    use crate::state::CapturedOutput;
    use mediapm_utils::Timestamp;
    use std::collections::BTreeSet;

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

    fn empty_state() -> ConductorState {
        ConductorState::new_empty()
    }

    fn base_instance(
        tool_call_id: &str,
        key_version: u32,
        command_args: Vec<Hash>,
        outputs: BTreeMap<String, CapturedOutput>,
        materialized_inputs: BTreeMap<String, Hash>,
    ) -> ToolCallInstance {
        ToolCallInstance {
            key_version,
            attempted_key_version_nk: key_version,
            tool_call_id: tool_call_id.to_string(),
            impure_nk: false,
            invalidated_at_nk: None,
            executed_at_nk: Timestamp::default(),
            last_referenced_at_nk: Timestamp::default(),
            step_ids_nk: BTreeSet::new(),
            command_args,
            env_vars_ck: BTreeMap::new(),
            materialized_inputs,
            outputs,
        }
    }

    fn current_v8(
        command_args: Vec<Hash>,
        outputs: &BTreeMap<String, OutputCaptureSpec>,
        materialized_inputs: BTreeMap<String, Hash>,
    ) -> CurrentMaterial {
        let output_tokens =
            outputs.iter().map(|(name, spec)| (name.clone(), spec.capture.to_token())).collect();
        CurrentMaterial { command_args, output_tokens, materialized_inputs }
    }

    #[test]
    fn fast_hit_on_k_latest() {
        let tool_call_id = "echo@v1";
        let command_args = vec![Hash::from_content(b"arg")];
        let outputs_spec = BTreeMap::from([("content".to_string(), spec("content", "stdout"))]);
        let current = current_v8(command_args.clone(), &outputs_spec, BTreeMap::new());
        let k_latest = current.key(tool_call_id, INSTANCE_KEY_VERSION).unwrap();

        let outputs = BTreeMap::from([(
            "content".to_string(),
            recorded_output(&outputs_spec["content"], Hash::from_content(b"bytes"), true),
        )]);
        let instance = base_instance(
            tool_call_id,
            INSTANCE_KEY_VERSION,
            command_args,
            outputs,
            BTreeMap::new(),
        );
        let mut state = empty_state();
        state.tool_call_instances.insert(k_latest, instance.clone());
        state
            .step_instances
            .insert("wf".to_string(), BTreeMap::from([("step".to_string(), k_latest)]));

        let result = lookup_instance("wf", "step", tool_call_id, &current, &state, &outputs_spec);
        match result {
            LookupResult::Hit { instance_key, rekeyed_from, .. } => {
                assert_eq!(instance_key, k_latest);
                assert!(rekeyed_from.is_none());
            }
            LookupResult::Miss { .. } => panic!("expected fast-path hit"),
        }
    }

    #[test]
    fn map_path_v7_hit_migrates_to_v8_when_unchanged() {
        let tool_call_id = "echo@v1";
        let command_args = vec![Hash::from_content(b"arg")];
        let outputs_spec = BTreeMap::from([("content".to_string(), spec("content", "stdout"))]);
        let current = current_v8(command_args.clone(), &outputs_spec, BTreeMap::new());
        let k7 = derive_instance_key_v7(tool_call_id, &command_args);
        let outputs = BTreeMap::from([(
            "content".to_string(),
            recorded_output(&outputs_spec["content"], Hash::from_content(b"bytes"), true),
        )]);
        let instance = base_instance(
            tool_call_id,
            V7_INSTANCE_KEY_VERSION,
            command_args,
            outputs,
            BTreeMap::new(),
        );
        let mut state = empty_state();
        state.tool_call_instances.insert(k7, instance);
        state.step_instances.insert("wf".to_string(), BTreeMap::from([("step".to_string(), k7)]));

        let result = lookup_instance("wf", "step", tool_call_id, &current, &state, &outputs_spec);
        let (migrated, new_key) = match result {
            LookupResult::Hit { instance, instance_key, rekeyed_from } => {
                assert_ne!(instance_key, k7);
                assert_eq!(rekeyed_from, Some(k7));
                assert_eq!(instance.key_version, INSTANCE_KEY_VERSION);
                assert_eq!(instance.attempted_key_version_nk, INSTANCE_KEY_VERSION);
                assert_eq!(instance_key, current.key(tool_call_id, INSTANCE_KEY_VERSION).unwrap());
                (instance, instance_key)
            }
            LookupResult::Miss { .. } => panic!("expected map-path hit with migration"),
        };

        // After settlement-like maintenance, the fast path must find it.
        state.tool_call_instances.remove(&k7);
        state.tool_call_instances.insert(new_key, migrated);
        state
            .step_instances
            .get_mut("wf")
            .expect("workflow map")
            .insert("step".to_string(), new_key);
        let fast = lookup_instance("wf", "step", tool_call_id, &current, &state, &outputs_spec);
        match fast {
            LookupResult::Hit { instance_key, rekeyed_from, .. } => {
                assert_eq!(instance_key, new_key);
                assert!(rekeyed_from.is_none(), "already at latest key");
            }
            LookupResult::Miss { .. } => panic!("migrated instance must be found by fast path"),
        }
    }

    #[test]
    fn invalidated_instance_is_miss() {
        let tool_call_id = "echo@v1";
        let command_args = vec![Hash::from_content(b"arg")];
        let outputs_spec = BTreeMap::new();
        let current = current_v8(command_args.clone(), &outputs_spec, BTreeMap::new());
        let k_latest = current.key(tool_call_id, INSTANCE_KEY_VERSION).unwrap();
        let mut instance = base_instance(
            tool_call_id,
            INSTANCE_KEY_VERSION,
            command_args,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        instance.invalidated_at_nk = Some(Timestamp::from_unix_nanos(1));
        let mut state = empty_state();
        state.tool_call_instances.insert(k_latest, instance);
        state
            .step_instances
            .insert("wf".to_string(), BTreeMap::from([("step".to_string(), k_latest)]));

        let result = lookup_instance("wf", "step", tool_call_id, &current, &state, &outputs_spec);
        assert!(matches!(result, LookupResult::Miss { .. }));
    }

    #[test]
    fn work_changed_is_miss_and_bumps_attempted() {
        let tool_call_id = "echo@v1";
        let old_args = vec![Hash::from_content(b"old")];
        let new_args = vec![Hash::from_content(b"new")];
        let outputs_spec = BTreeMap::new();
        let current = current_v8(new_args, &outputs_spec, BTreeMap::new());
        let k7 = derive_instance_key_v7(tool_call_id, &old_args);
        let instance = base_instance(
            tool_call_id,
            V7_INSTANCE_KEY_VERSION,
            old_args,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        let mut state = empty_state();
        state.tool_call_instances.insert(k7, instance);
        state.step_instances.insert("wf".to_string(), BTreeMap::from([("step".to_string(), k7)]));

        let result = lookup_instance("wf", "step", tool_call_id, &current, &state, &outputs_spec);
        match result {
            LookupResult::Miss { side_effects } => {
                let updated = side_effects.get(&k7).expect("old instance should be bumped");
                assert_eq!(updated.attempted_key_version_nk, INSTANCE_KEY_VERSION);
            }
            LookupResult::Hit { .. } => panic!("expected miss"),
        }
    }

    #[test]
    fn null_hash_requires_allow_empty() {
        let tool_call_id = "echo@v1";
        let command_args = vec![Hash::from_content(b"arg")];
        let mut required_allow = spec("content", "folder_regex:.*");
        required_allow.allow_empty = true;
        let mut required_disallow = spec("content", "folder_regex:.*");
        required_disallow.allow_empty = false;
        let outputs = BTreeMap::from([(
            "content".to_string(),
            CapturedOutput {
                capture: "folder_regex:.*".to_string(),
                hash_nk: None,
                saved_nk: false,
            },
        )]);
        let current = current_v8(command_args.clone(), &BTreeMap::new(), BTreeMap::new());
        let k_latest = current.key(tool_call_id, INSTANCE_KEY_VERSION).unwrap();
        let instance = base_instance(
            tool_call_id,
            INSTANCE_KEY_VERSION,
            command_args,
            outputs,
            BTreeMap::new(),
        );
        let mut state = empty_state();
        state.tool_call_instances.insert(k_latest, instance);
        state
            .step_instances
            .insert("wf".to_string(), BTreeMap::from([("step".to_string(), k_latest)]));

        let hit = lookup_instance(
            "wf",
            "step",
            tool_call_id,
            &current,
            &state,
            &BTreeMap::from([("content".to_string(), required_allow)]),
        );
        assert!(matches!(hit, LookupResult::Hit { .. }));
        let miss = lookup_instance(
            "wf",
            "step",
            tool_call_id,
            &current,
            &state,
            &BTreeMap::from([("content".to_string(), required_disallow)]),
        );
        assert!(matches!(miss, LookupResult::Miss { .. }));
    }

    #[test]
    fn map_hit_with_unchanged_material_migrates_and_advances_attempted_on_failure() {
        let tool_call_id = "echo@v1";
        let command_args = vec![Hash::from_content(b"arg")];
        // Current run declares stdout capture; stored v7 instance recorded stderr
        // capture for the same output name. v7 key is unchanged (inputs only), so
        // this is a map hit, but v8 migration cannot produce a key consistent
        // with current material and must leave attempted at LATEST.
        let current_outputs = BTreeMap::from([("content".to_string(), spec("content", "stdout"))]);
        let stored_spec = spec("content", "stderr");
        let current = current_v8(command_args.clone(), &current_outputs, BTreeMap::new());
        let k7 = derive_instance_key_v7(tool_call_id, &command_args);
        let outputs = BTreeMap::from([(
            "content".to_string(),
            recorded_output(&stored_spec, Hash::from_content(b"bytes"), true),
        )]);
        let instance = base_instance(
            tool_call_id,
            V7_INSTANCE_KEY_VERSION,
            command_args,
            outputs,
            BTreeMap::new(),
        );
        let mut state = empty_state();
        state.tool_call_instances.insert(k7, instance);
        state.step_instances.insert("wf".to_string(), BTreeMap::from([("step".to_string(), k7)]));

        let result =
            lookup_instance("wf", "step", tool_call_id, &current, &state, &current_outputs);
        match result {
            LookupResult::Hit { instance, instance_key, rekeyed_from } => {
                assert_eq!(instance_key, k7, "failed migration must not re-key");
                assert!(rekeyed_from.is_none());
                assert_eq!(instance.key_version, V7_INSTANCE_KEY_VERSION);
                assert_eq!(instance.attempted_key_version_nk, INSTANCE_KEY_VERSION);
            }
            LookupResult::Miss { .. } => panic!("unchanged v7 material is still a hit"),
        }
    }

    #[test]
    fn invalidated_instance_is_miss_on_fast_and_map_paths() {
        let tool_call_id = "echo@v1";
        let command_args = vec![Hash::from_content(b"arg")];
        let outputs_spec = BTreeMap::new();
        let current = current_v8(command_args.clone(), &outputs_spec, BTreeMap::new());
        let k_latest = current.key(tool_call_id, INSTANCE_KEY_VERSION).unwrap();
        let mut instance = base_instance(
            tool_call_id,
            INSTANCE_KEY_VERSION,
            command_args,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        instance.invalidated_at_nk = Some(Timestamp::from_unix_nanos(1));
        let mut state = empty_state();
        state.tool_call_instances.insert(k_latest, instance);
        state
            .step_instances
            .insert("wf".to_string(), BTreeMap::from([("step".to_string(), k_latest)]));

        let result = lookup_instance("wf", "step", tool_call_id, &current, &state, &outputs_spec);
        match result {
            LookupResult::Miss { side_effects } => {
                assert!(side_effects.is_empty(), "invalidation is not a migration attempt");
            }
            LookupResult::Hit { .. } => panic!("invalidated instance must miss on every path"),
        }
    }

    #[test]
    fn unknown_or_newer_key_version_is_a_miss() {
        let tool_call_id = "echo@v1";
        let command_args = vec![Hash::from_content(b"arg")];
        let outputs_spec = BTreeMap::new();
        let current = current_v8(command_args.clone(), &outputs_spec, BTreeMap::new());
        let mapped_key = Hash::from_content(b"synthetic-key");
        let mut instance =
            base_instance(tool_call_id, 99, command_args, BTreeMap::new(), BTreeMap::new());
        instance.attempted_key_version_nk = 99;
        let mut state = empty_state();
        state.tool_call_instances.insert(mapped_key, instance);
        state
            .step_instances
            .insert("wf".to_string(), BTreeMap::from([("step".to_string(), mapped_key)]));

        let result = lookup_instance("wf", "step", tool_call_id, &current, &state, &outputs_spec);
        assert!(matches!(result, LookupResult::Miss { .. }));
    }
}
