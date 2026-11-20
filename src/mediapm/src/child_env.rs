//! Child-environment helpers for mediapm's own runtime spawns.
//!
//! The conductor whitelists every workflow and passthrough child it spawns.
//! `mediapm` also spawns runtime children of its own — metadata probes and the
//! in-process tagging helpers — and those children must follow the same rule:
//! they receive exactly the host platform's configured inherited names and
//! nothing ambient. The configured names come from the resolved mediapm runtime
//! storage, so a user who extends `platform_inherited_env_vars` in `mediapm.ncl`
//! reaches these children too.

use std::collections::BTreeMap;

/// Resolves the host platform's inherited environment-variable pairs.
///
/// `platform_inherited_env_vars` is the resolved platform map from
/// `MediaRuntimeStorage::environment.inherited_env_vars`. The host platform's
/// entry is read when present; when the host platform has no entry, the
/// conductor's platform defaults are used so a missing platform key still
/// yields a deterministic whitelist.
///
/// Only names the host currently sets are returned, with their host values.
#[must_use]
pub fn host_inherited_env_pairs(
    platform_inherited_env_vars: &BTreeMap<String, Vec<String>>,
) -> Vec<(String, String)> {
    let platform_key = mediapm_conductor::config::host_platform_inherited_env_var_key();
    let names = platform_inherited_env_vars.get(platform_key).cloned().unwrap_or_else(|| {
        mediapm_conductor::config::default_platform_inherited_env_var_names(platform_key)
            .iter()
            .map(|&name| name.to_owned())
            .collect()
    });
    let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
    mediapm_utils::child_env::host_env_pairs(&name_refs)
}

/// Resolves the host platform's default inherited environment-variable pairs.
///
/// This is the whitelist a runtime child receives when no user-configured
/// platform map is in scope: the conductor's default inherited names for the
/// host platform, read from the current process environment.
#[must_use]
pub fn default_host_inherited_env_pairs() -> Vec<(String, String)> {
    host_inherited_env_pairs(&BTreeMap::new())
}

/// Clears `cmd`'s environment and sets only `pairs`.
///
/// This delegates to the shared whitelist applicator so mediapm's own runtime
/// spawns and its test helpers follow the same rule as every other child.
pub fn apply_std(cmd: &mut std::process::Command, pairs: &[(String, String)]) {
    mediapm_utils::child_env::apply_std(cmd, pairs);
}
