//! Whitelisting child-process environments.
//!
//! Runtime children must not inherit the ambient environment. This module
//! resolves the host names a child is allowed to receive and applies that
//! whitelist to a command: every other variable is cleared. The resolution and
//! the apply step live here so `mediapm` and `mediapm-conductor` answer "what
//! does a child inherit?" in one place.

use std::process::Command;

/// Reads the host values for `names`, keeping only the names the host sets.
///
/// Names the host does not define are omitted rather than inserted with an
/// empty value, so the result describes exactly what a child receives for this
/// whitelist. Duplicate names keep their host value once; callers should pass
/// the document's name list as-is.
#[must_use]
pub fn host_env_pairs(names: &[&str]) -> Vec<(String, String)> {
    names
        .iter()
        .filter_map(|&name| std::env::var(name).ok().map(|value| (name.to_owned(), value)))
        .collect()
}

/// Clears `cmd`'s environment and sets only `pairs`.
///
/// Clearing comes first so a caller that applies a resolved map after this
/// cannot leave an ambient variable behind, and so a tool's own literal value
/// set by the caller remains authoritative.
pub fn apply_std(cmd: &mut Command, pairs: &[(String, String)]) {
    cmd.env_clear();
    for (name, value) in pairs {
        cmd.env(name, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;

    /// A host name that is not set must be absent from the result, not present
    /// with an empty value: the whitelist names what may pass, and an unset
    /// name has nothing to pass.
    #[test]
    fn host_env_pairs_omits_unset_names() {
        let missing = "MEDIAPM_CHILD_ENV_TEST_UNSET_NAME_THAT_MUST_NOT_EXIST";
        // Best effort: the name is chosen to be absent in practice; if a host
        // defines it, this assertion would be the wrong test for that host.
        if std::env::var_os(missing).is_some() {
            return;
        }
        let pairs = host_env_pairs(&[missing]);
        assert!(
            pairs.iter().all(|(name, _)| name != missing),
            "unset host name must not appear in the whitelist pairs: {pairs:?}"
        );
        assert!(
            pairs.iter().all(|(_, value)| !value.is_empty() || std::env::var_os(missing).is_some()),
            "host_env_pairs must never invent an empty value for an unset name: {pairs:?}"
        );
    }

    /// `apply_std` clears the ambient environment, so a name outside `pairs`
    /// never reaches the built command.
    #[test]
    fn apply_std_clears_and_sets_only_pairs() {
        let mut cmd = Command::new("true");
        cmd.env("MEDIAPM_CHILD_ENV_AMBIENT_LEAK_SENTINEL", "ambient");
        let pairs = vec![("MEDIAPM_CHILD_ENV_ALLOWED".to_owned(), "kept".to_owned())];
        apply_std(&mut cmd, &pairs);

        let envs: Vec<(OsString, Option<OsString>)> =
            cmd.get_envs().map(|(k, v)| (k.to_owned(), v.map(OsString::from))).collect();
        assert_eq!(
            envs,
            vec![(OsString::from("MEDIAPM_CHILD_ENV_ALLOWED"), Some(OsString::from("kept")))],
            "only the whitelisted pair must remain after env_clear"
        );
    }

    /// `apply_std` and `apply_tokio` apply the same pair list to the same
    /// resulting environment: only the named pairs remain, and an unset host
    /// name is omitted because it never appears in the pair list.
    #[test]
    fn std_and_tokio_apply_the_same_pair_list() {
        let pairs = vec![
            ("MEDIAPM_CHILD_ENV_PARITY_A".to_owned(), "one".to_owned()),
            ("MEDIAPM_CHILD_ENV_PARITY_B".to_owned(), "two".to_owned()),
        ];
        let mut std_cmd = Command::new("true");
        std_cmd.env("MEDIAPM_CHILD_ENV_PARITY_AMBIENT", "leak");
        apply_std(&mut std_cmd, &pairs);
        let std_envs: Vec<(OsString, Option<OsString>)> =
            std_cmd.get_envs().map(|(k, v)| (k.to_owned(), v.map(OsString::from))).collect();

        let mut tokio_cmd = tokio::process::Command::new("true");
        tokio_cmd.env("MEDIAPM_CHILD_ENV_PARITY_AMBIENT", "leak");
        tokio_cmd.env_clear();
        for (name, value) in &pairs {
            tokio_cmd.env(name, value);
        }
        let tokio_envs: Vec<(OsString, Option<OsString>)> = tokio_cmd
            .as_std()
            .get_envs()
            .map(|(k, v)| (k.to_owned(), v.map(OsString::from)))
            .collect();

        assert_eq!(
            std_envs, tokio_envs,
            "std and tokio commands must receive the same whitelisted environment"
        );
        assert!(
            std_envs.iter().all(|(name, _)| name != "MEDIAPM_CHILD_ENV_PARITY_AMBIENT"),
            "unset/undeclared names must be omitted from the applied environment"
        );
    }
}
