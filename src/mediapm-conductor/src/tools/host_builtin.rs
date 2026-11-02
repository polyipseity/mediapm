//! Host-supplied builtin runners.
//!
//! A conductor build ships a fixed set of builtin tools, listed in
//! [`ALL_BUILTINS`](crate::tools::ALL_BUILTINS). This module is how a program built on the conductor adds
//! its own: a host implements [`HostBuiltin`] once per tool it wants the
//! conductor to run in-process, collects them in [`HostBuiltins`], and passes
//! that registry to every run through
//! [`RunWorkflowOptions::host_builtins`](crate::RunWorkflowOptions::host_builtins).
//!
//! A host tool is dispatched exactly like a static builtin: the step's tool
//! declares no command parts, and the tool's builtin id is the one a runner
//! answers for. [`HostBuiltins::new`] refuses an id a static builtin already
//! owns, so resolution order is a fact rather than a rule to remember, and the
//! run start check refuses a builtin id neither this build nor the host supplies
//! before the first step runs. Registration is not a document concern: the
//! document layer cannot know what a host supplies, so it accepts any id and the
//! run judges.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use mediapm_cas::{CasApi, CasError, Hash};

use crate::error::ConductorError;
use crate::orchestration::protocol::UnifiedNickelDocument;

/// The content store a host runner may read.
///
/// Reads only: a host builtin's outputs reach the store through the step's
/// capture spec, never by writing here, so the generic streaming plumbing
/// [`CasApi`] carries stays out of this seam. A host that needs more of the
/// store is asking for a different contract, not for this one widened.
#[async_trait]
pub trait HostBuiltinCas: Send + Sync {
    /// Returns the bytes stored under `hash`.
    ///
    /// # Errors
    ///
    /// Returns [`CasError::NotFound`] when the store holds no object with this
    /// hash, and any other [`CasError`] the backing store raises.
    async fn get(&self, hash: Hash) -> Result<Bytes, CasError>;
}

/// Every [`CasApi`] store is a [`HostBuiltinCas`] for free, so the conductor
/// hands a host runner the same store the step executes against.
#[async_trait]
impl<T: CasApi> HostBuiltinCas for T {
    async fn get(&self, hash: Hash) -> Result<Bytes, CasError> {
        CasApi::get(self, hash).await
    }
}

/// Everything one step hands a host runner.
///
/// The sandbox already exists when a runner is called, so it is where a runner
/// writes the files its step's `file:` captures read. The config directory is
/// passed through unchanged from what the coordinator hands a builtin, which is
/// also what every builtin this build ships receives.
pub struct HostBuiltinRequest<'a> {
    /// The step's resolved inputs, keyed by input name.
    pub args: &'a BTreeMap<String, String>,
    /// The run's content store, readable through [`HostBuiltinCas`].
    pub cas: &'a dyn HostBuiltinCas,
    /// Directory a builtin resolves relative path values against.
    ///
    /// This is the value the coordinator sets on every step request: the
    /// directory that directly contains the outermost conductor document the
    /// run loaded, which is also what every builtin this build ships receives.
    pub outermost_config_dir: &'a Path,
    /// The step's sandbox directory, where a runner writes the files its
    /// step's `file:` captures read.
    pub sandbox_dir: &'a Path,
}

/// What a host runner produced for one step.
///
/// The three fields are what the capture phase reads, mirroring the executor's
/// own execution result without exposing that private type: a declared
/// `stdout`/`stderr`/`process_code` capture reads the matching field, and a
/// `file:` capture reads the sandbox the runner was given.
#[derive(Debug, Clone, Default)]
pub struct HostBuiltinOutput {
    /// Bytes the step's `stdout` captures read.
    pub stdout: Vec<u8>,
    /// Bytes the step's `stderr` captures read.
    pub stderr: Vec<u8>,
    /// Status the step's `process_code` capture records.
    ///
    /// `0` is success, as it is for a child process, and a non-zero status is
    /// data rather than a verdict: it is what a `process_code` capture records,
    /// and a step capturing nothing of the sort is unaffected by it. A runner
    /// fails its step by returning [`Err`] from [`HostBuiltin::run`], and the run
    /// reports that the way it reports any failed step.
    pub exit_code: i32,
}

/// One builtin runner supplied by the host program.
///
/// Implementors are registered in [`HostBuiltins`]. The id is the document's
/// builtin id — the `builtin_id` of the tool a step names — so a runner is
/// found by the same string a static builtin would be found by.
#[async_trait]
pub trait HostBuiltin: std::fmt::Debug + Send + Sync {
    /// The builtin id a document names to dispatch to this runner.
    fn id(&self) -> &str;

    /// Runs one step and returns what its captures read.
    ///
    /// # Errors
    ///
    /// Returns a [`ConductorError`] when the work fails; the run treats it like
    /// any other step failure.
    async fn run(
        &self,
        request: HostBuiltinRequest<'_>,
    ) -> Result<HostBuiltinOutput, ConductorError>;
}

/// The host's builtin runners, keyed by builtin id.
///
/// Shared by every step of a run, and empty by default: a caller that supplies
/// nothing runs only the builtins this conductor build ships.
#[derive(Debug, Clone, Default)]
pub struct HostBuiltins {
    /// One runner per builtin id, so lookup cannot depend on registration
    /// order and an id names at most one runner.
    entries: BTreeMap<String, Arc<dyn HostBuiltin>>,
}

impl HostBuiltins {
    /// Builds a registry from the host's runners.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError::Workflow`] naming the id when two runners
    /// answer for one id, which would otherwise make the runner that executed
    /// depend on registration order, and when an id is one a builtin this build
    /// ships already owns, so a host can never shadow a static builtin.
    pub fn new(entries: Vec<Arc<dyn HostBuiltin>>) -> Result<Self, ConductorError> {
        let mut by_id: BTreeMap<String, Arc<dyn HostBuiltin>> = BTreeMap::new();
        for entry in entries {
            let id = entry.id().to_string();
            if static_builtin_owns(&id) {
                return Err(ConductorError::Workflow(format!(
                    "host builtin id '{id}' is a builtin this conductor build ships, so a host \
                     runner registered under it would never be reached; a host builtin may not \
                     shadow a static builtin"
                )));
            }
            if by_id.contains_key(&id) {
                return Err(ConductorError::Workflow(format!(
                    "host builtin id '{id}' is registered twice; one runner per builtin id"
                )));
            }
            by_id.insert(id, entry);
        }
        Ok(Self { entries: by_id })
    }

    /// Returns the runner registered for `id`, or `None` when the host
    /// registered none.
    #[must_use]
    pub fn get(&self, id: &str) -> Option<Arc<dyn HostBuiltin>> {
        self.entries.get(id).cloned()
    }

    /// Iterates the ids this registry supplies.
    ///
    /// Every one is host-only: [`HostBuiltins::new`] refuses an id a static
    /// builtin owns, so nothing here can be in [`ALL_BUILTINS`](crate::tools::ALL_BUILTINS).
    pub fn host_only_ids(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(String::as_str)
    }
}

/// Refuses a document whose tools name builtin ids this run cannot dispatch.
///
/// A tool that declares no command parts reaches a builtin runner, so it names
/// an id the document does not own: either this conductor build ships it (it is
/// in [`ALL_BUILTINS`](crate::tools::ALL_BUILTINS)) or the host registered one
/// under that id. The id is the tool's declared builtin id, and the tool's own
/// name when it declares none — dispatch derives it the same way, so a tool
/// with no `builtin` kind is judged here rather than passing this check and
/// failing at its step. An id neither side supplies has no implementation for
/// the whole run, so the run refuses it here — before its first step — rather
/// than failing at whichever step names it first, which would report a partially
/// executed workflow for a wiring mistake known up front.
///
/// The check is document-wide, not workflow-scoped: every workflow in the
/// document shares the tool catalog, and a run that asked for one workflow is
/// still reading a document that cannot be dispatched as a whole. The message
/// names each offender and what is missing, because the reader's next move
/// follows from which side the id belongs to.
///
/// # Errors
///
/// Returns [`ConductorError::Workflow`] listing every tool whose builtin id
/// neither this build nor `host` supplies.
pub(crate) fn ensure_document_builtins_run(
    unified: &UnifiedNickelDocument,
    host: &HostBuiltins,
) -> Result<(), ConductorError> {
    let mut missing: Vec<String> = Vec::new();
    for (tool_key, spec) in &unified.tools {
        // Only a tool with no command parts reaches a builtin runner; one with
        // command parts runs as a process and its name is not a builtin id.
        if !spec.command_parts.is_empty() {
            continue;
        }
        // The id dispatch resolves for such a tool: its declared builtin id,
        // and its own name when it declares none.
        let builtin_id = spec.builtin_id.as_deref().unwrap_or(spec.name.as_str());
        if static_builtin_owns(builtin_id) || host.get(builtin_id).is_some() {
            continue;
        }
        missing.push(format!(
            "tool '{tool_key}' dispatches builtin id '{builtin_id}', which this conductor build \
             does not ship and no host runner registers"
        ));
    }
    if missing.is_empty() {
        return Ok(());
    }

    let registered: Vec<&str> = host.host_only_ids().collect();
    let registered = if registered.is_empty() { "none".to_string() } else { registered.join(", ") };
    Err(ConductorError::Workflow(format!(
        "cannot dispatch builtin ids this run has no runner for: {}; the host must register a \
         host builtin under each of those ids (registered host builtins: {registered})",
        missing.join("; "),
    )))
}

/// Whether this conductor build ships a builtin with this id.
fn static_builtin_owns(id: &str) -> bool {
    super::ALL_BUILTINS.iter().any(|entry| entry.builtin_id == id)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use async_trait::async_trait;

    use super::{
        HostBuiltin, HostBuiltinOutput, HostBuiltinRequest, HostBuiltins,
        ensure_document_builtins_run,
    };
    use crate::config::ConductorRuntimeConfig;
    use crate::error::ConductorError;
    use crate::orchestration::protocol::{UnifiedNickelDocument, UnifiedToolSpec};

    /// A host runner that answers for one id and produces no output.
    #[derive(Debug)]
    struct IdRunner(&'static str);

    #[async_trait]
    impl HostBuiltin for IdRunner {
        fn id(&self) -> &str {
            self.0
        }

        async fn run(
            &self,
            _request: HostBuiltinRequest<'_>,
        ) -> Result<HostBuiltinOutput, ConductorError> {
            Ok(HostBuiltinOutput::default())
        }
    }

    /// Two runners for one id are refused: which of them a step reached would
    /// otherwise depend on registration order.
    #[test]
    fn strict_a_duplicate_host_builtin_id_is_refused() {
        let error = HostBuiltins::new(vec![
            Arc::new(IdRunner("retag@v1")) as Arc<dyn HostBuiltin>,
            Arc::new(IdRunner("retag@v1")) as Arc<dyn HostBuiltin>,
        ])
        .expect_err("one id may name one runner");

        let message = error.to_string();
        assert!(message.contains("retag@v1"), "the error must name the duplicated id: {message}");
    }

    /// A host id may not shadow a builtin this build ships.
    ///
    /// The static arms are what dispatch runs first, so a host runner
    /// registered under `echo@v1` would never be reached: the host would have
    /// wired a tool it cannot observe. Refusing the registry is what keeps the
    /// resolution order from being a rule anyone has to remember.
    #[test]
    fn strict_a_host_builtin_id_a_static_builtin_owns_is_refused() {
        let error = HostBuiltins::new(vec![Arc::new(IdRunner("echo@v1")) as Arc<dyn HostBuiltin>])
            .expect_err("a host runner may not shadow a static builtin");

        let message = error.to_string();
        assert!(message.contains("echo@v1"), "the error must name the colliding id: {message}");
    }

    /// A registered id resolves to its runner and is the only thing the
    /// registry offers.
    #[test]
    fn a_registered_runner_is_returned_by_id_and_listed() {
        let registry =
            HostBuiltins::new(vec![Arc::new(IdRunner("retag@v1")) as Arc<dyn HostBuiltin>])
                .expect("one fresh id is accepted");

        assert_eq!(registry.host_only_ids().collect::<Vec<_>>(), vec!["retag@v1"]);
        assert_eq!(
            registry.get("retag@v1").expect("the registered runner is returned").id(),
            "retag@v1",
        );
        assert!(registry.get("absent@v1").is_none(), "an unregistered id resolves to no runner");
    }

    /// One unified tool spec, as the document layer produces it.
    fn tool_spec(
        name: &str,
        builtin_id: Option<&str>,
        command_parts: Vec<String>,
    ) -> UnifiedToolSpec {
        UnifiedToolSpec {
            name: name.to_string(),
            version: None,
            is_impure: false,
            max_concurrent_calls: 0,
            max_retries: 0,
            inputs: BTreeMap::new(),
            default_inputs: BTreeMap::new(),
            command_parts,
            success_codes: vec![0],
            execution_env_vars: BTreeMap::new(),
            outputs: BTreeMap::new(),
            tool_content_map: BTreeMap::new(),
            payload_root: String::new(),
            payload_staging: crate::config::PayloadStaging::Copy,
            builtin_id: builtin_id.map(str::to_string),
        }
    }

    /// A merged document carrying exactly the given tools, keyed by name.
    fn document_with_tools(specs: Vec<UnifiedToolSpec>) -> UnifiedNickelDocument {
        UnifiedNickelDocument {
            tools: specs.into_iter().map(|spec| (spec.name.clone(), spec)).collect(),
            workflows: BTreeMap::new(),
            tool_content_hashes: BTreeSet::new(),
            external_data_policies: BTreeMap::new(),
            runtime: ConductorRuntimeConfig::default(),
        }
    }

    /// A tool with no command parts is judged by the id dispatch resolves for
    /// it, which is the tool's own name when it declares no builtin id.
    ///
    /// `kind = "executable"` with an empty `command` is contract-valid, and it
    /// is what [`ToolKindSpec::default`](crate::ToolKindSpec::default) produces:
    /// dispatch hands it to a builtin runner under the tool's name. Judging only
    /// tools that declare a builtin id left that id unchecked, so such a step
    /// passed this check and met `has no execute implementation` mid-run — the
    /// failure the check exists to move to run start. The registered-id half
    /// pins the derivation to the name rather than to any other id.
    #[test]
    fn strict_a_no_command_tool_without_a_builtin_id_is_judged_by_its_name() {
        let document = document_with_tools(vec![tool_spec("retag", None, Vec::new())]);

        let error = ensure_document_builtins_run(&document, &HostBuiltins::default())
            .expect_err("the name is the id dispatch resolves, and no runner answers it");
        let message = error.to_string();
        assert!(
            message.contains("retag"),
            "the error must name the id dispatch resolves: {message}"
        );

        let registry = HostBuiltins::new(vec![Arc::new(IdRunner("retag")) as Arc<dyn HostBuiltin>])
            .expect("a name no static builtin owns is a fresh id");
        assert!(
            ensure_document_builtins_run(&document, &registry).is_ok(),
            "a runner registered under the tool's name must satisfy the check"
        );
    }
}
