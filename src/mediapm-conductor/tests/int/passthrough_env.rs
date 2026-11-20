//! `conductor tool run` applies the tool's environment to the process it spawns.
//!
//! The passthrough path spawns a tool without a step behind it, so the proof is
//! the same child-process trick the step-path test uses: a copy of the stub
//! named `yt-dlp` reads the manifest path from `MEDIAPM_STUB_MANIFEST` and
//! writes the manifest's destination.
//!
//! Two tests state that name differently. The first states it in the tool's
//! `env_vars` as a literal, with nothing installing it in the test process, so
//! a passthrough that applies no environment leaves the stub with nothing to
//! read and it answers its own usage failure without writing the destination.
//! The second states the name with the empty value the resolver reads as
//! "read the host", installs the manifest path as the host value itself, and
//! asserts the child received it: an empty document value must not overwrite
//! the host's variable with an empty string.

use std::collections::BTreeMap;

use super::tool_env_vars::named_stub_copy;
use crate::{
    EnvVarGuard, RpcEnvGuard, TestConductor, doc_with_workflows, rpc_env_lock, workflow_with_steps,
};
use mediapm_conductor::{ToolKindSpec, ToolRuntime, ToolSpec};
use mediapm_test_support::MANIFEST_ENV;

/// A passthrough run gets the environment the document's tool spec states.
///
/// The proof is a real child process writing a file no other surface names:
/// the stub emits the destinations of the manifest `MEDIAPM_STUB_MANIFEST`
/// points at, and the only place that variable is stated is the tool's
/// `env_vars`. The host holds a decoy manifest that points at a different
/// destination, so the real destination proves the document's literal reached
/// the child and the decoy destination proves the host value did not.
///
/// The exit code and the emitted bytes are asserted together so a run that
/// failed and one that wrote the wrong bytes are distinguishable, and the
/// failure message names the variable a missing literal leaves unset.
#[tokio::test]
async fn a_passthrough_run_applies_the_tools_environment() {
    // The lock serialises this process-wide mutation against sibling tests
    // that mutate the environment; it is declared before the guards so locals
    // drop in reverse order and every guard restores while the lock is still
    // held.
    let _env_lock = rpc_env_lock().await;
    let _rpc_timeout = RpcEnvGuard::remove();

    let tc = TestConductor::new();
    let executable = named_stub_copy(tc.path(), "yt-dlp");
    let manifest = tc.path().join("emit.manifest");
    let source = tc.path().join("manifest-source.txt");
    let destination = tc.path().join("emitted.txt");
    let decoy_manifest = tc.path().join("decoy.manifest");
    let decoy_source = tc.path().join("decoy-source.txt");
    let decoy_destination = tc.path().join("decoy-emitted.txt");
    std::fs::write(&source, b"written by the stub").expect("write the manifest source");
    std::fs::write(&manifest, format!("{}\t{}\n", destination.display(), source.display()))
        .expect("write the emit manifest");
    std::fs::write(&decoy_source, b"written by the decoy").expect("write the decoy source");
    std::fs::write(
        &decoy_manifest,
        format!("{}\t{}\n", decoy_destination.display(), decoy_source.display()),
    )
    .expect("write the decoy manifest");
    let _host_manifest = EnvVarGuard::set(MANIFEST_ENV, &decoy_manifest.display().to_string());

    let emitter = ToolSpec {
        version: None,
        kind: ToolKindSpec::Executable {
            command: vec![executable.to_string_lossy().into_owned()],
            env_vars: BTreeMap::from([(MANIFEST_ENV.to_string(), manifest.display().to_string())]),
            success_codes: vec![0],
        },
        name: "yt-dlp".into(),
        inputs: BTreeMap::new(),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime::default(),
    };
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("yt-dlp".to_string(), emitter)]),
        Vec::new(),
    ));

    let exit_code = tc
        .conductor()
        .run_tool_passthrough("yt-dlp", &[])
        .await
        .expect("a passthrough reports an exit code");

    assert_eq!(
        (exit_code, std::fs::read(&destination).ok()),
        (0, Some(b"written by the stub".to_vec())),
        "the stub writes '{}' only when {MANIFEST_ENV} names the manifest, and the document states \
         that name in the tool's env_vars; a non-zero exit with no destination is the stub's \
         usage error naming the unset variable, so the document's literal never reached the \
         passthrough child",
        destination.display(),
    );
    assert!(
        !decoy_destination.exists(),
        "the host's decoy manifest must not drive the child: the document's literal manifest is \
         the one that must reach the process"
    );
    tc.shutdown().await;
}

/// A passthrough run whose document states an empty value for a tool env var
/// runs the child with the host's value for that name.
///
/// An empty literal is the document's spelling of "inherit the host's value",
/// so it must never reach the child as an empty string. The host variable is
/// the sentinel: the test installs the manifest path as the host value and
/// states the same name with `""`, and the stub can only write its destination
/// by reading that manifest path. A resolver that applied the empty literal
/// would hand the child an empty string, the stub would answer its usage
/// failure, and the destination would stay unwritten.
#[tokio::test]
async fn a_passthrough_empty_env_var_value_reads_the_host_value() {
    let tc = TestConductor::new();
    let executable = named_stub_copy(tc.path(), "yt-dlp");
    let manifest = tc.path().join("emit.manifest");
    let source = tc.path().join("manifest-source.txt");
    let destination = tc.path().join("emitted.txt");
    std::fs::write(&source, b"written by the stub").expect("write the manifest source");
    std::fs::write(&manifest, format!("{}\t{}\n", destination.display(), source.display()))
        .expect("write the emit manifest");

    let emitter = ToolSpec {
        version: None,
        kind: ToolKindSpec::Executable {
            command: vec![executable.to_string_lossy().into_owned()],
            // The empty value is the whole subject: it says "read the host",
            // and the host is set to the manifest path below before the run.
            env_vars: BTreeMap::from([(MANIFEST_ENV.to_string(), String::new())]),
            success_codes: vec![0],
        },
        name: "yt-dlp".into(),
        inputs: BTreeMap::new(),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime::default(),
    };
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("yt-dlp".to_string(), emitter)]),
        Vec::new(),
    ));

    // The lock serialises this process-wide mutation against the sibling tests
    // that mutate the environment; it is declared before the guards so locals
    // drop in reverse order and every guard restores while the lock is still
    // held.
    let _env_lock = rpc_env_lock().await;
    let _rpc_timeout = RpcEnvGuard::remove();
    let _host_manifest = EnvVarGuard::set(MANIFEST_ENV, &manifest.display().to_string());

    let exit_code = tc
        .conductor()
        .run_tool_passthrough("yt-dlp", &[])
        .await
        .expect("a passthrough reports an exit code");

    assert_eq!(
        (exit_code, std::fs::read(&destination).ok()),
        (0, Some(b"written by the stub".to_vec())),
        "the stub writes '{}' only when {MANIFEST_ENV} names the manifest, and the host holds \
         that path while the document states the name with the empty value the resolver reads \
         as \"the host's value\"; a non-zero exit with no destination means the child received \
         the empty literal instead of the host's value",
        destination.display(),
    );
    tc.shutdown().await;
}

/// A passthrough child receives no undeclared host variable.
///
/// The name is set in the test process but appears in neither the platform's
/// inherited whitelist nor the tool's declared environment. The stub observes
/// the child environment without a shell and writes the variable's value to a
/// file; an empty file is the proof the whitelist kept the ambient variable
/// out.
#[tokio::test]
async fn an_undeclared_host_var_does_not_reach_a_passthrough_child() {
    let _env_lock = rpc_env_lock().await;
    let _rpc_timeout = RpcEnvGuard::remove();
    let tc = TestConductor::new();
    let executable = named_stub_copy(tc.path(), "print-env-tool");
    let output = tc.path().join("undeclared-passthrough.txt");
    let _host_var = EnvVarGuard::set("MEDIAPM_T3_UNDECLARED_PASSTHROUGH", "ambient-leak");

    let tool = ToolSpec {
        version: None,
        kind: ToolKindSpec::Executable {
            command: vec![
                executable.to_string_lossy().into_owned(),
                "--mode".into(),
                "print-env".into(),
                "--name".into(),
                "MEDIAPM_T3_UNDECLARED_PASSTHROUGH".into(),
                "--output".into(),
                output.to_string_lossy().into_owned(),
            ],
            env_vars: BTreeMap::new(),
            success_codes: vec![0],
        },
        name: "env-probe".into(),
        inputs: BTreeMap::new(),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime::default(),
    };
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("env-probe".to_string(), tool)]),
        Vec::new(),
    ));

    let exit_code = tc
        .conductor()
        .run_tool_passthrough("env-probe", &[])
        .await
        .expect("a passthrough reports an exit code");
    let observed = std::fs::read_to_string(&output).unwrap_or_default();
    assert_eq!(
        (exit_code, observed.as_str()),
        (0, ""),
        "the host set MEDIAPM_T3_UNDECLARED_PASSTHROUGH, but the name is undeclared and not \
         inherited, so the whitelisted child must not receive it"
    );
    tc.shutdown().await;
}

/// A step child receives no undeclared host variable.
///
/// The step path applies the whitelist through the same resolved environment as
/// a passthrough: a name the host sets but the document never declares must not
/// reach the process the step runs.
#[tokio::test]
async fn an_undeclared_host_var_does_not_reach_a_step_child() {
    let _env_lock = rpc_env_lock().await;
    let _rpc_timeout = RpcEnvGuard::remove();
    let tc = TestConductor::new();
    let executable = named_stub_copy(tc.path(), "print-env-tool");
    let output = tc.path().join("undeclared-step.txt");
    let _host_var = EnvVarGuard::set("MEDIAPM_T3_UNDECLARED_STEP", "ambient-leak");

    let tool = ToolSpec {
        version: None,
        kind: ToolKindSpec::Executable {
            command: vec![
                executable.to_string_lossy().into_owned(),
                "--mode".into(),
                "print-env".into(),
                "--name".into(),
                "MEDIAPM_T3_UNDECLARED_STEP".into(),
                "--output".into(),
                output.to_string_lossy().into_owned(),
            ],
            env_vars: BTreeMap::new(),
            success_codes: vec![0],
        },
        name: "env-probe".into(),
        inputs: BTreeMap::new(),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime::default(),
    };
    let step = mediapm_conductor::WorkflowStepSpec {
        id: "s1".into(),
        tool: "env-probe".into(),
        inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        max_retries: 0,
        depends_on: Vec::new(),
    };
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("env-probe".to_string(), tool)]),
        vec![workflow_with_steps("default", vec![step])],
    ));

    let summary = tc
        .conductor()
        .run_workflow("default", mediapm_conductor::api::RunWorkflowOptions::default())
        .await
        .expect("a run reports a summary");
    let observed = std::fs::read_to_string(&output).unwrap_or_default();
    assert_eq!(
        (summary.failed_steps, observed.as_str()),
        (0, ""),
        "the host set MEDIAPM_T3_UNDECLARED_STEP, but the name is undeclared and not inherited, \
         so the whitelisted step child must not receive it"
    );
    tc.shutdown().await;
}

/// A declared inherited name reaches the child with the host's value.
///
/// The platform's inherited whitelist names the variable for the host platform,
/// so a child receives the host's current value for that name even when the tool
/// does not restate it. The child observes the value through the stub rather
/// than through a shell.
#[tokio::test]
async fn a_declared_inherited_name_reaches_the_child_with_the_host_value() {
    let _env_lock = rpc_env_lock().await;
    let _rpc_timeout = RpcEnvGuard::remove();
    let tc = TestConductor::new();
    let executable = named_stub_copy(tc.path(), "print-env-tool");
    let output = tc.path().join("inherited.txt");
    let _host_var = EnvVarGuard::set("MEDIAPM_T3_DECLARED_INHERITED", "host-value");

    let platform = mediapm_conductor::config::host_platform_inherited_env_var_key();
    let mut platform_vars = mediapm_conductor::config::PlatformInheritedEnvVars::default();
    let declared = vec!["MEDIAPM_T3_DECLARED_INHERITED".to_string()];
    match platform {
        "windows" => platform_vars.windows = declared,
        "linux" => platform_vars.linux = declared,
        "macos" => platform_vars.macos = declared,
        _ => unreachable!("known host platform"),
    }

    let tool = ToolSpec {
        version: None,
        kind: ToolKindSpec::Executable {
            command: vec![
                executable.to_string_lossy().into_owned(),
                "--mode".into(),
                "print-env".into(),
                "--name".into(),
                "MEDIAPM_T3_DECLARED_INHERITED".into(),
                "--output".into(),
                output.to_string_lossy().into_owned(),
            ],
            env_vars: BTreeMap::new(),
            success_codes: vec![0],
        },
        name: "env-probe".into(),
        inputs: BTreeMap::new(),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime::default(),
    };
    let mut doc = doc_with_workflows(BTreeMap::from([("env-probe".to_string(), tool)]), Vec::new());
    doc.runtime.environment.platform_inherited_env_vars = platform_vars;
    tc.write_config(doc);

    let exit_code = tc
        .conductor()
        .run_tool_passthrough("env-probe", &[])
        .await
        .expect("a passthrough reports an exit code");
    let observed = std::fs::read_to_string(&output).unwrap_or_default();
    assert_eq!(
        (exit_code, observed.as_str()),
        (0, "host-value"),
        "the platform's inherited whitelist declares MEDIAPM_T3_DECLARED_INHERITED, so the child \
         must receive the host's value for it"
    );
    tc.shutdown().await;
}

/// A literal `env_vars` value reaches the child even when the host differs.
///
/// The tool states the name with a fixed value, and the host holds a different
/// value for the same name. The literal is authoritative: the child must see the
/// document's value, not the host's.
#[tokio::test]
async fn a_literal_env_var_reaches_the_child_despite_a_different_host_value() {
    let _env_lock = rpc_env_lock().await;
    let _rpc_timeout = RpcEnvGuard::remove();
    let tc = TestConductor::new();
    let executable = named_stub_copy(tc.path(), "print-env-tool");
    let output = tc.path().join("literal.txt");
    let _host_var = EnvVarGuard::set("MEDIAPM_T3_LITERAL_OVERRIDE", "host-value");

    let tool = ToolSpec {
        version: None,
        kind: ToolKindSpec::Executable {
            command: vec![
                executable.to_string_lossy().into_owned(),
                "--mode".into(),
                "print-env".into(),
                "--name".into(),
                "MEDIAPM_T3_LITERAL_OVERRIDE".into(),
                "--output".into(),
                output.to_string_lossy().into_owned(),
            ],
            env_vars: BTreeMap::from([(
                "MEDIAPM_T3_LITERAL_OVERRIDE".to_string(),
                "literal-value".to_string(),
            )]),
            success_codes: vec![0],
        },
        name: "env-probe".into(),
        inputs: BTreeMap::new(),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime::default(),
    };
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("env-probe".to_string(), tool)]),
        Vec::new(),
    ));

    let exit_code = tc
        .conductor()
        .run_tool_passthrough("env-probe", &[])
        .await
        .expect("a passthrough reports an exit code");
    let observed = std::fs::read_to_string(&output).unwrap_or_default();
    assert_eq!(
        (exit_code, observed.as_str()),
        (0, "literal-value"),
        "the tool's literal env_vars value is authoritative, so the child must receive it even \
         when the host holds a different value for the same name"
    );
    tc.shutdown().await;
}
