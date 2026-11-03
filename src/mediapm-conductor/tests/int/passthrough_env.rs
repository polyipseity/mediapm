//! `conductor tool run` applies the tool's environment to the process it spawns.
//!
//! The passthrough path spawns a tool without a step behind it, so the proof is
//! the same child-process trick the step-path test uses: a copy of the stub
//! named `yt-dlp` reads the manifest path from `MEDIAPM_STUB_MANIFEST` and
//! writes the manifest's destination. The only place that name is stated is the
//! tool's `env_vars` in the written document, so a passthrough that applies no
//! environment leaves the stub with nothing to read and it answers its own
//! usage failure without writing the destination.
//!
//! The document is the only source of that name here: the manifest path is the
//! copy's own path under this test's tempdir, and nothing installs the variable
//! in the test process.

use std::collections::BTreeMap;

use super::tool_env_vars::named_stub_copy;
use crate::{TestConductor, doc_with_workflows};
use mediapm_conductor::{ToolKindSpec, ToolRuntime, ToolSpec};
use mediapm_test_support::MANIFEST_ENV;

/// A passthrough run gets the environment the document's tool spec states.
///
/// The proof is a real child process writing a file no other surface names:
/// the stub emits the destinations of the manifest `MEDIAPM_STUB_MANIFEST`
/// points at, and the only place that variable is stated is the tool's
/// `env_vars`. The exit code and the emitted bytes are asserted together so a
/// run that failed and one that wrote the wrong bytes are distinguishable, and
/// the failure message names the variable a missing literal leaves unset.
#[tokio::test]
async fn a_passthrough_run_applies_the_tools_environment() {
    // The child inherits the conductor's environment, so an ambient value would
    // make this test pass with the env application reverted. Fail loudly
    // instead, because the proof is that the document is the only source of
    // the name.
    assert!(
        std::env::var(MANIFEST_ENV).is_err(),
        "{MANIFEST_ENV} is set in this test process, so the child would inherit it and the \
         document's literal would prove nothing; unset it and re-run"
    );

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
    tc.shutdown().await;
}
