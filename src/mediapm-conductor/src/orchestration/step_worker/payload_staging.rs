//! Tests for the payload staging strategies a step may declare.
//!
//! Every step used to copy its tool's payload out of the CAS into its own
//! sandbox, so a workflow with sixteen steps wrote the same tool tree sixteen
//! times. The document's `payload_staging` decides what a step does instead:
//! execute from the provisioned tree, link that tree into the sandbox, or copy
//! it as before. These tests pin all three against one payload, and pin the
//! refusal that keeps a document with no usable root from running a tool.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use mediapm_cas::{CasApi, InMemoryCas};

use crate::config::{
    ConductorRuntimeConfig, OutputCaptureSpec, PayloadStaging, SaveMode, WorkflowStepSpec,
};
use crate::error::ConductorError;
use crate::orchestration::protocol::{
    StepExecutionBundle, StepExecutionRequest, UnifiedNickelDocument, UnifiedToolSpec,
};
use crate::state::ConductorState;

use super::executor::execute_step;

/// The tool body every fixture runs: it writes its argument into a
/// sandbox-relative path, which is what the capture phase reads.
const SCRIPT: &str = "#!/bin/sh\nmkdir -p downloads\nprintf '%s' \"$1\" > downloads/out.txt\n";

/// The argument the script echoes, so every strategy can be compared by bytes.
const ARGUMENT: &str = "payload-bytes";

/// The top-level payload directory this host executes from.
///
/// A provisioned payload carries every supported platform, and the linker keeps
/// only the host's directory, so a fixture has to put its tool where a real one
/// would be for the host it runs on.
fn host_platform_dir() -> &'static str {
    if cfg!(target_os = "macos") {
        "macos"
    } else if cfg!(target_os = "windows") {
        "windows"
    } else {
        "linux"
    }
}

/// The content-map key of the fixture script.
fn script_key() -> String {
    format!("{}/tool.sh", host_platform_dir())
}

/// The content-map key of the inlined companion script.
///
/// `inline_same_step_deps` copies a `SameStep` companion's payload into the
/// requester's content map under `deps/<tool id>/`, keeping the companion's own
/// key, so this is the shape a document carries when the executable it names is
/// a companion rather than the tool's own binary.
fn companion_key() -> String {
    format!("deps/companion/{}", script_key())
}

/// One run's filesystem: the conductor tmp root, the provisioned payload root,
/// and the CAS the payload was imported into.
struct Fixture {
    /// Kept alive for the whole test: the runtime root and the payload live
    /// under it. Read directly by a test that needs a path outside both.
    tmp: tempfile::TempDir,
    /// Runtime root handed to the worker as `conductor_tmp_dir`.
    runtime_root: PathBuf,
    /// Directory the provisioner would have filled.
    payload_root: PathBuf,
    /// Store the payload bytes were imported into.
    cas: InMemoryCas,
}

impl Fixture {
    /// Builds a fixture whose payload root already holds `linux/tool.sh`.
    async fn with_script() -> Self {
        let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let runtime_root = tmp.path().join("runtime");
        let payload_root = tmp.path().join("payload");
        let cas = InMemoryCas::new();
        write_script(&payload_root);
        let script =
            std::fs::read(payload_root.join(script_key())).expect("the fixture script is readable");
        cas.put(Bytes::from(script)).await.expect("import the fixture script");
        Self { tmp, runtime_root, payload_root, cas }
    }

    /// The content map that names the fixture script, which is also the payload
    /// entry each strategy has to place where the command expects it.
    async fn content_map(&self) -> BTreeMap<String, String> {
        self.content_map_for(&[script_key()]).await
    }

    /// The content map naming each of `keys`, with the bytes each payload file
    /// holds imported into the CAS first.
    ///
    /// A real content map names every entry the payload root carries, so a test
    /// that gives a tool more than its own binary builds its map the same way
    /// the reconciler does.
    async fn content_map_for(&self, keys: &[String]) -> BTreeMap<String, String> {
        let mut map = BTreeMap::new();
        for key in keys {
            let bytes = std::fs::read(self.payload_root.join(key))
                .expect("the fixture payload file is readable");
            let hash = self.cas.put(Bytes::from(bytes)).await.expect("import the fixture payload");
            map.insert(key.clone(), hash.to_hex());
        }
        map
    }
}

/// Writes the fixture script under its own key, executable where the platform
/// has a mode.
fn write_script(payload_root: &Path) -> PathBuf {
    write_script_at(payload_root, &script_key())
}

/// Writes the fixture script at `key` under `payload_root`.
///
/// A companion lives under a `deps/`-prefixed key, so the key is a parameter
/// rather than a constant: the writer has to be able to produce the whole tree
/// a reconciled payload root holds.
fn write_script_at(payload_root: &Path, key: &str) -> PathBuf {
    let path = payload_root.join(key);
    std::fs::create_dir_all(path.parent().expect("the script path names a directory"))
        .expect("create the payload directory");
    std::fs::write(&path, SCRIPT).expect("write the fixture script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = std::fs::metadata(&path).expect("script metadata").permissions();
        permissions.set_mode(permissions.mode() | 0o700);
        std::fs::set_permissions(&path, permissions).expect("make the script executable");
    }
    path
}

/// Builds the tool spec a fixture runs under `staging`.
fn script_tool(
    content_map: BTreeMap<String, String>,
    payload_root: &Path,
    staging: PayloadStaging,
) -> UnifiedToolSpec {
    UnifiedToolSpec {
        name: "script".to_string(),
        version: None,
        is_impure: false,
        max_concurrent_calls: 0,
        max_retries: 0,
        inputs: BTreeMap::new(),
        default_inputs: BTreeMap::new(),
        command_parts: vec![script_key(), ARGUMENT.to_string()],
        success_codes: vec![0],
        execution_env_vars: BTreeMap::new(),
        outputs: BTreeMap::from([(
            "out".to_string(),
            OutputCaptureSpec {
                name: "out".to_string(),
                capture: "file:downloads/out.txt".to_string(),
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: true,
            },
        )]),
        tool_content_map: content_map,
        payload_root: payload_root.to_string_lossy().into_owned(),
        payload_staging: staging,
        builtin_id: None,
    }
}

/// Runs one `script` step against `fixture` and answers its bundle.
///
/// `variant` becomes part of the tool key, which is the tool-call id the
/// instance key derives from. Two runs of the same work in one fixture share an
/// instance key, and with it a sandbox, so a test that compares topologies names
/// the runs apart.
async fn run_step(
    fixture: &Fixture,
    tool: UnifiedToolSpec,
    step_id: &str,
    variant: &str,
) -> Result<(StepExecutionBundle, PathBuf), ConductorError> {
    let request = StepExecutionRequest {
        unified: Arc::new(UnifiedNickelDocument {
            tools: BTreeMap::from([(format!("script@{variant}"), tool)]),
            workflows: BTreeMap::new(),
            tool_content_hashes: BTreeSet::new(),
            external_data_policies: BTreeMap::new(),
            runtime: ConductorRuntimeConfig::default(),
        }),
        step: WorkflowStepSpec {
            id: step_id.to_string(),
            tool: "script".to_string(),
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            max_retries: 0,
            depends_on: Vec::new(),
        },
        impure_timestamp: None,
        state_snapshot: Arc::new(ConductorState::new_empty()),
        outermost_config_dir: fixture.runtime_root.clone(),
        conductor_tmp_dir: fixture.runtime_root.clone(),
        step_outputs: Arc::new(BTreeMap::new()),
        required_output_names: BTreeSet::from(["out".to_string()]),
        force_rerun: false,
    };
    let bundle = execute_step(&fixture.cas, request).await?;
    // The sandbox is named for the instance key, so the answer names the tree the
    // run actually used rather than whichever tree a directory scan finds first.
    let sandbox = fixture
        .runtime_root
        .join("sandbox")
        .join(super::sandbox::sanitize_for_path(&bundle.instance.instance_key.to_hex()));
    Ok((bundle, sandbox))
}

/// Reads the file the fixture script wrote, which is the run's observable
/// result under every strategy.
fn written_output(sandbox: &Path) -> Vec<u8> {
    std::fs::read(sandbox.join("downloads").join("out.txt"))
        .expect("the script writes its argument into the sandbox")
}

/// The inode a path resolves to, where the platform has one.
#[cfg(unix)]
fn inode(path: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).expect("path metadata").ino()
}

/// `in_place` runs the tool where the provisioner put it.
///
/// The sandbox holds nothing from the content map — that is the whole point of
/// the strategy — while the command still resolves, so a step's inputs and
/// outputs keep their sandbox-relative names.
#[cfg(unix)]
#[tokio::test]
async fn a_step_executes_the_provisioned_payload() {
    let fixture = Fixture::with_script().await;
    let content_map = fixture.content_map().await;
    let tool = script_tool(content_map, &fixture.payload_root, PayloadStaging::InPlace);

    let (bundle, sandbox) =
        run_step(&fixture, tool, "in-place-step", "in-place").await.expect("the step runs");
    assert!(!bundle.cache_hit, "a first run of this work has no instance to reuse");
    assert!(
        !sandbox.join(host_platform_dir()).exists(),
        "an in-place step must not copy the payload into its sandbox",
    );
    assert_eq!(
        written_output(&sandbox),
        ARGUMENT.as_bytes(),
        "the payload binary must still run, or the strategy saved bytes by not working",
    );
    assert!(
        fixture.payload_root.join(script_key()).is_file(),
        "the provisioned payload has to survive the run it served",
    );
}

/// A relative output path stays relative to the sandbox under `in_place`.
///
/// The tool's working directory is the sandbox, not the payload root: a tool
/// that wrote `downloads/x` next to its own binary would put run output inside
/// the provisioned tree, where the next step would find it.
#[cfg(unix)]
#[tokio::test]
async fn a_relative_output_still_lands_in_the_sandbox() {
    let fixture = Fixture::with_script().await;
    let content_map = fixture.content_map().await;
    let tool = script_tool(content_map, &fixture.payload_root, PayloadStaging::InPlace);

    let (_bundle, sandbox) = run_step(&fixture, tool, "relative-output-step", "relative-output")
        .await
        .expect("the step runs");
    assert_eq!(written_output(&sandbox), ARGUMENT.as_bytes());
    assert!(
        !fixture.payload_root.join("downloads").exists(),
        "the payload directory must not collect the run's outputs",
    );
}

/// A hard-linked payload shares the provisioned inode instead of copying bytes.
///
/// The strategy exists to keep the payload out of the CAS-read path per step:
/// a link costs an entry, a copy costs the bytes.
#[cfg(unix)]
#[tokio::test]
async fn a_hard_linked_payload_shares_the_payload_inode() {
    let fixture = Fixture::with_script().await;
    let content_map = fixture.content_map().await;
    let tool = script_tool(content_map, &fixture.payload_root, PayloadStaging::HardLink);

    let (_bundle, sandbox) =
        run_step(&fixture, tool, "hard-link-step", "hard-link").await.expect("the step runs");
    let staged = sandbox.join(script_key());
    assert_eq!(
        inode(&staged),
        inode(&fixture.payload_root.join(script_key())),
        "a hard-linked payload has to share the provisioned inode",
    );
    assert_eq!(written_output(&sandbox), ARGUMENT.as_bytes());
}

/// An archive payload is unpacked into the payload root once, not per step.
///
/// The content map names a directory key, which the `copy` strategy unpacks into
/// the sandbox on every step. Under `hard_link` the provisioner's unpacked tree
/// is linked, so a second step extracts nothing and both sandboxes hold the same
/// inode.
#[cfg(unix)]
#[tokio::test]
async fn an_archive_payload_is_unpacked_once() {
    let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let runtime_root = tmp.path().join("runtime");
    let payload_root = tmp.path().join("payload");
    let cas = InMemoryCas::new();

    // What the provisioner leaves behind for a directory key: the archive is
    // unpacked under the key's relative directory.
    let archive = make_zip(&[("tool.sh", SCRIPT.as_bytes())]);
    let archive_hash = cas.put(Bytes::from(archive.clone())).await.expect("import the archive");
    mediapm_conductor_builtin_archive::unpack_zip_bytes_to_directory(
        &archive,
        &payload_root.join(host_platform_dir()),
    )
    .expect("unpack the payload the provisioner would have unpacked");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let unpacked = payload_root.join(script_key());
        let mut permissions = std::fs::metadata(&unpacked).expect("metadata").permissions();
        permissions.set_mode(permissions.mode() | 0o700);
        std::fs::set_permissions(&unpacked, permissions).expect("make the script executable");
    }
    let fixture = Fixture { tmp, runtime_root, payload_root, cas };
    let dir_key = format!("{}/", host_platform_dir());
    let content_map = BTreeMap::from([(dir_key, archive_hash.to_hex())]);

    for step_id in ["archive-step-a", "archive-step-b"] {
        let tool =
            script_tool(content_map.clone(), &fixture.payload_root, PayloadStaging::HardLink);
        let (_bundle, sandbox) =
            run_step(&fixture, tool, step_id, step_id).await.expect("the step runs");
        let staged = sandbox.join(script_key());
        assert_eq!(
            inode(&staged),
            inode(&fixture.payload_root.join(script_key())),
            "step {step_id} has to link the unpacked payload rather than extract it again",
        );
        assert_eq!(written_output(&sandbox), ARGUMENT.as_bytes());
    }

    let entries = std::fs::read_dir(fixture.payload_root.join(host_platform_dir()))
        .expect("the payload directory is readable")
        .count();
    assert_eq!(entries, 1, "two steps must not add anything to the payload tree");
}

/// All three strategies produce the same bytes, by the topology each declares.
///
/// This is the test that keeps `copy` honest: it stays in the enum, so a
/// strategy that misbehaves on a platform still has a covered alternative, and
/// every alternative has to produce what `copy` produced.
#[cfg(unix)]
#[tokio::test]
async fn payload_staging_matrix() {
    let payload = {
        let fixture = Fixture::with_script().await;
        let content_map = fixture.content_map().await;
        // Warm the CAS with the same bytes every strategy will see.
        fixture.cas.put(Bytes::from_static(SCRIPT.as_bytes())).await.expect("import script");
        (fixture, content_map)
    };
    let (fixture, content_map) = payload;

    let mut produced: Vec<(PayloadStaging, Vec<u8>)> = Vec::new();
    for (index, staging) in
        [PayloadStaging::InPlace, PayloadStaging::HardLink, PayloadStaging::Copy]
            .into_iter()
            .enumerate()
    {
        let tool = script_tool(content_map.clone(), &fixture.payload_root, staging);
        let step_id = format!("matrix-step-{index}");
        let (bundle, sandbox) =
            run_step(&fixture, tool, &step_id, staging.as_str()).await.expect("the step runs");
        let staged = sandbox.join(script_key());

        match staging {
            PayloadStaging::InPlace => {
                assert!(!staged.exists(), "in_place writes nothing into the sandbox for the tool");
            }
            PayloadStaging::HardLink => assert_eq!(
                inode(&staged),
                inode(&fixture.payload_root.join(script_key())),
                "hard_link shares the provisioned inode",
            ),
            PayloadStaging::Copy => {
                assert_eq!(
                    std::fs::read(&staged).expect("the copy is readable"),
                    SCRIPT.as_bytes(),
                    "copy writes the content map into the sandbox",
                );
                assert_ne!(
                    inode(&staged),
                    inode(&fixture.payload_root.join(script_key())),
                    "copy must not share the provisioned inode, or it is not the copy path",
                );
            }
        }

        assert!(
            bundle.instance.outputs.contains_key("out"),
            "every strategy has to capture the declared output, not only run the tool",
        );
        produced.push((staging, written_output(&sandbox)));
    }

    let (_, expected) = produced.first().expect("the matrix runs at least once").clone();
    for (staging, bytes) in &produced {
        assert_eq!(
            *bytes, expected,
            "{staging:?} has to produce the bytes every other strategy produces",
        );
    }
}

/// An inlined companion stages like the tool's own payload under every
/// strategy.
///
/// `inline_same_step_deps` copies a `SameStep` companion's payload into the
/// requester's content map under `deps/<tool id>/`, and a requester whose
/// executable is a companion names that key in `command_parts[0]`. A strategy
/// that resolved only the tool's own entries would leave the companion
/// unreachable, and the step would fail with "not found" for a payload the
/// document had declared.
#[cfg(unix)]
#[tokio::test]
async fn a_deps_prefixed_payload_stages_under_every_strategy() {
    let fixture = Fixture::with_script().await;
    write_script_at(&fixture.payload_root, &companion_key());
    let content_map = fixture.content_map_for(&[script_key(), companion_key()]).await;

    let mut produced: Vec<(PayloadStaging, Vec<u8>)> = Vec::new();
    for staging in [PayloadStaging::InPlace, PayloadStaging::HardLink, PayloadStaging::Copy] {
        let mut tool = script_tool(content_map.clone(), &fixture.payload_root, staging);
        tool.command_parts[0].clone_from(&companion_key());
        let step_id = format!("deps-step-{}", staging.as_str());
        let (bundle, sandbox) = run_step(&fixture, tool, &step_id, &step_id)
            .await
            .expect("the step runs with its companion payload");
        let staged = sandbox.join(companion_key());

        match staging {
            PayloadStaging::InPlace => assert!(
                !sandbox.join("deps").exists(),
                "an in-place step runs the companion where the provisioner put it",
            ),
            PayloadStaging::HardLink => assert_eq!(
                inode(&staged),
                inode(&fixture.payload_root.join(companion_key())),
                "hard_link links the companion tree as well as the tool's own",
            ),
            PayloadStaging::Copy => {
                assert_eq!(
                    std::fs::read(&staged).expect("the copied companion is readable"),
                    SCRIPT.as_bytes(),
                    "copy writes the companion into the sandbox",
                );
                assert_ne!(
                    inode(&staged),
                    inode(&fixture.payload_root.join(companion_key())),
                    "copy must not share the companion's inode, or it is not the copy path",
                );
            }
        }

        assert!(
            bundle.instance.outputs.contains_key("out"),
            "{staging:?} has to run the companion and capture the declared output",
        );
        produced.push((staging, written_output(&sandbox)));
    }

    let (_, expected) = produced.first().expect("the matrix runs at least once").clone();
    for (staging, bytes) in &produced {
        assert_eq!(*bytes, expected, "{staging:?} has to produce the bytes its companions produce");
    }
}

/// A strategy that reads a payload root refuses a document that names no usable
/// one, naming the tool and the path.
///
/// The check runs before the step, so the failure says which tool and which path
/// the document got wrong; a run that fell through to the command would report
/// "not found" and send the reader to the tool instead of to the document.
#[cfg(unix)]
#[tokio::test]
async fn strict_a_missing_payload_root_fails_before_execution() {
    let fixture = Fixture::with_script().await;
    let content_map = fixture.content_map().await;
    let not_a_directory = fixture.payload_root.join(script_key());

    for (label, root) in [
        ("empty", String::new()),
        ("relative", "relative/payload".to_string()),
        ("file", not_a_directory.to_string_lossy().into_owned()),
    ] {
        for staging in [PayloadStaging::InPlace, PayloadStaging::HardLink] {
            let mut tool = script_tool(content_map.clone(), &fixture.payload_root, staging);
            tool.payload_root.clone_from(&root);
            let step_id = format!("strict-root-{label}-{}", staging.as_str());
            let error = run_step(&fixture, tool, &step_id, &step_id)
                .await
                .expect_err("a strategy that needs a payload root must refuse one it cannot use");
            let message = error.to_string();
            assert!(
                message.contains("script"),
                "the {label} root diagnostic for {staging:?} has to name the tool: {message}",
            );
            assert!(
                message.contains("payload_root"),
                "the {label} root diagnostic for {staging:?} has to name the field: {message}",
            );
            if !root.is_empty() {
                assert!(
                    message.contains(&root),
                    "the {label} root diagnostic for {staging:?} has to name the path it \
                     looked at: {message}",
                );
            }
        }
    }
}

/// A payload root that exists but lacks a declared entry fails the step.
///
/// Executing whatever happens to be there is the failure this refuses: the tool
/// would run with the wrong tree behind it, and the run would report a tool
/// defect rather than a document one.
#[cfg(unix)]
#[tokio::test]
async fn strict_an_incomplete_payload_root_fails_before_execution() {
    let fixture = Fixture::with_script().await;
    let content_map = fixture.content_map().await;
    let empty_root = fixture.tmp.path().join("payload-without-the-script");
    std::fs::create_dir_all(empty_root.join(host_platform_dir()))
        .expect("create an empty payload root");

    let tool = script_tool(content_map, &empty_root, PayloadStaging::InPlace);
    let error = run_step(&fixture, tool, "strict-incomplete-root", "strict-incomplete")
        .await
        .expect_err("a payload root that lacks a declared entry must fail before the step runs");
    let message = error.to_string();
    assert!(message.contains("script"), "the diagnostic has to name the tool: {message}");
    assert!(
        message.contains(&script_key()),
        "the diagnostic has to name the entry that is missing: {message}",
    );
    assert!(
        message.contains(&empty_root.to_string_lossy().to_string()),
        "the diagnostic has to name the root it searched: {message}",
    );
}

/// A payload root that holds the tool's own script but not its inlined
/// companion fails before the step runs, naming the missing entry.
///
/// The completeness check reads every content-map key, not only the one the
/// command names: a companion the root lacks is a payload the document declared
/// and the disk does not hold, and finding that out before the step runs is what
/// keeps the failure attributable to the document.
#[cfg(unix)]
#[tokio::test]
async fn strict_a_missing_deps_entry_fails_before_execution() {
    let fixture = Fixture::with_script().await;
    // The companion's bytes are in the CAS, as a reconciled map would have them,
    // while the payload root holds only the tool's own entry.
    let companion_hash = fixture
        .cas
        .put(Bytes::from_static(SCRIPT.as_bytes()))
        .await
        .expect("import the companion script");
    let mut content_map = fixture.content_map().await;
    content_map.insert(companion_key(), companion_hash.to_hex());
    let expected_path = fixture.payload_root.join(companion_key());

    for staging in [PayloadStaging::InPlace, PayloadStaging::HardLink] {
        let tool = script_tool(content_map.clone(), &fixture.payload_root, staging);
        let step_id = format!("strict-deps-{}", staging.as_str());
        let error = run_step(&fixture, tool, &step_id, &step_id)
            .await
            .expect_err("a payload root missing a declared companion must fail the step");
        let message = error.to_string();
        assert!(
            message.contains(&companion_key()),
            "the {staging:?} diagnostic has to name the entry that is missing: {message}",
        );
        assert!(
            message.contains(&expected_path.to_string_lossy().to_string()),
            "the {staging:?} diagnostic has to name where it looked: {message}",
        );
    }
}

/// Creates a small uncompressed ZIP with the given entries.
///
/// The payload of a directory key is an uncompressed archive, which is what the
/// provisioner unpacks and what these fixtures hand it.
fn make_zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
    use std::io::Write;
    let cursor = std::io::Cursor::new(Vec::new());
    let mut writer = zip::ZipWriter::new(cursor);
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (name, content) in entries {
        writer.start_file(*name, options).expect("the ZIP entry starts");
        writer.write_all(content).expect("the ZIP entry is written");
    }
    writer.finish().expect("the ZIP is finished").into_inner()
}
