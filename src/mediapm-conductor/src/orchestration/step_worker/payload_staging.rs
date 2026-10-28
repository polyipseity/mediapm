//! Tests for the payload staging strategies a step may declare.
//!
//! Every step used to copy its tool's payload out of the CAS into its own
//! sandbox, so a workflow with sixteen steps wrote the same tool tree sixteen
//! times. The document's `payload_staging` decides what a step does instead:
//! execute from the provisioned tree, link that tree into the sandbox, or copy
//! it as before. These tests pin all three against one payload, and pin the
//! refusal that keeps a document with no usable root from running a tool.
//!
//! A test that runs a tool drives a `#!/bin/sh` fixture, so it compiles out on
//! Windows. The step that decides what the sandbox holds is not platform
//! specific, and `payload_staging_prepares_the_sandbox_on_every_platform`
//! covers it there.

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
use crate::state::{ConductorState, HashedValueRecord};

use super::executor::execute_step;
use super::sandbox::prepare_tool_payload;

/// The tool body that records its whole argument vector instead of one argument.
///
/// A companion path is an *argument*, so a fixture that asserted only one
/// argument could not tell where the others resolved.
const ARGV_SCRIPT: &str =
    "#!/bin/sh\nmkdir -p downloads\nprintf '%s\\n' \"$@\" > downloads/out.txt\n";

/// The archive companion's directory key, which the provisioner unpacks.
fn companion_dir_key() -> String {
    format!("deps/ffmpeg/{}/", host_platform_dir())
}

/// The path inside the archive companion's tree that a flag names.
fn companion_file_path_key() -> String {
    format!("deps/ffmpeg/{}/ffmpeg", host_platform_dir())
}

/// The file companion's content-map key.
fn companion_binary_key() -> String {
    format!("deps/deno/{}/deno", host_platform_dir())
}

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
    write_payload_file(payload_root, key, SCRIPT.as_bytes(), true)
}

/// Writes `bytes` at `key` under `payload_root`, executable where the platform
/// has a mode.
fn write_payload_file(payload_root: &Path, key: &str, bytes: &[u8], executable: bool) -> PathBuf {
    let path = payload_root.join(key);
    std::fs::create_dir_all(path.parent().expect("the payload path names a directory"))
        .expect("create the payload directory");
    std::fs::write(&path, bytes).expect("write the payload file");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if executable {
            let mut permissions = std::fs::metadata(&path).expect("payload metadata").permissions();
            permissions.set_mode(permissions.mode() | 0o700);
            std::fs::set_permissions(&path, permissions).expect("make the payload executable");
        }
    }
    #[cfg(not(unix))]
    {
        let _ = executable;
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

/// A step records the inputs its command staged, and nothing else.
///
/// `materialized_inputs` used to be a copy of the tool's content map — the
/// payload the provisioner handed the step — which no command ever read as an
/// input. What a selector stages is the fact the record exists for: the
/// destination in the sandbox and the object whose bytes were written there.
#[cfg(unix)]
#[tokio::test]
async fn a_step_records_the_inputs_its_command_stages() {
    let fixture = Fixture::with_script().await;
    let content_map = fixture.content_map().await;
    let first_hash = fixture
        .cas
        .put(Bytes::from_static(b"first-staged-bytes"))
        .await
        .expect("import the first input");
    let second_hash = fixture
        .cas
        .put(Bytes::from_static(b"second-staged-bytes"))
        .await
        .expect("import the second input");
    let mut tool = script_tool(content_map, &fixture.payload_root, PayloadStaging::InPlace);
    tool.command_parts = vec![
        script_key(),
        format!("${{external_data.{first_hash}:file(inputs/first.bin)}}"),
        format!("${{external_data.{second_hash}:file(inputs/second.bin)}}"),
    ];

    let (bundle, _sandbox) =
        run_step(&fixture, tool, "staged-input-step", "staged-input").await.expect("the step runs");

    assert_eq!(
        bundle.instance.materialized_inputs,
        BTreeMap::from([
            (
                "inputs/first.bin".to_string(),
                HashedValueRecord { hash: first_hash, deterministic: true },
            ),
            (
                "inputs/second.bin".to_string(),
                HashedValueRecord { hash: second_hash, deterministic: true },
            ),
        ]),
        "the record must name the destinations the command staged at and the objects staged \
         there, and carry no entry from the tool content map",
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
///
/// The rows compare execution, so they run on unix only; the staging step itself
/// is covered on every platform by
/// `payload_staging_prepares_the_sandbox_on_every_platform`.
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

/// Reads the argument vector the fixture script recorded.
///
/// The argument-writing script puts one argument per line, so the comparison can
/// say which argument resolved where instead of only that some argument did.
#[cfg(unix)]
fn recorded_arguments(sandbox: &Path) -> Vec<String> {
    let text = String::from_utf8(written_output(sandbox))
        .expect("the fixture script writes its arguments as text");
    text.lines().map(str::to_string).collect()
}

/// Builds a fixture whose payload root holds the tool and both shapes of
/// inlined companion: an archive unpacked under a directory key, and a file
/// under a file key.
///
/// The two argument spellings the fixture feeds the tool are the ones
/// `mediapm`'s yt-dlp spec emits (`--ffmpeg-location deps/ffmpeg/<os>/ffmpeg`
/// and `--js-runtimes deno:deps/deno/<os>/deno`). They are spelled out here
/// because this crate cannot depend on `mediapm` to import them, so a change to
/// those spellings has to be mirrored here.
///
/// These are the two shapes a reconciled content map carries, and a flag that
/// names a path inside an unpacked archive is the case a key-by-key comparison
/// would miss.
async fn fixture_with_companions() -> (Fixture, BTreeMap<String, String>) {
    let tmp = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let runtime_root = tmp.path().join("runtime");
    let payload_root = tmp.path().join("payload");
    let cas = InMemoryCas::new();

    write_payload_file(&payload_root, &script_key(), ARGV_SCRIPT.as_bytes(), true);
    // The provisioner unpacks an archive source under its directory key, so the
    // path a flag names inside it exists before any step runs.
    let archive = make_zip(&[("ffmpeg", b"companion-binary")]);
    let archive_hash = cas.put(Bytes::from(archive)).await.expect("import the archive");
    write_payload_file(&payload_root, &companion_file_path_key(), b"companion-binary", true);

    let deno_bytes = b"deno-binary";
    let deno_hash = cas.put(Bytes::from_static(deno_bytes)).await.expect("import the deno bytes");
    write_payload_file(&payload_root, &companion_binary_key(), deno_bytes, true);

    let tool_hash =
        cas.put(Bytes::from_static(ARGV_SCRIPT.as_bytes())).await.expect("import the tool body");
    let content_map = BTreeMap::from([
        (script_key(), tool_hash.to_hex()),
        (companion_dir_key(), archive_hash.to_hex()),
        (companion_binary_key(), deno_hash.to_hex()),
    ]);
    (Fixture { tmp, runtime_root, payload_root, cas }, content_map)
}

/// Builds the tool whose arguments name both companion shapes the way the
/// yt-dlp spec does: one as a bare path, one inside a `<runtime>:<path>` pair.
fn companion_argument_tool(
    content_map: BTreeMap<String, String>,
    payload_root: &Path,
    staging: PayloadStaging,
) -> UnifiedToolSpec {
    let mut tool = script_tool(content_map, payload_root, staging);
    tool.command_parts = vec![
        script_key(),
        "--ffmpeg-location".to_string(),
        companion_file_path_key(),
        "--js-runtimes".to_string(),
        format!("deno:{}", companion_binary_key()),
        "--marker".to_string(),
        "tail".to_string(),
    ];
    tool
}

/// `in_place` names every companion under the payload root.
///
/// A companion is named by an *argument*, not by the executable, and the sandbox
/// no longer holds the `deps/` tree it once did. A step that left those values
/// relative would hand the tool a path that does not exist, and the failure would
/// report itself as the tool's rather than as the document's.
#[cfg(unix)]
#[tokio::test]
async fn an_in_place_step_names_its_companions_absolutely() {
    let (fixture, content_map) = fixture_with_companions().await;
    let tool = companion_argument_tool(content_map, &fixture.payload_root, PayloadStaging::InPlace);
    let (bundle, sandbox) = run_step(&fixture, tool, "companions-in-place", "companions-in-place")
        .await
        .expect("the step runs with its companions");

    let root = fixture.payload_root.to_string_lossy().into_owned();
    assert_eq!(
        recorded_arguments(&sandbox),
        vec![
            "--ffmpeg-location".to_string(),
            format!("{root}/{}", companion_file_path_key()),
            "--js-runtimes".to_string(),
            format!("deno:{root}/{}", companion_binary_key()),
            "--marker".to_string(),
            "tail".to_string(),
        ],
        "an in-place step has to name each companion under the payload root, and has to leave a \
         value that names no payload entry alone",
    );
    assert!(
        !sandbox.join("deps").exists(),
        "in_place still must not copy any part of the payload into the sandbox",
    );
    assert!(
        bundle.instance.outputs.contains_key("out"),
        "the step has to run and capture its declared output",
    );
}

/// `hard_link` keeps the arguments the document wrote.
///
/// The tree is in the sandbox under exactly those names, so rewriting them would
/// point the tool at the provisioned tree instead of the staged one — and would
/// make the two strategies indistinguishable in the recorded arguments.
#[cfg(unix)]
#[tokio::test]
async fn a_hard_link_step_keeps_its_companion_arguments_relative() {
    let (fixture, content_map) = fixture_with_companions().await;
    let tool =
        companion_argument_tool(content_map, &fixture.payload_root, PayloadStaging::HardLink);
    let (_bundle, sandbox) =
        run_step(&fixture, tool, "companions-hard-link", "companions-hard-link")
            .await
            .expect("the step runs with its companions");

    assert_eq!(
        recorded_arguments(&sandbox),
        vec![
            "--ffmpeg-location".to_string(),
            companion_file_path_key(),
            "--js-runtimes".to_string(),
            format!("deno:{}", companion_binary_key()),
            "--marker".to_string(),
            "tail".to_string(),
        ],
        "a hard-linked step resolves the companions the document named, unchanged",
    );
    assert!(
        sandbox.join(companion_binary_key()).is_file(),
        "hard_link puts the companion tree in the sandbox under the key the document used",
    );
}

/// A comma-separated list of companions resolves every member.
///
/// A document may name several paths in one argument, and a comma ends one path
/// and begins the next, so a matcher that only accepted a separator before a
/// path would resolve the second member and leave the first relative.
#[tokio::test]
async fn a_comma_separated_list_of_companions_is_fully_qualified() {
    let (_fixture, content_map) = fixture_with_companions().await;
    let root = Path::new("/payload");
    let mut tool =
        companion_argument_tool(content_map, root, crate::config::PayloadStaging::InPlace);
    tool.command_parts = vec![
        script_key(),
        "--paths".to_string(),
        format!("{},{}", companion_file_path_key(), companion_binary_key()),
    ];

    let spec = tool.clone();
    super::sandbox::pre_qualify_payload_arguments(&mut tool.command_parts, &spec);

    assert_eq!(
        tool.command_parts[2],
        format!("/payload/{},/payload/{}", companion_file_path_key(), companion_binary_key()),
        "both members of a separated list name payload entries and both have to resolve",
    );
}

/// A value that names only a directory above a payload entry resolves too.
///
/// A tool takes a directory where another takes a file, so a value that spells
/// `deps/ffmpeg/<os>` — a tree the payload holds but the content map does not
/// name as a key — still has to point at the payload rather than at a sandbox the
/// `in_place` strategy left empty.
#[tokio::test]
async fn a_directory_that_holds_payload_entries_is_qualified() {
    let (_fixture, content_map) = fixture_with_companions().await;
    let root = Path::new("/payload");
    let mut tool =
        companion_argument_tool(content_map, root, crate::config::PayloadStaging::InPlace);
    let directory = format!("deps/ffmpeg/{}", host_platform_dir());
    tool.command_parts = vec![script_key(), "--dir".to_string(), directory.clone()];

    let spec = tool.clone();
    super::sandbox::pre_qualify_payload_arguments(&mut tool.command_parts, &spec);

    assert_eq!(
        tool.command_parts[2],
        format!("/payload/{directory}"),
        "a directory the payload holds has to resolve under the payload root",
    );
}

/// A companion spelled with backslashes resolves as the same tree.
///
/// Windows accepts both spellings of a separator, and a document written for it
/// may use either, so matching has to read the two the same way — while leaving
/// every byte of a value it does not rewrite exactly as written.
#[tokio::test]
async fn a_backslash_spelled_companion_is_qualified() {
    let (_fixture, content_map) = fixture_with_companions().await;
    let root = Path::new("/payload");
    let mut tool =
        companion_argument_tool(content_map, root, crate::config::PayloadStaging::InPlace);
    let spelled = companion_binary_key().replace('/', "\\");
    tool.command_parts = vec![script_key(), "--runtime".to_string(), spelled];

    let spec = tool.clone();
    super::sandbox::pre_qualify_payload_arguments(&mut tool.command_parts, &spec);

    assert_eq!(
        tool.command_parts[2],
        format!("/payload/{}", companion_binary_key()),
        "a backslash-spelled companion names the same payload entry and has to resolve",
    );
}

/// A drive-relative path is left alone.
///
/// `C:deps/deno/...` names a path relative to the working directory of the `C:`
/// drive, so the colon belongs to the path and not to a value separator.
/// Resolving it against the payload root would point the tool at a different
/// absolute path than the document meant.
#[tokio::test]
async fn a_drive_relative_path_is_not_qualified() {
    let (_fixture, content_map) = fixture_with_companions().await;
    let root = Path::new("/payload");
    let mut tool =
        companion_argument_tool(content_map, root, crate::config::PayloadStaging::InPlace);
    let drive_relative = format!("C:{}", companion_binary_key());
    tool.command_parts = vec![script_key(), "--runtime".to_string(), drive_relative.clone()];

    let spec = tool.clone();
    super::sandbox::pre_qualify_payload_arguments(&mut tool.command_parts, &spec);

    assert_eq!(
        tool.command_parts[2], drive_relative,
        "a drive-relative value names a path of its own and must not be rewritten",
    );
}

/// The staging step itself is platform-neutral, and this pins each strategy's
/// topology without executing anything.
///
/// The execution-based tests above drive a `#!/bin/sh` fixture, so they compile
/// out on Windows. The step that decides what the sandbox holds is not platform
/// specific, so it is checked here on every platform: `copy` writes the content
/// map, `in_place` writes nothing into the sandbox, and `hard_link` stages the
/// payload without going through the CAS.
#[tokio::test]
async fn payload_staging_prepares_the_sandbox_on_every_platform() {
    let fixture = Fixture::with_script().await;
    let content_map = fixture.content_map().await;

    for staging in [PayloadStaging::InPlace, PayloadStaging::HardLink, PayloadStaging::Copy] {
        let tool = script_tool(content_map.clone(), &fixture.payload_root, staging);
        let sandbox = fixture.tmp.path().join(format!("sandbox-{}", staging.as_str()));
        std::fs::create_dir_all(&sandbox).expect("create the sandbox");
        prepare_tool_payload(&fixture.cas, &tool, &sandbox)
            .await
            .expect("staging a payload root this fixture holds succeeds");
        let staged = sandbox.join(script_key());
        match staging {
            PayloadStaging::InPlace => assert!(
                !staged.exists(),
                "in_place stages nothing: the tool runs where the provisioner put it",
            ),
            PayloadStaging::HardLink => assert_eq!(
                std::fs::read(&staged).expect("the linked payload is readable"),
                SCRIPT.as_bytes(),
                "hard_link stages the payload bytes into the sandbox",
            ),
            PayloadStaging::Copy => assert_eq!(
                std::fs::read(&staged).expect("the copied payload is readable"),
                SCRIPT.as_bytes(),
                "copy stages the payload bytes into the sandbox",
            ),
        }
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

    // Each label carries the phrase the pre-execution check emits for that root,
    // so the assertion cannot be satisfied by a later diagnostic: the resolver
    // says "is not a file under its payload_root", which none of these match.
    for (label, root, expected) in [
        ("empty", String::new(), "records no payload_root"),
        ("relative", "relative/payload".to_string(), "is not an absolute path"),
        ("file", not_a_directory.to_string_lossy().into_owned(), "is not a directory"),
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
            assert!(
                message.contains(expected),
                "the {label} root diagnostic for {staging:?} has to be the one the check \
                 before execution emits ('{expected}'): {message}",
            );
            assert!(
                message.contains(staging.as_str()),
                "the {label} root diagnostic for {staging:?} has to name the strategy that \
                 needs the root: {message}",
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
    assert!(
        message.contains("does not hold the content-map entry"),
        "the step has to be refused by the check before execution, not by the resolver \
         afterwards: {message}",
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
        assert!(
            message.contains("does not hold the content-map entry"),
            "the {staging:?} step has to be refused before it runs, not fail as the tool: \
             {message}",
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
