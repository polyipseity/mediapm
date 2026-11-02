//! The offline tool chain, its provisioned payloads, and the mock tagger backend.
//!
//! Every test here runs the shape a hermetic run runs: a copy of the stub under
//! a real tool's name, with the command line that tool's preset renders, and the
//! environment the chain publishes. Nothing is asserted from an exit code alone —
//! a copy that succeeded without doing anything would pass that — so each test
//! also reads what the invocation produced or the record it left.

use std::path::{Path, PathBuf};

use mediapm_test_support::mock_musicbrainz::{
    ARTIST_NAME, MockMusicBrainz, RECORDING_MBID, RECORDING_TITLE, RELEASE_MBID,
};
use mediapm_test_support::stub_media::{
    CHAIN_TOOLS, INVOCATION_RECORD_ENV, MANIFEST_ENV, mount_managed_tool_upstreams,
    mount_offline_tool_chain,
};
use mediapm_test_support::{MockUpstream, media_fixture_path, stub_tool_path};

/// The media file the emitted artifact carries, resolved from this crate's
/// manifest directory.
///
/// `media_fixture_path` is given a manifest directory by its callers, and this
/// crate's own `CARGO_MANIFEST_DIR` is the one the relative path is written
/// against (`tests/fixtures/media/demo.mkv` under the `mediapm` crate, reached
/// here through the path helper's own relative walk).
fn fixture() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // `media_fixture_path` resolves `tests/fixtures/media/demo.mkv` under the
    // directory it is given. This crate does not hold that file; the mediapm
    // crate does, so the helper is pointed at its sibling.
    media_fixture_path(&manifest_dir.join("..").join("mediapm"))
}

/// Runs `program` with `args` and `env` in `cwd`, returning its exit code,
/// stdout and stderr.
///
/// The environment is added to the inherited one rather than replacing it,
/// which is how the conductor spawns a tool.
fn run(program: &Path, args: &[String], env: &[(&str, &str)], cwd: &Path) -> (i32, String, String) {
    let mut command = std::process::Command::new(program);
    command.args(args).current_dir(cwd);
    for (name, value) in env {
        command.env(name, value);
    }
    let output = command.output().expect("the chain executable must be runnable");
    (
        output.status.code().expect("the executable must exit with a code, not a signal"),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// Creates `sandbox` and writes each `(relative path, bytes)` under it.
fn stage_sandbox(sandbox: &Path, staged: &[(&str, &[u8])]) {
    std::fs::create_dir_all(sandbox).expect("create the sandbox");
    for (relative, body) in staged {
        let path = sandbox.join(relative);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create the staged input directory");
        }
        std::fs::write(&path, body).expect("write the staged input");
    }
}

/// One chain executable's own invocation shape, as the preset builders render it.
struct ToolShape {
    /// The tool the executable stands in for.
    tool: &'static str,
    /// The arguments the command line carries.
    args: Vec<String>,
    /// Sandbox-relative files the command line reads, with their bytes.
    staged: Vec<(&'static str, &'static [u8])>,
}

/// The command line each chain executable is invoked with, taken from the
/// builder that renders it for the demo's steps.
///
/// `src/mediapm/src/tools/workflows/yt_dlp.rs` renders the yt-dlp command as the
/// preset's `--no-progress --no-part` head, the step's option tokens, and the
/// source URL last; `ffmpeg.rs` renders the input slots, the option tokens
/// `FFMPEG_STATIC_DEFAULTS` supplies for a step that sets none, and the output
/// path last; `sd.rs` renders the pattern, the replacement and the staged
/// ffmetadata file, which sd rewrites in place; `rsgain.rs` renders its
/// `custom` mode with the demo's loudness options and the staged audio file
/// last; `deno.rs` renders the provisioned path alone; and
/// `try_fetch_local_source_metadata_with_ffprobe` in `src/mediapm/src/source_metadata.rs`
/// renders ffprobe's probe arguments followed by the file to probe.
fn tool_shapes() -> Vec<ToolShape> {
    let fixture = fixture();
    vec![
        ToolShape {
            tool: "yt-dlp",
            args: [
                "--no-progress",
                "--no-part",
                "-f",
                "best[height<=144]/best[height<=240]/worst",
                "--paths",
                "downloads",
                "--output",
                "%(title)s [%(id)s]%(playlist_index|)s__mediapm__.%(ext)s",
                "--sub-langs",
                "en",
                "--write-auto-subs",
                "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
            ]
            .iter()
            .map(|arg| (*arg).to_string())
            .collect(),
            staged: Vec::new(),
        },
        ToolShape {
            tool: "ffmpeg",
            args: [
                "-y",
                "-i",
                "inputs/input-0.bin",
                "-c",
                "copy",
                "-hide_banner",
                "-map_chapters",
                "0",
                "-map_metadata",
                "0",
                "-start_at_zero",
                "output-0.mkv",
            ]
            .iter()
            .map(|arg| (*arg).to_string())
            .collect(),
            staged: vec![("inputs/input-0.bin", b"media-bytes")],
        },
        ToolShape {
            tool: "ffprobe",
            args: [
                "-v",
                "error",
                "-print_format",
                "json",
                "-show_format",
                "-show_streams",
                fixture.to_str().expect("utf-8 fixture path"),
            ]
            .iter()
            .map(|arg| (*arg).to_string())
            .collect(),
            staged: Vec::new(),
        },
        ToolShape {
            tool: "rsgain",
            args: [
                "custom",
                "--tagmode",
                "i",
                "--loudness",
                "-18",
                "--clip-mode",
                "p",
                "--true-peak",
                "--max-peak",
                "0",
                "--preserve-mtimes",
                "inputs/input.flac",
            ]
            .iter()
            .map(|arg| (*arg).to_string())
            .collect(),
            staged: vec![("inputs/input.flac", b"audio-bytes")],
        },
        ToolShape {
            tool: "sd",
            args: ["(?i)REPLAYGAIN_", "replaygain_", "inputs/input.ffmeta"]
                .iter()
                .map(|arg| (*arg).to_string())
                .collect(),
            staged: vec![("inputs/input.ffmeta", b";FFMETADATA1\ntitle=Never Gonna Give You Up\n")],
        },
        ToolShape { tool: "deno", args: Vec::new(), staged: Vec::new() },
    ]
}

/// Every chain executable exists, answers its own invocation shape, and is
/// recorded as the tool it stands in for.
#[test]
fn every_chain_executable_answers_its_own_invocation_shape() {
    let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let chain = mount_offline_tool_chain(dir.path(), &fixture());
    let env = chain.env_pairs();
    let env: Vec<(&str, &str)> = env.iter().map(|(name, value)| (*name, value.as_str())).collect();

    assert_eq!(
        chain.executables().map(|(tool, _)| tool).collect::<Vec<&str>>(),
        CHAIN_TOOLS.to_vec(),
        "the chain must expose one executable per tool",
    );

    for shape in tool_shapes() {
        let sandbox = dir.path().join("sandbox").join(shape.tool);
        stage_sandbox(&sandbox, &shape.staged);
        let executable =
            chain.executable(shape.tool).expect("the chain must hold every tool's executable");

        let (code, stdout, stderr) = run(executable, &shape.args, &env, &sandbox);

        assert_eq!(code, 0, "{} must answer its own command line: {stderr}", shape.tool);
        assert!(stderr.is_empty(), "{} must not write to stderr: {stderr}", shape.tool);
        if shape.tool != "ffprobe" {
            assert!(stdout.is_empty(), "{} must not write to stdout: {stdout}", shape.tool);
        }
    }

    assert_eq!(
        chain
            .invocations()
            .iter()
            .map(|invocation| invocation.tool.as_str())
            .collect::<Vec<&str>>(),
        CHAIN_TOOLS.to_vec(),
        "every executable must be recorded under the name it was invoked by",
    );
    assert_eq!(
        chain.invocations()[1].args,
        tool_shapes()[1].args,
        "the record must carry the arguments the invocation carried",
    );
}

/// The `yt-dlp` stub writes the whole sidecar set the demo's hierarchy consumes,
/// with the fixture's bytes where a media file is expected.
#[test]
fn the_yt_dlp_stub_writes_the_whole_sidecar_set() {
    let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let fixture = fixture();
    let chain = mount_offline_tool_chain(dir.path(), &fixture);
    let env = chain.env_pairs();
    let env: Vec<(&str, &str)> = env.iter().map(|(name, value)| (*name, value.as_str())).collect();
    let sandbox = dir.path().join("sandbox");
    stage_sandbox(&sandbox, &[]);

    let shape = &tool_shapes()[0];
    let yt_dlp = chain.executable("yt-dlp").expect("the chain holds yt-dlp");
    let (code, _, stderr) = run(yt_dlp, &shape.args, &env, &sandbox);
    assert_eq!(code, 0, "the yt-dlp stub must emit the manifest: {stderr}");

    let stem = "Rick Astley - Never Gonna Give You Up [dQw4w9WgXcQ]__mediapm__";
    let downloads = sandbox.join("downloads");
    let media = downloads.join(format!("{stem}.mkv"));
    assert_eq!(
        std::fs::read(&media).expect("the emitted media file must exist"),
        std::fs::read(&fixture).expect("the fixture must be readable"),
        "the emitted media file must be the fixture's bytes",
    );

    let text = |name: String| {
        std::fs::read_to_string(downloads.join(name)).expect("the emitted text sidecar")
    };
    let subtitles = text(format!("{stem}.en.vtt"));
    assert!(subtitles.starts_with("WEBVTT"), "the subtitle track must be WebVTT: {subtitles}");
    assert!(
        subtitles.contains("Kind: captions") && subtitles.contains("Language: en"),
        "the subtitle track must name its kind and language: {subtitles}",
    );

    let thumbnail = std::fs::read(downloads.join(format!("{stem}.png"))).expect("the thumbnail");
    assert_eq!(
        thumbnail.get(..8),
        Some(&[137u8, 80, 78, 71, 13, 10, 26, 10][..]),
        "the thumbnail must carry PNG magic bytes",
    );

    let info: serde_json::Value =
        serde_json::from_str(&text(format!("{stem}.info.json"))).expect("info.json must be JSON");
    assert_eq!(info["id"], "dQw4w9WgXcQ", "info.json must name the video id: {info}");
    assert_eq!(info["extractor"], "youtube", "info.json must name the extractor: {info}");
    assert_eq!(
        info["title"], "Rick Astley - Never Gonna Give You Up",
        "info.json's title is the provider title the demo reads: {info}",
    );
    assert!(
        info["webpage_url"].as_str().unwrap_or_default().contains("dQw4w9WgXcQ"),
        "info.json must carry the page URL: {info}",
    );
    assert_eq!(
        info["upload_date"].as_str().unwrap_or_default().len(),
        8,
        "info.json's upload date must be YYYYMMDD: {info}",
    );
    assert!(
        info["duration"].as_f64().unwrap_or(0.0) > 0.0,
        "info.json's duration must be positive: {info}",
    );

    let url = text(format!("{stem}.url"));
    assert!(
        url.contains("[InternetShortcut]")
            && url.lines().any(|line| line.starts_with("URL="))
            && url.contains("https://www.youtube.com/watch?v=dQw4w9WgXcQ"),
        "the .url sidecar must be an InternetShortcut pointing at the source: {url}",
    );
    let webloc = text(format!("{stem}.webloc"));
    assert!(
        webloc.contains("<?xml")
            && webloc.contains("<plist")
            && webloc.contains("<key>URL</key>")
            && webloc.contains("<string>https://www.youtube.com/watch?v=dQw4w9WgXcQ</string>"),
        "the .webloc sidecar must be a plist pointing at the source: {webloc}",
    );
    let desktop = text(format!("{stem}.desktop"));
    assert!(
        desktop.contains("[Desktop Entry]")
            && desktop.contains("Type=Link")
            && desktop.contains("Encoding=UTF-8")
            && desktop.lines().any(|line| line.starts_with("Name="))
            && desktop.lines().any(|line| line.starts_with("URL=")),
        "the .desktop sidecar must be a link desktop entry: {desktop}",
    );

    assert!(
        !text(format!("{stem}.description")).trim().is_empty(),
        "the description sidecar must hold text",
    );
    assert!(!text("archive.txt".to_string()).trim().is_empty(), "the archive must hold text");
}

/// The mount provisions a tool whose host payload is the stub, so a sync
/// materializes the same bytes the chain runs.
#[tokio::test]
async fn the_mount_provisions_a_tool_whose_payload_executable_is_the_stub() {
    let mock = MockUpstream::start().await;
    let upstreams = mount_managed_tool_upstreams(&mock).await;
    let stub = std::fs::read(stub_tool_path()).expect("the stub must be readable");

    let yt_dlp_url = upstreams.host_payload_url("yt-dlp").expect("yt-dlp's host payload");
    let payload = reqwest::get(yt_dlp_url)
        .await
        .expect("the mock answers")
        .bytes()
        .await
        .expect("the payload body");
    assert_eq!(
        payload.as_ref(),
        stub.as_slice(),
        "yt-dlp is a bare binary, so its host payload must be the stub's own bytes",
    );

    let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let provisioned = dir.path().join(format!("yt-dlp{}", std::env::consts::EXE_SUFFIX));
    std::fs::write(&provisioned, &payload).expect("write the provisioned payload");
    // The provider materializes a payload with its container's permissions, so a
    // written-out payload has to be made executable before it can stand in for
    // one: on unix the bit comes from the archive entry, and a plain write drops
    // it.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = std::fs::metadata(&provisioned)
            .expect("the written payload's permissions")
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&provisioned, permissions).expect("make the payload executable");
    }
    // The provisioned file keeps the asset's name, so the run reaches it as
    // `yt-dlp` and it dispatches on that name: a bare invocation answers the
    // emit-mode failure naming the environment its manifest arrives in, which is
    // the stub's own message rather than the locator harness's.
    let (code, _, stderr) = run(&provisioned, &[], &[], dir.path());
    assert_eq!(code, 2, "the provisioned executable must behave like the stub: {stderr}");
    assert!(
        stderr.contains("mediapm-stub-tool") && stderr.contains(MANIFEST_ENV),
        "the provisioned executable must answer as the stub, naming {MANIFEST_ENV}: {stderr}",
    );

    let deno_url = upstreams.host_payload_url("deno").expect("deno's host payload");
    let payload = reqwest::get(deno_url)
        .await
        .expect("the mock answers")
        .bytes()
        .await
        .expect("the payload body");
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(payload.to_vec()))
        .expect("deno's payload must be a ZIP archive");
    let name = format!("deno{}", std::env::consts::EXE_SUFFIX);
    let mut entry = archive.by_name(&name).expect("the payload must hold the tool's executable");
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(&mut entry, &mut bytes).expect("read the packed executable");
    assert_eq!(bytes, stub, "an archive payload's executable entry must be the stub's own bytes");
}

/// Fetches `url` and returns its status and body as text.
async fn fetch_status_and_text(url: &str) -> (u16, String) {
    let response = reqwest::get(url).await.expect("the mock answers");
    let status = response.status().as_u16();
    let text = response.text().await.expect("response body");
    (status, text)
}

/// The mock tagger backend answers the two lookups a tagging step makes and
/// refuses a path outside them.
#[tokio::test]
async fn the_mock_tagger_backend_answers_its_lookups_and_refuses_another_path() {
    let mock = MockMusicBrainz::start().await;
    let endpoint = mock.endpoint();

    let (status, body) =
        fetch_status_and_text(&format!("{endpoint}/recording/{RECORDING_MBID}")).await;
    assert_eq!(status, 200, "the recording lookup must succeed: {body}");
    let recording: serde_json::Value = serde_json::from_str(&body).expect("the recording is JSON");
    assert_eq!(recording["id"], RECORDING_MBID, "the recording must name its MBID: {recording}");
    assert_eq!(
        recording["title"], RECORDING_TITLE,
        "the recording must carry the demo's title: {recording}",
    );
    assert_eq!(
        recording["artist-credit"][0]["name"], ARTIST_NAME,
        "the recording must credit the demo's artist: {recording}",
    );

    let (status, body) = fetch_status_and_text(&format!("{endpoint}/release/{RELEASE_MBID}")).await;
    assert_eq!(status, 200, "the release lookup must succeed: {body}");
    let release: serde_json::Value = serde_json::from_str(&body).expect("the release is JSON");
    assert_eq!(release["id"], RELEASE_MBID, "the release must name its MBID: {release}");
    assert_eq!(
        release["media"][0]["tracks"][0]["recording"]["id"], RECORDING_MBID,
        "the release's track list must name the recording: {release}",
    );

    let (status, _) = fetch_status_and_text(&format!("{endpoint}/artist/{RECORDING_MBID}")).await;
    assert_eq!(status, 404, "a path outside the mounted lookups must not be answered");
}

/// The chain publishes the environment the stub reads, and the record it names
/// is the one the run appends to.
#[test]
fn the_chain_publishes_the_environment_the_stub_reads() {
    let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let chain = mount_offline_tool_chain(dir.path(), &fixture());
    let pairs = chain.env_pairs();

    assert_eq!(
        pairs.iter().map(|(name, _)| *name).collect::<Vec<&str>>(),
        vec![MANIFEST_ENV, INVOCATION_RECORD_ENV],
        "the chain must publish both variables the stub reads",
    );
    assert_eq!(
        pairs[0].1,
        chain.manifest_path().display().to_string(),
        "the manifest variable must name the manifest the chain wrote",
    );
    assert_eq!(
        pairs[1].1,
        chain.invocations_path().display().to_string(),
        "the record variable must name the record the chain wrote",
    );
    assert!(chain.manifest_path().is_file(), "the manifest must exist after the mount");
    assert!(
        std::fs::read_to_string(chain.invocations_path()).expect("the record exists").is_empty(),
        "a freshly mounted chain must hold no invocation",
    );
}
