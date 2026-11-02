//! The offline tool chain: the stub executables a hermetic run invokes, and the
//! upstreams that provision them.
//!
//! A hermetic run needs two things a network run gets from the internet: the
//! tool executables, and the upstream hosts that serve them. Both are covered
//! here.
//!
//! [`mount_offline_tool_chain`] writes the executables — one copy of the
//! compiled stub (`crate::stub_tool::stub_tool_path`) per tool, named after the
//! tool — plus the manifest the `yt-dlp` copy emits, which is what produces the
//! media and sidecar set the demo's hierarchy consumes. A copy carries no
//! `--mode`, so it dispatches on its own file name; see the stub binary's module
//! documentation for the dispatch and for the two environment variables that
//! carry the manifest path and the invocation record.
//!
//! [`mount_managed_tool_upstreams`] answers the four provider seams with
//! payloads whose host-OS executable is the same stub bytes, so a sync
//! provisions these tools without a network. Every other OS slot keeps a text
//! body: the provider downloads and packs each slot, but only the host's
//! payload is ever materialized into a sandbox, so a stub in the other slots
//! would only cost time.
//!
//! # Known limitation
//!
//! A chain copy is handed a real tool's command line, so the copy-shaped
//! fallback has to guess its one output path from that line: it takes the
//! rightmost argument that is neither an option nor an option's value, and only
//! when that argument is path-shaped — carrying a path separator, or a last dot
//! that leaves a non-empty extension (`looks_like_a_path` in
//! `src/bin/mediapm-stub-tool.rs`). Whatever else a command line names is a
//! value, and writing one escapes the sandbox the step was given, so a line
//! with no path-shaped token writes nothing and succeeds.
//! An `ffmpeg` step that binds more than one output slot therefore gets
//! its highest-indexed path written and the others left absent. `ffmpeg`
//! renders one `${inputs.output_path_N}` per slot (`build_ffmpeg_command` in
//! `src/mediapm/src/tools/workflows/ffmpeg.rs`) and the command line carries
//! them as bare trailing arguments, so nothing on it says how many slots are
//! bound or which path belongs to which. The limitation is not fixable from a
//! real command line: a stub that assigned the positional arguments to slots
//! would write the wrong files rather than none, and a marker it could read
//! would be a flag the real tool would reject. An offline step that needs
//! several outputs is out of this chain's reach.
//!
//! The upstream shapes mirror `src/mediapm/tests/common/stub_upstream.rs`,
//! which cannot be reused: it is a test module of the `mediapm` crate, and this
//! crate does not depend on `mediapm` (a dependency that would put `wiremock`
//! into shipped binaries, which is why `mediapm-test-support` is a
//! dev-dependency of `mediapm` and never the other way around). The tag and
//! asset-name constants are restated here deliberately rather than approximated:
//! a provider's asset-name template is part of its resolve logic, so the two
//! mounts have to agree on the names, and a value that drifts would fail the
//! sync with a 404 rather than with a clear message.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use crate::mock_upstream::MockUpstream;
use crate::stub_payload::{PayloadFormat, StubEntry};
use crate::stub_tool::stub_tool_path;

/// Environment variable naming the manifest a `yt-dlp`-named stub emits.
///
/// The command line belongs to the real tool, so the manifest path cannot
/// travel on it. The stub binary reads this name from its environment, and the
/// chain publishes it through [`OfflineToolChain::env_pairs`];
/// `stub_tool::tests::the_stub_source_names_the_chain_environment_variables`
/// fails if the embedded stub source stops naming it.
pub const MANIFEST_ENV: &str = "MEDIAPM_STUB_MANIFEST";

/// Environment variable naming the file every stub invocation is recorded in.
///
/// Published the same way as [`MANIFEST_ENV`], and pinned by the same test.
pub const INVOCATION_RECORD_ENV: &str = "MEDIAPM_STUB_INVOCATIONS";

/// The tools an offline chain stands in for, in the order it writes them.
pub const CHAIN_TOOLS: [&str; 6] = ["yt-dlp", "ffmpeg", "ffprobe", "rsgain", "sd", "deno"];

/// The upstream title the chain's yt-dlp artifacts are named after.
///
/// The sandbox output template is `%(title)s [%(id)s]%(playlist_index|)s__mediapm__`, so this
/// text is both part of every emitted file name and the `title` the emitted
/// `info.json` reports — the demo reads that field as the provider title
/// (`load_resolved_demo_metadata` in
/// `src/mediapm/examples/mediapm_demo_online.rs`) and compares the link sidecars
/// against it. It is the artist-prefixed form because that is what upstream's
/// own title carries.
pub const PROVIDER_TITLE: &str = "Rick Astley - Never Gonna Give You Up";

/// The raw video id the chain's yt-dlp artifact names carry.
///
/// The demo pins the same id (`ONLINE_DEMO_YT_DLP_VIDEO_ID` in
/// `src/mediapm/src/demo_hierarchy_spec.rs`) and asserts it in the emitted
/// `info.json`, in the sidecar link names, and in the emitted URL files.
pub const VIDEO_ID: &str = "dQw4w9WgXcQ";

/// The page the chain's yt-dlp artifacts point at.
///
/// The demo asserts this URL in the emitted `.url`, `.webloc` and `.desktop`
/// files (`ONLINE_DEMO_YOUTUBE_URL` in `src/mediapm/src/demo_hierarchy_spec.rs`).
pub const SOURCE_URL: &str = "https://www.youtube.com/watch?v=dQw4w9WgXcQ";

/// The sandbox directory the yt-dlp preset's `--paths` option writes under.
const SANDBOX_DOWNLOADS_DIR: &str = "downloads";

/// The chain directory holding the sidecar source files the manifest reads.
const SIDECAR_SOURCE_DIR: &str = "emit";

/// One recorded invocation of a chain executable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StubInvocation {
    /// The program's file stem, which is what selected the stub's mode.
    pub tool: String,
    /// The arguments the invocation carried, in order.
    pub args: Vec<String>,
}

/// A mounted offline tool chain: the executables, the emit manifest, and the
/// invocation record.
///
/// The executables are copies of the compiled stub, so a run reaches them the
/// way it reaches a provisioned tool: by path, with a real tool's command line
/// and no stub flag. Every invocation appends a line to the record, which is
/// how a caller shows a tool was reached after the run.
pub struct OfflineToolChain {
    /// The directory holding every executable, the manifest and the record.
    dir: PathBuf,
    /// One executable per entry of [`CHAIN_TOOLS`].
    executables: BTreeMap<&'static str, PathBuf>,
    /// The manifest the `yt-dlp` copy emits.
    manifest: PathBuf,
    /// The file every invocation appends to.
    invocations: PathBuf,
}

impl OfflineToolChain {
    /// The directory holding every executable, the manifest and the record.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The executable standing in for `tool`, or `None` for a tool outside
    /// [`CHAIN_TOOLS`].
    #[must_use]
    pub fn executable(&self, tool: &str) -> Option<&Path> {
        self.executables.get(tool).map(PathBuf::as_path)
    }

    /// Every executable with the tool it stands in for, in [`CHAIN_TOOLS`] order.
    pub fn executables(&self) -> impl Iterator<Item = (&'static str, &Path)> {
        CHAIN_TOOLS.iter().filter_map(|tool| self.executable(tool).map(|path| (*tool, path)))
    }

    /// The manifest the `yt-dlp` copy emits.
    #[must_use]
    pub fn manifest_path(&self) -> &Path {
        &self.manifest
    }

    /// The file every invocation appends to.
    #[must_use]
    pub fn invocations_path(&self) -> &Path {
        &self.invocations
    }

    /// The environment pairs a run has to install for the chain to work.
    ///
    /// The stub reads both names from the environment the conductor passes to a
    /// spawned tool, which is the caller's environment: a tool's `env_vars` are
    /// serialized into the document but never reach the process
    /// (`to_unified` in `src/mediapm-conductor/src/config/documents.rs` drops
    /// them), so declaring them on a tool spec would not deliver them. Install
    /// the pairs under the environment lock a caller already holds — in a
    /// `mediapm` test that is `example_isolation::env_lock()` — so a parallel
    /// test cannot see them.
    #[must_use]
    pub fn env_pairs(&self) -> Vec<(&'static str, String)> {
        vec![
            (MANIFEST_ENV, self.manifest.display().to_string()),
            (INVOCATION_RECORD_ENV, self.invocations.display().to_string()),
        ]
    }

    /// Every invocation recorded so far, in the order the stubs ran.
    ///
    /// An absent or empty record reads as no invocations: a chain that was
    /// mounted and never used is not an error, and the mount leaves the file
    /// present so a caller can tell that apart from a missing directory.
    ///
    /// # Panics
    ///
    /// Panics when the record cannot be read or holds a line without the
    /// NUL-separated fields the stub writes.
    #[must_use]
    pub fn invocations(&self) -> Vec<StubInvocation> {
        let Ok(text) = std::fs::read_to_string(&self.invocations) else {
            return Vec::new();
        };
        text.lines()
            .map(|line| {
                let mut fields = line.split('\0');
                let tool = fields.next().unwrap_or_default().to_string();
                let args = fields.map(str::to_string).collect();
                StubInvocation { tool, args }
            })
            .collect()
    }
}

/// Writes the chain's executables, its emit manifest and its invocation record
/// into `dir`.
///
/// `fixture` is the media file the emitted artifact carries: the manifest names
/// it as the source of the media file, so the stub writes the committed fixture's
/// bytes without a copy of them in the chain.
///
/// # Panics
///
/// Panics when the stub cannot be located or copied, when a sidecar source or
/// the manifest cannot be written, or when `fixture` is not a readable file: a
/// chain that is missing a piece would fail the run it was mounted for with an
/// error that names the symptom rather than the cause.
#[must_use]
pub fn mount_offline_tool_chain(dir: &Path, fixture: &Path) -> OfflineToolChain {
    let stub = stub_tool_path();
    let mut executables = BTreeMap::new();
    for tool in CHAIN_TOOLS {
        let path = dir.join(format!("{tool}{}", std::env::consts::EXE_SUFFIX));
        std::fs::copy(&stub, &path).unwrap_or_else(|error| {
            panic!("cannot copy the stub to '{}': {error}", path.display());
        });
        executables.insert(tool, path);
    }

    let manifest = dir.join("yt-dlp.manifest");
    let contents = yt_dlp_manifest(dir, fixture);
    std::fs::write(&manifest, contents)
        .unwrap_or_else(|error| panic!("cannot write '{}': {error}", manifest.display()));

    // The record is created empty rather than left absent, so a caller reading
    // it after a run distinguishes "no invocation" from "no chain here", and a
    // chain directory reused across runs cannot answer with an earlier run's
    // invocations.
    let invocations = dir.join("invocations.log");
    std::fs::write(&invocations, b"")
        .unwrap_or_else(|error| panic!("cannot write '{}': {error}", invocations.display()));

    OfflineToolChain { dir: dir.to_path_buf(), executables, manifest, invocations }
}

/// Writes the sidecar sources into `dir` and returns the manifest's contents.
///
/// # Panics
///
/// Panics when a source or the fixture cannot be read, or when a source cannot
/// be written.
fn yt_dlp_manifest(dir: &Path, fixture: &Path) -> String {
    let sources = dir.join(SIDECAR_SOURCE_DIR);
    std::fs::create_dir_all(&sources)
        .unwrap_or_else(|error| panic!("cannot create '{}': {error}", sources.display()));

    let stem = format!("{PROVIDER_TITLE} [{VIDEO_ID}]__mediapm__");
    // The media file is the fixture itself, so the emitted artifact is the
    // committed file's bytes rather than a copy of them that could drift.
    let mut entries = vec![(format!("{SANDBOX_DOWNLOADS_DIR}/{stem}.mkv"), fixture.to_path_buf())];
    for (destination, name, body) in sidecar_set(&stem) {
        let source = sources.join(name);
        std::fs::write(&source, &body)
            .unwrap_or_else(|error| panic!("cannot write '{}': {error}", source.display()));
        entries.push((destination, source));
    }

    let mut manifest = String::new();
    for (destination, source) in entries {
        let _ = writeln!(manifest, "{destination}\t{}", source.display());
    }
    manifest
}

/// The sidecar set the `yt-dlp` stub emits, as `(destination, source name, bytes)`.
///
/// The destination names are the sandbox paths the yt-dlp preset's output
/// template and `--paths` option produce: `downloads/{title} [{id}]__mediapm__`
/// plus yt-dlp's own suffix for each artifact, and `downloads/archive.txt` for
/// the download archive. The demo's hierarchy consumes every one of them, and
/// the contents are the minimum each consumer asserts — see
/// `assert_sidecar_content_expectations`, `assert_sidecar_directory_family_content`
/// and `assert_link_file_content_format` in
/// `src/mediapm/examples/mediapm_demo_online.rs` for the assertions each body
/// satisfies.
fn sidecar_set(stem: &str) -> Vec<(String, &'static str, Vec<u8>)> {
    let destination = |suffix: &str| format!("{SANDBOX_DOWNLOADS_DIR}/{stem}{suffix}");
    vec![
        (destination(".en.vtt"), "subtitles.en.vtt", subtitles()),
        (destination(".png"), "thumbnail.png", thumbnail()),
        (destination(".description"), "description.txt", description()),
        (destination(".info.json"), "info.json", info_json()),
        (destination(".url"), "link.url", internet_shortcut()),
        (destination(".webloc"), "link.webloc", webloc()),
        (destination(".desktop"), "link.desktop", desktop_entry()),
        (format!("{SANDBOX_DOWNLOADS_DIR}/archive.txt"), "archive.txt", archive()),
    ]
}

/// The emitted subtitle track: a `WebVTT` document naming its language.
///
/// A `YouTube` auto-caption track starts with `WEBVTT` and carries the
/// `Kind: captions` and `Language: en` header lines, which is exactly what the
/// demo asserts before it checks the `en` language is the one the document
/// asked for.
fn subtitles() -> Vec<u8> {
    concat!(
        "WEBVTT\n",
        "Kind: captions\n",
        "Language: en\n",
        "\n",
        "00:00:00.000 --> 00:00:04.000\n",
        "We're no strangers to love\n",
        "\n",
        "00:00:04.000 --> 00:00:08.000\n",
        "Never gonna give you up\n",
    )
    .as_bytes()
    .to_vec()
}

/// The emitted thumbnail: a two-by-two PNG.
///
/// The bytes are a real PNG rather than a text body with an image name, because
/// the demo validates the image magic bytes of every thumbnail it materializes.
/// The dimensions do not matter: nothing reads them.
fn thumbnail() -> Vec<u8> {
    vec![
        137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 2, 0, 0, 0, 2, 8, 6,
        0, 0, 0, 114, 182, 13, 36, 0, 0, 0, 17, 73, 68, 65, 84, 120, 156, 99, 248, 207, 192, 240,
        31, 132, 25, 96, 12, 0, 71, 202, 7, 249, 103, 89, 110, 183, 0, 0, 0, 0, 73, 69, 78, 68,
        174, 66, 96, 130,
    ]
}

/// The emitted description: upstream's own description text.
///
/// Only its non-emptiness is asserted, because upstream owns the text.
fn description() -> Vec<u8> {
    format!("{PROVIDER_TITLE}\n\n{SOURCE_URL}\n").into_bytes()
}

/// The emitted `info.json`: the fields the demo reads out of it.
///
/// `id`, `extractor`, `webpage_url`, `upload_date` and `duration` are the fields
/// `assert_sidecar_content_expectations` checks; `title` is the field the demo
/// reads as the provider title, so it carries [`PROVIDER_TITLE`] rather than the
/// canonical title the tagging steps write.
fn info_json() -> Vec<u8> {
    serde_json::json!({
        "id": VIDEO_ID,
        "title": PROVIDER_TITLE,
        "extractor": "youtube",
        "extractor_key": "Youtube",
        "webpage_url": SOURCE_URL,
        "upload_date": "19870727",
        "duration": 10.007,
        "format_id": "251",
        "ext": "mkv",
    })
    .to_string()
    .into_bytes()
}

/// The emitted `.url`: an `InternetShortcut` block pointing at the source.
fn internet_shortcut() -> Vec<u8> {
    format!("[InternetShortcut]\nURL={SOURCE_URL}\n").into_bytes()
}

/// The emitted `.webloc`: the property list macOS reads as a URL bookmark.
fn webloc() -> Vec<u8> {
    concat!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
        "<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" ",
        "\"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n",
        "<plist version=\"1.0\">\n",
        "<dict>\n",
        "\t<key>URL</key>\n",
        "\t<string>https://www.youtube.com/watch?v=dQw4w9WgXcQ</string>\n",
        "</dict>\n",
        "</plist>\n",
    )
    .as_bytes()
    .to_vec()
}

/// The emitted `.desktop`: the desktop entry a Linux file manager reads.
fn desktop_entry() -> Vec<u8> {
    format!("[Desktop Entry]\nEncoding=UTF-8\nType=Link\nName={PROVIDER_TITLE}\nURL={SOURCE_URL}\n")
        .into_bytes()
}

/// The emitted download archive: one line per downloaded id.
///
/// The archive is what makes yt-dlp skip a re-download, and the demo asserts
/// only that it holds non-empty text.
fn archive() -> Vec<u8> {
    format!("youtube {VIDEO_ID}\n").into_bytes()
}

/// The tag the mocked yt-dlp release reports.
const YT_DLP_TAG: &str = "2026.01.01";

/// The tag the mocked deno release reports.
const DENO_TAG: &str = "v2.5.0";

/// The tag the mocked rsgain release reports.
///
/// The provider strips the leading `v` before building the versioned asset
/// names, so this value exercises that strip.
const RSGAIN_TAG: &str = "v3.7.0";

/// The tag the mocked sd release reports.
const SD_TAG: &str = "v1.1.0";

/// The commit hash every mocked GitHub tag ref reports as its object.
const COMMIT_SHA: &str = "0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f";

/// The commit hash the mocked deno tag ref reports, deliberately not [`COMMIT_SHA`].
///
/// deno and yt-dlp are both tagged releases whose canonical version is their
/// commit hash, so sharing one value would make a composite version's deno
/// segment indistinguishable from the requester's own version.
const DENO_COMMIT_SHA: &str = "1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e";

/// The `BtbN` autobuild tag the mocked releases list names.
const BTBN_TAG: &str = "autobuild-2026-01-01-12-00";

/// The martin-riedl build id the mocked permalink redirects to.
const MARTIN_RIEDL_BUILD_ID: &str = "1767225600_N-127260-g7bc3576910";

/// The jellyfin build line the mocked repository index names.
const JELLYFIN_LINE: &str = "latest-10.x";

/// The jellyfin version the mocked archive names carry.
const JELLYFIN_VERSION: &str = "7.1.1";

/// The host-OS payload URL of each managed tool the mount answered.
pub struct ManagedToolUpstreams {
    /// One payload URL per tool, for the OS slot this host runs.
    host_payload_urls: BTreeMap<&'static str, String>,
}

impl ManagedToolUpstreams {
    /// The URL of the payload this host's OS slot serves for `tool`, or `None`
    /// for a tool the mount does not cover.
    ///
    /// The bytes at that URL are the payload a sync would download and
    /// materialize: the stub for the host's executable, wrapped in the asset's
    /// own container (a bare binary for yt-dlp, an archive for the rest).
    #[must_use]
    pub fn host_payload_url(&self, tool: &str) -> Option<&str> {
        self.host_payload_urls.get(tool).map(String::as_str)
    }
}

/// Mounts every managed tool's upstream traffic with stub payloads for this
/// host's OS slot.
///
/// The returned value names the host payload URL per tool, so a caller can fetch
/// what a sync would download; the routes themselves are mounted on `mock`.
///
/// # Panics
///
/// Panics when this crate was built without `test-seams`: the routes would then
/// never be reached, and the run would fetch from the real upstream.
#[must_use]
pub async fn mount_managed_tool_upstreams(mock: &MockUpstream) -> ManagedToolUpstreams {
    let stub = std::fs::read(stub_tool_path())
        .unwrap_or_else(|error| panic!("cannot read the stub: {error}"));
    let mut host_payload_urls = BTreeMap::new();

    host_payload_urls.insert("yt-dlp", mount_yt_dlp(mock, &stub).await);
    host_payload_urls.insert("deno", mount_deno(mock, &stub).await);
    host_payload_urls.insert("rsgain", mount_rsgain(mock, &stub).await);
    host_payload_urls.insert("sd", mount_sd(mock, &stub).await);
    host_payload_urls.insert("ffmpeg", mount_ffmpeg(mock, &stub).await);

    ManagedToolUpstreams { host_payload_urls }
}

/// Returns the file name the provider's `find_os_executable` resolves in a slot.
///
/// The provider looks for `{tool_id}` and `{tool_id}.exe` in every slot, so
/// either name resolves; the entry still carries the name the slot's own OS
/// gives the tool, because a payload stands in for the asset upstream serves,
/// and a Windows slot holding the plain name on a unix host is not that shape.
/// Every slot label but `windows` is a unix slot: the per-OS loops pass their
/// OS, and the martin-riedl and jellyfin payloads pass the macOS architecture
/// they cover.
fn slot_executable_name(tool: &str, slot: &str) -> String {
    if slot == "windows" { format!("{tool}.exe") } else { tool.to_string() }
}

/// The architecture label the provider resolves for this host.
///
/// The provider maps every host outside ARM to `x86_64` rather than to the
/// host's own architecture (`host_arch_label` in
/// `src/mediapm-conductor/src/tools/provider/mod.rs`), so the mock has to follow
/// that mapping to name the assets the provider asks for. It is restated here
/// because this crate does not depend on the conductor.
fn provider_host_arch() -> &'static str {
    match std::env::consts::ARCH {
        "aarch64" => "aarch64",
        _ => "x86_64",
    }
}

/// Whether `os` is the OS label this host runs.
///
/// The provider layer's own OS label is out of reach here — this crate does not
/// depend on the conductor — so the comparison rests on
/// [`crate::stub_payload::host_os`], this crate's copy of that label, the same
/// way [`provider_host_arch`] restates the conductor's arch label.
fn is_host_os(os: &str) -> bool {
    os == crate::stub_payload::host_os()
}

/// Packs one OS slot's entries: the stub when this slot is the one the host runs.
///
/// Every slot's payload is downloaded and packed by the provider, but only the
/// host's is materialized, so the other slots carry a small text body naming the
/// slot they stand for. The entry's name is the slot's own, not this host's
/// ([`slot_executable_name`]).
fn slot_entries(tool: &str, slot: &str, stub: Option<&[u8]>) -> Vec<StubEntry> {
    let body = match stub {
        Some(stub) => stub.to_vec(),
        None => format!("{tool} stub for {slot}\n").into_bytes(),
    };
    vec![StubEntry::executable(slot_executable_name(tool, slot), body)]
}

/// Mounts yt-dlp's upstream traffic: one bare binary per OS slot.
///
/// The host slot's bytes are the stub itself, and the provisioned file keeps the
/// asset's basename, so the stub is reached as `yt-dlp_macos`, `yt-dlp_linux` or
/// `yt-dlp.exe` — every name its dispatch claims.
async fn mount_yt_dlp(mock: &MockUpstream, stub: &[u8]) -> String {
    let assets = [("windows", "yt-dlp.exe"), ("macos", "yt-dlp_macos"), ("linux", "yt-dlp_linux")];
    let mut host_url = String::new();
    let assets: Vec<crate::stub_payload::StubAsset> = assets
        .into_iter()
        .map(|(os, name)| {
            let bytes = if is_host_os(os) {
                host_url =
                    format!("{}/yt-dlp/yt-dlp/releases/download/{YT_DLP_TAG}/{name}", mock.base());
                stub.to_vec()
            } else {
                format!("yt-dlp stub for {os}\n").into_bytes()
            };
            crate::stub_payload::StubAsset { name: name.to_string(), bytes }
        })
        .collect();
    mock.mount_github_release("yt-dlp", "yt-dlp", YT_DLP_TAG, COMMIT_SHA, &assets).await;
    host_url
}

/// Mounts deno's upstream traffic: one ZIP per OS slot.
///
/// The macOS and Linux asset names follow the host architecture, exactly as the
/// provider's own slot table does. The provider wraps the executable it finds in
/// a permission shim and renames the stub to `deno.real` (`deno.real.exe` on
/// Windows), writing a shim at the original path that re-execs the rewritten
/// binary, so a provisioned deno runs under one of those two names; the stub's
/// dispatch covers both.
async fn mount_deno(mock: &MockUpstream, stub: &[u8]) -> String {
    let arch = provider_host_arch();
    let assets = [
        ("windows", "deno-x86_64-pc-windows-msvc.zip".to_string()),
        ("macos", format!("deno-{arch}-apple-darwin.zip")),
        ("linux", format!("deno-{arch}-unknown-linux-gnu.zip")),
    ];
    let mut host_url = String::new();
    let mut mounted = Vec::new();
    for (os, name) in assets {
        if is_host_os(os) {
            host_url = format!("{}/denoland/deno/releases/download/{DENO_TAG}/{name}", mock.base());
        }
        mounted.push(crate::stub_payload::StubAsset {
            name,
            bytes: PayloadFormat::Zip.pack(&slot_entries(
                "deno",
                os,
                is_host_os(os).then_some(stub),
            )),
        });
    }
    mock.mount_github_release("denoland", "deno", DENO_TAG, DENO_COMMIT_SHA, &mounted).await;
    host_url
}

/// Mounts rsgain's upstream traffic: two ZIPs and one `tar.xz`.
///
/// rsgain publishes a single `x86_64` build per OS, so no asset name follows the
/// host architecture. The asset names embed the version with the tag's leading
/// `v` stripped, which is the shape the provider's own rewrite builds.
async fn mount_rsgain(mock: &MockUpstream, stub: &[u8]) -> String {
    let version = RSGAIN_TAG.strip_prefix('v').unwrap_or(RSGAIN_TAG);
    let assets = [
        ("windows", format!("rsgain-{version}-win64.zip"), PayloadFormat::Zip),
        ("macos", format!("rsgain-{version}-macOS-x86_64.zip"), PayloadFormat::Zip),
        ("linux", format!("rsgain-{version}-Linux.tar.xz"), PayloadFormat::TarXz),
    ];
    let mut host_url = String::new();
    let mut mounted = Vec::new();
    for (os, name, format) in assets {
        if is_host_os(os) {
            host_url = format!(
                "{}/complexlogic/rsgain/releases/download/{RSGAIN_TAG}/{name}",
                mock.base()
            );
        }
        mounted.push(crate::stub_payload::StubAsset {
            name,
            bytes: format.pack(&slot_entries("rsgain", os, is_host_os(os).then_some(stub))),
        });
    }
    mock.mount_github_release("complexlogic", "rsgain", RSGAIN_TAG, COMMIT_SHA, &mounted).await;
    host_url
}

/// Mounts sd's upstream traffic: one ZIP and two `tar.gz` archives.
///
/// sd's asset names embed the tag itself, `v` included.
async fn mount_sd(mock: &MockUpstream, stub: &[u8]) -> String {
    let arch = provider_host_arch();
    let assets = [
        ("windows", format!("sd-{SD_TAG}-x86_64-pc-windows-msvc.zip"), PayloadFormat::Zip),
        ("macos", format!("sd-{SD_TAG}-{arch}-apple-darwin.tar.gz"), PayloadFormat::TarGz),
        ("linux", format!("sd-{SD_TAG}-x86_64-unknown-linux-gnu.tar.gz"), PayloadFormat::TarGz),
    ];
    let mut host_url = String::new();
    let mut mounted = Vec::new();
    for (os, name, format) in assets {
        if is_host_os(os) {
            host_url = format!("{}/chmln/sd/releases/download/{SD_TAG}/{name}", mock.base());
        }
        mounted.push(crate::stub_payload::StubAsset {
            name,
            bytes: format.pack(&slot_entries("sd", os, is_host_os(os).then_some(stub))),
        });
    }
    mock.mount_github_release("chmln", "sd", SD_TAG, COMMIT_SHA, &mounted).await;
    host_url
}

/// Mounts ffmpeg's upstream traffic: the `BtbN` alias downloads, the
/// martin-riedl macOS chain, and the jellyfin macOS fallback.
///
/// Both `BtbN` candidates are mounted, so a run does not depend on which of the
/// shared and non-shared assets the chain prefers. Both martin-riedl channels
/// are mounted for the same reason: the chain tries the snapshot permalink
/// first, and mounting the release permalink too keeps the fixture from encoding
/// that order. The macOS chain publishes both architectures, so the host arch's
/// payload carries the stub and the other arch's does not; the jellyfin
/// fallback's archive is resolved but never downloaded while the permalink
/// serves, so only its pointer and index routes need to exist.
async fn mount_ffmpeg(mock: &MockUpstream, stub: &[u8]) -> String {
    mock.mount_github_release_list("BtbN", "FFmpeg-Builds", &[BTBN_TAG]).await;
    let mut host_url = String::new();

    let btb_n = [
        ("windows", "ffmpeg-master-latest-win64-gpl-shared.zip", PayloadFormat::Zip),
        ("windows", "ffmpeg-master-latest-win64-gpl.zip", PayloadFormat::Zip),
        ("linux", "ffmpeg-master-latest-linux64-gpl-shared.tar.xz", PayloadFormat::TarXz),
        ("linux", "ffmpeg-master-latest-linux64-gpl.tar.xz", PayloadFormat::TarXz),
    ];
    let mut assets = Vec::new();
    for (os, name, format) in btb_n {
        if is_host_os(os) {
            host_url =
                format!("{}/BtbN/FFmpeg-Builds/releases/download/{BTBN_TAG}/{name}", mock.base());
        }
        assets.push(crate::stub_payload::StubAsset {
            name: name.to_string(),
            bytes: format.pack(&slot_entries("ffmpeg", os, is_host_os(os).then_some(stub))),
        });
    }
    mock.mount_github_release("BtbN", "FFmpeg-Builds", BTBN_TAG, COMMIT_SHA, &assets).await;

    let host_arch = provider_host_arch();
    for (canonical, site_label) in [("aarch64", "arm64"), ("x86_64", "amd64")] {
        let payload = PayloadFormat::Zip.pack(&slot_entries(
            "ffmpeg",
            canonical,
            (is_host_os("macos") && canonical == host_arch).then_some(stub),
        ));
        for channel in ["snapshot", "release"] {
            mock.mount_martin_riedl(site_label, channel, MARTIN_RIEDL_BUILD_ID, &payload).await;
            if is_host_os("macos") && canonical == host_arch && channel == "snapshot" {
                host_url = format!(
                    "{}/redirect/latest/macos/{site_label}/snapshot/ffmpeg.zip",
                    mock.base()
                );
            }
        }
    }

    for (canonical, site_dir, site_token) in
        [("aarch64", "arm64", "macarm64"), ("x86_64", "x86_64", "mac64")]
    {
        let archive =
            format!("jellyfin-ffmpeg_{JELLYFIN_VERSION}_portable_{site_token}-gpl.tar.xz");
        let payload = PayloadFormat::TarXz.pack(&slot_entries("ffmpeg", canonical, None));
        mock.mount_jellyfin(JELLYFIN_LINE, site_dir, site_token, &archive, &payload).await;
    }

    host_url
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A slot's entry carries the executable name that slot's own OS gives the tool.
    ///
    /// The mocked payloads stand in for the assets upstream serves, so the
    /// Windows slot has to hold `deno.exe` and the unix slots `deno`. Naming
    /// every slot with this host's suffix would leave the Windows slot holding
    /// the plain name on a unix host, which is a shape no upstream asset has.
    #[test]
    fn a_slot_entry_is_named_the_way_its_own_os_names_the_executable() {
        let name = |slot: &str| slot_entries("deno", slot, None)[0].name.clone();

        assert_eq!(name("windows"), "deno.exe", "the Windows slot's entry is the suffixed name");
        assert_eq!(name("linux"), "deno", "the Linux slot's entry is the plain name");
        assert_eq!(name("macos"), "deno", "the macOS slot's entry is the plain name");
        assert_eq!(name("aarch64"), "deno", "a macOS architecture label is not a Windows slot");
    }
}
