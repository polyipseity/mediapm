//! The mocked upstream contract shared by hermetic provisioning tests.
//!
//! A test that provisions a managed tool end to end needs a mock that answers
//! the exact requests the provider layer makes: release metadata, tag refs,
//! asset names following each provider's own naming scheme, and container
//! bytes the pipeline's extractors accept. That shape is spelled once here so
//! two test modules cannot disagree about it. A mock that drifts from the
//! provider's requests fails closed, but a mock that drifts from a sibling
//! test's fixture passes while covering nothing, and that second failure is
//! silent — which is why the shape has one definition.
//!
//! Every stub payload's body names the tool and the OS slot it was built for,
//! so a document that fills one slot with another slot's bytes is detectable
//! rather than merely unusual.

use mediapm_conductor::tools::provider::host_arch_label;
use mediapm_test_support::{MockUpstream, PayloadFormat, SeamGuard, StubAsset, StubEntry};

/// The tag the mocked yt-dlp release reports.
pub(crate) const STUB_YT_DLP_TAG: &str = "2026.01.01";

/// The tag the mocked deno release reports.
pub(crate) const STUB_DENO_TAG: &str = "v2.5.0";

/// The tag the mocked rsgain release reports.
///
/// rsgain tags carry a leading `v` that the provider strips before building
/// the versioned asset names, so this fixture exercises that strip.
pub(crate) const STUB_RSGAIN_TAG: &str = "v3.7.0";

/// The tag the mocked sd release reports.
pub(crate) const STUB_SD_TAG: &str = "v1.1.0";

/// The commit hash every mocked GitHub tag ref reports as its object.
///
/// The value is arbitrary: it becomes the tool's canonical version, which is
/// compared as a string and never parsed.
///
/// A tool whose composite version is asserted as carrying its *own* version
/// needs a value of its own; see [`STUB_DENO_COMMIT_SHA`].
pub(crate) const STUB_COMMIT_SHA: &str = "0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f";

/// The commit hash the mocked `deno` tag ref reports, deliberately not
/// [`STUB_COMMIT_SHA`].
///
/// deno and yt-dlp are both tagged releases whose canonical version is their
/// commit hash, so sharing one value would make a composite version's deno
/// segment indistinguishable from the requester's own version: an assertion
/// that a dependency carries its own version would then pass even if it carried
/// the requester's. The value is otherwise arbitrary.
pub(crate) const STUB_DENO_COMMIT_SHA: &str = "1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e";

/// The `BtbN` autobuild tag the mocked releases list names.
pub(crate) const STUB_BTBN_TAG: &str = "autobuild-2026-01-01-12-00";

/// The martin-riedl build id the mocked permalink redirects to.
///
/// martin-riedl publishes one build id for both macOS architectures, so both
/// permalinks redirect to a path carrying this id.
pub(crate) const STUB_MARTIN_RIEDL_BUILD_ID: &str = "1767225600_N-127260-g7bc3576910";

/// The jellyfin build line the mocked repository index names.
pub(crate) const STUB_JELLYFIN_LINE: &str = "latest-10.x";

/// The jellyfin version the mocked archive names carry.
pub(crate) const STUB_JELLYFIN_VERSION: &str = "7.1.1";

/// Builds the entries one archive slot packs: an executable named for the tool
/// plus its `.exe` twin.
///
/// The name matters twice. `find_os_executable` looks for `{tool_id}` and
/// `{tool_id}.exe`, and deno's permission wrapper renames the file it finds,
/// so a deno slot without either name fails the sync outright. The body names
/// the tool and the slot so two slots can never pack identical bytes.
pub(crate) fn archive_entries(tool: &str, slot: &str) -> Vec<StubEntry> {
    let body = |suffix: &str| format!("{tool} stub for {slot}{suffix}\n").into_bytes();
    vec![
        StubEntry::executable(tool, body("")),
        StubEntry::executable(format!("{tool}.exe"), body(".exe")),
    ]
}

/// Builds one archive asset with its slot's entries packed inside it.
pub(crate) fn archive_asset(
    name: &str,
    tool: &str,
    slot: &str,
    format: PayloadFormat,
) -> StubAsset {
    StubAsset { name: name.to_string(), bytes: format.pack(&archive_entries(tool, slot)) }
}

/// Builds one bare-binary asset whose bytes name the tool and the slot.
pub(crate) fn binary_asset(name: &str, tool: &str, slot: &str) -> StubAsset {
    StubAsset { name: name.to_string(), bytes: format!("{tool} stub for {slot}\n").into_bytes() }
}

/// Mounts yt-dlp's upstream traffic: one bare binary per OS slot.
///
/// yt-dlp's asset names carry no version token, so the tag reaches the URL
/// path alone.
pub(crate) async fn mount_yt_dlp(mock: &MockUpstream) {
    let assets = [
        binary_asset("yt-dlp.exe", "yt-dlp", "windows"),
        binary_asset("yt-dlp_macos", "yt-dlp", "macos"),
        binary_asset("yt-dlp_linux", "yt-dlp", "linux"),
    ];
    mock.mount_github_release("yt-dlp", "yt-dlp", STUB_YT_DLP_TAG, STUB_COMMIT_SHA, &assets).await;
}

/// Mounts deno's upstream traffic: one ZIP per OS slot.
///
/// The macOS and Linux asset names follow the host architecture, exactly as
/// the provider's own slot table does.
pub(crate) async fn mount_deno(mock: &MockUpstream) {
    let arch = host_arch_label();
    let assets = [
        archive_asset("deno-x86_64-pc-windows-msvc.zip", "deno", "windows", PayloadFormat::Zip),
        archive_asset(
            &format!("deno-{arch}-apple-darwin.zip"),
            "deno",
            "macos",
            PayloadFormat::Zip,
        ),
        archive_asset(
            &format!("deno-{arch}-unknown-linux-gnu.zip"),
            "deno",
            "linux",
            PayloadFormat::Zip,
        ),
    ];
    mock.mount_github_release("denoland", "deno", STUB_DENO_TAG, STUB_DENO_COMMIT_SHA, &assets)
        .await;
}

/// Mounts rsgain's upstream traffic: two ZIPs and one `tar.xz`.
///
/// rsgain is a single `x86_64` build per OS, so no asset name follows the host
/// architecture. Its asset names embed the version with the tag's leading `v`
/// stripped, which is the shape the provider's own rewrite builds.
pub(crate) async fn mount_rsgain(mock: &MockUpstream) {
    let version = STUB_RSGAIN_TAG.strip_prefix('v').unwrap_or(STUB_RSGAIN_TAG);
    let assets = [
        archive_asset(
            &format!("rsgain-{version}-win64.zip"),
            "rsgain",
            "windows",
            PayloadFormat::Zip,
        ),
        archive_asset(
            &format!("rsgain-{version}-macOS-x86_64.zip"),
            "rsgain",
            "macos",
            PayloadFormat::Zip,
        ),
        archive_asset(
            &format!("rsgain-{version}-Linux.tar.xz"),
            "rsgain",
            "linux",
            PayloadFormat::TarXz,
        ),
    ];
    mock.mount_github_release("complexlogic", "rsgain", STUB_RSGAIN_TAG, STUB_COMMIT_SHA, &assets)
        .await;
}

/// Mounts sd's upstream traffic: one ZIP and two `tar.gz` archives.
///
/// sd's asset names embed the tag itself, `v` included.
pub(crate) async fn mount_sd(mock: &MockUpstream) {
    let arch = host_arch_label();
    let tag = STUB_SD_TAG;
    let assets = [
        archive_asset(
            &format!("sd-{tag}-x86_64-pc-windows-msvc.zip"),
            "sd",
            "windows",
            PayloadFormat::Zip,
        ),
        archive_asset(
            &format!("sd-{tag}-{arch}-apple-darwin.tar.gz"),
            "sd",
            "macos",
            PayloadFormat::TarGz,
        ),
        archive_asset(
            &format!("sd-{tag}-x86_64-unknown-linux-gnu.tar.gz"),
            "sd",
            "linux",
            PayloadFormat::TarGz,
        ),
    ];
    mock.mount_github_release("chmln", "sd", STUB_SD_TAG, STUB_COMMIT_SHA, &assets).await;
}

/// Mounts ffmpeg's upstream traffic: the `BtbN` alias downloads, the
/// martin-riedl macOS chain, and the jellyfin macOS fallback.
///
/// Both `BtbN` candidates are mounted, so the fixture does not depend on which
/// of the shared/non-shared assets the chain prefers. Both martin-riedl
/// channels are mounted for the same reason: the chain tries the snapshot
/// permalink first, and mounting the release permalink too keeps the fixture
/// from encoding that order. The jellyfin index and both pointer files are
/// read during resolve even though the snapshot permalink serves the fetch, so
/// a fallback that is never downloaded is still resolved.
pub(crate) async fn mount_ffmpeg(mock: &MockUpstream) {
    mock.mount_github_release_list("BtbN", "FFmpeg-Builds", &[STUB_BTBN_TAG]).await;
    let assets = [
        archive_asset(
            "ffmpeg-master-latest-win64-gpl-shared.zip",
            "ffmpeg",
            "windows",
            PayloadFormat::Zip,
        ),
        archive_asset(
            "ffmpeg-master-latest-win64-gpl.zip",
            "ffmpeg",
            "windows",
            PayloadFormat::Zip,
        ),
        archive_asset(
            "ffmpeg-master-latest-linux64-gpl-shared.tar.xz",
            "ffmpeg",
            "linux",
            PayloadFormat::TarXz,
        ),
        archive_asset(
            "ffmpeg-master-latest-linux64-gpl.tar.xz",
            "ffmpeg",
            "linux",
            PayloadFormat::TarXz,
        ),
    ];
    mock.mount_github_release("BtbN", "FFmpeg-Builds", STUB_BTBN_TAG, STUB_COMMIT_SHA, &assets)
        .await;

    for (canonical, site_label) in [("aarch64", "arm64"), ("x86_64", "amd64")] {
        let payload = PayloadFormat::Zip.pack(&archive_entries("ffmpeg", canonical));
        for channel in ["snapshot", "release"] {
            mock.mount_martin_riedl(site_label, channel, STUB_MARTIN_RIEDL_BUILD_ID, &payload)
                .await;
        }
    }

    for (canonical, site_dir, site_token) in
        [("aarch64", "arm64", "macarm64"), ("x86_64", "x86_64", "mac64")]
    {
        let archive =
            format!("jellyfin-ffmpeg_{STUB_JELLYFIN_VERSION}_portable_{site_token}-gpl.tar.xz");
        let payload = PayloadFormat::TarXz.pack(&archive_entries("ffmpeg", canonical));
        mock.mount_jellyfin(STUB_JELLYFIN_LINE, site_dir, site_token, &archive, &payload).await;
    }
}

/// Mounts every managed tool's upstream traffic on one mock server.
///
/// media-tagger is absent on purpose: it is a launcher-only builtin whose
/// script is generated locally, so it has no upstream to mock.
pub(crate) async fn mount_managed_tool_upstreams(mock: &MockUpstream) {
    mount_yt_dlp(mock).await;
    mount_deno(mock).await;
    mount_rsgain(mock).await;
    mount_sd(mock).await;
    mount_ffmpeg(mock).await;
}

/// Starts a mock answering every managed tool's upstream traffic and installs
/// the provider seams pointing at it.
///
/// The returned guard holds the process environment lock for its lifetime and
/// restores every seam when it drops; the mock must outlive the guard's scope,
/// so bind both to locals rather than discarding the mock. Neither value
/// depends on the other's drop order: the guard only rewrites environment
/// variables, and the mock only stops its server.
///
/// Mounting every tool's routes serves a test that declares only some of them:
/// an unmounted route is never requested, and a test that declares a tool
/// whose route is missing fails closed under fail-fast provisioning.
///
/// # Panics
///
/// Panics when the build lacks `test-seams`, which the guard reports itself.
///
/// The returned guard holds the process environment lock, so the caller must not
/// already hold `example_isolation::env_lock()`: that mutex is not reentrant and
/// nesting it deadlocks rather than failing.
pub(crate) async fn mock_all_managed_tools() -> (MockUpstream, SeamGuard) {
    let mock = MockUpstream::start().await;
    mount_managed_tool_upstreams(&mock).await;
    let seams = mock.all_seams(mediapm::example_isolation::env_lock());
    (mock, seams)
}
