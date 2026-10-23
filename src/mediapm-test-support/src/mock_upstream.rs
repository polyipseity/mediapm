//! A local stand-in for the four upstream hosts the provider layer fetches from.
//!
//! One [`wiremock::MockServer`] answers every host's paths, because the paths
//! the four seams lead to do not overlap: the GitHub REST API is under
//! `/repos/…`, a GitHub release asset under `/{owner}/{repo}/releases/…`,
//! martin-riedl's under `/redirect/…` and `/download/…`, and jellyfin's under
//! `/files/ffmpeg/macos/…`. A test therefore points all four seams at
//! [`MockUpstream::base`] and mounts only the routes its tool resolves.
//!
//! Every mount helper writes the shape the provider actually parses, which is
//! narrower than a real upstream's response: the GitHub release document needs
//! `tag_name` and `assets[].name`, the tag-ref document needs
//! `object.type == "commit"` and `object.sha`, and the jellyfin index needs
//! `href="latest-{N}.x/"` links for the numeric line comparison.

use parking_lot::Mutex;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::seams::{self, SeamGuard};
use crate::stub_payload::StubAsset;

/// A `wiremock` server standing in for the four upstream hosts.
pub struct MockUpstream {
    /// The server every mounted route is attached to.
    server: MockServer,
}

impl MockUpstream {
    /// Starts a server with no routes mounted.
    ///
    /// The feature check runs directly rather than through a throwaway guard:
    /// the lock is non-reentrant, so a caller that already holds one and starts
    /// a second server would deadlock, and a deadlock is indistinguishable from
    /// a timeout.
    ///
    /// # Panics
    ///
    /// Panics when this crate was built without `test-seams`: a mock that
    /// answered while the seams were compiled out would leave the test
    /// fetching from the real upstream.
    pub async fn start() -> Self {
        seams::enforce_seam_feature(cfg!(feature = "test-seams"));
        Self { server: MockServer::start().await }
    }

    /// The underlying server, for a route this type does not mount.
    #[must_use]
    pub fn server(&self) -> &MockServer {
        &self.server
    }

    /// The base URL every seam is pointed at.
    #[must_use]
    pub fn base(&self) -> String {
        self.server.uri()
    }

    /// The GitHub REST API base to install as a seam.
    #[must_use]
    pub fn github_api_base(&self) -> String {
        self.server.uri()
    }

    /// The GitHub release-download base to install as a seam.
    #[must_use]
    pub fn github_download_base(&self) -> String {
        self.server.uri()
    }

    /// The martin-riedl base to install as a seam.
    #[must_use]
    pub fn martin_riedl_base(&self) -> String {
        self.server.uri()
    }

    /// The jellyfin repository base to install as a seam.
    #[must_use]
    pub fn jellyfin_base(&self) -> String {
        self.server.uri()
    }

    /// Every seam variable paired with this server's base URL.
    #[must_use]
    pub fn seam_pairs(&self) -> Vec<(&'static str, String)> {
        seams::seam_pairs(&self.server.uri())
    }

    /// Points every seam at this server under `lock`.
    ///
    /// The lock is a required parameter, so a caller that already owns a mutex
    /// names it rather than silently taking a second one; pass
    /// [`seams::env_lock`] when the consuming crate has no lock of its own.
    #[must_use]
    pub fn all_seams(&self, lock: &'static Mutex<()>) -> SeamGuard {
        SeamGuard::all_seams(lock, &self.server.uri())
    }

    /// Mounts one GitHub release: its latest endpoint, its release-by-tag
    /// document, its tag ref, and both download paths for every asset.
    ///
    /// The tag ref reports `type: "commit"`, so the provider reads `sha`
    /// directly rather than dereferencing an annotated tag.
    pub async fn mount_github_release(
        &self,
        owner: &str,
        repo: &str,
        tag: &str,
        commit_sha: &str,
        assets: &[StubAsset],
    ) {
        let base = self.server.uri();

        Mock::given(method("GET"))
            .and(path(format!("/repos/{owner}/{repo}/releases/latest")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "tag_name": tag })),
            )
            .mount(&self.server)
            .await;

        let asset_documents: Vec<serde_json::Value> = assets
            .iter()
            .map(|asset| {
                serde_json::json!({
                    "name": asset.name,
                    "browser_download_url": format!(
                        "{base}/{owner}/{repo}/releases/download/{tag}/{}",
                        asset.name
                    ),
                })
            })
            .collect();
        Mock::given(method("GET"))
            .and(path(format!("/repos/{owner}/{repo}/releases/tags/{tag}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "tag_name": tag,
                "assets": asset_documents,
            })))
            .mount(&self.server)
            .await;

        Mock::given(method("GET"))
            .and(path(format!("/repos/{owner}/{repo}/git/refs/tags/{tag}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "object": { "type": "commit", "sha": commit_sha },
            })))
            .mount(&self.server)
            .await;

        for asset in assets {
            let pinned = format!("/{owner}/{repo}/releases/download/{tag}/{}", asset.name);
            let latest = format!("/{owner}/{repo}/releases/latest/download/{}", asset.name);
            for route in [pinned, latest] {
                Mock::given(method("GET"))
                    .and(path(route))
                    .respond_with(ResponseTemplate::new(200).set_body_bytes(asset.bytes.clone()))
                    .mount(&self.server)
                    .await;
            }
        }
    }

    /// Mounts the GitHub releases list, newest first.
    ///
    /// The provider requests it as `/releases?per_page=10`; the matcher
    /// compares paths only, so the query string needs no separate route.
    pub async fn mount_github_release_list(&self, owner: &str, repo: &str, tags: &[&str]) {
        let releases: Vec<serde_json::Value> =
            tags.iter().map(|tag| serde_json::json!({ "tag_name": tag })).collect();

        Mock::given(method("GET"))
            .and(path(format!("/repos/{owner}/{repo}/releases")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!(releases)))
            .mount(&self.server)
            .await;
    }

    /// Mounts the martin-riedl macOS chain: a permalink that redirects to a
    /// versioned download, and that download itself.
    ///
    /// The redirect target is absolute and points back at this same server, so
    /// a client that follows it proves the whole seam path rather than only the
    /// first hop.
    pub async fn mount_martin_riedl(
        &self,
        arch_label: &str,
        channel: &str,
        build_id: &str,
        payload: &[u8],
    ) {
        let versioned = format!("/download/macos/{arch_label}/{build_id}/ffmpeg.zip");
        let location = format!("{}{versioned}", self.server.uri());

        Mock::given(method("GET"))
            .and(path(format!("/redirect/latest/macos/{arch_label}/{channel}/ffmpeg.zip")))
            .respond_with(ResponseTemplate::new(302).insert_header("location", location.as_str()))
            .mount(&self.server)
            .await;

        Mock::given(method("GET"))
            .and(path(versioned))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(payload.to_vec()))
            .mount(&self.server)
            .await;
    }

    /// Mounts the jellyfin macOS fallback: the directory index, one
    /// architecture's pointer file, and the archive that pointer names.
    ///
    /// The index also names a lower `latest-{N}.x` line, so a reader that picks
    /// the numerically highest line is exercised rather than merely satisfied.
    pub async fn mount_jellyfin(
        &self,
        line: &str,
        arch_dir: &str,
        file_token: &str,
        archive_name: &str,
        payload: &[u8],
    ) {
        let index = format!(
            "<html><body><pre><a href=\"latest-9.x/\">latest-9.x/</a>\n\
             <a href=\"{line}/\">{line}/</a>\n</pre></body></html>"
        );
        Mock::given(method("GET"))
            .and(path("/files/ffmpeg/macos/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(index))
            .mount(&self.server)
            .await;

        Mock::given(method("GET"))
            .and(path(format!("/files/ffmpeg/macos/{line}/{arch_dir}/{file_token}-gpl.txt")))
            .respond_with(ResponseTemplate::new(200).set_body_string(archive_name))
            .mount(&self.server)
            .await;

        Mock::given(method("GET"))
            .and(path(format!("/files/ffmpeg/macos/{line}/{arch_dir}/{archive_name}")))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(payload.to_vec()))
            .mount(&self.server)
            .await;
    }
}

#[cfg(all(test, feature = "test-seams"))]
mod tests {
    use super::*;

    /// Fetches `url` with a client that follows redirects, returning the body
    /// and the final URL the response came from.
    async fn fetch_following_redirects(url: &str) -> (Vec<u8>, String) {
        let response = reqwest::Client::new().get(url).send().await.expect("the mock answers");
        assert!(response.status().is_success(), "{url} answered {}", response.status());
        let final_url = response.url().to_string();
        let body = response.bytes().await.expect("response body").to_vec();
        (body, final_url)
    }

    /// Fetches `url` and parses the body as JSON.
    ///
    /// The workspace's `reqwest` does not enable its `json` feature, so the
    /// body is parsed here rather than through `Response::json`.
    async fn fetch_json(url: &str) -> serde_json::Value {
        let response = reqwest::get(url).await.expect("the mock answers");
        assert!(response.status().is_success(), "{url} answered {}", response.status());
        let bytes = response.bytes().await.expect("response body");
        serde_json::from_slice(&bytes).expect("the body is JSON")
    }

    /// Fetches `url` and returns the body as text.
    async fn fetch_text(url: &str) -> String {
        let response = reqwest::get(url).await.expect("the mock answers");
        assert!(response.status().is_success(), "{url} answered {}", response.status());
        response.text().await.expect("response body")
    }

    /// One asset to mount, so each test states its payload once.
    fn asset(name: &str, bytes: &[u8]) -> StubAsset {
        StubAsset { name: name.to_string(), bytes: bytes.to_vec() }
    }

    /// Every GitHub endpoint a provider requests answers with the shape the
    /// provider parses, on both the pinned and the latest download path.
    #[tokio::test]
    async fn the_mock_answers_every_github_endpoint_a_provider_requests() {
        let mock = MockUpstream::start().await;
        mock.mount_github_release(
            "yt-dlp",
            "yt-dlp",
            "2025.07.15",
            "c0ffee",
            &[asset("yt-dlp.exe", b"windows bytes"), asset("yt-dlp_linux", b"linux bytes")],
        )
        .await;
        let api = mock.github_api_base();
        let download = mock.github_download_base();

        let latest = fetch_json(&format!("{api}/repos/yt-dlp/yt-dlp/releases/latest")).await;
        assert_eq!(latest["tag_name"], "2025.07.15");

        let by_tag =
            fetch_json(&format!("{api}/repos/yt-dlp/yt-dlp/releases/tags/2025.07.15")).await;
        assert_eq!(by_tag["tag_name"], "2025.07.15");
        assert_eq!(by_tag["assets"][0]["name"], "yt-dlp.exe");
        assert_eq!(by_tag["assets"][1]["name"], "yt-dlp_linux");

        let tag_ref =
            fetch_json(&format!("{api}/repos/yt-dlp/yt-dlp/git/refs/tags/2025.07.15")).await;
        assert_eq!(tag_ref["object"]["type"], "commit");
        assert_eq!(tag_ref["object"]["sha"], "c0ffee");

        let (pinned, _) = fetch_following_redirects(&format!(
            "{download}/yt-dlp/yt-dlp/releases/download/2025.07.15/yt-dlp.exe"
        ))
        .await;
        assert_eq!(pinned, b"windows bytes");

        let (latest_asset, _) = fetch_following_redirects(&format!(
            "{download}/yt-dlp/yt-dlp/releases/latest/download/yt-dlp_linux"
        ))
        .await;
        assert_eq!(latest_asset, b"linux bytes");
    }

    /// The releases list answers, which is what the `BtbN` autobuild tag
    /// resolution reads.
    #[tokio::test]
    async fn the_mock_answers_the_releases_list_with_its_query_string() {
        let mock = MockUpstream::start().await;
        mock.mount_github_release_list(
            "BtbN",
            "FFmpeg-Builds",
            &["autobuild-2026-01-01-12-00", "autobuild-2025-12-01-12-00"],
        )
        .await;

        let releases = fetch_json(&format!(
            "{}/repos/BtbN/FFmpeg-Builds/releases?per_page=10",
            mock.github_api_base()
        ))
        .await;

        assert_eq!(releases.as_array().expect("array").len(), 2);
        assert_eq!(releases[0]["tag_name"], "autobuild-2026-01-01-12-00");
    }

    /// A permalink answers a self-directed redirect that a following client
    /// lands on, which is how the resolve probe learns the macOS leg.
    #[tokio::test]
    async fn the_mock_follows_its_own_redirect() {
        let mock = MockUpstream::start().await;
        let build_id = "1759500000_N-127260-g7bc3576910";
        mock.mount_martin_riedl("arm64", "snapshot", build_id, b"macos payload").await;

        let (body, final_url) = fetch_following_redirects(&format!(
            "{}/redirect/latest/macos/arm64/snapshot/ffmpeg.zip",
            mock.martin_riedl_base()
        ))
        .await;

        assert_eq!(body, b"macos payload");
        assert!(
            final_url.ends_with(&format!("/download/macos/arm64/{build_id}/ffmpeg.zip")),
            "the redirect must land on the versioned download, got {final_url}"
        );
        assert!(
            final_url.starts_with(&mock.base()),
            "the redirect must stay on the mock server, got {final_url}"
        );
    }

    /// The jellyfin index, pointer file and archive all answer, and the index
    /// names the requested line alongside a lower one.
    #[tokio::test]
    async fn the_mock_answers_the_jellyfin_index_pointer_and_archive() {
        let mock = MockUpstream::start().await;
        mock.mount_jellyfin(
            "latest-10.x",
            "arm64",
            "macarm64",
            "jellyfin-ffmpeg_7.1.1_portable_macarm64-gpl.tar.xz",
            b"jellyfin payload",
        )
        .await;
        let base = mock.jellyfin_base();

        let index = fetch_text(&format!("{base}/files/ffmpeg/macos/")).await;
        assert!(index.contains("href=\"latest-10.x/\""), "index must name the requested line");
        assert!(index.contains("href=\"latest-9.x/\""), "index must offer a lower line too");

        let pointer =
            fetch_text(&format!("{base}/files/ffmpeg/macos/latest-10.x/arm64/macarm64-gpl.txt"))
                .await;
        assert_eq!(pointer, "jellyfin-ffmpeg_7.1.1_portable_macarm64-gpl.tar.xz");

        let archive = reqwest::get(format!(
            "{base}/files/ffmpeg/macos/latest-10.x/arm64/jellyfin-ffmpeg_7.1.1_portable_macarm64-gpl.tar.xz"
        ))
        .await
        .expect("archive answers")
        .bytes()
        .await
        .expect("archive body");
        assert_eq!(archive.as_ref(), b"jellyfin payload");
    }

    /// Every accessor reports one base, because one server answers all four
    /// hosts' paths.
    #[test]
    fn every_seam_accessor_reports_the_same_base() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            let mock = MockUpstream::start().await;
            assert_eq!(mock.github_api_base(), mock.base());
            assert_eq!(mock.github_download_base(), mock.base());
            assert_eq!(mock.martin_riedl_base(), mock.base());
            assert_eq!(mock.jellyfin_base(), mock.base());
            assert_eq!(mock.seam_pairs().len(), seams::ALL_SEAMS.len());
            for (name, value) in mock.seam_pairs() {
                assert_eq!(value, mock.base(), "{name} must point at the mock");
            }
        });
    }

    /// Pointing every seam at the mock installs all four variables and removes
    /// them again, which is the whole scope a hermetic test runs in.
    #[test]
    fn all_seams_installs_and_restores_every_variable() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            let mock = MockUpstream::start().await;
            let base = mock.base();
            {
                let _guard = mock.all_seams(seams::env_lock());
                for name in seams::ALL_SEAMS {
                    assert_eq!(std::env::var(name).expect("seam installed"), base);
                }
            }
            for name in seams::ALL_SEAMS {
                assert!(seams::read_under_lock(name).is_none(), "{name} must be removed again");
            }
        });
    }
}

/// The refusal, which is the only behavior a feature-off build can exercise.
///
/// `MockUpstream::start` itself refuses without the feature, so every
/// route-mounting test needs it; the mock must refuse rather than serve routes
/// that no seam would ever reach.
#[cfg(all(test, not(feature = "test-seams")))]
mod without_feature {
    use super::*;

    /// A build without the feature refuses to start the mock.
    #[tokio::test]
    #[should_panic(expected = "test-seams")]
    async fn starting_the_mock_without_the_feature_refuses() {
        let _mock = MockUpstream::start().await;
    }
}
