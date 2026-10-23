//! Dev-only test support for hermetic upstream tests in the mediapm workspace.
//!
//! A test that exercises tool provisioning end to end needs two things a unit
//! test cannot fake: somewhere for the provider layer to fetch from, and bytes
//! for it to provision. [`MockUpstream`] supplies the first — one `wiremock`
//! server standing in for the four upstream hosts behind the provider seams —
//! and [`StubPayloadBuilder`] the second, packing caller-supplied stub bodies
//! into the per-OS containers the pipeline's extractors expect.
//!
//! The crate exists because neither consumer can host this for the other.
//! `mediapm` depends on `mediapm-conductor`, so the dependency cannot run
//! backwards, and a support module inside either crate would be unreachable
//! from the other's test binaries. It is a **dev-dependency** of both and must
//! never become a normal dependency of anything, which is also what keeps
//! `wiremock` out of a shipped binary.
//!
//! # Seams
//!
//! The provider layer reads its four base URLs from the environment only when
//! the consuming crate is built with its own `test-seams` feature. [`SeamGuard`]
//! refuses to install a seam in a build without *this* crate's `test-seams`,
//! loudly, naming the feature and the command to run instead.
//!
//! That refusal cannot see the consumer's feature. `mediapm`'s `test-seams` is
//! deliberately outside its default feature list, so a build of `mediapm`
//! without `--all-features` compiles the provider's environment reads out while
//! this crate's guard is satisfied and installs the variables anyway — the test
//! then reaches the real upstream, silently, which is the one failure mode the
//! refusal exists to prevent. **Seams are therefore only safe under
//! `--all-features`**, which is what every gate in this repository passes. The
//! compile-time half of that guarantee — a consumer-side assertion that its own
//! feature is on wherever seams are used — is Task 3's, not this crate's.
//!
//! Every guard takes its lock as a **required argument**, so no call site can
//! pick the wrong mutex by omission: `mediapm`'s tests pass
//! `mediapm::example_isolation::env_lock()`, and `mediapm-conductor`'s pass
//! [`env_lock`]. See [`seams`] for why the distinction is load-bearing.
//!
//! # A worked shape
//!
//! ```no_run
//! # async fn example() {
//! use mediapm_test_support::{MockUpstream, PayloadFormat, StubEntry, StubPayloadBuilder};
//!
//! let mock = MockUpstream::start().await;
//! let payload = StubPayloadBuilder::new("yt-dlp")
//!     .slot("linux", "x86_64", "yt-dlp_linux", PayloadFormat::Raw, &[
//!         StubEntry::executable("yt-dlp_linux", b"#!/bin/sh\nexit 0\n".as_slice()),
//!     ])
//!     .build();
//! mock.mount_github_release("yt-dlp", "yt-dlp", "2025.07.15", "c0ffee", &payload.assets()).await;
//!
//! let _seams = mock.all_seams(mediapm_test_support::env_lock());
//! // Every provider URL now resolves to the mock for this scope.
//! # }
//! ```

pub mod mock_upstream;
pub mod seams;
pub mod stub_payload;

pub use mock_upstream::MockUpstream;
pub use seams::{
    ALL_SEAMS, GITHUB_API_BASE, GITHUB_DOWNLOAD_BASE, JELLYFIN_BASE, MARTIN_RIEDL_BASE, SeamGuard,
    env_lock, lock_process_env, seam_pairs,
};
pub use stub_payload::{
    PayloadFormat, StubAsset, StubEntry, StubPayload, StubPayloadBuilder, host_arch, host_os,
};
