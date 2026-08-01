fn main() {
    // Embed git hash at build time for canonical version tracking.
    let git_hash = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok().map(|s| s.trim().to_string())
            } else {
                None
            }
        })
        .unwrap_or_default();
    println!("cargo:rustc-env=MEDIAPM_GIT_HASH={git_hash}");
    // Rebuild when HEAD changes.
    println!("cargo:rerun-if-changed=.git/HEAD");

    build_utils::generate_completions("mediapm", &[]);

    // ---------------------------------------------------------------
    // Regression check: enforce HTTP client configuration consistency.
    //
    // The shared HTTP client in `src/mediapm/src/http_client.rs` must
    // use the canonical env var name and the dynamic UA format. This
    // check prevents accidental reversion to (a) the legacy env var
    // name `MEDIAPM_DOWNLOAD_TIMEOUT_SECONDS` or (b) a hardcoded
    // `"mediapm/0.0.0"` version string.
    //
    // See also conductor's build.rs for the HTTP-module decoupling
    // regression check.
    // ---------------------------------------------------------------
    let http_client_path = std::path::Path::new("src/http_client.rs");
    if http_client_path.exists() {
        let content = std::fs::read_to_string(http_client_path).unwrap();
        let mut failed = false;

        // Check 1: env var name must use MEDIAPM_HTTP_TIMEOUT_SECONDS.
        if !content.contains("MEDIAPM_HTTP_TIMEOUT_SECONDS") {
            println!(
                "cargo:error=regression in http_client.rs: env var must be \
                 `MEDIAPM_HTTP_TIMEOUT_SECONDS`, not the legacy name"
            );
            failed = true;
        }

        // Check 2: UA format must use env!("CARGO_PKG_VERSION").
        if !content.contains("env!(\"CARGO_PKG_VERSION\")") {
            println!(
                "cargo:error=regression in http_client.rs: User-Agent must use \
                 `env!(\"CARGO_PKG_VERSION\")`, not a hardcoded version string"
            );
            failed = true;
        }

        assert!(!failed, "HTTP client configuration regression detected — see errors above");
    }
}
