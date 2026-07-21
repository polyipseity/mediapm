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
}
