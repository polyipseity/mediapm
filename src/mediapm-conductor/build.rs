// The HTTP-module decoupling rule lives in exactly one place, shared with
// `tests/int/http_decoupling.rs` via `include!` so the build guard and its
// test cannot drift apart. See that file for the rule itself.
include!("http_decoupling_check.rs");

fn main() {
    build_utils::generate_completions("mediapm-conductor", &["src/cli.rs"]);

    // ---------------------------------------------------------------
    // Regression check: enforce HTTP module decoupling invariant.
    //
    // The `src/http/` module must be fully self-contained — it must
    // import nothing from `crate::` and must never reference
    // `ConductorError`. This ensures the module can be extracted to a
    // standalone crate with zero code changes.
    //
    // If this check fails, the most likely cause is an accidental
    // `use crate::...` or `ConductorError` reference added to a file
    // in `src/mediapm-conductor/src/http/`.
    // ---------------------------------------------------------------
    let http_dir = std::path::Path::new("src/http");
    if http_dir.exists() {
        let violations = scan_http_decoupling_violations(http_dir);
        for violation in &violations {
            let path = violation.path.as_str();
            let line = violation.line;
            let reason = violation.reason;
            println!("cargo:error=decoupling violation in {path}:{line} — {reason}");
        }
        assert!(violations.is_empty(), "HTTP module decoupling violated — see errors above");
    }
}
