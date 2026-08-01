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
        let mut failed = false;
        for entry in std::fs::read_dir(http_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.extension().is_none_or(|ext| ext != "rs") {
                continue;
            }
            let content = std::fs::read_to_string(&path).unwrap();
            for (lineno, line) in content.lines().enumerate() {
                let trimmed = line.trim();
                // Skip comments and blank lines.
                if trimmed.is_empty() || trimmed.starts_with("//") || trimmed.starts_with("#[") {
                    continue;
                }
                // Forbidden pattern 1: `use crate::` imports.
                if trimmed.contains("use crate::") {
                    println!(
                        "cargo:error=decoupling violation in {}:{} — \
                         `use crate::` is forbidden in the HTTP module",
                        path.display(),
                        lineno + 1
                    );
                    failed = true;
                }
                // Forbidden pattern 2: `ConductorError` references.
                if trimmed.contains("ConductorError") {
                    println!(
                        "cargo:error=decoupling violation in {}:{} — \
                         `ConductorError` is forbidden in the HTTP module; \
                         use HttpClientError instead",
                        path.display(),
                        lineno + 1
                    );
                    failed = true;
                }
            }
        }
        assert!(!failed, "HTTP module decoupling violated — see errors above");
    }
}
