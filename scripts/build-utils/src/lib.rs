use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

/// Environment variable to skip completion generation (set by the build
/// script itself to prevent recursive `cargo run`).
pub const SKIP_ENV: &str = "MEDIAPM_SKIP_COMPLETION_BUILD";

/// Shells for which completions are generated.
pub const SHELLS: &[&str] = &["bash", "elvish", "fish", "powershell", "zsh"];

/// Generate shell completion scripts for `bin_name`.
///
/// Should be called from a `build.rs`. Only generates in release profile.
/// `extra_sources` may include additional source file paths (relative to
/// `CARGO_MANIFEST_DIR`) that should trigger regeneration on change.
pub fn generate_completions(bin_name: &str, extra_sources: &[&str]) {
    if env::var_os(SKIP_ENV).is_some() {
        return;
    }
    if env::var("PROFILE") != Ok("release".to_string()) {
        return;
    }
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));
    let completion_dir = out_dir.join("completions");
    let nested_target_dir = out_dir.join("completion-target");
    let manifest_path = manifest_dir.join("Cargo.toml");

    fs::create_dir_all(&completion_dir).expect("create completion dir");
    fs::create_dir_all(&nested_target_dir).expect("create nested target dir");

    println!("cargo:rerun-if-changed={}", manifest_path.display());
    println!("cargo:rerun-if-changed={}", manifest_dir.join("src/main.rs").display());
    for src in extra_sources {
        println!("cargo:rerun-if-changed={}", manifest_dir.join(src).display());
    }

    let cargo = env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());

    for shell in SHELLS {
        let output = Command::new(&cargo)
            .arg("run")
            .arg("--quiet")
            .arg("--manifest-path")
            .arg(&manifest_path)
            .arg("--bin")
            .arg(bin_name)
            .arg("--")
            .arg("completions")
            .arg(shell)
            .env(SKIP_ENV, "1")
            .env("CARGO_TARGET_DIR", &nested_target_dir)
            .current_dir(&manifest_dir)
            .output()
            .expect("run completion generation command");
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            panic!("failed generating {bin_name} completion for shell '{shell}': {stderr}");
        }
        let path = completion_dir.join(format!("{bin_name}.{shell}"));
        fs::write(&path, output.stdout).expect("write generated completion script");
    }
}
