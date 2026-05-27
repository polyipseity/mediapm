use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const SKIP_ENV: &str = "MEDIAPM_SKIP_COMPLETION_BUILD";
const BIN_NAME: &str = "mediapm-conductor";
const SHELLS: &[&str] = &["bash", "elvish", "fish", "powershell", "zsh"];

fn main() {
    if env::var_os(SKIP_ENV).is_some() {
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
    println!("cargo:rerun-if-changed={}", manifest_dir.join("src/cli.rs").display());
    println!("cargo:rerun-if-changed={}", manifest_dir.join("src/main.rs").display());

    let cargo = env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());

    for shell in SHELLS {
        let output = Command::new(&cargo)
            .arg("run")
            .arg("--quiet")
            .arg("--manifest-path")
            .arg(&manifest_path)
            .arg("--bin")
            .arg(BIN_NAME)
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
            panic!("failed generating {BIN_NAME} completion for shell '{shell}': {stderr}");
        }

        let path = completion_dir.join(format!("{BIN_NAME}.{shell}"));
        fs::write(&path, output.stdout).expect("write generated completion script");
        println!("cargo:warning=generated completion: {}", display_relative(&manifest_dir, &path));
    }
}

fn display_relative(base: &Path, path: &Path) -> String {
    path.strip_prefix(base)
        .map_or_else(|_| path.display().to_string(), |relative| relative.display().to_string())
}
