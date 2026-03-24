use std::process::Command;

fn mediapm_bin_command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_mediapm"))
}

#[test]
fn help_command_prints_usage() {
    let output = mediapm_bin_command().arg("--help").output().expect("help command should run");

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Declarative, workspace-local media reconciler"));
    assert!(stdout.contains("plan"));
    assert!(stdout.contains("sync"));
    assert!(stdout.contains("verify"));
    assert!(stdout.contains("gc"));
    assert!(stdout.contains("fmt"));
    assert!(stdout.contains("edit"));
    assert!(!stdout.contains("record-metadata-edit"));
    assert!(!stdout.contains("record-transcode"));
}

#[test]
fn invalid_workspace_fails_fast() {
    let output = mediapm_bin_command()
        .args(["--workspace", "definitely-not-a-real-workspace", "plan"])
        .output()
        .expect("plan command should run");

    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("workspace does not exist"));
}
