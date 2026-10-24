fn main() {
    let mut command = mediapm_cas_cli::command();
    build_utils::generate_completions(&mut command, "mediapm-cas");
}
