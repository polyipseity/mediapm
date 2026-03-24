use std::path::Path;

use mediapm::domain::canonical::{CanonicalUri, canonicalize_uri};

#[test]
fn canonicalizes_relative_file_path_into_file_uri() {
    let workspace = tempfile::tempdir().expect("temp workspace should create");
    let source_file = workspace.path().join("song.flac");
    std::fs::write(&source_file, b"audio-bytes").expect("source should be written");

    let uri = canonicalize_uri("song.flac", workspace.path()).expect("uri should canonicalize");

    assert!(uri.as_str().starts_with("file://"));
    let path = uri.to_file_path().expect("file uri should map to local path");
    let canonical_source =
        std::fs::canonicalize(&source_file).expect("canonical path should resolve");

    let normalize =
        |value: &std::path::Path| value.to_string_lossy().trim_start_matches(r"\\?\").to_owned();

    assert_eq!(normalize(&path), normalize(&canonical_source));
}

#[test]
fn canonicalizes_remote_url_host_and_fragment() {
    let input = "https://Example.COM/music?id=1#section";
    let cwd = Path::new(".");

    let uri = canonicalize_uri(input, cwd).expect("url should canonicalize");

    assert!(uri.as_str().starts_with("https://example.com/"));
    assert!(!uri.as_str().contains('#'));
}

#[test]
fn non_file_uri_cannot_convert_to_path() {
    let uri = CanonicalUri("https://example.com/song.flac".to_owned());
    let error = uri.to_file_path().expect_err("non-file URI should fail to convert");

    assert!(format!("{error:#}").contains("not a file URI"));
}
