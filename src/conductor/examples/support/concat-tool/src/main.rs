//! Tiny example binary used by conductor example tests to concatenate one
//! fixed resource file with either stdin or an explicitly named payload file.
//!
//! The implementation is intentionally small because the surrounding conductor
//! examples care about predictable file-system behavior more than CLI feature
//! depth. The local unit tests therefore focus on deterministic resource lookup
//! and on avoiding shared temporary-directory state across parallel test runs.

use std::error::Error;
use std::io::Read;
use std::path::Path;

/// Relative path to the fixed resource that must exist in the working directory.
const FIXED_RESOURCE_PATH: &str = "resource.txt";
/// Flag name that switches payload loading from stdin to a file path.
const INPUT_FILE_FLAG: &str = "--input-file";

/// Runs the tiny concat tool and prints the rendered payload to stdout.
///
/// When `--input-file <path>` is supplied the payload is loaded from that file;
/// otherwise the payload is read from stdin. In both modes the fixed resource
/// file must exist in the current working directory.
fn main() -> Result<(), Box<dyn Error>> {
    let rendered = if let Some(input_file) = parse_input_file_argument(std::env::args().skip(1))? {
        render_from_file(std::env::current_dir()?.as_path(), Path::new(&input_file))?
    } else {
        let mut stdin = std::io::stdin();
        render_from_reader(std::env::current_dir()?.as_path(), &mut stdin)?
    };
    print!("{rendered}");
    Ok(())
}

/// Parses supported CLI arguments and returns an optional payload file path.
///
/// Supported forms:
/// - no args: read payload from stdin,
/// - `--input-file <path>`: read payload bytes from `<path>`.
fn parse_input_file_argument<I>(mut args: I) -> Result<Option<String>, Box<dyn Error>>
where
    I: Iterator<Item = String>,
{
    let Some(first) = args.next() else {
        return Ok(None);
    };

    if first != INPUT_FILE_FLAG {
        return Err(format!("unsupported argument '{first}'; expected {INPUT_FILE_FLAG}").into());
    }

    let path =
        args.next().ok_or_else(|| format!("{INPUT_FILE_FLAG} requires one path argument"))?;
    if args.next().is_some() {
        return Err("unexpected trailing arguments".to_string().into());
    }
    Ok(Some(path))
}

/// Reads fixed file content plus stdin payload and returns their concatenation.
fn render_from_reader<R: Read>(
    working_dir: &Path,
    reader: &mut R,
) -> Result<String, Box<dyn Error>> {
    let fixed = std::fs::read_to_string(working_dir.join(FIXED_RESOURCE_PATH))?;

    let mut stdin_payload = String::new();
    reader.read_to_string(&mut stdin_payload)?;

    Ok(format!("{fixed}{stdin_payload}"))
}

/// Reads fixed file content plus payload read from one explicit file path.
fn render_from_file(working_dir: &Path, payload_path: &Path) -> Result<String, Box<dyn Error>> {
    let fixed = std::fs::read_to_string(working_dir.join(FIXED_RESOURCE_PATH))?;
    let payload = std::fs::read_to_string(payload_path)?;
    Ok(format!("{fixed}{payload}"))
}

#[cfg(test)]
mod tests {
    //! Unit tests that protect the example binary's file-resolution contract.

    use std::error::Error;
    use std::io::Cursor;

    use tempfile::TempDir;

    use super::{parse_input_file_argument, render_from_file, render_from_reader};

    /// Creates an isolated working directory for one test case.
    ///
    /// The directory stays alive for the lifetime of the returned `TempDir`,
    /// which prevents parallel tests from accidentally colliding on guessed
    /// names or deleting each other's fixtures mid-assertion.
    fn unique_temp_dir() -> Result<TempDir, Box<dyn Error>> {
        tempfile::Builder::new().prefix("mediapm-concat-tool-").tempdir().map_err(Into::into)
    }

    /// Guarantees that stdin-backed rendering prepends the fixed resource file.
    #[test]
    fn render_concatenates_fixed_resource_and_stdin() -> Result<(), Box<dyn Error>> {
        let dir = unique_temp_dir()?;
        std::fs::write(dir.path().join("resource.txt"), "fixed\n")?;

        let mut stdin = Cursor::new("input\n");
        let rendered = render_from_reader(dir.path(), &mut stdin)?;

        assert_eq!(rendered, "fixed\ninput\n");
        Ok(())
    }

    /// Guarantees that a missing fixed resource produces a surfaced I/O error.
    #[test]
    fn render_fails_when_fixed_resource_is_missing() -> Result<(), Box<dyn Error>> {
        let dir = unique_temp_dir()?;

        let mut stdin = Cursor::new("input");
        let error = render_from_reader(dir.path(), &mut stdin)
            .expect_err("missing resource.txt must return an error");

        let message = error.to_string();
        assert!(
            !message.is_empty(),
            "missing fixed resource should produce a non-empty error message"
        );

        Ok(())
    }

    /// Guarantees that file-backed rendering concatenates the two file payloads
    /// from the same isolated working directory.
    #[test]
    fn render_from_file_concatenates_fixed_and_file_payload() -> Result<(), Box<dyn Error>> {
        let dir = unique_temp_dir()?;
        std::fs::write(dir.path().join("resource.txt"), "fixed\n")?;
        let payload_path = dir.path().join("payload.txt");
        std::fs::write(&payload_path, "input\n")?;

        let rendered = render_from_file(dir.path(), &payload_path)?;
        assert_eq!(rendered, "fixed\ninput\n");

        Ok(())
    }

    /// Guarantees that the tiny CLI parser accepts the supported flag forms.
    #[test]
    fn parse_input_file_argument_supports_expected_forms() -> Result<(), Box<dyn Error>> {
        let none = parse_input_file_argument(Vec::<String>::new().into_iter())?;
        assert_eq!(none, None);

        let some = parse_input_file_argument(
            vec!["--input-file".to_string(), "payload.txt".to_string()].into_iter(),
        )?;
        assert_eq!(some, Some("payload.txt".to_string()));

        Ok(())
    }
}
