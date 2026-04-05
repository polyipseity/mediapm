use std::error::Error;
use std::io::Read;
use std::path::Path;

const FIXED_RESOURCE_PATH: &str = "resource.txt";
const INPUT_FILE_FLAG: &str = "--input-file";

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
    use std::error::Error;
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{parse_input_file_argument, render_from_file, render_from_reader};

    fn unique_temp_dir() -> Result<PathBuf, Box<dyn Error>> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path = std::env::temp_dir().join(format!("mediapm-concat-tool-{nanos}"));
        std::fs::create_dir_all(&path)?;
        Ok(path)
    }

    #[test]
    fn render_concatenates_fixed_resource_and_stdin() -> Result<(), Box<dyn Error>> {
        let dir = unique_temp_dir()?;
        std::fs::write(dir.join("resource.txt"), "fixed\n")?;

        let mut stdin = Cursor::new("input\n");
        let rendered = render_from_reader(&dir, &mut stdin)?;

        assert_eq!(rendered, "fixed\ninput\n");
        std::fs::remove_dir_all(dir)?;
        Ok(())
    }

    #[test]
    fn render_fails_when_fixed_resource_is_missing() -> Result<(), Box<dyn Error>> {
        let dir = unique_temp_dir()?;

        let mut stdin = Cursor::new("input");
        let error = render_from_reader(&dir, &mut stdin)
            .expect_err("missing resource.txt must return an error");

        let message = error.to_string();
        assert!(
            !message.is_empty(),
            "missing fixed resource should produce a non-empty error message"
        );

        std::fs::remove_dir_all(dir)?;
        Ok(())
    }

    #[test]
    fn render_from_file_concatenates_fixed_and_file_payload() -> Result<(), Box<dyn Error>> {
        let dir = unique_temp_dir()?;
        std::fs::write(dir.join("resource.txt"), "fixed\n")?;
        let payload_path = dir.join("payload.txt");
        std::fs::write(&payload_path, "input\n")?;

        let rendered = render_from_file(&dir, &payload_path)?;
        assert_eq!(rendered, "fixed\ninput\n");

        std::fs::remove_dir_all(dir)?;
        Ok(())
    }

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
