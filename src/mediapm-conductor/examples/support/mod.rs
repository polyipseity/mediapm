//! Shared helpers for conductor examples.

use std::error::Error;
use std::fs;
use std::path::Path;

/// Convenient result type for examples.
pub(crate) type ExampleResult<T> = Result<T, Box<dyn Error>>;

pub(crate) fn write_text_file(path: &Path, content: &str) -> ExampleResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, content)?;
    Ok(())
}
